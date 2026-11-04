use crate::error::{Error, Result};
use crate::schema::{Column, DataType};
use crate::value::Value;

/// Key space layout:
///
/// ```text
/// meta/table/<table>                       -> JSON table schema
/// meta/index/<table>/<index>               -> JSON index schema
/// row/<table>/<encoded-pk>                 -> JSON array of row values
/// idx/<table>/<index>/<encoded-values>...  -> encoded primary key
/// ```
///
/// Table and index names are validated ASCII identifiers, so `/` is an
/// unambiguous separator and each table or index forms a single iterator
/// prefix.
pub(crate) const META_PREFIX: &[u8] = b"meta/table/";
pub(crate) const INDEX_META_PREFIX: &[u8] = b"meta/index/";
pub(crate) const ROW_PREFIX: &[u8] = b"row/";
pub(crate) const INDEX_PREFIX: &[u8] = b"idx/";

pub(crate) fn meta_key(table: &str) -> Vec<u8> {
    let mut key = META_PREFIX.to_vec();
    key.extend_from_slice(table.as_bytes());
    key
}

pub(crate) fn index_meta_prefix(table: &str) -> Vec<u8> {
    let mut prefix = INDEX_META_PREFIX.to_vec();
    prefix.extend_from_slice(table.as_bytes());
    prefix.push(b'/');
    prefix
}

pub(crate) fn index_meta_key(table: &str, index: &str) -> Vec<u8> {
    let mut key = index_meta_prefix(table);
    key.extend_from_slice(index.as_bytes());
    key
}

pub(crate) fn index_table_prefix(table: &str) -> Vec<u8> {
    let mut prefix = INDEX_PREFIX.to_vec();
    prefix.extend_from_slice(table.as_bytes());
    prefix.push(b'/');
    prefix
}

pub(crate) fn index_prefix(table: &str, index: &str) -> Vec<u8> {
    let mut prefix = index_table_prefix(table);
    prefix.extend_from_slice(index.as_bytes());
    prefix.push(b'/');
    prefix
}

pub(crate) fn row_prefix(table: &str) -> Vec<u8> {
    let mut prefix = ROW_PREFIX.to_vec();
    prefix.extend_from_slice(table.as_bytes());
    prefix.push(b'/');
    prefix
}

pub(crate) fn row_key(table: &str, primary_key: &Value) -> Result<Vec<u8>> {
    let mut key = row_prefix(table);
    key.extend_from_slice(&encode_primary_key(primary_key)?);
    Ok(key)
}

pub(crate) fn key_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    while let Some(last) = bound.last_mut() {
        if *last == 0xFF {
            bound.pop();
        } else {
            *last += 1;
            return Some(bound);
        }
    }
    None
}

/// Encode one indexed value so that lexicographic key order matches SQL value
/// order. Each non-NULL value is prefixed with a type tag; NULL sorts first and
/// is encoded as a single tag byte.
pub(crate) fn encode_index_value(column: &Column, value: &Value) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(10);
    match (column.data_type, value) {
        (_, Value::Null) => encoded.push(0),
        (DataType::Integer, Value::Integer(value)) => {
            encoded.push(1);
            encoded.extend_from_slice(&((*value as u64) ^ (1u64 << 63)).to_be_bytes());
        }
        (DataType::Real, Value::Real(value)) => {
            // SQL compares -0.0 and 0.0 as equal. Use one representation so
            // equality lookups and unique indexes agree with that comparison.
            let value = if *value == 0.0 { 0.0 } else { *value };
            let bits = value.to_bits();
            let ordered = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            encoded.push(1);
            encoded.extend_from_slice(&ordered.to_be_bytes());
        }
        (DataType::Text, Value::Text(value)) => {
            encoded.push(1);
            encoded.extend_from_slice(&encode_text(value.as_bytes()));
        }
        (DataType::Boolean, Value::Boolean(value)) => {
            encoded.push(1);
            encoded.push(u8::from(*value));
        }
        (expected, actual) => {
            return Err(Error::TypeMismatch {
                column: column.name.clone(),
                expected: expected.sql_name().to_string(),
                actual: actual.type_name().to_string(),
            })
        }
    }
    Ok(encoded)
}

/// Decode a primary key so that RocksDB's byte ordering matches SQL ordering:
/// signed integers get their sign bit flipped, and text is null-escaped with a
/// terminator so that concatenation cannot create prefix collisions.
pub(crate) fn encode_primary_key(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::Integer(value) => Ok(((*value as u64) ^ (1u64 << 63)).to_be_bytes().to_vec()),
        Value::Text(value) => Ok(encode_text(value.as_bytes())),
        other => Err(Error::InvalidPrimaryKeyValue(format!(
            "expected INT or TEXT, got {} ({other})",
            other.type_name()
        ))),
    }
}

pub(crate) fn decode_primary_key(bytes: &[u8], data_type: DataType) -> Result<Value> {
    match data_type {
        DataType::Integer => {
            if bytes.len() != 8 {
                return Err(Error::CorruptData(format!(
                    "INT primary key should be 8 bytes, got {}",
                    bytes.len()
                )));
            }
            let raw = u64::from_be_bytes(bytes.try_into().expect("length checked"));
            Ok(Value::Integer((raw ^ (1u64 << 63)) as i64))
        }
        DataType::Text => Ok(Value::Text(decode_text(bytes)?)),
        other => Err(Error::CorruptData(format!(
            "unsupported primary key type {} in catalog",
            other.sql_name()
        ))),
    }
}

fn encode_text(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(bytes.len() + 2);
    for byte in bytes {
        if *byte == 0 {
            // 0x00 0xFF represents a literal NUL; 0x00 0x00 terminates.
            encoded.extend_from_slice(&[0, 0xFF]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
    encoded
}

fn decode_text(bytes: &[u8]) -> Result<String> {
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    loop {
        let byte = *bytes
            .get(index)
            .ok_or_else(|| Error::CorruptData("unterminated TEXT primary key".to_string()))?;
        index += 1;
        if byte != 0 {
            decoded.push(byte);
            continue;
        }
        let escape = *bytes
            .get(index)
            .ok_or_else(|| Error::CorruptData("truncated TEXT primary key escape".to_string()))?;
        index += 1;
        match escape {
            0x00 => break,
            0xFF => decoded.push(0),
            other => {
                return Err(Error::CorruptData(format!(
                    "invalid TEXT primary key escape 0x{other:02X}"
                )))
            }
        }
    }
    if index != bytes.len() {
        return Err(Error::CorruptData(
            "trailing bytes after TEXT primary key terminator".to_string(),
        ));
    }
    String::from_utf8(decoded)
        .map_err(|_| Error::CorruptData("TEXT primary key is not valid UTF-8".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_keys_sort_like_sql_ints() {
        let mut values = vec![i64::MAX, 0, -1, i64::MIN, 42, -42, 1];
        let mut encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|value| encode_primary_key(&Value::Integer(*value)).unwrap())
            .collect();
        encoded.sort();
        values.sort_unstable();
        let decoded: Vec<i64> = encoded
            .iter()
            .map(
                |bytes| match decode_primary_key(bytes, DataType::Integer).unwrap() {
                    Value::Integer(value) => value,
                    other => panic!("unexpected value {other:?}"),
                },
            )
            .collect();
        assert_eq!(decoded, values);
    }

    #[test]
    fn text_keys_are_order_preserving_and_round_trip() {
        let values = [
            "",
            "\u{0}",
            "\u{0}tail",
            "a",
            "a\u{0}",
            "a\u{0}b",
            "aa",
            "ab",
            "b",
            "中文",
        ];
        let mut expected: Vec<&str> = values.to_vec();
        expected.sort_unstable();

        let mut encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|value| encode_primary_key(&Value::Text((*value).to_string())).unwrap())
            .collect();
        encoded.sort();
        let decoded: Vec<String> = encoded
            .iter()
            .map(
                |bytes| match decode_primary_key(bytes, DataType::Text).unwrap() {
                    Value::Text(value) => value,
                    other => panic!("unexpected value {other:?}"),
                },
            )
            .collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn row_keys_share_a_table_prefix() {
        let a = row_key("users", &Value::Integer(1)).unwrap();
        let b = row_key("users", &Value::Integer(2)).unwrap();
        let other = row_key("users_archive", &Value::Integer(1)).unwrap();
        let prefix = row_prefix("users");
        assert!(a.starts_with(&prefix));
        assert!(b.starts_with(&prefix));
        assert!(!other.starts_with(&prefix));
    }

    #[test]
    fn rejects_unsupported_primary_key_values() {
        assert!(encode_primary_key(&Value::Null).is_err());
        assert!(encode_primary_key(&Value::Boolean(true)).is_err());
        assert!(encode_primary_key(&Value::Real(1.5)).is_err());
    }
}

#[cfg(test)]
mod index_key_tests {
    use super::*;

    fn column(name: &str, data_type: DataType) -> Column {
        Column {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    #[test]
    fn index_values_sort_in_sql_order_and_canonicalize_zero() {
        let int = column("i", DataType::Integer);
        let real = column("r", DataType::Real);
        let text = column("t", DataType::Text);
        let boolean = column("b", DataType::Boolean);

        let mut encoded = vec![
            encode_index_value(&int, &Value::Integer(1)).unwrap(),
            encode_index_value(&int, &Value::Integer(-1)).unwrap(),
            encode_index_value(&int, &Value::Null).unwrap(),
        ];
        encoded.sort();
        assert_eq!(
            encoded,
            vec![
                encode_index_value(&int, &Value::Null).unwrap(),
                encode_index_value(&int, &Value::Integer(-1)).unwrap(),
                encode_index_value(&int, &Value::Integer(1)).unwrap(),
            ]
        );

        let mut real_values = [1.5_f64, -1.5, 0.0, -0.0, 2.0];
        real_values.sort_by(|left, right| left.partial_cmp(right).unwrap());
        let mut encoded: Vec<Vec<u8>> = real_values
            .iter()
            .map(|value| encode_index_value(&real, &Value::Real(*value)).unwrap())
            .collect();
        encoded.sort();
        assert_eq!(
            encoded,
            vec![
                encode_index_value(&real, &Value::Real(-1.5)).unwrap(),
                encode_index_value(&real, &Value::Real(-0.0)).unwrap(),
                encode_index_value(&real, &Value::Real(0.0)).unwrap(),
                encode_index_value(&real, &Value::Real(1.5)).unwrap(),
                encode_index_value(&real, &Value::Real(2.0)).unwrap(),
            ]
        );
        assert_eq!(
            encode_index_value(&real, &Value::Real(-0.0)).unwrap(),
            encode_index_value(&real, &Value::Real(0.0)).unwrap()
        );

        let mut encoded = vec![
            encode_index_value(&text, &Value::Text("b".into())).unwrap(),
            encode_index_value(&text, &Value::Text("a".into())).unwrap(),
        ];
        encoded.sort();
        assert_eq!(
            encoded,
            vec![
                encode_index_value(&text, &Value::Text("a".into())).unwrap(),
                encode_index_value(&text, &Value::Text("b".into())).unwrap(),
            ]
        );

        let mut encoded = vec![
            encode_index_value(&boolean, &Value::Boolean(true)).unwrap(),
            encode_index_value(&boolean, &Value::Boolean(false)).unwrap(),
        ];
        encoded.sort();
        assert_eq!(
            encoded,
            vec![
                encode_index_value(&boolean, &Value::Boolean(false)).unwrap(),
                encode_index_value(&boolean, &Value::Boolean(true)).unwrap(),
            ]
        );
    }

    #[test]
    fn key_upper_bound_handles_ff_suffixes() {
        assert_eq!(key_upper_bound(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(key_upper_bound(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(key_upper_bound(b"\xff"), None);
    }
}
