//! Bounded selected-property materialization for prepared hierarchy pages.
use super::*;
use std::io::{self, Write};

#[cfg(test)]
pub(super) use crate::selected_properties::MAX_MAP_BYTES;
pub(super) use crate::selected_properties::{properties_bytes, validate_keys};
pub(super) const MAX_PAGE_ROWS: usize = 128;
pub(super) const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
pub(super) const STAGE_CEILING: Cost = Cost {
    bytes: 768 * 1024 * 1024,
    work: 2_000_000,
};

/// Maximum leaf count that fits the existing encoded hierarchy-batch cap.
/// Measure the actual tagged enum representation with maximal identity widths;
/// repetitions then add exactly one comma between adjacent leaves. Neither a
/// batch-sized allocation nor repeated serialization of a growing batch is needed.
pub(super) fn batch_rows(keys: &[String]) -> Result<usize, QueryFailure> {
    validate_keys(keys)?;
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > query::MAX_QUERY_TEXT_BYTES.saturating_sub(self.0) {
                return Err(io::Error::other("projection batch byte limit"));
            }
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut leaf_bytes = Counter(0);
    serde_json::to_writer(
        &mut leaf_bytes,
        &Fragment::Project {
            vertex: VertexId {
                partition: u32::MAX,
                id: u64::MAX,
            },
            keys: keys.to_vec(),
        },
    )
    .map_err(|_| QueryFailure::ResourceExhausted)?;
    let mut envelope_bytes = Counter(0);
    serde_json::to_writer(
        &mut envelope_bytes,
        &Fragment::HierarchyBatch {
            partition: u32::MAX,
            fragments: Vec::new(),
        },
    )
    .map_err(|_| QueryFailure::ResourceExhausted)?;
    let mut count = 0;
    let mut total = envelope_bytes.0;
    for rows in 1..=jobs::BATCH_ROWS {
        let additional = leaf_bytes.0 + usize::from(rows > 1);
        if additional > query::MAX_QUERY_TEXT_BYTES.saturating_sub(total) {
            break;
        }
        total += additional;
        count = rows;
    }
    if count == 0 {
        return Err(QueryFailure::ResourceExhausted);
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_rows_accounts_for_exact_envelope_and_escaped_keys() {
        let cases = [
            vec!["name".into(), "active".into(), "country".into()],
            (0..8).map(|i| format!("{i}{}", "x".repeat(63))).collect(),
            (0..8).map(|i| format!("{i}{}", "界".repeat(21))).collect(),
            (0..8).map(|i| format!("{i}{}", "\n".repeat(63))).collect(),
            (0..8)
                .map(|i| format!("{i}{}", "\u{0001}".repeat(63)))
                .collect(),
            (0..8)
                .map(|i| format!("{i}{}", "\"\\".repeat(31)))
                .collect(),
        ];
        for (index, keys) in cases.iter().enumerate() {
            let rows = batch_rows(keys).unwrap();
            let batch = |n| Fragment::HierarchyBatch {
                partition: u32::MAX,
                fragments: (0..n)
                    .map(|_| Fragment::Project {
                        vertex: VertexId {
                            partition: u32::MAX,
                            id: u64::MAX,
                        },
                        keys: keys.clone(),
                    })
                    .collect(),
            };
            assert!((1..=jobs::BATCH_ROWS).contains(&rows));
            assert!(serde_json::to_vec(&batch(rows)).unwrap().len() <= query::MAX_QUERY_TEXT_BYTES);
            assert!(batch(rows).validate().is_ok());
            if rows < jobs::BATCH_ROWS {
                assert!(
                    serde_json::to_vec(&batch(rows + 1)).unwrap().len()
                        > query::MAX_QUERY_TEXT_BYTES
                );
                assert!(batch(rows + 1).validate().is_err());
            }
            if index == 0 {
                assert_eq!(rows, jobs::BATCH_ROWS);
            } else {
                assert!(rows < jobs::BATCH_ROWS);
            }
        }
        assert!(batch_rows(&[]).is_err());
        assert!(batch_rows(&["a".into(), "a".into()]).is_err());
    }

    #[test]
    fn keys_are_bounded_unicode_strings_not_labels() {
        assert!(validate_keys(&["姓名".into(), "a,b".into()]).is_ok());
        assert!(validate_keys(&["界".repeat(21)]).is_ok());
        for keys in [
            vec![],
            vec![String::new()],
            vec!["a".into(), "a".into()],
            vec!["界".repeat(22)],
            (0..9).map(|i| i.to_string()).collect(),
        ] {
            assert!(validate_keys(&keys).is_err());
        }
    }

    #[test]
    fn map_counter_matches_encoded_bytes_and_preserves_null() {
        let properties = BTreeMap::from([
            ("姓名".into(), Value::Text("界\n\"\\".into())),
            ("null".into(), Value::Null),
            ("boolean".into(), Value::Boolean(false)),
            ("integer".into(), Value::Integer(i64::MIN)),
            ("real".into(), Value::Real(0.5)),
        ]);
        assert_eq!(
            properties_bytes(&properties).unwrap(),
            serde_json::to_vec(&properties).unwrap().len()
        );
        assert_eq!(properties.get("null"), Some(&Value::Null));
        assert_eq!(properties.get("missing"), None);
        assert_eq!(properties_bytes(&BTreeMap::new()).unwrap(), 2);
    }

    #[test]
    fn exact_map_byte_boundary_and_nonfinite_values() {
        let mut properties = BTreeMap::from([("x".into(), Value::Text(String::new()))]);
        let overhead = properties_bytes(&properties).unwrap();
        properties.insert(
            "x".into(),
            Value::Text("a".repeat(MAX_MAP_BYTES - overhead)),
        );
        assert_eq!(properties_bytes(&properties).unwrap(), MAX_MAP_BYTES);
        properties.insert(
            "x".into(),
            Value::Text("a".repeat(MAX_MAP_BYTES - overhead + 1)),
        );
        assert!(matches!(
            properties_bytes(&properties),
            Err(QueryFailure::ResourceExhausted)
        ));
        properties.insert("x".into(), Value::Text("\n".repeat(MAX_MAP_BYTES / 2)));
        assert!(properties_bytes(&properties).is_err());
        for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            properties.insert("x".into(), Value::Real(number));
            assert!(matches!(
                properties_bytes(&properties),
                Err(QueryFailure::IncompleteOrCorrupt)
            ));
        }
    }
}
