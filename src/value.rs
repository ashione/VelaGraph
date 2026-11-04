use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};
use sqlparser::ast::ValueWithSpan;

use crate::error::{Error, Result};
use crate::schema::DataType;

/// A single SQL value stored in a row.
///
/// Rows are serialized as JSON arrays of tagged values, which keeps the on-disk
/// format inspectable with RocksDB tooling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Boolean(bool),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Integer(_) => "INT",
            Value::Real(_) => "REAL",
            Value::Text(_) => "TEXT",
            Value::Boolean(_) => "BOOLEAN",
        }
    }

    /// Convert a sqlparser literal into an engine value.
    pub fn from_sql_literal(literal: &ValueWithSpan) -> Result<Value> {
        use sqlparser::ast::Value as SqlValue;

        match &literal.value {
            SqlValue::Number(text, _) => {
                if text.contains(['.', 'e', 'E']) {
                    let value: f64 = text.parse().map_err(|_| {
                        Error::InvalidLiteral(format!("`{text}` is not a valid REAL"))
                    })?;
                    if !value.is_finite() {
                        return Err(Error::InvalidLiteral(format!(
                            "`{text}` is outside the supported REAL range"
                        )));
                    }
                    Ok(Value::Real(value))
                } else {
                    let value: i64 = text.parse().map_err(|_| {
                        Error::InvalidLiteral(format!(
                            "`{text}` is not a valid INT (supported range is i64)"
                        ))
                    })?;
                    Ok(Value::Integer(value))
                }
            }
            SqlValue::SingleQuotedString(text)
            | SqlValue::DoubleQuotedString(text)
            | SqlValue::EscapedStringLiteral(text)
            | SqlValue::NationalStringLiteral(text)
            | SqlValue::TripleSingleQuotedString(text)
            | SqlValue::TripleDoubleQuotedString(text) => Ok(Value::Text(text.clone())),
            SqlValue::Boolean(value) => Ok(Value::Boolean(*value)),
            SqlValue::Null => Ok(Value::Null),
            other => Err(Error::Unsupported(format!("literal `{other}`"))),
        }
    }

    /// Coerce a value to the declared column type.
    pub fn coerce_to(&self, column: &str, data_type: DataType) -> Result<Value> {
        match (data_type, self) {
            (_, Value::Null) => Ok(Value::Null),
            (DataType::Integer, Value::Integer(value)) => Ok(Value::Integer(*value)),
            (DataType::Real, Value::Integer(value)) => Ok(Value::Real(*value as f64)),
            (DataType::Real, Value::Real(value)) => {
                if value.is_finite() {
                    Ok(Value::Real(*value))
                } else {
                    Err(Error::InvalidValue(format!(
                        "column `{column}` cannot store a non-finite REAL"
                    )))
                }
            }
            (DataType::Text, Value::Text(value)) => Ok(Value::Text(value.clone())),
            (DataType::Boolean, Value::Boolean(value)) => Ok(Value::Boolean(*value)),
            (expected, actual) => Err(Error::TypeMismatch {
                column: column.to_string(),
                expected: expected.sql_name().to_string(),
                actual: actual.type_name().to_string(),
            }),
        }
    }

    /// SQL comparison. Returns `None` when either side is NULL, following SQL
    /// three-valued logic. Numeric types compare across INT/REAL.
    pub fn compare(&self, other: &Value) -> Result<Option<Ordering>> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(None),
            (Value::Integer(a), Value::Integer(b)) => Ok(Some(a.cmp(b))),
            (Value::Integer(a), Value::Real(b)) => {
                Ok(Some((*a as f64).partial_cmp(b).ok_or_else(|| {
                    Error::InvalidValue("NaN cannot be compared".into())
                })?))
            }
            (Value::Real(a), Value::Integer(b)) => {
                Ok(Some(a.partial_cmp(&(*b as f64)).ok_or_else(|| {
                    Error::InvalidValue("NaN cannot be compared".into())
                })?))
            }
            (Value::Real(a), Value::Real(b)) => {
                Ok(Some(a.partial_cmp(b).ok_or_else(|| {
                    Error::InvalidValue("NaN cannot be compared".into())
                })?))
            }
            (Value::Text(a), Value::Text(b)) => Ok(Some(a.cmp(b))),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(Some(a.cmp(b))),
            (left, right) => Err(Error::IncomparableTypes {
                left: left.type_name().to_string(),
                right: right.type_name().to_string(),
            }),
        }
    }

    /// Total ordering used by `ORDER BY`. Values of one column always share a
    /// declared type, so the incomparable fallback is only defensive.
    pub fn sort_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Integer(a), Value::Real(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Real(a), Value::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }

    /// A stable byte key used to remove duplicate projected rows for DISTINCT.
    pub fn dedupe_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        match self {
            Value::Null => key.push(0),
            Value::Integer(value) => {
                key.push(1);
                key.extend_from_slice(&value.to_be_bytes());
            }
            Value::Real(value) => {
                key.push(2);
                key.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            Value::Text(value) => {
                key.push(3);
                key.extend_from_slice(value.as_bytes());
            }
            Value::Boolean(value) => {
                key.push(4);
                key.push(u8::from(*value));
            }
        }
        key
    }

    /// Render a value as it should appear in a result table (without SQL
    /// quoting for text).
    pub fn render(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Integer(value) => value.to_string(),
            Value::Real(value) => value.to_string(),
            Value::Text(value) => value.clone(),
            Value::Boolean(value) => value.to_string(),
        }
    }
}

impl fmt::Display for Value {
    /// Display uses SQL literal syntax, which makes error messages unambiguous.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(value) => write!(f, "{value}"),
            Value::Real(value) => write!(f, "{value}"),
            Value::Text(value) => write!(f, "'{}'", value.replace('\'', "''")),
            Value::Boolean(value) => write!(f, "{value}"),
        }
    }
}
