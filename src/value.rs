use std::fmt;

use serde::{Deserialize, Serialize};

/// A typed graph property value. Variant names and serialization are stable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Boolean(bool),
}

impl fmt::Display for Value {
    /// Display quotes text to make diagnostic values unambiguous.
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

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn graph_property_wire_values_remain_stable_without_sql() {
        for (value, encoded) in [
            (Value::Null, r#""Null""#),
            (Value::Integer(-7), r#"{"Integer":-7}"#),
            (Value::Real(1.5), r#"{"Real":1.5}"#),
            (Value::Text("Ada".into()), r#"{"Text":"Ada"}"#),
            (Value::Boolean(true), r#"{"Boolean":true}"#),
        ] {
            assert_eq!(serde_json::to_string(&value).unwrap(), encoded);
            assert_eq!(serde_json::from_str::<Value>(encoded).unwrap(), value);
        }
    }
}
