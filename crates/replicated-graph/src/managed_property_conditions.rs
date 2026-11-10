//! Bounded, exact stored-value preconditions. These are not Gremlin predicates.
use super::{Error, Result};
use rocksdb_sql_table::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Expected presence or exact tagged property value. Stored Null is not Missing.
/// Finite Real values compare by bits, so -0.0 and +0.0 differ. Other variants
/// use their ordinary equality; Integer and Real never compare equal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum PropertyCondition {
    Missing,
    Value(Value),
}

/// All entries must match. Requires 1..=1024 entries and at most 64 KiB of
/// logical key/value bytes (8 bytes per non-text Value, 1 per Missing). The
/// gateway's separate 64 KiB encoded request limit includes the whole mutation.
pub type PropertyConditions = BTreeMap<String, PropertyCondition>;

pub(crate) fn validate(conditions: &PropertyConditions) -> Result<()> {
    if conditions.is_empty() {
        return Err(Error::InvalidConditions);
    }
    if conditions.len() > 1024 {
        return Err(Error::Capacity);
    }
    let mut bytes = 0usize;
    for (key, expected) in conditions {
        if matches!(expected, PropertyCondition::Value(Value::Real(v)) if !v.is_finite()) {
            return Err(Error::InvalidConditions);
        }
        let value_bytes = match expected {
            PropertyCondition::Missing => 1,
            PropertyCondition::Value(Value::Text(text)) => text.len(),
            PropertyCondition::Value(_) => 8,
        };
        bytes = bytes.saturating_add(key.len()).saturating_add(value_bytes);
        if bytes > 64 * 1024 {
            return Err(Error::Capacity);
        }
    }
    Ok(())
}

pub(super) fn check(
    conditions: &PropertyConditions,
    properties: &BTreeMap<String, Value>,
) -> Result<()> {
    for (key, expected) in conditions {
        let matches = match (expected, properties.get(key)) {
            (PropertyCondition::Missing, None) => true,
            (PropertyCondition::Value(Value::Real(a)), Some(Value::Real(b))) => {
                a.to_bits() == b.to_bits()
            }
            (PropertyCondition::Value(a), Some(b)) => a == b,
            _ => false,
        };
        if !matches {
            return Err(Error::PreconditionFailed);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_type_presence_and_signed_zero_matrix() {
        let cases = [
            None,
            Some(Value::Null),
            Some(Value::Integer(0)),
            Some(Value::Real(0.0)),
            Some(Value::Real(-0.0)),
            Some(Value::Text("0".into())),
            Some(Value::Boolean(false)),
        ];
        for (i, expected) in cases.iter().enumerate() {
            let conditions = BTreeMap::from([(
                "key".into(),
                expected
                    .clone()
                    .map_or(PropertyCondition::Missing, PropertyCondition::Value),
            )]);
            validate(&conditions).unwrap();
            for (j, actual) in cases.iter().enumerate() {
                let properties = actual
                    .clone()
                    .map_or_else(BTreeMap::new, |v| BTreeMap::from([("key".into(), v)]));
                assert_eq!(check(&conditions, &properties).is_ok(), i == j, "{i}/{j}");
            }
        }
        let conditions = BTreeMap::from([
            (
                "".into(),
                PropertyCondition::Value(Value::Text("exact".into())),
            ),
            ("absent".into(), PropertyCondition::Missing),
        ]);
        validate(&conditions).unwrap();
        assert!(check(
            &conditions,
            &BTreeMap::from([("".into(), Value::Text("exact".into()))])
        )
        .is_ok());
        assert!(matches!(
            check(
                &conditions,
                &BTreeMap::from([("".into(), Value::Text("Exact".into()))])
            ),
            Err(Error::PreconditionFailed)
        ));
    }

    #[test]
    fn conditions_reject_invalid_and_bound_work_before_lookup() {
        assert!(matches!(
            validate(&BTreeMap::new()),
            Err(Error::InvalidConditions)
        ));
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(
                validate(&BTreeMap::from([(
                    "x".into(),
                    PropertyCondition::Value(Value::Real(value))
                )])),
                Err(Error::InvalidConditions)
            ));
        }
        let mut conditions: PropertyConditions = (0..1024)
            .map(|i| (i.to_string(), PropertyCondition::Missing))
            .collect();
        validate(&conditions).unwrap();
        conditions.insert("extra".into(), PropertyCondition::Missing);
        assert!(matches!(validate(&conditions), Err(Error::Capacity)));
        let mut conditions = BTreeMap::from([(
            "x".into(),
            PropertyCondition::Value(Value::Text("a".repeat(64 * 1024 - 1))),
        )]);
        validate(&conditions).unwrap();
        conditions.insert(
            "x".into(),
            PropertyCondition::Value(Value::Text("a".repeat(64 * 1024))),
        );
        assert!(matches!(validate(&conditions), Err(Error::Capacity)));
    }
}
