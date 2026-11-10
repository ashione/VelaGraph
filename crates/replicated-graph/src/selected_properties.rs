//! Shared bounded selected-vertex property contract for all query providers.
use crate::remote::query::QueryFailure;
use rocksdb_sql_table::Value;
use serde::Serialize;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::io::{self, Write};

pub(crate) const MAX_MAP_BYTES: usize = 2048;

pub(crate) fn validate_keys(keys: &[String]) -> Result<(), QueryFailure> {
    if !(1..=8).contains(&keys.len())
        || keys
            .iter()
            .enumerate()
            .any(|(i, key)| key.is_empty() || key.len() > 64 || keys[..i].contains(key))
    {
        return Err(QueryFailure::InvalidArgument);
    }
    Ok(())
}

/// Measure tagged JSON without allocating an encoded copy. Missing properties
/// are absent entries; an explicit Null remains a tagged Value::Null.
pub(crate) fn properties_bytes(
    properties: &BTreeMap<String, Value>,
) -> Result<usize, QueryFailure> {
    measure_properties(properties)
}

fn measure_properties<K: AsRef<str> + Ord + Serialize, V: Borrow<Value> + Serialize>(
    properties: &BTreeMap<K, V>,
) -> Result<usize, QueryFailure> {
    if properties.len() > 8
        || properties.iter().any(|(key, value)| {
            key.as_ref().is_empty()
                || key.as_ref().len() > 64
                || matches!(value.borrow(), Value::Real(number) if !number.is_finite())
        })
    {
        return Err(QueryFailure::IncompleteOrCorrupt);
    }
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > MAX_MAP_BYTES.saturating_sub(self.0) {
                return Err(io::Error::other("projection map byte limit"));
            }
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, properties).map_err(|_| QueryFailure::ResourceExhausted)?;
    Ok(counter.0)
}

/// Check untrusted singleton-list maps without cloning property payloads.
pub(crate) fn valid_map(properties: &BTreeMap<String, Vec<Value>>, keys: &[String]) -> bool {
    if validate_keys(keys).is_err()
        || properties.len() > keys.len()
        || properties
            .iter()
            .any(|(key, values)| !keys.contains(key) || values.len() != 1)
    {
        return false;
    }
    measure_properties(
        &properties
            .iter()
            .map(|(key, values)| (key.as_str(), &values[0]))
            .collect::<BTreeMap<_, _>>(),
    )
    .is_ok()
}
