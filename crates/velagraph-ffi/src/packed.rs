//! Versioned byte layout, independent of Rust object layout. See NATIVE_FFI_PACKED.md.
use crate::{
    output,
    status::{self, Result},
};
use velagraph::{ExecutionControl, GremlinResult, GremlinRow, PathElement, Value};
pub const HEADER: usize = 64;
pub const RESULT: usize = 16;
pub const ROW: usize = 24;
pub const ELEMENT: usize = 16;
const MAGIC: &[u8; 8] = b"VGQR0001";
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| status::limit("packed size overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| status::limit("packed size overflow"))
}
fn word(out: &mut [u8], at: usize, value: u64) {
    out[at..at + 8].copy_from_slice(&value.to_le_bytes());
}
pub fn encode(
    version: u64,
    metadata: Option<&[u8]>,
    results: &[GremlinResult],
    limit: usize,
    control: &ExecutionControl,
) -> Result<Vec<u8>> {
    control.check()?;
    let rows = results
        .iter()
        .try_fold(0usize, |n, r| add(n, r.rows.len()))?;
    let row_start = add(HEADER, mul(results.len(), RESULT)?)?;
    let payload_start = add(row_start, mul(rows, ROW)?)?;
    let mut size = add(payload_start, metadata.map_or(0, <[u8]>::len))?;
    for result in results {
        for row in &result.rows {
            control.check()?;
            size = add(
                size,
                match row {
                    GremlinRow::Value(Value::Text(s)) => s.len(),
                    GremlinRow::Path(p) => mul(p.len(), ELEMENT)?,
                    _ => 0,
                },
            )?;
        }
    }
    if size > limit {
        return Err(status::limit("packed result exceeds output limit"));
    }
    let mut bytes = output::allocate(size)?;
    bytes.resize(size, 0);
    bytes[..8].copy_from_slice(MAGIC);
    for (at, n) in [
        (8, version),
        (16, results.len() as u64),
        (24, rows as u64),
        (32, payload_start as u64),
        (40, metadata.map_or(0, <[u8]>::len) as u64),
        (48, u64::from(metadata.is_some())),
        (56, size as u64),
    ] {
        word(&mut bytes, at, n);
    }
    let mut payload = payload_start;
    if let Some(m) = metadata {
        bytes[payload..payload + m.len()].copy_from_slice(m);
        payload += m.len();
    }
    let mut index = 0;
    for (i, result) in results.iter().enumerate() {
        word(&mut bytes, HEADER + i * RESULT, index as u64);
        word(
            &mut bytes,
            HEADER + i * RESULT + 8,
            result.rows.len() as u64,
        );
        for row in &result.rows {
            control.check()?;
            let (tag, a, b) = match row {
                GremlinRow::Vertex(id) => (1, *id, 0),
                GremlinRow::Edge(id) => (2, *id, 0),
                GremlinRow::Value(Value::Integer(n)) => (3, *n as u64, 0),
                GremlinRow::Value(Value::Real(n)) => (4, n.to_bits(), 0),
                GremlinRow::Value(Value::Boolean(v)) => (5, u64::from(*v), 0),
                GremlinRow::Value(Value::Null) => (6, 0, 0),
                GremlinRow::Value(Value::Text(s)) => {
                    let at = payload;
                    bytes[at..at + s.len()].copy_from_slice(s.as_bytes());
                    payload += s.len();
                    (7, at as u64, s.len() as u64)
                }
                GremlinRow::Count(n) => (8, *n, 0),
                GremlinRow::Path(path) => {
                    let at = payload;
                    for element in path {
                        control.check()?;
                        let (tag, id) = match element {
                            PathElement::Vertex(id) => (1, *id),
                            PathElement::Edge(id) => (2, *id),
                        };
                        word(&mut bytes, payload, tag);
                        word(&mut bytes, payload + 8, id);
                        payload += ELEMENT;
                    }
                    (9, at as u64, path.len() as u64)
                }
            };
            let at = row_start + index * ROW;
            word(&mut bytes, at, tag);
            word(&mut bytes, at + 8, a);
            word(&mut bytes, at + 16, b);
            index += 1;
        }
    }
    control.check()?;
    Ok(bytes)
}
