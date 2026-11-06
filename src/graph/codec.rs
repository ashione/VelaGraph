//! Deterministic protobuf wire format for graph records.
//!
//! The schema is intentionally hand-written instead of generated: it keeps the
//! build free of `protoc`, while remaining wire-compatible with the following
//! `.proto` definitions (field numbers are part of the on-disk format):
//!
//! ```proto
//! message Record {
//!   uint32 version = 1;   // 1
//!   Kind kind = 2;        // 1 = vertex, 2 = edge
//!   bytes payload = 3;
//! }
//! message Vertex {
//!   uint64 id = 1;
//!   repeated string labels = 2;
//!   repeated Property properties = 3;
//! }
//! message Edge {
//!   uint64 id = 1;
//!   uint64 from = 2;
//!   uint64 to = 3;
//!   string label = 4;
//!   repeated Property properties = 5;
//! }
//! message Property { string name = 1; Value value = 2; }
//! message Value {
//!   oneof kind {
//!     bool   null    = 1;
//!     sint64 integer = 2;
//!     double real    = 3;
//!     string text    = 4;
//!     bool   boolean = 5;
//!   }
//! }
//! ```
//!
//! Properties are emitted in name order, so equal logical records have equal
//! bytes. Unknown fields are skipped on decode for forward compatibility; a
//! future field must not reuse an existing number or wire type.

use std::collections::BTreeMap;

use super::{Edge, Vertex};
use crate::error::{Error, Result};
use crate::value::Value;

const RECORD_VERSION: u32 = 1;
const KIND_VERTEX: u32 = 1;
const KIND_EDGE: u32 = 2;
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_LABELS: usize = 4096;
const MAX_PROPERTIES: usize = 65_536;

pub(crate) fn encode_vertex(vertex: &Vertex) -> Result<Vec<u8>> {
    if vertex.labels.len() > MAX_LABELS {
        return Err(Error::InvalidGraphProperty(format!(
            "vertex has {} labels, limit is {MAX_LABELS}",
            vertex.labels.len()
        )));
    }
    let mut payload = Vec::new();
    put_uint(&mut payload, 1, vertex.id);
    for label in &vertex.labels {
        put_bytes(&mut payload, 2, label.as_bytes());
    }
    encode_properties(&mut payload, 3, &vertex.properties)?;
    encode_record(KIND_VERTEX, payload)
}

pub(crate) fn encode_edge(edge: &Edge) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    put_uint(&mut payload, 1, edge.id);
    put_uint(&mut payload, 2, edge.from);
    put_uint(&mut payload, 3, edge.to);
    put_bytes(&mut payload, 4, edge.label.as_bytes());
    encode_properties(&mut payload, 5, &edge.properties)?;
    encode_record(KIND_EDGE, payload)
}

pub(crate) fn decode_vertex(bytes: &[u8]) -> Result<Vertex> {
    let payload = decode_record(bytes, KIND_VERTEX)?;
    decode_vertex_payload(&payload)
}

pub(crate) fn decode_edge(bytes: &[u8]) -> Result<Edge> {
    let payload = decode_record(bytes, KIND_EDGE)?;
    decode_edge_payload(&payload)
}

fn encode_record(kind: u32, payload: Vec<u8>) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(payload.len() + 16);
    put_uint(&mut out, 1, RECORD_VERSION as u64);
    put_uint(&mut out, 2, kind as u64);
    put_bytes(&mut out, 3, &payload);
    if out.len() > MAX_RECORD_BYTES {
        return Err(Error::InvalidGraphProperty(format!(
            "encoded graph record is {} bytes, limit is {MAX_RECORD_BYTES}",
            out.len()
        )));
    }
    Ok(out)
}

fn decode_record(bytes: &[u8], expected_kind: u32) -> Result<Vec<u8>> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::CorruptData(format!(
            "graph record is {} bytes, limit is {MAX_RECORD_BYTES}",
            bytes.len()
        )));
    }
    let mut reader = Reader::new(bytes);
    let mut version = None;
    let mut kind = None;
    let mut payload = None;
    while !reader.is_empty() {
        let (field, wire) = reader.key()?;
        match (field, wire) {
            (1, 0) => version = Some(reader.varint()?),
            (2, 0) => kind = Some(reader.varint()?),
            (3, 2) => payload = Some(reader.bytes()?.to_vec()),
            _ => reader.skip(wire)?,
        }
    }
    let version =
        version.ok_or_else(|| Error::CorruptData("graph record has no version".into()))?;
    if version != RECORD_VERSION as u64 {
        return Err(Error::CorruptData(format!(
            "unsupported graph record version {version}"
        )));
    }
    let kind = kind.ok_or_else(|| Error::CorruptData("graph record has no kind".into()))?;
    if kind != expected_kind as u64 {
        return Err(Error::CorruptData(format!(
            "graph record kind {kind} does not match expected {expected_kind}"
        )));
    }
    payload.ok_or_else(|| Error::CorruptData("graph record has no payload".into()))
}

fn encode_properties(
    out: &mut Vec<u8>,
    field: u32,
    properties: &BTreeMap<String, Value>,
) -> Result<()> {
    if properties.len() > MAX_PROPERTIES {
        return Err(Error::InvalidGraphProperty(format!(
            "record has {} properties, limit is {MAX_PROPERTIES}",
            properties.len()
        )));
    }
    for (name, value) in properties {
        let mut property = Vec::new();
        put_bytes(&mut property, 1, name.as_bytes());
        put_bytes(&mut property, 2, &encode_value(value));
        put_bytes(out, field, &property);
    }
    Ok(())
}

fn encode_value(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        Value::Null => put_bool(&mut out, 1, true),
        Value::Integer(value) => put_sint(&mut out, 2, *value),
        Value::Real(value) => put_double(&mut out, 3, *value),
        Value::Text(value) => put_bytes(&mut out, 4, value.as_bytes()),
        Value::Boolean(value) => put_bool(&mut out, 5, *value),
    }
    out
}

fn decode_vertex_payload(payload: &[u8]) -> Result<Vertex> {
    let mut reader = Reader::new(payload);
    let mut id = None;
    let mut labels = Vec::new();
    let mut properties = BTreeMap::new();
    while !reader.is_empty() {
        let (field, wire) = reader.key()?;
        match (field, wire) {
            (1, 0) => id = Some(reader.varint()?),
            (2, 2) => {
                if labels.len() >= MAX_LABELS {
                    return Err(Error::CorruptData(format!(
                        "vertex has more than {MAX_LABELS} labels"
                    )));
                }
                labels.push(reader.string()?.to_string());
            }
            (3, 2) => decode_property(&mut reader, &mut properties)?,
            _ => reader.skip(wire)?,
        }
    }
    Ok(Vertex {
        id: id.ok_or_else(|| Error::CorruptData("vertex has no id".into()))?,
        labels,
        properties,
    })
}

fn decode_edge_payload(payload: &[u8]) -> Result<Edge> {
    let mut reader = Reader::new(payload);
    let mut id = None;
    let mut from = None;
    let mut to = None;
    let mut label = None;
    let mut properties = BTreeMap::new();
    while !reader.is_empty() {
        let (field, wire) = reader.key()?;
        match (field, wire) {
            (1, 0) => id = Some(reader.varint()?),
            (2, 0) => from = Some(reader.varint()?),
            (3, 0) => to = Some(reader.varint()?),
            (4, 2) => label = Some(reader.string()?.to_string()),
            (5, 2) => decode_property(&mut reader, &mut properties)?,
            _ => reader.skip(wire)?,
        }
    }
    Ok(Edge {
        id: id.ok_or_else(|| Error::CorruptData("edge has no id".into()))?,
        from: from.ok_or_else(|| Error::CorruptData("edge has no source".into()))?,
        to: to.ok_or_else(|| Error::CorruptData("edge has no target".into()))?,
        label: label.ok_or_else(|| Error::CorruptData("edge has no label".into()))?,
        properties,
    })
}

fn decode_property(
    reader: &mut Reader<'_>,
    properties: &mut BTreeMap<String, Value>,
) -> Result<()> {
    if properties.len() >= MAX_PROPERTIES {
        return Err(Error::CorruptData(format!(
            "record has more than {MAX_PROPERTIES} properties"
        )));
    }
    let message = reader.bytes()?;
    let mut nested = Reader::new(message);
    let mut name = None;
    let mut value = None;
    while !nested.is_empty() {
        let (field, wire) = nested.key()?;
        match (field, wire) {
            (1, 2) => name = Some(nested.string()?.to_string()),
            (2, 2) => value = Some(decode_value(nested.bytes()?)?),
            _ => nested.skip(wire)?,
        }
    }
    let name = name.ok_or_else(|| Error::CorruptData("property has no name".into()))?;
    let value = value.ok_or_else(|| Error::CorruptData("property has no value".into()))?;
    if properties.insert(name.clone(), value).is_some() {
        return Err(Error::CorruptData(format!("duplicate property `{name}`")));
    }
    Ok(())
}

fn decode_value(bytes: &[u8]) -> Result<Value> {
    let mut reader = Reader::new(bytes);
    let mut value = None;
    while !reader.is_empty() {
        let (field, wire) = reader.key()?;
        match (field, wire) {
            (1, 0) => {
                if reader.varint()? != 0 {
                    value = Some(Value::Null);
                }
            }
            (2, 0) => value = Some(Value::Integer(reader.sint()?)),
            (3, 1) => {
                let real = reader.double()?;
                if !real.is_finite() {
                    return Err(Error::CorruptData(
                        "graph record contains a non-finite REAL".into(),
                    ));
                }
                value = Some(Value::Real(real));
            }
            (4, 2) => value = Some(Value::Text(reader.string()?.to_string())),
            (5, 0) => value = Some(Value::Boolean(reader.varint()? != 0)),
            _ => reader.skip(wire)?,
        }
    }
    value.ok_or_else(|| Error::CorruptData("value has no kind".into()))
}

fn put_key(out: &mut Vec<u8>, field: u32, wire: u8) {
    put_varint(out, u64::from(field) << 3 | u64::from(wire));
}

fn put_uint(out: &mut Vec<u8>, field: u32, value: u64) {
    put_key(out, field, 0);
    put_varint(out, value);
}

fn put_sint(out: &mut Vec<u8>, field: u32, value: i64) {
    put_key(out, field, 0);
    put_varint(out, ((value as u64) << 1) ^ ((value >> 63) as u64));
}

fn put_bool(out: &mut Vec<u8>, field: u32, value: bool) {
    put_uint(out, field, u64::from(value));
}

fn put_double(out: &mut Vec<u8>, field: u32, value: f64) {
    put_key(out, field, 1);
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, field: u32, value: &[u8]) {
    put_key(out, field, 2);
    put_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn key(&mut self) -> Result<(u32, u8)> {
        let value = self.varint()?;
        let field = value >> 3;
        let wire = (value & 0x07) as u8;
        if field == 0 || field > u64::from(u32::MAX) {
            return Err(Error::CorruptData(
                "protobuf field number is invalid".into(),
            ));
        }
        Ok((field as u32, wire))
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        for shift in (0..=63).step_by(7) {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or_else(|| Error::CorruptData("truncated varint".into()))?;
            self.offset += 1;
            if shift == 63 && byte > 1 {
                return Err(Error::CorruptData("varint exceeds 64 bits".into()));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                return Ok(value);
            }
        }
        Err(Error::CorruptData("varint exceeds 64 bits".into()))
    }

    fn sint(&mut self) -> Result<i64> {
        let value = self.varint()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    fn double(&mut self) -> Result<f64> {
        let bytes = self.take(8)?;
        Ok(f64::from_le_bytes(bytes.try_into().map_err(|_| {
            Error::CorruptData("truncated double".into())
        })?))
    }

    fn string(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.bytes()?)
            .map_err(|_| Error::CorruptData("string is not UTF-8".into()))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.varint()?;
        let len = usize::try_from(len)
            .map_err(|_| Error::CorruptData("length does not fit usize".into()))?;
        self.take(len)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| Error::CorruptData("length overflow".into()))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::CorruptData("truncated field".into()))?;
        self.offset = end;
        Ok(bytes)
    }

    fn skip(&mut self, wire: u8) -> Result<()> {
        match wire {
            0 => {
                self.varint()?;
            }
            1 => {
                self.take(8)?;
            }
            2 => {
                let len = self.varint()?;
                let len = usize::try_from(len)
                    .map_err(|_| Error::CorruptData("length does not fit usize".into()))?;
                self.take(len)?;
            }
            5 => {
                self.take(4)?;
            }
            _ => {
                return Err(Error::CorruptData(format!(
                    "unsupported protobuf wire type {wire}"
                )))
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vertex() -> Vertex {
        let mut vertex = Vertex::new(1);
        vertex.labels.push("Person".to_string());
        vertex
            .properties
            .insert("name".to_string(), Value::Text("Ada".into()));
        vertex
    }

    #[test]
    fn vertex_round_trip_is_deterministic() {
        let vertex = vertex();
        let encoded = encode_vertex(&vertex).unwrap();
        assert_eq!(decode_vertex(&encoded).unwrap(), vertex);
        assert_eq!(
            encode_vertex(&decode_vertex(&encoded).unwrap()).unwrap(),
            encoded
        );
    }

    #[test]
    fn edge_round_trip_is_deterministic() {
        let mut edge = Edge::new(7, 1, 2, "KNOWS");
        edge.properties
            .insert("since".to_string(), Value::Integer(2020));
        edge.properties
            .insert("active".to_string(), Value::Boolean(true));
        let encoded = encode_edge(&edge).unwrap();
        assert_eq!(decode_edge(&encoded).unwrap(), edge);
        assert_eq!(
            encode_edge(&decode_edge(&encoded).unwrap()).unwrap(),
            encoded
        );
    }

    #[test]
    fn property_order_does_not_change_bytes() {
        let mut first = Vertex::new(1);
        first.properties.insert("b".to_string(), Value::Integer(2));
        first.properties.insert("a".to_string(), Value::Integer(1));
        let mut second = Vertex::new(1);
        second.properties.insert("a".to_string(), Value::Integer(1));
        second.properties.insert("b".to_string(), Value::Integer(2));
        assert_eq!(
            encode_vertex(&first).unwrap(),
            encode_vertex(&second).unwrap()
        );
    }

    #[test]
    fn value_kinds_round_trip() {
        for value in [
            Value::Null,
            Value::Integer(-7),
            Value::Real(1.5),
            Value::Text("x".into()),
            Value::Boolean(false),
        ] {
            let mut vertex = Vertex::new(1);
            vertex.properties.insert("p".to_string(), value.clone());
            let encoded = encode_vertex(&vertex).unwrap();
            assert_eq!(decode_vertex(&encoded).unwrap().properties["p"], value);
        }
    }

    #[test]
    fn rejects_truncated_and_wrong_kind_records() {
        let encoded = encode_vertex(&vertex()).unwrap();
        for len in 0..encoded.len() {
            assert!(decode_vertex(&encoded[..len]).is_err());
        }
        assert!(decode_edge(&encoded).is_err());
    }

    #[test]
    fn golden_vertex_bytes_are_stable() {
        let mut vertex = Vertex::new(1);
        vertex.labels.push("A".to_string());
        vertex.properties.insert("x".to_string(), Value::Integer(1));
        assert_eq!(
            hex(&encode_vertex(&vertex).unwrap()),
            "080110011a0e08011201411a070a017812021002"
        );
    }

    #[test]
    fn golden_edge_bytes_are_stable() {
        let mut edge = Edge::new(7, 1, 2, "K");
        edge.properties
            .insert("x".to_string(), Value::Boolean(true));
        assert_eq!(
            hex(&encode_edge(&edge).unwrap()),
            "080110021a1208071001180222014b2a070a017812022801"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn unknown_fields_are_skipped() {
        let mut encoded = encode_vertex(&vertex()).unwrap();
        // Append a top-level field 9 with varint wire type.
        encoded.extend_from_slice(&[0x48, 0x2a]);
        assert_eq!(decode_vertex(&encoded).unwrap(), vertex());
    }
}
