//! Pre-allocation wire walk, bounded by bytes, decoded allocation, and nesting depth.
//! Collection amplification is rejected before prost creates any repeated objects.
use super::wire;
use prost::{bytes::Buf, Message};
use std::marker::PhantomData;
use tonic::{
    codec::{Codec, DecodeBuf, Decoder},
    Status,
};
pub struct BoundedCodec<T, U>(PhantomData<(T, U)>);
impl<T, U> Default for BoundedCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
pub trait BoundedMessage {
    fn schema() -> usize;
}
pub struct BoundedDecoder<U>(PhantomData<U>);
impl<T: Message + Send + 'static, U: Message + Default + Send + BoundedMessage + 'static> Codec
    for BoundedCodec<T, U>
{
    type Encode = T;
    type Decode = U;
    type Encoder = tonic_prost::ProstEncoder<T>;
    type Decoder = BoundedDecoder<U>;
    fn encoder(&mut self) -> Self::Encoder {
        Default::default()
    }
    fn decoder(&mut self) -> Self::Decoder {
        BoundedDecoder(PhantomData)
    }
}
impl<U: Message + Default + BoundedMessage> Decoder for BoundedDecoder<U> {
    type Item = U;
    type Error = Status;
    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<U>, Status> {
        if buf.chunk().len() != buf.remaining() {
            return Err(bad());
        }
        check::<U>(buf.chunk())?;
        U::decode(buf).map(Some).map_err(|_| bad())
    }
}
#[derive(Clone, Copy)]
enum Kind {
    Varint,
    Bool,
    Bytes,
    Child(usize),
}
struct Field {
    tag: u32,
    kind: Kind,
    repeated: bool,
    oneof: bool,
}
include!("scan_schema.rs");
const MAX_ALLOCATION: usize = 512 * 1024 * 1024;
fn bad() -> Status {
    Status::invalid_argument("invalid raft protobuf structure")
}
fn limit() -> Status {
    Status::out_of_range("raft protobuf resource budget")
}
fn varint(bytes: &[u8], i: &mut usize) -> Result<u64, Status> {
    let mut n = 0;
    for shift in (0..70).step_by(7) {
        let b = *bytes.get(*i).ok_or_else(bad)?;
        *i += 1;
        if shift == 63 && b > 1 {
            return Err(bad());
        }
        n |= u64::from(b & 127) << shift;
        if b < 128 {
            return Ok(n);
        }
    }
    Err(bad())
}
pub(super) fn check<T: BoundedMessage>(bytes: &[u8]) -> Result<(), Status> {
    if bytes.len() > super::MAX_MESSAGE_BYTES {
        return Err(limit());
    }
    let mut budget = MAX_ALLOCATION;
    scan(T::schema(), bytes, 0, &mut budget)
}
fn scan(schema: usize, bytes: &[u8], depth: usize, budget: &mut usize) -> Result<(), Status> {
    if depth > 64 {
        return Err(limit());
    }
    *budget = budget
        .checked_sub(SIZES[schema].saturating_mul(2))
        .ok_or_else(limit)?;
    let fields = SCHEMAS[schema];
    let mut seen = 0u128;
    let mut oneof = false;
    let mut i = 0;
    while i < bytes.len() {
        let key = varint(bytes, &mut i)?;
        let tag = u32::try_from(key >> 3).map_err(|_| bad())?;
        if tag == 0 {
            return Err(bad());
        }
        let wire = key & 7;
        let field = fields.iter().enumerate().find(|(_, f)| f.tag == tag);
        if let Some((index, f)) = field {
            if !f.repeated && seen & (1u128 << index) != 0 {
                return Err(bad());
            }
            seen |= 1u128 << index;
            if f.oneof {
                if oneof {
                    return Err(bad());
                }
                oneof = true
            }
        }
        match wire {
            0 => {
                let n = varint(bytes, &mut i)?;
                if let Some((_, f)) = field {
                    if f.repeated {
                        *budget = budget.checked_sub(16).ok_or_else(limit)?;
                    }
                    match f.kind {
                        Kind::Bool if n <= 1 => {}
                        Kind::Varint => {}
                        _ => return Err(bad()),
                    }
                }
            }
            2 => {
                let len = usize::try_from(varint(bytes, &mut i)?).map_err(|_| bad())?;
                let end = i
                    .checked_add(len)
                    .filter(|&e| e <= bytes.len())
                    .ok_or_else(bad)?;
                let data = &bytes[i..end];
                i = end;
                if let Some((_, f)) = field {
                    match f.kind {
                        Kind::Child(child) => scan(child, data, depth + 1, budget)?,
                        Kind::Bytes => {
                            *budget = budget
                                .checked_sub(data.len().saturating_mul(2))
                                .ok_or_else(limit)?;
                        }
                        Kind::Varint | Kind::Bool if f.repeated => {
                            let mut j = 0;
                            while j < data.len() {
                                *budget = budget.checked_sub(16).ok_or_else(limit)?;
                                let v = varint(data, &mut j)?;
                                if matches!(f.kind, Kind::Bool) && v > 1 {
                                    return Err(bad());
                                }
                            }
                        }
                        _ => return Err(bad()),
                    }
                }
            }
            1 | 5 if field.is_none() => {
                i = i
                    .checked_add(if wire == 1 { 8 } else { 4 })
                    .filter(|&e| e <= bytes.len())
                    .ok_or_else(bad)?
            }
            _ => return Err(bad()),
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicates_and_unknown_fields() {
        let v = wire::VoteRequest {
            version: 1,
            target: 2,
            vote: Some(wire::Vote {
                term: 1,
                node: 1,
                committed: false,
            }),
            last_log: None,
        };
        let mut bytes = v.encode_to_vec();
        check::<wire::VoteRequest>(&bytes).unwrap();
        bytes.extend_from_slice(&[8, 1]);
        assert!(check::<wire::VoteRequest>(&bytes).is_err());
        let mut bytes = v.encode_to_vec();
        bytes.extend_from_slice(&[0xa0, 6, 1]);
        check::<wire::VoteRequest>(&bytes).unwrap();
    }
    #[test]
    fn repeated_empty_entries_exhaust_before_decode() {
        let bytes = [0x2a, 0].repeat(MAX_ALLOCATION / (2 * std::mem::size_of::<wire::Entry>()) + 1);
        assert_eq!(
            check::<wire::AppendRequest>(&bytes).unwrap_err().code(),
            tonic::Code::OutOfRange
        );
    }
    #[test]
    fn truncated_length_and_missing_varint_rejected() {
        for bytes in [vec![0x1a, 0xff], vec![0x1a, 9, 1], vec![0]] {
            assert!(check::<wire::VoteRequest>(&bytes).is_err());
        }
    }
}
