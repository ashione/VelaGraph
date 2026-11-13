//! Allocation-budgeted protobuf prescan before prost allocates owned messages.
use super::{wire, MAX_FRAME_BYTES as MAX_LINE};
const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
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
pub trait Root {
    fn schema() -> S;
}
pub struct BoundedDecoder<U>(PhantomData<U>);
impl<T: Message + Send + 'static, U: Message + Default + Send + Root + 'static> Codec
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
impl<U: Message + Default + Root> Decoder for BoundedDecoder<U> {
    type Item = U;
    type Error = Status;
    fn decode(&mut self, b: &mut DecodeBuf<'_>) -> Result<Option<U>, Status> {
        if b.remaining() != b.chunk().len() {
            return Err(bad());
        }
        check::<U>(b.chunk())?;
        U::decode(b).map(Some).map_err(|_| bad())
    }
}
#[derive(Clone, Copy)]
pub enum S {
    Request,
    Response,
    Identity,
    Empty,
    Write,
    WriteOp,
    Put,
    Metadata,
    WriteResult,
    Failure,
    NotLeader,
    Timeout,
    ApplicationError,
    ErrorPair,
}
#[derive(Clone, Copy)]
enum K {
    V,
    Bytes(Option<usize>),
    Child(S),
}
#[derive(Clone, Copy)]
struct F {
    tag: u32,
    kind: K,
    repeated: bool,
}
fn f(tag: u32, kind: K) -> F {
    F {
        tag,
        kind,
        repeated: false,
    }
}
fn r(tag: u32, kind: K) -> F {
    F {
        tag,
        kind,
        repeated: true,
    }
}
fn field(s: S, t: u32) -> Option<F> {
    use K::*;
    use S::*;
    Some(match (s, t) {
        (Request, 1 | 3 | 4 | 5) => f(t, V),
        (Request, 2) => f(t, Child(Identity)),
        (Request, 6) => f(t, Child(Write)),
        (Response, 1) => f(t, V),
        (Response, 2) => f(t, Child(Empty)),
        (Response, 3) => f(t, Child(Metadata)),
        (Response, 4) => f(t, Child(WriteResult)),
        (Response, 5) => f(t, Child(Failure)),
        (Identity | ErrorPair, 1 | 2) => f(t, V),
        (Metadata | WriteResult, 1..=3) => f(t, V),
        (NotLeader, 1) => f(t, V),
        (Timeout, 1) => f(t, Bytes(Some(16))),
        (Write, 1 | 2 | 4) => f(t, V),
        (Write, 3) => f(t, Bytes(Some(16))),
        (Write, 5) => r(t, Child(WriteOp)),
        (WriteOp, 1) => f(t, Child(Put)),
        (WriteOp, 2) => f(t, Bytes(None)),
        (Put, 1 | 2) => f(t, Bytes(None)),
        (Failure, 1) => f(t, Child(NotLeader)),
        (Failure, 2) => f(t, Child(ApplicationError)),
        (Failure, 3 | 4 | 5 | 7 | 8) => f(t, Child(Empty)),
        (Failure, 6) => f(t, Child(Timeout)),
        (ApplicationError, 2 | 3 | 4 | 11) => f(t, Child(ErrorPair)),
        (ApplicationError, 44) => f(t, V),
        (ApplicationError, 1..=43) => f(t, Child(Empty)),
        _ => return None,
    })
}

fn bad() -> Status {
    Status::invalid_argument("invalid typed shard operation message")
}
fn limit() -> Status {
    Status::resource_exhausted("shard operation decoded allocation limit")
}
fn varint(d: &mut &[u8]) -> Result<u64, Status> {
    let mut n = 0;
    for i in 0..10 {
        let (&b, tail) = d.split_first().ok_or_else(bad)?;
        *d = tail;
        if i == 9 && b > 1 {
            return Err(bad());
        }
        n |= u64::from(b & 127) << (i * 7);
        if b < 128 {
            return Ok(n);
        }
    }
    Err(bad())
}
fn take<'a>(d: &mut &'a [u8], n: usize) -> Result<&'a [u8], Status> {
    if n > d.len() {
        return Err(bad());
    }
    let (a, b) = d.split_at(n);
    *d = b;
    Ok(a)
}
fn bytes<'a>(d: &mut &'a [u8]) -> Result<&'a [u8], Status> {
    let n: usize = varint(d)?.try_into().map_err(|_| bad())?;
    take(d, n)
}
fn charge(left: &mut usize, n: usize) -> Result<(), Status> {
    *left = left.checked_sub(n).ok_or_else(limit)?;
    Ok(())
}
fn scan(
    s: S,
    mut d: &[u8],
    depth: usize,
    left: &mut usize,
    end: tokio::time::Instant,
) -> Result<(), Status> {
    if depth > 16 {
        return Err(limit());
    }
    let mut seen = [false; 64];
    let mut oneof = 0;
    while !d.is_empty() {
        if tokio::time::Instant::now() >= end {
            return Err(Status::deadline_exceeded("shard operation decode deadline"));
        }
        let key = varint(&mut d)?;
        let tag: u32 = (key >> 3).try_into().map_err(|_| bad())?;
        let wt = key & 7;
        if tag == 0 || tag > 0x1fff_ffff {
            return Err(bad());
        }
        let Some(f) = field(s, tag) else {
            match wt {
                0 => {
                    varint(&mut d)?;
                }
                1 => {
                    take(&mut d, 8)?;
                }
                2 => {
                    bytes(&mut d)?;
                }
                5 => {
                    take(&mut d, 4)?;
                }
                _ => return Err(bad()),
            }
            continue;
        };
        if !f.repeated && seen[f.tag as usize] {
            return Err(bad());
        }
        seen[f.tag as usize] = true;
        let choice = match s {
            S::Request => tag >= 4,
            S::Response => tag >= 2,
            S::WriteOp | S::Failure | S::ApplicationError => true,
            _ => false,
        };
        if choice {
            oneof += 1;
            if oneof > 1 {
                return Err(bad());
            }
        }
        match (f.kind, wt) {
            (K::V, 0) => {
                let value = varint(&mut d)?;
                // Prost narrows uint32/enum varints: reject high bits rather than
                // allowing a wrapped protocol version or status to become valid.
                if (matches!(s, S::Request | S::Response)
                    && tag == 1
                    && value > u64::from(u32::MAX))
                    || (matches!((s, tag), (S::WriteResult, 2) | (S::ApplicationError, 44))
                        && value > i32::MAX as u64)
                {
                    return Err(bad());
                }
                if f.repeated {
                    charge(left, 16)?;
                }
            }
            (K::Bytes(exact), 2) => {
                let b = bytes(&mut d)?;
                if exact.is_some_and(|n| n != b.len()) {
                    return Err(bad());
                }
                charge(left, b.len())?;
            }
            (K::Child(child), 2) => {
                let b = bytes(&mut d)?;
                charge(left, 256)?;
                scan(child, b, depth + 1, left, end)?;
            }
            _ => return Err(bad()),
        }
    }
    if matches!(
        s,
        S::Request | S::Response | S::WriteOp | S::Failure | S::ApplicationError
    ) && oneof != 1
    {
        return Err(bad());
    }
    Ok(())
}
pub(super) fn check<U: Root>(data: &[u8]) -> Result<(), Status> {
    if data.len() > MAX_LINE {
        return Err(limit());
    }
    let mut budget = MAX_LINE;
    scan(
        U::schema(),
        data,
        0,
        &mut budget,
        tokio::time::Instant::now() + RPC_TIMEOUT,
    )
}
impl Root for wire::Request {
    fn schema() -> S {
        S::Request
    }
}
impl Root for wire::Response {
    fn schema() -> S {
        S::Response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_prescan() {
        for d in [
            vec![],
            vec![8, 1],
            vec![8, 1, 32, 0, 40, 0],
            vec![8, 1, 8, 1, 32, 0],
            vec![0x0b],
            vec![8, 0x81, 0x80, 0x80, 0x80, 0x10, 32, 0], // wrapped uint32 version 1
            vec![50, 3, 26, 1, 0],
        ] {
            assert!(check::<wire::Request>(&d).is_err());
        }
        assert!(check::<wire::Request>(&[8, 1, 32, 0]).is_ok());
        assert!(check::<wire::Response>(&[8, 1, 18, 0]).is_ok());
    }
    #[test]
    fn allocation_and_deadline() {
        let mut left = 511;
        assert_eq!(
            scan(
                S::Write,
                &[42, 2, 18, 0, 42, 2, 18, 0],
                0,
                &mut left,
                tokio::time::Instant::now() + RPC_TIMEOUT
            )
            .unwrap_err()
            .code(),
            tonic::Code::ResourceExhausted
        );
        let mut left = MAX_LINE;
        assert_eq!(
            scan(
                S::Request,
                &[32, 0],
                0,
                &mut left,
                tokio::time::Instant::now()
            )
            .unwrap_err()
            .code(),
            tonic::Code::DeadlineExceeded
        );
    }
}
