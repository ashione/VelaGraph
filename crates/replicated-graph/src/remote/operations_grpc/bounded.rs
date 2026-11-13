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
impl<T: Message + Send + Root + 'static, U: Message + Default + Send + Root + 'static> Codec
    for BoundedCodec<T, U>
{
    type Encode = T;
    type Decode = U;
    type Encoder = tonic_prost::ProstEncoder<T>;
    type Decoder = BoundedDecoder<U>;
    fn encoder(&mut self) -> Self::Encoder {
        if matches!(T::schema(), S::MetadataRequest | S::MetadataResponse) {
            tonic_prost::ProstEncoder::new(tonic::codec::BufferSettings::new(1024, 1024))
        } else {
            Default::default()
        }
    }
    fn decoder(&mut self) -> Self::Decoder {
        BoundedDecoder(PhantomData)
    }
}
impl<U: Message + Default + Root> Decoder for BoundedDecoder<U> {
    type Item = U;
    type Error = Status;
    fn buffer_settings(&self) -> tonic::codec::BufferSettings {
        if matches!(U::schema(), S::MetadataRequest | S::MetadataResponse) {
            tonic::codec::BufferSettings::new(1024, 1024)
        } else {
            Default::default()
        }
    }
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
    MetadataRequest,
    MetadataResponse,
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
        (MetadataRequest, 1 | 2 | 4 | 5) => f(t, V),
        (MetadataRequest, 3) => f(t, Child(Identity)),
        (MetadataResponse, 1 | 2) => f(t, V),
        (MetadataResponse, 3) => f(t, Child(Metadata)),
        (MetadataResponse, 4) => f(t, Child(Failure)),
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
            S::MetadataResponse => tag >= 3,
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
                if (matches!(
                    s,
                    S::Request | S::Response | S::MetadataRequest | S::MetadataResponse
                ) && tag == 1
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
        S::Request
            | S::Response
            | S::MetadataResponse
            | S::WriteOp
            | S::Failure
            | S::ApplicationError
    ) && oneof != 1
    {
        return Err(bad());
    }
    Ok(())
}
// Conservative wire proof: Request <= (1+5)+(1+10)+(1+1+22)+(1+10)+(1+10)=63.
// Response <= version/id (17) + Metadata's 3 u64 fields and envelope (35)=52.
// Largest RemoteFailure is nested ApplicationError::ErrorPair: 28+17=45.
// No string/repeated fields exist. Unknown fields still count against the cap.
pub(in crate::remote) const METADATA_MESSAGE_BYTES: usize = 512;
const METADATA_DECODE_BYTES: usize = 1024; // up to 3 nested owned structs * 256 B
pub(super) fn check<U: Root>(data: &[u8]) -> Result<(), Status> {
    let metadata = matches!(U::schema(), S::MetadataRequest | S::MetadataResponse);
    if data.len()
        > if metadata {
            METADATA_MESSAGE_BYTES
        } else {
            MAX_LINE
        }
    {
        return Err(limit());
    }
    let mut budget = if metadata {
        METADATA_DECODE_BYTES
    } else {
        MAX_LINE
    };
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

impl Root for wire::MetadataRequest {
    fn schema() -> S {
        S::MetadataRequest
    }
}
impl Root for wire::MetadataResponse {
    fn schema() -> S {
        S::MetadataResponse
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_scalar_frame_proof_golden_and_all_error_variants() {
        let request = wire::MetadataRequest {
            version: 1,
            id: 1,
            identity: Some(wire::Identity { group: 1, node: 2 }),
            shard: 7,
            timeout_millis: 1,
        };
        assert_eq!(
            request.encode_to_vec(),
            vec![8, 1, 16, 1, 26, 4, 8, 1, 16, 2, 32, 7, 40, 1]
        );
        check::<wire::MetadataRequest>(&request.encode_to_vec()).unwrap();
        let maximal = wire::MetadataRequest {
            version: u32::MAX,
            id: u64::MAX,
            identity: Some(wire::Identity {
                group: u64::MAX,
                node: u64::MAX,
            }),
            shard: u64::MAX,
            timeout_millis: u64::MAX,
        };
        assert_eq!(maximal.encoded_len(), 63); // schema maximum; version/budget also checked by server
        check::<wire::MetadataRequest>(&maximal.encode_to_vec()).unwrap();
        let response = wire::MetadataResponse {
            version: 1,
            id: 1,
            result: Some(wire::metadata_response::Result::Metadata(wire::Metadata {
                shard: 1,
                epoch: 2,
                sequence: 3,
            })),
        };
        assert_eq!(
            response.encode_to_vec(),
            vec![8, 1, 16, 1, 26, 6, 8, 1, 16, 2, 24, 3]
        );
        let maximal = wire::MetadataResponse {
            version: u32::MAX,
            id: u64::MAX,
            result: Some(wire::metadata_response::Result::Metadata(wire::Metadata {
                shard: u64::MAX,
                epoch: u64::MAX,
                sequence: u64::MAX,
            })),
        };
        assert_eq!(maximal.encoded_len(), 52);
        check::<wire::MetadataResponse>(&maximal.encode_to_vec()).unwrap();
        use d2_raft::managed_rpc::grpc::wire as managed;
        use prost::encoding::{encode_key, encode_varint, WireType};
        for tag in 1..=44 {
            let mut encoded = Vec::new();
            if tag == 44 {
                encode_key(tag, WireType::Varint, &mut encoded);
                encode_varint(1, &mut encoded);
            } else {
                encode_key(tag, WireType::LengthDelimited, &mut encoded);
                let payload = if matches!(tag, 2 | 3 | 4 | 11) {
                    managed::ErrorPair {
                        first: u64::MAX,
                        current: u64::MAX,
                    }
                    .encode_to_vec()
                } else {
                    vec![]
                };
                encode_varint(payload.len() as u64, &mut encoded);
                encoded.extend(payload);
            }
            let application = managed::ApplicationError::decode(encoded.as_slice()).unwrap();
            let failure = wire::RemoteFailure {
                kind: Some(wire::remote_failure::Kind::Application(application)),
            };
            super::super::codec::unfailure(failure.clone()).unwrap();
            let response = wire::MetadataResponse {
                version: u32::MAX,
                id: u64::MAX,
                result: Some(wire::metadata_response::Result::Error(failure)),
            };
            assert!(response.encoded_len() <= 45);
            check::<wire::MetadataResponse>(&response.encode_to_vec()).unwrap();
        }
        for error in [
            crate::remote::RemoteError::NotLeader {
                leader: Some(u64::MAX),
            },
            crate::remote::RemoteError::InvalidArgument,
            crate::remote::RemoteError::ResourceExhausted,
            crate::remote::RemoteError::Unavailable,
            crate::remote::RemoteError::Timeout {
                request_id: Some([255; 16]),
            },
            crate::remote::RemoteError::SessionUnavailable,
            crate::remote::RemoteError::Protocol,
        ] {
            let response = wire::MetadataResponse {
                version: u32::MAX,
                id: u64::MAX,
                result: Some(wire::metadata_response::Result::Error(
                    super::super::codec::failure(error),
                )),
            };
            assert!(response.encoded_len() <= 45);
            check::<wire::MetadataResponse>(&response.encode_to_vec()).unwrap();
        }
    }
    #[test]
    fn metadata_strict_frame_errors() {
        for bad in [
            vec![8, 1, 16, 1, 16, 2],
            vec![8, 1, 16, 1, 26, 0, 34, 0],
            vec![8, 0x81, 0x80, 0x80, 0x80, 0x10, 16, 1, 26, 0],
            vec![0; METADATA_MESSAGE_BYTES + 1],
        ] {
            assert!(check::<wire::MetadataResponse>(&bad).is_err());
        }
        let malformed = wire::MetadataResponse {
            version: 1,
            id: 1,
            result: Some(wire::metadata_response::Result::Error(
                wire::RemoteFailure {
                    kind: Some(wire::remote_failure::Kind::Timeout(wire::Timeout {
                        request_id: Some(vec![0; 15]),
                    })),
                },
            )),
        };
        assert!(check::<wire::MetadataResponse>(&malformed.encode_to_vec()).is_err());
    }
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
