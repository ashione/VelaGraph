//! Allocation-budgeted protobuf prescan before prost allocates owned messages.
use super::{wire, MAX_LINE, RPC_TIMEOUT};
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
    Empty,
    CapacityRequest,
    SetCapacity,
    Member,
    Initialize,
    Learner,
    Membership,
    Write,
    Key,
    Capacity,
    CapacityResponse,
    AdminResponse,
    Application,
    HistorySynced,
    Activated,
    ManagedEnabled,
    ManagedWrite,
    WriteResult,
    OptionalValue,
    NodeStatus,
    Error,
    ErrorPair,
    HistoryCursor,
    Authority,
    Cohort,
    CohortMember,
    ParticipantStatus,
}
#[derive(Clone, Copy)]
enum K {
    V,
    Bool,
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
        (Request, 1 | 2) => f(t, V),
        (Request, 3) => f(t, Child(CapacityRequest)),
        (Request, 4) => f(t, Child(SetCapacity)),
        (Request, 5) => f(t, Child(Initialize)),
        (Request, 6) => f(t, Child(Learner)),
        (Request, 7) => f(t, Child(Membership)),
        (Request, 8) => f(t, Child(Write)),
        (Request, 9 | 10) => f(t, Child(Key)),
        (Request, 11) => f(t, Child(Empty)),
        (Response, 1) => f(t, Child(CapacityResponse)),
        (Response, 2) => f(t, Child(AdminResponse)),
        (Response, 3) => f(t, Child(Empty)),
        (Response, 4) => f(t, V),
        (Response, 5) => f(t, Child(OptionalValue)),
        (Response, 6) => f(t, Child(NodeStatus)),
        (Response, 7) => f(t, Bytes(None)),
        (CapacityRequest | Activated | ErrorPair, 1 | 2) => f(t, V),
        (SetCapacity | NodeStatus, 1..=4) => f(t, V),
        (Capacity, 1..=5) => f(t, V),
        (Member, 1) => f(t, V),
        (Member, 2) => f(t, Bytes(None)),
        (Initialize, 1) => r(t, Child(Member)),
        (Learner, 1) => f(t, V),
        (Learner, 2) => f(t, Bytes(None)),
        (Learner, 3) => f(t, Bool),
        (Membership, 1) => r(t, V),
        (Write, 1 | 2) => f(t, Bytes(None)),
        (Key | OptionalValue, 1) => f(t, Bytes(None)),
        (CapacityResponse, 1) => f(t, Child(Capacity)),
        (CapacityResponse | AdminResponse, 2) => f(t, Child(Error)),
        (AdminResponse, 1) => f(t, Child(Application)),
        (Application, 1) => f(t, Child(HistorySynced)),
        (Application, 2 | 4 | 6 | 7) => f(t, V),
        (Application, 3) => f(t, Child(ParticipantStatus)),
        (Application, 5) => f(t, Child(Activated)),
        (Application, 8) => f(t, Child(ManagedEnabled)),
        (Application, 9) => f(t, Child(ManagedWrite)),
        (Application, 10) => f(t, Child(WriteResult)),
        (HistorySynced, 1) => f(t, Child(HistoryCursor)),
        (HistorySynced, 2) => f(t, V),
        (ManagedEnabled | ManagedWrite, 1) => f(t, Bytes(Some(16))),
        (ManagedEnabled, 2) => f(t, V),
        (ManagedWrite, 2..=4) => f(t, V),
        (WriteResult, 1..=3) => f(t, V),
        (Error, 2 | 3 | 4 | 11) => f(t, Child(ErrorPair)),
        (Error, 44) => f(t, V),
        (Error, 1..=43) => f(t, Child(Empty)),
        (HistoryCursor, 1 | 4) => f(t, V),
        (HistoryCursor, 2) => f(t, Child(Authority)),
        (HistoryCursor, 3) => f(t, Child(Cohort)),
        (HistoryCursor, 5) => f(t, Bytes(Some(32))),
        (Authority, 1) => f(t, V),
        (Authority, 2) => f(t, Bytes(Some(16))),
        (Cohort, 1 | 2 | 4) => f(t, V),
        (Cohort, 3) => r(t, Child(CohortMember)),
        (CohortMember, 1 | 2) => f(t, V),
        (CohortMember, 3) => f(t, Bytes(Some(16))),
        (ParticipantStatus, 1 | 2 | 4) => f(t, Child(Empty)),
        (ParticipantStatus, 3) => f(t, V),
        _ => return None,
    })
}
fn bad() -> Status {
    Status::invalid_argument("invalid typed control message")
}
fn limit() -> Status {
    Status::resource_exhausted("control decoded allocation limit")
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
fn scan_packed_ids(
    mut packed: &[u8],
    left: &mut usize,
    mut check_deadline: impl FnMut() -> Result<(), Status>,
) -> Result<(), Status> {
    let mut count = 0usize;
    while !packed.is_empty() {
        // Packed fields have no outer field boundary at which scan can check time.
        if count.is_multiple_of(128) {
            check_deadline()?;
        }
        varint(&mut packed)?;
        charge(left, 16)?;
        count += 1;
    }
    check_deadline()
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
            return Err(Status::deadline_exceeded("control decode deadline"));
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
            S::Request => tag >= 3,
            S::Response
            | S::CapacityResponse
            | S::AdminResponse
            | S::Application
            | S::Error
            | S::ParticipantStatus => true,
            _ => false,
        };
        if choice {
            oneof += 1;
            if oneof > 1 {
                return Err(bad());
            }
        }
        match (f.kind, wt) {
            (K::V | K::Bool, 0) => {
                let n = varint(&mut d)?;
                if matches!(f.kind, K::Bool) && n > 1 {
                    return Err(bad());
                }
                if f.repeated {
                    charge(left, 16)?;
                }
            }
            (K::V, 2) if f.repeated => {
                scan_packed_ids(bytes(&mut d)?, left, || {
                    if tokio::time::Instant::now() >= end {
                        Err(Status::deadline_exceeded("control decode deadline"))
                    } else {
                        Ok(())
                    }
                })?;
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
            | S::CapacityResponse
            | S::AdminResponse
            | S::Application
            | S::Error
            | S::ParticipantStatus
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
impl Root for wire::ControlRequest {
    fn schema() -> S {
        S::Request
    }
}
impl Root for wire::ControlResponse {
    fn schema() -> S {
        S::Response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_prescan_rejects_missing_duplicate_oneofs_and_malformed_fields() {
        for bytes in [
            vec![],
            vec![8, 1, 16, 1],
            vec![8, 1, 16, 1, 90, 0, 90, 0],
            vec![8, 1, 8, 1, 16, 1, 90, 0],
            vec![8, 1, 16, 1, 90, 0, 0x4a, 0],
            vec![0x80],
            vec![0x0b],
        ] {
            assert!(check::<wire::ControlRequest>(&bytes).is_err(), "{bytes:?}");
        }
        assert!(check::<wire::ControlRequest>(&[8, 1, 16, 1, 90, 0]).is_ok());
        assert!(check::<wire::ControlResponse>(&[0x1a, 0]).is_ok());
        assert!(check::<wire::ControlResponse>(&[0x1a, 0, 0x20, 1]).is_err());
    }
    #[test]
    fn packed_ids_check_deadline_during_scan_and_at_completion() {
        let packed = [1; 300];
        let mut left = MAX_LINE;
        let mut checks = 0;
        let error = scan_packed_ids(&packed, &mut left, || {
            checks += 1;
            if checks == 2 {
                Err(Status::deadline_exceeded("test expired"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(checks, 2);
        assert_eq!(MAX_LINE - left, 128 * 16);

        let mut checks = 0;
        scan_packed_ids(&packed, &mut left, || {
            checks += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(checks, 4); // Before IDs 0, 128, 256, and after the final ID.
        assert_eq!(MAX_LINE - left, (128 + packed.len()) * 16);
    }
    #[test]
    fn tiny_repeated_messages_spend_allocation_budget_before_prost() {
        let mut left = 511;
        // Each empty Member still requires an owned struct and tree-entry allowance.
        assert!(scan(
            S::Initialize,
            &[0x0a, 0, 0x0a, 0],
            0,
            &mut left,
            tokio::time::Instant::now() + RPC_TIMEOUT
        )
        .is_err());
        let mut left = MAX_LINE;
        assert!(scan(
            S::Request,
            &[8, 1, 16, 1, 90, 0],
            0,
            &mut left,
            tokio::time::Instant::now()
        )
        .is_err());
    }
}
