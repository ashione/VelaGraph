//! Allocation-free typed protobuf prescan. Singular/oneof duplication is rejected;
//! repeated fields are bounded before prost can allocate. Unknown fields are skipped.
use super::{pb, Error, MAX_FRAME_BYTES};
#[derive(Clone, Copy)]
enum Schema {
    Empty,
    Target,
    Authority,
    Member,
    Cohort,
    Participant,
    TerminalRecord,
    TerminalSegment,
    TerminalPrefix,
    ReadLimit,
    ErrorPair,
    ApplicationError,
    Identity,
    Request,
    Reply,
    RpcError,
    LogId,
    CapacityActivation,
    MemberCapacityGrant,
    HistoryCursor,
    HistoryPage,
    Put,
    WriteOp,
    D1WriteRequest,
    RetainedPrepare,
    AdmitRequest,
    ConfirmCohortCapacity,
    Begin,
    Prepared,
    Decide,
    Command,
    Journal,
    RequestRecord,
    Transaction,
    TargetRecord,
    TargetEvidence,
    RecoveredRequest,
    RequestLookup,
    HistoryStatus,
    CoordinatorSummary,
    VisibilityInfo,
    CapacityGrantStatus,
    CohortCapacity,
    Status,
    Entry,
    Snapshot,
    SnapshotWithProof,
    Value,
    Lookup,
    AuthorityEpoch,
    BoundTarget,
    HistoryPageRequest,
    SyncHistory,
    AdvanceRetained,
    TerminalRequest,
    CommandRequest,
    Prepare,
    Advance,
    Prefix,
    Read,
    Operation,
}
#[derive(Clone, Copy)]
enum Kind {
    Varint,
    Bool,
    Bytes(usize, bool),
    Child(Schema),
}
const MAX_TERMINAL_SEGMENTS: usize =
    crate::transaction_decisions::CapacityProfile::ByteBound16MiBV1
        .max_transactions()
        .div_ceil(crate::terminal_prefix::SEGMENT_TRANSACTIONS);
struct Field {
    tag: u32,
    kind: Kind,
    max: usize,
    repeated: bool,
    oneof: bool,
}
const fn f(tag: u32, kind: Kind) -> Field {
    Field {
        tag,
        kind,
        max: 1,
        repeated: false,
        oneof: false,
    }
}
const fn repeated(mut field: Field, max: usize) -> Field {
    field.max = max;
    field.repeated = true;
    field
}
const fn one(mut field: Field) -> Field {
    field.oneof = true;
    field
}
#[rustfmt::skip]
fn fields(s:Schema)->&'static [Field] {match s {
Schema::Empty=>const { &[] },
Schema::Target=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Bytes(16, true)),] },
Schema::Authority=>const { &[f(1,Kind::Varint),f(2,Kind::Bytes(16, true)),] },
Schema::Member=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Bytes(16, true)),] },
Schema::Cohort=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),repeated(f(3,Kind::Child(Schema::Member)),crate::transaction_decisions::MAX_PARTICIPANTS),f(4,Kind::Varint),] },
Schema::Participant=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Bytes(16, true)),f(4,Kind::Bytes(32, true)),] },
Schema::TerminalRecord=>const { &[f(1,Kind::Bytes(16, true)),f(2,Kind::Varint),repeated(f(3,Kind::Child(Schema::Participant)),crate::transaction_decisions::MAX_PARTICIPANTS),repeated(f(4,Kind::Bool),crate::transaction_decisions::MAX_PARTICIPANTS),f(5,Kind::Varint),] },
Schema::TerminalSegment=>const { &[repeated(f(1,Kind::Child(Schema::TerminalRecord)),crate::terminal_prefix::SEGMENT_TRANSACTIONS),] },
Schema::TerminalPrefix=>const { &[f(1,Kind::Varint),f(2,Kind::Bytes(16, true)),f(3,Kind::Child(Schema::Cohort)),f(4,Kind::Varint),repeated(f(5,Kind::Child(Schema::TerminalSegment)),MAX_TERMINAL_SEGMENTS),] },
Schema::ReadLimit=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Varint),] },
Schema::ErrorPair=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),] },
Schema::ApplicationError=>const { &[one(f(1,Kind::Child(Schema::Empty))),one(f(2,Kind::Child(Schema::ErrorPair))),one(f(3,Kind::Child(Schema::ErrorPair))),one(f(4,Kind::Child(Schema::ErrorPair))),one(f(5,Kind::Child(Schema::Empty))),one(f(6,Kind::Child(Schema::Empty))),one(f(7,Kind::Child(Schema::Empty))),one(f(8,Kind::Child(Schema::Empty))),one(f(9,Kind::Child(Schema::Empty))),one(f(10,Kind::Child(Schema::Empty))),one(f(11,Kind::Child(Schema::ErrorPair))),one(f(12,Kind::Child(Schema::Empty))),one(f(13,Kind::Child(Schema::Empty))),one(f(14,Kind::Child(Schema::Empty))),one(f(15,Kind::Child(Schema::Empty))),one(f(16,Kind::Child(Schema::Empty))),one(f(17,Kind::Child(Schema::Empty))),one(f(18,Kind::Child(Schema::Empty))),one(f(19,Kind::Child(Schema::Empty))),one(f(20,Kind::Child(Schema::Empty))),one(f(21,Kind::Child(Schema::Empty))),one(f(22,Kind::Child(Schema::Empty))),one(f(23,Kind::Child(Schema::Empty))),one(f(24,Kind::Child(Schema::Empty))),one(f(25,Kind::Child(Schema::Empty))),one(f(26,Kind::Child(Schema::Empty))),one(f(27,Kind::Child(Schema::Empty))),one(f(28,Kind::Child(Schema::Empty))),one(f(29,Kind::Child(Schema::Empty))),one(f(30,Kind::Child(Schema::Empty))),one(f(31,Kind::Child(Schema::Empty))),one(f(32,Kind::Child(Schema::Empty))),one(f(33,Kind::Child(Schema::Empty))),one(f(34,Kind::Child(Schema::Empty))),one(f(35,Kind::Child(Schema::Empty))),one(f(36,Kind::Child(Schema::Empty))),one(f(37,Kind::Child(Schema::Empty))),one(f(38,Kind::Child(Schema::Empty))),one(f(39,Kind::Child(Schema::Empty))),one(f(40,Kind::Child(Schema::Empty))),one(f(41,Kind::Child(Schema::Empty))),one(f(42,Kind::Child(Schema::Empty))),one(f(43,Kind::Child(Schema::Empty))),one(f(44,Kind::Varint)),] },
Schema::Identity=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Child(Schema::Target)),f(4,Kind::Varint),f(5,Kind::Child(Schema::Authority)),f(6,Kind::Child(Schema::Cohort)),] },
Schema::Request=>const { &[f(1,Kind::Varint),f(2,Kind::Child(Schema::Identity)),f(3,Kind::Varint),f(4,Kind::Child(Schema::Operation)),f(5,Kind::Varint),] },
Schema::Reply=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Child(Schema::Identity)),f(4,Kind::Varint),one(f(5,Kind::Child(Schema::Value))),one(f(6,Kind::Child(Schema::RpcError))),] },
Schema::RpcError=>const { &[one(f(1,Kind::Child(Schema::ApplicationError))),one(f(2,Kind::Child(Schema::Empty))),one(f(3,Kind::Child(Schema::Empty))),one(f(4,Kind::Child(Schema::Empty))),one(f(5,Kind::Child(Schema::Empty))),] },
Schema::LogId=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Varint),] },
Schema::CapacityActivation=>const { &[f(1,Kind::Bytes(32, true)),f(2,Kind::Child(Schema::LogId)),] },
Schema::MemberCapacityGrant=>const { &[f(1,Kind::Child(Schema::Member)),f(2,Kind::Child(Schema::CapacityActivation)),] },
Schema::HistoryCursor=>const { &[f(1,Kind::Varint),f(2,Kind::Child(Schema::Authority)),f(3,Kind::Child(Schema::Cohort)),f(4,Kind::Varint),f(5,Kind::Bytes(32, true)),] },
Schema::HistoryPage=>const { &[f(1,Kind::Varint),f(2,Kind::Child(Schema::HistoryCursor)),f(3,Kind::Child(Schema::HistoryCursor)),f(4,Kind::Child(Schema::HistoryCursor)),repeated(f(5,Kind::Child(Schema::Command)),crate::transaction_decisions::MAX_HISTORY_PAGE_EVENTS),] },
Schema::Put=>const { &[f(1,Kind::Bytes(MAX_FRAME_BYTES, false)),f(2,Kind::Bytes(MAX_FRAME_BYTES, false)),] },
Schema::WriteOp=>const { &[one(f(1,Kind::Child(Schema::Put))),one(f(2,Kind::Bytes(MAX_FRAME_BYTES, false))),] },
Schema::D1WriteRequest=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Bytes(16, true)),f(4,Kind::Varint),repeated(f(5,Kind::Child(Schema::WriteOp)),crate::MAX_BATCH_OPERATIONS),] },
Schema::RetainedPrepare=>const { &[f(1,Kind::Varint),f(2,Kind::Child(Schema::Authority)),f(3,Kind::Child(Schema::Participant)),f(4,Kind::Child(Schema::D1WriteRequest)),f(5,Kind::Child(Schema::HistoryCursor)),] },
Schema::AdmitRequest=>const { &[f(1,Kind::Bytes(16, true)),f(2,Kind::Bytes(crate::transaction_decisions::MAX_REQUEST_BYTES, false)),f(3,Kind::Bytes(crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES, false)),f(4,Kind::Bytes(16, true)),repeated(f(5,Kind::Child(Schema::Participant)),crate::transaction_decisions::MAX_PARTICIPANTS),] },
Schema::ConfirmCohortCapacity=>const { &[f(1,Kind::Child(Schema::Cohort)),repeated(f(2,Kind::Child(Schema::MemberCapacityGrant)),crate::transaction_decisions::MAX_PARTICIPANTS),] },
Schema::Begin=>const { &[f(1,Kind::Bytes(16, true)),repeated(f(2,Kind::Child(Schema::Participant)),crate::transaction_decisions::MAX_PARTICIPANTS),] },
Schema::Prepared=>const { &[f(1,Kind::Bytes(16, true)),f(2,Kind::Child(Schema::Participant)),] },
Schema::Decide=>const { &[f(1,Kind::Bytes(16, true)),f(2,Kind::Varint),] },
Schema::Command=>const { &[one(f(1,Kind::Child(Schema::AdmitRequest))),one(f(2,Kind::Child(Schema::Cohort))),one(f(3,Kind::Child(Schema::ConfirmCohortCapacity))),one(f(4,Kind::Child(Schema::Begin))),one(f(5,Kind::Child(Schema::Prepared))),one(f(6,Kind::Child(Schema::Decide))),] },
Schema::Journal=>const { &[f(1,Kind::Bytes(16, true)),repeated(f(2,Kind::Child(Schema::Command)),crate::capacity_policy::BYTE_MAX_HISTORY_EVENTS),] },
Schema::RequestRecord=>const { &[f(1,Kind::Bytes(16, true)),f(2,Kind::Bytes(crate::transaction_decisions::MAX_REQUEST_BYTES, false)),f(3,Kind::Bytes(crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES, false)),f(4,Kind::Bytes(16, true)),] },
Schema::Transaction=>const { &[f(1,Kind::Bytes(16, true)),f(2,Kind::Varint),repeated(f(3,Kind::Child(Schema::Participant)),crate::transaction_decisions::MAX_PARTICIPANTS),repeated(f(4,Kind::Bool),crate::transaction_decisions::MAX_PARTICIPANTS),f(5,Kind::Varint),] },
Schema::TargetRecord=>const { &[f(1,Kind::Child(Schema::Transaction)),f(2,Kind::Child(Schema::RequestRecord)),] },
Schema::TargetEvidence=>const { &[f(1,Kind::Varint),f(2,Kind::Child(Schema::HistoryCursor)),f(3,Kind::Varint),f(4,Kind::Bytes(16, true)),f(5,Kind::Child(Schema::TargetRecord)),] },
Schema::RecoveredRequest=>const { &[f(1,Kind::Child(Schema::RequestRecord)),f(2,Kind::Child(Schema::Transaction)),] },
Schema::RequestLookup=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Cohort)),f(3,Kind::Varint),f(4,Kind::Bytes(16, true)),f(5,Kind::Child(Schema::RecoveredRequest)),] },
Schema::HistoryStatus=>const { &[f(1,Kind::Child(Schema::HistoryCursor)),f(2,Kind::Varint),] },
Schema::CoordinatorSummary=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Cohort)),f(3,Kind::Varint),] },
Schema::VisibilityInfo=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Varint),f(3,Kind::Child(Schema::Cohort)),f(4,Kind::Varint),] },
Schema::CapacityGrantStatus=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Cohort)),f(3,Kind::Child(Schema::MemberCapacityGrant)),] },
Schema::CohortCapacity=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Cohort)),repeated(f(3,Kind::Child(Schema::MemberCapacityGrant)),crate::transaction_decisions::MAX_PARTICIPANTS),] },
Schema::Status=>const { &[one(f(1,Kind::Child(Schema::Empty))),one(f(2,Kind::Child(Schema::Empty))),one(f(3,Kind::Varint)),one(f(4,Kind::Child(Schema::Empty))),] },
Schema::Entry=>const { &[f(1,Kind::Bytes(MAX_FRAME_BYTES, false)),f(2,Kind::Bytes(MAX_FRAME_BYTES, false)),] },
Schema::Snapshot=>const { &[f(1,Kind::Varint),f(2,Kind::Varint),f(3,Kind::Child(Schema::Authority)),f(4,Kind::Varint),f(5,Kind::Varint),f(6,Kind::Child(Schema::Cohort)),f(7,Kind::Varint),f(8,Kind::Varint),f(9,Kind::Varint),repeated(f(10,Kind::Child(Schema::Entry)),crate::versioned_reads::MAX_READ_ROWS),] },
Schema::SnapshotWithProof=>const { &[f(1,Kind::Child(Schema::Snapshot)),f(2,Kind::Child(Schema::TerminalPrefix)),] },
Schema::Value=>const { &[one(f(1,Kind::Child(Schema::TargetEvidence))),one(f(2,Kind::Child(Schema::CapacityGrantStatus))),one(f(3,Kind::Child(Schema::CohortCapacity))),one(f(4,Kind::Child(Schema::HistoryStatus))),one(f(5,Kind::Child(Schema::HistoryPage))),one(f(6,Kind::Child(Schema::HistoryStatus))),one(f(7,Kind::Varint)),one(f(8,Kind::Child(Schema::RequestLookup))),one(f(9,Kind::Child(Schema::Journal))),one(f(10,Kind::Child(Schema::Empty))),one(f(11,Kind::Child(Schema::VisibilityInfo))),one(f(12,Kind::Child(Schema::Status))),one(f(13,Kind::Varint)),one(f(14,Kind::Child(Schema::CoordinatorSummary))),one(f(15,Kind::Child(Schema::TerminalPrefix))),one(f(16,Kind::Child(Schema::SnapshotWithProof))),] },
Schema::Lookup=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Varint),f(3,Kind::Bytes(16, true)),] },
Schema::AuthorityEpoch=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Varint),] },
Schema::BoundTarget=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Target)),f(3,Kind::Child(Schema::Cohort)),] },
Schema::HistoryPageRequest=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Varint),f(3,Kind::Child(Schema::HistoryCursor)),f(4,Kind::Child(Schema::HistoryCursor)),] },
Schema::SyncHistory=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Target)),f(3,Kind::Child(Schema::HistoryPage)),] },
Schema::AdvanceRetained=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Target)),f(3,Kind::Varint),f(4,Kind::Child(Schema::HistoryCursor)),] },
Schema::TerminalRequest=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Varint),f(3,Kind::Varint),] },
Schema::CommandRequest=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Varint),f(3,Kind::Child(Schema::Command)),] },
Schema::Prepare=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Participant)),f(3,Kind::Child(Schema::D1WriteRequest)),f(4,Kind::Child(Schema::Journal)),] },
Schema::Advance=>const { &[f(1,Kind::Child(Schema::Authority)),f(2,Kind::Child(Schema::Target)),f(3,Kind::Varint),f(4,Kind::Child(Schema::Journal)),] },
Schema::Prefix=>const { &[f(1,Kind::Child(Schema::Target)),f(2,Kind::Child(Schema::TerminalPrefix)),] },
Schema::Read=>const { &[f(1,Kind::Child(Schema::Target)),f(2,Kind::Child(Schema::ReadLimit)),f(3,Kind::Child(Schema::TerminalPrefix)),] },
Schema::Operation=>const { &[one(f(1,Kind::Child(Schema::Lookup))),one(f(2,Kind::Child(Schema::RetainedPrepare))),one(f(3,Kind::Child(Schema::BoundTarget))),one(f(4,Kind::Child(Schema::AuthorityEpoch))),one(f(5,Kind::Child(Schema::BoundTarget))),one(f(6,Kind::Child(Schema::HistoryPageRequest))),one(f(7,Kind::Child(Schema::SyncHistory))),one(f(8,Kind::Child(Schema::AdvanceRetained))),one(f(9,Kind::Child(Schema::Lookup))),one(f(10,Kind::Child(Schema::AuthorityEpoch))),one(f(11,Kind::Child(Schema::TerminalRequest))),one(f(12,Kind::Child(Schema::AuthorityEpoch))),one(f(13,Kind::Child(Schema::CommandRequest))),one(f(14,Kind::Child(Schema::Target))),one(f(15,Kind::Child(Schema::Prepare))),one(f(16,Kind::Child(Schema::Advance))),one(f(17,Kind::Child(Schema::Prefix))),one(f(18,Kind::Child(Schema::Read))),] },
}}
fn allocation(s: Schema) -> usize {
    match s {
        Schema::Empty => std::mem::size_of::<pb::Empty>(),
        Schema::Target => std::mem::size_of::<pb::Target>(),
        Schema::Authority => std::mem::size_of::<pb::Authority>(),
        Schema::Member => std::mem::size_of::<pb::Member>(),
        Schema::Cohort => std::mem::size_of::<pb::Cohort>(),
        Schema::Participant => std::mem::size_of::<pb::Participant>(),
        Schema::TerminalRecord => std::mem::size_of::<pb::TerminalRecord>(),
        Schema::TerminalSegment => std::mem::size_of::<pb::TerminalSegment>(),
        Schema::TerminalPrefix => std::mem::size_of::<pb::TerminalPrefix>(),
        Schema::ReadLimit => std::mem::size_of::<pb::ReadLimit>(),
        Schema::ErrorPair => std::mem::size_of::<pb::ErrorPair>(),
        Schema::ApplicationError => std::mem::size_of::<pb::ApplicationError>(),
        Schema::Identity => std::mem::size_of::<pb::Identity>(),
        Schema::Request => std::mem::size_of::<pb::Request>(),
        Schema::Reply => std::mem::size_of::<pb::Reply>(),
        Schema::RpcError => std::mem::size_of::<pb::RpcError>(),
        Schema::LogId => std::mem::size_of::<pb::LogId>(),
        Schema::CapacityActivation => std::mem::size_of::<pb::CapacityActivation>(),
        Schema::MemberCapacityGrant => std::mem::size_of::<pb::MemberCapacityGrant>(),
        Schema::HistoryCursor => std::mem::size_of::<pb::HistoryCursor>(),
        Schema::HistoryPage => std::mem::size_of::<pb::HistoryPage>(),
        Schema::Put => std::mem::size_of::<pb::Put>(),
        Schema::WriteOp => std::mem::size_of::<pb::WriteOp>(),
        Schema::D1WriteRequest => std::mem::size_of::<pb::D1WriteRequest>(),
        Schema::RetainedPrepare => std::mem::size_of::<pb::RetainedPrepare>(),
        Schema::AdmitRequest => std::mem::size_of::<pb::AdmitRequest>(),
        Schema::ConfirmCohortCapacity => std::mem::size_of::<pb::ConfirmCohortCapacity>(),
        Schema::Begin => std::mem::size_of::<pb::Begin>(),
        Schema::Prepared => std::mem::size_of::<pb::Prepared>(),
        Schema::Decide => std::mem::size_of::<pb::Decide>(),
        Schema::Command => std::mem::size_of::<pb::Command>(),
        Schema::Journal => std::mem::size_of::<pb::Journal>(),
        Schema::RequestRecord => std::mem::size_of::<pb::RequestRecord>(),
        Schema::Transaction => std::mem::size_of::<pb::Transaction>(),
        Schema::TargetRecord => std::mem::size_of::<pb::TargetRecord>(),
        Schema::TargetEvidence => std::mem::size_of::<pb::TargetEvidence>(),
        Schema::RecoveredRequest => std::mem::size_of::<pb::RecoveredRequest>(),
        Schema::RequestLookup => std::mem::size_of::<pb::RequestLookup>(),
        Schema::HistoryStatus => std::mem::size_of::<pb::HistoryStatus>(),
        Schema::CoordinatorSummary => std::mem::size_of::<pb::CoordinatorSummary>(),
        Schema::VisibilityInfo => std::mem::size_of::<pb::VisibilityInfo>(),
        Schema::CapacityGrantStatus => std::mem::size_of::<pb::CapacityGrantStatus>(),
        Schema::CohortCapacity => std::mem::size_of::<pb::CohortCapacity>(),
        Schema::Status => std::mem::size_of::<pb::Status>(),
        Schema::Entry => std::mem::size_of::<pb::Entry>(),
        Schema::Snapshot => std::mem::size_of::<pb::Snapshot>(),
        Schema::SnapshotWithProof => std::mem::size_of::<pb::SnapshotWithProof>(),
        Schema::Value => std::mem::size_of::<pb::Value>(),
        Schema::Lookup => std::mem::size_of::<pb::Lookup>(),
        Schema::AuthorityEpoch => std::mem::size_of::<pb::AuthorityEpoch>(),
        Schema::BoundTarget => std::mem::size_of::<pb::BoundTarget>(),
        Schema::HistoryPageRequest => std::mem::size_of::<pb::HistoryPageRequest>(),
        Schema::SyncHistory => std::mem::size_of::<pb::SyncHistory>(),
        Schema::AdvanceRetained => std::mem::size_of::<pb::AdvanceRetained>(),
        Schema::TerminalRequest => std::mem::size_of::<pb::TerminalRequest>(),
        Schema::CommandRequest => std::mem::size_of::<pb::CommandRequest>(),
        Schema::Prepare => std::mem::size_of::<pb::Prepare>(),
        Schema::Advance => std::mem::size_of::<pb::Advance>(),
        Schema::Prefix => std::mem::size_of::<pb::Prefix>(),
        Schema::Read => std::mem::size_of::<pb::Read>(),
        Schema::Operation => std::mem::size_of::<pb::Operation>(),
    }
}
fn varint(data: &mut &[u8]) -> Result<u64, Error> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = data.split_first().ok_or(Error::Protocol)?;
        *data = rest;
        if shift == 63 && byte > 1 {
            return Err(Error::Protocol);
        }
        value |= u64::from(byte & 127) << shift;
        if byte < 128 {
            return Ok(value);
        }
    }
    Err(Error::Protocol)
}
fn take<'a>(data: &mut &'a [u8], n: usize) -> Result<&'a [u8], Error> {
    if n > data.len() {
        return Err(Error::Protocol);
    }
    let (value, rest) = data.split_at(n);
    *data = rest;
    Ok(value)
}
fn delimited<'a>(data: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let n = usize::try_from(varint(data)?).map_err(|_| Error::Capacity)?;
    take(data, n)
}
fn charge(budget: &mut usize, n: usize) -> Result<(), Error> {
    *budget = budget.checked_sub(n).ok_or(Error::Capacity)?;
    Ok(())
}
fn scan(schema: Schema, mut data: &[u8], depth: usize, budget: &mut usize) -> Result<(), Error> {
    if depth > 32 {
        return Err(Error::Capacity);
    }
    charge(budget, allocation(schema))?;
    let spec = fields(schema);
    let mut counts = [0usize; 64];
    let mut oneof = false;
    while !data.is_empty() {
        let key = varint(&mut data)?;
        let tag = u32::try_from(key >> 3).map_err(|_| Error::Protocol)?;
        let wt = key & 7;
        if tag == 0 || tag > 0x1fff_ffff {
            return Err(Error::Protocol);
        }
        let Some((index, field)) = spec.iter().enumerate().find(|(_, f)| f.tag == tag) else {
            match wt {
                0 => {
                    varint(&mut data)?;
                }
                1 => {
                    take(&mut data, 8)?;
                }
                2 => {
                    delimited(&mut data)?;
                }
                5 => {
                    take(&mut data, 4)?;
                }
                _ => return Err(Error::Protocol),
            }
            continue;
        };
        if field.oneof {
            if oneof {
                return Err(Error::Protocol);
            }
            oneof = true;
        }
        let mut count = 1;
        match (field.kind, wt) {
            (Kind::Varint, 0) => {
                varint(&mut data)?;
            }
            (Kind::Bool, 0) => {
                if varint(&mut data)? > 1 {
                    return Err(Error::Protocol);
                }
            }
            (Kind::Varint | Kind::Bool, 2) if field.repeated => {
                let mut packed = delimited(&mut data)?;
                count = 0;
                while !packed.is_empty() {
                    let n = varint(&mut packed)?;
                    if matches!(field.kind, Kind::Bool) && n > 1 {
                        return Err(Error::Protocol);
                    }
                    count += 1;
                    if count > field.max {
                        return Err(Error::Capacity);
                    }
                }
            }
            (Kind::Bytes(n, exact), 2) => {
                let v = delimited(&mut data)?;
                if exact && v.len() != n {
                    return Err(Error::Protocol);
                }
                if v.len() > n {
                    return Err(Error::Capacity);
                }
                charge(budget, v.len())?;
            }
            (Kind::Child(s), 2) => {
                if counts[index] >= field.max {
                    return Err(Error::Capacity);
                }
                scan(s, delimited(&mut data)?, depth + 1, budget)?;
            }
            _ => return Err(Error::Protocol),
        }
        counts[index] = counts[index].checked_add(count).ok_or(Error::Capacity)?;
        if counts[index] > field.max {
            return Err(Error::Capacity);
        }
    }
    Ok(())
}
fn check(name: &str, bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() || bytes.len() > MAX_FRAME_BYTES {
        return Err(Error::Capacity);
    }
    // Bound aggregate pre-prost heap expansion as well as each collection; the
    // allowance includes fixed structs for up to MAX_READ_ROWS snapshot entries.
    let mut budget = MAX_FRAME_BYTES * 4;
    check_nested(name, bytes, &mut budget)
}
pub(in crate::managed_rpc) fn check_request(bytes: &[u8]) -> Result<(), Error> {
    check("Request", bytes)
}
pub(in crate::managed_rpc) fn check_reply(bytes: &[u8]) -> Result<(), Error> {
    check("Reply", bytes)
}
pub(crate) fn check_nested(name: &str, bytes: &[u8], budget: &mut usize) -> Result<(), Error> {
    let schema = match name {
        "Empty" => Schema::Empty,
        "Target" => Schema::Target,
        "Authority" => Schema::Authority,
        "Member" => Schema::Member,
        "Cohort" => Schema::Cohort,
        "Participant" => Schema::Participant,
        "TerminalRecord" => Schema::TerminalRecord,
        "TerminalSegment" => Schema::TerminalSegment,
        "TerminalPrefix" => Schema::TerminalPrefix,
        "ReadLimit" => Schema::ReadLimit,
        "ErrorPair" => Schema::ErrorPair,
        "ApplicationError" => Schema::ApplicationError,
        "Identity" => Schema::Identity,
        "Request" => Schema::Request,
        "Reply" => Schema::Reply,
        "RpcError" => Schema::RpcError,
        "LogId" => Schema::LogId,
        "CapacityActivation" => Schema::CapacityActivation,
        "MemberCapacityGrant" => Schema::MemberCapacityGrant,
        "HistoryCursor" => Schema::HistoryCursor,
        "HistoryPage" => Schema::HistoryPage,
        "Put" => Schema::Put,
        "WriteOp" => Schema::WriteOp,
        "D1WriteRequest" => Schema::D1WriteRequest,
        "RetainedPrepare" => Schema::RetainedPrepare,
        "AdmitRequest" => Schema::AdmitRequest,
        "ConfirmCohortCapacity" => Schema::ConfirmCohortCapacity,
        "Begin" => Schema::Begin,
        "Prepared" => Schema::Prepared,
        "Decide" => Schema::Decide,
        "Command" => Schema::Command,
        "Journal" => Schema::Journal,
        "RequestRecord" => Schema::RequestRecord,
        "Transaction" => Schema::Transaction,
        "TargetRecord" => Schema::TargetRecord,
        "TargetEvidence" => Schema::TargetEvidence,
        "RecoveredRequest" => Schema::RecoveredRequest,
        "RequestLookup" => Schema::RequestLookup,
        "HistoryStatus" => Schema::HistoryStatus,
        "CoordinatorSummary" => Schema::CoordinatorSummary,
        "VisibilityInfo" => Schema::VisibilityInfo,
        "CapacityGrantStatus" => Schema::CapacityGrantStatus,
        "CohortCapacity" => Schema::CohortCapacity,
        "Status" => Schema::Status,
        "Entry" => Schema::Entry,
        "Snapshot" => Schema::Snapshot,
        "SnapshotWithProof" => Schema::SnapshotWithProof,
        "Value" => Schema::Value,
        "Lookup" => Schema::Lookup,
        "AuthorityEpoch" => Schema::AuthorityEpoch,
        "BoundTarget" => Schema::BoundTarget,
        "HistoryPageRequest" => Schema::HistoryPageRequest,
        "SyncHistory" => Schema::SyncHistory,
        "AdvanceRetained" => Schema::AdvanceRetained,
        "TerminalRequest" => Schema::TerminalRequest,
        "CommandRequest" => Schema::CommandRequest,
        "Prepare" => Schema::Prepare,
        "Advance" => Schema::Advance,
        "Prefix" => Schema::Prefix,
        "Read" => Schema::Read,
        "Operation" => Schema::Operation,
        _ => return Err(Error::Protocol),
    };
    scan(schema, bytes, 0, budget)
}
