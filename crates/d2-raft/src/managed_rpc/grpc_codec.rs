//! Typed internal managed transport. These DTOs never replace canonical durable bytes.
use super::*;
use crate::request_index::RecoveredRequest;
use crate::target_evidence::TargetRecord;
use crate::terminal_prefix::transport::{TerminalPrefixParts, TerminalRecord};
use crate::transaction_decisions::{
    CapacityActivation, Decision, Member, RequestRecord, Transaction,
};
use crate::WriteOp;
use prost::Message;
#[path = "grpc_bounded.rs"]
pub(crate) mod bounded;
#[path = "managed_proto.rs"]
pub mod pb;
pub(super) use bounded::{check_reply, check_request};
fn required<T>(value: Option<T>) -> Result<T, Error> {
    value.ok_or(Error::Protocol)
}
fn size(value: u64) -> Result<usize, Error> {
    value.try_into().map_err(|_| Error::Protocol)
}
fn array<const N: usize>(value: Vec<u8>) -> Result<[u8; N], Error> {
    value.try_into().map_err(|_| Error::Protocol)
}
fn items<T, U>(input: Vec<T>) -> Result<Vec<U>, Error>
where
    U: TryFrom<T, Error = Error>,
{
    input.into_iter().map(TryInto::try_into).collect()
}
impl From<CapacityProfile> for pb::CapacityProfile {
    fn from(v: CapacityProfile) -> Self {
        match v {
            CapacityProfile::LegacyCountV1 => Self::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1 => Self::ByteBound16MibV1,
        }
    }
}
impl TryFrom<pb::CapacityProfile> for CapacityProfile {
    type Error = Error;
    fn try_from(v: pb::CapacityProfile) -> Result<Self, Self::Error> {
        Ok(match v {
            pb::CapacityProfile::LegacyCountV1 => Self::LegacyCountV1,
            pb::CapacityProfile::ByteBound16MibV1 => Self::ByteBound16MiBV1,
            pb::CapacityProfile::Unspecified => return Err(Error::Protocol),
        })
    }
}
impl From<Decision> for pb::TerminalDecision {
    fn from(v: Decision) -> Self {
        match v {
            Decision::Commit => Self::Commit,
            Decision::Abort => Self::Abort,
        }
    }
}
impl TryFrom<pb::TerminalDecision> for Decision {
    type Error = Error;
    fn try_from(v: pb::TerminalDecision) -> Result<Self, Self::Error> {
        Ok(match v {
            pb::TerminalDecision::Commit => Self::Commit,
            pb::TerminalDecision::Abort => Self::Abort,
            pb::TerminalDecision::Unspecified => return Err(Error::Protocol),
        })
    }
}
impl From<Target> for pb::Target {
    fn from(v: Target) -> Self {
        Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: v.mode.to_vec(),
        }
    }
}
impl TryFrom<pb::Target> for Target {
    type Error = Error;
    fn try_from(v: pb::Target) -> Result<Self, Self::Error> {
        let result = Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: array(v.mode)?,
        };
        Ok(result)
    }
}
impl From<Authority> for pb::Authority {
    fn from(v: Authority) -> Self {
        Self {
            shard: v.shard,
            mode: v.mode.to_vec(),
        }
    }
}
impl TryFrom<pb::Authority> for Authority {
    type Error = Error;
    fn try_from(v: pb::Authority) -> Result<Self, Self::Error> {
        let result = Self {
            shard: v.shard,
            mode: array(v.mode)?,
        };
        Ok(result)
    }
}
impl From<Member> for pb::Member {
    fn from(v: Member) -> Self {
        Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: v.mode.to_vec(),
        }
    }
}
impl TryFrom<pb::Member> for Member {
    type Error = Error;
    fn try_from(v: pb::Member) -> Result<Self, Self::Error> {
        let result = Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: array(v.mode)?,
        };
        Ok(result)
    }
}
impl From<Participant> for pb::Participant {
    fn from(v: Participant) -> Self {
        Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: v.mode.to_vec(),
            intent: v.intent.to_vec(),
        }
    }
}
impl TryFrom<pb::Participant> for Participant {
    type Error = Error;
    fn try_from(v: pb::Participant) -> Result<Self, Self::Error> {
        let result = Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: array(v.mode)?,
            intent: array(v.intent)?,
        };
        Ok(result)
    }
}
impl From<Cohort> for pb::Cohort {
    fn from(v: Cohort) -> Self {
        Self {
            coordinator_shard: v.coordinator_shard,
            coordinator_epoch: v.coordinator_epoch,
            members: v.members.into_iter().map(Into::into).collect(),
            profile: pb::CapacityProfile::from(v.profile) as i32,
        }
    }
}
impl TryFrom<pb::Cohort> for Cohort {
    type Error = Error;
    fn try_from(v: pb::Cohort) -> Result<Self, Self::Error> {
        let result = Self {
            coordinator_shard: v.coordinator_shard,
            coordinator_epoch: v.coordinator_epoch,
            members: v
                .members
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            profile: pb::CapacityProfile::try_from(v.profile)
                .map_err(|_| Error::Protocol)?
                .try_into()?,
        };
        result.validate().map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<TerminalRecord> for pb::TerminalRecord {
    fn from(v: TerminalRecord) -> Self {
        Self {
            id: v.id.to_vec(),
            ordinal: v.ordinal,
            participants: v.participants.into_iter().map(Into::into).collect(),
            prepared: v.prepared,
            decision: pb::TerminalDecision::from(v.decision) as i32,
        }
    }
}
impl TryFrom<pb::TerminalRecord> for TerminalRecord {
    type Error = Error;
    fn try_from(v: pb::TerminalRecord) -> Result<Self, Self::Error> {
        let result = Self {
            id: array(v.id)?,
            ordinal: v.ordinal,
            participants: v
                .participants
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            prepared: v.prepared,
            decision: pb::TerminalDecision::try_from(v.decision)
                .map_err(|_| Error::Protocol)?
                .try_into()?,
        };
        Ok(result)
    }
}
impl From<TerminalPrefix> for pb::TerminalPrefix {
    fn from(v: TerminalPrefix) -> Self {
        let v: TerminalPrefixParts = v.into();
        Self {
            format_version: v.format_version,
            authority: v.authority.to_vec(),
            cohort: Some(v.cohort.into()),
            through: v.through,
            segments: v
                .segments
                .into_iter()
                .map(|records| pb::TerminalSegment {
                    records: records.into_iter().map(Into::into).collect(),
                })
                .collect(),
        }
    }
}
impl TryFrom<pb::TerminalPrefix> for TerminalPrefix {
    type Error = Error;
    fn try_from(v: pb::TerminalPrefix) -> Result<Self, Self::Error> {
        let cohort: Cohort = required(v.cohort)?.try_into()?;
        if v.segments.len()
            > cohort
                .profile
                .max_transactions()
                .div_ceil(crate::terminal_prefix::SEGMENT_TRANSACTIONS)
            || v.segments.iter().any(|s| {
                s.records.len() > crate::terminal_prefix::SEGMENT_TRANSACTIONS
                    || s.records.iter().any(|r| {
                        r.participants.len() > crate::transaction_decisions::MAX_PARTICIPANTS
                            || r.prepared.len() > crate::transaction_decisions::MAX_PARTICIPANTS
                    })
            })
        {
            return Err(Error::Protocol);
        }
        TerminalPrefixParts {
            format_version: v.format_version,
            authority: array(v.authority)?,
            cohort,
            through: v.through,
            segments: v
                .segments
                .into_iter()
                .map(|s| {
                    s.records
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<_>, Error>>()
                })
                .collect::<Result<_, _>>()?,
        }
        .try_into()
        .map_err(|_| Error::Protocol)
    }
}
impl From<ReadLimit> for pb::ReadLimit {
    fn from(v: ReadLimit) -> Self {
        Self {
            rows: v.rows as u64,
            bytes: v.bytes as u64,
            operations: v.operations as u64,
        }
    }
}
impl TryFrom<pb::ReadLimit> for ReadLimit {
    type Error = Error;
    fn try_from(v: pb::ReadLimit) -> Result<Self, Self::Error> {
        let result = Self {
            rows: size(v.rows)?,
            bytes: size(v.bytes)?,
            operations: size(v.operations)?,
        };
        Ok(result)
    }
}
impl From<crate::transaction_decisions::Error> for pb::CoordinatorError {
    fn from(v: crate::transaction_decisions::Error) -> Self {
        match v {
            crate::transaction_decisions::Error::InvalidIdentity => {
                Self::CoordinatorInvalidIdentity
            }
            crate::transaction_decisions::Error::InvalidParticipants => {
                Self::CoordinatorInvalidParticipants
            }
            crate::transaction_decisions::Error::IdentityConflict => {
                Self::CoordinatorIdentityConflict
            }
            crate::transaction_decisions::Error::UnknownTransaction => {
                Self::CoordinatorUnknownTransaction
            }
            crate::transaction_decisions::Error::UnknownParticipant => {
                Self::CoordinatorUnknownParticipant
            }
            crate::transaction_decisions::Error::NotPrepared => Self::CoordinatorNotPrepared,
            crate::transaction_decisions::Error::AlreadyDecided => Self::CoordinatorAlreadyDecided,
            crate::transaction_decisions::Error::Capacity => Self::CoordinatorCapacity,
            crate::transaction_decisions::Error::CapacityNotConfirmed => {
                Self::CoordinatorCapacityNotConfirmed
            }
            crate::transaction_decisions::Error::InvalidJournal => Self::CoordinatorInvalidJournal,
            crate::transaction_decisions::Error::IncompatibleHistory => {
                Self::CoordinatorIncompatibleHistory
            }
        }
    }
}
impl TryFrom<pb::CoordinatorError> for crate::transaction_decisions::Error {
    type Error = Error;
    fn try_from(v: pb::CoordinatorError) -> Result<Self, Self::Error> {
        Ok(match v {
            pb::CoordinatorError::CoordinatorInvalidIdentity => Self::InvalidIdentity,
            pb::CoordinatorError::CoordinatorInvalidParticipants => Self::InvalidParticipants,
            pb::CoordinatorError::CoordinatorIdentityConflict => Self::IdentityConflict,
            pb::CoordinatorError::CoordinatorUnknownTransaction => Self::UnknownTransaction,
            pb::CoordinatorError::CoordinatorUnknownParticipant => Self::UnknownParticipant,
            pb::CoordinatorError::CoordinatorNotPrepared => Self::NotPrepared,
            pb::CoordinatorError::CoordinatorAlreadyDecided => Self::AlreadyDecided,
            pb::CoordinatorError::CoordinatorCapacity => Self::Capacity,
            pb::CoordinatorError::CoordinatorCapacityNotConfirmed => Self::CapacityNotConfirmed,
            pb::CoordinatorError::CoordinatorInvalidJournal => Self::InvalidJournal,
            pb::CoordinatorError::CoordinatorIncompatibleHistory => Self::IncompatibleHistory,
            pb::CoordinatorError::Unspecified => return Err(Error::Protocol),
        })
    }
}
impl From<D1Error> for pb::ApplicationError {
    fn from(v: D1Error) -> Self {
        use pb::application_error::Kind;
        Self {
            kind: Some(match v {
                D1Error::NotActivated => Kind::NotActivated(pb::Empty {}),
                D1Error::WrongShard { requested, current } => Kind::WrongShard(pb::ErrorPair {
                    first: requested,
                    current,
                }),
                D1Error::Fenced { requested, current } => Kind::Fenced(pb::ErrorPair {
                    first: requested,
                    current,
                }),
                D1Error::Conflict { expected, current } => Kind::Conflict(pb::ErrorPair {
                    first: expected,
                    current,
                }),
                D1Error::IdempotencyConflict => Kind::IdempotencyConflict(pb::Empty {}),
                D1Error::EmptyBatch => Kind::EmptyBatch(pb::Empty {}),
                D1Error::BatchTooLarge => Kind::BatchTooLarge(pb::Empty {}),
                D1Error::SequenceOverflow => Kind::SequenceOverflow(pb::Empty {}),
                D1Error::DedupCapacity => Kind::DedupCapacity(pb::Empty {}),
                D1Error::InvalidDedupCapacity => Kind::InvalidDedupCapacity(pb::Empty {}),
                D1Error::CapacityConflict { expected, current } => {
                    Kind::CapacityConflict(pb::ErrorPair {
                        first: expected as u64,
                        current: current as u64,
                    })
                }
                D1Error::LegacyDataPresent => Kind::LegacyDataPresent(pb::Empty {}),
                D1Error::LegacyWriteDisabled => Kind::LegacyWriteDisabled(pb::Empty {}),
                D1Error::InvalidEpoch => Kind::InvalidEpoch(pb::Empty {}),
                D1Error::ManagedWriteRequired => Kind::ManagedWriteRequired(pb::Empty {}),
                D1Error::ManagedWritesNotEnabled => Kind::ManagedWritesNotEnabled(pb::Empty {}),
                D1Error::InvalidManagedMode => Kind::InvalidManagedMode(pb::Empty {}),
                D1Error::ManagedModeConflict => Kind::ManagedModeConflict(pb::Empty {}),
                D1Error::UnverifiedWriteHistory => Kind::UnverifiedWriteHistory(pb::Empty {}),
                D1Error::VisibilityNotEnabled => Kind::VisibilityNotEnabled(pb::Empty {}),
                D1Error::VisibilityRequired => Kind::VisibilityRequired(pb::Empty {}),
                D1Error::VisibilityHistory => Kind::VisibilityHistory(pb::Empty {}),
                D1Error::VisibilityGap => Kind::VisibilityGap(pb::Empty {}),
                D1Error::VisibilityUnresolved => Kind::VisibilityUnresolved(pb::Empty {}),
                D1Error::VisibilityNotApplied => Kind::VisibilityNotApplied(pb::Empty {}),
                D1Error::VisibilityReadLimit => Kind::VisibilityReadLimit(pb::Empty {}),
                D1Error::ParticipantOnly => Kind::ParticipantOnly(pb::Empty {}),
                D1Error::ParticipantNotEnabled => Kind::ParticipantNotEnabled(pb::Empty {}),
                D1Error::ParticipantAuthority => Kind::ParticipantAuthority(pb::Empty {}),
                D1Error::ParticipantIdentity => Kind::ParticipantIdentity(pb::Empty {}),
                D1Error::ParticipantBusy => Kind::ParticipantBusy(pb::Empty {}),
                D1Error::ParticipantCapacity => Kind::ParticipantCapacity(pb::Empty {}),
                D1Error::ParticipantUnknown => Kind::ParticipantUnknown(pb::Empty {}),
                D1Error::ParticipantDecisionConflict => {
                    Kind::ParticipantDecisionConflict(pb::Empty {})
                }
                D1Error::CoordinatorOnly => Kind::CoordinatorOnly(pb::Empty {}),
                D1Error::CoordinatorNotEnabled => Kind::CoordinatorNotEnabled(pb::Empty {}),
                D1Error::CoordinatorRequiresEmptyShard => {
                    Kind::CoordinatorRequiresEmptyShard(pb::Empty {})
                }
                D1Error::DecisionIndexHistory => Kind::DecisionIndexHistory(pb::Empty {}),
                D1Error::DecisionIndexUnavailable => Kind::DecisionIndexUnavailable(pb::Empty {}),
                D1Error::EventLedgerHistory => Kind::EventLedgerHistory(pb::Empty {}),
                D1Error::EventLedgerUnavailable => Kind::EventLedgerUnavailable(pb::Empty {}),
                D1Error::RequestIndexHistory => Kind::RequestIndexHistory(pb::Empty {}),
                D1Error::RequestIndexUnavailable => Kind::RequestIndexUnavailable(pb::Empty {}),
                D1Error::CoordinatorRejected(v) => {
                    Kind::CoordinatorRejected(pb::CoordinatorError::from(v) as i32)
                }
            }),
        }
    }
}
impl TryFrom<pb::ApplicationError> for D1Error {
    type Error = Error;
    fn try_from(v: pb::ApplicationError) -> Result<Self, Self::Error> {
        use pb::application_error::Kind;
        Ok(match required(v.kind)? {
            Kind::NotActivated(_) => Self::NotActivated,
            Kind::WrongShard(v) => Self::WrongShard {
                requested: v.first,
                current: v.current,
            },
            Kind::Fenced(v) => Self::Fenced {
                requested: v.first,
                current: v.current,
            },
            Kind::Conflict(v) => Self::Conflict {
                expected: v.first,
                current: v.current,
            },
            Kind::IdempotencyConflict(_) => Self::IdempotencyConflict,
            Kind::EmptyBatch(_) => Self::EmptyBatch,
            Kind::BatchTooLarge(_) => Self::BatchTooLarge,
            Kind::SequenceOverflow(_) => Self::SequenceOverflow,
            Kind::DedupCapacity(_) => Self::DedupCapacity,
            Kind::InvalidDedupCapacity(_) => Self::InvalidDedupCapacity,
            Kind::CapacityConflict(v) => Self::CapacityConflict {
                expected: size(v.first)?,
                current: size(v.current)?,
            },
            Kind::LegacyDataPresent(_) => Self::LegacyDataPresent,
            Kind::LegacyWriteDisabled(_) => Self::LegacyWriteDisabled,
            Kind::InvalidEpoch(_) => Self::InvalidEpoch,
            Kind::ManagedWriteRequired(_) => Self::ManagedWriteRequired,
            Kind::ManagedWritesNotEnabled(_) => Self::ManagedWritesNotEnabled,
            Kind::InvalidManagedMode(_) => Self::InvalidManagedMode,
            Kind::ManagedModeConflict(_) => Self::ManagedModeConflict,
            Kind::UnverifiedWriteHistory(_) => Self::UnverifiedWriteHistory,
            Kind::VisibilityNotEnabled(_) => Self::VisibilityNotEnabled,
            Kind::VisibilityRequired(_) => Self::VisibilityRequired,
            Kind::VisibilityHistory(_) => Self::VisibilityHistory,
            Kind::VisibilityGap(_) => Self::VisibilityGap,
            Kind::VisibilityUnresolved(_) => Self::VisibilityUnresolved,
            Kind::VisibilityNotApplied(_) => Self::VisibilityNotApplied,
            Kind::VisibilityReadLimit(_) => Self::VisibilityReadLimit,
            Kind::ParticipantOnly(_) => Self::ParticipantOnly,
            Kind::ParticipantNotEnabled(_) => Self::ParticipantNotEnabled,
            Kind::ParticipantAuthority(_) => Self::ParticipantAuthority,
            Kind::ParticipantIdentity(_) => Self::ParticipantIdentity,
            Kind::ParticipantBusy(_) => Self::ParticipantBusy,
            Kind::ParticipantCapacity(_) => Self::ParticipantCapacity,
            Kind::ParticipantUnknown(_) => Self::ParticipantUnknown,
            Kind::ParticipantDecisionConflict(_) => Self::ParticipantDecisionConflict,
            Kind::CoordinatorOnly(_) => Self::CoordinatorOnly,
            Kind::CoordinatorNotEnabled(_) => Self::CoordinatorNotEnabled,
            Kind::CoordinatorRequiresEmptyShard(_) => Self::CoordinatorRequiresEmptyShard,
            Kind::DecisionIndexHistory(_) => Self::DecisionIndexHistory,
            Kind::DecisionIndexUnavailable(_) => Self::DecisionIndexUnavailable,
            Kind::EventLedgerHistory(_) => Self::EventLedgerHistory,
            Kind::EventLedgerUnavailable(_) => Self::EventLedgerUnavailable,
            Kind::RequestIndexHistory(_) => Self::RequestIndexHistory,
            Kind::RequestIndexUnavailable(_) => Self::RequestIndexUnavailable,
            Kind::CoordinatorRejected(v) => Self::CoordinatorRejected(
                pb::CoordinatorError::try_from(v)
                    .map_err(|_| Error::Protocol)?
                    .try_into()?,
            ),
        })
    }
}

impl From<Identity> for pb::Identity {
    fn from(v: Identity) -> Self {
        Self {
            group: v.group,
            node: v.node,
            target: Some(v.target.into()),
            role: match v.role {
                Role::Coordinator => pb::Role::Coordinator as i32,
                Role::Participant => pb::Role::Participant as i32,
            },
            authority: Some(v.authority.into()),
            cohort: Some(v.cohort.into()),
        }
    }
}
impl TryFrom<pb::Identity> for Identity {
    type Error = Error;
    fn try_from(v: pb::Identity) -> Result<Self, Error> {
        let result = Self {
            group: v.group,
            node: v.node,
            target: required(v.target)?.try_into()?,
            role: match pb::Role::try_from(v.role).map_err(|_| Error::Protocol)? {
                pb::Role::Coordinator => Role::Coordinator,
                pb::Role::Participant => Role::Participant,
                _ => return Err(Error::Protocol),
            },
            authority: required(v.authority)?.try_into()?,
            cohort: required(v.cohort)?.try_into()?,
        };
        result.validate().map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<CapacityActivation> for pb::CapacityActivation {
    fn from(v: CapacityActivation) -> Self {
        Self {
            nonce: v.nonce.to_vec(),
            log_id: Some(pb::LogId {
                term: v.log_id.leader_id.term,
                node: v.log_id.leader_id.node_id,
                index: v.log_id.index,
            }),
        }
    }
}
impl TryFrom<pb::CapacityActivation> for CapacityActivation {
    type Error = Error;
    fn try_from(v: pb::CapacityActivation) -> Result<Self, Error> {
        let result = Self {
            nonce: array(v.nonce)?,
            log_id: {
                let l = required(v.log_id)?;
                openraft::LogId::new(openraft::CommittedLeaderId::new(l.term, l.node), l.index)
            },
        };
        result.validate().map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<MemberCapacityGrant> for pb::MemberCapacityGrant {
    fn from(v: MemberCapacityGrant) -> Self {
        Self {
            member: Some(v.member.into()),
            activation: Some(v.activation.into()),
        }
    }
}
impl TryFrom<pb::MemberCapacityGrant> for MemberCapacityGrant {
    type Error = Error;
    fn try_from(v: pb::MemberCapacityGrant) -> Result<Self, Error> {
        let result = Self {
            member: required(v.member)?.try_into()?,
            activation: required(v.activation)?.try_into()?,
        };
        Ok(result)
    }
}
impl From<HistoryCursor> for pb::HistoryCursor {
    fn from(v: HistoryCursor) -> Self {
        Self {
            version: v.version,
            authority: Some(v.authority.into()),
            cohort: Some(v.cohort.into()),
            event_count: v.event_count,
            root: v.root.to_vec(),
        }
    }
}
impl TryFrom<pb::HistoryCursor> for HistoryCursor {
    type Error = Error;
    fn try_from(v: pb::HistoryCursor) -> Result<Self, Error> {
        let result = Self {
            version: v.version,
            authority: required(v.authority)?.try_into()?,
            cohort: required(v.cohort)?.try_into()?,
            event_count: v.event_count,
            root: array(v.root)?,
        };
        result.validate().map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<HistoryPage> for pb::HistoryPage {
    fn from(v: HistoryPage) -> Self {
        Self {
            version: v.version,
            base: Some(v.base.into()),
            end: Some(v.end.into()),
            cut: Some(v.cut.into()),
            events: v.events.into_iter().map(Into::into).collect(),
        }
    }
}
impl TryFrom<pb::HistoryPage> for HistoryPage {
    type Error = Error;
    fn try_from(v: pb::HistoryPage) -> Result<Self, Error> {
        let result = Self {
            version: v.version,
            base: required(v.base)?.try_into()?,
            end: required(v.end)?.try_into()?,
            cut: required(v.cut)?.try_into()?,
            events: items(v.events)?,
        };
        result.validate().map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<D1WriteRequest> for pb::D1WriteRequest {
    fn from(v: D1WriteRequest) -> Self {
        Self {
            shard: v.shard,
            epoch: v.epoch,
            request_id: v.request_id.to_vec(),
            expected_sequence: v.expected_sequence,
            operations: v.operations.into_iter().map(Into::into).collect(),
        }
    }
}
impl TryFrom<pb::D1WriteRequest> for D1WriteRequest {
    type Error = Error;
    fn try_from(v: pb::D1WriteRequest) -> Result<Self, Error> {
        let result = Self {
            shard: v.shard,
            epoch: v.epoch,
            request_id: array(v.request_id)?,
            expected_sequence: v.expected_sequence,
            operations: items(v.operations)?,
        };
        Ok(result)
    }
}
impl From<RetainedPrepare> for pb::RetainedPrepare {
    fn from(v: RetainedPrepare) -> Self {
        Self {
            version: v.version,
            authority: Some(v.authority.into()),
            participant: Some(v.participant.into()),
            request: Some(v.request.into()),
            cut: Some(v.cut.into()),
        }
    }
}
impl TryFrom<pb::RetainedPrepare> for RetainedPrepare {
    type Error = Error;
    fn try_from(v: pb::RetainedPrepare) -> Result<Self, Error> {
        let result = Self {
            version: v.version,
            authority: required(v.authority)?.try_into()?,
            participant: required(v.participant)?.try_into()?,
            request: required(v.request)?.try_into()?,
            cut: required(v.cut)?.try_into()?,
        };
        result.validate().map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<RequestRecord> for pb::RequestRecord {
    fn from(v: RequestRecord) -> Self {
        Self {
            id: v.id.to_vec(),
            request: v.request,
            payload: v.payload,
            transaction_id: v.transaction_id.to_vec(),
        }
    }
}
impl TryFrom<pb::RequestRecord> for RequestRecord {
    type Error = Error;
    fn try_from(v: pb::RequestRecord) -> Result<Self, Error> {
        let result = Self {
            id: array(v.id)?,
            request: v.request,
            payload: v.payload,
            transaction_id: array(v.transaction_id)?,
        };
        if result.request.len() > crate::transaction_decisions::MAX_REQUEST_BYTES
            || result.payload.len() > crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES
        {
            return Err(Error::Capacity);
        }
        Ok(result)
    }
}
impl From<Transaction> for pb::Transaction {
    fn from(v: Transaction) -> Self {
        Self {
            id: v.id.to_vec(),
            ordinal: v.ordinal,
            participants: v.participants.into_iter().map(Into::into).collect(),
            prepared: v.prepared,
            decision: v.decision.map(|d| pb::TerminalDecision::from(d) as i32),
        }
    }
}
impl TryFrom<pb::Transaction> for Transaction {
    type Error = Error;
    fn try_from(v: pb::Transaction) -> Result<Self, Error> {
        let result = Self {
            id: array(v.id)?,
            ordinal: v.ordinal,
            participants: items(v.participants)?,
            prepared: v.prepared,
            decision: v
                .decision
                .map(|d| {
                    pb::TerminalDecision::try_from(d)
                        .map_err(|_| Error::Protocol)?
                        .try_into()
                })
                .transpose()?,
        };
        if result.participants.len() > crate::transaction_decisions::MAX_PARTICIPANTS
            || result.prepared.len() != result.participants.len()
        {
            return Err(Error::Protocol);
        }
        Ok(result)
    }
}
impl From<TargetRecord> for pb::TargetRecord {
    fn from(v: TargetRecord) -> Self {
        Self {
            transaction: Some(v.transaction.into()),
            request: v.request.map(Into::into),
        }
    }
}
impl TryFrom<pb::TargetRecord> for TargetRecord {
    type Error = Error;
    fn try_from(v: pb::TargetRecord) -> Result<Self, Error> {
        let result = Self {
            transaction: required(v.transaction)?.try_into()?,
            request: v.request.map(TryInto::try_into).transpose()?,
        };
        Ok(result)
    }
}
impl From<TargetEvidence> for pb::TargetEvidence {
    fn from(v: TargetEvidence) -> Self {
        Self {
            version: v.version,
            cut: Some(v.cut.into()),
            resolved_prefix: v.resolved_prefix,
            id: v.id.to_vec(),
            record: v.record.map(Into::into),
        }
    }
}
impl TryFrom<pb::TargetEvidence> for TargetEvidence {
    type Error = Error;
    fn try_from(v: pb::TargetEvidence) -> Result<Self, Error> {
        let result = Self {
            version: v.version,
            cut: required(v.cut)?.try_into()?,
            resolved_prefix: v.resolved_prefix,
            id: array(v.id)?,
            record: v.record.map(TryInto::try_into).transpose()?,
        };
        result.validate().map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<RecoveredRequest> for pb::RecoveredRequest {
    fn from(v: RecoveredRequest) -> Self {
        Self {
            record: Some(v.record.into()),
            transaction: Some(v.transaction.into()),
        }
    }
}
impl TryFrom<pb::RecoveredRequest> for RecoveredRequest {
    type Error = Error;
    fn try_from(v: pb::RecoveredRequest) -> Result<Self, Error> {
        let result = Self {
            record: required(v.record)?.try_into()?,
            transaction: required(v.transaction)?.try_into()?,
        };
        Ok(result)
    }
}
impl From<RequestLookup> for pb::RequestLookup {
    fn from(v: RequestLookup) -> Self {
        Self {
            authority: Some(v.authority.into()),
            cohort: Some(v.cohort.into()),
            resolved_prefix: v.resolved_prefix,
            id: v.id.to_vec(),
            record: v.record.map(Into::into),
        }
    }
}
impl TryFrom<pb::RequestLookup> for RequestLookup {
    type Error = Error;
    fn try_from(v: pb::RequestLookup) -> Result<Self, Error> {
        let result = Self {
            authority: required(v.authority)?.try_into()?,
            cohort: required(v.cohort)?.try_into()?,
            resolved_prefix: v.resolved_prefix,
            id: array(v.id)?,
            record: v.record.map(TryInto::try_into).transpose()?,
        };
        result.validate().map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<HistoryStatus> for pb::HistoryStatus {
    fn from(v: HistoryStatus) -> Self {
        Self {
            cursor: Some(v.cursor.into()),
            applied_prefix: v.applied_prefix,
        }
    }
}
impl TryFrom<pb::HistoryStatus> for HistoryStatus {
    type Error = Error;
    fn try_from(v: pb::HistoryStatus) -> Result<Self, Error> {
        let result = Self {
            cursor: required(v.cursor)?.try_into()?,
            applied_prefix: v.applied_prefix,
        };
        Ok(result)
    }
}
impl From<CoordinatorSummary> for pb::CoordinatorSummary {
    fn from(v: CoordinatorSummary) -> Self {
        Self {
            authority: Some(v.authority.into()),
            cohort: Some(v.cohort.into()),
            resolved_prefix: v.resolved_prefix,
        }
    }
}
impl TryFrom<pb::CoordinatorSummary> for CoordinatorSummary {
    type Error = Error;
    fn try_from(v: pb::CoordinatorSummary) -> Result<Self, Error> {
        let result = Self {
            authority: required(v.authority)?.try_into()?,
            cohort: required(v.cohort)?.try_into()?,
            resolved_prefix: v.resolved_prefix,
        };
        Ok(result)
    }
}
impl From<VisibilityInfo> for pb::VisibilityInfo {
    fn from(v: VisibilityInfo) -> Self {
        Self {
            authority: Some(v.authority.into()),
            baseline_sequence: v.baseline_sequence,
            cohort: v.cohort.map(Into::into),
            applied_prefix: v.applied_prefix,
        }
    }
}
impl TryFrom<pb::VisibilityInfo> for VisibilityInfo {
    type Error = Error;
    fn try_from(v: pb::VisibilityInfo) -> Result<Self, Error> {
        let result = Self {
            authority: required(v.authority)?.try_into()?,
            baseline_sequence: v.baseline_sequence,
            cohort: v.cohort.map(TryInto::try_into).transpose()?,
            applied_prefix: v.applied_prefix,
        };
        Ok(result)
    }
}
impl From<CapacityGrantStatus> for pb::CapacityGrantStatus {
    fn from(v: CapacityGrantStatus) -> Self {
        Self {
            authority: Some(v.authority.into()),
            cohort: Some(v.cohort.into()),
            grant: Some(v.grant.into()),
        }
    }
}
impl TryFrom<pb::CapacityGrantStatus> for CapacityGrantStatus {
    type Error = Error;
    fn try_from(v: pb::CapacityGrantStatus) -> Result<Self, Error> {
        let result = Self {
            authority: required(v.authority)?.try_into()?,
            cohort: required(v.cohort)?.try_into()?,
            grant: required(v.grant)?.try_into()?,
        };
        Ok(result)
    }
}
impl From<CohortCapacity> for pb::CohortCapacity {
    fn from(v: CohortCapacity) -> Self {
        Self {
            authority: Some(v.authority.into()),
            cohort: Some(v.cohort.into()),
            grants: v.grants.into_iter().map(Into::into).collect(),
        }
    }
}
impl TryFrom<pb::CohortCapacity> for CohortCapacity {
    type Error = Error;
    fn try_from(v: pb::CohortCapacity) -> Result<Self, Error> {
        let result = Self {
            authority: required(v.authority)?.try_into()?,
            cohort: required(v.cohort)?.try_into()?,
            grants: items(v.grants)?,
        };
        crate::capacity_policy::validate_grants(&result.cohort, &result.grants)
            .map_err(|_| Error::Protocol)?;
        Ok(result)
    }
}
impl From<Snapshot> for pb::Snapshot {
    fn from(v: Snapshot) -> Self {
        Self {
            shard: v.shard,
            epoch: v.epoch,
            authority: Some(v.authority.into()),
            version: v.version,
            baseline_sequence: v.baseline_sequence,
            cohort: v.cohort.map(Into::into),
            applied_prefix: v.applied_prefix,
            local_sequence: v.local_sequence,
            replay_operations: v.replay_operations as u64,
            entries: v
                .entries
                .into_iter()
                .map(|(key, value)| pb::Entry { key, value })
                .collect(),
        }
    }
}
impl TryFrom<pb::Snapshot> for Snapshot {
    type Error = Error;
    fn try_from(v: pb::Snapshot) -> Result<Self, Error> {
        let result = Self {
            shard: v.shard,
            epoch: v.epoch,
            authority: required(v.authority)?.try_into()?,
            version: v.version,
            baseline_sequence: v.baseline_sequence,
            cohort: v.cohort.map(TryInto::try_into).transpose()?,
            applied_prefix: v.applied_prefix,
            local_sequence: v.local_sequence,
            replay_operations: size(v.replay_operations)?,
            entries: v.entries.into_iter().map(|e| (e.key, e.value)).collect(),
        };
        Ok(result)
    }
}

impl From<Journal> for pb::Journal {
    fn from(v: Journal) -> Self {
        Self {
            authority: v.authority().to_vec(),
            events: v.events().iter().cloned().map(Into::into).collect(),
        }
    }
}
impl TryFrom<pb::Journal> for Journal {
    type Error = Error;
    fn try_from(v: pb::Journal) -> Result<Self, Error> {
        if v.events.len() > crate::capacity_policy::BYTE_MAX_HISTORY_EVENTS {
            return Err(Error::Capacity);
        }
        let mut journal = Journal::new(array(v.authority)?).map_err(|_| Error::Protocol)?;
        for event in v.events {
            journal
                .replay_event(event.try_into()?)
                .map_err(|_| Error::Protocol)?;
        }
        Ok(journal)
    }
}
impl From<WriteOp> for pb::WriteOp {
    fn from(v: WriteOp) -> Self {
        Self {
            kind: Some(match v {
                WriteOp::Put { key, value } => pb::write_op::Kind::Put(pb::Put { key, value }),
                WriteOp::Delete { key } => pb::write_op::Kind::Delete(key),
            }),
        }
    }
}
impl TryFrom<pb::WriteOp> for WriteOp {
    type Error = Error;
    fn try_from(v: pb::WriteOp) -> Result<Self, Error> {
        Ok(match required(v.kind)? {
            pb::write_op::Kind::Put(v) => Self::Put {
                key: v.key,
                value: v.value,
            },
            pb::write_op::Kind::Delete(key) => Self::Delete { key },
        })
    }
}
impl From<Status> for pb::Status {
    fn from(v: Status) -> Self {
        use pb::status::Kind;
        Self {
            kind: Some(match v {
                Status::Unknown => Kind::Unknown(pb::Empty {}),
                Status::Prepared => Kind::Prepared(pb::Empty {}),
                Status::Aborted => Kind::Aborted(pb::Empty {}),
                Status::Committed { sequence } => Kind::CommittedSequence(sequence),
            }),
        }
    }
}
impl TryFrom<pb::Status> for Status {
    type Error = Error;
    fn try_from(v: pb::Status) -> Result<Self, Error> {
        use pb::status::Kind;
        Ok(match required(v.kind)? {
            Kind::Unknown(_) => Status::Unknown,
            Kind::Prepared(_) => Status::Prepared,
            Kind::Aborted(_) => Status::Aborted,
            Kind::CommittedSequence(sequence) => Status::Committed { sequence },
        })
    }
}
impl From<Command> for pb::Command {
    fn from(v: Command) -> Self {
        use pb::command::Kind;
        Self {
            kind: Some(match v {
                Command::AdmitRequest {
                    id,
                    request,
                    payload,
                    transaction_id,
                    participants,
                } => Kind::AdmitRequest(pb::AdmitRequest {
                    id: id.to_vec(),
                    request,
                    payload,
                    transaction_id: transaction_id.to_vec(),
                    participants: participants.into_iter().map(Into::into).collect(),
                }),
                Command::ConfirmCohortCapacity { cohort, grants } => {
                    Kind::ConfirmCohortCapacity(pb::ConfirmCohortCapacity {
                        cohort: Some(cohort.into()),
                        grants: grants.into_iter().map(Into::into).collect(),
                    })
                }
                Command::Begin { id, participants } => Kind::Begin(pb::Begin {
                    id: id.to_vec(),
                    participants: participants.into_iter().map(Into::into).collect(),
                }),
                Command::Prepared { id, participant } => Kind::Prepared(pb::Prepared {
                    id: id.to_vec(),
                    participant: Some(participant.into()),
                }),
                Command::Decide { id, decision } => Kind::Decide(pb::Decide {
                    id: id.to_vec(),
                    decision: pb::TerminalDecision::from(decision) as i32,
                }),
                Command::BindCohort { cohort } => Kind::BindCohort(cohort.into()),
            }),
        }
    }
}
impl TryFrom<pb::Command> for Command {
    type Error = Error;
    fn try_from(v: pb::Command) -> Result<Self, Error> {
        use pb::command::Kind;
        let result = match required(v.kind)? {
            Kind::AdmitRequest(v) => Command::AdmitRequest {
                id: array(v.id)?,
                request: v.request,
                payload: v.payload,
                transaction_id: array(v.transaction_id)?,
                participants: items(v.participants)?,
            },
            Kind::ConfirmCohortCapacity(v) => Command::ConfirmCohortCapacity {
                cohort: required(v.cohort)?.try_into()?,
                grants: items(v.grants)?,
            },
            Kind::Begin(v) => Command::Begin {
                id: array(v.id)?,
                participants: items(v.participants)?,
            },
            Kind::Prepared(v) => Command::Prepared {
                id: array(v.id)?,
                participant: required(v.participant)?.try_into()?,
            },
            Kind::Decide(v) => Command::Decide {
                id: array(v.id)?,
                decision: pb::TerminalDecision::try_from(v.decision)
                    .map_err(|_| Error::Protocol)?
                    .try_into()?,
            },
            Kind::BindCohort(cohort) => Command::BindCohort {
                cohort: cohort.try_into()?,
            },
        };
        if !bounded_queue_command(&result) {
            return Err(Error::Capacity);
        }
        Ok(result)
    }
}
impl From<Operation> for pb::Operation {
    fn from(v: Operation) -> Self {
        use pb::operation::Kind;
        Self {
            kind: Some(match v {
                Operation::LookupTarget {
                    authority,
                    epoch,
                    id,
                } => Kind::LookupTarget(pb::Lookup {
                    authority: Some(authority.into()),
                    epoch,
                    id: id.to_vec(),
                }),
                Operation::GrantStatus {
                    authority,
                    target,
                    cohort,
                } => Kind::GrantStatus(pb::BoundTarget {
                    authority: Some(authority.into()),
                    target: Some(target.into()),
                    cohort: Some(cohort.into()),
                }),
                Operation::ConfirmCapacity { authority, epoch } => {
                    Kind::ConfirmCapacity(pb::AuthorityEpoch {
                        authority: Some(authority.into()),
                        epoch,
                    })
                }
                Operation::HistoryStatus {
                    authority,
                    target,
                    cohort,
                } => Kind::HistoryStatus(pb::BoundTarget {
                    authority: Some(authority.into()),
                    target: Some(target.into()),
                    cohort: Some(cohort.into()),
                }),
                Operation::HistoryPage {
                    authority,
                    epoch,
                    base,
                    cut,
                } => Kind::HistoryPage(pb::HistoryPageRequest {
                    authority: Some(authority.into()),
                    epoch,
                    base: Some(base.into()),
                    cut: cut.map(Into::into),
                }),
                Operation::SyncHistory {
                    authority,
                    target,
                    page,
                } => Kind::SyncHistory(pb::SyncHistory {
                    authority: Some(authority.into()),
                    target: Some(target.into()),
                    page: Some(page.into()),
                }),
                Operation::AdvanceRetained {
                    authority,
                    target,
                    ordinal,
                    cut,
                } => Kind::AdvanceRetained(pb::AdvanceRetained {
                    authority: Some(authority.into()),
                    target: Some(target.into()),
                    ordinal,
                    cut: Some(cut.into()),
                }),
                Operation::LookupRequest {
                    authority,
                    epoch,
                    id,
                } => Kind::LookupRequest(pb::Lookup {
                    authority: Some(authority.into()),
                    epoch,
                    id: id.to_vec(),
                }),
                Operation::Summary { authority, epoch } => Kind::Summary(pb::AuthorityEpoch {
                    authority: Some(authority.into()),
                    epoch,
                }),
                Operation::Terminal {
                    authority,
                    epoch,
                    version,
                } => Kind::Terminal(pb::TerminalRequest {
                    authority: Some(authority.into()),
                    epoch,
                    version,
                }),
                Operation::Journal { authority, epoch } => Kind::Journal(pb::AuthorityEpoch {
                    authority: Some(authority.into()),
                    epoch,
                }),
                Operation::Command {
                    authority,
                    epoch,
                    command,
                } => Kind::Command(pb::CommandRequest {
                    authority: Some(authority.into()),
                    epoch,
                    command: Some(command.into()),
                }),
                Operation::Prepare {
                    authority,
                    participant,
                    request,
                    journal,
                } => Kind::Prepare(pb::Prepare {
                    authority: Some(authority.into()),
                    participant: Some(participant.into()),
                    request: Some(request.into()),
                    journal: Some(journal.into()),
                }),
                Operation::Advance {
                    authority,
                    target,
                    ordinal,
                    journal,
                } => Kind::Advance(pb::Advance {
                    authority: Some(authority.into()),
                    target: Some(target.into()),
                    ordinal,
                    journal: Some(journal.into()),
                }),
                Operation::Prefix { target, proof } => Kind::Prefix(pb::Prefix {
                    target: Some(target.into()),
                    proof: Some(proof.into()),
                }),
                Operation::Read {
                    target,
                    limit,
                    proof,
                } => Kind::Read(pb::Read {
                    target: Some(target.into()),
                    limit: Some(limit.into()),
                    proof: Some(proof.into()),
                }),
                Operation::Visibility { target } => Kind::Visibility(target.into()),
                Operation::PrepareRetained(v) => Kind::PrepareRetained((*v).into()),
            }),
        }
    }
}
impl TryFrom<pb::Operation> for Operation {
    type Error = Error;
    fn try_from(v: pb::Operation) -> Result<Self, Error> {
        use pb::operation::Kind;
        Ok(match required(v.kind)? {
            Kind::LookupTarget(v) => Operation::LookupTarget {
                authority: required(v.authority)?.try_into()?,
                epoch: v.epoch,
                id: array(v.id)?,
            },
            Kind::GrantStatus(v) => Operation::GrantStatus {
                authority: required(v.authority)?.try_into()?,
                target: required(v.target)?.try_into()?,
                cohort: required(v.cohort)?.try_into()?,
            },
            Kind::ConfirmCapacity(v) => Operation::ConfirmCapacity {
                authority: required(v.authority)?.try_into()?,
                epoch: v.epoch,
            },
            Kind::HistoryStatus(v) => Operation::HistoryStatus {
                authority: required(v.authority)?.try_into()?,
                target: required(v.target)?.try_into()?,
                cohort: required(v.cohort)?.try_into()?,
            },
            Kind::HistoryPage(v) => Operation::HistoryPage {
                authority: required(v.authority)?.try_into()?,
                epoch: v.epoch,
                base: required(v.base)?.try_into()?,
                cut: v.cut.map(TryInto::try_into).transpose()?,
            },
            Kind::SyncHistory(v) => Operation::SyncHistory {
                authority: required(v.authority)?.try_into()?,
                target: required(v.target)?.try_into()?,
                page: required(v.page)?.try_into()?,
            },
            Kind::AdvanceRetained(v) => Operation::AdvanceRetained {
                authority: required(v.authority)?.try_into()?,
                target: required(v.target)?.try_into()?,
                ordinal: v.ordinal,
                cut: required(v.cut)?.try_into()?,
            },
            Kind::LookupRequest(v) => Operation::LookupRequest {
                authority: required(v.authority)?.try_into()?,
                epoch: v.epoch,
                id: array(v.id)?,
            },
            Kind::Summary(v) => Operation::Summary {
                authority: required(v.authority)?.try_into()?,
                epoch: v.epoch,
            },
            Kind::Terminal(v) => Operation::Terminal {
                authority: required(v.authority)?.try_into()?,
                epoch: v.epoch,
                version: v.version,
            },
            Kind::Journal(v) => Operation::Journal {
                authority: required(v.authority)?.try_into()?,
                epoch: v.epoch,
            },
            Kind::Command(v) => Operation::Command {
                authority: required(v.authority)?.try_into()?,
                epoch: v.epoch,
                command: required(v.command)?.try_into()?,
            },
            Kind::Prepare(v) => Operation::Prepare {
                authority: required(v.authority)?.try_into()?,
                participant: required(v.participant)?.try_into()?,
                request: required(v.request)?.try_into()?,
                journal: required(v.journal)?.try_into()?,
            },
            Kind::Advance(v) => Operation::Advance {
                authority: required(v.authority)?.try_into()?,
                target: required(v.target)?.try_into()?,
                ordinal: v.ordinal,
                journal: required(v.journal)?.try_into()?,
            },
            Kind::Prefix(v) => Operation::Prefix {
                target: required(v.target)?.try_into()?,
                proof: required(v.proof)?.try_into()?,
            },
            Kind::Read(v) => Operation::Read {
                target: required(v.target)?.try_into()?,
                limit: required(v.limit)?.try_into()?,
                proof: required(v.proof)?.try_into()?,
            },
            Kind::Visibility(target) => Operation::Visibility {
                target: target.try_into()?,
            },
            Kind::PrepareRetained(v) => Operation::PrepareRetained(Box::new(v.try_into()?)),
        })
    }
}
impl From<Value> for pb::Value {
    fn from(v: Value) -> Self {
        use pb::value::Kind;
        Self {
            kind: Some(match v {
                Value::Target(v) => Kind::Target(v.into()),
                Value::Grant(v) => Kind::Grant(v.into()),
                Value::Capacity(v) => Kind::Capacity(v.into()),
                Value::HistoryStatus(v) => Kind::HistoryStatus(v.into()),
                Value::HistoryPage(v) => Kind::HistoryPage(v.into()),
                Value::HistorySynced(v) => Kind::HistorySynced(v.into()),
                Value::Request(v) => Kind::Request(v.into()),
                Value::Journal(v) => Kind::Journal(v.into()),
                Value::Visibility(v) => Kind::Visibility(v.into()),
                Value::Status(v) => Kind::Status(v.into()),
                Value::Summary(v) => Kind::Summary(v.into()),
                Value::Evidence(v) => Kind::Evidence(v.into()),
                Value::Prefix(v) => Kind::Prefix(v),
                Value::RetainedPrefix(v) => Kind::RetainedPrefix(v),
                Value::Unit => Kind::Unit(pb::Empty {}),
                Value::Snapshot(snapshot, proof) => Kind::Snapshot(pb::SnapshotWithProof {
                    snapshot: Some(snapshot.into()),
                    proof: Some(proof.into()),
                }),
            }),
        }
    }
}
impl TryFrom<pb::Value> for Value {
    type Error = Error;
    fn try_from(v: pb::Value) -> Result<Self, Error> {
        use pb::value::Kind;
        Ok(match required(v.kind)? {
            Kind::Target(v) => Value::Target(v.try_into()?),
            Kind::Grant(v) => Value::Grant(v.try_into()?),
            Kind::Capacity(v) => Value::Capacity(v.try_into()?),
            Kind::HistoryStatus(v) => Value::HistoryStatus(v.try_into()?),
            Kind::HistoryPage(v) => Value::HistoryPage(v.try_into()?),
            Kind::HistorySynced(v) => Value::HistorySynced(v.try_into()?),
            Kind::Request(v) => Value::Request(v.try_into()?),
            Kind::Journal(v) => Value::Journal(v.try_into()?),
            Kind::Visibility(v) => Value::Visibility(v.try_into()?),
            Kind::Status(v) => Value::Status(v.try_into()?),
            Kind::Summary(v) => Value::Summary(v.try_into()?),
            Kind::Evidence(v) => Value::Evidence(v.try_into()?),
            Kind::Prefix(v) => Value::Prefix(v),
            Kind::RetainedPrefix(v) => Value::RetainedPrefix(v),
            Kind::Unit(_) => Value::Unit,
            Kind::Snapshot(v) => Value::Snapshot(
                required(v.snapshot)?.try_into()?,
                required(v.proof)?.try_into()?,
            ),
        })
    }
}

impl From<Error> for pb::RpcError {
    fn from(v: Error) -> Self {
        use pb::rpc_error::Kind;
        Self {
            kind: Some(match v {
                Error::Application(e) => Kind::Application(e.into()),
                Error::Identity => Kind::Identity(pb::Empty {}),
                Error::Capacity => Kind::Capacity(pb::Empty {}),
                Error::Unavailable => Kind::Unavailable(pb::Empty {}),
                Error::Protocol => Kind::Protocol(pb::Empty {}),
            }),
        }
    }
}
impl TryFrom<pb::RpcError> for Error {
    type Error = Error;
    fn try_from(v: pb::RpcError) -> Result<Self, Error> {
        use pb::rpc_error::Kind;
        Ok(match required(v.kind)? {
            Kind::Application(e) => Error::Application(e.try_into()?),
            Kind::Identity(_) => Error::Identity,
            Kind::Capacity(_) => Error::Capacity,
            Kind::Unavailable(_) => Error::Unavailable,
            Kind::Protocol(_) => Error::Protocol,
        })
    }
}
pub(super) fn request_to_wire(v: Request, timeout_millis: u64) -> Result<pb::Request, Error> {
    let result = pb::Request {
        version: v.version,
        identity: Some(v.identity.into()),
        nonce: v.nonce,
        operation: Some(v.operation.into()),
        timeout_millis,
    };
    if result.encoded_len() > MAX_FRAME_BYTES {
        return Err(Error::Capacity);
    }
    Ok(result)
}
pub(super) fn request_from_wire(v: pb::Request) -> Result<(Request, u64), Error> {
    if v.version != PROTOCOL_VERSION
        || v.timeout_millis == 0
        || v.timeout_millis > MAX_TIMEOUT.as_millis() as u64
    {
        return Err(Error::Protocol);
    }
    Ok((
        Request {
            version: v.version,
            identity: required(v.identity)?.try_into()?,
            nonce: v.nonce,
            operation: required(v.operation)?.try_into()?,
        },
        v.timeout_millis,
    ))
}
pub(super) fn reply_to_wire(v: Reply) -> Result<pb::Reply, Error> {
    let result = pb::Reply {
        version: v.version,
        identity: Some(v.identity.into()),
        nonce: v.nonce,
        operation: v.operation as u32,
        result: Some(match v.result {
            Ok(v) => pb::reply::Result::Value(v.into()),
            Err(e) => pb::reply::Result::Error(e.into()),
        }),
    };
    if result.encoded_len() > MAX_FRAME_BYTES {
        return Err(Error::Capacity);
    }
    Ok(result)
}
pub(super) fn reply_from_wire(v: pb::Reply) -> Result<Reply, Error> {
    if v.version != PROTOCOL_VERSION {
        return Err(Error::Protocol);
    }
    Ok(Reply {
        version: v.version,
        identity: required(v.identity)?.try_into()?,
        nonce: v.nonce,
        operation: v.operation.try_into().map_err(|_| Error::Protocol)?,
        result: match required(v.result)? {
            pb::reply::Result::Value(v) => Ok(v.try_into()?),
            pb::reply::Result::Error(e) => Err(e.try_into()?),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn journal() -> Journal {
        Journal::decode(include_bytes!(
            "../../tests/fixtures/request-journal-v3.json"
        ))
        .unwrap()
    }
    fn identity(j: &Journal) -> Identity {
        Identity {
            group: 9,
            node: 1,
            target: Target {
                shard: 9,
                epoch: 1,
                mode: j.authority(),
            },
            role: Role::Coordinator,
            authority: Authority {
                shard: 9,
                mode: j.authority(),
            },
            cohort: j.cohort().unwrap().clone(),
        }
    }
    fn request(operation: Operation) -> Request {
        Request {
            version: PROTOCOL_VERSION,
            identity: identity(&journal()),
            nonce: 17,
            operation,
        }
    }
    fn assert_request(operation: Operation) {
        let before = request(operation);
        let expected = serde_json::to_value(&before).unwrap();
        let wire = request_to_wire(before, 1000).unwrap();
        let bytes = wire.encode_to_vec();
        check_request(&bytes).unwrap();
        let (after, timeout) =
            request_from_wire(pb::Request::decode(bytes.as_slice()).unwrap()).unwrap();
        assert_eq!(timeout, 1000);
        assert_eq!(serde_json::to_value(after).unwrap(), expected);
    }
    fn assert_reply(value: Value) {
        let before = Reply {
            version: PROTOCOL_VERSION,
            operation: 7,
            identity: identity(&journal()),
            nonce: 17,
            result: Ok(value),
        };
        let expected = serde_json::to_value(&before).unwrap();
        let wire = reply_to_wire(before).unwrap();
        let bytes = wire.encode_to_vec();
        check_reply(&bytes).unwrap();
        let after = reply_from_wire(pb::Reply::decode(bytes.as_slice()).unwrap()).unwrap();
        assert_eq!(serde_json::to_value(after).unwrap(), expected);
    }
    fn grant(cohort: &Cohort) -> MemberCapacityGrant {
        MemberCapacityGrant {
            member: cohort.members[0].clone(),
            activation: CapacityActivation {
                nonce: [3; 32],
                log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
            },
        }
    }
    #[test]
    fn all_operation_variants_roundtrip_without_canonical_identity_changes() {
        let j = journal();
        let i = identity(&j);
        let authority = i.authority;
        let cohort = i.cohort;
        let target = Target {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
        };
        let proof = j.terminal_prefix(j.resolved_prefix()).unwrap();
        let cut = j.history_cursor().unwrap();
        let base = j.history_cursor_at(0).unwrap();
        let page = j.history_page(&base, Some(&cut)).unwrap();
        let plan =
            crate::request_plans::Plan::decode(&j.request([1; 16]).unwrap().payload).unwrap();
        let part = plan.parts[0].descriptor.clone();
        let write = plan.parts[0].request.clone();
        let prepared = RetainedPrepare {
            version: 1,
            authority: authority.clone(),
            participant: part.clone(),
            request: write.clone(),
            cut: cut.clone(),
        };
        let operations = vec![
            Operation::LookupTarget {
                authority: authority.clone(),
                epoch: 1,
                id: [2; 16],
            },
            Operation::PrepareRetained(Box::new(prepared)),
            Operation::GrantStatus {
                authority: authority.clone(),
                target: target.clone(),
                cohort: cohort.clone(),
            },
            Operation::ConfirmCapacity {
                authority: authority.clone(),
                epoch: 1,
            },
            Operation::HistoryStatus {
                authority: authority.clone(),
                target: target.clone(),
                cohort: cohort.clone(),
            },
            Operation::HistoryPage {
                authority: authority.clone(),
                epoch: 1,
                base,
                cut: Some(cut.clone()),
            },
            Operation::SyncHistory {
                authority: authority.clone(),
                target: target.clone(),
                page,
            },
            Operation::AdvanceRetained {
                authority: authority.clone(),
                target: target.clone(),
                ordinal: 1,
                cut,
            },
            Operation::LookupRequest {
                authority: authority.clone(),
                epoch: 1,
                id: [1; 16],
            },
            Operation::Summary {
                authority: authority.clone(),
                epoch: 1,
            },
            Operation::Terminal {
                authority: authority.clone(),
                epoch: 1,
                version: proof.version(),
            },
            Operation::Journal {
                authority: authority.clone(),
                epoch: 1,
            },
            Operation::Command {
                authority: authority.clone(),
                epoch: 1,
                command: Command::Decide {
                    id: [2; 16],
                    decision: Decision::Abort,
                },
            },
            Operation::Visibility {
                target: target.clone(),
            },
            Operation::Prepare {
                authority: authority.clone(),
                participant: part,
                request: write,
                journal: j.clone(),
            },
            Operation::Advance {
                authority,
                target: target.clone(),
                ordinal: 1,
                journal: j,
            },
            Operation::Prefix {
                target: target.clone(),
                proof: proof.clone(),
            },
            Operation::Read {
                target,
                limit: ReadLimit {
                    rows: 100,
                    bytes: 4096,
                    operations: 100,
                },
                proof,
            },
        ];
        assert_eq!(operations.len(), 18);
        for operation in operations {
            assert_request(operation);
        }
    }
    #[test]
    fn all_value_variants_and_command_variants_roundtrip() {
        let j = journal();
        let i = identity(&j);
        let authority = i.authority;
        let cohort = i.cohort;
        let cut = j.history_cursor().unwrap();
        let base = j.history_cursor_at(0).unwrap();
        let page = j.history_page(&base, Some(&cut)).unwrap();
        let version = j.resolved_prefix();
        let proof = j.terminal_prefix(version).unwrap();
        let mut byte_cohort = cohort.clone();
        byte_cohort.profile = CapacityProfile::ByteBound16MiBV1;
        let values = vec![
            Value::Target(TargetEvidence::from_journal(&j, [2; 16]).unwrap()),
            Value::Grant(CapacityGrantStatus {
                authority: authority.clone(),
                cohort: cohort.clone(),
                grant: grant(&cohort),
            }),
            Value::Capacity(CohortCapacity {
                authority: authority.clone(),
                cohort: byte_cohort.clone(),
                grants: vec![grant(&byte_cohort)],
            }),
            Value::HistoryStatus(HistoryStatus {
                cursor: cut.clone(),
                applied_prefix: version,
            }),
            Value::HistoryPage(page),
            Value::HistorySynced(HistoryStatus {
                cursor: cut,
                applied_prefix: version,
            }),
            Value::RetainedPrefix(version),
            Value::Request(RequestLookup {
                authority: authority.clone(),
                cohort: cohort.clone(),
                resolved_prefix: version,
                id: [1; 16],
                record: Some(RecoveredRequest {
                    record: j.request([1; 16]).unwrap().clone(),
                    transaction: j.transaction([2; 16]).unwrap().clone(),
                }),
            }),
            Value::Journal(j.clone()),
            Value::Unit,
            Value::Visibility(VisibilityInfo {
                authority: authority.clone(),
                baseline_sequence: 0,
                cohort: Some(cohort.clone()),
                applied_prefix: version,
            }),
            Value::Status(Status::Committed { sequence: u64::MAX }),
            Value::Prefix(version),
            Value::Summary(CoordinatorSummary {
                authority: authority.clone(),
                cohort: cohort.clone(),
                resolved_prefix: version,
            }),
            Value::Evidence(proof.clone()),
            Value::Snapshot(
                Snapshot {
                    shard: 7,
                    epoch: 1,
                    authority,
                    version,
                    baseline_sequence: 0,
                    cohort: Some(cohort),
                    applied_prefix: version,
                    local_sequence: 1,
                    replay_operations: 1,
                    entries: vec![(vec![0, 255], vec![1, 2, 3])],
                },
                proof,
            ),
        ];
        assert_eq!(values.len(), 16);
        for v in values {
            assert_reply(v);
        }
        for command in j.events().iter().cloned().chain([
            Command::Begin {
                id: [4; 16],
                participants: j.transaction([2; 16]).unwrap().participants.clone(),
            },
            Command::Prepared {
                id: [2; 16],
                participant: j.transaction([2; 16]).unwrap().participants[0].clone(),
            },
            Command::Decide {
                id: [2; 16],
                decision: Decision::Abort,
            },
            Command::ConfirmCohortCapacity {
                cohort: byte_cohort.clone(),
                grants: vec![grant(&byte_cohort)],
            },
        ]) {
            let wire = pb::Command::from(command.clone());
            let decoded: Command = wire.try_into().unwrap();
            assert_eq!(command, decoded);
        }
        let bytes = j.encode().unwrap();
        let wire = pb::Journal::from(j);
        let decoded = Journal::try_from(wire).unwrap();
        assert_eq!(bytes, decoded.encode().unwrap());
    }
    #[test]
    fn invalid_proofs_unknown_enums_and_replayed_journal_events_are_rejected() {
        let j = journal();
        let mut encoded = pb::Journal::from(j.clone());
        encoded.events.push(encoded.events[0].clone());
        assert!(Journal::try_from(encoded).is_err());
        let mut cursor = pb::HistoryCursor::from(j.history_cursor_at(0).unwrap());
        cursor.root[0] ^= 1;
        assert!(HistoryCursor::try_from(cursor).is_err());
        let mut page = pb::HistoryPage::from(
            j.history_page(&j.history_cursor_at(0).unwrap(), None)
                .unwrap(),
        );
        page.end.as_mut().unwrap().root[0] ^= 1;
        assert!(HistoryPage::try_from(page).is_err());
        let mut id = pb::Identity::from(identity(&j));
        id.role = 99;
        assert!(Identity::try_from(id).is_err());
        let mut proof = pb::TerminalPrefix::from(j.terminal_prefix(0).unwrap());
        proof.authority.pop();
        assert!(TerminalPrefix::try_from(proof).is_err());
        assert!(D1Error::try_from(pb::ApplicationError {
            kind: Some(pb::application_error::Kind::CoordinatorRejected(99))
        })
        .is_err());
    }
    #[test]
    fn all_transport_error_variants_roundtrip() {
        let errors = [
            Error::Identity,
            Error::Capacity,
            Error::Unavailable,
            Error::Protocol,
            Error::Application(D1Error::Fenced {
                requested: 1,
                current: u64::MAX,
            }),
            Error::Application(D1Error::CapacityConflict {
                expected: 0,
                current: usize::MAX,
            }),
            Error::Application(D1Error::CoordinatorRejected(
                crate::transaction_decisions::Error::IncompatibleHistory,
            )),
        ];
        for error in errors {
            let reply = Reply {
                version: PROTOCOL_VERSION,
                operation: 1,
                identity: identity(&journal()),
                nonce: 1,
                result: Err(error.clone()),
            };
            let wire = reply_to_wire(reply).unwrap();
            let bytes = wire.encode_to_vec();
            check_reply(&bytes).unwrap();
            let decoded = reply_from_wire(pb::Reply::decode(bytes.as_slice()).unwrap()).unwrap();
            assert!(matches!(decoded.result,Err(e) if e==error));
        }
    }
    #[test]
    fn prescan_rejects_duplicates_collections_and_malformed_frames() {
        let original = request_to_wire(
            request(Operation::Summary {
                authority: identity(&journal()).authority,
                epoch: 1,
            }),
            1000,
        )
        .unwrap();
        let mut duplicate = original.encode_to_vec();
        duplicate.extend([8, 6]);
        assert!(check_request(&duplicate).is_err());
        let mut duplicate_oneof = pb::Operation {
            kind: Some(pb::operation::Kind::Summary(pb::AuthorityEpoch::default())),
        }
        .encode_to_vec();
        duplicate_oneof.extend(
            pb::Operation {
                kind: Some(pb::operation::Kind::Journal(pb::AuthorityEpoch::default())),
            }
            .encode_to_vec(),
        );
        let mut budget = MAX_FRAME_BYTES * 4;
        assert!(bounded::check_nested("Operation", &duplicate_oneof, &mut budget).is_err());
        let mut request = original;
        request
            .identity
            .as_mut()
            .unwrap()
            .cohort
            .as_mut()
            .unwrap()
            .members =
            vec![pb::Member::default(); crate::transaction_decisions::MAX_PARTICIPANTS + 1];
        assert!(check_request(&request.encode_to_vec()).is_err());
        assert!(check_request(&[0xff; 12]).is_err());
        assert!(check_request(&[0]).is_err());
        assert!(check_request(&[0x0b, 0x0c]).is_err());
        let mut budget = 1;
        assert!(bounded::check_nested("Request", &[8, 6], &mut budget).is_err());
    }
}
