//! Checked typed conversion for fixed-version and renewable query transport.
use super::super::fixed::*;
use super::*;
use d2_raft::{
    participant_writes::Authority,
    terminal_prefix::{
        transport::{TerminalPrefixParts, TerminalRecord},
        TerminalPrefix,
    },
    transaction_decisions::{CapacityProfile, Cohort, Decision, Member, Participant},
    versioned_reads::{ReadLimit, Target},
    D1Error,
};
use prost::Message;
fn required<T>(value: Option<T>) -> Result<T, RemoteError> {
    value.ok_or(RemoteError::Protocol)
}
fn size(value: u64) -> Result<usize, RemoteError> {
    value.try_into().map_err(|_| RemoteError::Protocol)
}
fn array<const N: usize>(value: Vec<u8>) -> Result<[u8; N], RemoteError> {
    value.try_into().map_err(|_| RemoteError::Protocol)
}
impl From<CapacityProfile> for wire::CapacityProfile {
    fn from(v: CapacityProfile) -> Self {
        match v {
            CapacityProfile::LegacyCountV1 => Self::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1 => Self::ByteBound16MibV1,
        }
    }
}
impl TryFrom<wire::CapacityProfile> for CapacityProfile {
    type Error = RemoteError;
    fn try_from(v: wire::CapacityProfile) -> Result<Self, Self::Error> {
        Ok(match v {
            wire::CapacityProfile::LegacyCountV1 => Self::LegacyCountV1,
            wire::CapacityProfile::ByteBound16MibV1 => Self::ByteBound16MiBV1,
            wire::CapacityProfile::Unspecified => return Err(RemoteError::Protocol),
        })
    }
}
impl From<LocalReadProfile> for wire::LocalReadProfile {
    fn from(v: LocalReadProfile) -> Self {
        match v {
            LocalReadProfile::Parity2MiBV1 => Self::Parity2MibV1,
            LocalReadProfile::LocalRead8MiBV1 => Self::LocalRead8MibV1,
        }
    }
}
impl TryFrom<wire::LocalReadProfile> for LocalReadProfile {
    type Error = RemoteError;
    fn try_from(v: wire::LocalReadProfile) -> Result<Self, Self::Error> {
        Ok(match v {
            wire::LocalReadProfile::Parity2MibV1 => Self::Parity2MiBV1,
            wire::LocalReadProfile::LocalRead8MibV1 => Self::LocalRead8MiBV1,
            wire::LocalReadProfile::Unspecified => return Err(RemoteError::Protocol),
        })
    }
}
impl From<Decision> for wire::TerminalDecision {
    fn from(v: Decision) -> Self {
        match v {
            Decision::Commit => Self::Commit,
            Decision::Abort => Self::Abort,
        }
    }
}
impl TryFrom<wire::TerminalDecision> for Decision {
    type Error = RemoteError;
    fn try_from(v: wire::TerminalDecision) -> Result<Self, Self::Error> {
        Ok(match v {
            wire::TerminalDecision::Commit => Self::Commit,
            wire::TerminalDecision::Abort => Self::Abort,
            wire::TerminalDecision::Unspecified => return Err(RemoteError::Protocol),
        })
    }
}
impl From<Target> for wire::Target {
    fn from(v: Target) -> Self {
        Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: v.mode.to_vec(),
        }
    }
}
impl TryFrom<wire::Target> for Target {
    type Error = RemoteError;
    fn try_from(v: wire::Target) -> Result<Self, Self::Error> {
        let result = Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: array(v.mode)?,
        };
        Ok(result)
    }
}
impl From<Authority> for wire::Authority {
    fn from(v: Authority) -> Self {
        Self {
            shard: v.shard,
            mode: v.mode.to_vec(),
        }
    }
}
impl TryFrom<wire::Authority> for Authority {
    type Error = RemoteError;
    fn try_from(v: wire::Authority) -> Result<Self, Self::Error> {
        let result = Self {
            shard: v.shard,
            mode: array(v.mode)?,
        };
        Ok(result)
    }
}
impl From<Member> for wire::Member {
    fn from(v: Member) -> Self {
        Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: v.mode.to_vec(),
        }
    }
}
impl TryFrom<wire::Member> for Member {
    type Error = RemoteError;
    fn try_from(v: wire::Member) -> Result<Self, Self::Error> {
        let result = Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: array(v.mode)?,
        };
        Ok(result)
    }
}
impl From<Participant> for wire::Participant {
    fn from(v: Participant) -> Self {
        Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: v.mode.to_vec(),
            intent: v.intent.to_vec(),
        }
    }
}
impl TryFrom<wire::Participant> for Participant {
    type Error = RemoteError;
    fn try_from(v: wire::Participant) -> Result<Self, Self::Error> {
        let result = Self {
            shard: v.shard,
            epoch: v.epoch,
            mode: array(v.mode)?,
            intent: array(v.intent)?,
        };
        Ok(result)
    }
}
impl From<Cohort> for wire::Cohort {
    fn from(v: Cohort) -> Self {
        Self {
            coordinator_shard: v.coordinator_shard,
            coordinator_epoch: v.coordinator_epoch,
            members: v.members.into_iter().map(Into::into).collect(),
            profile: wire::CapacityProfile::from(v.profile) as i32,
        }
    }
}
impl TryFrom<wire::Cohort> for Cohort {
    type Error = RemoteError;
    fn try_from(v: wire::Cohort) -> Result<Self, Self::Error> {
        let result = Self {
            coordinator_shard: v.coordinator_shard,
            coordinator_epoch: v.coordinator_epoch,
            members: v
                .members
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            profile: wire::CapacityProfile::try_from(v.profile)
                .map_err(|_| RemoteError::Protocol)?
                .try_into()?,
        };
        result.validate().map_err(|_| RemoteError::Protocol)?;
        Ok(result)
    }
}
impl From<TerminalRecord> for wire::TerminalRecord {
    fn from(v: TerminalRecord) -> Self {
        Self {
            id: v.id.to_vec(),
            ordinal: v.ordinal,
            participants: v.participants.into_iter().map(Into::into).collect(),
            prepared: v.prepared,
            decision: wire::TerminalDecision::from(v.decision) as i32,
        }
    }
}
impl TryFrom<wire::TerminalRecord> for TerminalRecord {
    type Error = RemoteError;
    fn try_from(v: wire::TerminalRecord) -> Result<Self, Self::Error> {
        let result = Self {
            id: array(v.id)?,
            ordinal: v.ordinal,
            participants: v
                .participants
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            prepared: v.prepared,
            decision: wire::TerminalDecision::try_from(v.decision)
                .map_err(|_| RemoteError::Protocol)?
                .try_into()?,
        };
        Ok(result)
    }
}
impl From<TerminalPrefix> for wire::TerminalPrefix {
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
                .map(|records| wire::TerminalSegment {
                    records: records.into_iter().map(Into::into).collect(),
                })
                .collect(),
        }
    }
}
impl TryFrom<wire::TerminalPrefix> for TerminalPrefix {
    type Error = RemoteError;
    fn try_from(v: wire::TerminalPrefix) -> Result<Self, Self::Error> {
        let cohort: Cohort = required(v.cohort)?.try_into()?;
        if v.segments.len()
            > cohort
                .profile
                .max_transactions()
                .div_ceil(d2_raft::terminal_prefix::SEGMENT_TRANSACTIONS)
            || v.segments.iter().any(|s| {
                s.records.len() > d2_raft::terminal_prefix::SEGMENT_TRANSACTIONS
                    || s.records.iter().any(|r| {
                        r.participants.len() > d2_raft::transaction_decisions::MAX_PARTICIPANTS
                            || r.prepared.len() > d2_raft::transaction_decisions::MAX_PARTICIPANTS
                    })
            })
        {
            return Err(RemoteError::Protocol);
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
                        .collect::<Result<Vec<_>, RemoteError>>()
                })
                .collect::<Result<_, _>>()?,
        }
        .try_into()
        .map_err(|_| RemoteError::Protocol)
    }
}
impl From<ReadLimit> for wire::ReadLimit {
    fn from(v: ReadLimit) -> Self {
        Self {
            rows: v.rows as u64,
            bytes: v.bytes as u64,
            operations: v.operations as u64,
        }
    }
}
impl TryFrom<wire::ReadLimit> for ReadLimit {
    type Error = RemoteError;
    fn try_from(v: wire::ReadLimit) -> Result<Self, Self::Error> {
        let result = Self {
            rows: size(v.rows)?,
            bytes: size(v.bytes)?,
            operations: size(v.operations)?,
        };
        Ok(result)
    }
}
impl From<FixedOpenRequest> for wire::FixedOpenRequest {
    fn from(v: FixedOpenRequest) -> Self {
        Self {
            profile: wire::LocalReadProfile::from(v.profile) as i32,
            target: Some(v.target.into()),
            proof: Some(v.proof.into()),
            capture_limit: Some(v.capture_limit.into()),
            ttl_millis: v.ttl_millis,
            acquire_timeout_millis: v.acquire_timeout_millis,
        }
    }
}
impl TryFrom<wire::FixedOpenRequest> for FixedOpenRequest {
    type Error = RemoteError;
    fn try_from(v: wire::FixedOpenRequest) -> Result<Self, Self::Error> {
        let result = Self {
            profile: wire::LocalReadProfile::try_from(v.profile)
                .map_err(|_| RemoteError::Protocol)?
                .try_into()?,
            target: required(v.target)?.try_into()?,
            proof: required(v.proof)?.try_into()?,
            capture_limit: required(v.capture_limit)?.try_into()?,
            ttl_millis: v.ttl_millis,
            acquire_timeout_millis: v.acquire_timeout_millis,
        };
        result.validate(result.target.shard)?;
        Ok(result)
    }
}
impl From<RenewableFixedOpenRequest> for wire::RenewableFixedOpenRequest {
    fn from(v: RenewableFixedOpenRequest) -> Self {
        Self {
            open: Some(v.open.into()),
            hard_lifetime_millis: v.hard_lifetime_millis,
        }
    }
}
impl TryFrom<wire::RenewableFixedOpenRequest> for RenewableFixedOpenRequest {
    type Error = RemoteError;
    fn try_from(v: wire::RenewableFixedOpenRequest) -> Result<Self, Self::Error> {
        let result = Self {
            open: required(v.open)?.try_into()?,
            hard_lifetime_millis: v.hard_lifetime_millis,
        };
        result.validate(result.open.target.shard)?;
        Ok(result)
    }
}
impl From<FixedDescriptor> for wire::FixedDescriptor {
    fn from(v: FixedDescriptor) -> Self {
        Self {
            profile: wire::LocalReadProfile::from(v.profile) as i32,
            stamp: Some(v.stamp.into()),
            incarnation: v.incarnation.to_vec(),
            token: v.token.to_vec(),
            authority: Some(v.authority.into()),
            cohort: Some(v.cohort.into()),
            version: v.version,
            mode: v.mode.to_vec(),
        }
    }
}
impl TryFrom<wire::FixedDescriptor> for FixedDescriptor {
    type Error = RemoteError;
    fn try_from(v: wire::FixedDescriptor) -> Result<Self, Self::Error> {
        let result = Self {
            profile: wire::LocalReadProfile::try_from(v.profile)
                .map_err(|_| RemoteError::Protocol)?
                .try_into()?,
            stamp: required(v.stamp)?.try_into()?,
            incarnation: array(v.incarnation)?,
            token: array(v.token)?,
            authority: required(v.authority)?.try_into()?,
            cohort: required(v.cohort)?.try_into()?,
            version: v.version,
            mode: array(v.mode)?,
        };
        Ok(result)
    }
}
impl From<CaptureCost> for wire::CaptureCost {
    fn from(v: CaptureCost) -> Self {
        Self {
            rows: v.rows as u64,
            bytes: v.bytes as u64,
            replay_operations: v.replay_operations as u64,
        }
    }
}
impl TryFrom<wire::CaptureCost> for CaptureCost {
    type Error = RemoteError;
    fn try_from(v: wire::CaptureCost) -> Result<Self, Self::Error> {
        let result = Self {
            rows: size(v.rows)?,
            bytes: size(v.bytes)?,
            replay_operations: size(v.replay_operations)?,
        };
        Ok(result)
    }
}
impl From<FixedOpened> for wire::FixedOpened {
    fn from(v: FixedOpened) -> Self {
        Self {
            descriptor: Some(v.descriptor.into()),
            retained_proof: Some(v.retained_proof.into()),
            capture_cost: Some(v.capture_cost.into()),
        }
    }
}
impl TryFrom<wire::FixedOpened> for FixedOpened {
    type Error = RemoteError;
    fn try_from(v: wire::FixedOpened) -> Result<Self, Self::Error> {
        let result = Self {
            descriptor: required(v.descriptor)?.try_into()?,
            retained_proof: required(v.retained_proof)?.try_into()?,
            capture_cost: required(v.capture_cost)?.try_into()?,
        };
        Ok(result)
    }
}
impl From<RenewableFixedOpened> for wire::RenewableFixedOpened {
    fn from(v: RenewableFixedOpened) -> Self {
        Self {
            opened: Some(v.opened.into()),
            grant: Some(v.grant.into()),
        }
    }
}
impl TryFrom<wire::RenewableFixedOpened> for RenewableFixedOpened {
    type Error = RemoteError;
    fn try_from(v: wire::RenewableFixedOpened) -> Result<Self, Self::Error> {
        let result = Self {
            opened: required(v.opened)?.try_into()?,
            grant: required(v.grant)?.try_into()?,
        };
        Ok(result)
    }
}
impl From<FixedRenewRequest> for wire::FixedRenewRequest {
    fn from(v: FixedRenewRequest) -> Self {
        Self {
            descriptor: Some(v.descriptor.into()),
            generation: v.generation,
            lease_millis: v.lease_millis,
        }
    }
}
impl TryFrom<wire::FixedRenewRequest> for FixedRenewRequest {
    type Error = RemoteError;
    fn try_from(v: wire::FixedRenewRequest) -> Result<Self, Self::Error> {
        let result = Self {
            descriptor: required(v.descriptor)?.try_into()?,
            generation: v.generation,
            lease_millis: v.lease_millis,
        };
        Ok(result)
    }
}
impl From<FixedLeaseGrant> for wire::FixedLeaseGrant {
    fn from(v: FixedLeaseGrant) -> Self {
        Self {
            descriptor: Some(v.descriptor.into()),
            generation: v.generation,
            granted_millis: v.granted_millis,
        }
    }
}
impl TryFrom<wire::FixedLeaseGrant> for FixedLeaseGrant {
    type Error = RemoteError;
    fn try_from(v: wire::FixedLeaseGrant) -> Result<Self, Self::Error> {
        let result = Self {
            descriptor: required(v.descriptor)?.try_into()?,
            generation: v.generation,
            granted_millis: v.granted_millis,
        };
        Ok(result)
    }
}
impl From<FixedFragmentRequest> for wire::FixedFragmentRequest {
    fn from(v: FixedFragmentRequest) -> Self {
        Self {
            descriptor: Some(v.descriptor.into()),
            request_sequence: v.request_sequence,
            fragment: Some(v.fragment.into()),
            remaining: Some(v.remaining.into()),
        }
    }
}
impl TryFrom<wire::FixedFragmentRequest> for FixedFragmentRequest {
    type Error = RemoteError;
    fn try_from(v: wire::FixedFragmentRequest) -> Result<Self, Self::Error> {
        let result = Self {
            descriptor: required(v.descriptor)?.try_into()?,
            request_sequence: v.request_sequence,
            fragment: required(v.fragment)?.try_into()?,
            remaining: required(v.remaining)?.try_into()?,
        };
        Ok(result)
    }
}
impl From<FixedFragmentOutput> for wire::FixedFragmentOutput {
    fn from(v: FixedFragmentOutput) -> Self {
        Self {
            descriptor: Some(v.descriptor.into()),
            request_sequence: v.request_sequence,
            cost: Some(v.cost.into()),
            value: Some(v.value.into()),
        }
    }
}
impl TryFrom<wire::FixedFragmentOutput> for FixedFragmentOutput {
    type Error = RemoteError;
    fn try_from(v: wire::FixedFragmentOutput) -> Result<Self, Self::Error> {
        let result = Self {
            descriptor: required(v.descriptor)?.try_into()?,
            request_sequence: v.request_sequence,
            cost: required(v.cost)?.try_into()?,
            value: required(v.value)?.try_into()?,
        };
        Ok(result)
    }
}
impl From<d2_raft::transaction_decisions::Error> for wire::CoordinatorError {
    fn from(v: d2_raft::transaction_decisions::Error) -> Self {
        match v {
            d2_raft::transaction_decisions::Error::InvalidIdentity => {
                Self::CoordinatorInvalidIdentity
            }
            d2_raft::transaction_decisions::Error::InvalidParticipants => {
                Self::CoordinatorInvalidParticipants
            }
            d2_raft::transaction_decisions::Error::IdentityConflict => {
                Self::CoordinatorIdentityConflict
            }
            d2_raft::transaction_decisions::Error::UnknownTransaction => {
                Self::CoordinatorUnknownTransaction
            }
            d2_raft::transaction_decisions::Error::UnknownParticipant => {
                Self::CoordinatorUnknownParticipant
            }
            d2_raft::transaction_decisions::Error::NotPrepared => Self::CoordinatorNotPrepared,
            d2_raft::transaction_decisions::Error::AlreadyDecided => {
                Self::CoordinatorAlreadyDecided
            }
            d2_raft::transaction_decisions::Error::Capacity => Self::CoordinatorCapacity,
            d2_raft::transaction_decisions::Error::CapacityNotConfirmed => {
                Self::CoordinatorCapacityNotConfirmed
            }
            d2_raft::transaction_decisions::Error::InvalidJournal => {
                Self::CoordinatorInvalidJournal
            }
            d2_raft::transaction_decisions::Error::IncompatibleHistory => {
                Self::CoordinatorIncompatibleHistory
            }
        }
    }
}
impl TryFrom<wire::CoordinatorError> for d2_raft::transaction_decisions::Error {
    type Error = RemoteError;
    fn try_from(v: wire::CoordinatorError) -> Result<Self, Self::Error> {
        Ok(match v {
            wire::CoordinatorError::CoordinatorInvalidIdentity => Self::InvalidIdentity,
            wire::CoordinatorError::CoordinatorInvalidParticipants => Self::InvalidParticipants,
            wire::CoordinatorError::CoordinatorIdentityConflict => Self::IdentityConflict,
            wire::CoordinatorError::CoordinatorUnknownTransaction => Self::UnknownTransaction,
            wire::CoordinatorError::CoordinatorUnknownParticipant => Self::UnknownParticipant,
            wire::CoordinatorError::CoordinatorNotPrepared => Self::NotPrepared,
            wire::CoordinatorError::CoordinatorAlreadyDecided => Self::AlreadyDecided,
            wire::CoordinatorError::CoordinatorCapacity => Self::Capacity,
            wire::CoordinatorError::CoordinatorCapacityNotConfirmed => Self::CapacityNotConfirmed,
            wire::CoordinatorError::CoordinatorInvalidJournal => Self::InvalidJournal,
            wire::CoordinatorError::CoordinatorIncompatibleHistory => Self::IncompatibleHistory,
            wire::CoordinatorError::Unspecified => return Err(RemoteError::Protocol),
        })
    }
}
impl From<D1Error> for wire::ApplicationError {
    fn from(v: D1Error) -> Self {
        use wire::application_error::Kind;
        Self {
            kind: Some(match v {
                D1Error::NotActivated => Kind::NotActivated(wire::Empty {}),
                D1Error::WrongShard { requested, current } => Kind::WrongShard(wire::ErrorPair {
                    first: requested,
                    current,
                }),
                D1Error::Fenced { requested, current } => Kind::Fenced(wire::ErrorPair {
                    first: requested,
                    current,
                }),
                D1Error::Conflict { expected, current } => Kind::Conflict(wire::ErrorPair {
                    first: expected,
                    current,
                }),
                D1Error::IdempotencyConflict => Kind::IdempotencyConflict(wire::Empty {}),
                D1Error::EmptyBatch => Kind::EmptyBatch(wire::Empty {}),
                D1Error::BatchTooLarge => Kind::BatchTooLarge(wire::Empty {}),
                D1Error::SequenceOverflow => Kind::SequenceOverflow(wire::Empty {}),
                D1Error::DedupCapacity => Kind::DedupCapacity(wire::Empty {}),
                D1Error::InvalidDedupCapacity => Kind::InvalidDedupCapacity(wire::Empty {}),
                D1Error::CapacityConflict { expected, current } => {
                    Kind::CapacityConflict(wire::ErrorPair {
                        first: expected as u64,
                        current: current as u64,
                    })
                }
                D1Error::LegacyDataPresent => Kind::LegacyDataPresent(wire::Empty {}),
                D1Error::LegacyWriteDisabled => Kind::LegacyWriteDisabled(wire::Empty {}),
                D1Error::InvalidEpoch => Kind::InvalidEpoch(wire::Empty {}),
                D1Error::ManagedWriteRequired => Kind::ManagedWriteRequired(wire::Empty {}),
                D1Error::ManagedWritesNotEnabled => Kind::ManagedWritesNotEnabled(wire::Empty {}),
                D1Error::InvalidManagedMode => Kind::InvalidManagedMode(wire::Empty {}),
                D1Error::ManagedModeConflict => Kind::ManagedModeConflict(wire::Empty {}),
                D1Error::UnverifiedWriteHistory => Kind::UnverifiedWriteHistory(wire::Empty {}),
                D1Error::VisibilityNotEnabled => Kind::VisibilityNotEnabled(wire::Empty {}),
                D1Error::VisibilityRequired => Kind::VisibilityRequired(wire::Empty {}),
                D1Error::VisibilityHistory => Kind::VisibilityHistory(wire::Empty {}),
                D1Error::VisibilityGap => Kind::VisibilityGap(wire::Empty {}),
                D1Error::VisibilityUnresolved => Kind::VisibilityUnresolved(wire::Empty {}),
                D1Error::VisibilityNotApplied => Kind::VisibilityNotApplied(wire::Empty {}),
                D1Error::VisibilityReadLimit => Kind::VisibilityReadLimit(wire::Empty {}),
                D1Error::ParticipantOnly => Kind::ParticipantOnly(wire::Empty {}),
                D1Error::ParticipantNotEnabled => Kind::ParticipantNotEnabled(wire::Empty {}),
                D1Error::ParticipantAuthority => Kind::ParticipantAuthority(wire::Empty {}),
                D1Error::ParticipantIdentity => Kind::ParticipantIdentity(wire::Empty {}),
                D1Error::ParticipantBusy => Kind::ParticipantBusy(wire::Empty {}),
                D1Error::ParticipantCapacity => Kind::ParticipantCapacity(wire::Empty {}),
                D1Error::ParticipantUnknown => Kind::ParticipantUnknown(wire::Empty {}),
                D1Error::ParticipantDecisionConflict => {
                    Kind::ParticipantDecisionConflict(wire::Empty {})
                }
                D1Error::CoordinatorOnly => Kind::CoordinatorOnly(wire::Empty {}),
                D1Error::CoordinatorNotEnabled => Kind::CoordinatorNotEnabled(wire::Empty {}),
                D1Error::CoordinatorRequiresEmptyShard => {
                    Kind::CoordinatorRequiresEmptyShard(wire::Empty {})
                }
                D1Error::DecisionIndexHistory => Kind::DecisionIndexHistory(wire::Empty {}),
                D1Error::DecisionIndexUnavailable => Kind::DecisionIndexUnavailable(wire::Empty {}),
                D1Error::EventLedgerHistory => Kind::EventLedgerHistory(wire::Empty {}),
                D1Error::EventLedgerUnavailable => Kind::EventLedgerUnavailable(wire::Empty {}),
                D1Error::RequestIndexHistory => Kind::RequestIndexHistory(wire::Empty {}),
                D1Error::RequestIndexUnavailable => Kind::RequestIndexUnavailable(wire::Empty {}),
                D1Error::CoordinatorRejected(v) => {
                    Kind::CoordinatorRejected(wire::CoordinatorError::from(v) as i32)
                }
            }),
        }
    }
}
impl TryFrom<wire::ApplicationError> for D1Error {
    type Error = RemoteError;
    fn try_from(v: wire::ApplicationError) -> Result<Self, Self::Error> {
        use wire::application_error::Kind;
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
                wire::CoordinatorError::try_from(v)
                    .map_err(|_| RemoteError::Protocol)?
                    .try_into()?,
            ),
        })
    }
}

pub(super) fn application_status(error: D1Error) -> tonic::Status {
    let detail = wire::FixedErrorDetail {
        application: Some(error.into()),
    };
    tonic::Status::with_details(
        tonic::Code::FailedPrecondition,
        "fixed query application error",
        detail.encode_to_vec().into(),
    )
}
pub(super) fn application_error(status: &tonic::Status) -> Result<Option<D1Error>, RemoteError> {
    if status.details().is_empty() {
        return Ok(None);
    }
    let detail =
        wire::FixedErrorDetail::decode(status.details()).map_err(|_| RemoteError::Protocol)?;
    let Some(application) = detail.application else {
        return Ok(None);
    };
    if status.code() != tonic::Code::FailedPrecondition {
        return Err(RemoteError::Protocol);
    }
    Ok(Some(application.try_into()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn proof() -> TerminalPrefix {
        TerminalPrefixParts {
            format_version: 1,
            authority: [9; 16],
            cohort: Cohort {
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
                profile: CapacityProfile::LegacyCountV1,
            },
            through: 1,
            segments: vec![vec![TerminalRecord {
                id: [1; 16],
                ordinal: 1,
                participants: vec![Participant {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                    intent: [3; 32],
                }],
                prepared: vec![true],
                decision: Decision::Commit,
            }]],
        }
        .try_into()
        .unwrap()
    }
    fn descriptor() -> FixedDescriptor {
        FixedDescriptor {
            profile: LocalReadProfile::Parity2MiBV1,
            stamp: QueryStamp {
                identity: RemoteIdentity { group: 7, node: 1 },
                shard: 7,
                epoch: 1,
                sequence: 123,
            },
            incarnation: [2; 16],
            token: [3; 16],
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: proof().cohort().clone(),
            version: 1,
            mode: [7; 16],
        }
    }
    fn open() -> FixedOpenRequest {
        FixedOpenRequest {
            profile: LocalReadProfile::Parity2MiBV1,
            target: Target {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            },
            proof: proof(),
            capture_limit: LocalReadProfile::Parity2MiBV1.capture_limit(),
            ttl_millis: 1000,
            acquire_timeout_millis: 1000,
        }
    }
    fn opened() -> FixedOpened {
        FixedOpened {
            descriptor: descriptor(),
            retained_proof: proof(),
            capture_cost: CaptureCost {
                rows: 1,
                bytes: 64,
                replay_operations: 1,
            },
        }
    }
    fn grant() -> FixedLeaseGrant {
        FixedLeaseGrant {
            descriptor: descriptor(),
            generation: 2,
            granted_millis: 1000,
        }
    }
    #[test]
    fn all_fixed_envelopes_roundtrip_without_loss() {
        macro_rules! check {
            ($native:expr,$wire:ty,$native_ty:ty) => {{
                let wire: $wire = ($native).into();
                let bytes = wire.encode_to_vec();
                let native = <$native_ty>::try_from(wire).unwrap();
                assert_eq!(<$wire>::from(native).encode_to_vec(), bytes);
            }};
        }
        check!(open(), wire::FixedOpenRequest, FixedOpenRequest);
        check!(
            RenewableFixedOpenRequest {
                open: open(),
                hard_lifetime_millis: 30000
            },
            wire::RenewableFixedOpenRequest,
            RenewableFixedOpenRequest
        );
        check!(opened(), wire::FixedOpened, FixedOpened);
        check!(
            RenewableFixedOpened {
                opened: opened(),
                grant: grant()
            },
            wire::RenewableFixedOpened,
            RenewableFixedOpened
        );
        check!(
            FixedRenewRequest {
                descriptor: descriptor(),
                generation: 2,
                lease_millis: 1000
            },
            wire::FixedRenewRequest,
            FixedRenewRequest
        );
        check!(grant(), wire::FixedLeaseGrant, FixedLeaseGrant);
        check!(
            FixedFragmentRequest {
                descriptor: descriptor(),
                request_sequence: u64::MAX,
                fragment: Fragment::Exists(VertexId {
                    partition: 7,
                    id: u64::MAX
                }),
                remaining: Cost {
                    bytes: MAX_BYTES,
                    work: MAX_WORK
                }
            },
            wire::FixedFragmentRequest,
            FixedFragmentRequest
        );
        check!(
            FixedFragmentOutput {
                descriptor: descriptor(),
                request_sequence: u64::MAX,
                cost: Cost { bytes: 1, work: 2 },
                value: FragmentValue::Bool(true)
            },
            wire::FixedFragmentOutput,
            FixedFragmentOutput
        );
        let wire: wire::TerminalPrefix = proof().into();
        assert_eq!(TerminalPrefix::try_from(wire).unwrap(), proof());
    }
    #[test]
    fn invalid_fixed_proofs_and_profiles_fail_closed() {
        assert!(FixedOpenRequest::try_from(wire::FixedOpenRequest::default()).is_err());
        let mut v: wire::FixedOpenRequest = open().into();
        v.profile = 0;
        assert!(FixedOpenRequest::try_from(v).is_err());
        let mut v: wire::FixedOpenRequest = open().into();
        v.profile = 999;
        assert!(FixedOpenRequest::try_from(v).is_err());
        let mut v: wire::TerminalPrefix = proof().into();
        v.format_version = 2;
        assert!(TerminalPrefix::try_from(v).is_err());
        let mut v: wire::TerminalPrefix = proof().into();
        v.segments[0].records[0].participants[0].intent.pop();
        assert!(TerminalPrefix::try_from(v).is_err());
        let mut v: wire::TerminalPrefix = proof().into();
        v.segments[0].records[0].decision = 999;
        assert!(TerminalPrefix::try_from(v).is_err());
        let mut v: wire::TerminalPrefix = proof().into();
        v.segments[0].records[0].prepared[0] = false;
        assert!(TerminalPrefix::try_from(v).is_err());
        let mut v: wire::TerminalPrefix = proof().into();
        v.segments[0].records[0].ordinal = 2;
        assert!(TerminalPrefix::try_from(v).is_err());
        let mut v: wire::TerminalPrefix = proof().into();
        v.cohort.as_mut().unwrap().profile = 0;
        assert!(TerminalPrefix::try_from(v).is_err());
        let mut v: wire::FixedDescriptor = descriptor().into();
        v.mode.pop();
        assert!(FixedDescriptor::try_from(v).is_err());
        let mut v: wire::RenewableFixedOpenRequest = RenewableFixedOpenRequest {
            open: open(),
            hard_lifetime_millis: 30000,
        }
        .into();
        v.open.as_mut().unwrap().profile = wire::LocalReadProfile::LocalRead8MibV1 as i32;
        assert!(RenewableFixedOpenRequest::try_from(v).is_err());
    }
    #[test]
    fn application_marker_and_status_are_strict() {
        assert!(application_error(&tonic::Status::unavailable("transport"))
            .unwrap()
            .is_none());
        let detail = wire::FixedErrorDetail {
            application: Some(wire::ApplicationError { kind: None }),
        }
        .encode_to_vec();
        assert!(application_error(&tonic::Status::with_details(
            tonic::Code::FailedPrecondition,
            "bad",
            detail.into()
        ))
        .is_err());
        let detail = wire::FixedErrorDetail {
            application: Some(D1Error::VisibilityReadLimit.into()),
        }
        .encode_to_vec();
        assert!(application_error(&tonic::Status::with_details(
            tonic::Code::Ok,
            "bad",
            detail.into()
        ))
        .is_err());
        let unknown = wire::ApplicationError {
            kind: Some(wire::application_error::Kind::CoordinatorRejected(999)),
        };
        assert!(D1Error::try_from(unknown).is_err());
    }
    #[test]
    fn every_application_error_preserves_exact_category_and_fields() {
        for error in [
            D1Error::NotActivated,
            D1Error::WrongShard {
                requested: u64::MAX,
                current: 1,
            },
            D1Error::Fenced {
                requested: u64::MAX,
                current: 1,
            },
            D1Error::Conflict {
                expected: u64::MAX,
                current: 1,
            },
            D1Error::IdempotencyConflict,
            D1Error::EmptyBatch,
            D1Error::BatchTooLarge,
            D1Error::SequenceOverflow,
            D1Error::DedupCapacity,
            D1Error::InvalidDedupCapacity,
            D1Error::CapacityConflict {
                expected: usize::MAX,
                current: 1,
            },
            D1Error::LegacyDataPresent,
            D1Error::LegacyWriteDisabled,
            D1Error::InvalidEpoch,
            D1Error::ManagedWriteRequired,
            D1Error::ManagedWritesNotEnabled,
            D1Error::InvalidManagedMode,
            D1Error::ManagedModeConflict,
            D1Error::UnverifiedWriteHistory,
            D1Error::VisibilityNotEnabled,
            D1Error::VisibilityRequired,
            D1Error::VisibilityHistory,
            D1Error::VisibilityGap,
            D1Error::VisibilityUnresolved,
            D1Error::VisibilityNotApplied,
            D1Error::VisibilityReadLimit,
            D1Error::ParticipantOnly,
            D1Error::ParticipantNotEnabled,
            D1Error::ParticipantAuthority,
            D1Error::ParticipantIdentity,
            D1Error::ParticipantBusy,
            D1Error::ParticipantCapacity,
            D1Error::ParticipantUnknown,
            D1Error::ParticipantDecisionConflict,
            D1Error::CoordinatorOnly,
            D1Error::CoordinatorNotEnabled,
            D1Error::CoordinatorRequiresEmptyShard,
            D1Error::DecisionIndexHistory,
            D1Error::DecisionIndexUnavailable,
            D1Error::EventLedgerHistory,
            D1Error::EventLedgerUnavailable,
            D1Error::RequestIndexHistory,
            D1Error::RequestIndexUnavailable,
            D1Error::CoordinatorRejected(d2_raft::transaction_decisions::Error::InvalidIdentity),
            D1Error::CoordinatorRejected(
                d2_raft::transaction_decisions::Error::InvalidParticipants,
            ),
            D1Error::CoordinatorRejected(d2_raft::transaction_decisions::Error::IdentityConflict),
            D1Error::CoordinatorRejected(d2_raft::transaction_decisions::Error::UnknownTransaction),
            D1Error::CoordinatorRejected(d2_raft::transaction_decisions::Error::UnknownParticipant),
            D1Error::CoordinatorRejected(d2_raft::transaction_decisions::Error::NotPrepared),
            D1Error::CoordinatorRejected(d2_raft::transaction_decisions::Error::AlreadyDecided),
            D1Error::CoordinatorRejected(d2_raft::transaction_decisions::Error::Capacity),
            D1Error::CoordinatorRejected(
                d2_raft::transaction_decisions::Error::CapacityNotConfirmed,
            ),
            D1Error::CoordinatorRejected(d2_raft::transaction_decisions::Error::InvalidJournal),
            D1Error::CoordinatorRejected(
                d2_raft::transaction_decisions::Error::IncompatibleHistory,
            ),
        ] {
            let status = application_status(error.clone());
            assert_eq!(application_error(&status).unwrap(), Some(error));
        }
    }
}
