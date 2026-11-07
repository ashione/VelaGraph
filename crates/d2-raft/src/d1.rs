//! Versioned D1 application commands. These are independent of the local D1
//! trait: this crate does not yet provide a remote graph storage adapter.
use serde::{Deserialize, Serialize};

use crate::WriteOp;

pub const MAX_BATCH_OPERATIONS: usize = 10_000;
pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
/// No eviction: exhaustion rejects new requests, never silently loses retries.
pub const MAX_DEDUP_REQUESTS: usize = 4_096;
pub const MAX_DEDUP_BYTES: usize = 64 * 1024 * 1024;
/// Explicit opt-in tier; never automatically raised and never pruned.
pub const EXTENDED_DEDUP_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct D1Capacity {
    pub bytes_limit: usize,
    pub retained_bytes: usize,
    pub retained_requests: usize,
    pub max_requests: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1WriteRequest {
    pub shard: u64,
    pub epoch: u64,
    pub request_id: [u8; 16],
    pub expected_sequence: Option<u64>,
    pub operations: Vec<WriteOp>,
}

/// One Raft group owns exactly one shard. Ownership epochs are not Raft terms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum D1Command {
    /// Opt into D1 on an empty legacy state machine. Repeating it is safe.
    Activate {
        shard: u64,
        epoch: u64,
    },
    /// Compare-and-set ownership fencing, replicated in the same log as writes.
    SetEpoch {
        shard: u64,
        expected_epoch: u64,
        new_epoch: u64,
    },
    SetDedupCapacity {
        shard: u64,
        epoch: u64,
        expected_bytes: usize,
        new_bytes: usize,
    },
    Write(D1WriteRequest),
    EnableVersionedReads {
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        journal: crate::transaction_decisions::Journal,
    },
    AdvanceVisibility {
        authority: crate::participant_writes::Authority,
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        ordinal: u64,
        journal: crate::transaction_decisions::Journal,
    },
    EnableParticipant {
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        authority: crate::participant_writes::Authority,
    },
    PrepareParticipant {
        authority: crate::participant_writes::Authority,
        participant: crate::transaction_decisions::Participant,
        request: D1WriteRequest,
    },
    ResolveParticipant {
        authority: crate::participant_writes::Authority,
        participant: crate::transaction_decisions::Participant,
        id: [u8; 16],
        decision: crate::transaction_decisions::Decision,
    },
    /// Irreversibly reserve an empty managed shard for coordinator decisions.
    EnableCoordinator {
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
    },
    Coordinator {
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        command: crate::transaction_decisions::Command,
    },
    /// Irreversible opt-in for this shard. This is not a global transaction mode.
    EnableManagedWrites {
        shard: u64,
        epoch: u64,
        expected_sequence: u64,
        mode_id: [u8; 16],
    },
    /// One atomic shard-local transaction, identified by request.request_id.
    ManagedWrite {
        mode_id: [u8; 16],
        request: D1WriteRequest,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum D1WriteStatus {
    Committed,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum D1Result {
    Visibility {
        applied_prefix: u64,
    },
    Participant(crate::participant_writes::Status),
    Coordinator {
        resolved_prefix: u64,
    },
    Activated {
        shard: u64,
        epoch: u64,
    },
    Epoch {
        epoch: u64,
    },
    DedupCapacity {
        bytes_limit: usize,
    },
    ManagedWritesEnabled {
        mode_id: [u8; 16],
        activated_sequence: u64,
    },
    ManagedWrite {
        mode_id: [u8; 16],
        sequence: u64,
        status: D1WriteStatus,
        applied_operations: usize,
    },
    Write {
        sequence: u64,
        status: D1WriteStatus,
        applied_operations: usize,
    },
}

/// Application rejection is a successful Raft apply, not a storage failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum D1Error {
    NotActivated,
    WrongShard { requested: u64, current: u64 },
    Fenced { requested: u64, current: u64 },
    Conflict { expected: u64, current: u64 },
    IdempotencyConflict,
    EmptyBatch,
    BatchTooLarge,
    SequenceOverflow,
    DedupCapacity,
    InvalidDedupCapacity,
    CapacityConflict { expected: usize, current: usize },
    LegacyDataPresent,
    LegacyWriteDisabled,
    InvalidEpoch,
    ManagedWriteRequired,
    ManagedWritesNotEnabled,
    InvalidManagedMode,
    ManagedModeConflict,
    UnverifiedWriteHistory,
    VisibilityNotEnabled,
    VisibilityRequired,
    VisibilityHistory,
    VisibilityGap,
    VisibilityUnresolved,
    VisibilityNotApplied,
    VisibilityReadLimit,
    ParticipantOnly,
    ParticipantNotEnabled,
    ParticipantAuthority,
    ParticipantIdentity,
    ParticipantBusy,
    ParticipantCapacity,
    ParticipantUnknown,
    ParticipantDecisionConflict,
    CoordinatorOnly,
    CoordinatorNotEnabled,
    CoordinatorRequiresEmptyShard,
    CoordinatorRejected(crate::transaction_decisions::Error),
}

/// Persistent single-shard write fence. The ID is a protocol identity, not a secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1ManagedMode {
    pub mode_id: [u8; 16],
    pub activated_sequence: u64,
}

/// Unknown never proves abort: an uncommitted Raft request may still apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum D1ManagedStatus {
    Committed { sequence: u64 },
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DedupRecord {
    pub(crate) request: D1WriteRequest,
    pub(crate) sequence: u64,
}

/// The format discriminator is mandatory, including in RocksDB metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct D1State {
    pub(crate) version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) dedup_bytes_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) managed: Option<D1ManagedMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) coordinator: Option<crate::transaction_decisions::Journal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) participant: Option<crate::participant_writes::State>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) visibility: Option<crate::versioned_reads::State>,
    pub shard: u64,
    pub epoch: u64,
    pub sequence: u64,
    pub(crate) dedup: Vec<DedupRecord>,
}

impl D1State {
    pub fn new(shard: u64, epoch: u64) -> Self {
        Self {
            version: 1,
            dedup_bytes_limit: None,
            managed: None,
            coordinator: None,
            participant: None,
            visibility: None,
            shard,
            epoch,
            sequence: 0,
            dedup: Vec::new(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(
                (
                    self.version,
                    self.dedup_bytes_limit,
                    self.managed.is_some(),
                    self.coordinator.is_some(),
                    self.participant.is_some(),
                    self.visibility.is_some()
                ),
                (1, None, false, false, false, false)
                    | (2, Some(EXTENDED_DEDUP_BYTES), false, false, false, false)
                    | (
                        3,
                        None | Some(EXTENDED_DEDUP_BYTES),
                        true,
                        false,
                        false,
                        false
                    )
                    | (
                        4,
                        None | Some(EXTENDED_DEDUP_BYTES),
                        true,
                        true,
                        false,
                        false
                    )
                    | (
                        5,
                        None | Some(EXTENDED_DEDUP_BYTES),
                        true,
                        false,
                        true,
                        false
                    )
                    | (
                        6,
                        None | Some(EXTENDED_DEDUP_BYTES),
                        true,
                        false,
                        true,
                        true
                    )
            ),
            "unsupported D1 state version/capacity"
        );
        anyhow::ensure!(
            self.dedup.len() <= MAX_DEDUP_REQUESTS,
            "D1 dedup capacity exceeded"
        );
        if let Some(mode) = &self.managed {
            anyhow::ensure!(
                mode.mode_id != [0; 16] && mode.activated_sequence <= self.sequence,
                "invalid managed mode"
            );
        }
        if let Some(journal) = &self.coordinator {
            if let Some(cohort) = journal.cohort() {
                anyhow::ensure!(
                    cohort.coordinator_shard == self.shard
                        && cohort.coordinator_epoch == self.epoch,
                    "cohort coordinator identity mismatch"
                );
            }
            anyhow::ensure!(
                self.managed
                    .as_ref()
                    .is_some_and(|mode| mode.mode_id == journal.authority()),
                "coordinator authority mismatch"
            );
            anyhow::ensure!(
                self.sequence == 0 && self.dedup.is_empty(),
                "coordinator contains data history"
            );
        }
        if let Some(participant) = &self.participant {
            participant.validate(self)?;
        }
        if let Some(visibility) = &self.visibility {
            visibility.validate(self)?;
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut previous = 0;
        let mut previous_epoch = 0;
        let mut bytes = 0usize;
        for record in &self.dedup {
            anyhow::ensure!(
                record.request.shard == self.shard && record.request.epoch <= self.epoch,
                "invalid D1 dedup ownership"
            );
            anyhow::ensure!(
                record.sequence > previous && record.sequence <= self.sequence,
                "invalid D1 dedup sequence"
            );
            anyhow::ensure!(
                ids.insert(record.request.request_id),
                "duplicate D1 request identity"
            );
            bytes = bytes.saturating_add(
                record
                    .request
                    .validate()
                    .map_err(|e| anyhow::anyhow!("invalid D1 request: {e:?}"))?,
            );
            anyhow::ensure!(
                record
                    .request
                    .expected_sequence
                    .is_none_or(|expected| expected == record.sequence - 1),
                "unreachable D1 CAS history"
            );
            anyhow::ensure!(
                record.request.epoch >= previous_epoch,
                "regressing D1 request epoch history"
            );
            previous_epoch = record.request.epoch;
            previous = record.sequence;
        }
        anyhow::ensure!(bytes <= self.bytes_limit(), "D1 dedup bytes exceeded");
        // This version never prunes dedup records. A missing record is corruption.
        anyhow::ensure!(
            self.sequence == self.dedup.len() as u64,
            "incomplete D1 dedup state"
        );
        Ok(())
    }

    pub(crate) fn replayed_data<'a>(
        &'a self,
        history: Option<&'a Self>,
    ) -> std::collections::BTreeMap<&'a [u8], &'a [u8]> {
        let mut data = std::collections::BTreeMap::new();
        for record in history
            .into_iter()
            .flat_map(|s| &s.dedup)
            .chain(&self.dedup)
        {
            for operation in &record.request.operations {
                match operation {
                    WriteOp::Put { key, value } => {
                        data.insert(key.as_slice(), value.as_slice());
                    }
                    WriteOp::Delete { key } => {
                        data.remove(key.as_slice());
                    }
                }
            }
        }
        data
    }

    pub(crate) fn matches_data(&self, data: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>) -> bool {
        let expected = self.replayed_data(None);
        expected.len() == data.len()
            && data
                .iter()
                .all(|(key, value)| expected.get(key.as_slice()).copied() == Some(value.as_slice()))
    }

    pub(crate) fn bytes_limit(&self) -> usize {
        self.dedup_bytes_limit.unwrap_or(MAX_DEDUP_BYTES)
    }

    pub(crate) fn capacity(&self) -> D1Capacity {
        D1Capacity {
            bytes_limit: self.bytes_limit(),
            retained_bytes: self.dedup.iter().fold(0usize, |sum, record| {
                sum.saturating_add(record.request.retained_size())
            }),
            retained_requests: self.dedup.len(),
            max_requests: MAX_DEDUP_REQUESTS,
            max_bytes: EXTENDED_DEDUP_BYTES,
        }
    }

    pub(crate) fn set_capacity(
        &mut self,
        shard: u64,
        epoch: u64,
        expected_bytes: usize,
        new_bytes: usize,
    ) -> Result<D1Result, D1Error> {
        self.check_shard(shard)?;
        self.check_epoch(epoch)?;
        if !matches!(expected_bytes, MAX_DEDUP_BYTES | EXTENDED_DEDUP_BYTES)
            || !matches!(new_bytes, MAX_DEDUP_BYTES | EXTENDED_DEDUP_BYTES)
            || new_bytes < expected_bytes
        {
            return Err(D1Error::InvalidDedupCapacity);
        }
        // Fencing precedes idempotence; an already reached target is safe.
        let current = self.bytes_limit();
        if current != new_bytes && current != expected_bytes {
            return Err(D1Error::CapacityConflict {
                expected: expected_bytes,
                current,
            });
        }
        if new_bytes < current {
            return Err(D1Error::InvalidDedupCapacity);
        }
        if new_bytes == EXTENDED_DEDUP_BYTES {
            self.version = if self.visibility.is_some() {
                6
            } else if self.participant.is_some() {
                5
            } else if self.coordinator.is_some() {
                4
            } else if self.managed.is_some() {
                3
            } else {
                2
            };
            self.dedup_bytes_limit = Some(new_bytes);
        }
        Ok(D1Result::DedupCapacity {
            bytes_limit: new_bytes,
        })
    }

    pub fn check_shard(&self, shard: u64) -> Result<(), D1Error> {
        if shard != self.shard {
            return Err(D1Error::WrongShard {
                requested: shard,
                current: self.shard,
            });
        }
        Ok(())
    }

    pub fn check_epoch(&self, epoch: u64) -> Result<(), D1Error> {
        if epoch != self.epoch {
            return Err(D1Error::Fenced {
                requested: epoch,
                current: self.epoch,
            });
        }
        Ok(())
    }

    /// Stage only metadata and new retry records; retained request bytes stay in
    /// the published state until the synchronous batch succeeds.
    pub(crate) fn stage(&self) -> Self {
        Self {
            version: self.version,
            dedup_bytes_limit: self.dedup_bytes_limit,
            managed: self.managed.clone(),
            coordinator: self.coordinator.clone(),
            participant: self.participant.clone(),
            visibility: self.visibility.clone(),
            shard: self.shard,
            epoch: self.epoch,
            sequence: self.sequence,
            dedup: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn write(&mut self, request: &D1WriteRequest) -> Result<D1Result, D1Error> {
        self.write_with_history(request, None)
    }

    pub(crate) fn write_with_history(
        &mut self,
        request: &D1WriteRequest,
        history: Option<&Self>,
    ) -> Result<D1Result, D1Error> {
        self.check_shard(request.shard)?;
        self.check_epoch(request.epoch)?;
        if self.managed.is_some() {
            return Err(D1Error::ManagedWriteRequired);
        }
        self.write_inner(request, history)
    }

    pub(crate) fn enable_managed(
        &mut self,
        shard: u64,
        epoch: u64,
        expected_sequence: u64,
        mode_id: [u8; 16],
    ) -> Result<D1Result, D1Error> {
        self.check_shard(shard)?;
        self.check_epoch(epoch)?;
        if mode_id == [0; 16] {
            return Err(D1Error::InvalidManagedMode);
        }
        if let Some(mode) = &self.managed {
            if mode.mode_id != mode_id || mode.activated_sequence != expected_sequence {
                return Err(D1Error::ManagedModeConflict);
            }
        } else {
            if self.sequence != expected_sequence {
                return Err(D1Error::Conflict {
                    expected: expected_sequence,
                    current: self.sequence,
                });
            }
            self.managed = Some(D1ManagedMode {
                mode_id,
                activated_sequence: expected_sequence,
            });
            self.version = 3;
        }
        Ok(D1Result::ManagedWritesEnabled {
            mode_id,
            activated_sequence: expected_sequence,
        })
    }

    pub(crate) fn managed_write(
        &mut self,
        mode_id: [u8; 16],
        request: &D1WriteRequest,
        history: Option<&Self>,
    ) -> Result<D1Result, D1Error> {
        self.check_shard(request.shard)?;
        self.check_epoch(request.epoch)?;
        let mode = self
            .managed
            .as_ref()
            .ok_or(D1Error::ManagedWritesNotEnabled)?;
        if mode.mode_id != mode_id {
            return Err(D1Error::ManagedModeConflict);
        }
        if self.participant.is_some() {
            return Err(D1Error::ParticipantOnly);
        }
        if self.coordinator.is_some() {
            return Err(D1Error::CoordinatorOnly);
        }
        // Activation does not erase historical retry identities. Never report a
        // pre-activation legacy write as a managed transaction's commit.
        if self
            .dedup
            .iter()
            .chain(history.into_iter().flat_map(|s| &s.dedup))
            .any(|r| {
                r.request.request_id == request.request_id && r.sequence <= mode.activated_sequence
            })
        {
            return Err(D1Error::IdempotencyConflict);
        }
        match self.write_inner(request, history)? {
            D1Result::Write {
                sequence,
                status,
                applied_operations,
            } => Ok(D1Result::ManagedWrite {
                mode_id,
                sequence,
                status,
                applied_operations,
            }),
            _ => unreachable!("write_inner returns only Write"),
        }
    }

    pub(crate) fn managed_status(
        &self,
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        request_id: [u8; 16],
    ) -> Result<D1ManagedStatus, D1Error> {
        self.check_shard(shard)?;
        self.check_epoch(epoch)?;
        let mode = self
            .managed
            .as_ref()
            .ok_or(D1Error::ManagedWritesNotEnabled)?;
        if mode.mode_id != mode_id {
            return Err(D1Error::ManagedModeConflict);
        }
        Ok(self
            .dedup
            .iter()
            .find(|r| r.request.request_id == request_id && r.sequence > mode.activated_sequence)
            .map_or(D1ManagedStatus::Unknown, |r| D1ManagedStatus::Committed {
                sequence: r.sequence,
            }))
    }

    pub(crate) fn write_inner(
        &mut self,
        request: &D1WriteRequest,
        history: Option<&Self>,
    ) -> Result<D1Result, D1Error> {
        let size = request.validate()?;
        if let Some(record) = self
            .dedup
            .iter()
            .chain(history.into_iter().flat_map(|state| &state.dedup))
            .find(|r| r.request.request_id == request.request_id)
        {
            // Epoch fences before dedup, but a request id never gains a new identity
            // after an ownership change. This matches local D1's retained dedup.
            if record.request.operations != request.operations
                || record.request.expected_sequence != request.expected_sequence
            {
                return Err(D1Error::IdempotencyConflict);
            }
            return Ok(D1Result::Write {
                sequence: record.sequence,
                status: D1WriteStatus::Duplicate,
                applied_operations: request.operations.len(),
            });
        }
        if let Some(expected) = request.expected_sequence {
            if expected != self.sequence {
                return Err(D1Error::Conflict {
                    expected,
                    current: self.sequence,
                });
            }
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(D1Error::SequenceOverflow)?;
        let retained = self
            .dedup
            .iter()
            .chain(history.into_iter().flat_map(|state| &state.dedup))
            .fold(0usize, |total, r| {
                total.saturating_add(r.request.retained_size())
            });
        if self.dedup.len() + history.map_or(0, |state| state.dedup.len()) >= MAX_DEDUP_REQUESTS
            || retained.saturating_add(size) > self.bytes_limit()
        {
            return Err(D1Error::DedupCapacity);
        }
        self.dedup.push(DedupRecord {
            request: request.clone(),
            sequence,
        });
        self.sequence = sequence;
        Ok(D1Result::Write {
            sequence,
            status: D1WriteStatus::Committed,
            applied_operations: request.operations.len(),
        })
    }
}

impl D1WriteRequest {
    fn byte_size(&self) -> usize {
        self.operations.iter().fold(0usize, |total, op| match op {
            WriteOp::Put { key, value } => {
                total.saturating_add(key.len()).saturating_add(value.len())
            }
            WriteOp::Delete { key } => total.saturating_add(key.len()),
        })
    }

    // Charge fixed per-request/per-operation overhead too: empty keys cannot
    // create millions of retained Vec allocations for zero accounted bytes.
    pub(crate) fn retained_size(&self) -> usize {
        self.byte_size()
            .saturating_add(self.operations.len().saturating_mul(64))
            .saturating_add(128)
    }

    pub(crate) fn validate(&self) -> Result<usize, D1Error> {
        if self.operations.is_empty() {
            return Err(D1Error::EmptyBatch);
        }
        if self.operations.len() > MAX_BATCH_OPERATIONS {
            return Err(D1Error::BatchTooLarge);
        }
        let size = self.byte_size();
        if size > MAX_BATCH_BYTES {
            return Err(D1Error::BatchTooLarge);
        }
        Ok(self.retained_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: u64) -> D1WriteRequest {
        let mut request_id = [0; 16];
        request_id[..8].copy_from_slice(&id.to_be_bytes());
        D1WriteRequest {
            shard: 7,
            epoch: 3,
            request_id,
            expected_sequence: None,
            operations: vec![WriteOp::put([], [])],
        }
    }

    #[test]
    fn limits_reject_without_reserving_identity() {
        let mut state = D1State::new(7, 3);
        let mut too_many = request(1);
        too_many.operations = vec![WriteOp::delete([]); MAX_BATCH_OPERATIONS + 1];
        let mut too_large = request(2);
        too_large.operations = vec![WriteOp::put([], vec![0; MAX_BATCH_BYTES + 1])];
        for request in [too_many, too_large] {
            let before = state.clone();
            assert_eq!(state.write(&request), Err(D1Error::BatchTooLarge));
            assert_eq!(state, before);
        }
        let mut exact = request(1);
        exact.operations = vec![WriteOp::delete([]); MAX_BATCH_OPERATIONS];
        assert!(state.write(&exact).is_ok());
        let mut exact_bytes = request(2);
        exact_bytes.operations = vec![WriteOp::put([], vec![0; MAX_BATCH_BYTES])];
        assert!(state.write(&exact_bytes).is_ok());
        state.validate().unwrap();
    }

    #[test]
    fn dedup_capacity_preserves_old_retries_and_overflow_is_nonmutating() {
        let mut state = D1State::new(7, 3);
        // Construct the bounded valid fixture directly; no timing-dependent loop.
        for id in 0..MAX_DEDUP_REQUESTS as u64 {
            state.dedup.push(DedupRecord {
                request: request(id),
                sequence: id + 1,
            });
        }
        state.sequence = MAX_DEDUP_REQUESTS as u64;
        state.validate().unwrap();
        let before = state.clone();
        assert_eq!(
            state.write(&request(MAX_DEDUP_REQUESTS as u64)),
            Err(D1Error::DedupCapacity)
        );
        assert_eq!(state, before);
        assert!(matches!(
            state.write(&request(0)),
            Ok(D1Result::Write {
                sequence: 1,
                status: D1WriteStatus::Duplicate,
                ..
            })
        ));
        assert_eq!(state, before);
        state.sequence = u64::MAX;
        let before = state.clone();
        assert_eq!(
            state.write(&request(u64::MAX)),
            Err(D1Error::SequenceOverflow)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn dedup_byte_capacity_includes_empty_operation_overhead() {
        let mut state = D1State::new(7, 3);
        let mut request = request(0);
        request.operations = vec![WriteOp::delete([]); MAX_BATCH_OPERATIONS];
        let count = MAX_DEDUP_BYTES / request.retained_size();
        for id in 0..count as u64 {
            request.request_id[..8].copy_from_slice(&id.to_be_bytes());
            state.write(&request).unwrap();
        }
        request.request_id = [255; 16];
        let before = state.clone();
        assert_eq!(state.write(&request), Err(D1Error::DedupCapacity));
        assert_eq!(state, before);
        state.validate().unwrap();
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    fn request(id: u128, bytes: usize) -> D1WriteRequest {
        D1WriteRequest {
            shard: 7,
            epoch: 3,
            request_id: id.to_be_bytes(),
            expected_sequence: None,
            operations: vec![WriteOp::put([], vec![0; bytes])],
        }
    }
    #[test]
    fn explicit_tiers_fence_compare_and_retry_without_reset() {
        let mut state = D1State::new(7, 3);
        state.write(&request(1, 0)).unwrap();
        let before = state.clone();
        for (shard, epoch, expected, new) in [
            (8, 3, MAX_DEDUP_BYTES, EXTENDED_DEDUP_BYTES),
            (7, 2, MAX_DEDUP_BYTES, EXTENDED_DEDUP_BYTES),
            (7, 3, 0, EXTENDED_DEDUP_BYTES),
            (7, 3, MAX_DEDUP_BYTES, EXTENDED_DEDUP_BYTES + 1),
            (7, 3, EXTENDED_DEDUP_BYTES, EXTENDED_DEDUP_BYTES),
        ] {
            assert!(state.set_capacity(shard, epoch, expected, new).is_err());
            assert_eq!(state, before);
        }
        let result = D1Result::DedupCapacity {
            bytes_limit: EXTENDED_DEDUP_BYTES,
        };
        assert_eq!(
            state.set_capacity(7, 3, MAX_DEDUP_BYTES, EXTENDED_DEDUP_BYTES),
            Ok(result.clone())
        );
        assert_eq!(
            state.set_capacity(7, 3, MAX_DEDUP_BYTES, EXTENDED_DEDUP_BYTES),
            Ok(result)
        );
        assert_eq!(state.sequence, 1);
        assert_eq!(state.dedup, before.dedup);
        assert!(matches!(
            state.write(&request(1, 0)),
            Ok(D1Result::Write {
                sequence: 1,
                status: D1WriteStatus::Duplicate,
                ..
            })
        ));
        assert_eq!(
            state.set_capacity(7, 3, EXTENDED_DEDUP_BYTES, MAX_DEDUP_BYTES),
            Err(D1Error::InvalidDedupCapacity)
        );
        state.validate().unwrap();
        let old = serde_json::to_string(&before).unwrap();
        assert!(!old.contains("dedup_bytes_limit"));
        let new = serde_json::to_string(&state).unwrap();
        assert!(new.contains("\"version\":2"));
        for invalid in [
            new.replace("\"version\":2", "\"version\":1"),
            new.replace(
                &EXTENDED_DEDUP_BYTES.to_string(),
                &(EXTENDED_DEDUP_BYTES + 1).to_string(),
            ),
        ] {
            assert!(serde_json::from_str::<D1State>(&invalid)
                .unwrap()
                .validate()
                .is_err());
        }
    }
    #[test]
    fn default_and_extended_byte_boundaries_preserve_exact_retries() {
        let mut state = D1State::new(7, 3);
        // Raw payload plus one operation/request overhead is exactly 16MiB.
        let bytes = MAX_BATCH_BYTES - 192;
        for id in 1..=4 {
            state.write(&request(id, bytes)).unwrap();
        }
        assert_eq!(state.capacity().retained_bytes, MAX_DEDUP_BYTES);
        assert_eq!(state.write(&request(5, 0)), Err(D1Error::DedupCapacity));
        state
            .set_capacity(7, 3, MAX_DEDUP_BYTES, EXTENDED_DEDUP_BYTES)
            .unwrap();
        for id in 5..=8 {
            state.write(&request(id, bytes)).unwrap();
        }
        assert_eq!(state.capacity().retained_bytes, EXTENDED_DEDUP_BYTES);
        assert_eq!(state.write(&request(9, 0)), Err(D1Error::DedupCapacity));
        assert!(matches!(
            state.write(&request(1, bytes)),
            Ok(D1Result::Write {
                sequence: 1,
                status: D1WriteStatus::Duplicate,
                ..
            })
        ));
        assert_eq!(state.sequence, 8);
        state.validate().unwrap();
    }
    #[test]
    fn extended_tier_does_not_raise_request_count_limit() {
        let mut state = D1State::new(7, 3);
        state
            .set_capacity(7, 3, MAX_DEDUP_BYTES, EXTENDED_DEDUP_BYTES)
            .unwrap();
        for id in 1..=MAX_DEDUP_REQUESTS {
            state.dedup.push(DedupRecord {
                request: request(id as u128, 0),
                sequence: id as u64,
            });
        }
        state.sequence = MAX_DEDUP_REQUESTS as u64;
        state.validate().unwrap();
        assert_eq!(state.write(&request(9999, 0)), Err(D1Error::DedupCapacity));
        assert!(matches!(
            state.write(&request(1, 0)),
            Ok(D1Result::Write {
                sequence: 1,
                status: D1WriteStatus::Duplicate,
                ..
            })
        ));
    }
}

#[cfg(test)]
mod managed_tests {
    use super::*;
    fn request(id: u8, expected: u64) -> D1WriteRequest {
        D1WriteRequest {
            shard: 7,
            epoch: 3,
            request_id: [id; 16],
            expected_sequence: Some(expected),
            operations: vec![WriteOp::Put {
                key: vec![1],
                value: vec![id],
            }],
        }
    }
    #[test]
    fn managed_mode_is_explicit_fenced_and_preserves_historical_identities() {
        let mut state = D1State::new(7, 3);
        let mode = [9; 16];
        assert_eq!(
            state.managed_write(mode, &request(1, 0), None),
            Err(D1Error::ManagedWritesNotEnabled)
        );
        state.write(&request(1, 0)).unwrap();
        assert_eq!(
            state.enable_managed(7, 3, 0, mode),
            Err(D1Error::Conflict {
                expected: 0,
                current: 1
            })
        );
        assert_eq!(
            state.enable_managed(7, 3, 1, [0; 16]),
            Err(D1Error::InvalidManagedMode)
        );
        state.enable_managed(7, 3, 1, mode).unwrap();
        assert_eq!(
            state.write(&request(1, 0)),
            Err(D1Error::ManagedWriteRequired)
        );
        assert_eq!(
            state.write(&request(2, 1)),
            Err(D1Error::ManagedWriteRequired)
        );
        assert_eq!(
            state.managed_write(mode, &request(1, 0), None),
            Err(D1Error::IdempotencyConflict)
        );
        assert_eq!(
            state.managed_write([8; 16], &request(2, 1), None),
            Err(D1Error::ManagedModeConflict)
        );
        assert!(matches!(
            state.managed_write(mode, &request(2, 1), None),
            Ok(D1Result::ManagedWrite {
                sequence: 2,
                status: D1WriteStatus::Committed,
                ..
            })
        ));
        assert!(matches!(
            state.managed_write(mode, &request(2, 1), None),
            Ok(D1Result::ManagedWrite {
                sequence: 2,
                status: D1WriteStatus::Duplicate,
                ..
            })
        ));
        let mut changed = request(2, 1);
        changed.operations.clear();
        assert_eq!(
            state.managed_write(mode, &changed, None),
            Err(D1Error::EmptyBatch)
        );
        changed.operations = vec![WriteOp::Delete { key: vec![1] }];
        assert_eq!(
            state.managed_write(mode, &changed, None),
            Err(D1Error::IdempotencyConflict)
        );
        assert_eq!(
            state.managed_status(7, 3, mode, [2; 16]),
            Ok(D1ManagedStatus::Committed { sequence: 2 })
        );
        for id in [1, 3] {
            assert_eq!(
                state.managed_status(7, 3, mode, [id; 16]),
                Ok(D1ManagedStatus::Unknown)
            );
        }
        assert_eq!(
            state.enable_managed(7, 3, 1, mode),
            Ok(D1Result::ManagedWritesEnabled {
                mode_id: mode,
                activated_sequence: 1
            })
        );
        assert_eq!(
            state.enable_managed(7, 3, 2, mode),
            Err(D1Error::ManagedModeConflict)
        );
        assert_eq!(
            state.enable_managed(7, 3, 1, [8; 16]),
            Err(D1Error::ManagedModeConflict)
        );
        state
            .set_capacity(7, 3, MAX_DEDUP_BYTES, EXTENDED_DEDUP_BYTES)
            .unwrap();
        assert_eq!(state.version, 3);
        state.validate().unwrap();
        let copy: D1State = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        copy.validate().unwrap();
        assert_eq!(copy, state);
        for invalid in 0..4 {
            let mut bad = copy.clone();
            match invalid {
                0 => bad.version = 2,
                1 => bad.managed = None,
                2 => bad.managed.as_mut().unwrap().mode_id = [0; 16],
                _ => bad.managed.as_mut().unwrap().activated_sequence = bad.sequence + 1,
            }
            assert!(bad.validate().is_err());
        }
        let mut bad_cas = copy.clone();
        bad_cas.dedup[1].request.expected_sequence = Some(0);
        assert!(bad_cas.validate().is_err());
        let mut bad_epoch = copy.clone();
        bad_epoch.dedup[1].request.epoch = 2;
        assert!(bad_epoch.validate().is_err());
        state.epoch = 4;
        assert_eq!(
            state.write(&request(3, 2)),
            Err(D1Error::Fenced {
                requested: 3,
                current: 4
            })
        );
    }
}
