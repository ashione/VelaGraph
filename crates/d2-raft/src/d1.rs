//! Versioned D1 application commands. These are independent of the local D1
//! trait: this crate does not yet provide a remote graph storage adapter.
use serde::{Deserialize, Serialize};

use crate::WriteOp;

pub const MAX_BATCH_OPERATIONS: usize = 10_000;
pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
/// No eviction: exhaustion rejects new requests, never silently loses retries.
pub const MAX_DEDUP_REQUESTS: usize = 4_096;
pub const MAX_DEDUP_BYTES: usize = 64 * 1024 * 1024;

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
    Write(D1WriteRequest),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum D1WriteStatus {
    Committed,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum D1Result {
    Activated {
        shard: u64,
        epoch: u64,
    },
    Epoch {
        epoch: u64,
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
    LegacyDataPresent,
    LegacyWriteDisabled,
    InvalidEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DedupRecord {
    request: D1WriteRequest,
    sequence: u64,
}

/// The format discriminator is mandatory, including in RocksDB metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct D1State {
    version: u32,
    pub shard: u64,
    pub epoch: u64,
    pub sequence: u64,
    dedup: Vec<DedupRecord>,
}

impl D1State {
    pub fn new(shard: u64, epoch: u64) -> Self {
        Self {
            version: 1,
            shard,
            epoch,
            sequence: 0,
            dedup: Vec::new(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.version == 1, "unsupported D1 state version");
        anyhow::ensure!(
            self.dedup.len() <= MAX_DEDUP_REQUESTS,
            "D1 dedup capacity exceeded"
        );
        let mut ids = std::collections::BTreeSet::new();
        let mut previous = 0;
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
            previous = record.sequence;
        }
        anyhow::ensure!(bytes <= MAX_DEDUP_BYTES, "D1 dedup bytes exceeded");
        // This version never prunes dedup records. A missing record is corruption.
        anyhow::ensure!(
            self.sequence == self.dedup.len() as u64,
            "incomplete D1 dedup state"
        );
        Ok(())
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

    pub fn write(&mut self, request: &D1WriteRequest) -> Result<D1Result, D1Error> {
        self.check_shard(request.shard)?;
        self.check_epoch(request.epoch)?;
        let size = request.validate()?;
        if let Some(record) = self
            .dedup
            .iter()
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
        let retained = self.dedup.iter().fold(0usize, |total, r| {
            total.saturating_add(r.request.retained_size())
        });
        if self.dedup.len() >= MAX_DEDUP_REQUESTS || retained.saturating_add(size) > MAX_DEDUP_BYTES
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
    fn retained_size(&self) -> usize {
        self.byte_size()
            .saturating_add(self.operations.len().saturating_mul(64))
            .saturating_add(128)
    }

    fn validate(&self) -> Result<usize, D1Error> {
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
