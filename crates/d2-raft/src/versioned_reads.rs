//! Bounded retained-history reads at a participant's applied decision prefix.
//! This is not global query admission or indexed MVCC. No history is pruned.
use crate::d1::D1State;
use crate::transaction_decisions::{Decision, Journal};
use crate::{D1Error, D1Result, WriteOp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_READ_ROWS: usize = 1_000_000;
pub const MAX_READ_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_REPLAY_OPERATIONS: usize = 4_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadLimit {
    pub rows: usize,
    pub bytes: usize,
    pub operations: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSnapshot {
    pub shard: u64,
    pub epoch: u64,
    pub authority: crate::participant_writes::Authority,
    pub version: u64,
    pub applied_prefix: u64,
    pub local_sequence: u64,
    pub entries: BTreeMap<Vec<u8>, Vec<u8>>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitVersion {
    pub ordinal: u64,
    pub id: [u8; 16],
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub baseline_sequence: u64,
    pub epoch: u64,
    pub applied: u64,
    pub journal: Journal,
    pub commits: Vec<CommitVersion>,
}
impl State {
    pub fn validate_successor(&self, next: &Self) -> bool {
        self.epoch == next.epoch
            && self.baseline_sequence == next.baseline_sequence
            && next.applied >= self.applied
            && self.journal.validate_successor(&next.journal).is_ok()
            && next.commits.starts_with(&self.commits)
    }
    pub fn validate(&self, d1: &D1State) -> anyhow::Result<()> {
        let participant = d1
            .participant
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("versioned read mode without participant"))?;
        anyhow::ensure!(
            self.epoch == d1.epoch
                && participant
                    .records
                    .iter()
                    .all(|r| r.participant.epoch == self.epoch),
            "versioned ownership epoch changed"
        );
        anyhow::ensure!(
            self.journal.authority() == participant.authority.mode
                && self.baseline_sequence == participant.activated_sequence,
            "invalid version authority/baseline"
        );
        anyhow::ensure!(
            self.applied <= self.journal.resolved_prefix(),
            "watermark crosses unresolved decision"
        );
        let mut sequence = self.baseline_sequence;
        let mut commits = 0;
        for ordinal in 1..=self.applied {
            let tx = self
                .journal
                .transaction_at(ordinal)
                .ok_or_else(|| anyhow::anyhow!("missing decision ordinal"))?;
            let record = participant.records.iter().find(|r| r.id == tx.id);
            if let Some(descriptor) = tx.participants.iter().find(|p| p.shard == d1.shard) {
                d1.check_participant(&participant.authority, descriptor)
                    .map_err(|e| anyhow::anyhow!("invalid versioned participant: {e:?}"))?;
                if record.is_none() && tx.decision == Some(Decision::Abort) {
                    anyhow::ensure!(
                        !d1.dedup.iter().any(|r| r.request.request_id == tx.id),
                        "abort aliases a committed identity"
                    );
                    continue;
                }
                let record = record.ok_or_else(|| {
                    anyhow::anyhow!("watermark crosses unapplied participant decision")
                })?;
                anyhow::ensure!(
                    record.participant == *descriptor && record.decision == tx.decision,
                    "participant decision mismatch"
                );
                if tx.decision == Some(Decision::Commit) {
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("version sequence overflow"))?;
                    let expected = CommitVersion {
                        ordinal,
                        id: tx.id,
                        sequence,
                    };
                    anyhow::ensure!(
                        self.commits.get(commits) == Some(&expected)
                            && record.sequence == Some(sequence),
                        "invalid global-to-local commit map"
                    );
                    commits += 1;
                }
            } else {
                anyhow::ensure!(record.is_none(), "nonparticipant decision has local intent");
            }
        }
        anyhow::ensure!(
            commits == self.commits.len() && sequence == d1.sequence,
            "unmapped data publication"
        );
        for record in &participant.records {
            if let Some(tx) = self.journal.transaction(record.id) {
                anyhow::ensure!(
                    tx.participants.contains(&record.participant),
                    "local intent conflicts with observed participant set"
                );
            }
            if record.decision.is_some() {
                anyhow::ensure!(
                    self.journal
                        .transaction(record.id)
                        .is_some_and(|tx| tx.ordinal <= self.applied),
                    "terminal decision beyond read watermark"
                );
            }
        }
        Ok(())
    }
}

impl D1State {
    /// Stage only on an unpublished candidate. Caller publishes data and this
    /// watermark in the same sync batch. Exactly one decision advances at a time.
    pub(crate) fn advance_visibility(
        &mut self,
        ordinal: u64,
        journal: Journal,
        history: Option<&Self>,
    ) -> Result<(D1Result, Option<crate::D1WriteRequest>), D1Error> {
        let versioned = self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?;
        if ordinal == 0 || ordinal > journal.resolved_prefix() {
            return Err(D1Error::VisibilityUnresolved);
        }
        if ordinal <= versioned.applied {
            if versioned.journal.validate_successor(&journal).is_err()
                && journal.validate_successor(&versioned.journal).is_err()
            {
                return Err(D1Error::VisibilityHistory);
            }
            return Ok((
                D1Result::Visibility {
                    applied_prefix: versioned.applied,
                },
                None,
            ));
        }
        if ordinal != versioned.applied + 1 {
            return Err(D1Error::VisibilityGap);
        }
        versioned
            .journal
            .validate_successor(&journal)
            .map_err(|_| D1Error::VisibilityHistory)?;
        let tx = journal
            .transaction_at(ordinal)
            .ok_or(D1Error::VisibilityHistory)?;
        let decision = tx.decision.ok_or(D1Error::VisibilityUnresolved)?;
        let participant = self
            .participant
            .as_ref()
            .ok_or(D1Error::ParticipantNotEnabled)?;
        for record in &participant.records {
            if journal
                .transaction(record.id)
                .is_some_and(|tx| !tx.participants.contains(&record.participant))
            {
                return Err(D1Error::VisibilityHistory);
            }
        }
        let authority = participant.authority.clone();
        let request = if let Some(descriptor) =
            tx.participants.iter().find(|p| p.shard == self.shard)
        {
            self.check_participant(&authority, descriptor)?;
            if decision == Decision::Abort && !participant.records.iter().any(|r| r.id == tx.id) {
                if self
                    .dedup
                    .iter()
                    .chain(history.into_iter().flat_map(|s| &s.dedup))
                    .any(|r| r.request.request_id == tx.id)
                {
                    return Err(D1Error::IdempotencyConflict);
                }
                // Retained applied journal is the negative identity. No extra
                // tombstone quota can strand this already decided Abort.
                None
            } else {
                let (_, request) = self.resolve_participant(
                    &authority,
                    descriptor.clone(),
                    tx.id,
                    decision,
                    history,
                )?;
                if decision == Decision::Commit && request.is_none() {
                    return Err(D1Error::VisibilityHistory);
                }
                request
            }
        } else {
            if participant.records.iter().any(|r| r.id == tx.id) {
                return Err(D1Error::VisibilityHistory);
            }
            None
        };
        let state = self.visibility.as_mut().unwrap();
        if request.is_some() {
            state.commits.push(CommitVersion {
                ordinal,
                id: tx.id,
                sequence: self.sequence,
            });
        }
        state.applied = ordinal;
        state.journal = journal;
        Ok((
            D1Result::Visibility {
                applied_prefix: ordinal,
            },
            request,
        ))
    }

    pub(crate) fn read_at(&self, version: u64, limit: ReadLimit) -> Result<ReadSnapshot, D1Error> {
        if limit.rows == 0
            || limit.rows > MAX_READ_ROWS
            || limit.bytes == 0
            || limit.bytes > MAX_READ_BYTES
            || limit.operations == 0
            || limit.operations > MAX_REPLAY_OPERATIONS
        {
            return Err(D1Error::VisibilityReadLimit);
        }
        let state = self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?;
        if version > state.applied {
            return Err(D1Error::VisibilityNotApplied);
        }
        let sequence = state
            .commits
            .iter()
            .take_while(|c| c.ordinal <= version)
            .last()
            .map_or(state.baseline_sequence, |c| c.sequence);
        let mut entries: BTreeMap<&[u8], &[u8]> = BTreeMap::new();
        let mut bytes = 0usize;
        let mut operations = 0usize;
        for record in self.dedup.iter().take_while(|r| r.sequence <= sequence) {
            for operation in &record.request.operations {
                operations += 1;
                if operations > limit.operations {
                    return Err(D1Error::VisibilityReadLimit);
                }
                match operation {
                    WriteOp::Put { key, value } => {
                        if let Some(previous) = entries.get(key.as_slice()) {
                            bytes -= key.len() + previous.len();
                        } else if entries.len() >= limit.rows {
                            return Err(D1Error::VisibilityReadLimit);
                        }
                        bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
                        if bytes > limit.bytes {
                            return Err(D1Error::VisibilityReadLimit);
                        }
                        entries.insert(key, value);
                    }
                    WriteOp::Delete { key } => {
                        if let Some(previous) = entries.remove(key.as_slice()) {
                            bytes -= key.len() + previous.len();
                        }
                    }
                }
            }
        }
        Ok(ReadSnapshot {
            shard: self.shard,
            epoch: self.epoch,
            authority: self.participant.as_ref().unwrap().authority.clone(),
            version,
            applied_prefix: state.applied,
            local_sequence: sequence,
            entries: entries
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect(),
        })
    }
}

/// Explicit fixed participant identity. No catalog discovery or failover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub shard: u64,
    pub epoch: u64,
    pub mode: [u8; 16],
}

/// Trusted in-process relay: observe coordinator history after a leader barrier,
/// then durably consume exactly one terminal decision. Timeout is uncertain.
pub async fn advance(
    coordinator: &crate::NodeHandle,
    node: &crate::NodeHandle,
    authority: crate::participant_writes::Authority,
    coordinator_epoch: u64,
    target: Target,
    ordinal: u64,
) -> Result<u64, crate::participant_driver::Error> {
    use crate::participant_driver::Error;
    let deadline = std::time::Duration::from_secs(5);
    tokio::time::timeout(deadline, coordinator.raft.ensure_linearizable())
        .await
        .map_err(|_| Error::Unavailable)?
        .map_err(|_| Error::Unavailable)?;
    let journal = coordinator
        .state_machine
        .coordinator_journal(authority.shard, coordinator_epoch, authority.mode)
        .await
        .map_err(Error::Application)?;
    let command = crate::D1Command::AdvanceVisibility {
        authority,
        shard: target.shard,
        epoch: target.epoch,
        mode_id: target.mode,
        ordinal,
        journal,
    };
    let result = tokio::time::timeout(
        deadline,
        node.raft.client_write(crate::LogCommand::D1V1(command)),
    )
    .await
    .map_err(|_| Error::Unavailable)?
    .map_err(|_| Error::Unavailable)?;
    match result
        .data
        .d1
        .ok_or(Error::Unexpected)?
        .map_err(Error::Application)?
    {
        D1Result::Visibility { applied_prefix } => Ok(applied_prefix),
        _ => Err(Error::Unexpected),
    }
}

/// Leader-barrier historical read of one participant. The caller must validate
/// a common authority/baseline/catalog across participants; this is NOT global
/// query admission and never selects different versions to make a read succeed.
pub async fn read(
    node: &crate::NodeHandle,
    target: Target,
    version: u64,
    limit: ReadLimit,
) -> Result<ReadSnapshot, crate::participant_driver::Error> {
    use crate::participant_driver::Error;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        node.raft.ensure_linearizable(),
    )
    .await
    .map_err(|_| Error::Unavailable)?
    .map_err(|_| Error::Unavailable)?;
    node.state_machine
        .participant_read_at(target.shard, target.epoch, target.mode, version, limit)
        .await
        .map_err(Error::Application)
}
