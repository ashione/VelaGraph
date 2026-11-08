//! Bounded retained-history reads at a participant's applied decision prefix.
//! This is not global query admission or indexed MVCC. No history is pruned.
use crate::coordinator_state::History;
use crate::d1::D1State;
use crate::transaction_decisions::{Command, Decision, Journal, RequestRecord, Transaction};
use crate::{D1Error, D1Result, WriteOp};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Point evidence from a completely verified raw history and accepted original
/// command overlay. Absence is authoritative only after full closure succeeds.
pub(crate) trait MemberHistory {
    fn transaction(&self, id: [u8; 16]) -> Result<Option<Transaction>, D1Error>;
    fn request_for_transaction(&self, id: [u8; 16]) -> Result<Option<RequestRecord>, D1Error>;
    fn transaction_at(&self, ordinal: u64) -> Result<Option<Transaction>, D1Error>;
}

// Explicit legacy/cold materialized-history boundary.
impl MemberHistory for Journal {
    fn transaction(&self, id: [u8; 16]) -> Result<Option<Transaction>, D1Error> {
        Ok(Journal::transaction(self, id).cloned())
    }
    fn request_for_transaction(&self, id: [u8; 16]) -> Result<Option<RequestRecord>, D1Error> {
        Ok(Journal::request_for_transaction(self, id).cloned())
    }
    fn transaction_at(&self, ordinal: u64) -> Result<Option<Transaction>, D1Error> {
        Ok(Journal::transaction_at(self, ordinal).cloned())
    }
}

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
    pub baseline_sequence: u64,
    pub cohort: Option<crate::transaction_decisions::Cohort>,
    pub applied_prefix: u64,
    pub local_sequence: u64,
    pub replay_operations: usize,
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
    pub journal: History,
    #[serde(deserialize_with = "deserialize_commits")]
    pub commits: Vec<CommitVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<crate::transaction_decisions::CapacityActivation>,
}
pub(crate) fn deserialize_commits<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<CommitVersion>, D::Error> {
    crate::capacity_policy::deserialize_bounded_vec(d, crate::MAX_DEDUP_REQUESTS)
}

impl State {
    pub fn validate_successor(&self, next: &Self) -> bool {
        self.journal.cohort() == next.journal.cohort()
            && self.activation == next.activation
            && self.epoch == next.epoch
            && self.baseline_sequence == next.baseline_sequence
            && next.applied >= self.applied
            && self
                .journal
                .full()
                .zip(next.journal.full())
                .is_some_and(|(old, new)| old.validate_successor(new).is_ok())
            && next.commits.starts_with(&self.commits)
    }
    pub fn validate(&self, d1: &D1State) -> anyhow::Result<()> {
        let journal = self
            .journal
            .full()
            .ok_or_else(|| anyhow::anyhow!("active member validation requires raw evidence"))?;
        let byte_bound = self.journal.capacity_profile()
            == crate::transaction_decisions::CapacityProfile::ByteBound16MiBV1;
        anyhow::ensure!(
            (byte_bound && d1.version == 7 && self.activation.is_some())
                || (!byte_bound && d1.version == 6 && self.activation.is_none()),
            "invalid visibility profile/activation version"
        );
        if let Some(activation) = &self.activation {
            activation
                .validate()
                .map_err(|e| anyhow::anyhow!("invalid activation: {e:?}"))?;
        }
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
        d1.check_cohort(journal)
            .map_err(|e| anyhow::anyhow!("invalid cohort: {e:?}"))?;
        let mut sequence = self.baseline_sequence;
        let mut commits = 0;
        for ordinal in 1..=self.applied {
            let tx = journal
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
        d1.validate_member_history(journal)
            .map_err(|error| anyhow::anyhow!("invalid retained member history: {error:?}"))?;
        Ok(())
    }
}

impl D1State {
    /// Validate the immutable activation against this replica's applied marker.
    /// LogId orders activation; only the random nonce distinguishes fresh state.
    pub(crate) fn validate_capacity_activation(
        &self,
        applied: Option<openraft::LogId<crate::NodeId>>,
    ) -> anyhow::Result<()> {
        if let Some(visibility) = &self.visibility {
            self.check_observed_cohort()
                .map_err(|e| anyhow::anyhow!("invalid capacity identity: {e:?}"))?;
            if let Some(activation) = &visibility.activation {
                activation
                    .validate()
                    .map_err(|e| anyhow::anyhow!("invalid activation: {e:?}"))?;
                anyhow::ensure!(
                    applied.is_some_and(|current| {
                        activation.log_id.index <= current.index
                            && activation.log_id.leader_id <= current.leader_id
                            && (activation.log_id.index != current.index
                                || activation.log_id == current)
                    }),
                    "capacity activation is not in applied history"
                );
            }
        }
        Ok(())
    }

    /// New-profile preparation is authorized by locally retained authority,
    /// including exact original Plan bytes when the transaction has an ID record.
    pub(crate) fn check_capacity_prepare(
        &self,
        descriptor: &crate::transaction_decisions::Participant,
        id: [u8; 16],
        request: Option<&crate::D1WriteRequest>,
    ) -> Result<(), D1Error> {
        let Some(visibility) = &self.visibility else {
            return Ok(());
        };
        let journal = visibility
            .journal
            .full()
            .ok_or(D1Error::VisibilityHistory)?;
        self.check_capacity_prepare_with_evidence(descriptor, id, request, journal)
    }

    pub(crate) fn check_capacity_prepare_with_evidence(
        &self,
        descriptor: &crate::transaction_decisions::Participant,
        id: [u8; 16],
        request: Option<&crate::D1WriteRequest>,
        evidence: &dyn MemberHistory,
    ) -> Result<(), D1Error> {
        let Some(visibility) = &self.visibility else {
            return Ok(());
        };
        if visibility.journal.capacity_profile()
            == crate::transaction_decisions::CapacityProfile::LegacyCountV1
        {
            return Ok(());
        }
        self.check_observed_cohort()?;
        if !visibility.journal.capacity_ready() {
            return Err(D1Error::CoordinatorRejected(
                crate::transaction_decisions::Error::CapacityNotConfirmed,
            ));
        }
        let tx = evidence
            .transaction(id)?
            .ok_or(D1Error::VisibilityHistory)?;
        if !tx.participants.contains(descriptor) {
            return Err(D1Error::ParticipantIdentity);
        }
        Self::check_original_plan(descriptor, id, request, evidence)
    }

    fn check_original_plan(
        descriptor: &crate::transaction_decisions::Participant,
        id: [u8; 16],
        request: Option<&crate::D1WriteRequest>,
        evidence: &dyn MemberHistory,
    ) -> Result<(), D1Error> {
        if let Some(record) = evidence.request_for_transaction(id)? {
            if record.transaction_id != id {
                return Err(D1Error::VisibilityHistory);
            }
            let plan = crate::request_plans::Plan::decode(&record.payload)
                .map_err(|_| D1Error::VisibilityHistory)?;
            if !plan
                .parts
                .iter()
                .any(|part| part.descriptor == *descriptor && request == Some(&part.request))
            {
                return Err(D1Error::IdempotencyConflict);
            }
        }
        Ok(())
    }

    pub(crate) fn check_observed_cohort(&self) -> Result<(), D1Error> {
        let observed = &self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?
            .journal;
        self.check_cohort_identity(
            observed.cohort(),
            observed.authority(),
            observed.capacity_grants(),
        )
    }

    pub(crate) fn check_cohort(&self, journal: &Journal) -> Result<(), D1Error> {
        self.check_cohort_identity(
            journal.cohort(),
            journal.authority(),
            journal.capacity_grants(),
        )
    }

    fn check_cohort_identity(
        &self,
        cohort: Option<&crate::transaction_decisions::Cohort>,
        authority: [u8; 16],
        grants: Option<&[crate::transaction_decisions::MemberCapacityGrant]>,
    ) -> Result<(), D1Error> {
        if self
            .visibility
            .as_ref()
            .is_some_and(|v| v.journal.cohort() != cohort)
        {
            return Err(D1Error::VisibilityHistory);
        }
        if let Some(cohort) = cohort {
            if let Some(visibility) = &self.visibility {
                let byte_bound = cohort.profile
                    == crate::transaction_decisions::CapacityProfile::ByteBound16MiBV1;
                if byte_bound != visibility.activation.is_some() {
                    return Err(D1Error::VisibilityHistory);
                }
                if let Some(grants) = grants {
                    let own = grants
                        .iter()
                        .find(|grant| grant.member.shard == self.shard)
                        .ok_or(D1Error::VisibilityHistory)?;
                    if visibility.activation.as_ref() != Some(&own.activation) {
                        return Err(D1Error::VisibilityHistory);
                    }
                }
            }
            let participant = self
                .participant
                .as_ref()
                .ok_or(D1Error::ParticipantNotEnabled)?;
            let baseline = self
                .visibility
                .as_ref()
                .map_or(self.sequence, |v| v.baseline_sequence);
            if baseline != 0
                || cohort.coordinator_shard != participant.authority.shard
                || authority != participant.authority.mode
                || !cohort.members.iter().any(|m| {
                    m.shard == self.shard
                        && m.epoch == self.epoch
                        && self
                            .managed
                            .as_ref()
                            .is_some_and(|mode| mode.mode_id == m.mode)
                })
            {
                return Err(D1Error::VisibilityHistory);
            }
        }
        Ok(())
    }
    /// Observe exact events without making their decisions readable. Called only
    /// on an unpublished candidate; a failed merge or intent check publishes none.
    pub(crate) fn sync_history(
        &mut self,
        page: &crate::transaction_decisions::HistoryPage,
        history: Option<&Self>,
    ) -> Result<D1Result, D1Error> {
        let journal = self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?
            .journal
            .full()
            .ok_or(D1Error::VisibilityHistory)?
            .merge_history_page(page)
            .map_err(|_| D1Error::VisibilityHistory)?;
        self.check_cohort(&journal)?;
        self.visibility.as_mut().unwrap().journal = journal.into();
        // ApplyDelta retains only newly accepted D1 retry records. Reconstitute
        // the complete validation view, including pending local identities and
        // applied unknown-Abort negative evidence, without publishing that copy.
        let mut checked = self.clone();
        if let Some(history) = history {
            checked.dedup.splice(0..0, history.dedup.iter().cloned());
        }
        checked.validate().map_err(|_| D1Error::VisibilityHistory)?;
        let observed = self.visibility.as_ref().unwrap();
        Ok(D1Result::HistorySynced {
            cursor: observed
                .journal
                .full()
                .ok_or(D1Error::VisibilityHistory)?
                .history_cursor()
                .map_err(|_| D1Error::VisibilityHistory)?,
            applied_prefix: observed.applied,
        })
    }

    /// Verify the requested frozen cut against the complete local authority,
    /// then preserve any later observation while consuming exactly one ordinal.
    pub(crate) fn advance_retained(
        &mut self,
        ordinal: u64,
        cut: &crate::transaction_decisions::HistoryCursor,
        history: Option<&Self>,
    ) -> Result<(D1Result, Option<crate::D1WriteRequest>), D1Error> {
        let journal = &self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?
            .journal;
        let journal = journal.full().ok_or(D1Error::VisibilityHistory)?;
        let prefix = journal
            .history_prefix(cut)
            .map_err(|_| D1Error::VisibilityHistory)?;
        if ordinal == 0 || ordinal > prefix.resolved_prefix() {
            return Err(D1Error::VisibilityUnresolved);
        }
        self.advance_visibility(ordinal, journal.clone(), history)
    }

    /// Stage only on an unpublished candidate. Caller publishes data and this
    /// watermark in the same sync batch. Exactly one decision advances at a time.
    pub(crate) fn advance_visibility(
        &mut self,
        ordinal: u64,
        journal: Journal,
        history: Option<&Self>,
    ) -> Result<(D1Result, Option<crate::D1WriteRequest>), D1Error> {
        self.check_cohort(&journal)?;
        let versioned = self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?;
        if ordinal == 0 || ordinal > journal.resolved_prefix() {
            return Err(D1Error::VisibilityUnresolved);
        }
        if ordinal <= versioned.applied {
            if versioned
                .journal
                .full()
                .ok_or(D1Error::VisibilityHistory)?
                .validate_successor(&journal)
                .is_err()
                && journal
                    .validate_successor(versioned.journal.full().ok_or(D1Error::VisibilityHistory)?)
                    .is_err()
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
            .full()
            .ok_or(D1Error::VisibilityHistory)?
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
        state.journal = journal.into();
        Ok((
            D1Result::Visibility {
                applied_prefix: ordinal,
            },
            request,
        ))
    }

    fn validate_member_observation(&self) -> Result<&State, D1Error> {
        let visibility = self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?;
        self.check_observed_cohort()?;
        if visibility.applied > visibility.journal.resolved_prefix() {
            return Err(D1Error::VisibilityHistory);
        }
        self.participant
            .as_ref()
            .ok_or(D1Error::ParticipantNotEnabled)?;
        Ok(visibility)
    }

    fn validate_member_record(
        &self,
        record: &crate::participant_writes::Intent,
        evidence: &dyn MemberHistory,
    ) -> Result<(), D1Error> {
        let visibility = self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?;
        self.check_capacity_prepare_with_evidence(
            &record.participant,
            record.id,
            record.request.as_ref(),
            evidence,
        )?;
        let tx = evidence.transaction(record.id)?;
        if let Some(tx) = &tx {
            if !tx.participants.contains(&record.participant) {
                return Err(D1Error::VisibilityHistory);
            }
            if tx.ordinal <= visibility.applied
                && (record.decision != tx.decision || record.decision.is_none())
            {
                return Err(D1Error::VisibilityHistory);
            }
        }
        if record.decision.is_some()
            && !tx.as_ref().is_some_and(|tx| {
                tx.ordinal <= visibility.applied && tx.decision == record.decision
            })
        {
            return Err(D1Error::VisibilityHistory);
        }
        Ok(())
    }

    /// Explicit cold/test validation of every resident identity against complete
    /// evidence. Ordinary apply uses the inductive changed-identity check below.
    pub(crate) fn validate_member_history(
        &self,
        evidence: &dyn MemberHistory,
    ) -> Result<(), D1Error> {
        self.validate_member_observation()?;
        for record in &self.participant.as_ref().unwrap().records {
            self.validate_member_record(record, evidence)?;
        }
        Ok(())
    }

    /// The caller has verified the private prior cut and exact accepted original
    /// suffix. That prior state is already valid: immutable admissions mean only
    /// identities touched by new commands can invalidate its resident intents.
    /// Rejected/no-op proposals and overlapping page commands must not be passed
    /// as newly accepted events. This changes neither A nor local data.
    pub(crate) fn validate_member_history_changes(
        &self,
        evidence: &dyn MemberHistory,
        events: &[Command],
    ) -> Result<(), D1Error> {
        let visibility = self.validate_member_observation()?;
        let profile = visibility.journal.capacity_profile();
        if events.len() as u64 > profile.max_history_events() {
            return Err(D1Error::VisibilityHistory);
        }
        let mut touched = BTreeSet::new();
        for event in events {
            let id = match event {
                Command::AdmitRequest { transaction_id, .. } => transaction_id,
                Command::Begin { id, .. }
                | Command::Prepared { id, .. }
                | Command::Decide { id, .. } => id,
                Command::BindCohort { .. } | Command::ConfirmCohortCapacity { .. } => continue,
            };
            if !touched.contains(id) {
                if touched.len() >= profile.max_transactions() {
                    return Err(D1Error::VisibilityHistory);
                }
                touched.insert(*id);
            }
        }
        for record in &self.participant.as_ref().unwrap().records {
            if touched.contains(&record.id) {
                self.validate_member_record(record, evidence)?;
            }
        }
        Ok(())
    }

    /// The caller must first prove this target is resolved at the exact requested
    /// frozen cut, including old retries. Current observed R is not that proof.
    /// Evidence includes the entire accepted original-command batch overlay.
    /// Any newly observed events must pass validate_member_history_changes first.
    pub(crate) fn advance_member_transaction(
        &mut self,
        tx: Transaction,
        evidence: &dyn MemberHistory,
        history: Option<&Self>,
    ) -> Result<(D1Result, Option<crate::D1WriteRequest>), D1Error> {
        let visibility = self.validate_member_observation()?;
        let ordinal = tx.ordinal;
        if ordinal == 0 || ordinal > visibility.journal.resolved_prefix() {
            return Err(D1Error::VisibilityUnresolved);
        }
        if evidence.transaction_at(ordinal)?.as_ref() != Some(&tx) {
            return Err(D1Error::VisibilityHistory);
        }
        let decision = tx.decision.ok_or(D1Error::VisibilityUnresolved)?;
        if ordinal <= visibility.applied {
            return Ok((
                D1Result::Visibility {
                    applied_prefix: visibility.applied,
                },
                None,
            ));
        }
        if visibility.applied.checked_add(1) != Some(ordinal) {
            return Err(D1Error::VisibilityGap);
        }
        let participant = self
            .participant
            .as_ref()
            .ok_or(D1Error::ParticipantNotEnabled)?;
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
                // Archived exact identity is the negative record; no tombstone.
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
        let visibility = self.visibility.as_mut().unwrap();
        if request.is_some() {
            visibility.commits.push(CommitVersion {
                ordinal,
                id: tx.id,
                sequence: self.sequence,
            });
        }
        visibility.applied = ordinal;
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
            baseline_sequence: state.baseline_sequence,
            cohort: state.journal.cohort().cloned(),
            applied_prefix: state.applied,
            local_sequence: sequence,
            replay_operations: operations,
            entries: entries
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect(),
        })
    }
}

/// Explicit fixed participant identity. No catalog discovery or failover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[cfg(test)]
mod bounded_decode_tests {
    use super::*;

    #[derive(Deserialize)]
    struct Commits(#[serde(deserialize_with = "deserialize_commits")] Vec<CommitVersion>);

    #[test]
    fn commit_map_decoder_enforces_d1_lifetime_ceiling() {
        let record = CommitVersion {
            ordinal: 1,
            id: [1; 16],
            sequence: 1,
        };
        let bytes = serde_json::to_vec(&vec![record.clone(); crate::MAX_DEDUP_REQUESTS]).unwrap();
        assert_eq!(
            serde_json::from_slice::<Commits>(&bytes).unwrap().0.len(),
            crate::MAX_DEDUP_REQUESTS
        );
        let bytes = serde_json::to_vec(&vec![record; crate::MAX_DEDUP_REQUESTS + 1]).unwrap();
        assert!(serde_json::from_slice::<Commits>(&bytes).is_err());
    }
}
