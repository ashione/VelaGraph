//! Durable shard participant state. This does not provide global read visibility.
use crate::d1::D1State;
use crate::transaction_decisions::{Decision, Participant};
use crate::{D1Error, D1Result, D1WriteRequest};
use serde::{Deserialize, Serialize};

pub const MAX_INTENTS: usize = 1024;
pub const MAX_INTENT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub shard: u64,
    pub mode: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    Unknown,
    Prepared,
    Committed { sequence: u64 },
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intent {
    pub id: [u8; 16],
    pub participant: Participant,
    pub request: Option<D1WriteRequest>,
    pub decision: Option<Decision>,
    pub sequence: Option<u64>,
}
impl Intent {
    pub fn status(&self) -> Status {
        match self.decision {
            None => Status::Prepared,
            Some(Decision::Abort) => Status::Aborted,
            Some(Decision::Commit) => Status::Committed {
                sequence: self.sequence.expect("validated commit sequence"),
            },
        }
    }
    fn bytes(&self) -> usize {
        256 + self
            .request
            .as_ref()
            .map_or(0, D1WriteRequest::retained_size)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub authority: Authority,
    pub activated_sequence: u64,
    pub records: Vec<Intent>,
}
impl State {
    pub fn pending(&self) -> bool {
        self.records.iter().any(|r| r.decision.is_none())
    }
    pub fn status(&self, id: [u8; 16]) -> Status {
        self.records
            .iter()
            .find(|r| r.id == id)
            .map_or(Status::Unknown, Intent::status)
    }
    pub fn validate_successor(&self, next: &Self) -> bool {
        self.authority == next.authority
            && self.activated_sequence == next.activated_sequence
            && next.records.len() >= self.records.len()
            && self.records.iter().zip(&next.records).all(|(old, new)| {
                old.id == new.id
                    && old.participant == new.participant
                    && old.request == new.request
                    && (old.decision.is_none()
                        || (old.decision == new.decision && old.sequence == new.sequence))
            })
    }
    pub fn validate(&self, d1: &D1State) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.authority.mode != [0; 16] && self.authority.shard != d1.shard,
            "invalid participant authority"
        );
        anyhow::ensure!(
            self.activated_sequence <= d1.sequence && self.records.len() <= MAX_INTENTS,
            "invalid participant history bounds"
        );
        anyhow::ensure!(
            self.records.iter().map(Intent::bytes).sum::<usize>() <= MAX_INTENT_BYTES,
            "participant retention exceeded"
        );
        let mode = &d1
            .managed
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("participant without managed mode"))?
            .mode_id;
        anyhow::ensure!(
            self.activated_sequence >= d1.managed.as_ref().unwrap().activated_sequence,
            "participant predates managed activation"
        );
        let mut ids = std::collections::BTreeSet::new();
        let mut pending = 0;
        let mut sequence = self.activated_sequence;
        let mut previous_epoch = 0;
        for record in &self.records {
            anyhow::ensure!(
                record.participant.epoch >= previous_epoch,
                "participant epoch history regressed"
            );
            previous_epoch = record.participant.epoch;
            anyhow::ensure!(
                record.id != [0; 16] && ids.insert(record.id),
                "invalid participant identity"
            );
            anyhow::ensure!(
                record.participant.shard == d1.shard
                    && record.participant.mode == *mode
                    && record.participant.epoch <= d1.epoch
                    && record.participant.intent != [0; 32],
                "invalid participant ownership"
            );
            anyhow::ensure!(
                !d1.dedup
                    .iter()
                    .any(|r| r.sequence <= self.activated_sequence
                        && r.request.request_id == record.id),
                "participant reuses baseline identity"
            );
            if let Some(request) = &record.request {
                let reserved = request
                    .validate()
                    .map_err(|e| anyhow::anyhow!("invalid prepare: {e:?}"))?;
                let retained = d1
                    .dedup
                    .iter()
                    .filter(|r| r.sequence <= sequence)
                    .map(|r| r.request.retained_size())
                    .sum::<usize>();
                anyhow::ensure!(
                    sequence < crate::MAX_DEDUP_REQUESTS as u64
                        && retained.saturating_add(reserved) <= d1.bytes_limit(),
                    "unreachable prepare capacity reservation"
                );
                anyhow::ensure!(
                    request.request_id == record.id
                        && request.shard == d1.shard
                        && request.epoch == record.participant.epoch
                        && request.expected_sequence == Some(sequence),
                    "unreachable prepare condition"
                );
            } else {
                anyhow::ensure!(
                    record.decision == Some(Decision::Abort),
                    "missing prepared request"
                );
            }
            match record.decision {
                None => {
                    pending += 1;
                    anyhow::ensure!(
                        record.participant.epoch == d1.epoch,
                        "pending intent ownership changed"
                    );
                    anyhow::ensure!(
                        record.sequence.is_none() && sequence == d1.sequence,
                        "unresolved prepare changed data sequence"
                    );
                }
                Some(Decision::Abort) => {
                    anyhow::ensure!(record.sequence.is_none(), "abort has commit sequence")
                }
                Some(Decision::Commit) => {
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("participant sequence overflow"))?;
                    anyhow::ensure!(
                        record.sequence == Some(sequence),
                        "invalid participant commit order"
                    );
                    let request = record
                        .request
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("missing commit request"))?;
                    anyhow::ensure!(
                        d1.dedup
                            .iter()
                            .any(|r| r.sequence == sequence && r.request == *request),
                        "participant commit missing exact D1 history"
                    );
                }
            }
        }
        anyhow::ensure!(
            pending <= 1 && sequence == d1.sequence,
            "untracked participant write or multiple pending prepares"
        );
        Ok(())
    }
}

impl D1State {
    pub(crate) fn check_participant(
        &self,
        authority: &Authority,
        descriptor: &Participant,
    ) -> Result<(), D1Error> {
        self.check_shard(descriptor.shard)?;
        self.check_epoch(descriptor.epoch)?;
        if self
            .managed
            .as_ref()
            .is_none_or(|m| m.mode_id != descriptor.mode)
        {
            return Err(D1Error::ManagedModeConflict);
        }
        let state = self
            .participant
            .as_ref()
            .ok_or(D1Error::ParticipantNotEnabled)?;
        if state.authority != *authority {
            return Err(D1Error::ParticipantAuthority);
        }
        if descriptor.intent == [0; 32] {
            return Err(D1Error::ParticipantIdentity);
        }
        Ok(())
    }

    pub(crate) fn prepare_participant(
        &mut self,
        authority: &Authority,
        descriptor: Participant,
        request: D1WriteRequest,
        history: Option<&Self>,
    ) -> Result<D1Result, D1Error> {
        self.check_participant(authority, &descriptor)?;
        request.validate()?;
        if request.request_id == [0; 16]
            || request.shard != descriptor.shard
            || request.epoch != descriptor.epoch
        {
            return Err(D1Error::ParticipantIdentity);
        }
        let state = self.participant.as_ref().unwrap();
        if let Some(record) = state.records.iter().find(|r| r.id == request.request_id) {
            if record.participant != descriptor
                || record.request.as_ref().is_some_and(|r| *r != request)
            {
                return Err(D1Error::IdempotencyConflict);
            }
            return Ok(D1Result::Participant(record.status()));
        }
        if let Some(visibility) = &self.visibility {
            if let Some(tx) = visibility.journal.transaction(request.request_id) {
                if !tx.participants.contains(&descriptor) {
                    return Err(D1Error::IdempotencyConflict);
                }
                if tx.ordinal <= visibility.applied {
                    return if tx.decision == Some(Decision::Abort) {
                        Ok(D1Result::Participant(Status::Aborted))
                    } else {
                        Err(D1Error::VisibilityHistory)
                    };
                }
                if tx.decision.is_some() {
                    return Err(D1Error::VisibilityUnresolved);
                }
            }
        }
        if state.pending() {
            return Err(D1Error::ParticipantBusy);
        }
        if request.expected_sequence != Some(self.sequence) {
            return Err(D1Error::Conflict {
                expected: request.expected_sequence.unwrap_or(u64::MAX),
                current: self.sequence,
            });
        }
        if self
            .dedup
            .iter()
            .chain(history.into_iter().flat_map(|s| &s.dedup))
            .any(|r| r.request.request_id == request.request_id)
        {
            return Err(D1Error::IdempotencyConflict);
        }
        let intent = Intent {
            id: request.request_id,
            participant: descriptor,
            request: Some(request.clone()),
            decision: None,
            sequence: None,
        };
        if state.records.len() >= MAX_INTENTS
            || state
                .records
                .iter()
                .map(Intent::bytes)
                .sum::<usize>()
                .saturating_add(intent.bytes())
                > MAX_INTENT_BYTES
        {
            return Err(D1Error::ParticipantCapacity);
        }
        // Reserve the future commit's existing D1 dedup slot and bytes now. No
        // other data write can consume that reservation while this prepare lives.
        let mut check = self.clone();
        check.write_inner(&request, history)?;
        self.participant.as_mut().unwrap().records.push(intent);
        Ok(D1Result::Participant(Status::Prepared))
    }

    pub(crate) fn resolve_participant(
        &mut self,
        authority: &Authority,
        descriptor: Participant,
        id: [u8; 16],
        decision: Decision,
        history: Option<&Self>,
    ) -> Result<(D1Result, Option<D1WriteRequest>), D1Error> {
        self.check_participant(authority, &descriptor)?;
        if id == [0; 16] {
            return Err(D1Error::ParticipantIdentity);
        }
        let state = self.participant.as_ref().unwrap();
        let Some(index) = state.records.iter().position(|r| r.id == id) else {
            if decision != Decision::Abort {
                return Err(D1Error::ParticipantUnknown);
            }
            if self
                .dedup
                .iter()
                .chain(history.into_iter().flat_map(|s| &s.dedup))
                .any(|r| r.request.request_id == id)
            {
                return Err(D1Error::IdempotencyConflict);
            }
            if state.records.len() >= MAX_INTENTS
                || state
                    .records
                    .iter()
                    .map(Intent::bytes)
                    .sum::<usize>()
                    .saturating_add(256)
                    > MAX_INTENT_BYTES
            {
                return Err(D1Error::ParticipantCapacity);
            }
            // A durable negative identity prevents a delayed Prepare from
            // resurrecting a coordinator-aborted transaction after a lost reply.
            self.participant.as_mut().unwrap().records.push(Intent {
                id,
                participant: descriptor,
                request: None,
                decision: Some(Decision::Abort),
                sequence: None,
            });
            return Ok((D1Result::Participant(Status::Aborted), None));
        };
        let record = &state.records[index];
        if record.participant != descriptor {
            return Err(D1Error::IdempotencyConflict);
        }
        if let Some(previous) = record.decision {
            if previous != decision {
                return Err(D1Error::ParticipantDecisionConflict);
            }
            return Ok((D1Result::Participant(record.status()), None));
        }
        let request = record.request.clone().ok_or(D1Error::ParticipantUnknown)?;
        let sequence = if decision == Decision::Commit {
            self.write_inner(&request, history)?;
            Some(self.sequence)
        } else {
            None
        };
        let record = &mut self.participant.as_mut().unwrap().records[index];
        record.decision = Some(decision);
        record.sequence = sequence;
        Ok((
            D1Result::Participant(record.status()),
            if decision == Decision::Commit {
                Some(request)
            } else {
                None
            },
        ))
    }
}
