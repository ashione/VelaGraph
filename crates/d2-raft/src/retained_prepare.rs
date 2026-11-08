//! Strict retained-history preparation. This is a new command contract; the
//! original trusted PrepareParticipant encoding and replay semantics are intact.
use crate::d1::D1State;
use crate::participant_writes::Authority;
use crate::target_evidence::TargetRecord;
use crate::transaction_decisions::{HistoryCursor, Participant};
use crate::versioned_reads::MemberHistory;
use crate::{D1Error, D1Result, D1WriteRequest, WriteOp, MAX_BATCH_BYTES, MAX_BATCH_OPERATIONS};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::fmt;

/// Preparation against an exact, already retained original-event cut. A cursor
/// is only a claim until deterministic application proves it from local history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Encoding")]
pub struct RetainedPrepare {
    pub version: u32,
    pub authority: Authority,
    pub participant: Participant,
    pub request: D1WriteRequest,
    pub cut: HistoryCursor,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Encoding {
    version: u32,
    authority: Authority,
    participant: Participant,
    #[serde(deserialize_with = "deserialize_request")]
    request: D1WriteRequest,
    cut: HistoryCursor,
}
impl TryFrom<Encoding> for RetainedPrepare {
    type Error = String;
    fn try_from(value: Encoding) -> Result<Self, Self::Error> {
        let prepare = Self {
            version: value.version,
            authority: value.authority,
            participant: value.participant,
            request: value.request,
            cut: value.cut,
        };
        prepare
            .validate()
            .map_err(|e| format!("invalid retained prepare: {e:?}"))?;
        Ok(prepare)
    }
}
impl RetainedPrepare {
    /// Structural validation only, never proof that the supplied cut exists.
    pub fn validate(&self) -> Result<(), D1Error> {
        self.request.validate()?;
        if self.version != 1
            || self.authority.mode == [0; 16]
            || self.participant.intent == [0; 32]
            || self.request.request_id == [0; 16]
            || self.request.shard != self.participant.shard
            || self.request.epoch != self.participant.epoch
            || !self.cut.cohort.contains(&self.participant)
            || self
                .cut
                .validate_identity(&self.authority, &self.cut.cohort)
                .is_err()
        {
            return Err(D1Error::ParticipantIdentity);
        }
        Ok(())
    }
}

// This adapter is exclusive to the new strict command. Do not silently tighten
// historical WriteOp or PrepareParticipant decoding while replaying old logs.
fn deserialize_request<'de, D: Deserializer<'de>>(d: D) -> Result<D1WriteRequest, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        shard: u64,
        epoch: u64,
        request_id: [u8; 16],
        #[serde(deserialize_with = "deserialize_sequence")]
        expected_sequence: Option<u64>,
        #[serde(deserialize_with = "deserialize_operations")]
        operations: Vec<WriteOp>,
    }
    let r = Request::deserialize(d)?;
    Ok(D1WriteRequest {
        shard: r.shard,
        epoch: r.epoch,
        request_id: r.request_id,
        expected_sequence: r.expected_sequence,
        operations: r.operations,
    })
}
// A missing field is different from the explicitly encoded legacy None value.
fn deserialize_sequence<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(d)
}
fn deserialize_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    crate::capacity_policy::deserialize_bounded_vec(d, MAX_BATCH_BYTES)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum Operation {
    Put {
        #[serde(deserialize_with = "deserialize_bytes")]
        key: Vec<u8>,
        #[serde(deserialize_with = "deserialize_bytes")]
        value: Vec<u8>,
    },
    Delete {
        #[serde(deserialize_with = "deserialize_bytes")]
        key: Vec<u8>,
    },
}
fn deserialize_operations<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<WriteOp>, D::Error> {
    struct Operations;
    impl<'de> de::Visitor<'de> for Operations {
        type Value = Vec<WriteOp>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("strict bounded D1 operations")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            if seq.size_hint().is_some_and(|n| n > MAX_BATCH_OPERATIONS) {
                return Err(de::Error::custom("too many D1 operations"));
            }
            let mut result = Vec::new();
            let mut bytes = 0usize;
            while result.len() < MAX_BATCH_OPERATIONS {
                let Some(operation) = seq.next_element::<Operation>()? else {
                    return Ok(result);
                };
                let (operation, size) = match operation {
                    Operation::Put { key, value } => {
                        let size = key.len().saturating_add(value.len());
                        (WriteOp::Put { key, value }, size)
                    }
                    Operation::Delete { key } => {
                        let size = key.len();
                        (WriteOp::Delete { key }, size)
                    }
                };
                bytes = bytes
                    .checked_add(size)
                    .filter(|n| *n <= MAX_BATCH_BYTES)
                    .ok_or_else(|| de::Error::custom("D1 operation bytes exceed limit"))?;
                result.push(operation);
            }
            seq.next_element_seed(crate::capacity_policy::RejectExtra)?;
            Ok(result)
        }
    }
    d.deserialize_seq(Operations)
}

impl D1State {
    pub(crate) fn check_retained_prepare(&self, prepare: &RetainedPrepare) -> Result<(), D1Error> {
        self.check_participant(&prepare.authority, &prepare.participant)?;
        prepare.validate()?;
        self.check_observed_cohort()?;
        let observed = &self
            .visibility
            .as_ref()
            .ok_or(D1Error::VisibilityNotEnabled)?
            .journal;
        if observed.cohort() != Some(&prepare.cut.cohort)
            || observed.authority() != prepare.authority.mode
        {
            return Err(D1Error::VisibilityHistory);
        }
        if !observed.capacity_ready() {
            return Err(D1Error::CoordinatorRejected(
                crate::transaction_decisions::Error::CapacityNotConfirmed,
            ));
        }
        Ok(())
    }

    /// Explicit Full/legacy boundary. Active members use the private overlay
    /// projection below, never history_prefix or a temporary local Journal.
    pub(crate) fn prepare_retained(
        &mut self,
        prepare: &RetainedPrepare,
        history: Option<&Self>,
    ) -> Result<D1Result, D1Error> {
        self.check_retained_prepare(prepare)?;
        let journal = self
            .visibility
            .as_ref()
            .unwrap()
            .journal
            .full()
            .ok_or(D1Error::VisibilityHistory)?;
        let prefix = journal
            .history_prefix(&prepare.cut)
            .map_err(|_| D1Error::VisibilityHistory)?;
        let target = prefix
            .transaction(prepare.request.request_id)
            .cloned()
            .map(|transaction| TargetRecord {
                request: prefix.request_for_transaction(transaction.id).cloned(),
                transaction,
            });
        self.check_retained_target(prepare, target.as_ref())?;
        self.prepare_participant(
            &prepare.authority,
            prepare.participant.clone(),
            prepare.request.clone(),
            history,
        )
    }

    pub(crate) fn prepare_retained_with_evidence(
        &mut self,
        prepare: &RetainedPrepare,
        target: Option<TargetRecord>,
        history: Option<&Self>,
        evidence: &dyn MemberHistory,
    ) -> Result<D1Result, D1Error> {
        self.check_retained_prepare(prepare)?;
        self.check_retained_target(prepare, target.as_ref())?;
        self.prepare_participant_with_evidence(
            &prepare.authority,
            prepare.participant.clone(),
            prepare.request.clone(),
            history,
            evidence,
        )
    }

    fn check_retained_target(
        &self,
        prepare: &RetainedPrepare,
        target: Option<&TargetRecord>,
    ) -> Result<(), D1Error> {
        let target = target.ok_or(D1Error::VisibilityHistory)?;
        target
            .validate(&prepare.authority, &prepare.cut.cohort)
            .map_err(|_| D1Error::VisibilityHistory)?;
        let tx = &target.transaction;
        if tx.id != prepare.request.request_id
            || tx.ordinal == 0
            || !tx.participants.contains(&prepare.participant)
        {
            return Err(D1Error::ParticipantIdentity);
        }
        // A later local idempotent status cannot authorize a terminal-at-cut
        // Prepare, or a cut preceding the original admission.
        if tx.decision.is_some() {
            return Err(D1Error::VisibilityUnresolved);
        }
        if let Some(record) = &target.request {
            let plan = crate::request_plans::Plan::decode(&record.payload)
                .map_err(|_| D1Error::VisibilityHistory)?;
            if !plan.parts.iter().any(|part| {
                part.descriptor == prepare.participant && part.request == prepare.request
            }) {
                return Err(D1Error::IdempotencyConflict);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "retained_prepare_tests.rs"]
mod tests;
