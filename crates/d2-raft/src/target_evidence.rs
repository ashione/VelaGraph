//! One exact retained transaction and optional original external request binding.
//! This is trusted-source point evidence, not an inclusion or visibility proof.
use crate::participant_writes::Authority;
use crate::transaction_decisions::{
    Cohort, Decision, Error, HistoryCursor, Journal, RequestRecord, Transaction, MAX_PARTICIPANTS,
    MAX_REQUEST_BYTES, MAX_REQUEST_PAYLOAD_BYTES,
};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetRecord {
    #[serde(deserialize_with = "deserialize_transaction")]
    pub transaction: Transaction,
    #[serde(deserialize_with = "deserialize_request")]
    pub request: Option<RequestRecord>,
}

fn deserialize_transaction<'de, D: Deserializer<'de>>(d: D) -> Result<Transaction, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Encoding {
        id: [u8; 16],
        ordinal: u64,
        #[serde(deserialize_with = "crate::transaction_decisions::deserialize_participants")]
        participants: Vec<crate::transaction_decisions::Participant>,
        #[serde(deserialize_with = "crate::transaction_decisions::deserialize_prepared")]
        prepared: Vec<bool>,
        #[serde(deserialize_with = "required_decision")]
        decision: Option<Decision>,
    }
    fn required_decision<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Decision>, D::Error> {
        Option::<Decision>::deserialize(d)
    }
    let tx = Encoding::deserialize(d)?;
    Ok(Transaction {
        id: tx.id,
        ordinal: tx.ordinal,
        participants: tx.participants,
        prepared: tx.prepared,
        decision: tx.decision,
    })
}

fn deserialize_request<'de, D: Deserializer<'de>>(d: D) -> Result<Option<RequestRecord>, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Encoding {
        id: [u8; 16],
        #[serde(deserialize_with = "request_bytes")]
        request: Vec<u8>,
        #[serde(deserialize_with = "plan_bytes")]
        payload: Vec<u8>,
        transaction_id: [u8; 16],
    }
    fn request_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        crate::capacity_policy::deserialize_bounded_vec(d, MAX_REQUEST_BYTES)
    }
    fn plan_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        crate::capacity_policy::deserialize_bounded_vec(d, MAX_REQUEST_PAYLOAD_BYTES)
    }
    Ok(Option::<Encoding>::deserialize(d)?.map(|r| RequestRecord {
        id: r.id,
        request: r.request,
        payload: r.payload,
        transaction_id: r.transaction_id,
    }))
}

impl TargetRecord {
    /// Check the complete transaction and retained original Plan binding. The
    /// source separately proves its actual admission ordinal at the chosen E.
    pub fn validate(&self, authority: &Authority, cohort: &Cohort) -> Result<(), Error> {
        let tx = &self.transaction;
        if authority.mode == [0; 16]
            || authority.shard != cohort.coordinator_shard
            || cohort.validate().is_err()
            || tx.id == [0; 16]
            || tx.ordinal == 0
            || tx.ordinal > cohort.profile.max_transactions() as u64
            || tx.participants.is_empty()
            || tx.participants.len() > MAX_PARTICIPANTS
            || tx.participants.windows(2).any(|p| p[0].shard >= p[1].shard)
            || tx
                .participants
                .iter()
                .any(|p| p.intent == [0; 32] || !cohort.contains(p))
            || tx.prepared.len() != tx.participants.len()
            || (tx.decision == Some(Decision::Commit) && !tx.prepared.iter().all(|p| *p))
        {
            return Err(Error::InvalidIdentity);
        }
        if let Some(record) = &self.request {
            if record.id == [0; 16]
                || record.transaction_id != tx.id
                || record.request.is_empty()
                || record.request.len() > MAX_REQUEST_BYTES
                || record.payload.is_empty()
                || record.payload.len() > MAX_REQUEST_PAYLOAD_BYTES
            {
                return Err(Error::IdentityConflict);
            }
            let plan = crate::request_plans::Plan::decode(&record.payload)?;
            if plan.id != tx.id
                || &plan.authority != authority
                || &plan.cohort != cohort
                || plan.validation_version >= tx.ordinal
                || !plan
                    .parts
                    .iter()
                    .map(|p| &p.descriptor)
                    .eq(tx.participants.iter())
            {
                return Err(Error::IdentityConflict);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Encoding")]
pub struct TargetEvidence {
    pub version: u32,
    pub cut: HistoryCursor,
    pub resolved_prefix: u64,
    pub id: [u8; 16],
    pub record: Option<TargetRecord>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Encoding {
    version: u32,
    cut: HistoryCursor,
    resolved_prefix: u64,
    id: [u8; 16],
    #[serde(deserialize_with = "required_record")]
    record: Option<TargetRecord>,
}
// Option fields in an evidence envelope are required: only explicit null is
// an absent result. A missing field must not silently become nonmembership.
fn required_record<'de, D: Deserializer<'de>>(d: D) -> Result<Option<TargetRecord>, D::Error> {
    Option::<TargetRecord>::deserialize(d)
}
impl TryFrom<Encoding> for TargetEvidence {
    type Error = String;
    fn try_from(value: Encoding) -> Result<Self, Self::Error> {
        let result = Self {
            version: value.version,
            cut: value.cut,
            resolved_prefix: value.resolved_prefix,
            id: value.id,
            record: value.record,
        };
        result
            .validate()
            .map_err(|e| format!("invalid target evidence: {e:?}"))?;
        Ok(result)
    }
}
impl TargetEvidence {
    pub fn validate(&self) -> Result<(), Error> {
        self.cut.validate()?;
        // A loose wire-shape bound only, not N. The anchored source projection
        // counts actual admissions at E and compares its exact checkpoint N.
        let admissions_bound = self
            .cut
            .event_count
            .checked_sub(1)
            .ok_or(Error::InvalidIdentity)?;
        if self.version != 1
            || self.id == [0; 16]
            || self.resolved_prefix > self.cut.cohort.profile.max_transactions() as u64
            || self.resolved_prefix > admissions_bound / 2
        {
            return Err(Error::InvalidIdentity);
        }
        if let Some(record) = &self.record {
            record.validate(&self.cut.authority, &self.cut.cohort)?;
            let tx = &record.transaction;
            if tx.id != self.id
                || tx.ordinal > admissions_bound
                || (tx.ordinal <= self.resolved_prefix && tx.decision.is_none())
            {
                return Err(Error::IdentityConflict);
            }
            if let Some(request) = &record.request {
                let plan = crate::request_plans::Plan::decode(&request.payload)?;
                if plan.validation_version > self.resolved_prefix {
                    return Err(Error::IdentityConflict);
                }
            }
        }
        Ok(())
    }

    /// Explicit Full legacy/memory boundary; durable active readers never use it.
    pub(crate) fn from_journal(journal: &Journal, id: [u8; 16]) -> Result<Self, Error> {
        let result = Self {
            version: 1,
            cut: journal.history_cursor()?,
            resolved_prefix: journal.resolved_prefix(),
            id,
            record: journal
                .transaction(id)
                .cloned()
                .map(|transaction| TargetRecord {
                    transaction,
                    request: journal.request_for_transaction(id).cloned(),
                }),
        };
        result.validate()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::{Command, Member, Participant};

    fn journal() -> Journal {
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: Cohort {
                    coordinator_shard: 9,
                    coordinator_epoch: 1,
                    members: vec![Member {
                        shard: 7,
                        epoch: 1,
                        mode: [7; 16],
                    }],
                    profile: Default::default(),
                },
            })
            .unwrap();
        for id in [1, 2] {
            journal
                .apply(Command::Begin {
                    id: [id; 16],
                    participants: vec![Participant {
                        shard: 7,
                        epoch: 1,
                        mode: [7; 16],
                        intent: [id; 32],
                    }],
                })
                .unwrap();
        }
        journal
            .apply(Command::Decide {
                id: [2; 16],
                decision: Decision::Abort,
            })
            .unwrap();
        journal
    }

    #[test]
    fn direct_targets_preserve_holes_and_absence_without_a_binding() {
        let journal = journal();
        let pending = TargetEvidence::from_journal(&journal, [1; 16]).unwrap();
        let terminal = TargetEvidence::from_journal(&journal, [2; 16]).unwrap();
        let absent = TargetEvidence::from_journal(&journal, [3; 16]).unwrap();
        assert_eq!(pending.resolved_prefix, 0);
        assert_eq!(terminal.resolved_prefix, 0);
        assert_eq!(terminal.record.as_ref().unwrap().transaction.ordinal, 2);
        assert_eq!(
            terminal.record.as_ref().unwrap().transaction.decision,
            Some(Decision::Abort)
        );
        assert!(pending.record.unwrap().request.is_none());
        assert!(absent.record.is_none());
        let bytes = serde_json::to_vec(&terminal).unwrap();
        assert_eq!(
            serde_json::from_slice::<TargetEvidence>(&bytes).unwrap(),
            terminal
        );
    }

    #[test]
    fn strict_target_fields_do_not_turn_structure_into_inclusion_authority() {
        let target = TargetEvidence::from_journal(&journal(), [2; 16]).unwrap();
        for edit in 0..7 {
            let mut bad = target.clone();
            match edit {
                0 => bad.version = 2,
                1 => bad.id = [3; 16],
                2 => bad.resolved_prefix = 2,
                3 => bad.record.as_mut().unwrap().transaction.ordinal = bad.cut.event_count,
                4 => bad.record.as_mut().unwrap().transaction.prepared.clear(),
                5 => bad.record.as_mut().unwrap().transaction.participants[0].mode = [6; 16],
                _ => bad.record.as_mut().unwrap().transaction.decision = Some(Decision::Commit),
            }
            assert!(bad.validate().is_err());
            assert!(
                serde_json::from_slice::<TargetEvidence>(&serde_json::to_vec(&bad).unwrap())
                    .is_err()
            );
        }
        let mut unknown = serde_json::to_value(&target).unwrap();
        unknown["record"]["transaction"]["extra"] = true.into();
        assert!(serde_json::from_value::<TargetEvidence>(unknown).is_err());
        let mut missing = serde_json::to_value(&target).unwrap();
        missing.as_object_mut().unwrap().remove("record");
        assert!(serde_json::from_value::<TargetEvidence>(missing).is_err());
        let mut null = serde_json::to_value(&target).unwrap();
        null["record"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<TargetEvidence>(null).is_ok());
        let mut missing_binding = serde_json::to_value(&target).unwrap();
        missing_binding["record"]
            .as_object_mut()
            .unwrap()
            .remove("request");
        assert!(serde_json::from_value::<TargetEvidence>(missing_binding).is_err());
        let mut missing_decision = serde_json::to_value(&target).unwrap();
        missing_decision["record"]["transaction"]
            .as_object_mut()
            .unwrap()
            .remove("decision");
        assert!(serde_json::from_value::<TargetEvidence>(missing_decision).is_err());
        let mut pending = serde_json::to_value(&target).unwrap();
        pending["record"]["transaction"]["decision"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<TargetEvidence>(pending).is_ok());
        let mut root = target;
        root.cut.root[0] ^= 1;
        // A point reply alone has no inclusion proof. Independent retained-page
        // and apply closure, not this shape check, reject an invented root.
        assert!(root.validate().is_ok());
    }

    #[test]
    fn original_binding_is_exact_and_byte_vectors_are_bounded_during_decode() {
        let journal =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        let request = &journal.requests()[0];
        let target = TargetEvidence::from_journal(&journal, request.transaction_id).unwrap();
        assert_eq!(
            target.record.as_ref().unwrap().request.as_ref(),
            Some(request)
        );
        let mut bad = target.clone();
        bad.record
            .as_mut()
            .unwrap()
            .request
            .as_mut()
            .unwrap()
            .transaction_id = [99; 16];
        assert!(bad.validate().is_err());
        for field in ["request", "payload"] {
            let limit = if field == "request" {
                MAX_REQUEST_BYTES
            } else {
                MAX_REQUEST_PAYLOAD_BYTES
            };
            let mut value = serde_json::to_value(&target).unwrap();
            value["record"]["request"][field] = serde_json::json!(vec![0u8; limit + 1]);
            assert!(serde_json::from_value::<TargetEvidence>(value).is_err());
        }
        let bytes = serde_json::to_string(&target).unwrap();
        let duplicate = bytes.replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
        assert!(serde_json::from_str::<TargetEvidence>(&duplicate).is_err());
    }
}
