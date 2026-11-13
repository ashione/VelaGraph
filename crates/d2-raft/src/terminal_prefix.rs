//! Exact, bounded terminal-prefix evidence, independent of request/Plan archives.
//! This is a deterministic projection of a replay-validated journal, not an
//! authenticated certificate. Every record through the chosen cut is present.
use crate::capacity_policy::{
    canonical_size, deserialize_bounded_vec, CapacityProfile, RejectExtra, BYTE_MAX_TRANSACTIONS,
    MAX_BYTE_PROFILE_TERMINAL_BYTES,
};
use crate::transaction_decisions::{
    deserialize_participants, deserialize_prepared, Cohort, Decision, Error, Journal, Participant,
    MAX_PARTICIPANTS,
};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::collections::BTreeSet;
use std::fmt;

pub mod transport;

/// Canonical segment size. Only the final segment may contain fewer records.
pub const SEGMENT_TRANSACTIONS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Terminal {
    pub(crate) id: [u8; 16],
    pub(crate) ordinal: u64,
    #[serde(deserialize_with = "deserialize_participants")]
    pub(crate) participants: Vec<Participant>,
    #[serde(deserialize_with = "deserialize_prepared")]
    pub(crate) prepared: Vec<bool>,
    pub(crate) decision: Decision,
}

/// Private checked evidence. Construction/deserialization checks the complete
/// contiguous terminal prefix; a caller cannot provide a cached boolean proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Encoding", into = "Encoding")]
pub struct TerminalPrefix {
    authority: [u8; 16],
    cohort: Cohort,
    through: u64,
    segments: Vec<Vec<Terminal>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Encoding {
    version: u32,
    authority: [u8; 16],
    cohort: Cohort,
    through: u64,
    #[serde(deserialize_with = "deserialize_segments")]
    segments: Vec<Vec<Terminal>>,
}
fn deserialize_segments<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Vec<Terminal>>, D::Error> {
    #[derive(Deserialize)]
    struct Segment(#[serde(deserialize_with = "deserialize_segment")] Vec<Terminal>);
    fn deserialize_segment<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Terminal>, D::Error> {
        deserialize_bounded_vec(d, SEGMENT_TRANSACTIONS)
    }
    struct Segments;
    impl<'de> de::Visitor<'de> for Segments {
        type Value = Vec<Vec<Terminal>>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("bounded exact terminal segments")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let limit = BYTE_MAX_TRANSACTIONS.div_ceil(SEGMENT_TRANSACTIONS);
            if seq.size_hint().is_some_and(|count| count > limit) {
                return Err(de::Error::custom("too many terminal segments"));
            }
            let mut segments = Vec::new();
            let mut bytes = 0;
            while segments.len() < limit {
                let Some(segment) = seq.next_element::<Segment>()? else {
                    return Ok(segments);
                };
                // Every possible legacy 1024-record proof is smaller than this
                // byte-derived bound too. Check incrementally before retaining
                // the next bounded segment; the complete envelope is checked below.
                bytes += canonical_size(&segment.0, MAX_BYTE_PROFILE_TERMINAL_BYTES - bytes)
                    .map_err(|_| de::Error::custom("terminal segment byte budget exceeded"))?;
                segments.push(segment.0);
            }
            seq.next_element_seed(RejectExtra)?;
            Ok(segments)
        }
    }
    d.deserialize_seq(Segments)
}

impl From<TerminalPrefix> for Encoding {
    fn from(value: TerminalPrefix) -> Self {
        Self {
            version: value.cohort.profile.terminal_prefix_version(),
            authority: value.authority,
            cohort: value.cohort,
            through: value.through,
            segments: value.segments,
        }
    }
}
impl TryFrom<Encoding> for TerminalPrefix {
    type Error = &'static str;
    fn try_from(value: Encoding) -> Result<Self, Self::Error> {
        if value.version != value.cohort.profile.terminal_prefix_version()
            || value.authority == [0; 16]
            || value.cohort.validate().is_err()
            || value.through > value.cohort.profile.max_transactions() as u64
        {
            return Err("invalid terminal prefix identity/version");
        }
        if value.cohort.profile == CapacityProfile::ByteBound16MiBV1 {
            canonical_size(&value, MAX_BYTE_PROFILE_TERMINAL_BYTES)
                .map_err(|_| "terminal prefix byte budget exceeded")?;
        }
        let count = usize::try_from(value.through).map_err(|_| "terminal prefix count overflow")?;
        if value.segments.len() != count.div_ceil(SEGMENT_TRANSACTIONS) {
            return Err("invalid terminal prefix segments");
        }
        let mut ordinal = 0;
        let mut ids = BTreeSet::new();
        for (index, segment) in value.segments.iter().enumerate() {
            if segment.len() != (count - index * SEGMENT_TRANSACTIONS).min(SEGMENT_TRANSACTIONS) {
                return Err("noncanonical terminal prefix segment");
            }
            for tx in segment {
                ordinal += 1;
                if tx.ordinal != ordinal
                    || tx.id == [0; 16]
                    || !ids.insert(tx.id)
                    || tx.participants.is_empty()
                    || tx.participants.len() > MAX_PARTICIPANTS
                    || tx.prepared.len() != tx.participants.len()
                    || tx.participants.windows(2).any(|p| p[0].shard >= p[1].shard)
                    || tx
                        .participants
                        .iter()
                        .any(|p| p.intent == [0; 32] || !value.cohort.contains(p))
                    || (tx.decision == Decision::Commit && tx.prepared.iter().any(|p| !p))
                {
                    return Err("invalid terminal prefix transaction");
                }
            }
        }
        Ok(Self {
            authority: value.authority,
            cohort: value.cohort,
            through: value.through,
            segments: value.segments,
        })
    }
}
impl TerminalPrefix {
    pub(crate) fn from_segments(
        authority: [u8; 16],
        cohort: Cohort,
        through: u64,
        segments: Vec<Vec<Terminal>>,
    ) -> Result<Self, Error> {
        Self::try_from(Encoding {
            version: cohort.profile.terminal_prefix_version(),
            authority,
            cohort,
            through,
            segments,
        })
        .map_err(|_| Error::InvalidJournal)
    }
    /// Compare every field of one exact terminal transaction with this prefix.
    /// A matching ordinal alone cannot certify a separately sampled target.
    pub fn matches_transaction(
        &self,
        transaction: &crate::transaction_decisions::Transaction,
    ) -> bool {
        let Some(index) = transaction
            .ordinal
            .checked_sub(1)
            .and_then(|n| usize::try_from(n).ok())
        else {
            return false;
        };
        self.segments
            .get(index / SEGMENT_TRANSACTIONS)
            .and_then(|segment| segment.get(index % SEGMENT_TRANSACTIONS))
            .is_some_and(|record| {
                record.id == transaction.id
                    && record.ordinal == transaction.ordinal
                    && record.participants == transaction.participants
                    && record.prepared == transaction.prepared
                    && Some(record.decision) == transaction.decision
            })
    }
    pub fn authority(&self) -> [u8; 16] {
        self.authority
    }
    pub fn cohort(&self) -> &Cohort {
        &self.cohort
    }
    pub fn version(&self) -> u64 {
        self.through
    }
}
impl Journal {
    /// Project only 1..=version from incrementally maintained transaction state.
    /// Never clones, serializes or replays the external request/Plan archive.
    /// Later transactions do not contribute evidence or projection work.
    pub fn terminal_prefix(&self, version: u64) -> Result<TerminalPrefix, Error> {
        let cohort = self.cohort().ok_or(Error::IncompatibleHistory)?.clone();
        if version > u64::try_from(self.transaction_count()).map_err(|_| Error::InvalidJournal)? {
            return Err(Error::UnknownTransaction);
        }
        let mut segments = Vec::new();
        for first in (1..=version).step_by(SEGMENT_TRANSACTIONS) {
            let mut segment = Vec::new();
            for ordinal in first..=version.min(first + SEGMENT_TRANSACTIONS as u64 - 1) {
                let tx = self.transaction_at(ordinal).ok_or(Error::InvalidJournal)?;
                segment.push(Terminal {
                    id: tx.id,
                    ordinal: tx.ordinal,
                    participants: tx.participants.clone(),
                    prepared: tx.prepared.clone(),
                    decision: tx.decision.ok_or(Error::NotPrepared)?,
                });
            }
            segments.push(segment);
        }
        TerminalPrefix::try_from(Encoding {
            version: cohort.profile.terminal_prefix_version(),
            authority: self.authority(),
            cohort,
            through: version,
            segments,
        })
        .map_err(|_| Error::InvalidJournal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::{Command, Member, MAX_TRANSACTIONS};
    use serde_json::{json, Value};

    fn cohort() -> Cohort {
        Cohort {
            profile: CapacityProfile::LegacyCountV1,
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: vec![Member {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            }],
        }
    }
    fn participant() -> Participant {
        Participant {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
            intent: [3; 32],
        }
    }
    fn journal() -> Journal {
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .apply(Command::BindCohort { cohort: cohort() })
            .unwrap();
        journal
    }
    fn begin(journal: &mut Journal, id: u8) {
        journal
            .apply(Command::Begin {
                id: [id; 16],
                participants: vec![participant()],
            })
            .unwrap();
    }
    fn decide(journal: &mut Journal, id: u8, decision: Decision) {
        if decision == Decision::Commit {
            journal
                .apply(Command::Prepared {
                    id: [id; 16],
                    participant: participant(),
                })
                .unwrap();
        }
        journal
            .apply(Command::Decide {
                id: [id; 16],
                decision,
            })
            .unwrap();
    }
    fn prefix(count: u8) -> TerminalPrefix {
        let mut journal = journal();
        for id in 1..=count {
            begin(&mut journal, id);
            decide(&mut journal, id, Decision::Abort);
        }
        journal.terminal_prefix(u64::from(count)).unwrap()
    }
    fn reject(value: Value) {
        assert!(serde_json::from_value::<TerminalPrefix>(value).is_err());
    }

    #[test]
    fn exact_v1_golden_and_roundtrip() {
        let evidence = prefix(1);
        // Literal bytes deliberately freeze field order, enums and complete identities.
        let golden = concat!(
            "{\"version\":1,\"authority\":[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9],",
            "\"cohort\":{\"coordinator_shard\":9,\"coordinator_epoch\":1,\"members\":[",
            "{\"shard\":7,\"epoch\":1,\"mode\":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]}]},",
            "\"through\":1,\"segments\":[[{\"id\":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],",
            "\"ordinal\":1,\"participants\":[{\"shard\":7,\"epoch\":1,",
            "\"mode\":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7],",
            "\"intent\":[3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3]}],",
            "\"prepared\":[false],\"decision\":\"Abort\"}]]}"
        );
        assert_eq!(serde_json::to_vec(&evidence).unwrap(), golden.as_bytes());
        assert_eq!(
            serde_json::from_str::<TerminalPrefix>(golden).unwrap(),
            evidence
        );
        assert_eq!(evidence.authority(), [9; 16]);
        assert_eq!(evidence.cohort(), &cohort());
        assert_eq!(evidence.version(), 1);
    }

    #[test]
    fn canonical_segments_cover_boundaries_without_padding() {
        for count in [0, 1, 31, 32, 33, 64, 65] {
            let evidence = prefix(count);
            let sizes: Vec<_> = evidence.segments.iter().map(Vec::len).collect();
            let expected: Vec<_> = (0..usize::from(count))
                .step_by(32)
                .map(|start| (usize::from(count) - start).min(32))
                .collect();
            assert_eq!(sizes, expected);
            assert_eq!(
                serde_json::from_slice::<TerminalPrefix>(&serde_json::to_vec(&evidence).unwrap())
                    .unwrap(),
                evidence
            );
        }
        let mut encoded = serde_json::to_value(prefix(33)).unwrap();
        let moved = encoded["segments"][0]
            .as_array_mut()
            .unwrap()
            .pop()
            .unwrap();
        encoded["segments"][1]
            .as_array_mut()
            .unwrap()
            .insert(0, moved);
        reject(encoded); // Same complete ordered records, wrong segment partition.
        let mut encoded = serde_json::to_value(prefix(32)).unwrap();
        encoded["segments"].as_array_mut().unwrap().push(json!([]));
        reject(encoded);
        let mut encoded = serde_json::to_value(prefix(0)).unwrap();
        encoded["segments"] = json!([[]]);
        reject(encoded);
    }

    #[test]
    fn rejects_missing_reordered_duplicate_and_mismatched_records() {
        let original = serde_json::to_value(prefix(3)).unwrap();
        let mut changed = original.clone();
        changed["segments"][0].as_array_mut().unwrap().remove(1);
        reject(changed);
        let mut changed = original.clone();
        changed["segments"][0].as_array_mut().unwrap().swap(0, 1);
        reject(changed);
        let mut changed = original.clone();
        changed["segments"][0][1]["id"] = original["segments"][0][0]["id"].clone();
        reject(changed);
        for through in [0, 2, 4, MAX_TRANSACTIONS as u64 + 1, u64::MAX] {
            let mut changed = original.clone();
            changed["through"] = json!(through);
            reject(changed);
        }
        for ordinal in [0, 2, u64::MAX] {
            let mut changed = original.clone();
            changed["segments"][0][0]["ordinal"] = json!(ordinal);
            reject(changed);
        }
    }

    #[test]
    fn checked_decode_rejects_invalid_identity_preparation_and_cohort() {
        let original = serde_json::to_value(prefix(1)).unwrap();
        let invalid: Vec<(&str, Value)> = vec![
            ("/version", json!(0)),
            ("/version", json!(2)),
            ("/authority", json!(([0; 16]))),
            ("/cohort/members", json!([])),
            ("/cohort/members/0/shard", json!(9)),
            ("/cohort/members/0/mode", json!(([0; 16]))),
            ("/cohort/members/0/epoch", json!(2)),
            ("/cohort/members/0/mode", json!(([8; 16]))),
            ("/segments/0/0/id", json!(([0; 16]))),
            ("/segments/0/0/participants", json!([])),
            ("/segments/0/0/participants/0/shard", json!(8)),
            ("/segments/0/0/participants/0/epoch", json!(2)),
            ("/segments/0/0/participants/0/mode", json!(([8; 16]))),
            ("/segments/0/0/participants/0/intent", json!(([0; 32]))),
            ("/segments/0/0/prepared", json!([])),
            ("/segments/0/0/prepared", json!([false, false])),
            ("/segments/0/0/decision", json!("Commit")),
            ("/segments/0/0/decision", Value::Null),
        ];
        for (path, replacement) in invalid {
            let mut changed = original.clone();
            *changed.pointer_mut(path).unwrap() = replacement;
            assert!(
                serde_json::from_value::<TerminalPrefix>(changed).is_err(),
                "accepted {path}"
            );
        }
        for field in ["version", "authority", "cohort", "through", "segments"] {
            let mut changed = original.clone();
            changed.as_object_mut().unwrap().remove(field);
            reject(changed);
        }
        let mut changed = original.clone();
        changed["cached_valid"] = json!(true);
        reject(changed);
        let mut changed = original.clone();
        changed["segments"][0][0]["cached_valid"] = json!(true);
        reject(changed);
        let mut changed = original;
        let duplicate = changed["segments"][0][0]["participants"][0].clone();
        changed["segments"][0][0]["participants"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        changed["segments"][0][0]["prepared"] = json!([false, false]);
        reject(changed);
    }

    #[test]
    fn canonical_participant_and_cohort_order_and_limits_are_checked() {
        let mut encoded = serde_json::to_value(prefix(1)).unwrap();
        encoded["cohort"]["members"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "shard": 8, "epoch": 1, "mode": [8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8]
            }));
        let mut second = encoded["segments"][0][0]["participants"][0].clone();
        second["shard"] = json!(8);
        second["mode"] = json!(([8; 16]));
        encoded["segments"][0][0]["participants"]
            .as_array_mut()
            .unwrap()
            .push(second);
        encoded["segments"][0][0]["prepared"] = json!([false, true]);
        assert!(serde_json::from_value::<TerminalPrefix>(encoded.clone()).is_ok());
        let mut changed = encoded.clone();
        changed["segments"][0][0]["participants"]
            .as_array_mut()
            .unwrap()
            .swap(0, 1);
        reject(changed);
        let mut changed = encoded.clone();
        changed["cohort"]["members"]
            .as_array_mut()
            .unwrap()
            .swap(0, 1);
        reject(changed);
        let mut changed = encoded.clone();
        changed["cohort"]["members"][1] = changed["cohort"]["members"][0].clone();
        reject(changed);
        let members: Vec<_> = (0..=MAX_PARTICIPANTS)
            .map(|i| {
                json!({
                    "shard": 100 + i, "epoch": 1, "mode": [7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]
                })
            })
            .collect();
        let mut changed = encoded.clone();
        changed["cohort"]["members"] = json!(members);
        reject(changed);
        let participants: Vec<_> = (0..=MAX_PARTICIPANTS)
            .map(|i| {
                let mut participant = encoded["segments"][0][0]["participants"][0].clone();
                participant["shard"] = json!(100 + i);
                participant
            })
            .collect();
        encoded["segments"][0][0]["participants"] = json!(participants);
        encoded["segments"][0][0]["prepared"] = json!(vec![false; MAX_PARTICIPANTS + 1]);
        reject(encoded);
    }

    #[test]
    fn valid_same_shaped_changes_never_compare_equal() {
        let evidence = prefix(1);
        let original = serde_json::to_value(&evidence).unwrap();
        for (path, replacement) in [
            ("/authority", json!(([8; 16]))),
            ("/cohort/coordinator_shard", json!(8)),
            ("/cohort/coordinator_epoch", json!(2)),
            ("/segments/0/0/id", json!(([2; 16]))),
            ("/segments/0/0/participants/0/intent", json!(([4; 32]))),
            ("/segments/0/0/prepared/0", json!(true)),
        ] {
            let mut changed = original.clone();
            *changed.pointer_mut(path).unwrap() = replacement;
            let changed = serde_json::from_value::<TerminalPrefix>(changed).unwrap();
            assert_ne!(evidence, changed, "ignored {path}");
        }
        let mut changed = original.clone();
        changed["segments"][0][0]["prepared"] = json!([true]);
        let prepared_abort = serde_json::from_value::<TerminalPrefix>(changed.clone()).unwrap();
        changed["segments"][0][0]["decision"] = json!("Commit");
        assert_ne!(
            prepared_abort,
            serde_json::from_value::<TerminalPrefix>(changed).unwrap()
        );
        for field in ["epoch", "mode"] {
            let mut changed = original.clone();
            let replacement = if field == "epoch" {
                json!(2)
            } else {
                json!(([8; 16]))
            };
            changed["cohort"]["members"][0][field] = replacement.clone();
            changed["segments"][0][0]["participants"][0][field] = replacement;
            assert_ne!(
                evidence,
                serde_json::from_value::<TerminalPrefix>(changed).unwrap()
            );
        }
    }

    #[test]
    fn empty_future_unbound_and_unresolved_cuts_are_explicit() {
        let unbound = Journal::new([9; 16]).unwrap();
        assert_eq!(unbound.terminal_prefix(0), Err(Error::IncompatibleHistory));
        let mut journal = journal();
        assert_eq!(journal.terminal_prefix(0).unwrap(), prefix(0));
        assert_eq!(journal.terminal_prefix(1), Err(Error::UnknownTransaction));
        begin(&mut journal, 1);
        begin(&mut journal, 2);
        decide(&mut journal, 2, Decision::Abort);
        assert_eq!(journal.terminal_prefix(1), Err(Error::NotPrepared));
        assert_eq!(journal.terminal_prefix(2), Err(Error::NotPrepared));
        assert_eq!(
            journal.terminal_prefix(u64::MAX),
            Err(Error::UnknownTransaction)
        );
        decide(&mut journal, 1, Decision::Commit);
        assert!(journal.terminal_prefix(2).is_ok());
    }

    #[test]
    fn historical_cut_is_immutable_across_later_prepares_and_decisions() {
        let mut journal = journal();
        begin(&mut journal, 1);
        decide(&mut journal, 1, Decision::Commit);
        let first = journal.terminal_prefix(1).unwrap();
        let bytes = serde_json::to_vec(&first).unwrap();
        begin(&mut journal, 2);
        assert_eq!(journal.terminal_prefix(1).unwrap(), first);
        journal
            .apply(Command::Prepared {
                id: [2; 16],
                participant: participant(),
            })
            .unwrap();
        assert_eq!(journal.terminal_prefix(1).unwrap(), first);
        journal
            .apply(Command::Decide {
                id: [2; 16],
                decision: Decision::Abort,
            })
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&journal.terminal_prefix(1).unwrap()).unwrap(),
            bytes
        );
        let reopened = Journal::decode(&journal.encode().unwrap()).unwrap();
        assert_eq!(reopened.terminal_prefix(1).unwrap(), first);
        assert_ne!(reopened.terminal_prefix(2).unwrap(), first);
    }

    #[test]
    fn journal_versions_replay_and_request_plan_archives_do_not_enter_evidence() {
        let mut legacy = Journal::new([9; 16]).unwrap();
        begin(&mut legacy, 1);
        decide(&mut legacy, 1, Decision::Abort);
        assert_eq!(
            serde_json::from_slice::<Value>(&legacy.encode().unwrap()).unwrap()["version"],
            1
        );
        assert_eq!(
            Journal::decode(&legacy.encode().unwrap())
                .unwrap()
                .terminal_prefix(1),
            Err(Error::IncompatibleHistory)
        );
        let mut ordinary = journal();
        begin(&mut ordinary, 1);
        decide(&mut ordinary, 1, Decision::Abort);
        assert_eq!(
            serde_json::from_slice::<Value>(&ordinary.encode().unwrap()).unwrap()["version"],
            2
        );
        let expected = ordinary.terminal_prefix(1).unwrap();
        assert_eq!(
            Journal::decode(&ordinary.encode().unwrap())
                .unwrap()
                .terminal_prefix(1)
                .unwrap(),
            expected
        );
        for value in ["first archived plan", "different archived plan"] {
            let mut archived = journal();
            let payload = crate::request_plans::Plan {
                version: 1,
                id: [1; 16],
                authority: crate::participant_writes::Authority {
                    shard: 9,
                    mode: [9; 16],
                },
                cohort: cohort(),
                validation_version: 0,
                parts: vec![crate::request_plans::Part {
                    descriptor: participant(),
                    request: crate::D1WriteRequest {
                        shard: 7,
                        epoch: 1,
                        request_id: [1; 16],
                        expected_sequence: Some(0),
                        operations: vec![crate::WriteOp::put("x", value)],
                    },
                }],
            }
            .encode()
            .unwrap();
            archived
                .apply(Command::AdmitRequest {
                    id: [5; 16],
                    request: value.as_bytes().to_vec(),
                    payload,
                    transaction_id: [1; 16],
                    participants: vec![participant()],
                })
                .unwrap();
            decide(&mut archived, 1, Decision::Abort);
            let bytes = archived.encode().unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap()["version"],
                3
            );
            assert!(archived.request([5; 16]).is_some());
            assert_eq!(archived.terminal_prefix(1).unwrap(), expected);
            assert_eq!(
                Journal::decode(&bytes).unwrap().terminal_prefix(1).unwrap(),
                expected
            );
        }
    }
}
