//! Exact derived coordinator request index and bounded point recovery evidence.
//! The complete Journal remains authoritative; no identity/history is evicted.
use crate::d1::D1State;
use crate::participant_writes::Authority;
use crate::transaction_decisions::{
    Cohort, Decision, Error, Journal, RequestRecord, Transaction, MAX_REQUEST_BYTES,
    MAX_REQUEST_PAYLOAD_BYTES, MAX_TRANSACTIONS,
};
use crate::{D1Error, NodeId};
use openraft::LogId;
use rocksdb::{Direction, IteratorMode, WriteBatch, DB};
use serde::{Deserialize, Serialize};

/// One original binding and its transaction state from the same leader read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveredRequest {
    pub record: RequestRecord,
    pub transaction: Transaction,
}

/// Bounded point evidence, not a global read certificate or an Abort proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestLookup {
    pub authority: Authority,
    pub cohort: Cohort,
    pub resolved_prefix: u64,
    pub id: [u8; 16],
    pub record: Option<RecoveredRequest>,
}
impl RequestLookup {
    pub fn validate(&self) -> Result<(), Error> {
        if self.id == [0; 16]
            || self.authority.mode == [0; 16]
            || self.authority.shard != self.cohort.coordinator_shard
            || self.cohort.validate().is_err()
            || self.resolved_prefix > MAX_TRANSACTIONS as u64
        {
            return Err(Error::InvalidIdentity);
        }
        if let Some(found) = &self.record {
            let r = &found.record;
            let tx = &found.transaction;
            let plan = crate::request_plans::Plan::decode(&r.payload)?;
            if r.id != self.id
                || r.request.is_empty()
                || r.request.len() > MAX_REQUEST_BYTES
                || r.payload.len() > MAX_REQUEST_PAYLOAD_BYTES
                || r.transaction_id != tx.id
                || plan.id != tx.id
                || plan.authority != self.authority
                || plan.cohort != self.cohort
                || plan.validation_version > self.resolved_prefix
                || plan.validation_version >= tx.ordinal
                || tx.ordinal == 0
                || tx.ordinal > MAX_TRANSACTIONS as u64
                || tx.prepared.len() != tx.participants.len()
                || (tx.ordinal <= self.resolved_prefix && tx.decision.is_none())
                || (tx.decision == Some(Decision::Commit) && !tx.prepared.iter().all(|p| *p))
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

pub(crate) const PREFIX: &[u8] = b"sm/request/";
pub(crate) const PREFIX_END: &[u8] = b"sm/request0";
pub(crate) const MANIFEST_KEY: &[u8] = b"sm/request-manifest";

// Version and the complete coordinator authority domain precede the opaque ID.
pub(crate) fn key(state: &D1State, id: [u8; 16]) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(&1u32.to_be_bytes());
    key.extend_from_slice(&state.shard.to_be_bytes());
    key.extend_from_slice(&state.epoch.to_be_bytes());
    key.extend_from_slice(&state.coordinator.as_ref().unwrap().authority());
    key.extend_from_slice(&id);
    key
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Row {
    version: u32,
    record: RequestRecord,
    ordinal: u64,
}
impl Row {
    fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() <= crate::transaction_decisions::MAX_REQUEST_EVENT_BYTES,
            "request index row exceeds encoded limit"
        );
        Ok(serde_json::from_slice(bytes)?)
    }
    fn of(record: &RequestRecord, journal: &Journal) -> anyhow::Result<Self> {
        let tx = journal
            .transaction(record.transaction_id)
            .ok_or_else(|| anyhow::anyhow!("request transaction missing"))?;
        Ok(Self {
            version: 1,
            record: record.clone(),
            ordinal: tx.ordinal,
        })
    }
    fn matches(&self, journal: &Journal) -> bool {
        self.version == 1
            && journal.request(self.record.id) == Some(&self.record)
            && journal
                .transaction(self.record.transaction_id)
                .is_some_and(|tx| tx.ordinal == self.ordinal)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    authority: Authority,
    epoch: u64,
    cohort: Option<Cohort>,
    requests: u64,
    encoded_bytes: u64,
    applied: Option<LogId<NodeId>>,
}
impl Manifest {
    fn of(state: &D1State, applied: Option<LogId<NodeId>>) -> anyhow::Result<Self> {
        let journal = state.coordinator.as_ref().unwrap();
        let mut encoded_bytes = 0u64;
        for record in journal.requests() {
            encoded_bytes += (key(state, record.id).len()
                + serde_json::to_vec(&Row::of(record, journal)?)?.len())
                as u64;
        }
        Ok(Self {
            version: 1,
            authority: Authority {
                shard: state.shard,
                mode: journal.authority(),
            },
            epoch: state.epoch,
            cohort: journal.cohort().cloned(),
            requests: journal.requests().len() as u64,
            encoded_bytes,
            applied,
        })
    }
}

/// Reopen validates the entire derived range against strict-replayed authority.
/// Legacy/noncoordinator formats must have no index or manifest at all.
pub(crate) fn validate(
    db: &DB,
    state: Option<&D1State>,
    applied: Option<LogId<NodeId>>,
    indexed: bool,
) -> anyhow::Result<()> {
    let manifest = db.get(MANIFEST_KEY)?;
    anyhow::ensure!(
        manifest
            .as_ref()
            .is_none_or(|bytes| bytes.len() <= 64 * 1024),
        "request manifest exceeds encoded limit"
    );
    let mut count = 0usize;
    let mut bytes = 0u64;
    for item in db.iterator(IteratorMode::From(PREFIX, Direction::Forward)) {
        let (stored_key, value) = item?;
        if !stored_key.starts_with(PREFIX) {
            break;
        }
        anyhow::ensure!(indexed, "orphan or mixed-format request index");
        let state = state.ok_or_else(|| anyhow::anyhow!("orphan request index"))?;
        let journal = state.coordinator.as_ref().unwrap();
        count += 1;
        anyhow::ensure!(count <= journal.requests().len(), "extra request index row");
        let row = Row::decode(&value)?;
        anyhow::ensure!(
            stored_key.as_ref() == key(state, row.record.id) && row.matches(journal),
            "request index disagrees with journal"
        );
        bytes += (stored_key.len() + value.len()) as u64;
    }
    if indexed {
        let state = state.ok_or_else(|| anyhow::anyhow!("indexed state missing"))?;
        let expected = Manifest::of(state, applied)?;
        let actual: Manifest = serde_json::from_slice(
            &manifest.ok_or_else(|| anyhow::anyhow!("request manifest missing"))?,
        )?;
        anyhow::ensure!(actual == expected, "request manifest disagrees with state");
        anyhow::ensure!(
            count as u64 == expected.requests && bytes == expected.encoded_bytes,
            "request index incomplete or noncanonical"
        );
    } else {
        anyhow::ensure!(
            manifest.is_none(),
            "orphan or mixed-format request manifest"
        );
    }
    Ok(())
}

/// Append newly admitted rows only. The Journal remains the bounded source;
/// counters are recomputed exactly rather than introducing another authority.
pub(crate) fn stage(
    batch: &mut WriteBatch,
    before: Option<&D1State>,
    next: Option<&D1State>,
    applied: Option<LogId<NodeId>>,
    indexed: bool,
    replace: bool,
) -> anyhow::Result<Option<Vec<u8>>> {
    if replace {
        batch.delete_range(PREFIX, PREFIX_END);
        batch.delete(MANIFEST_KEY);
    }
    let Some(state) = next.filter(|s| s.coordinator.is_some()) else {
        return Ok(None);
    };
    let journal = state.coordinator.as_ref().unwrap();
    let skip = if indexed && !replace {
        before
            .and_then(|s| s.coordinator.as_ref())
            .map_or(0, |j| j.requests().len())
    } else {
        0
    };
    for record in journal.requests().iter().skip(skip) {
        batch.put(
            key(state, record.id),
            serde_json::to_vec(&Row::of(record, journal)?)?,
        );
    }
    let manifest = serde_json::to_vec(&Manifest::of(state, applied)?)?;
    batch.put(MANIFEST_KEY, &manifest);
    Ok(Some(manifest))
}

/// Called under the same read lock as transaction state. A missing indexed row
/// can never turn a known request into absence or trigger a legacy fallback.
pub(crate) fn lookup(
    state: &D1State,
    id: [u8; 16],
    index: Option<(&DB, &[u8])>,
) -> Result<RequestLookup, D1Error> {
    let journal = state
        .coordinator
        .as_ref()
        .ok_or(D1Error::CoordinatorNotEnabled)?;
    let expected = journal.request(id);
    let record = if let Some((db, manifest)) = index {
        if db
            .get(MANIFEST_KEY)
            .map_err(|_| D1Error::RequestIndexUnavailable)?
            .as_deref()
            != Some(manifest)
        {
            return Err(D1Error::RequestIndexHistory);
        }
        let stored = db
            .get(key(state, id))
            .map_err(|_| D1Error::RequestIndexUnavailable)?;
        match (expected, stored) {
            (None, None) => None,
            (Some(_), Some(bytes)) => {
                let row = Row::decode(&bytes).map_err(|_| D1Error::RequestIndexHistory)?;
                if row.record.id != id || !row.matches(journal) {
                    return Err(D1Error::RequestIndexHistory);
                }
                Some(row.record)
            }
            _ => return Err(D1Error::RequestIndexHistory),
        }
    } else {
        // Explicit legacy-v5 or memory-only mode, never indexed corruption.
        expected.cloned()
    };
    let record = record
        .map(|record| {
            let transaction = journal
                .transaction(record.transaction_id)
                .ok_or(D1Error::RequestIndexHistory)?
                .clone();
            Ok(RecoveredRequest {
                record,
                transaction,
            })
        })
        .transpose()?;
    let result = RequestLookup {
        authority: Authority {
            shard: state.shard,
            mode: journal.authority(),
        },
        cohort: journal
            .cohort()
            .cloned()
            .ok_or(D1Error::VisibilityHistory)?,
        resolved_prefix: journal.resolved_prefix(),
        id,
        record,
    };
    result
        .validate()
        .map_err(|_| D1Error::RequestIndexHistory)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_key_row_and_manifest_have_versioned_golden_encodings() {
        let mut state = D1State::new(7, 3);
        state.coordinator = Some(Journal::new([1; 16]).unwrap());
        assert_eq!(
            key(&state, [2; 16]),
            [
                b"sm/request/".as_slice(),
                &[0, 0, 0, 1],
                &7u64.to_be_bytes(),
                &3u64.to_be_bytes(),
                &[1; 16],
                &[2; 16]
            ]
            .concat()
        );
        let row = Row {
            version: 1,
            record: RequestRecord {
                id: [1; 16],
                request: vec![0, 255],
                payload: vec![3],
                transaction_id: [2; 16],
            },
            ordinal: 7,
        };
        let golden = r#"{"version":1,"record":{"id":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],"request":[0,255],"payload":[3],"transaction_id":[2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2]},"ordinal":7}"#;
        assert_eq!(serde_json::to_string(&row).unwrap(), golden);
        assert_eq!(serde_json::from_str::<Row>(golden).unwrap(), row);
        let manifest = Manifest::of(&state, None).unwrap();
        let golden = r#"{"version":1,"authority":{"shard":7,"mode":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]},"epoch":3,"cohort":null,"requests":0,"encoded_bytes":0,"applied":null}"#;
        assert_eq!(serde_json::to_string(&manifest).unwrap(), golden);
        assert_eq!(serde_json::from_str::<Manifest>(golden).unwrap(), manifest);
        assert!(serde_json::from_str::<Manifest>(
            &golden.replace("\"requests\":0", "\"unknown\":0")
        )
        .is_err());
    }
}
