//! Bounded point recovery evidence and strict readers for retired request indexes.
//! Bound coordinators write no duplicate index; original raw history is retained.
use crate::d1::D1State;
use crate::participant_writes::Authority;
use crate::transaction_decisions::{
    Cohort, Decision, Error, Journal, RequestRecord, Transaction, MAX_REQUEST_BYTES,
    MAX_REQUEST_PAYLOAD_BYTES,
};
use crate::{D1Error, NodeId};
use openraft::LogId;
use rocksdb::{Direction, IteratorMode, Snapshot, WriteBatch, DB};
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
            || self.resolved_prefix > self.cohort.profile.max_transactions() as u64
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
                || tx.ordinal > self.cohort.profile.max_transactions() as u64
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

// Profile format and coordinator authority precede the opaque ID. The manifest
// compares the full Cohort, and each row must exactly match the replayed original
// Plan/record whose Cohort is validated by Journal admission. A key alone is never
// evidence of identity or absence.
pub(crate) fn key(state: &D1State, id: [u8; 16]) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(
        &state
            .coordinator
            .as_ref()
            .unwrap()
            .capacity_profile()
            .storage_format()
            .to_be_bytes(),
    );
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
        let row: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(
            matches!(row.version, 1 | 2) && serde_json::to_vec(&row)? == bytes,
            "noncanonical request index row"
        );
        Ok(row)
    }
    fn of(record: &RequestRecord, journal: &Journal) -> anyhow::Result<Self> {
        let tx = journal
            .transaction(record.transaction_id)
            .ok_or_else(|| anyhow::anyhow!("request transaction missing"))?;
        Ok(Self {
            version: journal.capacity_profile().storage_format(),
            record: record.clone(),
            ordinal: tx.ordinal,
        })
    }
    fn matches(&self, journal: &Journal) -> bool {
        self.version == journal.capacity_profile().storage_format()
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
            version: journal.capacity_profile().storage_format(),
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

/// Validate the complete actual legacy index against strict-replayed authority.
/// Disk-15 and noncoordinator formats must have no index or manifest at all.
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
        let manifest = manifest.ok_or_else(|| anyhow::anyhow!("request manifest missing"))?;
        let actual: Manifest = serde_json::from_slice(&manifest)?;
        anyhow::ensure!(
            actual == expected && serde_json::to_vec(&actual)? == manifest,
            "request manifest disagrees with state"
        );
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

/// Disk-15 has no duplicate request namespace, including malformed/foreign rows.
/// Use the same snapshot as the raw reader so residue cannot cross read cuts.
pub(crate) fn reject_retired_snapshot(snapshot: &Snapshot<'_>) -> anyhow::Result<()> {
    // Inspect keys only: malformed retired values have no useful size bound and
    // must not be copied into memory merely to discover that they are forbidden.
    let mut rows = snapshot.raw_iterator();
    rows.seek(MANIFEST_KEY);
    rows.status()?;
    anyhow::ensure!(
        rows.key() != Some(MANIFEST_KEY),
        "retired request manifest is present"
    );
    rows.seek(PREFIX);
    rows.status()?;
    anyhow::ensure!(
        !rows.key().is_some_and(|key| key.starts_with(PREFIX)),
        "retired request index is present"
    );
    Ok(())
}

pub(crate) fn reject_retired_namespace(db: &DB) -> anyhow::Result<()> {
    reject_retired_snapshot(&db.snapshot())
}

/// The only production writer is the historical disk-8 empty manifest for a
/// genuinely unbound coordinator. Bound/member states retire the whole range.
/// Ordinary migration must validate the old index before calling this function;
/// only an explicit validated snapshot install may repair corrupt legacy rows.
/// Set `clear_existing` only for such replacement or a validated old index;
/// ordinary unindexed D1/member writes must neither add tombstones nor repair
/// unrelated orphan rows.
pub(crate) fn stage_unbound_or_retire(
    batch: &mut WriteBatch,
    next: Option<&D1State>,
    applied: Option<LogId<NodeId>>,
    clear_existing: bool,
) -> anyhow::Result<Option<Vec<u8>>> {
    let unbound = next.filter(|state| {
        state
            .coordinator
            .as_ref()
            .is_some_and(|journal| journal.cohort().is_none())
    });
    let manifest = unbound
        .map(|state| -> anyhow::Result<Vec<u8>> {
            let journal = state.coordinator.as_ref().unwrap();
            anyhow::ensure!(
                journal.cohort().is_none()
                    && journal.requests().is_empty()
                    && journal.capacity_profile()
                        == crate::capacity_policy::CapacityProfile::LegacyCountV1,
                "empty request manifest requires an unbound request-free coordinator"
            );
            serde_json::to_vec(&Manifest {
                version: 1,
                authority: Authority {
                    shard: state.shard,
                    mode: journal.authority(),
                },
                epoch: state.epoch,
                cohort: None,
                requests: 0,
                encoded_bytes: 0,
                applied,
            })
            .map_err(Into::into)
        })
        .transpose()?;
    if clear_existing {
        batch.delete_range(PREFIX, PREFIX_END);
        batch.delete(MANIFEST_KEY);
    }
    if let Some(manifest) = &manifest {
        batch.put(MANIFEST_KEY, manifest);
    }
    Ok(manifest)
}

/// Actual published legacy fixture writer, never a production migration target.
#[cfg(test)]
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

    fn byte_state() -> D1State {
        use crate::transaction_decisions::{
            CapacityActivation, CapacityProfile, Command, MemberCapacityGrant,
        };
        let legacy =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        let mut cohort = legacy.cohort().unwrap().clone();
        cohort.profile = CapacityProfile::ByteBound16MiBV1;
        let mut journal = Journal::new(legacy.authority()).unwrap();
        journal
            .replay_event(Command::BindCohort {
                cohort: cohort.clone(),
            })
            .unwrap();
        let grants = cohort
            .members
            .iter()
            .map(|member| MemberCapacityGrant {
                member: member.clone(),
                activation: CapacityActivation {
                    nonce: [3; 32],
                    log_id: LogId::new(openraft::CommittedLeaderId::new(1, 0), 1),
                },
            })
            .collect();
        journal
            .replay_event(Command::ConfirmCohortCapacity {
                cohort: cohort.clone(),
                grants,
            })
            .unwrap();
        for event in &legacy.events()[1..] {
            let mut event = event.clone();
            if let Command::AdmitRequest { payload, .. } = &mut event {
                let mut plan = crate::request_plans::Plan::decode(payload).unwrap();
                plan.cohort = cohort.clone();
                *payload = plan.encode().unwrap();
            }
            journal.replay_event(event).unwrap();
        }
        let mut state = D1State::new(cohort.coordinator_shard, cohort.coordinator_epoch);
        state.coordinator = Some(journal);
        state
    }

    #[test]
    fn production_staging_retires_bound_index_and_preserves_only_unbound_empty_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let bound = byte_state();
        let mut batch = WriteBatch::default();
        stage(&mut batch, None, Some(&bound), None, false, true).unwrap();
        db.write(batch).unwrap();
        assert!(reject_retired_namespace(&db).is_err());
        let old = db.snapshot();
        let mut batch = WriteBatch::default();
        assert!(
            stage_unbound_or_retire(&mut batch, Some(&bound), None, true)
                .unwrap()
                .is_none()
        );
        db.write(batch).unwrap();
        reject_retired_namespace(&db).unwrap();
        assert!(reject_retired_snapshot(&old).is_err());
        let mut unbound = D1State::new(7, 3);
        let mut journal = Journal::new([1; 16]).unwrap();
        journal
            .replay_event(crate::transaction_decisions::Command::Begin {
                id: [2; 16],
                participants: vec![crate::transaction_decisions::Participant {
                    shard: 9,
                    epoch: 3,
                    mode: [3; 16],
                    intent: [4; 32],
                }],
            })
            .unwrap();
        unbound.coordinator = Some(journal);
        let mut batch = WriteBatch::default();
        let bytes = stage_unbound_or_retire(&mut batch, Some(&unbound), None, true)
            .unwrap()
            .unwrap();
        db.write(batch).unwrap();
        validate(&db, Some(&unbound), None, true).unwrap();
        let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.cohort, None);
        assert_eq!(manifest.requests, 0);
        assert_eq!(manifest.encoded_bytes, 0);
        assert!(reject_retired_namespace(&db).is_err());
        let mut batch = WriteBatch::default();
        assert!(stage_unbound_or_retire(&mut batch, None, None, true)
            .unwrap()
            .is_none());
        db.write(batch).unwrap();
        reject_retired_namespace(&db).unwrap();
    }

    #[test]
    fn ordinary_unindexed_staging_writes_nothing_and_does_not_repair_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        db.put(b"sm/request/orphan", b"bad").unwrap();
        let sequence = db.latest_sequence_number();
        for state in [None, Some(D1State::new(7, 3)), Some(byte_state())] {
            let mut batch = WriteBatch::default();
            assert!(
                stage_unbound_or_retire(&mut batch, state.as_ref(), None, false)
                    .unwrap()
                    .is_none()
            );
            db.write(batch).unwrap();
            assert_eq!(db.latest_sequence_number(), sequence);
            assert_eq!(
                db.get(b"sm/request/orphan").unwrap().as_deref(),
                Some(b"bad".as_slice())
            );
        }
    }

    #[test]
    fn retired_namespace_checks_same_snapshot_and_every_reserved_key_shape() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let empty = db.snapshot();
        for key in [PREFIX, b"sm/request/foreign".as_slice(), MANIFEST_KEY] {
            db.put(key, b"invalid").unwrap();
            reject_retired_snapshot(&empty).unwrap();
            assert!(reject_retired_namespace(&db).is_err());
            db.delete(key).unwrap();
            reject_retired_namespace(&db).unwrap();
        }
        // Neighboring namespaces are not retired request rows.
        db.put(b"sm/request0", b"neighbor").unwrap();
        reject_retired_namespace(&db).unwrap();
    }

    #[test]
    fn byte_profile_request_index_reopens_and_binds_original_plan_and_namespace() {
        let state = byte_state();
        let journal = state.coordinator.as_ref().unwrap();
        let record = &journal.requests()[0];
        let dir = tempfile::tempdir().unwrap();
        let manifest = {
            let db = DB::open_default(dir.path()).unwrap();
            let mut batch = WriteBatch::default();
            let manifest = stage(&mut batch, None, Some(&state), None, false, true)
                .unwrap()
                .unwrap();
            db.write(batch).unwrap();
            assert_eq!(
                key(&state, record.id),
                [
                    PREFIX,
                    &[0, 0, 0, 2],
                    &state.shard.to_be_bytes(),
                    &state.epoch.to_be_bytes(),
                    &journal.authority(),
                    &record.id,
                ]
                .concat()
            );
            assert_eq!(
                serde_json::from_slice::<Manifest>(&manifest)
                    .unwrap()
                    .version,
                2
            );
            manifest
        };
        let db = DB::open_default(dir.path()).unwrap();
        validate(&db, Some(&state), None, true).unwrap();
        let found = lookup(&state, record.id, Some((&db, &manifest))).unwrap();
        assert_eq!(
            found.record.as_ref().unwrap().record.payload,
            record.payload
        );
        let mut wrong_profile = found.clone();
        wrong_profile.cohort.profile = crate::capacity_policy::CapacityProfile::LegacyCountV1;
        assert!(wrong_profile.validate().is_err());
        let mut absent = found.clone();
        absent.record = None;
        absent.resolved_prefix = 1025;
        absent.validate().unwrap();
        absent.cohort.profile = crate::capacity_policy::CapacityProfile::LegacyCountV1;
        assert!(absent.validate().is_err());
        for fault in 0..5 {
            let mut batch = WriteBatch::default();
            stage(&mut batch, None, Some(&state), None, false, true).unwrap();
            db.write(batch).unwrap();
            let mut row = Row::of(record, journal).unwrap();
            match fault {
                0 => {
                    db.delete(key(&state, record.id)).unwrap();
                }
                1 => {
                    row.version = 1;
                    db.put(key(&state, record.id), serde_json::to_vec(&row).unwrap())
                        .unwrap();
                }
                2 => {
                    let mut foreign = key(&state, record.id);
                    foreign[PREFIX.len() + 3] = 1;
                    db.put(foreign, serde_json::to_vec(&row).unwrap()).unwrap();
                }
                3 => {
                    let mut bad: Manifest = serde_json::from_slice(&manifest).unwrap();
                    bad.cohort.as_mut().unwrap().profile =
                        crate::capacity_policy::CapacityProfile::LegacyCountV1;
                    db.put(MANIFEST_KEY, serde_json::to_vec(&bad).unwrap())
                        .unwrap();
                }
                4 => {
                    let mut plan = crate::request_plans::Plan::decode(&row.record.payload).unwrap();
                    plan.cohort.profile = crate::capacity_policy::CapacityProfile::LegacyCountV1;
                    row.record.payload = plan.encode().unwrap();
                    db.put(key(&state, record.id), serde_json::to_vec(&row).unwrap())
                        .unwrap();
                }
                _ => unreachable!(),
            }
            assert!(
                validate(&db, Some(&state), None, true).is_err(),
                "fault {fault}"
            );
            if fault != 2 {
                assert!(
                    lookup(&state, record.id, Some((&db, &manifest))).is_err(),
                    "fault {fault}"
                );
            }
        }
    }

    #[test]
    fn byte_profile_maximum_width_request_row_fits_unchanged_event_bound() {
        let row = Row {
            version: 2,
            record: RequestRecord {
                id: [255; 16],
                request: vec![255; MAX_REQUEST_BYTES],
                payload: vec![255; MAX_REQUEST_PAYLOAD_BYTES],
                transaction_id: [255; 16],
            },
            ordinal: u64::MAX,
        };
        // A schema width proof, deliberately wider than any legal original Plan.
        let bytes = serde_json::to_vec(&row).unwrap();
        assert!(bytes.len() < crate::transaction_decisions::MAX_REQUEST_EVENT_BYTES);
        assert_eq!(Row::decode(&bytes).unwrap(), row);
    }

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
        let byte_golden = golden.replacen("\"version\":1", "\"version\":2", 1);
        let byte_row = Row { version: 2, ..row };
        assert_eq!(serde_json::to_string(&byte_row).unwrap(), byte_golden);
        assert_eq!(Row::decode(byte_golden.as_bytes()).unwrap(), byte_row);
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
