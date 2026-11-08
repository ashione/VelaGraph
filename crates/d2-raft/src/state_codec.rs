//! Private live-state disk codecs. Complete logical snapshots remain unchanged.
//!
//! Bound-cohort headers contain metadata and one ledger descriptor, never a
//! serialized Journal. Raw events must be replayed before constructing D1State.
use super::event_ledger::{self, Descriptor, Role};
use super::{D1_KEY, MAX_DEDUP_REQUESTS};
use crate::d1::D1State;
use crate::{D1ManagedMode, NodeId};
use openraft::LogId;
use rocksdb::DB;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::io::{self, Write};

// These published legacy headers remain readable, and still describe ordinary
// non-cohort modes. Keep their exact field order and optional-field semantics.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct D1MetadataV2 {
    pub(super) version: u32,
    pub(super) shard: u64,
    pub(super) epoch: u64,
    pub(super) sequence: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct D1MetadataV3 {
    pub(super) version: u32,
    pub(super) shard: u64,
    pub(super) epoch: u64,
    pub(super) sequence: u64,
    pub(super) dedup_bytes_limit: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct D1MetadataV4 {
    pub(super) version: u32,
    pub(super) shard: u64,
    pub(super) epoch: u64,
    pub(super) sequence: u64,
    pub(super) dedup_bytes_limit: Option<usize>,
    pub(super) managed: D1ManagedMode,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyMetadata<S> {
    version: u32,
    state: S,
}

// Generic field types let the same explicit schema own its decoded fields and
// borrow them when writing. There is no full-state clone or JSON-tree surgery.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorMetadata<M, L> {
    version: u32,
    shard: u64,
    epoch: u64,
    sequence: u64,
    #[serde(deserialize_with = "required_option")]
    dedup_bytes_limit: Option<usize>,
    managed: M,
    ledger: L,
}
type D1MetadataV11 = CoordinatorMetadata<D1ManagedMode, Descriptor>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VisibilityMetadata<C> {
    baseline_sequence: u64,
    epoch: u64,
    applied: u64,
    commits: C,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberMetadata<M, P, C, L> {
    version: u32,
    shard: u64,
    epoch: u64,
    sequence: u64,
    #[serde(deserialize_with = "required_option")]
    dedup_bytes_limit: Option<usize>,
    managed: M,
    participant: P,
    visibility: VisibilityMetadata<C>,
    ledger: L,
}
type D1MetadataV12 = MemberMetadata<
    D1ManagedMode,
    crate::participant_writes::State,
    Vec<crate::versioned_reads::CommitVersion>,
    Descriptor,
>;

// An Option normally accepts a missing JSON field. New headers require the
// explicit null-or-capacity field, so missing and duplicate fields both fail.
fn required_option<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::deserialize(deserializer)
}

// Reused participant fields have legacy Option decoding semantics. Canonical
// comparison also rejects omitted nested fields without changing their logical
// snapshot codec. Compare as a stream: a full 16 MiB intent can expand to a much
// larger JSON header and must not incur a second encoded allocation or row cap.
fn decode_new<T: DeserializeOwned + Serialize>(bytes: &[u8]) -> anyhow::Result<T> {
    struct Compare<'a>(&'a [u8]);
    impl Write for Compare<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if !self.0.starts_with(bytes) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "noncanonical D1 metadata",
                ));
            }
            self.0 = &self.0[bytes.len()..];
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let metadata = serde_json::from_slice(bytes)?;
    let mut compare = Compare(bytes);
    serde_json::to_writer(&mut compare, &metadata)?;
    anyhow::ensure!(compare.0.is_empty(), "noncanonical D1 metadata");
    Ok(metadata)
}

pub(super) struct LoadedState {
    pub(super) state: D1State,
    pub(super) legacy_dedup: bool,
    pub(super) request_indexed: bool,
    pub(super) decision_indexed: bool,
    pub(super) ledger: Option<event_ledger::Loaded>,
    pub(super) metadata_bytes: Vec<u8>,
}

fn validate_common(
    shard: u64,
    epoch: u64,
    sequence: u64,
    dedup_bytes_limit: Option<usize>,
    managed: &D1ManagedMode,
    descriptor: &Descriptor,
    role: Role,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        sequence <= MAX_DEDUP_REQUESTS as u64,
        "D1 dedup capacity exceeded"
    );
    anyhow::ensure!(
        matches!(dedup_bytes_limit, None | Some(crate::EXTENDED_DEDUP_BYTES)),
        "unsupported D1 capacity"
    );
    anyhow::ensure!(
        managed.mode_id != [0; 16] && managed.activated_sequence <= sequence,
        "invalid managed mode"
    );
    descriptor.validate_identity(shard, epoch, managed.mode_id, role)
}

/// Decode sm/d1 once, then replay its raw ledger before rehydrating logical
/// state. The caller loads incremental retry records and performs complete D1
/// validation afterwards, before validating any derived indexes.
pub(super) fn load(db: &DB, applied: Option<LogId<NodeId>>) -> anyhow::Result<Option<LoadedState>> {
    let Some(bytes) = db.get(D1_KEY)? else {
        event_ledger::reject_orphans(db)?;
        return Ok(None);
    };
    // Inspect only the discriminator, not a second materialized JSON tree.
    #[derive(Deserialize)]
    struct Version {
        version: u32,
    }
    let version = serde_json::from_slice::<Version>(&bytes)?.version;
    let mut legacy_dedup = false;
    let mut request_indexed = false;
    let mut decision_indexed = false;
    let mut ledger = None;
    if !matches!(version, 11 | 12) {
        event_ledger::reject_orphans(db)?;
    }
    let state = match version {
        1 => {
            legacy_dedup = true;
            serde_json::from_slice::<D1State>(&bytes)?
        }
        2 => {
            let meta: D1MetadataV2 = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                meta.sequence <= MAX_DEDUP_REQUESTS as u64,
                "D1 dedup capacity exceeded"
            );
            let mut state = D1State::new(meta.shard, meta.epoch);
            state.sequence = meta.sequence;
            state
        }
        3 => {
            let meta: D1MetadataV3 = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                meta.dedup_bytes_limit == crate::EXTENDED_DEDUP_BYTES,
                "unsupported D1 capacity"
            );
            anyhow::ensure!(
                meta.sequence <= MAX_DEDUP_REQUESTS as u64,
                "D1 dedup capacity exceeded"
            );
            let mut state = D1State::new(meta.shard, meta.epoch);
            state
                .set_capacity(
                    meta.shard,
                    meta.epoch,
                    crate::MAX_DEDUP_BYTES,
                    meta.dedup_bytes_limit,
                )
                .map_err(|e| anyhow::anyhow!("invalid capacity: {e:?}"))?;
            state.sequence = meta.sequence;
            state
        }
        4 => {
            let meta: D1MetadataV4 = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                meta.sequence <= MAX_DEDUP_REQUESTS as u64,
                "D1 dedup capacity exceeded"
            );
            let mut state = D1State::new(meta.shard, meta.epoch);
            state.version = 3;
            state.sequence = meta.sequence;
            state.dedup_bytes_limit = meta.dedup_bytes_limit;
            state.managed = Some(meta.managed);
            state
        }
        5 | 8 | 9 => {
            let meta: LegacyMetadata<D1State> = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                matches!(meta.version, 5 | 8 | 9) && meta.state.coordinator.is_some(),
                "invalid coordinator metadata"
            );
            request_indexed = matches!(meta.version, 8 | 9);
            anyhow::ensure!(
                meta.version != 9 || crate::decision_segments::enabled(&meta.state),
                "decision index requires bound coordinator"
            );
            decision_indexed = meta.version == 9;
            meta.state
        }
        6 => {
            let meta: LegacyMetadata<D1State> = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                meta.version == 6
                    && meta.state.participant.is_some()
                    && meta.state.dedup.is_empty(),
                "invalid participant metadata"
            );
            meta.state
        }
        7 | 10 => {
            let meta: LegacyMetadata<D1State> = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                matches!(meta.version, 7 | 10)
                    && meta.state.visibility.is_some()
                    && meta.state.dedup.is_empty(),
                "invalid visibility metadata"
            );
            anyhow::ensure!(
                meta.version != 10 || crate::decision_segments::enabled(&meta.state),
                "decision index requires bound member"
            );
            decision_indexed = meta.version == 10;
            meta.state
        }
        11 => {
            let meta: D1MetadataV11 = decode_new(&bytes)?;
            validate_common(
                meta.shard,
                meta.epoch,
                meta.sequence,
                meta.dedup_bytes_limit,
                &meta.managed,
                &meta.ledger,
                Role::Coordinator,
            )?;
            anyhow::ensure!(meta.sequence == 0, "coordinator contains data history");
            anyhow::ensure!(
                meta.ledger.applied == applied,
                "ledger applied marker mismatch"
            );
            let loaded = event_ledger::load(db, &meta.ledger)?;
            let state = D1State {
                version: 4,
                dedup_bytes_limit: meta.dedup_bytes_limit,
                managed: Some(meta.managed),
                coordinator: Some(loaded.journal.clone()),
                participant: None,
                visibility: None,
                shard: meta.shard,
                epoch: meta.epoch,
                sequence: meta.sequence,
                dedup: Vec::new(),
            };
            loaded.descriptor.validate_state(&state)?;
            ledger = Some(loaded);
            request_indexed = true;
            decision_indexed = true;
            state
        }
        12 => {
            let meta: D1MetadataV12 = decode_new(&bytes)?;
            validate_common(
                meta.shard,
                meta.epoch,
                meta.sequence,
                meta.dedup_bytes_limit,
                &meta.managed,
                &meta.ledger,
                Role::Member,
            )?;
            anyhow::ensure!(
                meta.ledger.applied == applied,
                "ledger applied marker mismatch"
            );
            let loaded = event_ledger::load(db, &meta.ledger)?;
            let state = D1State {
                version: 6,
                dedup_bytes_limit: meta.dedup_bytes_limit,
                managed: Some(meta.managed),
                coordinator: None,
                participant: Some(meta.participant),
                visibility: Some(crate::versioned_reads::State {
                    baseline_sequence: meta.visibility.baseline_sequence,
                    epoch: meta.visibility.epoch,
                    applied: meta.visibility.applied,
                    journal: loaded.journal.clone(),
                    commits: meta.visibility.commits,
                }),
                shard: meta.shard,
                epoch: meta.epoch,
                sequence: meta.sequence,
                dedup: Vec::new(),
            };
            loaded.descriptor.validate_state(&state)?;
            ledger = Some(loaded);
            decision_indexed = true;
            state
        }
        _ => anyhow::bail!("unsupported D1 disk version"),
    };
    Ok(Some(LoadedState {
        state,
        legacy_dedup,
        request_indexed,
        decision_indexed,
        ledger,
        metadata_bytes: bytes,
    }))
}

/// Encode the journal-less bound format, or the still-current unbound formats.
/// A bound state can never silently fall back to an embedded archive writer.
pub(super) fn d1_metadata_bytes(
    state: &D1State,
    descriptor: Option<&Descriptor>,
) -> anyhow::Result<Vec<u8>> {
    let bound = state
        .coordinator
        .iter()
        .chain(
            state
                .visibility
                .iter()
                .map(|visibility| &visibility.journal),
        )
        .any(|journal| journal.cohort().is_some());
    anyhow::ensure!(
        bound == descriptor.is_some(),
        "D1 ledger descriptor required for bound state only"
    );
    if let Some(descriptor) = descriptor {
        descriptor.validate_state(state)?;
        let managed = state
            .managed
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ledger state lacks managed mode"))?;
        if state.coordinator.is_some() {
            anyhow::ensure!(
                state.version == 4
                    && state.sequence == 0
                    && state.participant.is_none()
                    && state.visibility.is_none()
                    && state.dedup.is_empty(),
                "invalid coordinator metadata"
            );
            validate_common(
                state.shard,
                state.epoch,
                state.sequence,
                state.dedup_bytes_limit,
                managed,
                descriptor,
                Role::Coordinator,
            )?;
            return Ok(serde_json::to_vec(&CoordinatorMetadata {
                version: 11,
                shard: state.shard,
                epoch: state.epoch,
                sequence: state.sequence,
                dedup_bytes_limit: state.dedup_bytes_limit,
                managed,
                ledger: descriptor,
            })?);
        }
        let visibility = state
            .visibility
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ledger member lacks visibility"))?;
        let participant = state
            .participant
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ledger member lacks participant state"))?;
        anyhow::ensure!(state.version == 6, "invalid visibility metadata");
        validate_common(
            state.shard,
            state.epoch,
            state.sequence,
            state.dedup_bytes_limit,
            managed,
            descriptor,
            Role::Member,
        )?;
        return Ok(serde_json::to_vec(&MemberMetadata {
            version: 12,
            shard: state.shard,
            epoch: state.epoch,
            sequence: state.sequence,
            dedup_bytes_limit: state.dedup_bytes_limit,
            managed,
            participant,
            visibility: VisibilityMetadata {
                baseline_sequence: visibility.baseline_sequence,
                epoch: visibility.epoch,
                applied: visibility.applied,
                commits: &visibility.commits,
            },
            ledger: descriptor,
        })?);
    }
    // Only genuinely unbound/non-versioned modes reach the legacy writer.
    // Versions 5/9/10 are readers only; no compatibility-only writer remains.
    if state.visibility.is_some() || state.participant.is_some() {
        return Ok(serde_json::to_vec(&LegacyMetadata {
            version: if state.visibility.is_some() { 7 } else { 6 },
            state: state.stage(),
        })?);
    }
    if state.coordinator.is_some() {
        return Ok(serde_json::to_vec(&LegacyMetadata { version: 8, state })?);
    }
    if let Some(managed) = &state.managed {
        return Ok(serde_json::to_vec(&D1MetadataV4 {
            version: 4,
            shard: state.shard,
            epoch: state.epoch,
            sequence: state.sequence,
            dedup_bytes_limit: state.dedup_bytes_limit,
            managed: managed.clone(),
        })?);
    }
    Ok(match state.dedup_bytes_limit {
        None => serde_json::to_vec(&D1MetadataV2 {
            version: 2,
            shard: state.shard,
            epoch: state.epoch,
            sequence: state.sequence,
        })?,
        Some(dedup_bytes_limit) => serde_json::to_vec(&D1MetadataV3 {
            version: 3,
            shard: state.shard,
            epoch: state.epoch,
            sequence: state.sequence,
            dedup_bytes_limit,
        })?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::participant_writes::{Authority, Intent};
    use crate::transaction_decisions::{Command, HistoryCursor, Journal, Participant};
    use openraft::CommittedLeaderId;
    use rocksdb::WriteBatch;

    const COORDINATOR_BOUND: &[u8] =
        include_bytes!("../tests/fixtures/event-ledger-metadata-coordinator-bound-v11.json");
    const MEMBER_BOUND: &[u8] =
        include_bytes!("../tests/fixtures/event-ledger-metadata-member-bound-v12.json");
    const COORDINATOR_ADMITTED: &[u8] =
        include_bytes!("../tests/fixtures/event-ledger-metadata-coordinator-admitted-v11.json");
    const MEMBER_ADMITTED: &[u8] =
        include_bytes!("../tests/fixtures/event-ledger-metadata-member-admitted-v12.json");

    fn applied() -> Option<LogId<NodeId>> {
        Some(LogId::new(CommittedLeaderId::new(3, 2), 17))
    }

    fn journal(admitted: bool) -> Journal {
        if admitted {
            return serde_json::from_slice(include_bytes!(
                "../tests/fixtures/request-journal-v3.json"
            ))
            .unwrap();
        }
        let cursor: HistoryCursor =
            serde_json::from_slice(include_bytes!("../tests/fixtures/history-bound-v1.json"))
                .unwrap();
        let mut journal = Journal::new(cursor.authority.mode).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: cursor.cohort,
            })
            .unwrap();
        assert_eq!(journal.history_cursor().unwrap().root, cursor.root);
        journal
    }

    fn state(member: bool, admitted: bool) -> D1State {
        let journal = journal(admitted);
        let cohort = journal.cohort().unwrap();
        let authority = Authority {
            shard: cohort.coordinator_shard,
            mode: journal.authority(),
        };
        let (shard, epoch, mode) = if member {
            let member = &cohort.members[0];
            (member.shard, member.epoch, member.mode)
        } else {
            (
                cohort.coordinator_shard,
                cohort.coordinator_epoch,
                authority.mode,
            )
        };
        let mut state = D1State::new(shard, epoch);
        state.managed = Some(D1ManagedMode {
            mode_id: mode,
            activated_sequence: 0,
        });
        if member {
            state.version = 6;
            state.participant = Some(crate::participant_writes::State {
                authority,
                activated_sequence: 0,
                records: Vec::new(),
            });
            state.visibility = Some(crate::versioned_reads::State {
                baseline_sequence: 0,
                epoch,
                applied: 0,
                journal,
                commits: Vec::new(),
            });
        } else {
            state.version = 4;
            state.coordinator = Some(journal);
        }
        state.validate().unwrap();
        state
    }

    fn persist(db: &DB, state: &D1State) -> (Descriptor, Vec<u8>) {
        let mut batch = WriteBatch::default();
        let descriptor = event_ledger::stage(&mut batch, None, Some(state), applied(), false)
            .unwrap()
            .unwrap();
        let bytes = d1_metadata_bytes(state, Some(&descriptor)).unwrap();
        batch.put(D1_KEY, &bytes);
        batch.put(
            super::super::APPLIED_KEY,
            serde_json::to_vec(&applied()).unwrap(),
        );
        db.write(batch).unwrap();
        (descriptor, bytes)
    }

    #[test]
    fn bound_and_admitted_metadata_goldens_rehydrate_from_raw_events() {
        for (member, admitted, golden) in [
            (false, false, COORDINATOR_BOUND),
            (true, false, MEMBER_BOUND),
            (false, true, COORDINATOR_ADMITTED),
            (true, true, MEMBER_ADMITTED),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let state = state(member, admitted);
            let (_, bytes) = persist(&db, &state);
            assert_eq!(bytes, golden);
            for forbidden in [
                "\"coordinator\":",
                "\"journal\":",
                "\"events\":",
                "\"dedup\":",
                "\"state\":",
            ] {
                assert!(!std::str::from_utf8(&bytes).unwrap().contains(forbidden));
            }
            assert!(serde_json::from_slice::<D1State>(&bytes).is_err());
            assert!(serde_json::from_slice::<LegacyMetadata<D1State>>(&bytes).is_err());
            let loaded = load(&db, applied()).unwrap().unwrap();
            assert_eq!(loaded.state, state);
            assert_eq!(loaded.metadata_bytes, bytes);
            assert!(!loaded.legacy_dedup);
            assert_eq!(loaded.request_indexed, !member);
            assert!(loaded.decision_indexed);
            assert!(loaded.ledger.is_some());
            loaded.state.validate().unwrap();
            drop(db);
            let db = DB::open_default(dir.path()).unwrap();
            assert_eq!(load(&db, applied()).unwrap().unwrap().state, state);
        }
    }

    fn decodes_header(member: bool, bytes: &[u8]) -> bool {
        if member {
            serde_json::from_slice::<D1MetadataV12>(bytes).is_ok()
        } else {
            serde_json::from_slice::<D1MetadataV11>(bytes).is_ok()
        }
    }

    #[test]
    fn new_schemas_reject_missing_duplicate_unknown_and_embedded_archives() {
        for (member, golden) in [(false, COORDINATOR_BOUND), (true, MEMBER_BOUND)] {
            let value: serde_json::Value = serde_json::from_slice(golden).unwrap();
            for field in value.as_object().unwrap().keys() {
                let mut missing = value.clone();
                missing.as_object_mut().unwrap().remove(field);
                assert!(
                    !decodes_header(member, &serde_json::to_vec(&missing).unwrap()),
                    "missing {field}"
                );
            }
            for field in [
                "state",
                "coordinator",
                "journal",
                "events",
                "dedup",
                "extra",
            ] {
                let mut extra = value.clone();
                extra[field] = serde_json::json!([]);
                assert!(
                    !decodes_header(member, &serde_json::to_vec(&extra).unwrap()),
                    "extra {field}"
                );
            }
            let text = std::str::from_utf8(golden).unwrap();
            let duplicate = text.replacen("\"sequence\":0", "\"sequence\":0,\"sequence\":0", 1);
            assert!(!decodes_header(member, duplicate.as_bytes()));
        }
        let value: serde_json::Value = serde_json::from_slice(MEMBER_BOUND).unwrap();
        for field in ["baseline_sequence", "epoch", "applied", "commits"] {
            let mut missing = value.clone();
            missing["visibility"].as_object_mut().unwrap().remove(field);
            assert!(!decodes_header(
                true,
                &serde_json::to_vec(&missing).unwrap()
            ));
        }
        for field in ["journal", "events"] {
            let mut extra = value.clone();
            extra["visibility"][field] = serde_json::json!([]);
            assert!(!decodes_header(true, &serde_json::to_vec(&extra).unwrap()));
        }
        let spaced = [MEMBER_BOUND, b" "].concat();
        assert!(decodes_header(true, &spaced));
        assert!(decode_new::<D1MetadataV12>(&spaced).is_err());
    }

    #[test]
    fn ledger_header_cannot_fall_back_to_embedded_state_or_ignore_raw_orphans() {
        for member in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let state = state(member, false);
            assert!(d1_metadata_bytes(&state, None).is_err());
            let (descriptor, bytes) = persist(&db, &state);
            assert!(load(&db, None).is_err());
            let wrong_role = String::from_utf8(bytes.clone()).unwrap().replacen(
                if member {
                    "\"Member\""
                } else {
                    "\"Coordinator\""
                },
                if member {
                    "\"Coordinator\""
                } else {
                    "\"Member\""
                },
                1,
            );
            db.put(D1_KEY, wrong_role).unwrap();
            assert!(load(&db, applied()).is_err());
            let legacy = serde_json::to_vec(&LegacyMetadata {
                version: if member { 10 } else { 9 },
                state: &state,
            })
            .unwrap();
            db.put(D1_KEY, legacy).unwrap();
            assert!(load(&db, applied()).is_err());
            db.delete(D1_KEY).unwrap();
            assert!(load(&db, applied()).is_err());
            let empty = tempfile::tempdir().unwrap();
            let empty_db = DB::open_default(empty.path()).unwrap();
            empty_db.put(D1_KEY, bytes).unwrap();
            empty_db
                .put(
                    super::super::APPLIED_KEY,
                    serde_json::to_vec(&applied()).unwrap(),
                )
                .unwrap();
            assert!(load(&empty_db, applied()).is_err());
            let mut foreign = descriptor;
            foreign.domain.shard += 1;
            assert!(d1_metadata_bytes(&state, Some(&foreign)).is_err());
        }
    }

    #[test]
    fn published_legacy_readers_keep_their_index_and_retry_migration_flags() {
        for version in 1..=10 {
            let mut state = match version {
                1..=3 => D1State::new(9, 7),
                4 | 5 | 8 | 9 => state(false, false),
                _ => state(true, false),
            };
            if version == 3 {
                state
                    .set_capacity(9, 7, crate::MAX_DEDUP_BYTES, crate::EXTENDED_DEDUP_BYTES)
                    .unwrap();
            }
            if version == 4 {
                state.version = 3;
                state.coordinator = None;
            }
            if version == 6 {
                state.version = 5;
                state.visibility = None;
            }
            let bytes = match version {
                1 => serde_json::to_vec(&state).unwrap(),
                2..=4 => d1_metadata_bytes(&state, None).unwrap(),
                _ => serde_json::to_vec(&LegacyMetadata {
                    version,
                    state: &state,
                })
                .unwrap(),
            };
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            db.put(D1_KEY, bytes).unwrap();
            let loaded = load(&db, applied()).unwrap().unwrap();
            assert_eq!(loaded.state, state);
            assert_eq!(loaded.legacy_dedup, version == 1);
            assert_eq!(loaded.request_indexed, matches!(version, 8 | 9));
            assert_eq!(loaded.decision_indexed, matches!(version, 9 | 10));
            assert!(loaded.ledger.is_none());
        }
        let mut unbound = state(false, false);
        unbound.coordinator = Some(Journal::new([9; 16]).unwrap());
        let bytes = d1_metadata_bytes(&unbound, None).unwrap();
        let metadata: LegacyMetadata<D1State> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(metadata.version, 8);
        assert_eq!(metadata.state, unbound);
    }

    #[test]
    fn full_participant_intent_capacity_is_not_limited_by_event_row_bytes() {
        let mut state = state(true, false);
        state.dedup_bytes_limit = Some(crate::EXTENDED_DEDUP_BYTES);
        // Intent charges 256 bytes, request 128, and one operation 64.
        // Byte-array JSON for 255 expands each value byte fourfold.
        let payload = vec![255; crate::participant_writes::MAX_INTENT_BYTES - 256 - 128 - 64];
        let request = crate::D1WriteRequest {
            shard: state.shard,
            epoch: state.epoch,
            request_id: [1; 16],
            expected_sequence: Some(0),
            operations: vec![crate::WriteOp::put([], payload)],
        };
        assert_eq!(
            256 + request.retained_size(),
            crate::participant_writes::MAX_INTENT_BYTES
        );
        state.participant.as_mut().unwrap().records.push(Intent {
            id: request.request_id,
            participant: Participant {
                shard: state.shard,
                epoch: state.epoch,
                mode: state.managed.as_ref().unwrap().mode_id,
                intent: [2; 32],
            },
            request: Some(request),
            decision: None,
            sequence: None,
        });
        state.validate().unwrap();
        let mut batch = WriteBatch::default();
        let descriptor = event_ledger::stage(&mut batch, None, Some(&state), applied(), false)
            .unwrap()
            .unwrap();
        let bytes = d1_metadata_bytes(&state, Some(&descriptor)).unwrap();
        assert!(bytes.len() > 4 * crate::transaction_decisions::MAX_HISTORY_PAGE_BYTES);
        let metadata: D1MetadataV12 = decode_new(&bytes).unwrap();
        assert_eq!(metadata.participant, *state.participant.as_ref().unwrap());
        assert_eq!(
            metadata.dedup_bytes_limit,
            Some(crate::EXTENDED_DEDUP_BYTES)
        );
        // Existing participant Option fields stay unchanged in logical formats,
        // but an omitted null in a new disk header is not canonical metadata.
        let missing = String::from_utf8(bytes)
            .unwrap()
            .replacen("\"decision\":null,", "", 1);
        assert!(decode_new::<D1MetadataV12>(missing.as_bytes()).is_err());
    }
}
