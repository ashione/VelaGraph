//! Private live-state disk codecs. Complete logical snapshots remain unchanged.
//!
//! Bound-cohort headers contain metadata and one ledger descriptor, never a
//! serialized Journal. Raw events must be replayed before constructing D1State.
use super::event_ledger::{self, Descriptor, Role, Staged};
use super::{D1_KEY, MAX_DEDUP_REQUESTS};
use crate::capacity_policy::{CapacityActivation, CapacityProfile};
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
type CoordinatorHeader = CoordinatorMetadata<D1ManagedMode, Descriptor>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VisibilityMetadata<C> {
    baseline_sequence: u64,
    epoch: u64,
    applied: u64,
    commits: C,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activation: Option<CapacityActivation>,
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
type MemberHeader = MemberMetadata<
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

fn validate_disk_profile(version: u32, role: Role, descriptor: &Descriptor) -> anyhow::Result<()> {
    let expected = match (version, role) {
        (11, Role::Coordinator) | (12, Role::Member) => CapacityProfile::LegacyCountV1,
        (13, Role::Coordinator) | (14, Role::Member) => CapacityProfile::ByteBound16MiBV1,
        (15, Role::Coordinator) => match (descriptor.domain.cohort.profile, descriptor.version) {
            (CapacityProfile::LegacyCountV1, 1) => CapacityProfile::LegacyCountV1,
            (CapacityProfile::ByteBound16MiBV1, 2) => CapacityProfile::ByteBound16MiBV1,
            _ => anyhow::bail!("unsupported disk-15 ledger profile/format"),
        },
        _ => anyhow::bail!("invalid role for D1 disk version"),
    };
    anyhow::ensure!(
        descriptor.domain.cohort.profile == expected
            && descriptor.version == expected.storage_format(),
        "ledger capacity profile disagrees with disk version"
    );
    Ok(())
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
    if !matches!(version, 11..=15) {
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
        11 | 13 | 15 => {
            let meta: CoordinatorHeader = decode_new(&bytes)?;
            validate_disk_profile(version, Role::Coordinator, &meta.ledger)?;
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
            if version == 15 {
                crate::request_index::reject_retired_namespace(db)?;
            }
            let loaded = event_ledger::load(db, &meta.ledger)?;
            let state = D1State {
                version: 4,
                dedup_bytes_limit: meta.dedup_bytes_limit,
                managed: Some(meta.managed),
                coordinator: Some(loaded.journal().clone()),
                participant: None,
                visibility: None,
                shard: meta.shard,
                epoch: meta.epoch,
                sequence: meta.sequence,
                dedup: Vec::new(),
            };
            loaded.validate_hydrated_state(&state)?;
            ledger = Some(loaded);
            request_indexed = version != 15;
            decision_indexed = true;
            state
        }
        12 | 14 => {
            let meta: MemberHeader = decode_new(&bytes)?;
            validate_disk_profile(version, Role::Member, &meta.ledger)?;
            anyhow::ensure!(
                meta.visibility.activation.is_some() == (version == 14),
                "member activation disagrees with disk version"
            );
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
                version: if version == 14 { 7 } else { 6 },
                dedup_bytes_limit: meta.dedup_bytes_limit,
                managed: Some(meta.managed),
                coordinator: None,
                participant: Some(meta.participant),
                visibility: Some(crate::versioned_reads::State {
                    baseline_sequence: meta.visibility.baseline_sequence,
                    epoch: meta.visibility.epoch,
                    applied: meta.visibility.applied,
                    journal: loaded.journal().clone(),
                    commits: meta.visibility.commits,
                    activation: meta.visibility.activation,
                }),
                shard: meta.shard,
                epoch: meta.epoch,
                sequence: meta.sequence,
                dedup: Vec::new(),
            };
            loaded.validate_hydrated_state(&state)?;
            ledger = Some(loaded);
            decision_indexed = true;
            state
        }
        _ => anyhow::bail!("unsupported D1 disk version"),
    };
    if version <= 12 {
        anyhow::ensure!(
            state
                .coordinator
                .iter()
                .chain(state.visibility.iter().map(|v| &v.journal))
                .all(|j| j.capacity_profile() == CapacityProfile::LegacyCountV1)
                && state
                    .visibility
                    .as_ref()
                    .is_none_or(|v| v.activation.is_none()),
            "legacy disk metadata cannot contain a new capacity profile or activation"
        );
    }
    state.validate_capacity_activation(applied)?;
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
    staged: Option<&Staged<'_>>,
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
        bound == staged.is_some(),
        "D1 ledger descriptor required for bound state only"
    );
    if let Some(staged) = staged {
        let descriptor = staged.descriptor_for(state)?;
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
            validate_disk_profile(15, Role::Coordinator, descriptor)?;
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
                version: 15,
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
        anyhow::ensure!(
            state.version
                == match descriptor.domain.cohort.profile {
                    CapacityProfile::LegacyCountV1 => 6,
                    CapacityProfile::ByteBound16MiBV1 => 7,
                },
            "invalid visibility metadata"
        );
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
            version: match descriptor.domain.cohort.profile {
                CapacityProfile::LegacyCountV1 => 12,
                CapacityProfile::ByteBound16MiBV1 => 14,
            },
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
                activation: visibility.activation.clone(),
            },
            ledger: descriptor,
        })?);
    }
    anyhow::ensure!(
        state
            .visibility
            .as_ref()
            .is_none_or(|v| v.activation.is_none()),
        "unbound metadata cannot contain a capacity activation"
    );
    // Only genuinely unbound/non-versioned modes reach the legacy writer.
    // Versions 5/9/10/11/13 are readers only; no bound legacy writer remains.
    if state.visibility.is_some() || state.participant.is_some() {
        return Ok(serde_json::to_vec(&LegacyMetadata {
            version: if state.visibility.is_some() { 7 } else { 6 },
            state: state.stage(),
        })?);
    }
    if let Some(journal) = &state.coordinator {
        anyhow::ensure!(
            journal.cohort().is_none() && journal.requests().is_empty(),
            "unbound coordinator cannot contain a cohort or external requests"
        );
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
    use crate::transaction_decisions::{
        Command, HistoryCursor, Journal, MemberCapacityGrant, Participant,
    };
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
                activation: None,
            });
        } else {
            state.version = 4;
            state.coordinator = Some(journal);
        }
        state.validate().unwrap();
        state
    }

    fn byte_state(member: bool, confirmed: bool) -> D1State {
        let mut state = state(member, false);
        let old = if member {
            &state.visibility.as_ref().unwrap().journal
        } else {
            state.coordinator.as_ref().unwrap()
        };
        let mut cohort = old.cohort().unwrap().clone();
        cohort.profile = CapacityProfile::ByteBound16MiBV1;
        let activation = CapacityActivation {
            nonce: [3; 32],
            log_id: LogId::new(CommittedLeaderId::new(3, 2), 1),
        };
        let mut journal = Journal::new(old.authority()).unwrap();
        journal
            .replay_event(Command::BindCohort {
                cohort: cohort.clone(),
            })
            .unwrap();
        if confirmed {
            let grants = cohort
                .members
                .iter()
                .map(|member| MemberCapacityGrant {
                    member: member.clone(),
                    activation: activation.clone(),
                })
                .collect();
            journal
                .replay_event(Command::ConfirmCohortCapacity { cohort, grants })
                .unwrap();
        }
        if member {
            state.version = 7;
            let visibility = state.visibility.as_mut().unwrap();
            visibility.journal = journal;
            visibility.activation = Some(activation);
        } else {
            state.coordinator = Some(journal);
        }
        state.validate().unwrap();
        state.validate_capacity_activation(applied()).unwrap();
        state
    }

    fn persist(db: &DB, state: &D1State) -> (Descriptor, Vec<u8>) {
        let mut batch = WriteBatch::default();
        let staged = event_ledger::stage(&mut batch, None, Some(state), applied(), false)
            .unwrap()
            .unwrap();
        let bytes = d1_metadata_bytes(state, Some(&staged)).unwrap();
        let descriptor = staged.descriptor_for(state).unwrap().clone();
        batch.put(D1_KEY, &bytes);
        batch.put(
            super::super::APPLIED_KEY,
            serde_json::to_vec(&applied()).unwrap(),
        );
        db.write(batch).unwrap();
        (descriptor, bytes)
    }

    fn state_journal_mut(state: &mut D1State) -> &mut Journal {
        match &mut state.coordinator {
            Some(journal) => journal,
            None => &mut state.visibility.as_mut().unwrap().journal,
        }
    }

    fn append_begin(state: &mut D1State) {
        let journal = state_journal_mut(state);
        let member = &journal.cohort().unwrap().members[0];
        journal
            .replay_event(Command::Begin {
                id: [42; 16],
                participants: vec![Participant {
                    shard: member.shard,
                    epoch: member.epoch,
                    mode: member.mode,
                    intent: [43; 32],
                }],
            })
            .unwrap();
    }

    #[test]
    fn staged_metadata_requires_exact_candidate_for_append_noop_and_replacement() {
        for member in [false, true] {
            for old in [state(member, false), byte_state(member, true)] {
                let dir = tempfile::tempdir().unwrap();
                let db = DB::open_default(dir.path()).unwrap();
                let (descriptor, _) = persist(&db, &old);
                let loaded = event_ledger::load(&db, &descriptor).unwrap();
                for append in [false, true] {
                    let mut candidate = old.clone();
                    if append {
                        append_begin(&mut candidate);
                    }
                    for has_prior in [false, true] {
                        for replace in [false, true] {
                            let mut batch = WriteBatch::default();
                            let staged = event_ledger::stage(
                                &mut batch,
                                has_prior.then_some(&loaded),
                                Some(&candidate),
                                applied(),
                                replace,
                            )
                            .unwrap()
                            .unwrap();
                            let staged_descriptor = staged.descriptor_for(&candidate).unwrap();
                            assert_eq!(
                                staged_descriptor.event_count,
                                descriptor.event_count + u64::from(append)
                            );
                            assert_eq!(staged_descriptor.applied, applied());
                            let bytes = d1_metadata_bytes(&candidate, Some(&staged)).unwrap();
                            let metadata: serde_json::Value =
                                serde_json::from_slice(&bytes).unwrap();
                            assert_eq!(
                                metadata["ledger"],
                                serde_json::to_value(staged_descriptor).unwrap()
                            );
                            let equal_candidate = candidate.clone();
                            assert_eq!(equal_candidate, candidate);
                            assert!(!std::ptr::eq(&equal_candidate, &candidate));
                            assert!(d1_metadata_bytes(&equal_candidate, Some(&staged)).is_err());
                            let mut foreign = candidate.clone();
                            foreign.shard += 1;
                            assert!(d1_metadata_bytes(&foreign, Some(&staged)).is_err());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn loaded_authority_requires_exact_journal_and_complete_hydrated_domain() {
        for member in [false, true] {
            for candidate in [state(member, false), byte_state(member, true)] {
                let dir = tempfile::tempdir().unwrap();
                let db = DB::open_default(dir.path()).unwrap();
                let (descriptor, _) = persist(&db, &candidate);
                let loaded = event_ledger::load(&db, &descriptor).unwrap();
                loaded.validate_hydrated_state(&candidate).unwrap();
                // Hydration clones the reconstructed value; unlike staging, it
                // deliberately requires exact value equality, not pointer identity.
                loaded.validate_hydrated_state(&candidate.clone()).unwrap();
                for fault in 0..6 {
                    let mut foreign = candidate.clone();
                    match fault {
                        0 => foreign.shard += 1,
                        1 => foreign.epoch += 1,
                        2 => foreign.managed.as_mut().unwrap().mode_id[0] ^= 1,
                        3 => append_begin(&mut foreign),
                        4 => {
                            let journal = state_journal_mut(&mut foreign);
                            let mut cohort = journal.cohort().unwrap().clone();
                            cohort.members[0].epoch += 1;
                            *journal = Journal::new(journal.authority()).unwrap();
                            journal
                                .replay_event(Command::BindCohort { cohort })
                                .unwrap();
                        }
                        5 => {
                            let journal = state_journal_mut(&mut foreign);
                            let cohort = journal.cohort().unwrap().clone();
                            *journal = Journal::new([99; 16]).unwrap();
                            journal
                                .replay_event(Command::BindCohort { cohort })
                                .unwrap();
                        }
                        _ => unreachable!(),
                    }
                    assert!(
                        loaded.validate_hydrated_state(&foreign).is_err(),
                        "fault {fault}"
                    );
                }
                let foreign_role = if member {
                    let mut coordinator = state(false, false);
                    coordinator.coordinator = Some(loaded.journal().clone());
                    coordinator
                } else {
                    let mut participant = state(true, false);
                    participant.visibility.as_mut().unwrap().journal = loaded.journal().clone();
                    participant
                };
                assert!(loaded.validate_hydrated_state(&foreign_role).is_err());
            }
        }
    }

    #[test]
    fn reused_authority_still_checks_activation_and_full_applied_identity() {
        let candidate = byte_state(true, true);
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let (descriptor, _) = persist(&db, &candidate);
        let loaded = event_ledger::load(&db, &descriptor).unwrap();
        for fault in 0..4 {
            let mut changed = candidate.clone();
            let activation = &mut changed.visibility.as_mut().unwrap().activation;
            match fault {
                0 => *activation = None,
                1 => activation.as_mut().unwrap().nonce = [4; 32],
                2 => activation.as_mut().unwrap().log_id.index = 18,
                3 => {
                    activation.as_mut().unwrap().log_id =
                        LogId::new(CommittedLeaderId::new(3, 3), 17)
                }
                _ => unreachable!(),
            }
            assert!(
                loaded.validate_hydrated_state(&changed).is_err(),
                "fault {fault}"
            );
            for replace in [false, true] {
                let mut batch = WriteBatch::default();
                assert!(event_ledger::stage(
                    &mut batch,
                    Some(&loaded),
                    Some(&changed),
                    applied(),
                    replace,
                )
                .is_err());
            }
        }
        for applied in [None, Some(LogId::new(CommittedLeaderId::new(3, 3), 1))] {
            for replace in [false, true] {
                let mut batch = WriteBatch::default();
                assert!(event_ledger::stage(
                    &mut batch,
                    Some(&loaded),
                    Some(&candidate),
                    applied,
                    replace,
                )
                .is_err());
            }
        }
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
            if member {
                assert_eq!(bytes, golden);
            } else {
                // Preserve published v11 bytes as an actual legacy fixture.
                // The new header changes only its storage-version contract.
                let suffix = golden.strip_prefix(b"{\"version\":11,").unwrap();
                assert_eq!(bytes, [b"{\"version\":15,".as_slice(), suffix].concat());
            }
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
            assert!(!loaded.request_indexed);
            assert!(loaded.decision_indexed);
            assert!(loaded.ledger.is_some());
            loaded.state.validate().unwrap();
            drop(db);
            let db = DB::open_default(dir.path()).unwrap();
            assert_eq!(load(&db, applied()).unwrap().unwrap().state, state);
        }
    }

    #[test]
    fn byte_profile_metadata_reopens_with_exact_immutable_activation() {
        for member in [false, true] {
            for confirmed in [false, true] {
                let state = byte_state(member, confirmed);
                let dir = tempfile::tempdir().unwrap();
                let bytes = {
                    let db = DB::open_default(dir.path()).unwrap();
                    let (descriptor, bytes) = persist(&db, &state);
                    assert_eq!(descriptor.version, 2);
                    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(value["version"], if member { 14 } else { 15 });
                    assert_eq!(
                        value["ledger"]["domain"]["cohort"]["profile"],
                        "ByteBound16MiBV1"
                    );
                    assert_eq!(value["ledger"]["version"], 2);
                    for forbidden in ["journal", "coordinator", "events", "state"] {
                        assert!(value.get(forbidden).is_none());
                    }
                    if member {
                        assert_eq!(
                            value["visibility"]["activation"],
                            serde_json::to_value(
                                state
                                    .visibility
                                    .as_ref()
                                    .unwrap()
                                    .activation
                                    .as_ref()
                                    .unwrap()
                            )
                            .unwrap()
                        );
                        assert!(value["visibility"].get("journal").is_none());
                    }
                    bytes
                };
                let db = DB::open_default(dir.path()).unwrap();
                let loaded = load(&db, applied()).unwrap().unwrap();
                assert_eq!(loaded.state, state);
                assert_eq!(loaded.metadata_bytes, bytes);
                assert!(loaded.decision_indexed);
                assert!(!loaded.request_indexed);
                assert!(loaded.ledger.is_some());
            }
        }
    }

    #[test]
    fn published_coordinator_headers_keep_strict_legacy_index_contracts() {
        // Derive this fixture from the admitted cohort, not the distinct
        // bound-only golden (whose member identities intentionally differ).
        let mut byte = state(false, true);
        let original = byte.coordinator.take().unwrap();
        let mut cohort = original.cohort().unwrap().clone();
        cohort.profile = CapacityProfile::ByteBound16MiBV1;
        let mut history = Journal::new(original.authority()).unwrap();
        history
            .replay_event(Command::BindCohort {
                cohort: cohort.clone(),
            })
            .unwrap();
        history
            .replay_event(Command::ConfirmCohortCapacity {
                cohort: cohort.clone(),
                grants: cohort
                    .members
                    .iter()
                    .cloned()
                    .map(|member| MemberCapacityGrant {
                        member,
                        activation: CapacityActivation {
                            nonce: [3; 32],
                            log_id: applied().unwrap(),
                        },
                    })
                    .collect(),
            })
            .unwrap();
        byte.coordinator = Some(history);
        for event in &original.events()[1..] {
            let mut event = event.clone();
            if let Command::AdmitRequest { payload, .. } = &mut event {
                let mut plan = crate::request_plans::Plan::decode(payload).unwrap();
                plan.cohort = cohort.clone();
                *payload = plan.encode().unwrap();
            }
            byte.coordinator
                .as_mut()
                .unwrap()
                .replay_event(event)
                .unwrap();
        }
        for (state, version) in [(state(false, true), 11), (byte, 13)] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let (_, bytes) = persist(&db, &state);
            let mut header: CoordinatorHeader = decode_new(&bytes).unwrap();
            header.version = version;
            let legacy_bytes = serde_json::to_vec(&header).unwrap();
            if version == 11 {
                assert_eq!(legacy_bytes, COORDINATOR_ADMITTED);
            }
            let mut batch = WriteBatch::default();
            crate::request_index::stage(&mut batch, None, Some(&state), applied(), false, true)
                .unwrap();
            batch.put(D1_KEY, &legacy_bytes);
            db.write(batch).unwrap();
            let loaded = load(&db, applied()).unwrap().unwrap();
            assert_eq!(loaded.state, state);
            assert!(loaded.request_indexed);
            assert!(loaded.decision_indexed);
            crate::request_index::validate(&db, Some(&loaded.state), applied(), true).unwrap();
            let record = &state.coordinator.as_ref().unwrap().requests()[0];
            db.delete(crate::request_index::key(&state, record.id))
                .unwrap();
            assert!(
                crate::request_index::validate(&db, Some(&loaded.state), applied(), true).is_err()
            );
            // Reinterpretation as disk 15 cannot conceal the remaining manifest.
            header.version = 15;
            db.put(D1_KEY, serde_json::to_vec(&header).unwrap())
                .unwrap();
            assert!(load(&db, applied()).is_err());
        }
    }

    #[test]
    fn disk15_rejects_request_residue_under_both_unchanged_profiles() {
        for state in [state(false, true), byte_state(false, true)] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let (_, bytes) = persist(&db, &state);
            let header: CoordinatorHeader = decode_new(&bytes).unwrap();
            assert_eq!(header.version, 15);
            assert!(!load(&db, applied()).unwrap().unwrap().request_indexed);
            for key in [
                crate::request_index::PREFIX,
                b"sm/request/foreign-row".as_slice(),
                crate::request_index::MANIFEST_KEY,
            ] {
                db.put(key, b"not a valid index record").unwrap();
                assert!(load(&db, applied()).is_err());
                db.delete(key).unwrap();
                assert!(!load(&db, applied()).unwrap().unwrap().request_indexed);
            }
        }
    }

    #[test]
    fn disk15_requires_typed_bound_coordinator_and_exact_profile_domain_marker() {
        for state in [state(false, false), byte_state(false, true)] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let (_, bytes) = persist(&db, &state);
            for fault in 0..10 {
                let mut meta: CoordinatorHeader = decode_new(&bytes).unwrap();
                match fault {
                    0 => meta.ledger.version = 3,
                    1 => meta.ledger.version = if meta.ledger.version == 1 { 2 } else { 1 },
                    2 => meta.ledger.domain.role = Role::Member,
                    3 => meta.ledger.domain.shard += 1,
                    4 => meta.ledger.domain.epoch += 1,
                    5 => meta.ledger.domain.mode[0] ^= 1,
                    6 => meta.ledger.domain.authority.mode[0] ^= 1,
                    7 => meta.ledger.domain.cohort.members[0].epoch += 1,
                    8 => meta.ledger.applied = Some(LogId::new(CommittedLeaderId::new(3, 3), 17)),
                    9 => meta.sequence = 1,
                    _ => unreachable!(),
                }
                db.put(D1_KEY, serde_json::to_vec(&meta).unwrap()).unwrap();
                assert!(load(&db, applied()).is_err(), "fault {fault}");
            }
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            for field in value.as_object().unwrap().keys() {
                let mut missing = value.clone();
                missing.as_object_mut().unwrap().remove(field);
                db.put(D1_KEY, serde_json::to_vec(&missing).unwrap())
                    .unwrap();
                assert!(load(&db, applied()).is_err(), "missing {field}");
            }
            for field in [
                "state",
                "coordinator",
                "journal",
                "participant",
                "visibility",
                "extra",
            ] {
                let mut mixed = value.clone();
                mixed[field] = serde_json::json!({});
                db.put(D1_KEY, serde_json::to_vec(&mixed).unwrap()).unwrap();
                assert!(load(&db, applied()).is_err(), "mixed {field}");
            }
            for field in ["domain", "applied", "head_root"] {
                let mut missing = value.clone();
                missing["ledger"].as_object_mut().unwrap().remove(field);
                db.put(D1_KEY, serde_json::to_vec(&missing).unwrap())
                    .unwrap();
                assert!(load(&db, applied()).is_err(), "missing ledger {field}");
            }
            let duplicate = String::from_utf8(bytes.clone()).unwrap().replacen(
                "\"version\":15",
                "\"version\":15,\"version\":15",
                1,
            );
            db.put(D1_KEY, duplicate).unwrap();
            assert!(load(&db, applied()).is_err());
            db.put(D1_KEY, &bytes).unwrap();
            assert!(load(&db, None).is_err());
            assert_eq!(load(&db, applied()).unwrap().unwrap().state, state);
            let mut legacy = state.clone();
            legacy.coordinator =
                Some(Journal::new(state.managed.as_ref().unwrap().mode_id).unwrap());
            db.put(
                D1_KEY,
                serde_json::to_vec(&LegacyMetadata {
                    version: 15,
                    state: legacy,
                })
                .unwrap(),
            )
            .unwrap();
            assert!(load(&db, applied()).is_err());
        }
        // A valid member header cannot be accepted by merely changing its version.
        for state in [state(true, false), byte_state(true, true)] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let (_, bytes) = persist(&db, &state);
            let mut header: MemberHeader = decode_new(&bytes).unwrap();
            header.version = 15;
            db.put(D1_KEY, serde_json::to_vec(&header).unwrap())
                .unwrap();
            assert!(load(&db, applied()).is_err());
        }
    }

    #[test]
    fn byte_member_metadata_rejects_missing_changed_or_future_activation() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let state = byte_state(true, true);
        let (_, bytes) = persist(&db, &state);
        for fault in 0..9 {
            let mut meta: MemberHeader = decode_new(&bytes).unwrap();
            match fault {
                0 => meta.version = 12,
                1 => meta.ledger.version = 1,
                2 => meta.ledger.domain.cohort.profile = CapacityProfile::LegacyCountV1,
                3 => meta.visibility.activation = None,
                4 => meta.visibility.activation.as_mut().unwrap().nonce = [0; 32],
                5 => meta.visibility.activation.as_mut().unwrap().nonce = [4; 32],
                6 => meta.visibility.activation.as_mut().unwrap().log_id.index = 18,
                7 => {
                    meta.visibility.activation.as_mut().unwrap().log_id =
                        LogId::new(CommittedLeaderId::new(3, 3), 17)
                }
                8 => {
                    meta.visibility
                        .activation
                        .as_mut()
                        .unwrap()
                        .log_id
                        .leader_id
                        .term = 0
                }
                _ => unreachable!(),
            }
            db.put(D1_KEY, serde_json::to_vec(&meta).unwrap()).unwrap();
            assert!(load(&db, applied()).is_err(), "fault {fault}");
        }
        let text = std::str::from_utf8(&bytes).unwrap();
        let activation = serde_json::to_string(
            state
                .visibility
                .as_ref()
                .unwrap()
                .activation
                .as_ref()
                .unwrap(),
        )
        .unwrap();
        for replacement in ["null", "{}"] {
            db.put(
                D1_KEY,
                text.replace(
                    &format!("\"activation\":{activation}"),
                    &format!("\"activation\":{replacement}"),
                ),
            )
            .unwrap();
            assert!(load(&db, applied()).is_err());
        }
        for field in ["nonce", "log_id", "extra"] {
            let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if field == "extra" {
                value["visibility"]["activation"][field] = serde_json::json!(0);
            } else {
                value["visibility"]["activation"]
                    .as_object_mut()
                    .unwrap()
                    .remove(field);
            }
            assert!(serde_json::from_value::<MemberHeader>(value).is_err());
        }
        // An activation cannot be smuggled into a legacy disk-12 envelope.
        let legacy_dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(legacy_dir.path()).unwrap();
        let legacy = self::state(true, false);
        let (_, bytes) = persist(&db, &legacy);
        let mut meta: MemberHeader = decode_new(&bytes).unwrap();
        meta.visibility.activation = Some(CapacityActivation {
            nonce: [3; 32],
            log_id: applied().unwrap(),
        });
        db.put(D1_KEY, serde_json::to_vec(&meta).unwrap()).unwrap();
        assert!(load(&db, applied()).is_err());
    }

    #[test]
    fn legacy_disk_envelopes_never_reinterpret_byte_profile_archives() {
        for member in [false, true] {
            let state = byte_state(member, true);
            for version in if member {
                vec![6, 7, 10]
            } else {
                vec![5, 8, 9]
            } {
                let dir = tempfile::tempdir().unwrap();
                let db = DB::open_default(dir.path()).unwrap();
                let bytes = serde_json::to_vec(&LegacyMetadata {
                    version,
                    state: &state,
                })
                .unwrap();
                db.put(D1_KEY, bytes).unwrap();
                assert!(load(&db, applied()).is_err(), "legacy version {version}");
            }
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let (_, bytes) = persist(&db, &state);
            if !member {
                let mut meta: CoordinatorHeader = decode_new(&bytes).unwrap();
                meta.version = 11;
                db.put(D1_KEY, serde_json::to_vec(&meta).unwrap()).unwrap();
                assert!(load(&db, applied()).is_err());
            }
        }
    }

    fn decodes_header(member: bool, bytes: &[u8]) -> bool {
        if member {
            serde_json::from_slice::<MemberHeader>(bytes).is_ok()
        } else {
            serde_json::from_slice::<CoordinatorHeader>(bytes).is_ok()
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
        assert!(decode_new::<MemberHeader>(&spaced).is_err());
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
            assert!(event_ledger::load(&db, &foreign).is_err());
            let mut batch = WriteBatch::default();
            let staged = event_ledger::stage(&mut batch, None, Some(&state), applied(), false)
                .unwrap()
                .unwrap();
            let mut foreign = state.clone();
            foreign.shard += 1;
            assert!(d1_metadata_bytes(&foreign, Some(&staged)).is_err());
        }
    }

    #[test]
    fn published_legacy_readers_keep_their_index_and_retry_migration_flags() {
        for version in 1..=10 {
            let mut state = match version {
                1..=3 => D1State::new(9, 7),
                4 | 5 => state(false, false),
                8 | 9 => state(false, true),
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
            if matches!(version, 8 | 9) {
                let mut batch = WriteBatch::default();
                crate::request_index::stage(&mut batch, None, Some(&state), applied(), false, true)
                    .unwrap();
                db.write(batch).unwrap();
            }
            let loaded = load(&db, applied()).unwrap().unwrap();
            assert_eq!(loaded.state, state);
            assert_eq!(loaded.legacy_dedup, version == 1);
            assert_eq!(loaded.request_indexed, matches!(version, 8 | 9));
            assert_eq!(loaded.decision_indexed, matches!(version, 9 | 10));
            assert!(loaded.ledger.is_none());
            crate::request_index::validate(
                &db,
                Some(&loaded.state),
                applied(),
                loaded.request_indexed,
            )
            .unwrap();
            if loaded.request_indexed {
                let record = &state.coordinator.as_ref().unwrap().requests()[0];
                db.delete(crate::request_index::key(&state, record.id))
                    .unwrap();
                assert!(
                    crate::request_index::validate(&db, Some(&loaded.state), applied(), true,)
                        .is_err()
                );
            }
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
        let metadata: MemberHeader = decode_new(&bytes).unwrap();
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
        assert!(decode_new::<MemberHeader>(missing.as_bytes()).is_err());
    }
}
