//! Real state-machine recovery readers and mandatory request-index retirement.
//! Legacy fixtures contain the actual typed headers, raw rows, indexes and marker.
use super::*;
use crate::participant_writes::Authority;
use crate::request_index::{MANIFEST_KEY, PREFIX};
use crate::request_plans::{Part, Plan};
use crate::transaction_decisions::{
    CapacityProfile, Cohort, Command, Decision, HistoryCursor, Journal, Member, Participant,
};
use openraft::CommittedLeaderId;

fn id(n: u64) -> [u8; 16] {
    let mut value = [0; 16];
    value[8..].copy_from_slice(&n.to_be_bytes());
    value
}
fn authority() -> Authority {
    Authority {
        shard: 7,
        mode: [1; 16],
    }
}
fn cohort(profile: CapacityProfile) -> Cohort {
    Cohort {
        profile,
        coordinator_shard: 7,
        coordinator_epoch: 3,
        members: vec![
            Member {
                shard: 8,
                epoch: 1,
                mode: [3; 16],
            },
            Member {
                shard: 9,
                epoch: 1,
                mode: [5; 16],
            },
        ],
    }
}
fn participant() -> Participant {
    Participant {
        shard: 8,
        epoch: 1,
        mode: [3; 16],
        intent: [4; 32],
    }
}
fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(LogCommand::D1V1(command)),
    }
}
fn blank(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Blank,
    }
}
fn coordinator(command: Command) -> D1Command {
    D1Command::Coordinator {
        shard: 7,
        epoch: 3,
        mode_id: [1; 16],
        command,
    }
}
fn admit(profile: CapacityProfile, external: u64, transaction: u64) -> D1Command {
    admit_parts(profile, external, transaction, vec![participant()])
}
fn admit_parts(
    profile: CapacityProfile,
    external: u64,
    transaction: u64,
    participants: Vec<Participant>,
) -> D1Command {
    let payload = Plan {
        version: 1,
        id: id(transaction),
        authority: authority(),
        cohort: cohort(profile),
        validation_version: 0,
        parts: participants
            .iter()
            .map(|descriptor| Part {
                descriptor: descriptor.clone(),
                request: crate::D1WriteRequest {
                    shard: descriptor.shard,
                    epoch: descriptor.epoch,
                    request_id: id(transaction),
                    expected_sequence: Some(0),
                    operations: vec![WriteOp::put("retained", "original")],
                },
            })
            .collect(),
    }
    .encode()
    .unwrap();
    coordinator(Command::AdmitRequest {
        id: id(external),
        request: format!("original-{external}").into_bytes(),
        payload,
        transaction_id: id(transaction),
        participants,
    })
}
fn abort(transaction: u64) -> D1Command {
    coordinator(Command::Decide {
        id: id(transaction),
        decision: Decision::Abort,
    })
}
async fn apply(sm: &mut Arc<StateMachine>, commands: Vec<D1Command>) {
    let first = sm.applied().await.map_or(1, |v| v.index + 1);
    let results = sm
        .apply(
            commands
                .into_iter()
                .enumerate()
                .map(|(n, command)| entry(first + n as u64, command)),
        )
        .await
        .unwrap();
    assert!(
        results.iter().all(|r| r.d1.as_ref().unwrap().is_ok()),
        "{results:?}"
    );
}
async fn setup(sm: &mut Arc<StateMachine>, profile: CapacityProfile) {
    apply(
        sm,
        vec![
            D1Command::Activate { shard: 7, epoch: 3 },
            D1Command::EnableManagedWrites {
                shard: 7,
                epoch: 3,
                expected_sequence: 0,
                mode_id: [1; 16],
            },
            D1Command::EnableCoordinator {
                shard: 7,
                epoch: 3,
                mode_id: [1; 16],
            },
            coordinator(Command::BindCohort {
                cohort: cohort(profile),
            }),
        ],
    )
    .await;
    if profile == CapacityProfile::ByteBound16MiBV1 {
        // Obtain a real member grant from its durable activation path. The
        // trusted coordinator proposal is not a fabricated grant or quota bypass.
        let mut grants = Vec::new();
        for descriptor in cohort(profile).members {
            let member_dir = tempfile::tempdir().unwrap();
            let mut member = Arc::new(StateMachine::open(member_dir.path()).unwrap());
            let mut bound = Journal::new(authority().mode).unwrap();
            bound
                .apply(Command::BindCohort {
                    cohort: cohort(profile),
                })
                .unwrap();
            let shard = descriptor.shard;
            let mode_id = descriptor.mode;
            apply(
                &mut member,
                vec![
                    D1Command::Activate { shard, epoch: 1 },
                    D1Command::EnableManagedWrites {
                        shard,
                        epoch: 1,
                        expected_sequence: 0,
                        mode_id,
                    },
                    D1Command::EnableParticipant {
                        shard,
                        epoch: 1,
                        mode_id,
                        authority: authority(),
                    },
                    D1Command::EnableByteBoundReads {
                        shard,
                        epoch: 1,
                        mode_id,
                        journal: bound,
                        generation_nonce: [shard as u8; 32],
                    },
                ],
            )
            .await;
            let grant = member
                .participant_capacity_grant(shard, 1, mode_id, &authority(), &cohort(profile))
                .await
                .unwrap();
            assert_eq!(grant.activation.log_id.index, 4);
            grants.push(grant);
        }
        apply(
            sm,
            vec![coordinator(Command::ConfirmCohortCapacity {
                cohort: cohort(profile),
                grants,
            })],
        )
        .await;
    }
}
fn profiles() -> [CapacityProfile; 2] {
    [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ]
}
fn disk(sm: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .iterator(IteratorMode::Start)
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.to_vec(), value.to_vec())
        })
        .collect()
}
fn rows(sm: &StateMachine, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    disk(sm)
        .into_iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .collect()
}
// The published 11/13/15 schema shares these fields with disk16.
// Only disk16 carries the trailing checkpoint; legacy fixtures omit it.
// Keeping this strict copy local prevents JSON-tree rewriting from accidentally
// manufacturing a noncanonical fixture which fails before index validation.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    version: u32,
    shard: u64,
    epoch: u64,
    sequence: u64,
    dedup_bytes_limit: Option<usize>,
    managed: crate::D1ManagedMode,
    ledger: event_ledger::Descriptor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint: Option<crate::transaction_decisions::checkpoint::ActiveCheckpoint>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyRow {
    version: u32,
    record: crate::transaction_decisions::RequestRecord,
    ordinal: u64,
}
fn header(sm: &StateMachine) -> Header {
    let bytes = sm
        .storage
        .as_ref()
        .unwrap()
        .db
        .get(D1_KEY)
        .unwrap()
        .unwrap();
    let value: Header = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&value).unwrap(), bytes);
    value
}
fn assert_retired(sm: &StateMachine, profile: CapacityProfile) {
    let value = header(sm);
    assert_eq!(value.version, 16);
    assert!(value.checkpoint.is_some());
    assert_eq!(value.ledger.domain.cohort, cohort(profile));
    assert_eq!(value.ledger.version, profile.storage_format());
    assert!(rows(sm, PREFIX).is_empty());
    let storage = sm.storage.as_ref().unwrap();
    assert!(storage.db.get(MANIFEST_KEY).unwrap().is_none());
    assert!(!storage.request_indexed.load(Ordering::Relaxed));
}
async fn lookup(sm: &StateMachine, external: u64) -> crate::request_index::RequestLookup {
    sm.coordinator_request(7, 3, [1; 16], id(external))
        .await
        .unwrap()
}
async fn strict(sm: &StateMachine) -> Journal {
    sm.coordinator_journal(7, 3, [1; 16]).await.unwrap()
}
async fn page(
    sm: &StateMachine,
    base: &HistoryCursor,
    cut: Option<&HistoryCursor>,
) -> crate::transaction_decisions::HistoryPage {
    sm.coordinator_history_page(7, 3, [1; 16], base, cut)
        .await
        .unwrap()
}
async fn legacy_fixture(path: &Path, profile: CapacityProfile) {
    let mut sm = Arc::new(StateMachine::open(path).unwrap());
    setup(&mut sm, profile).await;
    apply(&mut sm, vec![admit(profile, 900, 1), abort(1)]).await;
    let state = sm.state.read().await;
    let full = sm.load_disk_state(&state).unwrap().unwrap().state;
    let mut value = header(&sm);
    assert_eq!(value.version, 16);
    assert!(value.checkpoint.is_some());
    value.version = if profile == CapacityProfile::LegacyCountV1 {
        11
    } else {
        13
    };
    value.checkpoint = None;
    let storage = sm.storage.as_ref().unwrap();
    let mut batch = WriteBatch::default();
    crate::request_index::stage(
        &mut batch,
        None,
        Some(&full),
        state.last_applied_log,
        false,
        true,
    )
    .unwrap();
    batch.put(D1_KEY, serde_json::to_vec(&value).unwrap());
    storage.write_sync(batch).unwrap();
    crate::request_index::validate(&storage.db, Some(&full), state.last_applied_log, true).unwrap();
    assert_eq!(rows(&sm, PREFIX).len(), 1);
    assert!(storage.db.get(MANIFEST_KEY).unwrap().is_some());
    drop(state);
    drop(sm);
    // Opening the actual old format, and checking its exact original result,
    // ensures corruption tests cannot pass on an invalid starting fixture.
    let sm = StateMachine::open(path).unwrap();
    assert!(sm
        .storage
        .as_ref()
        .unwrap()
        .request_indexed
        .load(Ordering::Relaxed));
    let original = lookup(&sm, 900).await.record.unwrap();
    assert_eq!(original.record.transaction_id, id(1));
    assert_eq!(original.transaction.decision, Some(Decision::Abort));
    assert_eq!(original.transaction.ordinal, 1);
}

#[test]
fn archive_reads_match_strict_replay_across_profiles_and_frozen_cuts() {
    // Drive reads on this same thread so the no-Journal tripwire covers the
    // actual StateMachine methods, including their inline blocking sections.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for profile in profiles() {
        let dir = tempfile::tempdir().unwrap();
        let (sm, frozen, journal) = runtime.block_on(async {
            let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
            setup(&mut sm, profile).await;
            // Begin consumes ordinal 1 but no external ID. Its unresolved hole
            // must not hide a later Commit, Abort, or partially prepared transaction.
            apply(
                &mut sm,
                vec![coordinator(Command::Begin {
                    id: id(700),
                    participants: vec![participant()],
                })],
            )
            .await;
            apply(
                &mut sm,
                (1..=32).map(|n| admit(profile, n, 1000 + n)).collect(),
            )
            .await;
            apply(
                &mut sm,
                vec![admit_parts(
                    profile,
                    33,
                    1033,
                    vec![
                        participant(),
                        Participant {
                            shard: 9,
                            epoch: 1,
                            mode: [5; 16],
                            intent: [6; 32],
                        },
                    ],
                )],
            )
            .await;
            let frozen = strict(&sm).await;
            apply(
                &mut sm,
                vec![
                    coordinator(Command::Prepared {
                        id: id(1001),
                        participant: participant(),
                    }),
                    coordinator(Command::Decide {
                        id: id(1001),
                        decision: Decision::Commit,
                    }),
                    abort(1017),
                    coordinator(Command::Prepared {
                        id: id(1033),
                        participant: participant(),
                    }),
                    // First Plan wins and this no-op is not an extra archive event.
                    admit(profile, 1, 9999),
                ],
            )
            .await;
            let journal = strict(&sm).await;
            (sm, frozen, journal)
        });
        assert_eq!(journal.resolved_prefix(), 0);
        let cut = frozen.history_cursor().unwrap();
        for external in [1, 17, 33, 999] {
            let expected = runtime.block_on(async {
                let state = sm.state.read().await;
                let full = sm.load_disk_state(&state).unwrap().unwrap().state;
                crate::request_index::lookup(&full, id(external), None).unwrap()
            });
            let actual = crate::transaction_decisions::without_full_journal(|| {
                runtime.block_on(lookup(&sm, external))
            });
            assert_eq!(actual, expected);
            actual.validate().unwrap();
            match external {
                1 => {
                    let transaction = actual.record.unwrap().transaction;
                    assert_eq!(transaction.ordinal, 2);
                    assert_eq!(transaction.decision, Some(Decision::Commit));
                }
                17 => assert_eq!(
                    actual.record.unwrap().transaction.decision,
                    Some(Decision::Abort)
                ),
                33 => {
                    let transaction = actual.record.unwrap().transaction;
                    assert_eq!(transaction.prepared, vec![true, false]);
                    assert_eq!(transaction.decision, None);
                }
                _ => assert!(actual.record.is_none()),
            }
        }
        for count in [0, 1, 31, 32, 33, cut.event_count] {
            let base = frozen.history_cursor_at(count).unwrap();
            let expected_frozen = frozen.history_page(&base, Some(&cut)).unwrap();
            let expected_current = journal.history_page(&base, None).unwrap();
            let (actual_frozen, actual_current) =
                crate::transaction_decisions::without_full_journal(|| {
                    runtime.block_on(async {
                        (
                            page(&sm, &base, Some(&cut)).await,
                            page(&sm, &base, None).await,
                        )
                    })
                });
            assert_eq!(actual_frozen, expected_frozen);
            assert_eq!(actual_current, expected_current);
        }
        let zero = journal.history_cursor_at(0).unwrap();
        let empty = crate::transaction_decisions::without_full_journal(|| {
            runtime.block_on(page(&sm, &zero, Some(&zero)))
        });
        assert!(empty.events.is_empty());
        assert_retired(&sm, profile);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn archive_read_closes_full_namespace_after_found_missing_and_old_cut() {
    for profile in profiles() {
        for case in 0..6 {
            let dir = tempfile::tempdir().unwrap();
            let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
            setup(&mut sm, profile).await;
            apply(&mut sm, vec![admit(profile, 900, 1)]).await;
            let frozen = strict(&sm).await;
            let base = frozen.history_cursor_at(0).unwrap();
            let cut = frozen.history_cursor().unwrap();
            apply(&mut sm, (2..=34).map(|n| admit(profile, n, n)).collect()).await;
            let cached = sm.state.read().await.d1.as_ref().unwrap().clone();
            let original = lookup(&sm, 900).await;
            assert!(original.record.is_some());
            assert!(lookup(&sm, 999).await.record.is_none());
            assert_eq!(
                page(&sm, &base, Some(&cut)).await,
                frozen.history_page(&base, Some(&cut)).unwrap()
            );
            let ledger = rows(&sm, event_ledger::PREFIX);
            assert!(ledger.len() >= 2);
            let (last_key, last_value) = ledger.last().unwrap();
            let db = &sm.storage.as_ref().unwrap().db;
            match case {
                0 => db.put(last_key, b"malformed tail").unwrap(),
                1 => db.delete(last_key).unwrap(),
                2 => db
                    .put([event_ledger::PREFIX, b"zz-extra"].concat(), b"{}")
                    .unwrap(),
                3 => {
                    let mut foreign = last_key.clone();
                    foreign[event_ledger::PREFIX.len() + 4] ^= 1;
                    db.put(foreign, last_value).unwrap();
                }
                4 => {
                    let mut noncanonical = last_value.clone();
                    noncanonical.push(b' ');
                    db.put(last_key, noncanonical).unwrap();
                }
                _ => {
                    let mut changed = header(&sm);
                    changed.ledger.head_root[0] ^= 1;
                    db.put(D1_KEY, serde_json::to_vec(&changed).unwrap())
                        .unwrap();
                }
            }
            assert_eq!(sm.state.read().await.d1.as_ref().unwrap().clone(), cached);
            for external in [900, 999] {
                assert!(
                    sm.coordinator_request(7, 3, [1; 16], id(external))
                        .await
                        .is_err(),
                    "profile {profile:?}, corruption {case}, external {external}"
                );
            }
            assert!(sm
                .coordinator_history_page(7, 3, [1; 16], &base, Some(&cut))
                .await
                .is_err());
            assert!(sm
                .coordinator_history_page(7, 3, [1; 16], &cut, Some(&cut))
                .await
                .is_err());
            assert!(sm.build_snapshot().await.is_err());
            drop(sm);
            assert!(
                StateMachine::open(dir.path()).is_err(),
                "profile {profile:?}, corruption {case}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn legacy_index_damage_cannot_be_repaired_by_ordinary_migration() {
    for profile in profiles() {
        for case in 0..5 {
            let dir = tempfile::tempdir().unwrap();
            legacy_fixture(dir.path(), profile).await;
            let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
            let marker = sm.applied().await;
            let cached = sm.state.read().await.d1.as_ref().unwrap().clone();
            let (key, value) = rows(&sm, PREFIX).pop().unwrap();
            let db = &sm.storage.as_ref().unwrap().db;
            match case {
                0 => db.delete(&key).unwrap(),
                1 => {
                    let mut changed: LegacyRow = serde_json::from_slice(&value).unwrap();
                    assert_eq!(serde_json::to_vec(&changed).unwrap(), value);
                    changed.record.request[0] ^= 1;
                    let changed = serde_json::to_vec(&changed).unwrap();
                    // Same encoded size and valid canonical row: manifest byte
                    // counts alone cannot detect this changed original binding.
                    assert_eq!(changed.len(), value.len());
                    db.put(&key, changed).unwrap();
                }
                2 => {
                    let mut extra = key.clone();
                    *extra.last_mut().unwrap() ^= 1;
                    db.put(extra, value).unwrap();
                }
                3 => db.delete(MANIFEST_KEY).unwrap(),
                _ => db.put([PREFIX, b"foreign"].concat(), b"{}").unwrap(),
            }
            let damaged = disk(&sm);
            assert!(
                sm.apply([blank(marker.unwrap().index + 1)]).await.is_err(),
                "{profile:?}, {case}"
            );
            assert_eq!(disk(&sm), damaged);
            assert_eq!(sm.applied().await, marker);
            assert_eq!(sm.state.read().await.d1.as_ref().unwrap().clone(), cached);
            assert_eq!(
                header(&sm).version,
                if profile == CapacityProfile::LegacyCountV1 {
                    11
                } else {
                    13
                }
            );
            drop(sm);
            assert!(StateMachine::open(dir.path()).is_err());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn disk16_reintroduced_request_row_or_manifest_rejects_all_authoritative_paths() {
    for profile in profiles() {
        for manifest in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
            setup(&mut sm, profile).await;
            apply(&mut sm, vec![admit(profile, 900, 1)]).await;
            let journal = strict(&sm).await;
            let zero = journal.history_cursor_at(0).unwrap();
            assert_retired(&sm, profile);
            let key = if manifest {
                MANIFEST_KEY.to_vec()
            } else {
                [PREFIX, b"orphan"].concat()
            };
            sm.storage.as_ref().unwrap().db.put(key, b"{}").unwrap();
            for external in [900, 999] {
                assert!(sm
                    .coordinator_request(7, 3, [1; 16], id(external))
                    .await
                    .is_err());
            }
            assert!(sm
                .coordinator_history_page(7, 3, [1; 16], &zero, None)
                .await
                .is_err());
            assert!(sm.coordinator_summary(7, 3, [1; 16]).await.is_err());
            assert!(sm
                .coordinator_terminal_prefix(7, 3, [1; 16], 0)
                .await
                .is_err());
            assert!(sm.coordinator_journal(7, 3, [1; 16]).await.is_err());
            assert!(sm.build_snapshot().await.is_err());
            let before = disk(&sm);
            let next = sm.applied().await.unwrap().index + 1;
            assert!(sm.apply([blank(next)]).await.is_err());
            assert_eq!(disk(&sm), before);
            drop(sm);
            assert!(StateMachine::open(dir.path()).is_err());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn blank_and_admitting_migration_retire_index_in_one_synced_generation() {
    for profile in profiles() {
        for append in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            legacy_fixture(dir.path(), profile).await;
            let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
            let original = lookup(&sm, 900).await;
            let journal = strict(&sm).await;
            let zero = journal.history_cursor_at(0).unwrap();
            let cut = journal.history_cursor().unwrap();
            let old_v = sm
                .coordinator_terminal_prefix(7, 3, [1; 16], 1)
                .await
                .unwrap();
            let old_marker = sm.applied().await.unwrap();
            let old_disk = disk(&sm);
            let next = if append {
                entry(old_marker.index + 1, admit(profile, 901, 2))
            } else {
                blank(old_marker.index + 1)
            };
            sm.storage
                .as_ref()
                .unwrap()
                .fail_next_write
                .store(true, Ordering::Relaxed);
            assert!(sm.apply([next.clone()]).await.is_err());
            assert_eq!(disk(&sm), old_disk);
            assert_eq!(sm.applied().await, Some(old_marker));
            assert_eq!(lookup(&sm, 900).await, original);
            drop(sm);
            let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
            assert_eq!(disk(&sm), old_disk);
            assert!(sm
                .storage
                .as_ref()
                .unwrap()
                .request_indexed
                .load(Ordering::Relaxed));
            sm.apply([next.clone()]).await.unwrap();
            assert_retired(&sm, profile);
            assert_eq!(sm.applied().await, Some(next.log_id));
            assert_eq!(header(&sm).ledger.applied, Some(next.log_id));
            assert_eq!(lookup(&sm, 900).await, original);
            assert_eq!(lookup(&sm, 901).await.record.is_some(), append);
            assert_eq!(
                page(&sm, &zero, Some(&cut)).await,
                journal.history_page(&zero, Some(&cut)).unwrap()
            );
            assert_eq!(
                sm.coordinator_terminal_prefix(7, 3, [1; 16], 1)
                    .await
                    .unwrap(),
                old_v
            );
            drop(sm);
            let sm = StateMachine::open(dir.path()).unwrap();
            assert_retired(&sm, profile);
            assert_eq!(sm.applied().await, Some(next.log_id));
            assert_eq!(lookup(&sm, 900).await, original);
            assert_eq!(
                sm.coordinator_terminal_prefix(7, 3, [1; 16], 1)
                    .await
                    .unwrap(),
                old_v
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn archive_rejects_old_metadata_or_full_marker_mixed_with_new_generation() {
    for profile in profiles() {
        for case in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
            setup(&mut sm, profile).await;
            apply(&mut sm, vec![admit(profile, 900, 1)]).await;
            let journal = strict(&sm).await;
            let zero = journal.history_cursor_at(0).unwrap();
            let cut = journal.history_cursor().unwrap();
            let old_metadata = sm
                .storage
                .as_ref()
                .unwrap()
                .db
                .get(D1_KEY)
                .unwrap()
                .unwrap();
            let old_marker = sm.applied().await;
            apply(&mut sm, vec![admit(profile, 901, 2)]).await;
            let current = sm.applied().await.unwrap();
            let db = &sm.storage.as_ref().unwrap().db;
            match case {
                0 => db.put(D1_KEY, old_metadata).unwrap(),
                1 => db
                    .put(APPLIED_KEY, serde_json::to_vec(&old_marker).unwrap())
                    .unwrap(),
                _ => {
                    // Same index is insufficient: every leader-id component is pinned.
                    let different_leader =
                        Some(LogId::new(CommittedLeaderId::new(2, 1), current.index));
                    db.put(APPLIED_KEY, serde_json::to_vec(&different_leader).unwrap())
                        .unwrap();
                }
            }
            assert!(sm
                .coordinator_request(7, 3, [1; 16], id(900))
                .await
                .is_err());
            assert!(sm
                .coordinator_request(7, 3, [1; 16], id(999))
                .await
                .is_err());
            assert!(sm
                .coordinator_history_page(7, 3, [1; 16], &zero, Some(&cut))
                .await
                .is_err());
            let mixed = disk(&sm);
            assert!(sm.apply([blank(current.index + 1)]).await.is_err());
            assert_eq!(disk(&sm), mixed);
            assert!(sm.build_snapshot().await.is_err());
            drop(sm);
            assert!(StateMachine::open(dir.path()).is_err());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_retirement_crash_child() -> anyhow::Result<()> {
    let Some(path) = std::env::var_os("D2_ARCHIVE_RETIREMENT_CRASH_DIRECTORY") else {
        return Ok(());
    };
    let path = std::path::PathBuf::from(path);
    let append = std::env::var("D2_ARCHIVE_RETIREMENT_APPEND")? == "1";
    let mut sm = Arc::new(StateMachine::open(path.join("state.db"))?);
    let profile = header(&sm).ledger.domain.cohort.profile;
    assert!(sm
        .storage
        .as_ref()
        .unwrap()
        .request_indexed
        .load(Ordering::Relaxed));
    assert_eq!(rows(&sm, PREFIX).len(), 1);
    let next = sm.applied().await.unwrap().index + 1;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (_release, gate) = std::sync::mpsc::channel();
    *sm.apply_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
        entered,
        release: gate,
    });
    let observer = sm.clone();
    let writer = tokio::spawn(async move {
        let command = if append {
            entry(next, admit(profile, 901, 2))
        } else {
            blank(next)
        };
        sm.apply([command]).await.map(|_| ())
    });
    ready.await?;
    // This marker is written only after the existing post-sync gate is reached,
    // and the independently inspected disk already contains the complete retirement.
    let published = header(&observer);
    assert_eq!(published.version, 16);
    assert_eq!(published.ledger.domain.cohort, cohort(profile));
    assert!(rows(&observer, PREFIX).is_empty());
    assert!(observer
        .storage
        .as_ref()
        .unwrap()
        .db
        .get(MANIFEST_KEY)?
        .is_none());
    assert_eq!(published.ledger.applied.unwrap().index, next);
    std::fs::write(path.join("synced"), b"retired")?;
    writer.await??;
    anyhow::bail!("parent must kill child after retirement sync and before publication")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn process_death_after_index_retirement_sync_recovers_exact_old_id_plan_and_v(
) -> anyhow::Result<()> {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for profile in profiles() {
        for append in [false, true] {
            let dir = tempfile::tempdir()?;
            let state_path = dir.path().join("state.db");
            legacy_fixture(&state_path, profile).await;
            let sm = StateMachine::open(&state_path)?;
            let original = lookup(&sm, 900).await;
            let journal = strict(&sm).await;
            let zero = journal.history_cursor_at(0).unwrap();
            let cut = journal.history_cursor().unwrap();
            let old_v = sm
                .coordinator_terminal_prefix(7, 3, [1; 16], 1)
                .await
                .unwrap();
            let old_marker = sm.applied().await.unwrap();
            drop(sm);
            let mut child = Child(
                std::process::Command::new(std::env::current_exe()?)
                    .args([
                        "--exact",
                        "state_machine::archive_recovery_tests::archive_retirement_crash_child",
                        "--nocapture",
                    ])
                    .env("D2_ARCHIVE_RETIREMENT_CRASH_DIRECTORY", dir.path())
                    .env(
                        "D2_ARCHIVE_RETIREMENT_APPEND",
                        if append { "1" } else { "0" },
                    )
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn()?,
            );
            // Preserve the existing process-crash test budget; no timeout relaxation.
            tokio::time::timeout(std::time::Duration::from_secs(8), async {
                while !dir.path().join("synced").exists() {
                    anyhow::ensure!(
                        child.0.try_wait()?.is_none(),
                        "child exited before retirement sync"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                anyhow::Ok(())
            })
            .await??;
            child.0.kill()?;
            child.0.wait()?;
            let mut sm = Arc::new(StateMachine::open(&state_path)?);
            assert_retired(&sm, profile);
            assert_eq!(
                sm.applied().await,
                Some(LogId::new(old_marker.leader_id, old_marker.index + 1))
            );
            assert_eq!(header(&sm).ledger.applied, sm.applied().await);
            assert_eq!(lookup(&sm, 900).await, original);
            assert_eq!(lookup(&sm, 901).await.record.is_some(), append);
            assert_eq!(
                page(&sm, &zero, Some(&cut)).await,
                journal.history_page(&zero, Some(&cut)).unwrap()
            );
            assert_eq!(
                sm.coordinator_terminal_prefix(7, 3, [1; 16], 0)
                    .await
                    .unwrap(),
                journal.terminal_prefix(0).unwrap()
            );
            assert_eq!(
                sm.coordinator_terminal_prefix(7, 3, [1; 16], 1)
                    .await
                    .unwrap(),
                old_v
            );
            // Lost ACK followed by a losing Plan still returns the exact original
            // payload, ID, ordinal and terminal outcome after witness reconstruction.
            apply(&mut sm, vec![admit(profile, 900, 9999)]).await;
            assert_eq!(lookup(&sm, 900).await, original);
            assert_retired(&sm, profile);
        }
    }
    Ok(())
}

// OpenRaft 0.9.25 shutdown joins its core/tick, not the state-machine worker.
// Keep the final owner until that worker (and any snapshot worker) has dropped
// its Arc, then close RocksDB synchronously before the single reopen attempt.
async fn close_after_background_owners(
    mut state: Arc<StateMachine>,
    timeout: std::time::Duration,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(timeout, async move {
        loop {
            match Arc::try_unwrap(state) {
                Ok(state) => {
                    drop(state);
                    return;
                }
                Err(shared) => {
                    state = shared;
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
}

#[tokio::test]
async fn close_waits_for_actual_owner_release_before_reopen() {
    use std::{future::Future, task::Poll, time::Duration};

    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(StateMachine::open(dir.path()).unwrap());
    let background_owner = state.clone();
    let mut close = std::pin::pin!(close_after_background_owners(state, Duration::from_secs(5)));
    // Poll exactly once while the owner is deliberately retained. This checks
    // the causal wait, without depending on a lucky worker scheduling order.
    assert!(std::future::poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx).is_pending())).await);
    drop(background_owner);
    close.await.unwrap();
    let reopened = StateMachine::open(dir.path()).unwrap();
    assert_eq!(reopened.applied().await, None);
}

#[tokio::test]
async fn close_reports_timeout_if_an_owner_is_not_released() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(StateMachine::open(dir.path()).unwrap());
    let background_owner = state.clone();
    assert!(
        close_after_background_owners(state, std::time::Duration::ZERO)
            .await
            .is_err()
    );
    drop(background_owner);
    let reopened = StateMachine::open(dir.path()).unwrap();
    assert_eq!(reopened.applied().await, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn managed_tcp_archive_reads_reject_damaged_tail_and_recover_exact_evidence() {
    use crate::managed_rpc::{Client, Identity, NodeRef, Role, Server};
    use crate::versioned_reads::Target;
    use std::time::Duration;

    let profile = CapacityProfile::LegacyCountV1;
    let identity = Identity {
        group: 700,
        node: 1,
        target: Target {
            shard: 7,
            epoch: 3,
            mode: [1; 16],
        },
        role: Role::Coordinator,
        authority: authority(),
        cohort: cohort(profile),
    };
    let dir = tempfile::tempdir().unwrap();
    let cluster = crate::Cluster::new().in_group(identity.group);
    let node = Arc::new(
        cluster
            .start_durable(identity.node, dir.path())
            .await
            .unwrap(),
    );
    node.raft
        .initialize(BTreeMap::from([(identity.node, BasicNode::default())]))
        .await
        .unwrap();
    node.raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics.current_leader == Some(identity.node) && metrics.last_applied.is_some()
            },
            "archive reader leader",
        )
        .await
        .unwrap();
    // Every active-node mutation goes through Raft, never direct StateMachine::apply.
    for command in [
        D1Command::Activate { shard: 7, epoch: 3 },
        D1Command::EnableManagedWrites {
            shard: 7,
            epoch: 3,
            expected_sequence: 0,
            mode_id: [1; 16],
        },
        D1Command::EnableCoordinator {
            shard: 7,
            epoch: 3,
            mode_id: [1; 16],
        },
        coordinator(Command::BindCohort {
            cohort: cohort(profile),
        }),
        admit(profile, 900, 1),
    ] {
        node.raft
            .client_write(LogCommand::D1V1(command))
            .await
            .unwrap()
            .data
            .d1
            .unwrap()
            .unwrap();
    }
    let original = lookup(&node.state_machine, 900).await;
    let frozen = strict(&node.state_machine).await;
    let base = frozen.history_cursor_at(0).unwrap();
    let cut = frozen.history_cursor().unwrap();
    let old_page = frozen.history_page(&base, Some(&cut)).unwrap();
    for transaction in 2..=34 {
        node.raft
            .client_write(LogCommand::D1V1(admit(profile, transaction, transaction)))
            .await
            .unwrap()
            .data
            .d1
            .unwrap()
            .unwrap();
    }
    // A later losing candidate must not overwrite the payload returned over TCP.
    node.raft
        .client_write(LogCommand::D1V1(admit(profile, 900, 9999)))
        .await
        .unwrap()
        .data
        .d1
        .unwrap()
        .unwrap();
    let server = Server::bind(
        node.clone(),
        identity.clone(),
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let client = Client::new(server.local_addr(), identity, Duration::from_secs(2)).unwrap();
    let remote = NodeRef::Remote(&client);
    assert_eq!(
        remote.request(&authority(), 3, id(900)).await.unwrap(),
        original
    );
    assert!(remote
        .request(&authority(), 3, id(999))
        .await
        .unwrap()
        .record
        .is_none());
    assert_eq!(
        remote
            .history_page(&authority(), 3, &base, Some(&cut))
            .await
            .unwrap(),
        old_page
    );
    let ledger = rows(&node.state_machine, event_ledger::PREFIX);
    assert!(ledger.len() >= 2);
    let (tail_key, tail_value) = ledger.last().unwrap();
    let db = &node.state_machine.storage.as_ref().unwrap().db;
    db.put(tail_key, b"damaged after requested ID and frozen cut")
        .unwrap();
    assert!(remote.request(&authority(), 3, id(900)).await.is_err());
    assert!(remote.request(&authority(), 3, id(999)).await.is_err());
    assert!(remote
        .history_page(&authority(), 3, &base, Some(&cut))
        .await
        .is_err());
    assert!(remote
        .history_page(&authority(), 3, &cut, Some(&cut))
        .await
        .is_err());
    // Restore the exact row, not a reconstructed substitute or repaired witness.
    db.put(tail_key, tail_value).unwrap();
    assert_eq!(
        remote.request(&authority(), 3, id(900)).await.unwrap(),
        original
    );
    assert!(remote
        .request(&authority(), 3, id(999))
        .await
        .unwrap()
        .record
        .is_none());
    assert_eq!(
        remote
            .history_page(&authority(), 3, &base, Some(&cut))
            .await
            .unwrap(),
        old_page
    );
    assert_retired(&node.state_machine, profile);
    server.shutdown().await;
    let closing_state = node.state_machine.clone();
    cluster.shutdown(&node).await;
    drop(node);
    close_after_background_owners(closing_state, Duration::from_secs(5))
        .await
        .expect("background state-machine owners must release before reopening RocksDB");
    let sm = StateMachine::open(dir.path().join("state.db")).unwrap();
    assert_eq!(lookup(&sm, 900).await, original);
    assert_eq!(page(&sm, &base, Some(&cut)).await, old_page);
    assert_retired(&sm, profile);
}

#[tokio::test]
async fn disk16_missing_private_cut_never_falls_back_and_membership_refreshes_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    let profile = CapacityProfile::LegacyCountV1;
    setup(&mut sm, profile).await;
    apply(&mut sm, vec![admit(profile, 900, 1)]).await;
    let expected = lookup(&sm, 900).await;
    let zero = strict(&sm).await.history_cursor_at(0).unwrap();
    let saved = sm.state.write().await.archive_cut.take().unwrap();
    assert_eq!(
        sm.coordinator_request(7, 3, [1; 16], id(900)).await,
        Err(D1Error::EventLedgerHistory)
    );
    assert_eq!(
        sm.coordinator_history_page(7, 3, [1; 16], &zero, None)
            .await,
        Err(D1Error::EventLedgerHistory)
    );
    sm.state.write().await.archive_cut = Some(saved.clone());
    let old_rows = rows(&sm, event_ledger::PREFIX);
    let next = sm.applied().await.unwrap().index + 1;
    sm.apply([Entry {
        log_id: LogId::new(CommittedLeaderId::new(2, 3), next),
        payload: EntryPayload::Membership(openraft::Membership::new(
            vec![std::collections::BTreeSet::from([3])],
            BTreeMap::from([(3, BasicNode::default())]),
        )),
    }])
    .await
    .unwrap();
    assert_eq!(rows(&sm, event_ledger::PREFIX), old_rows);
    assert_eq!(header(&sm).ledger.applied, sm.applied().await);
    assert!(
        event_ledger::lookup_request(&sm.storage.as_ref().unwrap().db, &saved, id(900)).is_err()
    );
    assert_eq!(lookup(&sm, 900).await, expected);
    drop(sm);
    let sm = StateMachine::open(dir.path()).unwrap();
    assert_eq!(lookup(&sm, 900).await, expected);
    assert_eq!(header(&sm).ledger.applied, sm.applied().await);
}

#[tokio::test]
async fn ordinary_member_apply_does_not_silently_repair_request_orphans() {
    for profile in profiles() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        let mut journal = Journal::new(authority().mode).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: cohort(profile),
            })
            .unwrap();
        let enable = if profile.is_legacy() {
            D1Command::EnableVersionedReads {
                shard: 8,
                epoch: 1,
                mode_id: [3; 16],
                journal,
            }
        } else {
            D1Command::EnableByteBoundReads {
                shard: 8,
                epoch: 1,
                mode_id: [3; 16],
                journal,
                generation_nonce: [17; 32],
            }
        };
        apply(
            &mut sm,
            vec![
                D1Command::Activate { shard: 8, epoch: 1 },
                D1Command::EnableManagedWrites {
                    shard: 8,
                    epoch: 1,
                    expected_sequence: 0,
                    mode_id: [3; 16],
                },
                D1Command::EnableParticipant {
                    shard: 8,
                    epoch: 1,
                    mode_id: [3; 16],
                    authority: authority(),
                },
                enable,
            ],
        )
        .await;
        let key = b"sm/request/orphan";
        sm.storage.as_ref().unwrap().db.put(key, b"bad").unwrap();
        let next = sm.applied().await.unwrap().index + 1;
        sm.apply([blank(next)]).await.unwrap();
        assert_eq!(
            sm.storage.as_ref().unwrap().db.get(key).unwrap().as_deref(),
            Some(b"bad".as_slice())
        );
        drop(sm);
        assert!(StateMachine::open(dir.path()).is_err());
    }
}
