//! Actual durable Raft apply consumes the active checkpoint after migration.
//! Cold/control/snapshot materialization is deliberately outside the tripwire.
use super::*;
use crate::participant_writes::Authority;
use crate::request_plans::{Part, Plan};
use crate::transaction_decisions::{
    CapacityProfile, Cohort, Command, Decision, Journal, Member, Participant,
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
    let entries: Vec<_> = commands
        .into_iter()
        .enumerate()
        .map(|(n, command)| entry(first + n as u64, command))
        .collect();
    let active = sm
        .state
        .read()
        .await
        .d1
        .as_ref()
        .and_then(|d| d.coordinator.as_ref())
        .and_then(Coordinator::active)
        .is_some();
    let results = if active {
        no_journal(sm.apply(entries)).await.unwrap()
    } else {
        sm.apply(entries).await.unwrap()
    };
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

// Each poll runs under the thread-local guard, including the real synchronous
// RocksDB staging/publication section. No executor or spawned task escapes it.
async fn no_journal<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        crate::transaction_decisions::without_full_journal(|| {
            std::future::Future::poll(future.as_mut(), cx)
        })
    })
    .await
}

async fn checkpoint(sm: &StateMachine) -> ActiveCheckpoint {
    let state = sm.state.read().await;
    let coordinator = state.d1.as_ref().unwrap().coordinator.as_ref().unwrap();
    assert!(
        coordinator.full().is_none(),
        "published coordinator owns a Journal"
    );
    let active = coordinator.active_arc().unwrap();
    assert!(Arc::ptr_eq(
        active,
        state.archive_cut.as_ref().unwrap().checkpoint_arc()
    ));
    let header: serde_json::Value = serde_json::from_slice(
        &sm.storage
            .as_ref()
            .unwrap()
            .db
            .get(D1_KEY)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(header["version"], 16);
    active.as_ref().clone()
}

fn begin(transaction: u64, participants: Vec<Participant>) -> D1Command {
    coordinator(Command::Begin {
        id: id(transaction),
        participants,
    })
}
fn prepare(transaction: u64, participant: Participant) -> D1Command {
    coordinator(Command::Prepared {
        id: id(transaction),
        participant,
    })
}

#[tokio::test]
async fn actual_apply_hole_drain_and_same_batch_first_plan_both_profiles() {
    for profile in profiles() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        let bound = checkpoint(&sm).await;
        // Binding and confirmation exact retries are ordinary checkpoint apply.
        apply(
            &mut sm,
            vec![coordinator(Command::BindCohort {
                cohort: cohort(profile),
            })],
        )
        .await;
        if let Some(grants) = bound.capacity_grants() {
            apply(
                &mut sm,
                vec![coordinator(Command::ConfirmCohortCapacity {
                    cohort: cohort(profile),
                    grants: grants.to_vec(),
                })],
            )
            .await;
        }
        let original = admit(profile, 1, 1);
        apply(&mut sm, vec![original.clone()]).await;
        let mut commands = Vec::new();
        for n in 2..=33 {
            commands.extend([admit(profile, n, n), abort(n)]);
        }
        apply(&mut sm, commands).await;
        let hole = checkpoint(&sm).await;
        assert_eq!(hole.resolved_prefix(), 0);
        assert_eq!(hole.active_transactions().len(), 33);
        assert!(hole.active_transactions()[1..]
            .iter()
            .all(|tx| tx.decision == Some(Decision::Abort)));
        assert!(rows(&sm, crate::decision_segments::PREFIX).is_empty());
        let sealed_raw = rows(&sm, event_ledger::PREFIX);
        apply(&mut sm, vec![abort(1)]).await;
        let drained = checkpoint(&sm).await;
        assert_eq!(drained.resolved_prefix(), 33);
        assert!(drained.active_transactions().is_empty());
        // Draining must not refund already charged original history.
        assert!(drained.reserved_bytes() >= hole.history_bytes());
        let next_raw = rows(&sm, event_ledger::PREFIX);
        for (key, value) in sealed_raw.iter().take(sealed_raw.len().saturating_sub(1)) {
            assert_eq!(next_raw.iter().find(|(k, _)| k == key).unwrap().1, *value);
        }
        assert_eq!(rows(&sm, crate::decision_segments::PREFIX).len(), 2);
        apply(&mut sm, vec![original, abort(1)]).await;
        assert_eq!(checkpoint(&sm).await, drained);

        // The first Admit and terminal drain occur in this same apply batch.
        // A retry proposing another valid Plan/transaction keeps the first one.
        apply(
            &mut sm,
            vec![admit(profile, 34, 34), abort(34), admit(profile, 34, 35)],
        )
        .await;
        let found = no_journal(sm.coordinator_request(7, 3, [1; 16], id(34)))
            .await
            .unwrap()
            .record
            .unwrap();
        assert_eq!(found.record.transaction_id, id(34));
        assert_eq!(Plan::decode(&found.record.payload).unwrap().id, id(34));
        assert_eq!(checkpoint(&sm).await.transaction_count(), 34);
        drop(sm);
        let mut reopened = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert!(checkpoint(&reopened).await.active_transactions().is_empty());
        apply(&mut reopened, vec![admit(profile, 34, 35), abort(34)]).await;
        assert_eq!(checkpoint(&reopened).await.transaction_count(), 34);
    }
}

#[tokio::test]
async fn actual_apply_prepared_commit_blank_membership_and_rejected_reuse() {
    for profile in profiles() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        let second = Participant {
            shard: 9,
            epoch: 1,
            mode: [5; 16],
            intent: [6; 32],
        };
        let participants = vec![participant(), second.clone()];
        apply(
            &mut sm,
            vec![begin(1, participants.clone()), prepare(1, participant())],
        )
        .await;
        let partial = checkpoint(&sm).await;
        assert_eq!(partial.active_transactions()[0].prepared, vec![true, false]);
        let first = sm.applied().await.unwrap().index + 1;
        let before = disk(&sm);
        let results = no_journal(sm.apply([entry(
            first,
            coordinator(Command::Decide {
                id: id(1),
                decision: Decision::Commit,
            }),
        )]))
        .await
        .unwrap();
        assert!(results[0].d1.as_ref().unwrap().is_err());
        assert_eq!(checkpoint(&sm).await, partial);
        for prefix in [event_ledger::PREFIX, crate::decision_segments::PREFIX] {
            assert_eq!(
                rows(&sm, prefix),
                before
                    .iter()
                    .filter(|(k, _)| k.starts_with(prefix))
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
        apply(
            &mut sm,
            vec![
                prepare(1, second),
                coordinator(Command::Decide {
                    id: id(1),
                    decision: Decision::Commit,
                }),
                begin(1, participants.clone()),
                prepare(1, participant()),
            ],
        )
        .await;
        let done = checkpoint(&sm).await;
        assert_eq!(done.resolved_prefix(), 1);
        assert!(done.active_transactions().is_empty());
        let index = sm.applied().await.unwrap().index + 1;
        let membership = Entry {
            log_id: LogId::new(CommittedLeaderId::new(2, 1), index + 1),
            payload: EntryPayload::Membership(openraft::Membership::new(
                vec![std::collections::BTreeSet::from([1])],
                BTreeMap::from([(1, BasicNode::default())]),
            )),
        };
        no_journal(sm.apply([blank(index), membership.clone()]))
            .await
            .unwrap();
        assert_eq!(sm.applied().await, Some(membership.log_id));
        assert_eq!(checkpoint(&sm).await, done);
        let raw = rows(&sm, event_ledger::PREFIX);
        let terminal = rows(&sm, crate::decision_segments::PREFIX);
        let mut changed = participants;
        changed[0].intent = [99; 32];
        let mut rejected = entry(index + 2, begin(1, changed));
        rejected.log_id = LogId::new(CommittedLeaderId::new(2, 1), index + 2);
        let results = no_journal(sm.apply([rejected])).await.unwrap();
        assert!(results[0].d1.as_ref().unwrap().is_err());
        assert_eq!(checkpoint(&sm).await, done);
        assert_eq!(rows(&sm, event_ledger::PREFIX), raw);
        assert_eq!(rows(&sm, crate::decision_segments::PREFIX), terminal);
    }
}

#[tokio::test]
async fn rejected_metadata_tampering_never_repairs_from_resident_checkpoint() {
    for field in ["events", "reserved_bytes", "resolved"] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, CapacityProfile::LegacyCountV1).await;
        apply(&mut sm, vec![admit(CapacityProfile::LegacyCountV1, 1, 1)]).await;
        let old = checkpoint(&sm).await;
        let marker = sm.applied().await;
        let db = &sm.storage.as_ref().unwrap().db;
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&db.get(D1_KEY).unwrap().unwrap()).unwrap();
        let value = metadata["checkpoint"]["header"][field].as_u64().unwrap();
        metadata["checkpoint"]["header"][field] = (value + 1).into();
        db.put(D1_KEY, serde_json::to_vec(&metadata).unwrap())
            .unwrap();
        let corrupt = disk(&sm);
        assert!(
            no_journal(sm.apply([entry(marker.unwrap().index + 1, abort(1))]))
                .await
                .is_err()
        );
        assert_eq!(disk(&sm), corrupt);
        assert_eq!(sm.applied().await, marker);
        assert_eq!(
            sm.state
                .read()
                .await
                .d1
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .active()
                .unwrap(),
            &old
        );
        drop(sm);
        assert!(StateMachine::open(dir.path()).is_err());
    }
}

#[tokio::test]
async fn sync_failure_preserves_checkpoint_raw_terminals_and_marker_then_retries() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::ByteBound16MiBV1).await;
    apply(
        &mut sm,
        vec![admit(CapacityProfile::ByteBound16MiBV1, 1, 1)],
    )
    .await;
    let before = disk(&sm);
    let old = checkpoint(&sm).await;
    let marker = sm.applied().await;
    sm.storage
        .as_ref()
        .unwrap()
        .fail_next_write
        .store(true, Ordering::Relaxed);
    let index = marker.unwrap().index + 1;
    assert!(no_journal(sm.apply([entry(index, abort(1))]))
        .await
        .is_err());
    assert_eq!(disk(&sm), before);
    assert_eq!(checkpoint(&sm).await, old);
    assert_eq!(sm.applied().await, marker);
    drop(sm);
    let mut reopened = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_eq!(checkpoint(&reopened).await, old);
    apply(&mut reopened, vec![abort(1)]).await;
    assert_eq!(checkpoint(&reopened).await.resolved_prefix(), 1);
}

#[tokio::test]
async fn exact_snapshot_repairs_damaged_raw_and_successor_rejects_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::LegacyCountV1).await;
    apply(
        &mut sm,
        vec![admit(CapacityProfile::LegacyCountV1, 1, 1), abort(1)],
    )
    .await;
    // Snapshot construction intentionally materializes complete raw authority.
    let old = sm.build_snapshot().await.unwrap();
    let original = checkpoint(&sm).await;
    let raw_key = rows(&sm, event_ledger::PREFIX)[0].0.clone();
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .put(&raw_key, b"damaged")
        .unwrap();
    let index = sm.applied().await.unwrap().index + 1;
    assert!(no_journal(sm.apply([blank(index)])).await.is_err());
    sm.install_snapshot(
        &old.meta,
        Box::new(Cursor::new(old.snapshot.get_ref().clone())),
    )
    .await
    .unwrap();
    assert_eq!(checkpoint(&sm).await, original);
    let peer_dir = tempfile::tempdir().unwrap();
    let mut peer = Arc::new(StateMachine::open(peer_dir.path()).unwrap());
    peer.install_snapshot(
        &old.meta,
        Box::new(Cursor::new(old.snapshot.get_ref().clone())),
    )
    .await
    .unwrap();
    apply(
        &mut peer,
        vec![admit(CapacityProfile::LegacyCountV1, 2, 2), abort(2)],
    )
    .await;
    let successor = peer.build_snapshot().await.unwrap();
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .put(&raw_key, b"damaged again")
        .unwrap();
    sm.install_snapshot(&successor.meta, successor.snapshot)
        .await
        .unwrap();
    assert_eq!(checkpoint(&sm).await.resolved_prefix(), 2);
    let before = disk(&sm);
    assert!(sm.install_snapshot(&old.meta, old.snapshot).await.is_err());
    assert_eq!(disk(&sm), before);
    drop(sm);
    let mut reopened = Arc::new(StateMachine::open(dir.path()).unwrap());
    apply(
        &mut reopened,
        vec![admit(CapacityProfile::LegacyCountV1, 1, 99), abort(1)],
    )
    .await;
    assert_eq!(checkpoint(&reopened).await.resolved_prefix(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_checkpoint_crash_child() -> anyhow::Result<()> {
    let Some(path) = std::env::var_os("D2_ACTIVE_CHECKPOINT_CRASH_DIRECTORY") else {
        return Ok(());
    };
    let path = std::path::PathBuf::from(path);
    let mut sm = Arc::new(StateMachine::open(path.join("state.db"))?);
    setup(&mut sm, CapacityProfile::ByteBound16MiBV1).await;
    apply(
        &mut sm,
        vec![admit(CapacityProfile::ByteBound16MiBV1, 1, 1)],
    )
    .await;
    assert_eq!(checkpoint(&sm).await.active_transactions().len(), 1);
    let index = sm.applied().await.unwrap().index + 1;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (_release, gate) = std::sync::mpsc::channel();
    *sm.apply_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
        entered,
        release: gate,
    });
    let writer = tokio::spawn(async move { no_journal(sm.apply([entry(index, abort(1))])).await });
    ready.await?;
    std::fs::write(path.join("synced"), index.to_string())?;
    // Parent kills the complete process while the state write lock and durable
    // publication gate are held, so this is an actual lost-ACK crash boundary.
    writer.await??;
    anyhow::bail!("parent must kill child after sync and before publication")
}

#[tokio::test]
async fn process_death_after_active_sync_recovers_checkpoint_and_original_retry(
) -> anyhow::Result<()> {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir()?;
    let mut child = Child(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "state_machine::active_checkpoint_state_tests::active_checkpoint_crash_child",
                "--nocapture",
            ])
            .env("D2_ACTIVE_CHECKPOINT_CRASH_DIRECTORY", dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?,
    );
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        while !dir.path().join("synced").exists() {
            anyhow::ensure!(
                child.0.try_wait()?.is_none(),
                "active checkpoint child exited before sync"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    child.0.kill()?;
    child.0.wait()?;
    let index: u64 = std::fs::read_to_string(dir.path().join("synced"))?.parse()?;
    let mut sm = Arc::new(StateMachine::open(dir.path().join("state.db"))?);
    assert_eq!(sm.applied().await.unwrap().index, index);
    let recovered = checkpoint(&sm).await;
    assert_eq!(recovered.resolved_prefix(), 1);
    assert!(recovered.active_transactions().is_empty());
    let found = no_journal(sm.coordinator_request(7, 3, [1; 16], id(1)))
        .await
        .unwrap()
        .record
        .unwrap();
    assert_eq!(found.record.transaction_id, id(1));
    assert_eq!(found.transaction.decision, Some(Decision::Abort));
    apply(
        &mut sm,
        vec![admit(CapacityProfile::ByteBound16MiBV1, 1, 2), abort(1)],
    )
    .await;
    assert_eq!(checkpoint(&sm).await, recovered);
    Ok(())
}

#[tokio::test]
async fn actual_disk15_migration_is_atomic_preserves_old_ids_plans_and_versions() {
    for profile in profiles() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        apply(
            &mut sm,
            vec![admit(profile, 900, 1), abort(1), admit(profile, 901, 2)],
        )
        .await;
        let original = sm
            .coordinator_request(7, 3, [1; 16], id(900))
            .await
            .unwrap();
        let old_v0 = sm
            .coordinator_terminal_prefix(7, 3, [1; 16], 0)
            .await
            .unwrap();
        let old_v1 = sm
            .coordinator_terminal_prefix(7, 3, [1; 16], 1)
            .await
            .unwrap();
        let zero = crate::transaction_decisions::HistoryCursor::empty(authority(), cohort(profile))
            .unwrap();
        let history = sm
            .coordinator_history_page(7, 3, [1; 16], &zero, None)
            .await
            .unwrap();
        let raw = rows(&sm, event_ledger::PREFIX);
        let terminals = rows(&sm, crate::decision_segments::PREFIX);
        let marker = sm.applied().await;
        // Build the real canonical legacy schema through its retained typed
        // writer. Do not delete checkpoint fields from arbitrary JSON values.
        {
            let state = sm.state.read().await;
            let full = sm.load_disk_state(&state).unwrap().unwrap();
            assert!(full.state.coordinator.as_ref().unwrap().full().is_some());
            let mut batch = WriteBatch::default();
            let staged = event_ledger::stage(
                &mut batch,
                full.ledger.as_ref(),
                Some(&full.state),
                marker,
                false,
            )
            .unwrap();
            let metadata = state_codec::d1_metadata_bytes(&full.state, staged.as_ref()).unwrap();
            let header: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
            assert_eq!(header["version"], 15);
            assert!(header.get("checkpoint").is_none());
            batch.put(D1_KEY, metadata);
            sm.storage.as_ref().unwrap().write_sync(batch).unwrap();
        }
        assert_eq!(rows(&sm, event_ledger::PREFIX), raw);
        assert_eq!(rows(&sm, crate::decision_segments::PREFIX), terminals);
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert!(sm
            .state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .coordinator
            .as_ref()
            .unwrap()
            .full()
            .is_some());
        assert_eq!(
            sm.coordinator_request(7, 3, [1; 16], id(900))
                .await
                .unwrap(),
            original
        );
        assert_eq!(
            sm.coordinator_history_page(7, 3, [1; 16], &zero, None)
                .await
                .unwrap(),
            history
        );
        // Legacy corruption must fail before any opportunistic migration. The
        // test restores a saved fixture row explicitly, never via normal apply.
        let (key, value) = &raw[0];
        sm.storage
            .as_ref()
            .unwrap()
            .db
            .put(key, b"broken legacy raw")
            .unwrap();
        let corrupt = disk(&sm);
        assert!(sm
            .apply([entry(marker.unwrap().index + 1, abort(2))])
            .await
            .is_err());
        assert_eq!(disk(&sm), corrupt);
        assert_eq!(sm.applied().await, marker);
        drop(sm);
        assert!(StateMachine::open(dir.path()).is_err());
        {
            let db = DB::open_default(dir.path()).unwrap();
            db.put(key, value).unwrap();
        }
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        let legacy = disk(&sm);
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm
            .apply([entry(marker.unwrap().index + 1, abort(2))])
            .await
            .is_err());
        assert_eq!(disk(&sm), legacy);
        assert_eq!(sm.applied().await, marker);
        assert!(sm
            .state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .coordinator
            .as_ref()
            .unwrap()
            .full()
            .is_some());
        // The first successful legacy apply is allowed to use complete replay.
        // Every later helper apply must run with the no-Journal tripwire.
        apply(&mut sm, vec![abort(2)]).await;
        assert_eq!(checkpoint(&sm).await.resolved_prefix(), 2);
        let recovered = no_journal(sm.coordinator_request(7, 3, [1; 16], id(900)))
            .await
            .unwrap();
        assert_eq!(recovered.record, original.record);
        assert_eq!(
            sm.coordinator_terminal_prefix(7, 3, [1; 16], 0)
                .await
                .unwrap(),
            old_v0
        );
        assert_eq!(
            sm.coordinator_terminal_prefix(7, 3, [1; 16], 1)
                .await
                .unwrap(),
            old_v1
        );
        apply(&mut sm, vec![admit(profile, 900, 99), abort(1)]).await;
        assert_eq!(checkpoint(&sm).await.transaction_count(), 2);
        drop(sm);
        let reopened = StateMachine::open(dir.path()).unwrap();
        assert_eq!(checkpoint(&reopened).await.resolved_prefix(), 2);
        assert_eq!(
            no_journal(reopened.coordinator_request(7, 3, [1; 16], id(900)))
                .await
                .unwrap()
                .record,
            original.record
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn actual_managed_tcp_commands_use_no_journal_preflight_and_raft_apply() {
    let _resources = crate::managed_rpc::test_resource_guard().await;
    use crate::managed_rpc::{Client, Identity, NodeRef, Role, Server};
    use crate::versioned_reads::Target;
    use std::time::Duration;

    let profile = CapacityProfile::LegacyCountV1;
    let identity = Identity {
        group: 701,
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
            "active checkpoint command leader",
        )
        .await
        .unwrap();
    // Initialization and migration are real Raft writes. Server::bind may use
    // explicit complete control materialization, before enabling the tripwire.
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
    let before = checkpoint(&node.state_machine).await;
    let raw_before = rows(&node.state_machine, event_ledger::PREFIX);
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
    // This switch installs the guard on the server preflight and actual Raft
    // apply worker, not just the calling test's thread-local executor context.
    node.state_machine.forbid_coordinator_journal(true);
    let mut marker = node.state_machine.applied().await.unwrap();
    for command in [
        begin(1, vec![participant()]),
        prepare(1, participant()),
        coordinator(Command::Decide {
            id: id(1),
            decision: Decision::Commit,
        }),
        admit(profile, 900, 2),
        abort(2),
        admit(profile, 900, 99),
        abort(2),
    ] {
        let D1Command::Coordinator { command, .. } = command else {
            unreachable!("all remote commands target the coordinator");
        };
        remote.command(&authority(), 3, command).await.unwrap();
        let next = node.state_machine.applied().await.unwrap();
        assert!(
            next.index > marker.index,
            "remote command must reach Raft apply, even on retry"
        );
        marker = next;
    }
    let after = checkpoint(&node.state_machine).await;
    assert_eq!(after.event_count(), before.event_count() + 5);
    assert_eq!(after.transaction_count(), 2);
    assert_eq!(after.request_count(), 1);
    assert_eq!(after.resolved_prefix(), 2);
    assert!(after.active_transactions().is_empty());
    assert_ne!(rows(&node.state_machine, event_ledger::PREFIX), raw_before);
    let recovered = remote
        .request(&authority(), 3, id(900))
        .await
        .unwrap()
        .record
        .unwrap();
    assert_eq!(recovered.record.transaction_id, id(2));
    assert_eq!(Plan::decode(&recovered.record.payload).unwrap().id, id(2));
    assert_eq!(recovered.transaction.decision, Some(Decision::Abort));
    node.state_machine.forbid_coordinator_journal(false);
    server.shutdown().await;
    let closing_state = node.state_machine.clone();
    cluster.shutdown(&node).await;
    drop(node);
    close_after_background_owners(closing_state, Duration::from_secs(5))
        .await
        .expect("background state-machine owners must release before fixture teardown");
}

// OpenRaft 0.9.25 shutdown joins its core/tick, not the state-machine worker.
// Keep the final owner until that worker (and any snapshot worker) has dropped
// its Arc, then close RocksDB synchronously before fixture teardown.
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
async fn active_runtime_rejects_implicit_serialization_but_snapshot_materializes_authority() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    let profile = CapacityProfile::LegacyCountV1;
    setup(&mut sm, profile).await;
    apply(
        &mut sm,
        vec![
            admit(profile, 900, 1),
            abort(1),
            begin(2, vec![participant()]),
        ],
    )
    .await;
    let published = checkpoint(&sm).await;
    let shared = {
        let state = sm.state.read().await;
        let d1 = state.d1.as_ref().unwrap();
        let coordinator = d1.coordinator.as_ref().unwrap();
        for result in [serde_json::to_vec(coordinator), serde_json::to_vec(d1)] {
            let error =
                result.expect_err("active runtime must not silently encode a legacy Journal");
            assert!(error
                .to_string()
                .contains("requires explicit complete snapshot materialization"));
        }
        Arc::clone(coordinator.active_arc().unwrap())
    };
    // This explicit boundary is allowed to reconstruct full raw authority.
    let snapshot = sm.build_snapshot().await.unwrap();
    let logical: SnapshotV1 = serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
    let complete = logical.d1.coordinator.as_ref().unwrap().full().unwrap();
    assert_eq!(complete.transaction_count(), 2);
    assert_eq!(complete.resolved_prefix(), 1);
    assert_eq!(complete.request(id(900)).unwrap().transaction_id, id(1));
    assert_eq!(
        complete.transaction(id(1)).unwrap().decision,
        Some(Decision::Abort)
    );
    assert_eq!(complete.transaction(id(2)).unwrap().decision, None);
    assert_eq!(checkpoint(&sm).await, published);
    let state = sm.state.read().await;
    assert!(Arc::ptr_eq(
        &shared,
        state
            .d1
            .as_ref()
            .unwrap()
            .coordinator
            .as_ref()
            .unwrap()
            .active_arc()
            .unwrap()
    ));
    assert!(Arc::ptr_eq(
        &shared,
        state.archive_cut.as_ref().unwrap().checkpoint_arc()
    ));
}

#[tokio::test]
async fn structurally_valid_checkpoint_substitutions_require_exact_raw_replay() {
    for fault in ["transaction_id", "participant_intent", "grant_nonce"] {
        let profile = if fault == "grant_nonce" {
            CapacityProfile::ByteBound16MiBV1
        } else {
            CapacityProfile::LegacyCountV1
        };
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        apply(&mut sm, vec![admit(profile, 900, 1)]).await;
        let original = checkpoint(&sm).await;
        let marker = sm.applied().await;
        let raw = rows(&sm, event_ledger::PREFIX);
        let mut value = serde_json::to_value(&original).unwrap();
        // Same decimal widths preserve canonical event/pending byte charges.
        // Every replacement remains structurally valid and nonzero.
        match fault {
            "transaction_id" => value["active"][0]["id"][15] = 2.into(),
            "participant_intent" => value["active"][0]["participants"][0]["intent"][0] = 5.into(),
            "grant_nonce" => {
                value["header"]["capacity_grants"][0]["activation"]["nonce"][0] = 7.into()
            }
            _ => unreachable!(),
        }
        let changed: ActiveCheckpoint = serde_json::from_value(value).unwrap();
        changed
            .validate()
            .expect("substitution must pass structural validation");
        let canonical = changed.encode().unwrap();
        assert_eq!(ActiveCheckpoint::decode(&canonical).unwrap(), changed);
        assert_eq!(canonical.len(), original.encode().unwrap().len());
        assert_eq!(changed.reserved_bytes(), original.reserved_bytes());
        assert_eq!(changed.pending_bytes(), original.pending_bytes());
        assert_ne!(changed, original);
        let (mut shell, ledger) = {
            let state = sm.state.read().await;
            (
                state.d1.as_ref().unwrap().clone(),
                state.archive_cut.as_ref().unwrap().descriptor().clone(),
            )
        };
        shell.coordinator = Some(Coordinator::Active(Arc::new(changed.clone())));
        // Use the actual typed writer to preserve exact canonical field order.
        // The original raw descriptor/root, counters and complete marker stay
        // unchanged; the checkpoint substitution is the only disk change.
        let bytes = state_codec::active_metadata_bytes(&shell, &ledger, &changed).unwrap();
        state_codec::validate_active_metadata(&bytes, &ledger, &changed).unwrap();
        sm.storage.as_ref().unwrap().db.put(D1_KEY, bytes).unwrap();
        let corrupt = disk(&sm);
        assert!(
            no_journal(sm.apply([entry(marker.unwrap().index + 1, abort(1))]))
                .await
                .is_err()
        );
        assert_eq!(disk(&sm), corrupt);
        assert_eq!(rows(&sm, event_ledger::PREFIX), raw);
        assert_eq!(sm.applied().await, marker);
        assert_eq!(checkpoint(&sm).await, original);
        drop(sm);
        // Demand the exact replay-comparison error, not a malformed codec,
        // noncanonical byte sequence, or structural-validation rejection.
        let error = StateMachine::open(dir.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("persisted checkpoint disagrees with strict raw replay"),
            "{fault}: {error:#}"
        );
    }
}

async fn row_witness_mode<F: std::future::Future>(original: bool, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        if original {
            event_ledger::with_original_coordinator_verify(|| {
                std::future::Future::poll(future.as_mut(), cx)
            })
        } else {
            std::future::Future::poll(future.as_mut(), cx)
        }
    })
    .await
}

#[tokio::test]
#[ignore = "paired durable apply measurement including witness staging, not Submit/lifecycle"]
async fn row_witness_real_apply_cost() {
    let build = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    for resolved in [false, true] {
        for count in [32, 128, 512] {
            let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
            let mut stores = [
                Arc::new(StateMachine::open(dirs[0].path()).unwrap()),
                Arc::new(StateMachine::open(dirs[1].path()).unwrap()),
            ];
            for (slot, sm) in stores.iter_mut().enumerate() {
                row_witness_mode(slot == 0, setup(sm, CapacityProfile::LegacyCountV1)).await;
                let commands = (1..=count)
                    .flat_map(|n| {
                        let begin = coordinator(Command::Begin {
                            id: id(n),
                            participants: vec![participant()],
                        });
                        if resolved {
                            vec![begin, abort(n)]
                        } else {
                            vec![begin]
                        }
                    })
                    .collect();
                row_witness_mode(slot == 0, apply(sm, commands)).await;
            }
            assert_eq!(disk(&stores[0]), disk(&stores[1]));
            let mut times = [Vec::new(), Vec::new()];
            for round in 0..24 {
                let n = count + round + 1;
                for slot in if round % 2 == 0 { [0, 1] } else { [1, 0] } {
                    let commands = vec![
                        coordinator(Command::Begin {
                            id: id(n),
                            participants: vec![participant()],
                        }),
                        abort(n),
                    ];
                    let now = std::time::Instant::now();
                    row_witness_mode(slot == 0, apply(&mut stores[slot], commands)).await;
                    if round >= 4 {
                        times[slot].push(now.elapsed().as_micros());
                    }
                }
                assert_eq!(disk(&stores[0]), disk(&stores[1]));
                assert_eq!(checkpoint(&stores[0]).await, checkpoint(&stores[1]).await);
            }
            for samples in &mut times {
                samples.sort_unstable();
            }
            eprintln!("ROW_WITNESS_APPLY initial_tx={count} resolved_prefix={resolved} original_median_us={} witness_median_us={} samples=20 build={build} command=Begin+Abort same_batch sync=true", times[0][10], times[1][10]);
            eprintln!(
                "ROW_WITNESS_APPLY_SAMPLES original_us={:?} witness_us={:?}",
                times[0], times[1]
            );
        }
    }
}
