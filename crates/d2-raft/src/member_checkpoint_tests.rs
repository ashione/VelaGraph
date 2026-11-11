//! Acceptance seams for the durable, consumed member checkpoint path.
//! Whole-Journal construction below is test input generation, never member work.
use super::*;
use crate::participant_writes::{Authority, Status};
use crate::transaction_decisions::{
    CapacityActivation, CapacityProfile, Cohort, Command, Decision, HistoryCursor, HistoryPage,
    Journal, Member, MemberCapacityGrant, Participant,
};
use openraft::CommittedLeaderId;

fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn cohort(profile: CapacityProfile) -> Cohort {
    Cohort {
        profile,
        coordinator_shard: 9,
        coordinator_epoch: 1,
        members: vec![7, 8]
            .into_iter()
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .collect(),
    }
}
fn id(n: u8) -> [u8; 16] {
    [n; 16]
}
fn descriptor(shard: u64) -> Participant {
    Participant {
        shard,
        epoch: 1,
        mode: [shard as u8; 16],
        intent: [4; 32],
    }
}
fn empty(profile: CapacityProfile) -> Journal {
    let mut journal = Journal::new([9; 16]).unwrap();
    journal
        .apply(Command::BindCohort {
            cohort: cohort(profile),
        })
        .unwrap();
    journal
}
fn begin(n: u8) -> Command {
    Command::Begin {
        id: id(n),
        participants: vec![descriptor(7)],
    }
}
fn abort(n: u8) -> Command {
    Command::Decide {
        id: id(n),
        decision: Decision::Abort,
    }
}
fn request(n: u8) -> crate::D1WriteRequest {
    crate::D1WriteRequest {
        shard: 7,
        epoch: 1,
        request_id: id(n),
        expected_sequence: Some(0),
        operations: vec![WriteOp::put("x", "committed")],
    }
}
fn prepare(n: u8) -> D1Command {
    D1Command::PrepareParticipant {
        authority: authority(),
        participant: descriptor(7),
        request: request(n),
    }
}
fn sync(page: HistoryPage) -> D1Command {
    D1Command::SyncHistory {
        authority: authority(),
        shard: 7,
        epoch: 1,
        mode_id: [7; 16],
        page: Box::new(page),
    }
}
fn advance(ordinal: u64, cut: &HistoryCursor) -> D1Command {
    D1Command::AdvanceRetained {
        authority: authority(),
        shard: 7,
        epoch: 1,
        mode_id: [7; 16],
        ordinal,
        cut: cut.clone(),
    }
}
fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(LogCommand::D1V1(command)),
    }
}
async fn apply(sm: &mut Arc<StateMachine>, command: D1Command) -> Result<D1Result, D1Error> {
    let next = sm.applied().await.map_or(1, |log| log.index + 1);
    sm.apply([entry(next, command)])
        .await
        .unwrap()
        .remove(0)
        .d1
        .unwrap()
}
async fn setup(sm: &mut Arc<StateMachine>, profile: CapacityProfile) {
    for command in [
        D1Command::Activate { shard: 7, epoch: 1 },
        D1Command::EnableManagedWrites {
            shard: 7,
            epoch: 1,
            expected_sequence: 0,
            mode_id: [7; 16],
        },
        D1Command::EnableParticipant {
            shard: 7,
            epoch: 1,
            mode_id: [7; 16],
            authority: authority(),
        },
        if profile.is_legacy() {
            D1Command::EnableVersionedReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: empty(profile),
            }
        } else {
            D1Command::EnableByteBoundReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: empty(profile),
                generation_nonce: [17; 32],
            }
        },
    ] {
        apply(sm, command).await.unwrap();
    }
    assert_active(sm).await;
    sm.forbid_coordinator_journal(true);
}
async fn assert_active(sm: &StateMachine) {
    let state = sm.state.read().await;
    let history = &state
        .d1
        .as_ref()
        .unwrap()
        .visibility
        .as_ref()
        .unwrap()
        .journal;
    assert!(history.full().is_none());
    assert!(history.active_arc().is_some());
}
async fn status(sm: &StateMachine) -> (HistoryCursor, u64) {
    sm.participant_history_status(7, 1, [7; 16]).await.unwrap()
}
async fn sync_all(sm: &mut Arc<StateMachine>, journal: &Journal) -> HistoryCursor {
    let (mut cursor, _) = status(sm).await;
    let cut = journal.history_cursor().unwrap();
    loop {
        let page = journal.history_page(&cursor, Some(&cut)).unwrap();
        cursor = page.end.clone();
        apply(sm, sync(page)).await.unwrap();
        if cursor == cut {
            return cut;
        }
    }
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

#[tokio::test]
async fn durable_member_freezes_old_cut_before_already_applied_retry() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::LegacyCountV1).await;
    let mut journal = empty(CapacityProfile::LegacyCountV1);
    for command in [begin(1), begin(2), abort(2)] {
        journal.apply(command).unwrap();
    }
    let old_cut = sync_all(&mut sm, &journal).await;
    assert_eq!(status(&sm).await, (old_cut.clone(), 0));
    apply(&mut sm, prepare(1)).await.unwrap();
    journal.apply(abort(1)).unwrap();
    let cut = sync_all(&mut sm, &journal).await;
    assert_eq!(status(&sm).await, (cut.clone(), 0));
    {
        let state = sm.state.read().await;
        assert_eq!(
            state
                .d1
                .as_ref()
                .unwrap()
                .visibility
                .as_ref()
                .unwrap()
                .journal
                .resolved_prefix(),
            2
        );
    }
    assert_eq!(
        sm.participant_visibility_info(7, 1, [7; 16])
            .await
            .unwrap()
            .applied_prefix,
        0
    );
    for ordinal in [1, 2] {
        apply(&mut sm, advance(ordinal, &cut)).await.unwrap();
    }
    assert_eq!(
        apply(&mut sm, advance(2, &old_cut)).await,
        Err(D1Error::VisibilityUnresolved)
    );
    assert_eq!(status(&sm).await, (cut.clone(), 2));
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(2)).await.unwrap(),
        Status::Aborted
    );
    assert_eq!(
        apply(&mut sm, prepare(1)).await.unwrap(),
        D1Result::Participant(Status::Aborted)
    );
    assert_eq!(sm.get_raw(b"x").await, None);
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_active(&sm).await;
    sm.forbid_coordinator_journal(true);
    assert_eq!(status(&sm).await, (cut, 2));
    assert_eq!(
        apply(&mut sm, advance(2, &old_cut)).await,
        Err(D1Error::VisibilityUnresolved)
    );
}

#[tokio::test]
async fn durable_member_same_batch_overlay_preserves_frozen_hole() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::LegacyCountV1).await;
    let mut journal = empty(CapacityProfile::LegacyCountV1);
    for command in [begin(1), begin(2), abort(2)] {
        journal.apply(command).unwrap();
    }
    let old_cut = journal.history_cursor().unwrap();
    journal.apply(abort(1)).unwrap();
    let page = journal.history_page(&status(&sm).await.0, None).unwrap();
    let cut = page.cut.clone();
    let next = sm.applied().await.unwrap().index + 1;
    let results = sm
        .apply([
            entry(next, sync(page)),
            entry(next + 1, prepare(1)),
            entry(next + 2, advance(2, &old_cut)),
            entry(next + 3, advance(1, &cut)),
            entry(next + 4, prepare(1)),
            entry(next + 5, advance(2, &cut)),
        ])
        .await
        .unwrap();
    assert!(results[0].d1.as_ref().unwrap().is_ok());
    // The immutable history witness is unchanged across these entries, while A
    // changes. Prepare must consult the current local candidate, not cache A0.
    assert_eq!(results[1].d1, Some(Err(D1Error::VisibilityUnresolved)));
    assert_eq!(results[2].d1, Some(Err(D1Error::VisibilityUnresolved)));
    assert!(results[3].d1.as_ref().unwrap().is_ok());
    assert_eq!(
        results[4].d1,
        Some(Ok(D1Result::Participant(Status::Aborted)))
    );
    assert!(results[5].d1.as_ref().unwrap().is_ok());
    assert_eq!(status(&sm).await, (cut, 2));
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(1)).await.unwrap(),
        Status::Aborted
    );
}

#[tokio::test]
async fn durable_member_bad_page_rolls_back_partial_candidate_and_accepted_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::LegacyCountV1).await;
    let base = status(&sm).await.0;
    let events = vec![begin(1), begin(0)];
    let end = base.extend(&events).unwrap();
    let bad = HistoryPage {
        version: base.version,
        base: base.clone(),
        end: end.clone(),
        cut: end,
        events,
    };
    bad.validate().unwrap(); // Structurally valid, second admission semantically invalid.
    let mut journal = empty(CapacityProfile::LegacyCountV1);
    for command in [begin(2), abort(2)] {
        journal.apply(command).unwrap();
    }
    let good = journal.history_page(&base, None).unwrap();
    let cut = good.cut.clone();
    let next = sm.applied().await.unwrap().index + 1;
    let results = sm
        .apply([
            entry(next, sync(bad)),
            entry(next + 1, sync(good)),
            entry(next + 2, advance(1, &cut)),
        ])
        .await
        .unwrap();
    assert!(results[0].d1.as_ref().unwrap().is_err());
    assert!(results[1].d1.as_ref().unwrap().is_ok());
    assert!(results[2].d1.as_ref().unwrap().is_ok());
    assert_eq!(status(&sm).await, (cut.clone(), 1));
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(1)).await.unwrap(),
        Status::Unknown
    );
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(2)).await.unwrap(),
        Status::Aborted
    );
    drop(sm);
    let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_active(&sm).await;
    sm.forbid_coordinator_journal(true);
    assert_eq!(status(&sm).await, (cut, 1));
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(1)).await.unwrap(),
        Status::Unknown
    );
}

#[tokio::test]
async fn durable_member_pre_sync_failure_publishes_no_raw_checkpoint_or_marker() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::LegacyCountV1).await;
    let mut journal = empty(CapacityProfile::LegacyCountV1);
    for command in [begin(1), abort(1)] {
        journal.apply(command).unwrap();
    }
    let page = journal.history_page(&status(&sm).await.0, None).unwrap();
    let cut = page.cut.clone();
    for command in [sync(page), advance(1, &cut)] {
        let before_disk = disk(&sm);
        let before_state = sm.state.read().await.d1.clone();
        let before_status = status(&sm).await;
        let marker = sm.applied().await;
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm
            .apply([entry(marker.unwrap().index + 1, command.clone())])
            .await
            .is_err());
        assert_eq!(disk(&sm), before_disk);
        assert_eq!(sm.state.read().await.d1, before_state);
        assert_eq!(status(&sm).await, before_status);
        assert_eq!(sm.applied().await, marker);
        drop(sm);
        sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        sm.forbid_coordinator_journal(true);
        assert_eq!(status(&sm).await, before_status);
        assert_eq!(sm.applied().await, marker);
        apply(&mut sm, command).await.unwrap();
    }
    assert_eq!(status(&sm).await, (cut, 1));
}

#[tokio::test]
async fn durable_member_missing_raw_or_changed_metadata_is_not_unknown() {
    for corrupt_metadata in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, CapacityProfile::LegacyCountV1).await;
        let before = sm.state.read().await.d1.clone();
        let marker = sm.applied().await;
        let base = status(&sm).await.0;
        let db = &sm.storage.as_ref().unwrap().db;
        if corrupt_metadata {
            db.put(D1_KEY, b"{}").unwrap();
        } else {
            let key = db
                .iterator(IteratorMode::From(event_ledger::PREFIX, Direction::Forward))
                .next()
                .unwrap()
                .unwrap()
                .0;
            assert!(key.starts_with(event_ledger::PREFIX));
            db.delete(key).unwrap();
        }
        assert!(sm.participant_history_status(7, 1, [7; 16]).await.is_err());
        assert!(sm.participant_visibility_info(7, 1, [7; 16]).await.is_err());
        assert!(sm.participant_status(7, 1, [7; 16], id(99)).await.is_err());
        assert!(sm
            .apply([entry(marker.unwrap().index + 1, advance(1, &base))])
            .await
            .is_err());
        assert_eq!(sm.state.read().await.d1, before);
        assert_eq!(sm.applied().await, marker);
    }
}

#[tokio::test]
async fn durable_byte_member_checks_original_plan_before_idempotent_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    let profile = CapacityProfile::ByteBound16MiBV1;
    setup(&mut sm, profile).await;
    let grant = sm
        .participant_capacity_grant(7, 1, [7; 16], &authority(), &cohort(profile))
        .await
        .unwrap();
    assert_eq!(grant.activation.nonce, [17; 32]);
    assert_eq!(
        apply(&mut sm, prepare(1)).await,
        Err(D1Error::CoordinatorRejected(
            crate::transaction_decisions::Error::CapacityNotConfirmed
        ))
    );
    let mut journal = empty(profile);
    journal
        .apply(Command::ConfirmCohortCapacity {
            cohort: cohort(profile),
            grants: vec![
                grant.clone(),
                MemberCapacityGrant {
                    member: cohort(profile).members[1].clone(),
                    activation: CapacityActivation {
                        nonce: [18; 32],
                        log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                    },
                },
            ],
        })
        .unwrap();
    let plan = crate::request_plans::Plan {
        version: 1,
        id: id(1),
        authority: authority(),
        cohort: cohort(profile),
        validation_version: 0,
        parts: vec![crate::request_plans::Part {
            descriptor: descriptor(7),
            request: request(1),
        }],
    };
    journal
        .apply(Command::AdmitRequest {
            id: id(99),
            request: b"original".to_vec(),
            payload: plan.encode().unwrap(),
            transaction_id: id(1),
            participants: vec![descriptor(7)],
        })
        .unwrap();
    let page = journal.history_page(&status(&sm).await.0, None).unwrap();
    let next = sm.applied().await.unwrap().index + 1;
    let results = sm
        .apply([entry(next, sync(page)), entry(next + 1, prepare(1))])
        .await
        .unwrap();
    assert!(results
        .iter()
        .all(|result| result.d1.as_ref().unwrap().is_ok()));
    for command in [
        Command::Prepared {
            id: id(1),
            participant: descriptor(7),
        },
        Command::Decide {
            id: id(1),
            decision: Decision::Commit,
        },
    ] {
        journal.apply(command).unwrap();
    }
    let cut = sync_all(&mut sm, &journal).await;
    assert_eq!(status(&sm).await.1, 0);
    apply(&mut sm, advance(1, &cut)).await.unwrap();
    assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
    assert_eq!(
        apply(&mut sm, prepare(1)).await.unwrap(),
        D1Result::Participant(Status::Committed { sequence: 1 })
    );
    let mut changed = request(1);
    changed.operations = vec![WriteOp::put("x", "changed")];
    assert_eq!(
        apply(
            &mut sm,
            D1Command::PrepareParticipant {
                authority: authority(),
                participant: descriptor(7),
                request: changed
            }
        )
        .await,
        Err(D1Error::IdempotencyConflict)
    );
    assert_eq!(
        sm.participant_capacity_grant(7, 1, [7; 16], &authority(), &cohort(profile))
            .await
            .unwrap(),
        grant
    );
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(1)).await.unwrap(),
        Status::Committed { sequence: 1 }
    );
    assert_active(&sm).await;
}

fn rows(sm: &StateMachine, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    disk(sm)
        .into_iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .collect()
}
fn blank(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Blank,
    }
}
async fn no_member_journal<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        crate::transaction_decisions::without_full_journal(|| {
            std::future::Future::poll(future.as_mut(), cx)
        })
    })
    .await
}

// Return exact admitted original commands with Commit observed but not applied.
// Journal construction is fixture generation, outside the ordinary-path guard.
async fn observed_commit(sm: &mut Arc<StateMachine>, profile: CapacityProfile) -> Journal {
    let mut journal = empty(profile);
    if !profile.is_legacy() {
        let own = sm
            .participant_capacity_grant(7, 1, [7; 16], &authority(), &cohort(profile))
            .await
            .unwrap();
        journal
            .apply(Command::ConfirmCohortCapacity {
                cohort: cohort(profile),
                grants: vec![
                    own,
                    MemberCapacityGrant {
                        member: cohort(profile).members[1].clone(),
                        activation: CapacityActivation {
                            nonce: [18; 32],
                            log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                        },
                    },
                ],
            })
            .unwrap();
    }
    let plan = crate::request_plans::Plan {
        version: 1,
        id: id(1),
        authority: authority(),
        cohort: cohort(profile),
        validation_version: 0,
        parts: vec![crate::request_plans::Part {
            descriptor: descriptor(7),
            request: request(1),
        }],
    };
    journal
        .apply(Command::AdmitRequest {
            id: id(99),
            request: b"original".to_vec(),
            payload: plan.encode().unwrap(),
            transaction_id: id(1),
            participants: vec![descriptor(7)],
        })
        .unwrap();
    sync_all(sm, &journal).await;
    no_member_journal(apply(sm, prepare(1))).await.unwrap();
    journal
        .apply(Command::Prepared {
            id: id(1),
            participant: descriptor(7),
        })
        .unwrap();
    journal
        .apply(Command::Decide {
            id: id(1),
            decision: Decision::Commit,
        })
        .unwrap();
    sync_all(sm, &journal).await;
    assert_eq!(status(sm).await.1, 0);
    assert_eq!(sm.get_raw(b"x").await, None);
    journal
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_checkpoint_crash_child() -> anyhow::Result<()> {
    let Some(path) = std::env::var_os("D2_MEMBER_CHECKPOINT_CRASH_DIRECTORY") else {
        return Ok(());
    };
    let path = std::path::PathBuf::from(path);
    let mut sm = Arc::new(StateMachine::open(path.join("state.db"))?);
    setup(&mut sm, CapacityProfile::ByteBound16MiBV1).await;
    let journal = observed_commit(&mut sm, CapacityProfile::ByteBound16MiBV1).await;
    let cut = journal.history_cursor().unwrap();
    std::fs::write(path.join("expected-cut.json"), serde_json::to_vec(&cut)?)?;
    let index = sm.applied().await.unwrap().index + 1;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (_release, gate) = std::sync::mpsc::channel();
    *sm.apply_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
        entered,
        release: gate,
    });
    let disk_probe = Arc::clone(&sm);
    let writer =
        tokio::spawn(
            async move { no_member_journal(sm.apply([entry(index, advance(1, &cut))])).await },
        );
    ready.await?;
    // Reading RocksDB does not acquire the blocked in-memory state write lock.
    // Persist the complete synced generation to compare after an actual process kill.
    std::fs::write(
        path.join("committed-disk.json"),
        serde_json::to_vec(&disk(&disk_probe))?,
    )?;
    std::fs::write(path.join("synced"), index.to_string())?;
    writer.await??;
    anyhow::bail!("parent must kill member child after sync and before publication/ACK")
}

#[tokio::test]
async fn process_death_after_member_sync_recovers_commit_and_exact_retry() -> anyhow::Result<()> {
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
                "state_machine::member_checkpoint_tests::member_checkpoint_crash_child",
                "--nocapture",
            ])
            .env("D2_MEMBER_CHECKPOINT_CRASH_DIRECTORY", dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?,
    );
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        while !dir.path().join("synced").exists() {
            anyhow::ensure!(
                child.0.try_wait()?.is_none(),
                "member checkpoint child exited before sync"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    child.0.kill()?;
    child.0.wait()?;
    let index: u64 = std::fs::read_to_string(dir.path().join("synced"))?.parse()?;
    let expected: Vec<(Vec<u8>, Vec<u8>)> =
        serde_json::from_slice(&std::fs::read(dir.path().join("committed-disk.json"))?)?;
    let cut: HistoryCursor =
        serde_json::from_slice(&std::fs::read(dir.path().join("expected-cut.json"))?)?;
    let mut sm = Arc::new(StateMachine::open(dir.path().join("state.db"))?);
    assert_eq!(disk(&sm), expected);
    assert_eq!(
        sm.applied().await,
        Some(LogId::new(CommittedLeaderId::new(1, 1), index))
    );
    assert_active(&sm).await;
    sm.forbid_coordinator_journal(true);
    assert_eq!(status(&sm).await, (cut.clone(), 1));
    assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
    let recovered = sm.state.read().await.d1.clone().unwrap();
    assert_eq!(recovered.sequence, 1);
    assert_eq!(recovered.dedup.len(), 1);
    assert_eq!(recovered.dedup[0].request, request(1));
    assert_eq!(recovered.dedup[0].sequence, 1);
    assert_eq!(
        recovered.visibility.as_ref().unwrap().commits,
        vec![crate::versioned_reads::CommitVersion {
            ordinal: 1,
            id: id(1),
            sequence: 1
        }]
    );
    assert_eq!(
        recovered.participant.as_ref().unwrap().status(id(1)),
        Status::Committed { sequence: 1 }
    );
    let raw = rows(&sm, event_ledger::PREFIX);
    let terminals = rows(&sm, crate::decision_segments::PREFIX);
    assert_eq!(
        no_member_journal(apply(&mut sm, prepare(1))).await.unwrap(),
        D1Result::Participant(Status::Committed { sequence: 1 })
    );
    no_member_journal(apply(&mut sm, advance(1, &cut)))
        .await
        .unwrap();
    assert_eq!(sm.state.read().await.d1.as_ref().unwrap(), &recovered);
    assert_eq!(rows(&sm, event_ledger::PREFIX), raw);
    assert_eq!(rows(&sm, crate::decision_segments::PREFIX), terminals);
    Ok(())
}

#[tokio::test]
async fn actual_disk12_disk14_member_migration_is_atomic() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        let mut journal = observed_commit(&mut sm, profile).await;
        let first_cut = journal.history_cursor().unwrap();
        apply(&mut sm, advance(1, &first_cut)).await.unwrap();
        journal.apply(begin(2)).unwrap();
        journal.apply(abort(2)).unwrap();
        let cut = sync_all(&mut sm, &journal).await;
        let marker = sm.applied().await;
        let raw = rows(&sm, event_ledger::PREFIX);
        let terminals = rows(&sm, crate::decision_segments::PREFIX);
        let legacy_version = if profile.is_legacy() { 12 } else { 14 };
        // Use the retained published typed writer, not a fabricated v17 rewrite.
        let metadata = {
            let state = sm.state.read().await;
            let full = sm.load_disk_state(&state).unwrap().unwrap();
            assert!(full
                .state
                .visibility
                .as_ref()
                .unwrap()
                .journal
                .full()
                .is_some());
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
            assert_eq!(header["version"], legacy_version);
            assert!(header.get("checkpoint").is_none());
            batch.put(D1_KEY, &metadata);
            sm.storage.as_ref().unwrap().write_sync(batch).unwrap();
            metadata
        };
        assert_eq!(rows(&sm, event_ledger::PREFIX), raw);
        assert_eq!(rows(&sm, crate::decision_segments::PREFIX), terminals);
        drop(sm);
        // Strict legacy decode rejects an unknown field before migration.
        {
            let db = DB::open_default(dir.path()).unwrap();
            let mut invalid: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
            invalid
                .as_object_mut()
                .unwrap()
                .insert("unexpected".into(), true.into());
            db.put(D1_KEY, serde_json::to_vec(&invalid).unwrap())
                .unwrap();
        }
        assert!(StateMachine::open(dir.path()).is_err());
        {
            let db = DB::open_default(dir.path()).unwrap();
            db.put(D1_KEY, &metadata).unwrap();
        }
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert!(sm
            .state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .visibility
            .as_ref()
            .unwrap()
            .journal
            .full()
            .is_some());
        assert_eq!(status(&sm).await, (cut.clone(), 1));
        assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
        let old = disk(&sm);
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm.apply([blank(marker.unwrap().index + 1)]).await.is_err());
        assert_eq!(disk(&sm), old);
        assert_eq!(sm.applied().await, marker);
        assert!(sm
            .state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .visibility
            .as_ref()
            .unwrap()
            .journal
            .full()
            .is_some());
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert_eq!(disk(&sm), old);
        sm.apply([blank(marker.unwrap().index + 1)]).await.unwrap();
        assert_active(&sm).await;
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
        assert_eq!(header["version"], 17);
        assert_eq!(status(&sm).await, (cut.clone(), 1));
        assert_eq!(rows(&sm, event_ledger::PREFIX), raw);
        assert_eq!(rows(&sm, crate::decision_segments::PREFIX), terminals);
        sm.forbid_coordinator_journal(true);
        assert_eq!(
            no_member_journal(apply(&mut sm, prepare(1))).await.unwrap(),
            D1Result::Participant(Status::Committed { sequence: 1 })
        );
        no_member_journal(apply(&mut sm, advance(2, &cut)))
            .await
            .unwrap();
        assert_eq!(sm.state.read().await.d1.as_ref().unwrap().sequence, 1);
        drop(sm);
        let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert_active(&sm).await;
        assert_eq!(status(&sm).await, (cut, 2));
        assert_eq!(
            sm.participant_status(7, 1, [7; 16], id(1)).await.unwrap(),
            Status::Committed { sequence: 1 }
        );
    }
}

#[tokio::test]
async fn member_snapshot_repairs_raw_and_preserves_applied_successor() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    let profile = CapacityProfile::ByteBound16MiBV1;
    setup(&mut sm, profile).await;
    let mut journal = observed_commit(&mut sm, profile).await;
    journal.apply(begin(2)).unwrap();
    journal.apply(abort(2)).unwrap();
    let old_cut = sync_all(&mut sm, &journal).await;
    assert_eq!(status(&sm).await, (old_cut.clone(), 0));
    assert_eq!(
        sm.state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .visibility
            .as_ref()
            .unwrap()
            .journal
            .resolved_prefix(),
        2
    );
    let old = sm.build_snapshot().await.unwrap();
    let old_bytes = old.snapshot.get_ref().clone();
    let original = sm.state.read().await.d1.clone();
    let raw_key = rows(&sm, event_ledger::PREFIX)[0].0.clone();
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .put(&raw_key, b"damaged member raw")
        .unwrap();
    let marker = sm.applied().await;
    assert!(
        no_member_journal(sm.apply([blank(marker.unwrap().index + 1)]))
            .await
            .is_err()
    );
    assert_eq!(sm.applied().await, marker);
    sm.install_snapshot(&old.meta, Box::new(Cursor::new(old_bytes.clone())))
        .await
        .unwrap();
    assert_eq!(sm.state.read().await.d1, original);
    assert_active(&sm).await;
    assert_eq!(status(&sm).await, (old_cut.clone(), 0));
    let peer_dir = tempfile::tempdir().unwrap();
    let mut peer = Arc::new(StateMachine::open(peer_dir.path()).unwrap());
    peer.install_snapshot(&old.meta, Box::new(Cursor::new(old_bytes.clone())))
        .await
        .unwrap();
    peer.forbid_coordinator_journal(true);
    no_member_journal(apply(&mut peer, advance(1, &old_cut)))
        .await
        .unwrap();
    journal.apply(begin(3)).unwrap();
    journal.apply(abort(3)).unwrap();
    let new_cut = sync_all(&mut peer, &journal).await;
    let successor = peer.build_snapshot().await.unwrap();
    assert_eq!(status(&peer).await, (new_cut.clone(), 1));
    // A valid full incoming state can still rewrite a drained unknown Abort
    // before old E. Counts, A and current compact checkpoint alone cannot detect
    // this equal-width descriptor substitution; exact original-prefix proof must.
    let mut incompatible: SnapshotV1 =
        serde_json::from_slice(successor.snapshot.get_ref()).unwrap();
    let incoming = incompatible
        .d1
        .visibility
        .as_ref()
        .unwrap()
        .journal
        .full()
        .unwrap();
    let mut rewritten = Journal::new(incoming.authority()).unwrap();
    for command in incoming.events() {
        let mut command = command.clone();
        if let Command::Begin {
            id: transaction_id,
            participants,
        } = &mut command
        {
            if *transaction_id == id(2) {
                participants[0].intent = [5; 32];
            }
        }
        rewritten.apply(command).unwrap();
    }
    incompatible.d1.visibility.as_mut().unwrap().journal = rewritten.into();
    incompatible.d1.validate().unwrap();
    let before_conflict = disk(&sm);
    assert!(sm
        .install_snapshot(
            &successor.meta,
            Box::new(Cursor::new(serde_json::to_vec(&incompatible).unwrap()))
        )
        .await
        .is_err());
    assert_eq!(disk(&sm), before_conflict);
    assert_eq!(status(&sm).await, (old_cut.clone(), 0));
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .put(&raw_key, b"damaged again")
        .unwrap();
    sm.install_snapshot(&successor.meta, successor.snapshot)
        .await
        .unwrap();
    assert_eq!(status(&sm).await, (new_cut.clone(), 1));
    assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
    assert_eq!(sm.state.read().await.d1, peer.state.read().await.d1);
    // A newer self-reported Raft marker cannot repair by rolling back old A/E.
    let before = disk(&sm);
    let mut fabricated_meta = old.meta.clone();
    fabricated_meta.last_log_id = Some(LogId::new(
        CommittedLeaderId::new(3, 1),
        sm.applied().await.unwrap().index + 1,
    ));
    assert!(sm
        .install_snapshot(&fabricated_meta, Box::new(Cursor::new(old_bytes)))
        .await
        .is_err());
    assert_eq!(disk(&sm), before);
    assert!(sm.install_snapshot(&old.meta, old.snapshot).await.is_err());
    assert_eq!(disk(&sm), before);
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_active(&sm).await;
    sm.forbid_coordinator_journal(true);
    assert_eq!(status(&sm).await, (new_cut.clone(), 1));
    assert_eq!(
        no_member_journal(apply(&mut sm, prepare(1))).await.unwrap(),
        D1Result::Participant(Status::Committed { sequence: 1 })
    );
    no_member_journal(apply(&mut sm, advance(2, &old_cut)))
        .await
        .unwrap();
    assert_eq!(status(&sm).await, (new_cut, 2));
    assert_eq!(
        sm.state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .visibility
            .as_ref()
            .unwrap()
            .commits
            .len(),
        1
    );
}

#[tokio::test]
async fn member_blank_membership_preserves_history_with_full_log_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::LegacyCountV1).await;
    let mut journal = empty(CapacityProfile::LegacyCountV1);
    journal.apply(begin(1)).unwrap();
    journal.apply(abort(1)).unwrap();
    let cut = sync_all(&mut sm, &journal).await;
    let before = sm.state.read().await.d1.clone();
    let raw = rows(&sm, event_ledger::PREFIX);
    let terminal = rows(&sm, crate::decision_segments::PREFIX);
    let index = sm.applied().await.unwrap().index + 1;
    let membership = Entry {
        log_id: LogId::new(CommittedLeaderId::new(2, 7), index + 1),
        payload: EntryPayload::Membership(openraft::Membership::new(
            vec![std::collections::BTreeSet::from([7])],
            BTreeMap::from([(7, BasicNode::default())]),
        )),
    };
    no_member_journal(sm.apply([blank(index), membership.clone()]))
        .await
        .unwrap();
    assert_eq!(sm.applied().await, Some(membership.log_id));
    assert_eq!(sm.state.read().await.d1, before);
    assert_eq!(status(&sm).await, (cut.clone(), 0));
    assert_eq!(rows(&sm, event_ledger::PREFIX), raw);
    assert_eq!(rows(&sm, crate::decision_segments::PREFIX), terminal);
    let saved_membership = sm.state.read().await.last_membership.clone();
    assert_eq!(saved_membership.log_id(), &Some(membership.log_id));
    let stored_marker: Option<LogId<NodeId>> = serde_json::from_slice(
        &sm.storage
            .as_ref()
            .unwrap()
            .db
            .get(APPLIED_KEY)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(stored_marker, Some(membership.log_id));
    let bytes = sm
        .storage
        .as_ref()
        .unwrap()
        .db
        .get(D1_KEY)
        .unwrap()
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        metadata["ledger"]["applied"],
        serde_json::to_value(Some(membership.log_id)).unwrap()
    );
    drop(sm);
    let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_active(&sm).await;
    assert_eq!(sm.applied().await, Some(membership.log_id));
    assert_eq!(sm.state.read().await.last_membership, saved_membership);
    assert_eq!(status(&sm).await, (cut, 0));
}

#[tokio::test]
async fn durable_member_raw_command_matrix_preserves_inductive_state() {
    struct Case {
        name: &'static str,
        command: D1Command,
        result: Result<D1Result, D1Error>,
        expanded_capacity: bool,
    }
    impl Case {
        fn exact(
            name: &'static str,
            command: D1Command,
            result: Result<D1Result, D1Error>,
        ) -> Self {
            Self {
                name,
                command,
                result,
                expanded_capacity: false,
            }
        }
    }
    fn enable(profile: CapacityProfile, shard: u64, epoch: u64, mode_id: [u8; 16]) -> D1Command {
        if profile.is_legacy() {
            D1Command::EnableVersionedReads {
                shard,
                epoch,
                mode_id,
                journal: empty(profile),
            }
        } else {
            D1Command::EnableByteBoundReads {
                shard,
                epoch,
                mode_id,
                journal: empty(profile),
                generation_nonce: [17; 32],
            }
        }
    }
    let wrong_shard = D1Error::WrongShard {
        requested: 8,
        current: 7,
    };
    let wrong_epoch = D1Error::Fenced {
        requested: 2,
        current: 1,
    };
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        let mut journal = observed_commit(&mut sm, profile).await;
        let first_cut = journal.history_cursor().unwrap();
        no_member_journal(apply(&mut sm, advance(1, &first_cut)))
            .await
            .unwrap();
        journal.apply(begin(2)).unwrap();
        journal.apply(abort(2)).unwrap();
        let cut = sync_all(&mut sm, &journal).await;
        let bad_authority = Authority {
            shard: 10,
            mode: [10; 16],
        };
        let mut wrong_authority_journal = Journal::new([10; 16]).unwrap();
        wrong_authority_journal
            .apply(Command::BindCohort {
                cohort: cohort(profile),
            })
            .unwrap();
        let wrong_authority_enable = if profile.is_legacy() {
            D1Command::EnableVersionedReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: wrong_authority_journal,
            }
        } else {
            D1Command::EnableByteBoundReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: wrong_authority_journal,
                generation_nonce: [17; 32],
            }
        };
        let mut cases = vec![
            Case::exact(
                "exact visibility enable",
                enable(profile, 7, 1, [7; 16]),
                Ok(D1Result::Visibility { applied_prefix: 1 }),
            ),
            Case::exact(
                "visibility wrong shard",
                enable(profile, 8, 1, [7; 16]),
                Err(wrong_shard.clone()),
            ),
            Case::exact(
                "visibility wrong epoch",
                enable(profile, 7, 2, [7; 16]),
                Err(wrong_epoch.clone()),
            ),
            Case::exact(
                "visibility wrong mode",
                enable(profile, 7, 1, [8; 16]),
                Err(D1Error::ManagedModeConflict),
            ),
            Case::exact(
                "visibility wrong authority",
                wrong_authority_enable,
                Err(D1Error::VisibilityHistory),
            ),
            Case::exact(
                "exact Activate",
                D1Command::Activate { shard: 7, epoch: 1 },
                Ok(D1Result::Activated { shard: 7, epoch: 1 }),
            ),
            Case::exact(
                "Activate wrong shard",
                D1Command::Activate { shard: 8, epoch: 1 },
                Err(wrong_shard.clone()),
            ),
            Case::exact(
                "Activate wrong epoch",
                D1Command::Activate { shard: 7, epoch: 2 },
                Err(wrong_epoch.clone()),
            ),
            Case::exact(
                "exact managed enable",
                D1Command::EnableManagedWrites {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    expected_sequence: 0,
                },
                Ok(D1Result::ManagedWritesEnabled {
                    mode_id: [7; 16],
                    activated_sequence: 0,
                }),
            ),
            Case::exact(
                "managed enable wrong mode",
                D1Command::EnableManagedWrites {
                    shard: 7,
                    epoch: 1,
                    mode_id: [8; 16],
                    expected_sequence: 0,
                },
                Err(D1Error::ManagedModeConflict),
            ),
            Case::exact(
                "managed enable wrong baseline",
                D1Command::EnableManagedWrites {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    expected_sequence: 1,
                },
                Err(D1Error::ManagedModeConflict),
            ),
            Case::exact(
                "managed enable wrong shard",
                D1Command::EnableManagedWrites {
                    shard: 8,
                    epoch: 1,
                    mode_id: [7; 16],
                    expected_sequence: 0,
                },
                Err(wrong_shard.clone()),
            ),
            Case::exact(
                "managed enable wrong epoch",
                D1Command::EnableManagedWrites {
                    shard: 7,
                    epoch: 2,
                    mode_id: [7; 16],
                    expected_sequence: 0,
                },
                Err(wrong_epoch.clone()),
            ),
            Case::exact(
                "exact participant enable",
                D1Command::EnableParticipant {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    authority: authority(),
                },
                Ok(D1Result::Participant(Status::Unknown)),
            ),
            Case::exact(
                "participant wrong authority",
                D1Command::EnableParticipant {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    authority: bad_authority.clone(),
                },
                Err(D1Error::ParticipantAuthority),
            ),
            Case::exact(
                "participant wrong mode",
                D1Command::EnableParticipant {
                    shard: 7,
                    epoch: 1,
                    mode_id: [8; 16],
                    authority: authority(),
                },
                Err(D1Error::ManagedModeConflict),
            ),
            Case::exact(
                "participant wrong shard",
                D1Command::EnableParticipant {
                    shard: 8,
                    epoch: 1,
                    mode_id: [7; 16],
                    authority: authority(),
                },
                Err(wrong_shard.clone()),
            ),
            Case::exact(
                "participant wrong epoch",
                D1Command::EnableParticipant {
                    shard: 7,
                    epoch: 2,
                    mode_id: [7; 16],
                    authority: authority(),
                },
                Err(wrong_epoch.clone()),
            ),
            Case::exact(
                "forbidden raw Write",
                D1Command::Write(request(3)),
                Err(D1Error::ManagedWriteRequired),
            ),
            Case::exact(
                "forbidden ManagedWrite",
                D1Command::ManagedWrite {
                    mode_id: [7; 16],
                    request: request(3),
                },
                Err(D1Error::ParticipantOnly),
            ),
            Case::exact(
                "ManagedWrite mode precedence",
                D1Command::ManagedWrite {
                    mode_id: [8; 16],
                    request: request(3),
                },
                Err(D1Error::ManagedModeConflict),
            ),
            Case::exact(
                "forbidden direct Resolve",
                D1Command::ResolveParticipant {
                    authority: authority(),
                    participant: descriptor(7),
                    id: id(2),
                    decision: Decision::Abort,
                },
                Err(D1Error::VisibilityRequired),
            ),
            Case::exact(
                "Resolve visibility precedence",
                D1Command::ResolveParticipant {
                    authority: bad_authority,
                    participant: descriptor(8),
                    id: id(2),
                    decision: Decision::Abort,
                },
                Err(D1Error::VisibilityRequired),
            ),
            Case::exact(
                "forbidden coordinator enable",
                D1Command::EnableCoordinator {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                },
                Err(D1Error::ParticipantOnly),
            ),
            Case::exact(
                "forbidden coordinator command",
                D1Command::Coordinator {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    command: begin(3),
                },
                Err(D1Error::CoordinatorNotEnabled),
            ),
            Case::exact(
                "forbidden epoch change",
                D1Command::SetEpoch {
                    shard: 7,
                    expected_epoch: 1,
                    new_epoch: 2,
                },
                Err(D1Error::VisibilityRequired),
            ),
            Case::exact(
                "harmless same epoch retry",
                D1Command::SetEpoch {
                    shard: 7,
                    expected_epoch: 1,
                    new_epoch: 1,
                },
                Ok(D1Result::Epoch { epoch: 1 }),
            ),
            Case::exact(
                "epoch wrong shard precedence",
                D1Command::SetEpoch {
                    shard: 8,
                    expected_epoch: 1,
                    new_epoch: 2,
                },
                Err(wrong_shard.clone()),
            ),
            Case::exact(
                "same dedup capacity",
                D1Command::SetDedupCapacity {
                    shard: 7,
                    epoch: 1,
                    expected_bytes: crate::MAX_DEDUP_BYTES,
                    new_bytes: crate::MAX_DEDUP_BYTES,
                },
                Ok(D1Result::DedupCapacity {
                    bytes_limit: crate::MAX_DEDUP_BYTES,
                }),
            ),
            Case::exact(
                "dedup capacity wrong shard",
                D1Command::SetDedupCapacity {
                    shard: 8,
                    epoch: 1,
                    expected_bytes: crate::MAX_DEDUP_BYTES,
                    new_bytes: crate::EXTENDED_DEDUP_BYTES,
                },
                Err(wrong_shard.clone()),
            ),
            Case::exact(
                "dedup capacity wrong epoch",
                D1Command::SetDedupCapacity {
                    shard: 7,
                    epoch: 2,
                    expected_bytes: crate::MAX_DEDUP_BYTES,
                    new_bytes: crate::EXTENDED_DEDUP_BYTES,
                },
                Err(wrong_epoch.clone()),
            ),
            Case::exact(
                "dedup invalid capacity",
                D1Command::SetDedupCapacity {
                    shard: 7,
                    epoch: 1,
                    expected_bytes: 0,
                    new_bytes: crate::MAX_DEDUP_BYTES,
                },
                Err(D1Error::InvalidDedupCapacity),
            ),
        ];
        if !profile.is_legacy() {
            cases.push(Case::exact(
                "changed byte activation nonce",
                D1Command::EnableByteBoundReads {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    journal: empty(profile),
                    generation_nonce: [19; 32],
                },
                Err(D1Error::IdempotencyConflict),
            ));
        }
        cases.extend([
            Case {
                name: "explicit dedup capacity expansion",
                command: D1Command::SetDedupCapacity {
                    shard: 7,
                    epoch: 1,
                    expected_bytes: crate::MAX_DEDUP_BYTES,
                    new_bytes: crate::EXTENDED_DEDUP_BYTES,
                },
                result: Ok(D1Result::DedupCapacity {
                    bytes_limit: crate::EXTENDED_DEDUP_BYTES,
                }),
                expanded_capacity: true,
            },
            Case::exact(
                "dedup expansion exact retry",
                D1Command::SetDedupCapacity {
                    shard: 7,
                    epoch: 1,
                    expected_bytes: crate::MAX_DEDUP_BYTES,
                    new_bytes: crate::EXTENDED_DEDUP_BYTES,
                },
                Ok(D1Result::DedupCapacity {
                    bytes_limit: crate::EXTENDED_DEDUP_BYTES,
                }),
            ),
            Case::exact(
                "dedup capacity cannot shrink",
                D1Command::SetDedupCapacity {
                    shard: 7,
                    epoch: 1,
                    expected_bytes: crate::EXTENDED_DEDUP_BYTES,
                    new_bytes: crate::MAX_DEDUP_BYTES,
                },
                Err(D1Error::InvalidDedupCapacity),
            ),
        ]);
        for case in cases {
            let mut expected = sm.state.read().await.d1.clone().unwrap();
            let checkpoint = Arc::clone(
                expected
                    .visibility
                    .as_ref()
                    .unwrap()
                    .journal
                    .active_arc()
                    .unwrap(),
            );
            let before_rows = [
                event_ledger::PREFIX,
                crate::decision_segments::PREFIX,
                DATA_PREFIX,
                DEDUP_PREFIX,
            ]
            .map(|prefix| rows(&sm, prefix));
            let marker = sm.applied().await.unwrap();
            if case.expanded_capacity {
                expected.dedup_bytes_limit = Some(crate::EXTENDED_DEDUP_BYTES);
            }
            let result = no_member_journal(apply(&mut sm, case.command)).await;
            assert_eq!(result, case.result, "{profile:?}: {}", case.name);
            let state = sm.state.read().await;
            let actual = state.d1.as_ref().unwrap();
            assert_eq!(
                actual, &expected,
                "{profile:?}: {} mutated logical member state",
                case.name
            );
            assert!(
                Arc::ptr_eq(
                    actual
                        .visibility
                        .as_ref()
                        .unwrap()
                        .journal
                        .active_arc()
                        .unwrap(),
                    &checkpoint
                ),
                "{} replaced unchanged checkpoint",
                case.name
            );
            drop(state);
            assert_eq!(
                status(&sm).await,
                (cut.clone(), 1),
                "{} changed cursor/A",
                case.name
            );
            assert_eq!(
                sm.applied().await,
                Some(LogId::new(CommittedLeaderId::new(1, 1), marker.index + 1)),
                "{} lost existing marker semantics",
                case.name
            );
            assert_eq!(
                [
                    event_ledger::PREFIX,
                    crate::decision_segments::PREFIX,
                    DATA_PREFIX,
                    DEDUP_PREFIX
                ]
                .map(|prefix| rows(&sm, prefix)),
                before_rows,
                "{} changed retained rows",
                case.name
            );
        }
        let expected = sm.state.read().await.d1.clone();
        let marker = sm.applied().await;
        drop(sm);
        let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert_active(&sm).await;
        assert_eq!(sm.state.read().await.d1, expected);
        assert_eq!(sm.applied().await, marker);
        assert_eq!(status(&sm).await, (cut, 1));
        assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
        assert_eq!(
            sm.participant_status(7, 1, [7; 16], id(1)).await.unwrap(),
            Status::Committed { sequence: 1 }
        );
        assert_eq!(
            sm.participant_status(7, 1, [7; 16], id(3)).await.unwrap(),
            Status::Unknown
        );
    }
}

fn status_journal(count: u64) -> Journal {
    let mut journal = empty(CapacityProfile::LegacyCountV1);
    for ordinal in 1..=count {
        let mut id = [0; 16];
        id[..8].copy_from_slice(&ordinal.to_be_bytes());
        journal
            .apply(Command::Begin {
                id,
                participants: vec![descriptor(7)],
            })
            .unwrap();
        journal
            .apply(Command::Decide {
                id,
                decision: Decision::Abort,
            })
            .unwrap();
    }
    journal
}

// Fixture construction is outside the measured region. Transactions are aborted
// so the observed cursor can be ahead of A without graph data or intent changes.
async fn status_fixture(count: u64) -> (tempfile::TempDir, Arc<StateMachine>, HistoryCursor) {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::LegacyCountV1).await;
    let journal = status_journal(count);
    let cut = sync_all(&mut sm, &journal).await;
    let first = sm.applied().await.unwrap().index + 1;
    if count > 1 {
        let results = sm
            .apply(
                (1..=count / 2).map(|ordinal| entry(first + ordinal - 1, advance(ordinal, &cut))),
            )
            .await
            .unwrap();
        for result in results {
            result.d1.unwrap().unwrap();
        }
    }
    (dir, sm, cut)
}

#[tokio::test]
#[ignore = "focused local microbenchmark; run explicitly with --release --ignored --nocapture"]
async fn history_status_validation_benchmark() {
    use std::time::Instant;
    for count in [0, 32, 256, 1024] {
        let (_dir, sm, cut) = status_fixture(count).await;
        // Alternate the order to reduce warm-cache/order bias. No timing
        // threshold is an acceptance gate; verification counts are exact.
        for sample in 0..7 {
            for mode in if sample % 2 == 0 { [0, 1] } else { [1, 0] } {
                let iterations = 10;
                let before = sm.member_verifications.load(Ordering::Relaxed);
                let started = Instant::now();
                for _ in 0..iterations {
                    let (info, cursor, applied) = if mode == 0 {
                        let info = sm.participant_visibility_info(7, 1, [7; 16]).await.unwrap();
                        let (cursor, applied) =
                            sm.participant_history_status(7, 1, [7; 16]).await.unwrap();
                        (info, cursor, applied)
                    } else {
                        let (info, cursor) = sm
                            .participant_active_history_status(7, 1, [7; 16])
                            .await
                            .unwrap()
                            .unwrap();
                        let applied = info.applied_prefix;
                        (info, cursor, applied)
                    };
                    assert_eq!(cursor, cut);
                    assert_eq!(applied, count / 2);
                    assert_eq!(info.applied_prefix, applied);
                }
                let elapsed = started.elapsed();
                let scans = sm.member_verifications.load(Ordering::Relaxed) - before;
                assert_eq!(scans, iterations * if mode == 0 { 2 } else { 1 });
                println!("history_status transactions={count} events={} mode={mode} sample={sample} iterations={iterations} verifications={scans} elapsed_ns={}", cut.event_count, elapsed.as_nanos());
            }
        }
    }
}

#[tokio::test]
async fn history_status_observation_reopens_and_rechecks_sealed_rows() {
    let (dir, sm, cut) = status_fixture(80).await;
    let before = sm.member_verifications.load(Ordering::Relaxed);
    for _ in 0..2 {
        let (info, cursor) = sm
            .participant_active_history_status(7, 1, [7; 16])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor, cut);
        assert_eq!(info.applied_prefix, 40);
        assert_eq!(info.authority, authority());
        assert_eq!(info.cohort, Some(cohort(CapacityProfile::LegacyCountV1)));
    }
    assert_eq!(sm.member_verifications.load(Ordering::Relaxed) - before, 2);
    drop(sm);
    let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    sm.forbid_coordinator_journal(true);
    let (info, cursor) = sm
        .participant_active_history_status(7, 1, [7; 16])
        .await
        .unwrap()
        .unwrap();
    assert_eq!((cursor, info.applied_prefix), (cut.clone(), 40));
    for (shard, epoch, mode) in [(8, 1, [7; 16]), (7, 2, [7; 16]), (7, 1, [8; 16])] {
        assert_eq!(
            sm.participant_active_history_status(shard, epoch, mode)
                .await
                .err(),
            sm.participant_visibility_info(shard, epoch, mode)
                .await
                .err()
        );
    }
    let db = &sm.storage.as_ref().unwrap().db;
    for prefix in [event_ledger::PREFIX, crate::decision_segments::PREFIX] {
        let all = rows(&sm, prefix);
        assert!(
            all.len() >= 2,
            "must corrupt an old sealed row, not only the active tail"
        );
        let (key, value) = &all[0];
        db.put(key, b"{}").unwrap();
        let corrupted = disk(&sm);
        assert!(sm
            .participant_active_history_status(7, 1, [7; 16])
            .await
            .is_err());
        assert_eq!(disk(&sm), corrupted);
        db.put(key, value).unwrap();
        assert_eq!(
            sm.participant_active_history_status(7, 1, [7; 16])
                .await
                .unwrap()
                .unwrap()
                .1,
            cut
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_status_observation_excludes_concurrent_publication() {
    let (_dir, sm, old_cut) = status_fixture(4).await;
    let journal = status_journal(5);
    let new_cut = journal.history_cursor().unwrap();
    let page = journal.history_page(&old_cut, Some(&new_cut)).unwrap();
    let first = sm.applied().await.unwrap().index + 1;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    *sm.history_status_gate.lock().unwrap() = Some(ApplyPublishGate {
        entered,
        release: gate,
    });
    let reader = sm.clone();
    let read = tokio::spawn(async move {
        reader
            .participant_active_history_status(7, 1, [7; 16])
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    assert!(
        sm.state.try_write().is_err(),
        "verified observation must retain its read guard"
    );
    let mut writer = sm.clone();
    let commands = [
        entry(first, sync(page)),
        entry(first + 1, advance(3, &new_cut)),
        entry(first + 2, advance(4, &new_cut)),
        entry(first + 3, advance(5, &new_cut)),
    ];
    let (waiting, queued) = tokio::sync::oneshot::channel();
    let write = tokio::spawn(async move {
        use std::future::Future;
        let mut apply = Box::pin(writer.apply(commands));
        std::future::poll_fn(|cx| {
            assert!(
                apply.as_mut().poll(cx).is_pending(),
                "apply must wait for the reader"
            );
            std::task::Poll::Ready(())
        })
        .await;
        waiting.send(()).unwrap();
        apply.await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), queued)
        .await
        .unwrap()
        .unwrap();
    release.send(()).unwrap();
    let (info, cursor) = read.await.unwrap().unwrap().unwrap();
    assert_eq!((cursor, info.applied_prefix), (old_cut, 2));
    for result in write.await.unwrap().unwrap() {
        result.d1.unwrap().unwrap();
    }
    let (info, cursor) = sm
        .participant_active_history_status(7, 1, [7; 16])
        .await
        .unwrap()
        .unwrap();
    assert_eq!((cursor, info.applied_prefix), (new_cut, 5));
}

#[tokio::test]
async fn history_status_observation_keeps_in_memory_fallback() {
    for (durable, bound, baseline) in [
        (false, false, 0),
        (false, true, 0),
        (false, false, 1),
        (true, false, 0),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(if durable {
            StateMachine::open(dir.path()).unwrap()
        } else {
            StateMachine::default()
        });
        let journal = if bound {
            empty(CapacityProfile::LegacyCountV1)
        } else {
            Journal::new([9; 16]).unwrap()
        };
        apply(&mut sm, D1Command::Activate { shard: 7, epoch: 1 })
            .await
            .unwrap();
        if baseline == 1 {
            apply(&mut sm, D1Command::Write(request(1))).await.unwrap();
        }
        for command in [
            D1Command::EnableManagedWrites {
                shard: 7,
                epoch: 1,
                expected_sequence: baseline,
                mode_id: [7; 16],
            },
            D1Command::EnableParticipant {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                authority: authority(),
            },
            D1Command::EnableVersionedReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: journal.clone(),
            },
        ] {
            apply(&mut sm, command).await.unwrap();
        }
        assert!(sm
            .participant_active_history_status(7, 1, [7; 16])
            .await
            .unwrap()
            .is_none());
        let info = sm.participant_visibility_info(7, 1, [7; 16]).await.unwrap();
        assert_eq!(info.cohort.is_some(), bound);
        assert_eq!(info.baseline_sequence, baseline);
        if bound {
            assert_eq!(status(&sm).await, (journal.history_cursor().unwrap(), 0));
        } else {
            assert_eq!(
                sm.participant_history_status(7, 1, [7; 16]).await,
                Err(D1Error::VisibilityHistory)
            );
        }
        assert_eq!(sm.member_verifications.load(Ordering::Relaxed), 0);
    }
}

// The original apply verification/projection/checker sequence is retained here
// as a differential oracle, including errors for a tampered live A.
fn checked_member_tail(
    sm: &StateMachine,
    state: &StateMachineData,
    fused: bool,
) -> Result<Vec<crate::transaction_decisions::Transaction>, StorageError<NodeId>> {
    let fail = |error: anyhow::Error| write_error(std::io::Error::other(error.to_string()));
    let (verified, collected) = (if fused {
        sm.verified_member_apply(state)
    } else {
        sm.verified_member(state)
            .map(|value| value.map(|archive| (archive, Vec::new())))
    })
    .map_err(|e| fail(anyhow::anyhow!("member archive: {e:?}")))?
    .ok_or_else(|| fail(anyhow::anyhow!("active member lacks publication cut")))?;
    let applied = state
        .d1
        .as_ref()
        .unwrap()
        .visibility
        .as_ref()
        .unwrap()
        .applied;
    let domain = &verified.descriptor().domain;
    let identity = crate::decision_segments::CheckpointIdentity {
        role: crate::decision_segments::Role::Member,
        authority: domain.authority.clone(),
        shard: domain.shard,
        epoch: domain.epoch,
        mode: domain.mode,
        cohort: domain.cohort.clone(),
    };
    let key = crate::decision_segments::checkpoint_tail_key(&identity, applied).map_err(fail)?;
    let bytes = key
        .as_deref()
        .map(|key| verified.get(key))
        .transpose()
        .map_err(fail)?
        .flatten();
    let tail = if applied.is_multiple_of(32) {
        Vec::new()
    } else if fused && applied == verified.terminal_through() {
        collected
    } else {
        verified
            .transactions(applied / 32 * 32 + 1, applied, &[])
            .map_err(fail)?
    };
    crate::decision_segments::check_checkpoint_tail(
        &identity,
        applied,
        state.last_applied_log,
        verified.decision_manifest(),
        verified.decision_manifest(),
        bytes.as_deref(),
        &tail,
    )
    .map_err(fail)?;
    Ok(tail)
}

#[tokio::test]
async fn member_apply_tail_fusion_removes_only_the_duplicate_walk() {
    for count in [0, 2, 62, 64, 66, 126, 128, 130] {
        let (_dir, mut sm, _) = status_fixture(count).await;
        let before = event_ledger::raw_walks();
        let expected = checked_member_tail(&sm, &*sm.state.read().await, false).unwrap();
        assert_eq!(
            event_ledger::raw_walks() - before,
            1 + u64::from(!expected.is_empty())
        );
        let first = sm.applied().await.unwrap().index + 1;
        let before = event_ledger::raw_walks();
        // One apply with several entries must remove one walk, not one per entry.
        sm.apply([blank(first), blank(first + 1), blank(first + 2)])
            .await
            .unwrap();
        assert_eq!(event_ledger::raw_walks() - before, 1, "A={}", count / 2);
        let before = event_ledger::raw_walks();
        let _ = sm.participant_history_status(7, 1, [7; 16]).await.unwrap();
        assert_eq!(event_ledger::raw_walks() - before, 1);
    }
}

#[tokio::test]
async fn member_apply_tail_fusion_keeps_malformed_live_a_errors() {
    let (_dir, sm, _) = status_fixture(80).await;
    let before_disk = disk(&sm);
    let mut state = sm.state.write().await;
    for applied in [0, 1, 32, 33, 64, 81, 1025, u64::MAX] {
        state
            .d1
            .as_mut()
            .unwrap()
            .visibility
            .as_mut()
            .unwrap()
            .applied = applied;
        let expected = checked_member_tail(&sm, &state, false).unwrap_err();
        let actual = sm
            .apply_member_entries(&mut state, [blank(500)])
            .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string(), "A={applied}");
        assert_eq!(disk(&sm), before_disk);
    }
}

#[tokio::test]
async fn member_apply_tail_fusion_nonempty_tail_failure_and_reopen() {
    for applied in [31, 32, 33] {
        let (dir, mut sm, cut) = status_fixture(applied * 2).await;
        let old_disk = disk(&sm);
        let old_d1 = sm.state.read().await.d1.clone();
        let old_marker = sm.applied().await;
        let next = old_marker.unwrap().index + 1;
        let commands = [
            entry(next, advance(applied + 1, &cut)),
            entry(next + 1, advance(applied + 2, &cut)),
        ];
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm.apply(commands.clone()).await.is_err());
        assert_eq!(disk(&sm), old_disk);
        assert_eq!(sm.state.read().await.d1, old_d1);
        assert_eq!(sm.applied().await, old_marker);
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        sm.forbid_coordinator_journal(true);
        assert_eq!(disk(&sm), old_disk);
        assert_eq!(status(&sm).await, (cut.clone(), applied));
        let results = sm.apply(commands).await.unwrap();
        assert!(results.iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
        assert_eq!(status(&sm).await, (cut.clone(), applied + 2));
        // A rejected final entry cannot leak its candidate. Earlier successful
        // entries and exact terminal retry remain accepted in the same batch.
        let next = sm.applied().await.unwrap().index + 1;
        let mut bad = advance(applied + 3, &cut);
        if let D1Command::AdvanceRetained { epoch, .. } = &mut bad {
            *epoch = 9;
        }
        let results = sm
            .apply([
                entry(next, advance(applied + 2, &cut)),
                entry(next + 1, bad),
            ])
            .await
            .unwrap();
        assert!(results[0].d1.as_ref().unwrap().is_ok());
        assert!(results[1].d1.as_ref().unwrap().is_err());
        drop(sm);
        let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert_eq!(status(&sm).await, (cut, applied + 2));
    }
}

#[tokio::test]
#[ignore = "local paired verification benchmark; run explicitly with --release --ignored --nocapture"]
async fn member_apply_tail_fusion_benchmark() {
    use std::time::Instant;
    for count in [0, 32, 64, 256, 512, 1024] {
        let (_dir, sm, _) = status_fixture(count).await;
        // Avoid only measuring sealed A while preserving the exact same cut
        // for both alternatives. Fixture construction is never timed.
        let mut sm = sm;
        if count > 1 {
            let cut = status(&sm).await.0;
            apply(&mut sm, advance(count / 2 + 1, &cut)).await.unwrap();
        }
        let state = sm.state.read().await;
        let expected = checked_member_tail(&sm, &state, false).unwrap();
        for sample in 0..9 {
            for mode in if sample % 2 == 0 { [0, 1] } else { [1, 0] } {
                let iterations = 20;
                let before = event_ledger::raw_walks();
                let started = Instant::now();
                for _ in 0..iterations {
                    let tail = checked_member_tail(&sm, &state, mode == 1).unwrap();
                    assert_eq!(tail, expected);
                }
                let elapsed = started.elapsed();
                let walks = event_ledger::raw_walks() - before;
                assert_eq!(
                    walks,
                    iterations * (1 + u64::from(mode == 0 && !expected.is_empty()))
                );
                println!("member_tail transactions={count} mode={mode} sample={sample} iterations={iterations} walks={walks} elapsed_ns={}", elapsed.as_nanos());
            }
        }
    }
}

#[tokio::test]
async fn member_apply_tail_fusion_keeps_original_admit_commit_and_abort() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        let mut journal = observed_commit(&mut sm, profile).await;
        for command in [begin(2), abort(2), begin(3), begin(4), abort(4)] {
            journal.apply(command).unwrap();
        }
        let cut = sync_all(&mut sm, &journal).await;
        for ordinal in [1, 2] {
            apply(&mut sm, advance(ordinal, &cut)).await.unwrap();
        }
        let expected: Vec<_> = (1..=2)
            .map(|n| journal.transaction_at(n).unwrap().clone())
            .collect();
        assert_eq!(expected[0].decision, Some(Decision::Commit));
        assert_eq!(expected[0].prepared, vec![true]);
        assert_eq!(expected[1].decision, Some(Decision::Abort));
        assert_eq!(expected[1].prepared, vec![false]);
        for reopen in [false, true] {
            if reopen {
                drop(sm);
                sm = Arc::new(StateMachine::open(dir.path()).unwrap());
                sm.forbid_coordinator_journal(true);
            }
            let state = sm.state.read().await;
            assert_eq!(checked_member_tail(&sm, &state, false).unwrap(), expected);
            assert_eq!(checked_member_tail(&sm, &state, true).unwrap(), expected);
            assert_eq!(
                state
                    .d1
                    .as_ref()
                    .unwrap()
                    .visibility
                    .as_ref()
                    .unwrap()
                    .journal
                    .resolved_prefix(),
                2
            );
        }
    }
}

#[tokio::test]
async fn member_apply_tail_fusion_discards_partially_invalid_page() {
    let (dir, mut sm, base) = status_fixture(62).await;
    let (_control_dir, mut control, _) = status_fixture(62).await;
    assert_eq!(status(&sm).await.1, 31);
    let events = vec![begin(200), begin(0)];
    let end = base.extend(&events).unwrap();
    let bad = HistoryPage {
        version: base.version,
        base: base.clone(),
        end: end.clone(),
        cut: end,
        events,
    };
    bad.validate().unwrap();
    let mut journal = status_journal(62);
    journal.apply(begin(201)).unwrap();
    journal.apply(abort(201)).unwrap();
    let good = journal.history_page(&base, None).unwrap();
    let cut = good.cut.clone();
    let next = sm.applied().await.unwrap().index + 1;
    let results = sm
        .apply([
            entry(next, sync(bad)),
            entry(next + 1, sync(good.clone())),
            entry(next + 2, advance(32, &cut)),
        ])
        .await
        .unwrap();
    assert!(results[0].d1.as_ref().unwrap().is_err());
    assert!(results[1..].iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
    control
        .apply([
            blank(next),
            entry(next + 1, sync(good)),
            entry(next + 2, advance(32, &cut)),
        ])
        .await
        .unwrap();
    assert_eq!(disk(&sm), disk(&control));
    assert_eq!(sm.state.read().await.d1, control.state.read().await.d1);
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(200)).await.unwrap(),
        Status::Unknown
    );
    let expected = disk(&sm);
    drop(sm);
    let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_eq!(disk(&sm), expected);
    assert_eq!(status(&sm).await, (cut, 32));
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(200)).await.unwrap(),
        Status::Unknown
    );
}

#[path = "member_projection_tests.rs"]
mod projection_tests;
