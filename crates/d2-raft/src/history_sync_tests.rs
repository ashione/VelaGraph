use super::*;
use crate::participant_writes::{Authority, Status};
use crate::transaction_decisions::{
    Cohort, Command, Decision, HistoryCursor, HistoryPage, Journal, Member, Participant,
};
use crate::versioned_reads::ReadLimit;
use openraft::CommittedLeaderId;

fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn cohort() -> Cohort {
    Cohort {
        profile: Default::default(),
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
fn id(n: u64) -> [u8; 16] {
    let mut id = [0; 16];
    id[8..].copy_from_slice(&n.to_be_bytes());
    id
}
fn descriptor(shard: u64) -> Participant {
    Participant {
        shard,
        epoch: 1,
        mode: [shard as u8; 16],
        intent: [4; 32],
    }
}
fn empty() -> Journal {
    let mut j = Journal::new([9; 16]).unwrap();
    j.apply(Command::BindCohort { cohort: cohort() }).unwrap();
    j
}
fn aborted(count: u64) -> Journal {
    let mut j = empty();
    for n in 1..=count {
        j.apply(Command::Begin {
            id: id(n),
            participants: vec![descriptor(8)],
        })
        .unwrap();
        j.apply(Command::Decide {
            id: id(n),
            decision: Decision::Abort,
        })
        .unwrap();
    }
    j
}
fn committed() -> Journal {
    let mut j = empty();
    j.apply(Command::Begin {
        id: id(1),
        participants: vec![descriptor(7)],
    })
    .unwrap();
    j.apply(Command::Prepared {
        id: id(1),
        participant: descriptor(7),
    })
    .unwrap();
    j.apply(Command::Decide {
        id: id(1),
        decision: Decision::Commit,
    })
    .unwrap();
    j
}
fn request(n: u64) -> crate::D1WriteRequest {
    crate::D1WriteRequest {
        shard: 7,
        epoch: 1,
        request_id: id(n),
        expected_sequence: Some(0),
        operations: vec![WriteOp::put("x", "committed")],
    }
}
fn prepare(n: u64) -> D1Command {
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
    let next = sm.applied().await.map_or(1, |v| v.index + 1);
    sm.apply([entry(next, command)])
        .await
        .unwrap()
        .remove(0)
        .d1
        .unwrap()
}
async fn setup(sm: &mut Arc<StateMachine>) {
    setup_activation(
        sm,
        D1Command::EnableVersionedReads {
            shard: 7,
            epoch: 1,
            mode_id: [7; 16],
            journal: empty(),
        },
    )
    .await;
}
async fn setup_activation(sm: &mut Arc<StateMachine>, activation: D1Command) {
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
        activation,
    ] {
        apply(sm, command).await.unwrap();
    }
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
async fn d1_bytes(sm: &StateMachine) -> Vec<u8> {
    let state = sm.state.read().await;
    let mut complete = sm
        .load_disk_state(&state)
        .unwrap()
        .map(|loaded| loaded.state)
        .unwrap_or_else(|| state.d1.as_ref().unwrap().clone());
    complete.dedup = state.d1.as_ref().unwrap().dedup.clone();
    serde_json::to_vec(&complete).unwrap()
}
fn limit() -> ReadLimit {
    ReadLimit {
        rows: 10,
        bytes: 1024,
        operations: 10,
    }
}
async fn read(
    sm: &StateMachine,
    version: u64,
    journal: &Journal,
) -> Result<crate::versioned_reads::ReadSnapshot, D1Error> {
    sm.certified_participant_read(7, 1, [7; 16], version, limit(), journal)
        .await
}

#[tokio::test]
async fn observed_commit_is_not_applied_and_its_exact_cut_survives_reopen_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm).await;
    apply(&mut sm, prepare(1)).await.unwrap();
    let j = committed();
    let cut = sync_all(&mut sm, &j).await;
    assert_eq!(status(&sm).await, (cut.clone(), 0));
    assert_eq!(sm.get_raw(b"x").await, None);
    assert!(matches!(
        read(&sm, 1, &j).await,
        Err(D1Error::VisibilityNotApplied)
    ));
    assert!(read(&sm, 0, &j).await.unwrap().entries.is_empty());
    let image = sm.build_snapshot().await.unwrap();
    let restore_dir = tempfile::tempdir().unwrap();
    let mut restored = Arc::new(StateMachine::open(restore_dir.path()).unwrap());
    restored
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    assert_eq!(status(&restored).await, (cut.clone(), 0));
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_eq!(status(&sm).await, (cut.clone(), 0));
    apply(&mut sm, advance(1, &cut)).await.unwrap();
    apply(&mut restored, advance(1, &cut)).await.unwrap();
    assert_eq!(
        read(&sm, 1, &j).await.unwrap().entries,
        read(&restored, 1, &j).await.unwrap().entries
    );
    assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
    assert!(read(&sm, 0, &j).await.unwrap().entries.is_empty());
    let before = d1_bytes(&sm).await;
    apply(&mut sm, advance(1, &cut)).await.unwrap();
    assert_eq!(d1_bytes(&sm).await, before);
}

#[tokio::test]
async fn pending_descriptor_conflict_rejects_sync_before_any_observation() {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    apply(&mut sm, prepare(1)).await.unwrap();
    let before = d1_bytes(&sm).await;
    let j = aborted(1); // Same ID, different shard, even though decision is terminal.
    let page = j.history_page(&status(&sm).await.0, None).unwrap();
    assert_eq!(
        apply(&mut sm, sync(page)).await,
        Err(D1Error::VisibilityHistory)
    );
    assert_eq!(d1_bytes(&sm).await, before);
    // A conflicting still-pending descriptor must also be rejected.
    let mut j = empty();
    let mut wrong = descriptor(7);
    wrong.intent = [5; 32];
    j.apply(Command::Begin {
        id: id(1),
        participants: vec![wrong],
    })
    .unwrap();
    let page = j.history_page(&status(&sm).await.0, None).unwrap();
    assert_eq!(
        apply(&mut sm, sync(page)).await,
        Err(D1Error::VisibilityHistory)
    );
    assert_eq!(d1_bytes(&sm).await, before);
}

#[tokio::test]
async fn pages_resume_from_event_positions_not_ordinals_and_preserve_frozen_cut() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm).await;
    let j = aborted(33);
    let start = status(&sm).await.0;
    let page1 = j.history_page(&start, None).unwrap();
    let cut = page1.cut.clone();
    assert_eq!(page1.events.len(), 32);
    apply(&mut sm, sync(page1.clone())).await.unwrap();
    assert_eq!(status(&sm).await, (page1.end.clone(), 0));
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    // A lost ACK resends the identical page, not a newly selected cut.
    let before = d1_bytes(&sm).await;
    apply(&mut sm, sync(page1.clone())).await.unwrap();
    assert_eq!(d1_bytes(&sm).await, before);
    let newer = aborted(34);
    let page2 = newer.history_page(&page1.end, Some(&cut)).unwrap();
    apply(&mut sm, sync(page2.clone())).await.unwrap();
    let page3 = newer.history_page(&page2.end, Some(&cut)).unwrap();
    apply(&mut sm, sync(page3)).await.unwrap();
    assert_eq!(status(&sm).await, (cut.clone(), 0));
    // Observe later events, then consume the old cut without rolling back history.
    let newer_cut = sync_all(&mut sm, &newer).await;
    apply(&mut sm, advance(1, &cut)).await.unwrap();
    assert_eq!(status(&sm).await, (newer_cut.clone(), 1));
    assert_eq!(
        apply(&mut sm, advance(34, &cut)).await,
        Err(D1Error::VisibilityUnresolved)
    );
    assert_eq!(
        apply(&mut sm, advance(3, &cut)).await,
        Err(D1Error::VisibilityGap)
    );
    let before = d1_bytes(&sm).await;
    let mut wrong = cut.clone();
    wrong.root[0] ^= 1;
    assert_eq!(
        apply(&mut sm, advance(1, &wrong)).await,
        Err(D1Error::VisibilityHistory)
    );
    assert_eq!(d1_bytes(&sm).await, before);
    for n in 2..=33 {
        apply(&mut sm, advance(n, &cut)).await.unwrap();
    }
    assert_eq!(status(&sm).await, (newer_cut, 33));
    assert!(read(&sm, 1, &j).await.unwrap().entries.is_empty());
}

#[tokio::test]
async fn hole_closure_observation_and_unknown_abort_never_skip_an_ordinal_or_allocate_intents() {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    let mut j = empty();
    for n in 1..=2 {
        j.apply(Command::Begin {
            id: id(n),
            participants: vec![descriptor(7)],
        })
        .unwrap();
    }
    j.apply(Command::Decide {
        id: id(2),
        decision: Decision::Abort,
    })
    .unwrap();
    let hole_cut = sync_all(&mut sm, &j).await;
    assert_eq!(
        apply(&mut sm, advance(2, &hole_cut)).await,
        Err(D1Error::VisibilityUnresolved)
    );
    assert_eq!(status(&sm).await.1, 0);
    j.apply(Command::Decide {
        id: id(1),
        decision: Decision::Abort,
    })
    .unwrap();
    let cut = sync_all(&mut sm, &j).await;
    assert_eq!(
        apply(&mut sm, advance(2, &cut)).await,
        Err(D1Error::VisibilityGap)
    );
    apply(&mut sm, advance(1, &cut)).await.unwrap();
    assert_eq!(
        apply(&mut sm, prepare(1)).await.unwrap(),
        D1Result::Participant(Status::Aborted)
    );
    assert_eq!(
        apply(&mut sm, prepare(2)).await,
        Err(D1Error::VisibilityUnresolved)
    );
    apply(&mut sm, advance(2, &cut)).await.unwrap();
    let state = sm.state.read().await;
    assert!(state
        .d1
        .as_ref()
        .unwrap()
        .participant
        .as_ref()
        .unwrap()
        .records
        .is_empty());
    assert!(state.d1.as_ref().unwrap().dedup.is_empty());
}

#[tokio::test]
async fn malformed_sync_and_snapshot_regression_leave_application_history_unchanged() {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    let old = sm.build_snapshot().await.unwrap();
    let j = aborted(20);
    let page = j.history_page(&status(&sm).await.0, None).unwrap();
    for variant in 0..6 {
        let mut bad = page.clone();
        match variant {
            0 => bad.base.root[0] ^= 1,
            1 => bad.end.event_count += 1,
            2 => bad.cut.authority.mode[0] ^= 1,
            3 => bad.events.swap(0, 1),
            4 => {
                bad.events.pop();
            }
            _ => bad.version = 2,
        }
        let before = d1_bytes(&sm).await;
        assert_eq!(
            apply(&mut sm, sync(bad)).await,
            Err(D1Error::VisibilityHistory)
        );
        assert_eq!(d1_bytes(&sm).await, before);
    }
    apply(&mut sm, sync(page)).await.unwrap();
    let before = d1_bytes(&sm).await;
    let mut meta = old.meta;
    meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 100));
    assert!(sm.install_snapshot(&meta, old.snapshot).await.is_err());
    assert_eq!(d1_bytes(&sm).await, before);
}

#[tokio::test]
async fn full_nonparticipant_journal_still_consumes_reserved_last_abort() {
    let mut j = aborted(1023);
    j.apply(Command::Begin {
        id: id(1024),
        participants: vec![descriptor(8)],
    })
    .unwrap();
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    sync_all(&mut sm, &j).await;
    assert!(j
        .apply(Command::Begin {
            id: id(1025),
            participants: vec![descriptor(8)]
        })
        .is_err());
    j.apply(Command::Decide {
        id: id(1024),
        decision: Decision::Abort,
    })
    .unwrap();
    let cut = sync_all(&mut sm, &j).await;
    // Existing sequential consume, batched only in this test's Raft apply call.
    let first = sm.applied().await.unwrap().index + 1;
    let results = sm
        .apply((1..=1024).map(|n| entry(first + n - 1, advance(n, &cut))))
        .await
        .unwrap();
    assert!(results.iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
    assert_eq!(status(&sm).await, (cut, 1024));
    let state = sm.state.read().await;
    assert!(state
        .d1
        .as_ref()
        .unwrap()
        .participant
        .as_ref()
        .unwrap()
        .records
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn history_sync_crash_child() -> anyhow::Result<()> {
    let Some(path) = std::env::var_os("D2_HISTORY_CRASH_DIRECTORY") else {
        return Ok(());
    };
    let path = std::path::PathBuf::from(path);
    let mode = std::env::var("D2_HISTORY_CRASH_MODE")?;
    let mut sm = Arc::new(StateMachine::open(path.join("state.db"))?);
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (_release, gate) = std::sync::mpsc::channel();
    let writer = if mode == "snapshot" {
        let mut source = Arc::new(StateMachine::default());
        setup(&mut source).await;
        apply(&mut source, prepare(1)).await.unwrap();
        sync_all(&mut source, &committed()).await;
        let image = source.build_snapshot().await?;
        *sm.snapshot_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
            entered,
            release: gate,
        });
        tokio::spawn(async move {
            sm.install_snapshot(&image.meta, image.snapshot)
                .await
                .map(|_| ())
        })
    } else {
        let command = if mode == "sync" {
            sync(
                committed()
                    .history_page(&status(&sm).await.0, None)
                    .unwrap(),
            )
        } else {
            advance(1, &committed().history_cursor().unwrap())
        };
        let next = sm.applied().await.unwrap().index + 1;
        *sm.apply_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
            entered,
            release: gate,
        });
        tokio::spawn(async move { sm.apply([entry(next, command)]).await.map(|_| ()) })
    };
    ready.await?;
    std::fs::write(path.join("synced"), b"ready")?;
    writer.await??;
    anyhow::bail!("parent must kill child before publication")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn process_death_after_sync_before_ack_preserves_history_or_atomic_commit(
) -> anyhow::Result<()> {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for mode in ["sync", "advance", "snapshot"] {
        let dir = tempfile::tempdir()?;
        let state_path = dir.path().join("state.db");
        let page = committed()
            .history_page(&empty().history_cursor().unwrap(), None)
            .unwrap();
        {
            let mut sm = Arc::new(StateMachine::open(&state_path)?);
            setup(&mut sm).await;
            apply(&mut sm, prepare(1)).await.unwrap();
            if mode == "advance" {
                apply(&mut sm, sync(page.clone())).await.unwrap();
            }
        }
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "state_machine::history_sync_tests::history_sync_crash_child",
                    "--nocapture",
                ])
                .env("D2_HISTORY_CRASH_DIRECTORY", dir.path())
                .env("D2_HISTORY_CRASH_MODE", mode)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?,
        );
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            while !dir.path().join("synced").exists() {
                anyhow::ensure!(
                    child.0.try_wait()?.is_none(),
                    "child exited before sync: {mode}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            anyhow::Ok(())
        })
        .await??;
        child.0.kill()?;
        child.0.wait()?;
        let mut sm = Arc::new(StateMachine::open(&state_path)?);
        let cut = committed().history_cursor().unwrap();
        assert_eq!(
            status(&sm).await,
            (cut.clone(), u64::from(mode == "advance"))
        );
        assert_eq!(
            sm.get_raw(b"x").await,
            (mode == "advance").then(|| b"committed".to_vec())
        );
        // Resend the original page even after a durable unknown outcome.
        apply(&mut sm, sync(page)).await.unwrap();
        apply(&mut sm, advance(1, &cut)).await.unwrap();
        assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
        assert!(read(&sm, 0, &committed()).await.unwrap().entries.is_empty());
        assert_eq!(sm.d1_metadata().await, Some((7, 1, 1)));
        if mode == "snapshot" {
            assert!(sm.get_current_snapshot().await?.is_some());
        }
    }
    Ok(())
}

#[tokio::test]
async fn synced_original_request_and_plan_survive_abort_retry_reopen_and_snapshot() {
    let mut j =
        Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
    let original = j.request([1; 16]).unwrap().clone();
    let mut bound = Journal::new(j.authority()).unwrap();
    bound
        .apply(Command::BindCohort {
            cohort: j.cohort().unwrap().clone(),
        })
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
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
        D1Command::EnableVersionedReads {
            shard: 7,
            epoch: 1,
            mode_id: [7; 16],
            journal: bound,
        },
    ] {
        apply(&mut sm, command).await.unwrap();
    }
    sync_all(&mut sm, &j).await;
    // Coordinator exact external-ID retry retains the original Plan even when
    // a newly attempted plan would differ; suffix replay must retain that winner.
    let before = j.encode().unwrap();
    j.apply(Command::AdmitRequest {
        id: original.id,
        request: original.request.clone(),
        payload: b"different attempted plan".to_vec(),
        transaction_id: [3; 16],
        participants: vec![descriptor(7)],
    })
    .unwrap();
    assert_eq!(j.encode().unwrap(), before);
    j.apply(Command::Decide {
        id: [2; 16],
        decision: Decision::Abort,
    })
    .unwrap();
    let cut = sync_all(&mut sm, &j).await;
    apply(&mut sm, advance(1, &cut)).await.unwrap();
    let image = sm.build_snapshot().await.unwrap();
    drop(sm);
    let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    let state = sm.state.read().await;
    let complete = sm.load_disk_state(&state).unwrap().unwrap();
    let retained = complete
        .state
        .visibility
        .as_ref()
        .unwrap()
        .journal
        .full()
        .unwrap();

    assert_eq!(retained.request([1; 16]), Some(&original));
    assert_eq!(retained.encode().unwrap(), j.encode().unwrap());
    drop(state);
    let mut restored = Arc::new(StateMachine::default());
    restored
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    assert_eq!(status(&restored).await, (cut, 1));
    let state = restored.state.read().await;
    assert_eq!(
        state
            .d1
            .as_ref()
            .unwrap()
            .visibility
            .as_ref()
            .unwrap()
            .journal
            .full()
            .unwrap()
            .request([1; 16]),
        Some(&original)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn suffix_sync_with_full_intent_budget_preserves_unknown_abort_capacity() {
    full_intent_abort(false).await;
    full_intent_abort(true).await;
}
async fn full_intent_abort(byte_profile: bool) {
    let directory = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(directory.path()).unwrap());
    let mut j = if byte_profile {
        byte_capacity::setup_byte(&mut sm, [17; 32]).await;
        byte_capacity::confirmed(&sm).await
    } else {
        setup(&mut sm).await;
        empty()
    };
    j.apply(Command::Begin {
        id: id(1),
        participants: vec![descriptor(7)],
    })
    .unwrap();
    sync_all(&mut sm, &j).await;
    let value = vec![0; crate::participant_writes::MAX_INTENT_BYTES - 256 - 128 - 64 - 3];
    let mut large = request(1);
    large.operations = vec![WriteOp::put("big", value)];
    apply(
        &mut sm,
        D1Command::PrepareParticipant {
            authority: authority(),
            participant: descriptor(7),
            request: large,
        },
    )
    .await
    .unwrap();
    j.apply(Command::Prepared {
        id: id(1),
        participant: descriptor(7),
    })
    .unwrap();
    j.apply(Command::Decide {
        id: id(1),
        decision: Decision::Commit,
    })
    .unwrap();
    let cut = sync_all(&mut sm, &j).await;
    apply(&mut sm, advance(1, &cut)).await.unwrap();
    j.apply(Command::Begin {
        id: id(2),
        participants: vec![descriptor(7)],
    })
    .unwrap();
    sync_all(&mut sm, &j).await;
    let mut small = request(2);
    small.expected_sequence = Some(1);
    let attempt = D1Command::PrepareParticipant {
        authority: authority(),
        participant: descriptor(7),
        request: small,
    };
    assert_eq!(
        apply(&mut sm, attempt.clone()).await,
        Err(D1Error::ParticipantCapacity)
    );
    j.apply(Command::Decide {
        id: id(2),
        decision: Decision::Abort,
    })
    .unwrap();
    let cut = sync_all(&mut sm, &j).await;
    // Observing the future Abort prevents a late Prepare before it is applied.
    assert_eq!(
        apply(&mut sm, attempt.clone()).await,
        Err(D1Error::VisibilityUnresolved)
    );
    apply(&mut sm, advance(2, &cut)).await.unwrap();
    assert_eq!(
        apply(&mut sm, attempt).await.unwrap(),
        D1Result::Participant(Status::Aborted)
    );
    let state = sm.state.read().await;
    let d1 = state.d1.as_ref().unwrap();
    assert_eq!(d1.participant.as_ref().unwrap().records.len(), 1);
    assert_eq!(d1.sequence, 1);
    assert_eq!(d1.visibility.as_ref().unwrap().applied, 2);
    // The resident active checkpoint deliberately cannot stand in for complete
    // validation. Exercise the real cold materialization boundary, then retain
    // the original full D1 invariant and compare both observed and applied cuts.
    let mut complete = sm.load_disk_state(&state).unwrap().unwrap().state;
    complete.dedup = d1.dedup.clone();
    let cold_visibility = complete.visibility.as_ref().unwrap();
    let resident_visibility = d1.visibility.as_ref().unwrap();
    assert_eq!(cold_visibility.applied, resident_visibility.applied);
    assert_eq!(
        &ActiveCheckpoint::from_journal(cold_visibility.journal.full().unwrap()).unwrap(),
        resident_visibility.journal.active().unwrap()
    );
    complete.validate().unwrap();
    let marker = state.last_applied_log;
    drop(state);
    drop(sm);
    let sm = StateMachine::open(directory.path()).unwrap();
    assert_eq!(
        sm.participant_history_status(7, 1, [7; 16]).await.unwrap(),
        (cut, 2)
    );
    assert_eq!(sm.applied().await, marker);
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], id(2)).await.unwrap(),
        Status::Aborted
    );
    assert_eq!(
        sm.state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .participant
            .as_ref()
            .unwrap()
            .records
            .len(),
        1
    );
}

#[tokio::test]
async fn pre_sync_failure_keeps_cursor_data_watermark_and_raft_marker_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm).await;
    apply(&mut sm, prepare(1)).await.unwrap();
    let page = committed()
        .history_page(&status(&sm).await.0, None)
        .unwrap();
    for command in [sync(page.clone()), advance(1, &page.cut)] {
        let before = d1_bytes(&sm).await;
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
        assert_eq!(sm.applied().await, marker);
        assert_eq!(d1_bytes(&sm).await, before);
        assert_eq!(sm.get_raw(b"x").await, None);
        drop(sm);
        sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert_eq!(sm.applied().await, marker);
        assert_eq!(d1_bytes(&sm).await, before);
        apply(&mut sm, command).await.unwrap();
    }
    assert_eq!(status(&sm).await, (page.cut, 1));
    assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
}

mod byte_capacity {
    use super::*;
    use crate::transaction_decisions::{CapacityActivation, CapacityProfile, MemberCapacityGrant};

    fn binding() -> Cohort {
        Cohort {
            profile: CapacityProfile::ByteBound16MiBV1,
            ..cohort()
        }
    }
    fn empty_byte() -> Journal {
        let mut journal = Journal::new(authority().mode).unwrap();
        journal
            .apply(Command::BindCohort { cohort: binding() })
            .unwrap();
        journal
    }
    fn activation(nonce: [u8; 32]) -> D1Command {
        D1Command::EnableByteBoundReads {
            shard: 7,
            epoch: 1,
            mode_id: [7; 16],
            journal: empty_byte(),
            generation_nonce: nonce,
        }
    }
    pub(super) async fn setup_byte(sm: &mut Arc<StateMachine>, nonce: [u8; 32]) {
        setup_activation(sm, activation(nonce)).await;
    }
    async fn grant(sm: &StateMachine) -> MemberCapacityGrant {
        sm.participant_capacity_grant(7, 1, [7; 16], &authority(), &binding())
            .await
            .unwrap()
    }
    // Raw deterministic state tests use trusted proposals. Actual server-owned
    // origin/leader barriers and every member are exercised by managed RPC tests.
    pub(super) async fn confirmed(sm: &StateMachine) -> Journal {
        let mut journal = empty_byte();
        journal
            .apply(Command::ConfirmCohortCapacity {
                cohort: binding(),
                grants: vec![
                    grant(sm).await,
                    MemberCapacityGrant {
                        member: binding().members[1].clone(),
                        activation: CapacityActivation {
                            nonce: [18; 32],
                            log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                        },
                    },
                ],
            })
            .unwrap();
        journal
    }

    #[tokio::test]
    async fn activation_is_atomic_immutable_and_preserved_by_reopen_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(directory.path()).unwrap());
        setup_byte(&mut sm, [17; 32]).await;
        let first = grant(&sm).await;
        assert_eq!(first.activation.log_id.index, 4);
        apply(&mut sm, activation([17; 32])).await.unwrap();
        assert_eq!(grant(&sm).await, first);
        let before = d1_bytes(&sm).await;
        assert_eq!(
            apply(&mut sm, activation([19; 32])).await,
            Err(D1Error::IdempotencyConflict)
        );
        assert_eq!(d1_bytes(&sm).await, before);
        assert_eq!(
            apply(
                &mut sm,
                D1Command::EnableVersionedReads {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    journal: empty_byte(),
                }
            )
            .await,
            Err(D1Error::VisibilityHistory)
        );
        apply(
            &mut sm,
            D1Command::SetDedupCapacity {
                shard: 7,
                epoch: 1,
                expected_bytes: crate::MAX_DEDUP_BYTES,
                new_bytes: crate::EXTENDED_DEDUP_BYTES,
            },
        )
        .await
        .unwrap();
        assert_eq!(sm.state.read().await.d1.as_ref().unwrap().version, 7);
        let image = sm.build_snapshot().await.unwrap();
        drop(sm);
        let sm = Arc::new(StateMachine::open(directory.path()).unwrap());
        assert_eq!(grant(&sm).await, first);
        let mut restored = Arc::new(StateMachine::default());
        restored
            .install_snapshot(&image.meta, image.snapshot)
            .await
            .unwrap();
        assert_eq!(grant(&restored).await, first);
        assert_eq!(d1_bytes(&restored).await, d1_bytes(&sm).await);
    }

    #[tokio::test]
    async fn fresh_same_log_id_nonce_cannot_reuse_grant_or_snapshot_replace_binding() {
        let mut first = Arc::new(StateMachine::default());
        setup_byte(&mut first, [17; 32]).await;
        let journal = confirmed(&first).await;
        let image = first.build_snapshot().await.unwrap();
        let mut fresh = Arc::new(StateMachine::default());
        setup_byte(&mut fresh, [19; 32]).await;
        assert_eq!(
            grant(&first).await.activation.log_id,
            grant(&fresh).await.activation.log_id
        );
        assert_ne!(
            grant(&first).await.activation.nonce,
            grant(&fresh).await.activation.nonce
        );
        let before = d1_bytes(&fresh).await;
        let page = journal.history_page(&status(&fresh).await.0, None).unwrap();
        assert_eq!(
            apply(&mut fresh, sync(page)).await,
            Err(D1Error::VisibilityHistory)
        );
        assert_eq!(d1_bytes(&fresh).await, before);
        let mut meta = image.meta;
        meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 100));
        assert!(fresh.install_snapshot(&meta, image.snapshot).await.is_err());
        assert_eq!(d1_bytes(&fresh).await, before);
    }

    #[tokio::test]
    async fn raw_prepare_requires_retained_confirmation_and_exact_original_plan() {
        let mut sm = Arc::new(StateMachine::default());
        setup_byte(&mut sm, [17; 32]).await;
        assert_eq!(
            apply(&mut sm, prepare(1)).await,
            Err(D1Error::CoordinatorRejected(
                crate::transaction_decisions::Error::CapacityNotConfirmed,
            ))
        );
        let mut journal = confirmed(&sm).await;
        sync_all(&mut sm, &journal).await;
        assert_eq!(
            apply(&mut sm, prepare(1)).await,
            Err(D1Error::VisibilityHistory)
        );
        let plan = crate::request_plans::Plan {
            version: 1,
            id: id(1),
            authority: authority(),
            cohort: binding(),
            validation_version: 0,
            parts: vec![crate::request_plans::Part {
                descriptor: descriptor(7),
                request: request(1),
            }],
        };
        let payload = plan.encode().unwrap();
        journal
            .apply(Command::AdmitRequest {
                id: id(99),
                request: b"original".to_vec(),
                payload: payload.clone(),
                transaction_id: id(1),
                participants: vec![descriptor(7)],
            })
            .unwrap();
        sync_all(&mut sm, &journal).await;
        let mut wrong = request(1);
        wrong.operations = vec![WriteOp::put("x", "changed")];
        assert_eq!(
            apply(
                &mut sm,
                D1Command::PrepareParticipant {
                    authority: authority(),
                    participant: descriptor(7),
                    request: wrong,
                }
            )
            .await,
            Err(D1Error::IdempotencyConflict)
        );
        apply(&mut sm, prepare(1)).await.unwrap();
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
        let cut = sync_all(&mut sm, &journal).await;
        assert_eq!(status(&sm).await.1, 0);
        assert_eq!(sm.get_raw(b"x").await, None);
        apply(&mut sm, advance(1, &cut)).await.unwrap();
        assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
        assert_eq!(
            apply(&mut sm, prepare(1)).await.unwrap(),
            D1Result::Participant(Status::Committed { sequence: 1 })
        );
        let before = journal.encode().unwrap();
        journal
            .apply(Command::AdmitRequest {
                id: id(99),
                request: b"original".to_vec(),
                payload: b"losing candidate".to_vec(),
                transaction_id: id(2),
                participants: vec![descriptor(7)],
            })
            .unwrap();
        assert_eq!(journal.encode().unwrap(), before);
        assert_eq!(journal.request(id(99)).unwrap().payload, payload);
        assert!(read(&sm, 0, &journal).await.unwrap().entries.is_empty());
    }

    #[tokio::test]
    async fn legacy_binding_and_malformed_generation_never_activate_new_profile() {
        let mut legacy = Arc::new(StateMachine::default());
        setup(&mut legacy).await;
        let before = d1_bytes(&legacy).await;
        assert_eq!(
            apply(&mut legacy, activation([17; 32])).await,
            Err(D1Error::VisibilityHistory)
        );
        assert_eq!(d1_bytes(&legacy).await, before);
        assert!(legacy
            .participant_capacity_grant(7, 1, [7; 16], &authority(), &binding())
            .await
            .is_err());
        let mut state = legacy.state.read().await.d1.as_ref().unwrap().clone();
        state.visibility.as_mut().unwrap().activation = Some(CapacityActivation {
            nonce: [17; 32],
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
        });
        assert!(state.validate().is_err());
        let mut sm = Arc::new(StateMachine::default());
        setup_byte(&mut sm, [17; 32]).await;
        let mut state = sm.state.read().await.d1.as_ref().unwrap().clone();
        state.visibility.as_mut().unwrap().activation = None;
        assert!(state.validate().is_err());
        assert!(sm
            .state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .validate_capacity_activation(Some(LogId::new(CommittedLeaderId::new(1, 1), 3)))
            .is_err());
    }

    #[tokio::test]
    async fn failed_activation_sync_publishes_no_nonce_then_exact_retry_succeeds() {
        let directory = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(directory.path()).unwrap());
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
        ] {
            apply(&mut sm, command).await.unwrap();
        }
        assert_eq!(
            sm.participant_capacity_grant(7, 1, [7; 16], &authority(), &binding())
                .await,
            Err(D1Error::VisibilityNotEnabled)
        );
        let before = d1_bytes(&sm).await;
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm.apply([entry(4, activation([17; 32]))]).await.is_err());
        assert_eq!(d1_bytes(&sm).await, before);
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(directory.path()).unwrap());
        assert_eq!(d1_bytes(&sm).await, before);
        apply(&mut sm, activation([17; 32])).await.unwrap();
        assert_eq!(grant(&sm).await.activation.nonce, [17; 32]);
    }

    #[tokio::test]
    async fn validated_full_d1_history_still_consumes_unknown_abort_and_nonparticipant() {
        let mut origin = Arc::new(StateMachine::default());
        setup_byte(&mut origin, [17; 32]).await;
        let mut state = origin.state.read().await.d1.as_ref().unwrap().clone();
        let mut journal = confirmed(&origin).await;
        // Construct one complete validated recovery fixture. This deliberately
        // does not stand in for the separate 1025 real gateway-commit acceptance.
        for ordinal in 1..=crate::MAX_DEDUP_REQUESTS as u64 {
            let mut req = request(ordinal);
            req.expected_sequence = Some(ordinal - 1);
            req.operations = vec![WriteOp::put("x", ordinal.to_string())];
            journal
                .replay_event(Command::Begin {
                    id: id(ordinal),
                    participants: vec![descriptor(7)],
                })
                .unwrap();
            journal
                .replay_event(Command::Prepared {
                    id: id(ordinal),
                    participant: descriptor(7),
                })
                .unwrap();
            journal
                .replay_event(Command::Decide {
                    id: id(ordinal),
                    decision: Decision::Commit,
                })
                .unwrap();
            state
                .participant
                .as_mut()
                .unwrap()
                .records
                .push(crate::participant_writes::Intent {
                    id: id(ordinal),
                    participant: descriptor(7),
                    request: Some(req.clone()),
                    decision: Some(Decision::Commit),
                    sequence: Some(ordinal),
                });
            state.dedup.push(DedupRecord {
                request: req,
                sequence: ordinal,
            });
            state.visibility.as_mut().unwrap().commits.push(
                crate::versioned_reads::CommitVersion {
                    ordinal,
                    id: id(ordinal),
                    sequence: ordinal,
                },
            );
        }
        state.sequence = crate::MAX_DEDUP_REQUESTS as u64;
        state.visibility.as_mut().unwrap().applied = state.sequence;
        state.visibility.as_mut().unwrap().journal = journal.clone().into();
        state.validate().unwrap();
        let first_request = state.dedup[0].request.clone();
        let mut image = origin.build_snapshot().await.unwrap();
        image.meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 20_000));
        let snapshot = SnapshotV1 {
            version: 1,
            entries: vec![(b"x".to_vec(), state.sequence.to_string().into_bytes())],
            d1: state,
        };
        let directory = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(directory.path()).unwrap());
        sm.install_snapshot(
            &image.meta,
            Box::new(Cursor::new(serde_json::to_vec(&snapshot).unwrap())),
        )
        .await
        .unwrap();
        let exhausted = crate::MAX_DEDUP_REQUESTS as u64 + 1;
        journal
            .apply(Command::Begin {
                id: id(exhausted),
                participants: vec![descriptor(7)],
            })
            .unwrap();
        sync_all(&mut sm, &journal).await;
        let mut req = request(exhausted);
        req.expected_sequence = Some(exhausted - 1);
        assert_eq!(
            apply(
                &mut sm,
                D1Command::PrepareParticipant {
                    authority: authority(),
                    participant: descriptor(7),
                    request: req,
                }
            )
            .await,
            Err(D1Error::DedupCapacity)
        );
        journal
            .apply(Command::Decide {
                id: id(exhausted),
                decision: Decision::Abort,
            })
            .unwrap();
        let cut = sync_all(&mut sm, &journal).await;
        apply(&mut sm, advance(exhausted, &cut)).await.unwrap();
        journal
            .apply(Command::Begin {
                id: id(exhausted + 1),
                participants: vec![descriptor(8)],
            })
            .unwrap();
        journal
            .apply(Command::Decide {
                id: id(exhausted + 1),
                decision: Decision::Abort,
            })
            .unwrap();
        let cut = sync_all(&mut sm, &journal).await;
        apply(&mut sm, advance(exhausted + 1, &cut)).await.unwrap();
        assert_eq!(
            apply(
                &mut sm,
                D1Command::PrepareParticipant {
                    authority: authority(),
                    participant: descriptor(7),
                    request: first_request,
                }
            )
            .await
            .unwrap(),
            D1Result::Participant(Status::Committed { sequence: 1 })
        );
        assert_eq!(
            sm.state
                .read()
                .await
                .d1
                .as_ref()
                .unwrap()
                .participant
                .as_ref()
                .unwrap()
                .records
                .len(),
            crate::MAX_DEDUP_REQUESTS
        );
        drop(sm);
        let sm = Arc::new(StateMachine::open(directory.path()).unwrap());
        assert_eq!(status(&sm).await, (cut, exhausted + 1));
        assert_eq!(
            read(&sm, 1, &journal)
                .await
                .unwrap()
                .entries
                .get(b"x".as_slice()),
            Some(&b"1".to_vec())
        );
        assert!(read(&sm, 0, &journal).await.unwrap().entries.is_empty());
    }
}
