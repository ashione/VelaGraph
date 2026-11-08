use super::*;
use crate::participant_writes::Authority;
use crate::request_index::{MANIFEST_KEY, PREFIX, PREFIX_END};
use crate::request_plans::{Part, Plan};
use crate::transaction_decisions::{Cohort, Command, Decision, Member, Participant};
use openraft::CommittedLeaderId;

fn id(n: u64) -> [u8; 16] {
    let mut id = [0; 16];
    id[8..].copy_from_slice(&n.to_be_bytes());
    id
}
fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(LogCommand::D1V1(command)),
    }
}
fn command(command: Command) -> D1Command {
    D1Command::Coordinator {
        shard: 7,
        epoch: 3,
        mode_id: [1; 16],
        command,
    }
}
fn cohort() -> Cohort {
    Cohort {
        profile: Default::default(),
        coordinator_shard: 7,
        coordinator_epoch: 3,
        members: vec![Member {
            shard: 8,
            epoch: 1,
            mode: [3; 16],
        }],
    }
}
fn admit(external: u64, tx: u64) -> D1Command {
    let participant = Participant {
        shard: 8,
        epoch: 1,
        mode: [3; 16],
        intent: [4; 32],
    };
    let payload = Plan {
        version: 1,
        id: id(tx),
        authority: Authority {
            shard: 7,
            mode: [1; 16],
        },
        cohort: cohort(),
        validation_version: 0,
        parts: vec![Part {
            descriptor: participant.clone(),
            request: crate::D1WriteRequest {
                shard: 8,
                epoch: 1,
                request_id: id(tx),
                expected_sequence: Some(0),
                operations: vec![WriteOp::put("key", "value")],
            },
        }],
    }
    .encode()
    .unwrap();
    command(Command::AdmitRequest {
        id: id(external),
        request: format!("canonical-{external}").into_bytes(),
        payload,
        transaction_id: id(tx),
        participants: vec![participant],
    })
}
async fn setup(sm: &mut Arc<StateMachine>) {
    let results = sm
        .apply([
            entry(1, D1Command::Activate { shard: 7, epoch: 3 }),
            entry(
                2,
                D1Command::EnableManagedWrites {
                    shard: 7,
                    epoch: 3,
                    expected_sequence: 0,
                    mode_id: [1; 16],
                },
            ),
            entry(
                3,
                D1Command::EnableCoordinator {
                    shard: 7,
                    epoch: 3,
                    mode_id: [1; 16],
                },
            ),
            entry(4, command(Command::BindCohort { cohort: cohort() })),
        ])
        .await
        .unwrap();
    assert!(results.iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
}
async fn lookup(sm: &StateMachine, external: u64) -> crate::request_index::RequestLookup {
    sm.coordinator_request(7, 3, [1; 16], id(external))
        .await
        .unwrap()
}
fn disk(sm: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .iterator(IteratorMode::Start)
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}
fn index_rows(sm: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    disk(sm)
        .into_iter()
        .filter(|(k, _)| k.starts_with(PREFIX))
        .collect()
}
fn fail_sync(sm: &StateMachine) {
    sm.storage
        .as_ref()
        .unwrap()
        .fail_next_write
        .store(true, Ordering::Relaxed);
}

#[tokio::test]
async fn coordinator_v5_v9_and_logical_snapshot_golden() {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    let ones = vec!["1"; 16].join(",");
    let threes = vec!["3"; 16].join(",");
    let golden_state = format!("{{\"version\":4,\"managed\":{{\"mode_id\":[{ones}],\"activated_sequence\":0}},\"coordinator\":{{\"version\":2,\"authority\":[{ones}],\"events\":[{{\"BindCohort\":{{\"cohort\":{{\"coordinator_shard\":7,\"coordinator_epoch\":3,\"members\":[{{\"shard\":8,\"epoch\":1,\"mode\":[{threes}]}}]}}}}}}]}},\"shard\":7,\"epoch\":3,\"sequence\":0,\"dedup\":[]}}");
    let state = sm.state.read().await;
    let d1 = state.d1.as_ref().unwrap();
    assert_eq!(serde_json::to_string(d1).unwrap(), golden_state);
    #[derive(Serialize)]
    struct Legacy<'a> {
        version: u32,
        state: &'a D1State,
    }
    assert_eq!(
        serde_json::to_string(&Legacy {
            version: 5,
            state: d1
        })
        .unwrap(),
        format!("{{\"version\":5,\"state\":{golden_state}}}")
    );
    // v9 is a published migration input, no longer a bound-state writer.
    let legacy = serde_json::to_string(&Legacy {
        version: 9,
        state: d1,
    })
    .unwrap();
    assert_eq!(
        legacy,
        format!("{{\"version\":9,\"state\":{golden_state}}}")
    );
    drop(state);
    let snapshot = sm.build_snapshot().await.unwrap();
    assert_eq!(
        snapshot.snapshot.get_ref(),
        format!("{{\"version\":1,\"entries\":[],\"d1\":{golden_state}}}").as_bytes()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn index_admission_batch_retry_failure_and_reopen_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm).await;
    let before = disk(&sm);
    fail_sync(&sm);
    assert!(sm.apply([entry(5, admit(900, 1))]).await.is_err());
    assert_eq!(disk(&sm), before);
    assert!(lookup(&sm, 900).await.record.is_none());
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert!(lookup(&sm, 900).await.record.is_none());
    // Same-ID competing candidate in one apply overlay must lose to first Plan.
    let results = sm
        .apply([
            entry(5, admit(900, 1)),
            entry(6, admit(900, 2)),
            entry(7, admit(2, 3)),
        ])
        .await
        .unwrap();
    assert!(results.iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
    assert_eq!(index_rows(&sm).len(), 2);
    let original = lookup(&sm, 900).await.record.unwrap();
    assert_eq!(original.record.transaction_id, id(1));
    assert_eq!(original.transaction.ordinal, 1);
    assert_eq!(lookup(&sm, 2).await.record.unwrap().transaction.ordinal, 2);
    let mut changed = admit(900, 4);
    if let D1Command::Coordinator {
        command: Command::AdmitRequest { request, .. },
        ..
    } = &mut changed
    {
        *request = b"changed canonical".to_vec();
    }
    let result = sm
        .apply([entry(8, changed), entry(9, admit(900, 5))])
        .await
        .unwrap();
    assert_eq!(
        result[0].d1,
        Some(Err(D1Error::CoordinatorRejected(
            crate::transaction_decisions::Error::IdentityConflict
        )))
    );
    assert!(result[1].d1.as_ref().unwrap().is_ok());
    assert_eq!(lookup(&sm, 900).await.record.unwrap(), original);
    assert_eq!(index_rows(&sm).len(), 2);
    drop(sm);
    let sm = StateMachine::open(dir.path()).unwrap();
    assert_eq!(lookup(&sm, 900).await.record.unwrap(), original);
    assert_eq!(sm.applied().await.unwrap().index, 9);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &sm.storage
                .as_ref()
                .unwrap()
                .db
                .get(D1_KEY)
                .unwrap()
                .unwrap()
        )
        .unwrap()["version"],
        11
    );
}

async fn legacy_fixture(path: &Path) {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    sm.apply([entry(5, admit(900, 1))]).await.unwrap();
    let state = sm.state.read().await;
    #[derive(Serialize)]
    struct Legacy<'a> {
        version: u32,
        state: &'a D1State,
    }
    let db = DB::open_default(path).unwrap();
    let mut batch = WriteBatch::default();
    batch.put(
        D1_KEY,
        serde_json::to_vec(&Legacy {
            version: 5,
            state: state.d1.as_ref().unwrap(),
        })
        .unwrap(),
    );
    batch.put(
        APPLIED_KEY,
        serde_json::to_vec(&state.last_applied_log).unwrap(),
    );
    batch.put(
        MEMBERSHIP_KEY,
        serde_json::to_vec(&state.last_membership).unwrap(),
    );
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn legacy_migration_is_atomic_and_legacy_point_read_is_explicit() {
    let dir = tempfile::tempdir().unwrap();
    legacy_fixture(dir.path()).await;
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    let original = lookup(&sm, 900).await;
    assert!(original.record.is_some());
    assert!(index_rows(&sm).is_empty());
    assert!(!sm
        .storage
        .as_ref()
        .unwrap()
        .request_indexed
        .load(Ordering::Relaxed));
    let before = disk(&sm);
    fail_sync(&sm);
    assert!(sm.apply([entry(6, admit(900, 2))]).await.is_err());
    assert_eq!(disk(&sm), before);
    assert_eq!(lookup(&sm, 900).await, original);
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    sm.apply([entry(6, admit(900, 2))]).await.unwrap();
    assert!(sm
        .storage
        .as_ref()
        .unwrap()
        .request_indexed
        .load(Ordering::Relaxed));
    assert_eq!(index_rows(&sm).len(), 1);
    assert_eq!(lookup(&sm, 900).await, original);
    drop(sm);
    let sm = StateMachine::open(dir.path()).unwrap();
    assert_eq!(lookup(&sm, 900).await, original);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn corrupt_missing_extra_or_rewritten_index_fails_closed() {
    for case in 0..12 {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm).await;
        sm.apply([entry(5, admit(900, 1))]).await.unwrap();
        let (key, value) = index_rows(&sm).pop().unwrap();
        let db = &sm.storage.as_ref().unwrap().db;
        match case {
            0 => db.delete(&key).unwrap(),
            1 => db.delete(MANIFEST_KEY).unwrap(),
            2 => {
                let mut other = key.clone();
                *other.last_mut().unwrap() ^= 1;
                db.put(other, &value).unwrap();
            }
            3..=6 => {
                let mut v: serde_json::Value = serde_json::from_slice(&value).unwrap();
                match case {
                    3 => v["record"]["payload"][0] = 0.into(),
                    4 => v["record"]["request"][0] = 0.into(),
                    5 => v["ordinal"] = 2.into(),
                    _ => v["version"] = 2.into(),
                }
                db.put(&key, serde_json::to_vec(&v).unwrap()).unwrap();
            }
            7 => {
                let mut other = key.clone();
                other[PREFIX.len() + 4] = 1;
                db.delete(&key).unwrap();
                db.put(other, &value).unwrap();
            }
            8 => {
                let mut v: serde_json::Value =
                    serde_json::from_slice(&db.get(MANIFEST_KEY).unwrap().unwrap()).unwrap();
                v["requests"] = 0.into();
                db.put(MANIFEST_KEY, serde_json::to_vec(&v).unwrap())
                    .unwrap();
            }
            9 => {
                let mut v: serde_json::Value =
                    serde_json::from_slice(&db.get(D1_KEY).unwrap().unwrap()).unwrap();
                v["version"] = 11.into();
                db.put(D1_KEY, serde_json::to_vec(&v).unwrap()).unwrap();
            }
            10 => {
                db.put(&key, b"{\"version\":1,\"version\":1}").unwrap();
            }
            _ => db
                .put(
                    &key,
                    vec![b' '; crate::transaction_decisions::MAX_REQUEST_EVENT_BYTES + 1],
                )
                .unwrap(),
        }
        if matches!(case, 0 | 1 | 3..=8 | 10 | 11) {
            assert_eq!(
                sm.coordinator_request(7, 3, [1; 16], id(900)).await,
                Err(D1Error::RequestIndexHistory),
                "case {case}"
            );
        }
        if matches!(case, 1 | 8) {
            let before = disk(&sm);
            assert!(sm.apply([entry(6, admit(900, 2))]).await.is_err());
            assert_eq!(disk(&sm), before);
        }
        drop(sm);
        assert!(StateMachine::open(dir.path()).is_err(), "case {case}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn snapshot_rebuild_replaces_index_atomically_and_rejects_history_loss() {
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = Arc::new(StateMachine::open(source_dir.path()).unwrap());
    setup(&mut source).await;
    source.apply([entry(5, admit(900, 1))]).await.unwrap();
    let original = lookup(&source, 900).await;
    let old = source.build_snapshot().await.unwrap();
    source.apply([entry(6, admit(2, 3))]).await.unwrap();
    let image = source.build_snapshot().await.unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mut target = Arc::new(StateMachine::open(target_dir.path()).unwrap());
    target
        .install_snapshot(
            &old.meta,
            Box::new(Cursor::new(old.snapshot.get_ref().clone())),
        )
        .await
        .unwrap();
    // An orphan derived row is replaced only by an explicitly validated snapshot.
    let (mut extra_key, extra_value) = index_rows(&target).pop().unwrap();
    *extra_key.last_mut().unwrap() ^= 1;
    target
        .storage
        .as_ref()
        .unwrap()
        .db
        .put(&extra_key, extra_value)
        .unwrap();
    let before = disk(&target);
    fail_sync(&target);
    assert!(target
        .install_snapshot(
            &image.meta,
            Box::new(Cursor::new(image.snapshot.get_ref().clone()))
        )
        .await
        .is_err());
    assert_eq!(disk(&target), before);
    assert_eq!(lookup(&target, 900).await, original);
    target
        .install_snapshot(
            &image.meta,
            Box::new(Cursor::new(image.snapshot.get_ref().clone())),
        )
        .await
        .unwrap();
    assert_eq!(index_rows(&target), index_rows(&source));
    let mut fake_newer = old.meta.clone();
    fake_newer.last_log_id = Some(LogId::new(CommittedLeaderId::new(2, 1), 100));
    let before = disk(&target);
    assert!(target
        .install_snapshot(&fake_newer, old.snapshot)
        .await
        .is_err());
    assert_eq!(disk(&target), before);
    let mut bad: serde_json::Value = serde_json::from_slice(image.snapshot.get_ref()).unwrap();
    bad["d1"]["coordinator"]["events"][1]["AdmitRequest"]["request"][0] = 0.into();
    assert!(target
        .install_snapshot(
            &fake_newer,
            Box::new(Cursor::new(serde_json::to_vec(&bad).unwrap()))
        )
        .await
        .is_err());
    assert_eq!(disk(&target), before);
    drop(target);
    let target = StateMachine::open(target_dir.path()).unwrap();
    assert_eq!(lookup(&target, 900).await, original);
    assert!(lookup(&target, 2).await.record.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn request_quota_does_not_expire_old_ids_or_reserved_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm).await;
    let mut entries = Vec::new();
    for n in 1..=crate::transaction_decisions::MAX_REQUESTS as u64 {
        entries.push(entry(4 + n, admit(n, 1000 + n)));
    }
    let results = sm.apply(entries).await.unwrap();
    assert!(results.iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
    let original = lookup(&sm, 1).await.record.unwrap();
    let mut changed = admit(1, 4000);
    if let D1Command::Coordinator {
        command: Command::AdmitRequest { request, .. },
        ..
    } = &mut changed
    {
        *request = b"changed at full quota".to_vec();
    }
    let results = sm
        .apply([
            entry(261, admit(257, 2000)),
            entry(262, admit(1, 3000)),
            entry(
                263,
                command(Command::Decide {
                    id: id(1001),
                    decision: Decision::Abort,
                }),
            ),
            entry(264, changed),
        ])
        .await
        .unwrap();
    assert_eq!(
        results[0].d1,
        Some(Err(D1Error::CoordinatorRejected(
            crate::transaction_decisions::Error::Capacity
        )))
    );
    assert!(results[1..3].iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
    assert_eq!(
        results[3].d1,
        Some(Err(D1Error::CoordinatorRejected(
            crate::transaction_decisions::Error::IdentityConflict
        )))
    );
    let after = lookup(&sm, 1).await.record.unwrap();
    assert_eq!(after.record, original.record);
    assert_eq!(after.transaction.decision, Some(Decision::Abort));
    assert!(lookup(&sm, 257).await.record.is_none());
    drop(sm);
    let sm = StateMachine::open(dir.path()).unwrap();
    assert_eq!(lookup(&sm, 1).await.record.unwrap(), after);
    assert_eq!(index_rows(&sm).len(), 256);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn legacy_index_mixture_is_rejected_and_not_repaired() {
    let dir = tempfile::tempdir().unwrap();
    legacy_fixture(dir.path()).await;
    {
        let db = DB::open_default(dir.path()).unwrap();
        db.put([PREFIX, b"orphan"].concat(), b"{}").unwrap();
    }
    assert!(StateMachine::open(dir.path()).is_err());
    assert!(PREFIX < PREFIX_END);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn request_index_crash_child() -> anyhow::Result<()> {
    let Some(path) = std::env::var_os("D2_REQUEST_INDEX_CRASH_DIRECTORY") else {
        return Ok(());
    };
    let path = std::path::PathBuf::from(path);
    let mode = std::env::var("D2_REQUEST_INDEX_CRASH_MODE")?;
    let mut sm = Arc::new(StateMachine::open(path.join("state.db"))?);
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (_release, gate) = std::sync::mpsc::channel();
    let writer = if mode == "snapshot" {
        let mut source = Arc::new(StateMachine::default());
        setup(&mut source).await;
        source.apply([entry(5, admit(900, 1))]).await?;
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
        *sm.apply_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
            entered,
            release: gate,
        });
        let command = if mode == "migration" {
            entry(6, admit(2, 3))
        } else {
            entry(5, admit(900, 1))
        };
        tokio::spawn(async move { sm.apply([command]).await.map(|_| ()) })
    };
    ready.await?;
    std::fs::write(path.join("synced"), b"ready")?;
    writer.await??;
    anyhow::bail!("parent must kill child after sync and before publication")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn lost_ack_process_death_recovers_admission_migration_and_snapshot() -> anyhow::Result<()> {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for mode in ["admission", "migration", "snapshot"] {
        let dir = tempfile::tempdir()?;
        let state_path = dir.path().join("state.db");
        if mode == "migration" {
            legacy_fixture(&state_path).await;
        } else if mode == "admission" {
            let mut sm = Arc::new(StateMachine::open(&state_path)?);
            setup(&mut sm).await;
        }
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "state_machine::request_index_tests::request_index_crash_child",
                    "--nocapture",
                ])
                .env("D2_REQUEST_INDEX_CRASH_DIRECTORY", dir.path())
                .env("D2_REQUEST_INDEX_CRASH_MODE", mode)
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
        assert!(sm
            .storage
            .as_ref()
            .unwrap()
            .request_indexed
            .load(Ordering::Relaxed));
        let original = lookup(&sm, 900).await.record.unwrap();
        assert_eq!(original.record.transaction_id, id(1));
        assert_eq!(original.transaction.ordinal, 1);
        assert_eq!(
            index_rows(&sm).len(),
            if mode == "migration" { 2 } else { 1 }
        );
        if mode == "snapshot" {
            assert!(sm.get_current_snapshot().await?.is_some());
        }
        // The caller saw no ACK, then retries with a different candidate Plan.
        let next = sm.applied().await.unwrap().index + 1;
        sm.apply([entry(next, admit(900, 999))]).await?;
        assert_eq!(lookup(&sm, 900).await.record.unwrap(), original);
    }
    Ok(())
}
