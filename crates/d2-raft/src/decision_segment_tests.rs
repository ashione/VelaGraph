use super::*;
use crate::decision_segments::{MANIFEST_KEY, PREFIX, PREFIX_END};
use crate::participant_writes::Authority;
use crate::transaction_decisions::{Cohort, Command, Decision, Journal, Member, Participant};
use openraft::CommittedLeaderId;

fn id(n: u64) -> [u8; 16] {
    let mut id = [0; 16];
    id[8..].copy_from_slice(&n.to_be_bytes());
    id
}
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn cohort() -> Cohort {
    Cohort {
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
fn participant() -> Participant {
    Participant {
        shard: 8,
        epoch: 1,
        mode: [8; 16],
        intent: [4; 32],
    }
}
fn begin(n: u64) -> Command {
    Command::Begin {
        id: id(n),
        participants: vec![participant()],
    }
}
fn decide(n: u64) -> Command {
    Command::Decide {
        id: id(n),
        decision: Decision::Abort,
    }
}
fn journal(count: u64) -> Journal {
    let mut journal = Journal::new([9; 16]).unwrap();
    journal
        .apply(Command::BindCohort { cohort: cohort() })
        .unwrap();
    for n in 1..=count {
        journal.apply(begin(n)).unwrap();
        journal.apply(decide(n)).unwrap();
    }
    journal
}
fn coordinator(command: Command) -> D1Command {
    D1Command::Coordinator {
        shard: 9,
        epoch: 1,
        mode_id: [9; 16],
        command,
    }
}
fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(LogCommand::D1V1(command)),
    }
}
fn setup_commands(member: bool) -> Vec<D1Command> {
    let shard = if member { 7 } else { 9 };
    let mut commands = vec![
        D1Command::Activate { shard, epoch: 1 },
        D1Command::EnableManagedWrites {
            shard,
            epoch: 1,
            expected_sequence: 0,
            mode_id: [shard as u8; 16],
        },
    ];
    if member {
        commands.push(D1Command::EnableParticipant {
            shard,
            epoch: 1,
            mode_id: [7; 16],
            authority: authority(),
        });
        commands.push(D1Command::EnableVersionedReads {
            shard,
            epoch: 1,
            mode_id: [7; 16],
            journal: journal(0),
        });
    } else {
        commands.push(D1Command::EnableCoordinator {
            shard,
            epoch: 1,
            mode_id: [9; 16],
        });
        commands.push(coordinator(Command::BindCohort { cohort: cohort() }));
    }
    commands
}
async fn apply(sm: &mut Arc<StateMachine>, commands: Vec<D1Command>) {
    let first = sm.applied().await.map_or(1, |v| v.index + 1);
    let results = sm
        .apply(
            commands
                .into_iter()
                .enumerate()
                .map(|(n, c)| entry(first + n as u64, c)),
        )
        .await
        .unwrap();
    assert!(
        results.iter().all(|r| r.d1.as_ref().unwrap().is_ok()),
        "{results:?}"
    );
}
async fn setup(sm: &mut Arc<StateMachine>, member: bool) {
    apply(sm, setup_commands(member)).await;
}
async fn append(sm: &mut Arc<StateMachine>, from: u64, to: u64) {
    apply(
        sm,
        (from..=to)
            .flat_map(|n| [coordinator(begin(n)), coordinator(decide(n))])
            .collect(),
    )
    .await;
}
fn advance(ordinal: u64, journal: &Journal) -> D1Command {
    D1Command::AdvanceVisibility {
        authority: authority(),
        shard: 7,
        epoch: 1,
        mode_id: [7; 16],
        ordinal,
        journal: journal.clone(),
    }
}
fn rows(sm: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .iterator(IteratorMode::From(PREFIX, Direction::Forward))
        .map(|r| r.unwrap())
        .take_while(|(k, _)| k.starts_with(PREFIX))
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect()
}
fn manifest(sm: &StateMachine) -> serde_json::Value {
    serde_json::from_slice(
        &sm.storage
            .as_ref()
            .unwrap()
            .db
            .get(MANIFEST_KEY)
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
async fn prefix(sm: &StateMachine, v: u64, member: bool) -> crate::terminal_prefix::TerminalPrefix {
    if member {
        sm.participant_terminal_prefix(7, 1, [7; 16], &journal(v).terminal_prefix(v).unwrap())
            .await
            .unwrap()
    } else {
        sm.coordinator_terminal_prefix(9, 1, [9; 16], v)
            .await
            .unwrap()
    }
}
fn limit() -> crate::versioned_reads::ReadLimit {
    crate::versioned_reads::ReadLimit {
        rows: 10,
        bytes: 1024,
        operations: 10,
    }
}
fn fail_sync(sm: &StateMachine) {
    sm.storage
        .as_ref()
        .unwrap()
        .fail_next_write
        .store(true, Ordering::Relaxed);
}
async fn downgrade(sm: &StateMachine, member: bool) {
    let storage = sm.storage.as_ref().unwrap();
    let state = sm.state.read().await;
    // Construct an actual published legacy header, not a new header with a
    // changed number. The fixture is reopened before any read or mutation.
    let metadata = serde_json::json!({
        "version": if member { 7 } else { 8 },
        "state": state.d1.as_ref().unwrap().stage(),
    });
    let mut batch = WriteBatch::default();
    batch.put(D1_KEY, serde_json::to_vec(&metadata).unwrap());
    batch.delete_range(PREFIX, PREFIX_END);
    batch.delete(MANIFEST_KEY);
    batch.delete_range(event_ledger::PREFIX, b"sm/ledger0");
    storage.write_sync(batch).unwrap();
}

#[tokio::test]
async fn boundaries_holes_and_member_applied_are_distinct() {
    let coord_dir = tempfile::tempdir().unwrap();
    let member_dir = tempfile::tempdir().unwrap();
    let mut coord = Arc::new(StateMachine::open(coord_dir.path()).unwrap());
    let mut member = Arc::new(StateMachine::open(member_dir.path()).unwrap());
    setup(&mut coord, false).await;
    setup(&mut member, true).await;
    assert_eq!(
        prefix(&coord, 0, false).await,
        prefix(&member, 0, true).await
    );
    assert_eq!(manifest(&coord)["through"], 0);
    append(&mut coord, 1, 31).await;
    let old = prefix(&coord, 1, false).await;
    assert_eq!(rows(&coord).len(), 1);
    append(&mut coord, 32, 32).await;
    let sealed = rows(&coord)[0].clone();
    // 33 is unresolved while 34..65 are terminal: no segment may jump the hole.
    apply(&mut coord, vec![coordinator(begin(33))]).await;
    append(&mut coord, 34, 65).await;
    assert_eq!(manifest(&coord)["through"], 32);
    assert_eq!(rows(&coord), vec![sealed.clone()]);
    assert_eq!(
        coord.coordinator_terminal_prefix(9, 1, [9; 16], 33).await,
        Err(D1Error::VisibilityUnresolved)
    );
    // Filling the hole publishes several absolute-ordinal segments atomically.
    apply(&mut coord, vec![coordinator(decide(33))]).await;
    assert_eq!(manifest(&coord)["through"], 65);
    assert_eq!(rows(&coord).len(), 3);
    assert_eq!(rows(&coord)[0], sealed);
    assert_eq!(prefix(&coord, 1, false).await, old);
    let history = coord
        .state
        .read()
        .await
        .d1
        .as_ref()
        .unwrap()
        .coordinator
        .clone()
        .unwrap();
    for ordinal in 1..=64 {
        apply(&mut member, vec![advance(ordinal, &history)]).await;
        if [31, 32, 33, 64].contains(&ordinal) {
            assert_eq!(manifest(&member)["through"], ordinal);
            assert_eq!(
                prefix(&member, ordinal, true).await,
                prefix(&coord, ordinal, false).await
            );
        }
    }
    assert_eq!(
        member
            .participant_visibility_info(7, 1, [7; 16])
            .await
            .unwrap()
            .applied_prefix,
        64
    );
    assert_eq!(
        member
            .participant_terminal_prefix(7, 1, [7; 16], &history.terminal_prefix(65).unwrap())
            .await,
        Err(D1Error::VisibilityNotApplied)
    );
    assert_eq!(member.d1_metadata().await, Some((7, 1, 0))); // Nonparticipant consumes no D1 slot.
    assert_eq!(
        member
            .state
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
        0
    );
    drop(coord);
    drop(member);
    let coord = StateMachine::open(coord_dir.path()).unwrap();
    let member = StateMachine::open(member_dir.path()).unwrap();
    assert_eq!(prefix(&coord, 1, false).await, old);
    assert_eq!(
        prefix(&member, 64, true).await,
        prefix(&coord, 64, false).await
    );
}

#[tokio::test]
async fn indexed_reads_detect_online_corruption_and_do_not_read_later_segments() {
    for member_mode in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, member_mode).await;
        if member_mode {
            apply(
                &mut sm,
                (1..=33).map(|n| advance(n, &journal(33))).collect(),
            )
            .await;
        } else {
            append(&mut sm, 1, 33).await;
        }
        let old = prefix(&sm, 1, member_mode).await;
        let stored = rows(&sm);
        let db = &sm.storage.as_ref().unwrap().db;
        db.put(&stored[1].0, b"broken later segment").unwrap();
        assert_eq!(prefix(&sm, 1, member_mode).await, old); // Does not scan unrelated segment.
        if member_mode {
            let expected = journal(33).terminal_prefix(33).unwrap();
            assert_eq!(
                sm.certified_terminal_read(7, 1, [7; 16], limit(), &expected)
                    .await,
                Err(D1Error::DecisionIndexHistory)
            );
        } else {
            assert_eq!(
                sm.coordinator_terminal_prefix(9, 1, [9; 16], 33).await,
                Err(D1Error::DecisionIndexHistory)
            );
        }
        db.put(&stored[1].0, &stored[1].1).unwrap();
        db.delete(&stored[0].0).unwrap();
        if member_mode {
            assert_eq!(
                sm.participant_terminal_prefix(7, 1, [7; 16], &old).await,
                Err(D1Error::DecisionIndexHistory)
            );
        } else {
            assert_eq!(
                sm.coordinator_terminal_prefix(9, 1, [9; 16], 1).await,
                Err(D1Error::DecisionIndexHistory)
            );
        }
        // V0 requires the exact manifest/domain but no terminal rows.
        assert_eq!(prefix(&sm, 0, member_mode).await.version(), 0);
        db.put(MANIFEST_KEY, b"{}").unwrap();
        if member_mode {
            assert_eq!(
                sm.participant_visibility_info(7, 1, [7; 16])
                    .await
                    .unwrap_err(),
                D1Error::DecisionIndexHistory
            );
        } else {
            assert_eq!(
                sm.coordinator_summary(9, 1, [9; 16]).await,
                Err(D1Error::DecisionIndexHistory)
            );
        }
        drop(sm);
        assert!(StateMachine::open(dir.path()).is_err());
    }
}

#[tokio::test]
async fn malformed_missing_extra_wrong_domain_and_commit_evidence_reject_reopen() {
    for case in 0..16 {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, false).await;
        append(&mut sm, 1, 33).await;
        let stored = rows(&sm);
        let db = &sm.storage.as_ref().unwrap().db;
        let mut value: serde_json::Value = serde_json::from_slice(&stored[0].1).unwrap();
        match case {
            0 => {
                db.delete(&stored[0].0).unwrap();
            }
            1 => {
                db.put([PREFIX, b"extra"].concat(), &stored[0].1).unwrap();
            }
            2 => {
                value["version"] = 2.into();
            }
            3 => {
                value["first"] = 2.into();
            }
            4 => {
                value["domain"]["role"] = "Member".into();
            }
            5 => {
                value["domain"]["cohort"]["members"][0]["epoch"] = 2.into();
            }
            6 => {
                value["records"][0]["decision"] = "Commit".into();
            }
            7 => {
                value["records"][0]["ordinal"] = 2.into();
            }
            8 => {
                value["records"].as_array_mut().unwrap().remove(0);
            }
            9 => {
                value["records"].as_array_mut().unwrap().swap(0, 1);
            }
            10 => {
                value["unknown"] = true.into();
            }
            11 => {
                value["records"][0]["prepared"] = serde_json::json!([false, true]);
            }
            12 => {
                db.delete(MANIFEST_KEY).unwrap();
            }
            13 => {
                let mut m = manifest(&sm);
                m["applied"]["index"] = 900.into();
                db.put(MANIFEST_KEY, serde_json::to_vec(&m).unwrap())
                    .unwrap();
            }
            14 => {
                let mut metadata: serde_json::Value =
                    serde_json::from_slice(&db.get(D1_KEY).unwrap().unwrap()).unwrap();
                metadata["version"] = 8.into();
                db.put(D1_KEY, serde_json::to_vec(&metadata).unwrap())
                    .unwrap();
            }
            _ => {
                db.put(&stored[0].0, &stored[1].1).unwrap();
            }
        }
        if (2..=11).contains(&case) {
            db.put(&stored[0].0, serde_json::to_vec(&value).unwrap())
                .unwrap();
        }
        drop(sm);
        assert!(
            StateMachine::open(dir.path()).is_err(),
            "accepted case {case}"
        );
    }
}

#[tokio::test]
async fn legacy_migration_partial_tail_and_snapshot_publish_are_atomic() {
    for member_mode in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, member_mode).await;
        if member_mode {
            apply(
                &mut sm,
                (1..=31).map(|n| advance(n, &journal(33))).collect(),
            )
            .await;
        } else {
            append(&mut sm, 1, 31).await;
        }
        let original = prefix(&sm, 31, member_mode).await;
        downgrade(&sm, member_mode).await;
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert!(rows(&sm).is_empty());
        assert_eq!(prefix(&sm, 31, member_mode).await, original);
        let next = if member_mode {
            vec![advance(32, &journal(33)), advance(33, &journal(33))]
        } else {
            vec![
                coordinator(begin(32)),
                coordinator(decide(32)),
                coordinator(begin(33)),
                coordinator(decide(33)),
            ]
        };
        let index = sm.applied().await.unwrap().index + 1;
        fail_sync(&sm);
        assert!(sm
            .apply(
                next.clone()
                    .into_iter()
                    .enumerate()
                    .map(|(n, c)| entry(index + n as u64, c))
            )
            .await
            .is_err());
        assert!(rows(&sm).is_empty());
        assert_eq!(prefix(&sm, 31, member_mode).await, original);
        apply(&mut sm, next).await;
        assert_eq!(rows(&sm).len(), 2);
        assert_eq!(prefix(&sm, 31, member_mode).await, original);
        let image = sm.build_snapshot().await.unwrap();
        let fresh_dir = tempfile::tempdir().unwrap();
        let mut fresh = Arc::new(StateMachine::open(fresh_dir.path()).unwrap());
        fail_sync(&fresh);
        assert!(fresh
            .install_snapshot(
                &image.meta,
                Box::new(Cursor::new(image.snapshot.get_ref().clone()))
            )
            .await
            .is_err());
        assert!(rows(&fresh).is_empty());
        assert_eq!(fresh.d1_metadata().await, None);
        // Explicit valid snapshot install clears corrupt/orphan derived rows.
        fresh
            .storage
            .as_ref()
            .unwrap()
            .db
            .put([PREFIX, b"orphan"].concat(), b"bad")
            .unwrap();
        fresh
            .install_snapshot(&image.meta, image.snapshot)
            .await
            .unwrap();
        assert_eq!(rows(&fresh), rows(&sm));
        assert_eq!(prefix(&fresh, 31, member_mode).await, original);
        drop(fresh);
        let fresh = StateMachine::open(fresh_dir.path()).unwrap();
        assert_eq!(
            prefix(&fresh, 33, member_mode).await,
            prefix(&sm, 33, member_mode).await
        );
    }
}

#[tokio::test]
async fn corrupt_partial_tail_cannot_be_repaired_by_an_ordinary_apply() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, false).await;
    append(&mut sm, 1, 1).await;
    let row = rows(&sm).remove(0);
    sm.storage.as_ref().unwrap().db.put(&row.0, b"bad").unwrap();
    let before = sm.applied().await;
    assert!(sm
        .apply([entry(before.unwrap().index + 1, coordinator(begin(2)))])
        .await
        .is_err());
    assert_eq!(sm.applied().await, before);
    assert_eq!(
        sm.storage
            .as_ref()
            .unwrap()
            .db
            .get(&row.0)
            .unwrap()
            .unwrap(),
        b"bad"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_rpc_source_and_certified_reads_use_persisted_segments() {
    use crate::managed_rpc::{Client, Error, Identity, NodeRef, Role, Server};
    for (member, raw) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let cluster = crate::Cluster::new();
        let node = Arc::new(cluster.start_durable(1, dir.path()).await.unwrap());
        node.raft
            .initialize(BTreeMap::from([(1, BasicNode::default())]))
            .await
            .unwrap();
        node.raft
            .wait(Some(std::time::Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        let commands = setup_commands(member).into_iter().chain(if member {
            vec![advance(1, &journal(1))]
        } else {
            vec![coordinator(begin(1)), coordinator(decide(1))]
        });
        for command in commands {
            node.raft
                .client_write(LogCommand::D1V1(command))
                .await
                .unwrap()
                .data
                .d1
                .unwrap()
                .unwrap();
        }
        let target = crate::versioned_reads::Target {
            shard: if member { 7 } else { 9 },
            epoch: 1,
            mode: [if member { 7 } else { 9 }; 16],
        };
        let identity = Identity {
            group: node.group_id(),
            node: 1,
            target: target.clone(),
            role: if member {
                Role::Participant
            } else {
                Role::Coordinator
            },
            authority: authority(),
            cohort: cohort(),
        };
        let server = Server::bind(
            node.clone(),
            identity.clone(),
            "127.0.0.1:0".parse().unwrap(),
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
        let client = Client::new(
            server.local_addr(),
            identity,
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        let remote = NodeRef::Remote(&client);
        let proof = journal(1).terminal_prefix(1).unwrap();
        if member {
            remote
                .read_terminal(&target, limit(), &proof)
                .await
                .unwrap();
        } else {
            assert_eq!(remote.terminal(&authority(), 1, 1).await.unwrap(), proof);
        }
        if raw {
            let row = node
                .state_machine
                .storage
                .as_ref()
                .unwrap()
                .db
                .iterator(IteratorMode::From(event_ledger::PREFIX, Direction::Forward))
                .next()
                .unwrap()
                .unwrap();
            node.state_machine
                .storage
                .as_ref()
                .unwrap()
                .db
                .delete(row.0)
                .unwrap();
            let expected = Error::Application(D1Error::VisibilityHistory);
            if member {
                assert_eq!(
                    remote
                        .history_status(&authority(), &target, &cohort())
                        .await,
                    Err(expected)
                );
            } else {
                assert_eq!(remote.journal(&authority(), 1).await, Err(expected.clone()));
                assert_eq!(
                    remote
                        .history_page(&authority(), 1, &journal(0).history_cursor().unwrap(), None)
                        .await,
                    Err(expected)
                );
            }
            server.shutdown().await;
            cluster.shutdown(&node).await;
            continue;
        }
        let row = rows(&node.state_machine).remove(0);
        node.state_machine
            .storage
            .as_ref()
            .unwrap()
            .db
            .delete(row.0)
            .unwrap();
        if member {
            assert_eq!(
                remote.prefix_terminal(&target, &proof).await,
                Err(Error::Application(D1Error::DecisionIndexHistory))
            );
            assert_eq!(
                remote
                    .read_terminal(&target, limit(), &proof)
                    .await
                    .unwrap_err(),
                Error::Application(D1Error::DecisionIndexHistory)
            );
        } else {
            assert_eq!(
                remote.terminal(&authority(), 1, 1).await,
                Err(Error::Application(D1Error::DecisionIndexHistory))
            );
        }
        server.shutdown().await;
        cluster.shutdown(&node).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn decision_segment_crash_child() -> anyhow::Result<()> {
    let Some(path) = std::env::var_os("D2_SEGMENT_CRASH_DIRECTORY") else {
        return Ok(());
    };
    let path = std::path::PathBuf::from(path);
    let mode = std::env::var("D2_SEGMENT_CRASH_MODE")?;
    let member = mode == "member";
    let mut sm = Arc::new(StateMachine::open(path.join("state.db"))?);
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (_release, gate) = std::sync::mpsc::channel();
    let writer = if mode == "snapshot" {
        let mut source = Arc::new(StateMachine::default());
        setup(&mut source, false).await;
        append(&mut source, 1, 33).await;
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
        let first = sm.applied().await.unwrap().index + 1;
        let commands = if member {
            vec![advance(32, &journal(33)), advance(33, &journal(33))]
        } else {
            vec![
                coordinator(begin(32)),
                coordinator(decide(32)),
                coordinator(begin(33)),
                coordinator(decide(33)),
            ]
        };
        *sm.apply_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
            entered,
            release: gate,
        });
        tokio::spawn(async move {
            sm.apply(
                commands
                    .into_iter()
                    .enumerate()
                    .map(|(n, c)| entry(first + n as u64, c)),
            )
            .await
            .map(|_| ())
        })
    };
    ready.await?;
    std::fs::write(path.join("synced"), b"ready")?;
    writer.await??;
    anyhow::bail!("parent must kill child before publication")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn process_death_preserves_filled_tail_migration_member_and_snapshot() -> anyhow::Result<()> {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for mode in ["coordinator", "member", "migration", "snapshot"] {
        let dir = tempfile::tempdir()?;
        let state_path = dir.path().join("state.db");
        let member = mode == "member";
        {
            let mut sm = Arc::new(StateMachine::open(&state_path)?);
            setup(&mut sm, member).await;
            if member {
                apply(
                    &mut sm,
                    (1..=31).map(|n| advance(n, &journal(33))).collect(),
                )
                .await;
            } else {
                append(&mut sm, 1, 31).await;
            }
            if mode == "migration" {
                downgrade(&sm, false).await;
            }
        }
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "state_machine::decision_segment_tests::decision_segment_crash_child",
                    "--nocapture",
                ])
                .env("D2_SEGMENT_CRASH_DIRECTORY", dir.path())
                .env("D2_SEGMENT_CRASH_MODE", mode)
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
        let recovered = sm
            .load_disk_state(&*sm.state.read().await)
            .unwrap()
            .unwrap();
        let ledger = recovered.ledger.unwrap();
        assert_eq!(ledger.journal, journal(33));
        assert_eq!(ledger.descriptor.event_count, 67);
        assert_eq!(ledger.descriptor.segment_count, 3);
        assert_eq!(ledger.descriptor.applied, sm.applied().await);
        assert_eq!(rows(&sm).len(), 2);
        assert_eq!(manifest(&sm)["through"], 33);
        assert_eq!(
            prefix(&sm, 31, member).await,
            journal(31).terminal_prefix(31).unwrap()
        );
        assert_eq!(
            prefix(&sm, 33, member).await,
            journal(33).terminal_prefix(33).unwrap()
        );
        if member {
            apply(&mut sm, vec![advance(33, &journal(33))]).await;
        } else {
            apply(&mut sm, vec![coordinator(decide(33))]).await;
        }
        assert_eq!(manifest(&sm)["through"], 33);
        if mode == "snapshot" {
            assert!(sm.get_current_snapshot().await?.is_some());
        }
    }
    Ok(())
}

#[tokio::test]
async fn exact_segment_key_row_manifest_and_bound_member_metadata_goldens() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, false).await;
    append(&mut sm, 1, 1).await;
    let row = rows(&sm).remove(0);
    let mode = |n: u8, count: usize| vec![n.to_string(); count].join(",");
    let nines = mode(9, 16);
    let sevens = mode(7, 16);
    let eights = mode(8, 16);
    let fours = mode(4, 32);
    let cohort = format!("{{\"coordinator_shard\":9,\"coordinator_epoch\":1,\"members\":[{{\"shard\":7,\"epoch\":1,\"mode\":[{sevens}]}},{{\"shard\":8,\"epoch\":1,\"mode\":[{eights}]}}]}}");
    let domain = format!("{{\"role\":\"Coordinator\",\"shard\":9,\"epoch\":1,\"mode\":[{nines}],\"authority\":{{\"shard\":9,\"mode\":[{nines}]}},\"cohort\":{cohort}}}");
    let golden = format!("{{\"version\":1,\"domain\":{domain},\"first\":1,\"records\":[{{\"id\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1],\"ordinal\":1,\"participants\":[{{\"shard\":8,\"epoch\":1,\"mode\":[{eights}],\"intent\":[{fours}]}}],\"prepared\":[false],\"decision\":\"Abort\"}}]}}");
    assert_eq!(row.1, golden.as_bytes());
    let key = [
        b"sm/decision/".as_slice(),
        &[0, 0, 0, 1, 0],
        &[0, 0, 0, 0, 0, 0, 0, 9],
        &[0, 0, 0, 0, 0, 0, 0, 1],
        &[9; 16],
        &[0, 0, 0, 0, 0, 0, 0, 9],
        &[9; 16],
        &[0, 0, 0, 0, 0, 0, 0, 1],
        &[0, 0, 0, 0, 0, 0, 0, 1],
    ]
    .concat();
    assert_eq!(row.0, key);
    let bytes = key.len() + golden.len();
    let expected = format!("{{\"version\":1,\"domain\":{domain},\"through\":1,\"segments\":1,\"encoded_bytes\":{bytes},\"applied\":{{\"leader_id\":{{\"term\":1,\"node_id\":1}},\"index\":6}}}}");
    assert_eq!(
        sm.storage
            .as_ref()
            .unwrap()
            .db
            .get(MANIFEST_KEY)
            .unwrap()
            .unwrap(),
        expected.as_bytes()
    );
    let member_dir = tempfile::tempdir().unwrap();
    let mut member = Arc::new(StateMachine::open(member_dir.path()).unwrap());
    setup(&mut member, true).await;
    let state = member.state.read().await;
    let d1 = state.d1.as_ref().unwrap();
    // v10 remains a readable full-state migration input; new bound writes
    // use the journal-less v12 metadata covered by the codec goldens.
    let legacy = serde_json::json!({"version":10,"state":d1.stage()});
    assert_eq!(
        legacy["state"]["visibility"]["journal"],
        serde_json::to_value(&d1.visibility.as_ref().unwrap().journal).unwrap()
    );
    let member_domain = format!("{{\"role\":\"Member\",\"shard\":7,\"epoch\":1,\"mode\":[{sevens}],\"authority\":{{\"shard\":9,\"mode\":[{nines}]}},\"cohort\":{cohort}}}");
    let expected = format!("{{\"version\":1,\"domain\":{member_domain},\"through\":0,\"segments\":0,\"encoded_bytes\":0,\"applied\":{{\"leader_id\":{{\"term\":1,\"node_id\":1}},\"index\":4}}}}");
    assert_eq!(
        member
            .storage
            .as_ref()
            .unwrap()
            .db
            .get(MANIFEST_KEY)
            .unwrap()
            .unwrap(),
        expected.as_bytes()
    );
}

#[tokio::test]
async fn later_snapshot_marker_cannot_erase_existing_segment_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, false).await;
    append(&mut sm, 1, 31).await;
    let old = sm.build_snapshot().await.unwrap();
    append(&mut sm, 32, 33).await;
    let before = rows(&sm);
    let old_prefix = prefix(&sm, 31, false).await;
    let mut meta = old.meta;
    meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(2, 1), 999));
    assert!(sm.install_snapshot(&meta, old.snapshot).await.is_err());
    assert_eq!(rows(&sm), before);
    assert_eq!(prefix(&sm, 31, false).await, old_prefix);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn unchanged_transaction_cap_preserves_last_reserved_decision_and_nonparticipant_catchup() {
    use crate::transaction_decisions::{Error, MAX_TRANSACTIONS};
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, false).await;
    // Full lifetime quota, with the final existing identity still unresolved.
    append(&mut sm, 1, MAX_TRANSACTIONS as u64 - 1).await;
    apply(&mut sm, vec![coordinator(begin(MAX_TRANSACTIONS as u64))]).await;
    let first = sm.applied().await.unwrap().index + 1;
    let results = sm
        .apply([
            entry(first, coordinator(begin(MAX_TRANSACTIONS as u64 + 1))),
            entry(first + 1, coordinator(begin(1))),
            entry(first + 2, coordinator(decide(MAX_TRANSACTIONS as u64))),
        ])
        .await
        .unwrap();
    assert_eq!(
        results[0].d1,
        Some(Err(D1Error::CoordinatorRejected(Error::Capacity)))
    );
    assert!(results[1..].iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
    assert_eq!(manifest(&sm)["through"], 1024);
    assert_eq!(rows(&sm).len(), 32);
    assert_eq!(
        prefix(&sm, 1, false).await,
        journal(1).terminal_prefix(1).unwrap()
    );
    let history = sm
        .state
        .read()
        .await
        .d1
        .as_ref()
        .unwrap()
        .coordinator
        .clone()
        .unwrap();
    let member_dir = tempfile::tempdir().unwrap();
    let mut member = Arc::new(StateMachine::open(member_dir.path()).unwrap());
    setup(&mut member, true).await;
    // One apply batch keeps the test bounded while preserving each state-machine
    // ordinal transition. No new local intent or D1 data history is allocated.
    apply(
        &mut member,
        (1..=MAX_TRANSACTIONS as u64)
            .map(|n| advance(n, &history))
            .collect(),
    )
    .await;
    assert_eq!(manifest(&member)["through"], 1024);
    assert_eq!(
        prefix(&member, 1024, true).await,
        prefix(&sm, 1024, false).await
    );
    assert_eq!(member.d1_metadata().await, Some((7, 1, 0)));
    drop(member);
    let member = StateMachine::open(member_dir.path()).unwrap();
    assert_eq!(prefix(&member, 1024, true).await.version(), 1024);
}

#[tokio::test]
async fn canonical_but_substituted_terminal_facts_fail_online_and_on_reopen() {
    for (from, to) in [
        ("\"decision\":\"Abort\"", "\"decision\":\"Commit\""),
        ("\"prepared\":[false]", "\"prepared\":[true]"),
        ("\"ordinal\":1", "\"ordinal\":2"),
        ("\"role\":\"Coordinator\"", "\"role\":\"Member\""),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, false).await;
        append(&mut sm, 1, 1).await;
        let (key, value) = rows(&sm).remove(0);
        // Keep canonical field order and encoding: rejection must compare the
        // exact authoritative facts, not merely reject reserialized JSON order.
        let changed = String::from_utf8(value).unwrap().replacen(from, to, 1);
        sm.storage.as_ref().unwrap().db.put(key, changed).unwrap();
        assert_eq!(
            sm.coordinator_terminal_prefix(9, 1, [9; 16], 1).await,
            Err(D1Error::DecisionIndexHistory)
        );
        drop(sm);
        assert!(StateMachine::open(dir.path()).is_err());
    }
}

// Share the fixed-cohort fixture and fault gates rather than duplicate a
// second setup/command protocol in the authoritative ledger regression suite.
#[path = "event_ledger_state_tests.rs"]
mod event_ledger_state_tests;
