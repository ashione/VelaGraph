use super::*;
use crate::participant_writes::Authority;
use crate::transaction_decisions::{Command, Decision, Journal, Participant};
use crate::versioned_reads::{ReadLimit, ReadSnapshot};
use openraft::CommittedLeaderId;
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn descriptor(shard: u64) -> Participant {
    Participant {
        shard,
        epoch: 1,
        mode: [shard as u8; 16],
        intent: [shard as u8; 32],
    }
}
fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(LogCommand::D1V1(command)),
    }
}
async fn apply(
    sm: &mut Arc<StateMachine>,
    index: &mut u64,
    command: D1Command,
) -> Result<D1Result, D1Error> {
    *index += 1;
    sm.apply([entry(*index, command)])
        .await
        .unwrap()
        .remove(0)
        .d1
        .unwrap()
}
async fn setup(sm: &mut Arc<StateMachine>, shard: u64) -> u64 {
    let mut index = 0;
    apply(sm, &mut index, D1Command::Activate { shard, epoch: 1 })
        .await
        .unwrap();
    apply(
        sm,
        &mut index,
        D1Command::Write(crate::D1WriteRequest {
            shard,
            epoch: 1,
            request_id: [1; 16],
            expected_sequence: Some(0),
            operations: vec![WriteOp::put("x", "old"), WriteOp::put("y", "old")],
        }),
    )
    .await
    .unwrap();
    apply(
        sm,
        &mut index,
        D1Command::EnableManagedWrites {
            shard,
            epoch: 1,
            expected_sequence: 1,
            mode_id: [shard as u8; 16],
        },
    )
    .await
    .unwrap();
    apply(
        sm,
        &mut index,
        D1Command::EnableParticipant {
            shard,
            epoch: 1,
            mode_id: [shard as u8; 16],
            authority: authority(),
        },
    )
    .await
    .unwrap();
    apply(
        sm,
        &mut index,
        D1Command::EnableVersionedReads {
            shard,
            epoch: 1,
            mode_id: [shard as u8; 16],
            journal: Journal::new([9; 16]).unwrap(),
        },
    )
    .await
    .unwrap();
    index
}
fn prepare(shard: u64, id: u8, sequence: u64, operations: Vec<WriteOp>) -> D1Command {
    D1Command::PrepareParticipant {
        authority: authority(),
        participant: descriptor(shard),
        request: crate::D1WriteRequest {
            shard,
            epoch: 1,
            request_id: [id; 16],
            expected_sequence: Some(sequence),
            operations,
        },
    }
}
fn advance(shard: u64, ordinal: u64, journal: &Journal) -> D1Command {
    D1Command::AdvanceVisibility {
        authority: authority(),
        shard,
        epoch: 1,
        mode_id: [shard as u8; 16],
        ordinal,
        journal: journal.clone(),
    }
}
async fn read(sm: &StateMachine, shard: u64, version: u64) -> Result<ReadSnapshot, D1Error> {
    sm.participant_read_at(
        shard,
        1,
        [shard as u8; 16],
        version,
        ReadLimit {
            rows: 20,
            bytes: 1024,
            operations: 1000,
        },
    )
    .await
}
fn begin(j: &mut Journal, id: u8, shards: &[u64]) {
    j.apply(Command::Begin {
        id: [id; 16],
        participants: shards.iter().copied().map(descriptor).collect(),
    })
    .unwrap();
}
fn commit(j: &mut Journal, id: u8, shards: &[u64]) {
    for shard in shards {
        j.apply(Command::Prepared {
            id: [id; 16],
            participant: descriptor(*shard),
        })
        .unwrap();
    }
    j.apply(Command::Decide {
        id: [id; 16],
        decision: Decision::Commit,
    })
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn partial_apply_cannot_serve_new_version_and_old_versions_survive_more_writes() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let mut a = Arc::new(StateMachine::open(a_dir.path()).unwrap());
    let mut b = Arc::new(StateMachine::open(b_dir.path()).unwrap());
    let mut ai = setup(&mut a, 7).await;
    let mut bi = setup(&mut b, 8).await;
    let mut j = Journal::new([9; 16]).unwrap();
    begin(&mut j, 2, &[7, 8]);
    for (sm, index, shard) in [(&mut a, &mut ai, 7), (&mut b, &mut bi, 8)] {
        apply(
            sm,
            index,
            prepare(
                shard,
                2,
                1,
                vec![WriteOp::put("x", "new"), WriteOp::put("y", "new")],
            ),
        )
        .await
        .unwrap();
    }
    commit(&mut j, 2, &[7, 8]);
    apply(&mut a, &mut ai, advance(7, 1, &j)).await.unwrap();
    assert_eq!(read(&b, 8, 1).await, Err(D1Error::VisibilityNotApplied));
    assert_eq!(
        read(&a, 7, 0)
            .await
            .unwrap()
            .entries
            .get(b"x".as_slice())
            .unwrap(),
        b"old"
    );
    assert_eq!(
        read(&b, 8, 0)
            .await
            .unwrap()
            .entries
            .get(b"x".as_slice())
            .unwrap(),
        b"old"
    );
    apply(&mut b, &mut bi, advance(8, 1, &j)).await.unwrap();
    let pinned = read(&a, 7, 1).await.unwrap();
    assert_eq!(pinned.entries, read(&b, 8, 1).await.unwrap().entries);
    begin(&mut j, 3, &[7, 8]);
    for (sm, index, shard) in [(&mut a, &mut ai, 7), (&mut b, &mut bi, 8)] {
        apply(
            sm,
            index,
            prepare(
                shard,
                3,
                2,
                vec![
                    WriteOp::put("x", "temporary"),
                    WriteOp::delete("x"),
                    WriteOp::put("x", "second"),
                    WriteOp::delete("y"),
                ],
            ),
        )
        .await
        .unwrap();
    }
    commit(&mut j, 3, &[7, 8]);
    a.storage
        .as_ref()
        .unwrap()
        .fail_next_write
        .store(true, Ordering::Relaxed);
    assert!(a.apply([entry(ai + 1, advance(7, 2, &j))]).await.is_err());
    assert_eq!(read(&a, 7, 2).await, Err(D1Error::VisibilityNotApplied));
    drop(a);
    a = Arc::new(StateMachine::open(a_dir.path()).unwrap());
    assert_eq!(read(&a, 7, 2).await, Err(D1Error::VisibilityNotApplied));
    for (sm, index, shard) in [(&mut a, &mut ai, 7), (&mut b, &mut bi, 8)] {
        apply(sm, index, advance(shard, 2, &j)).await.unwrap();
    }
    assert_eq!(read(&a, 7, 1).await.unwrap().entries, pinned.entries);
    assert_eq!(
        read(&a, 7, 2)
            .await
            .unwrap()
            .entries
            .get(b"x".as_slice())
            .unwrap(),
        b"second"
    );
    assert!(!read(&a, 7, 2)
        .await
        .unwrap()
        .entries
        .contains_key(b"y".as_slice()));
    let image = a.build_snapshot().await.unwrap();
    let mut restored = Arc::new(StateMachine::default());
    restored
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    assert_eq!(read(&restored, 7, 1).await.unwrap().entries, pinned.entries);
    drop(a);
    drop(b);
    let a = StateMachine::open(a_dir.path()).unwrap();
    let b = StateMachine::open(b_dir.path()).unwrap();
    for version in 0..=2 {
        assert_eq!(
            read(&a, 7, version).await.unwrap().entries,
            read(&b, 8, version).await.unwrap().entries
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn unresolved_holes_nonparticipant_decisions_and_legacy_resolve_are_fenced() {
    let mut sm = Arc::new(StateMachine::default());
    let mut index = setup(&mut sm, 7).await;
    let mut j = Journal::new([9; 16]).unwrap();
    begin(&mut j, 2, &[8]);
    begin(&mut j, 3, &[7]);
    apply(
        &mut sm,
        &mut index,
        prepare(7, 3, 1, vec![WriteOp::put("x", "new")]),
    )
    .await
    .unwrap();
    commit(&mut j, 3, &[7]);
    assert_eq!(
        apply(&mut sm, &mut index, advance(7, 2, &j)).await,
        Err(D1Error::VisibilityUnresolved)
    );
    assert_eq!(
        apply(
            &mut sm,
            &mut index,
            D1Command::ResolveParticipant {
                authority: authority(),
                participant: descriptor(7),
                id: [3; 16],
                decision: Decision::Commit
            }
        )
        .await,
        Err(D1Error::VisibilityRequired)
    );
    j.apply(Command::Decide {
        id: [2; 16],
        decision: Decision::Abort,
    })
    .unwrap();
    assert_eq!(
        apply(&mut sm, &mut index, advance(7, 2, &j)).await,
        Err(D1Error::VisibilityGap)
    );
    apply(&mut sm, &mut index, advance(7, 1, &j)).await.unwrap();
    assert_eq!(
        read(&sm, 7, 1)
            .await
            .unwrap()
            .entries
            .get(b"x".as_slice())
            .unwrap(),
        b"old"
    );
    apply(&mut sm, &mut index, advance(7, 2, &j)).await.unwrap();
    apply(&mut sm, &mut index, advance(7, 2, &j)).await.unwrap();
    assert_eq!(read(&sm, 7, 2).await.unwrap().local_sequence, 2);
    assert_eq!(
        apply(
            &mut sm,
            &mut index,
            D1Command::SetEpoch {
                shard: 7,
                expected_epoch: 1,
                new_epoch: 2
            }
        )
        .await,
        Err(D1Error::VisibilityRequired)
    );
    assert_eq!(
        sm.participant_read_at(
            7,
            1,
            [7; 16],
            2,
            ReadLimit {
                rows: 20,
                bytes: 1024,
                operations: 1
            }
        )
        .await,
        Err(D1Error::VisibilityReadLimit)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn snapshot_cannot_raise_watermark_past_apply_or_regress_nonparticipant_prefix() {
    let mut sm = Arc::new(StateMachine::default());
    let mut index = setup(&mut sm, 7).await;
    let mut old = sm.build_snapshot().await.unwrap();
    let mut j = Journal::new([9; 16]).unwrap();
    begin(&mut j, 2, &[8]);
    j.apply(Command::Decide {
        id: [2; 16],
        decision: Decision::Abort,
    })
    .unwrap();
    apply(&mut sm, &mut index, advance(7, 1, &j)).await.unwrap();
    old.meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 100));
    assert!(sm.install_snapshot(&old.meta, old.snapshot).await.is_err());
    let mut image = sm.build_snapshot().await.unwrap();
    let mut decoded: SnapshotV1 = serde_json::from_slice(image.snapshot.get_ref()).unwrap();
    decoded.d1.visibility.as_mut().unwrap().applied = 2;
    image.meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 101));
    let mut fresh = Arc::new(StateMachine::default());
    assert!(fresh
        .install_snapshot(
            &image.meta,
            Box::new(Cursor::new(serde_json::to_vec(&decoded).unwrap()))
        )
        .await
        .is_err());
    assert_eq!(read(&sm, 7, 1).await.unwrap().local_sequence, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn full_intent_budget_cannot_strand_abort_prefix_or_resurrect_late_prepare() {
    let mut sm = Arc::new(StateMachine::default());
    let mut index = setup(&mut sm, 7).await;
    let mut j = Journal::new([9; 16]).unwrap();
    begin(&mut j, 2, &[7]);
    // Exact current accounted intent cap: Intent 256 + request 128 + op 64
    // plus three key bytes and the payload. No test-only capacity bypass.
    let value = vec![0; crate::participant_writes::MAX_INTENT_BYTES - 256 - 128 - 64 - 3];
    apply(
        &mut sm,
        &mut index,
        prepare(7, 2, 1, vec![WriteOp::put("big", value)]),
    )
    .await
    .unwrap();
    commit(&mut j, 2, &[7]);
    apply(&mut sm, &mut index, advance(7, 1, &j)).await.unwrap();
    begin(&mut j, 3, &[7]);
    let small = vec![WriteOp::put("x", "later")];
    assert_eq!(
        apply(&mut sm, &mut index, prepare(7, 3, 2, small.clone())).await,
        Err(D1Error::ParticipantCapacity)
    );
    j.apply(Command::Decide {
        id: [3; 16],
        decision: Decision::Abort,
    })
    .unwrap();
    apply(&mut sm, &mut index, advance(7, 2, &j)).await.unwrap();
    assert_eq!(
        sm.participant_status(7, 1, [7; 16], [3; 16]).await.unwrap(),
        crate::participant_writes::Status::Aborted
    );
    assert_eq!(
        apply(&mut sm, &mut index, prepare(7, 3, 2, small.clone()))
            .await
            .unwrap(),
        D1Result::Participant(crate::participant_writes::Status::Aborted)
    );
    let bytes = serde_json::to_vec(sm.state.read().await.d1.as_ref().unwrap()).unwrap();
    let mut recovered: D1State = serde_json::from_slice(&bytes).unwrap();
    recovered.validate().unwrap();
    assert_eq!(recovered.participant.as_ref().unwrap().records.len(), 1);
    let req = crate::D1WriteRequest {
        shard: 7,
        epoch: 1,
        request_id: [3; 16],
        expected_sequence: Some(2),
        operations: small,
    };
    assert_eq!(
        recovered
            .prepare_participant(&authority(), descriptor(7), req, None)
            .unwrap(),
        D1Result::Participant(crate::participant_writes::Status::Aborted)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn visibility_metadata_v7_golden() {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm, 7).await;
    let state = sm.state.read().await;
    let bytes = d1_metadata_bytes(state.d1.as_ref().unwrap()).unwrap();
    let sevens = vec!["7"; 16].join(",");
    let nines = vec!["9"; 16].join(",");
    let expected=format!("{{\"version\":7,\"state\":{{\"version\":6,\"managed\":{{\"mode_id\":[{sevens}],\"activated_sequence\":1}},\"participant\":{{\"authority\":{{\"shard\":9,\"mode\":[{nines}]}},\"activated_sequence\":1,\"records\":[]}},\"visibility\":{{\"baseline_sequence\":1,\"epoch\":1,\"applied\":0,\"journal\":{{\"version\":1,\"authority\":[{nines}],\"events\":[]}},\"commits\":[]}},\"shard\":7,\"epoch\":1,\"sequence\":1,\"dedup\":[]}}}}");
    assert_eq!(bytes, expected.as_bytes());
    assert!(serde_json::from_slice::<D1MetadataV4>(&bytes).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn observed_future_decisions_cannot_acquire_conflicting_intents() {
    let mut sm = Arc::new(StateMachine::default());
    let mut index = setup(&mut sm, 7).await;
    let mut j = Journal::new([9; 16]).unwrap();
    begin(&mut j, 2, &[8]);
    begin(&mut j, 3, &[8]);
    begin(&mut j, 4, &[7]);
    for id in [2, 4] {
        j.apply(Command::Decide {
            id: [id; 16],
            decision: Decision::Abort,
        })
        .unwrap();
    }
    apply(&mut sm, &mut index, advance(7, 1, &j)).await.unwrap();
    assert_eq!(
        apply(
            &mut sm,
            &mut index,
            prepare(7, 3, 1, vec![WriteOp::put("x", "bad")])
        )
        .await,
        Err(D1Error::IdempotencyConflict)
    );
    assert_eq!(
        apply(
            &mut sm,
            &mut index,
            prepare(7, 4, 1, vec![WriteOp::put("x", "late")])
        )
        .await,
        Err(D1Error::VisibilityUnresolved)
    );
    j.apply(Command::Decide {
        id: [3; 16],
        decision: Decision::Abort,
    })
    .unwrap();
    apply(&mut sm, &mut index, advance(7, 2, &j)).await.unwrap();
    apply(&mut sm, &mut index, advance(7, 3, &j)).await.unwrap();
    assert_eq!(read(&sm, 7, 3).await.unwrap().local_sequence, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn unknown_abort_cannot_cross_participant_ownership_identity() {
    for wrong_epoch in [false, true] {
        let mut sm = Arc::new(StateMachine::default());
        let mut index = setup(&mut sm, 7).await;
        let mut j = Journal::new([9; 16]).unwrap();
        let mut p = descriptor(7);
        if wrong_epoch {
            p.epoch = 2;
        } else {
            p.mode = [6; 16];
        }
        j.apply(Command::Begin {
            id: [2; 16],
            participants: vec![p],
        })
        .unwrap();
        j.apply(Command::Decide {
            id: [2; 16],
            decision: Decision::Abort,
        })
        .unwrap();
        assert!(apply(&mut sm, &mut index, advance(7, 1, &j)).await.is_err());
        assert_eq!(read(&sm, 7, 1).await, Err(D1Error::VisibilityNotApplied));
        let image = sm.build_snapshot().await.unwrap();
        let mut decoded: SnapshotV1 = serde_json::from_slice(image.snapshot.get_ref()).unwrap();
        let visibility = decoded.d1.visibility.as_mut().unwrap();
        visibility.journal = j;
        visibility.applied = 1;
        let mut fresh = Arc::new(StateMachine::default());
        assert!(fresh
            .install_snapshot(
                &image.meta,
                Box::new(Cursor::new(serde_json::to_vec(&decoded).unwrap()))
            )
            .await
            .is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn future_observation_cannot_persist_an_incompatible_existing_intent() {
    let mut sm = Arc::new(StateMachine::default());
    let mut index = setup(&mut sm, 7).await;
    apply(
        &mut sm,
        &mut index,
        prepare(7, 3, 1, vec![WriteOp::put("x", "pending")]),
    )
    .await
    .unwrap();
    let mut j = Journal::new([9; 16]).unwrap();
    begin(&mut j, 2, &[8]);
    begin(&mut j, 3, &[8]);
    j.apply(Command::Decide {
        id: [2; 16],
        decision: Decision::Abort,
    })
    .unwrap();
    assert_eq!(
        apply(&mut sm, &mut index, advance(7, 1, &j)).await,
        Err(D1Error::VisibilityHistory)
    );
    assert_eq!(read(&sm, 7, 1).await, Err(D1Error::VisibilityNotApplied));
    let image = sm.build_snapshot().await.unwrap();
    let mut fresh = Arc::new(StateMachine::default());
    fresh
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    assert_eq!(read(&fresh, 7, 0).await.unwrap().local_sequence, 1);
}
