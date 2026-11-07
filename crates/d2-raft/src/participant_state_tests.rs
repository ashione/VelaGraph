use super::*;
use crate::participant_writes::{Authority, Status};
use crate::transaction_decisions::{Decision, Participant};
use openraft::CommittedLeaderId;
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn descriptor() -> Participant {
    Participant {
        shard: 7,
        epoch: 3,
        mode: [1; 16],
        intent: [4; 32],
    }
}
fn request(id: u8) -> crate::D1WriteRequest {
    crate::D1WriteRequest {
        shard: 7,
        epoch: 3,
        request_id: [id; 16],
        expected_sequence: Some(1),
        operations: vec![WriteOp::put("x", "new"), WriteOp::put("y", "new")],
    }
}
fn prepare(id: u8) -> D1Command {
    D1Command::PrepareParticipant {
        authority: authority(),
        participant: descriptor(),
        request: request(id),
    }
}
fn resolve(id: u8, decision: Decision) -> D1Command {
    D1Command::ResolveParticipant {
        authority: authority(),
        participant: descriptor(),
        id: [id; 16],
        decision,
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
    index: u64,
    command: D1Command,
) -> Result<D1Result, D1Error> {
    sm.apply([entry(index, command)])
        .await
        .unwrap()
        .remove(0)
        .d1
        .unwrap()
}
async fn setup(sm: &mut Arc<StateMachine>) {
    apply(sm, 1, D1Command::Activate { shard: 7, epoch: 3 })
        .await
        .unwrap();
    let mut seed = request(1);
    seed.expected_sequence = Some(0);
    seed.operations = vec![WriteOp::put("x", "old"), WriteOp::put("y", "old")];
    apply(sm, 2, D1Command::Write(seed)).await.unwrap();
    apply(
        sm,
        3,
        D1Command::EnableManagedWrites {
            shard: 7,
            epoch: 3,
            expected_sequence: 1,
            mode_id: [1; 16],
        },
    )
    .await
    .unwrap();
    apply(
        sm,
        4,
        D1Command::EnableParticipant {
            shard: 7,
            epoch: 3,
            mode_id: [1; 16],
            authority: authority(),
        },
    )
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pending_reopen_atomic_commit_and_stable_read_views() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm).await;
    let pinned = sm.d1_read_snapshot(10, 100).await.unwrap().unwrap();
    assert_eq!(
        apply(&mut sm, 5, prepare(2)).await.unwrap(),
        D1Result::Participant(Status::Prepared)
    );
    assert_eq!(sm.get("x").await.as_deref(), Some("old"));
    assert_eq!(
        apply(&mut sm, 6, prepare(3)).await,
        Err(D1Error::ParticipantBusy)
    );
    assert_eq!(
        apply(
            &mut sm,
            7,
            D1Command::ManagedWrite {
                mode_id: [1; 16],
                request: request(4)
            }
        )
        .await,
        Err(D1Error::ParticipantOnly)
    );
    assert_eq!(
        apply(
            &mut sm,
            8,
            D1Command::SetEpoch {
                shard: 7,
                expected_epoch: 3,
                new_epoch: 4
            }
        )
        .await,
        Err(D1Error::ParticipantBusy)
    );
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_eq!(
        sm.participant_status(7, 3, [1; 16], [2; 16]).await.unwrap(),
        Status::Prepared
    );
    sm.storage
        .as_ref()
        .unwrap()
        .fail_next_write
        .store(true, Ordering::Relaxed);
    assert!(sm
        .apply([entry(9, resolve(2, Decision::Commit))])
        .await
        .is_err());
    assert_eq!(sm.get("x").await.as_deref(), Some("old"));
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_eq!(
        apply(&mut sm, 9, resolve(2, Decision::Commit))
            .await
            .unwrap(),
        D1Result::Participant(Status::Committed { sequence: 2 })
    );
    assert_eq!(
        apply(&mut sm, 10, resolve(2, Decision::Commit))
            .await
            .unwrap(),
        D1Result::Participant(Status::Committed { sequence: 2 })
    );
    assert_eq!(sm.get("x").await.as_deref(), Some("new"));
    assert_eq!(sm.get("y").await.as_deref(), Some("new"));
    assert_eq!(pinned.entries.get(b"x".as_slice()).unwrap(), b"old");
    assert_eq!(
        apply(&mut sm, 11, resolve(2, Decision::Abort)).await,
        Err(D1Error::ParticipantDecisionConflict)
    );
    let image = sm.build_snapshot().await.unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut restored = Arc::new(StateMachine::open(target.path()).unwrap());
    restored
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    drop(restored);
    drop(sm);
    let restored = StateMachine::open(target.path()).unwrap();
    assert_eq!(restored.get("y").await.as_deref(), Some("new"));
    assert_eq!(
        restored
            .participant_status(7, 3, [1; 16], [2; 16])
            .await
            .unwrap(),
        Status::Committed { sequence: 2 }
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn abort_tombstone_prevents_late_prepare_and_identity_reuse() {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    assert_eq!(
        apply(&mut sm, 5, resolve(2, Decision::Abort))
            .await
            .unwrap(),
        D1Result::Participant(Status::Aborted)
    );
    assert_eq!(
        apply(&mut sm, 6, prepare(2)).await.unwrap(),
        D1Result::Participant(Status::Aborted)
    );
    assert_eq!(
        apply(&mut sm, 7, resolve(1, Decision::Abort)).await,
        Err(D1Error::IdempotencyConflict)
    );
    assert_eq!(
        apply(&mut sm, 8, resolve(3, Decision::Commit)).await,
        Err(D1Error::ParticipantUnknown)
    );
    assert_eq!(sm.get("x").await.as_deref(), Some("old"));
    apply(&mut sm, 9, prepare(3)).await.unwrap();
    let mut changed = request(3);
    changed.operations[0] = WriteOp::put("x", "different");
    assert_eq!(
        apply(
            &mut sm,
            10,
            D1Command::PrepareParticipant {
                authority: authority(),
                participant: descriptor(),
                request: changed
            }
        )
        .await,
        Err(D1Error::IdempotencyConflict)
    );
    apply(&mut sm, 11, resolve(3, Decision::Abort))
        .await
        .unwrap();
    apply(&mut sm, 12, prepare(4)).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn snapshot_cannot_drop_or_rewrite_pending_intent() {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    let mut old = sm.build_snapshot().await.unwrap();
    apply(&mut sm, 5, prepare(2)).await.unwrap();
    old.meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 10));
    assert!(sm.install_snapshot(&old.meta, old.snapshot).await.is_err());
    let image = sm.build_snapshot().await.unwrap();
    let mut decoded: SnapshotV1 = serde_json::from_slice(image.snapshot.get_ref()).unwrap();
    decoded.d1.participant.as_mut().unwrap().records[0]
        .request
        .as_mut()
        .unwrap()
        .operations[0] = WriteOp::put("x", "forged");
    let mut meta = image.meta;
    meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 11));
    assert!(sm
        .install_snapshot(
            &meta,
            Box::new(Cursor::new(serde_json::to_vec(&decoded).unwrap()))
        )
        .await
        .is_err());
    assert_eq!(
        sm.participant_status(7, 3, [1; 16], [2; 16]).await.unwrap(),
        Status::Prepared
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pending_ownership_change_is_rejected_by_snapshot_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm).await;
    apply(&mut sm, 5, prepare(2)).await.unwrap();
    let image = sm.build_snapshot().await.unwrap();
    let mut decoded: SnapshotV1 = serde_json::from_slice(image.snapshot.get_ref()).unwrap();
    decoded.d1.epoch = 4;
    let mut meta = image.meta;
    meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 20));
    let bytes = serde_json::to_vec(&decoded).unwrap();
    let mut fresh = Arc::new(StateMachine::default());
    assert!(fresh
        .install_snapshot(&meta, Box::new(Cursor::new(bytes.clone())))
        .await
        .is_err());
    assert!(sm
        .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
        .await
        .is_err());
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .put(D1_KEY, d1_metadata_bytes(&decoded.d1).unwrap())
        .unwrap();
    drop(sm);
    assert!(StateMachine::open(dir.path()).is_err());
}

#[test]
fn recovered_prepare_must_have_reserved_a_future_dedup_slot() {
    let mut d1 = D1State::new(7, 3);
    for index in 1..=crate::MAX_DEDUP_REQUESTS as u64 {
        let mut id = [0; 16];
        id[..8].copy_from_slice(&index.to_le_bytes());
        d1.write_with_history(
            &crate::D1WriteRequest {
                shard: 7,
                epoch: 3,
                request_id: id,
                expected_sequence: Some(index - 1),
                operations: vec![WriteOp::put("key", "value")],
            },
            None,
        )
        .unwrap();
    }
    d1.enable_managed(7, 3, d1.sequence, [1; 16]).unwrap();
    d1.version = 5;
    let mut id = [0; 16];
    id[..8].copy_from_slice(&4097u64.to_le_bytes());
    let mut req = request(2);
    req.request_id = id;
    req.expected_sequence = Some(d1.sequence);
    d1.participant = Some(crate::participant_writes::State {
        authority: authority(),
        activated_sequence: d1.sequence,
        records: Vec::new(),
    });
    assert_eq!(
        d1.prepare_participant(&authority(), descriptor(), req.clone(), None),
        Err(D1Error::DedupCapacity)
    );
    d1.participant
        .as_mut()
        .unwrap()
        .records
        .push(crate::participant_writes::Intent {
            id,
            participant: descriptor(),
            request: Some(req),
            decision: None,
            sequence: None,
        });
    assert!(d1.validate().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn participant_metadata_v6_golden_and_old_shape_rejection() {
    let mut sm = Arc::new(StateMachine::default());
    setup(&mut sm).await;
    let state = sm.state.read().await;
    let bytes = d1_metadata_bytes(state.d1.as_ref().unwrap()).unwrap();
    let ones = vec!["1"; 16].join(",");
    let nines = vec!["9"; 16].join(",");
    let expected=format!("{{\"version\":6,\"state\":{{\"version\":5,\"managed\":{{\"mode_id\":[{ones}],\"activated_sequence\":1}},\"participant\":{{\"authority\":{{\"shard\":9,\"mode\":[{nines}]}},\"activated_sequence\":1,\"records\":[]}},\"shard\":7,\"epoch\":3,\"sequence\":1,\"dedup\":[]}}}}");
    assert_eq!(bytes, expected.as_bytes());
    assert!(serde_json::from_slice::<D1MetadataV4>(&bytes).is_err());
}
