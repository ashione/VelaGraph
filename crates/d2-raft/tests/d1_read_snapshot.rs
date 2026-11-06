use d2_raft::{
    D1Command, D1ReadLimit, D1WriteRequest, LogCommand, StateMachine, TypeConfig, WriteOp,
};
use openraft::{storage::RaftStateMachine, Entry, EntryPayload, LogId};
use std::sync::Arc;

async fn apply(state: &mut Arc<StateMachine>, index: u64, command: D1Command) {
    state
        .apply([Entry::<TypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LogCommand::D1V1(command)),
        }])
        .await
        .unwrap();
}

#[tokio::test]
async fn bounded_snapshot_pairs_sequence_and_entries_atomically() {
    let mut state = Arc::new(StateMachine::default());
    assert!(state.d1_read_snapshot(1, 1).await.unwrap().is_none());
    apply(&mut state, 1, D1Command::Activate { shard: 7, epoch: 3 }).await;
    let reader = state.clone();
    let writer = tokio::spawn(async move {
        for id in 1u8..=100 {
            apply(
                &mut state,
                u64::from(id) + 1,
                D1Command::Write(D1WriteRequest {
                    shard: 7,
                    epoch: 3,
                    request_id: [id; 16],
                    expected_sequence: Some(u64::from(id) - 1),
                    operations: vec![WriteOp::put(b"x", [id]), WriteOp::put(b"y", [id])],
                }),
            )
            .await;
            tokio::task::yield_now().await;
        }
    });
    for _ in 0..200 {
        let view = reader.d1_read_snapshot(2, 4).await.unwrap().unwrap();
        assert_eq!((view.shard, view.epoch), (7, 3));
        if view.sequence != 0 {
            assert_eq!(view.entries.len(), 2);
            assert_eq!(
                view.entries.get(b"x".as_slice()),
                Some(&vec![view.sequence as u8])
            );
            assert_eq!(
                view.entries.get(b"y".as_slice()),
                Some(&vec![view.sequence as u8])
            );
        }
        tokio::task::yield_now().await;
    }
    writer.await.unwrap();
    assert_eq!(
        reader.d1_read_snapshot(1, 4).await.unwrap_err(),
        D1ReadLimit
    );
    assert_eq!(
        reader.d1_read_snapshot(2, 3).await.unwrap_err(),
        D1ReadLimit
    );
    assert_eq!(
        reader
            .d1_read_snapshot(2, 4)
            .await
            .unwrap()
            .unwrap()
            .sequence,
        100
    );
}
