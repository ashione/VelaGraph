//! D1-shaped opaque key/value batches through the Raft state machine.

use std::sync::Arc;

use d2_raft::{LogCommand, StateMachine, WriteOp};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

fn entry(index: u64, operations: Vec<WriteOp>) -> Entry<d2_raft::TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(LogCommand::txn(operations)),
    }
}

#[tokio::test]
async fn opaque_batches_apply_atomically_and_survive_snapshot_transfer() -> anyhow::Result<()> {
    let source_dir = tempfile::tempdir()?;
    let mut source = Arc::new(StateMachine::open(source_dir.path().join("state.db"))?);

    let key_one = b"graph/v1/p/0/vertex/\x00\x01".to_vec();
    let key_two = b"graph/v1/p/0/vertex/\xff".to_vec();
    let key_three = b"graph/v1/p/0/edge/\x00\x02".to_vec();

    let replies = source
        .apply([entry(
            1,
            vec![
                WriteOp::put(&key_one, b"vertex-one"),
                WriteOp::put(&key_two, b"vertex-two"),
                WriteOp::delete(&key_two),
                WriteOp::put(&key_three, b"edge-three"),
            ],
        )])
        .await?;
    assert_eq!(replies.len(), 1);
    assert_eq!(
        source.get_raw(&key_one).await.as_deref(),
        Some(&b"vertex-one"[..])
    );
    assert_eq!(source.get_raw(&key_two).await, None);
    assert_eq!(
        source.get_raw(&key_three).await.as_deref(),
        Some(&b"edge-three"[..])
    );
    assert_eq!(source.applied().await.map(|log| log.index), Some(1));

    source
        .apply([entry(
            2,
            vec![
                WriteOp::delete(&key_one),
                WriteOp::put(&key_two, b"vertex-two-replacement"),
            ],
        )])
        .await?;
    assert_eq!(source.get_raw(&key_one).await, None);
    assert_eq!(
        source.get_raw(&key_two).await.as_deref(),
        Some(&b"vertex-two-replacement"[..])
    );

    let mut builder = source.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await?;
    let snapshot_meta = snapshot.meta;
    let snapshot_data = snapshot.snapshot;
    let destination_dir = tempfile::tempdir()?;
    let mut destination = Arc::new(StateMachine::open(destination_dir.path().join("state.db"))?);
    destination
        .install_snapshot(&snapshot_meta, snapshot_data)
        .await?;
    assert_eq!(
        destination.scan_raw().await,
        source.scan_raw().await,
        "snapshot transfer must preserve opaque D1 keys and values"
    );
    assert_eq!(destination.applied().await.map(|log| log.index), Some(2));

    drop(destination);
    drop(destination_dir);
    drop(builder);
    drop(source);
    let reopened = StateMachine::open(source_dir.path().join("state.db"))?;
    assert_eq!(
        reopened.get_raw(&key_two).await.as_deref(),
        Some(&b"vertex-two-replacement"[..])
    );
    assert_eq!(
        reopened.get_raw(&key_three).await.as_deref(),
        Some(&b"edge-three"[..])
    );
    Ok(())
}
