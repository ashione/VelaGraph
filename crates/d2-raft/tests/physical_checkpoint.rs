use d2_raft::{
    restore_physical_checkpoint, Cluster, D1Command, D1Result, D1WriteRequest, D1WriteStatus,
    LogCommand, StateMachine, WriteOp,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use std::collections::BTreeMap;

fn request(id: u8) -> LogCommand {
    LogCommand::D1V1(D1Command::Write(D1WriteRequest {
        shard: 7,
        epoch: 3,
        request_id: [id; 16],
        expected_sequence: None,
        operations: vec![WriteOp::put([id], [id])],
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_concurrent_writes_restore_membership_and_dedup() -> anyhow::Result<()> {
    let dirs = tempfile::tempdir()?;
    let cluster = Cluster::new();
    let node = cluster.start_durable(1, dirs.path().join("source")).await?;
    node.raft
        .initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
        .await?;
    cluster
        .wait_for_leader(std::slice::from_ref(&node), 10_000)
        .await?;
    node.raft
        .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
        .await?;
    node.raft.client_write(request(1)).await?;
    let mut builder = node.state_machine.clone().get_snapshot_builder().await;
    builder.build_snapshot().await?;
    let backup = dirs.path().join("backup");
    let raft = node.raft.clone();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let writer_barrier = barrier.clone();
    let writer = tokio::spawn(async move {
        writer_barrier.wait().await;
        for id in 2..=32 {
            raft.client_write(request(id)).await?;
        }
        anyhow::Ok(())
    });
    barrier.wait().await;
    node.physical_checkpoint(&backup).await?;
    writer.await??;
    node.raft.client_write(request(99)).await?;
    assert!(
        node.physical_checkpoint(&backup).await.is_err(),
        "no overwrite on retry"
    );
    let restored = dirs.path().join("restored");
    assert!(restore_physical_checkpoint(&backup, &restored, 0, 2).is_err());
    assert!(!restored.exists());
    restore_physical_checkpoint(&backup, &restored, 0, 1)?;
    restore_physical_checkpoint(&backup, dirs.path().join("second"), 0, 1)?;
    assert!(restore_physical_checkpoint(&backup, &restored, 0, 1).is_err());
    {
        let state = StateMachine::open(restored.join("state.db"))?;
        assert_eq!(state.get_raw(&[1]).await, Some(vec![1]));
        assert_eq!(
            state.get_raw(&[99]).await,
            None,
            "post-cut writes are NOT remotely durable"
        );
    }
    cluster.shutdown(&node).await;
    drop(builder);
    drop(node);
    assert!(cluster.start_durable(1, &restored).await.is_err());
    // Test-only: the original single-node in-process cluster has been shut down
    // and no other replicas exist. Clear the guard ONLY to test physical format
    // and dedup in a wholly isolated new registry, never to test live rejoin.
    std::fs::remove_file(restored.join("RESTORED_OFFLINE_ONLY"))?;
    let cluster = Cluster::new();
    let node = cluster.start_durable(1, &restored).await?;
    cluster
        .wait_for_leader(std::slice::from_ref(&node), 10_000)
        .await?;
    let reply = node.raft.client_write(request(1)).await?;
    assert!(matches!(
        reply.data.d1,
        Some(Ok(D1Result::Write {
            status: D1WriteStatus::Duplicate,
            ..
        }))
    ));
    node.raft.client_write(request(100)).await?;
    assert_eq!(node.state_machine.get_raw(&[100]).await, Some(vec![100]));
    cluster.shutdown(&node).await;
    drop(node);
    // Corruption is rejected before creating destination; unrelated data untouched.
    let manifest = std::fs::read(backup.join("checkpoint.json"))?;
    let json: serde_json::Value = serde_json::from_slice(&manifest)?;
    let name = json["files"][0]["name"].as_str().unwrap();
    std::fs::write(backup.join(name), b"corrupt")?;
    let bad = dirs.path().join("bad");
    assert!(restore_physical_checkpoint(&backup, &bad, 0, 1).is_err());
    assert!(!bad.exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_backup_and_restore_fail_closed() -> anyhow::Result<()> {
    let dirs = tempfile::tempdir()?;
    let backup = dirs.path().join("backup");
    std::fs::create_dir(&backup)?;
    let target = dirs.path().join("target");
    assert!(restore_physical_checkpoint(&backup, &target, 0, 1).is_err());
    assert!(!target.exists());
    std::fs::write(backup.join("checkpoint.json"), b"{\"format\":1")?;
    assert!(restore_physical_checkpoint(&backup, &target, 0, 1).is_err());
    std::fs::create_dir(&target)?;
    std::fs::write(target.join("RESTORE_INCOMPLETE"), b"1")?;
    assert!(Cluster::new().start_durable(1, &target).await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_start_rejects_restored_identity_mismatch() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    for (group, id) in [(0, 2), (7, 1)] {
        std::fs::write(
            dir.path().join("raft-group.json"),
            serde_json::to_vec(&serde_json::json!({"version":1,"group_id":group,"node_id":id}))?,
        )?;
        let result = d2_raft::TcpNode::start(d2_raft::TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: dir.path().to_path_buf(),
        })
        .await;
        assert!(result.is_err());
        assert!(!dir.path().join("raft.db").exists());
    }
    Ok(())
}
