//! Real two-DB checkpoint bytes through the production object-store adapter.
//! Memory backend is ordinary CI; the ignored test needs disposable SeaweedFS.
//! Neither test permits restored old votes to rejoin a live cluster.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use checkpoint_object_store::{CheckpointStore, CreateCapability, Limits};
use d2_raft::{
    restore_physical_checkpoint, Cluster, D1Command, D1Result, D1WriteRequest, D1WriteStatus,
    LogCommand, StateMachine, WriteOp,
};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path as ObjectPath,
    ObjectStore, PutMode,
};
use openraft::{
    storage::{RaftSnapshotBuilder, RaftStateMachine},
    Entry, EntryPayload,
};

fn request(id: u8) -> LogCommand {
    LogCommand::D1V1(D1Command::Write(D1WriteRequest {
        shard: 7,
        epoch: 3,
        request_id: [id; 16],
        expected_sequence: None,
        operations: vec![WriteOp::put([id], vec![id; 4097])],
    }))
}

async fn scenario(store: Arc<dyn ObjectStore>) -> anyhow::Result<()> {
    let key = ObjectPath::from("rocksdb-e2e/atomic-create");
    let (a, b) = tokio::join!(
        store.put_opts(&key, "a".into(), PutMode::Create.into()),
        store.put_opts(&key, "b".into(), PutMode::Create.into()),
    );
    anyhow::ensure!(a.is_ok() != b.is_ok(), "endpoint lacks atomic create");
    anyhow::ensure!(
        matches!(
            a.err().or(b.err()),
            Some(
                object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. }
            )
        ),
        "conflicting create must be an explicit precondition failure"
    );
    let transport = CheckpointStore::new(
        store,
        "rocksdb-e2e/backups".into(),
        Limits {
            chunk_bytes: 1024,
            timeout: Duration::from_secs(120),
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )?;
    let dirs = tempfile::tempdir()?;
    let source = dirs.path().join("source");
    let backup = dirs.path().join("backup");
    let cluster = Cluster::new();
    let node = cluster.start_durable(1, &source).await?;
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
    node.raft.client_write(request(2)).await?;
    let mut builder = node.state_machine.clone().get_snapshot_builder().await;
    builder.build_snapshot().await?;
    node.physical_checkpoint(&backup).await?;
    let expected_manifest = std::fs::read(backup.join("checkpoint.json"))?;
    let manifest: serde_json::Value = serde_json::from_slice(&expected_manifest)?;
    node.raft.client_write(request(3)).await?;
    let receipt = transport.upload(&backup, "actual-rocksdb").await?;
    let retried = transport.upload(&backup, "actual-rocksdb").await?;
    assert_eq!(receipt.manifest_sha256, retried.manifest_sha256);
    cluster.shutdown(&node).await;
    drop(builder);
    drop(node);
    drop(cluster);
    // Only test-owned temporary directories: prove no local SST/WAL fallback.
    std::fs::remove_dir_all(&source)?;
    std::fs::remove_dir_all(&backup)?;
    let downloaded = dirs.path().join("downloaded");
    transport.download(&receipt, &downloaded).await?;
    assert_eq!(
        std::fs::read(downloaded.join("checkpoint.json"))?,
        expected_manifest
    );
    let restored = dirs.path().join("restored");
    assert!(restore_physical_checkpoint(&downloaded, &restored, 0, 2).is_err());
    assert!(!restored.exists());
    restore_physical_checkpoint(&downloaded, &restored, 0, 1)?;
    assert!(restored.join("RESTORED_OFFLINE_ONLY").is_file());
    let error = match Cluster::new().start_durable(1, &restored).await {
        Err(error) => error,
        Ok(_) => panic!("old votes must never silently rejoin consensus"),
    };
    assert!(error.to_string().contains("offline-only"));
    let mut state = Arc::new(StateMachine::open(restored.join("state.db"))?);
    assert_eq!(state.d1_metadata().await, Some((7, 3, 2)));
    assert_eq!(state.get_raw(&[1]).await, Some(vec![1; 4097]));
    assert_eq!(state.get_raw(&[2]).await, Some(vec![2; 4097]));
    assert_eq!(
        state.get_raw(&[3]).await,
        None,
        "post-cut ACK is not remote durable"
    );
    let (applied, membership) = state.applied_state().await?;
    assert_eq!(serde_json::to_value(applied)?, manifest["applied"]);
    assert_eq!(serde_json::to_value(membership)?, manifest["membership"]);
    let snapshot = state
        .get_current_snapshot()
        .await?
        .expect("persisted snapshot");
    assert_eq!(serde_json::to_value(snapshot.meta)?, manifest["snapshot"]);
    // Test retry state offline; never remove the guard or start old-vote Raft.
    let applied = applied.expect("activated state");
    let replies = state
        .apply([Entry {
            log_id: openraft::LogId::new(applied.leader_id, applied.index + 1),
            payload: EntryPayload::Normal(request(1)),
        }])
        .await?;
    assert!(matches!(
        replies[0].d1,
        Some(Ok(D1Result::Write {
            status: D1WriteStatus::Duplicate,
            ..
        }))
    ));
    assert_eq!(state.d1_metadata().await, Some((7, 3, 2)));
    assert!(restored.join("RESTORED_OFFLINE_ONLY").exists());
    println!("real RocksDB -> object transport -> offline restore: metadata, snapshot, dedup and guard verified; no remote ACK or live rejoin claim");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_rocksdb_checkpoint_through_memory_object_store() -> anyhow::Result<()> {
    scenario(Arc::new(object_store::memory::InMemory::new())).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable loopback SeaweedFS service; not a cloud-provider certification"]
async fn actual_rocksdb_checkpoint_through_s3() -> anyhow::Result<()> {
    let endpoint = std::env::var("VELAGRAPH_TEST_S3_ENDPOINT")?;
    let port = endpoint
        .strip_prefix("http://127.0.0.1:")
        .ok_or_else(|| anyhow::anyhow!("test only permits loopback HTTP"))?
        .parse::<u16>()?;
    anyhow::ensure!(port != 0, "test endpoint needs a bound port");
    let bucket = std::env::var("VELAGRAPH_TEST_S3_BUCKET")?;
    anyhow::ensure!(
        bucket == "velagraph-ci",
        "only disposable CI bucket is allowed"
    );
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_access_key_id("velagraph-ci-only")
        .with_secret_access_key("velagraph-ci-only-secret")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()?;
    scenario(Arc::new(store)).await
}
