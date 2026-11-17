//! Explicit multi-file transport coverage; blob options apply only to this fixture.
use super::*;
use checkpoint_object_store::{CheckpointStore, CreateCapability, Limits};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path as ObjectPath,
    ObjectStore, PutMode,
};

const ROWS: u32 = 2048;
const VALUE_BYTES: usize = 16 * 1024;
const BATCH: u32 = 64;

fn value(id: u32) -> Vec<u8> {
    (0..VALUE_BYTES)
        .map(|i| ((id as usize * 37 + i * 17 + i / 251) % 251) as u8)
        .collect()
}

async fn roundtrip(blob: bool) -> anyhow::Result<()> {
    let endpoint = std::env::var("VELAGRAPH_TEST_S3_ENDPOINT")?;
    let port = endpoint
        .strip_prefix("http://127.0.0.1:")
        .ok_or_else(|| anyhow::anyhow!("loopback only"))?
        .parse::<u16>()?;
    anyhow::ensure!(port != 0, "bound port required");
    anyhow::ensure!(
        std::env::var("VELAGRAPH_TEST_S3_BUCKET")? == "velagraph-ci",
        "disposable bucket only"
    );
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name("velagraph-ci")
            .with_region("us-east-1")
            .with_access_key_id("velagraph-ci-only")
            .with_secret_access_key("velagraph-ci-only-secret")
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?,
    );
    let prefix = if blob { "scale-blob" } else { "scale-sst" };
    let key = ObjectPath::from(format!("{prefix}/atomic-create"));
    store
        .put_opts(&key, "a".into(), PutMode::Create.into())
        .await?;
    assert!(matches!(
        store
            .put_opts(&key, "b".into(), PutMode::Create.into())
            .await,
        Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. })
    ));
    let transport = CheckpointStore::new(
        store,
        prefix.into(),
        Limits::default(),
        CreateCapability::AtomicCreateVerified,
    )?;
    let dirs = tempfile::tempdir()?;
    let source = dirs.path().join("source");
    let backup = dirs.path().join("backup");
    let cluster = crate::Cluster::new();
    let node = cluster.start_durable(1, &source).await?;
    // Force real files through production apply + RocksDB flush, never pad a backup.
    let db = &node.state_machine.storage.as_ref().unwrap().db;
    db.set_options(&[
        ("disable_auto_compactions", "true"),
        ("level0_stop_writes_trigger", "100"),
        ("level0_slowdown_writes_trigger", "80"),
    ])?;
    if blob {
        db.set_options(&[
            ("enable_blob_files", "true"),
            ("min_blob_size", "4096"),
            ("blob_file_size", "1048576"),
        ])?;
    }
    node.raft
        .initialize(BTreeMap::from([(1, BasicNode::default())]))
        .await?;
    cluster
        .wait_for_leader(std::slice::from_ref(&node), 10_000)
        .await?;
    node.raft
        .client_write(crate::LogCommand::D1V1(crate::D1Command::Activate {
            shard: 7,
            epoch: 3,
        }))
        .await?;
    for start in (0..ROWS).step_by(BATCH as usize) {
        let reply = node
            .raft
            .client_write(crate::LogCommand::D1V1(crate::D1Command::Write(
                crate::D1WriteRequest {
                    shard: 7,
                    epoch: 3,
                    request_id: [((start / BATCH) + 1) as u8; 16],
                    expected_sequence: None,
                    operations: (start..start + BATCH)
                        .map(|id| crate::WriteOp::put(id.to_be_bytes(), value(id)))
                        .collect(),
                },
            )))
            .await?;
        assert!(matches!(
            reply.data.d1,
            Some(Ok(crate::D1Result::Write { .. }))
        ));
        db.flush()?;
    }
    node.physical_checkpoint(&backup).await?;
    let bytes = fs::read(backup.join("checkpoint.json"))?;
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    let sst = manifest
        .files
        .iter()
        .filter(|f| f.name.ends_with(".sst"))
        .count();
    let blobs = manifest
        .files
        .iter()
        .filter(|f| f.name.ends_with(".blob"))
        .count();
    anyhow::ensure!(sst >= 32, "expected >=32 actual SST files, got {sst}");
    if blob {
        anyhow::ensure!(blobs >= 16, "expected >=16 actual blob files, got {blobs}");
    } else {
        assert_eq!(blobs, 0);
    }
    let total = manifest.files.iter().map(|f| f.size).sum::<u64>() + bytes.len() as u64;
    println!("SCALE_CHECKPOINT mode={prefix} rows={ROWS} value_bytes={} files={} sst={sst} blob={blobs} checkpoint_bytes={total}", ROWS as usize * VALUE_BYTES, manifest.files.len()+1);
    let receipt = transport.upload(&backup, "checkpoint").await?;
    println!("SCALE_UPLOAD mode={prefix} complete");
    cluster.shutdown(&node).await;
    drop(node);
    drop(cluster);
    fs::remove_dir_all(&source)?;
    fs::remove_dir_all(&backup)?;
    let downloaded = dirs.path().join("downloaded");
    transport.download(&receipt, &downloaded).await?;
    println!("SCALE_DOWNLOAD mode={prefix} complete");
    assert_eq!(fs::read(downloaded.join("checkpoint.json"))?, bytes);
    let restored = dirs.path().join("restored");
    restore_physical_checkpoint(&downloaded, &restored, 0, 1)?;
    let state = StateMachine::open(restored.join("state.db"))?;
    for id in 0..ROWS {
        assert_eq!(
            state.get_raw(&id.to_be_bytes()).await,
            Some(value(id)),
            "row {id}"
        );
    }
    assert_eq!(
        state.d1_metadata().await,
        Some((7, 3, (ROWS / BATCH) as u64))
    );
    // Do not let an open RocksDB handle mask a missing startup guard.
    drop(state);
    assert!(restored.join("RESTORED_OFFLINE_ONLY").is_file());
    assert!(crate::Cluster::new()
        .start_durable(1, &restored)
        .await
        .is_err());
    assert!(restore_physical_checkpoint(&downloaded, &restored, 0, 1).is_err());
    println!("SCALE_RESTORE mode={prefix} all {ROWS} rows verified; source removed; offline guard and no-overwrite verified");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit multi-file SeaweedFS validation; not a performance benchmark"]
async fn many_sst_files_through_s3() -> anyhow::Result<()> {
    roundtrip(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit test-only blob options and SeaweedFS; production blob mode is not enabled"]
async fn many_blob_files_through_s3() -> anyhow::Result<()> {
    roundtrip(true).await
}
