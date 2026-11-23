use super::*;
use checkpoint_object_store::{
    cohort::PrefixReceipt, CheckpointReader, CheckpointStore, CreateCapability,
    Error as StoreError, Limits,
};
use d2_raft::cohort_inputs::generation;
use futures::stream::BoxStream;
use object_store::{local::LocalFileSystem, path::Path as Key, *};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    path::Path,
    process,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

/// Lose the committed final PUT response, then lose its reconciliation GET.
/// The same backend persists across verifier processes through LocalFileSystem.
#[derive(Debug)]
struct LoseFinalAck {
    inner: Arc<LocalFileSystem>,
    phase: AtomicU8,
}
impl std::fmt::Display for LoseFinalAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("lose-final-ack")
    }
}
fn disconnect() -> object_store::Error {
    object_store::Error::Generic {
        store: "fault-injection",
        source: std::io::Error::other("injected lost response").into(),
    }
}
#[tonic::async_trait]
impl ObjectStore for LoseFinalAck {
    async fn put_opts(
        &self,
        key: &Key,
        data: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        assert!(matches!(opts.mode, PutMode::Create));
        let done = self.inner.put_opts(key, data, opts).await?;
        if key.filename() == Some("generation.json")
            && self
                .phase
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(disconnect());
        }
        Ok(done)
    }
    async fn get_opts(&self, key: &Key, opts: GetOptions) -> object_store::Result<GetResult> {
        if key.filename() == Some("generation.json")
            && self
                .phase
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(disconnect());
        }
        self.inner.get_opts(key, opts).await
    }
    async fn put_multipart_opts(
        &self,
        _: &Key,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unreachable!()
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, object_store::Result<Key>>,
    ) -> BoxStream<'static, object_store::Result<Key>> {
        unreachable!()
    }
    fn list(&self, _: Option<&Key>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        unreachable!()
    }
    async fn list_with_delimiter(&self, _: Option<&Key>) -> object_store::Result<ListResult> {
        unreachable!()
    }
    async fn copy_opts(&self, _: &Key, _: &Key, _: CopyOptions) -> object_store::Result<()> {
        unreachable!()
    }
}

fn backend(base: &Path) -> anyhow::Result<Arc<LocalFileSystem>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(base)?))
}
fn reader(base: &Path, limits: Limits) -> anyhow::Result<CheckpointReader> {
    Ok(CheckpointReader::new(
        backend(base)?,
        "backup".into(),
        limits,
    )?)
}
fn outer(base: &Path, receipt: &PrefixReceipt) -> std::path::PathBuf {
    let digest = sha2::digest::Output::<Sha256>::from(receipt.local_sha256);
    base.join(format!("backup/d2-prefixes/{digest:x}/generation.json"))
}
fn component(base: &Path, receipt: &PrefixReceipt, group: u64) -> std::path::PathBuf {
    let digest = sha2::digest::Output::<Sha256>::from(receipt.local_sha256);
    base.join(format!(
        "backup/checkpoints/d2-{digest:x}-{group}/checkpoint.json"
    ))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_prefix_verified_after_sources_and_publisher_exit() -> anyhow::Result<()> {
    let auth = authority();
    let cohort = binding();
    let scratch = tempfile::tempdir()?;
    if let Some(base) = env::var_os("VELA_TEST_REMOTE_PREFIX_BACKEND") {
        let receipt: PrefixReceipt =
            serde_json::from_str(&env::var("VELA_TEST_REMOTE_PREFIX_RECEIPT")?)?;
        reader(Path::new(&base), Limits::default())?
            .verify_prefix(&receipt, &auth, &cohort, 1, scratch.path())
            .await?;
        println!("verified remote prefix in child");
        return Ok(());
    }
    let local = tempfile::tempdir()?;
    let live = tempfile::tempdir()?;
    let objects = tempfile::tempdir()?;
    let base = objects.path();
    let cluster = Cluster::new();
    let durable = |shard: u64| {
        let group = cluster.in_group(shard);
        let path = live.path().join(shard.to_string());
        async move { initialize(group.start_durable(1, path).await.unwrap(), shard).await }
    };
    let c = durable(9).await;
    let a = durable(7).await;
    let b = durable(8).await;
    let graph = graph(&c, &a, &b);
    let nodes = [&a, &b];
    let faulty = Arc::new(LoseFinalAck {
        inner: backend(base)?,
        phase: AtomicU8::new(0),
    });
    let store = CheckpointStore::new(
        faulty.clone(),
        "backup".into(),
        Limits::default(),
        CreateCapability::AtomicCreateVerified,
    )?;
    let mut receipts = Vec::new();
    for shard in [7, 8] {
        let write = snapshot(&graph)
            .await
            .prepare_vertex(shard, &Vertex::new(shard.into()))
            .unwrap();
        assert!(matches!(
            graph.execute(&write).await.unwrap(),
            Outcome::Committed { visible: true, .. }
        ));
        // Second generation adds data on 8, but both certify the same ordinal.
        let capture = local.path().join(shard.to_string());
        let prepared = generation::prepare(&capture, &auth, &cohort, 1, &c, &nodes).await?;
        prepared.publish().await?;
        let staged = store
            .stage_prefix(&capture, prepared.digest(), &auth, &cohort, 1)
            .await?;
        let receipt = staged.receipt().clone();
        assert!(!outer(base, &receipt).exists());
        if shard == 7 {
            // Remove an actual physical file chunk, not the child manifest.
            let remote: serde_json::Value =
                serde_json::from_slice(&fs::read(component(base, &receipt, 8))?)?;
            let digest = remote["files"]
                .as_array()
                .unwrap()
                .iter()
                .find(|file| file["name"] == "state.db/CURRENT")
                .unwrap()["chunks"][0]["sha256"]
                .as_str()
                .unwrap();
            let blob = base.join("backup/blobs").join(digest);
            let retained = local.path().join("retained-blob");
            fs::rename(&blob, &retained)?;
            assert!(staged.publish(scratch.path()).await.is_err());
            assert!(!outer(base, &receipt).exists());
            fs::rename(&retained, &blob)?;
            assert!(matches!(
                staged.publish(scratch.path()).await,
                Err(StoreError::OutcomeUnknown(_))
            ));
            assert_eq!(faulty.phase.load(Ordering::SeqCst), 2);
            reader(base, Limits::default())?
                .verify_prefix(&receipt, &auth, &cohort, 1, scratch.path())
                .await?;
            fs::rename(&blob, &retained)?;
            assert!(reader(base, Limits::default())?
                .verify_prefix(&receipt, &auth, &cohort, 1, scratch.path())
                .await
                .is_err());
            fs::rename(retained, blob)?;
            // Each child fits this budget in isolation. The sum cannot fit.
            let mut largest = 0;
            for group in [9, 7, 8] {
                let child: serde_json::Value =
                    serde_json::from_slice(&fs::read(component(base, &receipt, group))?)?;
                largest = largest.max(child["files"].as_array().unwrap().len());
            }
            let limits = Limits {
                files: largest,
                ..Limits::default()
            };
            let bounded = CheckpointStore::new(
                backend(base)?,
                "backup".into(),
                limits.clone(),
                CreateCapability::AtomicCreateVerified,
            )?;
            assert!(matches!(
                bounded
                    .stage_prefix(&capture, prepared.digest(), &auth, &cohort, 1)
                    .await,
                Err(StoreError::Limit)
            ));
            assert!(matches!(
                reader(base, limits)?
                    .verify_prefix(&receipt, &auth, &cohort, 1, scratch.path())
                    .await,
                Err(StoreError::Limit)
            ));
        }
        staged.publish(scratch.path()).await?;
        receipts.push(receipt);
    }

    for node in [&c, &a, &b] {
        node.raft.shutdown().await?;
    }
    drop((a, b, c, store));
    live.close()?;
    local.close()?;
    let first = &receipts[0];
    let output = process::Command::new(env::current_exe()?)
        .args([
            "--exact",
            std::thread::current().name().unwrap(),
            "--nocapture",
        ])
        .env("VELA_TEST_REMOTE_PREFIX_BACKEND", base)
        .env(
            "VELA_TEST_REMOTE_PREFIX_RECEIPT",
            serde_json::to_string(first)?,
        )
        .output()?;
    assert!(
        output.status.success()
            && String::from_utf8_lossy(&output.stdout).contains("verified remote prefix in child"),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let read = reader(base, Limits::default())?;
    assert!(read
        .verify_prefix(first, &auth, &cohort, 2, scratch.path())
        .await
        .is_err());
    // Substitute a valid physical shard from another generation, preserving the
    // expected object ID and authenticating the altered transport envelope.
    // The independently selected local producer record remains unchanged.
    let root_key = outer(base, first);
    let original_outer = fs::read(&root_key)?;
    let member_key = component(base, first, 8);
    let original_member = fs::read(&member_key)?;
    let foreign = fs::read(component(base, &receipts[1], 8))?;
    let mut envelope: serde_json::Value = serde_json::from_slice(&original_outer)?;
    // Authenticate altered outer bytes too, so these failures exercise the
    // required replica set independently of the transport envelope digest.
    for omit in [true, false] {
        let mut invalid = envelope.clone();
        let members = invalid["replicas"].as_array_mut().unwrap();
        if omit {
            members.pop();
        } else {
            members[2] = members[1].clone();
        }
        let bytes = serde_json::to_vec(&invalid)?;
        let selection = PrefixReceipt {
            local_sha256: first.local_sha256,
            envelope_sha256: format!("{:x}", Sha256::digest(&bytes)),
        };
        fs::write(&root_key, bytes)?;
        assert!(
            matches!(read.verify_prefix(&selection, &auth, &cohort, 1, scratch.path()).await, Err(StoreError::Invalid(reason)) if reason == "generation replica set")
        );
    }
    envelope["replicas"][2]["manifest_sha256"] = format!("{:x}", Sha256::digest(&foreign)).into();
    let mixed = serde_json::to_vec(&envelope)?;
    let mixed_receipt = PrefixReceipt {
        local_sha256: first.local_sha256,
        envelope_sha256: format!("{:x}", Sha256::digest(&mixed)),
    };
    fs::write(&member_key, foreign)?;
    fs::write(&root_key, mixed)?;
    let error = read
        .verify_prefix(&mixed_receipt, &auth, &cohort, 1, scratch.path())
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::Integrity(ref field) if field == "group 8 physical manifest"),
        "{error}"
    );
    fs::write(member_key, original_member)?;
    fs::write(root_key, original_outer)?;
    read.verify_prefix(first, &auth, &cohort, 1, scratch.path())
        .await?;
    assert_eq!(fs::read_dir(scratch.path())?.count(), 0);
    Ok(())
}
