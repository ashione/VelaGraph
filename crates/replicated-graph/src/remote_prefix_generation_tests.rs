use super::*;
use checkpoint_object_store::{
    cohort::PrefixReceipt, CheckpointReader, CheckpointStore, CreateCapability,
    Error as StoreError, Limits,
};
use d2_raft::cohort_inputs::generation;
use futures::stream::BoxStream;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    local::LocalFileSystem,
    path::Path as Key,
    prefix::PrefixStore,
    *,
};
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
#[derive(Debug)]
struct LoseFinalAck {
    inner: Arc<dyn ObjectStore>,
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

fn outer(receipt: &PrefixReceipt) -> Key {
    let digest = sha2::digest::Output::<Sha256>::from(receipt.local_sha256);
    Key::from(format!("backup/d2-prefixes/{digest:x}/generation.json"))
}
fn component(receipt: &PrefixReceipt, group: u64) -> Key {
    let digest = sha2::digest::Output::<Sha256>::from(receipt.local_sha256);
    Key::from(format!(
        "backup/checkpoints/d2-{digest:x}-{group}/checkpoint.json"
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_prefix_verified_after_sources_and_publisher_exit() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let base = env::var_os("VELA_TEST_REMOTE_PREFIX_BACKEND")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| objects.path().to_owned());
    scenario(
        Arc::new(LocalFileSystem::new_with_prefix(&base)?),
        Some(&base),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable loopback SeaweedFS S3 endpoint"]
async fn two_shard_prefix_through_seaweedfs() -> anyhow::Result<()> {
    let endpoint = env::var("VELAGRAPH_TEST_S3_ENDPOINT")?;
    let port = endpoint
        .strip_prefix("http://127.0.0.1:")
        .ok_or_else(|| anyhow::anyhow!("expected loopback HTTP endpoint"))?
        .parse::<u16>()?;
    let bucket = env::var("VELAGRAPH_TEST_S3_BUCKET")?;
    anyhow::ensure!(
        port != 0 && bucket == "velagraph-ci",
        "expected disposable CI bucket"
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
    scenario(Arc::new(store), None).await
}

const SELECTED_VERSION: u64 = 3;

async fn scenario(store: Arc<dyn ObjectStore>, local_backend: Option<&Path>) -> anyhow::Result<()> {
    let auth = authority();
    let mut link = edge(7, 8, 42);
    link.label = "connects".into();
    let cohort = binding();
    let scratch = tempfile::tempdir()?;
    // Isolate the destructive negative fixtures even on concurrent runs.
    let namespace = env::var("VELA_TEST_REMOTE_PREFIX_NAMESPACE").unwrap_or_else(|_| {
        scratch
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    });
    let objects = Arc::new(PrefixStore::new(store, Key::from(namespace.as_str())));
    let reader = |limits| CheckpointReader::new(objects.clone(), "backup".into(), limits);
    if env::var_os("VELA_TEST_REMOTE_PREFIX_RECEIPT").is_some() {
        let receipt: PrefixReceipt =
            serde_json::from_str(&env::var("VELA_TEST_REMOTE_PREFIX_RECEIPT")?)?;
        reader(Limits::default())?
            .verify_prefix(&receipt, &auth, &cohort, SELECTED_VERSION, scratch.path())
            .await?;
        let output = tempfile::tempdir()?;
        let interrupted = output.path().join("interrupted");
        let read = reader(Limits::default())?;
        {
            use std::{
                future::{poll_fn, Future},
                task::Poll,
            };
            let mut work = Box::pin(read.restore_prefix(
                &receipt,
                &auth,
                &cohort,
                SELECTED_VERSION,
                scratch.path(),
                &interrupted,
            ));
            poll_fn(|cx| {
                assert!(work.as_mut().poll(cx).is_pending());
                for entry in fs::read_dir(scratch.path()).unwrap() {
                    let staging = entry.unwrap().path();
                    if staging.join("9/checkpoint.json").is_file() {
                        assert!(staging.join("RESTORED_OFFLINE_ONLY").is_file());
                        assert!(staging.join("9/RESTORED_OFFLINE_ONLY").is_file());
                        return Poll::Ready(());
                    }
                }
                Poll::Pending
            })
            .await;
        }
        assert!(!interrupted.exists());
        assert_eq!(fs::read_dir(scratch.path())?.count(), 0);
        let restored = output.path().join("restored");
        read.restore_prefix(
            &receipt,
            &auth,
            &cohort,
            SELECTED_VERSION,
            scratch.path(),
            &restored,
        )
        .await?;
        assert_eq!(fs::read_dir(scratch.path())?.count(), 0);
        let existing = output.path().join("used");
        fs::create_dir(&existing)?;
        fs::write(existing.join("sentinel"), b"keep")?;
        assert!(reader(Limits::default())?
            .restore_prefix(
                &receipt,
                &auth,
                &cohort,
                SELECTED_VERSION,
                scratch.path(),
                &existing
            )
            .await
            .is_err());
        assert_eq!(fs::read(existing.join("sentinel"))?, b"keep");
        assert_eq!(fs::read_dir(&existing)?.count(), 1);
        assert_eq!(fs::read_dir(scratch.path())?.count(), 0);
        let session = generation::read_snapshot(
            &restored,
            receipt.local_sha256,
            &auth,
            &cohort,
            SELECTED_VERSION,
            limit(),
            Duration::from_secs(60),
        )
        .await?;
        let view = GraphSnapshot::from_session(session).unwrap();
        assert_eq!(view.version().unwrap(), SELECTED_VERSION);
        for endpoint in [link.from, link.to] {
            assert_eq!(view.vertex(endpoint).unwrap(), Some(Vertex::new(1)));
        }
        assert_eq!(view.edge(link.identity()).unwrap(), Some(link.clone()));
        assert_eq!(
            view.observed_adjacency(link.from, Direction::Out).unwrap(),
            vec![link.clone()]
        );
        assert_eq!(
            view.observed_adjacency(link.to, Direction::In).unwrap(),
            vec![link]
        );
        for group in [0, 9, 7, 8] {
            let path = if group == 0 {
                restored.clone()
            } else {
                restored.join(group.to_string())
            };
            assert!(path.join("RESTORED_OFFLINE_ONLY").is_file());
            let result = Cluster::new().in_group(group).start_durable(1, path).await;
            assert!(result
                .err()
                .expect("restored votes started")
                .to_string()
                .contains("offline-only"));
        }
        println!("verified remote prefix in child");
        return Ok(());
    }
    let local = tempfile::tempdir()?;
    let live = tempfile::tempdir()?;
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
        inner: objects.clone(),
        phase: AtomicU8::new(0),
    });
    let store = CheckpointStore::new(
        faulty.clone(),
        "backup".into(),
        Limits::default(),
        CreateCapability::AtomicCreateVerified,
    )?;
    let mut receipts = Vec::new();
    vertex(&graph, 7, 1).await;
    vertex(&graph, 8, 1).await;
    let create = snapshot(&graph).await.prepare_edge(&link).unwrap();
    assert!(matches!(
        graph.execute(&create).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    for name in ["before-delete", "after-delete"] {
        if name == "after-delete" {
            let delete = snapshot(&graph)
                .await
                .prepare_edge_delete(link.identity())
                .unwrap();
            assert!(matches!(
                graph.execute(&delete).await.unwrap(),
                Outcome::Committed { visible: true, .. }
            ));
            let latest = snapshot(&graph).await;
            assert_eq!(latest.version().unwrap(), SELECTED_VERSION + 1);
            assert_eq!(latest.edge(link.identity()).unwrap(), None);
        }
        let capture = local.path().join(name);
        let prepared =
            generation::prepare(&capture, &auth, &cohort, SELECTED_VERSION, &c, &nodes).await?;
        prepared.publish().await?;
        let staged = store
            .stage_prefix(
                &capture,
                prepared.digest(),
                &auth,
                &cohort,
                SELECTED_VERSION,
            )
            .await?;
        let receipt = staged.receipt().clone();
        assert!(matches!(
            objects.head(&outer(&receipt)).await,
            Err(object_store::Error::NotFound { .. })
        ));
        if name == "before-delete" {
            // Remove an actual physical file chunk, not the child manifest.
            let remote: serde_json::Value = serde_json::from_slice(
                &objects.get(&component(&receipt, 8)).await?.bytes().await?,
            )?;
            let digest = remote["files"]
                .as_array()
                .unwrap()
                .iter()
                .find(|file| file["name"] == "state.db/CURRENT")
                .unwrap()["chunks"][0]["sha256"]
                .as_str()
                .unwrap();
            let blob = Key::from(format!("backup/blobs/{digest}"));
            let retained = objects.get(&blob).await?.bytes().await?;
            objects.delete(&blob).await?;
            assert!(staged.publish(scratch.path()).await.is_err());
            assert_eq!(fs::read_dir(scratch.path())?.count(), 0);
            assert!(matches!(
                objects.head(&outer(&receipt)).await,
                Err(object_store::Error::NotFound { .. })
            ));
            objects.put(&blob, retained.clone().into()).await?;
            assert!(matches!(
                staged.publish(scratch.path()).await,
                Err(StoreError::OutcomeUnknown(_))
            ));
            assert_eq!(faulty.phase.load(Ordering::SeqCst), 2);
            reader(Limits::default())?
                .verify_prefix(&receipt, &auth, &cohort, SELECTED_VERSION, scratch.path())
                .await?;
            objects.delete(&blob).await?;
            assert!(reader(Limits::default())?
                .verify_prefix(&receipt, &auth, &cohort, SELECTED_VERSION, scratch.path())
                .await
                .is_err());
            let absent = local.path().join("missing-blob");
            assert!(reader(Limits::default())?
                .restore_prefix(
                    &receipt,
                    &auth,
                    &cohort,
                    SELECTED_VERSION,
                    scratch.path(),
                    &absent
                )
                .await
                .is_err());
            assert!(!absent.exists());
            assert_eq!(fs::read_dir(scratch.path())?.count(), 0);
            objects.put(&blob, retained.into()).await?;
            // Each child fits this budget in isolation. The sum cannot fit.
            let mut largest = 0;
            for group in [9, 7, 8] {
                let child: serde_json::Value = serde_json::from_slice(
                    &objects
                        .get(&component(&receipt, group))
                        .await?
                        .bytes()
                        .await?,
                )?;
                largest = largest.max(child["files"].as_array().unwrap().len());
            }
            let limits = Limits {
                files: largest,
                ..Limits::default()
            };
            let bounded = CheckpointStore::new(
                objects.clone(),
                "backup".into(),
                limits.clone(),
                CreateCapability::AtomicCreateVerified,
            )?;
            assert!(matches!(
                bounded
                    .stage_prefix(
                        &capture,
                        prepared.digest(),
                        &auth,
                        &cohort,
                        SELECTED_VERSION
                    )
                    .await,
                Err(StoreError::Limit)
            ));
            assert!(matches!(
                reader(limits)?
                    .verify_prefix(&receipt, &auth, &cohort, SELECTED_VERSION, scratch.path())
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
    let mut child = process::Command::new(env::current_exe()?);
    if let Some(base) = local_backend {
        child.env("VELA_TEST_REMOTE_PREFIX_BACKEND", base);
    } else {
        child.arg("--ignored");
    }
    let output = child
        .args([
            "--exact",
            std::thread::current().name().unwrap(),
            "--nocapture",
        ])
        .env("VELA_TEST_REMOTE_PREFIX_NAMESPACE", namespace)
        .env(
            "VELA_TEST_REMOTE_PREFIX_RECEIPT",
            serde_json::to_string(&receipts[1])?,
        )
        .output()?;
    assert!(
        output.status.success()
            && String::from_utf8_lossy(&output.stdout).contains("verified remote prefix in child"),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let read = reader(Limits::default())?;
    assert!(read
        .verify_prefix(first, &auth, &cohort, SELECTED_VERSION + 1, scratch.path())
        .await
        .is_err());
    // Substitute a valid physical shard from another generation, preserving the
    // expected object ID and authenticating the altered transport envelope.
    // The independently selected local producer record remains unchanged.
    let root_key = outer(first);
    let original_outer = objects.get(&root_key).await?.bytes().await?;
    let member_key = component(first, 8);
    let original_member = objects.get(&member_key).await?.bytes().await?;
    let foreign = objects
        .get(&component(&receipts[1], 8))
        .await?
        .bytes()
        .await?;
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
        objects.put(&root_key, bytes.into()).await?;
        assert!(
            matches!(read.verify_prefix(&selection, &auth, &cohort, SELECTED_VERSION, scratch.path()).await, Err(StoreError::Invalid(reason)) if reason == "generation replica set")
        );
    }
    envelope["replicas"][2]["manifest_sha256"] = format!("{:x}", Sha256::digest(&foreign)).into();
    let mixed = serde_json::to_vec(&envelope)?;
    let mixed_receipt = PrefixReceipt {
        local_sha256: first.local_sha256,
        envelope_sha256: format!("{:x}", Sha256::digest(&mixed)),
    };
    objects.put(&member_key, foreign.into()).await?;
    objects.put(&root_key, mixed.into()).await?;
    let error = read
        .verify_prefix(
            &mixed_receipt,
            &auth,
            &cohort,
            SELECTED_VERSION,
            scratch.path(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StoreError::Integrity(ref field) if field == "group 8 physical manifest"),
        "{error}"
    );
    let mixed_destination = scratch.path().join("mixed");
    assert!(matches!(
        read.restore_prefix(
            &mixed_receipt,
            &auth,
            &cohort,
            SELECTED_VERSION,
            scratch.path(),
            &mixed_destination
        )
        .await,
        Err(StoreError::Integrity(_))
    ));
    assert!(!mixed_destination.exists());
    objects.put(&member_key, original_member.into()).await?;
    objects.put(&root_key, original_outer.into()).await?;
    read.verify_prefix(first, &auth, &cohort, SELECTED_VERSION, scratch.path())
        .await?;
    assert_eq!(fs::read_dir(scratch.path())?.count(), 0);
    Ok(())
}
