use super::*;
use checkpoint_object_store::{
    cohort::StagedPrefix, CheckpointReader, CheckpointStore, CreateCapability, Error as StoreError,
    Limits,
};
use d2_raft::cohort_inputs::generation;
use object_store::{memory::InMemory, path::Path as Key, ObjectStoreExt};
use sha2::{digest::Output, Sha256};
use std::{
    fs,
    future::{poll_fn, Future},
    os::unix::fs::PermissionsExt,
    path::Path,
    pin::pin,
    sync::Arc,
};

async fn stage<'a>(
    writer: &'a CheckpointStore,
    sources: &Path,
) -> anyhow::Result<StagedPrefix<'a>> {
    let cluster = Cluster::new();
    let durable = |shard: u64| {
        let group = cluster.in_group(shard);
        let path = sources.join(shard.to_string());
        async move { initialize(group.start_durable(1, path).await.unwrap(), shard).await }
    };
    let c = durable(9).await;
    let a = durable(7).await;
    let b = durable(8).await;
    let graph = graph(&c, &a, &b);
    let write = snapshot(&graph)
        .await
        .prepare_vertex(7, &Vertex::new(1))
        .unwrap();
    assert!(matches!(
        graph.execute(&write).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    let root = sources.join("capture");
    let prep = generation::prepare(&root, &authority(), &binding(), 1, &c, &[&a, &b]).await?;
    prep.publish().await?;
    for node in [&c, &a, &b] {
        node.raft.shutdown().await?;
    }
    Ok(writer
        .stage_prefix(&root, prep.digest(), &authority(), &binding(), 1)
        .await?)
}

// Remove write permission on the scratch parent as soon as private staging
// exists. The replica files remain accessible until the final cleanup.
async fn assert_cleanup_denied(
    parent: &Path,
    operation: impl Future<Output = checkpoint_object_store::Result<()>>,
) -> anyhow::Result<()> {
    let permissions = parent.metadata()?.permissions();
    let mut pending = pin!(operation);
    let mut denied = false;
    let result = poll_fn(|cx| {
        let step = pending.as_mut().poll(cx);
        if step.is_pending() && !denied && fs::read_dir(parent).unwrap().next().is_some() {
            fs::set_permissions(parent, fs::Permissions::from_mode(0o500)).unwrap();
            denied = true;
        }
        step
    })
    .await;
    fs::set_permissions(parent, permissions)?;
    assert!(denied);
    assert!(
        matches!(result, Err(StoreError::Io(ref error)) if error.kind() == std::io::ErrorKind::PermissionDenied),
        "{result:?}"
    );
    assert_eq!(fs::read_dir(parent)?.count(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_failure_rejects_prefix_publication() -> anyhow::Result<()> {
    if rustix::process::geteuid().is_root() {
        return Ok(()); // Root bypasses the parent directory mode.
    }
    let sources = tempfile::tempdir()?;
    let backend = Arc::new(InMemory::new());
    let writer = CheckpointStore::new(
        backend.clone(),
        "cleanup".into(),
        Limits::default(),
        CreateCapability::AtomicCreateVerified,
    )?;
    let staged = stage(&writer, sources.path()).await?;
    let parent = tempfile::tempdir()?;
    assert_cleanup_denied(parent.path(), staged.publish(parent.path())).await?;
    let digest = Output::<Sha256>::from(staged.receipt().local_sha256);
    let key = Key::from(format!("cleanup/d2-prefixes/{digest:x}/generation.json"));
    assert!(matches!(
        backend.get(&key).await,
        Err(object_store::Error::NotFound { .. })
    ));
    staged.publish(parent.path()).await?;
    backend.get(&key).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_failure_after_restore_leaves_verified_offline_destination() -> anyhow::Result<()> {
    if rustix::process::geteuid().is_root() {
        return Ok(()); // Root bypasses the parent directory mode.
    }
    let sources = tempfile::tempdir()?;
    let backend = Arc::new(InMemory::new());
    let writer = CheckpointStore::new(
        backend.clone(),
        "cleanup".into(),
        Limits::default(),
        CreateCapability::AtomicCreateVerified,
    )?;
    let staged = stage(&writer, sources.path()).await?;
    let scratch = tempfile::tempdir()?;
    staged.publish(scratch.path()).await?;
    let reader = CheckpointReader::new(backend, "cleanup".into(), Limits::default())?;
    let output = tempfile::tempdir()?;
    let restored = output.path().join("restored");
    let auth = authority();
    let cohort = binding();
    let receipt = staged.receipt();
    assert_cleanup_denied(
        scratch.path(),
        reader.restore_prefix(receipt, &auth, &cohort, 1, scratch.path(), &restored),
    )
    .await?;
    generation::verify(&restored, receipt.local_sha256, &auth, &cohort, 1).await?;
    for group in [0, 9, 7, 8] {
        let path = if group == 0 {
            restored.clone()
        } else {
            restored.join(group.to_string())
        };
        super::offline_cohort_restore_tests::assert_offline(&path, group).await;
    }
    Ok(())
}
