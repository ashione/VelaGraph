use super::*;
use checkpoint_object_store::{CheckpointStore, CreateCapability, Error as StoreError, Limits};
use d2_raft::cohort_inputs::generation;
use object_store::{memory::InMemory, path::Path as Key, ObjectStoreExt};
use sha2::{digest::Output, Sha256};
use std::{
    fs,
    future::{poll_fn, Future},
    os::unix::fs::PermissionsExt,
    pin::pin,
    sync::Arc,
};

// Deny removal of the scratch root while leaving replica contents readable and
// writable. Poll the real verifier to its first I/O suspension after creation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_failure_rejects_prefix_publication() -> anyhow::Result<()> {
    if rustix::process::geteuid().is_root() {
        return Ok(()); // Root bypasses the parent directory mode.
    }
    let sources = tempfile::tempdir()?;
    let cluster = Cluster::new();
    let durable = |shard: u64| {
        let group = cluster.in_group(shard);
        let path = sources.path().join(shard.to_string());
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
    let root = sources.path().join("capture");
    let prep = generation::prepare(&root, &authority(), &binding(), 1, &c, &[&a, &b]).await?;
    prep.publish().await?;
    for node in [&c, &a, &b] {
        node.raft.shutdown().await?;
    }
    let backend = Arc::new(InMemory::new());
    let writer = CheckpointStore::new(
        backend.clone(),
        "cleanup".into(),
        Limits::default(),
        CreateCapability::AtomicCreateVerified,
    )?;
    let staged = writer
        .stage_prefix(&root, prep.digest(), &authority(), &binding(), 1)
        .await?;
    let parent = tempfile::tempdir()?;
    let permissions = parent.path().metadata()?.permissions();
    let mut pending = pin!(staged.publish(parent.path()));
    let mut denied = false;
    let result = poll_fn(|cx| {
        let step = pending.as_mut().poll(cx);
        if step.is_pending() && !denied && fs::read_dir(parent.path()).unwrap().next().is_some() {
            fs::set_permissions(parent.path(), fs::Permissions::from_mode(0o500)).unwrap();
            denied = true;
        }
        step
    })
    .await;
    fs::set_permissions(parent.path(), permissions)?;
    assert!(denied);
    assert!(
        matches!(result, Err(StoreError::Io(ref error)) if error.kind() == std::io::ErrorKind::PermissionDenied),
        "{result:?}"
    );
    assert_eq!(fs::read_dir(parent.path())?.count(), 1);
    let digest = Output::<Sha256>::from(prep.digest());
    let key = Key::from(format!("cleanup/d2-prefixes/{digest:x}/generation.json"));
    assert!(matches!(
        backend.get(&key).await,
        Err(object_store::Error::NotFound { .. })
    ));
    staged.publish(parent.path()).await?;
    backend.get(&key).await?;
    Ok(())
}
