use super::*;
use d2_raft::cohort_inputs::generation::{prepare, read_snapshot, restore_offline, verify};
use std::{env, fs, future::Future, path::Path, process, task::Poll};

async fn assert_offline(path: &Path, group: u64) {
    assert!(path.join("RESTORED_OFFLINE_ONLY").is_file());
    let result = Cluster::new().in_group(group).start_durable(1, path).await;
    assert!(result
        .err()
        .expect("restored votes started")
        .to_string()
        .contains("offline-only"));
}

async fn durable_cohort(cluster: &Cluster, root: &Path) -> [NodeHandle; 3] {
    let durable = |shard| {
        let group = cluster.in_group(shard);
        let path = root.join(shard.to_string());
        async move { initialize(group.start_durable(1, path).await.unwrap(), shard).await }
    };
    [durable(9).await, durable(7).await, durable(8).await]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_cohort_restores_offline_and_reopens_after_source_loss() -> anyhow::Result<()> {
    let auth = authority();
    let cohort = binding();
    if let Some(destination) = env::var_os("VELA_TEST_OFFLINE_RESTORE_DESTINATION") {
        let destination = Path::new(&destination);
        let digest = serde_json::from_str(&env::var("VELA_TEST_OFFLINE_RESTORE_DIGEST")?)?;
        verify(destination, digest, &auth, &cohort, 1).await?;
        assert_offline(destination, 0).await;
        for group in [9, 7, 8] {
            let path = destination.join(group.to_string());
            assert_offline(&path, group).await;
            let identity: serde_json::Value =
                serde_json::from_slice(&fs::read(path.join("raft-group.json"))?)?;
            assert_eq!(
                identity,
                serde_json::json!({"version":1,"group_id":group,"node_id":1})
            );
        }
        println!("verified restored cohort without source");
        return Ok(());
    }
    let live = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let output = tempfile::tempdir()?;
    let cluster = Cluster::new();
    let [c, a, b] = durable_cohort(&cluster, live.path()).await;
    let graph = graph(&c, &a, &b);
    let write = snapshot(&graph)
        .await
        .prepare_vertex(7, &Vertex::new(10))
        .unwrap();
    assert!(matches!(
        graph.execute(&write).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    let source = backup.path().join("generation");
    let capture = prepare(&source, &auth, &cohort, 1, &c, &[&a, &b]).await?;
    capture.publish().await?;
    let digest = capture.digest();
    for node in [&c, &a, &b] {
        node.raft.shutdown().await?;
    }
    drop((c, a, b));
    live.close()?;

    let destination = output.path().join("complete");
    assert!(
        restore_offline(&source, &destination, digest, &auth, &cohort, 0)
            .await
            .is_err()
    );
    assert!(!destination.exists());
    let nested = source.join("8/state.db/restore");
    assert!(restore_offline(&source, &nested, digest, &auth, &cohort, 1)
        .await
        .is_err());
    assert!(!nested.exists());

    let partial = output.path().join("cancelled");
    {
        let mut work = Box::pin(restore_offline(
            &source, &partial, digest, &auth, &cohort, 1,
        ));
        std::future::poll_fn(|cx| {
            assert!(work.as_mut().poll(cx).is_pending());
            if partial.join("9/RESTORED_OFFLINE_ONLY").exists() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
    assert_offline(&partial, 0).await;
    assert_offline(&partial.join("9"), 9).await;
    assert!(!partial.join("7").exists());
    assert!(!partial.join("prefix-generation.json").exists());
    assert!(
        restore_offline(&source, &partial, digest, &auth, &cohort, 1)
            .await
            .is_err()
    );

    // Same valid cut and inventory, different producer manifest bytes.
    let manifest = source.join("8/checkpoint.json");
    let original = fs::read(&manifest)?;
    let mut changed = original.clone();
    changed.push(b' ');
    fs::write(&manifest, changed)?;
    let mixed = output.path().join("mixed");
    let err = restore_offline(&source, &mixed, digest, &auth, &cohort, 1)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("physical checkpoint generation mismatch"));
    assert!(!mixed.join("8").exists());
    assert!(!mixed.join("prefix-generation.json").exists());
    assert_offline(&mixed, 0).await;
    fs::write(manifest, original)?;

    // Damage the copied last member before the destination prefix check.
    let damaged = output.path().join("damaged");
    let mut work = Box::pin(restore_offline(
        &source, &damaged, digest, &auth, &cohort, 1,
    ));
    std::future::poll_fn(|cx| {
        assert!(work.as_mut().poll(cx).is_pending());
        if damaged.join("8/RESTORED_OFFLINE_ONLY").exists() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    fs::remove_file(damaged.join("8/state.db/CURRENT"))?;
    assert!(work.await.is_err());
    assert!(!damaged.join("prefix-generation.json").exists());
    assert_offline(&damaged, 0).await;

    restore_offline(&source, &destination, digest, &auth, &cohort, 1).await?;
    assert!(
        restore_offline(&source, &destination, digest, &auth, &cohort, 1)
            .await
            .is_err()
    );
    backup.close()?;
    let test = std::thread::current().name().unwrap().to_owned();
    let child = process::Command::new(env::current_exe()?)
        .args(["--exact", &test, "--nocapture"])
        .env("VELA_TEST_OFFLINE_RESTORE_DESTINATION", destination)
        .env(
            "VELA_TEST_OFFLINE_RESTORE_DIGEST",
            serde_json::to_string(&digest)?,
        )
        .output()?;
    assert!(
        child.status.success()
            && String::from_utf8_lossy(&child.stdout)
                .contains("verified restored cohort without source"),
        "child stdout={} stderr={}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restored_graph_reads_selected_version_after_later_edge_delete() -> anyhow::Result<()> {
    let auth = authority();
    let cohort = binding();
    let e = edge(7, 8, 10);
    let later = VertexRef {
        partition: 8,
        id: 99,
    };
    let lifetime = Duration::from_secs(60);
    if let Some(root) = env::var_os("VELA_TEST_RESTORED_GRAPH_ROOT") {
        let root = Path::new(&root);
        let digest = serde_json::from_str(&env::var("VELA_TEST_RESTORED_GRAPH_DIGEST")?)?;
        let session = read_snapshot(root, digest, &auth, &cohort, 3, limit(), lifetime).await?;
        let view = GraphSnapshot::from_session(session).unwrap();
        assert_eq!(view.version().unwrap(), 3);
        for endpoint in [e.from, e.to] {
            assert_eq!(view.vertex(endpoint).unwrap(), Some(Vertex::new(1)));
        }
        assert_eq!(view.edge(e.identity()).unwrap(), Some(e.clone()));
        assert_eq!(
            view.observed_adjacency(e.from, Direction::Out).unwrap(),
            vec![e.clone()]
        );
        assert_eq!(
            view.observed_adjacency(e.to, Direction::In).unwrap(),
            vec![e]
        );
        assert_eq!(view.vertex(later).unwrap(), None);
        assert_offline(root, 0).await;
        for shard in [7, 8, 9] {
            assert_offline(&root.join(shard.to_string()), shard).await;
        }
        println!("verified historical edge in isolated restore");
        return Ok(());
    }
    let live = tempfile::tempdir()?;
    let backup = tempfile::tempdir()?;
    let output = tempfile::tempdir()?;
    let cluster = Cluster::new();
    let [c, a, b] = durable_cohort(&cluster, live.path()).await;
    let graph = graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    vertex(&graph, 8, 1).await;
    let create = snapshot(&graph).await.prepare_edge(&e).unwrap();
    assert!(matches!(
        graph.execute(&create).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    let delete = snapshot(&graph)
        .await
        .prepare_edge_delete(e.identity())
        .unwrap();
    assert!(matches!(
        graph.execute(&delete).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    vertex(&graph, later.partition, later.id).await;
    let latest = snapshot(&graph).await;
    assert_eq!(latest.version().unwrap(), 5);
    assert!(latest.edge(e.identity()).unwrap().is_none());
    assert!(latest.vertex(later).unwrap().is_some());
    let source = backup.path().join("v3");
    let capture = prepare(&source, &auth, &cohort, 3, &c, &[&a, &b]).await?;
    capture.publish().await?;
    let digest = capture.digest();
    for node in [&c, &a, &b] {
        node.raft.shutdown().await?;
    }
    drop((c, a, b));
    live.close()?;
    let root = output.path().join("restored");
    restore_offline(&source, &root, digest, &auth, &cohort, 3).await?;
    backup.close()?;
    assert!(
        read_snapshot(&root, digest, &auth, &cohort, 5, limit(), lifetime)
            .await
            .is_err()
    );
    assert!(
        read_snapshot(&root, digest, &auth, &cohort, 3, limit(), Duration::ZERO)
            .await
            .is_err()
    );
    let expired = read_snapshot(
        &root,
        digest,
        &auth,
        &cohort,
        3,
        limit(),
        Duration::from_nanos(1),
    )
    .await
    .unwrap_err();
    assert_eq!(
        expired.downcast_ref::<cohort_reads::Error>(),
        Some(&cohort_reads::Error::Expired)
    );
    let record = root.join("prefix-generation.json");
    let unpublished = output.path().join("unpublished");
    fs::rename(&record, &unpublished)?;
    assert!(
        read_snapshot(&root, digest, &auth, &cohort, 3, limit(), lifetime)
            .await
            .is_err()
    );
    fs::rename(unpublished, record)?;
    // Entire first shard fits, but the complete cohort exceeds this budget.
    let session = read_snapshot(&root, digest, &auth, &cohort, 3, limit(), lifetime).await?;
    let first = session.shard(7)?;
    for low in [
        versioned_reads::ReadLimit {
            rows: first.entries.len(),
            ..limit()
        },
        versioned_reads::ReadLimit {
            bytes: first.entries.iter().map(|(k, v)| k.len() + v.len()).sum(),
            ..limit()
        },
        versioned_reads::ReadLimit {
            operations: first.replay_operations,
            ..limit()
        },
    ] {
        let error = read_snapshot(&root, digest, &auth, &cohort, 3, low, lifetime)
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<cohort_reads::Error>(),
            Some(&cohort_reads::Error::Capacity)
        );
    }
    let file = root.join("8/checkpoint.json");
    let original = fs::read(&file)?;
    let mut changed = original.clone();
    changed.push(b' ');
    fs::write(&file, changed)?;
    let err = read_snapshot(&root, digest, &auth, &cohort, 3, limit(), lifetime)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("group 8 checkpoint digest mismatch"),
        "{err:#}"
    );
    fs::write(file, original)?;
    let test = std::thread::current().name().unwrap().to_owned();
    let child = process::Command::new(env::current_exe()?)
        .args(["--exact", &test, "--nocapture"])
        .env("VELA_TEST_RESTORED_GRAPH_ROOT", root)
        .env(
            "VELA_TEST_RESTORED_GRAPH_DIGEST",
            serde_json::to_string(&digest)?,
        )
        .output()?;
    assert!(
        child.status.success()
            && String::from_utf8_lossy(&child.stdout)
                .contains("verified historical edge in isolated restore"),
        "child stdout={} stderr={}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    Ok(())
}
