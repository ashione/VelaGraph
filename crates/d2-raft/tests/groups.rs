//! In-process namespaces are independent even when replica numbers overlap.
use std::collections::BTreeMap;
use std::time::Duration;

use d2_raft::{Cluster, LogCommand, NodeHandle};
use openraft::BasicNode;

async fn bootstrap(node: &NodeHandle) -> anyhow::Result<()> {
    node.raft
        .initialize(BTreeMap::from([(node.id, BasicNode::default())]))
        .await?;
    node.raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |m| {
                m.membership_config.log_id().is_some()
                    && m.last_applied.as_ref() >= m.membership_config.log_id().as_ref()
            },
            "initial membership committed",
        )
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_replicas_do_not_cross_deliver() -> anyhow::Result<()> {
    let base = Cluster::new();
    let a = base.in_group(10);
    let b = base.in_group(20);
    let a1 = a.start(1).await?;
    let a2 = a.start(2).await?;
    let b1 = b.start(1).await?;
    let b2 = b.start(2).await?;
    assert_eq!(a1.group_id(), 10);
    assert_eq!(b1.group_id(), 20);
    assert!(base.registry.get(1).await.is_none());
    assert!(a.start(1).await.is_err());
    bootstrap(&a1).await?;
    bootstrap(&b1).await?;
    a1.raft.add_learner(2, BasicNode::default(), true).await?;
    b1.raft.add_learner(2, BasicNode::default(), true).await?;
    a1.raft.client_write(LogCommand::put("shared", "a")).await?;
    b1.raft.client_write(LogCommand::put("shared", "b")).await?;
    a.wait_for_value(&a2, "shared", "a", 5000).await?;
    b.wait_for_value(&b2, "shared", "b", 5000).await?;
    assert_eq!(a1.state_machine.get("shared").await.as_deref(), Some("a"));
    assert_eq!(b1.state_machine.get("shared").await.as_deref(), Some("b"));
    // Even a caller passing a different namespace cannot unregister its replica.
    base.shutdown(&a1).await;
    assert!(a.registry.get(1).await.is_none());
    assert!(b.registry.get(1).await.is_some());
    a.shutdown(&a2).await;
    b.shutdown(&b1).await;
    b.shutdown(&b2).await;
    Ok(())
}

#[tokio::test]
async fn stale_handle_shutdown_does_not_remove_replacement() -> anyhow::Result<()> {
    let cluster = Cluster::new();
    let old = cluster.start(1).await?;
    cluster.shutdown(&old).await;
    let replacement = cluster.start(1).await?;
    cluster.shutdown(&old).await;
    assert!(cluster.registry.get(1).await.is_some());
    cluster.shutdown(&replacement).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_group_binding_reopens_and_rejects_mismatch() -> anyhow::Result<()> {
    let base = Cluster::new();
    let a = base.in_group(7);
    let dir = tempfile::tempdir()?;
    let node = a.start_durable(1, dir.path()).await?;
    bootstrap(&node).await?;
    node.raft
        .client_write(LogCommand::put("durable", "a"))
        .await?;
    a.shutdown(&node).await;
    drop(node);
    let bytes = std::fs::read(dir.path().join("raft-group.json"))?;
    assert_eq!(bytes, br#"{"version":1,"group_id":7,"node_id":1}"#);
    assert!(base.in_group(8).start_durable(1, dir.path()).await.is_err());
    assert!(base.start_durable(1, dir.path()).await.is_err());
    assert!(a.start_durable(2, dir.path()).await.is_err());
    let reopened = a.start_durable(1, dir.path()).await?;
    assert_eq!(reopened.group_id(), 7);
    assert_eq!(
        reopened.state_machine.get("durable").await.as_deref(),
        Some("a")
    );
    a.shutdown(&reopened).await;
    Ok(())
}

#[tokio::test]
async fn legacy_directories_are_only_adopted_by_group_zero() -> anyhow::Result<()> {
    let base = Cluster::new();
    let dir = tempfile::tempdir()?;
    let node = base.start_durable(1, dir.path()).await?;
    bootstrap(&node).await?;
    node.raft
        .client_write(LogCommand::put("legacy", "value"))
        .await?;
    base.shutdown(&node).await;
    drop(node);
    std::fs::remove_file(dir.path().join("raft-group.json"))?;
    assert!(base.in_group(9).start_durable(1, dir.path()).await.is_err());
    assert!(!dir.path().join("raft-group.json").exists());
    let node = base.start_durable(1, dir.path()).await?;
    assert_eq!(node.group_id(), 0);
    assert_eq!(
        node.state_machine.get("legacy").await.as_deref(),
        Some("value")
    );
    base.shutdown(&node).await;
    drop(node);
    std::fs::write(
        dir.path().join("raft-group.json"),
        b"{\"version\":2,\"group_id\":0,\"node_id\":1}",
    )?;
    assert!(base.start_durable(1, dir.path()).await.is_err());
    std::fs::write(dir.path().join("raft-group.json"), b"{")?;
    assert!(base.start_durable(1, dir.path()).await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_start_only_publishes_one_replica() -> anyhow::Result<()> {
    let cluster = Cluster::new().in_group(11);
    let (first, second) = tokio::join!(cluster.start(1), cluster.start(1));
    assert_ne!(first.is_ok(), second.is_ok());
    let node = first.or(second)?;
    let dir = tempfile::tempdir()?;
    assert!(cluster.start_durable(1, dir.path()).await.is_err());
    assert!(std::fs::read_dir(dir.path())?.next().is_none());
    cluster.shutdown(&node).await;
    Ok(())
}
