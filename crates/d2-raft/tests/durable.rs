//! A cluster restart must recover the Raft log and applied state from RocksDB.

use std::collections::BTreeMap;
use std::time::Duration;

use d2_raft::{initialize_cluster, Cluster, DurableLogStore, LogCommand, NodeHandle};
use openraft::storage::RaftLogStorage;
use openraft::{CommittedLeaderId, LogId, Vote};
use tempfile::TempDir;
use tokio::time::timeout;

async fn write(handle: &NodeHandle, key: &str, value: &str) -> anyhow::Result<()> {
    handle
        .raft
        .client_write(LogCommand::put(key, value))
        .await?;
    Ok(())
}

async fn scenario() -> anyhow::Result<()> {
    let cluster = Cluster::new();
    let dirs: Vec<TempDir> = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<Result<_, _>>()?;
    let mut nodes = Vec::new();
    for (index, dir) in dirs.iter().enumerate() {
        nodes.push(cluster.start_durable(index as u64 + 1, dir.path()).await?);
    }
    let addrs: BTreeMap<u64, String> = nodes
        .iter()
        .map(|node| (node.id, format!("127.0.0.1:{}", 19000 + node.id)))
        .collect();
    initialize_cluster(&nodes[0].raft, addrs.clone()).await?;
    let leader = cluster.wait_for_leader(&nodes, 10_000).await?;
    let leader_handle = nodes
        .iter()
        .find(|node| node.id == leader)
        .expect("leader handle");
    write(leader_handle, "durable", "before-restart").await?;
    for node in &nodes {
        cluster
            .wait_for_value(node, "durable", "before-restart", 10_000)
            .await?;
    }
    let applied = leader_handle
        .raft
        .metrics()
        .borrow()
        .last_applied
        .map(|log| log.index)
        .unwrap_or(0);
    leader_handle.raft.trigger().snapshot().await?;
    timeout(Duration::from_secs(10), async {
        loop {
            if leader_handle
                .raft
                .metrics()
                .borrow()
                .snapshot
                .map(|log| log.index)
                .unwrap_or(0)
                >= applied
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;

    // Shut every node down while retaining its data directory.
    for node in &nodes {
        cluster.shutdown(node).await;
    }
    drop(nodes);

    // Reopen the same directories. No bootstrap call and no in-memory fallback.
    let mut restarted = Vec::new();
    for (index, dir) in dirs.iter().enumerate() {
        restarted.push(cluster.start_durable(index as u64 + 1, dir.path()).await?);
    }
    let leader = cluster.wait_for_leader(&restarted, 10_000).await?;
    for node in &restarted {
        cluster
            .wait_for_value(node, "durable", "before-restart", 10_000)
            .await?;
    }
    let leader_handle = restarted
        .iter()
        .find(|node| node.id == leader)
        .expect("restarted leader handle");
    write(leader_handle, "durable", "after-restart").await?;
    for node in &restarted {
        cluster
            .wait_for_value(node, "durable", "after-restart", 10_000)
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_restart_recovers_log_and_state_machine() {
    let result = timeout(Duration::from_secs(60), scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("durable restart test failed: {error:#}"),
        Err(_) => panic!("durable restart test timed out"),
    }
}

#[tokio::test]
async fn log_metadata_survives_reopen() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let log_id = LogId::new(CommittedLeaderId::new(3, 1), 7);
    let vote = Vote::new_committed(3, 1);
    {
        let mut store = DurableLogStore::open(dir.path().join("raft.db"))?;
        store.save_vote(&vote).await?;
        store.save_committed(Some(log_id)).await?;
        store.purge(log_id).await?;
    }

    let mut reopened = DurableLogStore::open(dir.path().join("raft.db"))?;
    assert_eq!(reopened.read_vote().await?, Some(vote));
    assert_eq!(reopened.read_committed().await?, Some(log_id));
    let state = reopened.get_log_state().await?;
    assert_eq!(state.last_purged_log_id, Some(log_id));
    assert_eq!(state.last_log_id, Some(log_id));
    Ok(())
}
