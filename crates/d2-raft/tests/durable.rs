//! A cluster restart must recover the Raft log and applied state from RocksDB.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use d2_raft::{initialize_cluster, Cluster, DurableLogStore, LogCommand, NodeHandle};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{CommittedLeaderId, LogId, RaftLogReader, Vote};
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

fn cut(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

async fn seed_startup_cut(
    dir: &Path,
    applied: Option<LogId<u64>>,
    purged: LogId<u64>,
) -> anyhow::Result<()> {
    let mut state = Arc::new(d2_raft::StateMachine::open(dir.join("state.db"))?);
    if let Some(log_id) = applied {
        state
            .apply([openraft::Entry {
                log_id,
                payload: openraft::EntryPayload::Normal(LogCommand::put("witness", "published")),
            }])
            .await?;
    }
    let mut logs = DurableLogStore::open(dir.join("raft.db"))?;
    logs.save_committed(applied).await?;
    logs.purge(purged).await?;
    Ok(())
}

fn tcp_config(dir: &Path) -> d2_raft::TcpNodeConfig {
    d2_raft::TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: dir.to_owned(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_durable_starts_reject_uncovered_purge_before_recovery() -> anyhow::Result<()> {
    for (applied, purged) in [
        (None, cut(3, 7)),
        (Some(cut(3, 7)), cut(3, 8)),
        // Index alone and LogId ordering alone would each accept invalid cuts.
        (Some(cut(3, 7)), cut(2, 7)),
        (Some(cut(3, 7)), cut(2, 8)),
        (Some(cut(3, 7)), cut(4, 6)),
    ] {
        let dir = tempfile::tempdir()?;
        seed_startup_cut(dir.path(), applied, purged).await?;
        for error in [
            Cluster::new().start_durable(1, dir.path()).await.err(),
            d2_raft::TcpNode::start(tcp_config(dir.path())).await.err(),
        ] {
            let error = error.expect("inconsistent durable cut must reject startup");
            assert!(
                error
                    .to_string()
                    .contains("does not cover purged log boundary"),
                "{error:#}"
            );
        }
        let mut logs = DurableLogStore::open(dir.path().join("raft.db"))?;
        assert_eq!(logs.get_log_state().await?.last_purged_log_id, Some(purged));
        assert_eq!(logs.read_committed().await?, applied);
        let state = d2_raft::StateMachine::open(dir.path().join("state.db"))?;
        assert_eq!(state.applied().await, applied);
        assert_eq!(
            state.get("witness").await.as_deref(),
            applied.map(|_| "published")
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_start_cleans_up_to_published_state_and_reopens_at_equality() -> anyhow::Result<()>
{
    let dir = tempfile::tempdir()?;
    let applied = cut(3, 7);
    seed_startup_cut(dir.path(), Some(applied), cut(2, 5)).await?;
    let cluster = Cluster::new();
    let node = cluster.start_durable(1, dir.path()).await?;
    assert_eq!(
        node.state_machine.get("witness").await.as_deref(),
        Some("published")
    );
    cluster.shutdown(&node).await;
    drop(node);
    {
        let mut logs = DurableLogStore::open(dir.path().join("raft.db"))?;
        assert_eq!(
            logs.get_log_state().await?.last_purged_log_id,
            Some(applied)
        );
    }
    let tcp = d2_raft::TcpNode::start(tcp_config(dir.path())).await?;
    assert_eq!(tcp.state_machine.applied().await, Some(applied));
    assert_eq!(
        tcp.state_machine.get("witness").await.as_deref(),
        Some("published")
    );
    tcp.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_validates_the_purged_identity_before_replaying_a_committed_suffix(
) -> anyhow::Result<()> {
    let applied = cut(3, 7);
    let committed = cut(4, 8);
    let suffix = [
        openraft::Entry::<d2_raft::TypeConfig> {
            log_id: committed,
            payload: openraft::EntryPayload::Normal(LogCommand::put("witness", "replayed")),
        },
        openraft::Entry {
            log_id: cut(5, 9),
            payload: openraft::EntryPayload::Normal(LogCommand::put("witness", "uncommitted")),
        },
    ];
    for purged in [cut(2, 7), applied] {
        let dir = tempfile::tempdir()?;
        seed_startup_cut(dir.path(), Some(applied), purged).await?;
        {
            let mut logs = DurableLogStore::open(dir.path().join("raft.db"))?;
            logs.blocking_append(suffix.clone()).await?;
            logs.save_committed(Some(committed)).await?;
        }
        let cluster = Cluster::new();
        let start = cluster.start_durable(1, dir.path()).await;
        let expected_value = if purged == applied {
            let node = start?;
            cluster.shutdown(&node).await;
            "replayed"
        } else {
            let error = start
                .err()
                .expect("committed suffix cannot authorize a mismatched cut");
            assert!(
                error
                    .to_string()
                    .contains("does not cover purged log boundary"),
                "{error:#}"
            );
            "published"
        };
        let state = d2_raft::StateMachine::open(dir.path().join("state.db"))?;
        assert_eq!(state.get("witness").await.as_deref(), Some(expected_value));
        assert_eq!(
            state.applied().await,
            Some(if purged == applied {
                committed
            } else {
                applied
            })
        );
        let mut logs = DurableLogStore::open(dir.path().join("raft.db"))?;
        assert_eq!(logs.get_log_state().await?.last_purged_log_id, Some(purged));
        assert_eq!(logs.read_committed().await?, Some(committed));
        assert_eq!(logs.try_get_log_entries(8..=9).await?, suffix);
    }
    Ok(())
}

// Exercise actual Raft catch-up after a follower restarts with an old durable
// log. The in-process transport isolates log-reader behavior from TCP framing;
// large TCP workload evidence is recorded separately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_follower_catches_up_multiple_bounded_log_batches() -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let cluster = Cluster::new();
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let mut nodes = Vec::new();
        for (index, dir) in dirs.iter().enumerate() {
            nodes.push(cluster.start_durable(index as u64 + 1, dir.path()).await?);
        }
        initialize_cluster(
            &nodes[0].raft,
            nodes
                .iter()
                .map(|n| (n.id, format!("node-{}", n.id)))
                .collect(),
        )
        .await?;
        let leader = cluster.wait_for_leader(&nodes, 10_000).await?;
        let stopped_index = nodes.iter().position(|n| n.id != leader).unwrap();
        let stopped = nodes.remove(stopped_index);
        let stopped_id = stopped.id;
        cluster.shutdown(&stopped).await;
        drop(stopped);
        let leader_node = nodes.iter().find(|n| n.id == leader).unwrap();
        // More than two 256-entry batches, plus payloads that force the byte
        // cap to bind before the entry cap. Keep the normal election settings.
        for index in 0..600 {
            write(leader_node, &format!("small-{index}"), "value").await?;
        }
        let value = "x".repeat(100 * 1024);
        for index in 0..12 {
            write(leader_node, &format!("large-{index}"), &value).await?;
        }
        let applied = leader_node
            .raft
            .metrics()
            .borrow()
            .last_applied
            .unwrap()
            .index;
        assert!(leader_node.raft.metrics().borrow().snapshot.is_none());
        let follower = cluster
            .start_durable(stopped_id, dirs[stopped_id as usize - 1].path())
            .await?;
        cluster.wait_for_applied(&follower, applied, 10_000).await?;
        for index in 0..600 {
            assert_eq!(
                follower
                    .state_machine
                    .get(&format!("small-{index}"))
                    .await
                    .as_deref(),
                Some("value")
            );
        }
        for index in 0..12 {
            assert_eq!(
                follower
                    .state_machine
                    .get(&format!("large-{index}"))
                    .await
                    .as_deref(),
                Some(value.as_str())
            );
        }
        assert!(follower.raft.metrics().borrow().snapshot.is_none());
        // Ensure the converged follower is durable, not just live state.
        cluster.shutdown(&follower).await;
        drop(follower);
        let follower = cluster
            .start_durable(stopped_id, dirs[stopped_id as usize - 1].path())
            .await?;
        cluster.wait_for_applied(&follower, applied, 10_000).await?;
        assert_eq!(
            follower.state_machine.get("large-11").await.as_deref(),
            Some(value.as_str())
        );
        cluster.shutdown(&follower).await;
        for node in &nodes {
            cluster.shutdown(node).await;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await?
}
