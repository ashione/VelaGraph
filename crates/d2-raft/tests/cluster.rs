use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use d2_raft::{Cluster, LogCommand, NodeHandle};
use openraft::BasicNode;
use tokio::time::timeout;

fn node(id: u64) -> BasicNode {
    BasicNode {
        addr: format!("inproc://{id}"),
    }
}

async fn start_three() -> anyhow::Result<(Cluster, Vec<NodeHandle>)> {
    let cluster = Cluster::new();
    let n1 = cluster.start(1).await?;
    let n2 = cluster.start(2).await?;
    let n3 = cluster.start(3).await?;
    n1.raft.initialize(BTreeMap::from([(1, node(1))])).await?;
    n1.raft.add_learner(2, node(2), true).await?;
    n1.raft.add_learner(3, node(3), true).await?;
    n1.raft
        .change_membership(BTreeSet::from([1, 2, 3]), false)
        .await?;
    Ok((cluster, vec![n1, n2, n3]))
}

fn handle(nodes: &[NodeHandle], id: u64) -> &NodeHandle {
    nodes.iter().find(|n| n.id == id).expect("node handle")
}

async fn leader_of(nodes: &[NodeHandle], timeout_ms: u64) -> anyhow::Result<u64> {
    timeout(Duration::from_millis(timeout_ms), async {
        loop {
            for n in nodes {
                let current = n.raft.metrics().borrow().current_leader;
                if current == Some(n.id)
                    && timeout(Duration::from_millis(150), n.raft.ensure_linearizable())
                        .await
                        .is_ok()
                {
                    return n.id;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("leader not elected within {timeout_ms}ms"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_writes_and_survive_leader_loss() -> anyhow::Result<()> {
    let (cluster, nodes) = start_three().await?;
    let leader = leader_of(&nodes, 5_000).await?;

    for i in 0..100 {
        let response = handle(&nodes, leader)
            .raft
            .client_write(LogCommand::put(format!("k{i:03}"), format!("v{i:03}")))
            .await?;
        let index = response.log_id.index;
        for n in &nodes {
            cluster.wait_for_applied(n, index, 5_000).await?;
        }
    }

    for n in &nodes {
        let state = n.state_machine.scan().await;
        assert_eq!(state.len(), 100);
        assert_eq!(state["k000"], "v000");
        assert_eq!(state["k099"], "v099");
    }
    handle(&nodes, leader).raft.ensure_linearizable().await?;

    // Stop the leader; the remaining two nodes must elect a new leader and accept writes.
    cluster.shutdown(handle(&nodes, leader)).await;
    let remaining: Vec<u64> = nodes
        .iter()
        .map(|n| n.id)
        .filter(|id| *id != leader)
        .collect();
    let new_leader = timeout(Duration::from_millis(5_000), async {
        loop {
            for id in &remaining {
                let n = handle(&nodes, *id);
                if let Some(current) = n.raft.metrics().borrow().current_leader {
                    if current != leader && current != 0 {
                        return current;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    handle(&nodes, new_leader)
        .raft
        .client_write(LogCommand::put("after", "failover"))
        .await?;
    for id in &remaining {
        cluster
            .wait_for_value(handle(&nodes, *id), "after", "failover", 5_000)
            .await?;
    }

    // Restart the old leader with empty local storage and let it catch up.
    let restarted = cluster.start(leader).await?;
    cluster
        .wait_for_value(&restarted, "after", "failover", 5_000)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_leader_cannot_commit_until_healed() -> anyhow::Result<()> {
    let (cluster, nodes) = start_three().await?;
    let leader = leader_of(&nodes, 5_000).await?;
    for id in 1..=3 {
        if id != leader {
            cluster.policy.deny(leader, id).await;
            cluster.policy.deny(id, leader).await;
        }
    }

    let blocked = timeout(
        Duration::from_millis(800),
        handle(&nodes, leader)
            .raft
            .client_write(LogCommand::put("blocked", "value")),
    )
    .await;
    assert!(
        blocked.is_err(),
        "write must not commit on a partitioned leader"
    );

    cluster.policy.clear().await;
    let new_leader = leader_of(&nodes, 5_000).await?;
    handle(&nodes, new_leader)
        .raft
        .client_write(LogCommand::put("healed", "value"))
        .await?;
    for n in &nodes {
        cluster.wait_for_value(n, "healed", "value", 5_000).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_learner_installs_a_snapshot() -> anyhow::Result<()> {
    let (cluster, nodes) = start_three().await?;
    let leader = leader_of(&nodes, 5_000).await?;
    for i in 0..20 {
        handle(&nodes, leader)
            .raft
            .client_write(LogCommand::put(format!("s{i}"), "v"))
            .await?;
    }
    let applied = handle(&nodes, leader)
        .raft
        .metrics()
        .borrow()
        .last_applied
        .map(|l| l.index)
        .unwrap_or(0);
    handle(&nodes, leader).raft.trigger().snapshot().await?;
    timeout(Duration::from_millis(5_000), async {
        loop {
            if handle(&nodes, leader)
                .raft
                .metrics()
                .borrow()
                .snapshot
                .map(|l| l.index)
                .unwrap_or(0)
                >= applied
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    handle(&nodes, leader)
        .raft
        .trigger()
        .purge_log(applied)
        .await?;
    timeout(Duration::from_millis(5_000), async {
        loop {
            if handle(&nodes, leader)
                .raft
                .metrics()
                .borrow()
                .purged
                .map(|l| l.index)
                .unwrap_or(0)
                >= applied
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;

    // A fresh learner has no local log; the leader must transfer the snapshot.
    let learner = cluster.start(4).await?;
    handle(&nodes, leader)
        .raft
        .add_learner(4, node(4), true)
        .await?;
    cluster.wait_for_value(&learner, "s19", "v", 5_000).await?;
    let installed = learner
        .raft
        .get_snapshot()
        .await?
        .expect("learner must have installed a snapshot");
    assert!(installed.meta.last_log_id.map(|l| l.index).unwrap_or(0) >= applied);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opaque_d1_batch_replicates_across_nodes() -> anyhow::Result<()> {
    let (_cluster, nodes) = start_three().await?;
    let leader = leader_of(&nodes, 5_000).await?;
    let key = b"graph/v1/p/0/vertex/\x00\xff";
    let value = b"opaque-vertex";

    let response = handle(&nodes, leader)
        .raft
        .client_write(LogCommand::put(key, value))
        .await?;
    for n in &nodes {
        _cluster
            .wait_for_applied(n, response.log_id.index, 5_000)
            .await?;
        assert_eq!(
            n.state_machine.get_raw(key).await.as_deref(),
            Some(&value[..]),
            "node {} must expose the opaque value",
            n.id
        );
    }
    Ok(())
}
