//! Fault tests for replicated D1 semantics, not a distributed graph API.
use std::collections::BTreeMap;
use std::time::Duration;

use d2_raft::{
    Cluster, D1Command, D1Error, D1Result, D1WriteRequest, D1WriteStatus, LogCommand, NodeHandle,
    WriteOp,
};
use openraft::storage::RaftStateMachine;
use openraft::BasicNode;
use tokio::time::{sleep, timeout};

fn request(id: u8, expected_sequence: u64) -> D1Command {
    D1Command::Write(D1WriteRequest {
        shard: 7,
        epoch: 3,
        request_id: [id; 16],
        expected_sequence: Some(expected_sequence),
        operations: vec![WriteOp::put(b"key", [id]), WriteOp::put(b"other", [id])],
    })
}

async fn leader(nodes: &[NodeHandle], excluded: Option<u64>) -> anyhow::Result<usize> {
    timeout(Duration::from_secs(10), async {
        loop {
            for (index, node) in nodes.iter().enumerate() {
                if Some(node.id) == excluded {
                    continue;
                }
                if node.raft.metrics().borrow().current_leader == Some(node.id)
                    && matches!(
                        timeout(Duration::from_millis(150), node.raft.ensure_linearizable()).await,
                        Ok(Ok(_))
                    )
                {
                    return index;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(Into::into)
}

async fn write(
    node: &NodeHandle,
    command: D1Command,
) -> anyhow::Result<(u64, Result<D1Result, D1Error>)> {
    let response = timeout(
        Duration::from_secs(10),
        node.raft.client_write(LogCommand::D1V1(command)),
    )
    .await??;
    Ok((response.log_id.index, response.data.d1.expect("D1 reply")))
}

async fn converge(
    cluster: &Cluster,
    nodes: &[NodeHandle],
    index: u64,
    sequence: u64,
    id: u8,
) -> anyhow::Result<()> {
    for node in nodes {
        cluster.wait_for_applied(node, index, 10_000).await?;
        assert_eq!(
            node.state_machine.d1_metadata().await,
            Some((7, 3, sequence))
        );
        assert_eq!(node.state_machine.get_raw(b"key").await, Some(vec![id]));
        assert_eq!(node.state_machine.get_raw(b"other").await, Some(vec![id]));
    }
    Ok(())
}

fn success(sequence: u64, status: D1WriteStatus) -> Result<D1Result, D1Error> {
    Ok(D1Result::Write {
        sequence,
        status,
        applied_operations: 2,
    })
}

async fn scenario() -> anyhow::Result<()> {
    let cluster = Cluster::new();
    let dirs: Vec<_> = (0..4)
        .map(|_| tempfile::tempdir())
        .collect::<Result<_, _>>()?;
    let mut nodes = Vec::new();
    for (index, dir) in dirs.iter().take(3).enumerate() {
        nodes.push(cluster.start_durable(index as u64 + 1, dir.path()).await?);
    }
    nodes[0]
        .raft
        .initialize(
            nodes
                .iter()
                .map(|n| {
                    (
                        n.id,
                        BasicNode {
                            addr: format!("inproc://{}", n.id),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .await?;
    let first_leader = leader(&nodes, None).await?;
    let (_, activated) = write(
        &nodes[first_leader],
        D1Command::Activate { shard: 7, epoch: 3 },
    )
    .await?;
    assert_eq!(activated, Ok(D1Result::Activated { shard: 7, epoch: 3 }));
    let first = request(1, 0);
    // Commit, but deliberately discard the client result: model an unknown outcome.
    let (index, _) = write(&nodes[first_leader], first.clone()).await?;
    converge(&cluster, &nodes, index, 1, 1).await?;
    cluster.shutdown(&nodes[first_leader]).await;
    let old_id = nodes[first_leader].id;
    let stopped = nodes.remove(first_leader);
    drop(stopped);
    let next_leader = leader(&nodes, None).await?;
    let (index, retry) = write(&nodes[next_leader], first.clone()).await?;
    assert_eq!(retry, success(1, D1WriteStatus::Duplicate));
    converge(&cluster, &nodes, index, 1, 1).await?;
    nodes.push(
        cluster
            .start_durable(old_id, dirs[(old_id - 1) as usize].path())
            .await?,
    );
    converge(&cluster, &nodes, index, 1, 1).await?;

    // A minority cannot acknowledge or apply a new conditional mutation.
    let isolated = leader(&nodes, None).await?;
    let isolated_id = nodes[isolated].id;
    for node in &nodes {
        if node.id != isolated_id {
            cluster.policy.deny(isolated_id, node.id).await;
            cluster.policy.deny(node.id, isolated_id).await;
        }
    }
    let second = request(2, 1);
    let attempted = timeout(
        Duration::from_millis(800),
        nodes[isolated]
            .raft
            .client_write(LogCommand::D1V1(second.clone())),
    )
    .await;
    assert!(
        !matches!(attempted, Ok(Ok(_))),
        "minority must not acknowledge a write"
    );
    for node in &nodes {
        assert_eq!(node.state_machine.d1_metadata().await, Some((7, 3, 1)));
        assert_eq!(node.state_machine.get_raw(b"key").await, Some(vec![1]));
    }
    let majority = leader(&nodes, Some(isolated_id)).await?;
    let (index, result) = write(&nodes[majority], second.clone()).await?;
    assert_eq!(result, success(2, D1WriteStatus::Committed));
    cluster.policy.clear().await;
    converge(&cluster, &nodes, index, 2, 2).await?;
    let current = leader(&nodes, None).await?;
    let (index, retry) = write(&nodes[current], second.clone()).await?;
    assert_eq!(retry, success(2, D1WriteStatus::Duplicate));
    converge(&cluster, &nodes, index, 2, 2).await?;
    let (rejected_index, rejected) = write(&nodes[current], request(3, 0)).await?;
    assert_eq!(
        rejected,
        Err(D1Error::Conflict {
            expected: 0,
            current: 2
        })
    );
    converge(&cluster, &nodes, rejected_index, 2, 2).await?;

    // Compacted state, including retry identity, must reach a fresh learner.
    nodes[current].raft.trigger().snapshot().await?;
    let snapshot_index = timeout(Duration::from_secs(10), async {
        loop {
            if let Some(log) = nodes[current].raft.metrics().borrow().snapshot {
                if log.index >= rejected_index {
                    return log.index;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    nodes[current]
        .raft
        .trigger()
        .purge_log(snapshot_index)
        .await?;
    timeout(Duration::from_secs(10), async {
        loop {
            if nodes[current]
                .raft
                .metrics()
                .borrow()
                .purged
                .map(|log| log.index)
                .unwrap_or(0)
                >= snapshot_index
            {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let learner = cluster.start_durable(4, dirs[3].path()).await?;
    nodes[current]
        .raft
        .add_learner(
            4,
            BasicNode {
                addr: "inproc://4".into(),
            },
            true,
        )
        .await?;
    cluster
        .wait_for_applied(&learner, snapshot_index, 10_000)
        .await?;
    assert_eq!(learner.state_machine.d1_metadata().await, Some((7, 3, 2)));
    let mut learner_sm = learner.state_machine.clone();
    let installed = learner_sm
        .get_current_snapshot()
        .await?
        .expect("snapshot installed on empty learner");
    let snapshot_json: serde_json::Value = serde_json::from_slice(installed.snapshot.get_ref())?;
    assert_eq!(snapshot_json["d1"]["dedup"].as_array().unwrap().len(), 2);
    drop(learner_sm);
    nodes.push(learner);
    for node in &nodes {
        cluster.shutdown(node).await;
    }
    drop(nodes);

    // Full durable restart: no bootstrap, no regenerated retry ids.
    let mut nodes = Vec::new();
    for (index, dir) in dirs.iter().enumerate() {
        nodes.push(cluster.start_durable(index as u64 + 1, dir.path()).await?);
    }
    let current = leader(&nodes, None).await?;
    let (index, retry) = write(&nodes[current], first).await?;
    assert_eq!(retry, success(1, D1WriteStatus::Duplicate));
    converge(&cluster, &nodes, index, 2, 2).await?;
    let (_, rejected) = write(&nodes[current], request(3, 0)).await?;
    assert_eq!(
        rejected,
        Err(D1Error::Conflict {
            expected: 0,
            current: 2
        })
    );
    let (index, result) = write(&nodes[current], request(3, 2)).await?;
    assert_eq!(result, success(3, D1WriteStatus::Committed));
    converge(&cluster, &nodes, index, 3, 3).await?;
    for node in &nodes {
        cluster.shutdown(node).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_identity_survives_lost_reply_failover_partition_snapshot_and_restart(
) -> anyhow::Result<()> {
    timeout(Duration::from_secs(90), scenario()).await?
}
