use d2_raft::{Cluster, D1Command, LogCommand};
use replicated_graph::{run_blocking, ReplicatedStorageNode};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use velagraph::{Error, GraphRecordStore, StorageNodeError, Vertex};
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_after_submit_then_exact_retry_and_epoch_fence() {
    tokio::time::timeout(Duration::from_secs(30), scenario())
        .await
        .unwrap();
}

async fn scenario() {
    let cluster = Cluster::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        nodes.push(cluster.start(id).await.unwrap());
    }
    nodes[0]
        .raft
        .initialize(BTreeMap::from_iter(
            (1..=3).map(|id| (id, openraft::BasicNode::default())),
        ))
        .await
        .unwrap();
    let i = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (i, n) in nodes.iter().enumerate() {
                if n.raft.ensure_linearizable().await.is_ok() {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let node = &nodes[i];
    node.raft
        .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
        .await
        .unwrap();
    let graph = Arc::new(GraphRecordStore::new(
        ReplicatedStorageNode::new(node, 7, Duration::from_millis(300)).unwrap(),
        7,
        0,
    ));
    let g = graph.clone();
    let p = run_blocking(move || g.prepare_create_vertex(&Vertex::new(1)))
        .await
        .unwrap()
        .unwrap();
    let guard = node.state_machine.pause_state().await;
    let g = graph.clone();
    let retry = p.clone();
    let result = run_blocking(move || g.commit_prepared(&retry))
        .await
        .unwrap();
    eprintln!("real queued write while apply locked: {result:?}");
    assert!(
        matches!(result,Err(Error::StorageNode(StorageNodeError::Timeout{request_id:Some(id)})) if id==p.request_id())
    );
    drop(guard);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if node.state_machine.d1_metadata().await == Some((7, 3, 1)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let g = graph.clone();
    let retry = p.clone();
    run_blocking(move || g.commit_prepared(&retry))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(node.state_machine.d1_metadata().await, Some((7, 3, 1)));
    node.raft
        .client_write(LogCommand::D1V1(D1Command::SetEpoch {
            shard: 7,
            expected_epoch: 3,
            new_epoch: 4,
        }))
        .await
        .unwrap();
    let g = graph.clone();
    let result = run_blocking(move || g.commit_prepared(&p)).await.unwrap();
    assert!(matches!(
        result,
        Err(Error::StorageNode(StorageNodeError::Fenced {
            current: 4,
            ..
        }))
    ));
    for n in &nodes {
        cluster.shutdown(n).await;
    }
}
