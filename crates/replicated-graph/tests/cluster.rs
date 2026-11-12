//! Real durable replicas, not a mock StorageNode. Every async scenario is bounded.
use d2_raft::{Cluster, D1Command, D1Result, LogCommand, NodeHandle};
use openraft::{storage::RaftStateMachine, BasicNode};
use replicated_graph::{run_blocking, ReplicatedStorageNode};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};
use velagraph::{
    Edge, Error, Export, GraphRecordStore, KeyRange, PreparedGraphMutation, ScanLimit, StorageNode,
    StorageNodeError, Value, Vertex,
};

type Graph = Arc<GraphRecordStore<ReplicatedStorageNode>>;
fn graph(node: &NodeHandle) -> Graph {
    Arc::new(GraphRecordStore::new(
        ReplicatedStorageNode::new(node, 7, Duration::from_millis(800)).unwrap(),
        7,
        0,
    ))
}
async fn on<T: Send + 'static>(
    g: &Graph,
    f: impl FnOnce(&GraphRecordStore<ReplicatedStorageNode>) -> velagraph::Result<T> + Send + 'static,
) -> velagraph::Result<T> {
    let g = g.clone();
    run_blocking(move || f(&g)).await?
}
async fn commit(g: &Graph, p: &PreparedGraphMutation) -> velagraph::Result<()> {
    let p = p.clone();
    on(g, move |g| g.commit_prepared(&p)).await
}
async fn leader(nodes: &[NodeHandle], excluded: Option<u64>) -> anyhow::Result<usize> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            for (i, n) in nodes.iter().enumerate() {
                if Some(n.id) != excluded
                    && n.raft.metrics().borrow().current_leader == Some(n.id)
                    && matches!(
                        timeout(Duration::from_millis(150), n.raft.ensure_linearizable()).await,
                        Ok(Ok(_))
                    )
                {
                    return i;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?)
}
async fn start(cluster: &Cluster, dirs: &[tempfile::TempDir]) -> anyhow::Result<Vec<NodeHandle>> {
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(cluster.start_durable(i as u64 + 1, dir.path()).await?);
    }
    Ok(nodes)
}
async fn bootstrap(nodes: &[NodeHandle]) -> anyhow::Result<usize> {
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
    let i = leader(nodes, None).await?;
    let response = nodes[i]
        .raft
        .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
        .await?;
    assert_eq!(
        response.data.d1.unwrap(),
        Ok(D1Result::Activated { shard: 7, epoch: 3 })
    );
    Ok(i)
}
async fn converge(cluster: &Cluster, nodes: &[NodeHandle], current: usize) -> anyhow::Result<()> {
    // State-machine application can precede publication of Raft metrics.
    // Wait for the actual applied index, not a possibly stale metrics index.
    let index = nodes[current]
        .state_machine
        .clone()
        .applied_state()
        .await?
        .0
        .unwrap()
        .index;
    let expected = nodes[current]
        .state_machine
        .d1_read_snapshot(10000, 64 * 1024 * 1024)
        .await
        .expect("bounded graph snapshot")
        .unwrap();
    for n in nodes {
        cluster.wait_for_applied(n, index, 10000).await?;
        let actual = n
            .state_machine
            .d1_read_snapshot(10000, 64 * 1024 * 1024)
            .await
            .expect("bounded graph snapshot")
            .unwrap();
        assert_eq!(
            (actual.shard, actual.epoch, actual.sequence),
            (expected.shard, expected.epoch, expected.sequence)
        );
        assert_eq!(actual.entries, expected.entries);
    }
    Ok(())
}
fn conflict(result: velagraph::Result<()>) {
    assert!(
        matches!(
            result,
            Err(Error::StorageNode(StorageNodeError::Conflict { .. }))
        ),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_crud_conflicts_and_follower_rejection() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        let cluster = Cluster::new();
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let g = graph(&nodes[current]);
        assert!(matches!(
            g.get_vertex(1),
            Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
        ));
        assert!(matches!(
            g.create_vertex(&Vertex::new(100)),
            Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
        ));
        on(&g, |g| {
            for id in 1..=3 {
                g.create_vertex(&Vertex::new(id))?;
            }
            let mut vertex = Vertex::new(1);
            vertex.labels.push("person".into());
            vertex.properties.insert("age".into(), Value::Integer(42));
            g.update_vertex(&vertex)?;
            assert_eq!(g.get_vertex(1)?, Some(vertex));
            g.create_edge(&Edge::new(10, 1, 2, "knows"))?;
            assert_eq!(
                g.out_edges(1, Some("knows"), 10)?,
                vec![Edge::new(10, 1, 2, "knows")]
            );
            assert_eq!(g.in_edges(2, None, 10)?.len(), 1);
            let mut edge = Edge::new(10, 3, 1, "likes");
            edge.properties.insert("weight".into(), Value::Integer(5));
            g.update_edge(&edge)?;
            assert_eq!(g.get_edge(10)?, Some(edge.clone()));
            assert!(g.out_edges(1, None, 10)?.is_empty());
            assert!(g.in_edges(2, None, 10)?.is_empty());
            assert_eq!(g.out_edges(3, Some("likes"), 10)?, vec![edge]);
            g.delete_edge(10)?;
            assert!(g.get_edge(10)?.is_none());
            assert!(g.out_edges(3, None, 10)?.is_empty());
            assert!(g.in_edges(1, None, 10)?.is_empty());
            for e in [
                Edge::new(11, 1, 2, "out"),
                Edge::new(12, 2, 1, "in"),
                Edge::new(13, 1, 1, "self"),
            ] {
                g.create_edge(&e)?;
            }
            g.delete_vertex(1)?;
            assert!(g.get_vertex(1)?.is_none());
            assert!(g.get_vertex(2)?.is_some());
            for id in 11..=13 {
                assert!(g.get_edge(id)?.is_none());
            }
            for id in 2..=3 {
                assert!(g.out_edges(id, None, 10)?.is_empty());
                assert!(g.in_edges(id, None, 10)?.is_empty());
            }
            Ok(())
        })
        .await?;
        // Two stores have independent mutexes and independently prepared identities.
        let other = graph(&nodes[current]);
        let a = on(&g, |g| g.prepare_create_vertex(&Vertex::new(20))).await?;
        let b = on(&other, |g| g.prepare_create_vertex(&Vertex::new(20))).await?;
        assert_ne!(a.request_id(), b.request_id());
        assert_eq!(a.expected_sequence(), b.expected_sequence());
        commit(&g, &a).await?;
        conflict(commit(&other, &b).await);
        let edge = on(&g, |g| g.prepare_create_edge(&Edge::new(30, 2, 3, "race"))).await?;
        let deletion = on(&other, |g| g.prepare_delete_vertex(3)).await?;
        commit(&other, &deletion).await?;
        conflict(commit(&g, &edge).await);
        on(&g, |g| {
            assert!(g.get_edge(30)?.is_none());
            assert!(g.out_edges(2, None, 10)?.is_empty());
            Ok(())
        })
        .await?;
        // Reverse order: a stale cascade must not leave a newly created edge dangling.
        on(&g, |g| g.create_vertex(&Vertex::new(3))).await?;
        let deletion = on(&other, |g| g.prepare_delete_vertex(3)).await?;
        on(&g, |g| g.create_edge(&Edge::new(31, 2, 3, "race"))).await?;
        conflict(commit(&other, &deletion).await);
        on(&g, |g| {
            g.delete_vertex(3)?;
            assert!(g.get_edge(31)?.is_none());
            Ok(())
        })
        .await?;
        let follower = graph(&nodes[(current + 1) % 3]);
        assert!(matches!(
            on(&follower, |g| g.get_vertex(2)).await,
            Err(Error::StorageNode(StorageNodeError::NotLeader { .. }))
        ));
        let prepared = on(&g, |g| g.prepare_create_vertex(&Vertex::new(99))).await?;
        assert!(matches!(
            commit(&follower, &prepared).await,
            Err(Error::StorageNode(StorageNodeError::NotLeader { .. }))
        ));
        converge(&cluster, &nodes, current).await?;
        drop((g, other, follower));
        for node in &nodes {
            cluster.shutdown(node).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_graph_retry_survives_failover_partition_snapshot_and_restart(
) -> anyhow::Result<()> {
    timeout(Duration::from_secs(90), async {
        let cluster = Cluster::new();
        let dirs = (0..4)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let mut nodes = start(&cluster, &dirs[..3]).await?;
        let current = bootstrap(&nodes).await?;
        let g = graph(&nodes[current]);
        let retained = on(&g, |g| g.prepare_create_vertex(&Vertex::new(1))).await?;
        // Commit then discard the reply, modeling a lost response rather than a failed write.
        commit(&g, &retained).await?;
        converge(&cluster, &nodes, current).await?;
        let old_id = nodes[current].id;
        cluster.shutdown(&nodes[current]).await;
        drop(g);
        drop(nodes.remove(current));
        let current = leader(&nodes, None).await?;
        let g = graph(&nodes[current]);
        commit(&g, &retained).await?;
        assert_eq!(
            nodes[current].state_machine.d1_metadata().await,
            Some((7, 3, 1))
        );
        drop(g);
        nodes.push(
            cluster
                .start_durable(old_id, dirs[old_id as usize - 1].path())
                .await?,
        );
        converge(&cluster, &nodes, current).await?;
        let isolated = leader(&nodes, None).await?;
        let g = graph(&nodes[isolated]);
        let second = on(&g, |g| g.prepare_create_vertex(&Vertex::new(2))).await?;
        let isolated_id = nodes[isolated].id;
        for n in &nodes {
            if n.id != isolated_id {
                cluster.policy.deny(isolated_id, n.id).await;
                cluster.policy.deny(n.id, isolated_id).await;
            }
        }
        let result = commit(&g, &second).await;
        assert!(
            matches!(
                result,
                Err(Error::StorageNode(
                    StorageNodeError::Timeout { .. }
                        | StorageNodeError::NotLeader { .. }
                        | StorageNodeError::Unavailable(_)
                ))
            ),
            "minority acknowledged: {result:?}"
        );
        assert!(
            on(&g, |g| g.get_vertex(1)).await.is_err(),
            "isolated leader must not serve a new linearizable view"
        );
        for n in &nodes {
            assert_eq!(n.state_machine.d1_metadata().await, Some((7, 3, 1)));
        }
        drop(g);
        let current = leader(&nodes, Some(isolated_id)).await?;
        let g = graph(&nodes[current]);
        commit(&g, &second).await?;
        cluster.policy.clear().await;
        converge(&cluster, &nodes, current).await?;
        commit(&g, &second).await?;
        assert_eq!(
            nodes[current].state_machine.d1_metadata().await,
            Some((7, 3, 2))
        );
        on(&g, |g| {
            assert_eq!(g.get_vertex(1)?, Some(Vertex::new(1)));
            assert_eq!(g.get_vertex(2)?, Some(Vertex::new(2)));
            Ok(())
        })
        .await?;
        drop(g);
        let applied = nodes[current]
            .raft
            .metrics()
            .borrow()
            .last_applied
            .unwrap()
            .index;
        nodes[current].raft.trigger().snapshot().await?;
        let snapshot_index = timeout(Duration::from_secs(10), async {
            loop {
                if let Some(log) = nodes[current].raft.metrics().borrow().snapshot {
                    if log.index >= applied {
                        break log.index;
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
                    break;
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
            .wait_for_applied(&learner, snapshot_index, 10000)
            .await?;
        let mut sm = learner.state_machine.clone();
        assert!(
            sm.get_current_snapshot().await?.is_some(),
            "empty learner must install compacted snapshot"
        );
        drop(sm);
        nodes.push(learner);
        converge(&cluster, &nodes, current).await?;
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        drop(nodes);
        let nodes = start(&cluster, &dirs).await?;
        let current = leader(&nodes, None).await?;
        let g = graph(&nodes[current]);
        commit(&g, &retained).await?;
        commit(&g, &second).await?;
        assert_eq!(
            nodes[current].state_machine.d1_metadata().await,
            Some((7, 3, 2)),
            "retries must not create new logical writes"
        );
        on(&g, |g| {
            assert_eq!(g.get_vertex(1)?, Some(Vertex::new(1)));
            assert_eq!(g.get_vertex(2)?, Some(Vertex::new(2)));
            g.create_edge(&Edge::new(5, 1, 2, "after_restart"))?;
            assert_eq!(g.in_edges(2, None, 10)?.len(), 1);
            Ok(())
        })
        .await?;
        converge(&cluster, &nodes, current).await?;
        drop(g);
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_views_bounded_scans_and_replicated_epoch_fencing() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        let cluster = Cluster::new();
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let g = graph(&nodes[current]);
        let committed = on(&g, |g| g.prepare_create_vertex(&Vertex::new(1))).await?;
        commit(&g, &committed).await?;
        on(&g, |g| g.create_vertex(&Vertex::new(2))).await?;
        let old_view = on(&g, |g| Ok(g.node().read_view()?)).await?;
        assert_eq!(
            (old_view.shard, old_view.epoch, old_view.sequence),
            (7, 3, 2)
        );
        let saved_view = old_view.clone();
        let old_rows = on(&g, move |g| {
            Ok(g.node()
                .scan(KeyRange::all(), ScanLimit::default(), &saved_view)?)
        })
        .await?;
        assert_eq!(old_rows.len(), 2);
        on(&g, |g| {
            let mut updated = Vertex::new(1);
            updated.labels.push("updated".into());
            updated
                .properties
                .insert("version".into(), Value::Integer(2));
            g.update_vertex(&updated)
        })
        .await?;
        let saved_view = old_view.clone();
        let saved_rows = old_rows.clone();
        on(&g, move |g| {
            let node = g.node();
            assert_eq!(saved_view.sequence, 2);
            for row in &saved_rows {
                assert_eq!(node.get(&row.key, &saved_view)?, Some(row.value.clone()));
            }
            assert_eq!(
                node.scan(KeyRange::all(), ScanLimit::default(), &saved_view)?,
                saved_rows
            );
            let fresh = node.read_view()?;
            assert_eq!(fresh.sequence, 3);
            let fresh_rows = node.scan(KeyRange::all(), ScanLimit::default(), &fresh)?;
            assert_ne!(fresh_rows, saved_rows);
            assert!(saved_rows
                .iter()
                .any(|row| node.get(&row.key, &fresh).unwrap() != Some(row.value.clone())));
            let bytes = saved_rows
                .iter()
                .map(|row| row.key.len() + row.value.len())
                .sum::<usize>();
            assert_eq!(
                node.scan(KeyRange::all(), ScanLimit::new(2, bytes)?, &saved_view)?,
                saved_rows
            );
            // A limit is an all-or-error contract, never a truncated successful result.
            assert!(matches!(
                node.scan(KeyRange::all(), ScanLimit::new(1, bytes)?, &saved_view),
                Err(StorageNodeError::ResourceExhausted(_))
            ));
            assert!(matches!(
                node.scan(KeyRange::all(), ScanLimit::new(2, bytes - 1)?, &saved_view),
                Err(StorageNodeError::ResourceExhausted(_))
            ));
            let first_only = KeyRange::new(saved_rows[0].key.clone(), saved_rows[1].key.clone())?;
            assert_eq!(
                node.scan(first_only, ScanLimit::new(1, bytes)?, &saved_view)?,
                vec![saved_rows[0].clone()]
            );
            assert!(matches!(
                node.admin().export(),
                Err(StorageNodeError::Unsupported(_))
            ));
            assert!(matches!(
                node.admin().import(Export {
                    shard: 7,
                    epoch: 3,
                    sequence: 2,
                    entries: saved_rows
                }),
                Err(StorageNodeError::Unsupported(_))
            ));
            assert!(matches!(
                node.admin().set_epoch(4),
                Err(StorageNodeError::Unsupported(_))
            ));
            Ok(())
        })
        .await?;
        let pending = on(&g, |g| g.prepare_create_vertex(&Vertex::new(3))).await?;
        let response = nodes[current]
            .raft
            .client_write(LogCommand::D1V1(D1Command::SetEpoch {
                shard: 7,
                expected_epoch: 3,
                new_epoch: 4,
            }))
            .await?;
        assert_eq!(response.data.d1.unwrap(), Ok(D1Result::Epoch { epoch: 4 }));
        converge(&cluster, &nodes, current).await?;
        for prepared in [&committed, &pending] {
            assert!(matches!(
                commit(&g, prepared).await,
                Err(Error::StorageNode(StorageNodeError::Fenced {
                    shard: 7,
                    epoch: 3,
                    current: 4
                }))
            ));
        }
        let key = old_rows[0].key.clone();
        on(&g, move |g| {
            assert!(matches!(
                g.node().get(&key, &old_view),
                Err(StorageNodeError::Fenced {
                    shard: 7,
                    epoch: 3,
                    current: 4
                })
            ));
            assert!(matches!(
                g.node()
                    .scan(KeyRange::all(), ScanLimit::default(), &old_view),
                Err(StorageNodeError::Fenced {
                    shard: 7,
                    epoch: 3,
                    current: 4
                })
            ));
            let fresh = g.node().read_view()?;
            assert_eq!((fresh.epoch, fresh.sequence), (4, 3));
            assert!(g.get_vertex(3)?.is_none());
            g.create_vertex(&Vertex::new(3))?;
            assert!(g.get_vertex(3)?.is_some());
            Ok(())
        })
        .await?;
        converge(&cluster, &nodes, current).await?;
        drop(g);
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paged_snapshot_survives_failover_without_switching_view() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        use velagraph::{WriteOp, WriteRequest};
        let cluster = Cluster::new();
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let adapter = ReplicatedStorageNode::new(&nodes[current], 7, Duration::from_secs(2))?;
        let a = adapter.clone();
        let view = run_blocking(move || {
            a.batch_write(WriteRequest {
                shard: 7,
                epoch: 3,
                request_id: [91; 16],
                operations: vec![
                    WriteOp::put(b"a", b"1"),
                    WriteOp::put(b"b", b"2"),
                    WriteOp::put(b"c", b"3"),
                ],
            })?;
            a.read_view()
        })
        .await??;
        let limit = ScanLimit::new(1, 2)?;
        // Every adapter page must reject synchronous execution on a Tokio worker.
        assert!(matches!(
            adapter.scan_page(KeyRange::all(), limit, &view, None),
            Err(StorageNodeError::Unsupported(_))
        ));
        let a = adapter.clone();
        let v = view.clone();
        let first = run_blocking(move || a.scan_page(KeyRange::all(), limit, &v, None)).await??;
        assert_eq!(first.rows[0].key, b"a");
        converge(&cluster, &nodes, current).await?;
        cluster.shutdown(&nodes[current]).await;
        let next = leader(&nodes, Some(nodes[current].id)).await?;
        let adapter = ReplicatedStorageNode::new(&nodes[next], 7, Duration::from_secs(2))?;
        let a = adapter.clone();
        let fresh = run_blocking(move || {
            a.batch_write(WriteRequest {
                shard: 7,
                epoch: 3,
                request_id: [92; 16],
                operations: vec![WriteOp::delete(b"b"), WriteOp::put(b"d", b"4")],
            })?;
            a.read_view()
        })
        .await??;
        let cursor = first.next_cursor.unwrap();
        let a = adapter.clone();
        let c = cursor.clone();
        // A freshly acquired view cannot silently replace the original snapshot.
        assert!(matches!(
            run_blocking(move || a.scan_page(KeyRange::all(), limit, &fresh, Some(&c))).await?,
            Err(StorageNodeError::InvalidArgument(_))
        ));
        let a = adapter.clone();
        let v = view.clone();
        let second =
            run_blocking(move || a.scan_page(KeyRange::all(), limit, &v, Some(&cursor))).await??;
        assert_eq!(second.rows[0].key, b"b");
        let a = adapter.clone();
        let v = view.clone();
        let cursor = second.next_cursor.unwrap();
        let last =
            run_blocking(move || a.scan_page(KeyRange::all(), limit, &v, Some(&cursor))).await??;
        assert_eq!(last.rows[0].key, b"c");
        assert!(last.next_cursor.is_none());
        let response = nodes[next]
            .raft
            .client_write(LogCommand::D1V1(D1Command::SetEpoch {
                shard: 7,
                expected_epoch: 3,
                new_epoch: 4,
            }))
            .await?;
        assert!(response.data.d1.unwrap().is_ok());
        let a = adapter.clone();
        assert!(matches!(
            run_blocking(move || a.scan_page(KeyRange::all(), limit, &view, None)).await?,
            Err(StorageNodeError::Fenced { current: 4, .. })
        ));
        for (i, node) in nodes.iter().enumerate() {
            if i != current {
                cluster.shutdown(node).await;
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
