//! Query execution against real durable Raft replicas, including retained snapshots.
use d2_raft::{Cluster, D1Command, D1Result, LogCommand, NodeHandle};
use openraft::{storage::RaftStateMachine, BasicNode};
use replicated_graph::{run_blocking, ReplicatedStorageNode};
use rocksdb_sql_table::{
    Edge, Error, GraphEngine, GraphRecordStore, GremlinQuery, GremlinRow, StorageNodeError, Value,
    Vertex,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};

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
    f: impl FnOnce(&GraphRecordStore<ReplicatedStorageNode>) -> rocksdb_sql_table::Result<T>
        + Send
        + 'static,
) -> rocksdb_sql_table::Result<T> {
    let g = g.clone();
    run_blocking(move || f(&g)).await?
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

fn seed(g: &GraphRecordStore<ReplicatedStorageNode>) -> rocksdb_sql_table::Result<()> {
    for (id, name) in [(1, "alice"), (2, "bob"), (3, "carol")] {
        let mut v = Vertex::new(id);
        v.labels.push("person".into());
        v.properties.insert("name".into(), Value::Text(name.into()));
        g.create_vertex(&v)?;
    }
    g.create_edge(&Edge::new(10, 1, 2, "knows"))?;
    g.create_edge(&Edge::new(11, 2, 3, "knows"))?;
    Ok(())
}

// Compare the existing semantic corpus against identical local and replicated
// fixtures, including rejected syntax and unsupported-step error categories.
fn crosscheck_semantic_corpus(
    replicated: &GraphRecordStore<ReplicatedStorageNode>,
) -> rocksdb_sql_table::Result<()> {
    let dir = tempfile::tempdir().expect("local semantic fixture directory");
    let local = GraphEngine::open(dir.path())?;
    for (id, name) in [(1, "alice"), (2, "bob"), (3, "carol")] {
        let mut vertex = Vertex::new(id);
        vertex.labels.push("person".into());
        vertex
            .properties
            .insert("name".into(), Value::Text(name.into()));
        local.create_vertex(&vertex)?;
    }
    local.create_edge(&Edge::new(10, 1, 2, "knows"))?;
    local.create_edge(&Edge::new(11, 2, 3, "knows"))?;
    let corpus = include_str!("../../../tests/gremlin.rs");
    let mut checked = 0;
    for query in corpus.split('"').filter(|text| text.starts_with("g.")) {
        match (local.gremlin(query), replicated.execute_gremlin(query)) {
            (Ok(expected), Ok(actual)) => assert_eq!(actual, expected, "query: {query}"),
            (Err(expected), Err(actual)) => assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&expected),
                "query: {query}; local: {expected:?}; replicated: {actual:?}",
            ),
            (expected, actual) => {
                panic!("query: {query}; local: {expected:?}; replicated: {actual:?}")
            }
        }
        checked += 1;
    }
    assert!(
        checked > 50,
        "semantic query corpus unexpectedly empty or small"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gremlin_reads_survive_real_leader_failover() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        let cluster = Cluster::new();
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let g = graph(&nodes[current]);
        on(&g, seed).await?;
        on(&g, crosscheck_semantic_corpus).await?;
        let query = "g.V(1).out('knows').out('knows').values('name')";
        let before = on(&g, move |g| g.execute_gremlin(query)).await?;
        assert_eq!(
            before.rows,
            vec![GremlinRow::Value(Value::Text("carol".into()))]
        );
        let follower = graph(&nodes[(current + 1) % 3]);
        assert!(matches!(
            on(&follower, |g| g.execute_gremlin("g.V().count()")).await,
            Err(Error::StorageNode(StorageNodeError::NotLeader { .. }))
        ));
        converge(&cluster, &nodes, current).await?;
        cluster.shutdown(&nodes[current]).await;
        let next = leader(&nodes, Some(nodes[current].id)).await?;
        let next_graph = graph(&nodes[next]);
        on(&next_graph, crosscheck_semantic_corpus).await?;
        let after = on(&next_graph, move |g| {
            GremlinQuery::parse(query)?.execute_records(g)
        })
        .await?;
        assert_eq!(before, after);
        on(&next_graph, |g| {
            g.create_edge(&Edge::new(12, 1, 3, "knows"))?;
            assert_eq!(
                g.execute_gremlin("g.V(1).out('knows').count()")?.rows,
                vec![GremlinRow::Count(2)]
            );
            Ok(())
        })
        .await?;
        for (i, node) in nodes.iter().enumerate() {
            if i != current {
                cluster.shutdown(node).await;
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_gremlin_view_survives_partition_but_is_epoch_fenced() -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let cluster = Cluster::new();
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let g = graph(&nodes[current]);
        on(&g, seed).await?;
        converge(&cluster, &nodes, current).await?;
        assert!(matches!(
            g.read_view(),
            Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
        ));
        assert!(matches!(
            g.execute_gremlin("g.V().count()"),
            Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
        ));
        assert!(matches!(
            GremlinQuery::parse("g.V().count()")?.execute_records(&g),
            Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
        ));

        // Keep the borrowed graph view alive on an allowed blocking thread.
        // Channels synchronize partition and epoch changes without sleep-based races.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (partition_tx, partition_rx) = std::sync::mpsc::channel();
        let (checked_tx, checked_rx) = tokio::sync::oneshot::channel();
        let (epoch_tx, epoch_rx) = std::sync::mpsc::channel();
        let saved_graph = g.clone();
        let runtime = tokio::runtime::Handle::current();
        let work = tokio::spawn(run_blocking(move || -> anyhow::Result<()> {
            let view = saved_graph.read_view()?;
            let query = GremlinQuery::parse("g.V(1).out('knows').values('name')")?;
            let expected = query.execute_record_view(&view)?;
            assert_eq!(
                expected.rows,
                vec![GremlinRow::Value(Value::Text("bob".into()))]
            );
            // Enter a runtime on another thread without run_blocking's permit.
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        let _entered = runtime.enter();
                        for text in ["g.V().count()", "g.V(999)"] {
                            assert!(matches!(
                                view.execute_gremlin(text),
                                Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
                            ));
                            assert!(matches!(
                                GremlinQuery::parse(text)
                                    .unwrap()
                                    .execute_record_view(&view),
                                Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
                            ));
                        }
                    })
                    .join()
                    .unwrap();
            });
            ready_tx.send(()).unwrap();
            partition_rx.recv_timeout(Duration::from_secs(15))?;
            assert_eq!(
                view.execute_gremlin("g.V(1).out('knows').values('name')")?,
                expected
            );
            assert_eq!(query.execute_record_view(&view)?, expected);
            assert_eq!(
                view.execute_gremlin("g.V().count()")?.rows,
                vec![GremlinRow::Count(3)]
            );
            let fresh = saved_graph.execute_gremlin("g.V().count()");
            assert!(
                matches!(
                    fresh,
                    Err(Error::StorageNode(
                        StorageNodeError::Timeout { .. }
                            | StorageNodeError::NotLeader { .. }
                            | StorageNodeError::Unavailable(_)
                    ))
                ),
                "partition served fresh query: {fresh:?}"
            );
            checked_tx.send(()).unwrap();
            epoch_rx.recv_timeout(Duration::from_secs(20))?;
            for text in ["g.V().count()", "g.V(1)", "g.V(999)"] {
                assert!(matches!(
                    view.execute_gremlin(text),
                    Err(Error::StorageNode(StorageNodeError::Fenced {
                        shard: 7,
                        epoch: 3,
                        current: 4
                    }))
                ));
                assert!(matches!(
                    GremlinQuery::parse(text)?.execute_record_view(&view),
                    Err(Error::StorageNode(StorageNodeError::Fenced {
                        shard: 7,
                        epoch: 3,
                        current: 4
                    }))
                ));
            }
            Ok(())
        }));
        timeout(Duration::from_secs(10), ready_rx).await??;
        let isolated_id = nodes[current].id;
        for node in &nodes {
            if node.id != isolated_id {
                cluster.policy.deny(isolated_id, node.id).await;
                cluster.policy.deny(node.id, isolated_id).await;
            }
        }
        let next = leader(&nodes, Some(isolated_id)).await?;
        let next_graph = graph(&nodes[next]);
        on(&next_graph, |g| {
            g.delete_edge(10)?;
            g.create_edge(&Edge::new(12, 1, 3, "knows"))?;
            assert_eq!(
                g.execute_gremlin("g.V(1).out('knows').values('name')")?
                    .rows,
                vec![GremlinRow::Value(Value::Text("carol".into()))]
            );
            Ok(())
        })
        .await?;
        partition_tx.send(())?;
        timeout(Duration::from_secs(10), checked_rx).await??;
        let response = nodes[next]
            .raft
            .client_write(LogCommand::D1V1(D1Command::SetEpoch {
                shard: 7,
                expected_epoch: 3,
                new_epoch: 4,
            }))
            .await?;
        assert_eq!(response.data.d1.unwrap(), Ok(D1Result::Epoch { epoch: 4 }));
        cluster.policy.clear().await;
        converge(&cluster, &nodes, next).await?;
        epoch_tx.send(())?;
        work.await???;
        let fresh_leader = leader(&nodes, None).await?;
        on(&graph(&nodes[fresh_leader]), |g| {
            assert_eq!(
                g.execute_gremlin("g.V().count()")?.rows,
                vec![GremlinRow::Count(3)]
            );
            Ok(())
        })
        .await?;
        for node in &nodes {
            cluster.shutdown(node).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
