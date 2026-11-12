//! Nine independent OS processes: one metadata group and two data groups.
//! Functional routing/identity coverage, not failover, TLS or crash evidence.
use d2_raft::{send_control, ControlRequest, ControlResponse};
use replicated_graph::{
    catalog::{Catalog, CatalogError, Placement},
    remote::{snapshot_identified, RemoteIdentity},
    routing::{RemoteReplica, RouteError, RouteResolver},
    run_blocking, RemoteStorageNode,
};
use std::{
    net::SocketAddr,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    time::{sleep, timeout},
};
use velagraph::{
    Edge, GremlinRow, KeyRange, ScanLimit, StorageNode, StorageNodeError, Value, Vertex,
};

const BUDGET: Duration = Duration::from_secs(30);
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Group {
    // Drop children before removing their durable directories, including errors.
    _processes: Vec<Process>,
    _dirs: Vec<tempfile::TempDir>,
    placement: Placement,
    replicas: Vec<RemoteReplica>,
    leader: usize,
}
impl Group {
    async fn start(group: u64, shard: u64) -> anyhow::Result<Self> {
        let mut reservations = Vec::new();
        for _ in 0..9 {
            reservations.push(TcpListener::bind("127.0.0.1:0").await?);
        }
        let addresses = reservations
            .iter()
            .map(|s| s.local_addr())
            .collect::<std::io::Result<Vec<SocketAddr>>>()?;
        let members = (0..3)
            .map(|i| format!("{}={}", i + 1, addresses[3 * i]))
            .collect::<Vec<_>>()
            .join(",");
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<std::io::Result<Vec<_>>>()?;
        let mut processes = Vec::new();
        drop(reservations);
        for i in 0..3 {
            let mut command = Command::new(env!("CARGO_BIN_EXE_graph-node"));
            command.env("TOKIO_WORKER_THREADS", "2").args([
                "--allow-insecure-development",
                "--group",
                &group.to_string(),
                "--id",
                &(i + 1).to_string(),
                "--raft",
                &addresses[3 * i].to_string(),
                "--control",
                &addresses[3 * i + 1].to_string(),
                "--graph",
                &addresses[3 * i + 2].to_string(),
                "--shard",
                &shard.to_string(),
                "--epoch",
                "1",
                "--data-dir",
                dirs[i].path().to_str().unwrap(),
            ]);
            if i == 0 {
                command.args(["--bootstrap", &members]);
            }
            processes.push(Process(
                command
                    .stdout(Stdio::null())
                    .stderr(Stdio::inherit())
                    .spawn()?,
            ));
        }
        let replicas = (0..3)
            .map(|i| RemoteReplica {
                group,
                node: (i + 1) as u64,
                endpoint: addresses[3 * i + 2],
            })
            .collect::<Vec<_>>();
        let leader = timeout(Duration::from_secs(35), async {
            loop {
                for i in 0..3 {
                    if let Ok(ControlResponse::Status(s)) =
                        send_control(&addresses[3 * i + 1].to_string(), ControlRequest::Status)
                            .await
                    {
                        if s.leader == Some((i + 1) as u64)
                            && snapshot_identified(
                                replicas[i].endpoint,
                                RemoteIdentity {
                                    group,
                                    node: (i + 1) as u64,
                                },
                                shard,
                                Duration::from_millis(300),
                            )
                            .await
                            .is_ok()
                        {
                            return i;
                        }
                    }
                }
                sleep(Duration::from_millis(30)).await;
            }
        })
        .await?;
        // Every configured endpoint must be listening before candidate discovery.
        timeout(Duration::from_secs(10), async {
            for replica in &replicas {
                loop {
                    if replicated_graph::remote::metadata_identified(
                        replica.endpoint,
                        RemoteIdentity {
                            group,
                            node: replica.node,
                        },
                        shard,
                        Duration::from_millis(300),
                    )
                    .await
                    .is_ok()
                    {
                        break;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            }
        })
        .await?;
        Ok(Self {
            _processes: processes,
            _dirs: dirs,
            placement: Placement {
                shard,
                group,
                epoch: 1,
                replicas: vec![1, 2, 3],
            },
            replicas,
            leader,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_catalog_routes_two_shards_with_overlapping_ids() -> anyhow::Result<()> {
    timeout(Duration::from_secs(150), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let metadata = Group::start(10, 110).await?;
    let a = Group::start(20, 120).await?;
    let b = Group::start(30, 130).await?;
    let catalog = Catalog::new_remote(
        metadata.replicas[metadata.leader].endpoint,
        metadata.replicas[metadata.leader].node,
        metadata.placement.clone(),
        BUDGET,
    )?;
    let data = a
        .replicas
        .iter()
        .chain(&b.replicas)
        .cloned()
        .collect::<Vec<_>>();
    let resolver =
        RouteResolver::new_remote(metadata.placement.clone(), &metadata.replicas, &data)?;
    assert!(
        RouteResolver::new_remote(metadata.placement.clone(), &metadata.replicas[..2], &data)
            .is_err()
    );
    let incomplete = RouteResolver::new_remote(
        metadata.placement.clone(),
        &metadata.replicas,
        &a.replicas[..2],
    )?;
    let a_placement = a.placement.clone();
    let b_placement = b.placement.clone();
    let endpoint = a.replicas[a.leader].endpoint;
    let node_id = a.replicas[a.leader].node;
    // Correct shard, deliberately wrong group or node. No fallback can mask it.
    let wrong_group = RemoteStorageNode::new_identified(
        endpoint,
        b.placement.group,
        node_id,
        a.placement.shard,
        BUDGET,
    )?;
    let wrong_node = RemoteStorageNode::new_identified(
        endpoint,
        a.placement.group,
        node_id % 3 + 1,
        a.placement.shard,
        BUDGET,
    )?;
    run_blocking(move || -> anyhow::Result<()> {
        let registration = catalog.prepare_registration([1; 16], 1, a_placement.clone())?;
        let stale_registration = catalog.prepare_registration([9; 16], 9, b_placement.clone())?;
        catalog.commit_registration(&registration)?;
        catalog.commit_registration(&registration)?;
        assert_eq!(catalog.snapshot()?.sequence(), 1);
        assert!(matches!(
            catalog.commit_registration(&stale_registration),
            Err(CatalogError::Storage(StorageNodeError::Conflict { .. }))
        ));
        let second = catalog.prepare_registration([2; 16], 2, b_placement.clone())?;
        catalog.commit_registration(&second)?;
        // Replay the original bytes after an unrelated metadata commit.
        catalog.commit_registration(&registration)?;
        assert_eq!(catalog.snapshot()?.sequence(), 2);
        assert!(matches!(
            resolver.read_view(999, BUDGET),
            Err(RouteError::MissingPartition(999))
        ));
        assert!(matches!(
            incomplete.read_view(1, BUDGET),
            Err(RouteError::MissingReplica { group: 20, node: 3 })
        ));
        assert!(matches!(
            wrong_group.read_view(),
            Err(StorageNodeError::InvalidArgument(_))
        ));
        assert!(matches!(
            wrong_node.read_view(),
            Err(StorageNodeError::InvalidArgument(_))
        ));
        for (partition, name, placement) in [(1, "alpha", a_placement), (2, "beta", b_placement)] {
            for id in [1, 2] {
                let view = resolver.read_view(partition, BUDGET)?;
                assert_eq!(view.placement(), &placement);
                let mut vertex = Vertex::new(id);
                vertex.labels.push("person".into());
                vertex
                    .properties
                    .insert("name".into(), Value::Text(name.into()));
                let prepared = view.prepare_create_vertex(&vertex)?;
                resolver.commit_prepared(&prepared, BUDGET)?;
                resolver.commit_prepared(&prepared, BUDGET)?;
                assert_eq!(resolver.read_view(partition, BUDGET)?.sequence(), id);
            }
            let view = resolver.read_view(partition, BUDGET)?;
            assert!(view
                .prepare_create_edge(&Edge::new(9, 1, 99, "knows"))
                .is_err());
            let edge = Edge::new(9, 1, 2, "knows");
            resolver.commit_prepared(&view.prepare_create_edge(&edge)?, BUDGET)?;
            let retained = resolver.read_view(partition, BUDGET)?;
            assert_eq!(retained.get_edge(9)?, Some(edge));
            assert_eq!(
                retained
                    .execute_gremlin("g.V(1).out('knows').values('name')")?
                    .rows,
                vec![GremlinRow::Value(Value::Text(name.into()))]
            );
            assert!(retained.execute_gremlin("g.V().madeUpStep()").is_err());
            let first =
                retained.scan_page(KeyRange::all(), ScanLimit::new(1, 4096).unwrap(), None)?;
            let cursor = first.next_cursor.expect("multiple graph records");
            let historical_page = retained.scan_page(
                KeyRange::all(),
                ScanLimit::new(100, 4096).unwrap(),
                Some(&cursor),
            )?;
            let mut vertex = retained.get_vertex(2)?.unwrap();
            vertex
                .properties
                .insert("name".into(), Value::Text("updated".into()));
            let winner = retained.prepare_update_vertex(&vertex)?;
            let loser = retained.prepare_create_vertex(&Vertex::new(99))?;
            resolver.commit_prepared(&winner, BUDGET)?;
            assert!(matches!(
                resolver.commit_prepared(&loser, BUDGET),
                Err(RouteError::Storage(StorageNodeError::Conflict { .. }))
            ));
            let fresh = resolver.read_view(partition, BUDGET)?;
            assert_eq!(fresh.get_vertex(2)?, Some(vertex));
            assert!(fresh.get_vertex(99)?.is_none());
            let mut edge = fresh.get_edge(9)?.unwrap();
            edge.label = "likes".into();
            resolver.commit_prepared(&fresh.prepare_update_edge(&edge)?, BUDGET)?;
            let fresh = resolver.read_view(partition, BUDGET)?;
            assert_eq!(fresh.get_edge(9)?, Some(edge));
            assert_eq!(
                fresh.execute_gremlin("g.V(1).out('likes').count()")?.rows,
                vec![GremlinRow::Count(1)]
            );
            resolver.commit_prepared(&fresh.prepare_delete_edge(9)?, BUDGET)?;
            let fresh = resolver.read_view(partition, BUDGET)?;
            assert!(fresh.get_edge(9)?.is_none());
            resolver.commit_prepared(&fresh.prepare_delete_vertex(2)?, BUDGET)?;
            // Retained points, pages and Gremlin remain at the same old snapshot.
            assert_eq!(retained.sequence(), 3);
            assert_eq!(retained.metadata_sequence(), 2);
            assert_eq!(
                retained.get_vertex(2)?.unwrap().properties["name"],
                Value::Text(name.into())
            );
            assert_eq!(retained.get_edge(9)?.unwrap().label, "knows");
            assert_eq!(
                retained.execute_gremlin("g.V().count()")?.rows,
                vec![GremlinRow::Count(2)]
            );
            let page = retained.scan_page(
                KeyRange::all(),
                ScanLimit::new(100, 4096).unwrap(),
                Some(&cursor),
            )?;
            assert!(!page.rows.is_empty());
            assert_eq!(page.rows, historical_page.rows);
            assert!(page.next_cursor.is_none());
            let fresh = resolver.read_view(partition, BUDGET)?;
            assert_eq!(fresh.sequence(), 7);
            assert!(fresh.get_vertex(2)?.is_none());
            assert_eq!(
                fresh.get_vertex(1)?.unwrap().properties["name"],
                Value::Text(name.into())
            );
            assert_eq!(
                fresh.execute_gremlin("g.V().count()")?.rows,
                vec![GremlinRow::Count(1)]
            );
            // Original graph retry after later writes preserves exact CAS/identity.
            resolver.commit_prepared(&winner, BUDGET)?;
            assert_eq!(resolver.read_view(partition, BUDGET)?.sequence(), 7);
        }
        // Identical numeric IDs in the two partitions still retain distinct data.
        for (partition, name) in [(1, "alpha"), (2, "beta")] {
            assert_eq!(
                resolver
                    .read_view(partition, BUDGET)?
                    .get_vertex(1)?
                    .unwrap()
                    .properties["name"],
                Value::Text(name.into())
            );
        }
        Ok(())
    })
    .await??;
    Ok(())
}
