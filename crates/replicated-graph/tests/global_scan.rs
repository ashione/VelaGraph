//! Real TCP metadata + two data processes. Single voters deliberately isolate
//! global query/routing semantics; this is not replication or failover evidence.
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{snapshot_identified, RemoteIdentity},
    routing::{RemoteReplica, RouteError, RouteResolver, ScanScope},
    run_blocking, RemoteStorageNode,
};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    time::{sleep, timeout},
};
use velagraph::{
    Edge, Error, GlobalScanOutput, GremlinRow, QualifiedGremlinRow, StorageNode, Value, Vertex,
    WriteOp, WriteRequest,
};

const BUDGET: Duration = Duration::from_secs(20);
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Group {
    process: Process,
    _dir: tempfile::TempDir,
    placement: Placement,
    replicas: Vec<RemoteReplica>,
}
impl Group {
    async fn start(group: u64, shard: u64) -> anyhow::Result<Self> {
        let raft = TcpListener::bind("127.0.0.1:0").await?;
        let control = TcpListener::bind("127.0.0.1:0").await?;
        let graph = TcpListener::bind("127.0.0.1:0").await?;
        let raft_addr = raft.local_addr()?;
        let control_addr = control.local_addr()?;
        let endpoint = graph.local_addr()?;
        let dir = tempfile::tempdir()?;
        drop((raft, control, graph));
        let process = Process(
            Command::new(env!("CARGO_BIN_EXE_graph-node"))
                .env("TOKIO_WORKER_THREADS", "2")
                .args([
                    "--allow-insecure-development",
                    "--group",
                    &group.to_string(),
                    "--id",
                    "1",
                    "--raft",
                    &raft_addr.to_string(),
                    "--control",
                    &control_addr.to_string(),
                    "--graph",
                    &endpoint.to_string(),
                    "--shard",
                    &shard.to_string(),
                    "--epoch",
                    "1",
                    "--data-dir",
                    dir.path().to_str().unwrap(),
                    "--bootstrap",
                    &format!("1={raft_addr}"),
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        timeout(Duration::from_secs(30), async {
            loop {
                if snapshot_identified(
                    endpoint,
                    RemoteIdentity { group, node: 1 },
                    shard,
                    Duration::from_millis(300),
                )
                .await
                .is_ok()
                {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await?;
        Ok(Self {
            process,
            _dir: dir,
            placement: Placement {
                shard,
                group,
                epoch: 1,
                replicas: vec![1],
            },
            replicas: vec![RemoteReplica {
                group,
                node: 1,
                endpoint,
            }],
        })
    }
}

fn rows(pairs: &[(u32, u64)], edge: bool) -> GlobalScanOutput {
    GlobalScanOutput::Rows(
        pairs
            .iter()
            .map(|&(partition, id)| QualifiedGremlinRow {
                partition,
                row: if edge {
                    GremlinRow::Edge(id)
                } else {
                    GremlinRow::Vertex(id)
                },
            })
            .collect(),
    )
}

// Bare vertex fixture using the frozen GRAPH_FORMAT.md v1 key/protobuf format.
// Bulk loading avoids thousands of RPCs and does not implement a new codec API.
fn bare_vertex(partition: u32, id: u64) -> WriteOp {
    let mut key = b"graph/v1/p/".to_vec();
    key.extend_from_slice(&partition.to_be_bytes());
    key.extend_from_slice(b"/vertex/");
    key.extend_from_slice(&id.to_be_bytes());
    let mut payload = vec![0x08];
    let mut value = id;
    while value >= 128 {
        payload.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    payload.push(value as u8);
    let mut record = vec![0x08, 1, 0x10, 1, 0x1a, payload.len() as u8];
    record.extend(payload);
    WriteOp::put(key, record)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_scan_merges_real_remote_shards_and_fails_closed() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), scenario()).await?
}

async fn scenario() -> anyhow::Result<()> {
    let metadata = Group::start(10, 110).await?;
    let a = Group::start(20, 120).await?;
    let mut b = Group::start(30, 130).await?;
    let catalog = Catalog::new_remote(
        metadata.replicas[0].endpoint,
        1,
        metadata.placement.clone(),
        BUDGET,
    )?;
    let data = [a.replicas.clone(), b.replicas.clone()].concat();
    let resolver =
        RouteResolver::new_remote(metadata.placement.clone(), &metadata.replicas, &data)?;
    let a_placement = a.placement.clone();
    let b_placement = b.placement.clone();
    let a_node = RemoteStorageNode::new_identified(a.replicas[0].endpoint, 20, 1, 120, BUDGET)?;
    let b_node = RemoteStorageNode::new_identified(b.replicas[0].endpoint, 30, 1, 130, BUDGET)?;
    let resolver = run_blocking(move || -> anyhow::Result<RouteResolver> {
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(0)
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::Selected(vec![]), "g.E().count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(0)
        );
        for scope in [
            ScanScope::Selected(vec![1, 1]),
            ScanScope::Selected((0..33).collect()),
        ] {
            assert!(resolver
                .execute_global_scan(scope, "g.V()", BUDGET)
                .is_err());
        }
        for timeout in [
            Duration::ZERO,
            Duration::from_secs(61),
            Duration::from_nanos(1),
        ] {
            assert!(resolver
                .execute_global_scan(ScanScope::All, "g.V()", timeout)
                .is_err());
        }
        for (partition, placement) in [(1, a_placement.clone()), (2, b_placement), (3, a_placement)]
        {
            let mut request_id = [0; 16];
            request_id[0] = partition as u8;
            catalog.commit_registration(
                &catalog.prepare_registration(request_id, partition, placement)?,
            )?;
            for id in [2, 1] {
                let mut vertex = Vertex::new(id);
                vertex.labels.push("person".into());
                vertex
                    .properties
                    .insert("partition".into(), Value::Integer(partition.into()));
                let view = resolver.read_view(partition, BUDGET)?;
                resolver.commit_prepared(&view.prepare_create_vertex(&vertex)?, BUDGET)?;
            }
            let view = resolver.read_view(partition, BUDGET)?;
            resolver.commit_prepared(
                &view.prepare_create_edge(&Edge::new(7, 1, 2, "knows"))?,
                BUDGET,
            )?;
        }
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.E().count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(3)
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.E().limit(2).count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(2)
        );
        let expected = rows(&[(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (3, 2)], false);
        for scope in [ScanScope::All, ScanScope::Selected(vec![3, 2, 1])] {
            let result = resolver.execute_global_scan(scope, "g.V()", BUDGET)?;
            assert_eq!(result.metadata_sequence, 3);
            assert_eq!(result.result, expected);
            // Partitions 1 and 3 share precisely one retained shard snapshot.
            assert_eq!(result.snapshots.len(), 2);
            assert_eq!(
                result
                    .snapshots
                    .iter()
                    .map(|s| (s.group, s.shard, s.epoch, s.sequence))
                    .collect::<Vec<_>>(),
                vec![(20, 120, 1, 6), (30, 130, 1, 3)]
            );
        }
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.E()", BUDGET)?
                .result,
            rows(&[(1, 7), (2, 7), (3, 7)], true)
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().limit(2)", BUDGET)?
                .result,
            rows(&[(1, 1), (2, 1)], false)
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().limit(2).count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(2)
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(6)
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::Selected(vec![2]), "g.V().count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(2)
        );
        assert!(matches!(
            resolver.execute_global_scan(
                ScanScope::Selected(vec![1, 999]),
                "g.V().limit(1)",
                BUDGET
            ),
            Err(RouteError::MissingPartition(999))
        ));
        for query in [
            "g.V(1)",
            "g.V().out()",
            "g.V().id()",
            "g.V().path()",
            "g.V().madeUpStep()",
        ] {
            assert!(
                resolver
                    .execute_global_scan(ScanScope::All, query, BUDGET)
                    .is_err(),
                "{query}"
            );
        }
        // The same local ID has different properties in each partition.
        for (query, expected) in [
            (
                "g.V().hasLabel('person').has('partition',2)",
                rows(&[(2, 1), (2, 2)], false),
            ),
            ("g.V().limit(2).has('partition',2)", rows(&[(2, 1)], false)),
            (
                "g.V().has('partition',2).limit(2)",
                rows(&[(2, 1), (2, 2)], false),
            ),
            ("g.V().has('partition','2')", rows(&[], false)),
            ("g.V().has('missing').limit(1)", rows(&[], false)),
            (
                "g.E().hasLabel('knows').limit(2)",
                rows(&[(1, 7), (2, 7)], true),
            ),
        ] {
            assert_eq!(
                resolver
                    .execute_global_scan(ScanScope::Selected(vec![3, 2, 1]), query, BUDGET)?
                    .result,
                expected,
                "{query}"
            );
        }
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().values('partition').limit(2)", BUDGET)?
                .result,
            GlobalScanOutput::Rows(vec![
                QualifiedGremlinRow {
                    partition: 1,
                    row: GremlinRow::Value(Value::Integer(1))
                },
                QualifiedGremlinRow {
                    partition: 2,
                    row: GremlinRow::Value(Value::Integer(2))
                },
            ])
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.E().label().limit(1)", BUDGET)?
                .result,
            GlobalScanOutput::Rows(vec![QualifiedGremlinRow {
                partition: 1,
                row: GremlinRow::Value(Value::Text("knows".into()))
            }])
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().values('missing').count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(0)
        );
        // 120 candidates demonstrate that the implicit 100 is global, not 100
        // per shard. Existing IDs 1 and 2 are replaced with bare v1 vertices.
        for (node, shard, partitions) in [(&a_node, 120, vec![1, 3]), (&b_node, 130, vec![2])] {
            node.batch_write(WriteRequest {
                shard,
                epoch: 1,
                request_id: [40; 16],
                operations: partitions
                    .iter()
                    .flat_map(|&p| (1..=40).map(move |id| bare_vertex(p, id)))
                    .collect(),
            })?;
        }
        let result = resolver.execute_global_scan(ScanScope::All, "g.V()", BUDGET)?;
        let GlobalScanOutput::Rows(default_rows) = result.result else {
            panic!("expected rows");
        };
        assert_eq!(default_rows.len(), 100);
        assert_eq!(
            default_rows[99],
            QualifiedGremlinRow {
                partition: 1,
                row: GremlinRow::Vertex(34)
            }
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(120)
        );
        for partition in [1, 2] {
            for id in 121..=240 {
                let mut vertex = Vertex::new(id);
                vertex.labels.push("late".into());
                vertex
                    .properties
                    .insert("x".into(), Value::Integer(id as i64));
                let view = resolver.read_view(partition, BUDGET)?;
                resolver.commit_prepared(&view.prepare_create_vertex(&vertex)?, BUDGET)?;
            }
        }
        let projected = resolver.execute_global_scan(
            ScanScope::All,
            "g.V().hasLabel('late').values('x')",
            BUDGET,
        )?;
        let GlobalScanOutput::Rows(projected) = projected.result else {
            panic!("rows")
        };
        assert_eq!(projected.len(), 100);
        assert_eq!(
            projected[0],
            QualifiedGremlinRow {
                partition: 1,
                row: GremlinRow::Value(Value::Integer(121))
            }
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().has('x').count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(240)
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().values('x').limit(1)", BUDGET)?
                .result,
            GlobalScanOutput::Rows(vec![QualifiedGremlinRow {
                partition: 1,
                row: GremlinRow::Value(Value::Integer(121))
            }])
        );
        // Each partition remains below 10k, but their merged candidates exceed it.
        for (node, shard, partition) in [(&a_node, 120, 1), (&b_node, 130, 2)] {
            node.batch_write(WriteRequest {
                shard,
                epoch: 1,
                request_id: [50; 16],
                operations: (41..=5000).map(|id| bare_vertex(partition, id)).collect(),
            })?;
        }
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::Selected(vec![1]), "g.V().count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(5000)
        );
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::Selected(vec![2, 1]), "g.V().count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(10_000)
        );
        for query in [
            "g.V()",
            "g.V().count()",
            "g.V().has('missing').limit(1)",
            "g.V().values('missing').count()",
        ] {
            assert!(
                matches!(
                    resolver.execute_global_scan(ScanScope::All, query, BUDGET),
                    Err(RouteError::Graph(Error::TraversalLimitExceeded(_)))
                ),
                "{query}"
            );
        }
        assert_eq!(
            resolver
                .execute_global_scan(ScanScope::All, "g.V().limit(2).count()", BUDGET)?
                .result,
            GlobalScanOutput::Count(2)
        );
        Ok(resolver)
    })
    .await??;
    b.process.0.kill()?;
    b.process.0.wait()?;
    run_blocking(move || -> anyhow::Result<()> {
        // Even a leading limit must first acquire every selected shard; a
        // healthy first partition cannot conceal a later unavailable shard.
        for query in ["g.V().limit(1)", "g.E().count()"] {
            assert!(
                matches!(
                    resolver.execute_global_scan(ScanScope::All, query, BUDGET),
                    Err(RouteError::Storage(_))
                ),
                "{query}"
            );
        }
        assert_eq!(
            resolver
                .execute_global_scan(
                    ScanScope::Selected(vec![3]),
                    "g.V().limit(1).count()",
                    BUDGET
                )?
                .result,
            GlobalScanOutput::Count(1)
        );
        Ok(())
    })
    .await??;
    Ok(())
}
