//! Independent metadata + two data OS processes; normal path, not HA evidence.
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{snapshot_identified, RemoteIdentity},
    routing::{
        experimental::{
            AdjacencyCompleteness, ExperimentalRouteError, ObservedPathElement, ObservedRow, Stage,
            StageOutcome,
        },
        RemoteReplica, RouteResolver,
    },
    run_blocking,
};
use rocksdb_sql_table::{
    experimental_graph::{CrossEdge, Direction, EdgeRef, ExperimentalError, VertexRef},
    Value, Vertex,
};
use std::{
    collections::BTreeMap,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    time::{sleep, timeout},
};
const BUDGET: Duration = Duration::from_secs(20);
// Keep the two process fixtures from competing for ephemeral-port startup.
static PROCESS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_v2_cross_edges_in_real_processes() -> anyhow::Result<()> {
    let _fixture = PROCESS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(120), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let metadata = Group::start(410, 510).await?;
    let a = Group::start(420, 520).await?;
    let mut b = Group::start(430, 530).await?;
    let catalog = Catalog::new_remote(
        metadata.replicas[0].endpoint,
        1,
        metadata.placement.clone(),
        BUDGET,
    )?;
    let resolver = RouteResolver::new_remote(
        metadata.placement.clone(),
        &metadata.replicas,
        &[a.replicas.clone(), b.replicas.clone()].concat(),
    )?;
    let ap = a.placement.clone();
    let bp = b.placement.clone();
    let (resolver, unknown_source, unknown_reverse) = run_blocking(move || -> anyhow::Result<_> {
        for (p, placement) in [(1, ap.clone()), (2, bp), (3, ap.clone())] {
            catalog.commit_registration(&catalog.prepare_registration(
                [p as u8; 16],
                p,
                placement,
            )?)?;
            let session = resolver.experimental_read_session(BUDGET)?;
            let mut v = Vertex::new(7);
            v.labels.push(format!("P{p}"));
            v.properties.insert(
                "typed".into(),
                match p {
                    1 => Value::Integer(1),
                    2 => Value::Real(1.0),
                    _ => Value::Text("1".into()),
                },
            );
            if p != 3 {
                v.properties.insert("nullable".into(), Value::Null);
            }
            v.properties
                .insert("enabled".into(), Value::Boolean(p == 2));
            let vertex = session.prepare_vertex(p, &v)?;
            resolver.experimental_commit(&vertex, BUDGET)?;
            resolver.experimental_commit(&vertex, BUDGET)?; // exact dedup
        }
        let edge = |id, from, to| CrossEdge {
            id,
            from: VertexRef {
                partition: from,
                id: 7,
            },
            to: VertexRef {
                partition: to,
                id: 7,
            },
            label: "K".into(),
            properties: BTreeMap::new(),
        };
        let mut large = Vertex::new(99);
        large.labels.push("Large".into());
        large.properties.insert(
            "blob".into(),
            rocksdb_sql_table::Value::Text("x".repeat(700_000)),
        );
        let prepared_large = resolver
            .experimental_read_session(BUDGET)?
            .prepare_vertex(1, &large)?;
        resolver.experimental_commit(&prepared_large, BUDGET)?;
        let old = resolver.experimental_read_session(BUDGET)?;
        for e in [
            edge(9, 1, 2),
            edge(10, 1, 2),
            edge(9, 2, 1),
            edge(11, 1, 3),
            edge(12, 1, 1),
        ] {
            let session = resolver.experimental_read_session(BUDGET)?;
            let prepared = session.prepare_edge(&e)?;
            resolver.experimental_commit(&prepared, BUDGET)?;
            resolver.experimental_commit(&prepared, BUDGET)?;
        }
        assert!(old
            .get_edge(EdgeRef {
                partition: 1,
                id: 9
            })?
            .is_none());
        let session = resolver.experimental_read_session(Duration::from_secs(60))?;
        assert_eq!(
            session
                .get_edge(EdgeRef {
                    partition: 1,
                    id: 9
                })?
                .unwrap()
                .to
                .partition,
            2
        );
        assert_eq!(
            session
                .get_edge(EdgeRef {
                    partition: 2,
                    id: 9
                })?
                .unwrap()
                .to
                .partition,
            1
        );
        let incoming = session.observed_adjacency(
            VertexRef {
                partition: 2,
                id: 7,
            },
            Direction::In,
        )?;
        assert_eq!(incoming.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(
            incoming.edges.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![9, 10]
        );
        assert_eq!(
            session
                .observed_adjacency(
                    VertexRef {
                        partition: 1,
                        id: 7
                    },
                    Direction::Out
                )?
                .edges
                .len(),
            4
        );
        assert_eq!(
            session
                .observed_adjacency(
                    VertexRef {
                        partition: 1,
                        id: 7
                    },
                    Direction::In
                )?
                .edges
                .len(),
            2
        );
        let vr = |partition| VertexRef { partition, id: 7 };
        let v = |p| ObservedPathElement::Vertex(vr(p));
        let e = |partition, id| ObservedPathElement::Edge(EdgeRef { partition, id });
        // Global starts ignore even an unregistered start partition, include
        // partitions sharing a shard, and sort by (local ID, partition).
        let global = session.observed_gremlin(999, "g.V()")?;
        let global_rows = vec![
            ObservedRow::Vertex(vr(1)),
            ObservedRow::Vertex(vr(2)),
            ObservedRow::Vertex(vr(3)),
            ObservedRow::Vertex(VertexRef {
                partition: 1,
                id: 99,
            }),
        ];
        assert_eq!(global.rows, global_rows);
        assert_eq!(global.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(global.snapshots, session.snapshots);
        assert_eq!(global.metadata_sequence, session.metadata_sequence);
        assert_eq!(session.snapshots.len(), 2); // three partitions, two shards
        assert_eq!(
            session.observed_gremlin(2, "g.V().limit(2)")?.rows,
            global_rows[..2]
        );
        assert_eq!(
            session.observed_gremlin(1, "g.V(99,7,99)")?.rows,
            vec![
                global_rows[3].clone(),
                global_rows[0].clone(),
                global_rows[3].clone()
            ]
        );
        assert_eq!(
            session.observed_gremlin(2, "g.V(7,7)")?.rows,
            vec![ObservedRow::Vertex(vr(2)); 2]
        );
        for (query, expected) in [
            ("g.V().count()", 4),
            ("g.V().has('typed').count()", 3),
            ("g.V().has('typed',1).count()", 1),
            ("g.V().has('typed',1.0).count()", 1),
            ("g.V().has('typed','1').count()", 1),
            ("g.V().has('typed',true).count()", 0),
            ("g.V().has('nullable').count()", 2),
            ("g.V().has('nullable',null).count()", 2),
            ("g.V().has('missing').count()", 0),
            ("g.V().has('missing',null).count()", 0),
            ("g.V().has('enabled',true).count()", 1),
            ("g.V().has('enabled',false).count()", 2),
            ("g.V().limit(1).has('enabled',true).count()", 0),
            ("g.V().has('enabled',true).limit(1).count()", 1),
            ("g.V().hasLabel('P2').limit(1).count()", 1),
            ("g.V().limit(1).hasLabel('P2').count()", 0),
            ("g.V(7).out().has('typed',1.0).count()", 2),
            ("g.V(7).out().has('typed',1).count()", 1),
            ("g.V(7).out().has('nullable').count()", 3),
            ("g.V(7).out().has('nullable',null).count()", 3),
            ("g.V(7).out().has('missing',null).count()", 0),
        ] {
            let result = session.observed_gremlin(1, query)?;
            assert_eq!(result.rows, vec![ObservedRow::Count(expected)], "{query}");
            assert_eq!(result.completeness, AdjacencyCompleteness::Unverified);
            assert_eq!(result.snapshots, session.snapshots);
            assert_eq!(result.metadata_sequence, session.metadata_sequence);
        }
        assert_eq!(
            session
                .observed_gremlin(999, "g.V().has('typed').path()")?
                .rows,
            vec![
                ObservedRow::Path(vec![v(1)]),
                ObservedRow::Path(vec![v(2)]),
                ObservedRow::Path(vec![v(3)])
            ]
        );
        assert_eq!(
            session
                .observed_gremlin(999, "g.V().has('typed',1).out().has('typed',1.0).path()")?
                .rows,
            vec![
                ObservedRow::Path(vec![v(1), e(1, 9), v(2)]),
                ObservedRow::Path(vec![v(1), e(1, 10), v(2)]),
            ]
        );
        let both = session.observed_gremlin(1, "g.V(7).both().path()")?;
        assert_eq!(both.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(both.snapshots, session.snapshots);
        assert_eq!(both.metadata_sequence, session.metadata_sequence);
        assert_eq!(
            both.rows,
            vec![
                ObservedRow::Path(vec![v(1), e(1, 9), v(2)]), // edge 9 owner 1
                ObservedRow::Path(vec![v(1), e(2, 9), v(2)]), // edge 9 owner 2
                ObservedRow::Path(vec![v(1), e(1, 10), v(2)]), // parallel edge 10
                ObservedRow::Path(vec![v(1), e(1, 11), v(3)]),
                ObservedRow::Path(vec![v(1), e(1, 12), v(1)]), // self-loop once
            ]
        );
        assert_eq!(
            session
                .observed_gremlin(1, "g.V(7).out('K').hasLabel('P2').count()")?
                .rows,
            vec![ObservedRow::Count(2)]
        );
        assert_eq!(
            session.observed_gremlin(2, "g.V(7).in().path()")?.rows,
            vec![
                ObservedRow::Path(vec![v(2), e(1, 9), v(1)]),
                ObservedRow::Path(vec![v(2), e(1, 10), v(1)])
            ]
        );
        assert_eq!(
            session
                .observed_gremlin(1, "g.V(7).out().limit(2).count()")?
                .rows,
            vec![ObservedRow::Count(2)]
        );
        assert_eq!(
            session
                .observed_gremlin(1, "g.V(7).limit(1).out().count()")?
                .rows,
            vec![ObservedRow::Count(4)]
        );
        assert_eq!(
            session.observed_gremlin(1, "g.V(999,7,7).count()")?.rows,
            vec![ObservedRow::Count(2)]
        );
        assert_eq!(
            session.observed_gremlin(1, "g.V(999).count()")?.rows,
            vec![ObservedRow::Count(0)]
        );
        assert_eq!(
            session
                .observed_gremlin(1, "g.V(7).out('Absent').count()")?
                .rows,
            vec![ObservedRow::Count(0)]
        );
        let multi = session.observed_gremlin(3, "g.V(7).in().out().path()")?;
        assert_eq!(
            multi.rows,
            vec![
                ObservedRow::Path(vec![v(3), e(1, 11), v(1), e(1, 9), v(2)]),
                ObservedRow::Path(vec![v(3), e(1, 11), v(1), e(1, 10), v(2)]),
                ObservedRow::Path(vec![v(3), e(1, 11), v(1), e(1, 11), v(3)]),
                ObservedRow::Path(vec![v(3), e(1, 11), v(1), e(1, 12), v(1)]),
            ]
        );
        assert_eq!(
            old.observed_gremlin(1, "g.V(7).both().count()")?.rows,
            vec![ObservedRow::Count(0)]
        );
        for text in [
            "g.E(9)",
            "g.V(7).id()",
            "g.V(7).label()",
            "g.V(7).values('x')",
        ] {
            assert!(matches!(
                session.observed_gremlin(1, text),
                Err(ExperimentalRouteError::Unsupported(_))
            ));
        }
        assert!(matches!(
            session.observed_gremlin(1, "g.V(7).repeat(out())"),
            Err(ExperimentalRouteError::Query(
                rocksdb_sql_table::Error::UnsupportedGremlinStep(_)
            ))
        ));
        assert!(session
            .observed_gremlin(1, "g.V(7).out().out().out().out()")
            .is_err());
        assert!(session.observed_gremlin(1, "g.V(7).limit(0)").is_err());
        for oversized in [
            format!("g.V({}).limit(1).out().count()", vec!["7"; 8000].join(",")),
            format!("g.V({}7{})", "[".repeat(1000), "]".repeat(1000)),
        ] {
            assert!(oversized.len() < 16 * 1024);
            assert!(matches!(
                session.observed_gremlin(1, &oversized),
                Err(ExperimentalRouteError::Query(
                    rocksdb_sql_table::Error::GremlinComplexityLimit { .. }
                ))
            ));
        }
        let repeated = format!("g.V({}).both()", vec!["7"; 30].join(","));
        assert_eq!(session.observed_gremlin(1, &repeated)?.rows.len(), 100);
        assert_eq!(
            session
                .observed_gremlin(1, &(repeated.clone() + ".count()"))?
                .rows,
            vec![ObservedRow::Count(150)]
        );
        assert_eq!(
            session
                .observed_gremlin(1, &(repeated + ".limit(150)"))?
                .rows
                .len(),
            150
        );
        let repeated_large = format!(
            "g.V({}).hasLabel('Large').count()",
            vec!["99"; 50].join(",")
        );
        assert!(matches!(
            session.observed_gremlin(1, &repeated_large),
            Err(ExperimentalRouteError::Query(
                rocksdb_sql_table::Error::TraversalLimitExceeded(_)
            ))
        ));
        // Filters and terminal output caps cannot refund preceding acquisition.
        for suffix in [".has('missing').count()", ".has('blob').limit(1)"] {
            let query = format!("g.V({}){suffix}", vec!["99"; 50].join(","));
            assert!(
                matches!(
                    session.observed_gremlin(1, &query),
                    Err(ExperimentalRouteError::Query(
                        rocksdb_sql_table::Error::TraversalLimitExceeded(_)
                    ))
                ),
                "{query}"
            );
        }
        let leading = format!(
            "g.V({}).limit(1).hasLabel('Large').count()",
            vec!["99"; 50].join(",")
        );
        assert_eq!(
            session.observed_gremlin(1, &leading)?.rows,
            vec![ObservedRow::Count(1)]
        );
        let many = format!(
            "g.V({}).both().both().both().count()",
            vec!["7"; 50].join(",")
        );
        assert_eq!(
            session.observed_gremlin(1, &many)?.rows,
            vec![ObservedRow::Count(3250)]
        );
        assert!(matches!(
            session.prepare_edge(&edge(9, 1, 2)),
            Err(ExperimentalRouteError::Graph(
                ExperimentalError::AlreadyExists
            ))
        ));
        let mut missing = edge(13, 1, 2);
        missing.to.id = 999;
        assert!(matches!(
            session.prepare_edge(&missing),
            Err(ExperimentalRouteError::Graph(
                ExperimentalError::MissingEndpoint(_)
            ))
        ));
        // v1 remains isolated, cannot see or mutate v2 vertices/edges.
        assert!(resolver.read_view(1, BUDGET)?.get_vertex(7)?.is_none());
        assert!(resolver.read_view(1, BUDGET)?.get_edge(9)?.is_none());
        let prepared = session.prepare_edge(&edge(20, 1, 2))?;
        let target_write = session.prepare_vertex(2, &Vertex::new(8))?;
        resolver.experimental_commit(&target_write, BUDGET)?;
        match resolver.experimental_commit(&prepared, BUDGET) {
            Err(ExperimentalRouteError::Stage {
                stage: Stage::Reverse,
                outcome: StageOutcome::Incomplete,
                source_acknowledged_this_attempt: true,
                request_id,
                ..
            }) => assert_eq!(Some(request_id), prepared.reverse_request_id()),
            other => panic!("expected honest incomplete outcome: {other:?}"),
        }
        // Catalog and data acquisition stay frozen independently: this new
        // same-shard partition and its low-ID vertex must not leak into session.
        catalog.commit_registration(&catalog.prepare_registration([4; 16], 4, ap)?)?;
        let fresh = resolver.experimental_read_session(BUDGET)?;
        let new_partition_vertex = fresh.prepare_vertex(4, &Vertex::new(2))?;
        resolver.experimental_commit(&new_partition_vertex, BUDGET)?;
        drop(fresh);
        // Source remains; no claim of abort. Index-only incoming cannot detect it.
        let current = resolver.experimental_read_session(BUDGET)?;
        let pinned_global = session.observed_gremlin(4, "g.V()")?;
        assert_eq!(pinned_global.rows, global_rows);
        assert_eq!(pinned_global.snapshots, session.snapshots);
        assert_eq!(pinned_global.metadata_sequence, session.metadata_sequence);
        assert_eq!(
            pinned_global.completeness,
            AdjacencyCompleteness::Unverified
        );
        let fresh_global = current.observed_gremlin(999, "g.V()")?;
        assert_eq!(
            fresh_global.rows,
            vec![
                ObservedRow::Vertex(VertexRef {
                    partition: 4,
                    id: 2
                }),
                ObservedRow::Vertex(vr(1)),
                ObservedRow::Vertex(vr(2)),
                ObservedRow::Vertex(vr(3)),
                ObservedRow::Vertex(VertexRef {
                    partition: 2,
                    id: 8
                }),
                ObservedRow::Vertex(VertexRef {
                    partition: 1,
                    id: 99
                }),
            ]
        );
        assert_eq!(fresh_global.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(fresh_global.snapshots, current.snapshots);
        assert_eq!(fresh_global.metadata_sequence, current.metadata_sequence);
        assert_ne!(fresh_global.metadata_sequence, session.metadata_sequence);
        assert_ne!(fresh_global.snapshots, session.snapshots);
        assert_eq!(current.snapshots.len(), 2);

        assert!(matches!(
            current.get_edge(EdgeRef {
                partition: 1,
                id: 20
            }),
            Err(ExperimentalRouteError::Graph(
                ExperimentalError::Incomplete(_)
            ))
        ));
        assert_eq!(
            current
                .observed_adjacency(
                    VertexRef {
                        partition: 2,
                        id: 7
                    },
                    Direction::In
                )?
                .edges
                .len(),
            2
        );
        assert!(current
            .observed_adjacency(
                VertexRef {
                    partition: 1,
                    id: 7
                },
                Direction::Out
            )
            .is_err());
        // Unknown completeness propagates even into scalar counts; visible
        // incompleteness is an error, not a partial vector or zero count.
        let observed = current.observed_gremlin(2, "g.V(7).in().count()")?;
        assert_eq!(observed.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(observed.rows, vec![ObservedRow::Count(2)]);
        assert!(matches!(
            current.observed_gremlin(1, "g.V(7).out().count()"),
            Err(ExperimentalRouteError::Graph(
                ExperimentalError::Incomplete(_)
            ))
        ));
        assert_eq!(
            session.observed_gremlin(1, "g.V(7).out().count()")?.rows,
            vec![ObservedRow::Count(4)]
        ); // original frozen views, never refresh
           // Exact retry retains the original rejected target sequence, no implicit repair.
        assert!(matches!(
            resolver.experimental_commit(&prepared, BUDGET),
            Err(ExperimentalRouteError::Stage {
                outcome: StageOutcome::Incomplete,
                ..
            })
        ));
        let unknown_source = current.prepare_edge(&edge(30, 2, 1))?;
        let unknown_reverse = current.prepare_edge(&edge(31, 1, 2))?;
        Ok((resolver, unknown_source, unknown_reverse))
    })
    .await??;
    // Stop one participant after preparation. Both source- and reverse-stage
    // unknown outcomes must be honest; this is not an ACK-loss simulation.
    b.process.0.kill()?;
    b.process.0.wait()?;
    run_blocking(move || -> anyhow::Result<()> {
        assert!(matches!(
            resolver.experimental_commit(&unknown_reverse, BUDGET),
            Err(ExperimentalRouteError::Stage {
                stage: Stage::Reverse,
                outcome: StageOutcome::Unknown,
                source_acknowledged_this_attempt: true,
                ..
            })
        ));
        assert!(matches!(
            resolver.experimental_commit(&unknown_source, BUDGET),
            Err(ExperimentalRouteError::Stage {
                stage: Stage::Source,
                outcome: StageOutcome::Unknown,
                source_acknowledged_this_attempt: false,
                ..
            })
        ));
        Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_registry_is_rejected_before_any_write() -> anyhow::Result<()> {
    let _fixture = PROCESS_TEST_LOCK.lock().await;
    let metadata = Group::start(410, 510).await?;
    let a = Group::start(420, 520).await?;
    let b = Group::start(430, 530).await?;
    let catalog = Catalog::new_remote(
        metadata.replicas[0].endpoint,
        1,
        metadata.placement.clone(),
        BUDGET,
    )?;
    let resolver = RouteResolver::new_remote(
        metadata.placement.clone(),
        &metadata.replicas,
        &[a.replicas.clone(), b.replicas.clone()].concat(),
    )?;
    let partial =
        RouteResolver::new_remote(metadata.placement.clone(), &metadata.replicas, &a.replicas)?;
    let ap = a.placement.clone();
    let bp = b.placement.clone();
    run_blocking(move || -> anyhow::Result<_> {
        for (p, placement) in [(1, ap.clone()), (2, bp), (3, ap)] {
            catalog.commit_registration(&catalog.prepare_registration(
                [p as u8; 16],
                p,
                placement,
            )?)?;
            let session = resolver.experimental_read_session(BUDGET)?;
            let mut v = Vertex::new(7);
            v.labels.push(format!("P{p}"));
            let vertex = session.prepare_vertex(p, &v)?;
            resolver.experimental_commit(&vertex, BUDGET)?;
            resolver.experimental_commit(&vertex, BUDGET)?; // exact dedup
        }
        let edge = |id, from, to| CrossEdge {
            id,
            from: VertexRef {
                partition: from,
                id: 7,
            },
            to: VertexRef {
                partition: to,
                id: 7,
            },
            label: "K".into(),
            properties: BTreeMap::new(),
        };
        let session = resolver.experimental_read_session(BUDGET)?;
        let prepared = session.prepare_edge(&edge(40, 1, 2))?;
        let result = partial.experimental_commit(&prepared, BUDGET);
        assert!(
            matches!(result, Err(ExperimentalRouteError::Route(_))),
            "{result:?}"
        );
        let fresh = resolver.experimental_read_session(BUDGET)?;
        assert!(fresh
            .get_edge(EdgeRef {
                partition: 1,
                id: 40
            })?
            .is_none());
        Ok(())
    })
    .await??;
    Ok(())
}
