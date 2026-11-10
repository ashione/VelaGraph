//! Independent metadata + two data OS processes; normal path, not HA evidence.
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{snapshot_identified, RemoteIdentity},
    routing::{
        experimental::{
            AdjacencyCompleteness, EdgeMutationKind, ExperimentalReadSession,
            ExperimentalRouteError, MutationAcknowledgement, ObservedElementRef,
            ObservedPathElement, ObservedRow, ObservedRowV2, ObservedRowV3, Stage, StageOutcome,
        },
        RemoteReplica, RouteError, RouteResolver,
    },
    run_blocking,
};
use rocksdb_sql_table::{
    experimental_graph::{
        CrossEdge, Direction, EdgePropertyPatch, EdgeRef, ExperimentalError, VertexPropertyPatch,
        VertexRef,
    },
    StorageNode, StorageNodeError, Value, Vertex, WriteOp, WriteRequest,
};
use std::{
    collections::{BTreeMap, BTreeSet},
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
        let scalar = |partition, value| ObservedRowV3::Value {
            source: ObservedElementRef::Vertex(vr(partition)), value,
        };
        for (text, expected) in [
            ("g.V(7).out('K').values('typed').is(1.0)", vec![scalar(2, Value::Real(1.0)); 2]),
            ("g.V(7).out('K').values('typed').is(1)", vec![scalar(1, Value::Integer(1))]),
            ("g.V(7).out('K').values('typed').is('1')", vec![scalar(3, Value::Text("1".into()))]),
            ("g.V(7).out('K').values('nullable').is(null)", vec![scalar(2, Value::Null), scalar(2, Value::Null), scalar(1, Value::Null)]),
            ("g.V(7).out('K').id().is(7)", vec![scalar(2, Value::Integer(7)), scalar(2, Value::Integer(7)), scalar(3, Value::Integer(7)), scalar(1, Value::Integer(7))]),
            ("g.V(7).out('K').id().is(7).dedup().is(7).count()", vec![ObservedRowV3::Count(1)]),
            ("g.V(7).out('K').values('typed').limit(1).is(1).count()", vec![ObservedRowV3::Count(0)]),
            ("g.V(7).out('K').values('typed').is(1).limit(1).count()", vec![ObservedRowV3::Count(1)]),
            ("g.V(999).values('missing').is(null).count()", vec![ObservedRowV3::Count(0)]),
        ] {
            let result = session.observed_gremlin_v3(1, text)?;
            assert_eq!(result.rows, expected, "{text}");
            assert_eq!(result.snapshots, session.snapshots);
            assert_eq!(result.metadata_sequence, session.metadata_sequence);
            assert_eq!(result.completeness, AdjacencyCompleteness::Unverified);
        }
        for text in ["g.V(999).is(7)", "g.E(999).is(7)", "g.V(999).id().is(7).out()", "g.V(999).label().is('P1').path()", "g.E(999).id().is(7).inV()", "g.V(999).id().is(eq(7))", "g.V(999).id().is(gt(7))"] {
            assert!(session.observed_gremlin_v3(1, text).is_err(), "accepted {text}");
        }
        let filter = "g.V(7).id().is(7)";
        assert!(session.observed_gremlin(1, filter).is_err());
        assert!(session.observed_gremlin_v2(1, filter).is_err());
        // Independent explicit oracle: parallel edges 9/10, a cross-shard
        // cycle, and self-loop 12. Emission is level-major, never DFS order.
        let first_level = vec![
            vec![v(1), e(1, 9), v(2)],
            vec![v(1), e(1, 10), v(2)],
            vec![v(1), e(1, 11), v(3)],
            vec![v(1), e(1, 12), v(1)],
        ];
        let second_level = vec![
            vec![v(1), e(1, 9), v(2), e(2, 9), v(1)],
            vec![v(1), e(1, 10), v(2), e(2, 9), v(1)],
            vec![v(1), e(1, 12), v(1), e(1, 9), v(2)],
            vec![v(1), e(1, 12), v(1), e(1, 10), v(2)],
            vec![v(1), e(1, 12), v(1), e(1, 11), v(3)],
            vec![v(1), e(1, 12), v(1), e(1, 12), v(1)],
        ];
        for (text, expected) in [
            (
                "g.V(7).repeat(out('K')).times(2).path()",
                second_level.clone(),
            ),
            (
                "g.V(7).repeat(out('K')).times(2).emit().path()",
                [first_level.clone(), second_level.clone()].concat(),
            ),
            (
                "g.V(7,7).repeat(out('K')).emit().times(2).path()",
                [
                    first_level.clone(),
                    first_level,
                    second_level.clone(),
                    second_level,
                ]
                .concat(),
            ),
        ] {
            let result = session.observed_gremlin_v3(1, text)?;
            assert_eq!(
                result.rows,
                expected
                    .into_iter()
                    .map(ObservedRowV3::Path)
                    .collect::<Vec<_>>(),
                "{text}"
            );
            assert_eq!(result.snapshots, session.snapshots);
            assert_eq!(result.metadata_sequence, session.metadata_sequence);
            assert_eq!(result.completeness, AdjacencyCompleteness::Unverified);
        }
        for (text, partitions) in [
            ("g.V(7,7).valueMap('typed','nullable','missing')", vec![1, 1]),
            ("g.V(7).out('K').valueMap('typed','nullable','missing')", vec![2, 2, 3, 1]),
            ("g.V(7).out('K').dedup().valueMap('typed','nullable','missing')", vec![2, 3, 1]),
            ("g.E(9).bothV().valueMap('typed','nullable','missing')", vec![1, 2]),
            ("g.V(7).repeat(out('K')).times(2).emit().dedup().valueMap('typed','nullable','missing')", vec![2, 3, 1]),
        ] {
            let expected = partitions.into_iter().map(|partition| {
                let mut properties = BTreeMap::from([("typed".into(), vec![match partition { 1 => Value::Integer(1), 2 => Value::Real(1.0), _ => Value::Text("1".into()) }])]);
                if partition != 3 { properties.insert("nullable".into(), vec![Value::Null]); }
                ObservedRowV3::PropertyMap { source: vr(partition), properties }
            }).collect::<Vec<_>>();
            let result = session.observed_gremlin_v3(1, text)?;
            assert_eq!(result.rows, expected, "{text}");
            assert_eq!(result.snapshots, session.snapshots);
            assert_eq!(result.completeness, AdjacencyCompleteness::Unverified);
        }
        assert!(session.observed_gremlin(1, "g.V(7).valueMap('typed')").is_err());
        assert!(session.observed_gremlin_v2(1, "g.V(7).valueMap('typed')").is_err());
        assert!(matches!(session.observed_gremlin_v3(1, "g.V(99).valueMap('blob')"), Err(ExperimentalRouteError::Query(rocksdb_sql_table::Error::TraversalLimitExceeded(_)))));
        for (text, partitions) in [
            ("g.V(7).repeat(in('K')).times(2)", vec![1, 1, 2, 1]),
            (
                "g.V(7).out('K').repeat(in('K')).times(1)",
                vec![1, 1, 1, 1, 1, 2, 1],
            ),
            (
                "g.V(7).repeat(in('K')).times(1).out('K')",
                vec![1, 2, 2, 3, 1],
            ),
            (
                "g.V(7).repeat(out('K')).times(2).emit().dedup()",
                vec![2, 3, 1],
            ),
            (
                "g.V(7).repeat(out('K')).times(2).emit().hasLabel('P2').limit(3)",
                vec![2, 2, 2],
            ),
            ("g.E(9).inV().repeat(out('K')).times(2)", vec![2, 2, 3, 1]),
            ("g.V(999).repeat(out()).times(32)", vec![]),
        ] {
            assert_eq!(
                session.observed_gremlin_v3(1, text)?.rows,
                partitions
                    .into_iter()
                    .map(|p| ObservedRowV3::Vertex(vr(p)))
                    .collect::<Vec<_>>(),
                "{text}"
            );
        }
        assert_eq!(
            session
                .observed_gremlin_v3(
                    1,
                    "g.V(7).repeat(out('K')).times(2).emit().id().dedup().count()"
                )?
                .rows,
            vec![ObservedRowV3::Count(1)]
        );
        assert_eq!(
            session
                .observed_gremlin_v3(1, "g.V(7).repeat(out('absent')).times(32).emit().count()")?
                .rows,
            vec![ObservedRowV3::Count(0)]
        );
        for text in [
            "g.E(999).repeat(out()).times(1)",
            "g.V(999).values('missing').repeat(out()).times(1)",
            "g.V(999).repeat(both()).times(1)",
            "g.V(999).repeat(out().out()).times(1)",
            "g.V(999).repeat(out()).times(32).out()",
            "g.V(999).out().repeat(in()).times(32)",
            "g.V(999).repeat(out()).times(33)",
        ] {
            assert!(
                session.observed_gremlin_v3(1, text).is_err(),
                "accepted {text}"
            );
        }
        // New admission is confined to v3, not the legacy observed APIs.
        let repeat = "g.V(7).repeat(out('K')).times(2)";
        assert!(session.observed_gremlin(1, repeat).is_err());
        assert!(session.observed_gremlin_v2(1, repeat).is_err());
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
        for suffix in [".count()", ".dedup().limit(1)", ".has('missing').count()", ".id().is(-1).count()"] {
            let text = format!("g.V(7).repeat(out('K')).times(2){suffix}");
            assert!(matches!(
                current.observed_gremlin_v3(1, &text),
                Err(ExperimentalRouteError::Graph(
                    ExperimentalError::Incomplete(_)
                ))
            ));
        }
        assert_eq!(
            session
                .observed_gremlin_v3(1, "g.V(7).repeat(out('K')).times(2).count()")?
                .rows,
            vec![ObservedRowV3::Count(6)]
        );
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
        // Range never changes upstream validation, even if it returns no rows.
        assert!(matches!(current.observed_gremlin_v3(1, "g.V(7).out().range(0,0)"), Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(_)))));
        for (text, expected) in [
            ("g.V(7).out('K').range(1,3)", vec![ObservedRowV3::Vertex(vr(2)), ObservedRowV3::Vertex(vr(3))]),
            ("g.V(7).out('K').range(0,2).dedup()", vec![ObservedRowV3::Vertex(vr(2))]),
            ("g.V(7).out('K').dedup().range(0,2)", vec![ObservedRowV3::Vertex(vr(2)), ObservedRowV3::Vertex(vr(3))]),
            ("g.V(7).out('K').values('typed').is(1.0).range(1,2)", vec![scalar(2, Value::Real(1.0))]),
            ("g.V(7).out('K').range(1,2).values('typed').is(1).count()", vec![ObservedRowV3::Count(0)]),
            ("g.V(7).out('K').range(2,2)", vec![]),
            ("g.V(7).out('K').range(9,10)", vec![]),
            ("g.V(7).repeat(out('K')).times(2).range(1,3).count()", vec![ObservedRowV3::Count(2)]),
        ] {
            let result = session.observed_gremlin_v3(1, text)?;
            assert_eq!(result.rows, expected, "{text}");
            assert_eq!(result.snapshots, session.snapshots);
            assert_eq!(result.metadata_sequence, session.metadata_sequence);
        }
        for text in ["g.V(7).range(0,1)", "g.V(999).range(0,0)"] {
            assert!(session.observed_gremlin(1, text).is_err());
            assert!(session.observed_gremlin_v2(1, text).is_err());
        }
        // Run new expensive resource probes only after every pre-existing
        // retained-view assertion. Their work must not consume those leases.
        // One full-width resource case is sufficient at this transport layer;
        // deterministic unit tests separately cover all round/emission charges.
        {
            let text = "g.V(7).repeat(out('K')).times(32).dedup().limit(1)";
            assert!(
                matches!(
                    session.observed_gremlin_v3(1, text),
                    Err(ExperimentalRouteError::Query(
                        rocksdb_sql_table::Error::TraversalLimitExceeded(_)
                    ))
                ),
                "{text}"
            );
        }
        let byte_heavy = format!(
            "g.V({}).has('blob').repeat(out('absent')).times(1).count()",
            vec!["99"; 48].join(",")
        );
        let result = session.observed_gremlin_v3(1, &byte_heavy);
        assert!(
            matches!(
                &result,
                Err(ExperimentalRouteError::Query(
                    rocksdb_sql_table::Error::TraversalLimitExceeded(_)
                ))
            ),
            "{result:?}"
        );
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

// Value::PartialEq treats signed zeros as equal, so check real payloads by bits.
fn assert_property_edge_exact(actual: &CrossEdge, expected: &CrossEdge) {
    assert_eq!(actual.identity(), expected.identity());
    assert_eq!(actual.from, expected.from);
    assert_eq!(actual.to, expected.to);
    assert_eq!(actual.label, expected.label);
    assert_eq!(actual.properties.len(), expected.properties.len());
    for (key, expected_value) in &expected.properties {
        let actual_value = actual.properties.get(key).expect("property retained");
        match (actual_value, expected_value) {
            (Value::Real(actual), Value::Real(expected)) => {
                assert_eq!(actual.to_bits(), expected.to_bits(), "{key}");
            }
            _ => assert_eq!(actual_value, expected_value, "{key}"),
        }
    }
}

fn assert_property_edge_complete(
    session: &ExperimentalReadSession,
    expected: &CrossEdge,
) -> anyhow::Result<()> {
    assert_property_edge_exact(&session.get_edge(expected.identity())?.unwrap(), expected);
    // Independently traverse both indexes, including the self-loop's two roles.
    for (endpoint, direction) in [
        (expected.from, Direction::Out),
        (expected.to, Direction::In),
    ] {
        let adjacency = session.observed_adjacency(endpoint, direction)?;
        assert_eq!(adjacency.completeness, AdjacencyCompleteness::Unverified);
        let matches: Vec<_> = adjacency
            .edges
            .iter()
            .filter(|edge| edge.identity() == expected.identity())
            .collect();
        assert_eq!(matches.len(), 1);
        assert_property_edge_exact(matches[0], expected);
        let vertex = session.get_vertex(endpoint)?.expect("endpoint retained");
        assert_eq!(vertex.id, 7);
        assert_eq!(vertex.labels, vec![format!("P{}", endpoint.partition)]);
        assert_eq!(
            vertex.properties,
            BTreeMap::from([(
                "untouched".into(),
                Value::Integer(endpoint.partition as i64)
            ),])
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_v2_property_mutations_in_real_processes() -> anyhow::Result<()> {
    let _fixture = PROCESS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(120), property_mutation_scenario()).await?
}

async fn property_mutation_scenario() -> anyhow::Result<()> {
    let metadata = Group::start(410, 510).await?;
    let mut a = Group::start(420, 520).await?;
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
    let target_node = replicated_graph::RemoteStorageNode::new_identified(
        b.replicas[0].endpoint,
        430,
        1,
        530,
        BUDGET,
    )?;
    let ap = a.placement.clone();
    let bp = b.placement.clone();
    let (resolver, unknown) = run_blocking(move || -> anyhow::Result<_> {
        for (partition, placement) in [(1, ap.clone()), (2, bp), (3, ap)] {
            catalog.commit_registration(&catalog.prepare_registration(
                [partition as u8; 16], partition, placement,
            )?)?;
            let mut vertex = Vertex::new(7);
            vertex.labels.push(format!("P{partition}"));
            vertex.properties.insert("untouched".into(), Value::Integer(partition as i64));
            let prepared = resolver.experimental_read_session(BUDGET)?
                .prepare_vertex(partition, &vertex)?;
            resolver.experimental_commit(&prepared, BUDGET)?;
        }
        // Cross-shard, distinct partitions on one shard, and a true self-loop.
        for (id, target) in [(101, 2), (102, 3), (103, 1)] {
            let original = CrossEdge {
                id,
                from: VertexRef { partition: 1, id: 7 },
                to: VertexRef { partition: target, id: 7 },
                label: "Properties".into(),
                properties: BTreeMap::from([
                    ("typed".into(), Value::Integer(1)),
                    ("large".into(), Value::Integer(i64::MAX)),
                    ("zero".into(), Value::Real(0.0)),
                    ("removed".into(), Value::Null),
                    ("nullable".into(), Value::Text("before".into())),
                ]),
            };
            let create = resolver.experimental_read_session(BUDGET)?.prepare_edge(&original)?;
            resolver.experimental_commit(&create, BUDGET)?;
            let frozen = resolver.experimental_read_session(BUDGET)?;
            assert_property_edge_complete(&frozen, &original)?;
            let patch = EdgePropertyPatch {
                set: BTreeMap::from([
                    ("typed".into(), Value::Real(1.0)),
                    ("zero".into(), Value::Real(-0.0)),
                    ("nullable".into(), Value::Null),
                ]),
                remove: BTreeSet::from(["removed".into(), "already_absent".into()]),
            };
            let update1 = frozen.prepare_edge_properties(original.identity(), &patch)?;
            let competitor = frozen.prepare_edge_properties(original.identity(), &EdgePropertyPatch {
                set: BTreeMap::from([("competitor".into(), Value::Boolean(true))]),
                remove: BTreeSet::new(),
            })?;
            assert_eq!(update1.edge(), original.identity());
            assert_eq!(update1.kind(), EdgeMutationKind::Properties);
            assert_eq!(update1.operation_id(), update1.source_request_id());
            assert_ne!(update1.operation_id(), competitor.operation_id());
            assert_eq!(resolver.experimental_commit_mutation(&update1, BUDGET)?,
                MutationAcknowledgement::AllBatchesAcknowledged);
            let mut first = original.clone();
            first.properties.extend(patch.set);
            first.properties.remove("removed");
            let after_first = resolver.experimental_read_session(BUDGET)?;
            assert_property_edge_complete(&after_first, &first)?;
            assert_property_edge_complete(&frozen, &original)?;
            match resolver.experimental_commit_mutation(&competitor, BUDGET) {
                Err(ExperimentalRouteError::Stage {
                    stage: Stage::Source,
                    outcome: StageOutcome::Rejected,
                    request_id,
                    source_acknowledged_this_attempt: false,
                    error: RouteError::Storage(StorageNodeError::Conflict { .. }),
                }) => assert_eq!(request_id, competitor.source_request_id()),
                other => panic!("expected source CAS conflict: {other:?}"),
            }
            // Re-preparing from an old session after a successful update must
            // still use its original snapshot, rather than silently refreshing.
            let stale = frozen.prepare_edge_properties(original.identity(), &EdgePropertyPatch {
                set: BTreeMap::from([("stale".into(), Value::Boolean(true))]),
                remove: BTreeSet::new(),
            })?;
            match resolver.experimental_commit_mutation(&stale, BUDGET) {
                Err(ExperimentalRouteError::Stage {
                    stage: Stage::Source,
                    outcome: StageOutcome::Rejected,
                    request_id,
                    source_acknowledged_this_attempt: false,
                    error: RouteError::Storage(StorageNodeError::Conflict { .. }),
                }) => assert_eq!(request_id, stale.source_request_id()),
                other => panic!("expected stale frozen-session CAS conflict: {other:?}"),
            }
            // Historical create acknowledgements must never restore old properties.
            resolver.experimental_commit(&create, BUDGET)?;
            assert_property_edge_complete(&resolver.experimental_read_session(BUDGET)?, &first)?;
            let update2 = after_first.prepare_edge_properties(original.identity(), &EdgePropertyPatch {
                set: BTreeMap::from([
                    ("typed".into(), Value::Integer(1)),
                    ("large".into(), Value::Integer(i64::MIN)),
                    ("zero".into(), Value::Real(0.0)),
                ]),
                remove: BTreeSet::from(["nullable".into()]),
            })?;
            assert_eq!(resolver.experimental_commit_mutation(&update2, BUDGET)?,
                MutationAcknowledgement::AllBatchesAcknowledged);
            let mut second = first.clone();
            second.properties.insert("typed".into(), Value::Integer(1));
            second.properties.insert("large".into(), Value::Integer(i64::MIN));
            second.properties.insert("zero".into(), Value::Real(0.0));
            second.properties.remove("nullable");
            assert_eq!(resolver.experimental_commit_mutation(&update1, BUDGET)?,
                MutationAcknowledgement::AllBatchesAcknowledged);
            assert_property_edge_complete(&resolver.experimental_read_session(BUDGET)?, &second)?;
            assert_property_edge_complete(&after_first, &first)?;
            assert_property_edge_complete(&frozen, &original)?;
        }
        let session = resolver.experimental_read_session(BUDGET)?;
        let patch = EdgePropertyPatch {
            set: BTreeMap::from([("last".into(), Value::Boolean(true))]),
            remove: BTreeSet::new(),
        };
        let missing = EdgeRef { partition: 1, id: 999 };
        assert!(matches!(session.prepare_edge_properties(missing, &patch),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(edge))) if edge == missing));
        assert!(session.get_edge(missing)?.is_none());
        // Force a source-only append by invalidating only the reverse shard's
        // sequence between prepare and commit. Properties require a full edge.
        let incomplete = CrossEdge {
            id: 104,
            from: VertexRef { partition: 1, id: 7 },
            to: VertexRef { partition: 2, id: 7 },
            label: "Incomplete".into(),
            properties: BTreeMap::new(),
        };
        let append = session.prepare_edge(&incomplete)?;
        let target_write = session.prepare_vertex(2, &Vertex::new(8))?;
        resolver.experimental_commit(&target_write, BUDGET)?;
        match resolver.experimental_commit(&append, BUDGET) {
            Err(ExperimentalRouteError::Stage {
                stage: Stage::Reverse,
                outcome: StageOutcome::Incomplete,
                source_acknowledged_this_attempt: true,
                request_id,
                ..
            }) => assert_eq!(Some(request_id), append.reverse_request_id()),
            other => panic!("expected source-only append: {other:?}"),
        }
        let partial = resolver.experimental_read_session(BUDGET)?;
        assert!(matches!(partial.get_edge(incomplete.identity()),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(edge)))
                if edge == incomplete.identity()));
        assert!(matches!(partial.prepare_edge_properties(incomplete.identity(), &patch),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(edge)))
                if edge == incomplete.identity()));
        // Rejection is preparation-only: neither shard's sequence can advance,
        // and the absent reverse index must not be repaired as a side effect.
        let after_rejection = resolver.experimental_read_session(BUDGET)?;
        assert_eq!(partial.snapshots, after_rejection.snapshots);
        assert!(after_rejection.observed_adjacency(incomplete.to, Direction::In)?
            .edges.iter().all(|edge| edge.identity() != incomplete.identity()));
        assert!(matches!(after_rejection.get_edge(incomplete.identity()),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(edge)))
                if edge == incomplete.identity()));
        let unknown = after_rejection.prepare_edge_properties(EdgeRef { partition: 1, id: 101 }, &patch)?;
        // Trusted raw D1 fault injection: deleting the target endpoint must
        // make a fresh session reject preparation before any source mutation.
        let mut target_vertex_key = b"graph/v2/p/".to_vec();
        target_vertex_key.extend(2u32.to_be_bytes());
        target_vertex_key.extend(b"/vertex/");
        target_vertex_key.extend(7u64.to_be_bytes());
        target_node.batch_write(WriteRequest {
            shard: 530,
            epoch: 1,
            request_id: [234; 16],
            operations: vec![WriteOp::Delete { key: target_vertex_key }],
        })?;
        let invalid = resolver.experimental_read_session(BUDGET)?;
        let invalid_ref = EdgeRef { partition: 1, id: 101 };
        assert!(matches!(invalid.prepare_edge_properties(invalid_ref, &patch),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(edge)))
                if edge == invalid_ref));
        assert_eq!(invalid.snapshots, resolver.experimental_read_session(BUDGET)?.snapshots);
        Ok((resolver, unknown))
    }).await??;
    // Stop the source only after preparation. This is transport uncertainty,
    // not an ACK-loss test and not evidence that the mutation was rolled back.
    a.process.0.kill()?;
    a.process.0.wait()?;
    run_blocking(move || -> anyhow::Result<()> {
        match resolver.experimental_commit_mutation(&unknown, BUDGET) {
            Err(ExperimentalRouteError::Stage {
                stage: Stage::Source,
                outcome: StageOutcome::Unknown,
                source_acknowledged_this_attempt: false,
                request_id,
                ..
            }) => assert_eq!(request_id, unknown.source_request_id()),
            other => panic!("expected source unknown with original request ID: {other:?}"),
        }
        Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_v2_edge_deletions_in_real_processes() -> anyhow::Result<()> {
    let _fixture = PROCESS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(180), edge_deletion_scenario()).await?
}

async fn edge_deletion_scenario() -> anyhow::Result<()> {
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
        for (partition, placement) in [(1, ap.clone()), (2, bp), (3, ap)] {
            catalog.commit_registration(&catalog.prepare_registration([partition as u8; 16], partition, placement)?)?;
            let prepared = resolver.experimental_read_session(BUDGET)?.prepare_vertex(partition, &Vertex::new(7))?;
            resolver.experimental_commit(&prepared, BUDGET)?;
        }
        let edge = |id, target| CrossEdge {
            id, from: VertexRef { partition: 1, id: 7 },
            to: VertexRef { partition: target, id: 7 }, label: "Delete".into(),
            properties: BTreeMap::from([("payload".into(), Value::Text("discard me".into()))]),
        };
        // Cross-shard parallel edges, same-shard partitions, and a self-loop.
        let edges = [edge(1, 2), edge(2, 2), edge(3, 3), edge(4, 1)];
        let mut creates = vec![];
        for e in &edges {
            let create = resolver.experimental_read_session(BUDGET)?.prepare_edge(e)?;
            resolver.experimental_commit(&create, BUDGET)?;
            creates.push(create);
        }
        let frozen = resolver.experimental_read_session(BUDGET)?;
        for (i, e) in edges.iter().enumerate() {
            let before_update = resolver.experimental_read_session(BUDGET)?;
            let patch = EdgePropertyPatch { set: BTreeMap::from([("x".into(), Value::Integer(1))]), remove: BTreeSet::new() };
            let old_update = before_update.prepare_edge_properties(e.identity(), &patch)?;
            resolver.experimental_commit_mutation(&old_update, BUDGET)?;
            let before_delete = resolver.experimental_read_session(BUDGET)?;
            let deletion = before_delete.prepare_edge_delete(e.identity())?;
            let race = before_delete.prepare_edge_properties(e.identity(), &patch)?;
            assert_eq!(deletion.kind(), EdgeMutationKind::Delete);
            assert_eq!(deletion.edge(), e.identity());
            assert_eq!(deletion.operation_id(), deletion.source_request_id());
            assert_eq!(deletion.reverse_request_id().is_some(), e.to.partition == 2);
            assert_eq!(resolver.experimental_commit_mutation(&deletion, BUDGET)?, MutationAcknowledgement::AllBatchesAcknowledged);
            let deleted = resolver.experimental_read_session(BUDGET)?;
            assert!(deleted.get_edge(e.identity())?.is_none());
            assert!(before_delete.get_edge(e.identity())?.is_some());
            assert_eq!(frozen.get_edge(e.identity())?, Some(e.clone()));
            for (endpoint, direction) in [(e.from, Direction::Out), (e.to, Direction::In)] {
                assert!(deleted.observed_adjacency(endpoint, direction)?.edges.iter().all(|other| other.identity() != e.identity()));
                assert!(deleted.get_vertex(endpoint)?.is_some());
            }
            assert!(matches!(deleted.prepare_edge_delete(e.identity()),
                Err(ExperimentalRouteError::Graph(ExperimentalError::NotFound(id))) if id == e.identity()));
            assert!(matches!(deleted.prepare_edge(e), Err(ExperimentalRouteError::Graph(ExperimentalError::AlreadyExists))));
            assert!(matches!(resolver.experimental_commit_mutation(&race, BUDGET),
                Err(ExperimentalRouteError::Stage { stage: Stage::Source, outcome: StageOutcome::Rejected,
                    source_acknowledged_this_attempt: false, .. })));
            resolver.experimental_commit(&creates[i], BUDGET)?;
            resolver.experimental_commit_mutation(&old_update, BUDGET)?;
            resolver.experimental_commit_mutation(&deletion, BUDGET)?;
            let fresh = resolver.experimental_read_session(BUDGET)?;
            assert_eq!(deleted.snapshots, fresh.snapshots);
            assert!(fresh.get_edge(e.identity())?.is_none());
            assert_eq!(fresh.observed_gremlin_v3(999, "g.E().count()")?.rows,
                vec![replicated_graph::routing::experimental::ObservedRowV3::Count((edges.len() - i - 1) as u64)]);
            if i + 1 < edges.len() {
                assert_eq!(fresh.observed_gremlin_v3(999, "g.E().limit(1)")?.rows,
                    vec![replicated_graph::routing::experimental::ObservedRowV3::Edge(edges[i + 1].identity())]);
            }
        }
        let missing = EdgeRef { partition: 1, id: 999 };
        assert!(matches!(resolver.experimental_read_session(BUDGET)?.prepare_edge_delete(missing),
            Err(ExperimentalRouteError::Graph(ExperimentalError::NotFound(id))) if id == missing));
        // More than a page of tombstones must not consume the live leading limit.
        for id in 10..140 {
            let e = edge(id, 1);
            let create = resolver.experimental_read_session(BUDGET)?.prepare_edge(&e)?;
            resolver.experimental_commit(&create, BUDGET)?;
            let deletion = resolver.experimental_read_session(BUDGET)?.prepare_edge_delete(e.identity())?;
            resolver.experimental_commit_mutation(&deletion, BUDGET)?;
        }
        let survivor = edge(200, 2);
        resolver.experimental_commit(&resolver.experimental_read_session(BUDGET)?.prepare_edge(&survivor)?, BUDGET)?;
        let session = resolver.experimental_read_session(BUDGET)?;
        assert_eq!(session.observed_gremlin_v3(999, "g.E().limit(1)")?.rows, vec![replicated_graph::routing::experimental::ObservedRowV3::Edge(survivor.identity())]);
        assert_eq!(session.observed_gremlin_v3(999, "g.E().count()")?.rows, vec![replicated_graph::routing::experimental::ObservedRowV3::Count(1)]);
        // Updating only the reverse shard makes deletion source-acknowledged
        // but incomplete. A fresh session must not hide the remaining index.
        let deletion = session.prepare_edge_delete(survivor.identity())?;
        let target_write = session.prepare_vertex(2, &Vertex::new(8))?;
        resolver.experimental_commit(&target_write, BUDGET)?;
        for _ in 0..2 {
            match resolver.experimental_commit_mutation(&deletion, BUDGET) {
                Err(ExperimentalRouteError::Stage { stage: Stage::Reverse, outcome: StageOutcome::Incomplete,
                    source_acknowledged_this_attempt: true, request_id, .. }) => {
                    assert_eq!(Some(request_id), deletion.reverse_request_id());
                }
                other => panic!("expected partially committed deletion: {other:?}"),
            }
        }
        let partial = resolver.experimental_read_session(BUDGET)?;
        assert!(matches!(partial.get_edge(survivor.identity()),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(id))) if id == survivor.identity()));
        assert!(matches!(partial.observed_gremlin_v3(999, "g.E().count()"),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(id))) if id == survivor.identity()));
        for query in [
            "g.E(200)", "g.E(200).path()", "g.E(200).count()",
            "g.E(200).has('missing').count()", "g.E(200).limit(1)",
            "g.E().has('missing').count()", "g.E().limit(1)",
            "g.V(7).inE().count()",
        ] {
            let partition = if query.starts_with("g.V") { 2 } else { 1 };
            assert!(matches!(partial.observed_gremlin_v3(partition, query),
                Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(id))) if id == survivor.identity()),
                "partial deletion silently accepted {query}");
        }
        assert!(partial.observed_adjacency(survivor.to, Direction::In).is_err());
        assert!(matches!(partial.prepare_edge_delete(survivor.identity()),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(id))) if id == survivor.identity()));
        assert_eq!(partial.snapshots, resolver.experimental_read_session(BUDGET)?.snapshots);
        // Prepare both transport-uncertain stages before stopping a participant.
        let reverse_edge = edge(201, 2);
        let mut source_edge = edge(202, 1);
        source_edge.from.partition = 2;
        for e in [&reverse_edge, &source_edge] {
            resolver.experimental_commit(&resolver.experimental_read_session(BUDGET)?.prepare_edge(e)?, BUDGET)?;
        }
        let session = resolver.experimental_read_session(BUDGET)?;
        let unknown_source = session.prepare_edge_delete(source_edge.identity())?;
        let unknown_reverse = session.prepare_edge_delete(reverse_edge.identity())?;
        Ok((resolver, unknown_source, unknown_reverse))
    }).await??;
    b.process.0.kill()?;
    b.process.0.wait()?;
    run_blocking(move || -> anyhow::Result<()> {
        for (mutation, expected_stage, acknowledged) in [
            (&unknown_reverse, Stage::Reverse, true),
            (&unknown_source, Stage::Source, false),
        ] {
            match resolver.experimental_commit_mutation(mutation, BUDGET) {
                Err(ExperimentalRouteError::Stage {
                    stage,
                    outcome: StageOutcome::Unknown,
                    source_acknowledged_this_attempt,
                    request_id,
                    ..
                }) => {
                    assert_eq!(stage, expected_stage);
                    assert_eq!(source_acknowledged_this_attempt, acknowledged);
                    assert_eq!(
                        request_id,
                        if acknowledged {
                            mutation.reverse_request_id().unwrap()
                        } else {
                            mutation.source_request_id()
                        }
                    );
                }
                other => panic!("expected honest transport uncertainty: {other:?}"),
            }
        }
        Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_v2_vertex_properties_in_real_processes() -> anyhow::Result<()> {
    let _fixture = PROCESS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(120), vertex_property_scenario()).await?
}

async fn vertex_property_scenario() -> anyhow::Result<()> {
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
    let ap = a.placement.clone();
    let bp = b.placement.clone();
    run_blocking(move || -> anyhow::Result<()> {
        let vr = |partition| VertexRef { partition, id: 7 };
        let mut creates = Vec::new();
        for (partition, placement) in [(1, ap.clone()), (2, bp), (3, ap)] {
            catalog.commit_registration(&catalog.prepare_registration([partition as u8; 16], partition, placement)?)?;
            let mut vertex = Vertex::new(7);
            vertex.labels.push(format!("P{partition}"));
            vertex.properties = BTreeMap::from([
                ("typed".into(), Value::Integer(1)),
                ("nullable".into(), Value::Text("before".into())),
                ("removed".into(), Value::Null),
                ("zero".into(), Value::Real(0.0)),
                ("untouched".into(), Value::Integer(partition as i64)),
            ]);
            let create = resolver.experimental_read_session(BUDGET)?.prepare_vertex(partition, &vertex)?;
            resolver.experimental_commit(&create, BUDGET)?;
            creates.push(create);
        }
        // Each same-numbered vertex is qualified, including two partitions on
        // the same shard. Mutating one must not replace either of the others.
        for partition in 1..=3 {
            let frozen = resolver.experimental_read_session(BUDGET)?;
            let before: Vec<_> = (1..=3).map(|p| frozen.get_vertex(vr(p)).unwrap().unwrap()).collect();
            let patch = VertexPropertyPatch {
                set: BTreeMap::from([
                    ("typed".into(), Value::Real(1.0)),
                    ("nullable".into(), Value::Null),
                    ("zero".into(), Value::Real(-0.0)),
                    ("enabled".into(), Value::Boolean(true)),
                    ("large".into(), Value::Integer(i64::MAX)),
                ]),
                remove: BTreeSet::from(["removed".into(), "absent".into()]),
            };
            let update = frozen.prepare_vertex_properties(vr(partition), &patch)?;
            assert_eq!(update.vertex(), vr(partition));
            assert_eq!(resolver.experimental_commit_vertex_mutation(&update, BUDGET)?, MutationAcknowledgement::AllBatchesAcknowledged);
            let fresh = resolver.experimental_read_session(BUDGET)?;
            let mut expected = before[(partition - 1) as usize].clone();
            expected.properties.extend(patch.set);
            expected.properties.remove("removed");
            for p in 1..=3 {
                let actual = fresh.get_vertex(vr(p))?.unwrap();
                assert_eq!(actual, if p == partition { expected.clone() } else { before[(p - 1) as usize].clone() });
                assert_eq!(frozen.get_vertex(vr(p))?.unwrap(), before[(p - 1) as usize]);
            }
            assert!(matches!(fresh.get_vertex(vr(partition))?.unwrap().properties["zero"], Value::Real(value) if value.to_bits() == (-0.0f64).to_bits()));
            for (session, typed, null_count, removed_count) in [
                (&frozen, Value::Integer(1), 0, 1),
                (&fresh, Value::Real(1.0), 1, 0),
            ] {
                assert_eq!(session.observed_gremlin_v2(partition, "g.V(7).values('typed')")?.rows,
                    vec![ObservedRowV2::Value { source: vr(partition), value: typed }]);
                for (query, count) in [
                    ("g.V(7).has('nullable',null).count()", null_count),
                    ("g.V(7).has('removed').count()", removed_count),
                    ("g.V(7).has('absent',null).count()", 0),
                ] {
                    let result = session.observed_gremlin(partition, query)?;
                    assert_eq!(result.rows, vec![ObservedRow::Count(count)]);
                    assert_eq!(result.snapshots, session.snapshots);
                    assert_eq!(result.completeness, AdjacencyCompleteness::Unverified);
                }
            }
            let stale = frozen.prepare_vertex_properties(vr(partition), &VertexPropertyPatch {
                set: BTreeMap::from([("stale".into(), Value::Boolean(true))]), remove: BTreeSet::new(),
            })?;
            assert_ne!(stale.operation_id(), update.operation_id());
            assert!(matches!(resolver.experimental_commit_vertex_mutation(&stale, BUDGET),
                Err(ExperimentalRouteError::Stage { stage: Stage::Source, outcome: StageOutcome::Rejected,
                    request_id, source_acknowledged_this_attempt: false,
                    error: RouteError::Storage(StorageNodeError::Conflict { .. }) }) if request_id == stale.operation_id()));
            let later = fresh.prepare_vertex_properties(vr(partition), &VertexPropertyPatch {
                set: BTreeMap::from([("typed".into(), Value::Text("later".into()))]),
                remove: BTreeSet::from(["nullable".into()]),
            })?;
            resolver.experimental_commit_vertex_mutation(&later, BUDGET)?;
            resolver.experimental_commit(&creates[(partition - 1) as usize], BUDGET)?;
            assert_eq!(resolver.experimental_commit_vertex_mutation(&update, BUDGET)?, MutationAcknowledgement::AllBatchesAcknowledged);
            expected.properties.insert("typed".into(), Value::Text("later".into()));
            expected.properties.remove("nullable");
            assert_eq!(resolver.experimental_read_session(BUDGET)?.get_vertex(vr(partition))?.unwrap(), expected);
        }
        // Two real concurrent submissions, not sequential simulation. Both CAS
        // requests share a snapshot; exactly one can change this shard sequence.
        let session = resolver.experimental_read_session(BUDGET)?;
        let contenders: Vec<_> = ["left", "right"].into_iter().map(|value|
            session.prepare_vertex_properties(vr(1), &VertexPropertyPatch {
                set: BTreeMap::from([("winner".into(), Value::Text(value.into()))]), remove: BTreeSet::new(),
            }).unwrap()).collect();
        let barrier = std::sync::Barrier::new(2);
        let outcomes = std::thread::scope(|scope| {
            let handles: Vec<_> = contenders.iter().map(|prepared| {
                let resolver = &resolver;
                let barrier = &barrier;
                scope.spawn(move || { barrier.wait(); resolver.experimental_commit_vertex_mutation(prepared, BUDGET) })
            }).collect();
            handles.into_iter().map(|handle| handle.join().unwrap()).collect::<Vec<_>>()
        });
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        let winner = outcomes.iter().position(|result| result.is_ok()).unwrap();
        let loser = 1 - winner;
        assert!(matches!(&outcomes[loser], Err(ExperimentalRouteError::Stage {
            stage: Stage::Source, outcome: StageOutcome::Rejected, request_id,
            source_acknowledged_this_attempt: false,
            error: RouteError::Storage(StorageNodeError::Conflict { .. }),
        }) if *request_id == contenders[loser].operation_id()));
        let session = resolver.experimental_read_session(BUDGET)?;
        assert_eq!(session.get_vertex(vr(1))?.unwrap().properties["winner"], Value::Text(["left", "right"][winner].into()));
        // Invalid budgets and oversized caller patches fail before a write.
        assert!(matches!(resolver.experimental_commit_vertex_mutation(&contenders[winner], Duration::ZERO),
            Err(ExperimentalRouteError::Route(RouteError::InvalidArgument(_)))));
        let oversized = VertexPropertyPatch {
            set: BTreeMap::from([("oversized".into(), Value::Text("x".repeat(1024 * 1024)))]),
            remove: BTreeSet::new(),
        };
        assert!(matches!(session.prepare_vertex_properties(vr(1), &oversized),
            Err(ExperimentalRouteError::Graph(ExperimentalError::Invalid(_)))));
        let missing = VertexRef { partition: 1, id: 999 };
        assert!(matches!(session.prepare_vertex_properties(missing, &VertexPropertyPatch::default()),
            Err(ExperimentalRouteError::Graph(ExperimentalError::MissingEndpoint(vertex))) if vertex == missing));
        for vertex in [vr(1), vr(2), vr(3), missing] {
            assert!(matches!(session.prepare_vertex_delete(vertex), Err(ExperimentalRouteError::Unsupported(_))));
        }
        assert_eq!(session.snapshots, resolver.experimental_read_session(BUDGET)?.snapshots);
        // A canonical source-only edge is invisible to the target's incoming
        // index. Empty observed adjacency must not authorize target deletion.
        let edge = CrossEdge { id: 400, from: vr(1), to: vr(2), label: "Partial".into(), properties: BTreeMap::new() };
        let append = session.prepare_edge(&edge)?;
        let invalidate = session.prepare_vertex(2, &Vertex::new(8))?;
        resolver.experimental_commit(&invalidate, BUDGET)?;
        assert!(matches!(resolver.experimental_commit(&append, BUDGET), Err(ExperimentalRouteError::Stage {
            stage: Stage::Reverse, outcome: StageOutcome::Incomplete, source_acknowledged_this_attempt: true, ..
        })));
        let partial = resolver.experimental_read_session(BUDGET)?;
        assert!(partial.observed_adjacency(vr(2), Direction::In)?.edges.is_empty());
        assert!(matches!(partial.get_edge(edge.identity()), Err(ExperimentalRouteError::Graph(ExperimentalError::Incomplete(_)))));
        assert!(matches!(partial.prepare_vertex_delete(vr(2)), Err(ExperimentalRouteError::Unsupported(_))));
        let after = resolver.experimental_read_session(BUDGET)?;
        assert_eq!(partial.snapshots, after.snapshots);
        assert!(after.get_vertex(vr(2))?.is_some());
        Ok(())
    }).await??;
    Ok(())
}
