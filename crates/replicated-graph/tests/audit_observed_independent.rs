//! Independent metadata + two data OS processes; normal path, not HA evidence.
use replicated_graph::routing::experimental::{ExperimentalRouteError, ObservedPathElement};
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{snapshot_identified, RemoteIdentity},
    routing::{
        experimental::{AdjacencyCompleteness, ObservedRow},
        RemoteReplica, RouteResolver,
    },
    run_blocking,
};
use rocksdb_sql_table::{
    experimental_graph::{CrossEdge, EdgeRef, ExperimentalError, VertexRef},
    Vertex,
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
    _process: Process,
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
            _process: process,
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
async fn audit_observed_identity_order_and_frozen() -> anyhow::Result<()> {
    let _fixture = PROCESS_TEST_LOCK.lock().await;
    let m = Group::start(7410, 7510).await?;
    let a = Group::start(7420, 7520).await?;
    let b = Group::start(7430, 7530).await?;
    let catalog = Catalog::new_remote(m.replicas[0].endpoint, 1, m.placement.clone(), BUDGET)?;
    let resolver = RouteResolver::new_remote(
        m.placement.clone(),
        &m.replicas,
        &[a.replicas.clone(), b.replicas.clone()].concat(),
    )?;
    let ap = a.placement.clone();
    let bp = b.placement.clone();
    run_blocking(move || -> anyhow::Result<()> {
        let vr = |partition| VertexRef { partition, id: 7 };
        for (p, placement) in [(1, ap.clone()), (2, bp), (3, ap)] {
            catalog.commit_registration(&catalog.prepare_registration(
                [p as u8; 16],
                p,
                placement,
            )?)?;
            let mut v = Vertex::new(7);
            v.labels.push(format!("P{p}"));
            let prepared = resolver
                .experimental_read_session(BUDGET)?
                .prepare_vertex(p, &v)?;
            resolver.experimental_commit(&prepared, BUDGET)?;
        }
        let frozen = resolver.experimental_read_session(BUDGET)?;
        for (id, from, to) in [(9, 1, 2), (10, 1, 2), (9, 2, 1), (11, 1, 3), (12, 1, 1)] {
            let edge = CrossEdge {
                id,
                from: vr(from),
                to: vr(to),
                label: "K".into(),
                properties: BTreeMap::new(),
            };
            let prepared = resolver
                .experimental_read_session(BUDGET)?
                .prepare_edge(&edge)?;
            resolver.experimental_commit(&prepared, BUDGET)?;
        }
        assert_eq!(
            frozen.observed_gremlin(1, "g.V(7).both().count()")?.rows,
            vec![ObservedRow::Count(0)]
        );
        let s = resolver.experimental_read_session(BUDGET)?;
        let r = s.observed_gremlin(1, "g.V(7).both()")?;
        assert_eq!(r.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(
            r.rows,
            vec![
                ObservedRow::Vertex(vr(2)),
                ObservedRow::Vertex(vr(2)),
                ObservedRow::Vertex(vr(2)),
                ObservedRow::Vertex(vr(3)),
                ObservedRow::Vertex(vr(1))
            ]
        );
        assert_eq!(
            s.observed_gremlin(1, "g.V(7).both().hasLabel('P2').count()")?
                .rows,
            vec![ObservedRow::Count(3)]
        );
        assert_eq!(
            s.observed_gremlin(1, "g.V(7).both().limit(2).count()")?
                .rows,
            vec![ObservedRow::Count(2)]
        );
        assert_eq!(
            s.observed_gremlin(2, "g.V(7).in().count()")?.rows,
            vec![ObservedRow::Count(2)]
        );
        assert_eq!(
            s.observed_gremlin(1, "g.V(7,7).out().count()")?.rows,
            vec![ObservedRow::Count(8)]
        );
        assert_eq!(
            s.observed_gremlin(1, "g.V(7).out().out().count()")?.rows,
            vec![ObservedRow::Count(6)]
        );
        assert_eq!(
            s.observed_gremlin(1, "g.V(999).count()")?.rows,
            vec![ObservedRow::Count(0)]
        );
        assert!(s.observed_gremlin(99, "g.V(999).count()").is_err());
        assert_eq!(
            s.observed_gremlin(99, "g.V().count()")?.rows,
            vec![ObservedRow::Count(3)]
        );
        assert_eq!(
            s.observed_gremlin(99, "g.V().has('missing').count()")?.rows,
            vec![ObservedRow::Count(0)]
        );
        for q in [
            "g.V().label()",
            "g.E(7)",
            "g.V(7).values('x')",
            "g.V(7).path().out()",
            "g.V(7).count().out()",
            "g.V(7).out().out().out().out()",
        ] {
            assert!(
                s.observed_gremlin(1, q).is_err(),
                "unexpected accepted query: {q}"
            );
        }
        let paths = s.observed_gremlin(2, "g.V(7).in().path()")?;
        assert_eq!(
            paths.rows[0],
            ObservedRow::Path(vec![
                ObservedPathElement::Vertex(vr(2)),
                ObservedPathElement::Edge(EdgeRef {
                    partition: 1,
                    id: 9
                }),
                ObservedPathElement::Vertex(vr(1))
            ])
        );
        let edge = CrossEdge {
            id: 99,
            from: vr(1),
            to: vr(2),
            label: "K".into(),
            properties: BTreeMap::new(),
        };
        let prepared = s.prepare_edge(&edge)?;
        let target_write = s.prepare_vertex(2, &Vertex::new(8))?;
        resolver.experimental_commit(&target_write, BUDGET)?;
        assert!(resolver.experimental_commit(&prepared, BUDGET).is_err());
        let fresh = resolver.experimental_read_session(BUDGET)?;
        assert!(matches!(
            fresh.observed_gremlin(1, "g.V(7).out().count()"),
            Err(ExperimentalRouteError::Graph(
                ExperimentalError::Incomplete(_)
            ))
        ));
        let incoming = fresh.observed_gremlin(2, "g.V(7).in().count()")?;
        assert_eq!(incoming.rows, vec![ObservedRow::Count(2)]);
        assert_eq!(incoming.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(
            s.observed_gremlin(1, "g.V(7).out().count()")?.rows,
            vec![ObservedRow::Count(4)]
        );
        Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_projection_values_labels_and_frozen_provenance() -> anyhow::Result<()> {
    use replicated_graph::routing::experimental::{ObservedRowV2, ObservedTraversal};
    use rocksdb_sql_table::Value;
    // Compile-time regression: downstream users may still require Eq.
    fn requires_eq<T: Eq>() {}
    requires_eq::<ObservedRow>();
    requires_eq::<ObservedTraversal>();
    let _lock = PROCESS_TEST_LOCK.lock().await;
    let m = Group::start(10, 10).await?;
    let a = Group::start(20, 20).await?;
    let b = Group::start(30, 30).await?;
    let catalog = Catalog::new_remote(m.replicas[0].endpoint, 1, m.placement.clone(), BUDGET)?;
    let resolver = RouteResolver::new_remote(
        m.placement.clone(),
        &m.replicas,
        &[a.replicas.clone(), b.replicas.clone()].concat(),
    )?;
    run_blocking(move || -> anyhow::Result<()> {
        let vr = |partition, id| VertexRef { partition, id };
        for (p, placement) in [
            (1, a.placement.clone()),
            (2, b.placement.clone()),
            (3, a.placement.clone()),
        ] {
            catalog.commit_registration(&catalog.prepare_registration(
                [p as u8; 16],
                p,
                placement,
            )?)?;
            let mut v = Vertex::new(7);
            if p != 3 {
                v.labels = vec!["Z".into(), "A".into()];
            }
            if p == 1 {
                v.properties.insert("x".into(), Value::Null);
            }
            if p == 2 {
                v.properties.insert("x".into(), Value::Real(-0.0));
            }
            v.properties.insert("bool".into(), Value::Boolean(true));
            v.properties.insert("int".into(), Value::Integer(i64::MAX));
            v.properties
                .insert("text".into(), Value::Text("同值".into()));
            let prepared = resolver
                .experimental_read_session(BUDGET)?
                .prepare_vertex(p, &v)?;
            resolver.experimental_commit(&prepared, BUDGET)?;
        }
        for (id, to) in [(1, 2), (2, 2), (3, 3)] {
            let prepared =
                resolver
                    .experimental_read_session(BUDGET)?
                    .prepare_edge(&CrossEdge {
                        id,
                        from: vr(1, 7),
                        to: vr(to, 7),
                        label: "K".into(),
                        properties: BTreeMap::new(),
                    })?;
            resolver.experimental_commit(&prepared, BUDGET)?;
        }
        let s = resolver.experimental_read_session(BUDGET)?;
        let value = |source, value| ObservedRowV2::Value { source, value };
        let r = s.observed_gremlin_v2(999, "g.V().values('x')")?;
        assert_eq!(
            r.rows,
            vec![
                value(vr(1, 7), Value::Null),
                value(vr(2, 7), Value::Real(0.0))
            ]
        );
        let ObservedRowV2::Value {
            value: Value::Real(z),
            ..
        } = r.rows[1]
        else {
            panic!("REAL retained")
        };
        assert_eq!(z.to_bits(), (-0.0f64).to_bits());
        assert_eq!(r.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(r.metadata_sequence, s.metadata_sequence);
        assert_eq!(r.snapshots, s.snapshots);
        assert_eq!(r.snapshots.len(), 2);
        assert_eq!(
            s.observed_gremlin_v2(1, "g.V().label()")?.rows,
            vec![
                value(vr(1, 7), Value::Text("Z".into())),
                value(vr(2, 7), Value::Text("Z".into())),
                value(vr(3, 7), Value::Text(String::new())),
            ]
        );
        assert_eq!(
            s.observed_gremlin_v2(1, "g.V(7).out().values('x')")?.rows,
            vec![
                value(vr(2, 7), Value::Real(-0.0)),
                value(vr(2, 7), Value::Real(-0.0))
            ]
        );
        assert_eq!(
            s.observed_gremlin_v2(1, "g.V(7,7).out().values('x').count()")?
                .rows,
            vec![ObservedRowV2::Count(4)]
        );
        for (query, n) in [
            ("g.V().values('x').count()", 2),
            ("g.V().values('x').limit(1).count()", 1),
            ("g.V().limit(3).values('x').count()", 2),
            ("g.V().values('missing').count()", 0),
            ("g.V().label().count()", 3),
            ("g.V(999).values('x').count()", 0),
        ] {
            assert_eq!(
                s.observed_gremlin_v2(1, query)?.rows,
                vec![ObservedRowV2::Count(n)],
                "{query}"
            );
        }
        for key in ["bool", "int", "text"] {
            let query = format!("g.V().values('{key}')");
            let projected = s.observed_gremlin_v2(1, &query)?;
            assert_eq!(projected.rows.len(), 3);
            for (index, row) in projected.rows.iter().enumerate() {
                let expected = match key {
                    "bool" => Value::Boolean(true),
                    "int" => Value::Integer(i64::MAX),
                    _ => Value::Text("同值".into()),
                };
                assert_eq!(*row, value(vr(index as u32 + 1, 7), expected));
            }
        }
        for query in [
            "g.V().values('x').limit(0).count()",
            "g.V().values('x').out()",
            "g.V(999).label().has('x')",
            "g.V().values('missing').values('x')",
            "g.V().limit(0).label().path()",
            "g.V().label().label()",
            "g.V().values()",
            "g.V().values('x','bool')",
            "g.V().id()",
            "g.E().label()",
            "g.V().count().limit(1)",
        ] {
            assert!(s.observed_gremlin_v2(1, query).is_err(), "{query}");
        }
        assert!(matches!(
            s.observed_gremlin(1, "g.V().label()"),
            Err(ExperimentalRouteError::Unsupported(_))
        ));
        for real in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut v = Vertex::new(100);
            v.properties.insert("x".into(), Value::Real(real));
            assert!(s.prepare_vertex(1, &v).is_err());
        }
        // A later append must not refresh either the frozen scan or projections.
        let mut later = Vertex::new(1);
        later
            .properties
            .insert("x".into(), Value::Text("later".into()));
        let prepared = s.prepare_vertex(2, &later)?;
        resolver.experimental_commit(&prepared, BUDGET)?;
        assert_eq!(s.observed_gremlin_v2(1, "g.V().values('x')")?.rows, r.rows);
        let fresh = resolver.experimental_read_session(BUDGET)?;
        assert_eq!(
            fresh
                .observed_gremlin_v2(1, "g.V().limit(1).values('missing').count()")?
                .rows,
            vec![ObservedRowV2::Count(0)]
        );
        assert_eq!(
            fresh
                .observed_gremlin_v2(1, "g.V().values('x').limit(1)")?
                .rows,
            vec![value(vr(2, 1), Value::Text("later".into()))]
        );
        let mut wide = Vertex::new(90);
        wide.properties
            .insert("blob".into(), Value::Text("x".repeat(800_000)));
        let prepared = fresh.prepare_vertex(1, &wide)?;
        resolver.experimental_commit(&prepared, BUDGET)?;
        let wide_session = resolver.experimental_read_session(BUDGET)?;
        let ids = std::iter::repeat_n("90", 32).collect::<Vec<_>>().join(",");
        // Start acquisition fits; projection rereads and retained scalar payloads
        // cross the shared logical byte cap before a late limit can hide work.
        assert_eq!(
            wide_session
                .observed_gremlin_v2(1, &format!("g.V({ids}).count()"))?
                .rows,
            vec![ObservedRowV2::Count(32)]
        );
        assert!(matches!(
            wide_session.observed_gremlin_v2(1, &format!("g.V({ids}).values('blob').limit(1)")),
            Err(ExperimentalRouteError::Query(
                rocksdb_sql_table::Error::TraversalLimitExceeded(_)
            ))
        ));
        Ok(())
    })
    .await??;
    Ok(())
}
