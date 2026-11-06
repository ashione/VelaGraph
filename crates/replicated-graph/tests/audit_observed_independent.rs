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
        for q in [
            "g.V()",
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
