//! Independent metadata + two data OS processes; normal path, not HA evidence.
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{snapshot_identified, RemoteIdentity},
    routing::{
        experimental::{AdjacencyCompleteness, ExperimentalRouteError, Stage, StageOutcome},
        RemoteReplica, RouteResolver,
    },
    run_blocking,
};
use rocksdb_sql_table::{
    experimental_graph::{CrossEdge, Direction, EdgeRef, ExperimentalError, VertexRef},
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
        for (p, placement) in [(1, ap.clone()), (2, bp), (3, ap)] {
            catalog.commit_registration(&catalog.prepare_registration(
                [p as u8; 16],
                p,
                placement,
            )?)?;
            let session = resolver.experimental_read_session(BUDGET)?;
            let vertex = session.prepare_vertex(p, &Vertex::new(7))?;
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
        let session = resolver.experimental_read_session(BUDGET)?;
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
        // Source remains; no claim of abort. Index-only incoming cannot detect it.
        let current = resolver.experimental_read_session(BUDGET)?;
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
            let vertex = session.prepare_vertex(p, &Vertex::new(7))?;
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
