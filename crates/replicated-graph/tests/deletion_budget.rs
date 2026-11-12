//! Independent metadata + one data OS process; normal path, not HA evidence.
use replicated_graph::routing::experimental::ExperimentalRouteError;
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{snapshot_identified, RemoteIdentity},
    routing::{experimental::ObservedRowV3, RemoteReplica, RouteResolver},
    run_blocking,
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
use velagraph::{
    experimental_graph::{CrossEdge, VertexRef},
    StorageNode, Vertex, WriteOp, WriteRequest,
};
// Cumulative-work regressions intentionally verify thousands of primitive reads.
// Allow the supported maximum session deadline under slower CI hosts.
const BUDGET: Duration = Duration::from_secs(60);

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

fn varint(mut n: u64, out: &mut Vec<u8>) {
    while n >= 128 {
        out.push((n as u8 & 127) | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
// Trusted raw fixture injection, not an alternative supported mutation API.
fn deleted_record(id: u64) -> WriteOp {
    let mut key = b"graph/v2/p/".to_vec();
    key.extend(1u32.to_be_bytes());
    key.extend(b"/edge/");
    key.extend(id.to_be_bytes());
    let mut edge = vec![8];
    varint(id, &mut edge);
    edge.extend([16, 7, 24, 7, 34, 1, b'K']);
    let mut local = vec![8, 1, 16, 2, 26];
    varint(edge.len() as u64, &mut local);
    local.extend(edge);
    let mut value = vec![8, 3, 16, 3, 29, 1, 0, 0, 0, 34];
    varint(local.len() as u64, &mut value);
    value.extend(local);
    value.extend([42, 16]);
    value.extend([19; 16]);
    WriteOp::put(key, value)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleted_candidates_cannot_evade_global_work_budget() -> anyhow::Result<()> {
    let m = Group::start(8410, 8510).await?;
    let a = Group::start(8420, 8520).await?;
    let catalog = Catalog::new_remote(m.replicas[0].endpoint, 1, m.placement.clone(), BUDGET)?;
    let resolver = RouteResolver::new_remote(m.placement.clone(), &m.replicas, &a.replicas)?;
    let node = replicated_graph::RemoteStorageNode::new_identified(
        a.replicas[0].endpoint,
        8420,
        1,
        8520,
        BUDGET,
    )?;
    let ap = a.placement.clone();
    run_blocking(move || -> anyhow::Result<()> {
        catalog.commit_registration(&catalog.prepare_registration([1; 16], 1, ap)?)?;
        resolver.experimental_commit(
            &resolver
                .experimental_read_session(BUDGET)?
                .prepare_vertex(1, &Vertex::new(7))?,
            BUDGET,
        )?;
        node.batch_write(WriteRequest {
            shard: 8520,
            epoch: 1,
            request_id: [2; 16],
            operations: (1..=10_000).map(deleted_record).collect(),
        })?;
        let before = resolver.experimental_read_session(BUDGET)?;
        // Exactly 10,000 deleted candidates fit; no premature end at page 128.
        assert_eq!(
            before.observed_gremlin_v3(1, "g.E().count()")?.rows,
            vec![ObservedRowV3::Count(0)]
        );
        let live = CrossEdge {
            id: 10_001,
            from: VertexRef {
                partition: 1,
                id: 7,
            },
            to: VertexRef {
                partition: 1,
                id: 7,
            },
            label: "K".into(),
            properties: BTreeMap::new(),
        };
        resolver.experimental_commit(
            &resolver
                .experimental_read_session(BUDGET)?
                .prepare_edge(&live)?,
            BUDGET,
        )?;
        for query in ["g.E().limit(1)", "g.E().count()"] {
            let session = resolver.experimental_read_session(BUDGET)?;
            assert!(
                matches!(
                    session.observed_gremlin_v3(1, query),
                    Err(ExperimentalRouteError::Query(
                        velagraph::Error::TraversalLimitExceeded(_)
                    ))
                ),
                "{query}"
            );
        }
        Ok(())
    })
    .await??;
    Ok(())
}
