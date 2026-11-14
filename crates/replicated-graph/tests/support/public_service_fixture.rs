// Shared independent-process fixture; no acceptance tests are included.
use anyhow::{anyhow, Context};
use axum::body::{to_bytes, Body};
use d2_raft::{
    managed_rpc::{Identity, Role, Server},
    participant_writes::Authority,
    transaction_decisions::{Cohort, Command as DecisionCommand, Journal, Member},
    versioned_reads::Target,
    Cluster, D1Command, LogCommand, NodeHandle,
};
use prost::Message;
use replicated_graph::managed_gateway::public_proto as pb;
use replicated_graph::{
    managed_gateway::{Binding, Mapping},
    remote::GraphService,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
const BUDGET: Duration = Duration::from_secs(15);
const CHILD_ENV: &str = "VELAGRAPH_PUBLIC_SERVICE_NODE_DIR";
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn cohort() -> Cohort {
    Cohort {
        profile: Default::default(),
        coordinator_shard: 9,
        coordinator_epoch: 1,
        members: [7, 8]
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .to_vec(),
    }
}
fn target(shard: u64) -> Target {
    Target {
        shard,
        epoch: 1,
        mode: [shard as u8; 16],
    }
}
fn identity(shard: u64) -> Identity {
    Identity {
        group: shard,
        node: 1,
        target: target(shard),
        role: if shard == 9 {
            Role::Coordinator
        } else {
            Role::Participant
        },
        authority: authority(),
        cohort: cohort(),
    }
}
fn binding() -> Binding {
    Binding {
        authority: authority(),
        cohort: cohort(),
        mapping: Mapping::PartitionEqualsShardV1,
    }
}
async fn apply(node: &NodeHandle, command: D1Command) -> anyhow::Result<()> {
    node.raft
        .client_write(LogCommand::D1V1(command))
        .await?
        .data
        .d1
        .context("missing D1 reply")?
        .map_err(|e| anyhow!("activation: {e:?}"))?;
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct NodeReady {
    pid: u32,
    control: SocketAddr,
    graph: Option<SocketAddr>,
}
/// Fresh-node test bootstrap only. No snapshot installation or production
/// restoration/activation bypass is introduced by the public service executable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "private independent node-process entrypoint"]
async fn public_service_node_child() -> anyhow::Result<()> {
    let directory = PathBuf::from(std::env::var_os(CHILD_ENV).context("node directory")?);
    let shard: u64 = std::env::var("VELAGRAPH_PUBLIC_SERVICE_NODE_SHARD")?.parse()?;
    let internal_grpc = std::env::var("VELAGRAPH_PUBLIC_SERVICE_INTERNAL_GRPC")? == "1";
    let cluster = Cluster::new().in_group(shard);
    let node = Arc::new(cluster.start_durable(1, directory.join("data")).await?);
    node.raft
        .initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
        .await?;
    tokio::time::timeout(BUDGET, async {
        while node.raft.ensure_linearizable().await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    apply(&node, D1Command::Activate { shard, epoch: 1 }).await?;
    apply(
        &node,
        D1Command::EnableManagedWrites {
            shard,
            epoch: 1,
            expected_sequence: 0,
            mode_id: target(shard).mode,
        },
    )
    .await?;
    if shard == 9 {
        apply(
            &node,
            D1Command::EnableCoordinator {
                shard,
                epoch: 1,
                mode_id: target(shard).mode,
            },
        )
        .await?;
        apply(
            &node,
            D1Command::Coordinator {
                shard,
                epoch: 1,
                mode_id: target(shard).mode,
                command: DecisionCommand::BindCohort { cohort: cohort() },
            },
        )
        .await?;
    } else {
        apply(
            &node,
            D1Command::EnableParticipant {
                shard,
                epoch: 1,
                mode_id: target(shard).mode,
                authority: authority(),
            },
        )
        .await?;
        let mut journal = Journal::new(authority().mode).map_err(|e| anyhow!("{e:?}"))?;
        journal
            .apply(DecisionCommand::BindCohort { cohort: cohort() })
            .map_err(|e| anyhow!("{e:?}"))?;
        apply(
            &node,
            D1Command::EnableVersionedReads {
                shard,
                epoch: 1,
                mode_id: target(shard).mode,
                journal,
            },
        )
        .await?;
    }
    let managed = Server::bind(
        node.clone(),
        identity(shard),
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    let control = managed.local_addr();
    let graph = if shard != 9 {
        Some(
            GraphService::start_managed(&node, "127.0.0.1:0".parse()?, shard, shard, BUDGET)
                .await?,
        )
    } else {
        None
    };
    let graph_grpc = if internal_grpc {
        if let Some(graph) = graph.as_ref() {
            Some(
                replicated_graph::remote::grpc::GrpcSnapshotService::start(
                    graph,
                    "127.0.0.1:0".parse()?,
                )
                .await?,
            )
        } else {
            None
        }
    } else {
        None
    };
    let graph_address = if internal_grpc {
        graph_grpc
            .as_ref()
            .map(replicated_graph::remote::grpc::GrpcSnapshotService::local_addr)
    } else {
        graph.as_ref().map(GraphService::local_addr)
    };
    fs::write(
        directory.join("ready.tmp"),
        serde_json::to_vec(&NodeReady {
            pid: std::process::id(),
            control,
            graph: graph_address,
        })?,
    )?;
    fs::rename(directory.join("ready.tmp"), directory.join("ready.json"))?;
    std::future::pending::<()>().await;
    Ok(())
}
struct NodeProcess {
    child: Child,
    _directory: tempfile::TempDir,
    ready: NodeReady,
}
impl NodeProcess {
    async fn start(shard: u64, internal_grpc: bool) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "public_service_node_child",
                "--nocapture",
            ])
            .env(CHILD_ENV, directory.path())
            .env("VELAGRAPH_PUBLIC_SERVICE_NODE_SHARD", shard.to_string())
            .env(
                "VELAGRAPH_PUBLIC_SERVICE_INTERNAL_GRPC",
                if internal_grpc { "1" } else { "0" },
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?;
        let result = tokio::time::timeout(BUDGET, async {
            loop {
                anyhow::ensure!(
                    child.try_wait()?.is_none(),
                    "node child exited before readiness"
                );
                if let Ok(bytes) = fs::read(directory.path().join("ready.json")) {
                    let ready: NodeReady = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(ready.pid == child.id(), "stale node readiness");
                    return Ok::<_, anyhow::Error>(ready);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        match result {
            Ok(Ok(ready)) => Ok(Self {
                child,
                _directory: directory,
                ready,
            }),
            other => {
                let _ = child.kill();
                let _ = child.wait();
                other.context("node startup timeout")??;
                unreachable!()
            }
        }
    }
}
impl Drop for NodeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
struct ServiceProcess {
    child: Child,
    address: SocketAddr,
    grpc: SocketAddr,
    _output: PathBuf,
}
impl ServiceProcess {
    async fn start(config: &Path, output: PathBuf) -> anyhow::Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_graph-service"));
        command.args([
            "--config",
            config.to_str().context("config path")?,
            "--http",
            "127.0.0.1:0",
            "--grpc",
            "127.0.0.1:0",
            "--allow-insecure-development",
            "--timeout-ms",
            "15000",
        ]);
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::from(fs::File::create(&output)?))
            .stderr(Stdio::inherit())
            .spawn()?;
        let result = tokio::time::timeout(BUDGET, async {
            loop {
                anyhow::ensure!(
                    child.try_wait()?.is_none(),
                    "service exited before announce"
                );
                let text = fs::read_to_string(&output)?;
                if let Some(line) = text.lines().next() {
                    if let Ok(ready) = serde_json::from_str::<Value>(line) {
                        anyhow::ensure!(ready["pid"] == child.id(), "stale service announce");
                        anyhow::ensure!(
                            ready["restore_supported"] == false
                                && ready["production_ready"] == false,
                            "scope metadata changed"
                        );
                        #[cfg(target_os = "linux")]
                        assert_only_public_listeners(child.id(), &ready)?;
                        return Ok::<_, anyhow::Error>((
                            ready["http"]
                                .as_str()
                                .context("HTTP address")?
                                .parse::<SocketAddr>()?,
                            ready["grpc"]
                                .as_str()
                                .context("gRPC address")?
                                .parse::<SocketAddr>()?,
                        ));
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        match result {
            Ok(Ok((address, grpc))) => Ok(Self {
                child,
                address,
                grpc,
                _output: output,
            }),
            other => {
                let _ = child.kill();
                let _ = child.wait();
                other.context("service startup timeout")??;
                unreachable!()
            }
        }
    }
    async fn graceful_stop(&mut self) -> anyhow::Result<()> {
        #[cfg(unix)]
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(self.child.id() as i32).context("service pid")?,
            rustix::process::Signal::TERM,
        )?;
        #[cfg(not(unix))]
        self.child.kill()?;
        let status = tokio::time::timeout(BUDGET, async {
            loop {
                if let Some(status) = self.child.try_wait()? {
                    return Ok::<_, anyhow::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        #[cfg(unix)]
        anyhow::ensure!(status.success(), "graceful service exit failed: {status}");
        #[cfg(not(unix))]
        let _ = status;
        Ok(())
    }
}
impl Drop for ServiceProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
async fn post(address: SocketAddr, path: &str, value: Value) -> anyhow::Result<(u16, Value)> {
    tokio::time::timeout(BUDGET, async {
        let stream = tokio::net::TcpStream::connect(address).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await?;
        let connection = tokio::spawn(connection);
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(path)
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&value)?))?;
        let response = sender.send_request(request).await?;
        let status = response.status().as_u16();
        let bytes = to_bytes(Body::new(response.into_body()), 1024 * 1024).await?;
        connection.await??;
        Ok::<_, anyhow::Error>((status, serde_json::from_slice(&bytes)?))
    })
    .await?
}
fn vertex(external: u8, partition: u32, id: &str) -> Value {
    json!({"external_id":format!("{external:02x}").repeat(16),"mutation":{"type":"create_vertex","partition":partition,"vertex":{"id":id,"labels":["person"],"properties":{}}}})
}
fn context() -> Option<pb::Context> {
    Some(pb::Context {
            identity: None,
        version: 1,
        timeout_millis: 15_000,
    })
}
fn decode_hex(value: &Value) -> Vec<u8> {
    let s = value.as_str().unwrap();
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn session_proto(value: &Value) -> pb::Session {
    pb::Session {
        incarnation: decode_hex(&value["incarnation"]),
        token: decode_hex(&value["token"]),
        version: value["version"].as_str().unwrap().parse().unwrap(),
    }
}
fn session_json(session: &pb::Session) -> Value {
    let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    json!({"incarnation":hex(&session.incarnation),"token":hex(&session.token),"version":session.version.to_string()})
}
fn grpc_query() -> pb::Query {
    pb::Query {
        partition: 7,
        text: "g.V(1).out().path()".into(),
    }
}
fn edge_proto() -> pb::WriteRequest {
    pb::WriteRequest {
        context: context(),
        external_id: vec![3; 16],
        mutation: Some(pb::Mutation {
            kind: Some(pb::mutation::Kind::CreateEdge(pb::CreateEdge {
                edge: Some(pb::Edge {
                    id: 10,
                    from: Some(pb::Entity {
                        partition: 7,
                        id: 1,
                    }),
                    to: Some(pb::Entity {
                        partition: 8,
                        id: 2,
                    }),
                    label: "reports_to".into(),
                    properties: Vec::new(),
                }),
            })),
        }),
    }
}
async fn grpc_channel(address: SocketAddr) -> anyhow::Result<tonic::transport::Channel> {
    Ok(
        tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
            .connect_timeout(BUDGET)
            .timeout(BUDGET)
            .connect()
            .await?,
    )
}
fn fixed_request(session: &pb::FixedSession, text: &str) -> pb::FixedQueryRequest {
    pb::FixedQueryRequest {
        context: context(),
        session: Some(session.clone()),
        query: Some(pb::Query {
            partition: 7,
            text: text.into(),
        }),
    }
}
fn assert_reason(status: &tonic::Status, reason: pb::error_detail::Reason) {
    assert_eq!(
        pb::ErrorDetail::decode(status.details()).unwrap().reason,
        reason as i32
    );
}

// Inspect this process's owned socket inodes, not every socket in its network
// namespace. A hidden ephemeral compatibility listener must fail this check.
#[cfg(target_os = "linux")]
fn assert_only_public_listeners(pid: u32, ready: &Value) -> anyhow::Result<()> {
    let mut inodes = std::collections::BTreeSet::new();
    for fd in fs::read_dir(format!("/proc/{pid}/fd"))? {
        if let Ok(link) = fs::read_link(fd?.path()) {
            let link = link.to_string_lossy();
            if let Some(inode) = link
                .strip_prefix("socket:[")
                .and_then(|v| v.strip_suffix(']'))
            {
                inodes.insert(inode.to_string());
            }
        }
    }
    let mut ports = std::collections::BTreeSet::new();
    for protocol in ["tcp", "tcp6"] {
        for line in fs::read_to_string(format!("/proc/{pid}/net/{protocol}"))?
            .lines()
            .skip(1)
        {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() > 9 && fields[3] == "0A" && inodes.contains(fields[9]) {
                let port = fields[1].rsplit(':').next().context("listener port")?;
                ports.insert(u16::from_str_radix(port, 16)?);
            }
        }
    }
    let expected = ["http", "grpc"]
        .into_iter()
        .map(|name| {
            Ok(ready[name]
                .as_str()
                .context("public address")?
                .parse::<SocketAddr>()?
                .port())
        })
        .collect::<anyhow::Result<std::collections::BTreeSet<_>>>()?;
    anyhow::ensure!(
        ports == expected,
        "unexpected process listeners: {ports:?}, expected {expected:?}"
    );
    Ok(())
}
