//! Actual process boundary: three managed node processes, one public service
//! process and this HTTP client. Not multi-host or global recovery evidence.
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
        node.clone(), identity(shard), "127.0.0.1:0".parse()?, BUDGET,
    ).await?;
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
async fn verify_fixed_session_after_delete(
    client: &mut pb::fixed_session_service_client::FixedSessionServiceClient<
        tonic::transport::Channel,
    >,
    session: &pb::FixedSession,
) -> anyhow::Result<()> {
    use pb::error_detail::Reason;
    let element = |partition, id, edge| pb::QualifiedElement {
        kind: Some(if edge {
            pb::qualified_element::Kind::Edge(pb::Entity { partition, id })
        } else {
            pb::qualified_element::Kind::Vertex(pb::Entity { partition, id })
        }),
    };
    let expected = pb::Row {
        kind: Some(pb::row::Kind::Path(pb::Path {
            elements: vec![
                element(7, 1, false),
                element(7, 10, true),
                element(8, 2, false),
            ],
        })),
    };
    let old = client
        .traverse(fixed_request(session, "g.V(1).out().path()"))
        .await?
        .into_inner();
    assert_eq!(old.session.as_ref(), Some(session));
    assert_eq!(old.query.unwrap().text, "g.V(1).out().path()");
    let data = old.data.unwrap();
    assert_eq!(data.rows, vec![expected.clone()]);
    assert_eq!(data.completeness, pb::Completeness::Unverified as i32);
    let read = client
        .read(fixed_request(session, "g.V(1).outE()"))
        .await?
        .into_inner();
    assert_eq!(read.session.as_ref(), Some(session));
    assert_eq!(
        read.data.unwrap().rows,
        vec![pb::Row {
            kind: Some(pb::row::Kind::Element(element(7, 10, true))),
        }]
    );
    let unsupported = client
        .traverse(fixed_request(session, "g.V(1).inV()"))
        .await
        .unwrap_err();
    assert_reason(&unsupported, Reason::QueryUnsupported);
    let mut changed = session.clone();
    changed.version += 1;
    let wrong_version = client
        .traverse(fixed_request(&changed, "g.V(1)"))
        .await
        .unwrap_err();
    assert_reason(&wrong_version, Reason::IdentityMismatch);
    changed = session.clone();
    changed.incarnation[0] ^= 1;
    let wrong_owner = client
        .read(fixed_request(&changed, "g.V(1)"))
        .await
        .unwrap_err();
    assert_reason(&wrong_owner, Reason::SessionUnavailable);
    // Duplicate starts preserve bags; replay preserves the complete envelope.
    let first_request = pb::FixedPageRequest {
        context: context(),
        session: Some(session.clone()),
        query: Some(pb::Query {
            partition: 7,
            text: "g.V(1,1).out().path()".into(),
        }),
        cursor: None,
        page_size: 1,
    };
    let first = client.page(first_request.clone()).await?.into_inner();
    let page = first.page.as_ref().unwrap();
    assert_eq!((page.offset, page.total), (0, 2));
    assert_eq!(page.rows, vec![expected.clone()]);
    assert_eq!(
        client.page(first_request.clone()).await?.into_inner(),
        first
    );
    let mut next_request = first_request.clone();
    next_request.cursor = page.next.clone();
    assert!(next_request.cursor.is_some());
    let last = client.page(next_request.clone()).await?.into_inner();
    let page = last.page.as_ref().unwrap();
    assert_eq!((page.offset, page.total), (1, 2));
    assert_eq!(page.rows, vec![expected]);
    assert_eq!(page.next, None);
    assert_eq!(client.page(next_request.clone()).await?.into_inner(), last);
    next_request.cursor.as_mut().unwrap().token[0] ^= 1;
    let bad_cursor = client.page(next_request).await.unwrap_err();
    assert_reason(&bad_cursor, Reason::IdentityMismatch);
    let mut changed_query = first_request;
    changed_query.query.as_mut().unwrap().text = "g.V(1)".into();
    let bad_query = client.page(changed_query).await.unwrap_err();
    assert_reason(&bad_query, Reason::IdentityMismatch);
    let fresh = client
        .open(pb::FixedOpenRequest {
            context: context(),
            version: Some(4),
            ttl_millis: 60_000,
        })
        .await?
        .into_inner()
        .session
        .unwrap();
    assert_eq!(fresh.version, 4);
    let current = client
        .traverse(fixed_request(&fresh, "g.V(1).out().path()"))
        .await?
        .into_inner();
    assert!(current.data.unwrap().rows.is_empty());
    let closed = client
        .close(pb::FixedCloseRequest {
            context: context(),
            session: Some(fresh.clone()),
        })
        .await?
        .into_inner();
    assert_eq!(closed.session, Some(fresh.clone()));
    let closed_read = client
        .read(fixed_request(&fresh, "g.V(1)"))
        .await
        .unwrap_err();
    assert_reason(&closed_read, Reason::SessionUnavailable);
    let zero = client
        .open(pb::FixedOpenRequest {
            context: context(),
            version: Some(0),
            ttl_millis: 60_000,
        })
        .await?
        .into_inner()
        .session
        .unwrap();
    assert_eq!(zero.version, 0);
    assert!(client
        .traverse(fixed_request(&zero, "g.V()"))
        .await?
        .into_inner()
        .data
        .unwrap()
        .rows
        .is_empty());
    client
        .close(pb::FixedCloseRequest {
            context: context(),
            session: Some(zero),
        })
        .await?;
    // An unavailable exact future V must fail, never substitute current V.
    let unavailable = client
        .open(pb::FixedOpenRequest {
            context: context(),
            version: Some(100),
            ttl_millis: 60_000,
        })
        .await
        .unwrap_err();
    assert_reason(&unavailable, Reason::RejectedOutcomeUncertain);
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_process_public_http_grpc_consistency_and_restart() -> anyhow::Result<()> {
    public_scenario().await
}
async fn public_scenario() -> anyhow::Result<()> {
    let (coordinator, a, b) = tokio::try_join!(
        NodeProcess::start(9, true),
        NodeProcess::start(7, true),
        NodeProcess::start(8, true)
    )?;
    let directory = tempfile::tempdir()?;
    let config = json!({"binding":binding(),"coordinator":{"endpoint":coordinator.ready.control,"identity":identity(9)},"members":[{"endpoint":a.ready.control,"identity":identity(7),"graph_endpoint":a.ready.graph},{"endpoint":b.ready.control,"identity":identity(8),"graph_endpoint":b.ready.graph}]});
    let config_path = directory.path().join("service.json");
    fs::write(&config_path, serde_json::to_vec(&config)?)?;
    let mut service =
        ServiceProcess::start(&config_path, directory.path().join("service1.stdout")).await?;
    let pids = [
        std::process::id(),
        service.child.id(),
        coordinator.child.id(),
        a.child.id(),
        b.child.id(),
    ];
    anyhow::ensure!(
        pids.into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == 5,
        "client/service/nodes must be independent processes"
    );
    for write in [vertex(1, 7, "1"), vertex(2, 8, "2")] {
        let (status, body) = post(service.address, "/preview1/writes:submit", write).await?;
        assert_eq!(status, 200, "{body}");
    }
    let edge = json!({"external_id":"03030303030303030303030303030303","mutation":{"type":"create_edge","edge":{"id":"10","from":{"partition":7,"id":"1"},"to":{"partition":8,"id":"2"},"label":"reports_to","properties":{}}}});
    let (status, written) = post(service.address, "/preview1/writes:submit", edge.clone()).await?;
    assert_eq!(status, 200, "{written}");
    let (status, replayed) = post(service.address, "/preview1/writes:submit", edge.clone()).await?;
    assert_eq!(status, 200, "{replayed}");
    assert_eq!(replayed, written);
    let channel = grpc_channel(service.grpc).await?;
    let mut grpc_writes = pb::write_service_client::WriteServiceClient::new(channel.clone());
    let mut grpc_queries = pb::query_service_client::QueryServiceClient::new(channel.clone());
    let mut fixed_queries =
        pb::fixed_session_service_client::FixedSessionServiceClient::new(channel);
    // HTTP then gRPC replay use exactly one canonical managed command/decision.
    let grpc_replay = grpc_writes.submit(edge_proto()).await?.into_inner();
    assert_eq!(grpc_replay.external_id, vec![3; 16]);
    assert_eq!(
        grpc_replay.transaction_id,
        Some(decode_hex(&written["result"]["transaction_id"]))
    );
    assert_eq!(
        grpc_replay.status,
        Some(pb::write_response::Status::Committed(
            pb::TerminalDecision {
                ordinal: 3,
                visible: true
            }
        ))
    );
    let query = json!({"partition":7,"text":"g.V(1).out().path()"});
    let(status,opened)=post(service.address,"/preview1/queries:open",json!({"read":{"type":"minimum","version":"3"},"ttl_millis":60000,"query":query,"page_size":1})).await?;
    assert_eq!(status, 200, "{opened}");
    let session = opened["result"]["session"].clone();
    assert_eq!(session["version"], "3");
    assert_eq!(
        opened["result"]["page"]["rows"],
        json!([{"type":"path","elements":[{"type":"vertex","partition":7,"id":"1"},{"type":"edge","partition":7,"id":"10"},{"type":"vertex","partition":8,"id":"2"}]}])
    );
    // FixedSession is a separate contract: capture once, then run multiple
    // bounded queries at that V. It is not the data-local query token above.
    let fixed_session = fixed_queries
        .open(pb::FixedOpenRequest {
            context: context(),
            version: None,
            ttl_millis: 60_000,
        })
        .await?
        .into_inner()
        .session
        .unwrap();
    assert_eq!(fixed_session.version, 3);
    let(status,deleted)=post(service.address,"/preview1/writes:submit",json!({"external_id":"04040404040404040404040404040404","mutation":{"type":"delete_edge","edge":{"partition":7,"id":"10"}}})).await?;
    assert_eq!(status, 200, "{deleted}");
    verify_fixed_session_after_delete(&mut fixed_queries, &fixed_session).await?;
    let fetch = json!({"session":session,"query":query,"cursor":null,"page_size":1});
    let (status, old) = post(service.address, "/preview1/queries:fetch", fetch.clone()).await?;
    assert_eq!(status, 200, "{old}");
    assert_eq!(old["result"]["page"], opened["result"]["page"]);
    let old_grpc = grpc_queries
        .fetch(pb::FetchRequest {
            context: context(),
            session: Some(session_proto(&session)),
            query: Some(grpc_query()),
            cursor: None,
            page_size: 1,
        })
        .await?
        .into_inner();
    assert_eq!(old_grpc.session, Some(session_proto(&session)));
    let old_page = old_grpc.page.unwrap();
    assert_eq!(old_page.total, 1);
    assert!(
        matches!(old_page.rows[0].kind.as_ref(),Some(pb::row::Kind::Path(path)) if path.elements.len()==3)
    );
    // gRPC-opened session is the same owner-bound state seen by HTTP fetch/close.
    let grpc_open = grpc_queries
        .open(pb::OpenRequest {
            context: context(),
            read: Some(pb::ReadSelector {
                kind: Some(pb::read_selector::Kind::Minimum(4)),
            }),
            ttl_millis: 60_000,
            query: Some(grpc_query()),
            page_size: 1,
        })
        .await?
        .into_inner();
    let grpc_session = grpc_open.session.unwrap();
    assert_eq!(grpc_open.page.unwrap().total, 0);
    let cross_fetch =
        json!({"session":session_json(&grpc_session),"query":query,"cursor":null,"page_size":1});
    let (status, cross) = post(service.address, "/preview1/queries:fetch", cross_fetch).await?;
    assert_eq!(status, 200, "{cross}");
    assert_eq!(cross["result"]["page"]["total"], 0);
    assert_eq!(
        post(
            service.address,
            "/preview1/queries:close",
            json!({"session":session_json(&grpc_session)})
        )
        .await?
        .0,
        200
    );
    let closed = grpc_queries
        .fetch(pb::FetchRequest {
            context: context(),
            session: Some(grpc_session),
            query: Some(grpc_query()),
            cursor: None,
            page_size: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(
        pb::ErrorDetail::decode(closed.details())?.reason,
        pb::error_detail::Reason::SessionUnavailable as i32
    );
    let fresh = json!({"read":{"type":"minimum","version":"4"},"ttl_millis":60000,"query":query,"page_size":1});
    let (status, new) = post(service.address, "/preview1/queries:open", fresh.clone()).await?;
    assert_eq!(status, 200, "{new}");
    assert_eq!(new["result"]["page"]["total"], 0);
    // Conversely, gRPC close invalidates a session opened by HTTP.
    grpc_queries
        .close(pb::CloseRequest {
            context: context(),
            session: Some(session_proto(&new["result"]["session"])),
        })
        .await?;
    let (status, closed) = post(
        service.address,
        "/preview1/queries:fetch",
        json!({"session":new["result"]["session"],"query":query,"cursor":null,"page_size":1}),
    )
    .await?;
    assert_eq!(status, 410, "{closed}");
    drop(grpc_writes);
    drop(grpc_queries);
    drop(fixed_queries);
    service.graceful_stop().await?;
    let mut service =
        ServiceProcess::start(&config_path, directory.path().join("service2.stdout")).await?;
    let (status, stale) = post(service.address, "/preview1/queries:fetch", fetch).await?;
    assert_eq!(status, 410, "{stale}");
    let channel = grpc_channel(service.grpc).await?;
    let mut grpc_queries = pb::query_service_client::QueryServiceClient::new(channel.clone());
    let stale_rpc = grpc_queries
        .fetch(pb::FetchRequest {
            context: context(),
            session: Some(session_proto(&session)),
            query: Some(grpc_query()),
            cursor: None,
            page_size: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(
        pb::ErrorDetail::decode(stale_rpc.details())?.reason,
        pb::error_detail::Reason::SessionUnavailable as i32
    );
    let mut fixed_queries =
        pb::fixed_session_service_client::FixedSessionServiceClient::new(channel.clone());
    let stale_fixed = fixed_queries
        .traverse(fixed_request(&fixed_session, "g.V(1).out().path()"))
        .await
        .unwrap_err();
    assert_reason(&stale_fixed, pb::error_detail::Reason::SessionUnavailable);
    drop(fixed_queries);
    let mut grpc_writes = pb::write_service_client::WriteServiceClient::new(channel);
    assert_eq!(
        grpc_writes.status(edge_proto()).await?.into_inner(),
        grpc_replay
    );

    let (status, after_restart) = post(service.address, "/preview1/queries:open", fresh).await?;
    assert_eq!(status, 200, "{after_restart}");
    assert_eq!(after_restart["result"]["page"]["total"], 0);
    let (status, decision) = post(service.address, "/preview1/writes:status", edge).await?;
    assert_eq!(status, 200, "{decision}");
    assert_eq!(decision, written);
    drop(grpc_writes);
    drop(grpc_queries);
    service.graceful_stop().await?;
    // Deliberately wrong topology is rejected before publishing any listener.
    let mut invalid = config;
    invalid["binding"]["cohort"]["coordinator_epoch"] = json!(2);
    let bad_path = directory.path().join("bad.json");
    fs::write(&bad_path, serde_json::to_vec(&invalid)?)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_graph-service"));
    let failure = command
        .args([
            "--allow-insecure-development",
            "--config",
            bad_path.to_str().unwrap(),
            "--http",
            "127.0.0.1:0",
            "--timeout-ms",
            "1000",
        ])
        .output()?;
    assert!(!failure.status.success());
    assert!(failure.stdout.is_empty());
    // The HTTP listener must not be announced if the optional second bind fails.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0")?;
    let grpc_address = occupied.local_addr()?.to_string();
    let mut command = Command::new(env!("CARGO_BIN_EXE_graph-service"));
    let failure = command
        .args([
            "--allow-insecure-development",
            "--config",
            config_path.to_str().unwrap(),
            "--http",
            "127.0.0.1:0",
            "--grpc",
            &grpc_address,
            "--timeout-ms",
            "1000",
        ])
        .output()?;
    assert!(!failure.status.success());
    assert!(failure.stdout.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_never_falls_back_to_legacy_internal_endpoints() -> anyhow::Result<()> {
    let (coordinator, a, b) = tokio::try_join!(
        NodeProcess::start(9, false),
        NodeProcess::start(7, false),
        NodeProcess::start(8, false)
    )?;
    let (grpc_coordinator, grpc_a, grpc_b) = tokio::try_join!(
        NodeProcess::start(9, true),
        NodeProcess::start(7, true),
        NodeProcess::start(8, true)
    )?;
    // The shipped legacy managed listener is gone. This isolated protocol
    // rejection fixture speaks only the old frame prefix and records any
    // attempted fallback; genuine graph-side legacy coverage remains below.
    let old_control = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let old_control_addr = old_control.local_addr()?;
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = attempts.clone();
    let rejection_peer = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let (mut socket, _) = old_control.accept().await.unwrap();
            let mut first = [0; 4];
            if socket.read_exact(&mut first).await.is_err() { continue; }
            assert_eq!(&first, b"PRI ", "must not retry using the retired JSON framing");
            observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = socket.write_all(&[0, 0, 0, 2, b'{', b'}']).await;
        }
    });
    let directory = tempfile::tempdir()?;
    // Independently reject legacy control, legacy graph, and both. A failure
    // on one path must not conceal fallback on the other.
    for (legacy_control, legacy_graph) in [(true, true), (true, false), (false, true)] {
        let (c, ca, cb) = if legacy_control {
            (&coordinator, &a, &b)
        } else {
            (&grpc_coordinator, &grpc_a, &grpc_b)
        };
        let (ga, gb) = if legacy_graph {
            (&a, &b)
        } else {
            (&grpc_a, &grpc_b)
        };
        let config = json!({"binding":binding(),"coordinator":{"endpoint":if legacy_control { old_control_addr } else { c.ready.control },"identity":identity(9)},"members":[{"endpoint":if legacy_control { old_control_addr } else { ca.ready.control },"identity":identity(7),"graph_endpoint":ga.ready.graph},{"endpoint":if legacy_control { old_control_addr } else { cb.ready.control },"identity":identity(8),"graph_endpoint":gb.ready.graph}]});
        let path = directory.path().join("legacy.json");
        fs::write(&path, serde_json::to_vec(&config)?)?;
        if !legacy_control {
            // Connecting a Channel only proves a transport connection, not a
            // typed graph service. The first graph operation must fail rather
            // than retrying the legacy protocol. No fake read/write probe is
            // added to startup merely to test readiness.
            let service =
                ServiceProcess::start(&path, directory.path().join("mixed.stdout")).await?;
            let (status, body) = post(service.address, "/preview1/queries:open",
                json!({"read":{"type":"latest"},"ttl_millis":1000,"query":{"partition":7,"text":"g.V(1)"},"page_size":1})).await?;
            assert_eq!(
                status, 503,
                "wrong graph transport must be unavailable: {body}"
            );
            drop(service);
            continue;
        }
        let mut command = Command::new(env!("CARGO_BIN_EXE_graph-service"));
        command.args([
            "--allow-insecure-development",
            "--config",
            path.to_str().context("path")?,
            "--http",
            "127.0.0.1:0",
            "--grpc",
            "127.0.0.1:0",
            "--timeout-ms",
            "1000",
        ]);
        let output = directory.path().join("rejected.stdout");
        let mut child = command
            .stdout(Stdio::from(fs::File::create(&output)?))
            .stderr(Stdio::inherit())
            .spawn()?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                other => {
                    let _ = child.kill();
                    let _ = child.wait();
                    anyhow::bail!("legacy service did not exit within budget: {other:?}");
                }
            }
        };
        assert!(!status.success());
        assert!(
            fs::read(&output)?.is_empty(),
            "legacy endpoints must never announce serving"
        );
    }
    assert!(attempts.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(!rejection_peer.is_finished(), "legacy protocol fixture must observe no fallback");
    rejection_peer.abort();
    let _ = rejection_peer.await;
    Ok(())
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
