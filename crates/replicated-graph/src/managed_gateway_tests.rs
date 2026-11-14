use super::invocation::OpenDelivery;
use super::*;
// The client intentionally has a process-wide eight-call cap. Keep independent
// fixture tests from consuming each other's admission budget under parallel CI.
static TEST_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
use d2_raft::transaction_decisions::{Command, Decision, Journal, Member};
use d2_raft::{
    participant_driver, versioned_reads, Cluster, D1Command, D1Error, D1Result, LogCommand,
};
// Real HTTP/2 frames, including explicit reset/half-frame injection. These
// helpers deliberately bypass the normal client only for transport failures.
struct RpcDriver(tokio::task::JoinHandle<()>);
impl Drop for RpcDriver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn public_context() -> Option<public_proto::Context> {
    Some(public_proto::Context {
        version: 1,
        timeout_millis: 15_000,
        ..Default::default()
    })
}
fn grpc_frame(message: &impl prost::Message) -> Vec<u8> {
    let encoded = message.encode_to_vec();
    let mut bytes = Vec::with_capacity(encoded.len() + 5);
    bytes.push(0);
    bytes.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    bytes.extend(encoded);
    bytes
}
async fn raw_rpc(
    address: SocketAddr,
    method: &str,
    bytes: Vec<u8>,
    end_stream: bool,
) -> (
    h2::client::ResponseFuture,
    h2::SendStream<tonic::codegen::Bytes>,
    RpcDriver,
) {
    let socket = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = h2::client::handshake(socket).await.unwrap();
    let driver = RpcDriver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let request = tonic::codegen::http::Request::builder()
        .method("POST")
        .uri(format!("http://{address}/{method}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("grpc-timeout", "15000m")
        .body(())
        .unwrap();
    let (response, mut body) = sender.send_request(request, false).unwrap();
    body.send_data(bytes.into(), end_stream).unwrap();
    (response, body, driver)
}
async fn rejected_rpc(response: h2::client::ResponseFuture) {
    // Transport resets and non-OK gRPC statuses both reject the malformed call.
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let response = response.await?;
        let initial = response.headers().get("grpc-status").cloned();
        let mut body = response.into_body();
        while let Some(data) = body.data().await {
            let data = data?;
            body.flow_control().release_capacity(data.len())?;
        }
        let trailers = body.trailers().await?;
        let status = trailers
            .as_ref()
            .and_then(|h| h.get("grpc-status"))
            .cloned()
            .or(initial);
        Ok::<_, h2::Error>(status)
    })
    .await
    .expect("rejected RPC must terminate within deadline");
    if let Ok(status) = result {
        assert!(status.is_some_and(|status| status != "0"));
    }
}
async fn send_write_lost_ack(g: &Gateway, f: &Fixture, write: &Write) {
    let message = grpc_client::encode_write(public_context(), write).unwrap();
    let (response, mut body, driver) = raw_rpc(
        g.local_addr(),
        "velagraph.public.managed.v1.WriteService/Submit",
        grpc_frame(&message),
        true,
    )
    .await;
    // Wait for durable admission, not an arbitrary sleep or socket write. No
    // response is observed, so recovery must preserve this exact external ID.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if journal(f).await.request(write.external_id).is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    body.send_reset(h2::Reason::CANCEL);
    drop(response);
    drop(driver);
}
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn binding() -> Cohort {
    Cohort {
        profile: Default::default(),
        coordinator_shard: 9,
        coordinator_epoch: 1,
        members: vec![
            Member {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            },
            Member {
                shard: 8,
                epoch: 1,
                mode: [8; 16],
            },
        ],
    }
}
fn target(shard: u64) -> versioned_reads::Target {
    versioned_reads::Target {
        shard,
        epoch: 1,
        mode: [shard as u8; 16],
    }
}
fn limit() -> versioned_reads::ReadLimit {
    versioned_reads::ReadLimit {
        rows: 1000,
        bytes: 1024 * 1024,
        operations: 10000,
    }
}
async fn submit(node: &NodeHandle, command: D1Command) -> std::result::Result<D1Result, D1Error> {
    node.raft
        .client_write(LogCommand::D1V1(command))
        .await
        .unwrap()
        .data
        .d1
        .unwrap()
}
async fn start(cluster: &Cluster, shard: u64) -> NodeHandle {
    let node = cluster.in_group(shard).start(1).await.unwrap();
    initialize(node, shard).await
}
async fn initialize(node: NodeHandle, shard: u64) -> NodeHandle {
    node.raft
        .initialize(BTreeMap::from([(
            1,
            openraft::BasicNode {
                addr: format!("inproc://{shard}/1"),
            },
        )]))
        .await
        .unwrap();
    node.raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |m| m.current_leader == Some(1) && m.last_applied.is_some(),
            "ready",
        )
        .await
        .unwrap();
    submit(&node, D1Command::Activate { shard, epoch: 1 })
        .await
        .unwrap();
    submit(
        &node,
        D1Command::EnableManagedWrites {
            shard,
            epoch: 1,
            expected_sequence: 0,
            mode_id: [shard as u8; 16],
        },
    )
    .await
    .unwrap();
    if shard == 9 {
        submit(
            &node,
            D1Command::EnableCoordinator {
                shard,
                epoch: 1,
                mode_id: [9; 16],
            },
        )
        .await
        .unwrap();
        submit(
            &node,
            D1Command::Coordinator {
                shard,
                epoch: 1,
                mode_id: [9; 16],
                command: Command::BindCohort { cohort: binding() },
            },
        )
        .await
        .unwrap();
    } else {
        submit(
            &node,
            D1Command::EnableParticipant {
                shard,
                epoch: 1,
                mode_id: [shard as u8; 16],
                authority: authority(),
            },
        )
        .await
        .unwrap();
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .apply(Command::BindCohort { cohort: binding() })
            .unwrap();
        submit(
            &node,
            D1Command::EnableVersionedReads {
                shard,
                epoch: 1,
                mode_id: [shard as u8; 16],
                journal,
            },
        )
        .await
        .unwrap();
    }
    node
}

struct Fixture {
    cluster: Cluster,
    nodes: Vec<Arc<NodeHandle>>,
}
impl Fixture {
    async fn new() -> Self {
        let cluster = Cluster::new();
        let c = Arc::new(start(&cluster, 9).await);
        let a = Arc::new(start(&cluster, 7).await);
        let b = Arc::new(start(&cluster, 8).await);
        Self {
            cluster,
            nodes: vec![c, a, b],
        }
    }
    fn config(&self) -> Config {
        Config {
            coordinator: self.nodes[0].clone(),
            binding: bound(),
            members: self.nodes[1..].to_vec(),
        }
    }
    async fn gateway(&self) -> Gateway {
        Gateway::bind(
            self.config(),
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(15),
        )
        .await
        .unwrap()
    }
    async fn stop(self) {
        for (node, shard) in self.nodes.iter().zip([9, 7, 8]) {
            self.cluster.in_group(shard).shutdown(node).await;
        }
    }
}
fn bound() -> Binding {
    Binding {
        authority: authority(),
        cohort: binding(),
        mapping: Mapping::PartitionEqualsShardV1,
    }
}
fn vertex(id: u64) -> VertexData {
    VertexData {
        id,
        labels: vec!["person".into()],
        properties: BTreeMap::new(),
    }
}
fn create(external: u8, partition: u32, id: u64) -> Write {
    Write {
        external_id: [external; 16],
        mutation: Mutation::CreateVertex {
            partition,
            vertex: vertex(id),
        },
    }
}
fn entity(partition: u32, id: u64) -> Entity {
    Entity { partition, id }
}
fn edge() -> EdgeData {
    EdgeData {
        id: 10,
        from: entity(7, 1),
        to: entity(8, 1),
        label: "knows".into(),
        properties: BTreeMap::new(),
    }
}
fn patch(value: Value) -> Patch {
    Patch {
        set: BTreeMap::from([("weight".into(), value)]),
        remove: BTreeSet::new(),
    }
}
async fn call(g: &Gateway, operation: Operation) -> Result<Reply> {
    request(g.local_addr(), bound(), operation, Duration::from_secs(15)).await
}
fn committed(reply: &Reply) -> ([u8; 16], u64) {
    match reply {
        Reply::Write {
            transaction_id: Some(id),
            status:
                WriteStatus::Committed {
                    ordinal,
                    visible: true,
                },
            ..
        } => (*id, *ordinal),
        other => panic!("not visibly committed: {other:?}"),
    }
}
async fn open(g: &Gateway, version: Option<u64>) -> Session {
    match call(
        g,
        Operation::OpenSession {
            version,
            ttl_millis: 60000,
        },
    )
    .await
    .unwrap()
    {
        Reply::OpenSession(s) => s,
        _ => panic!(),
    }
}
fn query(partition: u32, text: &str) -> Query {
    Query {
        partition,
        text: text.into(),
    }
}
fn e(partition: u32, id: u64) -> QueryRow {
    QueryRow::Element(crate::remote::query::QualifiedElement::Edge { partition, id })
}
fn v(partition: u32, id: u64) -> QueryRow {
    QueryRow::Element(crate::remote::query::QualifiedElement::Vertex { partition, id })
}
fn value(source: crate::remote::query::QualifiedElement, value: Value) -> QueryRow {
    QueryRow::Value {
        source: Some(source),
        value,
    }
}
async fn read(g: &Gateway, session: &Session, query: Query) -> Vec<QueryRow> {
    match call(
        g,
        Operation::Read {
            session: session.clone(),
            query,
        },
    )
    .await
    .unwrap()
    {
        Reply::Read { data, .. } => {
            assert_eq!(data.completeness, QueryCompleteness::Unverified);
            data.rows
        }
        _ => panic!(),
    }
}
async fn close(g: &Gateway, session: Session) {
    call(g, Operation::Close { session }).await.unwrap();
}
async fn journal(f: &Fixture) -> Journal {
    f.nodes[0].raft.ensure_linearizable().await.unwrap();
    f.nodes[0]
        .state_machine
        .coordinator_journal(9, 1, [9; 16])
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_typed_binding_and_nonce_reject_before_write_admission() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    for case in 0..3 {
        let mut wrong = bound();
        match case {
            0 => wrong.authority.mode[0] ^= 1,
            1 => wrong.cohort.members[0].epoch += 1,
            _ => wrong.cohort.coordinator_epoch += 1,
        }
        let write = create(90 + case, 7, 90 + u64::from(case));
        assert_eq!(
            request(
                g.local_addr(),
                wrong,
                Operation::Submit(write.clone()),
                Duration::from_secs(2)
            )
            .await,
            Err(Error::Identity)
        );
        assert!(journal(&f).await.request(write.external_id).is_none());
        assert!(matches!(
            call(&g, Operation::Status(write)).await.unwrap(),
            Reply::Write {
                status: WriteStatus::Absent,
                ..
            }
        ));
    }
    let write = create(99, 7, 99);
    let context = Some(public_proto::Context {
        version: 2,
        timeout_millis: 2000,
        identity: Some(public_proto::CallIdentity {
            request_id: vec![1; 15],
            binding: Some(grpc_client::encode_binding(&bound())),
        }),
    });
    let mut client = public_proto::write_service_client::WriteServiceClient::connect(format!(
        "http://{}",
        g.local_addr()
    ))
    .await
    .unwrap();
    let error = client
        .submit(grpc_client::encode_write(context, &write).unwrap())
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(journal(&f).await.request(write.external_id).is_none());
    assert!(matches!(
        call(&g, Operation::Status(write)).await.unwrap(),
        Reply::Write {
            status: WriteStatus::Absent,
            ..
        }
    ));
    assert_eq!(journal(&f).await.resolved_prefix(), 0);
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test]
async fn typed_client_admission_precedes_connection() {
    let _test = TEST_GATE.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reserved = CLIENT_ADMISSION
        .acquire_many(MAX_CONNECTIONS as u32)
        .await
        .unwrap();
    assert_eq!(
        request(
            listener.local_addr().unwrap(),
            bound(),
            Operation::Status(create(1, 7, 1)),
            Duration::from_secs(1)
        )
        .await,
        Err(Error::Capacity)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    drop(reserved);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_rejects_non_loopback_before_binding() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    // No external listener is opened: this address must fail the public
    // adapter's policy check, not reach socket bind or graph acquisition.
    let result = Gateway::bind(
        f.config(),
        "0.0.0.0:0".parse().unwrap(),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(result, Err(Error::Protocol)));
    assert_eq!(journal(&f).await.resolved_prefix(), 0);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_mutations_fixed_versions_concurrent_identity_and_disconnect() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let a = create(1, 7, 1);
    let (first, retry) = tokio::join!(
        call(&g, Operation::Submit(a.clone())),
        call(&g, Operation::Submit(a.clone()))
    );
    assert_eq!(first.unwrap(), retry.unwrap());
    assert_eq!(journal(&f).await.resolved_prefix(), 1);
    committed(&call(&g, Operation::Submit(create(2, 8, 1))).await.unwrap());
    let edge_write = Write {
        external_id: [3; 16],
        mutation: Mutation::CreateEdge { edge: edge() },
    };
    committed(
        &call(&g, Operation::Submit(edge_write.clone()))
            .await
            .unwrap(),
    );
    let old = open(&g, None).await;
    let update = Write {
        external_id: [4; 16],
        mutation: Mutation::PatchEdge {
            edge: entity(7, 10),
            patch: patch(Value::Integer(1)),
        },
    };
    let unchanged = old.clone();
    let read_during = async {
        for _ in 0..4 {
            assert_eq!(
                read(&g, &unchanged, query(7, "g.E(10).values('weight')")).await,
                vec![]
            );
        }
    };
    let (reply, ()) = tokio::join!(call(&g, Operation::Submit(update.clone())), read_during);
    committed(&reply.unwrap());
    let mut different = update.clone();
    different.mutation = Mutation::PatchEdge {
        edge: entity(7, 10),
        patch: patch(Value::Real(1.0)),
    };
    assert_eq!(
        call(&g, Operation::Submit(different)).await,
        Err(Error::Identity)
    );
    let mut different = update.clone();
    different.mutation = Mutation::DeleteEdge {
        edge: entity(7, 10),
    };
    assert_eq!(
        call(&g, Operation::Submit(different)).await,
        Err(Error::Identity)
    );
    let new = open(&g, None).await;
    assert_eq!(
        read(&g, &new, query(7, "g.E(10).values('weight')")).await,
        vec![value(
            crate::remote::query::QualifiedElement::Edge {
                partition: 7,
                id: 10
            },
            Value::Integer(1)
        )]
    );
    assert_eq!(
        read(&g, &new, query(8, "g.V(1).inE()")).await,
        vec![e(7, 10)]
    );
    let vertex_patch = Write {
        external_id: [5; 16],
        mutation: Mutation::PatchVertex {
            vertex: entity(7, 1),
            patch: patch(Value::Null),
        },
    };
    committed(&call(&g, Operation::Submit(vertex_patch)).await.unwrap());
    let deletion = Write {
        external_id: [6; 16],
        mutation: Mutation::DeleteEdge {
            edge: entity(7, 10),
        },
    };
    send_write_lost_ack(&g, &f, &deletion).await;
    let deleted = call(&g, Operation::Submit(deletion.clone())).await.unwrap();
    let (_, ordinal) = committed(&deleted);
    assert_eq!(ordinal, 6);
    assert_eq!(
        call(&g, Operation::Abort(deletion.clone())).await.unwrap(),
        deleted
    );
    assert_eq!(
        call(&g, Operation::Status(deletion)).await.unwrap(),
        deleted
    );
    close(&g, new).await;
    let latest = open(&g, None).await;
    assert_eq!(read(&g, &latest, query(7, "g.E(10)")).await, vec![]);
    assert_eq!(read(&g, &latest, query(7, "g.V(1).outE()")).await, vec![]);
    assert_eq!(
        read(&g, &latest, query(7, "g.V(1).values('weight')")).await,
        vec![value(
            crate::remote::query::QualifiedElement::Vertex {
                partition: 7,
                id: 1
            },
            Value::Null
        )]
    );
    assert_eq!(read(&g, &old, query(7, "g.E(10)")).await, vec![e(7, 10)]);
    close(&g, old).await;
    close(&g, latest).await;
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_apply_refuses_explicit_new_version_and_preserves_commit() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    committed(&call(&g, Operation::Submit(create(2, 8, 1))).await.unwrap());
    let old = open(&g, None).await;
    let write = Write {
        external_id: [3; 16],
        mutation: Mutation::CreateEdge { edge: edge() },
    };
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    let candidate = write.mutation.prepare(&snapshot).unwrap();
    let plan = graph
        .admit_request(
            write.external_id,
            canonical(&bound(), &write.mutation).unwrap(),
            &candidate,
        )
        .await
        .unwrap();
    let record = journal(&f)
        .await
        .request(write.external_id)
        .unwrap()
        .clone();
    let durable = d2_raft::request_plans::Plan::decode(&record.payload).unwrap();
    for (part, node) in durable.parts.iter().zip(&f.nodes[1..]) {
        participant_driver::prepare(
            &f.nodes[0],
            node,
            authority(),
            1,
            part.descriptor.clone(),
            part.request.clone(),
        )
        .await
        .unwrap();
    }
    submit(
        &f.nodes[0],
        D1Command::Coordinator {
            shard: 9,
            epoch: 1,
            mode_id: [9; 16],
            command: Command::Decide {
                id: plan.id(),
                decision: Decision::Commit,
            },
        },
    )
    .await
    .unwrap();
    let ordinal = journal(&f).await.transaction(plan.id()).unwrap().ordinal;
    versioned_reads::advance(&f.nodes[0], &f.nodes[1], authority(), 1, target(7), ordinal)
        .await
        .unwrap();
    assert!(call(
        &g,
        Operation::OpenSession {
            version: Some(ordinal),
            ttl_millis: 10000
        }
    )
    .await
    .is_err());
    let common = open(&g, None).await;
    assert_eq!(common.version, old.version);
    assert_eq!(read(&g, &common, query(7, "g.E(10)")).await, vec![]);
    let status = call(&g, Operation::Status(write.clone())).await.unwrap();
    assert!(matches!(
        status,
        Reply::Write {
            status: WriteStatus::Committed { visible: false, .. },
            ..
        }
    ));
    // Status must not advance, decide, or create a transaction.
    assert_eq!(open(&g, None).await.version, old.version);
    committed(&call(&g, Operation::Abort(write)).await.unwrap());
    let latest = open(&g, Some(ordinal)).await;
    assert_eq!(read(&g, &latest, query(7, "g.E(10)")).await, vec![e(7, 10)]);
    g.shutdown().await;
    drop(config);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_budgets_expiry_malformed_fields_and_oversized_frames() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let mut sessions = Vec::new();
    for _ in 0..MAX_SESSIONS {
        sessions.push(open(&g, None).await);
    }
    assert_eq!(g.retained_payload_charge(), MAX_RETAINED_BYTES);
    assert_eq!(
        call(
            &g,
            Operation::OpenSession {
                version: None,
                ttl_millis: 500
            }
        )
        .await,
        Err(Error::Capacity)
    );
    assert_eq!(
        call(&g, Operation::Submit(create(1, 7, 1))).await,
        Err(Error::Capacity)
    );
    assert_eq!(journal(&f).await.resolved_prefix(), 0);
    let session = sessions.pop().unwrap();
    let mut bad = session.clone();
    bad.version += 1;
    assert_eq!(
        call(
            &g,
            Operation::Read {
                session: bad,
                query: query(7, "g.V(1)")
            }
        )
        .await,
        Err(Error::Identity)
    );
    let mut bad = session.clone();
    bad.token[0] ^= 1;
    assert_eq!(
        call(&g, Operation::Close { session: bad }).await,
        Err(Error::SessionUnavailable)
    );
    close(&g, session.clone()).await;
    assert_eq!(
        call(&g, Operation::Close { session }).await,
        Err(Error::SessionUnavailable)
    );
    let expired = match call(
        &g,
        Operation::OpenSession {
            version: None,
            ttl_millis: 100,
        },
    )
    .await
    .unwrap()
    {
        Reply::OpenSession(s) => s,
        _ => panic!(),
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        call(
            &g,
            Operation::Read {
                session: expired,
                query: query(7, "g.V(1)")
            }
        )
        .await,
        Err(Error::SessionUnavailable)
    );
    assert_eq!(g.retained_payload_charge(), 3 * SESSION_CHARGE);
    let mut wrong = bound();
    wrong.cohort.members[0].epoch += 1;
    assert_eq!(
        request(
            g.local_addr(),
            wrong,
            Operation::Status(create(1, 7, 1)),
            Duration::from_secs(2)
        )
        .await,
        Err(Error::Identity)
    );
    let req = public_proto::FixedQueryRequest {
        context: public_context(),
        session: Some(public_proto::FixedSession {
            incarnation: sessions[0].incarnation.to_vec(),
            token: sessions[0].token.to_vec(),
            version: sessions[0].version,
        }),
        query: Some(public_proto::Query {
            partition: 7,
            text: "g.V(1)".into(),
        }),
        ..Default::default()
    };
    let mut payload = prost::Message::encode_to_vec(&req);
    // Unknown field 127 must fail the strict protobuf prescan.
    payload.extend_from_slice(&[0xf8, 0x07, 0x01]);
    let mut frame = vec![0];
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend(payload);
    let (response, _body, _driver) = raw_rpc(
        g.local_addr(),
        "velagraph.public.managed.v1.FixedSessionService/Read",
        frame,
        true,
    )
    .await;
    rejected_rpc(response).await;
    let mut oversized = vec![0];
    oversized.extend_from_slice(&(MAX_REQUEST_BYTES as u32 + 1).to_be_bytes());
    let (response, _body, _driver) = raw_rpc(
        g.local_addr(),
        "velagraph.public.managed.v1.FixedSessionService/Read",
        oversized,
        true,
    )
    .await;
    rejected_rpc(response).await;
    for session in sessions {
        close(&g, session).await;
    }
    assert_eq!(g.retained_payload_charge(), 0);
    // A trusted local plan seeds escaped data whose remote JSON would exceed 1 MiB.
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    let mut large = Vertex::new(1);
    large
        .properties
        .insert("large".into(), Value::Text("\0".repeat(180000)));
    let plan = snapshot.prepare_vertex(7, &large).unwrap();
    graph.execute(&plan).await.unwrap();
    let session = open(&g, None).await;
    assert_eq!(
        call(
            &g,
            Operation::Read {
                session: session.clone(),
                query: query(7, "g.V(1).values('large')")
            }
        )
        .await,
        Err(Error::Capacity)
    );
    close(&g, session).await;
    g.shutdown().await;
    drop(config);
    f.stop().await;
}

#[test]
fn canonical_typed_identity_and_strict_fields() {
    let binding = bound();
    assert_eq!(
        canonical(&binding, &create(1, 7, 1).mutation).unwrap(),
        include_bytes!("../tests/fixtures/managed_gateway_command_v1.json")
    );
    let int = Mutation::PatchVertex {
        vertex: entity(7, 1),
        patch: patch(Value::Integer(1)),
    };
    let real = Mutation::PatchVertex {
        vertex: entity(7, 1),
        patch: patch(Value::Real(1.0)),
    };
    assert_ne!(
        canonical(&binding, &int).unwrap(),
        canonical(&binding, &real).unwrap()
    );
    assert!(canonical(
        &binding,
        &Mutation::PatchVertex {
            vertex: entity(7, 1),
            patch: patch(Value::Real(f64::NAN))
        }
    )
    .is_err());
    let req = Request {
        version: VERSION,
        request_id: [1; 16],
        binding: binding.clone(),
        operation: Operation::Status(create(1, 7, 1)),
    };
    let mut value = serde_json::to_value(&req).unwrap();
    value["operation"]["Status"]["mutation"]["CreateVertex"]["vertex"]["unexpected"] =
        serde_json::json!(true);
    assert!(serde_json::from_value::<Request>(value).is_err());
    let response = Response {
        version: VERSION,
        request_id: [2; 16],
        binding,
        result: Ok(Reply::Write {
            external_id: [1; 16],
            transaction_id: None,
            status: WriteStatus::Absent,
        }),
    };
    assert_eq!(validate_reply(&req, response), Err(Error::Identity));
    assert_eq!(encode(&"a".repeat(1000), 100), Err(Error::Capacity));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_restart_recovers_original_partial_plan_and_rejects_old_tokens() {
    let _test = TEST_GATE.lock().await;
    let cluster = Cluster::new();
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let mut nodes = Vec::new();
    for (shard, dir) in [9, 7, 8].into_iter().zip(&dirs) {
        nodes.push(Arc::new(
            initialize(
                cluster
                    .in_group(shard)
                    .start_durable(1, dir.path())
                    .await
                    .unwrap(),
                shard,
            )
            .await,
        ));
    }
    let f = Fixture { cluster, nodes };
    let g = f.gateway().await;
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    committed(&call(&g, Operation::Submit(create(2, 8, 1))).await.unwrap());
    let old = open(&g, None).await;
    let write = Write {
        external_id: [3; 16],
        mutation: Mutation::CreateEdge { edge: edge() },
    };
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    let candidate = write.mutation.prepare(&snapshot).unwrap();
    let original = graph
        .admit_request(
            write.external_id,
            canonical(&bound(), &write.mutation).unwrap(),
            &candidate,
        )
        .await
        .unwrap();
    let record = journal(&f)
        .await
        .request(write.external_id)
        .unwrap()
        .clone();
    let durable = d2_raft::request_plans::Plan::decode(&record.payload).unwrap();
    let part = &durable.parts[0];
    participant_driver::prepare(
        &f.nodes[0],
        &f.nodes[1],
        authority(),
        1,
        part.descriptor.clone(),
        part.request.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        call(&g, Operation::Status(write.clone())).await.unwrap(),
        Reply::Write {
            status: WriteStatus::Pending,
            ..
        }
    ));
    g.shutdown().await;
    drop(graph);
    drop(config);
    drop(snapshot);
    for (node, shard) in f.nodes.iter().zip([9, 7, 8]) {
        f.cluster.in_group(shard).shutdown(node).await;
    }
    let Fixture { cluster, nodes } = f;
    drop(nodes);
    let mut nodes = Vec::new();
    for (shard, dir) in [9, 7, 8].into_iter().zip(&dirs) {
        let node = cluster
            .in_group(shard)
            .start_durable(1, dir.path())
            .await
            .unwrap();
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader == Some(1), "reopened")
            .await
            .unwrap();
        nodes.push(Arc::new(node));
    }
    let f = Fixture { cluster, nodes };
    let g = f.gateway().await;
    assert_eq!(
        call(
            &g,
            Operation::Read {
                session: old,
                query: query(7, "g.E(10)")
            }
        )
        .await,
        Err(Error::SessionUnavailable)
    );
    let reply = call(&g, Operation::Submit(write.clone())).await.unwrap();
    let (id, ordinal) = committed(&reply);
    assert_eq!(id, original.id());
    assert_eq!(ordinal, 3);
    assert_eq!(
        journal(&f).await.request(write.external_id).unwrap(),
        &record
    );
    let session = open(&g, Some(3)).await;
    assert_eq!(
        read(&g, &session, query(7, "g.E(10)")).await,
        vec![e(7, 10)]
    );
    // Replacing only the gateway still recovers the original Plan after Commit.
    g.shutdown().await;
    let g = f.gateway().await;
    assert_eq!(
        call(&g, Operation::Submit(write.clone())).await.unwrap(),
        reply
    );
    assert_eq!(call(&g, Operation::Abort(write)).await.unwrap(), reply);
    assert_eq!(
        call(&g, Operation::Close { session }).await,
        Err(Error::SessionUnavailable)
    );
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_admitted_request_remains_pending_until_explicit_abort() {
    let _test = TEST_GATE.lock().await;
    for enabled in [false, true] {
        let f = Fixture::new().await;
        let (g, reports) = timing_gateway(&f, enabled).await;
        let config = OwnedConfig::from(f.config());
        let graph = config.graph();
        let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
        let stale = create(1, 7, 1);
        let stale_plan = stale.mutation.prepare(&snapshot).unwrap();
        committed(&call(&g, Operation::Submit(create(2, 7, 2))).await.unwrap());
        graph
            .admit_request(
                stale.external_id,
                canonical(&bound(), &stale.mutation).unwrap(),
                &stale_plan,
            )
            .await
            .unwrap();
        let record = journal(&f)
            .await
            .request(stale.external_id)
            .unwrap()
            .clone();
        assert_eq!(
            call(&g, Operation::Submit(stale.clone())).await,
            Err(Error::Rejected)
        );
        assert!(
            matches!(call(&g,Operation::Status(stale.clone())).await.unwrap(),Reply::Write { transaction_id:Some(id),status:WriteStatus::Pending,.. } if id==stale_plan.id())
        );
        assert_eq!(
            journal(&f).await.request(stale.external_id).unwrap(),
            &record
        );
        assert!(matches!(
            call(&g, Operation::Abort(stale.clone())).await.unwrap(),
            Reply::Write {
                status: WriteStatus::Aborted { visible: true, .. },
                ..
            }
        ));
        assert!(matches!(
            call(&g, Operation::Submit(stale)).await.unwrap(),
            Reply::Write {
                status: WriteStatus::Aborted { visible: true, .. },
                ..
            }
        ));
        let before = journal(&f).await.resolved_prefix();
        for operation in [
            Operation::Status(create(3, 7, 3)),
            Operation::Abort(create(3, 7, 3)),
        ] {
            assert!(matches!(
                call(&g, operation).await.unwrap(),
                Reply::Write {
                    transaction_id: None,
                    status: WriteStatus::Absent,
                    ..
                }
            ));
        }
        assert_eq!(journal(&f).await.resolved_prefix(), before);
        if let Some(reports) = reports {
            let reports: Vec<_> = reports.try_iter().collect();
            assert_eq!(reports.len(), 3);
            assert_eq!(reports[1].completion, Completion::Error);
            assert_eq!(reports[1].phases[Phase::Prepare as usize].calls, 1);
            assert_eq!(reports[1].phases[Phase::HistorySync as usize].calls, 1);
            assert_eq!(reports[1].phases[Phase::Cohort as usize].calls, 0);
            assert_eq!(reports[2].completion, Completion::Returned);
            assert_eq!(reports[2].phases[Phase::Prepare as usize].calls, 0);
            assert_eq!(reports[2].phases[Phase::Visibility as usize].calls, 2);
        }
        g.shutdown().await;
        drop(config);
        f.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_admission_and_total_deadline_leave_no_implicit_write() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = Gateway::bind(
        f.config(),
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_millis(500),
    )
    .await
    .unwrap();
    let work = Arc::new(g.state.connections.clone().try_acquire_owned().unwrap());
    assert_eq!(
        g.state
            .execute(Operation::Submit(create(1, 7, 1)), work, Instant::now())
            .await,
        Err(Error::Unknown)
    );
    let reserved = g
        .state
        .connections
        .clone()
        .acquire_many_owned(MAX_CONNECTIONS as u32)
        .await
        .unwrap();
    assert!(call(&g, Operation::Status(create(99, 7, 99)))
        .await
        .is_err());
    drop(reserved);
    // Incomplete protobuf body must expire without admitting any write.
    let (response, _body, _driver) = raw_rpc(
        g.local_addr(),
        "velagraph.public.managed.v1.WriteService/Submit",
        vec![0, 0, 0, 0, 100, 0x0a],
        false,
    )
    .await;
    rejected_rpc(response).await;
    let serial = g.state.submit.lock().await;
    let submitted = request(
        g.local_addr(),
        bound(),
        Operation::Submit(create(1, 7, 1)),
        Duration::from_secs(2),
    );
    let status = call(&g, Operation::Status(create(1, 7, 1)));
    let (submitted, status) = tokio::join!(submitted, status);
    assert_eq!(submitted, Err(Error::Unknown));
    assert!(matches!(
        status.unwrap(),
        Reply::Write {
            status: WriteStatus::Absent,
            ..
        }
    ));
    drop(serial);
    assert_eq!(journal(&f).await.resolved_prefix(), 0);
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    g.shutdown().await;
    f.stop().await;
}

#[derive(Clone)]
struct FakeFixedPeer {
    case: usize,
    rows: bool,
}
#[tonic::async_trait]
impl public_proto::fixed_session_service_server::FixedSessionService for FakeFixedPeer {
    async fn open(
        &self,
        request: tonic::Request<public_proto::FixedOpenRequest>,
    ) -> std::result::Result<tonic::Response<public_proto::FixedOpenResponse>, tonic::Status> {
        let mut identity = request.into_inner().context.and_then(|c| c.identity);
        match self.case {
            0 => identity.as_mut().unwrap().request_id[0] ^= 1,
            1 => {
                identity
                    .as_mut()
                    .unwrap()
                    .binding
                    .as_mut()
                    .unwrap()
                    .authority_mode[0] ^= 1
            }
            4 => identity = None,
            5 => {
                identity.as_mut().unwrap().request_id.pop();
            }
            6 => identity.as_mut().unwrap().binding.as_mut().unwrap().members[0].epoch += 1,
            _ => {}
        }
        Ok(tonic::Response::new(public_proto::FixedOpenResponse {
            session: Some(public_proto::FixedSession {
                incarnation: vec![1; 16],
                token: if self.case == 3 {
                    vec![2; MAX_RESPONSE_BYTES + 1]
                } else {
                    vec![2; 32]
                },
                version: if self.case == 2 { 5 } else { 4 },
            }),
            identity,
        }))
    }
    async fn read(
        &self,
        request: tonic::Request<public_proto::FixedQueryRequest>,
    ) -> std::result::Result<tonic::Response<public_proto::FixedQueryResponse>, tonic::Status> {
        use crate::remote::query::QualifiedElement as Q;
        assert!(self.rows);
        let request = request.into_inner();
        let identity = request.context.as_ref().and_then(|c| c.identity.clone());
        let operation = grpc_codec::fixed_read(request).unwrap();
        let Operation::Read { session, query } = operation else {
            unreachable!()
        };
        let rows = match self.case {
            0 => vec![v(7, 2)],
            1 | 2 => vec![e(8, 10)],
            3 => vec![e(10, 10)],
            4 | 5 => vec![],
            6 => vec![value(
                Q::Edge {
                    partition: 7,
                    id: 10,
                },
                Value::Integer(11),
            )],
            _ => vec![value(
                Q::Vertex {
                    partition: 7,
                    id: 2,
                },
                Value::Null,
            )],
        };
        let mut reply = grpc_codec::encode_fixed_query(&Reply::Read {
            session,
            query,
            data: QueryData {
                rows,
                completeness: QueryCompleteness::Unverified,
            },
        })
        .unwrap();
        reply.identity = identity;
        Ok(tonic::Response::new(reply))
    }
    async fn traverse(
        &self,
        _: tonic::Request<public_proto::FixedQueryRequest>,
    ) -> std::result::Result<tonic::Response<public_proto::FixedQueryResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not a fixture operation"))
    }
    async fn page(
        &self,
        _: tonic::Request<public_proto::FixedPageRequest>,
    ) -> std::result::Result<tonic::Response<public_proto::FixedPageResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not a fixture operation"))
    }
    async fn close(
        &self,
        _: tonic::Request<public_proto::FixedCloseRequest>,
    ) -> std::result::Result<tonic::Response<public_proto::FixedCloseResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not a fixture operation"))
    }
}
async fn fake_fixed_peer(case: usize, rows: bool) -> (SocketAddr, RpcDriver) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service =
        public_proto::fixed_session_service_server::FixedSessionServiceServer::new(FakeFixedPeer {
            case,
            rows,
        })
        .max_encoding_message_size(MAX_RESPONSE_BYTES * 2);
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (address, RpcDriver(task))
}
#[tokio::test]
async fn fake_peer_mismatches_and_response_cap_fail_closed() {
    let _test = TEST_GATE.lock().await;
    for test in 0..7 {
        let (addr, _server) = fake_fixed_peer(test, false).await;
        let result = request(
            addr,
            bound(),
            Operation::OpenSession {
                version: Some(4),
                ttl_millis: 1000,
            },
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            result,
            Err(if test == 3 {
                Error::Capacity
            } else {
                Error::Identity
            })
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_after_partial_prepare_keeps_id_and_retry_needs_no_capture_capacity() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = Gateway::bind(
        f.config(),
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_millis(500),
    )
    .await
    .unwrap();
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    committed(&call(&g, Operation::Submit(create(2, 8, 1))).await.unwrap());
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    let write = Write {
        external_id: [3; 16],
        mutation: Mutation::CreateEdge { edge: edge() },
    };
    let candidate = write.mutation.prepare(&snapshot).unwrap();
    let original = graph
        .admit_request(
            write.external_id,
            canonical(&bound(), &write.mutation).unwrap(),
            &candidate,
        )
        .await
        .unwrap();
    let record = journal(&f)
        .await
        .request(write.external_id)
        .unwrap()
        .clone();
    let durable = d2_raft::request_plans::Plan::decode(&record.payload).unwrap();
    let part = &durable.parts[0];
    participant_driver::prepare(
        &f.nodes[0],
        &f.nodes[1],
        authority(),
        1,
        part.descriptor.clone(),
        part.request.clone(),
    )
    .await
    .unwrap();
    for _ in 0..MAX_SESSIONS {
        open(&g, None).await;
    }
    assert_eq!(g.retained_payload_charge(), MAX_RETAINED_BYTES);
    let paused = f.nodes[2].state_machine.pause_state().await;
    assert_eq!(
        call(&g, Operation::Submit(write.clone())).await,
        Err(Error::Unknown)
    );
    drop(paused);
    assert_eq!(
        journal(&f).await.request(write.external_id).unwrap(),
        &record
    );
    assert!(matches!(
        call(&g, Operation::Status(write.clone())).await.unwrap(),
        Reply::Write {
            status: WriteStatus::Pending,
            ..
        }
    ));
    let (id, ordinal) = committed(&call(&g, Operation::Submit(write)).await.unwrap());
    assert_eq!(id, original.id());
    assert_eq!(ordinal, 3);
    assert_eq!(g.retained_payload_charge(), MAX_RETAINED_BYTES);
    assert_eq!(journal(&f).await.request([3; 16]).unwrap(), &record);
    g.shutdown().await;
    drop(config);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undelivered_open_guard_reclaims_on_cancellation() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let session = open(&g, None).await;
    let delivery = OpenDelivery {
        local_token: None,
        state: g.state.clone(),
        token: Some(session.token),
    };
    let held = async {
        let _delivery = delivery;
        std::future::pending::<()>().await;
    };
    assert!(tokio::time::timeout(Duration::from_millis(10), held)
        .await
        .is_err());
    assert_eq!(g.retained_payload_charge(), 0);
    assert_eq!(
        call(&g, Operation::Close { session }).await,
        Err(Error::SessionUnavailable)
    );
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test]
async fn fake_peer_point_and_adjacency_entities_must_match_request() {
    let _test = TEST_GATE.lock().await;
    for test in 0..8 {
        let (addr, _server) = fake_fixed_peer(test, true).await;
        let session = Session {
            incarnation: [1; 16],
            token: [2; 32],
            version: 4,
        };
        let query = match test {
            0 => query(7, "g.V(1)"),
            1 => query(7, "g.E(10)"),
            2 => query(7, "g.V(1).outE()"),
            3 => query(8, "g.V(1).inE()"),
            4 => query(10, "g.V(1)"),
            5 => query(10, "g.V(1).outE()"),
            6 => query(7, "g.E(10).id()"),
            _ => query(7, "g.V(1).values('weight')"),
        };
        let operation = Operation::Read {
            session: session.clone(),
            query: query.clone(),
        };
        assert_eq!(
            request(addr, bound(), operation, Duration::from_secs(2)).await,
            Err(Error::Identity)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_open_rpc_releases_undelivered_session_over_real_http2() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    for _ in 1..=MAX_SESSIONS + 1 {
        // Ensure the frame has reached capture before resetting the socket,
        // while preventing a reply until the peer has definitely sent RST.
        let paused = f.nodes[0].state_machine.pause_state().await;
        let request = public_proto::FixedOpenRequest {
            context: public_context(),
            version: None,
            ttl_millis: 60000,
            ..Default::default()
        };
        let (response, mut body, driver) = raw_rpc(
            g.local_addr(),
            "velagraph.public.managed.v1.FixedSessionService/Open",
            grpc_frame(&request),
            true,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while g.retained_payload_charge() != SESSION_CHARGE {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        body.send_reset(h2::Reason::CANCEL);
        drop(response);
        drop(paused);
        tokio::time::timeout(Duration::from_secs(2), async {
            while g.state.connections.available_permits() != MAX_CONNECTIONS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(g.retained_payload_charge(), 0);
        assert!(g.state.sessions.lock().unwrap().is_empty());
        drop(driver);
    }
    let session = open(&g, None).await;
    close(&g, session).await;
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gremlin_projection_limit_count_order_and_static_empty_stream_rejection() {
    let _test = TEST_GATE.lock().await;
    use crate::remote::query::QualifiedElement as Q;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let mut first = create(1, 7, 1);
    if let Mutation::CreateVertex { vertex, .. } = &mut first.mutation {
        vertex.labels = vec!["person".into(), "secondary".into()];
    }
    committed(&call(&g, Operation::Submit(first)).await.unwrap());
    committed(&call(&g, Operation::Submit(create(2, 8, 1))).await.unwrap());
    for (request_id, id, label, score, from, to) in [
        (3, 20, "z", None, 7, 8),
        (4, 10, "a", Some(Value::Integer(2)), 7, 8),
        (5, 30, "a", Some(Value::Null), 7, 8),
        (6, 40, "a", Some(Value::Integer(5)), 7, 7),
        (7, 50, "back", Some(Value::Integer(8)), 8, 7),
    ] {
        let mut edge = edge();
        edge.id = id;
        edge.label = label.into();
        edge.from.partition = from;
        edge.to.partition = to;
        if let Some(score) = score {
            edge.properties.insert("score".into(), score);
        }
        committed(
            &call(
                &g,
                Operation::Submit(Write {
                    external_id: [request_id; 16],
                    mutation: Mutation::CreateEdge { edge },
                }),
            )
            .await
            .unwrap(),
        );
    }
    let session = open(&g, None).await;
    assert_eq!(read(&g, &session, query(7, "g.V(1)")).await, vec![v(7, 1)]);
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).label()")).await,
        vec![value(
            Q::Vertex {
                partition: 7,
                id: 1
            },
            Value::Text("person".into())
        )]
    );
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).id()")).await,
        vec![value(
            Q::Vertex {
                partition: 7,
                id: 1
            },
            Value::Integer(1)
        )]
    );
    assert_eq!(read(&g, &session, query(8, "g.E(10)")).await, vec![]);
    assert_eq!(
        read(&g, &session, query(8, "g.E(50)")).await,
        vec![e(8, 50)]
    );
    assert_eq!(
        read(&g, &session, query(7, "g.E(10).label()")).await,
        vec![value(
            Q::Edge {
                partition: 7,
                id: 10
            },
            Value::Text("a".into())
        )]
    );
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).outE()")).await,
        vec![e(7, 10), e(7, 20), e(7, 30), e(7, 40)]
    );
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).inE()")).await,
        vec![e(7, 40), e(8, 50)]
    );
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).outE('a')")).await,
        vec![e(7, 10), e(7, 30), e(7, 40)]
    );
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).outE().values('score')")).await,
        vec![
            value(
                Q::Edge {
                    partition: 7,
                    id: 10
                },
                Value::Integer(2)
            ),
            value(
                Q::Edge {
                    partition: 7,
                    id: 30
                },
                Value::Null
            ),
            value(
                Q::Edge {
                    partition: 7,
                    id: 40
                },
                Value::Integer(5)
            ),
        ]
    );
    for (text, n) in [
        ("g.V(1).outE().count()", 4),
        ("g.V(1).outE().limit(2).count()", 2),
        ("g.V(1).outE().limit(2).values('score').count()", 1),
        ("g.V(1).outE().values('score').limit(2).count()", 2),
        ("g.V(999).outE().count()", 0),
        ("g.E(999).values('score').count()", 0),
    ] {
        assert_eq!(
            read(&g, &session, query(7, text)).await,
            vec![QueryRow::Count(n)]
        );
    }
    for (text, failure) in [
        ("g.V()", QueryFailure::Unsupported),
        ("g.E()", QueryFailure::Unsupported),
        ("g.V(1,2)", QueryFailure::Unsupported),
        ("g.V(999).out()", QueryFailure::Unsupported),
        ("g.V(999).outE().inV()", QueryFailure::Unsupported),
        ("g.V(999).has('x')", QueryFailure::Unsupported),
        (
            "g.V(999).repeat(__.out()).times(1)",
            QueryFailure::Unsupported,
        ),
        ("g.E(999).outE()", QueryFailure::InvalidArgument),
        (
            "g.V(999).values('x').label()",
            QueryFailure::InvalidArgument,
        ),
        ("g.V(999).values('x').outE()", QueryFailure::InvalidArgument),
        ("g.V(999).limit(1).outE()", QueryFailure::Unsupported),
        ("g.V(1);g.V(2)", QueryFailure::Parse),
        ("g.V(1) trailing", QueryFailure::Parse),
    ] {
        assert_eq!(
            call(
                &g,
                Operation::Read {
                    session: session.clone(),
                    query: query(7, text)
                }
            )
            .await,
            Err(Error::Query(failure)),
            "{text}"
        );
    }
    // Preserve the parser's own distinction rather than inventing ID coercion.
    for text in [
        "g.V(-1)",
        "g.E('1')",
        "g.V(1).limit(0)",
        "g.V(1).next()",
        "g.addV('person')",
    ] {
        let expected = crate::remote::query::graph_error(
            velagraph::ObservedGremlinQuery::parse(text).unwrap_err(),
        );
        assert_eq!(
            call(
                &g,
                Operation::Read {
                    session: session.clone(),
                    query: query(7, text)
                }
            )
            .await,
            Err(Error::Query(expected)),
            "{text}"
        );
    }
    let text = format!("g.V(1).values('{}')", "(".repeat(70));
    assert_eq!(
        call(
            &g,
            Operation::Read {
                session: session.clone(),
                query: query(7, &text)
            }
        )
        .await,
        Err(Error::Query(QueryFailure::ResourceExhausted))
    );
    let text = format!("g.V(1).values('{}')", "x".repeat(16 * 1024));
    assert_eq!(
        call(
            &g,
            Operation::Read {
                session: session.clone(),
                query: query(7, &text)
            }
        )
        .await,
        Err(Error::Query(QueryFailure::Parse))
    );
    close(&g, session.clone()).await;
    assert_eq!(
        call(
            &g,
            Operation::Read {
                session,
                query: query(7, "g.V(999).count()")
            }
        )
        .await,
        Err(Error::SessionUnavailable)
    );
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gremlin_default_cap_is_after_count_and_explicit_limit_is_ordered() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    committed(&call(&g, Operation::Submit(create(2, 8, 1))).await.unwrap());
    for index in 0..101u8 {
        let mut edge = edge();
        edge.id = 1000 + u64::from(index);
        committed(
            &call(
                &g,
                Operation::Submit(Write {
                    external_id: [10 + index; 16],
                    mutation: Mutation::CreateEdge { edge },
                }),
            )
            .await
            .unwrap(),
        );
    }
    let session = open(&g, None).await;
    let rows = read(&g, &session, query(7, "g.V(1).outE()")).await;
    assert_eq!(rows, (1000..1100).map(|id| e(7, id)).collect::<Vec<_>>());
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).outE().count()")).await,
        vec![QueryRow::Count(101)]
    );
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).outE().limit(99).count()")).await,
        vec![QueryRow::Count(99)]
    );
    assert_eq!(
        read(&g, &session, query(7, "g.V(1).outE().limit(101)"))
            .await
            .len(),
        101
    );
    close(&g, session).await;
    g.shutdown().await;
    f.stop().await;
}

#[test]
fn gremlin_wire_golden_and_strict_query_fields_preserve_write_canonical() {
    use crate::remote::query::QualifiedElement as Q;
    let session = Session {
        incarnation: [1; 16],
        token: [2; 32],
        version: 4,
    };
    let query = query(8, "g.V(1).inE().values('weight')");
    let req = Request {
        version: VERSION,
        request_id: [1; 16],
        binding: bound(),
        operation: Operation::Read {
            session: session.clone(),
            query: query.clone(),
        },
    };
    let response = Response {
        version: VERSION,
        request_id: [1; 16],
        binding: bound(),
        result: Ok(Reply::Read {
            session,
            query,
            data: QueryData {
                rows: vec![value(
                    Q::Edge {
                        partition: 7,
                        id: 10,
                    },
                    Value::Integer(1),
                )],
                completeness: QueryCompleteness::Unverified,
            },
        }),
    };
    assert_eq!(
        encode(&req, MAX_REQUEST_BYTES).unwrap(),
        include_bytes!("../tests/fixtures/managed_gateway_gremlin_request_v1.json")
    );
    assert_eq!(
        encode(&response, MAX_RESPONSE_BYTES).unwrap(),
        include_bytes!("../tests/fixtures/managed_gateway_gremlin_response_v1.json")
    );
    let mut malformed = serde_json::to_value(&response).unwrap();
    malformed["result"]["Ok"]["Read"]["data"]["rows"][0]["Value"]["unexpected"] =
        serde_json::json!(true);
    assert!(serde_json::from_value::<Response>(malformed).is_err());
    let mut malformed = serde_json::to_value(&response).unwrap();
    malformed["result"]["Ok"]["Read"]["data"]["rows"][0]["Value"]["source"]["Edge"]["unexpected"] =
        serde_json::json!(true);
    assert!(serde_json::from_value::<Response>(malformed).is_err());
    assert!(validate_reply(&req, response).is_ok());
    assert_eq!(
        canonical(&bound(), &create(1, 7, 1).mutation).unwrap(),
        include_bytes!("../tests/fixtures/managed_gateway_command_v1.json")
    );
    let mut raw = serde_json::to_value(&req).unwrap();
    raw["operation"]["Read"]["query"]["typed_step"] =
        serde_json::json!({"Vertex":{"partition":8,"id":1}});
    assert!(serde_json::from_value::<Request>(raw).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_typed_read_wire_is_rejected_instead_of_translated() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let session = open(&g, None).await;
    for read in [
        serde_json::json!({"Vertex":{"vertex":{"partition":7,"id":1}}}),
        serde_json::json!({"Edge":{"edge":{"partition":7,"id":1}}}),
        serde_json::json!({"ObservedAdjacency":{"vertex":{"partition":7,"id":1},"direction":"Out"}}),
    ] {
        let raw = serde_json::json!({"version":VERSION,"request_id":vec![1;16],"binding":bound(),"operation":{"Read":{"session":session,"read":read}}});
        assert!(serde_json::from_value::<Request>(raw.clone()).is_err());
        let mut socket = TcpStream::connect(g.local_addr()).await.unwrap();
        write_bytes(&mut socket, &serde_json::to_vec(&raw).unwrap())
            .await
            .unwrap();
        assert!(tokio::time::timeout(
            Duration::from_secs(2),
            read_frame::<Response>(&mut socket, MAX_RESPONSE_BYTES)
        )
        .await
        .unwrap()
        .is_err());
    }
    assert_eq!(read(&g, &session, query(7, "g.V(1)")).await, vec![]);
    assert_eq!(journal(&f).await.resolved_prefix(), 0);
    close(&g, session).await;
    g.shutdown().await;
    f.stop().await;
}

#[path = "managed_gateway_traversal_tests.rs"]
mod traversal_tests;

async fn timing_gateway(f: &Fixture, enabled: bool) -> (Gateway, Option<timing::Reports>) {
    if enabled {
        let (gateway, reports) = Gateway::bind_with_timing(
            f.config(),
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(15),
        )
        .await
        .unwrap();
        (gateway, Some(reports))
    } else {
        let gateway = f.gateway().await;
        assert!(gateway.state.timing.is_none());
        (gateway, None)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submission_timing_preserves_success_retry_errors_and_old_version() {
    let _test = TEST_GATE.lock().await;
    for enabled in [false, true] {
        let f = Fixture::new().await;
        let (g, reports) = timing_gateway(&f, enabled).await;
        let old = open(&g, Some(0)).await;
        let start = Instant::now();
        let write = create(41, 7, 1);
        let first = call(&g, Operation::Submit(write.clone())).await.unwrap();
        assert_eq!(committed(&first).1, 1);
        let retry = call(&g, Operation::Submit(write.clone())).await.unwrap();
        assert_eq!(first, retry);
        assert_eq!(
            call(&g, Operation::Submit(create(41, 7, 2))).await,
            Err(Error::Identity)
        );
        assert_eq!(
            call(&g, Operation::Submit(create(42, 7, 1))).await,
            Err(Error::Rejected)
        );
        assert_eq!(
            call(&g, Operation::Submit(create(0, 7, 2))).await,
            Err(Error::Identity)
        );
        eprintln!("submission timing enabled={enabled}: five attempts {:?} (functional observation, not an overhead benchmark)", start.elapsed());
        assert_eq!(journal(&f).await.resolved_prefix(), 1);
        assert_eq!(read(&g, &old, query(7, "g.V(1)")).await, vec![]);
        let current = open(&g, Some(1)).await;
        assert_eq!(read(&g, &current, query(7, "g.V(1)")).await, vec![v(7, 1)]);
        if let Some(reports) = reports {
            let reports: Vec<_> = reports.try_iter().collect();
            assert_eq!(reports.len(), 5);
            let first = &reports[0];
            assert_eq!(first.external_id, write.external_id);
            assert_eq!(first.completion, Completion::Returned);
            for phase in [
                Phase::Queue,
                Phase::Canonical,
                Phase::Recovery,
                Phase::Snapshot,
                Phase::Validation,
                Phase::Admission,
                Phase::Execute,
                Phase::Decision,
                Phase::Prepare,
            ] {
                assert_eq!(first.phases[phase as usize].calls, 1, "{phase:?}");
            }
            assert_eq!(first.phases[Phase::HistorySync as usize].calls, 3);
            assert_eq!(first.phases[Phase::Consume as usize].calls, 2);
            assert_eq!(first.phases[Phase::Visibility as usize].calls, 2);
            for phase in [
                Phase::Snapshot,
                Phase::Validation,
                Phase::Admission,
                Phase::Prepare,
            ] {
                assert_eq!(reports[1].phases[phase as usize].calls, 0);
            }
            assert!(reports[2..]
                .iter()
                .all(|r| r.completion == Completion::Error));
            assert_eq!(reports[2].phases[Phase::Recovery as usize].calls, 1);
            assert_eq!(reports[3].phases[Phase::Validation as usize].calls, 1);
            assert_eq!(reports[3].phases[Phase::Admission as usize].calls, 0);
        }
        g.shutdown().await;
        f.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submission_timing_cancellation_full_and_disconnected_sink_do_not_write() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let (g, reports) = timing_gateway(&f, true).await;
    let reports = reports.unwrap();
    {
        let _serial = g.state.submit.lock().await;
        let work = Arc::new(g.state.connections.clone().try_acquire_owned().unwrap());
        let mut pending = Box::pin(g.state.execute(
            Operation::Submit(create(1, 7, 1)),
            work,
            Instant::now() + Duration::from_secs(15),
        ));
        assert!(matches!(
            pending
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        drop(pending);
    }
    let cancelled = reports.try_recv().unwrap();
    assert_eq!(cancelled.completion, Completion::Interrupted);
    assert_eq!(cancelled.phases[Phase::Queue as usize].calls, 1);
    assert_eq!(cancelled.phases[Phase::Recovery as usize].calls, 0);
    assert_eq!(journal(&f).await.resolved_prefix(), 0);
    // Fill the diagnostic queue with rejected requests. Overflow must not change errors.
    for _ in 0..timing::REPORT_CAPACITY + 1 {
        assert_eq!(
            call(&g, Operation::Submit(create(0, 7, 1))).await,
            Err(Error::Identity)
        );
    }
    // A full sink must also permit a real successful commit.
    committed(&call(&g, Operation::Submit(create(2, 7, 2))).await.unwrap());
    assert_eq!(reports.try_iter().count(), timing::REPORT_CAPACITY);
    drop(reports);
    committed(&call(&g, Operation::Submit(create(3, 7, 3))).await.unwrap());
    assert_eq!(journal(&f).await.resolved_prefix(), 2);
    g.shutdown().await;
    f.stop().await;
}

#[path = "managed_submission_timing_tests.rs"]
mod submission_timing_tests;

#[path = "managed_gateway_condition_tests.rs"]
mod condition_tests;

#[path = "managed_gateway_local_tests.rs"]
mod local_tests;
#[path = "managed_gateway_replace_tests.rs"]
mod replace_tests;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_invocation_drop_cleans_new_session_and_holds_admission() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let work = Arc::new(g.state.connections.clone().try_acquire_owned().unwrap());
    let invocation = invocation::Invocation::begin(
        g.state.clone(),
        Request {
            version: VERSION,
            request_id: [42; 16],
            binding: bound(),
            operation: Operation::OpenSession {
                version: None,
                ttl_millis: 60_000,
            },
        },
        work,
        Instant::now() + Duration::from_secs(15),
    )
    .await
    .unwrap();
    assert_eq!(g.state.connections.available_permits(), MAX_CONNECTIONS - 1);
    assert_eq!(g.retained_payload_charge(), SESSION_CHARGE);
    let token = match &invocation.response.result {
        Ok(Reply::OpenSession(session)) => session.clone(),
        other => panic!("{other:?}"),
    };
    let (bytes, invocation) = tokio::task::spawn_blocking(move || {
        invocation.preflight_grpc().unwrap();
        let reply = invocation.response.result.as_ref().unwrap();
        let bytes = prost::Message::encode_to_vec(&grpc_codec::encode_fixed_open(reply).unwrap());
        (bytes, invocation)
    })
    .await
    .unwrap();
    assert!(!bytes.is_empty());
    assert_eq!(g.state.connections.available_permits(), MAX_CONNECTIONS - 1);
    drop(invocation); // cancelled or undelivered response: never call delivered
    assert_eq!(g.state.connections.available_permits(), MAX_CONNECTIONS);
    assert_eq!(g.retained_payload_charge(), 0);
    assert_eq!(
        call(&g, Operation::Close { session: token }).await,
        Err(Error::SessionUnavailable)
    );
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_idle_and_incomplete_body_share_grpc_admission_and_deadline() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let http = g
        .bind_http("127.0.0.1:0".parse().unwrap(), Duration::from_secs(5))
        .await
        .unwrap();
    let mut sockets = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        sockets.push(TcpStream::connect(http.local_addr()).await.unwrap());
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while g.state.connections.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(call(&g, Operation::Status(create(99, 7, 99)))
        .await
        .is_err());
    drop(sockets);
    tokio::time::timeout(Duration::from_secs(1), async {
        while g.state.connections.available_permits() != MAX_CONNECTIONS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut stalled = TcpStream::connect(http.local_addr()).await.unwrap();
    stalled.write_all(b"POST /preview1/writes:submit HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\nX-Velagraph-Timeout-Ms: 20\r\n\r\n{").await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stalled.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while g.state.connections.available_permits() != MAX_CONNECTIONS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        call(&g, Operation::Status(create(99, 7, 99)))
            .await
            .unwrap(),
        Reply::Write {
            status: WriteStatus::Absent,
            ..
        }
    ));
    http.shutdown().await;
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_free_service_expires_and_releases_sessions_on_shutdown_or_drop() {
    let _guard = TEST_GATE.lock().await;
    let fixture = Fixture::new().await;
    for graceful in [true, false] {
        let (service, address) =
            Service::start(fixture.config().into(), None, Duration::from_secs(15), None)
                .await
                .unwrap();
        assert_eq!(address, None);
        let http = service
            .bind_http("127.0.0.1:0".parse().unwrap(), Duration::from_secs(15))
            .await
            .unwrap();
        let grpc = service
            .bind_grpc("127.0.0.1:0".parse().unwrap(), Duration::from_secs(15))
            .await
            .unwrap();
        let public_addresses = [http.local_addr(), grpc.local_addr()];
        let state = service.state.clone();
        for ttl in [100, 5000] {
            let permit = Arc::new(state.connections.clone().acquire_owned().await.unwrap());
            state
                .execute(
                    Operation::OpenSession {
                        version: None,
                        ttl_millis: ttl,
                    },
                    permit,
                    Instant::now() + Duration::from_secs(15),
                )
                .await
                .unwrap();
            assert!(service.retained_payload_charge() > 0);
            if ttl == 100 {
                // No request invokes expire(): the independent lifecycle must do it.
                tokio::time::timeout(Duration::from_secs(2), async {
                    while !state.sessions.lock().unwrap().is_empty() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(service.retained_payload_charge(), 0);
            }
        }
        if graceful {
            service.shutdown().await;
        } else {
            drop(service);
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while !state.sessions.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(state
            .jobs_stopping
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(state.memory.available_permits(), MAX_SESSIONS);
        // Keep adapter handles alive: the shared stop signal must stop them,
        // rather than relying on their handles being dropped by the caller.
        for address in public_addresses {
            tokio::time::timeout(Duration::from_secs(2), async {
                while TcpStream::connect(address).await.is_ok() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        }
        http.shutdown().await;
        grpc.shutdown().await;
    }
    fixture.stop().await;
}
