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
        // A short nonce is malformed framing, not a valid mismatched identity.
        // The original JSON fixed-array decoder also classified this as Protocol.
        let expected = match test {
            3 => Error::Capacity,
            5 => Error::Protocol,
            _ => Error::Identity,
        };
        assert_eq!(result, Err(expected), "fake peer case {test}");
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
        // Keep capture paused until the server has observed queued RST_STREAM.
        tokio::time::timeout(Duration::from_secs(2), async {
            while g.state.connections.available_permits() != MAX_CONNECTIONS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(g.retained_payload_charge(), 0);
        assert!(g.state.sessions.lock().unwrap().is_empty());
        drop(paused);
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
        let service = Service::start(fixture.config().into(), Duration::from_secs(15), None)
            .await
            .unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_drain_finishes_admitted_call_rejects_new_stream_and_bounds_stall() {
    use prost::Message;
    let _guard = TEST_GATE.lock().await;
    let fixture = Fixture::new().await;
    let service = Service::start(fixture.config().into(), Duration::from_secs(15), None)
        .await
        .unwrap();
    for finish in [true, false] {
        let grpc = service
            .bind_grpc("127.0.0.1:0".parse().unwrap(), Duration::from_secs(15))
            .await
            .unwrap();
        let socket = TcpStream::connect(grpc.local_addr()).await.unwrap();
        let (mut sender, connection) = h2::client::handshake(socket).await.unwrap();
        let _driver = RpcDriver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let request = || {
            tonic::codegen::http::Request::builder()
                .method("POST")
                .uri(format!(
                    "http://{}/velagraph.public.managed.v1.WriteService/Status",
                    grpc.local_addr()
                ))
                .header("content-type", "application/grpc")
                .header("te", "trailers")
                .body(())
                .unwrap()
        };
        let (response, mut body) = sender.send_request(request(), false).unwrap();
        // Header admission is observable before sending the unary message body.
        tokio::time::timeout(Duration::from_secs(2), async {
            while service.state.connections.available_permits() == MAX_CONNECTIONS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        grpc.begin_drain(Duration::from_millis(if finish { 5000 } else { 200 }))
            .unwrap();
        // An existing HTTP/2 channel cannot bypass the admission fence. GOAWAY
        // may reject it at transport level, or the wrapper returns typed rejection.
        if let Ok((extra, mut extra_body)) = sender.send_request(request(), false) {
            let _ = extra_body.send_data(
                grpc_frame(
                    &grpc_client::encode_write(public_context(), &create(98, 7, 98)).unwrap(),
                )
                .into(),
                true,
            );
            rejected_rpc(extra).await;
        }
        if finish {
            let write = create(99, 7, 99);
            body.send_data(
                grpc_frame(&grpc_client::encode_write(public_context(), &write).unwrap()).into(),
                true,
            )
            .unwrap();
            let response = tokio::time::timeout(Duration::from_secs(2), response)
                .await
                .unwrap()
                .unwrap();
            let mut incoming = response.into_body();
            let mut bytes = Vec::new();
            while let Some(data) = incoming.data().await {
                let data = data.unwrap();
                incoming
                    .flow_control()
                    .release_capacity(data.len())
                    .unwrap();
                bytes.extend_from_slice(&data);
            }
            let trailers = incoming.trailers().await.unwrap().unwrap();
            assert_eq!(trailers.get("grpc-status").unwrap(), "0");
            assert!(bytes.len() >= 5);
            assert_eq!(bytes[0], 0);
            assert_eq!(
                u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize,
                bytes.len() - 5
            );
            let reply = public_proto::WriteResponse::decode(&bytes[5..]).unwrap();
            assert_eq!(reply.external_id, write.external_id);
            assert!(reply.transaction_id.is_none());
            assert!(matches!(
                reply.status,
                Some(public_proto::write_response::Status::Absent(_))
            ));
        } else {
            // begin_drain enforces its own bound, without a shutdown() waiter.
            rejected_rpc(response).await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while TcpStream::connect(grpc.local_addr()).await.is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        grpc.shutdown().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while service.state.connections.available_permits() != MAX_CONNECTIONS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    service.shutdown().await;
    fixture.stop().await;
}

// Counts actual TCP transports while forwarding real public gRPC traffic. Each
// connection is owned by the task's JoinSet and is aborted when the proxy drops.
async fn counted_gateway_proxy(
    destination: SocketAddr,
) -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>, RpcDriver) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = count.clone();
    let driver = RpcDriver(tokio::spawn(async move {
        let mut copies = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((mut incoming, _)) = accepted else { break };
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    copies.spawn(async move {
                        let mut outgoing = TcpStream::connect(destination).await.unwrap();
                        let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                    });
                }
                _ = copies.join_next(), if !copies.is_empty() => {}
            }
        }
    }));
    (address, count, driver)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusable_gateway_client_clones_share_transport_and_preserve_tokens() {
    let _test = TEST_GATE.lock().await;
    let fixture = Fixture::new().await;
    let gateway = fixture.gateway().await;
    let (address, connections, proxy) = counted_gateway_proxy(gateway.local_addr()).await;
    let client = GatewayClient::new(address, bound());
    let cloned = client.clone();
    let deadline = Duration::from_secs(15);
    let write = create(91, 7, 1);
    // Both clones enter the first (cold) connection concurrently.
    let (cold_left, cold_right) = tokio::join!(
        client.request(Operation::Status(write.clone()), deadline),
        cloned.request(Operation::Status(write.clone()), deadline)
    );
    assert_eq!(cold_left.unwrap(), cold_right.unwrap());
    assert_eq!(connections.load(std::sync::atomic::Ordering::SeqCst), 1);
    let reply = client
        .request(Operation::Submit(write.clone()), deadline)
        .await
        .unwrap();
    assert_eq!(committed(&reply).1, 1);
    let (left, right) = tokio::join!(
        client.request(Operation::Status(write.clone()), deadline),
        cloned.request(Operation::Status(write), deadline)
    );
    assert_eq!(left.unwrap(), reply);
    assert_eq!(right.unwrap(), reply);
    let session = match cloned
        .request(
            Operation::OpenSession {
                version: Some(1),
                ttl_millis: 60000,
            },
            deadline,
        )
        .await
        .unwrap()
    {
        Reply::OpenSession(session) => session,
        other => panic!("unexpected session: {other:?}"),
    };
    let query = query(7, "g.V(1)");
    let result = client
        .request(
            Operation::Read {
                session: session.clone(),
                query: query.clone(),
            },
            deadline,
        )
        .await
        .unwrap();
    match result {
        Reply::Read {
            session: actual,
            query: actual_query,
            data,
        } => {
            assert_eq!(actual, session);
            assert_eq!(actual_query, query);
            assert_eq!(data.rows, vec![v(7, 1)]);
            assert_eq!(data.completeness, QueryCompleteness::Unverified);
        }
        other => panic!("unexpected read: {other:?}"),
    }
    let mut stale = session.clone();
    stale.incarnation[0] ^= 1;
    assert_eq!(
        cloned
            .request(
                Operation::Read {
                    session: stale,
                    query: query.clone()
                },
                deadline
            )
            .await,
        Err(Error::SessionUnavailable)
    );
    client
        .request(
            Operation::Close {
                session: session.clone(),
            },
            deadline,
        )
        .await
        .unwrap();
    assert_eq!(
        cloned
            .request(Operation::Read { session, query }, deadline)
            .await,
        Err(Error::SessionUnavailable)
    );
    assert_eq!(connections.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(cloned);
    drop(client);
    drop(proxy);
    gateway.shutdown().await;
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusable_gateway_client_admission_and_cold_handshake_use_request_budget() {
    let _test = TEST_GATE.lock().await;
    let fixture = Fixture::new().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = GatewayClient::new(listener.local_addr().unwrap(), bound());
    let reserved = CLIENT_ADMISSION
        .acquire_many(MAX_CONNECTIONS as u32)
        .await
        .unwrap();
    assert_eq!(
        client
            .request(Operation::Status(create(92, 7, 1)), Duration::from_secs(1))
            .await,
        Err(Error::Capacity)
    );
    // No transport can be opened while admission is denied. Poll accept only
    // after the request has returned; the short bound is an absence assertion.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    drop(reserved);
    let (accepted, waiting) = tokio::join!(
        listener.accept(),
        tokio::time::timeout(
            Duration::from_secs(2),
            client.request(
                Operation::Status(create(92, 7, 1)),
                Duration::from_millis(50)
            )
        )
    );
    let (socket, _) = accepted.unwrap();
    assert_eq!(waiting.unwrap(), Err(Error::Unknown));
    assert_eq!(CLIENT_ADMISSION.available_permits(), MAX_CONNECTIONS);
    drop(socket);
    let address = listener.local_addr().unwrap();
    drop(listener);
    let gateway = Gateway::bind(fixture.config(), address, Duration::from_secs(15))
        .await
        .unwrap();
    let write = create(92, 7, 1);
    let reply = client
        .request(Operation::Submit(write), Duration::from_secs(15))
        .await
        .unwrap();
    assert_eq!(committed(&reply).1, 1);
    assert_eq!(
        client
            .request(Operation::Status(create(92, 7, 1)), Duration::ZERO)
            .await,
        Err(Error::Protocol)
    );
    drop(client);
    gateway.shutdown().await;
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusable_gateway_client_does_not_replay_mutation_after_stream_reset() {
    let _test = TEST_GATE.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = calls.clone();
    let peer = RpcDriver(tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(socket).await.unwrap();
        while let Some(call) = connection.accept().await {
            let (request, mut response) = call.unwrap();
            assert_eq!(
                request.uri().path(),
                "/velagraph.public.managed.v1.WriteService/Submit"
            );
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // This peer deliberately provides no application result: the
            // caller cannot infer whether the mutation committed or aborted.
            response.send_reset(h2::Reason::CANCEL);
        }
    }));
    let client = GatewayClient::new(address, bound());
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            client.request(Operation::Submit(create(93, 7, 1)), Duration::from_secs(2))
        )
        .await
        .unwrap(),
        Err(Error::Unknown)
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(CLIENT_ADMISSION.available_permits(), MAX_CONNECTIONS);
    drop(client);
    drop(peer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusable_gateway_client_cold_cancel_allows_later_explicit_request() {
    let _test = TEST_GATE.lock().await;
    let fixture = Fixture::new().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut client = GatewayClient::new(address, bound());
    let initializing = client.pause_first_connect();
    let pending_client = client.clone();
    let pending = tokio::spawn(async move {
        pending_client
            .request(Operation::Status(create(94, 7, 1)), Duration::from_secs(5))
            .await
    });
    let (socket, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    // TCP accept alone cannot distinguish initialization from an already
    // queued HTTP/2 body: the client handshake need not await server SETTINGS.
    // This barrier holds the real connector before it returns the socket, so
    // cancellation here must drop the only admission owner immediately.
    tokio::time::timeout(Duration::from_secs(2), initializing.notified())
        .await
        .unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    assert_eq!(CLIENT_ADMISSION.available_permits(), MAX_CONNECTIONS);
    drop(socket);
    drop(listener);
    let gateway = Gateway::bind(fixture.config(), address, Duration::from_secs(15))
        .await
        .unwrap();
    let write = create(94, 7, 1);
    let outcome = client
        .request(Operation::Submit(write.clone()), Duration::from_secs(15))
        .await
        .unwrap();
    assert_eq!(committed(&outcome).1, 1);
    assert_eq!(
        client
            .request(Operation::Status(write), Duration::from_secs(15))
            .await
            .unwrap(),
        outcome
    );
    drop(client);
    gateway.shutdown().await;
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusable_gateway_client_expired_queued_mutation_body_is_not_dispatched() {
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let _test = TEST_GATE.lock().await;
    let fixture = Fixture::new().await;
    let gateway = fixture.gateway().await;
    let destination = gateway.local_addr();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (disconnect, disconnected) = tokio::sync::oneshot::channel();
    let (accepted, reconnect_accepted) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    let submissions = Arc::new(AtomicUsize::new(0));
    let received_submissions = submissions.clone();
    let peer = RpcDriver(tokio::spawn(async move {
        // Warm the cached Channel against the real service, then drop both
        // sockets at an explicit barrier, not after a guessed sleep.
        let (mut incoming, _) = listener.accept().await.unwrap();
        let mut outgoing = TcpStream::connect(destination).await.unwrap();
        tokio::select! {
            _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing) => {},
            _ = disconnected => {},
        }
        drop(incoming);
        drop(outgoing);
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::Builder::new()
            .max_concurrent_streams(1)
            .handshake::<_, tonic::codegen::Bytes>(socket)
            .await
            .unwrap();
        // The read-only reconnect driver occupies the single allowed stream.
        // Merely delaying handshake is NOT a queue barrier: h2 may send a
        // request before receiving server SETTINGS. Wait for SETTINGS ACK too.
        let first = connection.accept().await.unwrap();
        assert!(first.as_ref().unwrap().0.uri().path().ends_with("/Status"));
        std::future::poll_fn(|cx| {
            assert!(connection.poll_accept(cx).is_pending());
            if connection.max_concurrent_recv_streams() == 1 {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
        accepted.send(()).unwrap();
        tokio::select! {
            biased;
            _ = resumed => {},
            unexpected = connection.accept() => panic!("request bypassed occupied single stream: {unexpected:?}"),
        }
        let mut first = Some(first);
        let mut replies = JoinSet::new();
        loop {
            let call = if let Some(first) = first.take() {
                Some(first)
            } else {
                connection.accept().await
            };
            let Some(call) = call else { break };
            let (request, mut response) = call.unwrap();
            let is_submit = request.uri().path().ends_with("/Submit");
            let received_submissions = received_submissions.clone();
            replies.spawn(async move {
                let mut body = request.into_body();
                let mut bytes = Vec::new();
                let mut counted = false;
                while let Some(chunk) = body.data().await {
                    let Ok(chunk) = chunk else { return };
                    if body.flow_control().release_capacity(chunk.len()).is_err() {
                        return;
                    }
                    bytes.extend_from_slice(&chunk);
                    // HEADERS can arrive for a cancelled buffered request.
                    // Count a mutation only when a complete, valid unary gRPC
                    // WriteRequest body is present, even if RST follows it.
                    if is_submit && !counted && bytes.len() >= 5 && bytes[0] == 0 {
                        let length = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
                        if bytes.len() == length + 5
                            && public_proto::WriteRequest::decode(&bytes[5..]).is_ok()
                        {
                            received_submissions.fetch_add(1, Ordering::SeqCst);
                            counted = true;
                        }
                    }
                }
                if bytes.len() < 5 || bytes[0] != 0 {
                    return;
                }
                let length = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
                if bytes.len() != length + 5 {
                    return;
                }
                let Ok(write) = public_proto::WriteRequest::decode(&bytes[5..]) else {
                    return;
                };
                let reply = public_proto::WriteResponse {
                    external_id: write.external_id,
                    transaction_id: None,
                    status: Some(public_proto::write_response::Status::Absent(
                        public_proto::Empty {},
                    )),
                    identity: write.context.and_then(|context| context.identity),
                };
                let headers = tonic::codegen::http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(())
                    .unwrap();
                let Ok(mut stream) = response.send_response(headers, false) else {
                    return;
                };
                if stream.send_data(grpc_frame(&reply).into(), false).is_err() {
                    return;
                }
                let mut trailers = tonic::codegen::http::HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                let _ = stream.send_trailers(trailers);
            });
        }
    }));
    let client = GatewayClient::new(address, bound());
    let write = create(95, 7, 1);
    assert!(matches!(
        client
            .request(Operation::Status(write.clone()), Duration::from_secs(15))
            .await
            .unwrap(),
        Reply::Write {
            status: WriteStatus::Absent,
            ..
        }
    ));
    disconnect.send(()).unwrap();
    // Read-only calls drive failure detection and reconnection. This does not
    // replay a mutation. The barrier proves its stream occupies the sole
    // acknowledged HTTP/2 stream slot on the replacement connection.
    let reader = client.clone();
    let status_write = write.clone();
    let read_driver = RpcDriver(tokio::spawn(async move {
        loop {
            let _ = reader
                .request(
                    Operation::Status(status_write.clone()),
                    Duration::from_secs(15),
                )
                .await;
            tokio::task::yield_now().await;
        }
    }));
    tokio::time::timeout(Duration::from_secs(5), reconnect_accepted)
        .await
        .unwrap()
        .unwrap();
    // Timed-out bodies still retained by the transport must keep admission
    // charged. Repeated short calls cannot accumulate an unbounded queue.
    let mut timed_out = 0;
    let mut capacity_rejected = 0;
    for external_id in 100..200 {
        let mutation_started = tokio::time::Instant::now();
        match client
            .request(
                Operation::Submit(create(external_id, 7, 1)),
                Duration::from_millis(50),
            )
            .await
        {
            Err(Error::Unknown) => {
                assert!(mutation_started.elapsed() >= Duration::from_millis(50));
                timed_out += 1;
            }
            Err(Error::Capacity) => capacity_rejected += 1,
            other => panic!("unexpected queued mutation outcome: {other:?}"),
        }
    }
    assert!(timed_out > 0 && timed_out <= MAX_CONNECTIONS);
    assert!(capacity_rejected > 0);
    assert_eq!(CLIENT_ADMISSION.available_permits(), 0);
    // Release the observation barrier before cancelling the occupying stream.
    // Its RST can immediately allow expired HEADERS through; that is permitted
    // and the body oracle below, not this pre-release barrier, judges dispatch.
    resume.send(()).unwrap();
    drop(read_driver);
    tokio::time::timeout(Duration::from_secs(5), async {
        while CLIENT_ADMISSION.available_permits() != MAX_CONNECTIONS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A new explicit call traverses the recovered channel and obtains a valid
    // correlated response. Expired Submit HEADERS may appear, but no complete
    // mutation body may dispatch. This cannot retract already buffered DATA.
    let reply = client
        .request(Operation::Status(write), Duration::from_secs(15))
        .await
        .unwrap();
    assert!(matches!(
        reply,
        Reply::Write {
            status: WriteStatus::Absent,
            ..
        }
    ));
    assert_eq!(submissions.load(Ordering::SeqCst), 0);
    drop(client);
    drop(peer);
    gateway.shutdown().await;
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusable_gateway_client_close_reclaims_permits_without_peer_quota_recovery() {
    let _test = TEST_GATE.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (ready, queued) = tokio::sync::oneshot::channel();
    let (disconnected, closed) = tokio::sync::oneshot::channel();
    let mut peer = RpcDriver(tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::Builder::new()
            .max_concurrent_streams(1)
            .handshake::<_, tonic::codegen::Bytes>(socket)
            .await
            .unwrap();
        // The first unanswered Status occupies the only acknowledged slot.
        // Never release it or raise SETTINGS: client close must tear down IO.
        let first = connection.accept().await.unwrap().unwrap();
        assert!(first.0.uri().path().ends_with("/Status"));
        std::future::poll_fn(|cx| {
            assert!(connection.poll_accept(cx).is_pending());
            if connection.max_concurrent_recv_streams() == 1 {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
        ready.send(()).unwrap();
        match connection.accept().await {
            None | Some(Err(_)) => {}
            Some(Ok(call)) => panic!(
                "unexpected request while quota occupied: {:?}",
                call.0.uri()
            ),
        }
        drop(first);
        disconnected.send(()).unwrap();
        // The listener stays available: a closed client must not reconnect.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }));
    let client = GatewayClient::new(address, bound());
    let cloned = client.clone();
    let first_client = client.clone();
    let first = tokio::spawn(async move {
        first_client
            .request(
                Operation::Status(create(201, 7, 1)),
                Duration::from_secs(15),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), queued)
        .await
        .unwrap()
        .unwrap();
    // Unknown also covers expiry before a body reaches the transport. Fill
    // admission using bounded, distinct explicit calls, not a presumed count
    // of retained bodies. The peer never frees its occupied stream quota.
    for id in 1..=100 {
        if CLIENT_ADMISSION.available_permits() == 0 {
            break;
        }
        assert_eq!(
            client
                .request(
                    Operation::Submit(create(id, 7, 1)),
                    Duration::from_millis(30)
                )
                .await,
            Err(Error::Unknown)
        );
    }
    assert_eq!(CLIENT_ADMISSION.available_permits(), 0);
    assert_eq!(
        client
            .request(Operation::Status(create(229, 7, 1)), Duration::from_secs(1))
            .await,
        Err(Error::Capacity)
    );
    cloned.close();
    client.close();
    assert!(client.is_closed() && cloned.is_closed());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), first)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Unknown)
    );
    tokio::time::timeout(Duration::from_secs(3), closed)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while CLIENT_ADMISSION.available_permits() != MAX_CONNECTIONS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for owner in [&client, &cloned] {
        assert_eq!(
            owner
                .request(Operation::Submit(create(229, 7, 1)), Duration::from_secs(1))
                .await,
            Err(Error::Unknown)
        );
    }
    // Await the absence check rather than aborting the peer before it runs.
    (&mut peer.0).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusable_gateway_client_close_during_cold_call_and_before_first_use() {
    let _test = TEST_GATE.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = GatewayClient::new(listener.local_addr().unwrap(), bound());
    let cloned = client.clone();
    let pending = tokio::spawn(async move {
        cloned
            .request(
                Operation::Status(create(231, 7, 1)),
                Duration::from_secs(15),
            )
            .await
    });
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    client.close();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), pending)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Unknown)
    );
    // Drain already-written handshake bytes until EOF. Merely aborting the
    // request would not establish that background transport had closed.
    let mut bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(3),
        tokio::io::AsyncReadExt::read_to_end(&mut socket, &mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while CLIENT_ADMISSION.available_permits() != MAX_CONNECTIONS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let never_connected = GatewayClient::new(listener.local_addr().unwrap(), bound());
    never_connected.close();
    never_connected.clone().close();
    assert!(never_connected.is_closed());
    assert_eq!(
        never_connected
            .request(Operation::Status(create(232, 7, 1)), Duration::from_secs(1))
            .await,
        Err(Error::Unknown)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reusable_gateway_client_close_after_submit_dispatch_is_unknown() {
    use prost::Message;
    let _test = TEST_GATE.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, dispatched) = tokio::sync::oneshot::channel();
    let mut peer = RpcDriver(tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::Builder::new()
            .handshake::<_, tonic::codegen::Bytes>(socket)
            .await
            .unwrap();
        let (request, response) = connection.accept().await.unwrap().unwrap();
        assert!(request.uri().path().ends_with("/Submit"));
        let read = async move {
            let mut body = request.into_body();
            let mut bytes = Vec::new();
            while let Some(chunk) = body.data().await {
                bytes.extend_from_slice(&chunk.unwrap());
            }
            assert!(bytes.len() > 5);
            assert_eq!(bytes[0], 0);
            assert_eq!(
                u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize,
                bytes.len() - 5
            );
            public_proto::WriteRequest::decode(&bytes[5..]).unwrap();
            // Full mutation DATA crossed the wire; intentionally withhold ACK.
            sent.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(response);
        };
        tokio::pin!(read);
        tokio::select! {
            _ = &mut read => unreachable!(),
            result = connection.accept() => assert!(result.is_none() || result.unwrap().is_err()),
        }
    }));
    let client = GatewayClient::new(address, bound());
    let pending_client = client.clone();
    let pending = tokio::spawn(async move {
        pending_client
            .request(
                Operation::Submit(create(233, 7, 1)),
                Duration::from_secs(15),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), dispatched)
        .await
        .unwrap()
        .unwrap();
    client.close();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), pending)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Unknown)
    );
    tokio::time::timeout(Duration::from_secs(3), &mut peer.0)
        .await
        .unwrap()
        .unwrap();
    // The controlled peer received the command. Unknown must not be interpreted
    // as proof of non-execution; this test does not simulate a durable commit.
}
