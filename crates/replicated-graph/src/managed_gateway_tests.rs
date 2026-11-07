use super::*;
// The client intentionally has a process-wide eight-call cap. Keep independent
// fixture tests from consuming each other's admission budget under parallel CI.
static TEST_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
use d2_raft::transaction_decisions::{Command, Decision, Journal, Member};
use d2_raft::{
    participant_driver, versioned_reads, Cluster, D1Command, D1Error, D1Result, LogCommand,
};
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn binding() -> Cohort {
    Cohort {
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
async fn tcp_mutations_fixed_versions_concurrent_identity_and_disconnect() {
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
    // Real socket delivers the full command, then discards the final reply.
    let request = Request {
        version: VERSION,
        request_id: [7; 16],
        binding: bound(),
        operation: Operation::Submit(deletion.clone()),
    };
    let mut socket = TcpStream::connect(g.local_addr()).await.unwrap();
    write_bytes(&mut socket, &encode(&request, MAX_REQUEST_BYTES).unwrap())
        .await
        .unwrap();
    drop(socket);
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
    assert_eq!(g.retained_payload_charge(), 3 * SNAPSHOT_CHARGE);
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
    let req = Request {
        version: VERSION,
        request_id: [1; 16],
        binding: bound(),
        operation: Operation::Read {
            session: sessions[0].clone(),
            query: query(7, "g.V(1)"),
        },
    };
    let mut value = serde_json::to_value(req).unwrap();
    value["operation"]["Read"]["cursor"] = serde_json::json!(1);
    let mut socket = TcpStream::connect(g.local_addr()).await.unwrap();
    write_bytes(&mut socket, &serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        read_frame::<Response>(&mut socket, MAX_RESPONSE_BYTES)
    )
    .await
    .unwrap()
    .is_err());
    let mut socket = TcpStream::connect(g.local_addr()).await.unwrap();
    socket
        .write_u32(MAX_REQUEST_BYTES as u32 + 1)
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        read_frame::<Response>(&mut socket, MAX_RESPONSE_BYTES)
    )
    .await
    .unwrap()
    .is_err());
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
    let f = Fixture::new().await;
    let g = f.gateway().await;
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
    g.shutdown().await;
    drop(config);
    f.stop().await;
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
    let mut socket = TcpStream::connect(g.local_addr()).await.unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        read_frame::<Response>(&mut socket, MAX_RESPONSE_BYTES)
    )
    .await
    .unwrap()
    .is_err());
    drop(reserved);
    let mut socket = TcpStream::connect(g.local_addr()).await.unwrap();
    socket.write_u32(100).await.unwrap();
    socket.write_all(b"{").await.unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        read_frame::<Response>(&mut socket, MAX_RESPONSE_BYTES)
    )
    .await
    .unwrap()
    .is_err());
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

#[tokio::test]
async fn fake_peer_mismatches_and_response_cap_fail_closed() {
    let _test = TEST_GATE.lock().await;
    for test in 0..4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let req: Request = read_frame(&mut socket, MAX_REQUEST_BYTES).await.unwrap();
            if test == 3 {
                socket
                    .write_u32(MAX_RESPONSE_BYTES as u32 + 1)
                    .await
                    .unwrap();
                return;
            }
            let mut response = Response {
                version: VERSION,
                request_id: req.request_id,
                binding: req.binding,
                result: Ok(Reply::OpenSession(Session {
                    incarnation: [1; 16],
                    token: [2; 32],
                    version: 5,
                })),
            };
            match test {
                0 => response.request_id[0] ^= 1,
                1 => response.binding.authority.mode[0] ^= 1,
                _ => {}
            }
            write_bytes(&mut socket, &encode(&response, MAX_RESPONSE_BYTES).unwrap())
                .await
                .unwrap();
        });
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
        server.await.unwrap();
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
    use crate::remote::query::QualifiedElement as Q;
    for test in 0..8 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
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
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let req: Request = read_frame(&mut socket, MAX_REQUEST_BYTES).await.unwrap();
            let rows = match test {
                0 => vec![v(7, 2)],
                1 => vec![e(8, 10)],
                2 => vec![e(8, 10)],
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
            let response = Response {
                version: VERSION,
                request_id: req.request_id,
                binding: req.binding,
                result: Ok(Reply::Read {
                    session,
                    query,
                    data: QueryData {
                        rows,
                        completeness: QueryCompleteness::Unverified,
                    },
                }),
            };
            write_bytes(&mut socket, &encode(&response, MAX_RESPONSE_BYTES).unwrap())
                .await
                .unwrap();
        });
        assert_eq!(
            request(addr, bound(), operation, Duration::from_secs(2)).await,
            Err(Error::Identity)
        );
        server.await.unwrap();
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_open_connection_releases_undelivered_session_over_real_tcp() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    for nonce in 1..=MAX_SESSIONS + 1 {
        // Ensure the frame has reached capture before resetting the socket,
        // while preventing a reply until the peer has definitely sent RST.
        let paused = f.nodes[0].state_machine.pause_state().await;
        let request = Request {
            version: VERSION,
            request_id: [nonce as u8; 16],
            binding: bound(),
            operation: Operation::OpenSession {
                version: None,
                ttl_millis: 60000,
            },
        };
        let mut socket = TcpStream::connect(g.local_addr()).await.unwrap();
        write_bytes(&mut socket, &encode(&request, MAX_REQUEST_BYTES).unwrap())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while g.retained_payload_charge() != SNAPSHOT_CHARGE {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let socket = socket.into_std().unwrap();
        rustix::net::sockopt::set_socket_linger(&socket, Some(Duration::ZERO)).unwrap();
        drop(socket);
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
            rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap_err(),
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
