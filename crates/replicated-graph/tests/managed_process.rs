//! Independent-process managed transport acceptance, not multi-host HA evidence.
//! Every parent data path crosses TCP. Only each child owns its own Raft handle.
include!("support/managed_process_fixture.rs");

use d2_raft::{
    managed_rpc::NodeRef,
    transaction_decisions::{Decision, MemberCapacityGrant, RequestRecord},
    versioned_reads::ReadLimit,
};
use replicated_graph::{
    managed_gateway::{
        self as gateway, EdgeData, Entity, Gateway, Mutation, Operation, PageCursor, Patch, Query,
        QueryPage, RemoteConfig, Reply, Session, VertexData, Write, WriteStatus,
    },
    managed_graph::GraphCohort,
    remote::query::{QualifiedElement, QueryCompleteness, QueryRow},
};
use rocksdb_sql_table::{
    experimental_graph::{CrossEdge, VertexRef},
    Value,
};
use std::collections::BTreeSet;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Independent retained-view fixture phases share this test runner's resources.
// Serialize only those phases, never their internal concurrent graph operations.
static RETAINED_FIXTURE_PHASE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
fn identity(shard: u64) -> Identity {
    identity_in(shard, cohort())
}
fn limit() -> ReadLimit {
    ReadLimit {
        rows: 1000,
        bytes: 1024 * 1024,
        operations: 10000,
    }
}
impl FixtureDirectory {
    fn retained(path: PathBuf) -> anyhow::Result<Self> {
        fs::create_dir_all(&path)?;
        Ok(Self {
            path,
            _temporary: None,
        })
    }
}

impl Process {
    async fn snapshot_restore(&mut self) -> anyhow::Result<()> {
        if self.directory.path().join("snapshot.restored").exists() {
            return Ok(());
        }
        atomic_json(&self.directory.path().join("snapshot.request"), &true)?;
        tokio::time::timeout(Duration::from_secs(300), async {
            loop {
                if let Some(status) = self.child.try_wait()? {
                    anyhow::ensure!(status.success(), "snapshot child failed: {status}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        anyhow::ensure!(
            self.directory.path().join("snapshot.json").exists(),
            "missing full snapshot"
        );
        let data = self.directory.path().join("data");
        fs::rename(data.join("state.db"), data.join("state.before-snapshot.db"))?;
        fs::remove_file(self.directory.path().join("snapshot.request"))?;
        self.reopen().await
    }
}

async fn call(g: &Gateway, operation: Operation) -> gateway::Result<Reply> {
    call_bound(g, binding(), operation).await
}
async fn call_bound(g: &Gateway, binding: Binding, operation: Operation) -> gateway::Result<Reply> {
    gateway::request(g.local_addr(), binding, operation, BUDGET).await
}
async fn open(g: &Gateway, version: Option<u64>) -> anyhow::Result<Session> {
    open_bound(g, binding(), version).await
}
async fn open_bound(
    g: &Gateway,
    binding: Binding,
    version: Option<u64>,
) -> anyhow::Result<Session> {
    match call_bound(
        g,
        binding,
        Operation::OpenSession {
            version,
            ttl_millis: 60000,
        },
    )
    .await?
    {
        Reply::OpenSession(s) => Ok(s),
        other => anyhow::bail!("expected session: {other:?}"),
    }
}
async fn read(
    g: &Gateway,
    session: &Session,
    partition: u32,
    text: &str,
) -> anyhow::Result<Vec<QueryRow>> {
    read_bound(g, binding(), session, partition, text).await
}
async fn read_bound(
    g: &Gateway,
    binding: Binding,
    session: &Session,
    partition: u32,
    text: &str,
) -> anyhow::Result<Vec<QueryRow>> {
    match call_bound(
        g,
        binding,
        Operation::Read {
            session: session.clone(),
            query: Query {
                partition,
                text: text.into(),
            },
        },
    )
    .await?
    {
        Reply::Read {
            session: returned,
            data,
            ..
        } => {
            assert_eq!(returned, *session);
            assert_eq!(data.completeness, QueryCompleteness::Unverified);
            Ok(data.rows)
        }
        other => anyhow::bail!("expected query: {other:?}"),
    }
}
fn committed(reply: Reply) -> ([u8; 16], u64) {
    match reply {
        Reply::Write {
            transaction_id: Some(id),
            status:
                WriteStatus::Committed {
                    ordinal,
                    visible: true,
                },
            ..
        } => (id, ordinal),
        other => panic!("expected visible Commit: {other:?}"),
    }
}
fn create(external: u8, partition: u32) -> Write {
    Write {
        external_id: [external; 16],
        mutation: Mutation::CreateVertex {
            partition,
            vertex: VertexData {
                id: 1,
                labels: vec!["person".into()],
                properties: BTreeMap::new(),
            },
        },
    }
}
fn vertex(partition: u32) -> QueryRow {
    QueryRow::Element(QualifiedElement::Vertex { partition, id: 1 })
}

fn edge_row() -> QueryRow {
    QueryRow::Element(QualifiedElement::Edge {
        partition: 7,
        id: 10,
    })
}

// Each deliberately wrong identity remains internally valid, so construction
// succeeds and rejection is exercised against the actual child TCP service.
async fn rejects_wrong_identity(process: &Process) -> anyhow::Result<()> {
    let base = identity(9);
    let mut node = base.clone();
    node.node = 2;
    let mut group = base.clone();
    group.group = 99;
    let mut role = base.clone();
    role.role = Role::Participant;
    role.target = target(7);
    let mut epoch = base.clone();
    epoch.target.epoch = 2;
    epoch.cohort.coordinator_epoch = 2;
    let mut cohort = base.clone();
    cohort.cohort.members[1].mode = [88; 16];
    for wrong in [node, group, role, epoch, cohort] {
        let client = Client::new(process.address, wrong.clone(), BUDGET)?;
        assert_eq!(
            NodeRef::Remote(&client)
                .journal(&wrong.authority, wrong.cohort.coordinator_epoch)
                .await,
            Err(d2_raft::managed_rpc::Error::Identity),
            "wrong process identity was accepted: {wrong:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_managed_processes_partial_commit_and_durable_recovery() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(240), async {
        scenario().await?;
        // Preserve every original check and deadline. The additive replacement
        // fixture starts only after the original cohort and sessions close.
        managed_process_edge_replace::scenario().await
    })
    .await?
}
async fn scenario() -> anyhow::Result<()> {
    let mut coordinator = Process::start(9).await?;
    let mut a = Process::start(7).await?;
    let mut b = Process::start(8).await?;
    assert_eq!(
        BTreeSet::from([
            coordinator.child.id(),
            a.child.id(),
            b.child.id(),
            std::process::id()
        ])
        .len(),
        4
    );
    assert_eq!(
        BTreeSet::from([coordinator.address, a.address, b.address]).len(),
        3
    );
    rejects_wrong_identity(&coordinator).await?;
    eprintln!(
        "managed processes: coordinator pid={} at {}, shard7 pid={} at {}, shard8 pid={} at {}, gateway/client pid={}",
        coordinator.child.id(), coordinator.address, a.child.id(), a.address,
        b.child.id(), b.address, std::process::id()
    );
    let c_client = coordinator.client()?;
    let a_client = a.client()?;
    let b_client = b.client()?;
    let g = Gateway::bind_remote(
        RemoteConfig {
            coordinator: c_client.clone(),
            binding: binding(),
            members: vec![a_client.clone(), b_client.clone()],
        },
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    committed(call(&g, Operation::Submit(create(1, 7))).await?);
    committed(call(&g, Operation::Submit(create(2, 8))).await?);
    let old = open(&g, None).await?;
    assert_eq!(old.version, 2);
    assert_eq!(read(&g, &old, 7, "g.V(1)").await?, vec![vertex(7)]);
    assert_eq!(read(&g, &old, 8, "g.V(1)").await?, vec![vertex(8)]);
    let edge = CrossEdge {
        id: 10,
        from: VertexRef {
            partition: 7,
            id: 1,
        },
        to: VertexRef {
            partition: 8,
            id: 1,
        },
        label: "knows".into(),
        properties: BTreeMap::new(),
    };
    let write = Write {
        external_id: [3; 16],
        mutation: Mutation::CreateEdge {
            edge: EdgeData::from(edge.clone()),
        },
    };
    // Controlled failure boundary: the trusted fixture prepares and admits the
    // same canonical command as the gateway, but pauses before the second apply.
    // No typed graph reads are used for any assertion.
    let graph = GraphCohort::from_endpoints(
        NodeRef::Remote(&c_client),
        authority(),
        1,
        vec![
            (NodeRef::Remote(&a_client), target(7)),
            (NodeRef::Remote(&b_client), target(8)),
        ],
    );
    let snapshot = graph
        .snapshot(None, limit(), Duration::from_secs(60))
        .await?;
    let candidate = snapshot.prepare_edge(&edge)?;
    let canonical = serde_json::to_vec(&("managed-graph-command-v1", binding(), &write.mutation))?;
    let plan = graph
        .admit_request(write.external_id, canonical, &candidate)
        .await?;
    let c = NodeRef::Remote(&c_client);
    let admitted = c.journal(&authority(), 1).await?;
    let record = admitted
        .request(write.external_id)
        .context("admitted request")?
        .clone();
    let durable = d2_raft::request_plans::Plan::decode(&record.payload)
        .map_err(|e| anyhow!("plan decode: {e:?}"))?;
    assert_eq!(durable.parts.len(), 2);
    // Internal transport must recover by transaction ID, not accidentally look
    // up the external ID and skip the admitted Plan's exact-body check.
    let mut changed_request = durable.parts[0].request.clone();
    changed_request.expected_sequence = Some(changed_request.expected_sequence.unwrap() + 1);
    assert!(matches!(
        NodeRef::Remote(&a_client)
            .prepare(
                &authority(),
                &durable.parts[0].descriptor,
                changed_request,
                &admitted,
            )
            .await,
        Err(d2_raft::managed_rpc::Error::Identity)
    ));
    for (part, client) in durable.parts.iter().zip([&a_client, &b_client]) {
        NodeRef::Remote(client)
            .prepare(
                &authority(),
                &part.descriptor,
                part.request.clone(),
                &admitted,
            )
            .await?;
        c.command(
            &authority(),
            1,
            DecisionCommand::Prepared {
                id: plan.id(),
                participant: part.descriptor.clone(),
            },
        )
        .await?;
    }
    c.command(
        &authority(),
        1,
        DecisionCommand::Decide {
            id: plan.id(),
            decision: Decision::Commit,
        },
    )
    .await?;
    let decided = c.journal(&authority(), 1).await?;
    let ordinal = decided
        .transaction(plan.id())
        .context("decided transaction")?
        .ordinal;
    assert_eq!(ordinal, 3);
    NodeRef::Remote(&a_client)
        .advance(&authority(), &target(7), ordinal, &decided)
        .await?;
    let partial = call(&g, Operation::Status(write.clone())).await?;
    assert_eq!(
        partial,
        Reply::Write {
            external_id: write.external_id,
            transaction_id: Some(plan.id()),
            status: WriteStatus::Committed {
                ordinal,
                visible: false
            }
        }
    );
    assert!(call(
        &g,
        Operation::OpenSession {
            version: Some(ordinal),
            ttl_millis: 60000
        }
    )
    .await
    .is_err());
    assert!(traverse(&g, &old, 7, "g.V(1).out('knows').path()")
        .await?
        .is_empty());
    let partial_page = page(&g, &old, 7, "g.V(1).out('knows').path()", None, 1).await?;
    assert!(partial_page.data.rows.is_empty());
    assert_eq!(partial_page.total, 0);
    assert!(partial_page.next.is_none());
    let common = open(&g, None).await?;
    assert_eq!(common.version, old.version);
    assert!(read(&g, &common, 7, "g.V(1).outE('knows')")
        .await?
        .is_empty());
    assert!(read(&g, &common, 8, "g.V(1).inE('knows')")
        .await?
        .is_empty());
    call(&g, Operation::Close { session: common }).await?;
    // Crash the prepared, unapplied participant. Commit remains known even
    // while visibility cannot be established; no timeout converts it to Abort.
    b.stop()?;
    // An unavailable member invalidates every new whole-cohort capture, even
    // an explicit old version. Materialized old sessions remain usable.
    for version in [None, Some(old.version), Some(ordinal)] {
        assert!(call(
            &g,
            Operation::OpenSession {
                version,
                ttl_millis: 60000
            }
        )
        .await
        .is_err());
    }
    assert_eq!(call(&g, Operation::Status(write.clone())).await?, partial);
    assert!(read(&g, &old, 7, "g.E(10)").await?.is_empty());
    b.reopen().await?;
    // Crash/reopen the coordinator too: retain the exact durable bytes, not a
    // freshly prepared plan or a reconstructed transaction identity.
    coordinator.stop()?;
    coordinator.reopen().await?;
    eprintln!(
        "reopened coordinator pid={}, shard8 pid={}, original transaction={:?}",
        coordinator.child.id(),
        b.child.id(),
        plan.id()
    );
    let recovered = c.journal(&authority(), 1).await?;
    assert_eq!(recovered.request(write.external_id), Some(&record));
    let point = c.request(&authority(), 1, write.external_id).await?;
    assert_eq!(point.resolved_prefix, recovered.resolved_prefix());
    let point = point
        .record
        .context("point recovery after coordinator restart")?;
    assert_eq!(point.record, record);
    assert_eq!(
        Some(&point.transaction),
        recovered.transaction(record.transaction_id)
    );
    assert_eq!(recovered.transaction_count(), 3);
    assert_eq!(call(&g, Operation::Status(write.clone())).await?, partial);
    let (completed, ()) = tokio::join!(call(&g, Operation::Submit(write.clone())), async {
        for _ in 0..3 {
            assert!(read(&g, &old, 7, "g.V(1).outE('knows')")
                .await
                .unwrap()
                .is_empty());
            assert!(read(&g, &old, 8, "g.V(1).inE('knows')")
                .await
                .unwrap()
                .is_empty());
        }
    });
    assert_eq!(committed(completed?), (plan.id(), ordinal));
    assert_eq!(
        committed(call(&g, Operation::Submit(write.clone())).await?),
        (plan.id(), ordinal)
    );
    let fresh = open(&g, Some(ordinal)).await?;
    assert_eq!(
        read(&g, &fresh, 7, "g.V(1).outE('knows')").await?,
        vec![edge_row()]
    );
    assert_eq!(
        read(&g, &fresh, 8, "g.V(1).inE('knows')").await?,
        vec![edge_row()]
    );
    assert_eq!(
        read(&g, &fresh, 7, "g.E(10)").await?,
        vec![QueryRow::Element(QualifiedElement::Edge {
            partition: 7,
            id: 10
        })]
    );
    let mut conflict = write.clone();
    if let Mutation::CreateEdge { edge } = &mut conflict.mutation {
        edge.label = "different".into();
    }
    assert_eq!(
        call(&g, Operation::Submit(conflict.clone())).await,
        Err(gateway::Error::Identity)
    );
    assert_eq!(
        call(&g, Operation::Status(conflict)).await,
        Err(gateway::Error::Identity)
    );
    assert!(call(
        &g,
        Operation::Read {
            session: fresh.clone(),
            query: Query {
                partition: 7,
                text: "g.V(1).madeUpStep()".into()
            }
        }
    )
    .await
    .is_err());
    // A member with no writes must still consume the terminal decision. A
    // participant-only watermark is not a whole-cohort readable version.
    let vertex = rocksdb_sql_table::Vertex::new(2);
    let vertex_write = Write {
        external_id: [4; 16],
        mutation: Mutation::CreateVertex {
            partition: 7,
            vertex: VertexData::from(vertex.clone()),
        },
    };
    let at_three = graph
        .snapshot(Some(ordinal), limit(), Duration::from_secs(60))
        .await?;
    let candidate = at_three.prepare_vertex(7, &vertex)?;
    let canonical = serde_json::to_vec(&(
        "managed-graph-command-v1",
        binding(),
        &vertex_write.mutation,
    ))?;
    let vertex_plan = graph
        .admit_request(vertex_write.external_id, canonical, &candidate)
        .await?;
    let admitted_vertex = c.journal(&authority(), 1).await?;
    let vertex_record = admitted_vertex
        .request(vertex_write.external_id)
        .context("admitted vertex")?
        .clone();
    let vertex_durable = d2_raft::request_plans::Plan::decode(&vertex_record.payload)
        .map_err(|e| anyhow!("vertex plan decode: {e:?}"))?;
    assert_eq!(vertex_durable.parts.len(), 1);
    let part = &vertex_durable.parts[0];
    assert_eq!(part.descriptor.shard, 7);
    NodeRef::Remote(&a_client)
        .prepare(
            &authority(),
            &part.descriptor,
            part.request.clone(),
            &admitted_vertex,
        )
        .await?;
    c.command(
        &authority(),
        1,
        DecisionCommand::Prepared {
            id: vertex_plan.id(),
            participant: part.descriptor.clone(),
        },
    )
    .await?;
    c.command(
        &authority(),
        1,
        DecisionCommand::Decide {
            id: vertex_plan.id(),
            decision: Decision::Commit,
        },
    )
    .await?;
    let vertex_decided = c.journal(&authority(), 1).await?;
    let vertex_ordinal = vertex_decided
        .transaction(vertex_plan.id())
        .context("vertex decision")?
        .ordinal;
    assert_eq!(vertex_ordinal, 4);
    NodeRef::Remote(&a_client)
        .advance(&authority(), &target(7), vertex_ordinal, &vertex_decided)
        .await?;
    assert_eq!(
        NodeRef::Remote(&b_client)
            .visibility(&target(8))
            .await?
            .applied_prefix,
        ordinal
    );
    assert_eq!(
        call(&g, Operation::Status(vertex_write.clone())).await?,
        Reply::Write {
            external_id: vertex_write.external_id,
            transaction_id: Some(vertex_plan.id()),
            status: WriteStatus::Committed {
                ordinal: vertex_ordinal,
                visible: false
            },
        }
    );
    assert!(call(
        &g,
        Operation::OpenSession {
            version: Some(vertex_ordinal),
            ttl_millis: 60000
        }
    )
    .await
    .is_err());
    let common_three = open(&g, None).await?;
    assert_eq!(common_three.version, ordinal);
    assert!(read(&g, &common_three, 7, "g.V(2)").await?.is_empty());
    // Release this temporary session before Submit's preparation reservation.
    call(
        &g,
        Operation::Close {
            session: common_three,
        },
    )
    .await?;
    assert_eq!(
        committed(call(&g, Operation::Submit(vertex_write.clone())).await?),
        (vertex_plan.id(), vertex_ordinal)
    );
    assert_eq!(
        NodeRef::Remote(&b_client)
            .visibility(&target(8))
            .await?
            .applied_prefix,
        vertex_ordinal
    );
    let at_four = open(&g, Some(vertex_ordinal)).await?;
    assert_eq!(
        read(&g, &at_four, 7, "g.V(2)").await?,
        vec![QueryRow::Element(QualifiedElement::Vertex {
            partition: 7,
            id: 2
        })]
    );
    assert!(read(&g, &fresh, 7, "g.V(2)").await?.is_empty());
    assert!(read(&g, &old, 7, "g.V(2)").await?.is_empty());
    call(&g, Operation::Close { session: at_four }).await?;
    let final_journal = c.journal(&authority(), 1).await?;
    assert_eq!(final_journal.transaction_count(), 4);
    assert_eq!(
        final_journal.request(vertex_write.external_id),
        Some(&vertex_record)
    );
    assert_eq!(final_journal.request(write.external_id), Some(&record));
    call(&g, Operation::Close { session: fresh }).await?;
    call(&g, Operation::Close { session: old }).await?;
    let (stale_session, stale_cursor) = fixed_version_traversal_scenario(&g).await?;
    g.shutdown().await;
    b.stop()?;
    coordinator.stop()?;
    b.reopen().await?;
    coordinator.reopen().await?;
    // A new gateway incarnation cannot revive old sessions or page tokens,
    // although all durable graph writes and their caller-supplied IDs survive.
    let reopened = Gateway::bind_remote(
        RemoteConfig {
            coordinator: c_client.clone(),
            binding: binding(),
            members: vec![a_client.clone(), b_client.clone()],
        },
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    assert!(page(
        &reopened,
        &stale_session,
        7,
        "g.V(101).out('reports').out('reports').out('reports').path()",
        Some(stale_cursor),
        1
    )
    .await
    .is_err());
    assert!(traverse(&reopened, &stale_session, 7, "g.V(101)")
        .await
        .is_err());
    let recovered_session = open(&reopened, None).await?;
    assert_eq!(
        traverse(
            &reopened,
            &recovered_session,
            7,
            "g.V(101,101).bothE('reports').dedup().count()"
        )
        .await?,
        vec![QueryRow::Count(2)]
    );
    assert_eq!(
        traverse(
            &reopened,
            &recovered_session,
            7,
            "g.V(101).out('reports').values('name')"
        )
        .await?,
        vec![value(8, 101, "Robert")]
    );
    assert_eq!(
        traverse(&reopened, &recovered_session, 8, "g.V(103).values('name')").await?,
        vec![value(8, 103, "Cara")]
    );
    call(
        &reopened,
        Operation::Close {
            session: recovered_session,
        },
    )
    .await?;
    // Separate short phase: never lengthen the earlier TTL-tight retained
    // fixture, and never replace its unrolled traversal assertions.
    // Wait only after the original sessions close and before opening ours.
    let _fixture_phase = RETAINED_FIXTURE_PHASE.lock().await;
    let (repeat_version, repeat_rows) = fixed_version_repeat_scenario(&reopened).await?;
    reopened.shutdown().await;
    a.stop()?;
    b.stop()?;
    coordinator.stop()?;
    a.reopen().await?;
    b.reopen().await?;
    coordinator.reopen().await?;
    let recovered = Gateway::bind_remote(
        RemoteConfig {
            coordinator: c_client,
            binding: binding(),
            members: vec![a_client, b_client],
        },
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    let historical = open(&recovered, Some(repeat_version)).await?;
    assert_eq!(
        traverse(&recovered, &historical, 7, REPEAT_PATH_QUERY).await?,
        repeat_rows
    );
    let historical_page = page(&recovered, &historical, 7, REPEAT_PATH_QUERY, None, 3).await?;
    assert_eq!(historical_page.total, 8);
    assert_eq!(historical_page.data.rows, repeat_rows[..3]);
    assert_eq!(
        page(&recovered, &historical, 7, REPEAT_PATH_QUERY, None, 3).await?,
        historical_page
    );
    call(
        &recovered,
        Operation::Close {
            session: historical,
        },
    )
    .await?;
    fixed_version_value_map_scenario(&recovered).await?;
    range_queries::historical(&recovered, stale_session.version).await?;
    recovered.shutdown().await;
    Ok(())
}

// Isolated post-reopen TCP phase. No earlier session lifetime is extended.
async fn fixed_version_value_map_scenario(g: &Gateway) -> anyhow::Result<()> {
    let session = open(g, None).await?;
    let map = |partition, name: &str| QueryRow::PropertyMap {
        source: element(partition, 101),
        properties: BTreeMap::from([
            ("name".into(), vec![Value::Text(name.into())]),
            ("nullable".into(), vec![Value::Null]),
        ]),
    };
    let alice = map(7, "Alice");
    let robert = map(8, "Robert");
    assert_eq!(
        traverse(
            g,
            &session,
            7,
            "g.E(101).bothV().valueMap('nullable','missing','name')"
        )
        .await?,
        vec![alice, robert.clone()]
    );
    let text = "g.V(101,101).out('reports').valueMap('name','nullable','missing')";
    let first = page(g, &session, 7, text, None, 1).await?;
    assert_eq!((first.offset, first.total), (0, 2));
    assert_eq!(first.data.rows, vec![robert.clone()]);
    assert_eq!(page(g, &session, 7, text, None, 1).await?, first);
    let last = page(g, &session, 7, text, first.next.clone(), 1).await?;
    assert_eq!((last.offset, last.total), (1, 2));
    assert_eq!(last.data.rows, vec![robert]);
    assert!(last.next.is_none());
    assert_eq!(page(g, &session, 7, text, first.next, 1).await?, last);
    assert_eq!(
        call(
            g,
            Operation::Traverse {
                session: session.clone(),
                query: Query {
                    partition: 7,
                    text: "g.E(101).valueMap('name')".into()
                },
            }
        )
        .await,
        Err(gateway::Error::Query(
            replicated_graph::remote::query::QueryFailure::Unsupported
        ))
    );
    call(g, Operation::Close { session }).await?;
    Ok(())
}

const REPEAT_PATH_QUERY: &str = "g.V(501).repeat(out()).times(2).emit().path()";

/// Small independent fixture with an explicit ordered oracle. Its session
/// lifetime and original query/result budgets are unchanged.
async fn fixed_version_repeat_scenario(g: &Gateway) -> anyhow::Result<(u64, Vec<QueryRow>)> {
    for write in [
        person_write(60, 7, 501, "repeat_person", "A"),
        person_write(61, 8, 501, "repeat_person", "B"),
        relation_write(62, 501, (7, 501), (8, 501), "repeat"),
        relation_write(63, 502, (7, 501), (8, 501), "repeat"),
        relation_write(64, 501, (8, 501), (7, 501), "repeat"),
        relation_write(65, 503, (7, 501), (7, 501), "single"),
    ] {
        committed(call(g, Operation::Submit(write)).await?);
    }
    let a = element(7, 501);
    let b = element(8, 501);
    let ab = QualifiedElement::Edge {
        partition: 7,
        id: 501,
    };
    let parallel = QualifiedElement::Edge {
        partition: 7,
        id: 502,
    };
    let ba = QualifiedElement::Edge {
        partition: 8,
        id: 501,
    };
    let aa = QualifiedElement::Edge {
        partition: 7,
        id: 503,
    };
    // Three first-level rows followed by five second-level rows. Equal vertex
    // IDs do not collapse partitions, parallel paths, or cyclic/self paths.
    let paths = vec![
        vec![a, ab, b],
        vec![a, parallel, b],
        vec![a, aa, a],
        vec![a, ab, b, ba, a],
        vec![a, parallel, b, ba, a],
        vec![a, aa, a, ab, b],
        vec![a, aa, a, parallel, b],
        vec![a, aa, a, aa, a],
    ]
    .into_iter()
    .map(QueryRow::Path)
    .collect::<Vec<_>>();
    let old = open(g, None).await?;
    let version = old.version;
    assert_eq!(
        traverse(g, &old, 7, "g.V(501).repeat(out()).times(2).path()").await?,
        paths[3..]
    );
    assert_eq!(
        traverse(
            g,
            &old,
            7,
            "g.V(501).repeat(in('repeat')).times(1).out('repeat').count()"
        )
        .await?,
        vec![QueryRow::Count(1)]
    );
    assert_eq!(
        traverse(
            g,
            &old,
            7,
            "g.E(501).inV().repeat(out('repeat')).times(2).id().dedup()"
        )
        .await?,
        vec![QueryRow::Value {
            source: Some(b),
            value: Value::Integer(501)
        }]
    );
    let mut longest = vec![aa, a];
    for _ in 0..32 {
        longest.extend([aa, a]);
    }
    assert_eq!(
        traverse(
            g,
            &old,
            7,
            "g.E(503).inV().repeat(out('single')).times(32).path()"
        )
        .await?,
        vec![QueryRow::Path(longest)]
    );
    assert!(
        read(g, &old, 7, "g.V(501).repeat(out()).times(1)")
            .await
            .is_err(),
        "original managed Read must stay unchanged"
    );
    let first = page(g, &old, 7, REPEAT_PATH_QUERY, None, 3).await?;
    assert_eq!(first.offset, 0);
    assert_eq!(first.total, paths.len() as u32);
    assert_eq!(first.data.rows, paths[..3]);
    let delete = Write {
        external_id: [66; 16],
        mutation: Mutation::DeleteEdge {
            edge: Entity {
                partition: 7,
                id: 502,
            },
        },
    };
    let (written, observed) = tokio::join!(
        call(g, Operation::Submit(delete)),
        traverse(g, &old, 7, REPEAT_PATH_QUERY),
    );
    let (_, latest_version) = committed(written?);
    assert!(latest_version > version);
    assert_eq!(observed?, paths);
    assert_eq!(page(g, &old, 7, REPEAT_PATH_QUERY, None, 3).await?, first);
    let mut cursor = first.next;
    let mut collected = first.data.rows;
    // Exactly two remaining pages. No open-ended pagination loop.
    for offset in [3, 6] {
        let token = cursor.take().context("repeat continuation")?;
        let next = page(g, &old, 7, REPEAT_PATH_QUERY, Some(token.clone()), 3).await?;
        assert_eq!(next.offset, offset);
        assert_eq!(next.total, 8);
        assert_eq!(
            next,
            page(g, &old, 7, REPEAT_PATH_QUERY, Some(token), 3).await?
        );
        collected.extend(next.data.rows);
        cursor = next.next;
    }
    assert!(cursor.is_none());
    assert_eq!(collected, paths);
    call(g, Operation::Close { session: old }).await?;
    let fresh = open(g, None).await?;
    assert_eq!(
        traverse(g, &fresh, 7, "g.V(501).repeat(out()).times(2).count()").await?,
        vec![QueryRow::Count(3)]
    );
    call(g, Operation::Close { session: fresh }).await?;
    Ok((version, paths))
}

async fn traverse(
    g: &Gateway,
    session: &Session,
    partition: u32,
    text: &str,
) -> anyhow::Result<Vec<QueryRow>> {
    let query = Query {
        partition,
        text: text.into(),
    };
    match call(
        g,
        Operation::Traverse {
            session: session.clone(),
            query: query.clone(),
        },
    )
    .await?
    {
        Reply::Traverse {
            session: returned,
            query: echoed,
            data,
        } => {
            assert_eq!(returned, *session);
            assert_eq!(echoed, query);
            assert_eq!(data.completeness, QueryCompleteness::Unverified);
            Ok(data.rows)
        }
        other => anyhow::bail!("expected traversal: {other:?}"),
    }
}
async fn page(
    g: &Gateway,
    session: &Session,
    partition: u32,
    text: &str,
    cursor: Option<PageCursor>,
    page_size: u16,
) -> anyhow::Result<QueryPage> {
    let query = Query {
        partition,
        text: text.into(),
    };
    match call(
        g,
        Operation::TraversePage {
            session: session.clone(),
            query: query.clone(),
            cursor: cursor.clone(),
            page_size,
        },
    )
    .await?
    {
        Reply::TraversePage {
            session: returned,
            query: echoed,
            cursor: returned_cursor,
            page_size: returned_size,
            page,
        } => {
            assert_eq!(returned, *session);
            assert_eq!(echoed, query);
            assert_eq!(returned_cursor, cursor);
            assert_eq!(returned_size, page_size);
            assert_eq!(page.data.completeness, QueryCompleteness::Unverified);
            Ok(page)
        }
        other => anyhow::bail!("expected page: {other:?}"),
    }
}
fn element(partition: u32, id: u64) -> QualifiedElement {
    QualifiedElement::Vertex { partition, id }
}
fn row(partition: u32, id: u64) -> QueryRow {
    QueryRow::Element(element(partition, id))
}
fn value(partition: u32, id: u64, text: &str) -> QueryRow {
    QueryRow::Value {
        source: Some(element(partition, id)),
        value: Value::Text(text.into()),
    }
}
fn person_write(external: u8, partition: u32, id: u64, label: &str, name: &str) -> Write {
    Write {
        external_id: [external; 16],
        mutation: Mutation::CreateVertex {
            partition,
            vertex: VertexData {
                id,
                labels: vec![label.into()],
                properties: BTreeMap::from([("name".into(), Value::Text(name.into()))]),
            },
        },
    }
}
fn relation_write(external: u8, id: u64, from: (u32, u64), to: (u32, u64), label: &str) -> Write {
    Write {
        external_id: [external; 16],
        mutation: Mutation::CreateEdge {
            edge: EdgeData {
                id,
                from: Entity {
                    partition: from.0,
                    id: from.1,
                },
                to: Entity {
                    partition: to.0,
                    id: to.1,
                },
                label: label.into(),
                properties: BTreeMap::new(),
            },
        },
    }
}

/// Small generic personnel graph, not a large CoreHR benchmark. Edge identity is
/// source-qualified: both partitions own edge 101 and vertex 101. Parallel
/// reporting edges must retain bag multiplicity even on repeated cyclic paths.
async fn fixed_version_traversal_scenario(g: &Gateway) -> anyhow::Result<(Session, PageCursor)> {
    let fixture = vec![
        person_write(20, 7, 101, "person", "Alice"),
        person_write(21, 8, 101, "person", "Bob"),
        person_write(22, 7, 102, "department", "Engineering"),
        person_write(23, 8, 102, "department", "Research"),
        relation_write(24, 101, (7, 101), (8, 101), "reports"),
        relation_write(25, 102, (7, 101), (8, 101), "reports"),
        relation_write(26, 101, (8, 101), (7, 101), "reports"),
        relation_write(27, 103, (7, 101), (7, 102), "member"),
        relation_write(28, 104, (7, 101), (8, 102), "member"),
        relation_write(29, 102, (8, 101), (8, 102), "member"),
    ];
    for mut write in fixture {
        // Reuse the personnel records: explicit Null is a value; departments
        // omit this property and must not produce scalar rows.
        if let Mutation::CreateVertex { vertex, .. } = &mut write.mutation {
            if vertex.id == 101 {
                vertex.properties.insert("nullable".into(), Value::Null);
            }
        }
        committed(call(g, Operation::Submit(write)).await?);
    }
    let old = open(g, None).await?;
    // Expansion is in edge-ID order. Each hop records its canonical edge,
    // so the two independent choices of parallel edge yield distinct paths.
    let paths: Vec<_> = [101, 102]
        .into_iter()
        .flat_map(|first_edge| {
            [101, 102].into_iter().map(move |last_edge| {
                QueryRow::Path(vec![
                    element(7, 101),
                    QualifiedElement::Edge {
                        partition: 7,
                        id: first_edge,
                    },
                    element(8, 101),
                    QualifiedElement::Edge {
                        partition: 8,
                        id: 101,
                    },
                    element(7, 101),
                    QualifiedElement::Edge {
                        partition: 7,
                        id: last_edge,
                    },
                    element(8, 101),
                ])
            })
        })
        .collect();
    let path_query = "g.V(101).out('reports').out('reports').out('reports').path()";
    // Reference expectations are fixture-derived, not obtained from the query
    // engine under test. In particular, no dedup is allowed on the four paths.
    let id_value = |source, id| QueryRow::Value {
        source: Some(source),
        value: Value::Integer(id),
    };
    let expectations = vec![
        (
            7,
            "g.E(101).bothV().id().is(101)",
            vec![
                id_value(element(7, 101), 101),
                id_value(element(8, 101), 101),
            ],
        ),
        (
            7,
            "g.V(101).out('reports').values('name').is('Bob').dedup()",
            vec![value(8, 101, "Bob")],
        ),
        (
            7,
            "g.V(101).out('member').label().is('department').count()",
            vec![QueryRow::Count(2)],
        ),
        (
            7,
            "g.V(101,102).values('nullable').is(null).count()",
            vec![QueryRow::Count(1)],
        ),
        (
            7,
            "g.V(101,101,102).id().limit(2).is(102).count()",
            vec![QueryRow::Count(0)],
        ),
        (
            7,
            "g.V(101,101,102).id().is(102).limit(2).count()",
            vec![QueryRow::Count(1)],
        ),
        (
            7,
            "g.E(101).bothV().id().dedup()",
            vec![id_value(element(7, 101), 101)],
        ),
        (
            8,
            "g.E(101).bothV().id().dedup()",
            vec![id_value(element(8, 101), 101)],
        ),
        (
            7,
            "g.V(101).bothE('reports').id().dedup()",
            vec![
                id_value(
                    QualifiedElement::Edge {
                        partition: 7,
                        id: 101,
                    },
                    101,
                ),
                id_value(
                    QualifiedElement::Edge {
                        partition: 7,
                        id: 102,
                    },
                    102,
                ),
            ],
        ),
        (
            7,
            "g.E(101).bothV().label().dedup()",
            vec![value(7, 101, "person")],
        ),
        (
            7,
            "g.V(101).out('reports').values('name').dedup()",
            vec![value(8, 101, "Bob")],
        ),
        (
            7,
            "g.V(101,102).values('nullable').dedup()",
            vec![QueryRow::Value {
                source: Some(element(7, 101)),
                value: Value::Null,
            }],
        ),
        (
            7,
            "g.E(101).bothV().values('nullable').dedup().count()",
            vec![QueryRow::Count(1)],
        ),
        (
            7,
            "g.V(101,101,102).id().limit(2).dedup().count()",
            vec![QueryRow::Count(1)],
        ),
        (
            7,
            "g.V(101,101,102).id().dedup().limit(2).count()",
            vec![QueryRow::Count(2)],
        ),
        (7, "g.V(999).values('missing').dedup()", vec![]),
        (7, "g.E(999).id().dedup()", vec![]),
        (
            7,
            "g.V(101,101).dedup().out('reports').count()",
            vec![QueryRow::Count(2)],
        ),
        (
            7,
            "g.V(101).out('reports').dedup().values('name')",
            vec![value(8, 101, "Bob")],
        ),
        (
            7,
            "g.V(101).out('member').dedup()",
            vec![row(7, 102), row(8, 102)],
        ),
        (
            7,
            "g.E(101,101).bothV().dedup()",
            vec![row(7, 101), row(8, 101)],
        ),
        (
            7,
            "g.V(101).bothE('reports').dedup().count()",
            vec![QueryRow::Count(3)],
        ),
        (
            7,
            "g.V(101).out('reports').out('reports').dedup().path()",
            vec![QueryRow::Path(vec![
                element(7, 101),
                QualifiedElement::Edge {
                    partition: 7,
                    id: 101,
                },
                element(8, 101),
                QualifiedElement::Edge {
                    partition: 8,
                    id: 101,
                },
                element(7, 101),
            ])],
        ),
        (7, "g.V(101,101,102).limit(2).dedup()", vec![row(7, 101)]),
        (
            7,
            "g.V(101,101,102).dedup().limit(2)",
            vec![row(7, 101), row(7, 102)],
        ),
        (7, "g.V(999).dedup().count()", vec![QueryRow::Count(0)]),
        (7, "g.V(101).id()", vec![id_value(element(7, 101), 101)]),
        (
            7,
            "g.V(101).out('reports').id()",
            vec![id_value(element(8, 101), 101); 2],
        ),
        (
            7,
            "g.V().hasLabel('person').id()",
            vec![
                // The recovery scenario created person 1 in both partitions
                // before adding this traversal fixture.
                id_value(element(7, 1), 1),
                id_value(element(8, 1), 1),
                id_value(element(7, 101), 101),
                id_value(element(8, 101), 101),
            ],
        ),
        (
            7,
            "g.E(101).id()",
            vec![id_value(
                QualifiedElement::Edge {
                    partition: 7,
                    id: 101,
                },
                101,
            )],
        ),
        (
            8,
            "g.E(101).id()",
            vec![id_value(
                QualifiedElement::Edge {
                    partition: 8,
                    id: 101,
                },
                101,
            )],
        ),
        (7, path_query, paths.clone()),
        (
            7,
            "g.V(101).out('reports').out('reports').out('reports').count()",
            vec![QueryRow::Count(4)],
        ),
        (
            7,
            "g.V(101).out('reports').has('name','Bob').values('name')",
            vec![value(8, 101, "Bob"); 2],
        ),
        (
            7,
            "g.V(101).out('member').hasLabel('department').values('name')",
            vec![value(7, 102, "Engineering"), value(8, 102, "Research")],
        ),
        (
            8,
            "g.V(102).in('member').hasLabel('person').count()",
            vec![QueryRow::Count(2)],
        ),
        (
            7,
            "g.V(101).both('reports').count()",
            vec![QueryRow::Count(3)],
        ),
        (7, "g.V(101).outE('reports').inV()", vec![row(8, 101); 2]),
        (8, "g.V(101).inE('reports').outV()", vec![row(7, 101); 2]),
        (7, "g.E(101).bothV()", vec![row(7, 101), row(8, 101)]),
        (8, "g.E(101).bothV()", vec![row(8, 101), row(7, 101)]),
        (
            7,
            "g.V(101).bothE('reports').count()",
            vec![QueryRow::Count(3)],
        ),
        (
            7,
            "g.V().hasLabel('department').values('name')",
            vec![value(7, 102, "Engineering"), value(8, 102, "Research")],
        ),
        (
            7,
            "g.E().hasLabel('reports').count()",
            vec![QueryRow::Count(3)],
        ),
        (7, "g.V(101).out('reports').limit(1)", vec![row(8, 101)]),
        (8, "g.V(101).label()", vec![value(8, 101, "person")]),
        (8, "g.V(103)", vec![]),
    ];
    for (partition, query, expected) in &expectations {
        assert_eq!(
            traverse(g, &old, *partition, query).await?,
            *expected,
            "{query}"
        );
    }
    let first = page(g, &old, 7, path_query, None, 1).await?;
    assert_eq!(first.offset, 0);
    assert_eq!(first.total, 4);
    assert_eq!(first.data.rows, paths[..1]);
    let next = first
        .next
        .clone()
        .context("four-row page has continuation")?;
    for (query, size) in [
        ("g.V(101).count()", 1),
        (path_query, 2),
        (path_query, 0),
        (path_query, 1001),
    ] {
        assert!(call(
            g,
            Operation::TraversePage {
                session: old.clone(),
                query: Query {
                    partition: 7,
                    text: query.into()
                },
                cursor: Some(next.clone()),
                page_size: size
            }
        )
        .await
        .is_err());
    }
    let mut altered_token = next.clone();
    altered_token.token[0] ^= 1;
    assert!(page(g, &old, 7, path_query, Some(altered_token), 1)
        .await
        .is_err());
    assert!(page(g, &old, 8, path_query, Some(next.clone()), 1)
        .await
        .is_err());
    let mut wrong_version = old.clone();
    wrong_version.version += 1;
    assert!(
        page(g, &wrong_version, 7, path_query, Some(next.clone()), 1)
            .await
            .is_err()
    );
    let another = open(g, Some(old.version)).await?;
    assert!(page(g, &another, 7, path_query, Some(next.clone()), 1)
        .await
        .is_err());
    call(g, Operation::Close { session: another }).await?;
    for unsupported in [
        "g.V(999).is(1)",
        "g.E(999).is(1)",
        "g.V(999).values('missing').is(null).out()",
        "g.E(999).id().is(1).inV()",
        "g.V(999).label().is('person').path()",
        "g.V(101).id().is(0).hasLabel('person')",
        "g.V(999).id().is(eq(1))",
        "g.V(999).id().is(gt(1))",
        "g.V(101).id().out()",
        "g.V(999).values('missing').dedup().out()",
        "g.E(999).id().dedup().inV()",
        "g.V(999).label().dedup().path()",
        "g.V(101).values('name').dedup().hasLabel('person')",
        "g.V(999).dedup('a')",
        "g.V(999).dedup().by('name')",
        "g.V(101).valueMap('name').count()",
        "g.V(101).repeat(both()).times(2)",
        "g.V(101).out().out().out().out()",
    ] {
        assert!(
            traverse(g, &old, 7, unsupported).await.is_err(),
            "accepted {unsupported}"
        );
    }
    let distinct_session = open(g, Some(old.version)).await?;
    let distinct_query = "g.V(101,101).bothE('reports').dedup()";
    let distinct_rows: Vec<_> = [(7, 101), (8, 101), (7, 102)]
        .into_iter()
        .map(|(partition, id)| QueryRow::Element(QualifiedElement::Edge { partition, id }))
        .collect();
    let distinct_first = page(g, &distinct_session, 7, distinct_query, None, 1).await?;
    assert_eq!(distinct_first.total, 3);
    assert_eq!(distinct_first.data.rows, distinct_rows[..1]);
    let scalar_session = open(g, Some(old.version)).await?;
    let scalar_query = "g.E(101,101).bothV().values('name').dedup()";
    let scalar_rows = vec![value(7, 101, "Alice"), value(8, 101, "Bob")];
    let scalar_first = page(g, &scalar_session, 7, scalar_query, None, 1).await?;
    assert_eq!(scalar_first.offset, 0);
    assert_eq!(scalar_first.total, 2);
    assert_eq!(scalar_first.data.rows, scalar_rows[..1]);
    let scalar_cursor = scalar_first.next.clone().context("two scalar values")?;
    let writes = vec![
        person_write(30, 8, 103, "person", "Cara"),
        Write {
            external_id: [31; 16],
            mutation: Mutation::PatchVertex {
                vertex: Entity {
                    partition: 8,
                    id: 101,
                },
                patch: Patch {
                    set: BTreeMap::from([("name".into(), Value::Text("Robert".into()))]),
                    remove: BTreeSet::new(),
                },
            },
        },
        Write {
            external_id: [32; 16],
            mutation: Mutation::DeleteEdge {
                edge: Entity {
                    partition: 7,
                    id: 102,
                },
            },
        },
        relation_write(33, 103, (8, 103), (7, 102), "member"),
    ];
    // Rendezvous makes each committed write and each read phase explicit. The
    // writer cannot race ahead of the reader; no sleep or scheduler luck is used.
    let (go_tx, mut go_rx) = tokio::sync::mpsc::channel::<()>(1);
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
    let address = g.local_addr();
    let writer_session = old.clone();
    let writer_paths = paths.clone();
    let writer = tokio::spawn(async move {
        for write in writes {
            go_rx.recv().await.context("reader released writer")?;
            // Poll Submit concurrently with sequential Traverse and page TCP
            // calls. Same-session queries intentionally never overlap because
            // their admission uses try_lock. Every write/read interleaving
            // must still return the same retained old V.
            let query = Query {
                partition: 7,
                text: path_query.into(),
            };
            let (result, (traversal, paged)) = tokio::join!(
                gateway::request(address, binding(), Operation::Submit(write), BUDGET),
                async {
                    let traversal = gateway::request(
                        address,
                        binding(),
                        Operation::Traverse {
                            session: writer_session.clone(),
                            query: query.clone(),
                        },
                        BUDGET,
                    )
                    .await;
                    let paged = gateway::request(
                        address,
                        binding(),
                        Operation::TraversePage {
                            session: writer_session.clone(),
                            query,
                            cursor: None,
                            page_size: 1,
                        },
                        BUDGET,
                    )
                    .await;
                    (traversal, paged)
                },
            );
            match traversal? {
                Reply::Traverse { session, data, .. } => {
                    assert_eq!(session, writer_session);
                    assert_eq!(data.rows, writer_paths);
                }
                other => anyhow::bail!("concurrent traversal: {other:?}"),
            }
            match paged? {
                Reply::TraversePage { session, page, .. } => {
                    assert_eq!(session, writer_session);
                    assert_eq!(page.total, 4);
                    assert_eq!(page.offset, 0);
                    assert_eq!(page.data.rows, writer_paths[..1]);
                }
                other => anyhow::bail!("concurrent page: {other:?}"),
            }
            done_tx
                .send(result)
                .await
                .map_err(|_| anyhow!("reader stopped"))?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let mut cursor = first.next;
    let mut collected = first.data.rows;
    let mut last_version = old.version;
    for phase in 0..4 {
        go_tx.send(()).await?;
        let (_, version) = committed(done_rx.recv().await.context("writer result")??);
        assert!(version > last_version);
        last_version = version;
        for (partition, query, expected) in &expectations {
            assert_eq!(
                traverse(g, &old, *partition, query).await?,
                *expected,
                "phase {phase}: {query}"
            );
        }
        if let Some(token) = cursor.take() {
            let next_page = page(g, &old, 7, path_query, Some(token.clone()), 1).await?;
            assert_eq!(
                next_page,
                page(g, &old, 7, path_query, Some(token), 1).await?,
                "continuation replay is immutable"
            );
            assert_eq!(next_page.offset as usize, collected.len());
            assert_eq!(next_page.total, 4);
            collected.extend(next_page.data.rows);
            cursor = next_page.next;
        }
        // Full deduplicated results and cursor replay stay at old V, including
        // after one of the source edges is deleted from the latest graph.
        assert_eq!(
            page(g, &distinct_session, 7, distinct_query, None, 1).await?,
            distinct_first
        );
        let mut distinct_cursor = distinct_first.next.clone();
        let mut distinct_collected = distinct_first.data.rows.clone();
        while let Some(token) = distinct_cursor {
            let next = page(
                g,
                &distinct_session,
                7,
                distinct_query,
                Some(token.clone()),
                1,
            )
            .await?;
            assert_eq!(
                next,
                page(g, &distinct_session, 7, distinct_query, Some(token), 1).await?
            );
            distinct_collected.extend(next.data.rows);
            distinct_cursor = next.next;
        }
        assert_eq!(distinct_collected, distinct_rows);
        assert_eq!(
            traverse(g, &scalar_session, 7, scalar_query).await?,
            scalar_rows
        );
        assert_eq!(
            page(g, &scalar_session, 7, scalar_query, None, 1).await?,
            scalar_first
        );
        let scalar_last = page(
            g,
            &scalar_session,
            7,
            scalar_query,
            Some(scalar_cursor.clone()),
            1,
        )
        .await?;
        assert_eq!(scalar_last.offset, 1);
        assert_eq!(scalar_last.total, 2);
        assert_eq!(scalar_last.data.rows, scalar_rows[1..]);
        assert!(scalar_last.next.is_none());
        assert_eq!(
            scalar_last,
            page(
                g,
                &scalar_session,
                7,
                scalar_query,
                Some(scalar_cursor.clone()),
                1
            )
            .await?
        );
        let fresh = open(g, Some(version)).await?;
        assert_eq!(
            traverse(g, &fresh, 8, "g.V(103).values('name')").await?,
            vec![value(8, 103, "Cara")]
        );
        let name = if phase == 0 { "Bob" } else { "Robert" };
        assert_eq!(
            traverse(g, &fresh, 7, scalar_query).await?,
            vec![value(7, 101, "Alice"), value(8, 101, name)]
        );
        // The old V still observes Bob after the later rename, while fresh V
        // rejects the renamed value. Both old parallel occurrences survive.
        let filtered_query = "g.E(101,101).bothV().values('name').is('Bob')";
        assert_eq!(
            traverse(g, &old, 7, filtered_query).await?,
            vec![value(8, 101, "Bob"); 2]
        );
        assert_eq!(
            traverse(g, &fresh, 7, filtered_query).await?,
            if phase == 0 {
                vec![value(8, 101, "Bob"); 2]
            } else {
                vec![]
            }
        );
        let multiplicity = if phase < 2 { 2 } else { 1 };
        assert_eq!(
            traverse(g, &fresh, 7, "g.V(101).out('reports').values('name')").await?,
            vec![value(8, 101, name); multiplicity]
        );
        assert_eq!(
            traverse(g, &fresh, 7, path_query).await?.len(),
            multiplicity * multiplicity
        );
        assert_eq!(
            traverse(g, &fresh, 7, "g.V(102).in('member').count()").await?,
            vec![QueryRow::Count(if phase == 3 { 2 } else { 1 })]
        );
        assert_eq!(
            traverse(
                g,
                &fresh,
                7,
                "g.V(101).out('reports').dedup().values('name')"
            )
            .await?,
            vec![value(8, 101, name)]
        );
        assert_eq!(
            traverse(g, &fresh, 7, distinct_query).await?.len(),
            if phase < 2 { 3 } else { 2 }
        );
        call(g, Operation::Close { session: fresh }).await?;
    }
    writer.await??;
    assert_eq!(collected, paths);
    assert!(cursor.is_none());
    // Even after every mutation, starting and replaying the first page is exact.
    let replay = page(g, &old, 7, path_query, None, 1).await?;
    assert_eq!(replay.data.rows, collected[..1]);
    assert_eq!(replay.total, 4);
    call(
        g,
        Operation::Close {
            session: distinct_session,
        },
    )
    .await?;
    call(
        g,
        Operation::Close {
            session: scalar_session,
        },
    )
    .await?;
    // Reuse released capacity, not a renewed session: materialize the original
    // historical V after the writes above. The live old session was also checked
    // with is() through every write; this separate session checks page replay.
    let filtered_session = open(g, Some(old.version)).await?;
    let filtered_query = "g.E(101,101).bothV().values('name').is('Bob')";
    let filtered_first = page(g, &filtered_session, 7, filtered_query, None, 1).await?;
    assert_eq!(filtered_first.offset, 0);
    assert_eq!(filtered_first.total, 2);
    assert_eq!(filtered_first.data.rows, vec![value(8, 101, "Bob")]);
    let filtered_cursor = filtered_first.next.clone().context("two matching values")?;
    let filtered_last = page(
        g,
        &filtered_session,
        7,
        filtered_query,
        Some(filtered_cursor.clone()),
        1,
    )
    .await?;
    assert_eq!(filtered_last.offset, 1);
    assert_eq!(filtered_last.total, 2);
    assert_eq!(filtered_last.data.rows, vec![value(8, 101, "Bob")]);
    assert!(filtered_last.next.is_none());
    assert_eq!(
        page(g, &filtered_session, 7, filtered_query, None, 1).await?,
        filtered_first
    );
    assert_eq!(
        page(
            g,
            &filtered_session,
            7,
            filtered_query,
            Some(filtered_cursor),
            1
        )
        .await?,
        filtered_last
    );
    call(
        g,
        Operation::Close {
            session: filtered_session,
        },
    )
    .await?;
    Ok((old, next))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_old_prefix_captures_while_other_transactions_commit() -> anyhow::Result<()> {
    let mut coordinator = Process::start(9).await?;
    let mut a = Process::start(7).await?;
    let mut b = Process::start(8).await?;
    let g = Gateway::bind_remote(
        RemoteConfig {
            coordinator: coordinator.client()?,
            binding: binding(),
            members: vec![a.client()?, b.client()?],
        },
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    committed(call(&g, Operation::Submit(create(1, 7))).await?);
    committed(call(&g, Operation::Submit(create(2, 8))).await?);
    let c = coordinator.client()?;
    let before = NodeRef::Remote(&c).terminal(&authority(), 1, 2).await?;
    let barrier = tokio::sync::Barrier::new(2);
    let writes = async {
        barrier.wait().await;
        for id in 3..=6 {
            let write = Write {
                external_id: [id; 16],
                mutation: Mutation::PatchVertex {
                    vertex: Entity {
                        partition: 7,
                        id: 1,
                    },
                    patch: Patch {
                        set: BTreeMap::from([("marker".into(), Value::Integer(id as i64))]),
                        remove: BTreeSet::new(),
                    },
                },
            };
            assert_eq!(
                committed(call(&g, Operation::Submit(write)).await?).1,
                id as u64
            );
        }
        Ok::<_, anyhow::Error>(())
    };
    let reads = async {
        barrier.wait().await;
        for _ in 0..6 {
            let old = open(&g, Some(2)).await?;
            assert_eq!(old.version, 2);
            assert_eq!(read(&g, &old, 7, "g.V(1)").await?, vec![vertex(7)]);
            assert!(read(&g, &old, 7, "g.V(1).values('marker')")
                .await?
                .is_empty());
            call(&g, Operation::Close { session: old }).await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let (writes, reads) = tokio::join!(writes, reads);
    writes?;
    reads?;
    assert_eq!(
        NodeRef::Remote(&c).terminal(&authority(), 1, 2).await?,
        before
    );
    let latest = open(&g, None).await?;
    assert_eq!(latest.version, 6);
    assert_eq!(
        read(&g, &latest, 7, "g.V(1).values('marker')").await?,
        vec![QueryRow::Value {
            source: Some(QualifiedElement::Vertex {
                partition: 7,
                id: 1
            }),
            value: Value::Integer(6)
        }]
    );
    call(&g, Operation::Close { session: latest }).await?;
    eprintln!("concurrent exact-prefix captures: 6 fixed V2 acquisitions, 4 new commits, final V6; child PIDs {:?}",
        [coordinator.child.id(), a.child.id(), b.child.id()]);
    // Reuse this fixture sequentially: the model adds no parallel child cohort
    // and leaves all original exact-prefix assertions above intact.
    // All prefix sessions are closed; do not overlap the new independent phase.
    let _fixture_phase = RETAINED_FIXTURE_PHASE.lock().await;
    dynamic_query_model::scenario(g, &mut coordinator, &mut a, &mut b).await?;
    Ok(())
}

#[path = "support/dynamic_query_model.rs"]
mod dynamic_query_model;

#[path = "support/lifecycle_metrics.rs"]
mod lifecycle_metrics;
use lifecycle_metrics::Metrics;

#[path = "support/lifecycle_attribution.rs"]
mod lifecycle_attribution;
use lifecycle_attribution::Attribution;

async fn lifecycle_gateway(
    config: RemoteConfig,
    enabled: bool,
    source: &str,
    elf: &str,
) -> anyhow::Result<(Gateway, Attribution)> {
    let address = "127.0.0.1:0".parse()?;
    let (gateway, reports) = if enabled {
        let (gateway, reports) = Gateway::bind_remote_with_timing(config, address, BUDGET).await?;
        (gateway, Some(reports))
    } else {
        (Gateway::bind_remote(config, address, BUDGET).await?, None)
    };
    Ok((gateway, Attribution::new(reports, source, elf)))
}

fn lifecycle_processes(
    coordinator: &Process,
    writer: &Process,
    nonparticipant: &Process,
) -> [(&'static str, u32); 4] {
    [
        ("gateway", std::process::id()),
        ("coordinator", coordinator.child.id()),
        ("writer", writer.child.id()),
        ("nonparticipant", nonparticipant.child.id()),
    ]
}

const EXTENDED_WRITES: u64 = 1025;
const RETAINED_ENV: &str = "BYTE_BOUND_PROCESS_DIRECTORY";

fn byte_binding() -> Binding {
    let mut value = binding();
    value.cohort.profile = CapacityProfile::ByteBound16MiBV1;
    value
}
fn extended_write(ordinal: u64) -> Write {
    Write {
        // The complete u128 identity is stable across every interrupted attempt.
        external_id: u128::from(ordinal).to_be_bytes(),
        mutation: Mutation::CreateVertex {
            partition: 7,
            vertex: VertexData {
                id: ordinal,
                labels: vec!["person".into()],
                properties: BTreeMap::new(),
            },
        },
    }
}
fn canonical_write(write: &Write) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        "managed-graph-command-v1",
        byte_binding(),
        &write.mutation,
    ))?)
}

// Harness persistence owns no production state and keeps all original assertion
// inputs in memory while publishing only small durable references.
#[path = "support/lifecycle_evidence.rs"]
mod lifecycle_evidence;
use lifecycle_evidence::LifecycleCheckpoint;

async fn original_request(coordinator: &Client, ordinal: u64) -> anyhow::Result<RequestRecord> {
    let write = extended_write(ordinal);
    let found = NodeRef::Remote(coordinator)
        .request(&authority(), 1, write.external_id)
        .await?
        .record
        .context("original point request missing")?;
    assert_eq!(found.record.id, write.external_id);
    assert_eq!(found.record.request, canonical_write(&write)?);
    assert_eq!(found.transaction.ordinal, ordinal);
    assert_eq!(found.transaction.decision, Some(Decision::Commit));
    let plan = d2_raft::request_plans::Plan::decode(&found.record.payload)
        .map_err(|e| anyhow!("original Plan: {e:?}"))?;
    assert_eq!(plan.id, found.record.transaction_id);
    assert_eq!(plan.cohort, byte_binding().cohort);
    assert_eq!(plan.parts.len(), 1);
    assert_eq!(plan.parts[0].descriptor.shard, 7);
    assert_eq!(plan.parts[0].request.expected_sequence, Some(ordinal - 1));
    Ok(found.record)
}

async fn retain_original(
    coordinator: &Client,
    ordinal: u64,
    checkpoint: &mut LifecycleCheckpoint,
) -> anyhow::Result<()> {
    let _span = checkpoint.metrics.span("milestone.original_request_plan");
    let record = original_request(coordinator, ordinal).await?;
    if let Some(original) = checkpoint.originals.get(&ordinal) {
        assert_eq!(
            &record, original,
            "original request/Plan changed at {ordinal}"
        );
    } else {
        checkpoint.originals.insert(ordinal, record);
    }
    Ok(())
}

fn assert_complete_commits(
    proof: &d2_raft::terminal_prefix::TerminalPrefix,
    version: u64,
) -> anyhow::Result<()> {
    assert_eq!(proof.version(), version);
    assert_eq!(proof.cohort(), &byte_binding().cohort);
    let encoded = serde_json::to_value(proof)?;
    let segments = encoded["segments"]
        .as_array()
        .context("terminal segments")?;
    let mut ordinal = 0;
    for segment in segments {
        for terminal in segment.as_array().context("terminal segment rows")? {
            ordinal += 1;
            assert_eq!(terminal["ordinal"], ordinal);
            assert_eq!(terminal["decision"], "Commit");
            assert_eq!(
                terminal["participants"]
                    .as_array()
                    .context("participants")?
                    .len(),
                1
            );
            assert_eq!(terminal["participants"][0]["shard"], 7);
            assert_eq!(terminal["prepared"], serde_json::json!([true]));
        }
    }
    assert_eq!(ordinal, version, "proof must contain every ordinal 1..=V");
    Ok(())
}

async fn retain_proof(
    coordinator: &Client,
    version: u64,
    checkpoint: &mut LifecycleCheckpoint,
) -> anyhow::Result<()> {
    let _span = checkpoint.metrics.span("milestone.proof_capture_assertion");
    let proof = NodeRef::Remote(coordinator)
        .terminal(&authority(), 1, version)
        .await?;
    assert_complete_commits(&proof, version)?;
    let bytes = serde_json::to_vec(&proof)?;
    if let Some(original) = checkpoint.proofs.get(&version) {
        assert_eq!(
            &bytes, original,
            "old terminal prefix changed at V{version}"
        );
    } else {
        checkpoint.proofs.insert(version, bytes);
    }
    Ok(())
}

async fn verify_old_versions(g: &Gateway, through: u64) -> anyhow::Result<()> {
    for version in [0, 1, 256, 1024].into_iter().filter(|v| *v <= through) {
        // Never retain a 60-second session through the long write loop.
        let session = open_bound(g, byte_binding(), Some(version)).await?;
        assert_eq!(session.version, version);
        let first = read_bound(g, byte_binding(), &session, 7, "g.V(1)").await?;
        assert_eq!(
            first,
            if version == 0 {
                vec![]
            } else {
                vec![row(7, 1)]
            }
        );
        if version > 0 {
            assert_eq!(
                read_bound(g, byte_binding(), &session, 7, &format!("g.V({version})")).await?,
                vec![row(7, version)]
            );
        }
        assert!(read_bound(
            g,
            byte_binding(),
            &session,
            7,
            &format!("g.V({})", version + 1)
        )
        .await?
        .is_empty());
        assert!(read_bound(g, byte_binding(), &session, 8, "g.V(1)")
            .await?
            .is_empty());
        call_bound(g, byte_binding(), Operation::Close { session }).await?;
    }
    Ok(())
}

async fn verify_retained_lifecycle(
    g: &Gateway,
    coordinator: &Client,
    members: [&Client; 2],
    checkpoint: &mut LifecycleCheckpoint,
) -> anyhow::Result<()> {
    let _span = checkpoint.metrics.span("recovery.complete_verification");
    let c = NodeRef::Remote(coordinator);
    let summary = c.summary(&authority(), 1).await?;
    assert_eq!(summary.resolved_prefix, EXTENDED_WRITES);
    assert_eq!(summary.cohort, byte_binding().cohort);
    assert_eq!(
        c.confirm_capacity(&authority(), 1).await?,
        checkpoint.grants
    );
    let proof = c.terminal(&authority(), 1, EXTENDED_WRITES).await?;
    assert_complete_commits(&proof, EXTENDED_WRITES)?;
    for (client, shard, sequence) in [(members[0], 7, EXTENDED_WRITES), (members[1], 8, 0)] {
        assert_eq!(
            NodeRef::Remote(client)
                .visibility(&target(shard))
                .await?
                .applied_prefix,
            EXTENDED_WRITES
        );
        let snapshot = NodeRef::Remote(client)
            .read_terminal(
                &target(shard),
                ReadLimit {
                    // Match the existing production gateway capture contract, not the
                    // legacy 1000-row test fixture. No production limit is changed.
                    rows: 10000,
                    bytes: 2 * 1024 * 1024,
                    operations: 100000,
                },
                &proof,
            )
            .await?;
        assert_eq!(snapshot.applied_prefix, EXTENDED_WRITES);
        assert_eq!(snapshot.local_sequence, sequence);
        let grant = NodeRef::Remote(client)
            .grant_status(&authority(), &target(shard), &byte_binding().cohort)
            .await?;
        assert_eq!(
            Some(&grant),
            checkpoint.grants.iter().find(|g| g.member.shard == shard)
        );
    }
    // A different, freshly prepared candidate must not replace the original
    // Plan for an old external ID with identical canonical request bytes.
    let graph = GraphCohort::from_endpoints(
        c,
        authority(),
        1,
        vec![
            (NodeRef::Remote(members[0]), target(7)),
            (NodeRef::Remote(members[1]), target(8)),
        ],
    );
    let snapshot = graph
        .snapshot(
            Some(EXTENDED_WRITES),
            ReadLimit {
                rows: 10000,
                bytes: 2 * 1024 * 1024,
                operations: 100000,
            },
            Duration::from_secs(60),
        )
        .await?;
    let candidate =
        snapshot.prepare_vertex(7, &rocksdb_sql_table::Vertex::new(EXTENDED_WRITES + 1))?;
    assert_ne!(candidate.id(), checkpoint.originals[&1].transaction_id);
    let recovered = graph
        .admit_request(
            extended_write(1).external_id,
            canonical_write(&extended_write(1))?,
            &candidate,
        )
        .await?;
    assert_eq!(recovered.id(), checkpoint.originals[&1].transaction_id);
    assert_eq!(recovered.validation_version(), 0);
    drop(snapshot);
    for ordinal in [1, 256, 1024, 1025] {
        retain_original(coordinator, ordinal, checkpoint).await?;
        let original = &checkpoint.originals[&ordinal];
        assert_eq!(
            committed(
                call_bound(
                    g,
                    byte_binding(),
                    Operation::Submit(extended_write(ordinal))
                )
                .await?
            ),
            (original.transaction_id, ordinal)
        );
        let mut changed = extended_write(ordinal);
        if let Mutation::CreateVertex { vertex, .. } = &mut changed.mutation {
            vertex.labels = vec!["changed".into()];
        }
        assert_eq!(
            call_bound(g, byte_binding(), Operation::Submit(changed)).await,
            Err(gateway::Error::Identity)
        );
        retain_original(coordinator, ordinal, checkpoint).await?;
    }
    for version in [0, 1, 256, 1024] {
        retain_proof(coordinator, version, checkpoint).await?;
    }
    assert_eq!(
        c.summary(&authority(), 1).await?.resolved_prefix,
        EXTENDED_WRITES
    );
    // Exact whole-journal cardinality is checked once per recovery phase. The
    // long loop itself uses bounded point/status/proof operations, not archives.
    assert_eq!(
        c.journal(&authority(), 1).await?.transaction_count(),
        EXTENDED_WRITES as usize
    );
    verify_old_versions(g, EXTENDED_WRITES).await?;
    Ok(())
}

/// Forward the actual gateway request verbatim, receive its entire bounded
/// response, then close without forwarding a byte of the ACK. The caller must
/// observe Unknown; this is transport ACK loss, not a fabricated status.
async fn discard_gateway_ack(g: &Gateway, write: Write) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let upstream = g.local_addr();
    let proxy = tokio::spawn(async move {
        tokio::time::timeout(BUDGET, async move {
            let (mut caller, _) = listener.accept().await?;
            let length = caller.read_u32().await?;
            anyhow::ensure!(
                length as usize <= gateway::MAX_REQUEST_BYTES,
                "proxy request frame"
            );
            let mut bytes = vec![0; length as usize];
            caller.read_exact(&mut bytes).await?;
            let mut server = tokio::net::TcpStream::connect(upstream).await?;
            server.write_u32(length).await?;
            server.write_all(&bytes).await?;
            let length = server.read_u32().await?;
            anyhow::ensure!(
                length as usize <= gateway::MAX_RESPONSE_BYTES,
                "proxy response frame"
            );
            let mut discarded = vec![0; length as usize];
            server.read_exact(&mut discarded).await?;
            // Validate a full JSON response was lost, without telling the client its
            // outcome. The durable point lookup below is the recovery authority.
            let _: serde_json::Value = serde_json::from_slice(&discarded)?;
            drop(caller);
            Ok::<_, anyhow::Error>(())
        })
        .await?
    });
    let result = gateway::request(address, byte_binding(), Operation::Submit(write), BUDGET).await;
    tokio::time::timeout(BUDGET, proxy).await???;
    assert_eq!(result, Err(gateway::Error::Unknown));
    Ok(())
}

// Ordinary independent-process coverage, not a reduced 1025-write acceptance run.
const SMALL_LIFECYCLE_WRITES: u64 = 4;

#[derive(Debug, PartialEq)]
struct SmallVersionEvidence {
    proof: Vec<u8>,
    rows: Vec<Vec<QueryRow>>,
}

async fn small_version_evidence(
    g: &Gateway,
    coordinator: &Client,
    version: u64,
) -> anyhow::Result<SmallVersionEvidence> {
    let proof = NodeRef::Remote(coordinator)
        .terminal(&authority(), 1, version)
        .await?;
    assert_complete_commits(&proof, version)?;
    let session = open_bound(g, byte_binding(), Some(version)).await?;
    assert_eq!(session.version, version);
    let mut rows = Vec::new();
    // Explicit standard Gremlin reads cover every written ID and the unused ID
    // of the fresh candidate below, including absent later data at V0 and V1.
    for partition in [7, 8] {
        for id in 1..=SMALL_LIFECYCLE_WRITES + 1 {
            let found = read_bound(
                g,
                byte_binding(),
                &session,
                partition,
                &format!("g.V({id})"),
            )
            .await?;
            assert_eq!(
                found,
                if partition == 7 && id <= version {
                    vec![row(7, id)]
                } else {
                    vec![]
                }
            );
            rows.push(found);
        }
    }
    call_bound(g, byte_binding(), Operation::Close { session }).await?;
    Ok(SmallVersionEvidence {
        proof: serde_json::to_vec(&proof)?,
        rows,
    })
}

async fn verify_small_lifecycle(
    g: &Gateway,
    coordinator: &Client,
    members: [&Client; 2],
    grants: &[MemberCapacityGrant],
    originals: &BTreeMap<u64, (Write, RequestRecord)>,
    evidence: &BTreeMap<u64, SmallVersionEvidence>,
) -> anyhow::Result<()> {
    let c = NodeRef::Remote(coordinator);
    let summary = c.summary(&authority(), 1).await?;
    assert_eq!(summary.cohort, byte_binding().cohort);
    assert_eq!(summary.resolved_prefix, SMALL_LIFECYCLE_WRITES);
    assert_eq!(c.confirm_capacity(&authority(), 1).await?, grants);
    assert_eq!(originals.keys().copied().collect::<Vec<_>>(), [1, 4]);
    assert_eq!(evidence.keys().copied().collect::<Vec<_>>(), [0, 1, 4]);
    let proof = c.terminal(&authority(), 1, SMALL_LIFECYCLE_WRITES).await?;
    assert_complete_commits(&proof, SMALL_LIFECYCLE_WRITES)?;
    for (client, shard, sequence) in [(members[0], 7, SMALL_LIFECYCLE_WRITES), (members[1], 8, 0)] {
        let member = NodeRef::Remote(client);
        assert_eq!(
            member.visibility(&target(shard)).await?.applied_prefix,
            SMALL_LIFECYCLE_WRITES
        );
        let snapshot = member
            .read_terminal(&target(shard), limit(), &proof)
            .await?;
        assert_eq!(snapshot.applied_prefix, SMALL_LIFECYCLE_WRITES);
        assert_eq!(snapshot.local_sequence, sequence);
        if shard == 8 {
            assert!(
                snapshot.entries.is_empty(),
                "nonparticipant wrote local data"
            );
        }
        assert_eq!(
            member
                .grant_status(&authority(), &target(shard), &byte_binding().cohort)
                .await?,
            *grants
                .iter()
                .find(|grant| grant.member.shard == shard)
                .unwrap()
        );
    }
    let graph = GraphCohort::from_endpoints(
        c,
        authority(),
        1,
        vec![
            (NodeRef::Remote(members[0]), target(7)),
            (NodeRef::Remote(members[1]), target(8)),
        ],
    );
    let snapshot = graph
        .snapshot(
            Some(SMALL_LIFECYCLE_WRITES),
            limit(),
            Duration::from_secs(60),
        )
        .await?;
    for (ordinal, (write, original)) in originals {
        assert_eq!(original_request(coordinator, *ordinal).await?, *original);
        let first_plan = graph
            .recover_request(original.id, &original.request)
            .await?
            .context("saved original Plan missing")?;
        assert_eq!(first_plan.id(), original.transaction_id);
        assert_eq!(first_plan.validation_version(), ordinal - 1);
        let candidate = snapshot.prepare_vertex(
            7,
            &rocksdb_sql_table::Vertex::new(SMALL_LIFECYCLE_WRITES + 1),
        )?;
        assert_ne!(candidate, first_plan);
        assert_ne!(candidate.id(), original.transaction_id);
        assert_eq!(
            graph
                .admit_request(original.id, original.request.clone(), &candidate)
                .await?,
            first_plan,
            "fresh candidate replaced the complete first Plan"
        );
        assert_eq!(
            committed(call_bound(g, byte_binding(), Operation::Submit(write.clone())).await?),
            (original.transaction_id, *ordinal)
        );
        let mut changed = write.clone();
        if let Mutation::CreateVertex { vertex, .. } = &mut changed.mutation {
            vertex.labels = vec!["changed".into()];
        }
        assert_eq!(
            call_bound(g, byte_binding(), Operation::Submit(changed)).await,
            Err(gateway::Error::Identity)
        );
        assert_eq!(original_request(coordinator, *ordinal).await?, *original);
    }
    drop(snapshot);
    // Compare immutable pre-crash observations, never replace an expectation
    // with data recovered after a restart or snapshot installation.
    for (version, original) in evidence {
        assert_eq!(
            small_version_evidence(g, coordinator, *version).await?,
            *original
        );
    }
    let current = open_bound(g, byte_binding(), None).await?;
    assert_eq!(current.version, SMALL_LIFECYCLE_WRITES);
    call_bound(g, byte_binding(), Operation::Close { session: current }).await?;
    assert_eq!(
        c.journal(&authority(), 1).await?.transaction_count(),
        SMALL_LIFECYCLE_WRITES as usize,
        "same-ID retries or fresh candidates added a second admission"
    );
    assert_eq!(
        c.summary(&authority(), 1).await?.resolved_prefix,
        SMALL_LIFECYCLE_WRITES
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn byte_bound_independent_processes_small_lifecycle_recovery() -> anyhow::Result<()> {
    // Keep one ordinary test and the original total scenario budget. A second
    // concurrent fixture would add pressure to process-wide RPC admission.
    tokio::time::timeout(Duration::from_secs(240), async {
        small_lifecycle_recovery(false).await?;
        small_lifecycle_recovery(true).await
    })
    .await?
}

async fn small_lifecycle_recovery(enabled: bool) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(240), async {
        let config = ProcessConfig {
            cohort: byte_binding().cohort,
            members: vec![],
        };
        let mut a = Process::configured(7, FixtureDirectory::temporary()?, config.clone()).await?;
        let mut b = Process::configured(8, FixtureDirectory::temporary()?, config).await?;
        let mut coordinator = Process::configured(
            9,
            FixtureDirectory::temporary()?,
            ProcessConfig {
                cohort: byte_binding().cohort,
                members: vec![(7, a.address), (8, b.address)],
            },
        )
        .await?;
        assert_eq!(
            BTreeSet::from([
                std::process::id(),
                coordinator.child.id(),
                a.child.id(),
                b.child.id(),
            ])
            .len(),
            4
        );
        let c_client = coordinator.client()?;
        let a_client = a.client()?;
        let b_client = b.client()?;
        let grants = NodeRef::Remote(&c_client)
            .confirm_capacity(&authority(), 1)
            .await?;
        assert_eq!(
            grants
                .iter()
                .map(|grant| grant.member.clone())
                .collect::<Vec<_>>(),
            byte_binding().cohort.members
        );
        for grant in &grants {
            grant
                .activation
                .validate()
                .map_err(|e| anyhow!("invalid initial grant: {e:?}"))?;
        }
        let (g, mut attribution) = lifecycle_gateway(
            RemoteConfig {
                coordinator: c_client.clone(),
                binding: byte_binding(),
                members: vec![a_client.clone(), b_client.clone()],
            },
            enabled,
            "unrecorded-small-functional-test",
            "unrecorded-small-functional-test",
        )
        .await?;
        assert_eq!(
            BTreeSet::from([g.local_addr(), coordinator.address, a.address, b.address]).len(),
            4
        );
        attribution.emit(
            "small_setup",
            0,
            false,
            &lifecycle_processes(&coordinator, &a, &b),
        );
        let mut originals = BTreeMap::new();
        let mut evidence = BTreeMap::from([(0, small_version_evidence(&g, &c_client, 0).await?)]);
        for ordinal in 1..=SMALL_LIFECYCLE_WRITES {
            let write = extended_write(ordinal);
            if ordinal == SMALL_LIFECYCLE_WRITES {
                // The real final gateway response is discarded, so the caller
                // sees Unknown. Capture its admitted original before crashing.
                discard_gateway_ack(&g, write.clone()).await?;
            } else {
                let (id, committed_ordinal) = committed(
                    call_bound(&g, byte_binding(), Operation::Submit(write.clone())).await?,
                );
                assert_eq!(committed_ordinal, ordinal);
                assert_eq!(
                    original_request(&c_client, ordinal).await?.transaction_id,
                    id
                );
            }
            attribution.drain();
            if [1, SMALL_LIFECYCLE_WRITES].contains(&ordinal) {
                originals.insert(
                    ordinal,
                    (write, original_request(&c_client, ordinal).await?),
                );
                evidence.insert(
                    ordinal,
                    small_version_evidence(&g, &c_client, ordinal).await?,
                );
            }
        }
        attribution.emit(
            "small_before_ack_reopen",
            SMALL_LIFECYCLE_WRITES,
            false,
            &lifecycle_processes(&coordinator, &a, &b),
        );
        b.stop()?;
        coordinator.stop()?;
        coordinator.reopen().await?;
        b.reopen().await?;
        // Retry the unknown last request first, with its exact saved ID/bytes.
        let (last_write, last_record) = &originals[&SMALL_LIFECYCLE_WRITES];
        assert_eq!(
            original_request(&c_client, SMALL_LIFECYCLE_WRITES).await?,
            *last_record
        );
        assert_eq!(
            committed(call_bound(&g, byte_binding(), Operation::Submit(last_write.clone())).await?),
            (last_record.transaction_id, SMALL_LIFECYCLE_WRITES)
        );
        verify_small_lifecycle(
            &g,
            &c_client,
            [&a_client, &b_client],
            &grants,
            &originals,
            &evidence,
        )
        .await?;
        a.stop()?;
        a.reopen().await?;
        verify_small_lifecycle(
            &g,
            &c_client,
            [&a_client, &b_client],
            &grants,
            &originals,
            &evidence,
        )
        .await?;
        // Each role exports complete state, closes, then installs into a fresh
        // state.db. The original DB remains beside it as fixture evidence.
        for process in [&mut a, &mut b, &mut coordinator] {
            assert!(!process.directory.path().join("snapshot.restored").exists());
            process.snapshot_restore().await?;
            assert!(process.directory.path().join("snapshot.restored").exists());
            assert!(process
                .directory
                .path()
                .join("data/state.before-snapshot.db")
                .exists());
        }
        verify_small_lifecycle(
            &g,
            &c_client,
            [&a_client, &b_client],
            &grants,
            &originals,
            &evidence,
        )
        .await?;
        g.shutdown().await;
        // Both fixtures run exactly the same recovery/data assertions. The
        // additional fixture proves that timing reports traverse the real path.
        if enabled {
            // Four original writes, one unknown-ACK retry, then three
            // verification passes of two originals plus two changed requests.
            assert_eq!(attribution.received(), 17);
        } else {
            assert_eq!(attribution.received(), 0);
        }
        attribution.emit(
            "small_complete",
            SMALL_LIFECYCLE_WRITES,
            true,
            &lifecycle_processes(&coordinator, &a, &b),
        );
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(
    debug_assertions,
    ignore = "long release functional test; mandatory in CoreHR release CI"
)]
async fn byte_bound_independent_processes_1025_writes_retain_original_lifecycle(
) -> anyhow::Result<()> {
    let timeout = std::env::var("BYTE_BOUND_PROCESS_TIMEOUT_SECS")
        .unwrap_or_else(|_| "21600".into())
        .parse::<u64>()?;
    anyhow::ensure!(
        (1..=86400).contains(&timeout),
        "invalid test harness timeout"
    );
    tokio::time::timeout(Duration::from_secs(timeout), extended_lifecycle(timeout))
        .await
        .context(
            "INCOMPLETE: lifecycle harness timeout; retain checkpoint and retry the original IDs",
        )?
}

async fn extended_lifecycle(timeout: u64) -> anyhow::Result<()> {
    let retained = std::env::var_os(RETAINED_ENV);
    let root = match retained {
        Some(path) => FixtureDirectory::retained(PathBuf::from(path))?,
        None => FixtureDirectory::temporary()?,
    };
    let is_retained = root._temporary.is_none();
    let mut checkpoint = LifecycleCheckpoint::load(root.path(), is_retained, timeout)?;
    checkpoint.save(root.path(), "setup")?;
    let member_config = ProcessConfig {
        cohort: byte_binding().cohort,
        members: vec![],
    };
    let mut a = Process::configured(
        7,
        FixtureDirectory::retained(root.path().join("shard-7"))?,
        member_config.clone(),
    )
    .await?;
    let mut b = Process::configured(
        8,
        FixtureDirectory::retained(root.path().join("shard-8"))?,
        member_config,
    )
    .await?;
    let mut coordinator = Process::configured(
        9,
        FixtureDirectory::retained(root.path().join("shard-9"))?,
        ProcessConfig {
            cohort: byte_binding().cohort,
            members: vec![(7, a.address), (8, b.address)],
        },
    )
    .await?;
    assert_eq!(
        BTreeSet::from([
            std::process::id(),
            a.child.id(),
            b.child.id(),
            coordinator.child.id()
        ])
        .len(),
        4
    );
    let c_client = coordinator.client()?;
    let a_client = a.client()?;
    let b_client = b.client()?;
    let c = NodeRef::Remote(&c_client);
    let grants = c.confirm_capacity(&authority(), 1).await?;
    if checkpoint.grants.is_empty() {
        checkpoint.grants = grants;
    } else {
        assert_eq!(
            checkpoint.grants, grants,
            "resume must preserve first member generations"
        );
    }
    assert_eq!(checkpoint.grants.len(), 2);
    let attribution_enabled =
        std::env::var("BYTE_BOUND_PROCESS_ATTRIBUTION").is_ok_and(|v| v == "1");
    let (g, mut attribution) = lifecycle_gateway(
        RemoteConfig {
            coordinator: c_client.clone(),
            binding: byte_binding(),
            members: vec![a_client.clone(), b_client.clone()],
        },
        attribution_enabled,
        &checkpoint.source_sha256,
        &checkpoint.elf_sha256,
    )
    .await?;
    retain_proof(&c_client, 0, &mut checkpoint).await?;
    checkpoint.save(root.path(), "setup")?;
    eprintln!("byte-bound lifecycle INCOMPLETE: directory={} verified={} source_sha256={} elf_sha256={} timeout={}s coordinator={} writer={} nonparticipant={}",
        root.path().display(), checkpoint.verified_ordinal, checkpoint.source_sha256, checkpoint.elf_sha256,
        timeout, coordinator.child.id(), a.child.id(), b.child.id());
    let stop_after = std::env::var("BYTE_BOUND_PROCESS_STOP_AFTER")
        .ok()
        .map(|v| v.parse::<u64>())
        .transpose()?;
    attribution.emit(
        "setup",
        checkpoint.verified_ordinal,
        false,
        &lifecycle_processes(&coordinator, &a, &b),
    );
    let started = std::time::Instant::now();
    for ordinal in checkpoint.verified_ordinal + 1..=EXTENDED_WRITES {
        let write = extended_write(ordinal);
        checkpoint.pending_external_id = Some(write.external_id);
        checkpoint.save(root.path(), "pending")?;
        if ordinal == EXTENDED_WRITES && !checkpoint.ack_loss_recovered {
            let _recovery_span = checkpoint.metrics.span("recovery.ack_loss");
            {
                let _span = checkpoint.metrics.span("gateway.discarded_ack_submit");
                discard_gateway_ack(&g, write.clone()).await?;
            }
            retain_original(&c_client, ordinal, &mut checkpoint).await?;
            checkpoint.save(root.path(), "ack_loss")?;
            attribution.emit(
                "before_ack_reopen",
                checkpoint.verified_ordinal,
                false,
                &lifecycle_processes(&coordinator, &a, &b),
            );
            b.stop()?;
            coordinator.stop()?;
            {
                let _span = checkpoint.metrics.span("recovery.reopen_coordinator");
                coordinator.reopen().await?;
            }
            {
                let _span = checkpoint.metrics.span("recovery.reopen_nonparticipant");
                b.reopen().await?;
            }
            attribution.emit(
                "after_ack_reopen",
                checkpoint.verified_ordinal,
                false,
                &lifecycle_processes(&coordinator, &a, &b),
            );
            retain_original(&c_client, ordinal, &mut checkpoint).await?;
        }
        let reply = {
            let _span = checkpoint.metrics.span("gateway.submit");
            call_bound(&g, byte_binding(), Operation::Submit(write)).await?
        };
        attribution.drain();
        let (transaction_id, committed_ordinal) = committed(reply);
        assert_eq!(committed_ordinal, ordinal);
        if [1, 256, 1024, 1025].contains(&ordinal) {
            retain_original(&c_client, ordinal, &mut checkpoint).await?;
            assert_eq!(
                checkpoint.originals[&ordinal].transaction_id,
                transaction_id
            );
            retain_proof(&c_client, ordinal, &mut checkpoint).await?;
            let _span = checkpoint.metrics.span("milestone.old_version_reads");
            verify_old_versions(&g, ordinal).await?;
        }
        if ordinal == EXTENDED_WRITES {
            checkpoint.ack_loss_recovered = true;
        }
        checkpoint.verified_ordinal = ordinal;
        checkpoint.pending_external_id = None;
        checkpoint.save(root.path(), "verified")?;
        if ordinal == 1 || ordinal % 16 == 0 || ordinal == EXTENDED_WRITES {
            attribution.emit(
                "writes",
                ordinal,
                false,
                &lifecycle_processes(&coordinator, &a, &b),
            );
            checkpoint.metrics.emit(
                &checkpoint.source_sha256,
                &checkpoint.elf_sha256,
                ordinal,
                false,
            );
            eprintln!("byte-bound lifecycle INCOMPLETE: verified={ordinal}/{EXTENDED_WRITES} external_id={:?} transaction_id={transaction_id:?} elapsed_this_attempt={:?} checkpoint={}",
                extended_write(ordinal).external_id, started.elapsed(), root.path().join("progress.json").display());
        }
        if stop_after.is_some_and(|cut| ordinal >= cut && ordinal < EXTENDED_WRITES) {
            anyhow::bail!("INCOMPLETE: intentional functional prefix stop at {ordinal}; resume this exact checkpoint and original IDs; this is not a 1025-write acceptance pass");
        }
    }
    verify_retained_lifecycle(&g, &c_client, [&a_client, &b_client], &mut checkpoint).await?;
    // Reopen the actual single writer too, then prove the old IDs and versions.
    attribution.emit(
        "before_writer_reopen",
        checkpoint.verified_ordinal,
        false,
        &lifecycle_processes(&coordinator, &a, &b),
    );
    a.stop()?;
    {
        let _span = checkpoint.metrics.span("recovery.reopen_writer");
        a.reopen().await?;
    }
    verify_retained_lifecycle(&g, &c_client, [&a_client, &b_client], &mut checkpoint).await?;
    attribution.emit(
        "after_writer_reopen",
        checkpoint.verified_ordinal,
        false,
        &lifecycle_processes(&coordinator, &a, &b),
    );
    if !checkpoint.snapshots_restored {
        // There are no concurrent gateway writes during these full snapshot cuts.
        // Each child closes RPC, shuts down Raft after a barrier, exports complete
        // meta/data, and installs it into a fresh state.db on the next launch.
        attribution.emit(
            "before_snapshot_restores",
            checkpoint.verified_ordinal,
            false,
            &lifecycle_processes(&coordinator, &a, &b),
        );
        for process in [&mut a, &mut b, &mut coordinator] {
            let _span = checkpoint.metrics.span("recovery.snapshot_export_install");
            process.snapshot_restore().await?;
        }
        attribution.emit(
            "after_snapshot_restores",
            checkpoint.verified_ordinal,
            false,
            &lifecycle_processes(&coordinator, &a, &b),
        );
        checkpoint.snapshots_restored = true;
        checkpoint.save(root.path(), "snapshot_flag")?;
    }
    verify_retained_lifecycle(&g, &c_client, [&a_client, &b_client], &mut checkpoint).await?;
    assert!(checkpoint.ack_loss_recovered && checkpoint.snapshots_restored);
    g.shutdown().await;
    checkpoint.completed = true;
    checkpoint.save(root.path(), "complete")?;
    attribution.emit(
        "complete",
        checkpoint.verified_ordinal,
        true,
        &lifecycle_processes(&coordinator, &a, &b),
    );
    eprintln!("byte-bound lifecycle COMPLETE: 1025 gateway writes; exact original IDs/Plans; complete old-V proofs; crash/reopen; three full snapshot restores; source_sha256={} elf_sha256={} checkpoint={}",
        checkpoint.source_sha256, checkpoint.elf_sha256, root.path().join("progress.json").display());
    Ok(())
}

#[path = "support/range_queries.rs"]
mod range_queries;

#[path = "support/managed_process_edge_replace.rs"]
mod managed_process_edge_replace;
async fn data_local_fixed_query_across_managed_processes(
    coordinator: &Process,
    a: &mut Process,
    b: &mut Process,
) -> anyhow::Result<()> {
    async fn route(p: &Process) -> anyhow::Result<gateway::LocalGraphRoute> {
        fs::write(p.directory.path().join("graph.request"), b"start")?;
        let endpoint = tokio::time::timeout(BUDGET, async {
            loop {
                if let Ok(bytes) = fs::read(p.directory.path().join("graph-ready.json")) {
                    let (pid, endpoint): (u32, SocketAddr) = serde_json::from_slice(&bytes)?;
                    if pid == p.child.id() {
                        return Ok::<_, anyhow::Error>(endpoint);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        Ok(gateway::LocalGraphRoute {
            endpoint,
            identity: replicated_graph::remote::RemoteIdentity {
                group: p.shard,
                node: 1,
            },
            managed_identity: identity(p.shard),
        })
    }
    let g = Gateway::bind_remote_local_queries(
        RemoteConfig {
            coordinator: coordinator.client()?,
            binding: binding(),
            members: vec![a.client()?, b.client()?],
        },
        vec![route(a).await?, route(b).await?],
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    const ROOT: u64 = 1_000_001;
    const EDGE: u64 = 1_000_010;
    let write = |sequence: u8, mutation| {
        let mut external_id = [0; 16];
        external_id[..3].copy_from_slice(&[b'L', b'F', sequence]);
        Operation::Submit(Write {
            external_id,
            mutation,
        })
    };
    for (sequence, partition) in [(1, 7), (2, 8)] {
        committed(
            call(
                &g,
                write(
                    sequence,
                    Mutation::CreateVertex {
                        partition,
                        vertex: VertexData {
                            id: ROOT,
                            labels: vec!["person".into()],
                            properties: BTreeMap::new(),
                        },
                    },
                ),
            )
            .await?,
        );
    }
    let edge = EdgeData {
        id: EDGE,
        from: Entity {
            partition: 7,
            id: ROOT,
        },
        to: Entity {
            partition: 8,
            id: ROOT,
        },
        label: "knows".into(),
        properties: BTreeMap::new(),
    };
    let (_, fixed_version) = committed(call(&g, write(3, Mutation::CreateEdge { edge })).await?);
    let query = Query {
        partition: 7,
        text: format!("g.V({ROOT},{ROOT}).out().path()"),
    };
    let Reply::OpenLocalQuery { session, page, .. } = call(
        &g,
        Operation::OpenLocalQuery {
            version: Some(fixed_version),
            ttl_millis: 60_000,
            query: query.clone(),
            page_size: 1,
        },
    )
    .await?
    else {
        anyhow::bail!("expected Ready");
    };
    let expected = QueryRow::Path(vec![
        QualifiedElement::Vertex {
            partition: 7,
            id: ROOT,
        },
        QualifiedElement::Edge {
            partition: 7,
            id: EDGE,
        },
        QualifiedElement::Vertex {
            partition: 8,
            id: ROOT,
        },
    ]);
    assert_eq!(session.version, fixed_version);
    assert_eq!(page.total, 2);
    assert_eq!(page.data.rows, vec![expected.clone()]);
    let old = open(&g, Some(fixed_version)).await?;
    let Reply::Traverse { data, .. } = call(
        &g,
        Operation::Traverse {
            session: old.clone(),
            query: query.clone(),
        },
    )
    .await?
    else {
        anyhow::bail!("expected Traverse");
    };
    assert_eq!(data.rows, vec![expected.clone(), expected.clone()]);
    call(&g, Operation::Close { session: old }).await?;
    // Replace the source owner while retaining the numerical edge ID. The old
    // Ready result must keep the original qualified edge and target at V.
    let (_, replaced_version) = committed(
        call(
            &g,
            write(
                4,
                Mutation::ReplaceEdge {
                    edge: Entity {
                        partition: 7,
                        id: EDGE,
                    },
                    replacement: EdgeData {
                        id: EDGE,
                        from: Entity {
                            partition: 8,
                            id: ROOT,
                        },
                        to: Entity {
                            partition: 7,
                            id: ROOT,
                        },
                        label: "updated".into(),
                        properties: BTreeMap::new(),
                    },
                },
            ),
        )
        .await?,
    );
    let replaced_query = Query {
        partition: 8,
        text: format!("g.V({ROOT},{ROOT}).out('updated').path()"),
    };
    let Reply::OpenLocalQuery {
        session: replaced,
        page: replaced_page,
        ..
    } = call(
        &g,
        Operation::OpenLocalQuery {
            version: Some(replaced_version),
            ttl_millis: 60_000,
            query: replaced_query.clone(),
            page_size: 1000,
        },
    )
    .await?
    else {
        anyhow::bail!("expected replaced Ready");
    };
    let replaced_path = QueryRow::Path(vec![
        QualifiedElement::Vertex {
            partition: 8,
            id: ROOT,
        },
        QualifiedElement::Edge {
            partition: 8,
            id: EDGE,
        },
        QualifiedElement::Vertex {
            partition: 7,
            id: ROOT,
        },
    ]);
    assert_eq!(
        replaced_page.data.rows,
        vec![replaced_path.clone(), replaced_path]
    );
    let materialized = open(&g, Some(replaced_version)).await?;
    let Reply::Traverse { data, .. } = call(
        &g,
        Operation::Traverse {
            session: materialized.clone(),
            query: replaced_query,
        },
    )
    .await?
    else {
        anyhow::bail!("expected replaced Traverse");
    };
    assert_eq!(data, replaced_page.data);
    call(
        &g,
        Operation::Close {
            session: materialized,
        },
    )
    .await?;
    call(&g, Operation::CloseLocalQuery { session: replaced }).await?;
    let fetch = Operation::FetchLocalQuery {
        session: session.clone(),
        query: query.clone(),
        cursor: page.next.clone(),
        page_size: 1,
    };
    let before = call(&g, fetch.clone()).await?;
    a.stop()?;
    b.stop()?;
    assert_eq!(call(&g, fetch.clone()).await?, before);
    assert_eq!(call(&g, fetch).await?, before);
    assert!(call(
        &g,
        Operation::OpenLocalQuery {
            version: Some(fixed_version),
            ttl_millis: 60_000,
            query: Query {
                partition: 7,
                text: "g.V(999).count()".into()
            },
            page_size: 1
        }
    )
    .await
    .is_err());
    // Known result replay does not acquire descriptors after process restart.
    a.reopen().await?;
    b.reopen().await?;
    assert_eq!(
        call(
            &g,
            Operation::FetchLocalQuery {
                session: session.clone(),
                query: query.clone(),
                cursor: None,
                page_size: 1
            }
        )
        .await?,
        Reply::FetchLocalQuery {
            session: session.clone(),
            query: query.clone(),
            cursor: None,
            page_size: 1,
            page
        }
    );
    call(&g, Operation::CloseLocalQuery { session }).await?;
    g.shutdown().await;
    Ok(())
}
