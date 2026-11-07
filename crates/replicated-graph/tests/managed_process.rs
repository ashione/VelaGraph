//! Independent-process managed transport acceptance, not multi-host HA evidence.
//! Every parent data path crosses TCP. Only each child owns its own Raft handle.
use anyhow::{anyhow, Context};
use d2_raft::{
    managed_rpc::{Client, Identity, NodeRef, Role, Server},
    participant_writes::Authority,
    transaction_decisions::{Cohort, Command as DecisionCommand, Decision, Journal, Member},
    versioned_reads::{ReadLimit, Target},
    Cluster, D1Command, LogCommand, NodeHandle,
};
use replicated_graph::{
    managed_gateway::{
        self as gateway, Binding, EdgeData, Entity, Gateway, Mapping, Mutation, Operation,
        PageCursor, Patch, Query, QueryPage, RemoteConfig, Reply, Session, VertexData, Write,
        WriteStatus,
    },
    managed_graph::GraphCohort,
    remote::query::{QualifiedElement, QueryCompleteness, QueryRow},
};
use rocksdb_sql_table::{
    experimental_graph::{CrossEdge, VertexRef},
    Value,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

const BUDGET: Duration = Duration::from_secs(15);
const CHILD_ENV: &str = "MANAGED_PROCESS_DIRECTORY";
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn cohort() -> Cohort {
    Cohort {
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
fn limit() -> ReadLimit {
    ReadLimit {
        rows: 1000,
        bytes: 1024 * 1024,
        operations: 10000,
    }
}
async fn local_command(node: &NodeHandle, command: D1Command) -> anyhow::Result<()> {
    node.raft
        .client_write(LogCommand::D1V1(command))
        .await?
        .data
        .d1
        .context("missing D1 result")?
        .map_err(|e| anyhow!("fixture activation: {e:?}"))?;
    Ok(())
}

/// Only the subprocess runs this ignored entrypoint; the ordinary suite cannot
/// accidentally construct local handles in the gateway/client process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "private child-process entrypoint"]
async fn managed_node_child() -> anyhow::Result<()> {
    let directory =
        std::path::PathBuf::from(std::env::var_os(CHILD_ENV).context("child directory")?);
    let shard: u64 = std::env::var("MANAGED_PROCESS_SHARD")?.parse()?;
    let address: SocketAddr = std::env::var("MANAGED_PROCESS_ADDRESS")?.parse()?;
    let fresh = std::env::var("MANAGED_PROCESS_FRESH")? == "1";
    let cluster = Cluster::new().in_group(shard);
    let node = Arc::new(cluster.start_durable(1, directory.join("data")).await?);
    if fresh {
        node.raft
            .initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
            .await?;
    }
    tokio::time::timeout(BUDGET, async {
        while node.raft.ensure_linearizable().await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    if fresh {
        local_command(&node, D1Command::Activate { shard, epoch: 1 }).await?;
        local_command(
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
            local_command(
                &node,
                D1Command::EnableCoordinator {
                    shard,
                    epoch: 1,
                    mode_id: target(shard).mode,
                },
            )
            .await?;
            local_command(
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
            local_command(
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
            local_command(
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
    }
    let server = Server::bind(node, identity(shard), address, BUDGET).await?;
    fs::write(
        directory.join("ready.tmp"),
        serde_json::to_vec(&(std::process::id(), server.local_addr()))?,
    )?;
    fs::rename(directory.join("ready.tmp"), directory.join("ready.json"))?;
    std::future::pending::<()>().await;
    Ok(())
}

struct Process {
    child: Child,
    directory: tempfile::TempDir,
    shard: u64,
    address: SocketAddr,
}
impl Process {
    async fn start(shard: u64) -> anyhow::Result<Self> {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = reservation.local_addr()?;
        let directory = tempfile::tempdir()?;
        drop(reservation);
        let child = Self::launch(directory.path(), shard, address, true)?;
        let mut process = Self {
            child,
            directory,
            shard,
            address,
        };
        process.ready().await?;
        Ok(process)
    }
    fn launch(
        directory: &std::path::Path,
        shard: u64,
        address: SocketAddr,
        fresh: bool,
    ) -> anyhow::Result<Child> {
        Ok(Command::new(std::env::current_exe()?)
            .args(["--ignored", "--exact", "managed_node_child", "--nocapture"])
            .env(CHILD_ENV, directory)
            .env("MANAGED_PROCESS_SHARD", shard.to_string())
            .env("MANAGED_PROCESS_ADDRESS", address.to_string())
            .env("MANAGED_PROCESS_FRESH", if fresh { "1" } else { "0" })
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?)
    }
    async fn ready(&mut self) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                anyhow::ensure!(
                    self.child.try_wait()?.is_none(),
                    "managed child exited before readiness"
                );
                if let Ok(bytes) = fs::read(self.directory.path().join("ready.json")) {
                    let (pid, address): (u32, SocketAddr) = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        pid == self.child.id() && address == self.address,
                        "stale or wrong readiness identity"
                    );
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        self.child.kill()?;
        self.child.wait()?;
        Ok(())
    }
    async fn reopen(&mut self) -> anyhow::Result<()> {
        fs::remove_file(self.directory.path().join("ready.json"))?;
        self.child = Self::launch(self.directory.path(), self.shard, self.address, false)?;
        self.ready().await
    }
    fn client(&self) -> anyhow::Result<Client> {
        Ok(Client::new(self.address, identity(self.shard), BUDGET)?)
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
async fn call(g: &Gateway, operation: Operation) -> gateway::Result<Reply> {
    gateway::request(g.local_addr(), binding(), operation, BUDGET).await
}
async fn open(g: &Gateway, version: Option<u64>) -> anyhow::Result<Session> {
    match call(
        g,
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
    match call(
        g,
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
    tokio::time::timeout(Duration::from_secs(240), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let mut coordinator = Process::start(9).await?;
    let a = Process::start(7).await?;
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
            coordinator: c_client,
            binding: binding(),
            members: vec![a_client, b_client],
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
    reopened.shutdown().await;
    Ok(())
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
    for write in fixture {
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
    let expectations = vec![
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
        "g.V(101).id()",
        "g.V(101).dedup()",
        "g.V(101).valueMap('name')",
        "g.V(101).repeat(out()).times(2)",
        "g.V(101).out().out().out().out()",
    ] {
        assert!(
            traverse(g, &old, 7, unsupported).await.is_err(),
            "accepted {unsupported}"
        );
    }
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
        let fresh = open(g, Some(version)).await?;
        assert_eq!(
            traverse(g, &fresh, 8, "g.V(103).values('name')").await?,
            vec![value(8, 103, "Cara")]
        );
        let name = if phase == 0 { "Bob" } else { "Robert" };
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
        call(g, Operation::Close { session: fresh }).await?;
    }
    writer.await??;
    assert_eq!(collected, paths);
    assert!(cursor.is_none());
    // Even after every mutation, starting and replaying the first page is exact.
    let replay = page(g, &old, 7, path_query, None, 1).await?;
    assert_eq!(replay.data.rows, collected[..1]);
    assert_eq!(replay.total, 4);
    Ok((old, next))
}
