use super::*;
use d2_raft::transaction_decisions::Member;
use d2_raft::{participant_driver, Cluster, D1Command, D1Result, LogCommand};
use rocksdb_sql_table::experimental_graph::Direction;
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
fn graph<'a>(c: &'a NodeHandle, a: &'a NodeHandle, b: &'a NodeHandle) -> GraphCohort<'a> {
    GraphCohort::new(c, authority(), 1, vec![(a, target(7)), (b, target(8))])
}
async fn snapshot(graph: &GraphCohort<'_>) -> GraphSnapshot {
    graph
        .snapshot(None, limit(), Duration::from_secs(60))
        .await
        .unwrap()
}
async fn vertex(graph: &GraphCohort<'_>, partition: u32, id: u64) {
    let plan = snapshot(graph)
        .await
        .prepare_vertex(partition, &Vertex::new(id))
        .unwrap();
    assert!(matches!(
        graph.execute(&plan).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
}
fn edge(from: u32, to: u32, id: u64) -> CrossEdge {
    CrossEdge {
        id,
        from: VertexRef {
            partition: from,
            id: 1,
        },
        to: VertexRef {
            partition: to,
            id: 1,
        },
        label: "reports_to".into(),
        properties: BTreeMap::new(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_cross_shard_commit_partial_apply_and_same_plan_retry() {
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    vertex(&graph, 8, 1).await;
    let old = snapshot(&graph).await;
    let e = edge(7, 8, 10);
    let plan = old.prepare_edge(&e).unwrap();
    assert_eq!(plan.participants(), 2);
    graph
        .command(Command::Begin {
            id: plan.id,
            participants: plan.parts.iter().map(|p| p.descriptor.clone()).collect(),
        })
        .await
        .unwrap();
    let part = &plan.parts[0];
    participant_driver::prepare(
        &c,
        &a,
        authority(),
        1,
        part.descriptor.clone(),
        part.request.clone(),
    )
    .await
    .unwrap();
    assert!(snapshot(&graph).await.edge(e.identity()).unwrap().is_none());
    // Resume the same plan after only the first participant became prepared.
    let (ordinal, decision) = graph.decide(&plan, false).await.unwrap();
    assert_eq!(decision, Decision::Commit);
    versioned_reads::advance(&c, &a, authority(), 1, target(7), ordinal)
        .await
        .unwrap();
    let common = snapshot(&graph).await;
    assert_eq!(common.version().unwrap(), old.version().unwrap());
    assert!(common.edge(e.identity()).unwrap().is_none());
    assert!(graph
        .snapshot(Some(ordinal), limit(), Duration::from_secs(20))
        .await
        .is_err());
    // An already committed retry must skip Prepare/legacy Resolve and finish
    // the missing member without creating a new transaction identity.
    assert_eq!(
        graph.execute(&plan).await.unwrap(),
        Outcome::Committed {
            ordinal,
            visible: true
        }
    );
    assert_eq!(
        graph.execute(&plan).await.unwrap(),
        Outcome::Committed {
            ordinal,
            visible: true
        }
    );
    let view = snapshot(&graph).await;
    assert_eq!(view.edge(e.identity()).unwrap(), Some(e.clone()));
    assert_eq!(
        view.observed_adjacency(e.from, Direction::Out).unwrap(),
        vec![e.clone()]
    );
    assert_eq!(
        view.observed_adjacency(e.to, Direction::In).unwrap(),
        vec![e.clone()]
    );
    assert!(old.edge(e.identity()).unwrap().is_none());
    let local = edge(7, 7, 11);
    let local_plan = view.prepare_edge(&local).unwrap();
    assert_eq!(local_plan.participants(), 1);
    let pinned_version = view.version().unwrap();
    let (write, ()) = tokio::join!(graph.execute(&local_plan), async {
        for _ in 0..5 {
            let concurrent = graph
                .snapshot(Some(pinned_version), limit(), Duration::from_secs(20))
                .await
                .unwrap();
            assert!(concurrent.edge(local.identity()).unwrap().is_none());
            assert_eq!(concurrent.edge(e.identity()).unwrap(), Some(e.clone()));
            tokio::task::yield_now().await;
        }
    });
    assert!(matches!(
        write.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    let next = snapshot(&graph).await;
    assert_eq!(next.edge(local.identity()).unwrap(), Some(local));
    // Prior ordinary write paths remain fenced after graph mode activation.
    assert!(submit(
        &a,
        D1Command::Write(D1WriteRequest {
            shard: 7,
            epoch: 1,
            request_id: [88; 16],
            expected_sequence: Some(3),
            operations: vec![d2_raft::WriteOp::put("bad", "bad")]
        })
    )
    .await
    .is_err());
    for node in [&a, &b, &c] {
        node.raft.shutdown().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_conflicts_abort_missing_endpoints_and_concurrent_retry_are_explicit() {
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    let before = snapshot(&graph).await;
    assert!(before.prepare_edge(&edge(7, 8, 10)).is_err());
    assert!(before.prepare_vertex(6, &Vertex::new(1)).is_err());
    let one = before.prepare_vertex(7, &Vertex::new(2)).unwrap();
    let stale = before.prepare_vertex(7, &Vertex::new(3)).unwrap();
    let (left, right) = tokio::join!(graph.execute(&one), graph.execute(&one));
    assert_eq!(left.unwrap(), right.unwrap());
    assert!(matches!(
        graph.execute(&stale).await,
        Err(Error::Application(D1Error::Conflict { .. }))
    ));
    assert!(matches!(
        graph.abort(&stale).await.unwrap(),
        Outcome::Aborted { visible: true, .. }
    ));
    assert!(snapshot(&graph)
        .await
        .vertex(VertexRef {
            partition: 7,
            id: 3
        })
        .unwrap()
        .is_none());
    // Explicit abort after Commit cannot rewrite the decision or data.
    assert!(matches!(
        graph.abort(&one).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    vertex(&graph, 8, 1).await;
    let view = snapshot(&graph).await;
    let e = edge(7, 8, 20);
    let first = view.prepare_edge(&e).unwrap();
    let second = view.prepare_edge(&e).unwrap();
    assert_ne!(first.id(), second.id());
    assert!(matches!(
        graph.execute(&first).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    assert!(graph.execute(&second).await.is_err());
    assert!(matches!(
        graph.abort(&second).await.unwrap(),
        Outcome::Aborted { .. }
    ));
    assert_eq!(
        snapshot(&graph)
            .await
            .observed_adjacency(e.from, Direction::Out)
            .unwrap()
            .len(),
        1
    );
    for node in [&a, &b, &c] {
        node.raft.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_decision_is_not_misreported_when_a_member_stops() {
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    vertex(&graph, 8, 1).await;
    let before = snapshot(&graph).await;
    let e = edge(7, 8, 30);
    let plan = before.prepare_edge(&e).unwrap();
    let (ordinal, decision) = graph.decide(&plan, false).await.unwrap();
    assert_eq!(decision, Decision::Commit);
    b.raft.shutdown().await.unwrap();
    assert_eq!(
        graph.execute(&plan).await.unwrap(),
        Outcome::Committed {
            ordinal,
            visible: false
        }
    );
    assert!(before.edge(e.identity()).unwrap().is_none());
    assert!(graph
        .snapshot(Some(ordinal), limit(), Duration::from_millis(50))
        .await
        .is_err());
    assert_eq!(
        graph.abort(&plan).await.unwrap(),
        Outcome::Committed {
            ordinal,
            visible: false
        }
    );
    c.raft.shutdown().await.unwrap();
    assert!(matches!(graph.execute(&plan).await, Err(Error::Uncertain)));
    a.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_does_not_require_unavailable_participants_or_nonparticipants() {
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    vertex(&graph, 8, 1).await;
    let view = snapshot(&graph).await;
    let edge_plan = view.prepare_edge(&edge(7, 8, 40)).unwrap();
    let vertex_plan = view.prepare_vertex(7, &Vertex::new(3)).unwrap();
    let independent_plan = view.prepare_vertex(7, &Vertex::new(4)).unwrap();
    graph
        .command(Command::Begin {
            id: edge_plan.id,
            participants: edge_plan
                .parts
                .iter()
                .map(|p| p.descriptor.clone())
                .collect(),
        })
        .await
        .unwrap();
    let part = &edge_plan.parts[0];
    participant_driver::prepare(
        &c,
        &a,
        authority(),
        1,
        part.descriptor.clone(),
        part.request.clone(),
    )
    .await
    .unwrap();
    b.raft.shutdown().await.unwrap();
    assert!(matches!(
        graph.abort(&edge_plan).await.unwrap(),
        Outcome::Aborted { visible: false, .. }
    ));
    assert_eq!(
        c.state_machine
            .coordinator_journal(9, 1, [9; 16])
            .await
            .unwrap()
            .transaction(edge_plan.id)
            .unwrap()
            .decision,
        Some(Decision::Abort)
    );
    assert_eq!(
        a.state_machine
            .participant_status(7, 1, [7; 16], edge_plan.id)
            .await
            .unwrap(),
        d2_raft::participant_writes::Status::Aborted
    );
    // The unavailable shard is not a participant in this second transaction.
    graph
        .command(Command::Begin {
            id: vertex_plan.id,
            participants: vertex_plan
                .parts
                .iter()
                .map(|p| p.descriptor.clone())
                .collect(),
        })
        .await
        .unwrap();
    let part = &vertex_plan.parts[0];
    participant_driver::prepare(
        &c,
        &a,
        authority(),
        1,
        part.descriptor.clone(),
        part.request.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        graph.abort(&vertex_plan).await.unwrap(),
        Outcome::Aborted { visible: false, .. }
    ));
    assert_eq!(
        a.state_machine
            .participant_status(7, 1, [7; 16], vertex_plan.id)
            .await
            .unwrap(),
        d2_raft::participant_writes::Status::Aborted
    );
    assert!(view
        .vertex(VertexRef {
            partition: 7,
            id: 3
        })
        .unwrap()
        .is_none());
    assert!(matches!(
        graph.execute(&independent_plan).await.unwrap(),
        Outcome::Committed { visible: false, .. }
    ));
    for node in [&a, &c] {
        node.raft.shutdown().await.unwrap();
    }
}

fn edge_patch(value: i64) -> rocksdb_sql_table::experimental_graph::EdgePropertyPatch {
    rocksdb_sql_table::experimental_graph::EdgePropertyPatch {
        set: BTreeMap::from([("weight".into(), rocksdb_sql_table::Value::Integer(value))]),
        remove: Default::default(),
    }
}
fn vertex_patch(value: i64) -> rocksdb_sql_table::experimental_graph::VertexPropertyPatch {
    rocksdb_sql_table::experimental_graph::VertexPropertyPatch {
        set: BTreeMap::from([("rank".into(), rocksdb_sql_table::Value::Integer(value))]),
        remove: Default::default(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_property_read_dependency_conflicts_when_only_target_changes() {
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    vertex(&graph, 8, 1).await;
    let e = edge(7, 8, 50);
    let create = snapshot(&graph).await.prepare_edge(&e).unwrap();
    graph.execute(&create).await.unwrap();
    let old = snapshot(&graph).await;
    let properties = old
        .prepare_edge_properties(e.identity(), &edge_patch(1))
        .unwrap();
    assert_eq!(properties.participants(), 2);
    let source_before = a.state_machine.d1_metadata().await.unwrap();
    let target_change = old
        .prepare_vertex_properties(e.to, &vertex_patch(2))
        .unwrap();
    graph.execute(&target_change).await.unwrap();
    assert_eq!(a.state_machine.d1_metadata().await.unwrap(), source_before);
    assert!(matches!(
        graph.execute(&properties).await,
        Err(Error::Application(D1Error::Conflict { .. }))
    ));
    assert!(matches!(
        graph.abort(&properties).await.unwrap(),
        Outcome::Aborted { visible: true, .. }
    ));
    assert_eq!(
        snapshot(&graph).await.edge(e.identity()).unwrap(),
        Some(e.clone())
    );
    let fresh = snapshot(&graph).await;
    let update = fresh
        .prepare_edge_properties(e.identity(), &edge_patch(3))
        .unwrap();
    let target_sequence = b.state_machine.d1_metadata().await.unwrap().2;
    assert!(matches!(
        graph.execute(&update).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    assert_eq!(
        b.state_machine.d1_metadata().await.unwrap().2,
        target_sequence + 1
    );
    let now = snapshot(&graph).await;
    assert_eq!(
        now.edge(e.identity()).unwrap().unwrap().properties["weight"],
        rocksdb_sql_table::Value::Integer(3)
    );
    assert_eq!(
        now.vertex(e.to).unwrap().unwrap().properties["rank"],
        rocksdb_sql_table::Value::Integer(2)
    );
    assert_eq!(old.edge(e.identity()).unwrap(), Some(e));
    for node in [&a, &b, &c] {
        node.raft.shutdown().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_target_guard_blocks_competitor_and_delete_preserves_old_views() {
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    vertex(&graph, 8, 1).await;
    let e = edge(8, 7, 60);
    let create = snapshot(&graph).await.prepare_edge(&e).unwrap();
    graph.execute(&create).await.unwrap();
    let old = snapshot(&graph).await;
    let update = old
        .prepare_edge_properties(e.identity(), &edge_patch(1))
        .unwrap();
    let competitor = old
        .prepare_vertex_properties(e.to, &vertex_patch(2))
        .unwrap();
    graph
        .command(Command::Begin {
            id: update.id,
            participants: update.parts.iter().map(|p| p.descriptor.clone()).collect(),
        })
        .await
        .unwrap();
    let guard = &update.parts[0];
    assert_eq!(guard.descriptor.shard, 7);
    participant_driver::prepare(
        &c,
        &a,
        authority(),
        1,
        guard.descriptor.clone(),
        guard.request.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        graph.execute(&competitor).await,
        Err(Error::Application(D1Error::ParticipantBusy))
    ));
    assert!(matches!(
        graph.abort(&competitor).await.unwrap(),
        Outcome::Aborted { visible: false, .. }
    ));
    assert!(matches!(
        graph.execute(&update).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    graph.abort(&competitor).await.unwrap();
    let before_delete = snapshot(&graph).await;
    let deletion = before_delete.prepare_edge_delete(e.identity()).unwrap();
    let stale_properties = before_delete
        .prepare_edge_properties(e.identity(), &edge_patch(9))
        .unwrap();
    let (ordinal, decision) = graph.decide(&deletion, false).await.unwrap();
    assert_eq!(decision, Decision::Commit);
    versioned_reads::advance(&c, &b, authority(), 1, target(8), ordinal)
        .await
        .unwrap();
    assert!(snapshot(&graph).await.edge(e.identity()).unwrap().is_some());
    assert!(graph
        .snapshot(Some(ordinal), limit(), Duration::from_secs(20))
        .await
        .is_err());
    assert!(matches!(
        graph.execute(&deletion).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    let after = snapshot(&graph).await;
    assert!(after.edge(e.identity()).unwrap().is_none());
    assert!(after
        .observed_adjacency(e.from, Direction::Out)
        .unwrap()
        .is_empty());
    assert!(after
        .observed_adjacency(e.to, Direction::In)
        .unwrap()
        .is_empty());
    assert!(before_delete.edge(e.identity()).unwrap().is_some());
    let tombstone = after
        .store(e.from.partition)
        .unwrap()
        .get_edge_record(e.id)
        .unwrap()
        .unwrap();
    match tombstone {
        EdgeRecord::Deleted(t) => assert_eq!(t.operation_id, deletion.id()),
        _ => panic!("expected tombstone"),
    }
    assert!(graph.execute(&stale_properties).await.is_err());
    graph.abort(&stale_properties).await.unwrap();
    assert!(after.prepare_edge(&e).is_err());
    assert_eq!(
        graph.execute(&deletion).await.unwrap(),
        Outcome::Committed {
            ordinal,
            visible: true
        }
    );
    for node in [&a, &b, &c] {
        node.raft.shutdown().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_shard_self_loop_mutations_and_tombstone_identity_survive_reopen() {
    let cluster = Cluster::new();
    let cdir = tempfile::tempdir().unwrap();
    let adir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let c = initialize(
        cluster
            .in_group(9)
            .start_durable(1, cdir.path())
            .await
            .unwrap(),
        9,
    )
    .await;
    let a = initialize(
        cluster
            .in_group(7)
            .start_durable(1, adir.path())
            .await
            .unwrap(),
        7,
    )
    .await;
    let b = initialize(
        cluster
            .in_group(8)
            .start_durable(1, bdir.path())
            .await
            .unwrap(),
        8,
    )
    .await;
    let graph = self::graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    let e = edge(7, 7, 70);
    let create = snapshot(&graph).await.prepare_edge(&e).unwrap();
    graph.execute(&create).await.unwrap();
    let props = snapshot(&graph)
        .await
        .prepare_edge_properties(e.identity(), &edge_patch(5))
        .unwrap();
    assert_eq!(props.participants(), 1);
    graph.execute(&props).await.unwrap();
    let old_version = snapshot(&graph).await.version().unwrap();
    let deletion = snapshot(&graph)
        .await
        .prepare_edge_delete(e.identity())
        .unwrap();
    assert_eq!(deletion.participants(), 1);
    graph.execute(&deletion).await.unwrap();
    let delete_id = deletion.id();
    drop(graph);
    for (node, shard) in [(&a, 7), (&b, 8), (&c, 9)] {
        cluster.in_group(shard).shutdown(node).await;
    }
    drop(a);
    drop(b);
    drop(c);
    let c = cluster
        .in_group(9)
        .start_durable(1, cdir.path())
        .await
        .unwrap();
    let a = cluster
        .in_group(7)
        .start_durable(1, adir.path())
        .await
        .unwrap();
    let b = cluster
        .in_group(8)
        .start_durable(1, bdir.path())
        .await
        .unwrap();
    for node in [&a, &b, &c] {
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader == Some(1), "reopened leader")
            .await
            .unwrap();
    }
    let graph = self::graph(&c, &a, &b);
    let after = snapshot(&graph).await;
    assert!(after.edge(e.identity()).unwrap().is_none());
    match after
        .store(7)
        .unwrap()
        .get_edge_record(e.id)
        .unwrap()
        .unwrap()
    {
        EdgeRecord::Deleted(t) => assert_eq!(t.operation_id, delete_id),
        _ => panic!("missing tombstone"),
    }
    let old = graph
        .snapshot(Some(old_version), limit(), Duration::from_secs(20))
        .await
        .unwrap();
    assert_eq!(
        old.edge(e.identity()).unwrap().unwrap().properties["weight"],
        rocksdb_sql_table::Value::Integer(5)
    );
    assert!(matches!(
        graph.execute(&deletion).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    for (node, shard) in [(&a, 7), (&b, 8), (&c, 9)] {
        cluster.in_group(shard).shutdown(node).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_request_recovers_exact_plan_after_partial_prepare_and_restart() {
    let cluster = Cluster::new();
    let cdir = tempfile::tempdir().unwrap();
    let adir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let c = initialize(
        cluster
            .in_group(9)
            .start_durable(1, cdir.path())
            .await
            .unwrap(),
        9,
    )
    .await;
    let a = initialize(
        cluster
            .in_group(7)
            .start_durable(1, adir.path())
            .await
            .unwrap(),
        7,
    )
    .await;
    let b = initialize(
        cluster
            .in_group(8)
            .start_durable(1, bdir.path())
            .await
            .unwrap(),
        8,
    )
    .await;
    let graph = self::graph(&c, &a, &b);
    vertex(&graph, 7, 1).await;
    vertex(&graph, 8, 1).await;
    let e = edge(7, 8, 80);
    let view = snapshot(&graph).await;
    let one = view.prepare_edge(&e).unwrap();
    let two = view.prepare_edge(&e).unwrap();
    assert_ne!(one.id(), two.id());
    let external = [81; 16];
    let request = b"v1/create-edge/7:1/8:1/80/reports_to".to_vec();
    let (x, y) = tokio::join!(
        graph.admit_request(external, request.clone(), &one),
        graph.admit_request(external, request.clone(), &two)
    );
    let winner = x.unwrap();
    assert_eq!(
        winner.durable_bytes().unwrap(),
        y.unwrap().durable_bytes().unwrap()
    );
    let winner_id = winner.id();
    let point = NodeRef::Local(&c)
        .request(&authority(), 1, external)
        .await
        .unwrap();
    let point = point.record.unwrap();
    assert_eq!(point.record.request, request);
    assert_eq!(point.record.payload, winner.durable_bytes().unwrap());
    assert_eq!(point.transaction.id, winner_id);
    assert_eq!(point.transaction.decision, None);
    assert!(graph
        .recover_request([82; 16], &request)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        c.state_machine
            .coordinator_journal(9, 1, [9; 16])
            .await
            .unwrap()
            .transaction_count(),
        3
    );
    let part = &winner.parts[0];
    participant_driver::prepare(
        &c,
        &a,
        authority(),
        1,
        part.descriptor.clone(),
        part.request.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        graph.recover_request(external, b"different").await,
        Err(Error::Identity)
    ));
    drop(winner);
    drop(one);
    drop(two);
    drop(view);
    drop(graph);
    for (node, shard) in [(&a, 7), (&b, 8), (&c, 9)] {
        cluster.in_group(shard).shutdown(node).await;
    }
    drop(a);
    drop(b);
    drop(c);
    let c = cluster
        .in_group(9)
        .start_durable(1, cdir.path())
        .await
        .unwrap();
    let a = cluster
        .in_group(7)
        .start_durable(1, adir.path())
        .await
        .unwrap();
    let b = cluster
        .in_group(8)
        .start_durable(1, bdir.path())
        .await
        .unwrap();
    for node in [&a, &b, &c] {
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader == Some(1), "restarted")
            .await
            .unwrap();
    }
    let graph = self::graph(&c, &a, &b);
    let recovered = graph
        .recover_request(external, &request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.id(), winner_id);
    let original = recovered.durable_bytes().unwrap();
    assert!(matches!(
        graph.execute(&recovered).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    // Model a lost final response: recover by external ID and retry, with no
    // candidate generation or new snapshot/identity.
    let again = graph
        .recover_request(external, &request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.durable_bytes().unwrap(), original);
    let point = NodeRef::Local(&c)
        .request(&authority(), 1, external)
        .await
        .unwrap();
    let point = point.record.unwrap();
    assert_eq!(point.record.payload, original);
    assert_eq!(point.transaction.id, winner_id);
    assert_eq!(point.transaction.decision, Some(Decision::Commit));
    assert!(matches!(
        graph.execute(&again).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    assert_eq!(snapshot(&graph).await.edge(e.identity()).unwrap(), Some(e));
    assert_eq!(
        c.state_machine
            .coordinator_journal(9, 1, [9; 16])
            .await
            .unwrap()
            .transaction_count(),
        3
    );
    for (node, shard) in [(&a, 7), (&b, 8), (&c, 9)] {
        cluster.in_group(shard).shutdown(node).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_plan_never_refreshes_a_stale_validation_sequence() {
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    let view = snapshot(&graph).await;
    let stale = view.prepare_vertex(7, &Vertex::new(1)).unwrap();
    let later = view.prepare_vertex(7, &Vertex::new(2)).unwrap();
    assert!(matches!(
        graph.execute(&later).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    let admitted = graph
        .admit_request([90; 16], b"create vertex 7:1".to_vec(), &stale)
        .await
        .unwrap();
    let recovered = graph
        .recover_request([90; 16], b"create vertex 7:1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recovered.durable_bytes().unwrap(),
        admitted.durable_bytes().unwrap()
    );
    assert!(matches!(
        graph.execute(&recovered).await,
        Err(Error::Application(D1Error::Conflict { .. }))
    ));
    graph.abort(&recovered).await.unwrap();
    graph.execute(&later).await.unwrap();
    let now = snapshot(&graph).await;
    assert!(now
        .vertex(VertexRef {
            partition: 7,
            id: 1
        })
        .unwrap()
        .is_none());
    assert!(now
        .vertex(VertexRef {
            partition: 7,
            id: 2
        })
        .unwrap()
        .is_some());
    for node in [&a, &b, &c] {
        node.raft.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_history_catches_up_every_member_after_observed_only_page() {
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    let old = snapshot(&graph).await;
    let mut last = None;
    for id in 1..=20 {
        let plan = old.prepare_vertex(7, &Vertex::new(id)).unwrap();
        assert_eq!(
            graph.decide(&plan, true).await.unwrap(),
            (id, Decision::Abort)
        );
        last = Some(plan);
    }
    let initial = NodeRef::Local(&a)
        .history_status(&authority(), &target(7), &binding())
        .await
        .unwrap();
    assert_eq!(initial.cursor.event_count, 1);
    let page = NodeRef::Local(&c)
        .history_page(&authority(), 1, &initial.cursor, None)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 32);
    assert!(page.end.event_count < page.cut.event_count);
    let observed = NodeRef::Local(&a)
        .sync_history(&authority(), &target(7), &page)
        .await
        .unwrap();
    assert_eq!(observed.cursor, page.end);
    assert_eq!(observed.applied_prefix, 0);
    assert_eq!(snapshot(&graph).await.version().unwrap(), 0);

    graph.advance_to(&last.unwrap(), 20).await.unwrap();
    for (node, shard) in [(&a, 7), (&b, 8)] {
        let status = NodeRef::Local(node)
            .history_status(&authority(), &target(shard), &binding())
            .await
            .unwrap();
        assert_eq!(status.cursor, page.cut);
        assert_eq!(status.applied_prefix, 20);
    }
    // Shard 8 never participated or prepared, but has consumed every Abort.
    assert_eq!(snapshot(&graph).await.version().unwrap(), 20);
    vertex(&graph, 7, 21).await;
    assert!(old
        .vertex(VertexRef {
            partition: 7,
            id: 21
        })
        .unwrap()
        .is_none());
    assert!(snapshot(&graph)
        .await
        .vertex(VertexRef {
            partition: 7,
            id: 21
        })
        .unwrap()
        .is_some());
    for node in [&c, &a, &b] {
        node.raft.shutdown().await.unwrap();
    }
}

/// Raw scripted peers verify the real managed wire path without permitting an
/// accidental full Journal/Advance fallback, including lost acknowledgements.
async fn history_peer(
    identity: managed_rpc::Identity,
    steps: Vec<(serde_json::Value, Option<serde_json::Value>)>,
) -> (managed_rpc::Client, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = managed_rpc::Client::new(
        listener.local_addr().unwrap(),
        identity,
        Duration::from_secs(2),
    )
    .unwrap();
    let peer = tokio::spawn(async move {
        for (expected, result) in steps {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let length = stream.read_u32().await.unwrap() as usize;
            assert!(length <= managed_rpc::MAX_FRAME_BYTES);
            let mut bytes = vec![0; length];
            stream.read_exact(&mut bytes).await.unwrap();
            let request: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(request["version"], 5);
            assert!(request["operation"].get("Journal").is_none());
            assert!(request["operation"].get("Advance").is_none());
            assert_eq!(request["operation"], expected);
            let Some(result) = result else {
                continue;
            };
            let tag = match expected
                .as_object()
                .unwrap()
                .keys()
                .next()
                .unwrap()
                .as_str()
            {
                "Terminal" => 9,
                "HistoryStatus" => 11,
                "HistoryPage" => 12,
                "SyncHistory" => 13,
                "AdvanceRetained" => 14,
                "Prefix" => 6,
                other => panic!("unexpected catch-up operation {other}"),
            };
            let reply = serde_json::json!({
                "version": 5, "operation": tag, "identity": request["identity"],
                "nonce": request["nonce"], "result": result,
            });
            let bytes = serde_json::to_vec(&reply).unwrap();
            stream.write_u32(bytes.len() as u32).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
        }
    });
    (client, peer)
}
fn history_identity(shard: u64) -> managed_rpc::Identity {
    managed_rpc::Identity {
        group: shard,
        node: 1,
        target: target(shard),
        role: if shard == 9 {
            managed_rpc::Role::Coordinator
        } else {
            managed_rpc::Role::Participant
        },
        authority: authority(),
        cohort: binding(),
    }
}
fn history_plan() -> Plan {
    Plan {
        id: [20; 16],
        authority: authority(),
        cohort: binding(),
        version: 0,
        parts: vec![Part {
            descriptor: Participant {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
                intent: [20; 32],
            },
            request: D1WriteRequest {
                shard: 7,
                epoch: 1,
                request_id: [20; 16],
                expected_sequence: Some(0),
                operations: vec![d2_raft::WriteOp::put("key", "value")],
            },
        }],
    }
}
fn history_journal() -> Journal {
    let mut journal = Journal::new(authority().mode).unwrap();
    journal
        .apply(Command::BindCohort { cohort: binding() })
        .unwrap();
    for id in 1..=20 {
        journal
            .apply(Command::Begin {
                id: [id; 16],
                participants: vec![Participant {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                    intent: [id; 32],
                }],
            })
            .unwrap();
        journal
            .apply(Command::Decide {
                id: [id; 16],
                decision: Decision::Abort,
            })
            .unwrap();
    }
    journal
}
#[tokio::test]
async fn advance_to_uses_only_pinned_suffix_protocol_and_retries_exact_unknown_commands() {
    use serde_json::json;
    let journal = history_journal();
    let base = journal.history_cursor_at(1).unwrap();
    let first = journal.history_page(&base, None).unwrap();
    let second = journal.history_page(&first.end, Some(&first.cut)).unwrap();
    let cut = first.cut.clone();
    let proof = journal.terminal_prefix(20).unwrap();
    let mut later = journal.clone();
    later
        .apply(Command::Begin {
            id: [21; 16],
            participants: history_plan()
                .parts
                .into_iter()
                .map(|p| p.descriptor)
                .collect(),
        })
        .unwrap();
    later
        .apply(Command::Decide {
            id: [21; 16],
            decision: Decision::Abort,
        })
        .unwrap();
    let ahead = later.history_cursor().unwrap();
    let terminal = json!({"Terminal": {"authority": authority(), "epoch": 1, "version": 20}});
    let page_first =
        json!({"HistoryPage": {"authority": authority(), "epoch": 1, "base": base, "cut": null}});
    let page_second = json!({"HistoryPage": {"authority": authority(), "epoch": 1, "base": first.end, "cut": cut}});
    let (coordinator, coordinator_peer) = history_peer(
        history_identity(9),
        vec![
            (terminal, Some(json!({"Ok": {"Evidence": proof}}))),
            (
                page_first.clone(),
                Some(json!({"Ok": {"HistoryPage": first}})),
            ),
            (
                page_second.clone(),
                Some(json!({"Ok": {"HistoryPage": second}})),
            ),
            (page_first, Some(json!({"Ok": {"HistoryPage": first}}))),
            (page_second, Some(json!({"Ok": {"HistoryPage": second}}))),
        ],
    )
    .await;
    let mut clients = Vec::new();
    let mut peers = Vec::new();
    for shard in [7, 8] {
        let status = json!({"HistoryStatus": {"authority": authority(), "target": target(shard), "cohort": binding()}});
        let sync_first = json!({"SyncHistory": {"authority": authority(), "target": target(shard), "page": first}});
        let sync_second = json!({"SyncHistory": {"authority": authority(), "target": target(shard), "page": second}});
        let mut steps = vec![(
            status,
            Some(json!({"Ok": {"HistoryStatus": {"cursor": base, "applied_prefix": 0}}})),
        )];
        if shard == 7 {
            // Drop the first durable-sync ACK; the complete request must repeat.
            steps.push((sync_first.clone(), None));
        }
        let returned = if shard == 7 { &ahead } else { &first.end };
        steps.push((
            sync_first,
            Some(json!({"Ok": {"HistorySynced": {"cursor": returned, "applied_prefix": 0}}})),
        ));
        steps.push((sync_second, Some(json!({"Ok": {"HistorySynced": {"cursor": if shard == 7 { &ahead } else { &cut }, "applied_prefix": 0}}}))));
        for ordinal in 1..=if shard == 7 { 1 } else { 20 } {
            let advance = json!({"AdvanceRetained": {"authority": authority(), "target": target(shard), "ordinal": ordinal, "cut": cut}});
            if shard == 7 {
                steps.push((advance.clone(), None));
            }
            // Concurrent consumption may have advanced beyond the requested V.
            steps.push((
                advance,
                Some(json!({"Ok": {"RetainedPrefix": if shard == 7 { 21 } else { ordinal }}})),
            ));
        }
        steps.push((
            json!({"Prefix": {"target": target(shard), "proof": proof}}),
            Some(json!({"Ok": {"Evidence": proof}})),
        ));
        let (client, peer) = history_peer(history_identity(shard), steps).await;
        clients.push(client);
        peers.push(peer);
    }
    let graph = GraphCohort::from_endpoints(
        NodeRef::Remote(&coordinator),
        authority(),
        1,
        vec![
            (NodeRef::Remote(&clients[0]), target(7)),
            (NodeRef::Remote(&clients[1]), target(8)),
        ],
    );
    graph.advance_to(&history_plan(), 20).await.unwrap();
    coordinator_peer.await.unwrap();
    for peer in peers {
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn advance_to_new_protocol_failure_has_no_archive_fallback() {
    use serde_json::json;
    let journal = history_journal();
    let base = journal.history_cursor_at(1).unwrap();
    let proof = journal.terminal_prefix(20).unwrap();
    let (coordinator, c_peer) = history_peer(history_identity(9), vec![
        (json!({"Terminal": {"authority": authority(), "epoch": 1, "version": 20}}), Some(json!({"Ok": {"Evidence": proof}}))),
        (json!({"HistoryPage": {"authority": authority(), "epoch": 1, "base": base, "cut": null}}), Some(json!({"Err": "Protocol"}))),
    ]).await;
    let (member, m_peer) = history_peer(history_identity(7), vec![
        (json!({"HistoryStatus": {"authority": authority(), "target": target(7), "cohort": binding()}}),
            Some(json!({"Ok": {"HistoryStatus": {"cursor": base, "applied_prefix": 0}}}))),
    ]).await;
    // The second member is never contacted after the source rejects v5 paging.
    let graph = GraphCohort::from_endpoints(
        NodeRef::Remote(&coordinator),
        authority(),
        1,
        vec![
            (NodeRef::Remote(&member), target(7)),
            (NodeRef::Remote(&member), target(8)),
        ],
    );
    assert!(matches!(
        graph.advance_to(&history_plan(), 20).await,
        Err(Error::Identity)
    ));
    c_peer.await.unwrap();
    m_peer.await.unwrap();
}
