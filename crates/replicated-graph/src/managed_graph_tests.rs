use super::*;
use d2_raft::transaction_decisions::Member;
use d2_raft::{Cluster, D1Result};
use rocksdb_sql_table::experimental_graph::Direction;
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
    let graph = graph(&c, &a, &b);
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
    let graph = graph(&c, &a, &b);
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
    let graph = graph(&c, &a, &b);
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
    let graph = graph(&c, &a, &b);
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
