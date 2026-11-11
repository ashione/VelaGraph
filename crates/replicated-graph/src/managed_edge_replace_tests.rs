use super::*;
use rocksdb_sql_table::Value;

struct FourShards {
    cluster: Cluster,
    nodes: Vec<NodeHandle>,
}
impl FourShards {
    async fn new() -> Self {
        let cluster = Cluster::new();
        let mut cohort = binding();
        cohort.members = [3, 5, 7, 8]
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .to_vec();
        let mut nodes = Vec::new();
        for shard in [9, 3, 5, 7, 8] {
            nodes.push(
                initialize_cohort(
                    cluster.in_group(shard).start(1).await.unwrap(),
                    shard,
                    cohort.clone(),
                )
                .await,
            );
        }
        let fixture = Self { cluster, nodes };
        for shard in [3, 5, 7, 8] {
            vertex(&fixture.graph(), shard, 1).await;
        }
        fixture
    }
    fn graph(&self) -> GraphCohort<'_> {
        GraphCohort::new(
            &self.nodes[0],
            authority(),
            1,
            self.nodes[1..]
                .iter()
                .zip([3, 5, 7, 8])
                .map(|(node, shard)| (node, target(shard)))
                .collect(),
        )
    }
    fn node(&self, shard: u64) -> &NodeHandle {
        &self.nodes[[9, 3, 5, 7, 8].iter().position(|s| *s == shard).unwrap()]
    }
    async fn stop(self) {
        for (node, shard) in self.nodes.iter().zip([9, 3, 5, 7, 8]) {
            self.cluster.in_group(shard).shutdown(node).await;
        }
    }
}
fn assert_edge(view: &GraphSnapshot, edge: &CrossEdge, present: bool) {
    assert_eq!(
        view.edge(edge.identity()).unwrap(),
        present.then(|| edge.clone())
    );
    for (vertex, direction) in [(edge.from, Direction::Out), (edge.to, Direction::In)] {
        assert_eq!(
            view.observed_adjacency(vertex, direction)
                .unwrap()
                .iter()
                .any(|e| e.identity() == edge.identity()),
            present
        );
    }
}
fn assert_tombstone(view: &GraphSnapshot, edge: &CrossEdge, plan: &Plan) {
    match view.verified_edge_record(edge.identity()).unwrap().unwrap() {
        EdgeRecord::Deleted(t) => assert_eq!(t.operation_id, plan.id()),
        _ => panic!("expected permanent old-identity tombstone"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_four_shards_partial_apply_retains_common_old_version() {
    let f = FourShards::new().await;
    let graph = f.graph();
    let old_edge = edge(8, 7, 40);
    let mut new_edge = edge(5, 3, 40); // Same numeric ID, different qualified source.
    new_edge.label = "moved".into();
    new_edge
        .properties
        .insert("weight".into(), Value::Integer(3));
    graph
        .execute(&snapshot(&graph).await.prepare_edge(&old_edge).unwrap())
        .await
        .unwrap();
    let old = snapshot(&graph).await;
    let plan = old
        .prepare_edge_replace(old_edge.identity(), &new_edge)
        .unwrap();
    assert_eq!(plan.participants(), 4);
    assert_eq!(
        plan.parts
            .iter()
            .map(|p| p.descriptor.shard)
            .collect::<Vec<_>>(),
        [3, 5, 7, 8]
    );
    assert_eq!(
        plan.parts
            .iter()
            .map(|p| p.request.operations.len())
            .sum::<usize>(),
        6
    );
    assert!(plan.parts.iter().all(|p| p.request.request_id == plan.id()));
    let tx = graph.decide(&plan, false).await.unwrap();
    assert_eq!(tx.decision, Some(Decision::Commit));
    // Publish the replacement half first. It must not become a certified V.
    for shard in [3, 5] {
        versioned_reads::advance(
            f.node(9),
            f.node(shard),
            authority(),
            1,
            target(shard),
            tx.ordinal,
        )
        .await
        .unwrap();
    }
    let common = snapshot(&graph).await;
    assert_eq!(common.version().unwrap(), old.version().unwrap());
    assert_edge(&common, &old_edge, true);
    assert_edge(&common, &new_edge, false);
    assert!(graph
        .snapshot(Some(tx.ordinal), limit(), Duration::from_secs(20))
        .await
        .is_err());
    let expected = Outcome::Committed {
        ordinal: tx.ordinal,
        visible: true,
    };
    assert_eq!(graph.execute(&plan).await.unwrap(), expected);
    assert_eq!(graph.execute(&plan).await.unwrap(), expected);
    let after = snapshot(&graph).await;
    assert_edge(&after, &old_edge, false);
    assert_edge(&after, &new_edge, true);
    assert_tombstone(&after, &old_edge, &plan);
    assert_edge(&old, &old_edge, true);
    assert_edge(&old, &new_edge, false);
    assert!(matches!(
        after.prepare_edge(&old_edge),
        Err(Error::Graph(ExperimentalError::AlreadyExists))
    ));
    drop(graph);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_all_fifteen_shard_overlap_partitions_preserve_both_halves() {
    let f = FourShards::new().await;
    let graph = f.graph();
    // Restricted-growth strings enumerate every equivalence partition of four
    // endpoint slots. This includes self-loops, reversed edges, shared targets,
    // disjoint local loops, and one, two, three and four participant plans.
    let patterns = [
        [0, 0, 0, 0],
        [0, 0, 0, 1],
        [0, 0, 1, 0],
        [0, 0, 1, 1],
        [0, 0, 1, 2],
        [0, 1, 0, 0],
        [0, 1, 0, 1],
        [0, 1, 0, 2],
        [0, 1, 1, 0],
        [0, 1, 1, 1],
        [0, 1, 1, 2],
        [0, 1, 2, 0],
        [0, 1, 2, 1],
        [0, 1, 2, 2],
        [0, 1, 2, 3],
    ];
    for (index, pattern) in patterns.into_iter().enumerate() {
        let [a, b, c, d] = pattern.map(|i| [8, 7, 5, 3][i]);
        let old_edge = edge(a, b, 100 + index as u64);
        let new_edge = edge(c, d, 200 + index as u64);
        graph
            .execute(&snapshot(&graph).await.prepare_edge(&old_edge).unwrap())
            .await
            .unwrap();
        let before = snapshot(&graph).await;
        let plan = before
            .prepare_edge_replace(old_edge.identity(), &new_edge)
            .unwrap();
        assert_eq!(
            plan.participants(),
            pattern.into_iter().max().unwrap() + 1,
            "{pattern:?}"
        );
        assert_eq!(
            plan.parts
                .iter()
                .map(|p| p.request.operations.len())
                .sum::<usize>(),
            6
        );
        assert!(matches!(
            graph.execute(&plan).await.unwrap(),
            Outcome::Committed { visible: true, .. }
        ));
        let after = snapshot(&graph).await;
        assert_edge(&before, &old_edge, true);
        assert_edge(&before, &new_edge, false);
        assert_edge(&after, &old_edge, false);
        assert_edge(&after, &new_edge, true);
        assert_tombstone(&after, &old_edge, &plan);
    }
    drop(graph);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_each_participant_cas_conflicts_and_explicit_abort_keeps_graph() {
    let f = FourShards::new().await;
    let graph = f.graph();
    let old_edge = edge(8, 7, 50);
    let new_edge = edge(5, 3, 51);
    graph
        .execute(&snapshot(&graph).await.prepare_edge(&old_edge).unwrap())
        .await
        .unwrap();
    for shard in [3, 5, 7, 8] {
        let before = snapshot(&graph).await;
        let stale = before
            .prepare_edge_replace(old_edge.identity(), &new_edge)
            .unwrap();
        // All four endpoints are read dependencies, even if on separate shards.
        let change = before
            .prepare_vertex_properties(
                VertexRef {
                    partition: shard,
                    id: 1,
                },
                &vertex_patch(shard as i64),
            )
            .unwrap();
        graph.execute(&change).await.unwrap();
        assert!(matches!(
            graph.execute(&stale).await,
            Err(Error::Application(D1Error::Conflict { .. }))
        ));
        assert!(graph.status(&stale).await.unwrap().is_none());
        assert!(matches!(
            graph.abort(&stale).await.unwrap(),
            Outcome::Aborted { visible: true, .. }
        ));
        let after = snapshot(&graph).await;
        assert_edge(&after, &old_edge, true);
        assert_edge(&after, &new_edge, false);
    }
    // Every partial prepare was released; a fresh original-CAS plan can commit.
    let plan = snapshot(&graph)
        .await
        .prepare_edge_replace(old_edge.identity(), &new_edge)
        .unwrap();
    assert!(matches!(
        graph.execute(&plan).await.unwrap(),
        Outcome::Committed { visible: true, .. }
    ));
    drop(graph);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_rejections_do_not_change_live_edge_or_admit_requests() {
    let f = FourShards::new().await;
    let graph = f.graph();
    let old_edge = edge(8, 7, 60);
    let existing = edge(5, 3, 61);
    let deleted = edge(5, 3, 62);
    for edge in [&old_edge, &existing, &deleted] {
        graph
            .execute(&snapshot(&graph).await.prepare_edge(edge).unwrap())
            .await
            .unwrap();
    }
    graph
        .execute(
            &snapshot(&graph)
                .await
                .prepare_edge_delete(deleted.identity())
                .unwrap(),
        )
        .await
        .unwrap();
    let view = snapshot(&graph).await;
    let version = view.version().unwrap();
    let new_edge = edge(5, 3, 63);
    assert!(matches!(
        view.prepare_edge_replace(
            EdgeRef {
                partition: 8,
                id: 999
            },
            &new_edge
        ),
        Err(Error::Graph(ExperimentalError::NotFound(_)))
    ));
    assert!(matches!(
        view.prepare_edge_replace(deleted.identity(), &new_edge),
        Err(Error::Graph(ExperimentalError::NotFound(_)))
    ));
    assert!(matches!(
        view.prepare_edge_replace(old_edge.identity(), &old_edge),
        Err(Error::Graph(ExperimentalError::Invalid(_)))
    ));
    for replacement in [&existing, &deleted] {
        assert!(matches!(
            view.prepare_edge_replace(old_edge.identity(), replacement),
            Err(Error::Graph(ExperimentalError::AlreadyExists))
        ));
    }
    for is_source in [false, true] {
        let mut missing = new_edge.clone();
        if is_source {
            missing.from.id = 999;
        } else {
            missing.to.id = 999;
        }
        assert!(matches!(
            view.prepare_edge_replace(old_edge.identity(), &missing),
            Err(Error::Graph(ExperimentalError::MissingEndpoint(_)))
        ));
    }
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut bad = new_edge.clone();
        bad.properties.insert("bad".into(), Value::Real(invalid));
        assert!(view
            .prepare_edge_replace(old_edge.identity(), &bad)
            .is_err());
    }
    let mut huge = new_edge.clone();
    huge.properties
        .insert("large".into(), Value::Text("x".repeat(1024 * 1024)));
    assert!(view
        .prepare_edge_replace(old_edge.identity(), &huge)
        .is_err());
    assert_eq!(snapshot(&graph).await.version().unwrap(), version);
    assert_edge(&view, &old_edge, true);
    assert_edge(&view, &new_edge, false);
    drop(graph);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_corrupt_old_halves_fail_before_plan() {
    let f = FourShards::new().await;
    let graph = f.graph();
    let old_edge = edge(8, 7, 80);
    let new_edge = edge(5, 3, 81);
    graph
        .execute(&snapshot(&graph).await.prepare_edge(&old_edge).unwrap())
        .await
        .unwrap();
    for (shard, marker) in [
        (8, b"/out/".as_slice()),
        (7, b"/in/"),
        (8, b"/vertex/"),
        (7, b"/vertex/"),
    ] {
        // Inject a malformed retained read view without any production bypass
        // or raw managed write. ReplaceEdge must still validate both old halves.
        let mut corrupt = snapshot(&graph).await;
        let retained = corrupt.session.shard(shard).unwrap();
        let mut entries = retained.entries.clone();
        let key = entries
            .keys()
            .find(|key| key.windows(marker.len()).any(|w| w == marker))
            .unwrap()
            .clone();
        entries.remove(&key).unwrap();
        let view = ReadView::from_entries(shard, retained.epoch, retained.local_sequence, entries)
            .unwrap();
        corrupt.views.insert(shard, view);
        assert!(matches!(
            corrupt.prepare_edge_replace(old_edge.identity(), &new_edge),
            Err(Error::Graph(ExperimentalError::Incomplete(_)))
        ));
    }
    let actual = snapshot(&graph).await;
    assert_edge(&actual, &old_edge, true);
    assert_edge(&actual, &new_edge, false);
    drop(graph);
    f.stop().await;
}
