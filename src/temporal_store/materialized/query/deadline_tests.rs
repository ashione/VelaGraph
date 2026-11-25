use super::*;

fn model() -> TemporalReadModel {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let valid = Interval { from: 0, to: None };
    store
        .commit(&[
            TemporalMutation::Vertex {
                valid,
                vertex: Vertex::new(1),
            },
            TemporalMutation::Vertex {
                valid,
                vertex: Vertex::new(2),
            },
            TemporalMutation::Vertex {
                valid: Interval { from: 20, to: None },
                vertex: Vertex::new(3),
            },
            TemporalMutation::Edge {
                valid,
                edge: Edge::new(10, 1, 2, "LINK"),
            },
            TemporalMutation::Edge {
                valid,
                edge: Edge::new(11, 1, 1, "LINK"),
            },
            TemporalMutation::Edge {
                valid,
                edge: Edge::new(12, 1, 3, "FUTURE"),
            },
        ])
        .unwrap();
    store
        .materialize(None, MaterializationLimits::default())
        .unwrap()
}

fn backend<'a, 'g>(view: &'a TemporalReadModelView<'g>, deadline: Instant) -> Backend<'a, 'g> {
    Backend {
        view,
        deadline,
        used: Cell::new(0),
        bytes: Cell::new(0),
        limits: MaterializedQueryLimits::default(),
    }
}

#[test]
fn expired_execution_rejects_hits_misses_and_empty_results() {
    let model = model();
    let view = model.view(2);
    for text in [
        "g.V(1)",
        "g.V(99)",
        "g.E(10)",
        "g.E(99)",
        "g.E(12)",
        "g.V(1).has('missing',1)",
        "g.V(1).out('ABSENT')",
    ] {
        let backend = backend(&view, Instant::now());
        let result = GremlinQuery::parse(text).unwrap().execute_on_read_budget(
            &backend,
            &|| backend.check(),
            &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
        );
        assert!(
            matches!(result, Err(crate::Error::TraversalLimitExceeded(_))),
            "{text}"
        );
    }
}

#[test]
fn cancelled_missing_empty_and_filtered_adjacency_never_visits() {
    let model = model();
    let view = model.view(2);
    let backend = backend(&view, Instant::now() + crate::GREMLIN_DEADLINE);
    for (id, label) in [(99, None), (2, None), (1, Some("ABSENT"))] {
        let result = backend.visit_query_edges(
            id,
            false,
            label,
            &mut || Err(work_error()),
            &mut |_| panic!("cancelled visitor ran"),
            &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
        );
        assert!(matches!(
            result,
            Err(crate::Error::TraversalLimitExceeded(_))
        ));
    }
}

#[test]
fn cancellation_after_final_visitor_is_not_success() {
    let model = model();
    let view = model.view(2);
    let backend = backend(&view, Instant::now() + crate::GREMLIN_DEADLINE);
    let cancelled = Cell::new(false);
    let visits = Cell::new(0);
    // Vertex 1 has one incoming candidate: its self-loop.
    let result = backend.visit_query_edges(
        1,
        true,
        None,
        &mut || {
            if cancelled.get() {
                Err(work_error())
            } else {
                Ok(())
            }
        },
        &mut |_| {
            visits.set(visits.get() + 1);
            cancelled.set(true);
            Ok(())
        },
        &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
    );
    assert_eq!(visits.get(), 1);
    assert!(matches!(
        result,
        Err(crate::Error::TraversalLimitExceeded(_))
    ));
}

#[test]
fn filtered_invisible_and_duplicate_candidates_keep_exact_work_admission() {
    let model = model();
    let view = model.view(2);
    for (text, operations, rows) in [
        // Start + three candidates + four endpoint checks + two targets.
        ("g.V(1).out('LINK').path()", 10, 2),
        // The invisible endpoint still consumes both visibility checks.
        ("g.V(1).out().path()", 12, 2),
        // Both sees the self-loop twice, charging the duplicate candidate and
        // endpoint checks, but the executor only reads its target once.
        ("g.V(1).both().path()", 15, 2),
        ("g.E(12)", 3, 0),
    ] {
        let query = GremlinQuery::parse(text).unwrap();
        let exact = MaterializedQueryLimits {
            max_operations: operations,
            ..Default::default()
        };
        assert_eq!(
            view.execute_query_with_limits(&query, exact)
                .unwrap()
                .rows
                .len(),
            rows,
            "{text}"
        );
        assert!(
            matches!(
                view.execute_query_with_limits(
                    &query,
                    MaterializedQueryLimits {
                        max_operations: operations - 1,
                        ..exact
                    }
                ),
                Err(crate::Error::TraversalLimitExceeded(_))
            ),
            "{text}"
        );
    }
}

#[test]
fn cancellation_between_filtered_candidates_stops_remaining_work() {
    let model = model();
    let view = model.view(2);
    let backend = backend(&view, Instant::now() + crate::GREMLIN_DEADLINE);
    let checks = Cell::new(0);
    let result = backend.visit_query_edges(
        1,
        false,
        Some("ABSENT"),
        &mut || {
            checks.set(checks.get() + 1);
            // Entry, first candidate, then cancel before the second candidate.
            if checks.get() == 3 {
                Err(work_error())
            } else {
                Ok(())
            }
        },
        &mut |_| panic!("label-rejected row reached visitor"),
        &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
    );
    assert!(matches!(
        result,
        Err(crate::Error::TraversalLimitExceeded(_))
    ));
    assert_eq!(checks.get(), 3);
    assert_eq!(backend.used.get(), 1);
}

#[test]
fn empty_paths_check_cancellation_after_initial_admission() {
    let model = model();
    let view = model.view(2);
    let backend = backend(&view, Instant::now() + crate::GREMLIN_DEADLINE);
    for id in [99, 2] {
        let checks = Cell::new(0);
        let result = backend.visit_query_edges(
            id,
            false,
            None,
            &mut || {
                checks.set(checks.get() + 1);
                if checks.get() == 2 {
                    Err(work_error())
                } else {
                    Ok(())
                }
            },
            &mut |_| panic!("empty adjacency reached visitor"),
            &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
        );
        assert!(matches!(
            result,
            Err(crate::Error::TraversalLimitExceeded(_))
        ));
        assert_eq!(checks.get(), 2);
    }
    let checks = Cell::new(0);
    // A missing point passes executor entry and the start-read checkpoint;
    // its empty result must still reach the final success checkpoint.
    let result = GremlinQuery::parse("g.V(99)")
        .unwrap()
        .execute_on_read_budget(
            &backend,
            &|| {
                checks.set(checks.get() + 1);
                if checks.get() == 3 {
                    Err(work_error())
                } else {
                    Ok(())
                }
            },
            &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
        );
    assert!(matches!(
        result,
        Err(crate::Error::TraversalLimitExceeded(_))
    ));
    assert_eq!(checks.get(), 3);
}

#[test]
fn pruned_blocks_observe_cancellation_and_preserve_partial_logical_charges() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let mut mutations = vec![
        TemporalMutation::Vertex {
            valid: Interval { from: 0, to: None },
            vertex: Vertex::new(1),
        },
        TemporalMutation::Vertex {
            valid: Interval { from: 0, to: None },
            vertex: Vertex::new(2),
        },
    ];
    mutations.extend((0..65).map(|id| TemporalMutation::Edge {
        valid: Interval {
            from: 100,
            to: Some(200),
        },
        edge: Edge::new(1000 + id, 1, 2, "FUTURE"),
    }));
    store.commit(&mutations).unwrap();
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let view = model.view(0);

    // Entry, first pruned block (32 rows), then cancellation before block 2.
    // No skipped candidate may reach a visitor, but all skipped logical fees
    // must be charged before the next checkpoint.
    let cancel_backend = backend(&view, Instant::now() + crate::GREMLIN_DEADLINE);
    let checks = Cell::new(0);
    let result = cancel_backend.visit_query_edges(
        1,
        false,
        None,
        &mut || {
            checks.set(checks.get() + 1);
            if checks.get() == 3 {
                Err(work_error())
            } else {
                Ok(())
            }
        },
        &mut |_| panic!("pruned candidate reached visitor"),
        &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
    );
    assert!(matches!(
        result,
        Err(crate::Error::TraversalLimitExceeded(_))
    ));
    assert_eq!(checks.get(), 3);
    assert_eq!(cancel_backend.used.get(), 32);
    assert_eq!(cancel_backend.bytes.get(), 32 * ROW_WORK_BYTES);

    // Cancellation at the final checkpoint must not turn an empty pruned
    // traversal into a successful response.
    let backend = backend(&view, Instant::now() + crate::GREMLIN_DEADLINE);
    let checks = Cell::new(0);
    let result = backend.visit_query_edges(
        1,
        false,
        None,
        &mut || {
            checks.set(checks.get() + 1);
            if checks.get() == 5 {
                Err(work_error())
            } else {
                Ok(())
            }
        },
        &mut |_| panic!("pruned candidate reached visitor"),
        &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
    );
    assert!(matches!(
        result,
        Err(crate::Error::TraversalLimitExceeded(_))
    ));
    assert_eq!(checks.get(), 5);
    assert_eq!(backend.used.get(), 65);
    assert_eq!(backend.bytes.get(), 65 * ROW_WORK_BYTES);
}
