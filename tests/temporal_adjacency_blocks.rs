use velagraph::{
    temporal_reference::Interval,
    temporal_store::{
        materialized::{MaterializationLimits, MaterializedQueryLimits},
        TemporalMutation, TemporalStore,
    },
    Edge, GremlinQuery, GremlinRow, Vertex,
};

fn future_edges(store: &TemporalStore, count: u64) {
    let mut mutations = vec![1, 2]
        .into_iter()
        .map(|id| TemporalMutation::Vertex {
            valid: Interval {
                from: i64::MIN,
                to: None,
            },
            vertex: Vertex::new(id),
        })
        .collect::<Vec<_>>();
    mutations.extend((0..count).map(|id| TemporalMutation::Edge {
        valid: Interval {
            from: 100,
            to: Some(200),
        },
        edge: Edge::new(1000 + id, 1, 2, "LINK"),
    }));
    store.commit(&mutations).unwrap();
}
#[test]
fn skipped_candidates_still_consume_the_original_operation_budget() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    future_edges(&store, 128);
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let query = GremlinQuery::parse("g.V(1).out().count()").unwrap();
    for operations in [128, 129] {
        let result = model.view(0).execute_query_with_limits(
            &query,
            MaterializedQueryLimits {
                max_operations: operations,
                ..Default::default()
            },
        );
        if operations == 128 {
            assert!(matches!(
                result,
                Err(velagraph::Error::TraversalLimitExceeded(_))
            ));
        } else {
            assert_eq!(result.unwrap().rows, vec![GremlinRow::Count(0)]);
        }
    }
}
#[cfg(feature = "query-profiling")]
#[test]
fn impossible_long_adjacency_does_not_visit_individual_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    future_edges(&store, 128);
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let query = GremlinQuery::parse("g.V(1).out().count()").unwrap();
    velagraph::query_profile::begin();
    let rows = model.view(0).execute_query(&query).unwrap().rows;
    let profile = velagraph::query_profile::finish();
    assert_eq!(rows, vec![GremlinRow::Count(0)]);
    assert_eq!(profile.materialized_candidates, 0);
    assert_eq!(profile.materialized_blocks, 4);
    assert_eq!(profile.materialized_skipped_candidates, 128);
}

#[test]
fn indexed_ranges_preserve_order_and_visibility_across_thresholds_and_reopen() {
    use velagraph::temporal_reference::Key;
    for count in [63, 64, 65, 97] {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        let mut mutations = vec![1, 2, 3]
            .into_iter()
            .map(|id| TemporalMutation::Vertex {
                valid: Interval {
                    from: i64::MIN,
                    to: None,
                },
                vertex: Vertex::new(id),
            })
            .collect::<Vec<_>>();
        for id in 0..count {
            // Intentionally non-monotonic times and labels in edge-ID order.
            let start = ((id * 37) % 97) as i64 - 50;
            let valid = match id {
                0 => Interval {
                    from: i64::MIN,
                    to: Some(i64::MAX),
                },
                1 => Interval {
                    from: i64::MAX,
                    to: None,
                },
                2 => Interval {
                    from: -10,
                    to: None,
                },
                _ => Interval {
                    from: start,
                    to: Some(start + 5),
                },
            };
            let from = 1;
            let to = if id % 11 == 0 { from } else { 2 };
            mutations.push(TemporalMutation::Edge {
                valid,
                edge: Edge::new(1000 + id, from, to, if id % 2 == 0 { "A" } else { "B" }),
            });
        }
        mutations.push(TemporalMutation::Edge {
            valid: Interval {
                from: i64::MIN,
                to: None,
            },
            edge: Edge::new(2000, 3, 2, "A"),
        });
        store.commit(&mutations).unwrap();
        store
            .commit(&[TemporalMutation::Absent {
                valid: Interval {
                    from: 0,
                    to: Some(10),
                },
                key: Key::Vertex(2),
            }])
            .unwrap();
        drop(store);
        let store = TemporalStore::open(dir.path()).unwrap();
        for version in [1, 2] {
            let model = store
                .materialize(Some(version), MaterializationLimits::default())
                .unwrap();
            for time in [
                i64::MIN,
                -51,
                -50,
                -10,
                -1,
                0,
                9,
                10,
                49,
                100,
                i64::MAX - 1,
                i64::MAX,
            ] {
                for text in [
                    "g.V(1,3,1).out().path()",
                    "g.V(2).in().path()",
                    "g.V(1,2,3).both('A').path()",
                    "g.V(2).in('B').count()",
                ] {
                    let query = GremlinQuery::parse(text).unwrap();
                    let expected = store
                        .view(Some(version), time)
                        .unwrap()
                        .execute_query(&query)
                        .unwrap();
                    let actual = model.view(time).execute_query(&query).unwrap();
                    assert_eq!(
                        actual, expected,
                        "count={count}, V={version}, T={time}, {text}"
                    );
                }
            }
        }
    }
}

#[cfg(feature = "query-profiling")]
#[test]
fn block_threshold_and_partial_tail_are_exercised() {
    for count in [63, 64, 65] {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        future_edges(&store, count);
        let model = store
            .materialize(None, MaterializationLimits::default())
            .unwrap();
        let q = GremlinQuery::parse("g.V(1).out().count()").unwrap();
        for (time, want) in [(99, 0), (100, count), (199, count), (200, 0)] {
            velagraph::query_profile::begin();
            let rows = model.view(time).execute_query(&q).unwrap().rows;
            let profile = velagraph::query_profile::finish();
            assert_eq!(rows, vec![GremlinRow::Count(want)]);
            assert_eq!(
                profile.materialized_blocks,
                if count < 64 { 0 } else { count.div_ceil(32) }
            );
            let skipped = if count >= 64 && want == 0 { count } else { 0 };
            assert_eq!(profile.materialized_skipped_candidates, skipped);
            assert_eq!(profile.materialized_candidates, count - skipped);
        }
    }
}
