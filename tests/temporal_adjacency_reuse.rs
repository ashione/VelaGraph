#![cfg(feature = "query-profiling")]
use velagraph::{
    query_profile,
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, GremlinQuery, GremlinRow, Vertex,
};

fn fixture(path: &std::path::Path) -> TemporalStore {
    let store = TemporalStore::open(path).unwrap();
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
                valid,
                vertex: Vertex::new(3),
            },
            TemporalMutation::Edge {
                valid,
                edge: Edge::new(7, 1, 3, "LINK"),
            },
            TemporalMutation::Edge {
                valid,
                edge: Edge::new(8, 2, 3, "LINK"),
            },
        ])
        .unwrap();
    store
}

#[test]
fn adjacency_reuses_the_head_validated_at_view_creation() {
    let dir = tempfile::tempdir().unwrap();
    let store = fixture(dir.path());
    let query = GremlinQuery::parse("g.V(1,2).out('LINK').count()").unwrap();
    query_profile::begin();
    let view = store.view(Some(1), 0).unwrap();
    assert_eq!(
        view.execute_query(&query).unwrap().rows,
        vec![GremlinRow::Count(2)]
    );
    let stats = query_profile::finish();
    assert_eq!(stats.head_reads, 1);
    query_profile::begin();
    assert_eq!(store.view(None, 9).unwrap().version(), 1);
    assert_eq!(query_profile::finish().head_reads, 0);
    store.commit(&[]).unwrap();
    query_profile::begin();
    assert_eq!(store.view(Some(1), 9).unwrap().version(), 1);
    assert_eq!(query_profile::finish().head_reads, 1);
}

#[test]
fn adjacency_cursor_reseeks_across_empty_and_nonempty_roots() {
    let dir = tempfile::tempdir().unwrap();
    let store = fixture(dir.path());
    let query = GremlinQuery::parse("g.V(3,1,2,3).out('LINK').count()").unwrap();
    let view = store.view(Some(1), 0).unwrap();
    query_profile::begin();
    assert_eq!(
        view.execute_query(&query).unwrap().rows,
        vec![GremlinRow::Count(2)]
    );
    let stats = query_profile::finish();
    assert_eq!(stats.adjacency_candidates, 2);
    assert_eq!(stats.adjacency_iterators, 1);
    assert_eq!(stats.adjacency_seeks, 2);
}

#[test]
fn an_old_selected_version_can_use_a_newer_snapshot_head() {
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
        ])
        .unwrap();
    store
        .commit(&[TemporalMutation::Edge {
            valid,
            edge: Edge::new(7, 1, 2, "LINK"),
        }])
        .unwrap();
    for (version, count) in [(1, 0), (2, 1)] {
        assert_eq!(
            store
                .view(Some(version), 0)
                .unwrap()
                .execute_gremlin("g.V(1).out('LINK').count()")
                .unwrap()
                .rows,
            vec![GremlinRow::Count(count)]
        );
    }
}

#[test]
fn ordered_and_repeated_lists_match_individual_reads_across_a_move() {
    let dir = tempfile::tempdir().unwrap();
    let store = fixture(dir.path());
    store
        .commit(&[TemporalMutation::Edge {
            valid: Interval {
                from: 5,
                to: Some(10),
            },
            edge: Edge::new(7, 3, 2, "LINK"),
        }])
        .unwrap();
    drop(store);
    let store = TemporalStore::open(dir.path()).unwrap();
    for (version, time) in [(1, 6), (2, 4), (2, 5), (2, 9), (2, 10)] {
        let view = store.view(Some(version), time).unwrap();
        for direction in ["out", "in", "both"] {
            for roots in [[1, 2, 3, 3], [1, 3, 2, 1], [3, 2, 1, 2]] {
                let mut expected = Vec::new();
                for root in roots {
                    expected.extend(
                        view.execute_gremlin(&format!("g.V({root}).{direction}('LINK').id()"))
                            .unwrap()
                            .rows,
                    );
                }
                let ids = roots.map(|id| id.to_string()).join(",");
                let actual = view
                    .execute_gremlin(&format!("g.V({ids}).{direction}('LINK').id()"))
                    .unwrap()
                    .rows;
                assert_eq!(actual, expected, "{version}/{time} {direction}({ids})");
            }
        }
    }
}

#[test]
fn ordered_sparse_lists_only_seek_once() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let valid = Interval { from: 0, to: None };
    let mut changes: Vec<_> = (1..=49)
        .map(|id| TemporalMutation::Vertex {
            valid,
            vertex: Vertex::new(id),
        })
        .collect();
    for id in (1..=48).step_by(4) {
        changes.push(TemporalMutation::Edge {
            valid,
            edge: Edge::new(id, id, 49, "LINK"),
        });
    }
    store.commit(&changes).unwrap();
    let view = store.view(None, 0).unwrap();
    let ids = (1..=48)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let query = GremlinQuery::parse(&format!("g.V({ids}).out('LINK').count()")).unwrap();
    query_profile::begin();
    assert_eq!(
        view.execute_query(&query).unwrap().rows,
        [GremlinRow::Count(12)]
    );
    let stats = query_profile::finish();
    assert_eq!(stats.adjacency_seeks, 1);
    assert_eq!(stats.adjacency_candidates, 12);
}
