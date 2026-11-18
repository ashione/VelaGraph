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
