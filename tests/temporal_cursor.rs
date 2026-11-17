#![cfg(feature = "query-profiling")]
use velagraph::{
    query_profile,
    temporal_reference::{Interval, Key},
    temporal_store::{TemporalMutation, TemporalStore},
    GremlinRow, Value, Vertex,
};

#[test]
fn batched_ids_share_one_cursor_and_preserve_order_duplicates_and_time() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let mutations: Vec<_> = (1..=3)
        .map(|id| TemporalMutation::Vertex {
            valid: Interval { from: 0, to: None },
            vertex: Vertex::new(id),
        })
        .collect();
    store.commit(&mutations).unwrap();
    store
        .commit(&[TemporalMutation::Absent {
            valid: Interval {
                from: 10,
                to: Some(20),
            },
            key: Key::Vertex(3),
        }])
        .unwrap();
    for (version, time, want) in [
        (1, 10, vec![3, 1, 3, 2]),
        (2, 9, vec![3, 1, 3, 2]),
        (2, 10, vec![1, 2]),
        (2, 20, vec![3, 1, 3, 2]),
    ] {
        query_profile::begin();
        let result = store
            .view(Some(version), time)
            .unwrap()
            .execute_gremlin("g.V(3,1,3,999,2).id()")
            .unwrap();
        let stats = query_profile::finish();
        assert_eq!(
            result.rows,
            want.into_iter()
                .map(|id| GremlinRow::Value(Value::Integer(id)))
                .collect::<Vec<_>>()
        );
        assert_eq!(stats.record_reads, 4);
        assert_eq!(stats.record_iterators, 1);
    }
}
#[test]
fn leading_limit_does_not_resolve_later_roots() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[TemporalMutation::Vertex {
            valid: Interval { from: 0, to: None },
            vertex: Vertex::new(1),
        }])
        .unwrap();
    query_profile::begin();
    let result = store
        .view(Some(1), 0)
        .unwrap()
        .execute_gremlin("g.V(1,999,1).limit(1).id()")
        .unwrap();
    let stats = query_profile::finish();
    assert_eq!(result.rows, vec![GremlinRow::Value(Value::Integer(1))]);
    assert_eq!(stats.record_reads, 1);
    assert_eq!(stats.record_iterators, 1);
}
