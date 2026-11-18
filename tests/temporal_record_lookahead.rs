#![cfg(feature = "query-profiling")]
use velagraph::{
    query_profile,
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    GremlinQuery, GremlinRow, Value, Vertex,
};

fn vertex(id: u64, valid: Interval, n: i64) -> TemporalMutation {
    let mut vertex = Vertex::new(id);
    vertex.properties.insert("n".into(), Value::Integer(n));
    TemporalMutation::Vertex { valid, vertex }
}

#[test]
fn an_expired_identity_can_supply_the_next_identity_without_another_seek() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(
                1,
                Interval {
                    from: 0,
                    to: Some(1),
                },
                1,
            ),
            vertex(2, Interval { from: 0, to: None }, 2),
        ])
        .unwrap();
    let query = GremlinQuery::parse("g.V(1,2,2).values('n')").unwrap();
    query_profile::begin();
    assert_eq!(
        store
            .view(None, 2)
            .unwrap()
            .execute_query(&query)
            .unwrap()
            .rows,
        vec![
            GremlinRow::Value(Value::Integer(2)),
            GremlinRow::Value(Value::Integer(2))
        ]
    );
    let stats = query_profile::finish();
    assert_eq!(stats.record_reads, 2);
    assert_eq!(stats.record_seeks, 1);
    assert_eq!(stats.record_lookahead_hits, 1);
}

#[test]
fn a_missing_identity_can_supply_lookahead_without_changing_order() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(3, Interval { from: 0, to: None }, 3),
            vertex(4, Interval { from: 0, to: None }, 4),
        ])
        .unwrap();
    query_profile::begin();
    assert_eq!(
        store
            .view(None, 0)
            .unwrap()
            .execute_gremlin("g.V(2,3,4).values('n')")
            .unwrap()
            .rows,
        vec![
            GremlinRow::Value(Value::Integer(3)),
            GremlinRow::Value(Value::Integer(4))
        ]
    );
    let stats = query_profile::finish();
    assert_eq!(stats.record_reads, 3);
    assert_eq!(stats.record_seeks, 2);
    assert_eq!(stats.record_lookahead_hits, 1);
}

#[test]
fn lookahead_must_not_read_a_future_revision_for_an_old_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(
                1,
                Interval {
                    from: 0,
                    to: Some(1),
                },
                1,
            ),
            vertex(2, Interval { from: 0, to: None }, 20),
        ])
        .unwrap();
    store
        .commit(&[vertex(2, Interval { from: 0, to: None }, 200)])
        .unwrap();
    let query = GremlinQuery::parse("g.V(1,2).values('n')").unwrap();
    for (version, value, seeks) in [(1, 20, 2), (2, 200, 1)] {
        query_profile::begin();
        assert_eq!(
            store
                .view(Some(version), 2)
                .unwrap()
                .execute_query(&query)
                .unwrap()
                .rows,
            vec![GremlinRow::Value(Value::Integer(value))]
        );
        assert_eq!(query_profile::finish().record_seeks, seeks);
    }
}

#[test]
fn lookahead_still_walks_multiple_ordinals_at_the_same_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(
                1,
                Interval {
                    from: 0,
                    to: Some(1),
                },
                1,
            ),
            vertex(
                2,
                Interval {
                    from: 0,
                    to: Some(1),
                },
                20,
            ),
            vertex(2, Interval { from: 1, to: None }, 21),
        ])
        .unwrap();
    query_profile::begin();
    assert_eq!(
        store
            .view(None, 2)
            .unwrap()
            .execute_gremlin("g.V(1,2).values('n')")
            .unwrap()
            .rows,
        vec![GremlinRow::Value(Value::Integer(21))]
    );
    let stats = query_profile::finish();
    assert_eq!(stats.record_seeks, 1);
    assert_eq!(stats.record_lookahead_hits, 1);
}

#[test]
fn nonmatching_and_descending_ids_seek_but_memo_hits_leave_lookahead_intact() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(3, Interval { from: 0, to: None }, 3),
            vertex(4, Interval { from: 0, to: None }, 4),
        ])
        .unwrap();
    let view = store.view(None, 0).unwrap();
    for (text, values, seeks, hits) in [
        ("g.V(2,4,3).values('n')", vec![4, 3], 3, 0),
        ("g.V(4,2,4,3).values('n')", vec![4, 4, 3], 2, 1),
    ] {
        query_profile::begin();
        assert_eq!(
            view.execute_gremlin(text).unwrap().rows,
            values
                .into_iter()
                .map(|v| GremlinRow::Value(Value::Integer(v)))
                .collect::<Vec<_>>()
        );
        let stats = query_profile::finish();
        assert_eq!(stats.record_seeks, seeks);
        assert_eq!(stats.record_lookahead_hits, hits);
    }
}
