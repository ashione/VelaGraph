#![cfg(feature = "query-profiling")]
use velagraph::{
    query_profile,
    temporal_reference::{Interval, Key},
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, GremlinRow, Value, Vertex,
};

fn vertex(id: u64) -> TemporalMutation {
    TemporalMutation::Vertex {
        valid: Interval { from: 0, to: None },
        vertex: Vertex::new(id),
    }
}
#[test]
fn reuses_edge_and_endpoint_records_but_preserves_duplicate_paths() {
    let t = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(t.path()).unwrap();
    s.commit(&[
        vertex(1),
        vertex(2),
        TemporalMutation::Edge {
            valid: Interval { from: 0, to: None },
            edge: Edge::new(7, 1, 2, "p"),
        },
    ])
    .unwrap();
    query_profile::begin();
    let rows = s
        .view(Some(1), 0)
        .unwrap()
        .execute_gremlin("g.V(1,1).out('p').path()")
        .unwrap()
        .rows;
    let p = query_profile::finish();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], rows[1]);
    assert_eq!(p.record_reads, 3);
    assert_eq!(p.record_decodes, 3);
    assert!(p.record_cache_hits >= 5);
}
#[test]
fn negative_entries_are_scoped_to_one_execution_and_view() {
    let t = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(t.path()).unwrap();
    query_profile::begin();
    assert_eq!(
        s.view(Some(0), 0)
            .unwrap()
            .execute_gremlin("g.V(1,1).count()")
            .unwrap()
            .rows,
        vec![GremlinRow::Count(0)]
    );
    let p = query_profile::finish();
    assert_eq!(p.record_reads, 1);
    assert_eq!(p.record_cache_hits, 1);
    let v1 = s.commit(&[vertex(1)]).unwrap();
    assert_eq!(
        s.view(Some(v1), 0)
            .unwrap()
            .execute_gremlin("g.V(1).count()")
            .unwrap()
            .rows,
        vec![GremlinRow::Count(1)]
    );
    let v2 = s
        .commit(&[TemporalMutation::Absent {
            valid: Interval { from: 10, to: None },
            key: Key::Vertex(1),
        }])
        .unwrap();
    for (v, time, count) in [(v1, 11, 1), (v2, 9, 1), (v2, 11, 0)] {
        assert_eq!(
            s.view(Some(v), time)
                .unwrap()
                .execute_gremlin("g.V(1,1).count()")
                .unwrap()
                .rows,
            vec![GremlinRow::Count(count * 2)]
        );
    }
}
#[test]
fn repeated_output_payloads_still_hit_memory_limit() {
    let t = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(t.path()).unwrap();
    let mut v = Vertex::new(1);
    v.properties
        .insert("large".into(), Value::Text("x".repeat(8 * 1024 * 1024)));
    s.commit(&[TemporalMutation::Vertex {
        valid: Interval { from: 0, to: None },
        vertex: v,
    }])
    .unwrap();
    let result = s
        .view(Some(1), 0)
        .unwrap()
        .execute_gremlin("g.V(1,1,1,1,1,1,1,1,1).count()");
    assert!(matches!(
        result,
        Err(velagraph::Error::TraversalLimitExceeded(_))
    ));
}

#[test]
fn cached_payload_copies_cannot_amplify_past_memory_limit() {
    let t = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(t.path()).unwrap();
    let valid = Interval { from: 0, to: None };
    let mut target = Vertex::new(2);
    target
        .properties
        .insert("payload".into(), Value::Text("x".repeat(128 * 1024)));
    let mut mutations = vec![
        vertex(1),
        TemporalMutation::Vertex {
            valid,
            vertex: target,
        },
    ];
    mutations.extend((1..=600).map(|id| TemporalMutation::Edge {
        valid,
        edge: Edge::new(id, 1, 2, "p"),
    }));
    for batch in mutations.chunks(256) {
        s.commit(batch).unwrap();
    }
    query_profile::begin();
    let result = s
        .view(None, 0)
        .unwrap()
        .execute_gremlin("g.V(1).out('p').count()");
    let stats = query_profile::finish();
    assert!(stats.record_cache_hits > 0, "must exercise cached copies");
    assert!(matches!(
        result,
        Err(velagraph::Error::TraversalLimitExceeded(_))
    ));
}
