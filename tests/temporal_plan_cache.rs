#![cfg(feature = "query-profiling")]
use std::sync::Arc;
use velagraph::{
    query_profile,
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    GremlinRow, Value, Vertex,
};

fn vertex(value: i64, from: i64) -> TemporalMutation {
    let mut v = Vertex::new(1);
    v.properties.insert("n".into(), Value::Integer(value));
    TemporalMutation::Vertex {
        valid: Interval { from, to: None },
        vertex: v,
    }
}
#[test]
fn reuses_only_plan_across_versions_and_business_times() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let v1 = store.commit(&[vertex(1, 0)]).unwrap();
    let v2 = store.commit(&[vertex(2, 10)]).unwrap();
    query_profile::begin();
    for (version, time, want) in [(v1, 11, 1), (v2, 9, 1), (v2, 11, 2)] {
        let result = store
            .view(Some(version), time)
            .unwrap()
            .execute_gremlin("g.V(1).values('n')")
            .unwrap();
        assert_eq!(result.rows, vec![GremlinRow::Value(Value::Integer(want))]);
    }
    let p = query_profile::finish();
    assert_eq!(p.parse_calls, 1);
    assert_eq!(p.cache_hits, 2);
}
#[test]
fn retains_global_start_rejection_and_does_not_cache_parse_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let view = store.view(Some(0), 0).unwrap();
    query_profile::begin();
    for _ in 0..2 {
        assert!(view.execute_gremlin("g.V().count()").is_err());
    }
    let p = query_profile::finish();
    assert_eq!(p.parse_calls, 1);
    assert_eq!(p.cache_hits, 1);
    assert_eq!(p.execute_ns, 0);
    query_profile::begin();
    for _ in 0..2 {
        assert!(view.execute_gremlin("not Gremlin").is_err());
    }
    let p = query_profile::finish();
    assert_eq!(p.parse_calls, 2);
    assert_eq!(p.cache_hits, 0);
}
#[test]
fn bounded_cache_evicts_least_recently_used_plan() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let view = store.view(Some(0), 0).unwrap();
    query_profile::begin();
    for id in 1..=1024 {
        view.execute_gremlin(&format!("g.V({id}).path()")).unwrap();
    }
    view.execute_gremlin("g.V(1).path()").unwrap();
    view.execute_gremlin("g.V(1025).path()").unwrap();
    view.execute_gremlin("g.V(1).path()").unwrap();
    view.execute_gremlin("g.V(2).path()").unwrap();
    let p = query_profile::finish();
    assert_eq!(p.parse_calls, 1026);
    assert_eq!(p.cache_hits, 2);
}
#[test]
fn oversized_cache_key_still_executes_normally() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let value = "x".repeat(4096);
    let mut node = Vertex::new(1);
    node.properties
        .insert("text".into(), Value::Text(value.clone()));
    store
        .commit(&[TemporalMutation::Vertex {
            valid: Interval { from: 0, to: None },
            vertex: node,
        }])
        .unwrap();
    let view = store.view(Some(1), 0).unwrap();
    let text = format!("g.V(1).has('text','{value}').path()");
    query_profile::begin();
    for _ in 0..2 {
        assert_eq!(view.execute_gremlin(&text).unwrap().rows.len(), 1);
    }
    let p = query_profile::finish();
    assert_eq!(p.parse_calls, 2);
    assert_eq!(p.cache_hits, 0);
}
#[test]
fn cache_is_store_scoped_and_each_execution_gets_fresh_budget() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let s1 = TemporalStore::open(d1.path()).unwrap();
    let s2 = TemporalStore::open(d2.path()).unwrap();
    s1.commit(&[vertex(1, 0)]).unwrap();
    s2.commit(&[vertex(2, 0)]).unwrap();
    query_profile::begin();
    for (s, want) in [(&s1, 1), (&s2, 2)] {
        for _ in 0..4100 {
            assert_eq!(
                s.view(Some(1), 0)
                    .unwrap()
                    .execute_gremlin("g.V(1).values('n')")
                    .unwrap()
                    .rows,
                vec![GremlinRow::Value(Value::Integer(want))]
            );
        }
    }
    let p = query_profile::finish();
    assert_eq!(p.parse_calls, 2);
    assert_eq!(p.cache_hits, 8198);
}
#[test]
fn shared_store_supports_concurrent_plan_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(TemporalStore::open(dir.path()).unwrap());
    store.commit(&[vertex(7, 0)]).unwrap();
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let s = store.clone();
            std::thread::spawn(move || {
                for _ in 0..50 {
                    assert_eq!(
                        s.view(Some(1), 0)
                            .unwrap()
                            .execute_gremlin("g.V(1).values('n')")
                            .unwrap()
                            .rows,
                        vec![GremlinRow::Value(Value::Integer(7))]
                    );
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
}
#[test]
fn aggregate_text_budget_evicts_before_entry_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let view = store.view(Some(0), 0).unwrap();
    let value = "x".repeat(3500);
    query_profile::begin();
    for id in 1..=305 {
        view.execute_gremlin(&format!("g.V({id}).has('n','{value}').path()"))
            .unwrap();
    }
    view.execute_gremlin(&format!("g.V(1).has('n','{value}').path()"))
        .unwrap();
    let p = query_profile::finish();
    assert_eq!(p.parse_calls, 306);
    assert_eq!(p.cache_hits, 0);
}
