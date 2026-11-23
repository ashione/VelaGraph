use velagraph::{
    temporal_reference::{Interval, Key},
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, GremlinRow, Value, Vertex,
};

fn vertex(id: u64, from: i64, to: Option<i64>, value: i64) -> TemporalMutation {
    let mut vertex = Vertex::new(id);
    vertex
        .properties
        .insert("value".into(), Value::Integer(value));
    TemporalMutation::Vertex {
        valid: Interval { from, to },
        vertex,
    }
}
fn value(store: &TemporalStore, version: u64, time: i64, id: u64) -> Option<Value> {
    store
        .view(Some(version), time)
        .unwrap()
        .get_vertex(id)
        .unwrap()
        .map(|v| v.properties["value"].clone())
}

#[test]
fn warm_reads_preserve_versions_intervals_deletion_endpoints_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(1, i64::MIN, None, 10),
            vertex(2, 0, None, 20),
            TemporalMutation::Edge {
                valid: Interval { from: 0, to: None },
                edge: Edge::new(1, 1, 2, "link"),
            },
        ])
        .unwrap();
    let old = store.view(Some(1), 15).unwrap();
    for _ in 0..2 {
        assert!(old.get_edge(1).unwrap().is_some());
    }
    store.commit(&[vertex(1, 10, Some(20), 30)]).unwrap();
    store
        .commit(&[TemporalMutation::Absent {
            key: Key::Vertex(2),
            valid: Interval {
                from: 12,
                to: Some(18),
            },
        }])
        .unwrap();
    store.commit(&[vertex(2, 14, Some(16), 40)]).unwrap();
    for _ in 0..2 {
        for time in [i64::MIN, -1, 0, 9, 10, 12, 14, 15, 16, 18, 19, 20, i64::MAX] {
            let view = store.view(Some(4), time).unwrap();
            assert_eq!(
                value(&store, 4, time, 1),
                Some(Value::Integer(if (10..20).contains(&time) {
                    30
                } else {
                    10
                }))
            );
            let visible = time >= 0 && (!(12..18).contains(&time) || (14..16).contains(&time));
            assert_eq!(view.get_edge(1).unwrap().is_some(), visible);
            assert_eq!(
                view.execute_gremlin("g.V(1).out('link').count()")
                    .unwrap()
                    .rows,
                vec![GremlinRow::Count(u64::from(visible))]
            );
        }
    }
    assert!(old.get_edge(1).unwrap().is_some());
    drop(old);
    drop(store);
    let reopened = TemporalStore::open(dir.path()).unwrap();
    assert_eq!(value(&reopened, 1, 15, 1), Some(Value::Integer(10)));
    assert_eq!(value(&reopened, 4, 15, 1), Some(Value::Integer(30)));
    let other_dir = tempfile::tempdir().unwrap();
    let other = TemporalStore::open(other_dir.path()).unwrap();
    other.commit(&[vertex(1, 0, None, 99)]).unwrap();
    assert_eq!(value(&other, 1, 15, 1), Some(Value::Integer(99)));
}

#[cfg(feature = "query-profiling")]
#[test]
fn unchanged_identity_reuses_revision_block_without_storage_seek() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(
            &(0..100)
                .map(|i| vertex(1, i * 10, Some(i * 10 + 10), i))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let query = velagraph::GremlinQuery::parse("g.V(1).values('value')").unwrap();
    // The latest valid-time interval is last in ordinal order: warm all rows.
    store
        .view(Some(1), 995)
        .unwrap()
        .execute_query(&query)
        .unwrap();
    store.commit(&[vertex(2, 0, None, 1)]).unwrap();
    velagraph::query_profile::begin();
    store
        .view(Some(2), 555)
        .unwrap()
        .execute_query(&query)
        .unwrap();
    let profile = velagraph::query_profile::finish();
    assert_eq!(
        profile.record_seeks, 0,
        "unchanged prefix must be retained across a commit"
    );
    assert_eq!(profile.record_iterators, 0);
}

#[test]
fn warm_blocks_preserve_tight_work_memory_and_cancellation_limits() {
    use std::time::Duration;
    use velagraph::{CancellationToken, ExecutionControl, GremlinQuery, QueryBatchLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let mut changes: Vec<_> = (0..32)
        .map(|i| vertex(1, i * 10, Some(i * 10 + 10), i))
        .collect();
    if let TemporalMutation::Vertex { vertex, .. } = &mut changes[0] {
        vertex
            .properties
            .insert("large".into(), Value::Text("x".repeat(8192)));
    }
    store.commit(&changes).unwrap();
    let query = GremlinQuery::parse("g.V(1).values('value')").unwrap();
    let view = store.view(None, 315).unwrap();
    let control =
        ExecutionControl::new(CancellationToken::default(), Duration::from_secs(60)).unwrap();
    // Verify identical failures before and after populating the entire prefix.
    let mut errors = Vec::new();
    for _ in 0..2 {
        let mut pass_errors = Vec::new();
        for limits in [
            QueryBatchLimits {
                max_work_operations: 31,
                ..Default::default()
            },
            QueryBatchLimits {
                max_work_bytes: 8000,
                ..Default::default()
            },
            QueryBatchLimits {
                max_live_bytes: 8000,
                ..Default::default()
            },
            QueryBatchLimits {
                max_live_bytes: 8000,
                max_work_operations: 2,
                ..Default::default()
            },
        ] {
            let error = view
                .execute_query_batch(std::slice::from_ref(&query), &control, limits)
                .unwrap_err();
            pass_errors.push(std::mem::discriminant(&error));
        }
        errors.push(pass_errors);
        assert!(view.execute_query(&query).is_ok());
    }
    assert_eq!(errors[0], errors[1]);
    let token = CancellationToken::default();
    let cancelled = ExecutionControl::new(token.clone(), Duration::from_secs(60)).unwrap();
    token.cancel();
    assert!(matches!(
        view.execute_query_batch(&[query], &cancelled, QueryBatchLimits::default()),
        Err(velagraph::Error::ExecutionCancelled)
    ));
}

#[test]
fn oversized_history_and_payload_fall_back_without_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    for start in (0..2304).step_by(256) {
        store
            .commit(
                &(start..start + 256)
                    .map(|i| vertex(1, i * 10, Some(i * 10 + 10), i))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
    }
    let mut big = Vertex::new(2);
    big.properties
        .insert("value".into(), Value::Text("x".repeat(300 * 1024)));
    store
        .commit(&[TemporalMutation::Vertex {
            valid: Interval { from: 0, to: None },
            vertex: big,
        }])
        .unwrap();
    for _ in 0..2 {
        assert_eq!(value(&store, 10, 5, 1), Some(Value::Integer(0)));
        assert_eq!(value(&store, 10, 23035, 1), Some(Value::Integer(2303)));
        assert_eq!(value(&store, 10, -1, 1), None);
        assert_eq!(
            value(&store, 10, 1, 2),
            Some(Value::Text("x".repeat(300 * 1024)))
        );
    }
}
