use velagraph::{
    temporal_reference::Interval,
    temporal_store::{
        materialized::{MaterializationLimits, MaterializedQueryLimits},
        TemporalMutation, TemporalStore,
    },
    Edge, GremlinQuery, Value, Vertex,
};
fn vertex(id: u64) -> TemporalMutation {
    TemporalMutation::Vertex {
        valid: Interval {
            from: i64::MIN,
            to: None,
        },
        vertex: Vertex::new(id),
    }
}
#[test]
fn owned_model_reuses_fixed_v_across_times_and_source_close() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(1),
            vertex(2),
            TemporalMutation::Edge {
                valid: Interval {
                    from: 0,
                    to: Some(10),
                },
                edge: Edge::new(3, 1, 2, "LINK"),
            },
        ])
        .unwrap();
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    store
        .commit(&[TemporalMutation::Absent {
            valid: Interval { from: 0, to: None },
            key: velagraph::temporal_reference::Key::Vertex(2),
        }])
        .unwrap();
    assert_eq!(model.version(), 1);
    assert_eq!(
        model
            .view(5)
            .execute_gremlin("g.V(1).out().count()")
            .unwrap()
            .rows,
        vec![velagraph::GremlinRow::Count(1)]
    );
    assert_eq!(
        model
            .view(10)
            .execute_gremlin("g.V(1).out().count()")
            .unwrap()
            .rows,
        vec![velagraph::GremlinRow::Count(0)]
    );
    drop(store);
    assert_eq!(
        model
            .view(5)
            .execute_gremlin("g.V(1).out().id()")
            .unwrap()
            .rows,
        vec![velagraph::GremlinRow::Value(Value::Integer(2))]
    );
}
#[test]
fn limits_are_bounded_and_global_starts_are_not_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store.commit(&[vertex(1), vertex(2)]).unwrap();
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_source_rows: 1,
                ..Default::default()
            }
        )
        .is_err());
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    assert!(model.view(0).execute_gremlin("g.V()").is_err());
    let q = GremlinQuery::parse("g.V(1,2).count()").unwrap();
    assert!(model
        .view(0)
        .execute_query_with_limits(
            &q,
            MaterializedQueryLimits {
                max_operations: 1,
                ..Default::default()
            }
        )
        .is_err());
    assert!(model
        .view(0)
        .execute_query_with_limits(
            &q,
            MaterializedQueryLimits {
                max_work_bytes: 1,
                ..Default::default()
            }
        )
        .is_err());
}
#[test]
fn models_are_shareable_but_local_versions_do_not_identify_other_stores() {
    use std::sync::Arc;
    use velagraph::temporal_store::materialized::{TemporalReadModel, TemporalReadModelView};
    fn send_sync<T: Send + Sync>() {}
    send_sync::<TemporalReadModel>();
    send_sync::<TemporalReadModelView<'static>>();
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let first_store = TemporalStore::open(first_dir.path()).unwrap();
    let second_store = TemporalStore::open(second_dir.path()).unwrap();
    for (store, value) in [(&first_store, "first"), (&second_store, "second")] {
        let mut v = Vertex::new(1);
        v.properties
            .insert("name".into(), Value::Text(value.into()));
        store
            .commit(&[TemporalMutation::Vertex {
                valid: Interval { from: 0, to: None },
                vertex: v,
            }])
            .unwrap();
    }
    let first = Arc::new(
        first_store
            .materialize(None, MaterializationLimits::default())
            .unwrap(),
    );
    let second = second_store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    assert_eq!(first.version(), second.version());
    drop(first_store);
    drop(second_store);
    let query = Arc::new(GremlinQuery::parse("g.V(1).values('name')").unwrap());
    let threads: Vec<_> = (0..4)
        .map(|time| {
            let model = Arc::clone(&first);
            let query = Arc::clone(&query);
            std::thread::spawn(move || model.view(time).execute_query(&query).unwrap().rows)
        })
        .collect();
    for thread in threads {
        assert_eq!(
            thread.join().unwrap(),
            vec![velagraph::GremlinRow::Value(Value::Text("first".into()))]
        );
    }
    assert_eq!(
        second.view(0).execute_query(&query).unwrap().rows,
        vec![velagraph::GremlinRow::Value(Value::Text("second".into()))]
    );
}
