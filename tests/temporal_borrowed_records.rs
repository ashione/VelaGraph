use velagraph::{
    temporal_reference::{Interval, Key},
    temporal_store::{materialized::MaterializationLimits, TemporalMutation, TemporalStore},
    Edge, GremlinQuery, GremlinRow, Value, Vertex,
};

fn fixture(store: &TemporalStore) {
    let valid = Interval { from: 0, to: None };
    let mut mutations = Vec::new();
    for id in 1..=4 {
        let mut vertex = Vertex::new(id);
        vertex.labels = vec!["Node".into()];
        vertex
            .properties
            .insert("name".into(), Value::Text(format!("node-{id}")));
        vertex
            .properties
            .insert("unused".into(), Value::Text("x".repeat(8192)));
        vertex.properties.insert("null".into(), Value::Null);
        mutations.push(TemporalMutation::Vertex { valid, vertex });
    }
    for (id, from, to, label) in [
        (10, 1, 2, "LINK"),
        (11, 1, 2, "LINK"),
        (12, 3, 2, "ALT"),
        (13, 1, 1, "LOOP"),
        (14, 2, 4, "LINK"),
    ] {
        let mut edge = Edge::new(id, from, to, label);
        edge.properties
            .insert("name".into(), Value::Text(format!("edge-{id}")));
        edge.properties
            .insert("unused".into(), Value::Text("y".repeat(8192)));
        mutations.push(TemporalMutation::Edge { valid, edge });
    }
    store.commit(&mutations).unwrap();
    store
        .commit(&[TemporalMutation::Absent {
            valid: Interval {
                from: 10,
                to: Some(20),
            },
            key: Key::Vertex(2),
        }])
        .unwrap();
}

#[test]
fn borrowed_records_preserve_temporal_bags_properties_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    fixture(&store);
    let queries = [
        "g.V(1,1,99).both().path()",
        "g.V(2).in().id()",
        "g.V(1).out('LINK').count()",
        "g.V(1).both().limit(2).id()",
        "g.V(1).out().hasLabel('Node').values('name')",
        "g.V(1).out().has('name','node-2').values('null')",
        "g.V(1).out().values('missing')",
        "g.V(1).both().label()",
        "g.V(1).out().out().path()",
        "g.E(10,12,10,99).values('name')",
        "g.E(10,12).label()",
        "g.E(10).has('name','edge-10').id()",
        "g.E(10).out()",
        "g.V(1).id().out()",
    ];
    for version in [1, 2] {
        let model = store
            .materialize(Some(version), MaterializationLimits::default())
            .unwrap();
        for time in [-1, 0, 9, 10, 19, 20] {
            let disk = store.view(Some(version), time).unwrap();
            for text in queries {
                let query = GremlinQuery::parse(text).unwrap();
                let expected = disk.execute_query(&query);
                let actual = model.view(time).execute_query(&query);
                match (expected, actual) {
                    (Ok(a), Ok(b)) => assert_eq!(a, b, "{version}/{time}: {text}"),
                    (Err(a), Err(b)) => assert_eq!(
                        std::mem::discriminant(&a),
                        std::mem::discriminant(&b),
                        "{text}"
                    ),
                    (a, b) => panic!("{version}/{time}: {text}: {a:?} != {b:?}"),
                }
            }
        }
    }
}

#[test]
fn selected_text_results_outlive_borrowed_model() {
    let results = {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        fixture(&store);
        let model = store
            .materialize(None, MaterializationLimits::default())
            .unwrap();
        [
            "g.V(1).out('LINK').values('name')",
            "g.E(10).values('name')",
            "g.E(10).label()",
        ]
        .map(|q| model.view(0).execute_gremlin(q).unwrap().rows)
    };
    assert_eq!(
        results[0],
        vec![GremlinRow::Value(Value::Text("node-2".into())); 2]
    );
    assert_eq!(
        results[1],
        vec![GremlinRow::Value(Value::Text("edge-10".into()))]
    );
    assert_eq!(
        results[2],
        vec![GremlinRow::Value(Value::Text("LINK".into()))]
    );
}

#[cfg(feature = "query-profiling")]
#[test]
fn immutable_query_avoids_whole_record_copies() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    fixture(&store);
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    for text in [
        "g.V(1).both().path()",
        "g.V(2).in().count()",
        "g.V(1).out().has('name').values('name')",
        "g.E(10).label()",
    ] {
        let q = GremlinQuery::parse(text).unwrap();
        velagraph::query_profile::begin();
        let result = model.view(0).execute_query(&q).unwrap();
        let profile = velagraph::query_profile::finish();
        assert!(!result.rows.is_empty());
        assert!(profile.materialized_vertex_borrows + profile.materialized_edge_borrows > 0);
        assert!(profile.materialized_edge_borrows > 0);
        if text.starts_with("g.V") {
            assert!(profile.materialized_vertex_borrows > 0);
            assert!(profile.materialized_candidates > 0);
        }
        assert_eq!(
            profile.materialized_vertex_clones + profile.materialized_edge_clones,
            0,
            "{text}: {profile:?}"
        );
    }
}
