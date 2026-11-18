use velagraph::{
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, GremlinBindings, GremlinParameters, GremlinQuery, GremlinRow, PathElement, Value, Vertex,
};

fn vertex(id: u64, n: i64) -> TemporalMutation {
    let mut v = Vertex::new(id);
    v.properties.insert("n".into(), Value::Integer(n));
    v.properties.insert("fixed".into(), Value::Integer(0));
    TemporalMutation::Vertex {
        valid: Interval { from: 0, to: None },
        vertex: v,
    }
}
#[test]
fn bound_start_ids_preserve_the_textual_integer_domain() {
    let prepared = GremlinQuery::parse("g.V(0).count()")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![],
        })
        .unwrap();
    for id in [0, i64::MAX as u64] {
        assert!(GremlinQuery::parse(&format!("g.V({id}).count()")).is_ok());
        assert!(prepared
            .bind(GremlinBindings {
                start_ids: Some(&[id]),
                values: &[]
            })
            .is_ok());
    }
    for id in [i64::MAX as u64 + 1, u64::MAX] {
        assert!(GremlinQuery::parse(&format!("g.V({id}).count()")).is_err());
        assert!(prepared
            .bind(GremlinBindings {
                start_ids: Some(&[id]),
                values: &[]
            })
            .is_err());
    }
}
#[test]
fn binds_changing_ids_and_typed_values_without_changing_fixed_literals() {
    let dir = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(dir.path()).unwrap();
    s.commit(&[vertex(1, 7), vertex(2, 9)]).unwrap();
    let p = GremlinQuery::parse("g.V(0).has('n',0).has('fixed',0).values('n')")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![0],
        })
        .unwrap();
    for (id, n) in [(1, 7), (2, 9)] {
        let values = [Value::Integer(n)];
        let result = s
            .view(Some(1), 0)
            .unwrap()
            .execute_prepared(
                &p,
                GremlinBindings {
                    start_ids: Some(&[id]),
                    values: &values,
                },
            )
            .unwrap();
        assert_eq!(result.rows, vec![GremlinRow::Value(Value::Integer(n))]);
    }
    assert!(p
        .bind(GremlinBindings {
            start_ids: Some(&[1]),
            values: &[Value::Text("7".into())]
        })
        .is_err());
}
#[test]
fn preserves_root_order_and_multiplicity_and_global_start_guard() {
    let dir = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(dir.path()).unwrap();
    s.commit(&[vertex(1, 1), vertex(2, 2)]).unwrap();
    let p = GremlinQuery::parse("g.V(0).limit(16).path()")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![],
        })
        .unwrap();
    let v = s.view(Some(1), 0).unwrap();
    let result = v
        .execute_prepared(
            &p,
            GremlinBindings {
                start_ids: Some(&[2, 2, 1]),
                values: &[],
            },
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            GremlinRow::Path(vec![PathElement::Vertex(2)]),
            GremlinRow::Path(vec![PathElement::Vertex(2)]),
            GremlinRow::Path(vec![PathElement::Vertex(1)])
        ]
    );
    assert!(p
        .bind(GremlinBindings {
            start_ids: Some(&[]),
            values: &[]
        })
        .is_err());
    assert!(p.bind(GremlinBindings::default()).is_err());
    assert!(v
        .execute_query(&GremlinQuery::parse("g.V().path()").unwrap())
        .is_err());
}
#[test]
fn bindings_enforce_limits_and_slot_admission() {
    assert!(GremlinQuery::parse("g.V(0).out('p')")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![0]
        })
        .is_err());
    assert!(GremlinQuery::parse("g.V(0).has('n',0)")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![0, 0]
        })
        .is_err());
    let p = GremlinQuery::parse("g.V(0).has('n','').path()")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![0],
        })
        .unwrap();
    assert!(p
        .bind(GremlinBindings {
            start_ids: Some(&vec![1; 128]),
            values: &[Value::Text("x".into())]
        })
        .is_err());
    assert!(p
        .bind(GremlinBindings {
            start_ids: Some(&[1]),
            values: &[Value::Text("x".repeat(17000))]
        })
        .is_err());
    assert!(p
        .bind(GremlinBindings {
            start_ids: Some(&[1]),
            values: &[Value::Text(";".repeat(70))]
        })
        .is_err());
    let p = GremlinQuery::parse("g.V(0).has('n',0.0)")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![0],
        })
        .unwrap();
    assert!(p
        .bind(GremlinBindings {
            start_ids: Some(&[1]),
            values: &[Value::Real(f64::NAN)]
        })
        .is_err());
}
#[test]
fn preserves_edge_start_kind_and_fresh_view() {
    let dir = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(dir.path()).unwrap();
    let v1 = s
        .commit(&[
            vertex(1, 1),
            vertex(2, 2),
            TemporalMutation::Edge {
                valid: Interval { from: 0, to: None },
                edge: Edge::new(9, 1, 2, "p"),
            },
        ])
        .unwrap();
    let p = GremlinQuery::parse("g.E(0).path()")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![],
        })
        .unwrap();
    assert_eq!(
        s.view(Some(v1), 0)
            .unwrap()
            .execute_prepared(
                &p,
                GremlinBindings {
                    start_ids: Some(&[9]),
                    values: &[]
                }
            )
            .unwrap()
            .rows,
        vec![GremlinRow::Path(vec![PathElement::Edge(9)])]
    );
    let p = GremlinQuery::parse("g.V(0).values('n')")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![],
        })
        .unwrap();
    let mut node = Vertex::new(1);
    node.properties.insert("n".into(), Value::Integer(8));
    let v2 = s
        .commit(&[TemporalMutation::Vertex {
            valid: Interval { from: 10, to: None },
            vertex: node,
        }])
        .unwrap();
    for (v, t, want) in [(v1, 11, 1), (v2, 9, 1), (v2, 11, 8)] {
        assert_eq!(
            s.view(Some(v), t)
                .unwrap()
                .execute_prepared(
                    &p,
                    GremlinBindings {
                        start_ids: Some(&[1]),
                        values: &[]
                    }
                )
                .unwrap()
                .rows,
            vec![GremlinRow::Value(Value::Integer(want))]
        );
    }
}
#[test]
fn bound_strings_remain_values_not_query_syntax() {
    let dir = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(dir.path()).unwrap();
    let payload = "'); g.V().drop(); ('";
    let mut v = Vertex::new(1);
    v.properties
        .insert("text".into(), Value::Text(payload.into()));
    s.commit(&[TemporalMutation::Vertex {
        valid: Interval { from: 0, to: None },
        vertex: v,
    }])
    .unwrap();
    let p = GremlinQuery::parse("g.V(0).has('text','').path()")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![0],
        })
        .unwrap();
    let values = [Value::Text(payload.into())];
    assert_eq!(
        s.view(Some(1), 0)
            .unwrap()
            .execute_prepared(
                &p,
                GremlinBindings {
                    start_ids: Some(&[1]),
                    values: &values
                }
            )
            .unwrap()
            .rows
            .len(),
        1
    );
}
#[test]
fn a_bound_batch_shares_budget_and_next_execution_starts_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let s = TemporalStore::open(dir.path()).unwrap();
    let mut mutations = Vec::new();
    for id in (1..=16).chain(100..300) {
        mutations.push(vertex(id, 0));
    }
    let mut edge_id = 1;
    for from in 1..=16 {
        for to in 100..300 {
            mutations.push(TemporalMutation::Edge {
                valid: Interval { from: 0, to: None },
                edge: Edge::new(edge_id, from, to, "p"),
            });
            edge_id += 1;
        }
    }
    let mut version = 0;
    for batch in mutations.chunks(256) {
        version = s.commit(batch).unwrap();
    }
    let p = GremlinQuery::parse("g.V(0).limit(16).out('p').path()")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![],
        })
        .unwrap();
    let view = s.view(Some(version), 0).unwrap();
    assert_eq!(
        view.execute_prepared(
            &p,
            GremlinBindings {
                start_ids: Some(&[1]),
                values: &[]
            }
        )
        .unwrap()
        .rows
        .len(),
        200
    );
    let roots: Vec<_> = (1..=16).collect();
    let result = view.execute_prepared(
        &p,
        GremlinBindings {
            start_ids: Some(&roots),
            values: &[],
        },
    );
    assert!(
        matches!(result,Err(velagraph::Error::TemporalStore(e)) if matches!(*e,velagraph::temporal_store::TemporalStoreError::Limit))
    );
    assert_eq!(
        view.execute_prepared(
            &p,
            GremlinBindings {
                start_ids: Some(&[1]),
                values: &[]
            }
        )
        .unwrap()
        .rows
        .len(),
        200
    );
}
