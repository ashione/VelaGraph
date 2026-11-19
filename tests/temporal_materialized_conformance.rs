use velagraph::{
    temporal_reference::{Interval, Key},
    temporal_store::{
        materialized::{MaterializationLimits, MaterializedQueryLimits},
        TemporalMutation, TemporalStore,
    },
    Edge, GremlinQuery, Value, Vertex,
};
fn vertex(id: u64, name: &str) -> TemporalMutation {
    let mut v = Vertex::new(id);
    v.properties.insert("name".into(), Value::Text(name.into()));
    TemporalMutation::Vertex {
        valid: Interval {
            from: i64::MIN,
            to: None,
        },
        vertex: v,
    }
}
fn edge(
    id: u64,
    from: u64,
    to: u64,
    start: i64,
    end: Option<i64>,
    label: &str,
) -> TemporalMutation {
    TemporalMutation::Edge {
        valid: Interval {
            from: start,
            to: end,
        },
        edge: Edge::new(id, from, to, label),
    }
}
#[test]
fn fixed_v_memory_rows_match_storage_before_and_after_source_writes() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(1, "one"),
            vertex(2, "two"),
            vertex(3, "three"),
            edge(10, 1, 2, 0, None, "L"),
            edge(11, 2, 2, 0, None, "L"),
        ])
        .unwrap();
    let old = store
        .materialize(Some(1), MaterializationLimits::default())
        .unwrap();
    store
        .commit(&[
            edge(10, 1, 3, 5, Some(20), "M"),
            TemporalMutation::Vertex {
                valid: Interval {
                    from: 7,
                    to: Some(15),
                },
                vertex: {
                    let mut v = Vertex::new(2);
                    v.properties
                        .insert("name".into(), Value::Text("changed".into()));
                    v
                },
            },
        ])
        .unwrap();
    store
        .commit(&[
            TemporalMutation::Absent {
                valid: Interval {
                    from: 8,
                    to: Some(10),
                },
                key: Key::Edge(10),
            },
            TemporalMutation::Absent {
                valid: Interval {
                    from: 12,
                    to: Some(18),
                },
                key: Key::Vertex(3),
            },
        ])
        .unwrap();
    store
        .commit(&[
            edge(10, 1, 2, 20, Some(30), "L"),
            edge(10, 3, 1, 40, None, "L"),
            edge(12, 1, 2, 0, None, "L"),
        ])
        .unwrap();
    let queries = [
        "g.V(1).out().path()",
        "g.V(2).both().id()",
        "g.V(3).in().count()",
        "g.V(1,1,2,3,99).values('name')",
        "g.V(1).out('L').values('name')",
        "g.E(10,11,12).label()",
    ]
    .map(|s| GremlinQuery::parse(s).unwrap());
    for version in 0..=4 {
        let first_model = store
            .materialize(Some(version), MaterializationLimits::default())
            .unwrap();
        let second_model = store
            .materialize(Some(version), MaterializationLimits::default())
            .unwrap();
        for t in [
            i64::MIN,
            -1,
            0,
            5,
            7,
            8,
            9,
            10,
            12,
            15,
            18,
            19,
            20,
            29,
            30,
            39,
            40,
            i64::MAX,
        ] {
            for q in &queries {
                let expected = store
                    .view(Some(version), t)
                    .unwrap()
                    .execute_query(q)
                    .unwrap()
                    .rows;
                assert_eq!(
                    first_model.view(t).execute_query(q).unwrap().rows,
                    expected,
                    "first_model V={version} T={t}"
                );
                assert_eq!(
                    second_model.view(t).execute_query(q).unwrap().rows,
                    expected,
                    "second_model V={version} T={t}"
                );
            }
        }
    }
    assert_eq!(
        old.view(6).execute_query(&queries[0]).unwrap().rows,
        store
            .view(Some(1), 6)
            .unwrap()
            .execute_query(&queries[0])
            .unwrap()
            .rows
    );
    let owned = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    drop(store);
    assert_eq!(owned.version(), 4);
    assert!(owned.view(6).execute_query(&queries[0]).is_ok());
}
#[test]
fn builder_limits_reject_incomplete_views_and_global_queries_stay_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store.commit(&[vertex(1, "one"), vertex(2, "two")]).unwrap();
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_source_rows: 1,
                ..Default::default()
            }
        )
        .is_err());
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_accounted_bytes: 1,
                ..Default::default()
            }
        )
        .is_err());
    let view = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    assert!(view
        .view(0)
        .execute_query(&GremlinQuery::parse("g.V()").unwrap())
        .is_err());
}
#[test]
fn properties_missing_endpoints_and_wide_ids_survive_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let mut a = Vertex::new(1);
    a.labels.push("Before".into());
    a.properties.insert("null".into(), Value::Null);
    a.properties.insert("flag".into(), Value::Boolean(true));
    a.properties.insert("real".into(), Value::Real(1.25));
    let mut first = Edge::new(10, 1, 2, "L");
    first.properties.insert("score".into(), Value::Integer(1));
    store
        .commit(&[
            TemporalMutation::Vertex {
                valid: Interval { from: 0, to: None },
                vertex: a.clone(),
            },
            TemporalMutation::Edge {
                valid: Interval { from: 0, to: None },
                edge: first,
            },
        ])
        .unwrap();
    store
        .commit(&[
            vertex(2, "two"),
            vertex(u64::MAX, "wide"),
            edge(u64::MAX, 1, u64::MAX, 0, None, "WIDE"),
        ])
        .unwrap();
    a.labels = vec!["After".into()];
    let mut second = Edge::new(10, 1, 2, "L");
    second.properties.insert("score".into(), Value::Integer(2));
    store
        .commit(&[
            TemporalMutation::Vertex {
                valid: Interval {
                    from: 5,
                    to: Some(10),
                },
                vertex: a,
            },
            TemporalMutation::Edge {
                valid: Interval {
                    from: 5,
                    to: Some(10),
                },
                edge: second,
            },
        ])
        .unwrap();
    let texts = [
        "g.E(10).values('score')",
        "g.E(10).has('score',2).count()",
        "g.V(1).has('null',null).count()",
        "g.V(1).has('missing').count()",
        "g.V(1).hasLabel('After').values('flag')",
        "g.V(1).values('real')",
        "g.V(1).out('WIDE').path()",
        "g.V(1,2).limit(1).id()",
    ];
    for version in 1..=3 {
        let memory = store
            .materialize(Some(version), MaterializationLimits::default())
            .unwrap();
        for t in [-1, 0, 4, 5, 9, 10, i64::MAX] {
            for text in texts {
                let q = GremlinQuery::parse(text).unwrap();
                let expected = store
                    .view(Some(version), t)
                    .unwrap()
                    .execute_query(&q)
                    .unwrap()
                    .rows;
                assert_eq!(
                    memory.view(t).execute_query(&q).unwrap().rows,
                    expected,
                    "{version} {t} {text}"
                );
            }
        }
    }
}
#[test]
fn exact_source_row_and_byte_caps_and_unavailable_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store.commit(&[vertex(1, "one"), vertex(2, "two")]).unwrap();
    let good = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let bytes = good.stats().scanned_raw_bytes;
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_source_rows: 2,
                max_raw_bytes: bytes,
                ..Default::default()
            }
        )
        .is_ok());
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_source_rows: 2,
                max_raw_bytes: bytes - 1,
                ..Default::default()
            }
        )
        .is_err());
    assert!(store
        .materialize(Some(2), MaterializationLimits::default())
        .is_err());
}
#[test]
fn query_work_limits_are_repeatable_and_have_an_exact_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            vertex(1, "one"),
            vertex(2, "two"),
            edge(10, 1, 2, 0, Some(1), "L"),
            edge(11, 1, 2, 0, None, "L"),
            edge(12, 1, 2, 0, None, "OTHER"),
        ])
        .unwrap();
    let a = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let b = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let q = GremlinQuery::parse("g.V(1).out('L').count()")
        .unwrap()
        .prepare(velagraph::GremlinParameters {
            start_ids: false,
            has_value_steps: vec![],
        })
        .unwrap();
    for limit in 1..=20 {
        let x = a
            .view(5)
            .execute_prepared_with_limits(
                &q,
                velagraph::GremlinBindings {
                    start_ids: None,
                    values: &[],
                },
                MaterializedQueryLimits {
                    max_operations: limit,
                    ..Default::default()
                },
            )
            .map(|x| x.rows)
            .map_err(|e| e.to_string());
        let y = b
            .view(5)
            .execute_prepared_with_limits(
                &q,
                velagraph::GremlinBindings {
                    start_ids: None,
                    values: &[],
                },
                MaterializedQueryLimits {
                    max_operations: limit,
                    ..Default::default()
                },
            )
            .map(|x| x.rows)
            .map_err(|e| e.to_string());
        assert_eq!(x, y, "limit={limit}");
        if limit >= 7 {
            assert_eq!(x.unwrap(), vec![velagraph::GremlinRow::Count(1)]);
        } else {
            assert!(x.is_err());
        }
    }
    let exact = b.stats().build_accounted_bytes;
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_accounted_bytes: exact,
                ..Default::default()
            }
        )
        .is_ok());
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_accounted_bytes: exact - 1,
                ..Default::default()
            }
        )
        .is_err());
    drop(store);
    let reopened = TemporalStore::open(dir.path()).unwrap();
    let rebuilt = reopened
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let query = GremlinQuery::parse("g.V(1).out().path()").unwrap();
    for t in [-1, 0, 1, 5] {
        assert_eq!(
            rebuilt.view(t).execute_query(&query).unwrap().rows,
            reopened
                .view(None, t)
                .unwrap()
                .execute_query(&query)
                .unwrap()
                .rows
        );
    }
}
#[test]
fn deterministic_mixed_history_matches_raw_queries_on_a_dense_grid() {
    fn next(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *seed
    }
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&(1..=8).map(|id| vertex(id, "seed")).collect::<Vec<_>>())
        .unwrap();
    let mut seed = 17u64;
    for i in 0..40 {
        let id = 1 + next(&mut seed) % 8;
        let start = (next(&mut seed) % 25) as i64 - 5;
        let valid = Interval {
            from: start,
            to: Some(start + 1 + (next(&mut seed) % 9) as i64),
        };
        let mutation = match next(&mut seed) % 5 {
            0 => TemporalMutation::Absent {
                valid,
                key: Key::Vertex(id),
            },
            1 => {
                let mut v = Vertex::new(id);
                v.properties.insert("n".into(), Value::Integer(i));
                TemporalMutation::Vertex { valid, vertex: v }
            }
            2 => TemporalMutation::Absent {
                valid,
                key: Key::Edge(9 + id),
            },
            _ => {
                let to = 1 + next(&mut seed) % 8;
                TemporalMutation::Edge {
                    valid,
                    edge: Edge::new(
                        9 + id,
                        id,
                        to,
                        if next(&mut seed).is_multiple_of(2) {
                            "L"
                        } else {
                            "M"
                        },
                    ),
                }
            }
        };
        store.commit(&[mutation]).unwrap();
    }
    let mut queries = Vec::new();
    for id in 1..=8 {
        for direction in ["out", "in", "both"] {
            for label in ["", "'L'", "'M'"] {
                queries.push(
                    GremlinQuery::parse(&format!("g.V({id}).{direction}({label}).path()")).unwrap(),
                );
            }
        }
    }
    for version in [0, 1, 2, 10, 20, 30, 41] {
        let a = store
            .materialize(Some(version), MaterializationLimits::default())
            .unwrap();
        let b = store
            .materialize(Some(version), MaterializationLimits::default())
            .unwrap();
        for time in -6..32 {
            let raw = store.view(Some(version), time).unwrap();
            for q in &queries {
                let expected = raw.execute_query(q).unwrap().rows;
                assert_eq!(a.view(time).execute_query(q).unwrap().rows, expected);
                assert_eq!(b.view(time).execute_query(q).unwrap().rows, expected);
            }
        }
    }
}
