use super::*;
fn seeded() -> (tempfile::TempDir, TemporalStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    store
        .commit(&[
            TemporalMutation::Vertex {
                valid: Interval { from: 0, to: None },
                vertex: Vertex::new(1),
            },
            TemporalMutation::Vertex {
                valid: Interval { from: 0, to: None },
                vertex: Vertex::new(2),
            },
            TemporalMutation::Edge {
                valid: Interval {
                    from: 0,
                    to: Some(10),
                },
                edge: Edge::new(7, 1, 2, "LINK"),
            },
        ])
        .unwrap();
    (dir, store)
}
#[test]
fn source_index_must_match_canonical_candidates() {
    for mutation in 0..5 {
        let (_dir, store) = seeded();
        let key = adjacency_key(1, false, 7);
        match mutation {
            0 => store.db.delete(&key).unwrap(),
            1 => store.db.put(&key, [0; 8]).unwrap(),
            2 => store
                .db
                .put(adjacency_key(99, false, 7), 1u64.to_be_bytes())
                .unwrap(),
            3 => store.db.put(&key, b"invalid").unwrap(),
            _ => {
                store.commit(&[]).unwrap();
                store.db.put(&key, 2u64.to_be_bytes()).unwrap();
            }
        }
        assert!(store
            .materialize(None, MaterializationLimits::default())
            .is_err());
    }
}
#[test]
fn eager_validation_rejects_corrupt_invisible_records_and_future_payloads() {
    let (_dir, store) = seeded();
    store
        .db
        .put(revision_key(Key::Edge(7), 1, 2), b"bad")
        .unwrap();
    assert!(store
        .materialize(None, MaterializationLimits::default())
        .is_err());
    let (_dir, store) = seeded();
    store
        .commit(&[TemporalMutation::Vertex {
            valid: Interval { from: 20, to: None },
            vertex: Vertex::new(2),
        }])
        .unwrap();
    store
        .db
        .put(
            revision_key(Key::Vertex(2), 2, 0),
            encode_assertion(Interval { from: 20, to: None }, Some(b"bad")).unwrap(),
        )
        .unwrap();
    assert!(store
        .materialize(Some(1), MaterializationLimits::default())
        .is_err());
}
#[test]
fn candidate_capture_counts_toward_source_limits() {
    let (_dir, store) = seeded();
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    assert_eq!(model.stats().scanned_revisions, 3);
    assert_eq!(model.stats().scanned_candidates, 2);
    let exact = MaterializationLimits {
        max_source_rows: 5,
        max_raw_bytes: model.stats().scanned_raw_bytes,
        ..Default::default()
    };
    assert!(store.materialize(None, exact).is_ok());
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_source_rows: 4,
                ..exact
            }
        )
        .is_err());
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_raw_bytes: exact.max_raw_bytes - 1,
                ..exact
            }
        )
        .is_err());
}
#[test]
fn old_v_still_validates_future_endpoint_candidates() {
    let (_dir, store) = seeded();
    store
        .commit(&[TemporalMutation::Edge {
            valid: Interval { from: 0, to: None },
            edge: Edge::new(7, 2, 1, "REVERSED"),
        }])
        .unwrap();
    store
        .commit(&[TemporalMutation::Absent {
            valid: Interval { from: 0, to: None },
            key: Key::Edge(7),
        }])
        .unwrap();
    let old = store
        .materialize(Some(1), MaterializationLimits::default())
        .unwrap();
    assert_eq!(old.stats().source_head, 3);
    assert_eq!(
        old.view(5)
            .execute_gremlin("g.V(1).out().count()")
            .unwrap()
            .rows,
        vec![crate::GremlinRow::Count(1)]
    );
    store
        .db
        .put(adjacency_key(2, false, 7), 1u64.to_be_bytes())
        .unwrap();
    assert!(store
        .materialize(Some(1), MaterializationLimits::default())
        .is_err());
}
#[test]
fn record_payload_bytes_have_an_exact_work_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let mut vertex = Vertex::new(1);
    vertex
        .properties
        .insert("payload".into(), crate::Value::Text("x".repeat(4096)));
    store
        .commit(&[TemporalMutation::Vertex {
            valid: Interval { from: 0, to: None },
            vertex,
        }])
        .unwrap();
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let slot = model.vertex_index[&1] as usize;
    let payload = model.vertices[slot].at(0).unwrap().payload as usize;
    let bytes = std::mem::size_of::<Piece>() + model.payloads[payload].bytes;
    let query = GremlinQuery::parse("g.V(1).count()").unwrap();
    let limits = MaterializedQueryLimits {
        max_operations: 1,
        max_work_bytes: bytes,
    };
    assert!(model
        .view(0)
        .execute_query_with_limits(&query, limits)
        .is_ok());
    assert!(model
        .view(0)
        .execute_query_with_limits(
            &query,
            MaterializedQueryLimits {
                max_work_bytes: bytes - 1,
                ..limits
            }
        )
        .is_err());
    let repeated = GremlinQuery::parse("g.V(1,1).hasLabel('absent').count()").unwrap();
    assert!(model
        .view(0)
        .execute_query_with_limits(
            &repeated,
            MaterializedQueryLimits {
                max_operations: 2,
                max_work_bytes: bytes * 2 - 1
            }
        )
        .is_err());
    assert_eq!(
        model
            .view(0)
            .execute_query_with_limits(
                &repeated,
                MaterializedQueryLimits {
                    max_operations: 2,
                    max_work_bytes: bytes * 2
                }
            )
            .unwrap()
            .rows,
        vec![crate::GremlinRow::Count(0)]
    );
}

#[test]
fn borrowed_self_loop_charges_discarded_edge_and_selected_text() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let valid = Interval { from: 0, to: None };
    let mut vertex = Vertex::new(1);
    let text = "selected".repeat(8192);
    vertex
        .properties
        .insert("selected".into(), crate::Value::Text(text.clone()));
    let mut edge = Edge::new(7, 1, 1, "LINK");
    edge.properties
        .insert("unused".into(), crate::Value::Text("unused".repeat(8192)));
    store
        .commit(&[
            TemporalMutation::Vertex { valid, vertex },
            TemporalMutation::Edge { valid, edge },
        ])
        .unwrap();
    let model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let vertex_bytes = model.payloads[model.vertices[0].at(0).unwrap().payload as usize].bytes;
    let edge_bytes = model.payloads[model.edges[0].at(0).unwrap().payload as usize].bytes;
    // Start, four endpoint checks, and one target resolution. The in-side copy
    // is discarded by both's self-loop dedup, but it must still consume work.
    let bytes =
        6 * std::mem::size_of::<Piece>() + 2 * ROW_WORK_BYTES + 2 * vertex_bytes + 2 * edge_bytes;
    let query = GremlinQuery::parse("g.V(1).both().values('selected')").unwrap();
    for (operations, work, succeeds) in [(8, bytes, true), (7, bytes, false), (8, bytes - 1, false)]
    {
        let result = model.view(0).execute_query_with_limits(
            &query,
            MaterializedQueryLimits {
                max_operations: operations,
                max_work_bytes: work,
            },
        );
        if succeeds {
            assert_eq!(
                result.unwrap().rows,
                vec![crate::GremlinRow::Value(crate::Value::Text(text.clone()))]
            );
        } else {
            assert!(matches!(
                result,
                Err(crate::Error::TraversalLimitExceeded(_))
            ));
        }
    }
}

#[test]
fn skipped_block_preserves_exact_byte_charges_and_summary_validation() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let mut changes = vec![TemporalMutation::Vertex {
        valid: Interval {
            from: i64::MIN,
            to: None,
        },
        vertex: Vertex::new(1),
    }];
    for id in 0..65 {
        changes.push(TemporalMutation::Edge {
            valid: Interval {
                from: 10,
                to: Some(20),
            },
            edge: Edge::new(1000 + id, 1, 1, "LOOP"),
        });
    }
    store.commit(&changes).unwrap();
    let mut model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let payload = model.vertices[0].at(0).unwrap().payload as usize;
    let bytes = std::mem::size_of::<Piece>() + model.payloads[payload].bytes + 130 * ROW_WORK_BYTES;
    let query = GremlinQuery::parse("g.V(1).both().count()").unwrap();
    for max_work_bytes in [bytes - 1, bytes] {
        let result = model.view(0).execute_query_with_limits(
            &query,
            MaterializedQueryLimits {
                max_operations: 131,
                max_work_bytes,
            },
        );
        assert_eq!(result.is_ok(), max_work_bytes == bytes);
    }
    assert!(model.stats().adjacency_block_bytes > 0);
    let build_charge = model.stats().build_accounted_bytes;
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_accounted_bytes: build_charge - 1,
                ..Default::default()
            }
        )
        .is_err());
    assert!(store
        .materialize(
            None,
            MaterializationLimits {
                max_accounted_bytes: build_charge,
                ..Default::default()
            }
        )
        .is_ok());
    model.outgoing_blocks[0].pop();
    let mut meter = Meter {
        limits: MaterializationLimits::default(),
        charged: 0,
        rows: 0,
        revisions: 0,
        candidates: 0,
        raw: 0,
        deadline: Instant::now() + BUILD_DEADLINE,
    };
    assert!(matches!(
        blocks::validate(
            &model.outgoing,
            &model.outgoing_blocks,
            &model.rows,
            &mut meter
        ),
        Err(TemporalStoreError::Corrupt)
    ));
}

#[test]
fn mixed_skips_and_visible_edges_have_identical_admission_to_linear_reads() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let mut vertex = Vertex::new(1);
    vertex
        .properties
        .insert("value".into(), crate::Value::Text("ok".into()));
    let mut changes = vec![TemporalMutation::Vertex {
        valid: Interval {
            from: i64::MIN,
            to: None,
        },
        vertex,
    }];
    for id in 0..65 {
        changes.push(TemporalMutation::Edge {
            valid: if id == 64 {
                Interval { from: 0, to: None }
            } else {
                Interval {
                    from: 10,
                    to: Some(20),
                }
            },
            edge: Edge::new(1000 + id, 1, 1, "LOOP"),
        });
    }
    store.commit(&changes).unwrap();
    let mut model = store
        .materialize(None, MaterializationLimits::default())
        .unwrap();
    let v = model.payloads[model.vertices[0].at(0).unwrap().payload as usize].bytes;
    let e = model.payloads[model.edges[64].at(0).unwrap().payload as usize].bytes;
    // Both directions charge 65 candidates; the one visible self-loop charges
    // two endpoints/edge borrows in each direction, but only one target borrow.
    let bytes = 6 * std::mem::size_of::<Piece>() + 2 * v + 2 * e + 130 * ROW_WORK_BYTES;
    let q = GremlinQuery::parse("g.V(1).both().values('value')").unwrap();
    let mut expected = Vec::new();
    for indexed in [true, false] {
        if !indexed {
            model.outgoing_blocks[0].clear();
            model.incoming_blocks[0].clear();
        }
        let mut actual = Vec::new();
        for operations in [135, 136, 137] {
            for work in [bytes - 1, bytes, bytes + 1] {
                let result = model.view(0).execute_query_with_limits(
                    &q,
                    MaterializedQueryLimits {
                        max_operations: operations,
                        max_work_bytes: work,
                    },
                );
                let succeeds = operations >= 136 && work >= bytes;
                assert_eq!(
                    result.is_ok(),
                    succeeds,
                    "indexed={indexed},ops={operations},bytes={work}"
                );
                if let Ok(r) = &result {
                    assert_eq!(
                        r.rows,
                        vec![crate::GremlinRow::Value(crate::Value::Text("ok".into()))]
                    );
                }
                actual.push(
                    result
                        .map(|r| r.rows)
                        .map_err(|e| std::mem::discriminant(&e)),
                );
            }
        }
        if indexed {
            expected = actual;
        } else {
            assert_eq!(actual, expected);
        }
    }
}
