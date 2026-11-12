use super::*;

fn conditional(external: u8, edge: bool, expected: PropertyCondition, value: Value) -> Write {
    let conditions = BTreeMap::from([("weight".into(), expected)]);
    let patch = patch(value);
    Write {
        external_id: [external; 16],
        mutation: if edge {
            Mutation::PatchEdgeIf {
                edge: entity(7, 10),
                conditions,
                patch,
            }
        } else {
            Mutation::PatchVertexIf {
                vertex: entity(7, 1),
                conditions,
                patch,
            }
        },
    }
}
async fn seed(f: &Fixture, g: &Gateway) {
    committed(&call(g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    committed(&call(g, Operation::Submit(create(2, 8, 1))).await.unwrap());
    committed(
        &call(
            g,
            Operation::Submit(Write {
                external_id: [3; 16],
                mutation: Mutation::CreateEdge { edge: edge() },
            }),
        )
        .await
        .unwrap(),
    );
    assert_eq!(journal(f).await.resolved_prefix(), 3);
}

#[test]
fn conditional_canonical_identity_and_strict_wire() {
    let missing = conditional(4, false, PropertyCondition::Missing, Value::Integer(1));
    assert_eq!(
        serde_json::to_string(&missing.mutation).unwrap(),
        r#"{"PatchVertexIf":{"vertex":{"partition":7,"id":1},"conditions":{"weight":"Missing"},"patch":{"set":{"weight":{"Integer":1}},"remove":[]}}}"#
    );
    let variants = [
        PropertyCondition::Missing,
        PropertyCondition::Value(Value::Null),
        PropertyCondition::Value(Value::Integer(0)),
        PropertyCondition::Value(Value::Real(0.0)),
        PropertyCondition::Value(Value::Real(-0.0)),
    ];
    let mut commands = BTreeSet::new();
    for expected in variants {
        let write = conditional(4, false, expected, Value::Integer(1));
        commands.insert(canonical(&bound(), &write.mutation).unwrap());
        let encoded = serde_json::to_vec(&write).unwrap();
        assert_eq!(
            canonical(
                &bound(),
                &serde_json::from_slice::<Write>(&encoded).unwrap().mutation
            )
            .unwrap(),
            canonical(&bound(), &write.mutation).unwrap()
        );
    }
    assert_eq!(commands.len(), 5);
    let mut wire = serde_json::to_value(&missing).unwrap();
    wire["mutation"]["PatchVertexIf"]["extra"] = serde_json::json!(true);
    assert!(serde_json::from_value::<Write>(wire).is_err());
    let mut wire = serde_json::to_value(&missing).unwrap();
    wire["mutation"]["PatchVertexIf"]["conditions"]["weight"] =
        serde_json::json!({"Value":"Null","extra":true});
    assert!(serde_json::from_value::<Write>(wire).is_err());
    assert_eq!(
        canonical(
            &bound(),
            &conditional(
                4,
                false,
                PropertyCondition::Value(Value::Real(f64::NAN)),
                Value::Integer(1)
            )
            .mutation
        ),
        Err(Error::Protocol)
    );
    assert_eq!(
        canonical(
            &bound(),
            &conditional(
                4,
                false,
                PropertyCondition::Value(Value::Text("x".repeat(MAX_REQUEST_BYTES))),
                Value::Integer(1)
            )
            .mutation
        ),
        Err(Error::Capacity)
    );
    let mut oversized = conditional(
        4,
        false,
        PropertyCondition::Missing,
        Value::Text("x".repeat(MAX_REQUEST_BYTES)),
    );
    assert_eq!(
        canonical(&bound(), &oversized.mutation),
        Err(Error::Capacity)
    );
    if let Mutation::PatchVertexIf { conditions, .. } = &mut oversized.mutation {
        conditions.clear();
    }
    assert_eq!(
        canonical(&bound(), &oversized.mutation),
        Err(Error::Protocol)
    );
    assert_eq!(
        serde_json::from_str::<Error>(&serde_json::to_string(&Error::PreconditionFailed).unwrap())
            .unwrap(),
        Error::PreconditionFailed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_tcp_preserves_versions_identity_and_edge_guards() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    seed(&f, &g).await;
    let old = open(&g, None).await;
    let write = conditional(4, false, PropertyCondition::Missing, Value::Null);
    let first = call(&g, Operation::Submit(write.clone())).await.unwrap();
    committed(&first);
    let record = journal(&f)
        .await
        .request(write.external_id)
        .unwrap()
        .clone();
    let mismatch = conditional(5, false, PropertyCondition::Missing, Value::Integer(9));
    assert_eq!(
        call(&g, Operation::Submit(mismatch.clone())).await,
        Err(Error::PreconditionFailed)
    );
    assert!(matches!(
        call(&g, Operation::Status(mismatch)).await.unwrap(),
        Reply::Write {
            transaction_id: None,
            status: WriteStatus::Absent,
            ..
        }
    ));
    committed(
        &call(
            &g,
            Operation::Submit(conditional(
                6,
                false,
                PropertyCondition::Value(Value::Null),
                Value::Integer(2),
            )),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        call(&g, Operation::Submit(write.clone())).await.unwrap(),
        first
    );
    assert_eq!(
        call(&g, Operation::Status(write.clone())).await.unwrap(),
        first
    );
    assert_eq!(
        journal(&f).await.request(write.external_id).unwrap(),
        &record
    );
    let changed = conditional(4, false, PropertyCondition::Value(Value::Null), Value::Null);
    assert_eq!(
        call(&g, Operation::Submit(changed)).await,
        Err(Error::Identity)
    );
    let edge_write = conditional(7, true, PropertyCondition::Missing, Value::Real(-0.0));
    // Send a complete request and intentionally lose the final reply. An exact
    // retry, whether admitted before or after this socket closes, is one write.
    let request = Request {
        version: VERSION,
        request_id: [99; 16],
        binding: bound(),
        operation: Operation::Submit(edge_write.clone()),
    };
    let mut socket = TcpStream::connect(g.local_addr()).await.unwrap();
    write_bytes(&mut socket, &encode(&request, MAX_REQUEST_BYTES).unwrap())
        .await
        .unwrap();
    drop(socket);
    let edge_reply = call(&g, Operation::Submit(edge_write.clone()))
        .await
        .unwrap();
    committed(&edge_reply);
    assert_eq!(
        call(
            &g,
            Operation::Submit(conditional(
                8,
                true,
                PropertyCondition::Value(Value::Real(0.0)),
                Value::Integer(9)
            ))
        )
        .await,
        Err(Error::PreconditionFailed)
    );
    let latest = open(&g, None).await;
    assert_eq!(
        read(&g, &old, query(7, "g.V(1).values('weight')")).await,
        vec![]
    );
    assert_eq!(
        read(&g, &old, query(7, "g.E(10).values('weight')")).await,
        vec![]
    );
    assert_eq!(
        read(&g, &latest, query(8, "g.V(1).values('weight')")).await,
        vec![]
    );
    assert_eq!(
        read(&g, &latest, query(7, "g.V(1).label()")).await,
        vec![value(
            crate::remote::query::QualifiedElement::Vertex {
                partition: 7,
                id: 1
            },
            Value::Text("person".into())
        )]
    );
    assert_eq!(
        read(&g, &latest, query(7, "g.V(1).outE()")).await,
        vec![e(7, 10)]
    );
    assert_eq!(
        read(&g, &latest, query(8, "g.V(1).inE()")).await,
        vec![e(7, 10)]
    );
    let edge_values = read(&g, &latest, query(7, "g.E(10).values('weight')")).await;
    assert!(
        matches!(&edge_values[..], [QueryRow::Value { value: Value::Real(v), .. }] if v.to_bits() == (-0.0_f64).to_bits())
    );
    close(&g, old).await;
    close(&g, latest).await;
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_stale_candidate_aba_and_reverse_guard_cannot_commit() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    seed(&f, &g).await;
    committed(
        &call(
            &g,
            Operation::Submit(conditional(
                4,
                false,
                PropertyCondition::Missing,
                Value::Integer(0),
            )),
        )
        .await
        .unwrap(),
    );
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    for is_edge in [false, true] {
        let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
        let stale = conditional(
            if is_edge { 9 } else { 7 },
            is_edge,
            if is_edge {
                PropertyCondition::Missing
            } else {
                PropertyCondition::Value(Value::Integer(0))
            },
            Value::Integer(100),
        );
        let candidate = stale.mutation.prepare(&snapshot).unwrap();
        assert_eq!(candidate.participants(), if is_edge { 2 } else { 1 });
        if is_edge {
            // Source property is unchanged. A target-shard change alone must
            // invalidate the original reverse guard and prevent the commit.
            committed(
                &call(
                    &g,
                    Operation::Submit(Write {
                        external_id: [8; 16],
                        mutation: Mutation::PatchVertex {
                            vertex: entity(8, 1),
                            patch: patch(Value::Integer(1)),
                        },
                    }),
                )
                .await
                .unwrap(),
            );
        } else {
            for (id, expected, updated) in [(5, 0, 1), (6, 1, 0)] {
                committed(
                    &call(
                        &g,
                        Operation::Submit(conditional(
                            id,
                            false,
                            PropertyCondition::Value(Value::Integer(expected)),
                            Value::Integer(updated),
                        )),
                    )
                    .await
                    .unwrap(),
                );
            }
        }
        // Conditions match again now, but original snapshot CAS must fail.
        graph
            .admit_request(
                stale.external_id,
                canonical(&bound(), &stale.mutation).unwrap(),
                &candidate,
            )
            .await
            .unwrap();
        let record = journal(&f)
            .await
            .request(stale.external_id)
            .unwrap()
            .clone();
        for _ in 0..2 {
            assert_eq!(
                call(&g, Operation::Submit(stale.clone())).await,
                Err(Error::Rejected)
            );
            assert!(
                matches!(call(&g, Operation::Status(stale.clone())).await.unwrap(), Reply::Write { transaction_id: Some(id), status: WriteStatus::Pending, .. } if id == candidate.id())
            );
            assert_eq!(
                journal(&f).await.request(stale.external_id).unwrap(),
                &record
            );
        }
        assert!(matches!(
            call(&g, Operation::Abort(stale.clone())).await.unwrap(),
            Reply::Write {
                status: WriteStatus::Aborted { visible: true, .. },
                ..
            }
        ));
        assert!(matches!(
            call(&g, Operation::Submit(stale)).await.unwrap(),
            Reply::Write {
                status: WriteStatus::Aborted { visible: true, .. },
                ..
            }
        ));
    }
    let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    assert_eq!(
        snapshot
            .vertex(entity(7, 1).vertex())
            .unwrap()
            .unwrap()
            .properties["weight"],
        Value::Integer(0)
    );
    assert!(snapshot
        .edge(entity(7, 10).edge())
        .unwrap()
        .unwrap()
        .properties
        .is_empty());
    g.shutdown().await;
    drop(graph);
    drop(config);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_competing_updates_have_one_winner() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    seed(&f, &g).await;
    for is_edge in [false, true] {
        let base = if is_edge { 6 } else { 4 };
        let a = conditional(base, is_edge, PropertyCondition::Missing, Value::Integer(1));
        let b = conditional(
            base + 1,
            is_edge,
            PropertyCondition::Missing,
            Value::Integer(2),
        );
        let (first, second) = tokio::join!(
            call(&g, Operation::Submit(a.clone())),
            call(&g, Operation::Submit(b.clone()))
        );
        let (winner, loser, reply) = match (first, second) {
            (Ok(reply), Err(Error::PreconditionFailed)) => (a, b, reply),
            (Err(Error::PreconditionFailed), Ok(reply)) => (b, a, reply),
            other => panic!("expected one conditional winner: {other:?}"),
        };
        committed(&reply);
        assert_eq!(call(&g, Operation::Submit(winner)).await.unwrap(), reply);
        assert!(matches!(
            call(&g, Operation::Status(loser)).await.unwrap(),
            Reply::Write {
                status: WriteStatus::Absent,
                ..
            }
        ));
    }
    assert_eq!(journal(&f).await.resolved_prefix(), 5);
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_partial_prepare_reopen_preserves_original_plan_and_old_v() {
    let _test = TEST_GATE.lock().await;
    let cluster = Cluster::new();
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let mut nodes = Vec::new();
    for (shard, dir) in [9, 7, 8].into_iter().zip(&dirs) {
        nodes.push(Arc::new(
            initialize(
                cluster
                    .in_group(shard)
                    .start_durable(1, dir.path())
                    .await
                    .unwrap(),
                shard,
            )
            .await,
        ));
    }
    let f = Fixture { cluster, nodes };
    let g = f.gateway().await;
    seed(&f, &g).await;
    let write = conditional(4, true, PropertyCondition::Missing, Value::Integer(1));
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    let candidate = write.mutation.prepare(&snapshot).unwrap();
    let original = graph
        .admit_request(
            write.external_id,
            canonical(&bound(), &write.mutation).unwrap(),
            &candidate,
        )
        .await
        .unwrap();
    let record = journal(&f)
        .await
        .request(write.external_id)
        .unwrap()
        .clone();
    let durable = d2_raft::request_plans::Plan::decode(&record.payload).unwrap();
    assert_eq!(durable.validation_version, 3);
    assert_eq!(durable.parts.len(), 2);
    let part = &durable.parts[0];
    participant_driver::prepare(
        &f.nodes[0],
        &f.nodes[1],
        authority(),
        1,
        part.descriptor.clone(),
        part.request.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        call(&g, Operation::Status(write.clone())).await.unwrap(),
        Reply::Write {
            status: WriteStatus::Pending,
            ..
        }
    ));
    g.shutdown().await;
    drop(snapshot);
    drop(graph);
    drop(config);
    for (node, shard) in f.nodes.iter().zip([9, 7, 8]) {
        f.cluster.in_group(shard).shutdown(node).await;
    }
    let Fixture { cluster, nodes } = f;
    drop(nodes);
    let mut nodes = Vec::new();
    for (shard, dir) in [9, 7, 8].into_iter().zip(&dirs) {
        let node = cluster
            .in_group(shard)
            .start_durable(1, dir.path())
            .await
            .unwrap();
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader == Some(1), "reopened")
            .await
            .unwrap();
        nodes.push(Arc::new(node));
    }
    let f = Fixture { cluster, nodes };
    let g = f.gateway().await;
    let reply = call(&g, Operation::Submit(write.clone())).await.unwrap();
    assert_eq!(committed(&reply), (original.id(), 4));
    assert_eq!(
        journal(&f).await.request(write.external_id).unwrap(),
        &record
    );
    committed(
        &call(
            &g,
            Operation::Submit(conditional(
                5,
                true,
                PropertyCondition::Value(Value::Integer(1)),
                Value::Integer(2),
            )),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        call(&g, Operation::Submit(write.clone())).await.unwrap(),
        reply
    );
    assert_eq!(call(&g, Operation::Abort(write)).await.unwrap(), reply);
    assert_eq!(journal(&f).await.resolved_prefix(), 5);
    let old = open(&g, Some(3)).await;
    let latest = open(&g, Some(5)).await;
    assert_eq!(
        read(&g, &old, query(7, "g.E(10).values('weight')")).await,
        vec![]
    );
    assert_eq!(
        read(&g, &latest, query(7, "g.E(10).values('weight')")).await,
        vec![value(
            crate::remote::query::QualifiedElement::Edge {
                partition: 7,
                id: 10
            },
            Value::Integer(2)
        )]
    );
    close(&g, old).await;
    close(&g, latest).await;
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_self_loop_combines_one_part_and_preserves_unrelated_values() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    let mut loop_edge = edge();
    loop_edge.to = loop_edge.from;
    loop_edge
        .properties
        .insert("keep".into(), Value::Boolean(true));
    loop_edge
        .properties
        .insert("remove".into(), Value::Text("old".into()));
    committed(
        &call(
            &g,
            Operation::Submit(Write {
                external_id: [2; 16],
                mutation: Mutation::CreateEdge {
                    edge: loop_edge.clone(),
                },
            }),
        )
        .await
        .unwrap(),
    );
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    assert!(matches!(
        snapshot.prepare_vertex_properties_if(
            entity(99, 1).vertex(),
            &BTreeMap::new(),
            &VertexPropertyPatch::default()
        ),
        Err(managed_graph::Error::InvalidConditions)
    ));
    let write = Write {
        external_id: [3; 16],
        mutation: Mutation::PatchEdgeIf {
            edge: entity(7, 10),
            conditions: BTreeMap::from([
                (
                    "keep".into(),
                    PropertyCondition::Value(Value::Boolean(true)),
                ),
                ("weight".into(), PropertyCondition::Missing),
                (
                    "remove".into(),
                    PropertyCondition::Value(Value::Text("old".into())),
                ),
            ]),
            patch: Patch {
                set: BTreeMap::from([("weight".into(), Value::Integer(1))]),
                remove: BTreeSet::from(["remove".into()]),
            },
        },
    };
    let candidate = write.mutation.prepare(&snapshot).unwrap();
    assert_eq!(candidate.participants(), 1);
    committed(&call(&g, Operation::Submit(write)).await.unwrap());
    let now = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    let updated = now.edge(entity(7, 10).edge()).unwrap().unwrap();
    assert_eq!(
        updated.properties,
        BTreeMap::from([
            ("keep".into(), Value::Boolean(true)),
            ("weight".into(), Value::Integer(1))
        ])
    );
    assert_eq!(updated.from, loop_edge.from.vertex());
    assert_eq!(updated.to, loop_edge.to.vertex());
    assert_eq!(updated.label, loop_edge.label);
    assert_eq!(
        now.observed_adjacency(
            entity(7, 1).vertex(),
            velagraph::experimental_graph::Direction::Out
        )
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        now.observed_adjacency(
            entity(7, 1).vertex(),
            velagraph::experimental_graph::Direction::In
        )
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        snapshot
            .edge(entity(7, 10).edge())
            .unwrap()
            .unwrap()
            .properties["remove"],
        Value::Text("old".into())
    );
    g.shutdown().await;
    drop(graph);
    drop(config);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_independently_admitted_competitors_cannot_both_commit() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    seed(&f, &g).await;
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    for is_edge in [false, true] {
        let base = if is_edge { 6 } else { 4 };
        let a = conditional(base, is_edge, PropertyCondition::Missing, Value::Integer(1));
        let b = conditional(
            base + 1,
            is_edge,
            PropertyCondition::Missing,
            Value::Integer(2),
        );
        // Independent admissions bypass the gateway's local Submit mutex. Both
        // candidates actually passed their condition at the identical old V.
        let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
        let a_plan = a.mutation.prepare(&snapshot).unwrap();
        let b_plan = b.mutation.prepare(&snapshot).unwrap();
        let a_plan = graph
            .admit_request(
                a.external_id,
                canonical(&bound(), &a.mutation).unwrap(),
                &a_plan,
            )
            .await
            .unwrap();
        let b_plan = graph
            .admit_request(
                b.external_id,
                canonical(&bound(), &b.mutation).unwrap(),
                &b_plan,
            )
            .await
            .unwrap();
        let _outcomes = tokio::join!(graph.execute(&a_plan), graph.execute(&b_plan));
        // A later-ordinal winner can be Commit but not yet visible because the
        // loser's earlier ordinal is still pending. Inspect decisions, not ACKs.
        let j = journal(&f).await;
        let decisions = [
            j.transaction(a_plan.id()).unwrap().decision,
            j.transaction(b_plan.id()).unwrap().decision,
        ];
        let (winner, loser) = match decisions {
            [Some(Decision::Commit), None] => (a, b),
            [None, Some(Decision::Commit)] => (b, a),
            other => panic!("expected exactly one committed candidate: {other:?}"),
        };
        assert!(matches!(
            call(&g, Operation::Abort(loser)).await.unwrap(),
            Reply::Write {
                status: WriteStatus::Aborted { .. },
                ..
            }
        ));
        committed(&call(&g, Operation::Submit(winner)).await.unwrap());
    }
    g.shutdown().await;
    drop(graph);
    drop(config);
    f.stop().await;
}
