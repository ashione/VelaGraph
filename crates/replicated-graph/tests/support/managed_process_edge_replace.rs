//! Atomic relationship replacement over real gateway and managed-node TCP.
use super::*;
use rocksdb_sql_table::experimental_graph::EdgeRef;

fn edge(partition: u32, id: u64) -> QueryRow {
    QueryRow::Element(QualifiedElement::Edge { partition, id })
}

async fn assert_view(g: &Gateway, s: &Session, replaced: bool) -> anyhow::Result<()> {
    let old = if replaced { vec![] } else { vec![edge(7, 10)] };
    let new = if replaced { vec![edge(8, 20)] } else { vec![] };
    for (partition, text, expected) in [
        (7, "g.E(10)", old.clone()),
        (7, "g.V(1).outE('reports')", old.clone()),
        (8, "g.V(1).inE('reports')", old),
        (8, "g.E(20)", new.clone()),
        (8, "g.V(2).outE('manages')", new.clone()),
        (7, "g.V(2).inE('manages')", new),
        (7, "g.V(1).outE('parallel')", vec![edge(7, 11)]),
        (8, "g.V(1).inE('parallel')", vec![edge(7, 11)]),
    ] {
        assert_eq!(
            read(g, s, partition, text)
                .await
                .with_context(|| format!("V={} {text}", s.version))?,
            expected,
            "V={} {text}",
            s.version
        );
    }
    Ok(())
}

// Run after the original process scenario has closed every session and dropped
// its cohort. An additional independently scheduled fixture would consume the
// same process-wide eight-client admission budget as unrelated tests.
pub(super) async fn scenario() -> anyhow::Result<()> {
    let _phase = RETAINED_FIXTURE_PHASE.lock().await;
    let mut coordinator = Process::start(9).await?;
    let mut a = Process::start(7).await?;
    let mut b = Process::start(8).await?;
    assert_eq!(
        BTreeSet::from([
            std::process::id(),
            coordinator.child.id(),
            a.child.id(),
            b.child.id()
        ])
        .len(),
        4
    );
    let cc = coordinator.client()?;
    let ac = a.client()?;
    let bc = b.client()?;
    let g = Gateway::bind_remote(
        RemoteConfig {
            coordinator: cc.clone(),
            binding: binding(),
            members: vec![ac.clone(), bc.clone()],
        },
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    for (external, partition, id) in [(1, 7, 1), (2, 8, 1), (3, 7, 2), (4, 8, 2)] {
        committed(
            call(
                &g,
                Operation::Submit(Write {
                    external_id: [external; 16],
                    mutation: Mutation::CreateVertex {
                        partition,
                        vertex: VertexData::from(rocksdb_sql_table::Vertex::new(id)),
                    },
                }),
            )
            .await?,
        );
    }
    for (external, id, label) in [(5, 10, "reports"), (6, 11, "parallel")] {
        committed(
            call(
                &g,
                Operation::Submit(Write {
                    external_id: [external; 16],
                    mutation: Mutation::CreateEdge {
                        edge: EdgeData::from(CrossEdge {
                            id,
                            from: VertexRef {
                                partition: 7,
                                id: 1,
                            },
                            to: VertexRef {
                                partition: 8,
                                id: 1,
                            },
                            label: label.into(),
                            properties: BTreeMap::new(),
                        }),
                    },
                }),
            )
            .await?,
        );
    }
    let old = open(&g, None).await?;
    assert_eq!(old.version, 6);
    assert_view(&g, &old, false).await?;
    let replacement = CrossEdge {
        id: 20,
        from: VertexRef {
            partition: 8,
            id: 2,
        },
        to: VertexRef {
            partition: 7,
            id: 2,
        },
        label: "manages".into(),
        properties: BTreeMap::from([("weight".into(), Value::Integer(42))]),
    };
    let write = Write {
        external_id: [7; 16],
        mutation: Mutation::ReplaceEdge {
            edge: Entity {
                partition: 7,
                id: 10,
            },
            replacement: EdgeData::from(replacement.clone()),
        },
    };
    // Trusted fixture pauses the exact gateway command after only one Prepare.
    // All observable graph assertions still cross the external gateway protocol.
    let graph = GraphCohort::from_endpoints(
        NodeRef::Remote(&cc),
        authority(),
        1,
        vec![
            (NodeRef::Remote(&ac), target(7)),
            (NodeRef::Remote(&bc), target(8)),
        ],
    );
    let snapshot = graph
        .snapshot(None, limit(), Duration::from_secs(60))
        .await?;
    let candidate = snapshot.prepare_edge_replace(
        EdgeRef {
            partition: 7,
            id: 10,
        },
        &replacement,
    )?;
    let canonical = serde_json::to_vec(&("managed-graph-command-v1", binding(), &write.mutation))?;
    let plan = graph
        .admit_request(write.external_id, canonical, &candidate)
        .await?;
    drop(snapshot);
    let c = NodeRef::Remote(&cc);
    let admitted = c.journal(&authority(), 1).await?;
    let record = admitted
        .request(write.external_id)
        .context("replacement record")?
        .clone();
    let durable =
        d2_raft::request_plans::Plan::decode(&record.payload).map_err(|e| anyhow!("{e:?}"))?;
    assert_eq!(
        durable.parts.len(),
        2,
        "one combined CAS per distinct shard"
    );
    let first = &durable.parts[0];
    NodeRef::Remote(&ac)
        .prepare(
            &authority(),
            &first.descriptor,
            first.request.clone(),
            &admitted,
        )
        .await?;
    c.command(
        &authority(),
        1,
        DecisionCommand::Prepared {
            id: plan.id(),
            participant: first.descriptor.clone(),
        },
    )
    .await?;
    assert_view(&g, &old, false).await?;
    a.stop()?;
    coordinator.stop()?;
    coordinator.reopen().await?;
    a.reopen().await?;
    assert_eq!(
        c.journal(&authority(), 1).await?.request(write.external_id),
        Some(&record)
    );
    // Re-prepare the durable first part idempotently, then commit but apply only
    // shard 7. Even this deliberately mixed physical state cannot become a V.
    let recovered = c.journal(&authority(), 1).await?;
    for (part, client) in durable.parts.iter().zip([&ac, &bc]) {
        NodeRef::Remote(client)
            .prepare(
                &authority(),
                &part.descriptor,
                part.request.clone(),
                &recovered,
            )
            .await?;
        c.command(
            &authority(),
            1,
            DecisionCommand::Prepared {
                id: plan.id(),
                participant: part.descriptor.clone(),
            },
        )
        .await?;
    }
    c.command(
        &authority(),
        1,
        DecisionCommand::Decide {
            id: plan.id(),
            decision: Decision::Commit,
        },
    )
    .await?;
    let decided = c.journal(&authority(), 1).await?;
    let ordinal = decided
        .transaction(plan.id())
        .context("replacement transaction")?
        .ordinal;
    assert_eq!(ordinal, 7);
    NodeRef::Remote(&ac)
        .advance(&authority(), &target(7), ordinal, &decided)
        .await?;
    assert!(call(
        &g,
        Operation::OpenSession {
            version: Some(ordinal),
            ttl_millis: 60000
        }
    )
    .await
    .is_err());
    let common = open(&g, None).await?;
    assert_eq!(common.version, old.version);
    assert_view(&g, &common, false).await?;
    call(&g, Operation::Close { session: common }).await?;
    let (retry, observed) = tokio::join!(call(&g, Operation::Submit(write.clone())), async {
        // Concurrent whole-cohort captures may choose old or new, never half.
        for _ in 0..3 {
            assert_view(&g, &old, false).await?;
            let s = open(&g, None).await?;
            assert!([6, 7].contains(&s.version));
            assert_view(&g, &s, s.version == 7).await?;
            call(&g, Operation::Close { session: s }).await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    observed?;
    assert_eq!(committed(retry?), (plan.id(), ordinal));
    let fresh = open(&g, None).await?;
    assert_eq!(fresh.version, ordinal);
    assert_view(&g, &fresh, true).await?;
    assert_eq!(
        read(&g, &fresh, 8, "g.E(20).values('weight')").await?,
        vec![QueryRow::Value {
            source: Some(QualifiedElement::Edge {
                partition: 8,
                id: 20
            }),
            value: Value::Integer(42)
        }]
    );
    assert_eq!(
        committed(call(&g, Operation::Submit(write.clone())).await?),
        (plan.id(), ordinal)
    );
    // Same external ID binds the complete old identity and replacement body.
    for variant in 0..9 {
        let mut changed = write.clone();
        if let Mutation::ReplaceEdge { edge, replacement } = &mut changed.mutation {
            match variant {
                0 => edge.id = 11,
                1 => replacement.from.id = 1,
                2 => replacement.to.id = 1,
                3 => replacement.label = "changed".into(),
                4 => replacement.id = 21,
                5 => edge.partition = 8,
                6 => replacement.from.partition = 7,
                7 => replacement.to.partition = 8,
                _ => {
                    replacement
                        .properties
                        .insert("weight".into(), Value::Integer(43));
                }
            }
        }
        assert_eq!(
            call(&g, Operation::Submit(changed.clone())).await,
            Err(gateway::Error::Identity)
        );
        assert_eq!(
            call(&g, Operation::Status(changed)).await,
            Err(gateway::Error::Identity)
        );
    }
    committed(
        call(
            &g,
            Operation::Submit(Write {
                external_id: [8; 16],
                mutation: Mutation::CreateVertex {
                    partition: 7,
                    vertex: VertexData::from(rocksdb_sql_table::Vertex::new(3)),
                },
            }),
        )
        .await?,
    );
    for process in [&mut a, &mut b, &mut coordinator] {
        process.stop()?;
        process.reopen().await?;
    }
    assert_eq!(
        c.journal(&authority(), 1).await?.request(write.external_id),
        Some(&record)
    );
    assert_eq!(
        committed(call(&g, Operation::Submit(write)).await?),
        (plan.id(), ordinal)
    );
    assert_eq!(c.journal(&authority(), 1).await?.transaction_count(), 8);
    assert_view(&g, &old, false).await?;
    let latest = open(&g, None).await?;
    assert_eq!(latest.version, 8);
    assert_view(&g, &latest, true).await?;
    // Exercise fresh replacement admission entirely through the external wire
    // too, rather than relying only on the controlled recovery fixture above.
    let mut next_edge = replacement;
    next_edge.id = 30;
    let next = Write {
        external_id: [9; 16],
        mutation: Mutation::ReplaceEdge {
            edge: Entity {
                partition: 8,
                id: 20,
            },
            replacement: EdgeData::from(next_edge),
        },
    };
    let next_result = committed(call(&g, Operation::Submit(next.clone())).await?);
    assert_eq!(next_result.1, 9);
    assert_eq!(
        committed(call(&g, Operation::Submit(next)).await?),
        next_result
    );
    assert_view(&g, &latest, true).await?;
    let newest = open(&g, None).await?;
    for (partition, text, expected) in [
        (8, "g.E(20)", vec![]),
        (8, "g.V(2).outE('manages')", vec![edge(8, 30)]),
        (7, "g.V(2).inE('manages')", vec![edge(8, 30)]),
        (7, "g.V(1).outE('parallel')", vec![edge(7, 11)]),
        (8, "g.V(1).inE('parallel')", vec![edge(7, 11)]),
    ] {
        assert_eq!(read(&g, &newest, partition, text).await?, expected);
    }
    g.shutdown().await;
    // Reuse this smaller certified history and the same three processes for
    // fixed-query/replacement interaction, after all earlier assertions finish.
    // Keep the existing phase lock and overall 240-second test deadline.
    data_local_fixed_query_across_managed_processes(&coordinator, &mut a, &mut b).await?;
    Ok(())
}
