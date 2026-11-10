use super::*;
use crate::remote::query::QualifiedElement as Q;

fn paged(session: &Session, text: &str, cursor: Option<PageCursor>, page_size: u16) -> Operation {
    Operation::TraversePage {
        session: session.clone(),
        query: query(7, text),
        cursor,
        page_size,
    }
}
fn page(reply: Reply) -> QueryPage {
    match reply {
        Reply::TraversePage { page, .. } => page,
        other => panic!("expected page, got {other:?}"),
    }
}
async fn traverse(g: &Gateway, session: &Session, text: &str) -> Result<Vec<QueryRow>> {
    match call(
        g,
        Operation::Traverse {
            session: session.clone(),
            query: query(7, text),
        },
    )
    .await?
    {
        Reply::Traverse { data, .. } => Ok(data.rows),
        other => panic!("expected traversal, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_fixed_snapshot_validates_every_step_before_execution() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let session = open(&g, Some(0)).await;
    for text in [
        "g.V()",
        "g.E()",
        "g.V(1).outE().inV().path()",
        "g.V(1).id()",
        "g.V(1).repeat(__.in()).emit().times(32).path()",
        "g.E(1).bothV().repeat(__.out()).times(32).dedup().id()",
    ] {
        assert!(traverse(&g, &session, text).await.unwrap().is_empty());
    }
    assert_eq!(
        traverse(&g, &session, "g.V().count()").await.unwrap(),
        vec![QueryRow::Count(0)]
    );
    for (text, expected) in [
        ("g.V(1).inV()", QueryFailure::Unsupported),
        ("g.E(1).outE()", QueryFailure::Unsupported),
        ("g.V(1).values('x').out()", QueryFailure::Unsupported),
        ("g.V(1).id().out()", QueryFailure::Unsupported),
        ("g.E(1).repeat(__.in()).times(1)", QueryFailure::Unsupported),
        (
            "g.V(1).values('x').repeat(__.in()).times(1)",
            QueryFailure::Unsupported,
        ),
        (
            "g.V(1).repeat(__.out()).times(32).out()",
            QueryFailure::ResourceExhausted,
        ),
    ] {
        assert_eq!(
            traverse(&g, &session, text).await,
            Err(Error::Query(expected))
        );
        assert_eq!(
            call(&g, paged(&session, text, None, 1)).await,
            Err(Error::Query(expected))
        );
        assert!(g
            .state
            .get(&session)
            .unwrap()
            .result
            .try_lock()
            .unwrap()
            .is_none());
    }
    for size in [0, MAX_PAGE_SIZE + 1] {
        assert_eq!(
            call(&g, paged(&session, "g.V()", None, size)).await,
            Err(Error::Protocol)
        );
    }
    let first = call(&g, paged(&session, "g.V()", None, 1)).await.unwrap();
    let empty = page(first.clone());
    assert_eq!((empty.offset, empty.total, empty.next), (0, 0, None));
    assert!(empty.data.rows.is_empty());
    assert_eq!(
        call(&g, paged(&session, "g.V()", None, 1)).await.unwrap(),
        first
    );
    close(&g, session).await;
    g.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_pages_replay_bags_fail_busy_and_reject_cross_identity_lifetimes() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    let session = open(&g, None).await;
    let text = "g.V(1,1).path()";
    let expected_path = QueryRow::Path(vec![Q::Vertex {
        partition: 7,
        id: 1,
    }]);
    assert_eq!(
        traverse(&g, &session, text).await.unwrap(),
        vec![expected_path.clone(), expected_path.clone()]
    );
    assert_eq!(
        traverse(&g, &session, "g.V(1,1).count()").await.unwrap(),
        vec![QueryRow::Count(2)]
    );
    // A first-page materialization must reserve this same gate while the
    // result slot is still empty; neither paged nor unpaged work may bypass it.
    let retained = g.state.get(&session).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let guard = retained.result.try_lock().unwrap();
        assert!(guard.is_none());
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        assert!(guard.is_none());
        drop(guard);
    });
    ready_rx.await.unwrap();
    let busy_first = call(&g, paged(&session, text, None, 1)).await;
    let busy_unpaged = traverse(&g, &session, text).await;
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(busy_first, Err(Error::Capacity));
    assert_eq!(busy_unpaged, Err(Error::Capacity));
    assert!(g
        .state
        .get(&session)
        .unwrap()
        .result
        .try_lock()
        .unwrap()
        .is_none());
    let first = call(&g, paged(&session, text, None, 1)).await.unwrap();
    let first_page = page(first.clone());
    assert_eq!(first_page.data.rows, vec![expected_path.clone()]);
    assert_eq!((first_page.offset, first_page.total), (0, 2));
    let cursor = first_page.next.unwrap();
    let continuation = paged(&session, text, Some(cursor.clone()), 1);
    let last = call(&g, continuation.clone()).await.unwrap();
    assert_eq!(page(last.clone()).data.rows, vec![expected_path]);
    assert_eq!(page(last.clone()).next, None);
    assert_eq!(call(&g, continuation.clone()).await.unwrap(), last);

    // Hold the actual result gate in another thread, with explicit readiness
    // and release channels. No scheduling assumption or lock-across-await.
    let retained = g.state.get(&session).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let guard = retained.result.try_lock().unwrap();
        assert!(guard.is_some());
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(guard);
    });
    ready_rx.await.unwrap();
    let busy = call(&g, continuation.clone()).await;
    let busy_unpaged = traverse(&g, &session, "g.V(1).count()").await;
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(busy, Err(Error::Capacity));
    assert_eq!(busy_unpaged, Err(Error::Capacity));
    assert_eq!(
        call(&g, paged(&session, text, None, 1)).await.unwrap(),
        first
    );
    assert_eq!(
        call(&g, paged(&session, text, None, 2)).await,
        Err(Error::Identity)
    );
    assert_eq!(
        call(&g, paged(&session, "g.V(1)", None, 1)).await,
        Err(Error::Identity)
    );
    let mut tampered = cursor.clone();
    tampered.token[0] ^= 1;
    assert_eq!(
        call(&g, paged(&session, text, Some(tampered), 1)).await,
        Err(Error::Identity)
    );
    let other = open(&g, Some(session.version)).await;
    assert_eq!(
        call(&g, paged(&other, text, Some(cursor.clone()), 1)).await,
        Err(Error::Identity)
    );
    let other_first = page(call(&g, paged(&other, text, None, 1)).await.unwrap());
    assert_ne!(other_first.next, Some(cursor.clone()));
    assert_eq!(
        call(&g, paged(&other, text, Some(cursor), 1)).await,
        Err(Error::Identity)
    );
    close(&g, other).await;
    // Unpaged queries neither overwrite nor become restricted by page identity.
    assert_eq!(
        traverse(&g, &session, "g.V(1).count()").await.unwrap(),
        vec![QueryRow::Count(1)]
    );
    assert_eq!(call(&g, continuation.clone()).await.unwrap(), last);
    close(&g, session.clone()).await;
    assert_eq!(
        call(&g, continuation.clone()).await,
        Err(Error::SessionUnavailable)
    );

    let expiring = match call(
        &g,
        Operation::OpenSession {
            version: Some(session.version),
            ttl_millis: 2000,
        },
    )
    .await
    .unwrap()
    {
        Reply::OpenSession(s) => s,
        _ => panic!(),
    };
    let first = page(call(&g, paged(&expiring, text, None, 1)).await.unwrap());
    let expired_continuation = paged(&expiring, text, first.next, 1);
    let expires = g.state.get(&expiring).unwrap().expires;
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        expires + Duration::from_millis(10),
    ))
    .await;
    assert_eq!(
        call(&g, expired_continuation).await,
        Err(Error::SessionUnavailable)
    );

    let before_restart = open(&g, None).await;
    let first = page(
        call(&g, paged(&before_restart, text, None, 1))
            .await
            .unwrap(),
    );
    let stale = paged(&before_restart, text, first.next, 1);
    g.shutdown().await;
    let restarted = f.gateway().await;
    assert_eq!(
        call(&restarted, stale).await,
        Err(Error::SessionUnavailable)
    );
    restarted.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_late_page_rejects_first_request_without_retaining_result() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    // Seed via the trusted local API: the gateway request frame intentionally
    // cannot carry this large property. Escaping expands the late row on wire.
    for (id, text) in [(1, "small".to_owned()), (2, "\0".repeat(174_000))] {
        let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
        let mut vertex = Vertex::new(id);
        vertex.properties.insert("large".into(), Value::Text(text));
        let plan = snapshot.prepare_vertex(7, &vertex).unwrap();
        graph.execute(&plan).await.unwrap();
    }
    let session = open(&g, None).await;
    let text = format!("g.V(1,2).values('large'){}", " ".repeat(12_000));
    let retained = g.state.get(&session).unwrap();
    let data = query::execute_traversal(
        &retained.snapshot,
        &query(7, &text),
        Instant::now() + MAX_DEADLINE,
    )
    .unwrap();
    assert_eq!(data.rows.len(), 2);
    assert!(encode(&data, MAX_RESPONSE_BYTES).is_ok());
    // Prove this is envelope admission, not a whole-result cap: the first
    // frame fits, but the later frame plus the exact echoed query does not.
    for (offset, row) in data.rows.iter().enumerate() {
        let response = Response {
            version: VERSION,
            request_id: [255; 16],
            binding: bound(),
            result: Ok(Reply::TraversePage {
                session: session.clone(),
                query: query(7, &text),
                cursor: (offset > 0).then_some(PageCursor { token: [255; 32] }),
                page_size: 1,
                page: QueryPage {
                    data: QueryData {
                        rows: vec![row.clone()],
                        completeness: QueryCompleteness::Unverified,
                    },
                    next: (offset == 0).then_some(PageCursor { token: [255; 32] }),
                    offset: offset as u32,
                    total: 2,
                },
            }),
        };
        assert_eq!(encode(&response, MAX_RESPONSE_BYTES).is_ok(), offset == 0);
    }
    assert_eq!(
        call(&g, paged(&session, &text, None, 1)).await,
        Err(Error::Capacity)
    );
    assert!(retained.result.try_lock().unwrap().is_none());
    assert_eq!(
        page(call(&g, paged(&session, "g.V(1)", None, 1)).await.unwrap())
            .data
            .rows,
        vec![v(7, 1)]
    );
    drop(retained);
    close(&g, session).await;
    g.shutdown().await;
    drop(config);
    f.stop().await;
}

fn synthetic(operation: Operation, reply: Reply) -> Result<Reply> {
    let request = Request {
        version: VERSION,
        request_id: [5; 16],
        binding: bound(),
        operation,
    };
    validate_reply(
        &request,
        Response {
            version: VERSION,
            request_id: request.request_id,
            binding: request.binding.clone(),
            result: Ok(reply),
        },
    )
}
fn synthetic_session() -> Session {
    Session {
        incarnation: [1; 16],
        token: [2; 32],
        version: 3,
    }
}

#[test]
fn forged_traversal_rows_and_paths_fail_closed_but_duplicate_paths_are_legal() {
    let session = synthetic_session();
    let text = "g.V(1).outE().inV().path()";
    let valid = vec![
        Q::Vertex {
            partition: 7,
            id: 1,
        },
        Q::Edge {
            partition: 7,
            id: 10,
        },
        Q::Vertex {
            partition: 8,
            id: 1,
        },
    ];
    let operation = Operation::Traverse {
        session: session.clone(),
        query: query(7, text),
    };
    let reply = |rows| Reply::Traverse {
        session: session.clone(),
        query: query(7, text),
        data: QueryData {
            rows,
            completeness: QueryCompleteness::Unverified,
        },
    };
    let bag = reply(vec![
        QueryRow::Path(valid.clone()),
        QueryRow::Path(valid.clone()),
    ]);
    assert_eq!(synthetic(operation.clone(), bag.clone()).unwrap(), bag);
    let mut wrong_start = valid.clone();
    wrong_start[0] = Q::Vertex {
        partition: 7,
        id: 99,
    };
    let mut wrong_type = valid.clone();
    wrong_type[1] = Q::Vertex {
        partition: 7,
        id: 10,
    };
    let mut wrong_owner = valid.clone();
    wrong_owner[1] = Q::Edge {
        partition: 8,
        id: 10,
    };
    let mut wrong_partition = valid.clone();
    wrong_partition[2] = Q::Vertex {
        partition: 99,
        id: 1,
    };
    for row in [
        v(7, 1),
        QueryRow::Count(2),
        QueryRow::Path(vec![]),
        QueryRow::Path(valid[..2].to_vec()),
        QueryRow::Path(wrong_start),
        QueryRow::Path(wrong_type),
        QueryRow::Path(wrong_partition),
        QueryRow::Path(wrong_owner),
    ] {
        assert_eq!(
            synthetic(operation.clone(), reply(vec![row])),
            Err(Error::Identity)
        );
    }
    // A normal element query must not accept a path-shaped or scalar reply.
    for row in [QueryRow::Path(valid), QueryRow::Count(1), e(7, 1)] {
        assert_eq!(
            synthetic(
                Operation::Traverse {
                    session: session.clone(),
                    query: query(7, "g.V(1)")
                },
                Reply::Traverse {
                    session: session.clone(),
                    query: query(7, "g.V(1)"),
                    data: QueryData {
                        rows: vec![row],
                        completeness: QueryCompleteness::Unverified
                    }
                },
            ),
            Err(Error::Identity)
        );
    }
}

#[test]
fn forged_page_offsets_cursors_cardinality_and_echoes_fail_closed() {
    let session = synthetic_session();
    let text = "g.V(1,2)";
    let operation = paged(&session, text, None, 1);
    let original = Reply::TraversePage {
        session: session.clone(),
        query: query(7, text),
        cursor: None,
        page_size: 1,
        page: QueryPage {
            data: QueryData {
                rows: vec![v(7, 1)],
                completeness: QueryCompleteness::Unverified,
            },
            next: Some(PageCursor { token: [3; 32] }),
            offset: 0,
            total: 2,
        },
    };
    assert_eq!(
        synthetic(operation.clone(), original.clone()).unwrap(),
        original
    );
    for variant in 0..16 {
        let mut forged = original.clone();
        let Reply::TraversePage {
            session,
            query,
            cursor,
            page_size,
            page,
        } = &mut forged
        else {
            unreachable!()
        };
        match variant {
            0 => session.version += 1,
            1 => query.text = "g.V(2)".into(),
            2 => *cursor = Some(PageCursor { token: [4; 32] }),
            3 => *page_size = 2,
            4 => page.offset = 1,
            5 => page.total = 0,
            6 => page.total = 10_001,
            7 => page.total = 10_000, // Within the global cap, outside this query's bound.
            8 => page.next = None,
            9 => page.next = Some(PageCursor { token: [0; 32] }),
            10 => page.data.rows.clear(),
            11 => page.data.rows.push(v(7, 2)),
            12 => page.data.rows = vec![e(7, 1)],
            13 => page.data.completeness = QueryCompleteness::SinglePartition,
            14 => page.data.rows = vec![v(99, 1)],
            _ => page.data.rows = vec![v(7, 99)],
        }
        assert_eq!(
            synthetic(operation.clone(), forged),
            Err(Error::Identity),
            "variant {variant}"
        );
    }
    // A continuation cannot loop to its own token or pretend it is page zero.
    let token = PageCursor { token: [4; 32] };
    let operation = paged(&session, "g.V(1,2,3)", Some(token.clone()), 1);
    for offset in [0, 1] {
        let reply = Reply::TraversePage {
            session: session.clone(),
            query: query(7, "g.V(1,2,3)"),
            cursor: Some(token.clone()),
            page_size: 1,
            page: QueryPage {
                data: QueryData {
                    rows: vec![v(7, 2)],
                    completeness: QueryCompleteness::Unverified,
                },
                next: Some(token.clone()),
                offset,
                total: 3,
            },
        };
        assert_eq!(synthetic(operation.clone(), reply), Err(Error::Identity));
    }
    // The count result's cardinality is one, not its numeric count value.
    let operation = paged(&session, "g.V().count()", None, 1);
    for total in [0, 2] {
        let reply = Reply::TraversePage {
            session: session.clone(),
            query: query(7, "g.V().count()"),
            cursor: None,
            page_size: 1,
            page: QueryPage {
                data: QueryData {
                    rows: vec![QueryRow::Count(42)],
                    completeness: QueryCompleteness::Unverified,
                },
                next: (total > 1).then_some(PageCursor { token: [3; 32] }),
                offset: 0,
                total,
            },
        };
        assert_eq!(synthetic(operation.clone(), reply), Err(Error::Identity));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn id_projection_keeps_fixed_version_bags_pages_and_overflow_errors() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    let old = open(&g, None).await;
    let text = "g.V(1,1).id()";
    let row = QueryRow::Value {
        source: Some(Q::Vertex {
            partition: 7,
            id: 1,
        }),
        value: Value::Integer(1),
    };
    assert_eq!(
        traverse(&g, &old, text).await.unwrap(),
        vec![row.clone(), row.clone()]
    );
    let first = page(call(&g, paged(&old, text, None, 1)).await.unwrap());
    assert_eq!(first.data.rows, vec![row.clone()]);
    committed(
        &call(&g, Operation::Submit(create(3, 7, i64::MAX as u64)))
            .await
            .unwrap(),
    );
    committed(
        &call(&g, Operation::Submit(create(2, 7, i64::MAX as u64 + 1)))
            .await
            .unwrap(),
    );
    let next = page(call(&g, paged(&old, text, first.next, 1)).await.unwrap());
    assert_eq!(next.data.rows, vec![row.clone()]);
    assert!(next.next.is_none());
    assert_eq!(
        traverse(&g, &old, "g.V().id()").await.unwrap(),
        vec![row.clone()]
    );
    let fresh = open(&g, None).await;
    assert_eq!(
        traverse(&g, &fresh, &format!("g.V({}).id()", i64::MAX))
            .await
            .unwrap(),
        vec![QueryRow::Value {
            source: Some(Q::Vertex {
                partition: 7,
                id: i64::MAX as u64
            }),
            value: Value::Integer(i64::MAX)
        }]
    );
    for query in ["g.V().id()", "g.V().id().count()", "g.V().id().limit(1)"] {
        assert_eq!(
            traverse(&g, &fresh, query).await,
            Err(Error::Query(QueryFailure::Unsupported))
        );
    }
    assert_eq!(
        traverse(&g, &fresh, "g.V().limit(1).id()").await.unwrap(),
        vec![row]
    );
    close(&g, old).await;
    close(&g, fresh).await;
    g.shutdown().await;
    f.stop().await;
}

#[test]
fn forged_id_values_must_match_qualified_source() {
    let session = synthetic_session();
    let operation = Operation::Traverse {
        session: session.clone(),
        query: query(7, "g.V(1).id()"),
    };
    for value in [
        Value::Integer(2),
        Value::Integer(-1),
        Value::Text("1".into()),
        Value::Null,
    ] {
        let reply = Reply::Traverse {
            session: session.clone(),
            query: query(7, "g.V(1).id()"),
            data: QueryData {
                rows: vec![QueryRow::Value {
                    source: Some(Q::Vertex {
                        partition: 7,
                        id: 1,
                    }),
                    value,
                }],
                completeness: QueryCompleteness::Unverified,
            },
        };
        assert_eq!(synthetic(operation.clone(), reply), Err(Error::Identity));
    }
}

#[test]
fn repeat_reply_validation_preserves_each_emitted_shape_and_canonical_owner() {
    let session = synthetic_session();
    let text = "g.V(1).out().repeat(__.in()).emit().times(2).out().path()";
    let operation = Operation::Traverse {
        session: session.clone(),
        query: query(7, text),
    };
    let reply = |rows| Reply::Traverse {
        session: session.clone(),
        query: query(7, text),
        data: QueryData {
            rows,
            completeness: QueryCompleteness::Unverified,
        },
    };
    let vertex = |partition, id| Q::Vertex { partition, id };
    let edge = |partition, id| Q::Edge { partition, id };
    let short = vec![
        vertex(7, 1),
        edge(7, 10),
        vertex(8, 2),
        edge(7, 11),
        vertex(7, 3),
        edge(7, 12),
        vertex(8, 4),
    ];
    let long = vec![
        vertex(7, 1),
        edge(7, 10),
        vertex(8, 2),
        edge(7, 11),
        vertex(7, 3),
        edge(8, 12),
        vertex(8, 4),
        edge(8, 13),
        vertex(7, 5),
    ];
    let good = reply(vec![
        QueryRow::Path(short.clone()),
        QueryRow::Path(long.clone()),
        QueryRow::Path(short.clone()),
    ]);
    assert_eq!(synthetic(operation.clone(), good.clone()).unwrap(), good);
    for base in [short, long] {
        for position in (1..base.len()).step_by(2) {
            let mut bad = base.clone();
            let Q::Edge { partition, .. } = &mut bad[position] else {
                unreachable!()
            };
            *partition = if *partition == 7 { 8 } else { 7 };
            assert_eq!(
                synthetic(operation.clone(), reply(vec![QueryRow::Path(bad)])),
                Err(Error::Identity)
            );
        }
        for length in [0, base.len() - 1] {
            assert_eq!(
                synthetic(
                    operation.clone(),
                    reply(vec![QueryRow::Path(base[..length].to_vec())])
                ),
                Err(Error::Identity)
            );
        }
    }
    // Repeat expands after a prefix limit; only a suffix limit bounds its output.
    for (text, total, expected) in [
        ("g.V(1).limit(1).repeat(__.out()).times(2)", 10_000, true),
        ("g.V(1).repeat(__.out()).times(2).limit(1)", 2, false),
        ("g.V(1).repeat(__.out()).times(2)", 101, false),
        ("g.V(1).repeat(__.out()).times(2).count()", 1, true),
    ] {
        assert_eq!(
            super::query::valid_traversal_total(&query(7, text), total),
            expected
        );
    }
    let text = "g.E(10).inV().repeat(__.out()).times(32).path()";
    let mut elements = vec![edge(7, 10), vertex(8, 1)];
    for id in 0..32 {
        elements.extend([edge(8, id), vertex(8, id + 2)]);
    }
    assert_eq!(elements.len(), 66);
    let operation = Operation::Traverse {
        session: session.clone(),
        query: query(7, text),
    };
    let reply = Reply::Traverse {
        session: session.clone(),
        query: query(7, text),
        data: QueryData {
            rows: vec![QueryRow::Path(elements)],
            completeness: QueryCompleteness::Unverified,
        },
    };
    assert_eq!(synthetic(operation, reply.clone()).unwrap(), reply);
}
