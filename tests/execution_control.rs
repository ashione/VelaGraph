use std::time::Duration;
use velagraph::{
    CancellationToken, Error, ExecutionControl, GremlinQuery, IncrementalGraph, IncrementalLimits,
    IncrementalMode, QueryBatchLimits, Vertex,
};
fn graph() -> IncrementalGraph {
    IncrementalGraph::from_records(
        "batch",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vec![Vertex::new(1), Vertex::new(2)],
        vec![],
        vec![],
    )
    .unwrap()
}
#[test]
fn batch_rejects_pre_cancelled_and_expired_controls() {
    let g = graph();
    let q = GremlinQuery::parse("g.V(1)").unwrap();
    let token = CancellationToken::default();
    token.cancel();
    let c = ExecutionControl::new(token, Duration::from_secs(5)).unwrap();
    assert!(matches!(
        g.read().unwrap().execute_query_batch(
            std::slice::from_ref(&q),
            &c,
            QueryBatchLimits::default()
        ),
        Err(Error::ExecutionCancelled)
    ));
    let c = ExecutionControl::new(CancellationToken::default(), Duration::ZERO).unwrap();
    assert!(matches!(
        g.read()
            .unwrap()
            .execute_query_batch(&[q], &c, QueryBatchLimits::default()),
        Err(Error::ExecutionDeadlineExceeded)
    ));
}
#[test]
fn batch_cannot_reset_work_or_result_limits_between_queries() {
    let g = graph();
    let qs = [
        GremlinQuery::parse("g.V(1)").unwrap(),
        GremlinQuery::parse("g.V(2)").unwrap(),
    ];
    let c = ExecutionControl::new(CancellationToken::default(), Duration::from_secs(5)).unwrap();
    for l in [
        QueryBatchLimits {
            max_work_operations: 1,
            ..Default::default()
        },
        QueryBatchLimits {
            max_rows: 1,
            ..Default::default()
        },
    ] {
        assert!(g.read().unwrap().execute_query_batch(&qs, &c, l).is_err());
    }
    let rows = g
        .read()
        .unwrap()
        .execute_query_batch(&qs, &c, QueryBatchLimits::default())
        .unwrap();
    assert_eq!(rows[0], g.read().unwrap().execute_query(&qs[0]).unwrap());
    assert_eq!(rows[1], g.read().unwrap().execute_query(&qs[1]).unwrap());
}

#[test]
fn controlled_memory_write_and_checkpoint_reject_before_publication() {
    let g = graph();
    let before = g.token().unwrap();
    let token = CancellationToken::default();
    token.cancel();
    let c = ExecutionControl::new(token, Duration::from_secs(5)).unwrap();
    assert!(matches!(
        g.apply_with_control(&before, &[], b"cancelled", &c),
        Err(Error::ExecutionCancelled)
    ));
    assert_eq!(g.token().unwrap(), before);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancelled.bin");
    assert!(matches!(
        g.checkpoint_with_control(&path, &c),
        Err(Error::ExecutionCancelled)
    ));
    assert!(!path.exists());
}

#[test]
fn temporal_batches_keep_old_versions_and_stricter_shared_revision_budget() {
    use velagraph::temporal_reference::Interval;
    use velagraph::temporal_store::{TemporalMutation, TemporalStore, TemporalStoreError};
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let changes: Vec<_> = (1..=41)
        .map(|id| TemporalMutation::Vertex {
            valid: Interval {
                from: 10,
                to: Some(20),
            },
            vertex: Vertex::new(id),
        })
        .collect();
    for _ in 0..100 {
        store.commit(&changes).unwrap();
    }
    let control =
        ExecutionControl::new(CancellationToken::default(), Duration::from_secs(5)).unwrap();
    let queries: Vec<_> = (1..=41)
        .map(|id| GremlinQuery::parse(&format!("g.V({id})")).unwrap())
        .collect();
    let view = store.view(Some(100), 0).unwrap();
    assert_eq!(
        view.execute_query_batch(&queries[..40], &control, QueryBatchLimits::default())
            .unwrap()
            .len(),
        40
    );
    assert!(
        matches!(view.execute_query_batch(&queries,&control,QueryBatchLimits::default()),Err(Error::TemporalStore(e)) if matches!(*e,TemporalStoreError::Limit))
    );
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let control = ExecutionControl::new(cancelled, Duration::from_secs(5)).unwrap();
    assert!(matches!(
        store.commit_with_control(100, &[], &control),
        Err(TemporalStoreError::Graph(Error::ExecutionCancelled))
    ));
    assert_eq!(store.view(None, 0).unwrap().version(), 100);
    let control =
        ExecutionControl::new(CancellationToken::default(), Duration::from_secs(5)).unwrap();
    assert!(matches!(
        store.commit_with_control(99, &[], &control),
        Err(TemporalStoreError::Graph(Error::TransactionConflict))
    ));
    store.commit_with_control(100, &[], &control).unwrap();
    assert_eq!(view.version(), 100);
}

#[test]
fn batch_work_and_retained_results_share_byte_budgets() {
    let mut v = Vertex::new(1);
    v.properties
        .insert("text".into(), velagraph::Value::Text("x".repeat(4096)));
    let graph = IncrementalGraph::from_records(
        "bytes",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vec![v],
        vec![],
        vec![],
    )
    .unwrap();
    let view = graph.read().unwrap();
    let q = GremlinQuery::parse("g.V(1).values('text')").unwrap();
    let c = ExecutionControl::new(CancellationToken::default(), Duration::from_secs(30)).unwrap();
    let empty = GremlinQuery::parse("g.V(999)").unwrap();
    for live in [false, true] {
        // Keep query count fixed: only the retained first payload changes.
        // A second result cannot reset the shared work or live budget.
        let mut lo = 1;
        let mut hi = 65536;
        let limits = |n| {
            if live {
                QueryBatchLimits {
                    max_live_bytes: n,
                    ..Default::default()
                }
            } else {
                QueryBatchLimits {
                    max_work_bytes: n,
                    ..Default::default()
                }
            }
        };
        while lo < hi {
            let mid = (lo + hi) / 2;
            if view
                .execute_query_batch(&[empty.clone(), q.clone()], &c, limits(mid))
                .is_ok()
            {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        view.execute_query_batch(&[empty.clone(), q.clone()], &c, limits(lo))
            .unwrap();
        assert!(matches!(
            view.execute_query_batch(&[q.clone(), q.clone()], &c, limits(lo)),
            Err(Error::TraversalLimitExceeded(_))
        ));
    }
    graph
        .apply(
            &graph.token().unwrap(),
            &[velagraph::IncrementalMutation::DeleteVertex(1)],
            &[],
        )
        .unwrap();
    assert_eq!(
        view.execute_query_batch(std::slice::from_ref(&q), &c, QueryBatchLimits::default())
            .unwrap()[0]
            .rows
            .len(),
        1
    );
    assert!(graph
        .read()
        .unwrap()
        .execute_query_batch(&[q], &c, QueryBatchLimits::default())
        .unwrap()[0]
        .rows
        .is_empty());
}
