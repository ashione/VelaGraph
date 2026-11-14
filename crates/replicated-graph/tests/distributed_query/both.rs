//! Explicit-ID both traversal expectations use fixture edge tuples, not the
//! production traversal or a second query implementation.
use super::*;

fn cycle_paths(roots: &[u64], directions: &[&str]) -> Vec<QueryRow> {
    let edges = [
        (7000, (0, 11), (0, 11)),
        (7001, (0, 12), (1, 12)),
        (7002, (0, 12), (1, 12)),
        (7003, (1, 12), (0, 12)),
    ];
    let mut paths: Vec<_> = roots.iter().map(|id| vec![vertex(0, *id)]).collect();
    for direction in directions {
        let mut next = Vec::new();
        for path in paths {
            let current = path.last().unwrap();
            for (id, from, to) in edges {
                let outgoing = *current == vertex(from.0, from.1);
                let incoming = *current == vertex(to.0, to.1);
                if (outgoing && *direction != "in") || (incoming && *direction != "out") {
                    let mut expanded = path.clone();
                    expanded.push(QualifiedElement::Edge {
                        partition: from.0,
                        id,
                    });
                    let target = if outgoing && *direction != "in" {
                        to
                    } else {
                        from
                    };
                    expanded.push(vertex(target.0, target.1));
                    next.push(expanded);
                }
            }
        }
        paths = next;
    }
    paths.into_iter().map(QueryRow::Path).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_both_order_paths_bounds_and_fail_closed() -> anyhow::Result<()> {
    let _guard = PROCESS_TESTS.lock().await;
    let (ae, be) = run_blocking(fixture).await??;
    let a = Group::start(0, 120).await?;
    let b = Group::start(1, 130).await?;
    a.seed(ae).await?;
    b.seed(be).await?;
    let ap = AuditProxy::start(a.route.endpoint).await?;
    let bp = AuditProxy::start(b.route.endpoint).await?;
    let mut ar = a.route.clone();
    let mut br = b.route.clone();
    ar.endpoint = ap.endpoint;
    br.endpoint = bp.endpoint;
    let service = CoordinatorService::start(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: vec![br, ar],
        },
        BUDGET,
    )
    .await?;
    let endpoint = service.local_addr();
    for roots in [vec![11], vec![12], vec![12, 12], vec![11, 12], vec![999]] {
        for directions in [
            vec!["both"],
            vec!["both", "both"],
            vec!["both", "in", "both"],
            vec!["out", "both", "in"],
        ] {
            let ids = roots
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let suffix = directions
                .iter()
                .map(|d| format!(".{d}('hierarchy')"))
                .collect::<String>();
            let text = format!("g.V({ids}){suffix}.path()");
            let opened_a = ap.opened.load(Ordering::SeqCst);
            let opened_b = bp.opened.load(Ordering::SeqCst);
            assert_eq!(
                rows(endpoint, &text).await?,
                cycle_paths(&roots, &directions),
                "{text}"
            );
            assert_eq!(ap.opened.load(Ordering::SeqCst) - opened_a, 1);
            assert_eq!(bp.opened.load(Ordering::SeqCst) - opened_b, 1);
        }
    }
    // Fixture edges 1000..1105 all join (0,7) to (1,1). A default
    // terminal cap affects returned identities, never count or explicit limits.
    assert_eq!(
        rows(endpoint, "g.V(7).both()").await?,
        vec![QueryRow::Element(vertex(1, 1)); 100]
    );
    assert_eq!(
        rows(endpoint, "g.V(7).both().limit(105)").await?,
        vec![QueryRow::Element(vertex(1, 1)); 105]
    );
    assert_eq!(
        rows(endpoint, "g.V(7).both().count()").await?,
        vec![QueryRow::Count(105)]
    );
    assert_eq!(
        rows(endpoint, "g.V(7).both().dedup().count()").await?,
        vec![QueryRow::Count(1)]
    );
    // Edge 20 and 30 each exist under two canonical owners. Stable ordering
    // uses the owner only as a tie-break, and never coalesces same-number IDs.
    let expected = [
        (10, 0, 2),
        (20, 0, 1),
        (20, 1, 1),
        (30, 0, 3),
        (30, 1, 3),
        (40, 0, 1),
    ]
    .into_iter()
    .map(|(id, partition, to)| {
        QueryRow::Path(vec![
            vertex(0, 1),
            QualifiedElement::Edge { partition, id },
            vertex(1, to),
        ])
    })
    .collect::<Vec<_>>();
    assert_eq!(rows(endpoint, "g.V(1).both().path()").await?, expected);
    assert_eq!(
        rows(endpoint, "g.V(12).both().dedup()").await?,
        vec![QueryRow::Element(vertex(1, 12))]
    );
    assert_eq!(
        rows(endpoint, "g.V(11,12).both().dedup().count()").await?,
        vec![QueryRow::Count(2)]
    );
    assert_eq!(
        rows(endpoint, "g.V(11).both().values('nullable')").await?,
        vec![QueryRow::Value {
            source: Some(vertex(0, 11)),
            value: Value::Null
        }]
    );
    assert_eq!(
        rows(endpoint, "g.V(12).both('absent').count()").await?,
        vec![QueryRow::Count(0)]
    );
    assert_eq!(
        rows(
            endpoint,
            "g.V(12).both().hasLabel('absent').valueMap('name')"
        )
        .await?,
        vec![]
    );
    assert_eq!(
        rows(
            endpoint,
            "g.V(11).both().repeat(__.out('hierarchy')).times(30).both().count()"
        )
        .await?,
        vec![QueryRow::Count(1)]
    );

    for (partition, id) in [(0, 9), (0, 40), (0, 41), (0, 43), (0, 44), (1, 8)] {
        for suffix in [
            "both('absent').limit(1).count()",
            "both().dedup().limit(1).count()",
            "both().values('absent')",
        ] {
            let text = format!("g.V({id}).{suffix}");
            let result = distributed::query(
                endpoint,
                DistributedRequest {
                    start_partition: partition,
                    text: text.clone(),
                    timeout_millis: 5000,
                },
                BUDGET,
            )
            .await?;
            assert_eq!(
                result,
                Err(QueryFailure::IncompleteOrCorrupt),
                "{partition}: {text}"
            );
        }
    }
    for suffix in ["count()", "limit(1).count()", "dedup().limit(1).count()"] {
        let text =
            format!("g.V(12).both('hierarchy').repeat(__.out('hierarchy')).times(31).{suffix}");
        let result = distributed::query(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: text.clone(),
                timeout_millis: 5000,
            },
            BUDGET,
        )
        .await?;
        assert_eq!(result, Err(QueryFailure::ResourceExhausted), "{text}");
    }
    for text in [
        "g.V(999).both().both().both().both().count()",
        "g.V(999).both().repeat(__.out()).times(32).count()",
    ] {
        let result = distributed::query(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: text.into(),
                timeout_millis: 5000,
            },
            BUDGET,
        )
        .await?;
        assert_eq!(result, Err(QueryFailure::ResourceExhausted), "{text}");
    }
    for text in [
        "g.V(999).repeat(__.both()).times(1).count()",
        "g.V(999).both('a','b').count()",
        "g.V().hasLabel('absent').both().path()",
        "g.V().hasLabel('absent').repeat(__.both()).times(1).count()",
    ] {
        let result = distributed::query(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: text.into(),
                timeout_millis: 5000,
            },
            BUDGET,
        )
        .await?;
        assert!(
            matches!(
                result,
                Err(QueryFailure::Unsupported | QueryFailure::UnsupportedScope)
            ),
            "{text}: {result:?}"
        );
    }
    ap.rejects_snapshots().await?;
    bp.rejects_snapshots().await?;
    service.shutdown().await;
    Ok(())
}
