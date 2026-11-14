//! Characterization of current guarantees, NOT a global snapshot-isolation test.
//! The fractured-read assertion intentionally records a known unsupported boundary.
use super::*;
use distributed::jobs::{self, Output, Request};

fn data(partition: u32, shard: u64, revision: i64, edges: bool) -> anyhow::Result<Export> {
    let dir = tempfile::tempdir()?;
    let node = Shared(Arc::new(LocalStorageNode::open(dir.path(), shard)?));
    let store = AppendOnlyStore::new(node.clone(), shard, partition);
    let mut v = Vertex::new(1);
    v.properties
        .insert("revision".into(), Value::Integer(revision));
    store.prepare_vertex(&v)?.submit(&node)?;
    if edges {
        let edge = CrossEdge {
            id: 1,
            from: VertexRef { partition, id: 1 },
            to: VertexRef { partition, id: 1 },
            label: "self".into(),
            properties: BTreeMap::new(),
        };
        store.prepare_source(&edge)?.submit(&node)?;
        store.prepare_reverse(&edge)?.submit(&node)?;
    }
    Ok(node.admin().export()?)
}
async fn update(group: &Group, revision: i64) -> anyhow::Result<()> {
    let partition = group.route.partition;
    let shard = group.route.shard;
    let export = run_blocking(move || data(partition, shard, revision, false)).await??;
    let mut request_id = [0; 16];
    request_id[..8].copy_from_slice(&revision.to_be_bytes());
    request_id[8..].copy_from_slice(&shard.to_be_bytes());
    // Distinguish update IDs from Group::seed IDs, including revision zero.
    request_id[0] = 0x7f;
    typed_write(
        group.route.endpoint,
        group.route.identity,
        d2_raft::D1WriteRequest {
            shard,
            epoch: 1,
            request_id,
            expected_sequence: None,
            operations: export
                .entries
                .into_iter()
                .map(|e| d2_raft::WriteOp::Put {
                    key: e.key,
                    value: e.value,
                })
                .collect(),
        },
        BUDGET,
    )
    .await?;
    Ok(())
}
fn projected(partition: u32, revision: i64) -> QueryRow {
    QueryRow::PropertyMap {
        source: vertex(partition, 1),
        properties: BTreeMap::from([("revision".into(), vec![Value::Integer(revision)])]),
    }
}
async fn job(endpoint: SocketAddr, request: Request) -> anyhow::Result<Output> {
    Ok(jobs::request(endpoint, request, BUDGET).await??)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_consistency_boundaries() -> anyhow::Result<()> {
    let _serial = PROCESS_TESTS.lock().await;
    timeout(Duration::from_secs(90), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let a = Group::start(0, 120).await?;
    let b = Group::start(1, 130).await?;
    a.seed(run_blocking(|| data(0, 120, 0, true)).await??)
        .await?;
    b.seed(run_blocking(|| data(1, 130, 0, true)).await??)
        .await?;
    let ap = AuditProxy::start(a.route.endpoint).await?;
    let bp = AuditProxy::start(b.route.endpoint).await?;
    let mut ar = a.route.clone();
    ar.endpoint = ap.endpoint;
    let mut br = b.route.clone();
    br.endpoint = bp.endpoint;
    let service = CoordinatorService::start(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: vec![ar, br],
        },
        BUDGET,
    )
    .await?;
    let endpoint = service.local_addr();
    const BOTH: &str = "g.V().out('self').valueMap('revision')";
    assert_eq!(
        rows(endpoint, BOTH).await?,
        vec![projected(0, 0), projected(1, 0)]
    );

    // A is frozen before B's OpenQuery reaches its node. Sequential acknowledged
    // writes have real states (0,0), (1,0), (1,1), never (0,1).
    bp.gate.pause_open.store(true, Ordering::SeqCst);
    let mixed = tokio::spawn(async move {
        distributed::consistency::query_with_consistency(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: BOTH.into(),
                timeout_millis: 5000,
            },
            distributed::consistency::ReadConsistency::IndependentShardSnapshots,
            BUDGET,
        )
        .await
    });
    timeout(Duration::from_secs(3), bp.gate.entered.notified()).await?;
    // While a weaker query has frozen only A, strict admission must reject
    // rather than join that incomplete cut or wait for all writes to stop.
    assert!(matches!(
        distributed::consistency::query_with_consistency(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: BOTH.into(),
                timeout_millis: 5000
            },
            distributed::consistency::ReadConsistency::GlobalCommittedSnapshot,
            BUDGET,
        )
        .await?,
        Err(QueryFailure::Unsupported)
    ));
    update(&a, 1).await?;
    update(&b, 1).await?;
    bp.gate.resume.notify_one();
    let observed = mixed.await???.rows;
    assert_eq!(observed, vec![projected(0, 0), projected(1, 1)]);
    for (av, bv) in [(0, 0), (1, 0), (1, 1)] {
        assert_ne!(observed, vec![projected(0, av), projected(1, bv)]);
    }
    assert_eq!(
        rows(endpoint, BOTH).await?,
        vec![projected(0, 1), projected(1, 1)]
    );

    // Positive guarantee: after acquisition a single-shard view is immutable,
    // even when a write completes before the first fragment executes.
    ap.gate.pause.store(true, Ordering::SeqCst);
    let fixed = tokio::spawn(rows(endpoint, "g.V(1).out('self').valueMap('revision')"));
    timeout(Duration::from_secs(3), ap.gate.entered.notified()).await?;
    update(&a, 2).await?;
    ap.gate.resume.notify_one();
    assert_eq!(fixed.await??, vec![projected(0, 1)]);
    assert_eq!(
        rows(endpoint, "g.V(1).valueMap('revision')").await?,
        vec![projected(0, 2)]
    );

    // Positive guarantee: projection and result pagination retain the job's
    // original view, including updates during preparation and between pages.
    let mut state = result_pages::start(
        endpoint,
        "g.V(1,1).repeat(__.out('self')).times(1).valueMap('revision')",
        1,
    )
    .await?;
    let Output::Preparing {
        token,
        ref snapshots,
        ready: false,
        next_sequence: 0,
        ..
    } = state
    else {
        anyhow::bail!("expected preparing");
    };
    let stamps = snapshots.clone();
    update(&a, 3).await?;
    for _ in 0..100 {
        let Output::Preparing {
            ready,
            next_sequence,
            ref snapshots,
            ..
        } = state
        else {
            anyhow::bail!("expected preparing");
        };
        assert_eq!(snapshots, &stamps);
        if ready {
            break;
        }
        state = job(
            endpoint,
            Request::Advance {
                token,
                sequence: next_sequence,
            },
        )
        .await?;
    }
    ensure!(matches!(state, Output::Preparing { ready: true, .. }));
    let first = job(endpoint, Request::Read { token, cursor: 0 }).await?;
    let Output::ProjectedPage {
        ref rows,
        next_cursor: Some(cursor),
        ref snapshots,
        done: false,
        ..
    } = first
    else {
        anyhow::bail!("expected first projected page");
    };
    assert_eq!(snapshots, &stamps);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, vertex(0, 1));
    assert_eq!(
        rows[0].properties,
        BTreeMap::from([("revision".into(), vec![Value::Integer(2)])])
    );
    update(&a, 4).await?;
    let replay = job(endpoint, Request::Read { token, cursor: 0 }).await?;
    assert_eq!(
        serde_json::to_value(&first)?,
        serde_json::to_value(&replay)?
    );
    let Output::ProjectedPage {
        rows: last,
        next_cursor: None,
        snapshots,
        done: true,
        ..
    } = job(endpoint, Request::Read { token, cursor }).await?
    else {
        anyhow::bail!("expected final projected page");
    };
    assert_eq!(snapshots, stamps);
    assert_eq!(&last, rows);
    job(endpoint, Request::Cancel { token }).await?;
    assert_eq!(
        super::rows(endpoint, "g.V(1).valueMap('revision')").await?,
        vec![projected(0, 4)]
    );
    ap.rejects_snapshots().await?;
    bp.rejects_snapshots().await?;
    Ok(())
}
