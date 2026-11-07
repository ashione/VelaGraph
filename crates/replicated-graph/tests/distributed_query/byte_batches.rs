//! Generic wide-edge batches: no CoreHR names or population-dependent execution.
use super::*;
use distributed::jobs::{self, Output, Request};
const N: u64 = 128;
fn fixture() -> anyhow::Result<(Export, Export)> {
    let ad = tempfile::tempdir()?;
    let bd = tempfile::tempdir()?;
    let a = Shared(Arc::new(LocalStorageNode::open(ad.path(), 120)?));
    let b = Shared(Arc::new(LocalStorageNode::open(bd.path(), 130)?));
    let sa = AppendOnlyStore::new(a.clone(), 120, 0);
    let sb = AppendOnlyStore::new(b.clone(), 130, 1);
    sa.prepare_vertex(&Vertex::new(1))?.submit(&a)?;
    sb.prepare_vertex(&Vertex::new(500))?.submit(&b)?;
    for i in 0..N {
        sa.prepare_vertex(&Vertex::new(1000 + i))?.submit(&a)?;
        sb.prepare_vertex(&Vertex::new(2000 + i))?.submit(&b)?;
        for (id, from, to) in [
            (u64::MAX - i, 1, 2000 + i % (N / 2)),
            (u64::MAX - 1000 - i, 1000 + i % (N / 2), 500),
        ] {
            let e = CrossEdge {
                id,
                from: VertexRef {
                    partition: 0,
                    id: from,
                },
                to: VertexRef {
                    partition: 1,
                    id: to,
                },
                label: "x".repeat(64),
                properties: BTreeMap::new(),
            };
            sa.prepare_source(&e)?.submit(&a)?;
            sb.prepare_reverse(&e)?.submit(&b)?;
        }
    }
    // A late missing reverse/canonical entry must still invalidate the entire job
    // after earlier byte-sized batches have succeeded.
    sa.prepare_vertex(&Vertex::new(2))?.submit(&a)?;
    sb.prepare_vertex(&Vertex::new(501))?.submit(&b)?;
    for i in 0..=N {
        let out = CrossEdge {
            id: 4000 + i,
            from: VertexRef {
                partition: 0,
                id: 2,
            },
            to: VertexRef {
                partition: 1,
                id: 2000 + i % N,
            },
            label: "x".repeat(64),
            properties: BTreeMap::new(),
        };
        sa.prepare_source(&out)?.submit(&a)?;
        if i < N {
            sb.prepare_reverse(&out)?.submit(&b)?;
        }
        let incoming = CrossEdge {
            id: 6000 + i,
            from: VertexRef {
                partition: 0,
                id: 1000 + i % N,
            },
            to: VertexRef {
                partition: 1,
                id: 501,
            },
            label: "x".repeat(64),
            properties: BTreeMap::new(),
        };
        sb.prepare_reverse(&incoming)?.submit(&b)?;
        if i < N {
            sa.prepare_source(&incoming)?.submit(&a)?;
        }
    }
    Ok((a.admin().export()?, b.admin().export()?))
}
async fn count(endpoint: SocketAddr, partition: u32, text: &str) -> anyhow::Result<u64> {
    let mut out = jobs::request(
        endpoint,
        Request::Start {
            start_partition: partition,
            text: text.into(),
        },
        BUDGET,
    )
    .await??;
    let Output::Progress { ref snapshots, .. } = out else {
        anyhow::bail!("expected progress")
    };
    let stamps = snapshots.clone();
    for _ in 0..100 {
        let Output::Progress {
            token,
            next_sequence,
            complete,
            count,
            snapshots,
            ..
        } = out
        else {
            anyhow::bail!("expected progress")
        };
        ensure!(snapshots == stamps);
        if complete {
            return count.context("no completed count");
        }
        ensure!(count.is_none());
        out = jobs::request(
            endpoint,
            Request::Advance {
                token,
                sequence: next_sequence,
            },
            BUDGET,
        )
        .await??;
        let replay = jobs::request(
            endpoint,
            Request::Advance {
                token,
                sequence: next_sequence,
            },
            BUDGET,
        )
        .await??;
        ensure!(serde_json::to_value(&out)? == serde_json::to_value(replay)?);
    }
    anyhow::bail!("job did not terminate")
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_labels_and_wide_edges_preserve_both_directions() -> anyhow::Result<()> {
    let _guard = PROCESS_TESTS.lock().await;
    let a = Group::start(0, 120).await?;
    let b = Group::start(1, 130).await?;
    let (ad, bd) = run_blocking(fixture).await??;
    a.seed(ad).await?;
    b.seed(bd).await?;
    let service = CoordinatorService::start(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: vec![a.route.clone(), b.route.clone()],
        },
        BUDGET,
    )
    .await?;
    let endpoint = service.local_addr();
    for (partition, id, direction) in [(0, 1, "out"), (1, 500, "in")] {
        let text = format!(
            "g.V({id}).repeat(__.{direction}('{}')).times(1).count()",
            "x".repeat(64)
        );
        let got = count(endpoint, partition, &text)
            .await
            .with_context(|| format!("{direction} 128 legal wide edges"))?;
        ensure!(got == N, "{direction}: expected {N}, got {got}");
        let row_text = text.strip_suffix(".count()").unwrap();
        ensure!(count(endpoint, partition, &format!("{row_text}.dedup().count()")).await? == N / 2);
        let pending = jobs::request(
            endpoint,
            Request::StartRows {
                start_partition: partition,
                text: row_text.into(),
                page_size: 17,
            },
            BUDGET,
        )
        .await??;
        let actual = result_pages::finish(endpoint, pending).await?;
        let expected: Vec<_> = (0..N)
            .rev()
            .map(|i| {
                if direction == "out" {
                    vertex(1, 2000 + i % (N / 2))
                } else {
                    vertex(0, 1000 + i % (N / 2))
                }
            })
            .collect();
        ensure!(
            actual == expected,
            "{direction} ordered rows lost, duplicated or reordered"
        );
    }
    for (partition, id, direction) in [(0, 2, "out"), (1, 501, "in")] {
        let text = format!(
            "g.V({id}).repeat(__.{direction}('{}')).times(1).count()",
            "x".repeat(64)
        );
        let error = count(endpoint, partition, &text).await.unwrap_err();
        ensure!(
            matches!(
                error.downcast_ref::<QueryFailure>(),
                Some(QueryFailure::IncompleteOrCorrupt)
            ),
            "late corrupt {direction} must not become partial success: {error}"
        );
    }
    service.shutdown().await;
    Ok(())
}
