use super::*;
use distributed::jobs::{self, Output, Request};
async fn send(endpoint: SocketAddr, request: Request) -> anyhow::Result<jobs::Response> {
    Ok(jobs::request(endpoint, request, Duration::from_secs(5)).await?)
}
async fn run(endpoint: SocketAddr, text: &str) -> anyhow::Result<u64> {
    let response = send(
        endpoint,
        Request::Start {
            start_partition: 0,
            text: text.into(),
        },
    )
    .await??;
    finish(endpoint, response).await
}
async fn finish(endpoint: SocketAddr, mut response: Output) -> anyhow::Result<u64> {
    let Output::Progress { ref snapshots, .. } = response else {
        anyhow::bail!("expected start");
    };
    let stamps = snapshots.clone();
    for _ in 0..1000 {
        let Output::Progress {
            token,
            next_sequence,
            complete,
            count,
            completeness,
            snapshots,
        } = response
        else {
            anyhow::bail!("unexpected cancellation");
        };
        ensure!(completeness == QueryCompleteness::Unverified && snapshots == stamps);
        if complete {
            return count.context("complete count missing");
        }
        ensure!(count.is_none(), "progress must not expose a partial count");
        response = send(
            endpoint,
            Request::Advance {
                token,
                sequence: next_sequence,
            },
        )
        .await??;
        // At-most-once frontier mutation, including a lost final response.
        let replay = send(
            endpoint,
            Request::Advance {
                token,
                sequence: next_sequence,
            },
        )
        .await??;
        ensure!(serde_json::to_value(&replay)? == serde_json::to_value(&response)?);
    }
    anyhow::bail!("job did not terminate")
}
pub(super) async fn check(endpoint: SocketAddr) -> anyhow::Result<()> {
    for (text, expected) in [
        ("g.V(11).repeat(__.out('hierarchy')).times(32).count()", 1),
        (
            "g.V(11,11).repeat(__.out('hierarchy')).emit().times(32).count()",
            64,
        ),
        (
            "g.V(11).repeat(__.out('hierarchy')).emit().times(32).dedup().count()",
            1,
        ),
        (
            "g.V(12).repeat(__.out('hierarchy')).emit().times(32).count()",
            262140,
        ),
        (
            "g.V(12).repeat(__.out('hierarchy')).times(32).count()",
            65536,
        ),
        (
            "g.V(12).repeat(__.in('hierarchy')).emit().times(32).dedup().count()",
            2,
        ),
        (
            "g.V(12,12).repeat(__.out('hierarchy')).emit().times(32).dedup().count()",
            2,
        ),
        (
            "g.V(12).repeat(__.out('hierarchy')).emit().times(1).dedup().count()",
            1,
        ),
        (
            "g.V(12).repeat(__.out('hierarchy')).emit().times(2).dedup().count()",
            2,
        ),
        (
            "g.V(999).repeat(__.out()).emit().times(32).dedup().count()",
            0,
        ),
        ("g.V(999).repeat(__.out()).times(32).count()", 0),
        (
            "g.V(1).repeat(__.out('missing')).emit().times(2).count()",
            0,
        ),
    ] {
        ensure!(run(endpoint, text).await? == expected, "{text}");
    }
    for text in [
        "g.V(999).repeat(__.out()).times(2).range(0,0).count()",
        "g.V().count()",
        "g.V(11).repeat(__.out()).times(2)",
        "g.V(11).repeat(__.out()).times(2).limit(1).count()",
    ] {
        ensure!(matches!(
            send(
                endpoint,
                Request::Start {
                    start_partition: 0,
                    text: text.into()
                }
            )
            .await?,
            Err(QueryFailure::Unsupported)
        ));
    }
    for text in [
        "g.V(10).repeat(__.in('unrelated')).times(2).count()",
        "g.V(8).repeat(__.out('knows')).times(2).count()",
        "g.V(10).repeat(__.in('unrelated')).emit().times(2).dedup().count()",
        "g.V(8).repeat(__.out('knows')).emit().times(2).dedup().count()",
    ] {
        let error = run(endpoint, text).await.unwrap_err();
        ensure!(
            matches!(
                error.downcast_ref::<QueryFailure>(),
                Some(QueryFailure::IncompleteOrCorrupt)
            ),
            "{text}: {error}"
        );
    }
    let Output::Progress { token, .. } = send(
        endpoint,
        Request::Start {
            start_partition: 0,
            text: "g.V(11).repeat(__.out()).times(32).count()".into(),
        },
    )
    .await??
    else {
        anyhow::bail!("start");
    };
    ensure!(matches!(
        send(endpoint, Request::Advance { token, sequence: 1 }).await?,
        Err(QueryFailure::InvalidArgument)
    ));
    ensure!(matches!(
        send(endpoint, Request::Cancel { token }).await??,
        Output::Cancelled
    ));
    ensure!(matches!(
        send(endpoint, Request::Advance { token, sequence: 0 }).await?,
        Err(QueryFailure::Unavailable)
    ));
    Ok(())
}

pub(super) async fn fixed_snapshot(endpoint: SocketAddr, group: &Group) -> anyhow::Result<()> {
    let text = "g.V(11).repeat(__.out('hierarchy')).times(2).count()";
    let row_pending =
        result_pages::start(endpoint, "g.V(11).repeat(__.out('hierarchy')).times(2)", 1).await?;
    let pending = send(
        endpoint,
        Request::Start {
            start_partition: 0,
            text: text.into(),
        },
    )
    .await??;
    let data = run_blocking(|| -> anyhow::Result<Export> {
        let dir = tempfile::tempdir()?;
        let node = Shared(Arc::new(LocalStorageNode::open(dir.path(), 120)?));
        let store = AppendOnlyStore::new(node.clone(), 120, 0);
        store
            .prepare_vertex(&Vertex {
                id: 11,
                labels: vec!["person".into()],
                properties: BTreeMap::new(),
            })?
            .submit(&node)?;
        let edge = CrossEdge {
            id: 7004,
            from: VertexRef {
                partition: 0,
                id: 11,
            },
            to: VertexRef {
                partition: 0,
                id: 11,
            },
            label: "hierarchy".into(),
            properties: BTreeMap::new(),
        };
        store.prepare_source(&edge)?.submit(&node)?;
        store.prepare_reverse(&edge)?.submit(&node)?;
        Ok(node.admin().export()?)
    })
    .await??;
    remote::write_identified(
        group.route.endpoint,
        group.route.identity,
        d2_raft::D1WriteRequest {
            shard: group.route.shard,
            epoch: 1,
            request_id: *b"job-fixed-update",
            expected_sequence: None,
            operations: data
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
    ensure!(
        finish(endpoint, pending).await? == 1,
        "job refreshed a pinned snapshot"
    );
    ensure!(
        result_pages::finish(endpoint, row_pending).await? == vec![vertex(0, 11)],
        "row job refreshed snapshot"
    );
    ensure!(
        run(endpoint, text).await? == 4,
        "fresh job did not observe both self loops"
    );
    let error = run(
        endpoint,
        "g.V(11).repeat(__.out('hierarchy')).times(32).count()",
    )
    .await
    .unwrap_err();
    ensure!(
        matches!(
            error.downcast_ref::<QueryFailure>(),
            Some(QueryFailure::ResourceExhausted)
        ),
        "weighted path mass was not bounded: {error}"
    );
    // Even a one-identity dedup result cannot erase the path-mass ceiling.
    let error = run(
        endpoint,
        "g.V(11).repeat(__.out('hierarchy')).emit().times(32).dedup().count()",
    )
    .await
    .unwrap_err();
    ensure!(
        matches!(
            error.downcast_ref::<QueryFailure>(),
            Some(QueryFailure::ResourceExhausted)
        ),
        "compact dedup bypassed path mass: {error}"
    );
    Ok(())
}
