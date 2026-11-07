use super::*;
use distributed::jobs::{self, Output, ProjectedRow, Request};
async fn send(endpoint: SocketAddr, request: Request) -> anyhow::Result<jobs::Response> {
    Ok(jobs::request(endpoint, request, BUDGET).await?)
}
pub(super) async fn finish(
    endpoint: SocketAddr,
    mut output: Output,
) -> anyhow::Result<Vec<ProjectedRow>> {
    let Output::Preparing {
        token,
        ref snapshots,
        ..
    } = output
    else {
        anyhow::bail!("expected preparing")
    };
    let stamps = snapshots.clone();
    ensure!(matches!(
        send(endpoint, Request::Read { token, cursor: 0 }).await?,
        Err(QueryFailure::InvalidArgument)
    ));
    for _ in 0..1000 {
        let Output::Preparing {
            next_sequence,
            ready,
            ref snapshots,
            ..
        } = output
        else {
            anyhow::bail!("expected preparing")
        };
        ensure!(snapshots == &stamps);
        if ready {
            break;
        }
        output = send(
            endpoint,
            Request::Advance {
                token,
                sequence: next_sequence,
            },
        )
        .await??;
        let replay = send(
            endpoint,
            Request::Advance {
                token,
                sequence: next_sequence,
            },
        )
        .await??;
        ensure!(serde_json::to_value(&output)? == serde_json::to_value(replay)?);
    }
    ensure!(matches!(output, Output::Preparing { ready: true, .. }));
    let mut all = vec![];
    let mut cursor = 0;
    loop {
        let page = send(endpoint, Request::Read { token, cursor }).await??;
        let replay = send(endpoint, Request::Read { token, cursor }).await??;
        ensure!(serde_json::to_value(&page)? == serde_json::to_value(replay)?);
        let Output::ProjectedPage {
            rows,
            next_cursor,
            snapshots,
            done,
            ..
        } = page
        else {
            anyhow::bail!("wrong result variant")
        };
        ensure!(snapshots == stamps && done == next_cursor.is_none());
        all.extend(rows);
        if done {
            break;
        }
        cursor = next_cursor.unwrap();
    }
    send(endpoint, Request::Cancel { token }).await??;
    ensure!(matches!(
        send(endpoint, Request::Read { token, cursor: 0 }).await?,
        Err(QueryFailure::Unavailable)
    ));
    Ok(all)
}
async fn run(endpoint: SocketAddr, text: &str, size: usize) -> anyhow::Result<Vec<ProjectedRow>> {
    finish(endpoint, result_pages::start(endpoint, text, size).await?).await
}
pub(super) async fn check(endpoint: SocketAddr) -> anyhow::Result<()> {
    let text = "g.V(11,11).repeat(__.out('hierarchy')).emit().times(2).valueMap('nullable','unicode','signed','real','enabled','absent')";
    let expected = ProjectedRow {
        source: vertex(0, 11),
        properties: BTreeMap::from([
            ("nullable".into(), vec![Value::Null]),
            ("unicode".into(), vec![Value::Text("姓名\n\"".into())]),
            ("signed".into(), vec![Value::Integer(i64::MIN)]),
            ("real".into(), vec![Value::Real(0.5)]),
            ("enabled".into(), vec![Value::Boolean(true)]),
        ]),
    };
    for size in [1, 3, 128] {
        ensure!(run(endpoint, text, size).await? == vec![expected.clone(); 4]);
    }
    let maps = run(
        endpoint,
        "g.V(1).repeat(__.out('knows')).times(1).valueMap('name','active')",
        3,
    )
    .await?;
    ensure!(maps.len() == 4 && maps[0].source == vertex(1, 2));
    ensure!(maps[0].properties == BTreeMap::from([("active".into(), vec![Value::Boolean(false)])]));
    ensure!(maps[1] == maps[3] && maps[1].source == vertex(1, 1));
    let absent = run(
        endpoint,
        "g.V(11).repeat(__.out('hierarchy')).times(1).valueMap('absent')",
        128,
    )
    .await?;
    ensure!(absent.len() == 1 && absent[0].properties.is_empty());
    ensure!(run(
        endpoint,
        "g.V(99999).repeat(__.out()).times(1).valueMap('name')",
        128
    )
    .await?
    .is_empty());
    // All eight maximum-length keys are legal. More than one wire batch is
    // required, even though every absent property yields an empty map.
    let long_keys = (0..8)
        .map(|i| format!("'{}{}'", i, "x".repeat(63)))
        .collect::<Vec<_>>()
        .join(",");
    let long_maps = run(
        endpoint,
        &format!("g.V(7).repeat(__.out('knows')).times(1).valueMap({long_keys})"),
        128,
    )
    .await?;
    ensure!(long_maps.len() == 105 && long_maps.iter().all(|row| row.properties.is_empty()));
    let too_large = "g.V(11).repeat(__.out('hierarchy')).times(1).valueMap('large')";
    let pending = result_pages::start(endpoint, too_large, 1).await?;
    let Output::Preparing { token, .. } = pending else {
        unreachable!()
    };
    let error = finish(endpoint, pending).await.unwrap_err();
    ensure!(matches!(
        error.downcast_ref::<QueryFailure>(),
        Some(QueryFailure::ResourceExhausted)
    ));
    ensure!(matches!(
        send(endpoint, Request::Read { token, cursor: 0 }).await?,
        Err(QueryFailure::Unavailable)
    ));
    ensure!(result_pages::start(endpoint, text, 129).await.is_err());
    // The additive synchronous subset must agree with the existing job result.
    let synchronous = distributed::query(
        endpoint,
        DistributedRequest {
            start_partition: 0,
            text: text.into(),
            timeout_millis: 5000,
        },
        BUDGET,
    )
    .await??;
    ensure!(
        synchronous.rows
            == vec![
                QueryRow::PropertyMap {
                    source: expected.source,
                    properties: expected.properties,
                };
                4
            ]
    );
    Ok(())
}
pub(super) async fn fixed_snapshot(endpoint: SocketAddr, group: &Group) -> anyhow::Result<()> {
    // The existing count/ID snapshot test just replaced vertex11 with no properties.
    let text =
        "g.V(11).repeat(__.out('hierarchy')).times(1).dedup().valueMap('nullable','unicode')";
    let pending = result_pages::start(endpoint, text, 1).await?;
    let data = run_blocking(|| -> anyhow::Result<Export> {
        let dir = tempfile::tempdir()?;
        let node = Shared(Arc::new(LocalStorageNode::open(dir.path(), 120)?));
        AppendOnlyStore::new(node.clone(), 120, 0)
            .prepare_vertex(&Vertex {
                id: 11,
                labels: vec![],
                properties: BTreeMap::from([
                    ("nullable".into(), Value::Null),
                    ("unicode".into(), Value::Text("new".into())),
                ]),
            })?
            .submit(&node)?;
        Ok(node.admin().export()?)
    })
    .await??;
    remote::write_identified(
        group.route.endpoint,
        group.route.identity,
        d2_raft::D1WriteRequest {
            shard: group.route.shard,
            epoch: 1,
            request_id: *b"proj-fixed-state",
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
    let pinned = finish(endpoint, pending).await?;
    ensure!(
        pinned.len() == 1 && pinned[0].properties.is_empty(),
        "projection refreshed the closed-over snapshot"
    );
    let fresh = run(endpoint, text, 1).await?;
    ensure!(fresh[0].properties.get("nullable") == Some(&vec![Value::Null]));
    ensure!(fresh[0].properties.get("unicode") == Some(&vec![Value::Text("new".into())]));
    Ok(())
}
