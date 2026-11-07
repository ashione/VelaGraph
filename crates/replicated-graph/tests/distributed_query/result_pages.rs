use super::*;
use distributed::jobs::{self, Output, Request};
async fn send(endpoint: SocketAddr, request: Request) -> anyhow::Result<jobs::Response> {
    Ok(jobs::request(endpoint, request, BUDGET).await?)
}
pub(super) async fn start(
    endpoint: SocketAddr,
    text: &str,
    page_size: usize,
) -> anyhow::Result<Output> {
    Ok(send(
        endpoint,
        Request::StartRows {
            start_partition: 0,
            text: text.into(),
            page_size,
        },
    )
    .await??)
}
pub(super) async fn finish(
    endpoint: SocketAddr,
    mut output: Output,
) -> anyhow::Result<Vec<QualifiedElement>> {
    let Output::Preparing {
        token,
        ref snapshots,
        ..
    } = output
    else {
        anyhow::bail!("not preparing")
    };
    let stamps = snapshots.clone();
    ensure!(matches!(
        send(endpoint, Request::Read { token, cursor: 0 }).await?,
        Err(QueryFailure::InvalidArgument)
    ));
    for _ in 0..10000 {
        let Output::Preparing {
            next_sequence,
            ready,
            ref snapshots,
            ..
        } = output
        else {
            anyhow::bail!("not progress")
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
        ensure!(serde_json::to_value(&replay)? == serde_json::to_value(&output)?);
    }
    ensure!(matches!(output, Output::Preparing { ready: true, .. }));
    let mut all = vec![];
    let mut cursor = 0;
    let mut first = None;
    loop {
        let page = send(endpoint, Request::Read { token, cursor }).await??;
        let replay = send(endpoint, Request::Read { token, cursor }).await??;
        ensure!(serde_json::to_value(&page)? == serde_json::to_value(&replay)?);
        if first.is_none() {
            first = Some(serde_json::to_value(&page)?);
        }
        let Output::Page {
            rows,
            next_cursor,
            done,
            snapshots,
            ..
        } = page
        else {
            anyhow::bail!("not result page")
        };
        ensure!(snapshots == stamps && done == next_cursor.is_none());
        all.extend(rows);
        if done {
            break;
        }
        ensure!(next_cursor.unwrap() > cursor);
        cursor = next_cursor.unwrap();
    }
    // An immutable old offset is replayable after reading later pages.
    ensure!(
        serde_json::to_value(send(endpoint, Request::Read { token, cursor: 0 }).await??)?
            == first.unwrap()
    );
    ensure!(matches!(
        send(
            endpoint,
            Request::Read {
                token,
                cursor: usize::MAX
            }
        )
        .await?,
        Err(QueryFailure::InvalidArgument)
    ));
    ensure!(matches!(
        send(endpoint, Request::Cancel { token }).await??,
        Output::Cancelled
    ));
    ensure!(matches!(
        send(endpoint, Request::Read { token, cursor: 0 }).await?,
        Err(QueryFailure::Unavailable)
    ));
    Ok(all)
}
pub(super) async fn check(endpoint: SocketAddr) -> anyhow::Result<()> {
    for text in [
        "g.V(11,11).repeat(__.out('hierarchy')).emit().times(3)",
        "g.V(12,12).repeat(__.out('hierarchy')).emit().times(3)",
        "g.V(12).repeat(__.in('hierarchy')).emit().times(3)",
        "g.V(12,12).repeat(__.out('hierarchy')).emit().times(3).dedup()",
        "g.V(11).repeat(__.out('hierarchy')).times(2)",
        "g.V(999999).repeat(__.out('hierarchy')).times(2)",
    ] {
        let expected = super::rows(endpoint, text)
            .await
            .with_context(|| format!("sync oracle {text}"))?
            .into_iter()
            .map(|r| match r {
                QueryRow::Element(e) => Ok(e),
                _ => anyhow::bail!("unexpected oracle type"),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        for size in [1, 3, 1000] {
            ensure!(
                finish(
                    endpoint,
                    start(endpoint, text, size)
                        .await
                        .with_context(|| format!("start {text} size {size}"))?
                )
                .await
                .with_context(|| format!("paged {text} size {size}"))?
                    == expected,
                "{text} page size {size}"
            );
        }
    }
    for text in [
        "g.V(10).repeat(__.in('unrelated')).times(2)",
        "g.V(8).repeat(__.out('knows')).emit().times(2)",
    ] {
        let pending = start(endpoint, text, 3)
            .await
            .context("valid-label corruption setup")?;
        let Output::Preparing { token, .. } = pending else {
            unreachable!()
        };
        let error = finish(endpoint, pending).await.unwrap_err();
        ensure!(matches!(
            error.downcast_ref::<QueryFailure>(),
            Some(QueryFailure::IncompleteOrCorrupt)
        ));
        ensure!(matches!(
            send(endpoint, Request::Read { token, cursor: 0 }).await?,
            Err(QueryFailure::Unavailable)
        ));
    }
    for size in [0, 1001] {
        ensure!(start(endpoint, "g.V(11).repeat(__.out()).times(1)", size)
            .await
            .is_err());
    }
    for text in [
        "g.V(11).repeat(__.out()).times(2).count()",
        "g.V(11).repeat(__.out()).times(2).path()",
        "g.V(11).repeat(__.out()).times(2).limit(1)",
    ] {
        ensure!(start(endpoint, text, 3).await.is_err());
    }
    // Ready rows retain the same global two-job admission slots.
    let mut retained = Vec::new();
    for _ in 0..2 {
        let mut output = start(endpoint, "g.V(11).repeat(__.out('hierarchy')).times(1)", 3).await?;
        loop {
            let Output::Preparing {
                token,
                next_sequence,
                ready,
                ..
            } = output
            else {
                unreachable!()
            };
            if ready {
                retained.push(token);
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
        }
    }
    let error = start(endpoint, "g.V(11).repeat(__.out()).times(1)", 3)
        .await
        .unwrap_err();
    ensure!(matches!(
        error.downcast_ref::<QueryFailure>(),
        Some(QueryFailure::ResourceExhausted)
    ));
    ensure!(matches!(
        send(
            endpoint,
            Request::Read {
                token: retained[0],
                cursor: 1
            }
        )
        .await?,
        Err(QueryFailure::InvalidArgument)
    ));
    for token in retained {
        send(endpoint, Request::Cancel { token }).await??;
    }
    let Output::Preparing { token, .. } =
        start(endpoint, "g.V(11).repeat(__.out()).times(1)", 3).await?
    else {
        unreachable!()
    };
    send(endpoint, Request::Cancel { token }).await??;
    Ok(())
}
