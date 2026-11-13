//! Explicit single-shard typed RPC commands, independent of legacy catalog routing.
use super::*;
use d2_raft::D1WriteRequest;
use replicated_graph::remote::{operations_grpc, RemoteIdentity};

pub(super) async fn run(words: &[String], mut opts: BTreeMap<String, String>) -> Result<Json> {
    let words: Vec<&str> = words.iter().map(String::as_str).collect();
    let operation = match words.as_slice() {
        ["rpc", op @ ("probe" | "metadata" | "write")] => *op,
        _ => bail!("unknown rpc command; use --help"),
    };
    let endpoint: SocketAddr = take(&mut opts, "--endpoint")?.parse()?;
    ensure!(
        endpoint.port() != 0 && !endpoint.ip().is_unspecified(),
        "endpoint must have a concrete address and nonzero port"
    );
    let identity = RemoteIdentity {
        group: number(&take(&mut opts, "--group")?)?,
        node: number(&take(&mut opts, "--node")?)?,
    };
    let shard: u64 = number(&take(&mut opts, "--shard")?)?;
    let timeout_ms: u64 = opts
        .remove("--timeout-ms")
        .map(|s| number(&s))
        .transpose()?
        .unwrap_or(3000);
    ensure!(
        (1..=60_000).contains(&timeout_ms),
        "timeout must be 1..=60000 ms"
    );
    // Validate all local arguments before opening a channel or submitting a write.
    let write: Option<D1WriteRequest> = if operation == "write" {
        let request: D1WriteRequest = read_json(&take(&mut opts, "--file")?)?;
        ensure!(request.shard == shard, "write shard differs from --shard");
        Some(request)
    } else {
        None
    };
    ensure!(opts.is_empty(), "unknown or unused rpc option");
    let request_id = write.as_ref().map(|request| request.request_id);
    let output = tokio::time::timeout(Duration::from_millis(timeout_ms), async move {
    let mut client =
        operations_grpc::Client::connect(endpoint, identity, Duration::from_millis(timeout_ms))
            .await?;
    let output = match operation {
        "probe" => {
            client.probe(shard).await?;
            json!({"shard":shard,"leader_barrier":true})
        }
        "metadata" => {
            let (actual, epoch, sequence) = client.metadata(shard).await?;
            json!({"shard":actual,"epoch":epoch,"sequence":sequence,"consistency":"local_applied"})
        }
        "write" => serde_json::to_value(client.write(write.expect("validated write")).await?)?,
        _ => unreachable!(),
    };
    anyhow::Ok(output)
    }).await.map_err(|_| remote::RemoteError::Timeout { request_id })??;
    Ok(json!({"version":1,"ok":true,"transport":"grpc","operation":operation,"output":output}))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn opts() -> BTreeMap<String, String> {
        [
            ("--endpoint", "127.0.0.1:1"),
            ("--group", "1"),
            ("--node", "1"),
            ("--shard", "7"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
    }
    #[tokio::test]
    async fn rejects_bad_local_arguments_before_dispatch() {
        let words = vec!["rpc".into(), "probe".into()];
        for value in ["0", "60001"] {
            let mut args = opts();
            args.insert("--timeout-ms".into(), value.into());
            assert!(run(&words, args)
                .await
                .unwrap_err()
                .to_string()
                .contains("timeout must"));
        }
        let mut args = opts();
        args.insert("--endpoint".into(), "0.0.0.0:1".into());
        assert!(run(&words, args)
            .await
            .unwrap_err()
            .to_string()
            .contains("concrete address"));
        let mut args = opts();
        args.insert("--config".into(), "legacy.json".into());
        assert!(run(&words, args)
            .await
            .unwrap_err()
            .to_string()
            .contains("unused rpc option"));
    }
}
