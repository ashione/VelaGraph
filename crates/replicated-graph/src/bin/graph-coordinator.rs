//! Trusted-network server for the isolated distributed observed-query slice.
use anyhow::{bail, Context};
use replicated_graph::remote::distributed::{
    query, CoordinatorService, DistributedRequest, StaticCatalog,
};
use std::{net::SocketAddr, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let command = args.next().context("expected serve or query")?;
    let (mut endpoint, mut catalog, mut partition, mut text) = (None, None, None, None);
    let mut timeout_ms = 5000u64;
    let mut trusted = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--allow-insecure-development" => trusted = true,
            "--endpoint" => {
                endpoint = Some(
                    args.next()
                        .context("--endpoint needs a value")?
                        .parse::<SocketAddr>()?,
                )
            }
            "--catalog" => catalog = Some(args.next().context("--catalog needs a file")?),
            "--partition" => {
                partition = Some(
                    args.next()
                        .context("--partition needs a value")?
                        .parse::<u32>()?,
                )
            }
            "--query" => text = Some(args.next().context("--query needs Gremlin")?),
            "--timeout-ms" => {
                timeout_ms = args.next().context("--timeout-ms needs a value")?.parse()?
            }
            _ => bail!("unknown option"),
        }
    }
    anyhow::ensure!(
        trusted,
        "plaintext requires --allow-insecure-development; trusted isolated networks only"
    );
    anyhow::ensure!(
        (1..=5000).contains(&timeout_ms),
        "timeout must be 1..=5000 ms"
    );
    let endpoint = endpoint.context("--endpoint is required")?;
    let timeout = Duration::from_millis(timeout_ms);
    match command.as_str() {
        "serve" => {
            anyhow::ensure!(
                partition.is_none() && text.is_none(),
                "query flags are not accepted by serve"
            );
            let file = std::fs::File::open(catalog.context("--catalog is required")?)?;
            anyhow::ensure!(
                file.metadata()?.len() <= 64 * 1024,
                "catalog configuration exceeds 64 KiB"
            );
            let catalog: StaticCatalog = serde_json::from_reader(file)?;
            let service = CoordinatorService::start(endpoint, catalog, timeout).await?;
            println!("coordinator={}", service.local_addr());
            tokio::signal::ctrl_c().await?;
            service.shutdown().await;
        }
        "query" => {
            anyhow::ensure!(catalog.is_none(), "catalog is server-side configuration");
            let request = DistributedRequest {
                start_partition: partition.context("--partition is required")?,
                text: text.context("--query is required")?,
                timeout_millis: timeout_ms,
            };
            // Typed failure is printed as an error, never as a partial result.
            let output = query(endpoint, request, timeout).await??;
            println!("{}", serde_json::to_string(&output)?);
        }
        _ => bail!("expected serve or query"),
    }
    Ok(())
}
