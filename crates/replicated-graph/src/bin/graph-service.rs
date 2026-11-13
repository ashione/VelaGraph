//! Fixed-topology loopback managed HTTP/gRPC service preview. This process attaches
//! to already activated live managed nodes; it never bootstraps or restores them.
use anyhow::{bail, Context};
use d2_raft::managed_rpc::{Client, Identity};
use replicated_graph::{
    managed_gateway::{Binding, Gateway, LocalGraphRoute, RemoteConfig},
    remote::RemoteIdentity,
};
use serde::Deserialize;
use std::{
    io::{Read, Write},
    net::SocketAddr,
    path::Path,
    time::Duration,
};

const MAX_CONFIG_BYTES: usize = 64 * 1024;
#[derive(Debug, PartialEq)]
struct Options {
    config: String,
    http: SocketAddr,
    grpc: Option<SocketAddr>,
    timeout_ms: u64,
}
fn options(mut args: impl Iterator<Item = String>) -> anyhow::Result<Options> {
    let (mut config, mut http, mut grpc, mut timeout_ms) = (None, None, None, None);
    let mut trusted = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--allow-insecure-development" if !trusted => trusted = true,
            "--config" if config.is_none() => {
                config = Some(args.next().context("--config needs a file")?)
            }
            "--http" if http.is_none() => {
                http = Some(
                    args.next()
                        .context("--http needs an address")?
                        .parse::<SocketAddr>()?,
                )
            }
            "--grpc" if grpc.is_none() => {
                grpc = Some(
                    args.next()
                        .context("--grpc needs an address")?
                        .parse::<SocketAddr>()?,
                );
            }
            "--timeout-ms" if timeout_ms.is_none() => {
                timeout_ms = Some(
                    args.next()
                        .context("--timeout-ms needs a value")?
                        .parse::<u64>()?,
                )
            }
            _ => bail!("unknown or repeated argument: {arg}"),
        }
    }
    anyhow::ensure!(
        trusted,
        "loopback plaintext service requires --allow-insecure-development"
    );
    let http = http.context("--http is required; no default public listener")?;
    anyhow::ensure!(http.ip().is_loopback(), "--http must be loopback");
    anyhow::ensure!(
        grpc.is_none_or(|address| address.ip().is_loopback()),
        "--grpc must be loopback"
    );
    let timeout_ms = timeout_ms.unwrap_or(15_000);
    anyhow::ensure!(
        (1..=60_000).contains(&timeout_ms),
        "--timeout-ms must be 1..=60000"
    );
    Ok(Options {
        config: config.context("--config is required")?,
        http,
        grpc,
        timeout_ms,
    })
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoint {
    endpoint: SocketAddr,
    identity: Identity,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberEndpoint {
    endpoint: SocketAddr,
    identity: Identity,
    graph_endpoint: SocketAddr,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceConfig {
    binding: Binding,
    coordinator: Endpoint,
    members: Vec<MemberEndpoint>,
}
fn read_config(path: &Path) -> anyhow::Result<ServiceConfig> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((MAX_CONFIG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= MAX_CONFIG_BYTES,
        "service configuration exceeds 64 KiB"
    );
    let config: ServiceConfig =
        serde_json::from_slice(&bytes).context("invalid service configuration")?;
    anyhow::ensure!(
        config.coordinator.endpoint.ip().is_loopback()
            && config
                .members
                .iter()
                .all(|m| m.endpoint.ip().is_loopback() && m.graph_endpoint.ip().is_loopback()),
        "preview managed/graph routes must all be loopback"
    );
    Ok(config)
}
impl ServiceConfig {
    fn routes(self, deadline: Duration) -> anyhow::Result<(RemoteConfig, Vec<LocalGraphRoute>)> {
        let coordinator = Client::new(
            self.coordinator.endpoint,
            self.coordinator.identity,
            deadline,
        )?;
        let mut members = Vec::with_capacity(self.members.len());
        let mut routes = Vec::with_capacity(self.members.len());
        for member in self.members {
            routes.push(LocalGraphRoute {
                endpoint: member.graph_endpoint,
                identity: RemoteIdentity {
                    group: member.identity.group,
                    node: member.identity.node,
                },
                managed_identity: member.identity.clone(),
            });
            members.push(Client::new(member.endpoint, member.identity, deadline)?);
        }
        Ok((
            RemoteConfig {
                coordinator,
                binding: self.binding,
                members,
            },
            routes,
        ))
    }
}
async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result=tokio::signal::ctrl_c()=>result?, _=term.recv()=>{} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let options = options(std::env::args().skip(1))?;
    let deadline = Duration::from_millis(options.timeout_ms);
    let (config, routes) = read_config(Path::new(&options.config))?.routes(deadline)?;
    // This extra legacy managed listener is deliberately loopback/ephemeral.
    // Its address is not published as a public service interface.
    let gateway =
        Gateway::bind_remote_local_queries(config, routes, "127.0.0.1:0".parse()?, deadline)
            .await
            .context("managed cohort validation failed; not serving")?;
    let http = match gateway.bind_http(options.http, deadline).await {
        Ok(http) => http,
        Err(error) => {
            gateway.shutdown().await;
            return Err(error.into());
        }
    };
    let grpc = match options.grpc {
        None => None,
        Some(bind) => match gateway.bind_grpc(bind, deadline).await {
            Ok(grpc) => Some(grpc),
            Err(error) => {
                http.shutdown().await;
                gateway.shutdown().await;
                return Err(error.into());
            }
        },
    };
    let announce = (|| -> anyhow::Result<()> {
        println!(
            "{}",
            serde_json::json!({"pid":std::process::id(),"http":http.local_addr(),"grpc":grpc.as_ref().map(|service|service.local_addr()),"grpc_api":grpc.as_ref().map(|_|"velagraph.public.managed.v1"),"state":"serving_live_managed_cohort","api":"velagraph.http.preview1","restore_supported":false,"production_ready":false})
        );
        std::io::stdout().flush()?;
        Ok(())
    })();
    let result = match announce {
        Ok(()) => shutdown_signal().await,
        Err(error) => Err(error),
    };
    if let Some(grpc) = grpc {
        grpc.shutdown().await;
    }
    http.shutdown().await;
    gateway.shutdown().await;
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    fn parse(args: &[&str]) -> anyhow::Result<Options> {
        options(args.iter().map(|s| s.to_string()))
    }
    #[test]
    fn explicit_loopback_and_opt_in_are_required() {
        assert!(parse(&["--config", "x", "--http", "127.0.0.1:0"]).is_err());
        assert!(parse(&[
            "--allow-insecure-development",
            "--config",
            "x",
            "--http",
            "0.0.0.0:8080"
        ])
        .is_err());
        assert!(parse(&["--allow-insecure-development", "--config", "x"]).is_err());
        assert_eq!(
            parse(&[
                "--allow-insecure-development",
                "--config",
                "x",
                "--http",
                "127.0.0.1:0"
            ])
            .unwrap()
            .timeout_ms,
            15000
        );
    }
    #[test]
    fn grpc_is_optional_explicit_and_loopback_only() {
        let base = [
            "--allow-insecure-development",
            "--config",
            "x",
            "--http",
            "127.0.0.1:0",
        ];
        assert_eq!(parse(&base).unwrap().grpc, None);
        let mut args = base.to_vec();
        args.extend(["--grpc", "127.0.0.1:0"]);
        assert!(parse(&args).unwrap().grpc.unwrap().ip().is_loopback());
        args.extend(["--grpc", "127.0.0.1:1"]);
        assert!(parse(&args).is_err());
        let mut args = base.to_vec();
        args.extend(["--grpc", "0.0.0.0:8081"]);
        assert!(parse(&args).is_err());
        assert!(parse(&[
            "--allow-insecure-development",
            "--config",
            "x",
            "--grpc",
            "127.0.0.1:0"
        ])
        .is_err());
    }
    #[test]
    fn repeated_flags_and_bad_budgets_reject() {
        for rest in [
            vec!["--timeout-ms", "0"],
            vec!["--timeout-ms", "60001"],
            vec!["--http", "127.0.0.1:1"],
            vec!["--allow-insecure-development"],
        ] {
            let mut args = vec![
                "--allow-insecure-development",
                "--config",
                "x",
                "--http",
                "127.0.0.1:0",
            ];
            args.extend(rest);
            assert!(parse(&args).is_err());
        }
    }
    #[test]
    fn config_input_is_bounded_and_unknown_fields_reject() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, vec![b' '; MAX_CONFIG_BYTES + 1]).unwrap();
        assert!(read_config(&path).is_err());
        std::fs::write(&path, b"{\"set_ready\":true}").unwrap();
        assert!(read_config(&path).is_err());
    }
}
