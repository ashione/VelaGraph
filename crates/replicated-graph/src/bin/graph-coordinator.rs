//! Trusted-network server and explicit clients for distributed observed queries.
//!
//! All commands require --endpoint and --allow-insecure-development; an optional
//! --timeout-ms is bounded to 1..=5000 (default 5000).
//! serve --catalog FILE; query --partition N --query GREMLIN;
//! hierarchy-start --partition N --query GREMLIN;
//! hierarchy-advance --token HEX32 --sequence N; hierarchy-cancel --token HEX32.
//! hierarchy-rows-start --partition N --query GREMLIN --page-size N;
//! hierarchy-read --token HEX32 --cursor N (only after Preparing.ready).
//! Job output is the serialized jobs::Output, including the token's 16 bytes.
//! Encode those bytes as 32 hexadecimal digits for subsequent commands. Tokens
//! are temporary bearer credentials: keep them private. Each invocation sends
//! one request only, with no automatic advancement, retry, or polling.
//! All commands use typed gRPC; serve-grpc and --grpc were temporary migration
//! switches and are rejected. There is no protocol probing or legacy fallback.
use anyhow::{bail, Context};
use replicated_graph::remote::distributed::{
    grpc::{CoordinatorClient, GrpcCoordinatorService},
    jobs::{Output, Request},
    DistributedRequest, StaticCatalog,
};
use std::{net::SocketAddr, time::Duration};

#[derive(Debug)]
enum Command {
    Serve { catalog: String },
    Query { partition: u32, text: String },
    Job(Request),
}

#[derive(Debug)]
struct Options {
    endpoint: SocketAddr,
    timeout_ms: u64,
    command: Command,
}

const COMMANDS: &str =
    "expected serve, query, hierarchy-start, hierarchy-rows-start, hierarchy-read, hierarchy-advance, or hierarchy-cancel";

fn parse_token(value: &str) -> anyhow::Result<[u8; 16]> {
    anyhow::ensure!(
        value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "--token requires exactly 32 hexadecimal digits"
    );
    let mut token = [0; 16];
    for (index, byte) in token.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(token)
}

fn parse_args(mut args: impl Iterator<Item = String>) -> anyhow::Result<Options> {
    let command = args.next().context(COMMANDS)?;
    let (mut endpoint, mut catalog, mut partition, mut text) = (None, None, None, None);
    let (mut token, mut sequence) = (None, None);
    let (mut page_size, mut cursor) = (None, None);
    let mut timeout_ms = 5000u64;
    let mut trusted = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--allow-insecure-development" => trusted = true,
            "--grpc" => bail!("--grpc was removed; all coordinator commands use typed gRPC"),
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
            "--token" => {
                anyhow::ensure!(token.is_none(), "--token must not be repeated");
                token = Some(parse_token(&args.next().context("--token needs a value")?)?);
            }
            "--sequence" => {
                anyhow::ensure!(sequence.is_none(), "--sequence must not be repeated");
                sequence = Some(
                    args.next()
                        .context("--sequence needs a value")?
                        .parse::<u64>()?,
                );
            }
            "--page-size" => {
                anyhow::ensure!(page_size.is_none(), "--page-size must not be repeated");
                page_size = Some(
                    args.next()
                        .context("--page-size needs a value")?
                        .parse::<usize>()?,
                );
            }
            "--cursor" => {
                anyhow::ensure!(cursor.is_none(), "--cursor must not be repeated");
                cursor = Some(
                    args.next()
                        .context("--cursor needs a value")?
                        .parse::<usize>()?,
                );
            }
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
    anyhow::ensure!(
        page_size.is_none() || command == "hierarchy-rows-start",
        "--page-size only accepted by hierarchy-rows-start"
    );
    anyhow::ensure!(
        cursor.is_none() || command == "hierarchy-read",
        "--cursor only accepted by hierarchy-read"
    );
    let endpoint = endpoint.context("--endpoint is required")?;
    let command = match command.as_str() {
        "serve-grpc" => bail!("serve-grpc was removed; use serve (typed gRPC only)"),
        "serve" => {
            anyhow::ensure!(
                partition.is_none() && text.is_none() && token.is_none() && sequence.is_none(),
                "query and job flags are not accepted by serve"
            );
            Command::Serve {
                catalog: catalog.context("--catalog is required")?,
            }
        }
        "query" | "hierarchy-start" | "hierarchy-rows-start" => {
            anyhow::ensure!(catalog.is_none(), "catalog is server-side configuration");
            anyhow::ensure!(
                token.is_none() && sequence.is_none(),
                "--token and --sequence are only accepted by continuation commands"
            );
            let partition = partition.context("--partition is required")?;
            let text = text.context("--query is required")?;
            if command == "query" {
                Command::Query { partition, text }
            } else if command == "hierarchy-rows-start" {
                let page_size = page_size.context("--page-size is required")?;
                anyhow::ensure!(
                    (1..=1000).contains(&page_size),
                    "page size must be 1..=1000"
                );
                Command::Job(Request::StartRows {
                    start_partition: partition,
                    text,
                    page_size,
                })
            } else {
                Command::Job(Request::Start {
                    start_partition: partition,
                    text,
                })
            }
        }
        "hierarchy-advance" | "hierarchy-cancel" | "hierarchy-read" => {
            anyhow::ensure!(
                catalog.is_none() && partition.is_none() && text.is_none(),
                "catalog, partition, and query flags are not accepted by continuation commands"
            );
            let token = token.context("--token is required")?;
            if command == "hierarchy-advance" {
                Command::Job(Request::Advance {
                    token,
                    sequence: sequence.context("--sequence is required")?,
                })
            } else if command == "hierarchy-read" {
                anyhow::ensure!(sequence.is_none(), "read does not accept --sequence");
                Command::Job(Request::Read {
                    token,
                    cursor: cursor.context("--cursor is required")?,
                })
            } else {
                anyhow::ensure!(sequence.is_none(), "cancel does not accept --sequence");
                Command::Job(Request::Cancel { token })
            }
        }
        _ => bail!(COMMANDS),
    };
    Ok(Options {
        endpoint,
        timeout_ms,
        command,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Options {
        endpoint,
        timeout_ms,
        command,
    } = parse_args(std::env::args().skip(1))?;
    let timeout = Duration::from_millis(timeout_ms);
    match command {
        Command::Serve { catalog } => {
            let file = std::fs::File::open(catalog)?;
            anyhow::ensure!(
                file.metadata()?.len() <= 64 * 1024,
                "catalog configuration exceeds 64 KiB"
            );
            let catalog: StaticCatalog = serde_json::from_reader(file)?;
            let service = GrpcCoordinatorService::start(endpoint, catalog, timeout).await?;
            println!("coordinator={}", service.local_addr());
            tokio::signal::ctrl_c().await?;
            service.shutdown().await;
        }
        Command::Query { partition, text } => {
            let request = DistributedRequest {
                start_partition: partition,
                text,
                timeout_millis: timeout_ms,
            };
            // Typed failure is printed as an error, never as a partial result.
            let end = tokio::time::Instant::now() + timeout;
            let client = CoordinatorClient::connect(endpoint, timeout).await?;
            let output = client
                .query(
                    request,
                    end.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await??;
            println!("{}", serde_json::to_string(&output)?);
        }
        Command::Job(job) => {
            // One explicit request; an incomplete Progress is not a count result.
            let end = tokio::time::Instant::now() + timeout;
            let client = CoordinatorClient::connect(endpoint, timeout).await?;
            let output: Output = client
                .job(
                    job,
                    end.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await??;
            println!("{}", serde_json::to_string(&output)?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_transport_switches_are_rejected() {
        for command in ["serve", "serve-grpc"] {
            assert!(parse(command, &["--catalog", "x", "--grpc"]).is_err());
        }
        assert!(parse("serve-grpc", &["--catalog", "x"]).is_err());
        assert!(parse(
            "query",
            &["--partition", "1", "--query", "g.V(1)", "--grpc"]
        )
        .is_err());
        assert!(parse("hierarchy-cancel", &["--token", TOKEN, "--grpc"]).is_err());
    }

    const TOKEN: &str = "00112233445566778899aAbBcCdDeEfF";

    fn parse(command: &str, flags: &[&str]) -> anyhow::Result<Options> {
        let mut args = vec![
            command,
            "--endpoint",
            "127.0.0.1:9000",
            "--allow-insecure-development",
        ];
        args.extend_from_slice(flags);
        parse_args(args.into_iter().map(str::to_owned))
    }

    #[test]
    fn explicit_job_requests_preserve_inputs() {
        let text = "g.V(1).repeat(out()).times(3).count()";
        assert!(matches!(
            parse("hierarchy-start", &["--partition", "42", "--query", text]).unwrap().command,
            Command::Job(Request::Start {start_partition: 42, text: actual}) if actual == text
        ));
        assert!(matches!(
            parse("hierarchy-advance", &["--token", TOKEN, "--sequence", "18446744073709551615"]).unwrap().command,
            Command::Job(Request::Advance {token, sequence: u64::MAX}) if token == parse_token(TOKEN).unwrap()
        ));
        assert!(matches!(
            parse("hierarchy-cancel", &["--token", TOKEN]).unwrap().command,
            Command::Job(Request::Cancel {token}) if token == parse_token(TOKEN).unwrap()
        ));
        assert!(parse("hierarchy-advance", &["--token", TOKEN, "--sequence", "0"]).is_ok());
    }

    #[test]
    fn token_is_exact_ascii_hex_without_echoing_secret() {
        assert_eq!(
            parse_token(TOKEN).unwrap(),
            [0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255]
        );
        for bad in [
            "",
            "00",
            "00112233445566778899aabbccddeeff00",
            "g0112233445566778899aabbccddeeff",
            "é112233445566778899aabbccddeeff",
            " 0112233445566778899aabbccddeeff",
        ] {
            assert!(parse_token(bad).is_err());
        }
    }

    #[test]
    fn incompatible_and_missing_flags_are_rejected() {
        for (command, valid) in [
            ("serve", vec!["--catalog", "catalog.json"]),
            ("query", vec!["--partition", "1", "--query", "g.V()"]),
            (
                "hierarchy-start",
                vec!["--partition", "1", "--query", "g.V()"],
            ),
        ] {
            for extra in [["--token", TOKEN], ["--sequence", "0"]] {
                let mut args = valid.clone();
                args.extend(extra);
                assert!(parse(command, &args).is_err());
            }
        }
        assert!(parse("serve", &["--catalog", "x", "--partition", "1"]).is_err());
        assert!(parse("serve", &["--catalog", "x", "--query", "g.V()"]).is_err());
        for command in ["query", "hierarchy-start"] {
            assert!(parse(
                command,
                &["--partition", "1", "--query", "g.V()", "--catalog", "x"]
            )
            .is_err());
        }
        for command in ["hierarchy-advance", "hierarchy-cancel"] {
            assert!(parse(command, &[]).is_err());
            for flags in [
                ["--catalog", "x"],
                ["--partition", "1"],
                ["--query", "g.V()"],
            ] {
                let mut args = vec!["--token", TOKEN];
                args.extend(flags);
                assert!(parse(command, &args).is_err());
            }
        }
        assert!(parse("hierarchy-start", &["--partition", "1"]).is_err());
        assert!(parse("hierarchy-start", &["--query", "g.V()"]).is_err());
        assert!(parse("hierarchy-advance", &["--token", TOKEN]).is_err());
        assert!(parse("hierarchy-cancel", &["--token", TOKEN, "--sequence", "0"]).is_err());
        for sequence in ["-1", "18446744073709551616", "1.5", ""] {
            assert!(parse(
                "hierarchy-advance",
                &["--token", TOKEN, "--sequence", sequence]
            )
            .is_err());
        }
        assert!(parse("hierarchy-cancel", &["--token", TOKEN, "--token", TOKEN]).is_err());
        assert!(parse(
            "hierarchy-advance",
            &["--token", TOKEN, "--sequence", "1", "--sequence", "2"]
        )
        .is_err());
        assert!(parse("bogus", &[]).is_err());
    }

    #[test]
    fn ordinary_commands_and_shared_guards_remain() {
        assert!(matches!(
            parse("serve", &["--catalog", "x"]).unwrap().command,
            Command::Serve { .. }
        ));
        assert!(matches!(
            parse(
                "query",
                &["--partition", "1", "--query", "g.V()", "--timeout-ms", "1"]
            )
            .unwrap()
            .command,
            Command::Query { partition: 1, .. }
        ));
        assert_eq!(
            parse("hierarchy-cancel", &["--token", TOKEN])
                .unwrap()
                .timeout_ms,
            5000
        );
        for timeout in ["0", "5001", "-1"] {
            assert!(parse(
                "hierarchy-cancel",
                &["--token", TOKEN, "--timeout-ms", timeout]
            )
            .is_err());
        }
        assert!(parse(
            "hierarchy-cancel",
            &["--token", TOKEN, "--timeout-ms", "5000"]
        )
        .is_ok());
        assert!(parse_args(
            ["serve", "--endpoint", "127.0.0.1:9000", "--catalog", "x"]
                .into_iter()
                .map(str::to_owned)
        )
        .is_err());
        assert!(parse_args(
            ["serve", "--allow-insecure-development", "--catalog", "x"]
                .into_iter()
                .map(str::to_owned)
        )
        .is_err());
    }
    #[test]
    fn row_page_cli_requires_explicit_page_and_cursor_flags() {
        assert!(matches!(
            parse(
                "hierarchy-rows-start",
                &[
                    "--partition",
                    "0",
                    "--query",
                    "g.V(1).repeat(__.out()).times(2)",
                    "--page-size",
                    "1000"
                ]
            )
            .unwrap()
            .command,
            Command::Job(Request::StartRows {
                page_size: 1000,
                ..
            })
        ));
        assert!(matches!(
            parse("hierarchy-read", &["--token", TOKEN, "--cursor", "0"])
                .unwrap()
                .command,
            Command::Job(Request::Read { cursor: 0, .. })
        ));
        for flags in [
            vec!["--token", TOKEN],
            vec!["--token", TOKEN, "--cursor", "0", "--sequence", "0"],
            vec!["--token", TOKEN, "--cursor", "0", "--page-size", "1"],
        ] {
            assert!(parse("hierarchy-read", &flags).is_err());
        }
        assert!(parse(
            "hierarchy-rows-start",
            &[
                "--partition",
                "0",
                "--query",
                "g.V(1).repeat(__.out()).times(2)",
                "--page-size",
                "1001"
            ]
        )
        .is_err());
    }
}
