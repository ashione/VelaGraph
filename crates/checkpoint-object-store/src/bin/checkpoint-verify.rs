//! Explicit read-only receipt verifier, not a restore or readiness command.
use checkpoint_object_store::{CheckpointReader, Error, Limits, Receipt};
use object_store::{
    aws::AmazonS3Builder,
    client::{HttpClient, HttpConnector},
    ClientOptions,
};
use serde::Deserialize;
use std::{
    io::{Read, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};

const INPUT_BYTES: u64 = 64 * 1024;
const USAGE: &str = "checkpoint-verify --config FILE --receipt FILE [--allow-http-loopback]";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    endpoint: String,
    bucket: String,
    region: String,
    prefix: String,
    limits: Budgets,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Budgets {
    chunk_bytes: usize,
    manifest_bytes: usize,
    files: usize,
    total_bytes: u64,
    timeout_millis: u64,
}
#[derive(Debug)]
struct DirectConnector(HttpClient);
impl HttpConnector for DirectConnector {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(self.0.clone())
    }
}
fn credential(name: &str) -> Result<String, &'static str> {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or("credentials")
}
struct Args {
    config: std::path::PathBuf,
    receipt: std::path::PathBuf,
    allow_http_loopback: bool,
}
fn args() -> Result<Option<Args>, &'static str> {
    let mut values = std::env::args_os().skip(1);
    let (mut config, mut receipt, mut allow_http_loopback) = (None, None, false);
    while let Some(flag) = values.next() {
        match flag.to_str() {
            Some("--help") if config.is_none() && receipt.is_none() && !allow_http_loopback => {
                return if values.next().is_none() {
                    Ok(None)
                } else {
                    Err("arguments")
                };
            }
            Some("--config") if config.is_none() => config = values.next().map(Into::into),
            Some("--receipt") if receipt.is_none() => receipt = values.next().map(Into::into),
            Some("--allow-http-loopback") if !allow_http_loopback => allow_http_loopback = true,
            _ => return Err("arguments"),
        }
    }
    Ok(Some(Args {
        config: config.ok_or("arguments")?,
        receipt: receipt.ok_or("arguments")?,
        allow_http_loopback,
    }))
}
fn input<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, &'static str> {
    // Caller-owned regular files only. Bound the actual read, not just metadata.
    let metadata = std::fs::symlink_metadata(path).map_err(|_| "input_io")?;
    if !metadata.is_file() || metadata.len() > INPUT_BYTES {
        return Err("input_limit_or_type");
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "input_io")?
        .take(INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "input_io")?;
    if bytes.len() as u64 > INPUT_BYTES {
        return Err("input_limit_or_type");
    }
    serde_json::from_slice(&bytes).map_err(|_| "input_json")
}
fn endpoint_valid(endpoint: &str, allow_http_loopback: bool) -> bool {
    if endpoint.contains(['@', '?', '#']) {
        return false;
    }
    if let Some(authority) = endpoint.strip_prefix("https://") {
        return !authority.is_empty()
            && !authority.starts_with('/')
            && !authority.chars().any(char::is_whitespace);
    }
    allow_http_loopback
        && endpoint
            .strip_prefix("http://")
            .and_then(|v| v.parse::<std::net::SocketAddr>().ok())
            .is_some_and(|a| a.ip().is_loopback() && a.port() != 0)
}
fn error_code(error: Error) -> &'static str {
    match error {
        Error::Invalid(_) | Error::Json(_) => "invalid_checkpoint",
        Error::Limit => "limit",
        Error::Integrity(_) => "integrity",
        Error::Deadline => "deadline",
        Error::Store(_) => "object_store",
        Error::Io(_) => "io",
        Error::OutcomeUnknown(_) => "unexpected_write_outcome",
    }
}
async fn run(args: Args) -> Result<(), &'static str> {
    let config: Config = input(&args.config)?;
    let receipt: Receipt = input(&args.receipt)?;
    if !endpoint_valid(&config.endpoint, args.allow_http_loopback)
        || config.bucket.is_empty()
        || config.region.is_empty()
    {
        return Err("endpoint_or_config");
    }
    // All budgets are explicit; configuration never carries credentials.
    if config.limits.timeout_millis == 0 || config.limits.timeout_millis > 86_400_000 {
        return Err("limit");
    }
    let limits = Limits {
        chunk_bytes: config.limits.chunk_bytes,
        manifest_bytes: config.limits.manifest_bytes,
        files: config.limits.files,
        total_bytes: config.limits.total_bytes,
        timeout: Duration::from_millis(config.limits.timeout_millis),
    };
    // Deliberately import only explicit credential values, never ambient AWS
    // endpoints, metadata providers, addressing, TLS or proxy configuration.
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .https_only(!args.allow_http_loopback)
        .build()
        .map_err(|_| "backend_config")?;
    let mut builder = AmazonS3Builder::new()
        .with_access_key_id(credential("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(credential("AWS_SECRET_ACCESS_KEY")?)
        .with_endpoint(config.endpoint)
        .with_bucket_name(config.bucket)
        .with_region(config.region)
        .with_virtual_hosted_style_request(false)
        .with_client_options(ClientOptions::new().with_allow_http(args.allow_http_loopback))
        .with_http_connector(DirectConnector(HttpClient::new(http)));
    if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
        if token.is_empty() {
            return Err("credentials");
        }
        builder = builder.with_token(token);
    }
    let backend = builder.build().map_err(|_| "backend_config")?;
    let reader =
        CheckpointReader::new(Arc::new(backend), config.prefix, limits).map_err(error_code)?;
    let report = reader.verify(&receipt).await.map_err(error_code)?;
    writeln!(std::io::stdout().lock(), "{}", serde_json::json!({"status":"verified","files":report.files,"bytes":report.bytes,"chunks":report.chunks})).map_err(|_| "output_io")?;
    Ok(())
}
#[tokio::main]
async fn main() -> std::process::ExitCode {
    let result = match args() {
        Ok(Some(args)) => run(args).await,
        Ok(None) => {
            return if writeln!(std::io::stdout().lock(), "{USAGE}").is_ok() {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::FAILURE
            };
        }
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(code) => {
            // Do not echo SDK errors, URLs, local paths, bucket names or secrets.
            let _ = writeln!(
                std::io::stderr().lock(),
                "{}",
                serde_json::json!({"status":"failed","code":code})
            );
            std::process::ExitCode::FAILURE
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_policy_is_explicit() {
        assert!(endpoint_valid("https://s3.example.test", false));
        for value in [
            "http://127.0.0.1:9000",
            "http://10.0.0.1:9000",
            "https://secret@example.test",
            "https://example.test?secret",
            "file:///tmp/data",
            "https://",
        ] {
            assert!(!endpoint_valid(value, false));
        }
        assert!(endpoint_valid("http://127.0.0.1:9000", true));
        assert!(endpoint_valid("http://[::1]:9000", true));
        for value in [
            "http://10.0.0.1:9000",
            "http://localhost:9000",
            "http://127.0.0.1:0",
            "http://127.0.0.1:9000/path",
        ] {
            assert!(!endpoint_valid(value, true));
        }
    }
}
