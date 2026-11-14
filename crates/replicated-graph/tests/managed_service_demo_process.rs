//! Clean-startup release gate: user-facing example, not a private test bootstrap.
//! Linux CI checks SIGINT shutdown and disappearance of every announced child.
#![cfg(target_os = "linux")]

use anyhow::Context;
use axum::body::{to_bytes, Body};
use replicated_graph::managed_gateway::public_proto as pb;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const BUDGET: Duration = Duration::from_secs(60);

struct Demo {
    child: Child,
}
impl Drop for Demo {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            return;
        }
        if let Some(pid) = rustix::process::Pid::from_raw(self.child.id() as i32) {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
        }
        let end = Instant::now() + Duration::from_secs(30);
        while Instant::now() < end {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn log_tail(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let length = file.metadata()?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(64 * 1024)))?;
    let mut bytes = Vec::new();
    file.take(64 * 1024).read_to_end(&mut bytes)?;
    Ok(bytes)
}

// Preserve bounded launcher/service diagnostics before TempDir cleanup, even on panic.
struct Evidence {
    source: PathBuf,
    destination: Option<PathBuf>,
}
impl Evidence {
    fn new(source: &Path) -> anyhow::Result<Self> {
        let destination = std::env::var_os("VELAGRAPH_REMOTE_EVIDENCE_DIR")
            .map(|root| PathBuf::from(root).join(format!("managed-demo-{}", std::process::id())));
        if let Some(path) = &destination {
            fs::create_dir_all(path)?;
        }
        Ok(Self {
            source: source.to_owned(),
            destination,
        })
    }
}
impl Drop for Evidence {
    fn drop(&mut self) {
        let Some(destination) = &self.destination else {
            return;
        };
        let Ok(entries) = fs::read_dir(&self.source) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("stdout" | "stderr")
            ) {
                // Logs only; never copy graph data or unrelated files.
                match log_tail(&path) {
                    Ok(bytes) => {
                        let _ = fs::write(destination.join(entry.file_name()), bytes);
                    }
                    Err(error) => eprintln!("preserve demo log {}: {error}", path.display()),
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct Ready {
    pid: u32,
    http: SocketAddr,
    grpc: SocketAddr,
    node_pids: Vec<u32>,
    service_pid: u32,
    fresh_only: bool,
    restore_supported: bool,
    production_ready: bool,
}

fn launch(binary: &Path, data: &Path, output: &Path, error: &Path) -> anyhow::Result<Demo> {
    Ok(Demo {
        child: Command::new(binary)
            .arg("--data-dir")
            .arg(data)
            .arg("--service-bin")
            .arg(env!("CARGO_BIN_EXE_graph-service"))
            .args([
                "--http",
                "127.0.0.1:0",
                "--grpc",
                "127.0.0.1:0",
                "--allow-insecure-development",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::from(fs::File::create(output)?))
            .stderr(Stdio::from(fs::File::create(error)?))
            .spawn()?,
    })
}

async fn await_ready(demo: &mut Demo, output: &Path, error: &Path) -> anyhow::Result<Ready> {
    tokio::time::timeout(BUDGET, async {
        loop {
            anyhow::ensure!(
                demo.child.try_wait()?.is_none(),
                "demo exited before readiness: {}",
                fs::read_to_string(error)?
            );
            let text = fs::read_to_string(output)?;
            if let Some((line, _)) = text.split_once('\n') {
                let ready: Ready = serde_json::from_str(line).context("demo readiness JSON")?;
                anyhow::ensure!(ready.pid == demo.child.id(), "wrong demo PID");
                return Ok(ready);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("demo startup deadline")?
}

async fn rejected_existing(
    binary: &Path,
    data: &Path,
    logs: &Path,
    label: &str,
) -> anyhow::Result<()> {
    let output = logs.join(format!("{label}.stdout"));
    let error = logs.join(format!("{label}.stderr"));
    let mut rejected = launch(binary, data, &output, &error)?;
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = rejected.child.try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("existing directory must reject promptly")??;
    anyhow::ensure!(!status.success(), "existing directory was accepted");
    anyhow::ensure!(
        fs::read(&output)?.is_empty(),
        "rejected launch must not announce serving"
    );
    Ok(())
}

async fn write_http(address: SocketAddr) -> anyhow::Result<()> {
    tokio::time::timeout(BUDGET, async {
        let stream = tokio::net::TcpStream::connect(address).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await?;
        let driver = tokio::spawn(connection);
        let body = json!({
            "external_id": "01010101010101010101010101010101",
            "mutation": {
                "type": "create_vertex",
                "partition": 7,
                "vertex": {"id": "7001", "labels": ["person"], "properties": {}}
            }
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/preview1/writes:submit")
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body)?))?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        let bytes = to_bytes(Body::new(response.into_body()), 1024 * 1024).await?;
        drop(sender);
        driver.await??;
        let result: Value = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(status.as_u16() == 200, "HTTP write failed: {result}");
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

fn context() -> Option<pb::Context> {
    Some(pb::Context {
        version: 1,
        timeout_millis: 10_000,
        identity: None,
    })
}

async fn query_vertex(address: SocketAddr) -> anyhow::Result<u64> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .connect()
        .await?;
    let mut client = pb::query_service_client::QueryServiceClient::new(channel)
        .max_encoding_message_size(64 * 1024)
        .max_decoding_message_size(1024 * 1024);
    let opened = client
        .open(pb::OpenRequest {
            context: context(),
            read: Some(pb::ReadSelector {
                kind: Some(pb::read_selector::Kind::Latest(pb::Empty {})),
            }),
            ttl_millis: 10_000,
            query: Some(pb::Query {
                partition: 7,
                text: "g.V(7001)".into(),
            }),
            page_size: 1,
            profile: None,
        })
        .await?
        .into_inner();
    let session = opened.session.context("session")?;
    let page = opened.page.context("page")?;
    assert_eq!(page.total, 1);
    assert_eq!(page.offset, 0);
    assert_eq!(page.next, None);
    assert_eq!(
        page.rows,
        vec![pb::Row {
            kind: Some(pb::row::Kind::Element(pb::QualifiedElement {
                kind: Some(pb::qualified_element::Kind::Vertex(pb::Entity {
                    partition: 7,
                    id: 7001,
                })),
            })),
        }]
    );
    let version = session.version;
    client
        .close(pb::CloseRequest {
            context: context(),
            session: Some(session),
        })
        .await?;
    Ok(version)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires exact-build VELAGRAPH_MANAGED_DEMO_BIN; mandatory clean-startup CI gate"]
async fn fresh_demo_write_query_reject_existing_and_stop_children() -> anyhow::Result<()> {
    let binary = PathBuf::from(
        std::env::var_os("VELAGRAPH_MANAGED_DEMO_BIN")
            .context("VELAGRAPH_MANAGED_DEMO_BIN is required; never skip this gate")?,
    );
    let binary = fs::canonicalize(binary).context("demo binary must exist")?;
    let logs = tempfile::tempdir()?;
    let _evidence = Evidence::new(logs.path())?;
    let data = logs.path().join("fresh-data");
    let output = logs.path().join("demo.stdout");
    let error = logs.path().join("demo.stderr");

    // Even an existing empty directory is not permission to overwrite/restore.
    let empty = logs.path().join("existing-empty");
    fs::create_dir(&empty)?;
    rejected_existing(&binary, &empty, logs.path(), "empty").await?;
    assert_eq!(fs::read_dir(&empty)?.count(), 0);

    let mut demo = launch(&binary, &data, &output, &error)?;
    let ready = await_ready(&mut demo, &output, &error).await?;
    assert!(ready.fresh_only);
    assert!(!ready.restore_supported);
    assert!(!ready.production_ready);
    assert!(ready.http.ip().is_loopback());
    assert!(ready.grpc.ip().is_loopback());
    assert_ne!(ready.http, ready.grpc);
    assert_eq!(ready.node_pids.len(), 3);
    let mut pids = ready.node_pids.clone();
    pids.extend([ready.pid, ready.service_pid, std::process::id()]);
    assert_eq!(
        pids.iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        6
    );
    for pid in &pids {
        assert!(
            Path::new(&format!("/proc/{pid}")).exists(),
            "missing PID {pid}"
        );
    }

    write_http(ready.http).await?;
    let before = query_vertex(ready.grpc).await?;
    assert!(before > 0);
    let sentinel = data.join("do-not-overwrite");
    fs::write(&sentinel, b"original data directory")?;
    rejected_existing(&binary, &data, logs.path(), "live").await?;
    assert_eq!(fs::read(&sentinel)?, b"original data directory");
    assert_eq!(query_vertex(ready.grpc).await?, before);

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(demo.child.id() as i32).context("demo pid")?,
        rustix::process::Signal::INT,
    )?;
    let status = tokio::time::timeout(BUDGET, async {
        loop {
            if let Some(status) = demo.child.try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("SIGINT must stop the demo")??;
    anyhow::ensure!(
        status.success(),
        "demo shutdown failed: {}",
        fs::read_to_string(&error)?
    );
    let children: Vec<_> = ready
        .node_pids
        .into_iter()
        .chain([ready.service_pid])
        .collect();
    tokio::time::timeout(Duration::from_secs(10), async {
        while children
            .iter()
            .any(|pid| Path::new(&format!("/proc/{pid}")).exists())
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("demo left child processes alive or unreaped after SIGINT")?;
    assert_eq!(fs::read(&sentinel)?, b"original data directory");
    rejected_existing(&binary, &data, logs.path(), "stopped").await?;
    assert_eq!(fs::read(&sentinel)?, b"original data directory");
    Ok(())
}
