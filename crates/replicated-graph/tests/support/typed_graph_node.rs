//! Test-only typed graph RPC fixture, never installed as a product binary.
//! Uses this integration executable as an ignored child, preserving independent
//! process assertions and optional second typed listener sharing the same state.
use std::collections::BTreeMap;
use std::io::Write;

use anyhow::{bail, Context};
use d2_raft::{D1Command, D1Result, LogCommand, TcpNode, TcpNodeConfig};
use replicated_graph::remote::GraphService;
use std::time::Duration;

// Diagnostic labels are a closed test-only vocabulary, never arbitrary env text.
pub const SCENARIO_ENV: &str = "VELA_SNAPSHOT_DIAGNOSTIC_SCENARIO";
fn scenario_label(value: &str) -> Option<&'static str> {
    match value {
        "differential" => Some("differential"),
        "expiry" => Some("expiry"),
        "quorum-loss" => Some("quorum-loss"),
        "read-confirmation" => Some("read-confirmation"),
        "managed-confirmation" => Some("managed-confirmation"),
        _ => None,
    }
}

pub fn child_mapping(scenario: &str, parent: u32, child: u32, node: u64) -> String {
    let scenario = scenario_label(scenario).unwrap_or("unknown");
    format!("snapshot_child scenario={scenario} parent_pid={parent} child_pid={child} node={node}")
}

fn startup_failure(scenario: &str, pid: u32, node: u64, stage: &'static str) -> String {
    // stage is supplied only by the fixed startup call sites below.
    let scenario = scenario_label(scenario).unwrap_or("unknown");
    format!("snapshot_child_failure scenario={scenario} child_pid={pid} node={node} stage={stage}")
}

fn startup_result<T>(
    result: anyhow::Result<T>,
    node: u64,
    stage: &'static str,
) -> anyhow::Result<T> {
    result.map_err(|error| {
        let scenario = std::env::var(SCENARIO_ENV)
            .ok()
            .and_then(|value| scenario_label(&value));
        if let Some(scenario) = scenario {
            let label = startup_failure(scenario, std::process::id(), node, stage);
            eprintln!("{label}");
            error.context(label)
        } else {
            // Other fixture users keep their original error chain and logging.
            error
        }
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut id = None;
    let mut group = 0u64;
    let mut raft_addr = None;
    let mut control_addr = None;
    let mut data_dir = None;
    let mut bootstrap = None;
    let mut graph_addr = None;
    let mut grpc_snapshot_addr = None;
    let mut shard = None;
    let mut epoch = None;
    let mut trusted = false;
    let args: Vec<String> = serde_json::from_str(
        &std::env::var("VELA_TYPED_TEST_NODE_ARGS").context("test fixture arguments")?,
    )?;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--allow-insecure-development" => trusted = true,
            "--grpc-snapshot" => {
                grpc_snapshot_addr = Some(args.next().context("--grpc-snapshot needs a value")?)
            }
            "--graph" => graph_addr = Some(args.next().context("--graph needs a value")?),
            "--shard" => shard = Some(args.next().context("--shard needs a value")?),
            "--epoch" => epoch = Some(args.next().context("--epoch needs a value")?),
            "--group" => group = args.next().context("--group needs a value")?.parse()?,
            "--id" => id = Some(args.next().context("--id needs a value")?),
            "--raft" => raft_addr = Some(args.next().context("--raft needs a value")?),
            "--control" => control_addr = Some(args.next().context("--control needs a value")?),
            "--data-dir" => data_dir = Some(args.next().context("--data-dir needs a value")?),
            "--bootstrap" => bootstrap = Some(args.next().context("--bootstrap needs a value")?),
            other => bail!("unknown argument: {other}"),
        }
    }
    anyhow::ensure!(
        trusted,
        "plaintext ports require --allow-insecure-development; use only a trusted isolated network"
    );
    let graph_addr = graph_addr.context("--graph is required")?.parse()?;
    let shard: u64 = shard.context("--shard is required")?.parse()?;
    let epoch: u64 = epoch.context("--epoch is required")?.parse()?;
    anyhow::ensure!(epoch > 0, "epoch must be positive");
    let id: u64 = id.context("--id is required")?.parse()?;
    let raft_addr = raft_addr.context("--raft is required")?;
    let control_addr = control_addr.context("--control is required")?;
    let data_dir = data_dir.context("--data-dir is required")?;

    startup_result(
        bind_graph_identity(std::path::Path::new(&data_dir), group, id),
        id,
        "identity",
    )?;

    let node = startup_result(
        TcpNode::start(TcpNodeConfig {
            id,
            raft_addr,
            control_addr,
            data_dir: data_dir.into(),
        })
        .await,
        id,
        "tcp-node-start",
    )?;

    if let Some(spec) = bootstrap {
        let members = parse_members(&spec)?;
        startup_result(node.bootstrap(members).await, id, "bootstrap")?;
    }

    // Initial activation is an explicit startup administrator action. Existing
    // ownership is checked, never overwritten. Every node may attempt the same
    // immutable activation; only a leader can commit it.
    startup_result(
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some((actual_shard, actual_epoch, _)) =
                    node.state_machine.d1_metadata().await
                {
                    anyhow::ensure!(
                        actual_shard == shard && actual_epoch == epoch,
                        "stored ownership differs from startup configuration"
                    );
                    return anyhow::Ok(());
                }
                if node.raft.ensure_linearizable().await.is_ok() {
                    if let Ok(reply) = node
                        .raft
                        .client_write(LogCommand::D1V1(D1Command::Activate { shard, epoch }))
                        .await
                    {
                        anyhow::ensure!(
                            reply.data.d1 == Some(Ok(D1Result::Activated { shard, epoch })),
                            "D1 activation rejected"
                        );
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(|result| result),
        id,
        "activation-wait",
    )?;
    let service = startup_result(
        GraphService::start_identified(&node, graph_addr, group, shard, Duration::from_secs(3))
            .await
            .map_err(anyhow::Error::from),
        id,
        "graph-service-start",
    )?;
    let grpc_snapshot = if let Some(address) = grpc_snapshot_addr {
        Some(startup_result(
            replicated_graph::remote::grpc::GrpcSnapshotService::start(&service, address.parse()?)
                .await
                .map_err(anyhow::Error::from),
            id,
            "grpc-snapshot-start",
        )?)
    } else {
        None
    };
    if let Some(grpc) = &grpc_snapshot {
        println!("grpc-snapshot={}", grpc.local_addr());
    }
    eprintln!("WARNING: plaintext unauthenticated development service; trusted network only");
    println!("graph={}", service.local_addr());
    println!(
        "d2-node id={} raft={} control={}",
        node.id, node.raft_addr, node.control_addr
    );
    std::io::stdout().flush()?;

    // Run until the parent kills this process or SIGINT.
    tokio::signal::ctrl_c().await?;
    if let Some(grpc) = grpc_snapshot {
        grpc.shutdown().await;
    }
    service.shutdown().await;
    node.shutdown().await;
    Ok(())
}

fn parse_members(spec: &str) -> anyhow::Result<BTreeMap<u64, String>> {
    let mut members = BTreeMap::new();
    for item in spec.split(',') {
        let (id, addr) = item
            .split_once('=')
            .with_context(|| format!("bootstrap member must be id=addr: {item}"))?;
        members.insert(id.parse()?, addr.to_owned());
    }
    Ok(members)
}

/// Graph endpoint identity only: the Raft TCP protocol remains unauthenticated.
/// Persist before opening the databases; partial files fail closed on reopen.
fn bind_graph_identity(dir: &std::path::Path, group: u64, node: u64) -> anyhow::Result<()> {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Identity {
        version: u32,
        group: u64,
        node: u64,
    }
    std::fs::create_dir_all(dir)?;
    let expected = Identity {
        version: 1,
        group,
        node,
    };
    let path = dir.join("graph-service-identity.json");
    match std::fs::read(&path) {
        Ok(bytes) => {
            let actual: Identity =
                serde_json::from_slice(&bytes).context("decode graph service identity")?;
            anyhow::ensure!(
                actual == expected,
                "stored graph service identity differs from startup configuration"
            );
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("read graph service identity"),
    }
    // Unmarked legacy data cannot establish its original node identity. Refuse
    // adoption even for group zero rather than accidentally rebinding a node.
    anyhow::ensure!(
        std::fs::read_dir(dir)?.next().transpose()?.is_none(),
        "cannot bind graph identity to an unmarked nonempty data directory"
    );
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(&serde_json::to_vec(&expected)?)?;
    file.sync_all()?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_diagnostic_labels_are_bounded_and_owned() {
        let first = child_mapping("differential", 100, 201, 1);
        let second = child_mapping("quorum-loss", 100, 202, 1);
        assert_eq!(
            first,
            "snapshot_child scenario=differential parent_pid=100 child_pid=201 node=1"
        );
        assert_ne!(first, second);
        let failure = startup_failure("differential", 201, 1, "graph-service-start");
        assert!(failure.contains("child_pid=201 node=1"));
        assert!(failure.contains("stage=graph-service-start"));
        assert!(!child_mapping("untrusted\nsecret", 1, 2, 3).contains("secret"));
        assert!(scenario_label("untrusted").is_none());
        for label in [
            "differential",
            "expiry",
            "quorum-loss",
            "read-confirmation",
            "managed-confirmation",
        ] {
            assert!(child_mapping(label, u32::MAX, u32::MAX, u64::MAX).len() < 180);
            assert!(startup_failure(label, u32::MAX, u64::MAX, "grpc-snapshot-start").len() < 180);
        }
    }

    #[test]
    fn durable_graph_identity_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        bind_graph_identity(dir.path(), 10, 1).unwrap();
        bind_graph_identity(dir.path(), 10, 1).unwrap();
        assert!(bind_graph_identity(dir.path(), 11, 1).is_err());
        assert!(bind_graph_identity(dir.path(), 10, 2).is_err());
        assert!(bind_graph_identity(dir.path(), 0, 1).is_err());
        std::fs::write(dir.path().join("graph-service-identity.json"), b"{").unwrap();
        assert!(bind_graph_identity(dir.path(), 10, 1).is_err());
    }

    #[test]
    fn unmarked_existing_data_is_never_rebound() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("legacy-data"), b"data").unwrap();
        assert!(bind_graph_identity(dir.path(), 10, 1).is_err());
        assert!(bind_graph_identity(dir.path(), 0, 1).is_err());
        assert!(!dir.path().join("graph-service-identity.json").exists());
    }
}

#[test]
#[ignore = "subprocess fixture, launched only by typed graph RPC tests"]
fn serve() {
    // A manual --include-ignored run is not a request to start a daemon.
    if std::env::var_os("VELA_TYPED_TEST_NODE_ARGS").is_none() {
        return;
    }
    main().expect("typed test node failed");
}

pub struct TestCommand {
    inner: std::process::Command,
    arguments: Vec<String>,
}
pub fn command() -> TestCommand {
    let mut inner = std::process::Command::new(std::env::current_exe().expect("test executable"));
    inner.args([
        "--ignored",
        "--exact",
        "typed_graph_node::serve",
        "--nocapture",
    ]);
    TestCommand {
        inner,
        arguments: Vec::new(),
    }
}
impl TestCommand {
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        self.arguments.extend(
            args.into_iter()
                .map(|s| s.as_ref().to_string_lossy().into_owned()),
        );
        self
    }
    pub fn env<K: AsRef<std::ffi::OsStr>, V: AsRef<std::ffi::OsStr>>(
        &mut self,
        key: K,
        value: V,
    ) -> &mut Self {
        self.inner.env(key, value);
        self
    }
    pub fn stdout(&mut self, cfg: std::process::Stdio) -> &mut Self {
        self.inner.stdout(cfg);
        self
    }
    pub fn stderr(&mut self, cfg: std::process::Stdio) -> &mut Self {
        self.inner.stderr(cfg);
        self
    }
    pub fn spawn(&mut self) -> std::io::Result<std::process::Child> {
        self.inner.env(
            "VELA_TYPED_TEST_NODE_ARGS",
            serde_json::to_string(&self.arguments).expect("fixture args"),
        );
        self.inner.spawn()
    }
}
