//! Real-process typed shard Probe/Metadata/Write. Not whole-repository RPC closure.
use anyhow::{ensure, Context};
use d2_raft::{
    send_control, ControlRequest, ControlResponse, D1Error, D1Result, D1WriteRequest,
    D1WriteStatus, WriteOp,
};
use replicated_graph::remote::{
    self,
    grpc::SnapshotClient,
    operations_grpc::{wire as w, Client},
    RemoteError, RemoteIdentity,
};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    net::SocketAddr,
    pin::Pin,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{watch, Notify},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
const BUDGET: Duration = Duration::from_secs(5);
const SHARD: u64 = 7;
const GROUP: u64 = 77;
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
// Failure-only evidence is emitted before Process kills the child. Each file
// is read through an independent handle and capped, without dumping graph data.
struct ObservedProcess {
    process: Process,
    log: tempfile::NamedTempFile,
    node: usize,
    success: bool,
}
fn log_tail(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    const LIMIT: u64 = 16 * 1024;
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    file.seek(SeekFrom::Start(len.saturating_sub(LIMIT)))?;
    let mut bytes = Vec::new();
    file.take(LIMIT).read_to_end(&mut bytes)?;
    Ok(bytes)
}
impl Drop for ObservedProcess {
    fn drop(&mut self) {
        if !self.success {
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(
                stderr,
                "shard_fixture node={} pid={} pre_cleanup_exit={:?}",
                self.node,
                self.process.0.id(),
                self.process.0.try_wait()
            );
            match log_tail(self.log.path()) {
                Ok(bytes) => {
                    let _ = writeln!(
                        stderr,
                        "shard_fixture node={} log_tail_bytes={}\n{}",
                        self.node,
                        bytes.len(),
                        String::from_utf8_lossy(&bytes)
                    );
                }
                Err(error) => {
                    let _ = writeln!(stderr, "shard_fixture node={} log_error={error}", self.node);
                }
            }
        }
    }
}
async fn reserve_addresses(count: usize) -> anyhow::Result<Vec<SocketAddr>> {
    let mut reservations = Vec::new();
    for _ in 0..count {
        reservations.push(TcpListener::bind("127.0.0.1:0").await?);
    }
    Ok(reservations
        .iter()
        .map(|x| x.local_addr())
        .collect::<std::io::Result<Vec<_>>>()?)
}

impl ObservedProcess {
    fn spawn(
        id: usize,
        addresses: &[SocketAddr],
        dir: &std::path::Path,
        bootstrap: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_graph-node"));
        command.env("TOKIO_WORKER_THREADS", "2").args([
            "--allow-insecure-development",
            "--group",
            &GROUP.to_string(),
            "--id",
            &id.to_string(),
            "--raft",
            &addresses[0].to_string(),
            "--control",
            &addresses[1].to_string(),
            "--graph",
            &addresses[3].to_string(),
            "--shard",
            "7",
            "--epoch",
            "3",
            "--data-dir",
            dir.to_str().context("data dir")?,
        ]);
        if let Some(bootstrap) = bootstrap {
            command.args(["--bootstrap", bootstrap]);
        }
        let log = tempfile::NamedTempFile::new()?;
        let output = log.reopen()?;
        let child = command
            .stdout(Stdio::from(output.try_clone()?))
            .stderr(Stdio::from(output))
            .spawn()
            .with_context(|| format!("spawn shard node {id}"))?;
        Ok(Self {
            process: Process(child),
            log,
            node: id,
            success: false,
        })
    }
}

struct Cluster {
    addresses: Vec<SocketAddr>,
    leader: usize,
    _processes: Vec<ObservedProcess>,
    _dirs: Vec<tempfile::TempDir>,
}
impl Cluster {
    async fn start() -> anyhow::Result<Self> {
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<std::io::Result<Vec<_>>>()?;
        let addresses = reserve_addresses(12).await?;
        Self::launch(addresses, dirs, true).await
    }

    async fn launch(
        addresses: Vec<SocketAddr>,
        dirs: Vec<tempfile::TempDir>,
        initialize: bool,
    ) -> anyhow::Result<Self> {
        let bootstrap = (0..3)
            .map(|i| format!("{}={}", i + 1, addresses[4 * i]))
            .collect::<Vec<_>>()
            .join(",");
        let mut processes = Vec::new();
        for i in 0..3 {
            processes.push(ObservedProcess::spawn(
                i + 1,
                &addresses[4 * i..4 * i + 4],
                dirs[i].path(),
                (initialize && i == 0).then_some(bootstrap.as_str()),
            )?);
        }
        let leader = timeout(Duration::from_secs(35), async {
            loop {
                for i in 0..3 {
                    if let Ok(ControlResponse::Status(status)) =
                        send_control(&addresses[4 * i + 1].to_string(), ControlRequest::Status)
                            .await
                    {
                        if status.leader == Some((i + 1) as u64) {
                            if let Ok(mut client) = Client::connect(
                                addresses[4 * i + 3],
                                RemoteIdentity {
                                    group: GROUP,
                                    node: (i + 1) as u64,
                                },
                                Duration::from_millis(500),
                            )
                            .await
                            {
                                if client.probe(SHARD).await.is_ok() {
                                    return i;
                                }
                            }
                        }
                    }
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("typed shard leader readiness")?;
        Ok(Self {
            addresses,
            leader,
            _processes: processes,
            _dirs: dirs,
        })
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        for process in &mut self._processes {
            process.process.0.kill()?;
        }
        for process in &mut self._processes {
            process.process.0.wait()?;
            process.success = true;
        }
        Ok(())
    }
    async fn restart(mut self) -> anyhow::Result<Self> {
        self.stop()?;
        drop(self._processes);
        Self::launch(self.addresses, self._dirs, false).await
    }

    fn identity(&self, i: usize) -> RemoteIdentity {
        RemoteIdentity {
            group: GROUP,
            node: (i + 1) as u64,
        }
    }
    fn grpc(&self, i: usize) -> SocketAddr {
        self.addresses[4 * i + 3]
    }
    fn tcp(&self, i: usize) -> SocketAddr {
        self.addresses[4 * i + 2]
    }
}
fn write(id: u8, sequence: Option<u64>) -> D1WriteRequest {
    D1WriteRequest {
        shard: SHARD,
        epoch: 3,
        request_id: [id; 16],
        expected_sequence: sequence,
        operations: vec![WriteOp::Put {
            key: vec![b'k', id, 0, 255],
            value: vec![0, 255, id],
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_shard_operations_preserve_real_process_semantics() -> anyhow::Result<()> {
    timeout(Duration::from_secs(90), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let mut cluster = Cluster::start().await.context("start shard cluster")?;
    let endpoint = cluster.grpc(cluster.leader);
    let identity = cluster.identity(cluster.leader);
    let mut client = Client::connect(endpoint, identity, BUDGET)
        .await
        .context("connect leader")?;
    client.probe(SHARD).await.context("leader probe")?;
    let initial = client.metadata(SHARD).await.context("leader metadata")?;
    assert_eq!(initial, (SHARD, 3, 0));
    let request = write(1, Some(initial.2));
    assert!(
        matches!(client.write(request.clone()).await.context("initial write")?, D1Result::Write { status: D1WriteStatus::Committed, sequence, applied_operations: 1 } if sequence == initial.2+1)
    );
    assert!(
        matches!(client.write(request.clone()).await.context("duplicate write")?, D1Result::Write { status: D1WriteStatus::Duplicate, sequence, applied_operations: 1 } if sequence == initial.2+1)
    );
    let mut conflict = request.clone();
    conflict.operations = vec![WriteOp::Delete {
        key: b"different".to_vec(),
    }];
    assert!(matches!(
        client.write(conflict).await,
        Err(RemoteError::Application(D1Error::IdempotencyConflict))
    ));
    assert!(matches!(client.write(write(2, Some(initial.2))).await,
        Err(RemoteError::Application(D1Error::Conflict { expected, current })) if expected == initial.2 && current == initial.2+1));
    let committed = client.metadata(SHARD).await.context("leader metadata")?;
    assert_eq!(
        committed.2,
        initial.2 + 1,
        "duplicate/rejected writes must not advance graph sequence"
    );
    let snapshots = SnapshotClient::connect(endpoint, identity, BUDGET)
        .await
        .context("connect leader")?;
    let descriptor = snapshots
        .open(SHARD, BUDGET)
        .await
        .context("snapshot open")?;
    let page = snapshots
        .page(descriptor.clone(), None, BUDGET)
        .await
        .context("snapshot page")?;
    assert!(
        page.entries
            .contains(&(vec![b'k', 1, 0, 255], vec![0, 255, 1])),
        "typed write must preserve opaque binary bytes"
    );
    snapshots
        .close(descriptor, BUDGET)
        .await
        .context("snapshot close")?;

    for wrong in [
        RemoteIdentity {
            group: GROUP + 1,
            ..identity
        },
        RemoteIdentity {
            node: identity.node + 100,
            ..identity
        },
    ] {
        let mut wrong = Client::connect(endpoint, wrong, BUDGET)
            .await
            .context("connect wrong-identity client")?;
        assert!(matches!(
            wrong.probe(SHARD).await,
            Err(RemoteError::InvalidArgument)
        ));
        assert!(matches!(
            wrong.metadata(SHARD).await,
            Err(RemoteError::InvalidArgument)
        ));
        assert!(matches!(
            wrong.write(write(3, None)).await,
            Err(RemoteError::InvalidArgument)
        ));
    }
    for requested_shard in [0, SHARD + 1] {
        assert!(client.probe(requested_shard).await.is_err());
        assert!(client.metadata(requested_shard).await.is_err());
        let mut wrong = write(3, None);
        wrong.shard = requested_shard;
        assert!(client.write(wrong).await.is_err());
    }
    let mut fenced = write(3, None);
    fenced.epoch = 2;
    assert!(matches!(
        client.write(fenced).await,
        Err(RemoteError::Application(D1Error::Fenced {
            requested: 2,
            current: 3
        }))
    ));
    let follower = (cluster.leader + 1) % 3;
    let mut other = Client::connect(cluster.grpc(follower), cluster.identity(follower), BUDGET)
        .await
        .context("connect follower client")?;
    for (operation, result) in [
        ("probe", other.probe(SHARD).await),
        ("write", other.write(write(3, None)).await.map(|_| ())),
    ] {
        assert!(
            matches!(result, Err(RemoteError::NotLeader { .. })),
            "follower {operation} must report typed NotLeader, not forward/retry: {result:?}"
        );
    }
    // Metadata deliberately retains the existing local-read semantics. It is
    // not a leader barrier and must not be advertised as a linearizable read.
    let local = other
        .metadata(SHARD)
        .await
        .context("follower local metadata")?;
    assert_eq!((local.0, local.1), (SHARD, 3));
    assert!(
        local.2 <= committed.2,
        "follower local metadata cannot be ahead of committed leader"
    );
    let mut too_many = write(3, None);
    too_many.operations = vec![WriteOp::Delete { key: vec![1] }; d2_raft::MAX_BATCH_OPERATIONS + 1];
    assert!(matches!(
        client.write(too_many).await,
        Err(RemoteError::ResourceExhausted)
    ));
    // Native bytes fit D1's batch ceiling, but their historical JSON logical
    // size exceeds 16 MiB. Typed transport must not silently widen admission.
    let mut expanded = write(3, None);
    expanded.operations = vec![WriteOp::Put {
        key: b"logical-limit".to_vec(),
        value: vec![255; 5 * 1024 * 1024],
    }];
    assert!(matches!(
        client.write(expanded).await,
        Err(RemoteError::ResourceExhausted)
    ));
    assert_eq!(
        client.metadata(SHARD).await.context("leader metadata")?,
        committed
    );
    malformed(endpoint, identity)
        .await
        .context("malformed requests")?;
    late_body_cannot_commit(endpoint, identity)
        .await
        .context("late body")?;
    // Wrong protocol never falls back. The retired legacy port must not listen.
    assert!(TcpStream::connect(cluster.tcp(cluster.leader))
        .await
        .is_err());
    if let Ok(mut wrong_transport) = Client::connect(
        cluster.tcp(cluster.leader),
        identity,
        Duration::from_millis(300),
    )
    .await
    {
        assert!(wrong_transport.probe(SHARD).await.is_err());
    }
    // Convenience wrappers now use the same typed service as explicit clients.
    remote::probe_identified(endpoint, identity, SHARD, BUDGET)
        .await
        .context("typed convenience probe")?;
    assert_eq!(
        remote::metadata_identified(endpoint, identity, SHARD, BUDGET)
            .await
            .context("typed convenience metadata")?,
        client
            .metadata(SHARD)
            .await
            .context("explicit metadata for convenience equivalence")?
    );
    raw_legacy_write_is_rejected(endpoint, identity)
        .await
        .context("raw legacy write rejection and no-side-effect checks")?;
    assert_eq!(
        client.metadata(SHARD).await.context("leader metadata")?,
        committed
    );
    cli_checks(endpoint, cluster.tcp(cluster.leader), identity, committed.2)
        .await
        .context("typed CLI and retired-port checks")?;
    unknown_outcome(endpoint, identity)
        .await
        .context("unknown outcome")?;
    cached_adapter_unknown_outcome(endpoint, identity)
        .await
        .context("cached adapter outcome")?;
    server_query_cli(endpoint, identity)
        .await
        .context("server query CLI")?;
    for process in &mut cluster._processes {
        process.success = true;
    }
    Ok(())
}

async fn raw_legacy_write_is_rejected(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
) -> anyhow::Result<()> {
    let mut typed = Client::connect(endpoint, identity, BUDGET)
        .await
        .context("raw rejection: connect typed control client")?;
    let before = typed
        .metadata(SHARD)
        .await
        .context("raw rejection: baseline metadata")?;
    let request = write(240, Some(before.2));
    let key = vec![b'k', 240, 0, 255];
    let body = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "identity": identity,
        "operation": {"Write": request}
    }))
    .context("raw rejection: encode legacy write fixture")?;
    let mut bytes = (body.len() as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(&body);
    let mut socket = TcpStream::connect(endpoint)
        .await
        .context("raw rejection: connect legacy socket")?;
    timeout(BUDGET, async {
        if let Err(error) = socket.write_all(&bytes).await {
            if !matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) {
                return Err(error.into());
            }
        }
        let mut received = Vec::new();
        loop {
            let mut chunk = [0; 1024];
            match socket.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => {
                    ensure!(
                        received.len() + n <= 4096,
                        "unexpected protocol rejection body"
                    );
                    received.extend_from_slice(&chunk[..n]);
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                    ) =>
                {
                    break
                }
                Err(error) => return Err(error.into()),
            }
        }
        // EOF/reset above is mandatory; any bytes before it must be complete,
        // valid HTTP/2 connection-control frames, never an application reply.
        ensure!(
            only_h2_connection_control(&received),
            "raw legacy request received application data or malformed framing"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("raw legacy request must terminate, not merely send HTTP/2 settings")??;
    // A typed leader barrier and full small-fixture snapshot check the rejected
    // request caused neither a sequence advance nor a hidden graph write.
    typed
        .probe(SHARD)
        .await
        .context("raw rejection: post-rejection leader barrier")?;
    assert_eq!(
        typed
            .metadata(SHARD)
            .await
            .context("raw rejection: post-rejection metadata")?,
        before
    );
    let snapshots = SnapshotClient::connect(endpoint, identity, BUDGET)
        .await
        .context("raw rejection: connect snapshot client")?;
    let descriptor = snapshots
        .open(SHARD, BUDGET)
        .await
        .context("raw rejection: open absence-check snapshot")?;
    let page = snapshots
        .page(descriptor.clone(), None, BUDGET)
        .await
        .context("raw rejection: read absence-check page")?;
    assert!(
        page.next.is_none(),
        "small fixture must fit one complete page"
    );
    assert!(page.entries.iter().all(|(actual, _)| actual != &key));
    snapshots
        .close(descriptor, BUDGET)
        .await
        .context("raw rejection: close snapshot")?;
    Ok(())
}

fn only_h2_connection_control(mut bytes: &[u8]) -> bool {
    while !bytes.is_empty() {
        if bytes.len() < 9 {
            return false;
        }
        let length =
            (usize::from(bytes[0]) << 16) | (usize::from(bytes[1]) << 8) | usize::from(bytes[2]);
        let kind = bytes[3];
        let flags = bytes[4];
        // No client streams exist. Also reject a set reserved stream-id bit.
        if bytes[5..9] != [0, 0, 0, 0] || length > bytes.len() - 9 {
            return false;
        }
        let payload = &bytes[9..9 + length];
        let settings = payload.as_chunks::<6>().0;
        let valid = match kind {
            // SETTINGS: only ACK is defined; ACK must have an empty payload.
            4 if flags == 1 => payload.is_empty(),
            4 if flags == 0 && length.is_multiple_of(6) => settings.iter().all(|setting| {
                let id = u16::from_be_bytes([setting[0], setting[1]]);
                let value = u32::from_be_bytes(setting[2..6].try_into().unwrap());
                match id {
                    2 | 8 => value <= 1,
                    4 => value <= 0x7fff_ffff,
                    5 => (16_384..=16_777_215).contains(&value),
                    _ => true,
                }
            }),
            // PING: eight opaque bytes, only the ACK flag is defined.
            6 => flags <= 1 && length == 8,
            // GOAWAY: last stream is necessarily zero, followed by error code
            // and optional opaque debug bytes.
            7 => flags == 0 && length >= 8 && payload[..4] == [0, 0, 0, 0],
            // Connection WINDOW_UPDATE: a nonzero, 31-bit increment.
            8 if flags == 0 && length == 4 => {
                let increment = u32::from_be_bytes(payload.try_into().unwrap());
                (1..=0x7fff_ffff).contains(&increment)
            }
            // DATA, HEADERS, stream controls, extensions and unknown framing
            // are not evidence of rejecting a pre-HTTP/2 JSON request.
            _ => false,
        };
        if !valid {
            return false;
        }
        bytes = &bytes[9 + length..];
    }
    true
}

#[test]
fn raw_rejection_accepts_only_complete_h2_connection_controls() {
    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let length = payload.len() as u32;
        let mut bytes = length.to_be_bytes()[1..].to_vec();
        bytes.extend([kind, flags]);
        bytes.extend(stream.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }
    assert!(only_h2_connection_control(&[]));
    let controls = [
        frame(4, 0, 0, &[0, 3, 0, 0, 0, 100]),
        frame(4, 1, 0, &[]),
        frame(6, 0, 0, &[0; 8]),
        frame(6, 1, 0, &[0; 8]),
        frame(8, 0, 0, &[0, 0, 0, 1]),
        frame(7, 0, 0, &[0, 0, 0, 0, 0, 0, 0, 1]),
    ];
    assert!(only_h2_connection_control(&controls.concat()));
    for control in &controls {
        assert!(only_h2_connection_control(control));
        for length in 1..control.len() {
            assert!(!only_h2_connection_control(&control[..length]));
        }
    }
    for reply in [
        serde_json::json!({"version":1,"result":{"Ok":"Probe"}}),
        serde_json::json!({"version":1,"result":{"Err":"InvalidArgument"}}),
    ] {
        let json = serde_json::to_vec(&reply).unwrap();
        let mut legacy = (json.len() as u32).to_be_bytes().to_vec();
        legacy.extend_from_slice(&json);
        assert!(!only_h2_connection_control(&legacy));
        assert!(!only_h2_connection_control(&json));
    }
    for invalid in [
        frame(0, 0, 0, &[]),
        frame(0, 1, 1, b"application data"),
        frame(1, 4, 1, &[]),
        frame(4, 0, 1, &[]),
        frame(4, 2, 0, &[]),
        frame(4, 1, 0, &[0; 6]),
        frame(4, 0, 0, &[0]),
        frame(4, 0, 0, &[0, 2, 0, 0, 0, 2]),
        frame(4, 0, 0, &[0, 4, 128, 0, 0, 0]),
        frame(4, 0, 0, &[0, 5, 0, 0, 0, 1]),
        frame(6, 0, 0, &[0; 7]),
        frame(7, 0, 0, &[0; 7]),
        frame(7, 0, 0, &[0, 0, 0, 1, 0, 0, 0, 0]),
        frame(8, 0, 0, &[0; 4]),
        frame(8, 0, 0, &[128, 0, 0, 1]),
        vec![0],
        vec![255; 9],
    ] {
        assert!(!only_h2_connection_control(&invalid), "{invalid:?}");
        let mut after_settings = controls[0].clone();
        after_settings.extend(invalid);
        assert!(!only_h2_connection_control(&after_settings));
    }
}

async fn malformed(endpoint: SocketAddr, identity: RemoteIdentity) -> anyhow::Result<()> {
    let mut client =
        w::shard_operations_client::ShardOperationsClient::connect(format!("http://{endpoint}"))
            .await?;
    for (version, millis, operation) in [
        (99, 1000, Some(w::request::Operation::Probe(SHARD))),
        (1, 0, Some(w::request::Operation::Probe(SHARD))),
        (1, 60001, Some(w::request::Operation::Probe(SHARD))),
        (1, 1000, None),
    ] {
        let result = client
            .call(w::Request {
                version,
                identity: Some(w::Identity {
                    group: identity.group,
                    node: identity.node,
                }),
                timeout_millis: millis,
                operation,
            })
            .await;
        match result {
            Err(_) => {}
            Ok(response) => assert!(matches!(
                response.into_inner().result,
                Some(w::response::Result::Error(_))
            )),
        }
    }
    Ok(())
}

async fn cli_checks(
    endpoint: SocketAddr,
    legacy: SocketAddr,
    identity: RemoteIdentity,
    sequence: u64,
) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("write.json");
    std::fs::write(&path, serde_json::to_vec(&write(9, Some(sequence)))?)?;
    for operation in ["probe", "metadata", "write"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_graph-client"));
        command.env("TOKIO_WORKER_THREADS", "2").args([
            "rpc",
            operation,
            "--allow-insecure-development",
            "--endpoint",
            &endpoint.to_string(),
            "--group",
            &identity.group.to_string(),
            "--node",
            &identity.node.to_string(),
            "--shard",
            &SHARD.to_string(),
        ]);
        if operation == "write" {
            command.args(["--file", path.to_str().unwrap()]);
        }
        assert!(run_cli(command).await?, "typed CLI {operation} failed");
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_graph-client"));
    command.env("TOKIO_WORKER_THREADS", "2").args([
        "rpc",
        "probe",
        "--allow-insecure-development",
        "--endpoint",
        &legacy.to_string(),
        "--group",
        &identity.group.to_string(),
        "--node",
        &identity.node.to_string(),
        "--shard",
        &SHARD.to_string(),
        "--timeout-ms",
        "300",
    ]);
    assert!(
        !run_cli(command).await?,
        "CLI must not silently fallback to legacy TCP"
    );
    Ok(())
}
async fn run_cli(command: Command) -> anyhow::Result<bool> {
    Ok(run_child(command).await?.success())
}

async fn run_child(mut command: Command) -> anyhow::Result<std::process::ExitStatus> {
    let mut child = Process(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    timeout(Duration::from_secs(8), async {
        loop {
            if let Some(status) = child.0.try_wait()? {
                return Ok(status);
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

// Counts and transport terminal states only: never request/response payloads.
#[derive(Debug, Default)]
struct ProxyEvidence {
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    request_eof: AtomicBool,
    response_eof: AtomicBool,
    tasks_finished: AtomicU64,
    tasks_failed: AtomicU64,
}
struct CountedRequest<R> {
    inner: R,
    evidence: Arc<ProxyEvidence>,
}
impl<R: AsyncRead + Unpin> AsyncRead for CountedRequest<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let capacity = buf.remaining();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let count = buf.filled().len() - before;
            self.evidence
                .request_bytes
                .fetch_add(count as u64, Ordering::Relaxed);
            if count == 0 && capacity > 0 {
                self.evidence.request_eof.store(true, Ordering::Relaxed);
            }
        }
        result
    }
}

struct Proxy {
    address: SocketAddr,
    paused: watch::Sender<bool>,
    blocked: Arc<Notify>,
    evidence: Arc<ProxyEvidence>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Proxy {
    async fn start(upstream: SocketAddr) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (paused, pause) = watch::channel(false);
        let blocked = Arc::new(Notify::new());
        let signal = blocked.clone();
        let evidence = Arc::new(ProxyEvidence::default());
        let task_evidence = evidence.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client,_)) = accepted else { break };
                        let mut pause = pause.clone(); let signal=signal.clone();
                        let evidence = task_evidence.clone();
                        tasks.spawn(async move {
                            let server=TcpStream::connect(upstream).await?;
                            let (cr,mut cw)=client.into_split(); let (mut sr,mut sw)=server.into_split();
                            let mut cr = CountedRequest { inner: cr, evidence: evidence.clone() };
                            let requests=tokio::io::copy(&mut cr,&mut sw);
                            let responses=async {
                                let mut buf=[0;16384];
                                loop {
                                    let n=sr.read(&mut buf).await?;
                                    evidence.response_bytes.fetch_add(n as u64, Ordering::Relaxed);
                                    if n==0 { evidence.response_eof.store(true, Ordering::Relaxed); break; }
                                    while *pause.borrow_and_update() { signal.notify_one(); if pause.changed().await.is_err() { return Ok::<(),std::io::Error>(()); } }
                                    cw.write_all(&buf[..n]).await?;
                                }
                                Ok::<(),std::io::Error>(())
                            };
                            tokio::select! {r=requests=>{r?;},r=responses=>{r?;}}
                            Ok::<(),std::io::Error>(())
                        });
                    },
                    finished=tasks.join_next(),if !tasks.is_empty()=>{
                        task_evidence.tasks_finished.fetch_add(1, Ordering::Relaxed);
                        if !matches!(finished, Some(Ok(Ok(())))) {
                            task_evidence.tasks_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
        Ok(Self {
            address,
            paused,
            blocked,
            evidence,
            task,
        })
    }
}
async fn unknown_outcome(endpoint: SocketAddr, identity: RemoteIdentity) -> anyhow::Result<()> {
    let proxy = Proxy::start(endpoint).await?;
    let mut slow = Client::connect(proxy.address, identity, Duration::from_millis(500)).await?;
    slow.probe(SHARD).await?;
    let mut direct = Client::connect(endpoint, identity, BUDGET)
        .await
        .context("connect leader")?;
    let before = direct.metadata(SHARD).await?.2;
    let request = write(10, Some(before));
    proxy.paused.send(true)?;
    let before_gate = format!("{:?}", proxy.evidence);
    let pending_request = request.clone();
    let pending = tokio::spawn(async move { slow.write(pending_request).await });
    if let Err(error) = timeout(BUDGET, proxy.blocked.notified()).await {
        eprintln!(
            "reply_gate_failure phase=unknown_outcome before={before_gate} after={:?}",
            proxy.evidence
        );
        if pending.is_finished() {
            eprintln!(
                "reply_gate_failure phase=unknown_outcome pending={:?}",
                pending.await
            );
        } else {
            eprintln!("reply_gate_failure phase=unknown_outcome pending=not_finished");
            pending.abort();
        }
        return Err(error).context("write response reached gate");
    }
    let result = timeout(Duration::from_secs(2), pending).await??;
    assert!(
        matches!(result,Err(RemoteError::Timeout {request_id:Some(id)}) if id==request.request_id),
        "unknown write outcome must preserve request identity: {result:?}"
    );
    proxy.paused.send(false)?;
    ensure!(
        direct.metadata(SHARD).await?.2 == before + 1,
        "timed-out write actually committed; no rollback inference"
    );
    assert!(
        matches!(direct.write(request).await?,D1Result::Write {status:D1WriteStatus::Duplicate,sequence,..} if sequence==before+1)
    );
    Ok(())
}

async fn cached_adapter_unknown_outcome(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
) -> anyhow::Result<()> {
    use replicated_graph::{run_blocking, RemoteStorageNode};
    use velagraph::{StorageNode, StorageNodeError, WriteRequest, WriteStatus};
    let proxy = Proxy::start(endpoint).await?;
    let adapter = RemoteStorageNode::new_identified(
        proxy.address,
        identity.group,
        identity.node,
        SHARD,
        Duration::from_millis(500),
    )?;
    let mut direct = Client::connect(endpoint, identity, BUDGET)
        .await
        .context("connect leader")?;
    let epoch = direct.metadata(SHARD).await?.1;
    let request = |id| WriteRequest {
        shard: SHARD,
        epoch,
        request_id: [id; 16],
        operations: vec![velagraph::WriteOp::Put {
            key: b"cached-adapter-unknown-outcome".to_vec(),
            value: vec![id],
        }],
    };
    // Warm the same cached channel with a completed write, then lose its next reply.
    let warm = adapter.clone();
    let warm_request = request(11);
    run_blocking(move || warm.batch_write(warm_request)).await??;
    let before = direct.metadata(SHARD).await?.2;
    let pending_adapter = adapter.clone();
    let pending_request = request(12);
    proxy.paused.send(true)?;
    let before_gate = format!("{:?}", proxy.evidence);
    let pending = tokio::spawn(run_blocking(move || {
        pending_adapter.batch_write(pending_request)
    }));
    if let Err(error) = timeout(BUDGET, proxy.blocked.notified()).await {
        eprintln!(
            "reply_gate_failure phase=cached_adapter before={before_gate} after={:?}",
            proxy.evidence
        );
        if pending.is_finished() {
            eprintln!(
                "reply_gate_failure phase=cached_adapter pending={:?}",
                pending.await
            );
        } else {
            eprintln!("reply_gate_failure phase=cached_adapter pending=not_finished");
            pending.abort();
        }
        return Err(error).context("cached write reply gate");
    }
    let result = timeout(Duration::from_secs(2), pending).await???;
    assert!(
        matches!(result, Err(StorageNodeError::Timeout {request_id:Some(id)}) if id == [12;16]),
        "cached adapter must preserve ambiguous write identity: {result:?}"
    );
    proxy.paused.send(false)?;
    ensure!(
        direct.metadata(SHARD).await?.2 == before + 1,
        "cached write committed despite a missing reply"
    );
    // Only an explicit application resubmission resolves the ambiguous outcome.
    let replay = request(12);
    let result = run_blocking(move || adapter.batch_write(replay)).await??;
    ensure!(result.status == WriteStatus::Duplicate && result.sequence == before + 1);
    Ok(())
}

async fn late_body_cannot_commit(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
) -> anyhow::Result<()> {
    use d2_raft::managed_rpc::grpc::wire as managed;
    use prost::Message;
    use tonic::codegen::http;
    let mut direct = Client::connect(endpoint, identity, BUDGET)
        .await
        .context("connect leader")?;
    let before = direct.metadata(SHARD).await?;
    let key = b"late-header-deadline".to_vec();
    let message = w::Request {
        version: 1,
        identity: Some(w::Identity {
            group: identity.group,
            node: identity.node,
        }),
        // The transport header is intentionally shorter than this DTO budget.
        timeout_millis: 5000,
        operation: Some(w::request::Operation::Write(managed::D1WriteRequest {
            shard: SHARD,
            epoch: 3,
            request_id: vec![11; 16],
            expected_sequence: Some(before.2),
            operations: vec![managed::WriteOp {
                kind: Some(managed::write_op::Kind::Put(managed::Put {
                    key: key.clone(),
                    value: b"must-not-commit".to_vec(),
                })),
            }],
        })),
    }
    .encode_to_vec();
    let socket = TcpStream::connect(endpoint).await?;
    let (mut sender, connection) = h2::client::handshake(socket).await?;
    let driver = tokio::spawn(connection);
    let headers = http::Request::builder()
        .method("POST")
        .uri(format!(
            "http://{endpoint}/velagraph.internal.operations.v1.ShardOperations/Call"
        ))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("grpc-timeout", "100m")
        .body(())?;
    let (reply, mut body) = sender.send_request(headers, false)?;
    let mut prefix = vec![0];
    prefix.extend_from_slice(&(message.len() as u32).to_be_bytes());
    prefix.extend_from_slice(&message[..message.len() - 1]);
    body.send_data(prefix.into(), false)?;
    // Full protobuf decoding and dispatch cannot happen until the final byte.
    // No response-direction proxy or mutation retry is involved in this case.
    sleep(Duration::from_millis(250)).await;
    let _ = body.send_data(
        prost::bytes::Bytes::copy_from_slice(&message[message.len() - 1..]),
        true,
    );
    let terminal = timeout(Duration::from_secs(2), async {
        let response = match reply.await {
            Ok(response) => response,
            Err(_) => return Ok::<bool, anyhow::Error>(true),
        };
        let header_failed = response
            .headers()
            .get("grpc-status")
            .is_some_and(|s| s != "0");
        let mut incoming = response.into_body();
        let mut bytes = Vec::new();
        while let Some(data) = incoming.data().await {
            match data {
                Err(_) => return Ok(true),
                Ok(data) => {
                    ensure!(
                        bytes.len() + data.len() <= 4096,
                        "unexpected large timeout reply"
                    );
                    bytes.extend_from_slice(&data);
                }
            }
        }
        let trailer_failed = match incoming.trailers().await {
            Err(_) => true,
            Ok(trailers) => trailers
                .as_ref()
                .and_then(|t| t.get("grpc-status"))
                .is_some_and(|s| s != "0"),
        };
        let typed_failed = if bytes.len() >= 5 {
            let length = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
            length == bytes.len() - 5
                && matches!(
                    w::Response::decode(&bytes[5..])?.result,
                    Some(w::response::Result::Error(_))
                )
        } else {
            false
        };
        Ok(header_failed || trailer_failed || typed_failed)
    })
    .await??;
    assert!(terminal, "expired incomplete Write must not return success");
    driver.abort();
    let _ = driver.await;
    direct.probe(SHARD).await?;
    assert_eq!(
        direct.metadata(SHARD).await?,
        before,
        "late body must not advance the graph sequence"
    );
    let snapshots = SnapshotClient::connect(endpoint, identity, BUDGET)
        .await
        .context("connect leader")?;
    let descriptor = snapshots
        .open(SHARD, BUDGET)
        .await
        .context("snapshot open")?;
    let page = snapshots
        .page(descriptor.clone(), None, BUDGET)
        .await
        .context("snapshot page")?;
    assert!(
        page.next.is_none(),
        "small fixture must fit one page for exhaustive absence check"
    );
    assert!(
        page.entries.iter().all(|(actual, _)| actual != &key),
        "expired request wrote its value"
    );
    snapshots
        .close(descriptor, BUDGET)
        .await
        .context("snapshot close")?;
    Ok(())
}

// The product node now has one graph endpoint: storage routing and both text
// query CLI modes must work on it, not only the low-level operations service.
async fn server_query_cli(endpoint: SocketAddr, identity: RemoteIdentity) -> anyhow::Result<()> {
    use replicated_graph::{run_blocking, RemoteStorageNode};
    use velagraph::{experimental_graph::AppendOnlyStore, GraphRecordStore, Value, Vertex};
    let node =
        RemoteStorageNode::new_identified(endpoint, identity.group, identity.node, SHARD, BUDGET)?;
    run_blocking(move || -> anyhow::Result<()> {
        let mut vertex = Vertex::new(777);
        vertex
            .properties
            .insert("name".into(), Value::Text("rpc-query".into()));
        GraphRecordStore::new(node.clone(), SHARD, 23).create_vertex(&vertex)?;
        AppendOnlyStore::new(node.clone(), SHARD, 24)
            .prepare_vertex(&vertex)?
            .submit(&node)?;
        Ok(())
    })
    .await??;
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({
            "version":1, "timeout_ms":5000,
            "metadata":{"shard":99,"group":88,"epoch":1,"replicas":[1]},
            "metadata_node":1,
            "metadata_nodes":[{"group":88,"node":1,"endpoint":"127.0.0.1:9"}],
            "data_nodes":[{"group":identity.group,"node":identity.node,"endpoint":endpoint}]
        }))?,
    )?;
    for (mode, partition) in [("v1", 23), ("experimental", 24)] {
        let output = dir.path().join(format!("{mode}.json"));
        let file = std::fs::File::create(&output)?;
        let mut child = Process(
            Command::new(env!("CARGO_BIN_EXE_graph-client"))
                .env("TOKIO_WORKER_THREADS", "2")
                .args([
                    "--allow-insecure-development",
                    "--config",
                    config.to_str().unwrap(),
                    mode,
                    "server-query",
                    "--group",
                    &identity.group.to_string(),
                    "--node",
                    &identity.node.to_string(),
                    "--shard",
                    &SHARD.to_string(),
                    "--partition",
                    &partition.to_string(),
                    "--query",
                    "g.V(777).values('name')",
                ])
                .stdout(Stdio::from(file))
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        let status = timeout(Duration::from_secs(8), async {
            loop {
                if let Some(status) = child.0.try_wait()? {
                    return Ok::<_, std::io::Error>(status);
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        assert!(status.success(), "{mode} typed server-query CLI failed");
        let result: serde_json::Value = serde_json::from_slice(&std::fs::read(output)?)?;
        assert_eq!(result["ok"], true);
        assert_eq!(result["execution"], "server");
        let rows = result["output"]["rows"].as_array().context("query rows")?;
        assert_eq!(
            rows.len(),
            1,
            "selected vertex must produce exactly one value"
        );
        let source = if mode == "v1" {
            serde_json::Value::Null
        } else {
            serde_json::json!({"Vertex":{"partition":partition,"id":777}})
        };
        assert_eq!(
            rows[0],
            serde_json::json!({"Value":{"source":source,"value":{"Text":"rpc-query"}}})
        );
    }
    Ok(())
}

#[test]
fn diagnostic_tail_is_bounded_and_does_not_move_writer_offset() -> anyhow::Result<()> {
    let file = tempfile::NamedTempFile::new()?;
    let mut writer = file.reopen()?;
    writer.write_all(&vec![b'a'; 20 * 1024])?;
    assert_eq!(log_tail(file.path())?, vec![b'a'; 16 * 1024]);
    writer.write_all(b"end")?;
    let tail = log_tail(file.path())?;
    assert_eq!(tail.len(), 16 * 1024);
    assert!(tail.ends_with(b"end"));
    Ok(())
}

#[tokio::test]
async fn proxy_records_request_eof_without_a_response_gate() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let received = Arc::new(Notify::new());
    let signal = received.clone();
    let upstream = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = [0; 4];
        stream.read_exact(&mut request).await?;
        ensure!(&request == b"test", "fixture request");
        signal.notify_one();
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await?;
        Ok::<_, anyhow::Error>(())
    });
    let proxy = Proxy::start(address).await?;
    proxy.paused.send(true)?;
    let mut client = TcpStream::connect(proxy.address).await?;
    client.write_all(b"test").await?;
    timeout(BUDGET, received.notified()).await?;
    client.shutdown().await?;
    timeout(BUDGET, async {
        while proxy.evidence.tasks_finished.load(Ordering::Relaxed) == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    timeout(BUDGET, upstream).await???;
    assert_eq!(proxy.evidence.request_bytes.load(Ordering::Relaxed), 4);
    assert_eq!(proxy.evidence.response_bytes.load(Ordering::Relaxed), 0);
    assert!(proxy.evidence.request_eof.load(Ordering::Relaxed));
    assert_eq!(proxy.evidence.tasks_failed.load(Ordering::Relaxed), 0);
    assert!(timeout(Duration::from_millis(10), proxy.blocked.notified())
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn proxy_records_held_response_without_changing_gate() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let upstream = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = [0; 4];
        stream.read_exact(&mut request).await?;
        ensure!(&request == b"test", "fixture request");
        stream.write_all(b"ok").await?;
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await?;
        Ok::<_, anyhow::Error>(())
    });
    let proxy = Proxy::start(address).await?;
    proxy.paused.send(true)?;
    let mut client = TcpStream::connect(proxy.address).await?;
    client.write_all(b"test").await?;
    timeout(BUDGET, proxy.blocked.notified()).await?;
    assert_eq!(proxy.evidence.request_bytes.load(Ordering::Relaxed), 4);
    assert_eq!(proxy.evidence.response_bytes.load(Ordering::Relaxed), 2);
    proxy.paused.send(false)?;
    let mut response = [0; 2];
    timeout(BUDGET, client.read_exact(&mut response)).await??;
    assert_eq!(&response, b"ok");
    client.shutdown().await?;
    timeout(BUDGET, upstream).await???;
    Ok(())
}

fn transaction_options(id: u8) -> velagraph::TransactionOptions {
    velagraph::TransactionOptions {
        shard: SHARD,
        epoch: 3,
        request_id: [id; 16],
        cancellation: Default::default(),
        deadline: std::time::Instant::now() + BUDGET,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "child process for the transaction recovery test"]
async fn exit_with_remote_staged_writes() -> anyhow::Result<()> {
    use replicated_graph::{run_blocking, RemoteStorageNode};
    use velagraph::{StorageNode, WriteOp};
    let endpoint = std::env::var("VG_TRANSACTION_ENDPOINT")?.parse()?;
    let leader = std::env::var("VG_TRANSACTION_MEMBER")?.parse()?;
    let node = RemoteStorageNode::new_identified(endpoint, GROUP, leader, SHARD, BUDGET)?;
    run_blocking(move || -> anyhow::Result<()> {
        let mut pending = node.begin_v1(transaction_options(72))?;
        pending.stage(&WriteOp::put(b"unpublished", [1]))?;
        // Exit with a live handle. No commit, abort, or Rust destructors run.
        std::process::exit(73)
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_transactions_survive_client_exit_lost_reply_and_cluster_restart(
) -> anyhow::Result<()> {
    timeout(Duration::from_secs(100), remote_transaction_recovery()).await?
}

async fn remote_transaction_recovery() -> anyhow::Result<()> {
    use replicated_graph::{run_blocking, RemoteStorageNode};
    use velagraph::{StorageNode, StorageNodeError, TransactionError, WriteOp};
    let cluster = Cluster::start().await?;
    let endpoint = cluster.grpc(cluster.leader);
    let id = cluster.identity(cluster.leader);
    let adapter = |addr, member, budget| {
        RemoteStorageNode::new_identified(addr, GROUP, member, SHARD, budget)
    };
    let reader = adapter(endpoint, id.node, BUDGET)?;
    let follower = (cluster.leader + 1) % 3;
    let other = adapter(
        cluster.grpc(follower),
        cluster.identity(follower).node,
        BUDGET,
    )?;
    run_blocking(move || -> anyhow::Result<()> {
        assert!(matches!(
            other.begin_v1(transaction_options(1)),
            Err(TransactionError::Node(StorageNodeError::NotLeader { .. }))
        ));
        let mut wrong_epoch = transaction_options(1);
        wrong_epoch.epoch = 2;
        assert!(matches!(
            reader.begin_v1(wrong_epoch),
            Err(TransactionError::Node(StorageNodeError::Fenced {
                current: 3,
                ..
            }))
        ));
        let mut stale = reader.begin_v1(transaction_options(70))?;
        stale.stage(&WriteOp::put(b"stale", [1]))?;
        let mut tx = reader.begin_v1(transaction_options(71))?;
        tx.stage(&WriteOp::put(b"one", [1]))?;
        tx.stage(&WriteOp::put(b"two", [2]))?;
        let pinned = reader.read_view()?;
        assert_eq!(pinned.sequence, 0);
        assert_eq!(tx.commit()?.sequence, 1);
        assert!(matches!(
            stale.commit(),
            Err(TransactionError::Node(StorageNodeError::Conflict {
                expected: 0,
                current: 1
            }))
        ));
        assert_eq!(reader.get(b"one", &pinned)?, None);
        let mut aborted = reader.begin_v1(transaction_options(77))?;
        aborted.stage(&WriteOp::put(b"aborted", [1]))?;
        aborted.abort();
        let options = transaction_options(78);
        let cancellation = options.cancellation.clone();
        let mut cancelled = reader.begin_v1(options)?;
        cancelled.stage(&WriteOp::put(b"cancelled", [1]))?;
        cancellation.cancel();
        assert!(matches!(
            cancelled.commit(),
            Err(TransactionError::Cancelled)
        ));
        let mut poisoned = reader.begin_v1(transaction_options(74))?;
        poisoned.stage(&WriteOp::put(b"prefix", [1]))?;
        assert!(matches!(
            poisoned.stage(&WriteOp::delete(vec![0; d2_raft::MAX_BATCH_BYTES])),
            Err(TransactionError::Node(StorageNodeError::ResourceExhausted(
                _
            )))
        ));
        assert!(matches!(poisoned.commit(), Err(TransactionError::Aborted)));
        Ok(())
    })
    .await??;
    let mut child = Command::new(std::env::current_exe()?);
    child
        .args(["--ignored", "--exact", "exit_with_remote_staged_writes"])
        .env("VG_TRANSACTION_ENDPOINT", endpoint.to_string())
        .env("VG_TRANSACTION_MEMBER", id.node.to_string());
    ensure!(
        run_child(child).await?.code() == Some(73),
        "client did not exit with live staging"
    );

    let proxy = Proxy::start(endpoint).await?;
    let writer = adapter(proxy.address, id.node, Duration::from_millis(900))?;
    let pause = proxy.paused.clone();
    let pending = tokio::spawn(run_blocking(move || -> anyhow::Result<()> {
        let mut tx = writer.begin_v1(transaction_options(73))?;
        tx.stage(&WriteOp::put(b"three", [3]))?;
        tx.stage(&WriteOp::put(b"four", [4]))?;
        pause.send(true)?;
        assert!(
            matches!(tx.commit(), Err(TransactionError::UnknownOutcome { request_id, .. }) if request_id == [73; 16])
        );
        Ok(())
    }));
    timeout(BUDGET, proxy.blocked.notified()).await?;
    pending.await???;
    proxy.paused.send(false)?;
    // Observe actual application independently before killing all voters.
    timeout(BUDGET, async {
        let mut verifier = Client::connect(endpoint, id, BUDGET).await?;
        while verifier.metadata(SHARD).await?.2 != 2 {
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<(), RemoteError>(())
    })
    .await??;
    drop(proxy);
    let mut reopened = cluster.restart().await?;
    let reader = adapter(
        reopened.grpc(reopened.leader),
        reopened.identity(reopened.leader).node,
        BUDGET,
    )?;
    run_blocking(move || -> anyhow::Result<()> {
        let view = reader.read_view()?;
        assert_eq!(view.sequence, 2);
        for (key, value) in [
            (b"one".as_slice(), 1),
            (b"two", 2),
            (b"three", 3),
            (b"four", 4),
        ] {
            assert_eq!(reader.get(key, &view)?, Some(vec![value]));
        }
        for key in [
            b"unpublished".as_slice(),
            b"stale",
            b"prefix",
            b"aborted",
            b"cancelled",
        ] {
            assert_eq!(reader.get(key, &view)?, None);
        }
        Ok(())
    })
    .await??;
    let endpoint = reopened.grpc(reopened.leader);
    let identity = reopened.identity(reopened.leader);
    let reader = adapter(endpoint, identity.node, BUDGET)?;
    let runtime = tokio::runtime::Handle::current();
    run_blocking(move || -> anyhow::Result<()> {
        let mut pending = reader.begin_v1(transaction_options(75))?;
        pending.stage(&WriteOp::put(b"managed", [5]))?;
        runtime.block_on(remote::managed::request(
            endpoint,
            identity,
            remote::managed::Request::Enable {
                shard: SHARD,
                epoch: 3,
                expected_sequence: 2,
                mode_id: [9; 16],
            },
            BUDGET,
        ))?;
        assert!(matches!(
            reader.begin_v1(transaction_options(76)),
            Err(TransactionError::Node(StorageNodeError::Unsupported(_)))
        ));
        assert!(matches!(
            pending.commit(),
            Err(TransactionError::Node(StorageNodeError::Unsupported(_)))
        ));
        let view = reader.read_view()?;
        assert_eq!(view.sequence, 2);
        assert_eq!(reader.get(b"managed", &view)?, None);
        Ok(())
    })
    .await??;
    for process in &mut reopened._processes {
        process.success = true;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_engine_recovers_department_membership_after_voter_restart() -> anyhow::Result<()> {
    use replicated_graph::{run_blocking, RemoteStorageNode};
    use velagraph::{Edge, Error, GraphEngine, GremlinRow, StorageNodeError, Value, Vertex};

    let engine = |cluster: &Cluster| -> anyhow::Result<_> {
        let node = RemoteStorageNode::new_identified(
            cluster.grpc(cluster.leader),
            GROUP,
            cluster.identity(cluster.leader).node,
            SHARD,
            BUDGET,
        )?;
        Ok(GraphEngine::from_storage_node(node, SHARD, 4))
    };
    let mut employee = Vertex::new(1);
    employee.labels.push("employee".into());
    employee
        .properties
        .insert("name".into(), Value::Text("Ada".into()));
    let mut department = Vertex::new(2);
    department.labels.push("department".into());
    let membership = Edge::new(9, 1, 2, "member_of");
    let cluster = Cluster::start().await?;
    let writer = engine(&cluster)?;
    let (employee, department, membership, committed, stale) =
        run_blocking(move || -> velagraph::Result<_> {
            writer.create_vertex(&employee)?;
            writer.create_vertex(&department)?;
            let before = writer.read_view()?;
            let stale = writer.records().prepare_delete_vertex(department.id)?;
            let committed = writer.records().prepare_create_edge(&membership)?;
            writer.records().commit_prepared(&committed)?;
            assert_eq!(before.sequence(), 2);
            assert_eq!(before.get_edge(membership.id)?, None);
            Ok((employee, department, membership, committed, stale))
        })
        .await??;

    // Kill all three OS processes and reopen their original databases.
    let mut reopened = cluster.restart().await?;
    let reader = engine(&reopened)?;
    run_blocking(move || -> velagraph::Result<()> {
        assert_eq!(reader.partition(), 4);
        assert_eq!(reader.read_view()?.sequence(), 3);
        reader.records().commit_prepared(&committed)?;
        assert!(matches!(
            reader.records().commit_prepared(&stale),
            Err(Error::StorageNode(StorageNodeError::Conflict {
                expected: 2,
                current: 3
            }))
        ));
        assert_eq!(reader.read_view()?.sequence(), 3);
        assert_eq!(reader.get_vertex(1)?, Some(employee));
        assert_eq!(reader.get_vertex(2)?, Some(department));
        assert_eq!(reader.get_edge(9)?, Some(membership.clone()));
        assert_eq!(
            reader.out_edges(1, Some("member_of"))?,
            vec![membership.clone()]
        );
        assert_eq!(reader.in_edges(2, Some("member_of"))?, vec![membership]);
        assert!(reader.out_edges(1, Some("reports_to"))?.is_empty());
        assert_eq!(
            reader.gremlin("g.V(1).out('member_of')")?.rows,
            vec![GremlinRow::Vertex(2)]
        );
        assert!(reader.delete_vertex(2)?);
        assert_eq!(reader.read_view()?.sequence(), 4);
        assert_eq!(reader.get_edge(9)?, None);
        assert!(reader.out_edges(1, None)?.is_empty());
        Ok(())
    })
    .await??;
    for process in &mut reopened._processes {
        process.success = true;
    }
    Ok(())
}

#[path = "support/graph_replication_delivery.rs"]
mod graph_replication_delivery;
