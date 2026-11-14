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
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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
struct Cluster {
    addresses: Vec<SocketAddr>,
    leader: usize,
    _processes: Vec<ObservedProcess>,
    _dirs: Vec<tempfile::TempDir>,
}
impl Cluster {
    async fn start() -> anyhow::Result<Self> {
        let mut reservations = Vec::new();
        for _ in 0..12 {
            reservations.push(TcpListener::bind("127.0.0.1:0").await?);
        }
        let addresses = reservations
            .iter()
            .map(|x| x.local_addr())
            .collect::<std::io::Result<Vec<_>>>()?;
        let bootstrap = (0..3)
            .map(|i| format!("{}={}", i + 1, addresses[4 * i]))
            .collect::<Vec<_>>()
            .join(",");
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<std::io::Result<Vec<_>>>()?;
        drop(reservations);
        let mut processes = Vec::new();
        for i in 0..3 {
            let mut command = Command::new(env!("CARGO_BIN_EXE_graph-node"));
            command.env("TOKIO_WORKER_THREADS", "2").args([
                "--allow-insecure-development",
                "--group",
                &GROUP.to_string(),
                "--id",
                &(i + 1).to_string(),
                "--raft",
                &addresses[4 * i].to_string(),
                "--control",
                &addresses[4 * i + 1].to_string(),
                "--graph",
                &addresses[4 * i + 3].to_string(),
                "--shard",
                "7",
                "--epoch",
                "3",
                "--data-dir",
                dirs[i].path().to_str().context("data dir")?,
            ]);
            if i == 0 {
                command.args(["--bootstrap", &bootstrap]);
            }
            let log = tempfile::NamedTempFile::new()?;
            let output = log.reopen()?;
            let child = command
                .stdout(Stdio::from(output.try_clone()?))
                .stderr(Stdio::from(output))
                .spawn()
                .with_context(|| format!("spawn shard node {}", i + 1))?;
            processes.push(ObservedProcess {
                process: Process(child),
                log,
                node: i + 1,
                success: false,
            });
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
        let mut wrong = Client::connect(endpoint, wrong, BUDGET).await?;
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
    let mut other =
        Client::connect(cluster.grpc(follower), cluster.identity(follower), BUDGET).await?;
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
    let local = other.metadata(SHARD).await?;
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
    remote::probe_identified(endpoint, identity, SHARD, BUDGET).await?;
    assert_eq!(
        remote::metadata_identified(endpoint, identity, SHARD, BUDGET).await?,
        client.metadata(SHARD).await?
    );
    raw_legacy_write_is_rejected(endpoint, identity).await?;
    assert_eq!(
        client.metadata(SHARD).await.context("leader metadata")?,
        committed
    );
    cli_checks(endpoint, cluster.tcp(cluster.leader), identity, committed.2).await?;
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
    let mut typed = Client::connect(endpoint, identity, BUDGET).await?;
    let before = typed.metadata(SHARD).await?;
    let request = write(240, Some(before.2));
    let key = vec![b'k', 240, 0, 255];
    let body = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "identity": identity,
        "operation": {"Write": request}
    }))?;
    let mut bytes = (body.len() as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(&body);
    let mut socket = TcpStream::connect(endpoint).await?;
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
                    ensure!(received.len() + n <= 4096, "unexpected protocol rejection body");
                    received.extend_from_slice(&chunk[..n]);
                }
                Err(error) if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) => break,
                Err(error) => return Err(error.into()),
            }
        }
        // HTTP/2 SETTINGS/GOAWAY bytes alone are not rejection evidence:
        // the loop must reach EOF/reset. Nor may a legacy JSON adapter answer.
        if received.len() >= 4 {
            let size = u32::from_be_bytes(received[..4].try_into().unwrap()) as usize;
            if size <= received.len() - 4 {
                ensure!(
                    serde_json::from_slice::<serde_json::Value>(&received[4..4 + size]).is_err(),
                    "retired JSON protocol received a legacy response"
                );
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("raw legacy request must terminate, not merely send HTTP/2 settings")??;
    // A typed leader barrier and full small-fixture snapshot check the rejected
    // request caused neither a sequence advance nor a hidden graph write.
    typed.probe(SHARD).await?;
    assert_eq!(typed.metadata(SHARD).await?, before);
    let snapshots = SnapshotClient::connect(endpoint, identity, BUDGET).await?;
    let descriptor = snapshots.open(SHARD, BUDGET).await?;
    let page = snapshots.page(descriptor.clone(), None, BUDGET).await?;
    assert!(page.next.is_none(), "small fixture must fit one complete page");
    assert!(page.entries.iter().all(|(actual, _)| actual != &key));
    snapshots.close(descriptor, BUDGET).await?;
    Ok(())
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
async fn run_cli(mut command: Command) -> anyhow::Result<bool> {
    let mut child = Process(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    timeout(Duration::from_secs(8), async {
        loop {
            if let Some(status) = child.0.try_wait()? {
                return Ok(status.success());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

struct Proxy {
    address: SocketAddr,
    paused: watch::Sender<bool>,
    blocked: Arc<Notify>,
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
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client,_)) = accepted else { break };
                        let mut pause = pause.clone(); let signal=signal.clone();
                        tasks.spawn(async move {
                            let server=TcpStream::connect(upstream).await?;
                            let (mut cr,mut cw)=client.into_split(); let (mut sr,mut sw)=server.into_split();
                            let requests=tokio::io::copy(&mut cr,&mut sw);
                            let responses=async {
                                let mut buf=[0;16384];
                                loop {
                                    let n=sr.read(&mut buf).await?; if n==0 { break; }
                                    while *pause.borrow_and_update() { signal.notify_one(); if pause.changed().await.is_err() { return Ok::<(),std::io::Error>(()); } }
                                    cw.write_all(&buf[..n]).await?;
                                }
                                Ok::<(),std::io::Error>(())
                            };
                            tokio::select! {r=requests=>{r?;},r=responses=>{r?;}}
                            Ok::<(),std::io::Error>(())
                        });
                    },
                    _=tasks.join_next(),if !tasks.is_empty()=>{}
                }
            }
        });
        Ok(Self {
            address,
            paused,
            blocked,
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
    let pending_request = request.clone();
    let pending = tokio::spawn(async move { slow.write(pending_request).await });
    timeout(BUDGET, proxy.blocked.notified())
        .await
        .context("write response reached gate")?;
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
    let pending = tokio::spawn(run_blocking(move || {
        pending_adapter.batch_write(pending_request)
    }));
    timeout(BUDGET, proxy.blocked.notified())
        .await
        .context("cached write reply gate")?;
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
