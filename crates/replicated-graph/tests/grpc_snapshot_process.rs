//! Real three-process typed convenience/direct gRPC snapshot differential tests. This is not a
//! throughput benchmark, authenticated transport, or a query/Raft migration test.
#[path = "support/typed_graph_node.rs"]
mod typed_graph_node;
use anyhow::{ensure, Context};
use d2_raft::{send_control, ControlRequest, ControlResponse};
use replicated_graph::{
    remote::{self, grpc::SnapshotClient, RemoteError, RemoteIdentity, SnapshotDescriptor},
    run_blocking, RemoteStorageNode,
};
use std::{
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Notify},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use velagraph::{GraphRecordStore, Value, Vertex};

const SHARD: u64 = 7;
const BUDGET: Duration = Duration::from_secs(10);
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Cluster {
    addresses: Vec<SocketAddr>,
    leader: usize,
    // Processes are dropped before their data directories.
    _processes: Vec<Process>,
    _dirs: Vec<tempfile::TempDir>,
}
impl Cluster {
    async fn start() -> anyhow::Result<Self> {
        let mut reserved = Vec::new();
        for _ in 0..12 {
            reserved.push(TcpListener::bind("127.0.0.1:0").await?);
        }
        let addresses = reserved
            .iter()
            .map(|l| l.local_addr())
            .collect::<std::io::Result<Vec<_>>>()?;
        let members = (0..3)
            .map(|i| format!("{}={}", i + 1, addresses[4 * i]))
            .collect::<Vec<_>>()
            .join(",");
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<std::io::Result<Vec<_>>>()?;
        drop(reserved);
        let mut processes = Vec::new();
        for i in 0..3 {
            let mut command = typed_graph_node::command();
            command.env("TOKIO_WORKER_THREADS", "2").args([
                "--allow-insecure-development",
                "--id",
                &(i + 1).to_string(),
                "--raft",
                &addresses[4 * i].to_string(),
                "--control",
                &addresses[4 * i + 1].to_string(),
                "--graph",
                &addresses[4 * i + 2].to_string(),
                "--grpc-snapshot",
                &addresses[4 * i + 3].to_string(),
                "--shard",
                "7",
                "--epoch",
                "3",
                "--data-dir",
                dirs[i].path().to_str().context("data path")?,
            ]);
            if i == 0 {
                command.args(["--bootstrap", &members]);
            }
            processes.push(Process(
                command
                    .stdout(Stdio::null())
                    .stderr(Stdio::inherit())
                    .spawn()?,
            ));
        }
        let leader = timeout(Duration::from_secs(35), async {
            loop {
                for i in 0..3 {
                    if let Ok(ControlResponse::Status(s)) =
                        send_control(&addresses[4 * i + 1].to_string(), ControlRequest::Status)
                            .await
                    {
                        if s.leader == Some((i + 1) as u64)
                            && remote::probe_identified(
                                addresses[4 * i + 2],
                                RemoteIdentity {
                                    group: 0,
                                    node: (i + 1) as u64,
                                },
                                SHARD,
                                Duration::from_millis(500),
                            )
                            .await
                            .is_ok()
                        {
                            return i;
                        }
                    }
                }
                sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .context("three-process leader readiness")?;
        Ok(Self {
            addresses,
            leader,
            _processes: processes,
            _dirs: dirs,
        })
    }
    fn identity(&self) -> RemoteIdentity {
        RemoteIdentity {
            group: 0,
            node: (self.leader + 1) as u64,
        }
    }
    fn convenience(&self) -> SocketAddr {
        self.addresses[4 * self.leader + 2]
    }
    fn grpc(&self) -> SocketAddr {
        self.addresses[4 * self.leader + 3]
    }
    async fn seed(&self) -> anyhow::Result<()> {
        let node = RemoteStorageNode::new_identified(
            self.grpc(),
            self.identity().group,
            self.identity().node,
            SHARD,
            BUDGET,
        )?;
        let result = run_blocking(move || -> anyhow::Result<()> {
            let graph = GraphRecordStore::new(node, SHARD, 0);
            // Over one page, without relying on fabricated storage-key layouts.
            for id in 1..=36 {
                let mut vertex = Vertex::new(id);
                vertex.labels.push("person".into());
                vertex
                    .properties
                    .insert("payload".into(), Value::Text("x".repeat(64 * 1024)));
                graph
                    .create_vertex(&vertex)
                    .with_context(|| format!("snapshot fixture seed vertex {id}/36"))?;
            }
            Ok(())
        })
        .await
        .context("snapshot fixture seed blocking task")
        .and_then(|result| result);
        if let Err(error) = result {
            // Observation only, after failure: never retry a mutation or treat
            // a later healthy status as proof that the failed barrier succeeded.
            return Err(error.context(self.failure_status().await));
        }
        Ok(())
    }
    async fn failure_status(&self) -> String {
        let mut pending = JoinSet::new();
        for i in 0..3 {
            let address = self.addresses[4 * i + 1];
            pending.spawn(async move {
                let response = timeout(
                    Duration::from_millis(300),
                    send_control(&address.to_string(), ControlRequest::Status),
                )
                .await;
                let summary = match response {
                    Ok(Ok(ControlResponse::Status(s))) => format!(
                        "node={} leader={:?} applied={} last_log={}",
                        s.id, s.leader, s.applied, s.last_log
                    ),
                    Ok(Ok(_)) => format!("node={} unexpected_status", i + 1),
                    Ok(Err(_)) => format!("node={} status_unavailable", i + 1),
                    Err(_) => format!("node={} status_timeout", i + 1),
                };
                (i, summary)
            });
        }
        let mut summaries = Vec::new();
        while let Some(result) = pending.join_next().await {
            summaries.push(match result {
                Ok(summary) => summary,
                Err(_) => (3, "diagnostic_task_failed".to_string()),
            });
        }
        summaries.sort_by_key(|(i, _)| *i);
        format!(
            "snapshot fixture failure observation (after error, selected_leader={}): {}",
            self.leader + 1,
            summaries
                .into_iter()
                .map(|(_, s)| s)
                .collect::<Vec<_>>()
                .join("; ")
        )
    }
}

// A transparent loopback proxy counts physical connections. Pausing only its
// response direction gives a deterministic pending client call, not a timing race.
struct Proxy {
    address: SocketAddr,
    connections: Arc<AtomicUsize>,
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
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let (paused, pause) = watch::channel(false);
        let blocked = Arc::new(Notify::new());
        let signal = blocked.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break };
                        count.fetch_add(1, Ordering::SeqCst);
                        let mut pause = pause.clone();
                        let signal = signal.clone();
                        tasks.spawn(async move {
                            let server = TcpStream::connect(upstream).await?;
                            let (mut cr, mut cw) = client.into_split();
                            let (mut sr, mut sw) = server.into_split();
                            let requests = tokio::io::copy(&mut cr, &mut sw);
                            let responses = async {
                                let mut buf = vec![0; 16*1024];
                                loop {
                                    let n = sr.read(&mut buf).await?;
                                    if n == 0 { break; }
                                    while *pause.borrow_and_update() {
                                        signal.notify_one();
                                        if pause.changed().await.is_err() { return Ok::<(), std::io::Error>(()); }
                                    }
                                    cw.write_all(&buf[..n]).await?;
                                }
                                Ok::<(), std::io::Error>(())
                            };
                            tokio::select! { result = requests => { result?; }, result = responses => { result?; } }
                            Ok::<(), std::io::Error>(())
                        });
                    },
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Ok(Self {
            address,
            connections,
            paused,
            blocked,
            task,
        })
    }
}

fn same_view(a: &SnapshotDescriptor, b: &SnapshotDescriptor) {
    assert_eq!(
        (a.version, a.identity, a.shard, a.epoch, a.sequence, a.rows, a.bytes),
        (b.version, b.identity, b.shard, b.epoch, b.sequence, b.rows, b.bytes)
    );
    // Both protocols share the same bounded service pool.
    assert_eq!(a.incarnation, b.incarnation);
    assert_ne!(a.session, b.session);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_typed_snapshot_matches_tcp_and_reuses_channel() -> anyhow::Result<()> {
    timeout(Duration::from_secs(180), differential()).await?
}
async fn differential() -> anyhow::Result<()> {
    let mut cluster = Cluster::start().await?;
    cluster.seed().await?;
    let proxy = Proxy::start(cluster.grpc()).await?;
    let client = SnapshotClient::connect(proxy.address, cluster.identity(), BUDGET).await?;
    let convenience = remote::open_snapshot(
        cluster.convenience(),
        Some(cluster.identity()),
        SHARD,
        BUDGET,
    )
    .await?;
    let grpc = client.open(SHARD, BUDGET).await?;
    same_view(&convenience, &grpc);
    ensure!(grpc.bytes > remote::MAX_PAGE_BYTES);
    let (mut tc, mut gc) = (None, None);
    let mut total = 0;
    let mut pages = 0;
    loop {
        let a = remote::snapshot_page(
            cluster.convenience(),
            Some(cluster.identity()),
            convenience.clone(),
            tc,
            BUDGET,
        )
        .await?;
        let b = client.page(grpc.clone(), gc.clone(), BUDGET).await?;
        assert_eq!(a.entries, b.entries);
        assert_eq!(a.next.is_some(), b.next.is_some());
        let repeat = client.page(grpc.clone(), gc, BUDGET).await?;
        assert_eq!(repeat.entries, b.entries);
        assert_eq!(repeat.next, b.next);
        total += b.entries.len();
        pages += 1;
        tc = a.next;
        gc = b.next;
        if gc.is_none() {
            break;
        }
        ensure!(pages <= grpc.rows, "bounded traversal must advance");
    }
    assert_eq!(total, grpc.rows);
    ensure!(pages > 1);

    // A new write cannot mutate either already admitted view.
    let node = RemoteStorageNode::new_identified(
        cluster.grpc(),
        cluster.identity().group,
        cluster.identity().node,
        SHARD,
        BUDGET,
    )?;
    run_blocking(move || GraphRecordStore::new(node, SHARD, 0).create_vertex(&Vertex::new(999)))
        .await??;
    let a = remote::snapshot_page(
        cluster.convenience(),
        Some(cluster.identity()),
        convenience.clone(),
        None,
        BUDGET,
    )
    .await?;
    let b = client.page(grpc.clone(), None, BUDGET).await?;
    assert_eq!(a.entries, b.entries);
    assert_eq!(b.descriptor, grpc);
    let latest = client.open(SHARD, BUDGET).await?;
    ensure!(latest.sequence > grpc.sequence);
    ensure!(latest.rows > grpc.rows);
    client.close(latest, BUDGET).await?;

    // Abort after a response is held in the proxy. This tests cancellation at
    // the caller and subsequent session reuse, NOT server-side rollback.
    proxy.paused.send(true)?;
    let pending_client = client.clone();
    let pending_descriptor = grpc.clone();
    let pending =
        tokio::spawn(async move { pending_client.page(pending_descriptor, None, BUDGET).await });
    timeout(BUDGET, proxy.blocked.notified())
        .await
        .context("page response reached proxy")?;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    proxy.paused.send(false)?;
    assert_eq!(
        client.page(grpc.clone(), None, BUDGET).await?.entries,
        b.entries
    );

    assert!(matches!(
        client.open(SHARD + 1, BUDGET).await,
        Err(RemoteError::InvalidArgument)
    ));
    assert!(matches!(
        remote::open_snapshot(
            cluster.convenience(),
            Some(cluster.identity()),
            SHARD + 1,
            BUDGET
        )
        .await,
        Err(RemoteError::InvalidArgument)
    ));
    let wrong = RemoteIdentity {
        group: 999,
        ..cluster.identity()
    };
    let wrong_client = SnapshotClient::connect(cluster.grpc(), wrong, BUDGET).await?;
    assert!(matches!(
        wrong_client.open(SHARD, BUDGET).await,
        Err(RemoteError::InvalidArgument)
    ));
    assert!(matches!(
        remote::open_snapshot(cluster.convenience(), Some(wrong), SHARD, BUDGET).await,
        Err(RemoteError::InvalidArgument)
    ));
    // Shared server pools allow gradual migration without duplicating budgets.
    assert_eq!(
        client
            .page(convenience.clone(), None, BUDGET)
            .await?
            .entries,
        a.entries
    );
    assert_eq!(
        remote::snapshot_page(
            cluster.convenience(),
            Some(cluster.identity()),
            grpc.clone(),
            None,
            BUDGET
        )
        .await?
        .entries,
        b.entries
    );

    // Exercise malformed protobuf directly, without the high-level client's
    // local validation hiding server-side contract violations.
    use remote::grpc::wire;
    let mut raw = wire::snapshot_service_client::SnapshotServiceClient::connect(format!(
        "http://{}",
        cluster.grpc()
    ))
    .await?;
    let context = wire::Context {
        version: 1,
        identity: Some(cluster.identity().into()),
        timeout_millis: 5000,
    };
    let error = raw
        .open(wire::OpenRequest {
            context: None,
            shard: SHARD,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let mut malformed: wire::Descriptor = grpc.clone().into();
    malformed.session.pop();
    let error = raw
        .page(wire::PageRequest {
            context: Some(context),
            descriptor: Some(malformed),
            cursor: None,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let error = raw
        .open(wire::OpenRequest {
            context: Some(wire::Context {
                timeout_millis: 0,
                ..context
            }),
            shard: SHARD,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    let error = raw
        .open(wire::OpenRequest {
            context: Some(wire::Context {
                version: 2,
                ..context
            }),
            shard: SHARD,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DataLoss);

    // Independent follower connections preserve typed leader guidance.
    let follower = (cluster.leader + 1) % 3;
    let fi = RemoteIdentity {
        group: 0,
        node: (follower + 1) as u64,
    };
    let fc = SnapshotClient::connect(cluster.addresses[4 * follower + 3], fi, BUDGET).await?;
    let fe = fc.open(SHARD, BUDGET).await;
    let te =
        remote::open_snapshot(cluster.addresses[4 * follower + 2], Some(fi), SHARD, BUDGET).await;
    match (fe, te) {
        (Err(RemoteError::NotLeader { leader: a }), Err(RemoteError::NotLeader { leader: b })) => {
            assert_eq!(a, b)
        }
        pair => anyhow::bail!("follower error mismatch: {pair:?}"),
    }
    client.close(convenience.clone(), BUDGET).await?;
    remote::close_snapshot(
        cluster.convenience(),
        Some(cluster.identity()),
        grpc.clone(),
        BUDGET,
    )
    .await?;
    assert!(matches!(
        client.page(grpc.clone(), None, BUDGET).await,
        Err(RemoteError::SessionUnavailable)
    ));
    assert!(matches!(
        remote::snapshot_page(
            cluster.convenience(),
            Some(cluster.identity()),
            convenience,
            None,
            BUDGET
        )
        .await,
        Err(RemoteError::SessionUnavailable)
    ));
    assert!(matches!(
        client.close(grpc, BUDGET).await,
        Err(RemoteError::SessionUnavailable)
    ));

    // Session admission backpressure is explicit; closing frees the slot.
    let mut held = Vec::new();
    for i in 0..remote::MAX_SESSIONS {
        held.push(if i % 2 == 0 {
            client.open(SHARD, BUDGET).await?
        } else {
            remote::open_snapshot(
                cluster.convenience(),
                Some(cluster.identity()),
                SHARD,
                BUDGET,
            )
            .await?
        });
    }
    assert!(matches!(
        client.open(SHARD, BUDGET).await,
        Err(RemoteError::ResourceExhausted)
    ));
    client.close(held.pop().unwrap(), BUDGET).await?;
    held.push(client.open(SHARD, BUDGET).await?);
    for d in held {
        client.close(d, BUDGET).await?;
    }
    assert_eq!(
        proxy.connections.load(Ordering::SeqCst),
        1,
        "one reusable HTTP/2 connection, not one TCP connect per RPC"
    );
    // Hold all HTTP/2 response windows at zero. Permits must cover queued
    // response bodies, not just handler execution. No client consumes DATA.
    let slow_descriptor = client.open(SHARD, BUDGET).await?;
    let socket = TcpStream::connect(cluster.grpc()).await?;
    let (mut sender, connection) = h2::client::Builder::new()
        .initial_window_size(0)
        .handshake(socket)
        .await?;
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut blocked_responses = Vec::new();
    use prost::Message;
    for _ in 0..remote::MAX_IN_FLIGHT {
        sender = sender.ready().await?;
        let message = wire::PageRequest {
            context: Some(wire::Context {
                version: 1,
                identity: Some(cluster.identity().into()),
                timeout_millis: 5000,
            }),
            descriptor: Some(slow_descriptor.clone().into()),
            cursor: None,
        }
        .encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
        frame.extend_from_slice(&message);
        let request = tonic::codegen::http::Request::builder()
            .method("POST")
            .uri(format!(
                "http://{}/velagraph.internal.snapshot.v1.SnapshotService/Page",
                cluster.grpc()
            ))
            .header("content-type", "application/grpc")
            .body(())?;
        let (response, mut send) = sender.send_request(request, false)?;
        send.send_data(tonic::codegen::Bytes::from(frame), true)?;
        let response = timeout(BUDGET, response).await??;
        ensure!(
            response.headers().get("grpc-status").is_none(),
            "slow-reader page rejected before body"
        );
        blocked_responses.push(response);
    }
    assert!(matches!(
        client.open(SHARD, BUDGET).await,
        Err(RemoteError::ResourceExhausted)
    ));
    // Signal the real process while responses cannot drain. Grpc shutdown must
    // close its connections rather than leave the process awaiting flow control.
    #[cfg(unix)]
    {
        let pid = cluster._processes[cluster.leader].0.id();
        ensure!(Command::new("kill")
            .args(["-INT", &pid.to_string()])
            .status()?
            .success());
        timeout(Duration::from_secs(8), async {
            loop {
                if cluster._processes[cluster.leader].0.try_wait()?.is_some() {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
            anyhow::Ok(())
        })
        .await
        .context("flow-controlled node shutdown exceeded its bound")??;
    }
    drop(blocked_responses);
    driver.abort();
    Ok(())
}

/// Fixed TTL uses real node clocks. Run explicitly in extended transport CI;
/// routine PR tests must not add an unconditional one-minute sleep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real server fixed 60-second expiry; extended transport validation"]
async fn three_process_tcp_and_grpc_expire_fixed_sessions() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), async {
        let cluster = Cluster::start().await?;
        let client = SnapshotClient::connect(cluster.grpc(), cluster.identity(), BUDGET).await?;
        let convenience = remote::open_snapshot(
            cluster.convenience(),
            Some(cluster.identity()),
            SHARD,
            BUDGET,
        )
        .await?;
        let grpc = client.open(SHARD, BUDGET).await?;
        sleep(remote::SESSION_TTL + Duration::from_millis(250)).await;
        assert!(matches!(
            client.page(grpc, None, BUDGET).await,
            Err(RemoteError::SessionUnavailable)
        ));
        assert!(matches!(
            remote::snapshot_page(
                cluster.convenience(),
                Some(cluster.identity()),
                convenience,
                None,
                BUDGET
            )
            .await,
            Err(RemoteError::SessionUnavailable)
        ));
        // Expiry is local to the original fixed session. Do not require this
        // replica to remain leader across the sixty-second wait: new Open is
        // independently covered by the ordinary capacity/release test.
        Ok::<(), anyhow::Error>(())
    })
    .await?
}

/// A failed quorum must remain an error. Later status observations are evidence,
/// not retries or a replacement for the original linearizable barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_quorum_loss_preserves_stage_and_bounded_status_evidence() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        let mut cluster = Cluster::start().await?;
        for (i, process) in cluster._processes.iter_mut().enumerate() {
            if i != cluster.leader {
                process.0.kill()?;
                process.0.wait()?;
            }
        }
        let error = timeout(Duration::from_secs(12), cluster.seed())
            .await?
            .expect_err("seed must not succeed without a quorum");
        let evidence = format!("{error:#}");
        ensure!(
            evidence.contains("seed vertex 1/36"),
            "missing seed stage: {evidence}"
        );
        ensure!(evidence.contains("after error, selected_leader="));
        for id in 1..=3 {
            ensure!(evidence.contains(&format!("node={id} ")));
        }
        ensure!(matches!(
            error.downcast_ref::<velagraph::Error>(),
            Some(velagraph::Error::StorageNode(
                velagraph::StorageNodeError::Unavailable(_)
            ))
        ));
        Ok::<(), anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn failure_status_collection_is_bounded_when_control_peers_do_not_reply() -> anyhow::Result<()>
{
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..12 {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        addresses.push(listener.local_addr()?);
        listeners.push(listener);
    }
    let cluster = Cluster {
        addresses,
        leader: 0,
        _processes: Vec::new(),
        _dirs: Vec::new(),
    };
    // Connections can enter the backlog, but no endpoint serves a response.
    let evidence = timeout(Duration::from_secs(3), cluster.failure_status()).await?;
    for id in 1..=3 {
        ensure!(evidence.contains(&format!("node={id} status_timeout")));
    }
    drop(listeners);
    Ok(())
}

/// Investigation only: process scheduling pause, not a production timeout fix.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "controlled #247 scheduling experiment"]
async fn investigate_read_confirmation_scheduling_window() -> anyhow::Result<()> {
    struct Resume(Vec<u32>);
    impl Drop for Resume {
        fn drop(&mut self) {
            for pid in &self.0 {
                let _ = Command::new("kill")
                    .args(["-CONT", &pid.to_string()])
                    .status();
            }
        }
    }
    for (pause_ms, follower_count, budget_ms) in [
        (0_u64, 0, 10000),
        (40, 2, 10000),
        (180, 2, 10000),
        (180, 1, 10000),
        (180, 2, 50),
    ] {
        for attempt in 1..=3 {
            let cluster = Cluster::start().await?;
            let client =
                SnapshotClient::connect(cluster.grpc(), cluster.identity(), BUDGET).await?;
            // Allow readiness traffic to finish before applying a controlled pause.
            sleep(Duration::from_millis(100)).await;
            let mut paused = Resume(Vec::new());
            if pause_ms > 0 {
                for (i, process) in cluster._processes.iter().enumerate() {
                    if i != cluster.leader && paused.0.len() < follower_count {
                        let pid = process.0.id();
                        ensure!(Command::new("kill")
                            .args(["-STOP", &pid.to_string()])
                            .status()?
                            .success());
                        paused.0.push(pid);
                    }
                }
            }
            let resume = tokio::spawn(async move {
                sleep(Duration::from_millis(pause_ms)).await;
                drop(paused);
            });
            let started = std::time::Instant::now();
            let result = client.open(SHARD, Duration::from_millis(budget_ms)).await;
            let elapsed = started.elapsed();
            resume.await?;
            eprintln!("EXPERIMENT pause_ms={pause_ms} follower_count={follower_count} budget_ms={budget_ms} attempt={attempt} selected_leader={} elapsed_us={} result={}", cluster.leader + 1, elapsed.as_micros(), match &result { Ok(_) => "ok", Err(RemoteError::Timeout { .. }) => "timeout", Err(_) => "other_error" });
            if budget_ms == 50 {
                ensure!(
                    matches!(result, Err(RemoteError::Timeout { .. })),
                    "short budget must fail closed: {result:?}"
                );
            } else {
                ensure!(
                    result.is_ok(),
                    "bounded read confirmation must recover: {result:?}"
                );
            }
            if let Ok(descriptor) = result {
                client.close(descriptor, BUDGET).await?;
            }
            sleep(Duration::from_millis(80)).await;
            eprintln!(
                "EXPERIMENT recovered_status {}",
                cluster.failure_status().await
            );
            let recovered = client.open(SHARD, BUDGET).await;
            ensure!(recovered.is_ok(), "recovered read failed: {recovered:?}");
        }
    }
    Ok(())
}
