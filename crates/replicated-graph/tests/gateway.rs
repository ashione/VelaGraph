//! Real-process transport tests. Fixture mutations use the child's private stdin,
//! never the read-only gateway; the public wire has no testing/write backdoor.
#![cfg(target_os = "linux")]

use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::PathBuf,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

use d2_raft::{Cluster, D1Command, D1Result, LogCommand, NodeHandle};
use openraft::BasicNode;
use replicated_graph::{
    catalog::{Catalog, Placement},
    gateway::{
        Client, GatewayConfig, GatewayError, GatewayServer, RemoteError, MAX_CONNECTIONS,
        MAX_REQUEST_BYTES,
    },
    routing::RouteResolver,
    run_blocking,
};
use velagraph::{GremlinRow, Value, Vertex};

const BUDGET: Duration = Duration::from_secs(10);
const CHILD_ENV: &str = "GRAPH_UDS_FIXTURE";

async fn node(root: &Cluster, group: u64) -> anyhow::Result<(NodeHandle, Placement)> {
    let cluster = root.in_group(group);
    let node = cluster.start(1).await?;
    node.raft
        .initialize(BTreeMap::from([(1, BasicNode::default())]))
        .await?;
    tokio::time::timeout(BUDGET, async {
        loop {
            if node.raft.ensure_linearizable().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let placement = Placement {
        shard: group + 100,
        group,
        epoch: 1,
        replicas: vec![1],
    };
    node.raft
        .client_write(LogCommand::D1V1(D1Command::Activate {
            shard: placement.shard,
            epoch: placement.epoch,
        }))
        .await?;
    Ok((node, placement))
}

async fn fixture() -> anyhow::Result<(RouteResolver, Vec<NodeHandle>)> {
    let root = Cluster::new();
    let (metadata, meta_placement) = node(&root, 10).await?;
    let (data, data_placement) = node(&root, 20).await?;
    let catalog = Catalog::new(&metadata, meta_placement.clone(), BUDGET)?;
    run_blocking(move || {
        let prepared = catalog.prepare_registration([1; 16], 1, data_placement)?;
        catalog.commit_registration(&prepared)
    })
    .await??;
    let resolver = RouteResolver::new(
        meta_placement,
        std::slice::from_ref(&metadata),
        std::slice::from_ref(&data),
    )?;
    add_vertex(resolver.clone(), 1, 0).await?;
    Ok((resolver, vec![metadata, data]))
}

async fn add_vertex(resolver: RouteResolver, id: u64, payload: usize) -> anyhow::Result<()> {
    run_blocking(move || {
        let view = resolver.read_view(1, BUDGET)?;
        let mut vertex = Vertex::new(id);
        vertex.labels.push("person".into());
        if payload != 0 {
            vertex
                .properties
                .insert("payload".into(), Value::Text("x".repeat(payload)));
        }
        let prepared = view.prepare_create_vertex(&vertex)?;
        resolver.commit_prepared(&prepared, BUDGET)
    })
    .await??;
    Ok(())
}

// Invoked only by this test binary's parent, with --exact and an explicit env.
#[test]
fn gateway_child_fixture() -> anyhow::Result<()> {
    let Some(directory) = std::env::var_os(CHILD_ENV) else {
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let (resolver, nodes) = runtime.block_on(fixture())?;
    let socket_path = PathBuf::from(directory).join("graph.sock");
    let peer_uid = std::env::var("GRAPH_UDS_UID")?.parse()?;
    let server = GatewayServer::bind(
        resolver.clone(),
        GatewayConfig {
            socket_path,
            peer_uid,
            io_timeout: Duration::from_secs(5),
        },
    )?;
    println!("FIXTURE READY");
    std::io::stdout().flush()?;
    for line in std::io::stdin().lock().lines() {
        match line?.as_str() {
            "add" => {
                runtime.block_on(add_vertex(resolver.clone(), 2, 0))?;
                println!("FIXTURE ADDED");
            }
            "large" => {
                for id in 10..18 {
                    runtime.block_on(add_vertex(resolver.clone(), id, 200_000))?;
                }
                println!("FIXTURE LARGE");
            }
            "fence" => {
                let result = runtime.block_on(nodes[1].raft.client_write(LogCommand::D1V1(
                    D1Command::SetEpoch {
                        shard: 120,
                        expected_epoch: 1,
                        new_epoch: 2,
                    },
                )))?;
                assert_eq!(result.data.d1.unwrap(), Ok(D1Result::Epoch { epoch: 2 }));
                println!("FIXTURE FENCED");
            }
            "active" => println!("FIXTURE ACTIVE {}", server.active_connections()),
            "stop" => break,
            _ => anyhow::bail!("unknown private fixture command"),
        }
        std::io::stdout().flush()?;
    }
    server.shutdown()?;
    for node in nodes {
        let _ = runtime.block_on(node.raft.shutdown());
    }
    Ok(())
}

struct Process {
    child: Child,
    input: ChildStdin,
    output: Receiver<String>,
    directory: tempfile::TempDir,
}
impl Process {
    fn start() -> anyhow::Result<Self> {
        Self::start_with_uid(uid())
    }
    fn start_with_uid(peer_uid: u32) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
        let (child, input, output) = Self::launch(directory.path(), peer_uid)?;
        Ok(Self {
            child,
            input,
            output,
            directory,
        })
    }
    fn launch(
        directory: &std::path::Path,
        peer_uid: u32,
    ) -> anyhow::Result<(Child, ChildStdin, Receiver<String>)> {
        let mut child = Command::new(std::env::current_exe()?)
            .args(["--exact", "gateway_child_fixture", "--nocapture"])
            .env(CHILD_ENV, directory)
            .env("GRAPH_UDS_UID", peer_uid.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, output) = mpsc::sync_channel(16);
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(at) = line.find("FIXTURE ") {
                    if tx.send(line[at..].to_owned()).is_err() {
                        break;
                    }
                }
            }
        });
        match output.recv_timeout(Duration::from_secs(30)) {
            Ok(line) if line == "FIXTURE READY" => Ok((child, input, output)),
            other => {
                let _ = child.kill();
                let _ = child.wait();
                anyhow::bail!("fixture startup: {other:?}")
            }
        }
    }
    fn path(&self) -> PathBuf {
        self.directory.path().join("graph.sock")
    }
    fn command(&mut self, command: &str) -> anyhow::Result<String> {
        writeln!(self.input, "{command}")?;
        self.input.flush()?;
        Ok(self.output.recv_timeout(Duration::from_secs(30))?)
    }
    fn wait_empty(&mut self) -> anyhow::Result<()> {
        let end = Instant::now() + Duration::from_secs(10);
        while self.command("active")? != "FIXTURE ACTIVE 0" {
            anyhow::ensure!(
                Instant::now() < end,
                "connection/session resources not released"
            );
            thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }
    fn connect(&self) -> anyhow::Result<Client> {
        Ok(Client::open(self.path(), uid(), 1, BUDGET)?)
    }
    fn raw(&self) -> anyhow::Result<UnixStream> {
        let stream = UnixStream::connect(self.path())?;
        stream.set_read_timeout(Some(Duration::from_secs(8)))?;
        stream.set_write_timeout(Some(Duration::from_secs(8)))?;
        Ok(stream)
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn uid() -> u32 {
    rustix::process::geteuid().as_raw()
}
fn frame(stream: &mut UnixStream, body: &[u8]) -> anyhow::Result<()> {
    stream.write_all(&(body.len() as u32).to_be_bytes())?;
    stream.write_all(body)?;
    Ok(())
}
fn raw_open(stream: &mut UnixStream) -> anyhow::Result<()> {
    frame(
        stream,
        br#"{"op":"open_read","version":1,"partition":1,"budget_ms":10000}"#,
    )?;
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    assert!(len <= 1024 * 1024);
    let mut opened = vec![0; len];
    stream.read_exact(&mut opened)?;
    let response: serde_json::Value = serde_json::from_slice(&opened)?;
    assert_eq!(response["status"], "opened");
    Ok(())
}
fn closed(stream: &mut UnixStream) {
    let mut byte = [0];
    match stream.read(&mut byte) {
        Ok(0) => (),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        other => panic!("expected bounded connection closure, got {other:?}"),
    }
}
fn count(client: &mut Client) -> anyhow::Result<u64> {
    match client.execute_gremlin("g.V().count()")?.rows.as_slice() {
        [GremlinRow::Count(n)] => Ok(*n),
        other => anyhow::bail!("unexpected result {other:?}"),
    }
}

#[test]
fn subprocess_snapshot_read_only_and_restart() -> anyhow::Result<()> {
    let mut process = Process::start()?;
    assert_eq!(
        fs::metadata(process.path())?.permissions().mode() & 0o777,
        0o600
    );
    let mut old = process.connect()?;
    let snapshot = old.snapshot().clone();
    assert_eq!(count(&mut old)?, 1);
    assert_eq!(process.command("add")?, "FIXTURE ADDED");
    assert_eq!(count(&mut old)?, 1);
    assert_eq!(old.snapshot().sequence, snapshot.sequence);
    let mut fresh = process.connect()?;
    assert_eq!(count(&mut fresh)?, 2);
    assert!(fresh.snapshot().sequence > snapshot.sequence);
    assert!(fresh.execute_gremlin("g.addV('forbidden')").is_err());
    drop(fresh);
    old.close()?;
    process.wait_empty()?;
    let mut invalidated = process.connect()?;
    process.child.kill()?;
    process.child.wait()?;
    assert!(count(&mut invalidated).is_err());
    // A stale pathname is deliberately NOT auto-unlinked/rebound. The owner
    // removes it explicitly; restart creates new connections and new sessions.
    fs::remove_file(process.path())?;
    let (child, input, output) = Process::launch(process.directory.path(), uid())?;
    process.child = child;
    process.input = input;
    process.output = output;
    assert_eq!(count(&mut process.connect()?)?, 1);
    assert!(count(&mut invalidated).is_err());
    process.wait_empty()?;
    Ok(())
}

#[test]
fn subprocess_frames_timeouts_and_admission_release() -> anyhow::Result<()> {
    let mut process = Process::start()?;
    for length in [0, MAX_REQUEST_BYTES as u32 + 1, u32::MAX] {
        let mut stream = process.raw()?;
        stream.write_all(&length.to_be_bytes())?;
        closed(&mut stream);
    }
    for body in [
        b"{".as_slice(),
        b"null",
        b"{}",
        b"{\"op\":\"write\"}",
        b"{\"op\":\"admin\"}",
        b"{\"op\":\"open_read\",\"version\":2,\"partition\":1,\"budget_ms\":1000}",
        b"{\"op\":\"open_read\",\"version\":1,\"partition\":1,\"budget_ms\":1000,\"extra\":true}",
        b"{\"op\":\"open_read\",\"version\":1,\"partition\":1,\"budget_ms\":0}",
        b"{\"op\":\"open_read\",\"version\":1,\"partition\":1,\"budget_ms\":60001}",
        b"{\"op\":\"close\"} {}",
    ] {
        let mut stream = process.raw()?;
        frame(&mut stream, body)?;
        // A bounded protocol-error reply is permitted; consume until EOF.
        let mut bytes = Vec::new();
        stream.take(4096).read_to_end(&mut bytes)?;
    }
    process.wait_empty()?;
    for request in [
        br#"{"op":"write","vertex":99}"#.as_slice(),
        br#"{"op":"admin","action":"activate"}"#,
        br#"{"op":"close","extra":true}"#,
        br#"{"op":"gremlin","query":"g.V().count()","extra":true}"#,
        br#"{"op":"open_read","version":1,"partition":1,"budget_ms":1000}"#,
    ] {
        let mut stream = process.raw()?;
        raw_open(&mut stream)?;
        frame(&mut stream, request)?;
        closed(&mut stream);
        assert_eq!(count(&mut process.connect()?)?, 1);
    }
    process.wait_empty()?;
    for partial in [vec![0], vec![0, 0, 0, 100, b'{']] {
        let mut stream = process.raw()?;
        stream.write_all(&partial)?;
        closed(&mut stream);
    }
    let mut short = Client::open(process.path(), uid(), 1, Duration::from_millis(100))?;
    thread::sleep(Duration::from_millis(150));
    assert!(count(&mut short).is_err());
    drop(short);
    process.wait_empty()?;
    // Fill transport admission without eight sequential Raft opens. Wait for
    // explicit worker admission rather than racing the accept thread.
    let held: Vec<_> = (0..MAX_CONNECTIONS)
        .map(|_| process.raw())
        .collect::<Result<_, _>>()?;
    let admitted = format!("FIXTURE ACTIVE {MAX_CONNECTIONS}");
    let admission_deadline = Instant::now() + Duration::from_secs(3);
    while process.command("active")? != admitted {
        anyhow::ensure!(
            Instant::now() < admission_deadline,
            "connections were not admitted"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert!(process.connect().is_err());
    drop(held);
    process.wait_empty()?;
    // Repeat beyond the resolver's 32 retained-session slots to detect leaks.
    for _ in 0..40 {
        let mut client = process.connect()?;
        assert_eq!(count(&mut client)?, 1);
        client.close()?;
    }
    process.wait_empty()?;
    Ok(())
}

#[test]
fn subprocess_rejects_peer_uid_and_client_server_uid() -> anyhow::Result<()> {
    let mut wrong = Process::start_with_uid(uid().wrapping_add(1))?;
    assert!(wrong.connect().is_err());
    wrong.wait_empty()?;
    let mut process = Process::start()?;
    assert!(Client::open(process.path(), uid().wrapping_add(1), 1, BUDGET).is_err());
    process.wait_empty()?;
    assert_eq!(count(&mut process.connect()?)?, 1);
    Ok(())
}

#[test]
fn subprocess_bounded_response_and_slow_reader() -> anyhow::Result<()> {
    let mut process = Process::start()?;
    assert_eq!(process.command("large")?, "FIXTURE LARGE");
    let mut client = process.connect()?;
    // 1.6 MiB of values exceeds the 1 MiB serialized response cap. It must
    // fail, never yield a truncated successful result.
    assert!(matches!(
        client.execute_gremlin("g.V().values('payload')"),
        Err(GatewayError::Remote(RemoteError::ResourceExhausted {}))
    ));
    drop(client);
    process.wait_empty()?;
    let mut stream = process.raw()?;
    raw_open(&mut stream)?;
    // A valid response much larger than the kernel's UDS send buffer. Keep
    // the client connected without consuming the reply: write timeout must
    // release the session, not merely timeout a caller awaiting its worker.
    frame(
        &mut stream,
        br#"{"op":"gremlin","query":"g.V(10,11,12,13).values('payload')"}"#,
    )?;
    process.wait_empty()?;
    assert_eq!(count(&mut process.connect()?)?, 9);
    Ok(())
}

#[test]
fn trusted_paths_and_existing_socket_are_not_replaced() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let (resolver, nodes) = runtime.block_on(fixture())?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("graph.sock");
    let config = || GatewayConfig {
        socket_path: path.clone(),
        peer_uid: uid(),
        io_timeout: Duration::from_millis(100),
    };
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755))?;
    assert!(GatewayServer::bind(resolver.clone(), config()).is_err());
    assert!(!path.exists());
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    fs::write(&path, "do not replace")?;
    assert!(GatewayServer::bind(resolver.clone(), config()).is_err());
    assert_eq!(fs::read_to_string(&path)?, "do not replace");
    fs::remove_file(&path)?;
    let listener = std::os::unix::net::UnixListener::bind(&path)?;
    assert!(GatewayServer::bind(resolver.clone(), config()).is_err());
    assert!(path.exists());
    drop(listener);
    fs::remove_file(&path)?;
    let outside = tempfile::tempdir()?;
    std::os::unix::fs::symlink(outside.path(), dir.path().join("link"))?;
    assert!(GatewayServer::bind(
        resolver.clone(),
        GatewayConfig {
            socket_path: dir.path().join("link/graph.sock"),
            ..config()
        }
    )
    .is_err());
    assert!(!outside.path().join("graph.sock").exists());
    let writable_ancestor = dir.path().join("writable");
    let safe_leaf = writable_ancestor.join("private");
    fs::create_dir_all(&safe_leaf)?;
    fs::set_permissions(&safe_leaf, fs::Permissions::from_mode(0o700))?;
    for mode in [0o770, 0o777] {
        fs::set_permissions(&writable_ancestor, fs::Permissions::from_mode(mode))?;
        let unsafe_path = safe_leaf.join("graph.sock");
        assert!(GatewayServer::bind(
            resolver.clone(),
            GatewayConfig {
                socket_path: unsafe_path.clone(),
                ..config()
            }
        )
        .is_err());
        assert!(!unsafe_path.exists());
    }
    let server = GatewayServer::bind(resolver, config())?;
    server.shutdown()?;
    assert!(!path.exists());
    for node in nodes {
        let _ = runtime.block_on(node.raft.shutdown());
    }
    Ok(())
}

#[test]
fn gateway_client_fixture() -> anyhow::Result<()> {
    let Some(path) = std::env::var_os("GRAPH_UDS_CLIENT") else {
        return Ok(());
    };
    let _client = Client::open(path, uid(), 1, BUDGET)?;
    println!("CLIENT READY");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(())
}

#[test]
fn subprocess_killed_client_releases_session() -> anyhow::Result<()> {
    let mut server = Process::start()?;
    let mut client = Command::new(std::env::current_exe()?)
        .args(["--exact", "gateway_client_fixture", "--nocapture"])
        .env("GRAPH_UDS_CLIENT", server.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let stdout = client.stdout.take().unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line.contains("CLIENT READY") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let ready = rx.recv_timeout(Duration::from_secs(10));
    let _ = client.kill();
    client.wait()?;
    ready?;
    server.wait_empty()?;
    assert_eq!(count(&mut server.connect()?)?, 1);
    Ok(())
}

#[test]
fn subprocess_epoch_fencing_is_typed_and_not_retried() -> anyhow::Result<()> {
    let mut process = Process::start()?;
    let mut retained = process.connect()?;
    assert_eq!(count(&mut retained)?, 1);
    assert_eq!(process.command("fence")?, "FIXTURE FENCED");
    assert!(matches!(
        retained.execute_gremlin("g.V().count()"),
        Err(GatewayError::Remote(RemoteError::Fenced {
            shard: 120,
            epoch: 1,
            current: 2
        }))
    ));
    assert!(matches!(
        Client::open(process.path(), uid(), 1, BUDGET),
        Err(GatewayError::Remote(RemoteError::Fenced {
            shard: 120,
            epoch: 1,
            current: 2
        }))
    ));
    drop(retained);
    process.wait_empty()?;
    Ok(())
}

#[test]
fn client_rejects_oversized_response_header_without_reading_body() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    let path = directory.path().join("fake.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let (tx, rx) = mpsc::sync_channel(1);
    let server = thread::spawn(move || -> anyhow::Result<()> {
        let end = Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    anyhow::ensure!(Instant::now() < end, "client never connected");
                    thread::sleep(Duration::from_millis(5));
                }
                Err(e) => return Err(e.into()),
            }
        };
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        let mut header = [0; 4];
        stream.read_exact(&mut header)?;
        let length = u32::from_be_bytes(header) as usize;
        anyhow::ensure!(length <= MAX_REQUEST_BYTES);
        let mut request = vec![0; length];
        stream.read_exact(&mut request)?;
        stream.write_all(&u32::MAX.to_be_bytes())?;
        // Keep the connection alive without sending any body. The client must
        // reject the header itself, not wait for timeout or peer EOF.
        rx.recv_timeout(Duration::from_secs(5))?;
        Ok(())
    });
    let result = Client::open(&path, uid(), 1, Duration::from_secs(1));
    let _ = tx.send(());
    server.join().expect("fake server panicked")?;
    assert!(
        matches!(result, Err(GatewayError::Io(e)) if e.kind() == std::io::ErrorKind::InvalidInput)
    );
    Ok(())
}
