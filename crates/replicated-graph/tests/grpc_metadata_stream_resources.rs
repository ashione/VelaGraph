//! Adversarial real sockets for Metadata streaming. No connection roles:
//! unrelated unary RPCs may legally share the exact same HTTP/2 connection.
use anyhow::ensure;
use d2_raft::{D1Command, LogCommand, TcpNode, TcpNodeConfig};
use prost::{bytes::Bytes, Message};
use replicated_graph::remote::{grpc::GrpcSnapshotService, operations_grpc::wire as w, GraphState};
use std::{collections::BTreeMap, future::poll_fn, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinHandle,
    time::{sleep, timeout, Instant},
};
use tonic::codegen::http;
const METADATA: &str = "/velagraph.internal.operations.v1.MetadataService/Exchange";
const UNARY: &str = "/velagraph.internal.operations.v1.ShardOperations/Call";
struct Fixture {
    service: GrpcSnapshotService,
    node: TcpNode,
    _dir: tempfile::TempDir,
}
impl Fixture {
    async fn start(deadline: Duration) -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;
        let node = TcpNode::start(TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: dir.path().into(),
        })
        .await?;
        node.bootstrap(BTreeMap::from([(1, node.raft_addr.clone())]))
            .await?;
        timeout(Duration::from_secs(5), async {
            while node.raft.ensure_linearizable().await.is_err() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        node.raft
            .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
            .await?;
        let state = GraphState::from_tcp(&node, 9, 7, deadline)?;
        let service = GrpcSnapshotService::start(&state, "127.0.0.1:0".parse()?).await?;
        Ok(Self {
            service,
            node,
            _dir: dir,
        })
    }
    fn address(&self) -> SocketAddr {
        self.service.local_addr()
    }
    async fn shutdown(self) {
        self.service.shutdown().await;
        self.node.shutdown().await;
    }
}
struct H2 {
    sender: h2::client::SendRequest<Bytes>,
    driver: JoinHandle<Result<(), h2::Error>>,
    address: SocketAddr,
}
impl Drop for H2 {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
impl H2 {
    async fn connect(address: SocketAddr, window: u32) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let (sender, connection) = h2::client::Builder::new()
            .initial_window_size(window)
            .handshake::<_, Bytes>(stream)
            .await?;
        Ok(Self {
            sender,
            driver: tokio::spawn(connection),
            address,
        })
    }
    async fn request(
        &mut self,
        path: &str,
    ) -> anyhow::Result<(h2::client::ResponseFuture, h2::SendStream<Bytes>)> {
        poll_fn(|cx| self.sender.poll_ready(cx)).await?;
        let headers = http::Request::builder()
            .method("POST")
            .uri(format!("http://{}{path}", self.address))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(())?;
        Ok(self.sender.send_request(headers, false)?)
    }
    async fn unary(&mut self) -> anyhow::Result<()> {
        let (response, mut body) = self.request(UNARY).await?;
        body.send_data(
            frame(&w::Request {
                version: 1,
                identity: Some(w::Identity { group: 9, node: 1 }),
                timeout_millis: 1000,
                operation: Some(w::request::Operation::Metadata(7)),
            }),
            true,
        )?;
        let response = timeout(Duration::from_secs(2), response).await??;
        let (bytes, status) = read_response(response).await?;
        ensure!(status.as_deref() == Some("0"), "unary status {status:?}");
        ensure!(bytes.len() >= 5, "missing unary body");
        let reply = w::Response::decode(&bytes[5..])?;
        ensure!(
            matches!(reply.result,Some(w::response::Result::Metadata(m)) if m.shard==7&&m.epoch==3),
            "bad unary response"
        );
        Ok(())
    }
}
fn frame(message: &impl Message) -> Bytes {
    let payload = message.encode_to_vec();
    let mut bytes = vec![0];
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend(payload);
    bytes.into()
}
fn metadata(id: u64) -> w::MetadataRequest {
    w::MetadataRequest {
        version: 1,
        id,
        identity: Some(w::Identity { group: 9, node: 1 }),
        shard: 7,
        timeout_millis: 1000,
    }
}
async fn read_response(
    response: http::Response<h2::RecvStream>,
) -> anyhow::Result<(Vec<u8>, Option<String>)> {
    let mut status = response
        .headers()
        .get("grpc-status")
        .map(|v| v.to_str().unwrap().to_owned());
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), async {
        while let Some(chunk) = body.data().await {
            let chunk = chunk?;
            ensure!(bytes.len() + chunk.len() <= 4096, "unbounded test response");
            bytes.extend_from_slice(&chunk);
            body.flow_control().release_capacity(chunk.len())?;
        }
        if let Some(trailers) = body.trailers().await? {
            if let Some(code) = trailers.get("grpc-status") {
                status = Some(code.to_str()?.to_owned());
            }
        }
        anyhow::Ok(())
    })
    .await??;
    Ok((bytes, status))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_graph_sockets_are_capped_and_released() -> anyhow::Result<()> {
    timeout(Duration::from_secs(15), async {
        let fixture = Fixture::start(Duration::from_millis(300)).await?;
        let address = fixture.address();
        let mut held = Vec::new();
        for _ in 0..32 {
            let mut stream = TcpStream::connect(address).await?;
            stream.write_all(b"PRI * HTTP/2.0\r\n").await?;
            held.push(stream);
        }
        // These are incomplete handshakes, not Metadata slots. Observe the
        // service-owned ESTABLISHED sockets, excluding clients and Raft ports.
        #[cfg(target_os = "linux")]
        timeout(Duration::from_secs(2), async {
            loop {
                if server_connections(address.port()).unwrap() == 32 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let mut excess = TcpStream::connect(address).await?;
        excess.write_all(b"PRI * HTTP/2.0\r\n").await?;
        closed(&mut excess).await?;
        #[cfg(target_os = "linux")]
        assert_eq!(server_connections(address.port())?, 32);
        drop(held.pop());
        #[cfg(target_os = "linux")]
        timeout(Duration::from_secs(2), async {
            while server_connections(address.port()).unwrap() != 31 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let mut replacement = H2::connect(address, 65535).await?;
        replacement.unary().await?;
        drop(replacement);
        timeout(Duration::from_secs(2), fixture.shutdown()).await?;
        for mut stream in held {
            closed(&mut stream).await?;
        }
        Ok(())
    })
    .await?
}
async fn closed(stream: &mut TcpStream) -> anyhow::Result<()> {
    timeout(Duration::from_secs(2), async {
        let mut bytes = [0; 4096];
        for _ in 0..16 {
            match stream.read(&mut bytes).await {
                Ok(0) | Err(_) => return Ok(()),
                Ok(_) => {}
            }
        }
        anyhow::bail!("socket survived close")
    })
    .await?
}
#[cfg(target_os = "linux")]
fn server_connections(port: u16) -> anyhow::Result<usize> {
    let mut owned = std::collections::BTreeSet::new();
    for fd in std::fs::read_dir("/proc/self/fd")? {
        if let Ok(path) = std::fs::read_link(fd?.path()) {
            let path = path.to_string_lossy();
            if let Some(inode) = path
                .strip_prefix("socket:[")
                .and_then(|s| s.strip_suffix(']'))
            {
                owned.insert(inode.to_owned());
            }
        }
    }
    let mut count = 0;
    for path in ["/proc/self/net/tcp", "/proc/self/net/tcp6"] {
        for line in std::fs::read_to_string(path)?.lines().skip(1) {
            let f = line.split_whitespace().collect::<Vec<_>>();
            if f.len() > 9
                && f[3] == "01"
                && owned.contains(f[9])
                && u16::from_str_radix(f[1].rsplit(':').next().unwrap(), 16)? == port
            {
                count += 1;
            }
        }
    }
    Ok(count)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_metadata_frame_times_out_without_aborting_same_connection_unary(
) -> anyhow::Result<()> {
    let fixture = Fixture::start(Duration::from_millis(200)).await?;
    let mut connection = H2::connect(fixture.address(), 65535).await?;
    let (response, mut input) = connection.request(METADATA).await?;
    let complete = frame(&metadata(1));
    input.send_data(complete.slice(..complete.len() - 1), false)?;
    let response = timeout(Duration::from_secs(2), response).await??;
    let began = Instant::now();
    let (_, status) = read_response(response).await?;
    ensure!(
        status.as_deref() == Some("4"),
        "partial frame must hit its RPC deadline: {status:?}"
    );
    ensure!(began.elapsed() < Duration::from_secs(2));
    // This exact SendRequest has one connection driver and no reconnect path.
    connection.unary().await?;
    let _ = input.send_data(complete.slice(complete.len() - 1..), true);
    connection.unary().await?;
    drop(connection);
    fixture.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_reader_is_flow_controlled_holds_stream_slot_but_not_unary_slot() -> anyhow::Result<()>
{
    timeout(Duration::from_secs(25), async {
        let fixture = Fixture::start(Duration::from_secs(3)).await?;
        // 64 bytes per response stream lets a normal unary response finish,
        // but blocks a long Metadata response stream without reading its body.
        let mut connection = H2::connect(fixture.address(), 64).await?;
        let (response, mut input) = connection.request(METADATA).await?;
        let held = timeout(Duration::from_secs(2), response).await??;
        ensure!(held.headers().get("grpc-status").is_none());
        let mut id = 1;
        let mut sent = 0usize;
        let mut blocked = false;
        'pump: for _ in 0..2048 {
            let mut batch = Vec::new();
            while batch.len() < 2048 {
                batch.extend_from_slice(&frame(&metadata(id)));
                id += 1;
            }
            let batch = Bytes::from(batch);
            let mut offset = 0;
            while offset < batch.len() {
                input.reserve_capacity(batch.len() - offset);
                match timeout(
                    Duration::from_millis(150),
                    poll_fn(|cx| input.poll_capacity(cx)),
                )
                .await
                {
                    Err(_) => {
                        blocked = true;
                        break 'pump;
                    }
                    Ok(Some(Ok(capacity))) => {
                        let n = capacity.min(batch.len() - offset);
                        ensure!(n > 0, "zero capacity notification");
                        input.send_data(batch.slice(offset..offset + n), false)?;
                        offset += n;
                        sent += n;
                    }
                    other => anyhow::bail!("stream ended instead of backpressuring: {other:?}"),
                }
            }
        }
        ensure!(
            blocked,
            "reader retained >4MiB without propagating flow control"
        );
        ensure!(
            sent > 65535,
            "must exceed one input window to exercise sustained response backpressure ({sent})"
        );
        eprintln!(
            "metadata slow-reader input backpressured after {sent} bytes (4 MiB test ceiling)"
        );
        // No connection role fence or exclusive operation permit may prevent
        // unary Metadata on the same physical HTTP/2 connection.
        connection.unary().await?;
        // Fill the other fifteen stream permits elsewhere. This blocked body
        // must still own its permit even if its sequential actor hits a timeout.
        let mut other = H2::connect(fixture.address(), 65535).await?;
        let mut pending = Vec::new();
        for _ in 0..15 {
            let (reply, body) = other.request(METADATA).await?;
            let reply = timeout(Duration::from_secs(1), reply).await??;
            ensure!(reply.headers().get("grpc-status").is_none());
            pending.push((reply, body));
        }
        let mut excess = H2::connect(fixture.address(), 65535).await?;
        let (reply, mut excess_input) = excess.request(METADATA).await?;
        let (_, status) = read_response(timeout(Duration::from_secs(1), reply).await??).await?;
        ensure!(
            status.as_deref() == Some("8"),
            "blocked response lost stream permit: {status:?}"
        );
        excess_input.send_reset(h2::Reason::CANCEL);
        input.send_reset(h2::Reason::CANCEL);
        drop(held);
        timeout(Duration::from_secs(2), async {
            loop {
                let (reply, mut body) = excess.request(METADATA).await?;
                let reply = timeout(Duration::from_secs(1), reply).await??;
                if reply.headers().get("grpc-status").is_none() {
                    body.send_reset(h2::Reason::CANCEL);
                    return anyhow::Ok(());
                }
                body.send_reset(h2::Reason::CANCEL);
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        connection.unary().await?;
        // Keep remaining stream handles alive across shutdown: the server must
        // release its tasks/sockets, not rely on the client dropping handles.
        timeout(Duration::from_secs(7), fixture.shutdown()).await?;
        ensure!(connection.unary().await.is_err());
        drop(pending);
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_deadline_does_not_abort_inflight_write_on_same_http2_connection(
) -> anyhow::Result<()> {
    use d2_raft::managed_rpc::grpc::wire as managed;
    timeout(Duration::from_secs(10), async {
        let fixture = Fixture::start(Duration::from_secs(2)).await?;
        let before = fixture.node.state_machine.d1_metadata().await.unwrap();
        let mut metrics = fixture.node.raft.metrics();
        // Include the known applied activation index even if a metrics
        // publication trails its client response, so that cannot satisfy the
        // subsequent "Write appended" observation accidentally.
        let applied_index = fixture.node.state_machine.applied().await.map(|id| id.index);
        let last_log = metrics.borrow().last_log_index.max(applied_index);
        let mut connection = H2::connect(fixture.address(), 65535).await?;
        let request_id = vec![42; 16];
        let write = w::Request {
            version: 1,
            identity: Some(w::Identity { group: 9, node: 1 }),
            timeout_millis: 1000,
            operation: Some(w::request::Operation::Write(managed::D1WriteRequest {
                shard: 7,
                epoch: 3,
                request_id,
                expected_sequence: Some(before.2),
                operations: vec![managed::WriteOp { kind: Some(managed::write_op::Kind::Put(managed::Put {
                    key: b"same-connection-write".to_vec(),
                    value: b"committed-after-metadata-timeout".to_vec(),
                })) }],
            })),
        };
        // A real state-machine lock, not an arbitrary server delay: apply cannot
        // complete, while log append remains independently observable.
        let paused = fixture.node.state_machine.pause_state().await;
        let (mut write_reply, mut write_body) = connection.request(UNARY).await?;
        write_body.send_data(frame(&write), true)?;
        timeout(Duration::from_millis(500), async {
            loop {
                if metrics.borrow().last_log_index > last_log { return anyhow::Ok(()); }
                metrics.changed().await?;
            }
        }).await??;
        let (metadata_reply, mut metadata_body) = connection.request(METADATA).await?;
        let mut short = metadata(1);
        short.timeout_millis = 20;
        metadata_body.send_data(frame(&short), false)?;
        let (_, status) = read_response(timeout(Duration::from_secs(1), metadata_reply).await??).await?;
        ensure!(status.as_deref() == Some("4"), "expected only Metadata RPC to time out: {status:?}");
        // The Write was appended before Metadata started, and is still waiting
        // on apply. An old whole-connection abort would resolve this as an error.
        let pending = poll_fn(|cx| {
            use std::future::Future;
            std::task::Poll::Ready(matches!(std::pin::Pin::new(&mut write_reply).poll(cx), std::task::Poll::Pending))
        }).await;
        ensure!(pending, "Metadata timeout prematurely completed/reset the in-flight Write");
        drop(paused);
        let (bytes, status) = read_response(timeout(Duration::from_secs(1), write_reply).await??).await?;
        ensure!(status.as_deref() == Some("0"));
        ensure!(bytes.len() >= 5);
        let committed = w::Response::decode(&bytes[5..])?;
        ensure!(matches!(committed.result, Some(w::response::Result::Write(result)) if result.status == w::WriteStatus::Committed as i32 && result.sequence == before.2 + 1 && result.applied_operations == 1));
        // Replay the exact encoded request, including its original request ID,
        // explicitly on this same SendRequest/driver. No client reconnect/retry.
        let (replay, mut replay_body) = connection.request(UNARY).await?;
        replay_body.send_data(frame(&write), true)?;
        let (bytes, status) = read_response(timeout(Duration::from_secs(1), replay).await??).await?;
        ensure!(status.as_deref() == Some("0"));
        ensure!(bytes.len() >= 5);
        let duplicate = w::Response::decode(&bytes[5..])?;
        ensure!(matches!(duplicate.result, Some(w::response::Result::Write(result)) if result.status == w::WriteStatus::Duplicate as i32 && result.sequence == before.2 + 1));
        assert_eq!(fixture.node.state_machine.d1_metadata().await, Some((7,3,before.2+1)));
        assert_eq!(fixture.node.state_machine.get_raw(b"same-connection-write").await, Some(b"committed-after-metadata-timeout".to_vec()));
        connection.unary().await?;
        drop((metadata_body,connection));
        fixture.shutdown().await;
        Ok(())
    }).await?
}
