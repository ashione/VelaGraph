//! Real transport/time coverage. No paused clock or registry-clock injection.
use super::*;
use crate::remote::{GraphService, RemoteIdentity};
use grpc::wire;
use tonic::transport::Channel;
use wire::query_service_client::QueryServiceClient;

const IDENTITY: RemoteIdentity = RemoteIdentity { group: 10, node: 1 };
type Client = QueryServiceClient<Channel>;

async fn open(client: &mut Client, millis: u64) -> Descriptor {
    let mut request = tonic::Request::new(wire::OpenRequest {
        context: Some(wire::Context {
            version: VERSION,
            identity: Some(wire::Identity { group: 10, node: 1 }),
            timeout_millis: millis,
        }),
        shard: 7,
    });
    request.set_timeout(Duration::from_millis(millis));
    client
        .open(request)
        .await
        .unwrap()
        .into_inner()
        .try_into()
        .unwrap()
}
fn exists(descriptor: &Descriptor) -> FragmentRequest {
    FragmentRequest {
        descriptor: descriptor.clone(),
        fragment: Fragment::Exists(VertexId {
            partition: 0,
            id: 1,
        }),
        remaining: CEILING,
    }
}
// A failed assertion while waiting for worker entry must not detach the caller.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// Always unblock a real worker on panic, timeout or task cancellation.
struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

async fn start_service() -> (tempfile::TempDir, d2_raft::TcpNode, GraphService) {
    let directory = tempfile::tempdir().unwrap();
    let node = d2_raft::TcpNode::start(d2_raft::TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: directory.path().into(),
    })
    .await
    .unwrap();
    node.bootstrap(BTreeMap::from([(1, node.raft_addr.clone())]))
        .await
        .unwrap();
    node.raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "wire lease leader")
        .await
        .unwrap();
    for command in [
        D1Command::Activate { shard: 7, epoch: 1 },
        D1Command::Write(d2_raft::D1WriteRequest {
            shard: 7,
            epoch: 1,
            request_id: [45; 16],
            expected_sequence: None,
            operations: vec![d2_raft::WriteOp::put(b"wire-lease-payload", b"bounded")],
        }),
    ] {
        node.raft
            .client_write(LogCommand::D1V1(command))
            .await
            .unwrap()
            .data
            .d1
            .unwrap()
            .unwrap();
    }
    let service = GraphService::start_identified(
        &node,
        "127.0.0.1:0".parse().unwrap(),
        10,
        7,
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    (directory, node, service)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_deadline_wire_crosses_rpc_cap_and_cancelled_caller_keeps_resources_charged() {
    let _fixtures = TEST_SESSION_FIXTURES.lock().await;
    let (_directory, node, service) = start_service().await;
    let address = service.local_addr();
    let sessions = service.query_sessions.clone();
    let mut scenario = tokio::spawn(async move {
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut raw = Client::new(channel);
        let client =
            grpc::QueryClient::connect_lazy(address, IDENTITY, Duration::from_secs(5)).unwrap();
        let before = Instant::now();
        let descriptor = open(&mut raw, 5000).await;
        let after = Instant::now();
        let retained = sessions.lookup(IDENTITY, 7, &descriptor).unwrap();
        let lease = Instant::from_std(retained.node.end);
        assert!(lease >= before + Duration::from_secs(5));
        assert!(lease <= after + Duration::from_secs(5));
        assert!(
            lease > after + Duration::from_secs(3),
            "grant interval cannot distinguish old cap"
        );
        let query_end = before + Duration::from_secs(5);
        let target = after + Duration::from_millis(3100);
        assert!(target < query_end, "no valid cross-cap experiment window");
        tokio::time::sleep_until(target).await;
        let sent = Instant::now();
        assert!(sent > after + Duration::from_secs(3));
        assert!(
            sent < query_end,
            "scheduler missed the original query deadline"
        );
        let output = client
            .fragment(exists(&descriptor), query_end - sent)
            .await
            .unwrap()
            .unwrap();
        let finished = Instant::now();
        assert!(
            finished < query_end,
            "successful call outlived original query budget"
        );
        assert!(matches!(output.value, FragmentValue::Bool(false)));
        assert!(output.cost.bytes > 0 && output.cost.work > 0);
        assert_eq!(retained.node.end, lease.into_std());
        eprintln!(
            "wire lease: open_ns={} send_ns={} finish_ns={} lease_ns={}",
            (after - before).as_nanos(),
            (sent - before).as_nanos(),
            (finished - before).as_nanos(),
            (lease - before).as_nanos()
        );
        tokio::time::sleep_until(lease).await;
        // New RPC only diagnoses expiry; it does not extend the original query.
        assert!(matches!(
            client
                .fragment(exists(&descriptor), Duration::from_secs(1))
                .await,
            Err(RemoteError::SessionUnavailable)
        ));
        assert!(!sessions
            .values
            .lock()
            .unwrap()
            .contains_key(&descriptor.token));
        drop(retained);

        // A shorter requested lifetime remains short despite the 5-second maximum.
        let short_before = Instant::now();
        let short = open(&mut raw, 1000).await;
        let short_after = Instant::now();
        let held = sessions.lookup(IDENTITY, 7, &short).unwrap();
        assert!(held.node.end >= (short_before + Duration::from_secs(1)).into_std());
        assert!(held.node.end <= (short_after + Duration::from_secs(1)).into_std());
        client
            .close(short.clone(), Duration::from_secs(1))
            .await
            .unwrap();
        drop(held);

        let slots_before = SESSION_SLOTS.available_permits();
        let bytes_before = SESSION_BYTES.available_permits();
        let workers_before = FRAGMENT_SLOTS.available_permits();
        let cancelled = open(&mut raw, 5000).await;
        let held = sessions.lookup(IDENTITY, 7, &cancelled).unwrap();
        let weak = Arc::downgrade(&held);
        let bytes_held = SESSION_BYTES.available_permits();
        assert!(bytes_held < bytes_before);
        drop(held);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
        *sessions.fragment_test_gate.lock().unwrap() = Some(FragmentTestGate {
            entered: entered_tx,
            release: release_rx,
            exited: exited_tx,
        });
        let release = Release(Some(release_tx));
        let request = exists(&cancelled);
        let caller_client =
            grpc::QueryClient::connect_lazy(address, IDENTITY, Duration::from_secs(3)).unwrap();
        let mut caller = AbortOnDrop(tokio::spawn(async move {
            caller_client
                .fragment(request, Duration::from_secs(3))
                .await
        }));
        tokio::time::timeout(Duration::from_secs(1), entered_rx)
            .await
            .unwrap()
            .unwrap();
        caller.0.abort();
        assert!((&mut caller.0).await.unwrap_err().is_cancelled());
        client
            .close(cancelled.clone(), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(!sessions
            .values
            .lock()
            .unwrap()
            .contains_key(&cancelled.token));
        assert!(weak.upgrade().is_some());
        assert_eq!(SESSION_SLOTS.available_permits(), slots_before - 1);
        assert_eq!(SESSION_BYTES.available_permits(), bytes_held);
        assert_eq!(FRAGMENT_SLOTS.available_permits(), workers_before - 1);
        drop(release);
        tokio::time::timeout(Duration::from_secs(2), exited_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(weak.upgrade().is_none());
        assert_eq!(SESSION_SLOTS.available_permits(), slots_before);
        assert_eq!(SESSION_BYTES.available_permits(), bytes_before);
        assert_eq!(FRAGMENT_SLOTS.available_permits(), workers_before);
    });
    // Join captures scenario assertion panic so service/node shutdown still occurs.
    // This bound excludes setup and shutdown. The external process supervisor
    // remains responsible for a setup panic or stuck native work/shutdown.
    let outcome = tokio::time::timeout(Duration::from_secs(15), &mut scenario).await;
    if outcome.is_err() {
        scenario.abort();
        let _ = scenario.await;
    }
    service.shutdown().await;
    node.shutdown().await;
    outcome
        .expect("bounded wire scenario")
        .expect("wire assertions");
}

const OPEN_PATH: &str = "/velagraph.internal.query.v1.QueryService/Open";

#[tokio::test]
async fn queued_observed_open_expires_while_the_only_stream_is_occupied() {
    use tonic::codegen::Bytes;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client =
        grpc::QueryClient::connect_lazy(address, IDENTITY, Duration::from_secs(5)).unwrap();
    let first = client.open(7, Duration::from_secs(5));
    tokio::pin!(first);
    let peer = async {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::Builder::new()
            .max_concurrent_streams(1)
            .handshake::<_, Bytes>(socket)
            .await
            .unwrap();
        let (first_request, _held_response) = connection.accept().await.unwrap().unwrap();
        assert_eq!(first_request.uri().path(), OPEN_PATH);
        // Drive the peer while its only response stream is occupied.
        tokio::select! {
            result = client.open(7, Duration::from_millis(250)) => {
                assert!(matches!(result, Err(RemoteError::Timeout { request_id: None })));
            }
            extra = connection.accept() => panic!("unexpected stream before release: {}", extra.is_some()),
        }
    };
    tokio::select! {
        _ = &mut first => panic!("first Open completed before the queued deadline"),
        result = tokio::time::timeout(Duration::from_secs(3), peer) => result.unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undelivered_open_descriptor_releases_snapshot_on_background_expiry() {
    use prost::Message;
    use tonic::codegen::{http, Bytes};
    let _fixtures = TEST_SESSION_FIXTURES.lock().await;
    let (_directory, node, service) = start_service().await;
    let address = service.local_addr();
    let sessions = service.query_sessions.clone();
    let slots = SESSION_SLOTS.available_permits();
    let bytes = SESSION_BYTES.available_permits();
    let scenario = tokio::spawn(async move {
        let socket = tokio::net::TcpStream::connect(address).await.unwrap();
        let (sender, connection) = h2::client::Builder::new()
            .initial_window_size(0)
            .handshake::<_, Bytes>(socket)
            .await
            .unwrap();
        let _driver = AbortOnDrop(tokio::spawn(connection));
        let mut sender = sender.ready().await.unwrap();
        let message = wire::OpenRequest {
            context: Some(wire::Context {
                version: VERSION,
                identity: Some(wire::Identity { group: 10, node: 1 }),
                timeout_millis: 2000,
            }),
            shard: 7,
        }
        .encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
        frame.extend(message);
        let request = http::Request::post(format!("http://{address}{OPEN_PATH}"))
            .header("content-type", "application/grpc")
            .body(())
            .unwrap();
        let (response, mut stream) = sender.send_request(request, false).unwrap();
        stream.send_data(frame.into(), true).unwrap();
        let response = response.await.unwrap();
        assert_eq!(response.status(), 200);
        assert!(response.headers().get("grpc-status").is_none());
        assert_eq!(sessions.values.lock().unwrap().len(), 1);
        assert_eq!(SESSION_SLOTS.available_permits(), slots - 1);
        assert!(SESSION_BYTES.available_permits() < bytes);
        // HEADERS arrived, but a zero window prevents delivery of descriptor DATA.
        // No Close, lookup or manual expiry after the caller abandons the stream.
        stream.send_reset(h2::Reason::CANCEL);
        drop(response);
        while !sessions.values.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(SESSION_SLOTS.available_permits(), slots);
        assert_eq!(SESSION_BYTES.available_permits(), bytes);
    });
    let mut scenario = AbortOnDrop(scenario);
    let result = tokio::time::timeout(Duration::from_secs(8), &mut scenario.0).await;
    if result.is_err() {
        scenario.0.abort();
        let _ = (&mut scenario.0).await;
    }
    service.shutdown().await;
    node.shutdown().await;
    result.unwrap().unwrap();
}
