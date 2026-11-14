//! Scripted typed gRPC peers exercise the complete downloader, not just validators.
use super::*;

fn descriptor() -> SnapshotDescriptor {
    SnapshotDescriptor {
        version: VERSION,
        identity: RemoteIdentity { group: 0, node: 2 },
        incarnation: [3; 16],
        session: [4; 16],
        shard: 7,
        epoch: 1,
        sequence: 8,
        rows: 2,
        bytes: 4,
    }
}

fn page(d: &SnapshotDescriptor, key: u8, more: bool) -> SnapshotPage {
    SnapshotPage {
        descriptor: d.clone(),
        entries: vec![(vec![key], vec![9])],
        next: more.then_some(SnapshotCursor {
            incarnation: d.incarnation,
            session: d.session,
            position: 1,
            token: [5; 16],
        }),
    }
}

#[derive(Clone)]
struct ScriptedPeer {
    descriptor: SnapshotDescriptor,
    state: std::sync::Arc<std::sync::Mutex<ScriptState>>,
    delay: Duration,
    expect_close: bool,
}
struct ScriptState {
    opened: bool,
    pages: std::collections::VecDeque<SnapshotPage>,
    page_index: usize,
    closed: bool,
    finished: Option<tokio::sync::oneshot::Sender<()>>,
    close_gate: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
}
impl ScriptedPeer {
    fn check_context(&self, context: Option<grpc::wire::Context>) {
        let context = context.expect("explicit context");
        assert_eq!(context.version, VERSION);
        let identity = context.identity.expect("explicit identity");
        assert_eq!(identity.group, self.descriptor.identity.group);
        assert_eq!(identity.node, self.descriptor.identity.node);
        assert!(context.timeout_millis > 0);
    }
}
#[tonic::async_trait]
impl grpc::wire::snapshot_service_server::SnapshotService for ScriptedPeer {
    async fn open(
        &self,
        request: tonic::Request<grpc::wire::OpenRequest>,
    ) -> Result<tonic::Response<grpc::wire::Descriptor>, tonic::Status> {
        let request = request.into_inner();
        self.check_context(request.context);
        assert_eq!(request.shard, 7);
        let mut state = self.state.lock().unwrap();
        assert!(!state.opened, "downloader must not reopen or retry");
        state.opened = true;
        Ok(tonic::Response::new(self.descriptor.clone().into()))
    }
    async fn page(
        &self,
        request: tonic::Request<grpc::wire::PageRequest>,
    ) -> Result<tonic::Response<grpc::wire::PageResponse>, tonic::Status> {
        let request = request.into_inner();
        self.check_context(request.context);
        let descriptor: SnapshotDescriptor = request
            .descriptor
            .expect("page descriptor")
            .try_into()
            .unwrap();
        assert_eq!(descriptor, self.descriptor);
        let (page, finished) = {
            let mut state = self.state.lock().unwrap();
            assert!(state.opened && !state.closed);
            assert_eq!(
                request.cursor.map(|c| c.position),
                (state.page_index != 0).then_some(state.page_index as u64)
            );
            state.page_index += 1;
            let page = state
                .pages
                .pop_front()
                .expect("unexpected fetch or retry after invalid page");
            let finished = if state.pages.is_empty() && !self.expect_close {
                state.finished.take()
            } else {
                None
            };
            (page, finished)
        };
        // Shutdown signal proves the final page was requested even if the client
        // deadline cancels its response. Graceful shutdown drains this RPC.
        if let Some(finished) = finished {
            finished.send(()).unwrap();
        }
        tokio::time::sleep(self.delay).await;
        Ok(tonic::Response::new(grpc::wire::PageResponse {
            descriptor: Some(page.descriptor.into()),
            entries: page
                .entries
                .into_iter()
                .map(|(key, value)| grpc::wire::Entry { key, value })
                .collect(),
            next: page.next.map(Into::into),
        }))
    }
    async fn close(
        &self,
        request: tonic::Request<grpc::wire::CloseRequest>,
    ) -> Result<tonic::Response<grpc::wire::Empty>, tonic::Status> {
        let request = request.into_inner();
        self.check_context(request.context);
        let descriptor: SnapshotDescriptor = request
            .descriptor
            .expect("close descriptor")
            .try_into()
            .unwrap();
        assert_eq!(descriptor, self.descriptor);
        let (gate, finished) = {
            let mut state = self.state.lock().unwrap();
            assert!(self.expect_close && state.opened && !state.closed);
            assert!(
                state.pages.is_empty(),
                "must stop fetching at the scripted boundary"
            );
            state.closed = true;
            (state.close_gate.take(), state.finished.take())
        };
        if let Some((closing, released)) = gate {
            closing.send(()).unwrap();
            released.await.unwrap();
        }
        finished.expect("one close only").send(()).unwrap();
        Ok(tonic::Response::new(grpc::wire::Empty {}))
    }
}
async fn peer(
    d: SnapshotDescriptor,
    pages: Vec<SnapshotPage>,
    delay: Duration,
    expect_close: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    peer_with_gate(d, pages, delay, expect_close, None).await
}
async fn peer_with_gate(
    d: SnapshotDescriptor,
    pages: Vec<SnapshotPage>,
    delay: Duration,
    expect_close: bool,
    close_gate: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let (finished, shutdown) = tokio::sync::oneshot::channel();
    let state = std::sync::Arc::new(std::sync::Mutex::new(ScriptState {
        opened: false,
        pages: pages.into(),
        page_index: 0,
        closed: false,
        finished: Some(finished),
        close_gate,
    }));
    let service = ScriptedPeer {
        descriptor: d,
        state: state.clone(),
        delay,
        expect_close,
    };
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(grpc::wire::snapshot_service_server::SnapshotServiceServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown.await;
                },
            )
            .await
            .unwrap();
        let state = state.lock().unwrap();
        assert!(state.opened && state.pages.is_empty());
        assert_eq!(state.closed, expect_close);
    });
    (endpoint, task)
}

#[tokio::test]
async fn malformed_downloads_fail_and_close_without_partial_results() {
    // A boundary duplicate is individually valid on both pages, but invalid as
    // a complete snapshot. Descending boundary keys must likewise fail.
    for last_key in [1, 0] {
        let d = descriptor();
        let pages = vec![page(&d, 1, true), page(&d, last_key, false)];
        let (endpoint, server) = peer(d, pages, Duration::ZERO, true).await;
        assert!(matches!(
            paged_snapshot_identified(endpoint, descriptor().identity, 7, Duration::from_secs(2))
                .await,
            Err(RemoteError::Protocol)
        ));
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
    // Individually plausible pages still must exactly match the declared total.
    let mut d = descriptor();
    d.bytes = 5;
    let pages = vec![page(&d, 1, true), page(&d, 2, false)];
    let (endpoint, server) = peer(d, pages, Duration::ZERO, true).await;
    assert!(matches!(
        paged_snapshot_identified(endpoint, descriptor().identity, 7, Duration::from_secs(2)).await,
        Err(RemoteError::Protocol)
    ));
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    // Early metadata, cursor and progress violations must stop immediately.
    for fault in 0..3 {
        let d = descriptor();
        let mut bad = page(&d, 1, true);
        match fault {
            0 => bad.descriptor.sequence += 1,
            1 => bad.next = None,
            _ => bad.entries.clear(),
        }
        let (endpoint, server) = peer(d, vec![bad], Duration::ZERO, true).await;
        assert!(matches!(
            paged_snapshot_identified(endpoint, descriptor().identity, 7, Duration::from_secs(2))
                .await,
            Err(RemoteError::Protocol)
        ));
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn successive_pages_share_one_deadline() {
    let d = descriptor();
    let pages = vec![page(&d, 1, true), page(&d, 2, false)];
    // Each individual page fits a 400 ms budget; both together cannot. The
    // server asserts that the second page was requested (rather than a retry).
    let (endpoint, server) = peer(d, pages, Duration::from_millis(250), false).await;
    let started = Instant::now();
    assert!(matches!(
        paged_snapshot_identified(
            endpoint,
            descriptor().identity,
            7,
            Duration::from_millis(400)
        )
        .await,
        Err(RemoteError::Timeout { request_id: None })
    ));
    assert!(started.elapsed() < Duration::from_millis(900));
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn completed_download_retains_admission_until_close_finishes() {
    let mut d = descriptor();
    d.rows = 100;
    d.bytes = 100 * 1024;
    let (closing, reached_close) = tokio::sync::oneshot::channel();
    let (release_close, released) = tokio::sync::oneshot::channel();
    let entries = (0..100u32)
        .map(|n| (n.to_be_bytes().to_vec(), vec![0; 1020]))
        .collect();
    let page = SnapshotPage {
        descriptor: d.clone(),
        entries,
        next: None,
    };
    let (endpoint, server) = peer_with_gate(
        d,
        vec![page],
        Duration::ZERO,
        true,
        Some((closing, released)),
    )
    .await;
    let download = tokio::spawn(paged_snapshot_identified(
        endpoint,
        descriptor().identity,
        7,
        Duration::from_secs(3),
    ));
    tokio::time::timeout(Duration::from_secs(2), reached_close)
        .await
        .unwrap()
        .unwrap();
    assert!(!download.is_finished());
    // Other concurrently running tests may hold additional permits, so assert
    // the upper bound rather than an exact global count. The completed result
    // remains owned by download while Close is pending.
    assert!(DOWNLOAD_BYTES.available_permits() <= MAX_RETAINED_BYTES - 100 * 1024);
    assert!(DOWNLOAD_ROWS.available_permits() <= MAX_SESSION_ROWS * MAX_SESSIONS - 100);
    release_close.send(()).unwrap();
    assert_eq!(download.await.unwrap().unwrap().entries.len(), 100);
    server.await.unwrap();
}
