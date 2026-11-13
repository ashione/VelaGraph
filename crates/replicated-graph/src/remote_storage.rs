//! Synchronous graph adapter for the trusted-network typed gRPC service.
//!
//! Construct on a live Tokio runtime and invoke graph work through `run_blocking`.
//! Snapshots are bounded full copies; all data reads use the pinned copy. Each
//! read checks the selected replica's applied epoch, not a fresh quorum fence.
//! Thus concurrent epoch changes have the same check/read race as the in-process
//! adapter. No leader discovery, automatic retry, or administrative RPC is used.
use std::cell::Cell;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use d2_raft::{D1Result, D1WriteRequest, D1WriteStatus};
use tokio::runtime::Handle;
use tokio::sync::{OnceCell, Semaphore};
use velagraph::{
    Epoch, Export, KeyRange, KeyValue, ReadView, RequestId, ScanCursor, ScanLimit, ScanPage,
    Sequence, ShardId, StorageAdmin, StorageNode, StorageNodeError, WriteOp, WriteRequest,
    WriteResult, WriteStatus,
};

use crate::remote::{self, RemoteError};
use crate::{application_error, poll_before_deadline, Result, BLOCKING_SCOPE};

/// One explicitly selected remote replica and shard. Clones share a maximum of
/// 32 outstanding operations. A timeout or transport loss on a write retains its
/// original request ID in `Timeout`; it does not prove that the write aborted.
#[derive(Clone)]
pub struct RemoteStorageNode {
    endpoint: SocketAddr,
    shard: ShardId,
    deadline: Duration,
    absolute_deadline: Option<Instant>,
    identity: remote::RemoteIdentity,
    runtime: Handle,
    admission: Arc<Semaphore>,
    client: Arc<OnceCell<remote::operations_grpc::Client>>,
    metadata_client: remote::metadata_stream_client::Client,
}

impl RemoteStorageNode {
    /// Capture the runtime that dispatches typed RPC I/O. Every request validates
    /// the explicit group/node identity; construction performs no I/O or D1 activation.
    pub fn new_identified(
        endpoint: SocketAddr,
        group: u64,
        node: u64,
        shard: ShardId,
        deadline: Duration,
    ) -> Result<Self> {
        if deadline.is_zero()
            || deadline > remote::MAX_DEADLINE
            || Instant::now().checked_add(deadline).is_none()
        {
            return Err(StorageNodeError::InvalidArgument("invalid deadline".into()));
        }
        let runtime = Handle::try_current().map_err(|_| {
            StorageNodeError::InvalidArgument("construct remote adapter on a runtime".into())
        })?;
        Ok(Self {
            endpoint,
            shard,
            deadline,
            absolute_deadline: None,
            identity: remote::RemoteIdentity { group, node },
            runtime,
            admission: Arc::new(Semaphore::new(32)),
            client: Arc::new(OnceCell::new()),
            metadata_client: remote::metadata_stream_client::Client::new(
                endpoint,
                remote::RemoteIdentity { group, node },
            ),
        })
    }

    pub(crate) fn scoped(&self, shard: ShardId, end: Instant, admission: Arc<Semaphore>) -> Self {
        let mut node = self.clone();
        node.shard = shard;
        node.absolute_deadline = Some(end);
        node.admission = admission;
        node
    }

    fn operation_deadline(&self) -> Instant {
        self.absolute_deadline
            .unwrap_or_else(|| Instant::now() + self.deadline)
    }

    fn client(
        &self,
        end: Instant,
    ) -> impl Future<Output = std::result::Result<remote::operations_grpc::Client, RemoteError>>
           + Send
           + 'static {
        let cache = self.client.clone();
        let (endpoint, identity, budget) = (self.endpoint, self.identity, self.deadline);
        async move {
            let end = tokio::time::Instant::from_std(end);
            if tokio::time::Instant::now() >= end {
                return Err(RemoteError::Timeout { request_id: None });
            }
            // Waiters and the initializer share each caller's absolute deadline.
            // Cache only a successful channel; no application RPC is replayed.
            let client = tokio::time::timeout_at(
                end,
                cache.get_or_try_init(|| {
                    remote::operations_grpc::Client::connect(endpoint, identity, budget)
                }),
            )
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })??;
            Ok(client.clone().with_deadline(end))
        }
    }

    pub(crate) fn probe_leader(&self) -> Result<()> {
        let shard = self.shard;
        let end = self.operation_deadline();
        let client = self.client(end);
        self.dispatch(None, async move {
            if Instant::now() >= end {
                return Err(StorageNodeError::Timeout { request_id: None });
            }
            let mut client = client.await.map_err(|e| map_error(e, shard, 0, None))?;
            client
                .probe(shard)
                .await
                .map_err(|e| map_error(e, shard, 0, None))
        })
    }

    fn dispatch<T, F>(&self, request_id: Option<RequestId>, future: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        self.check_execution_context()?;
        let end = match self.absolute_deadline {
            Some(end) => end,
            None => Instant::now()
                .checked_add(self.deadline)
                .ok_or_else(|| StorageNodeError::InvalidArgument("deadline overflow".into()))?,
        };
        if Instant::now() >= end {
            return Err(StorageNodeError::Timeout { request_id });
        }
        let permit = self.admission.clone().try_acquire_owned().map_err(|_| {
            StorageNodeError::ResourceExhausted(
                "32 remote adapter operations already in flight".into(),
            )
        })?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let task = self.runtime.spawn(async move {
            let _permit = permit;
            let result = poll_before_deadline(end, request_id, future).await;
            let _ = sender.send(result);
        });
        match receiver.recv_timeout(end.saturating_duration_since(Instant::now())) {
            Ok(result) if Instant::now() < end => result,
            Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {
                task.abort();
                Err(StorageNodeError::Timeout { request_id })
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => match request_id {
                Some(_) => Err(StorageNodeError::Timeout { request_id }),
                None => Err(StorageNodeError::Unavailable(
                    "remote adapter runtime stopped".into(),
                )),
            },
        }
    }

    pub(crate) fn check_view(&self, view: &ReadView) -> Result<()> {
        if view.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(
                "view belongs to a different shard".into(),
            ));
        }
        let (shard, epoch) = (self.shard, view.epoch);
        let end = self.operation_deadline();
        let client = self.metadata_client.clone();
        self.dispatch(None, async move {
            if Instant::now() >= end {
                return Err(StorageNodeError::Timeout { request_id: None });
            }
            let (current_shard, current, _) = client
                .metadata(shard, tokio::time::Instant::from_std(end))
                .await
                .map_err(|e| map_error(e, shard, epoch, None))?;
            if current_shard != shard {
                return Err(StorageNodeError::Corrupt(
                    "remote metadata shard mismatch".into(),
                ));
            }
            if current != epoch {
                return Err(StorageNodeError::Fenced {
                    shard,
                    epoch,
                    current,
                });
            }
            Ok(())
        })
    }
}

fn map_error(
    error: RemoteError,
    shard: ShardId,
    epoch: Epoch,
    id: Option<RequestId>,
) -> StorageNodeError {
    match error {
        RemoteError::NotLeader { leader } => StorageNodeError::NotLeader { leader },
        RemoteError::Application(d2_raft::D1Error::Fenced { requested, current })
            if id.is_none() && epoch == 0 =>
        {
            StorageNodeError::Fenced {
                shard,
                epoch: requested,
                current,
            }
        }
        RemoteError::Application(error) => {
            application_error(error, shard, epoch, id.unwrap_or([0; 16]))
        }
        RemoteError::InvalidArgument => {
            StorageNodeError::InvalidArgument("invalid remote request".into())
        }
        RemoteError::ResourceExhausted => {
            StorageNodeError::ResourceExhausted("remote service limit exceeded".into())
        }
        RemoteError::Timeout { .. } => StorageNodeError::Timeout { request_id: id },
        RemoteError::Unavailable | RemoteError::SessionUnavailable | RemoteError::Protocol
            if id.is_some() =>
        {
            StorageNodeError::Timeout { request_id: id }
        }
        RemoteError::Unavailable | RemoteError::SessionUnavailable => {
            StorageNodeError::Unavailable("remote service unavailable".into())
        }
        RemoteError::Protocol => {
            StorageNodeError::Corrupt("invalid remote protocol response".into())
        }
    }
}

impl StorageNode for RemoteStorageNode {
    fn check_execution_context(&self) -> Result<()> {
        if Handle::try_current().is_ok() && !BLOCKING_SCOPE.with(Cell::get) {
            return Err(StorageNodeError::Unsupported(
                "synchronous remote calls on a Tokio worker; use run_blocking".into(),
            ));
        }
        Ok(())
    }

    fn read_view(&self) -> Result<ReadView> {
        let (endpoint, shard) = (self.endpoint, self.shard);
        let end = self
            .absolute_deadline
            .unwrap_or_else(|| Instant::now() + self.deadline);
        let identity = self.identity;
        self.dispatch(None, async move {
            if Instant::now() >= end {
                return Err(StorageNodeError::Timeout { request_id: None });
            }
            let deadline = end.saturating_duration_since(Instant::now());
            let snapshot = remote::grpc::paged_snapshot(endpoint, identity, shard, deadline)
                .await
                .map_err(|e| map_error(e, shard, 0, None))?;
            // Transport validated all rows and metadata before exposing a result.
            // Move into the existing complete local view, checking the same
            // absolute deadline throughout this bounded synchronous assembly.
            let mut entries = std::collections::BTreeMap::new();
            for (key, value) in snapshot.entries {
                if Instant::now() >= end {
                    return Err(StorageNodeError::Timeout { request_id: None });
                }
                entries.insert(key, value);
            }
            let view =
                ReadView::from_entries(snapshot.shard, snapshot.epoch, snapshot.sequence, entries)?;
            if Instant::now() >= end {
                return Err(StorageNodeError::Timeout { request_id: None });
            }
            Ok(view)
        })
    }

    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>> {
        self.check_view(view)?;
        Ok(view.get(key))
    }

    fn scan(&self, range: KeyRange, limit: ScanLimit, view: &ReadView) -> Result<Vec<KeyValue>> {
        self.check_view(view)?;
        view.scan(range, limit)
    }

    fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
        cursor: Option<&ScanCursor>,
    ) -> Result<ScanPage> {
        self.check_view(view)?;
        view.scan_page(range, limit, cursor)
    }

    fn batch_write(&self, request: WriteRequest) -> Result<WriteResult> {
        self.batch_write_conditional(request, None)
    }

    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        expected_sequence: Option<Sequence>,
    ) -> Result<WriteResult> {
        self.check_execution_context()?;
        if request.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(
                "request belongs to a different shard".into(),
            ));
        }
        if request.operations.is_empty() {
            return Err(StorageNodeError::InvalidArgument("empty batch".into()));
        }
        let bytes = request.operations.iter().fold(0usize, |n, op| match op {
            WriteOp::Put { key, value } => n.saturating_add(key.len()).saturating_add(value.len()),
            WriteOp::Delete { key } => n.saturating_add(key.len()),
        });
        if request.operations.len() > d2_raft::MAX_BATCH_OPERATIONS
            || bytes > d2_raft::MAX_BATCH_BYTES
        {
            return Err(StorageNodeError::ResourceExhausted(
                "D1 batch limit exceeded".into(),
            ));
        }
        let end = self.operation_deadline();
        let (shard, epoch, id) = (self.shard, request.epoch, request.request_id);
        let request = D1WriteRequest {
            shard,
            epoch,
            request_id: id,
            expected_sequence,
            operations: request
                .operations
                .into_iter()
                .map(|op| match op {
                    WriteOp::Put { key, value } => d2_raft::WriteOp::Put { key, value },
                    WriteOp::Delete { key } => d2_raft::WriteOp::Delete { key },
                })
                .collect(),
        };
        let client = self.client(end);
        self.dispatch(Some(id), async move {
            if Instant::now() >= end {
                return Err(StorageNodeError::Timeout {
                    request_id: Some(id),
                });
            }
            let mut client = client
                .await
                .map_err(|e| map_error(e, shard, epoch, Some(id)))?;
            let result = client
                .write(request)
                .await
                .map_err(|e| map_error(e, shard, epoch, Some(id)))?;
            match result {
                D1Result::Write {
                    sequence,
                    status,
                    applied_operations,
                } => Ok(WriteResult {
                    sequence,
                    applied_operations,
                    status: match status {
                        D1WriteStatus::Committed => WriteStatus::Committed,
                        D1WriteStatus::Duplicate => WriteStatus::Duplicate,
                    },
                }),
                _ => Err(StorageNodeError::Timeout {
                    request_id: Some(id),
                }),
            }
        })
    }

    fn admin(&self) -> &dyn StorageAdmin {
        self
    }
}

impl StorageAdmin for RemoteStorageNode {
    fn export(&self) -> Result<Export> {
        Err(StorageNodeError::Unsupported("remote export".into()))
    }
    fn import(&self, _: Export) -> Result<()> {
        Err(StorageNodeError::Unsupported("remote import".into()))
    }
    fn set_epoch(&self, _: Epoch) -> Result<()> {
        Err(StorageNodeError::Unsupported(
            "remote epoch management requires explicit D1V1 CAS".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_blocking;

    fn endpoint() -> SocketAddr {
        "127.0.0.1:1".parse().unwrap()
    }

    #[test]
    fn constructor_requires_runtime() {
        assert!(matches!(
            RemoteStorageNode::new_identified(endpoint(), 1, 1, 7, Duration::from_secs(1)),
            Err(StorageNodeError::InvalidArgument(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_guard_limits_and_timeout_identity() {
        let adapter =
            RemoteStorageNode::new_identified(endpoint(), 1, 1, 7, Duration::from_millis(20))
                .unwrap();
        assert!(RemoteStorageNode::new_identified(endpoint(), 1, 1, 7, Duration::ZERO).is_err());
        assert!(
            RemoteStorageNode::new_identified(endpoint(), 1, 1, 7, Duration::from_secs(61))
                .is_err()
        );
        assert!(matches!(
            adapter.read_view(),
            Err(StorageNodeError::Unsupported(_))
        ));
        let held = adapter
            .admission
            .clone()
            .acquire_many_owned(32)
            .await
            .unwrap();
        let worker = adapter.clone();
        assert!(matches!(
            run_blocking(move || worker.dispatch(None, async { Ok(()) }))
                .await
                .unwrap(),
            Err(StorageNodeError::ResourceExhausted(_))
        ));
        drop(held);
        let worker = adapter.clone();
        let result = run_blocking(move || {
            worker.dispatch(Some([9; 16]), std::future::pending::<Result<()>>())
        })
        .await
        .unwrap();
        assert!(
            matches!(result, Err(StorageNodeError::Timeout { request_id: Some(id) }) if id == [9;16])
        );
        assert!(matches!(
            adapter.admin().export(),
            Err(StorageNodeError::Unsupported(_))
        ));
        assert!(matches!(
            adapter.admin().set_epoch(4),
            Err(StorageNodeError::Unsupported(_))
        ));
    }

    #[test]
    fn application_errors_are_typed_and_unknown_writes_keep_identity() {
        let id = [4; 16];
        assert!(
            matches!(map_error(RemoteError::Application(d2_raft::D1Error::IdempotencyConflict),7,3,Some(id)), StorageNodeError::IdempotencyConflict(actual) if actual == id)
        );
        assert!(matches!(
            map_error(
                RemoteError::Application(d2_raft::D1Error::Conflict {
                    expected: 1,
                    current: 2
                }),
                7,
                3,
                Some(id)
            ),
            StorageNodeError::Conflict {
                expected: 1,
                current: 2
            }
        ));
        assert!(matches!(
            map_error(
                RemoteError::Application(d2_raft::D1Error::Fenced {
                    requested: 3,
                    current: 4
                }),
                7,
                3,
                Some(id)
            ),
            StorageNodeError::Fenced {
                shard: 7,
                epoch: 3,
                current: 4
            }
        ));
        for error in [
            RemoteError::Unavailable,
            RemoteError::SessionUnavailable,
            RemoteError::Protocol,
            RemoteError::Timeout { request_id: None },
        ] {
            assert!(
                matches!(map_error(error,7,3,Some(id)), StorageNodeError::Timeout { request_id: Some(actual) } if actual == id)
            );
        }
        assert!(matches!(
            map_error(RemoteError::NotLeader { leader: Some(2) }, 7, 3, Some(id)),
            StorageNodeError::NotLeader { leader: Some(2) }
        ));
    }

    #[test]
    fn stopped_runtime_does_not_hang_or_lose_identity() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let adapter = {
            let _entered = runtime.enter();
            RemoteStorageNode::new_identified(endpoint(), 1, 1, 7, Duration::from_millis(50))
                .unwrap()
        };
        drop(runtime);
        assert!(
            matches!(adapter.dispatch(Some([8;16]), async { Ok(()) }), Err(StorageNodeError::Timeout { request_id: Some(id) }) if id == [8;16])
        );
        assert!(matches!(
            adapter.dispatch(None, async { Ok(()) }),
            Err(StorageNodeError::Unavailable(_))
        ));
    }
}

#[cfg(test)]
mod routing_deadline_tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn expired_scope_never_dispatches_and_clones_share_admission() {
        let node = RemoteStorageNode::new_identified(
            "127.0.0.1:1".parse().unwrap(),
            4,
            1,
            7,
            Duration::from_secs(60),
        )
        .unwrap();
        let admission = Arc::new(Semaphore::new(1));
        let expired = node.scoped(
            7,
            Instant::now() - Duration::from_millis(1),
            admission.clone(),
        );
        let id = [5; 16];
        assert!(
            matches!(crate::run_blocking(move || expired.dispatch(Some(id), async { panic!("must not poll") })).await.unwrap(), Err::<(), _>(StorageNodeError::Timeout { request_id: Some(actual) }) if actual == id)
        );
        let scoped = node.scoped(
            7,
            Instant::now() + Duration::from_secs(1),
            admission.clone(),
        );
        let held = admission.try_acquire_owned().unwrap();
        assert!(matches!(
            crate::run_blocking(move || scoped.dispatch(None, async { Ok(()) }))
                .await
                .unwrap(),
            Err(StorageNodeError::ResourceExhausted(_))
        ));
        drop(held);
    }
}

#[cfg(test)]
mod channel_cache_tests {
    use super::*;
    use crate::remote::operations_grpc::wire;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;

    struct CountedIo {
        io: tokio::net::TcpStream,
        active: Arc<AtomicUsize>,
        closed: Pin<Box<dyn Future<Output = ()> + Send>>,
    }
    impl Drop for CountedIo {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
    impl tonic::transport::server::Connected for CountedIo {
        type ConnectInfo = ();
        fn connect_info(&self) {}
    }
    impl AsyncRead for CountedIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.closed.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Ok(()));
            }
            Pin::new(&mut self.io).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for CountedIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.io).poll_write(cx, buf)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.io).poll_flush(cx)
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.io).poll_shutdown(cx)
        }
    }
    use std::io;
    #[derive(Clone)]
    struct Probe(Arc<AtomicUsize>);
    #[tonic::async_trait]
    impl wire::shard_operations_server::ShardOperations for Probe {
        async fn call(
            &self,
            _: tonic::Request<wire::Request>,
        ) -> std::result::Result<tonic::Response<wire::Response>, tonic::Status> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(tonic::Response::new(wire::Response {
                version: 1,
                result: Some(wire::response::Result::Probe(wire::Empty {})),
            }))
        }
    }
    async fn until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("bounded lifecycle condition");
    }
    #[tokio::test]
    async fn concurrent_scopes_share_channel_reconnect_and_release() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let generation = Arc::new(std::sync::Mutex::new(CancellationToken::new()));
        let (a, b, g) = (accepted.clone(), active.clone(), generation.clone());
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |item| {
            item.map(|io| {
                a.fetch_add(1, Ordering::SeqCst);
                b.fetch_add(1, Ordering::SeqCst);
                CountedIo {
                    io,
                    active: b.clone(),
                    closed: Box::pin(g.lock().unwrap().clone().cancelled_owned()),
                }
            })
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let service =
            wire::shard_operations_server::ShardOperationsServer::new(Probe(calls.clone()));
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let node =
            RemoteStorageNode::new_identified(address, 1, 1, 7, Duration::from_secs(2)).unwrap();
        let short = node.scoped(
            7,
            Instant::now() + Duration::from_millis(200),
            node.admission.clone(),
        );
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let scope = short.clone();
            workers.spawn(async move {
                scope
                    .client(scope.operation_deadline())
                    .await
                    .unwrap()
                    .probe(7)
                    .await
                    .unwrap();
            });
        }
        while let Some(result) = workers.join_next().await {
            result.unwrap();
        }
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 8);
        tokio::time::sleep(Duration::from_millis(220)).await;
        // First short scope is not cached as the permanent channel budget.
        node.client(node.operation_deadline())
            .await
            .unwrap()
            .probe(7)
            .await
            .unwrap();
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
        {
            let mut old = generation.lock().unwrap();
            old.cancel();
            *old = CancellationToken::new();
        }
        until(|| active.load(Ordering::SeqCst) == 0).await;
        let mut attempts = 0;
        let mut recovered = false;
        // Only explicitly issued read-only operations may test reconnection.
        for _ in 0..10 {
            attempts += 1;
            let result = node
                .client(node.operation_deadline())
                .await
                .unwrap()
                .probe(7)
                .await;
            if result.is_ok() {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(recovered);
        assert!(accepted.load(Ordering::SeqCst) >= 2);
        assert!(
            calls.load(Ordering::SeqCst) <= 9 + attempts,
            "no application replay"
        );
        drop(short);
        drop(node);
        until(|| active.load(Ordering::SeqCst) == 0).await;
        stop.send(()).unwrap();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn cancelled_initializer_and_waiter_do_not_poison_cache() {
        let node = RemoteStorageNode::new_identified(
            "127.0.0.1:1".parse().unwrap(),
            1,
            1,
            7,
            Duration::from_secs(1),
        )
        .unwrap();
        let cache = node.client.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn(async move {
            cache
                .get_or_init(|| async {
                    let _ = started.send(());
                    std::future::pending::<remote::operations_grpc::Client>().await
                })
                .await;
        });
        ready.await.unwrap();
        assert!(matches!(
            node.client(Instant::now() + Duration::from_millis(10))
                .await,
            Err(RemoteError::Timeout { .. })
        ));
        assert!(node.client.get().is_none());
        holder.abort();
        let _ = holder.await;
        assert!(node.client.get().is_none());
        // A later initializer gets the permit; a failed connection is not cached.
        assert!(node
            .client(Instant::now() + Duration::from_millis(100))
            .await
            .is_err());
        assert!(node.client.get().is_none());
    }
}
