//! Typed gRPC snapshot transfer. Trusted-network plaintext only, not authentication.
//! Shared endpoint/admission for separate typed snapshot and observed-query services.
//! Neither is the public API or Raft RPC.
use super::*;
use prost::Message;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::codegen::{http, BoxFuture, Bytes, Service};
use tonic::{transport::Channel, Code, Status};

/// Versioned internal protobuf contract. No opaque JSON envelopes.
pub mod wire {
    include!("snapshot_proto.rs");
}
use wire::snapshot_service_client::SnapshotServiceClient;
use wire::snapshot_service_server::{SnapshotService, SnapshotServiceServer};

// Request messages contain only bounded identities/cursors, never graph data.
// Response allowance covers the existing raw page cap plus protobuf entry tags.
const REQUEST_LIMIT: usize = 4096;
const RESPONSE_LIMIT: usize = MAX_PAGE_BYTES + MAX_PAGE_ROWS * 32 + 1024;

pub(in crate::remote) fn status(error: RemoteError) -> Status {
    let (code, reason) = match &error {
        RemoteError::NotLeader { .. } => (Code::FailedPrecondition, "not_leader"),
        RemoteError::Application(D1Error::NotActivated) => {
            (Code::FailedPrecondition, "not_activated")
        }
        RemoteError::Application(_) => (Code::FailedPrecondition, "application"),
        RemoteError::InvalidArgument => (Code::InvalidArgument, "invalid_argument"),
        RemoteError::ResourceExhausted => (Code::ResourceExhausted, "resource_exhausted"),
        RemoteError::Unavailable => (Code::Unavailable, "unavailable"),
        RemoteError::Timeout { .. } => (Code::DeadlineExceeded, "deadline"),
        RemoteError::SessionUnavailable => (Code::NotFound, "session_unavailable"),
        RemoteError::Protocol => (Code::DataLoss, "protocol"),
    };
    let detail = match error {
        RemoteError::NotLeader { leader } => wire::ErrorDetail {
            reason: wire::error_detail::Reason::NotLeader as i32,
            leader,
            ..Default::default()
        },
        RemoteError::Application(D1Error::NotActivated) => wire::ErrorDetail {
            reason: wire::error_detail::Reason::NotActivated as i32,
            leader: None,
            ..Default::default()
        },
        RemoteError::Application(D1Error::Fenced { requested, current }) => wire::ErrorDetail {
            reason: wire::error_detail::Reason::Fenced as i32,
            requested_epoch: Some(requested),
            current_epoch: Some(current),
            ..Default::default()
        },
        RemoteError::SessionUnavailable => wire::ErrorDetail {
            reason: wire::error_detail::Reason::SessionUnavailable as i32,
            leader: None,
            ..Default::default()
        },
        _ => wire::ErrorDetail {
            reason: 0,
            leader: None,
            ..Default::default()
        },
    };
    Status::with_details(code, reason, detail.encode_to_vec().into())
}
pub(in crate::remote) fn remote_status(error: Status) -> RemoteError {
    let detail = wire::ErrorDetail::decode(error.details()).ok();
    match (
        error.code(),
        detail
            .as_ref()
            .and_then(|d| wire::error_detail::Reason::try_from(d.reason).ok()),
    ) {
        (Code::FailedPrecondition, Some(wire::error_detail::Reason::NotLeader)) => {
            RemoteError::NotLeader {
                leader: detail.and_then(|d| d.leader),
            }
        }
        (Code::FailedPrecondition, Some(wire::error_detail::Reason::NotActivated)) => {
            RemoteError::Application(D1Error::NotActivated)
        }
        (Code::FailedPrecondition, Some(wire::error_detail::Reason::Fenced)) => match detail {
            Some(wire::ErrorDetail {
                requested_epoch: Some(requested),
                current_epoch: Some(current),
                ..
            }) => RemoteError::Application(D1Error::Fenced { requested, current }),
            _ => RemoteError::Protocol,
        },
        (Code::NotFound, Some(wire::error_detail::Reason::SessionUnavailable)) => {
            RemoteError::SessionUnavailable
        }
        (Code::InvalidArgument, _) => RemoteError::InvalidArgument,
        (Code::ResourceExhausted, _) => RemoteError::ResourceExhausted,
        (Code::DeadlineExceeded | Code::Cancelled, _) => RemoteError::Timeout { request_id: None },
        (Code::DataLoss | Code::Unimplemented | Code::FailedPrecondition | Code::NotFound, _) => {
            RemoteError::Protocol
        }
        _ => RemoteError::Unavailable,
    }
}
impl From<RemoteIdentity> for wire::Identity {
    fn from(value: RemoteIdentity) -> Self {
        Self {
            group: value.group,
            node: value.node,
        }
    }
}
impl From<wire::Identity> for RemoteIdentity {
    fn from(value: wire::Identity) -> Self {
        Self {
            group: value.group,
            node: value.node,
        }
    }
}
impl From<SnapshotDescriptor> for wire::Descriptor {
    fn from(d: SnapshotDescriptor) -> Self {
        Self {
            version: d.version,
            identity: Some(d.identity.into()),
            incarnation: d.incarnation.to_vec(),
            session: d.session.to_vec(),
            shard: d.shard,
            epoch: d.epoch,
            sequence: d.sequence,
            rows: d.rows as u64,
            bytes: d.bytes as u64,
        }
    }
}
impl TryFrom<wire::Descriptor> for SnapshotDescriptor {
    type Error = RemoteError;
    fn try_from(d: wire::Descriptor) -> Result<Self, Self::Error> {
        Ok(Self {
            version: d.version,
            identity: d.identity.ok_or(RemoteError::InvalidArgument)?.into(),
            incarnation: d
                .incarnation
                .try_into()
                .map_err(|_| RemoteError::InvalidArgument)?,
            session: d
                .session
                .try_into()
                .map_err(|_| RemoteError::InvalidArgument)?,
            shard: d.shard,
            epoch: d.epoch,
            sequence: d.sequence,
            rows: d
                .rows
                .try_into()
                .map_err(|_| RemoteError::ResourceExhausted)?,
            bytes: d
                .bytes
                .try_into()
                .map_err(|_| RemoteError::ResourceExhausted)?,
        })
    }
}
impl From<SnapshotCursor> for wire::Cursor {
    fn from(c: SnapshotCursor) -> Self {
        Self {
            incarnation: c.incarnation.to_vec(),
            session: c.session.to_vec(),
            position: c.position as u64,
            token: c.token.to_vec(),
        }
    }
}
impl TryFrom<wire::Cursor> for SnapshotCursor {
    type Error = RemoteError;
    fn try_from(c: wire::Cursor) -> Result<Self, Self::Error> {
        Ok(Self {
            incarnation: c
                .incarnation
                .try_into()
                .map_err(|_| RemoteError::InvalidArgument)?,
            session: c
                .session
                .try_into()
                .map_err(|_| RemoteError::InvalidArgument)?,
            position: c
                .position
                .try_into()
                .map_err(|_| RemoteError::ResourceExhausted)?,
            token: c
                .token
                .try_into()
                .map_err(|_| RemoteError::InvalidArgument)?,
        })
    }
}
#[derive(Clone)]
pub(in crate::remote) struct Handler {
    raft: openraft::Raft<TypeConfig>,
    state: Arc<StateMachine>,
    identity: RemoteIdentity,
    shard: u64,
    sessions: Arc<Sessions>,
    query_sessions: Arc<distributed::Sessions>,
    deadline: Duration,
}
impl Handler {
    pub(in crate::remote) async fn execute(
        &self,
        context: Option<wire::Context>,
        operation: Operation,
    ) -> Result<(Reply, Instant), Status> {
        self.execute_raw(context, operation).await.map_err(status)
    }
    pub(in crate::remote) async fn execute_raw(
        &self,
        context: Option<wire::Context>,
        operation: Operation,
    ) -> Result<(Reply, Instant), RemoteError> {
        self.execute_until(context, operation, Instant::now() + self.deadline)
            .await
    }
    pub(in crate::remote) async fn execute_until(
        &self,
        context: Option<wire::Context>,
        operation: Operation,
        admitted_end: Instant,
    ) -> Result<(Reply, Instant), RemoteError> {
        let context = context.ok_or(RemoteError::InvalidArgument)?;
        let identity = context.identity.ok_or(RemoteError::InvalidArgument)?;
        if context.version != VERSION {
            return Err(RemoteError::Protocol);
        }
        if context.timeout_millis == 0 || context.timeout_millis > 60_000 {
            return Err(RemoteError::InvalidArgument);
        }
        let end = admitted_end.min(
            Instant::now()
                + self
                    .deadline
                    .min(Duration::from_millis(context.timeout_millis)),
        );
        if Instant::now() >= end {
            return Err(RemoteError::Timeout { request_id: None });
        }
        tokio::time::timeout_at(
            end,
            super::super::execute(
                self.raft.clone(),
                self.state.clone(),
                self.identity,
                self.shard,
                Request {
                    version: VERSION,
                    identity: Some(identity.into()),
                    operation,
                },
                (self.sessions.clone(), self.query_sessions.clone()),
                end,
            ),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
        .map(|reply| (reply, end))
    }
}
#[derive(Clone, Copy)]
struct ResponseDeadline(Instant);
#[derive(Clone, Copy)]
pub(in crate::remote) struct RequestDeadline(pub Instant);
pub(in crate::remote) fn response<T>(value: T, end: Instant) -> tonic::Response<T> {
    let mut response = tonic::Response::new(value);
    response.extensions_mut().insert(ResponseDeadline(end));
    response
}
#[tonic::async_trait]
impl SnapshotService for Handler {
    async fn open(
        &self,
        request: tonic::Request<wire::OpenRequest>,
    ) -> Result<tonic::Response<wire::Descriptor>, Status> {
        let request = request.into_inner();
        match self
            .execute(
                request.context,
                Operation::OpenSnapshot {
                    shard: request.shard,
                },
            )
            .await?
        {
            (Reply::OpenSnapshot(d), end) => Ok(response(d.into(), end)),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn page(
        &self,
        request: tonic::Request<wire::PageRequest>,
    ) -> Result<tonic::Response<wire::PageResponse>, Status> {
        let request = request.into_inner();
        let descriptor = request
            .descriptor
            .ok_or_else(|| status(RemoteError::InvalidArgument))?
            .try_into()
            .map_err(status)?;
        let cursor = request
            .cursor
            .map(TryInto::try_into)
            .transpose()
            .map_err(status)?;
        match self
            .execute(
                request.context,
                Operation::SnapshotPage { descriptor, cursor },
            )
            .await?
        {
            (Reply::SnapshotPage(p), end) => Ok(response(
                wire::PageResponse {
                    descriptor: Some(p.descriptor.into()),
                    entries: p
                        .entries
                        .into_iter()
                        .map(|(key, value)| wire::Entry { key, value })
                        .collect(),
                    next: p.next.map(Into::into),
                },
                end,
            )),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn close(
        &self,
        request: tonic::Request<wire::CloseRequest>,
    ) -> Result<tonic::Response<wire::Empty>, Status> {
        let request = request.into_inner();
        let descriptor = request
            .descriptor
            .ok_or_else(|| status(RemoteError::InvalidArgument))?
            .try_into()
            .map_err(status)?;
        match self
            .execute(request.context, Operation::CloseSnapshot { descriptor })
            .await?
        {
            (Reply::CloseSnapshot, end) => Ok(response(wire::Empty {}, end)),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
}
// Admission spans protobuf decoding, execution, encoding and body consumption.
// Never free a budget merely because a Page response has been constructed.
#[derive(Clone)]
pub(in crate::remote) struct AdmittedService<S> {
    pub(in crate::remote) inner: S,
    pub(in crate::remote) admission: Arc<Semaphore>,
    pub(in crate::remote) deadline: Duration,
}
impl<S: tonic::server::NamedService> tonic::server::NamedService for AdmittedService<S> {
    const NAME: &'static str = S::NAME;
}
impl<B, S> Service<http::Request<B>> for AdmittedService<S>
where
    S: Service<
            http::Request<B>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let Ok(permit) = self.admission.clone().try_acquire_owned() else {
            return Box::pin(async { Ok(status(RemoteError::ResourceExhausted).into_http()) });
        };
        let budget = request
            .headers()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
            .and_then(grpc_timeout)
            .map_or(self.deadline, |d| d.min(self.deadline));
        let end = Instant::now() + budget;
        request.extensions_mut().insert(RequestDeadline(end));
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await?;
            let end = response
                .extensions()
                .get::<ResponseDeadline>()
                .map_or(end, |operation| end.min(operation.0));
            Ok(response.map(|body| {
                tonic::body::Body::new(AdmittedBody {
                    body,
                    _permit: permit,
                    deadline: Box::pin(tokio::time::sleep_until(end)),
                })
            }))
        })
    }
}
// Standard gRPC timeout grammar (at most eight digits plus one unit).
// Tonic enforces it while producing the response; retain it for the body too.
fn grpc_timeout(value: &str) -> Option<Duration> {
    if !value.is_ascii() || value.len() < 2 || value.len() > 9 {
        return None;
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = digits.parse::<u64>().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n * 3600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}
struct AdmittedBody {
    body: tonic::body::Body,
    _permit: tokio::sync::OwnedSemaphorePermit,
    deadline: Pin<Box<tokio::time::Sleep>>,
}
impl http_body::Body for AdmittedBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        if self.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(Status::deadline_exceeded("response deadline"))));
        }
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

// Bound accepted HTTP/2 connections as well as admitted operations. The permit
// lives with the socket, not merely with the accept future or request handler.
pub(in crate::remote) struct LimitedIo {
    pub(in crate::remote) socket: TcpStream,
    pub(in crate::remote) _permit: tokio::sync::OwnedSemaphorePermit,
    pub(in crate::remote) cancelled: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}
impl tonic::transport::server::Connected for LimitedIo {
    type ConnectInfo = <TcpStream as tonic::transport::server::Connected>::ConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.socket.connect_info()
    }
}
impl AsyncRead for LimitedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}
impl AsyncWrite for LimitedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
/// Optional separate endpoint; it shares Raft/state, not a JSON proxy connection.
/// Use on a trusted isolated network only. Identity matching is not authentication.
pub struct GrpcSnapshotService {
    address: SocketAddr,
    sessions: Arc<Sessions>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
    hard_stop: CancellationToken,
    deadline: Duration,
}
impl GrpcSnapshotService {
    pub async fn start(source: &GraphService, bind: SocketAddr) -> Result<Self, RemoteError> {
        let deadline = source.deadline;
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let address = listener
            .local_addr()
            .map_err(|_| RemoteError::Unavailable)?;
        // One session/byte budget across both protocols during migration.
        let sessions = source.sessions.clone();
        let handler = Handler {
            raft: source.raft.clone(),
            state: source.state.clone(),
            identity: source.identity,
            shard: source.shard,
            sessions: sessions.clone(),
            query_sessions: source.query_sessions.clone(),
            deadline,
        };
        let admission = source.admission.clone();
        let hard_stop = CancellationToken::new();
        let connection_stop = hard_stop.clone();
        let (stop, mut stopped) = watch::channel(false);
        let cleanup = sessions.clone();
        let query_cleanup = source.query_sessions.clone();
        let task = tokio::spawn(async move {
            let shutdown = async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tokio::select! { _ = stopped.changed() => break, _ = interval.tick() => { cleanup.expire(); query_cleanup.expire(); } }
                }
            };
            let connections = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).filter_map(
                move |accepted| match accepted {
                    Err(error) => Some(Err(error)),
                    Ok(socket) => connections.clone().try_acquire_owned().ok().map(|permit| {
                        Ok(LimitedIo {
                            socket,
                            _permit: permit,
                            cancelled: Box::pin(connection_stop.clone().cancelled_owned()),
                        })
                    }),
                },
            );
            let operations_service = super::super::operations_grpc::service(handler.clone());
            let fixed_service = distributed::grpc::fixed_service(handler.clone());
            let query_service = distributed::grpc::service(handler.clone());
            let service = SnapshotServiceServer::new(handler)
                .max_decoding_message_size(REQUEST_LIMIT)
                .max_encoding_message_size(RESPONSE_LIMIT);
            let _ = tonic::transport::Server::builder()
                .timeout(deadline)
                .max_concurrent_streams(Some(MAX_IN_FLIGHT as u32))
                .add_service(AdmittedService {
                    inner: service,
                    admission: admission.clone(),
                    deadline,
                })
                .add_service(AdmittedService {
                    inner: query_service,
                    admission: admission.clone(),
                    deadline,
                })
                .add_service(AdmittedService {
                    inner: operations_service,
                    admission: admission.clone(),
                    deadline,
                })
                .add_service(AdmittedService {
                    inner: fixed_service,
                    admission,
                    deadline,
                })
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await;
        });
        Ok(Self {
            address,
            sessions,
            stop,
            task: Some(task),
            hard_stop,
            deadline,
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub fn snapshot_stats(&self) -> SnapshotStats {
        self.sessions.stats()
    }
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(mut task) = self.task.take() {
            if tokio::time::timeout(self.deadline, &mut task)
                .await
                .is_err()
            {
                // Flow-controlled peers cannot indefinitely retain the service.
                self.hard_stop.cancel();
                if tokio::time::timeout(self.deadline, &mut task)
                    .await
                    .is_err()
                {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
    }
}
impl Drop for GrpcSnapshotService {
    fn drop(&mut self) {
        self.hard_stop.cancel();
        let _ = self.stop.send(true);
    }
}

/// Clones multiplex a single HTTP/2 Channel. No automatic application retry or
/// leader forwarding; caller retains the channel and explicit endpoint identity.
#[derive(Clone)]
pub struct SnapshotClient {
    client: SnapshotServiceClient<Channel>,
    identity: RemoteIdentity,
}
impl SnapshotClient {
    pub async fn connect(
        address: SocketAddr,
        identity: RemoteIdentity,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        let end = checked_deadline(deadline, None)?;
        let endpoint = Channel::from_shared(format!("http://{address}"))
            .map_err(|_| RemoteError::InvalidArgument)?
            .connect_timeout(deadline)
            .buffer_size(MAX_IN_FLIGHT)
            .concurrency_limit(MAX_IN_FLIGHT);
        let channel = tokio::time::timeout_at(end, endpoint.connect())
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(|_| RemoteError::Unavailable)?;
        Ok(Self {
            client: SnapshotServiceClient::new(channel)
                .max_decoding_message_size(RESPONSE_LIMIT)
                .max_encoding_message_size(REQUEST_LIMIT),
            identity,
        })
    }
    fn context(&self, end: Instant) -> Result<wire::Context, RemoteError> {
        check_time(end)?;
        let millis = end.saturating_duration_since(Instant::now()).as_millis();
        if millis == 0 {
            return Err(RemoteError::Timeout { request_id: None });
        }
        Ok(wire::Context {
            version: VERSION,
            identity: Some(self.identity.into()),
            timeout_millis: millis as u64,
        })
    }
    fn request<T>(message: T, end: Instant) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(end.saturating_duration_since(Instant::now()));
        request
    }
    pub async fn open(
        &self,
        shard: u64,
        deadline: Duration,
    ) -> Result<SnapshotDescriptor, RemoteError> {
        let end = checked_deadline(deadline, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let request = Self::request(
            wire::OpenRequest {
                context: Some(self.context(end)?),
                shard,
            },
            end,
        );
        let response = tokio::time::timeout_at(end, self.client.clone().open(request))
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(remote_status)?;
        let descriptor = SnapshotDescriptor::try_from(response.into_inner())
            .map_err(|_| RemoteError::Protocol)?;
        validate_descriptor(&descriptor, Some(self.identity), shard)?;
        Ok(descriptor)
    }
    pub async fn page(
        &self,
        descriptor: SnapshotDescriptor,
        cursor: Option<SnapshotCursor>,
        deadline: Duration,
    ) -> Result<SnapshotPage, RemoteError> {
        let end = checked_deadline(deadline, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        validate_descriptor(&descriptor, Some(self.identity), descriptor.shard)?;
        let request = Self::request(
            wire::PageRequest {
                context: Some(self.context(end)?),
                descriptor: Some(descriptor.clone().into()),
                cursor: cursor.clone().map(Into::into),
            },
            end,
        );
        let response = tokio::time::timeout_at(end, self.client.clone().page(request))
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(remote_status)?
            .into_inner();
        let page = SnapshotPage {
            descriptor: response
                .descriptor
                .ok_or(RemoteError::Protocol)?
                .try_into()
                .map_err(|_| RemoteError::Protocol)?,
            entries: response
                .entries
                .into_iter()
                .map(|e| (e.key, e.value))
                .collect(),
            next: response
                .next
                .map(TryInto::try_into)
                .transpose()
                .map_err(|_| RemoteError::Protocol)?,
        };
        validate_page(&descriptor, cursor.as_ref(), &page)?;
        Ok(page)
    }
    pub async fn close(
        &self,
        descriptor: SnapshotDescriptor,
        deadline: Duration,
    ) -> Result<(), RemoteError> {
        let end = checked_deadline(deadline, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let request = Self::request(
            wire::CloseRequest {
                context: Some(self.context(end)?),
                descriptor: Some(descriptor.into()),
            },
            end,
        );
        tokio::time::timeout_at(end, self.client.clone().close(request))
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(remote_status)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor() -> SnapshotDescriptor {
        SnapshotDescriptor {
            version: 1,
            identity: RemoteIdentity { group: 9, node: 2 },
            incarnation: [7; 16],
            session: [8; 16],
            shard: 3,
            epoch: 4,
            sequence: 5,
            rows: 6,
            bytes: 7,
        }
    }
    #[test]
    fn standard_timeout_units_are_bounded_and_not_reset() {
        assert_eq!(grpc_timeout("12m"), Some(Duration::from_millis(12)));
        assert_eq!(
            grpc_timeout("99999999n"),
            Some(Duration::from_nanos(99_999_999))
        );
        assert_eq!(grpc_timeout("0S"), Some(Duration::ZERO));
        for bad in ["", "1", "123456789S", "-1S", "1x", "é"] {
            assert_eq!(grpc_timeout(bad), None);
        }
    }
    #[test]
    fn typed_descriptor_and_cursor_round_trip_reject_bad_tokens() {
        let expected = descriptor();
        let bytes = wire::Descriptor::from(expected.clone()).encode_to_vec();
        assert_eq!(
            SnapshotDescriptor::try_from(wire::Descriptor::decode(bytes.as_slice()).unwrap())
                .unwrap(),
            expected
        );
        let cursor = SnapshotCursor {
            incarnation: [1; 16],
            session: [2; 16],
            position: 5,
            token: [3; 16],
        };
        assert_eq!(
            SnapshotCursor::try_from(wire::Cursor::from(cursor.clone())).unwrap(),
            cursor
        );
        let mut bad = wire::Descriptor::from(expected);
        bad.session.push(9);
        assert!(matches!(
            SnapshotDescriptor::try_from(bad),
            Err(RemoteError::InvalidArgument)
        ));
        let mut bad = wire::Cursor::from(cursor);
        bad.token.clear();
        assert!(matches!(
            SnapshotCursor::try_from(bad),
            Err(RemoteError::InvalidArgument)
        ));
    }
    #[test]
    fn error_details_not_human_messages_determine_semantics() {
        let hint = status(RemoteError::NotLeader { leader: Some(3) });
        let translated = Status::with_details(
            hint.code(),
            "different human wording",
            hint.details().to_vec().into(),
        );
        assert!(matches!(
            remote_status(translated),
            RemoteError::NotLeader { leader: Some(3) }
        ));
        assert!(matches!(
            remote_status(Status::failed_precondition("not_leader")),
            RemoteError::Protocol
        ));
        assert!(matches!(
            remote_status(status(RemoteError::SessionUnavailable)),
            RemoteError::SessionUnavailable
        ));
        assert!(matches!(
            remote_status(status(RemoteError::Application(D1Error::NotActivated))),
            RemoteError::Application(D1Error::NotActivated)
        ));
        assert!(matches!(
            remote_status(status(RemoteError::Application(D1Error::Fenced {
                requested: 3,
                current: 4
            }))),
            RemoteError::Application(D1Error::Fenced {
                requested: 3,
                current: 4
            })
        ));
        assert!(matches!(
            remote_status(status(RemoteError::ResourceExhausted)),
            RemoteError::ResourceExhausted
        ));
    }
    #[test]
    fn page_wire_budget_includes_per_entry_protobuf_overhead() {
        let entries = (0..MAX_PAGE_ROWS)
            .map(|i| wire::Entry {
                key: (i as u64).to_be_bytes().to_vec(),
                value: vec![0; MAX_PAGE_BYTES / MAX_PAGE_ROWS - 8],
            })
            .collect();
        let page = wire::PageResponse {
            descriptor: Some(descriptor().into()),
            entries,
            next: None,
        };
        assert!(page.encoded_len() <= RESPONSE_LIMIT);
        const { assert!(RESPONSE_LIMIT <= MAX_FRAME_BYTES) };
    }
}
