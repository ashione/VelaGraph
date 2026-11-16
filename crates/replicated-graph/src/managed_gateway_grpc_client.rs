//! Domain client for the typed managed public services. No JSON tunnel, retry,
//! peer substitution or new execution engine. Every response is correlated.
use super::grpc_codec as codec;
use super::public_proto as pb;
use super::*;
use prost::Message;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::CancellationToken;
use tonic::codegen::{http, Service};
use tonic::transport::Channel;

// Closing a client must stop the actual socket, not merely drop its cached
// Channel handle: Hyper can still own queued request bodies and their leases.
struct CloseableIo {
    stream: Option<tokio::net::TcpStream>,
    stop: CancellationToken,
    read_stop: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    write_stop: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}
impl CloseableIo {
    fn new(stream: tokio::net::TcpStream, stop: CancellationToken) -> Self {
        Self {
            stream: Some(stream),
            read_stop: Box::pin(stop.clone().cancelled_owned()),
            write_stop: Box::pin(stop.clone().cancelled_owned()),
            stop,
        }
    }
    fn check_open(&mut self, cx: &mut TaskContext<'_>, read: bool) -> std::io::Result<()> {
        let cancelled = if read {
            self.read_stop.as_mut().poll(cx).is_ready()
        } else {
            self.write_stop.as_mut().poll(cx).is_ready()
        };
        if cancelled || self.stop.is_cancelled() || self.stream.is_none() {
            self.stream.take();
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "gateway client closed",
            ));
        }
        Ok(())
    }
}
impl AsyncRead for CloseableIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Err(error) = self.check_open(cx, true) {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream.as_mut().expect("checked open")).poll_read(cx, buf)
    }
}
impl AsyncWrite for CloseableIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Err(error) = self.check_open(cx, false) {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream.as_mut().expect("checked open")).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        if let Err(error) = self.check_open(cx, false) {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream.as_mut().expect("checked open")).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Err(error) = self.check_open(cx, false) {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream.as_mut().expect("checked open")).poll_shutdown(cx)
    }
}
#[derive(Clone)]
struct ClientConnector {
    address: SocketAddr,
    stop: CancellationToken,
}
impl Service<http::Uri> for ClientConnector {
    type Response = hyper_util::rt::TokioIo<CloseableIo>;
    type Error = std::io::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = std::io::Result<Self::Response>> + Send>>;
    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        if self.stop.is_cancelled() {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "gateway client closed",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn call(&mut self, _uri: http::Uri) -> Self::Future {
        let address = self.address;
        let stop = self.stop.clone();
        Box::pin(async move {
            let stream = tokio::select! {
                biased;
                _ = stop.cancelled() => return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "gateway client closed")),
                stream = tokio::net::TcpStream::connect(address) => stream?,
            };
            stream.set_nodelay(true)?;
            if stop.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "gateway client closed",
                ));
            }
            Ok(hyper_util::rt::TokioIo::new(CloseableIo::new(stream, stop)))
        })
    }
}

// The shared transport can retain a pending HTTP/2 stream after its caller
// drops the response future. Guard the outbound body at the point the transport
// consumes it, not only at enqueue. Bytes already consumed by hyper/h2 cannot
// be recalled: a timed-out write remains Unknown, never a rollback promise.
struct OutboundLifetime {
    cancelled: Arc<AtomicBool>,
    permit: Arc<tokio::sync::SemaphorePermit<'static>>,
}
impl Drop for OutboundLifetime {
    fn drop(&mut self) {
        self.cancelled.store(true, AtomicOrdering::Release);
    }
}
#[derive(Clone)]
struct GuardedChannel {
    channel: Channel,
    permit: Arc<tokio::sync::SemaphorePermit<'static>>,
    end: Instant,
    cancelled: Arc<AtomicBool>,
}
impl Service<http::Request<tonic::body::Body>> for GuardedChannel {
    type Response = <Channel as Service<http::Request<tonic::body::Body>>>::Response;
    type Error = <Channel as Service<http::Request<tonic::body::Body>>>::Error;
    type Future = <Channel as Service<http::Request<tonic::body::Body>>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.channel.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let end = self.end;
        let cancelled = self.cancelled.clone();
        let permit = self.permit.clone();
        self.channel.call(request.map(|inner| {
            tonic::body::Body::new(GuardedBody {
                _permit: permit,
                inner,
                end,
                cancelled,
            })
        }))
    }
}
struct GuardedBody {
    // Caller timeout must not release admission while a buffered transport
    // request still retains its outbound payload. This lease is shared, not
    // another semaphore acquisition; it lasts until the body is dropped.
    _permit: Arc<tokio::sync::SemaphorePermit<'static>>,
    inner: tonic::body::Body,
    end: Instant,
    cancelled: Arc<AtomicBool>,
}
impl http_body::Body for GuardedBody {
    type Data = <tonic::body::Body as http_body::Body>::Data;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.cancelled.load(AtomicOrdering::Acquire) || Instant::now() >= self.end {
            // An error resets the stream; successful EOF could instead make a
            // partially consumed request appear complete to the receiver.
            return Poll::Ready(Some(Err(tonic::Status::cancelled(
                "outbound request expired",
            ))));
        }
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

pub(super) use codec::encode_binding;
fn required<T>(value: Option<T>) -> Result<T> {
    codec::required(value)
}
fn remaining(end: Instant) -> Result<Duration> {
    let value = end.saturating_duration_since(Instant::now());
    if value.as_millis() == 0 {
        Err(Error::Unknown)
    } else {
        Ok(value)
    }
}
fn verify_identity(actual: Option<&pb::CallIdentity>, expected: &pb::CallIdentity) -> Result<()> {
    let actual = actual.ok_or(Error::Identity)?;
    if actual.request_id != expected.request_id {
        return Err(Error::Identity);
    }
    let actual = codec::decode_binding(required(actual.binding.clone())?)?;
    let expected = codec::decode_binding(required(expected.binding.clone())?)?;
    if actual != expected {
        return Err(Error::Identity);
    }
    Ok(())
}
fn remote_error(status: tonic::Status, expected: &pb::CallIdentity) -> Error {
    // HTTP/2 transport failures and pre-decode rejection cannot prove a write
    // outcome. Do not manufacture an Aborted result from a lost ACK.
    if status.details().is_empty() {
        // These retain transport decode/admission classification. Like the old
        // response-frame capacity error, neither proves a mutation rolled back.
        return match status.code() {
            tonic::Code::ResourceExhausted | tonic::Code::OutOfRange => Error::Capacity,
            tonic::Code::InvalidArgument => Error::Protocol,
            _ => Error::Unknown,
        };
    }
    let Ok(detail) = codec::decode_error_detail(&status) else {
        return Error::Protocol;
    };
    if let Err(error) = verify_identity(detail.identity.as_ref(), expected) {
        return error;
    }
    use pb::error_detail::Reason as R;
    let error = match R::try_from(detail.reason) {
        Ok(R::InvalidArgument) => Error::Protocol,
        Ok(R::IdentityMismatch) => Error::Identity,
        Ok(R::ResourceExhausted) => Error::Capacity,
        Ok(R::RejectedOutcomeUncertain) => Error::Rejected,
        Ok(R::PreconditionFailed) => Error::PreconditionFailed,
        Ok(R::OutcomeUnknown) => Error::Unknown,
        Ok(R::SessionUnavailable) => Error::SessionUnavailable,
        Ok(R::JobBusy) => Error::JobBusy,
        Ok(R::QuerySyntax) => Error::Query(QueryFailure::Parse),
        Ok(R::QueryUnsupported) => Error::Query(QueryFailure::Unsupported),
        Ok(R::QueryUnsupportedScope) => Error::Query(QueryFailure::UnsupportedScope),
        Ok(R::QueryInvalidArgument) => Error::Query(QueryFailure::InvalidArgument),
        Ok(R::QueryResourceExhausted) => Error::Query(QueryFailure::ResourceExhausted),
        Ok(R::QueryDeadline) => Error::Query(QueryFailure::Timeout),
        Ok(R::QueryIncompleteOrCorrupt) => Error::Query(QueryFailure::IncompleteOrCorrupt),
        Ok(R::QueryUnavailable) => Error::Query(QueryFailure::Unavailable),
        _ => return Error::Protocol,
    };
    if codec::error(error.clone()).code() != status.code() {
        return Error::Protocol;
    }
    error
}

/// An owned, clonable public gRPC client pinned to one endpoint and binding.
///
/// Clones share one lazily connected channel. Connection setup, queueing and the
/// RPC all consume the caller's original deadline and global client admission.
/// No RPC is retried and no alternate endpoint is selected. Transport reconnect
/// may establish a new socket to the same address; every response still verifies
/// the binding/correlation identity, and old session/job tokens remain tied to
/// their server incarnation. This is not authenticated discovery or failover.
#[derive(Clone)]
pub struct GatewayClient {
    inner: Arc<ClientState>,
}
struct ClientState {
    address: SocketAddr,
    binding: Binding,
    channel: tokio::sync::OnceCell<Channel>,
    closed: CancellationToken,
}
impl GatewayClient {
    /// Construct a client without network I/O. A Tokio runtime is needed only
    /// when a request is performed. Dropping the final clone releases the cached
    /// channel; no process-global endpoint registry is retained.
    pub fn new(address: SocketAddr, binding: Binding) -> Self {
        Self {
            inner: Arc::new(ClientState {
                address,
                binding,
                channel: tokio::sync::OnceCell::new(),
                closed: CancellationToken::new(),
            }),
        }
    }

    /// Permanently stop this client and all its clones. Socket/queue cleanup
    /// runs on the owning Tokio runtime; this signal does not wait for cleanup.
    /// Already-dispatched writes remain unknown, not rolled back.
    pub fn close(&self) {
        self.inner.closed.cancel();
    }

    /// Whether explicit close has been requested for this shared client.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.is_cancelled()
    }

    /// Perform one operation without retry. Unknown write outcomes require
    /// explicit original-ID recovery, never automatic mutation replay.
    pub async fn request(&self, operation: Operation, deadline: Duration) -> Result<Reply> {
        request(self, operation, deadline).await
    }
}

async fn request(
    client: &GatewayClient,
    operation: Operation,
    deadline: Duration,
) -> Result<Reply> {
    check_deadline(deadline)?;
    if client.is_closed() {
        return Err(Error::Unknown);
    }
    // Admission precedes construction and connection, including cold requests.
    let permit = Arc::new(
        CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| Error::Capacity)?,
    );
    let end = Instant::now() + deadline;
    let outbound = OutboundLifetime {
        permit,
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    let binding = client.inner.binding.clone();
    let request_id = random()?;
    let expected = pb::CallIdentity {
        request_id: request_id.to_vec(),
        binding: Some(encode_binding(&binding)),
    };
    let original = Request {
        version: VERSION,
        request_id,
        binding,
        operation,
    };
    tokio::select! {
        biased;
        _ = client.inner.closed.cancelled() => Err(Error::Unknown),
        result = tokio::time::timeout_at(tokio::time::Instant::from_std(end), async {
        // Preserve the original logical request envelope budget before opening
        // a socket. Protobuf compression is not permission to expand admission.
        encode(&original, MAX_REQUEST_BYTES)?;
        // OnceCell serializes cold connection establishment among clones without
        // holding a synchronous mutex across await. Failure/cancellation leaves
        // it empty, so a later explicit operation may establish the connection.
        // The request timeout includes waiting for this single-flight setup.
        let channel = client
            .inner
            .channel
            .get_or_try_init(|| async {
                let endpoint = Channel::from_shared(format!("http://{}", client.inner.address))
                    .map_err(|_| Error::Protocol)?
                    // Do not freeze the first caller's shorter budget into all
                    // later reconnects. The outer absolute deadline bounds each
                    // call; this separately bounds transport connection work.
                    .connect_timeout(MAX_DEADLINE)
                    .buffer_size(MAX_CONNECTIONS)
                    .concurrency_limit(MAX_CONNECTIONS);
                Box::pin(endpoint.connect_with_connector(ClientConnector { address: client.inner.address, stop: client.inner.closed.clone() }))
                    .await
                    .map_err(|_| Error::Unknown)
            })
            .await?
            .clone();
        let channel = GuardedChannel {
            permit: outbound.permit.clone(),
            channel,
            end,
            cancelled: outbound.cancelled.clone(),
        };
        let mut writes = pb::write_service_client::WriteServiceClient::new(channel.clone())
            .max_encoding_message_size(MAX_REQUEST_BYTES)
            .max_decoding_message_size(MAX_RESPONSE_BYTES);
        let mut queries = pb::query_service_client::QueryServiceClient::new(channel.clone())
            .max_encoding_message_size(MAX_REQUEST_BYTES)
            .max_decoding_message_size(MAX_RESPONSE_BYTES);
        let mut fixed =
            pb::fixed_session_service_client::FixedSessionServiceClient::new(channel.clone())
                .max_encoding_message_size(MAX_REQUEST_BYTES)
                .max_decoding_message_size(MAX_RESPONSE_BYTES);
        let mut jobs =
            pb::managed_query_job_service_client::ManagedQueryJobServiceClient::new(channel)
                .max_encoding_message_size(MAX_REQUEST_BYTES)
                .max_decoding_message_size(MAX_RESPONSE_BYTES);
        let context = || -> Result<Option<pb::Context>> {
            Ok(Some(pb::Context {
                version: 2,
                timeout_millis: remaining(end)?.as_millis().min(60_000) as u64,
                identity: Some(expected.clone()),
            }))
        };
        macro_rules! call {
            ($client:ident, $method:ident, $message:expr) => {{
                let message = $message;
                if message.encoded_len() > MAX_REQUEST_BYTES {
                    return Err(Error::Capacity);
                }
                validate_message(&message)?;
                let mut request = tonic::Request::new(message);
                request.set_timeout(remaining(end)?);
                // Generated unary futures include the HTTP/2 transport stack.
                // Box at the leaf, rather than boxing the already-large outer
                // request future after its state has propagated to callers.
                let response = Box::pin($client.$method(request))
                    .await
                    .map_err(|e| remote_error(e, &expected))?
                    .into_inner();
                verify_identity(response.identity.as_ref(), &expected)?;
                response
            }};
        }
        let reply = match &original.operation {
            Operation::Submit(write) => {
                decode_write(call!(writes, submit, encode_write(context()?, write)?))?
            }
            Operation::Status(write) => {
                decode_write(call!(writes, status, encode_write(context()?, write)?))?
            }
            Operation::Abort(write) => {
                decode_write(call!(writes, abort, encode_write(context()?, write)?))?
            }
            Operation::OpenSession {
                version,
                ttl_millis,
            } => {
                let response = call!(
                    fixed,
                    open,
                    pb::FixedOpenRequest {
                        context: context()?,
                        version: *version,
                        ttl_millis: *ttl_millis
                    }
                );
                Reply::OpenSession(codec::fixed_session(required(response.session)?)?)
            }
            Operation::Read { session, query } => {
                let response = call!(
                    fixed,
                    read,
                    pb::FixedQueryRequest {
                        context: context()?,
                        session: Some(codec::out_fixed_session(session)),
                        query: Some(codec::out_query(query))
                    }
                );
                Reply::Read {
                    session: codec::fixed_session(required(response.session)?)?,
                    query: codec::query(required(response.query)?)?,
                    data: decode_data(required(response.data)?)?,
                }
            }
            Operation::Traverse { session, query } => {
                let response = call!(
                    fixed,
                    traverse,
                    pb::FixedQueryRequest {
                        context: context()?,
                        session: Some(codec::out_fixed_session(session)),
                        query: Some(codec::out_query(query))
                    }
                );
                Reply::Traverse {
                    session: codec::fixed_session(required(response.session)?)?,
                    query: codec::query(required(response.query)?)?,
                    data: decode_data(required(response.data)?)?,
                }
            }
            Operation::TraversePage {
                session,
                query,
                cursor,
                page_size,
            } => {
                let response = call!(
                    fixed,
                    page,
                    pb::FixedPageRequest {
                        context: context()?,
                        session: Some(codec::out_fixed_session(session)),
                        query: Some(codec::out_query(query)),
                        cursor: cursor.as_ref().map(codec::out_cursor),
                        page_size: u32::from(*page_size)
                    }
                );
                Reply::TraversePage {
                    session: codec::fixed_session(required(response.session)?)?,
                    query: codec::query(required(response.query)?)?,
                    cursor: decode_cursor(response.cursor)?,
                    page_size: codec::size(response.page_size)?,
                    page: decode_page(required(response.page)?)?,
                }
            }
            Operation::Close { session } => {
                let response = call!(
                    fixed,
                    close,
                    pb::FixedCloseRequest {
                        context: context()?,
                        session: Some(codec::out_fixed_session(session))
                    }
                );
                Reply::Close(codec::fixed_session(required(response.session)?)?)
            }
            Operation::OpenLocalQuery { .. }
            | Operation::OpenLocalQueryAfter { .. }
            | Operation::OpenLocalQueryWithProfile { .. } => {
                let response = call!(queries, open, encode_open(context()?, &original.operation)?);
                let session = codec::session(required(response.session)?)?;
                let query = codec::query(required(response.query)?)?;
                let page_size = codec::size(response.page_size)?;
                let page = decode_page(required(response.page)?)?;
                match response.profile {
                    Some(profile) => Reply::OpenLocalQueryWithProfile {
                        profile: codec::decode_profile(profile)?,
                        session,
                        query,
                        page_size,
                        page,
                    },
                    None => Reply::OpenLocalQuery {
                        session,
                        query,
                        page_size,
                        page,
                    },
                }
            }
            Operation::FetchLocalQuery {
                session,
                query,
                cursor,
                page_size,
            } => {
                let response = call!(
                    queries,
                    fetch,
                    pb::FetchRequest {
                        context: context()?,
                        session: Some(codec::out_session(session)),
                        query: Some(codec::out_query(query)),
                        cursor: cursor.as_ref().map(codec::out_cursor),
                        page_size: u32::from(*page_size)
                    }
                );
                Reply::FetchLocalQuery {
                    session: codec::session(required(response.session)?)?,
                    query: codec::query(required(response.query)?)?,
                    cursor: decode_cursor(response.cursor)?,
                    page_size: codec::size(response.page_size)?,
                    page: decode_page(required(response.page)?)?,
                }
            }
            Operation::CloseLocalQuery { session } => {
                let response = call!(
                    queries,
                    close,
                    pb::CloseRequest {
                        context: context()?,
                        session: Some(codec::out_session(session))
                    }
                );
                Reply::CloseLocalQuery(codec::session(required(response.session)?)?)
            }
            Operation::StartLocalQueryJob {
                operation_id,
                version,
                query,
                page_size,
                policy,
            } => {
                let response = call!(
                    jobs,
                    start,
                    pb::JobStartRequest {
                        context: context()?,
                        operation_id: operation_id.to_vec(),
                        version: *version,
                        query: Some(codec::out_query(query)),
                        page_size: u32::from(*page_size),
                        policy: Some(pb::JobPolicy {
                            idle_millis: policy.idle_millis,
                            execution_millis: policy.execution_millis,
                            lifetime_millis: policy.lifetime_millis
                        })
                    }
                );
                Reply::StartLocalQueryJob {
                    operation_id: codec::token(response.operation_id)?,
                    job: codec::job(required(response.job)?)?,
                }
            }
            Operation::GetLocalQueryJob { job } => {
                let response = call!(
                    jobs,
                    get,
                    pb::JobGetRequest {
                        context: context()?,
                        job: Some(codec::out_job(job))
                    }
                );
                Reply::GetLocalQueryJob {
                    job: codec::job(required(response.job)?)?,
                    status: decode_job_status(required(response.status)?)?,
                }
            }
            Operation::CancelLocalQueryJob { job } => {
                let response = call!(
                    jobs,
                    cancel,
                    pb::JobCancelRequest {
                        context: context()?,
                        job: Some(codec::out_job(job))
                    }
                );
                Reply::CancelLocalQueryJob {
                    job: codec::job(required(response.job)?)?,
                    status: decode_job_status(required(response.status)?)?,
                }
            }
            Operation::RenewLocalQueryJob {
                job,
                expected_generation,
            } => {
                let response = call!(
                    jobs,
                    renew,
                    pb::JobRenewRequest {
                        context: context()?,
                        job: Some(codec::out_job(job)),
                        expected_generation: *expected_generation
                    }
                );
                Reply::RenewLocalQueryJob {
                    job: codec::job(required(response.job)?)?,
                    grant: JobGrant {
                        generation: response.generation,
                        remaining_millis: response.remaining_millis,
                    },
                }
            }
            Operation::FetchLocalQueryJob {
                job,
                query,
                cursor,
                page_size,
            } => {
                let response = call!(
                    jobs,
                    fetch,
                    pb::JobFetchRequest {
                        context: context()?,
                        job: Some(codec::out_job(job)),
                        query: Some(codec::out_query(query)),
                        cursor: cursor.as_ref().map(codec::out_cursor),
                        page_size: u32::from(*page_size)
                    }
                );
                Reply::FetchLocalQueryJob {
                    job: codec::job(required(response.job)?)?,
                    query: codec::query(required(response.query)?)?,
                    cursor: decode_cursor(response.cursor)?,
                    page_size: codec::size(response.page_size)?,
                    version: response.version,
                    page: decode_page(required(response.page)?)?,
                }
            }
        };
        let response = Response {
            version: VERSION,
            request_id,
            binding: original.binding.clone(),
            result: Ok(reply),
        };
        // The typed wire may be more compact; keep the original logical reply
        // budget even for a malicious peer returning highly escaped strings.
        encode(&response, MAX_RESPONSE_BYTES)?;
        remaining(end)?;
        validate_reply(&original, response)
    }) => result.map_err(|_| Error::Unknown)?,
    }
}

fn properties(values: &BTreeMap<String, Value>) -> Result<Vec<pb::Property>> {
    values
        .iter()
        .map(|(key, value)| {
            Ok(pb::Property {
                key: key.clone(),
                value: Some(codec::out_scalar(value)?),
            })
        })
        .collect()
}
fn entity(value: &Entity) -> pb::Entity {
    pb::Entity {
        partition: value.partition,
        id: value.id,
    }
}
fn edge(value: &EdgeData) -> Result<pb::Edge> {
    Ok(pb::Edge {
        id: value.id,
        from: Some(entity(&value.from)),
        to: Some(entity(&value.to)),
        label: value.label.clone(),
        properties: properties(&value.properties)?,
    })
}
fn patch(value: &Patch) -> Result<pb::Patch> {
    Ok(pb::Patch {
        set: properties(&value.set)?,
        remove: value.remove.iter().cloned().collect(),
    })
}
fn conditions(values: &PropertyConditions) -> Result<Vec<pb::PropertyCondition>> {
    values
        .iter()
        .map(|(key, value)| {
            Ok(pb::PropertyCondition {
                key: key.clone(),
                condition: Some(pb::Condition {
                    kind: Some(match value {
                        PropertyCondition::Missing => pb::condition::Kind::Missing(pb::Empty {}),
                        PropertyCondition::Value(value) => {
                            pb::condition::Kind::Value(codec::out_scalar(value)?)
                        }
                    }),
                }),
            })
        })
        .collect()
}
pub(super) fn encode_write(
    context: Option<pb::Context>,
    write: &Write,
) -> Result<pb::WriteRequest> {
    use pb::mutation::Kind;
    // The shared Submit handler owns semantic validation and its timing
    // diagnostics. Typed encoding validates representation, not admission.
    let kind = match &write.mutation {
        Mutation::CreateVertex { partition, vertex } => Kind::CreateVertex(pb::CreateVertex {
            partition: *partition,
            vertex: Some(pb::Vertex {
                id: vertex.id,
                labels: vertex.labels.clone(),
                properties: properties(&vertex.properties)?,
            }),
        }),
        Mutation::CreateEdge { edge: value } => Kind::CreateEdge(pb::CreateEdge {
            edge: Some(edge(value)?),
        }),
        Mutation::PatchVertex {
            vertex,
            patch: value,
        } => Kind::PatchVertex(pb::PatchEntity {
            entity: Some(entity(vertex)),
            patch: Some(patch(value)?),
        }),
        Mutation::PatchEdge { edge, patch: value } => Kind::PatchEdge(pb::PatchEntity {
            entity: Some(entity(edge)),
            patch: Some(patch(value)?),
        }),
        Mutation::PatchVertexIf {
            vertex,
            conditions: value,
            patch: p,
        } => Kind::PatchVertexIf(pb::ConditionalPatch {
            entity: Some(entity(vertex)),
            patch: Some(patch(p)?),
            conditions: conditions(value)?,
        }),
        Mutation::PatchEdgeIf {
            edge,
            conditions: value,
            patch: p,
        } => Kind::PatchEdgeIf(pb::ConditionalPatch {
            entity: Some(entity(edge)),
            patch: Some(patch(p)?),
            conditions: conditions(value)?,
        }),
        Mutation::DeleteEdge { edge } => Kind::DeleteEdge(entity(edge)),
        Mutation::ReplaceEdge {
            edge: value,
            replacement,
        } => Kind::ReplaceEdge(pb::ReplaceEdge {
            edge: Some(entity(value)),
            replacement: Some(edge(replacement)?),
        }),
    };
    Ok(pb::WriteRequest {
        context,
        external_id: write.external_id.to_vec(),
        mutation: Some(pb::Mutation { kind: Some(kind) }),
    })
}
fn encode_open(context: Option<pb::Context>, operation: &Operation) -> Result<pb::OpenRequest> {
    use pb::read_selector::Kind;
    let (read, ttl_millis, query, page_size, profile) = match operation {
        Operation::OpenLocalQuery {
            version,
            ttl_millis,
            query,
            page_size,
        } => (
            version.map_or(Kind::Latest(pb::Empty {}), Kind::Exact),
            *ttl_millis,
            query,
            *page_size,
            None,
        ),
        Operation::OpenLocalQueryAfter {
            minimum_version,
            ttl_millis,
            query,
            page_size,
        } => (
            Kind::Minimum(*minimum_version),
            *ttl_millis,
            query,
            *page_size,
            None,
        ),
        Operation::OpenLocalQueryWithProfile {
            version,
            ttl_millis,
            query,
            page_size,
            profile,
        } => (
            version.map_or(Kind::Latest(pb::Empty {}), Kind::Exact),
            *ttl_millis,
            query,
            *page_size,
            Some(codec::encode_profile(*profile)),
        ),
        _ => return Err(Error::Protocol),
    };
    Ok(pb::OpenRequest {
        context,
        read: Some(pb::ReadSelector { kind: Some(read) }),
        ttl_millis,
        query: Some(codec::out_query(query)),
        page_size: u32::from(page_size),
        profile,
    })
}
fn decode_write(response: pb::WriteResponse) -> Result<Reply> {
    use pb::write_response::Status;
    Ok(Reply::Write {
        external_id: codec::token(response.external_id)?,
        transaction_id: response.transaction_id.map(codec::token).transpose()?,
        status: match required(response.status)? {
            Status::Absent(_) => WriteStatus::Absent,
            Status::Pending(_) => WriteStatus::Pending,
            Status::Committed(value) => WriteStatus::Committed {
                ordinal: value.ordinal,
                visible: value.visible,
            },
            Status::Aborted(value) => WriteStatus::Aborted {
                ordinal: value.ordinal,
                visible: value.visible,
            },
        },
    })
}
fn decode_element(value: pb::QualifiedElement) -> Result<crate::remote::query::QualifiedElement> {
    use crate::remote::query::QualifiedElement as Q;
    Ok(match required(value.kind)? {
        pb::qualified_element::Kind::Vertex(v) => Q::Vertex {
            partition: v.partition,
            id: v.id,
        },
        pb::qualified_element::Kind::Edge(v) => Q::Edge {
            partition: v.partition,
            id: v.id,
        },
    })
}
fn decode_row(value: pb::Row) -> Result<QueryRow> {
    use pb::row::Kind;
    Ok(match required(value.kind)? {
        Kind::Element(value) => QueryRow::Element(decode_element(value)?),
        Kind::Path(value) => QueryRow::Path(
            value
                .elements
                .into_iter()
                .map(decode_element)
                .collect::<Result<_>>()?,
        ),
        Kind::Count(value) => QueryRow::Count(value),
        Kind::Value(value) => QueryRow::Value {
            source: value.source.map(decode_element).transpose()?,
            value: codec::scalar(required(value.value)?)?,
        },
        Kind::PropertyMap(value) => {
            let mut properties = BTreeMap::new();
            for item in value.properties {
                if properties
                    .insert(
                        item.key,
                        item.values
                            .into_iter()
                            .map(codec::scalar)
                            .collect::<Result<_>>()?,
                    )
                    .is_some()
                {
                    return Err(Error::Protocol);
                }
            }
            QueryRow::PropertyMap {
                source: decode_element(required(value.source)?)?,
                properties,
            }
        }
    })
}
fn completeness(value: i32) -> Result<QueryCompleteness> {
    match pb::Completeness::try_from(value).map_err(|_| Error::Protocol)? {
        pb::Completeness::SinglePartition => Ok(QueryCompleteness::SinglePartition),
        pb::Completeness::Unverified => Ok(QueryCompleteness::Unverified),
        _ => Err(Error::Protocol),
    }
}
fn decode_data(value: pb::QueryData) -> Result<QueryData> {
    Ok(QueryData {
        rows: value
            .rows
            .into_iter()
            .map(decode_row)
            .collect::<Result<_>>()?,
        completeness: completeness(value.completeness)?,
    })
}
fn decode_cursor(value: Option<pb::Cursor>) -> Result<Option<PageCursor>> {
    value
        .map(|v| {
            Ok(PageCursor {
                token: codec::token(v.token)?,
            })
        })
        .transpose()
}
fn decode_page(value: pb::Page) -> Result<QueryPage> {
    Ok(QueryPage {
        data: QueryData {
            rows: value
                .rows
                .into_iter()
                .map(decode_row)
                .collect::<Result<_>>()?,
            completeness: completeness(value.completeness)?,
        },
        next: decode_cursor(value.next)?,
        offset: value.offset,
        total: value.total,
    })
}
fn decode_job_failure(value: i32) -> Result<Error> {
    use pb::JobFailure as F;
    Ok(match F::try_from(value).map_err(|_| Error::Protocol)? {
        F::Protocol => Error::Protocol,
        F::Busy => Error::JobBusy,
        F::Identity => Error::Identity,
        F::Capacity => Error::Capacity,
        F::Rejected => Error::Rejected,
        F::PreconditionFailed => Error::PreconditionFailed,
        F::Unknown => Error::Unknown,
        F::SessionUnavailable => Error::SessionUnavailable,
        F::QueryParse => Error::Query(QueryFailure::Parse),
        F::QueryUnsupported => Error::Query(QueryFailure::Unsupported),
        F::QueryInvalidArgument => Error::Query(QueryFailure::InvalidArgument),
        F::QueryResourceExhausted => Error::Query(QueryFailure::ResourceExhausted),
        F::QueryTimeout => Error::Query(QueryFailure::Timeout),
        F::QueryUnsupportedScope => Error::Query(QueryFailure::UnsupportedScope),
        F::QueryIncompleteOrCorrupt => Error::Query(QueryFailure::IncompleteOrCorrupt),
        F::QueryUnavailable => Error::Query(QueryFailure::Unavailable),
        F::Unspecified => return Err(Error::Protocol),
    })
}
fn decode_job_status(value: pb::JobStatus) -> Result<JobStatus> {
    use pb::JobPhase as P;
    let status = JobStatus {
        phase: match P::try_from(value.phase).map_err(|_| Error::Protocol)? {
            P::Capturing => JobPhase::Capturing,
            P::Running => JobPhase::Running,
            P::Sealing => JobPhase::Sealing,
            P::Ready => JobPhase::Ready,
            P::Cancelled => JobPhase::Cancelled,
            P::Expired => JobPhase::Expired,
            P::Failed => JobPhase::Failed,
            P::Unspecified => return Err(Error::Protocol),
        },
        version: value.version,
        total: value.total,
        failure: value.failure.map(decode_job_failure).transpose()?,
    };
    if !jobs::valid_status(&status) {
        return Err(Error::Protocol);
    }
    Ok(status)
}

fn validate_message<M: Message + codec::bounded::BoundedMessage>(message: &M) -> Result<()> {
    codec::bounded::validate_bytes::<M>(&message.encode_to_vec()).map_err(|_| Error::Protocol)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestBody {
        pending: Arc<AtomicBool>,
        frames: std::collections::VecDeque<http_body::Frame<axum::body::Bytes>>,
    }
    impl http_body::Body for TestBody {
        type Data = axum::body::Bytes;
        type Error = tonic::Status;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut TaskContext<'_>,
        ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>> {
            if self.pending.load(AtomicOrdering::Acquire) {
                Poll::Pending
            } else {
                Poll::Ready(self.frames.pop_front().map(Ok))
            }
        }
        fn size_hint(&self) -> http_body::SizeHint {
            http_body::SizeHint::with_exact(
                self.frames
                    .iter()
                    .filter_map(|f| f.data_ref())
                    .map(|b| b.len() as u64)
                    .sum(),
            )
        }
        fn is_end_stream(&self) -> bool {
            self.frames.is_empty()
        }
    }
    fn guarded_test_body(pending: bool) -> (GuardedBody, OutboundLifetime, Arc<AtomicBool>) {
        static TEST_ADMISSION: Semaphore = Semaphore::const_new(32);
        let permit = Arc::new(TEST_ADMISSION.try_acquire().unwrap());
        let pending = Arc::new(AtomicBool::new(pending));
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut trailers = http::HeaderMap::new();
        trailers.insert("test-trailer", http::HeaderValue::from_static("ok"));
        let body = TestBody {
            pending: pending.clone(),
            frames: [
                http_body::Frame::data(axum::body::Bytes::from_static(b"payload")),
                http_body::Frame::trailers(trailers),
            ]
            .into(),
        };
        (
            GuardedBody {
                _permit: permit.clone(),
                inner: tonic::body::Body::new(body),
                end: Instant::now() + Duration::from_secs(10),
                cancelled: cancelled.clone(),
            },
            OutboundLifetime { cancelled, permit },
            pending,
        )
    }
    #[test]
    fn outbound_body_preserves_data_trailers_and_hints() {
        use http_body::Body;
        let (mut body, _owner, _) = guarded_test_body(false);
        let mut cx = TaskContext::from_waker(std::task::Waker::noop());
        assert_eq!(body.size_hint().exact(), Some(7));
        assert!(!body.is_end_stream());
        let Poll::Ready(Some(Ok(data))) = Pin::new(&mut body).poll_frame(&mut cx) else {
            panic!("missing data")
        };
        assert_eq!(data.into_data().unwrap(), "payload");
        assert_eq!(body.size_hint().exact(), Some(0));
        assert!(!body.is_end_stream());
        let Poll::Ready(Some(Ok(trailers))) = Pin::new(&mut body).poll_frame(&mut cx) else {
            panic!("missing trailers")
        };
        assert_eq!(trailers.into_trailers().unwrap()["test-trailer"], "ok");
        assert!(body.is_end_stream());
        assert!(matches!(
            Pin::new(&mut body).poll_frame(&mut cx),
            Poll::Ready(None)
        ));
    }
    #[test]
    fn outbound_body_pending_then_expired_rejects_frame() {
        use http_body::Body;
        let (mut body, _owner, pending) = guarded_test_body(true);
        let mut cx = TaskContext::from_waker(std::task::Waker::noop());
        assert!(Pin::new(&mut body).poll_frame(&mut cx).is_pending());
        body.end = Instant::now();
        pending.store(false, AtomicOrdering::Release);
        assert!(matches!(
            Pin::new(&mut body).poll_frame(&mut cx),
            Poll::Ready(Some(Err(_)))
        ));
    }
    #[test]
    fn outbound_body_pending_then_owner_future_dropped_rejects_frame() {
        use http_body::Body;
        let (mut body, owner, pending) = guarded_test_body(true);
        let mut cx = TaskContext::from_waker(std::task::Waker::noop());
        let mut request = Box::pin(async move {
            let _owner = owner;
            std::future::pending::<()>().await;
        });
        assert!(std::future::Future::poll(request.as_mut(), &mut cx).is_pending());
        assert!(Pin::new(&mut body).poll_frame(&mut cx).is_pending());
        drop(request);
        pending.store(false, AtomicOrdering::Release);
        assert!(matches!(
            Pin::new(&mut body).poll_frame(&mut cx),
            Poll::Ready(Some(Err(_)))
        ));
    }

    fn identity() -> pb::CallIdentity {
        pb::CallIdentity {
            request_id: vec![1; 16],
            binding: Some(pb::Binding {
                authority_shard: 1,
                authority_mode: vec![2; 16],
                coordinator_shard: 1,
                coordinator_epoch: 1,
                members: vec![pb::BindingMember {
                    shard: 2,
                    epoch: 1,
                    mode: vec![3; 16],
                }],
                capacity_profile: pb::CapacityProfile::LegacyCountV1 as i32,
                mapping: pb::Mapping::PartitionEqualsShardV1 as i32,
            }),
        }
    }
    #[test]
    fn bound_error_detail_preserves_every_domain_error_and_rejects_uncorrelated_peer() {
        let identity = identity();
        let errors = [
            Error::Protocol,
            Error::Identity,
            Error::Capacity,
            Error::Rejected,
            Error::PreconditionFailed,
            Error::Unknown,
            Error::SessionUnavailable,
            Error::JobBusy,
            Error::Query(QueryFailure::Parse),
            Error::Query(QueryFailure::Unsupported),
            Error::Query(QueryFailure::UnsupportedScope),
            Error::Query(QueryFailure::InvalidArgument),
            Error::Query(QueryFailure::ResourceExhausted),
            Error::Query(QueryFailure::Timeout),
            Error::Query(QueryFailure::IncompleteOrCorrupt),
            Error::Query(QueryFailure::Unavailable),
        ];
        for error in errors {
            let status = codec::identified_error(error.clone(), Some(identity.clone()));
            assert_eq!(remote_error(status, &identity), error);
        }
        assert_eq!(
            remote_error(tonic::Status::unavailable("lost ACK"), &identity),
            Error::Unknown
        );
        assert_eq!(
            remote_error(tonic::Status::resource_exhausted("frame budget"), &identity),
            Error::Capacity
        );
        assert_eq!(
            remote_error(
                tonic::Status::out_of_range("decoded message length"),
                &identity
            ),
            Error::Capacity
        );
        assert_eq!(
            remote_error(codec::error(Error::Rejected), &identity),
            Error::Identity
        );
        // Old raw trailers are not an alternative protocol after standardization.
        let raw = pb::ErrorDetail {
            reason: pb::error_detail::Reason::RejectedOutcomeUncertain as i32,
            identity: Some(identity.clone()),
        }
        .encode_to_vec();
        assert_eq!(
            remote_error(
                tonic::Status::with_details(
                    tonic::Code::Unavailable,
                    "REJECTED_OUTCOME_UNCERTAIN",
                    raw.into(),
                ),
                &identity
            ),
            Error::Protocol
        );
        let mut wrong = identity.clone();
        wrong.request_id[0] ^= 1;
        assert_eq!(
            remote_error(
                codec::identified_error(Error::Rejected, Some(wrong)),
                &identity
            ),
            Error::Identity
        );
        let mut wrong = identity.clone();
        wrong.binding.as_mut().unwrap().authority_mode[0] ^= 1;
        assert_eq!(
            remote_error(
                codec::identified_error(Error::Rejected, Some(wrong)),
                &identity
            ),
            Error::Identity
        );
    }
    #[test]
    fn domain_submit_keeps_zero_id_rejection_in_shared_handler() {
        let write = Write {
            external_id: [0; 16],
            mutation: Mutation::CreateVertex {
                partition: 2,
                vertex: VertexData {
                    id: 1,
                    labels: vec![],
                    properties: BTreeMap::new(),
                },
            },
        };
        let context = Some(pb::Context {
            version: 2,
            timeout_millis: 1000,
            identity: Some(identity()),
        });
        let message = encode_write(context, &write).unwrap();
        assert_eq!(
            codec::submit(message).unwrap(),
            Operation::Submit(write.clone())
        );
        // Existing v1 public DTO validation remains explicit, while v2's
        // shared State rejection retains Attempt/Queue/Canonical diagnostics.
        let message = encode_write(
            Some(pb::Context {
                version: 1,
                timeout_millis: 1000,
                identity: None,
            }),
            &write,
        )
        .unwrap();
        assert_eq!(codec::submit(message), Err(Error::Identity));
    }
    #[test]
    fn typed_query_preserves_official_parser_text_limit_error() {
        let text = format!(
            "g.V(1).values('{}')",
            "x".repeat(velagraph::MAX_GREMLIN_QUERY_BYTES)
        );
        let query = codec::query(pb::Query {
            partition: 2,
            text: text.clone(),
        })
        .unwrap();
        assert_eq!(query.text, text);
        let error = velagraph::ObservedGremlinQuery::parse(&query.text).unwrap_err();
        assert_eq!(
            crate::remote::query::graph_error(error),
            QueryFailure::Parse
        );
    }
    #[test]
    fn explicit_profile_and_zero_version_survive_typed_request() {
        for profile in [
            LocalReadProfile::Parity2MiBV1,
            LocalReadProfile::LocalRead8MiBV1,
        ] {
            let operation = Operation::OpenLocalQueryWithProfile {
                profile,
                version: Some(0),
                ttl_millis: 1000,
                query: Query {
                    partition: 2,
                    text: "g.V(1)".into(),
                },
                page_size: 1,
            };
            let wire = encode_open(
                Some(pb::Context {
                    version: 2,
                    timeout_millis: 1000,
                    identity: Some(identity()),
                }),
                &operation,
            )
            .unwrap();
            assert_eq!(codec::open(wire).unwrap(), operation);
        }
    }
}
