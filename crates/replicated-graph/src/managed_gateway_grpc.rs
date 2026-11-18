//! Typed public managed gRPC adapter. Shares invocation and retention with TCP
//! and HTTP; loopback-only, not an authenticated production service.
use super::*;
use super::{grpc_codec as codec, public_proto as wire};
use prost::Message;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{
    codegen::{http, BoxFuture, Service},
    Status,
};
use wire::{
    fixed_session_service_server::{FixedSessionService, FixedSessionServiceServer},
    graph_service_server::{GraphService, GraphServiceServer},
    managed_query_job_service_server::{ManagedQueryJobService, ManagedQueryJobServiceServer},
    query_service_server::{QueryService, QueryServiceServer},
    readiness_service_server::{ReadinessService, ReadinessServiceServer},
    write_service_server::{WriteService, WriteServiceServer},
};

type Delivery = Arc<Mutex<Option<invocation::Invocation>>>;
#[derive(Clone)]
struct Call {
    started: Instant,
    end: Instant,
    work: Arc<OwnedSemaphorePermit>,
    delivery: Delivery,
    budget: watch::Sender<Instant>,
    _watchdog: Arc<WatchdogGuard>,
}
#[derive(Clone)]
struct ConnectionStop(CancellationToken);
struct WatchdogGuard(CancellationToken);
impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
fn watchdog(
    end: Instant,
    connection: ConnectionStop,
) -> (watch::Sender<Instant>, Arc<WatchdogGuard>) {
    let (budget, mut changed) = watch::channel(end);
    let stop = CancellationToken::new();
    let stopped = stop.clone();
    tokio::spawn(async move {
        let mut end = end;
        loop {
            end = end.min(*changed.borrow_and_update());
            if Instant::now() >= end {
                connection.0.cancel();
                break;
            }
            tokio::select! {
                _ = stopped.cancelled() => break,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(end)) => { connection.0.cancel(); break; },
                value = changed.changed() => { if value.is_err() { break; } },
            }
        }
    });
    (budget, Arc::new(WatchdogGuard(stop)))
}
#[derive(Clone, Copy)]
struct ResponseEnd(Instant);
#[derive(Clone)]
struct GraphMemory(Arc<OwnedSemaphorePermit>);
#[derive(Clone)]
struct Handler {
    state: Arc<State>,
}
fn admitted<T>(request: &tonic::Request<T>) -> std::result::Result<Call, Status> {
    request
        .extensions()
        .get::<Call>()
        .cloned()
        .ok_or_else(|| Status::internal("missing admission"))
}
impl Handler {
    fn prepare(
        &self,
        context: &Option<wire::Context>,
    ) -> std::result::Result<(Duration, Option<wire::CallIdentity>), Status> {
        let budget = codec::context(context).map_err(codec::error)?;
        let Some(identity) = context.as_ref().and_then(|c| c.identity.as_ref()) else {
            return Ok((budget, None));
        };
        let request_id = codec::token::<16>(identity.request_id.clone()).map_err(codec::error)?;
        let response_identity = Some(wire::CallIdentity {
            request_id: request_id.to_vec(),
            binding: Some(codec::encode_binding(&self.state.config.binding)),
        });
        let binding = identity
            .binding
            .clone()
            .ok_or_else(|| codec::identified_error(Error::Protocol, response_identity.clone()))?;
        let binding = codec::decode_binding(binding)
            .map_err(|e| codec::identified_error(e, response_identity.clone()))?;
        if binding != self.state.config.binding {
            return Err(codec::identified_error(Error::Identity, response_identity));
        }
        Ok((budget, response_identity))
    }
    async fn run<T: codec::Identified + 'static>(
        &self,
        call: Call,
        operation: Operation,
        budget: Duration,
        identity: Option<wire::CallIdentity>,
        encode: fn(&Reply) -> Result<T>,
    ) -> std::result::Result<tonic::Response<T>, Status> {
        if self
            .state
            .jobs_stopping
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(codec::identified_error(Error::Rejected, identity));
        }
        let end = invocation::operation_end(&operation, call.end.min(call.started + budget));
        call.budget.send_replace(end);
        let request = Request {
            version: VERSION,
            request_id: match &identity {
                Some(value) => codec::token(value.request_id.clone()).map_err(codec::error)?,
                None => random().map_err(codec::error)?,
            },
            binding: self.state.config.binding.clone(),
            operation,
        };
        let result = tokio::time::timeout_at(tokio::time::Instant::from_std(end), async {
            let invocation =
                invocation::Invocation::begin(self.state.clone(), request, call.work.clone(), end)
                    .await?;
            // The decoder/executor and response encoding use the same admission.
            // A cancelled blocking encoder retains its work and delivery guard.
            let response_identity = identity.clone();
            let (message, invocation) = tokio::task::spawn_blocking(move || {
                invocation.preflight_grpc()?;
                let reply = invocation.response.result.as_ref().map_err(Clone::clone)?;
                let mut message = encode(reply)?;
                *message.identity_mut() = response_identity;
                codec::bounded::validate_bytes::<T>(&message.encode_to_vec())
                    .map_err(|_| Error::Capacity)?;
                if message.encoded_len() > MAX_RESPONSE_BYTES {
                    return Err(Error::Capacity);
                }
                invocation.live()?;
                Ok::<_, Error>((message, invocation))
            })
            .await
            .map_err(|_| Error::Unknown)??;
            invocation.live()?;
            let end = invocation.delivery_end()?;
            call.budget.send_replace(end);
            *call.delivery.lock().unwrap() = Some(invocation);
            let mut response = tonic::Response::new(message);
            response.extensions_mut().insert(ResponseEnd(end));
            Ok::<_, Error>(response)
        })
        .await;
        match result {
            Ok(result) => result.map_err(|e| codec::identified_error(e, identity.clone())),
            Err(_) => Err(codec::identified_error(Error::Unknown, identity)),
        }
    }
}
#[tonic::async_trait]
impl WriteService for Handler {
    async fn submit(
        &self,
        request: tonic::Request<wire::WriteRequest>,
    ) -> std::result::Result<tonic::Response<wire::WriteResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::submit(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_write)
            .await
    }
    async fn status(
        &self,
        request: tonic::Request<wire::WriteRequest>,
    ) -> std::result::Result<tonic::Response<wire::WriteResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::status(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_write)
            .await
    }
    async fn abort(
        &self,
        request: tonic::Request<wire::WriteRequest>,
    ) -> std::result::Result<tonic::Response<wire::WriteResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::abort(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_write)
            .await
    }
}
#[tonic::async_trait]
impl QueryService for Handler {
    async fn open(
        &self,
        request: tonic::Request<wire::OpenRequest>,
    ) -> std::result::Result<tonic::Response<wire::OpenResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::open(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_open)
            .await
    }
    async fn fetch(
        &self,
        request: tonic::Request<wire::FetchRequest>,
    ) -> std::result::Result<tonic::Response<wire::FetchResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::fetch(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_fetch)
            .await
    }
    async fn close(
        &self,
        request: tonic::Request<wire::CloseRequest>,
    ) -> std::result::Result<tonic::Response<wire::CloseResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::close(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_close)
            .await
    }
}

#[tonic::async_trait]
impl FixedSessionService for Handler {
    async fn open(
        &self,
        request: tonic::Request<wire::FixedOpenRequest>,
    ) -> std::result::Result<tonic::Response<wire::FixedOpenResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::fixed_open(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_fixed_open)
            .await
    }
    async fn read(
        &self,
        request: tonic::Request<wire::FixedQueryRequest>,
    ) -> std::result::Result<tonic::Response<wire::FixedQueryResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::fixed_read(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_fixed_query)
            .await
    }
    async fn traverse(
        &self,
        request: tonic::Request<wire::FixedQueryRequest>,
    ) -> std::result::Result<tonic::Response<wire::FixedQueryResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::fixed_traverse(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_fixed_query)
            .await
    }
    async fn page(
        &self,
        request: tonic::Request<wire::FixedPageRequest>,
    ) -> std::result::Result<tonic::Response<wire::FixedPageResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::fixed_page(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_fixed_page)
            .await
    }
    async fn close(
        &self,
        request: tonic::Request<wire::FixedCloseRequest>,
    ) -> std::result::Result<tonic::Response<wire::FixedCloseResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::fixed_close(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_fixed_close)
            .await
    }
}
#[derive(Clone)]
struct Admitted<S> {
    inner: S,
    drain: Arc<Drain>,
    state: Arc<State>,
    deadline: Duration,
}
impl<S: tonic::server::NamedService> tonic::server::NamedService for Admitted<S> {
    const NAME: &'static str = S::NAME;
}
impl<S, B> Service<http::Request<B>> for Admitted<S>
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    S: Service<
            http::Request<B>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        if !self.drain.admitting() {
            return Box::pin(async { Ok(codec::error(Error::Rejected).into_http()) });
        }
        let Ok(permit) = self.state.connections.clone().try_acquire_owned() else {
            return Box::pin(async { Ok(codec::error(Error::Capacity).into_http()) });
        };
        let budget = match request.headers().get("grpc-timeout") {
            None => self.deadline,
            Some(value) => match value.to_str().ok().and_then(grpc_timeout) {
                Some(budget) => budget.min(self.deadline),
                None => return Box::pin(async { Ok(codec::error(Error::Protocol).into_http()) }),
            },
        };
        if request.headers().get_all("grpc-timeout").iter().count() > 1 {
            return Box::pin(async { Ok(codec::error(Error::Protocol).into_http()) });
        }
        let Some(connection) = request.extensions().get::<ConnectionStop>().cloned() else {
            return Box::pin(async {
                Ok(Status::internal("missing connection ownership").into_http())
            });
        };
        let started = Instant::now();
        let end = started + budget;
        let (budget_watch, guard) = watchdog(end, connection);
        let call = Call {
            started,
            end,
            work: Arc::new(permit),
            delivery: Arc::new(Mutex::new(None)),
            budget: budget_watch,
            _watchdog: guard,
        };
        request.extensions_mut().insert(call.clone());
        let future = self.inner.call(request);
        Box::pin(async move {
            let response =
                match tokio::time::timeout_at(tokio::time::Instant::from_std(end), future).await {
                    Ok(response) => response?,
                    Err(_) => return Ok(codec::error(Error::Unknown).into_http()),
                };
            let end = response
                .extensions()
                .get::<ResponseEnd>()
                .map_or(end, |deadline| end.min(deadline.0));
            let graph_memory = response
                .extensions()
                .get::<GraphMemory>()
                .map(|m| m.0.clone());
            Ok(response.map(|body| {
                tonic::body::Body::new(AdmittedBody {
                    body,
                    call,
                    _graph_memory: graph_memory,
                    deadline: Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                        end,
                    ))),
                })
            }))
        })
    }
}
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
    _graph_memory: Option<Arc<OwnedSemaphorePermit>>,
    body: tonic::body::Body,
    call: Call,
    deadline: Pin<Box<tokio::time::Sleep>>,
}
impl http_body::Body for AdmittedBody {
    type Data = tonic::codegen::Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Status>>> {
        if self.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(Status::deadline_exceeded("response deadline"))));
        }
        if self
            .call
            .delivery
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|invocation| invocation.live().is_err())
        {
            return Poll::Ready(Some(Err(codec::error(Error::SessionUnavailable))));
        }
        let result = Pin::new(&mut self.body).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &result {
            if frame
                .trailers_ref()
                .and_then(|trailers| trailers.get("grpc-status"))
                .is_some_and(|status| status == "0")
            {
                if let Some(invocation) = self.call.delivery.lock().unwrap().as_mut() {
                    invocation.delivered();
                }
            }
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
struct LimitedIo {
    socket: TcpStream,
    _permit: OwnedSemaphorePermit,
    stop: ConnectionStop,
    cancelled: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}
impl tonic::transport::server::Connected for LimitedIo {
    type ConnectInfo = ConnectionStop;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.stop.clone()
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
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
/// Loopback-only public typed gRPC service sharing the existing managed gateway.
pub struct GrpcService {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    hard_stop: CancellationToken,
    drain: Arc<Drain>,
    task: Option<JoinHandle<()>>,
    deadline: Duration,
}
impl GrpcService {
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    /// Fence new RPC admission and send GOAWAY, allowing admitted RPCs to finish
    /// within the grace period and original deadlines. Sessions are not transferred.
    pub fn begin_drain(&self, grace: Duration) -> Result<()> {
        self.drain.begin(grace)?;
        let _ = self.stop.send(true);
        Ok(())
    }
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        let remaining = match self.drain.end() {
            Some(end) => end.saturating_duration_since(Instant::now()),
            None => {
                self.hard_stop.cancel();
                self.deadline
            }
        };
        if let Some(mut task) = self.task.take() {
            if tokio::time::timeout(remaining, &mut task).await.is_err() {
                self.hard_stop.cancel();
                task.abort();
                let _ = task.await;
            }
        }
    }
}
impl Drop for GrpcService {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        self.hard_stop.cancel();
    }
}
pub(super) async fn bind(
    state: Arc<State>,
    mut gateway_stop: watch::Receiver<bool>,
    bind: SocketAddr,
    deadline: Duration,
) -> Result<GrpcService> {
    if !bind.ip().is_loopback() || *gateway_stop.borrow() {
        return Err(Error::Rejected);
    }
    check_deadline(deadline)?;
    let listener = TcpListener::bind(bind).await.map_err(|_| Error::Unknown)?;
    let address = listener.local_addr().map_err(|_| Error::Unknown)?;
    let (stop, mut stopped) = watch::channel(false);
    let hard_stop = CancellationToken::new();
    let cancellation = hard_stop.clone();
    let drain = Arc::new(Drain::default());
    let draining = drain.clone();
    let mut bound_stop = stop.subscribe();
    let mut bound_gateway = gateway_stop.clone();
    let task = tokio::spawn(async move {
        let connection_budget = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let connection_stop = cancellation.clone();
        let incoming =
            tokio_stream::wrappers::TcpListenerStream::new(listener).filter_map(move |accepted| {
                match accepted {
                    Err(error) => Some(Err(error)),
                    Ok(socket) => {
                        connection_budget
                            .clone()
                            .try_acquire_owned()
                            .ok()
                            .map(|permit| {
                                // Custom incoming bypasses Tonic's TCP_NODELAY default.
                                socket.set_nodelay(true)?;
                                let token = connection_stop.child_token();
                                Ok(LimitedIo {
                                    socket,
                                    _permit: permit,
                                    stop: ConnectionStop(token.clone()),
                                    cancelled: Box::pin(token.cancelled_owned()),
                                })
                            })
                    }
                }
            });
        let handler = Handler {
            state: state.clone(),
        };
        let writes = WriteServiceServer::new(handler.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(MAX_RESPONSE_BYTES);
        let fixed = FixedSessionServiceServer::new(handler.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(MAX_RESPONSE_BYTES);
        let jobs = ManagedQueryJobServiceServer::new(handler.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(MAX_RESPONSE_BYTES);
        let readiness = ReadinessServiceServer::new(handler.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(MAX_RESPONSE_BYTES);
        let graph = GraphServiceServer::new(handler.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(MAX_RESPONSE_BYTES);
        let queries = QueryServiceServer::new(handler)
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(MAX_RESPONSE_BYTES);
        let bound_drain = draining.clone();
        let bound_cancel = cancellation.clone();
        let hard_limit = async move {
            tokio::select! { _ = bound_stop.changed() => {}, _ = bound_gateway.changed() => {} }
            if let Some(end) = bound_drain.end().filter(|_| !*bound_gateway.borrow()) {
                tokio::select! {
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(end)) => {},
                    _ = bound_gateway.changed() => {},
                }
            }
            bound_cancel.cancel();
        };
        let shutdown_drain = draining.clone();
        let shutdown = async move {
            tokio::select! {_=stopped.changed()=>{},_=gateway_stop.changed()=>{}}
            if shutdown_drain.admitting() || *gateway_stop.borrow() {
                cancellation.cancel();
            }
        };
        let server = tonic::transport::Server::builder()
            .timeout(deadline)
            .max_concurrent_streams(Some(MAX_CONNECTIONS as u32))
            .add_service(Admitted {
                drain: draining.clone(),
                inner: writes,
                state: state.clone(),
                deadline,
            })
            .add_service(Admitted {
                drain: draining.clone(),
                inner: fixed,
                state: state.clone(),
                deadline,
            })
            .add_service(Admitted {
                drain: draining.clone(),
                inner: jobs,
                state: state.clone(),
                deadline,
            })
            .add_service(Admitted {
                drain: draining.clone(),
                inner: readiness,
                state: state.clone(),
                deadline,
            })
            .add_service(Admitted {
                drain: draining.clone(),
                inner: graph,
                state: state.clone(),
                deadline,
            })
            .add_service(Admitted {
                drain: draining.clone(),
                inner: queries,
                state,
                deadline,
            })
            .serve_with_incoming_shutdown(incoming, shutdown);
        // Enforce the bound even if the owner calls begin_drain without awaiting
        // shutdown. Closing the IO tokens also stops Tonic's spawned connections.
        tokio::select! { _ = server => {}, _ = hard_limit => {} }
    });
    Ok(GrpcService {
        address,
        stop,
        hard_stop,
        drain,
        task: Some(task),
        deadline,
    })
}

#[tonic::async_trait]
impl ManagedQueryJobService for Handler {
    async fn start(
        &self,
        request: tonic::Request<wire::JobStartRequest>,
    ) -> std::result::Result<tonic::Response<wire::JobStartResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::job_start(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_job_start)
            .await
    }
    async fn get(
        &self,
        request: tonic::Request<wire::JobGetRequest>,
    ) -> std::result::Result<tonic::Response<wire::JobStatusResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::job_get(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_job_status)
            .await
    }
    async fn renew(
        &self,
        request: tonic::Request<wire::JobRenewRequest>,
    ) -> std::result::Result<tonic::Response<wire::JobRenewResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::job_renew(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_job_renew)
            .await
    }
    async fn fetch(
        &self,
        request: tonic::Request<wire::JobFetchRequest>,
    ) -> std::result::Result<tonic::Response<wire::JobFetchResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::job_fetch(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_job_fetch)
            .await
    }
    async fn cancel(
        &self,
        request: tonic::Request<wire::JobCancelRequest>,
    ) -> std::result::Result<tonic::Response<wire::JobStatusResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        let operation = codec::job_cancel(request.into_inner())
            .map_err(|e| codec::identified_error(e, identity.clone()))?;
        self.run(call, operation, budget, identity, codec::encode_job_status)
            .await
    }
}

#[tonic::async_trait]
impl ReadinessService for Handler {
    async fn check(
        &self,
        request: tonic::Request<wire::ReadinessRequest>,
    ) -> std::result::Result<tonic::Response<wire::ReadinessResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        if self
            .state
            .jobs_stopping
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(codec::identified_error(Error::Rejected, identity));
        }
        let end = call.end.min(call.started + budget);
        call.budget.send_replace(end);
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(end),
            self.state.probe_readiness(call.work.clone(), end),
        )
        .await;
        let version = match result {
            Ok(result) => result.map_err(|e| codec::identified_error(e, identity.clone()))?,
            Err(_) => return Err(codec::identified_error(Error::Unknown, identity)),
        };
        before_deadline(end).map_err(|e| codec::identified_error(e, identity.clone()))?;
        let message = wire::ReadinessResponse { version, identity };
        codec::bounded::validate_bytes::<wire::ReadinessResponse>(&message.encode_to_vec())?;
        let mut response = tonic::Response::new(message);
        response.extensions_mut().insert(ResponseEnd(end));
        Ok(response)
    }
}
#[tonic::async_trait]
impl GraphService for Handler {
    async fn execute(
        &self,
        request: tonic::Request<wire::GraphExecuteRequest>,
    ) -> std::result::Result<tonic::Response<wire::GraphExecuteResponse>, Status> {
        let call = admitted(&request)?;
        let (budget, identity) = self.prepare(&request.get_ref().context)?;
        if self
            .state
            .jobs_stopping
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(codec::identified_error(Error::Rejected, identity));
        }
        let end = invocation::query_end(call.end.min(call.started + budget));
        call.budget.send_replace(end);
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(end),
            self.state
                .execute_graph(request.into_inner(), call.work.clone(), end),
        )
        .await;
        let (version, data, end, memory) = match result {
            Ok(result) => result.map_err(|e| codec::identified_error(e, identity.clone()))?,
            Err(_) => return Err(codec::identified_error(Error::Unknown, identity)),
        };
        before_deadline(end).map_err(|e| codec::identified_error(e, identity.clone()))?;
        call.budget.send_replace(end);
        let message = wire::GraphExecuteResponse {
            version,
            data: Some(data),
            identity,
        };
        codec::bounded::validate_bytes::<wire::GraphExecuteResponse>(&message.encode_to_vec())?;
        let mut response = tonic::Response::new(message);
        response.extensions_mut().insert(ResponseEnd(end));
        response
            .extensions_mut()
            .insert(GraphMemory(Arc::new(memory)));
        Ok(response)
    }
}
