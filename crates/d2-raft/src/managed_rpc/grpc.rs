//! Typed managed coordinator/member RPC. Trusted plaintext deployment only.
//! Transport selection is explicit; no transparent retry or TCP fallback.
use super::{grpc_codec as codec, *};
use prost::Message;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::OwnedSemaphorePermit;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{
    codegen::{http, BoxFuture, Service},
    transport::Channel,
    Status,
};

/// Versioned typed wire schema; internal listeners must not be exposed publicly.
pub use super::grpc_codec::pb as wire;
use wire::managed_service_client::ManagedServiceClient;
use wire::managed_service_server::{ManagedService, ManagedServiceServer};

#[derive(Clone)]
pub(super) struct Transport {
    channel: Channel,
}
impl Transport {
    pub(super) fn new(address: SocketAddr, timeout: Duration) -> Result<Self, Error> {
        tokio::runtime::Handle::try_current().map_err(|_| Error::Unavailable)?;
        let channel = Channel::from_shared(format!("http://{address}"))
            .map_err(|_| Error::Protocol)?
            .connect_timeout(timeout)
            .concurrency_limit(CONCURRENCY)
            .buffer_size(CONCURRENCY)
            .connect_lazy();
        Ok(Self { channel })
    }
    pub(super) async fn call(
        &self,
        request: Request,
        deadline: tokio::time::Instant,
    ) -> Result<Reply, Error> {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(Error::Unavailable)?;
        // Floor, never extend the original caller's budget.
        let millis: u64 = remaining
            .as_millis()
            .try_into()
            .map_err(|_| Error::Capacity)?;
        if millis == 0 {
            return Err(Error::Unavailable);
        }
        let message = cpu(move || codec::request_to_wire(request, millis)).await?;
        if message.encoded_len() > MAX_FRAME_BYTES {
            return Err(Error::Capacity);
        }
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(Error::Unavailable)?;
        let mut request = tonic::Request::new(message);
        request.set_timeout(remaining);
        let mut client = ManagedServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_FRAME_BYTES)
            .max_encoding_message_size(MAX_FRAME_BYTES);
        let reply = client
            .call(request)
            .await
            .map_err(remote_status)?
            .into_inner();
        cpu(move || codec::reply_from_wire(reply)).await
    }
}
fn status(error: Error) -> Status {
    match error {
        Error::Capacity => Status::resource_exhausted("managed capacity"),
        Error::Identity => Status::failed_precondition("managed identity"),
        Error::Protocol => Status::invalid_argument("managed protocol"),
        Error::Unavailable => Status::unavailable("managed outcome unavailable"),
        Error::Application(_) => Status::internal("managed application envelope"),
    }
}
fn remote_status(error: Status) -> Error {
    match error.code() {
        tonic::Code::ResourceExhausted => Error::Capacity,
        tonic::Code::FailedPrecondition => Error::Identity,
        tonic::Code::InvalidArgument | tonic::Code::DataLoss | tonic::Code::Unimplemented => {
            Error::Protocol
        }
        _ => Error::Unavailable,
    }
}

// The generated client/server use this codec, so bounds apply before prost allocates.
pub(crate) struct BoundedCodec<T, U>(std::marker::PhantomData<(T, U)>);
impl<T, U> Default for BoundedCodec<T, U> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}
pub(crate) struct Encoder<T>(std::marker::PhantomData<T>);
pub(crate) struct Decoder<T>(std::marker::PhantomData<T>);
impl<T: Message + Send + 'static, U: Message + Default + Send + 'static> tonic::codec::Codec
    for BoundedCodec<T, U>
{
    type Encode = T;
    type Decode = U;
    type Encoder = Encoder<T>;
    type Decoder = Decoder<U>;
    fn encoder(&mut self) -> Self::Encoder {
        Encoder(std::marker::PhantomData)
    }
    fn decoder(&mut self) -> Self::Decoder {
        Decoder(std::marker::PhantomData)
    }
}
impl<T: Message> tonic::codec::Encoder for Encoder<T> {
    type Item = T;
    type Error = Status;
    fn encode(&mut self, item: T, buf: &mut tonic::codec::EncodeBuf<'_>) -> Result<(), Status> {
        if item.encoded_len() > MAX_FRAME_BYTES {
            return Err(status(Error::Capacity));
        }
        item.encode(buf).map_err(|_| status(Error::Protocol))
    }
}
impl<T: Message + Default + 'static> tonic::codec::Decoder for Decoder<T> {
    type Item = T;
    type Error = Status;
    fn decode(&mut self, buf: &mut tonic::codec::DecodeBuf<'_>) -> Result<Option<T>, Status> {
        use prost::bytes::Buf;
        let bytes = buf.chunk();
        if bytes.len() != buf.remaining() {
            return Err(status(Error::Protocol));
        }
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(status(Error::Capacity));
        }
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<wire::Request>() {
            codec::check_request(bytes).map_err(status)?;
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<wire::Reply>() {
            codec::check_reply(bytes).map_err(status)?;
        } else {
            return Err(status(Error::Protocol));
        }
        T::decode(buf)
            .map(Some)
            .map_err(|_| status(Error::Protocol))
    }
}

#[derive(Clone)]
struct ConnectionStop(CancellationToken);
struct Watchdog(CancellationToken);
impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[derive(Clone)]
struct Call {
    started: tokio::time::Instant,
    end: tokio::time::Instant,
    budget: tokio::sync::watch::Sender<tokio::time::Instant>,
    permit: Arc<OwnedSemaphorePermit>,
    _watchdog: Arc<Watchdog>,
}
fn watchdog(
    end: tokio::time::Instant,
    connection: ConnectionStop,
) -> (
    tokio::sync::watch::Sender<tokio::time::Instant>,
    Arc<Watchdog>,
) {
    let token = CancellationToken::new();
    let stop = token.clone();
    let (budget, mut changed) = tokio::sync::watch::channel(end);
    tokio::spawn(async move {
        let mut end = end;
        loop {
            end = end.min(*changed.borrow_and_update());
            if tokio::time::Instant::now() >= end {
                connection.0.cancel();
                break;
            }
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep_until(end) => { connection.0.cancel(); break; },
                result = changed.changed() => { if result.is_err() { break; } },
            }
        }
    });
    (budget, Arc::new(Watchdog(token)))
}
#[derive(Clone)]
struct Handler {
    node: Arc<NodeHandle>,
    identity: Identity,
    members: Option<Arc<[Client]>>,
}
#[tonic::async_trait]
impl ManagedService for Handler {
    async fn call(
        &self,
        request: tonic::Request<wire::Request>,
    ) -> Result<tonic::Response<wire::Reply>, Status> {
        let call = request
            .extensions()
            .get::<Call>()
            .cloned()
            .ok_or_else(|| Status::internal("missing admission"))?;
        let message = request.into_inner();
        let decode_permit = call.permit.clone();
        let (request, millis) = cpu(move || {
            let _permit = decode_permit;
            codec::request_from_wire(message)
        })
        .await
        .map_err(status)?;
        if millis == 0 || millis > MAX_TIMEOUT.as_millis() as u64 {
            return Err(status(Error::Protocol));
        }
        let end = call.end.min(call.started + Duration::from_millis(millis));
        call.budget.send_replace(end);
        if tokio::time::Instant::now() >= end {
            return Err(status(Error::Unavailable));
        }
        let operation = request.operation.tag();
        let result = if request.version != PROTOCOL_VERSION {
            Err(Error::Protocol)
        } else if request.identity != self.identity {
            Err(Error::Identity)
        } else {
            tokio::time::timeout_at(
                end,
                dispatch(
                    &self.node,
                    Some(self.identity.clone()),
                    self.members.clone(),
                    request.operation,
                    end,
                ),
            )
            .await
            .unwrap_or(Err(Error::Unavailable))
        };
        let reply = Reply {
            version: PROTOCOL_VERSION,
            operation,
            identity: self.identity.clone(),
            nonce: request.nonce,
            result,
        };
        let permit = call.permit.clone();
        let message = cpu(move || {
            let _permit = permit;
            codec::reply_to_wire(reply)
        })
        .await
        .map_err(status)?;
        if tokio::time::Instant::now() >= end {
            return Err(status(Error::Unavailable));
        }
        if message.encoded_len() > MAX_FRAME_BYTES {
            return Err(status(Error::Capacity));
        }
        Ok(tonic::Response::new(message))
    }
}
#[derive(Clone)]
struct Admitted<S> {
    inner: S,
    slots: Arc<Semaphore>,
    timeout: Duration,
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
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let Ok(permit) = self.slots.clone().try_acquire_owned() else {
            return Box::pin(async { Ok(status(Error::Capacity).into_http()) });
        };
        let budget = match request.headers().get("grpc-timeout") {
            None => self.timeout,
            Some(value) => match value.to_str().ok().and_then(grpc_timeout) {
                Some(value) => value.min(self.timeout),
                None => return Box::pin(async { Ok(status(Error::Protocol).into_http()) }),
            },
        };
        if request.headers().get_all("grpc-timeout").iter().count() > 1 {
            return Box::pin(async { Ok(status(Error::Protocol).into_http()) });
        }
        let Some(connection) = request.extensions().get::<ConnectionStop>().cloned() else {
            return Box::pin(async { Ok(Status::internal("missing connection").into_http()) });
        };
        let started = tokio::time::Instant::now();
        let end = started + budget;
        let (budget, watchdog) = watchdog(end, connection);
        let call = Call {
            started,
            end,
            budget,
            permit: Arc::new(permit),
            _watchdog: watchdog,
        };
        request.extensions_mut().insert(call.clone());
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = match tokio::time::timeout_at(end, future).await {
                Ok(response) => response?,
                Err(_) => return Ok(status(Error::Unavailable).into_http()),
            };
            Ok(response.map(|body| {
                tonic::body::Body::new(AdmittedBody {
                    body,
                    _call: call,
                    deadline: Box::pin(tokio::time::sleep_until(end)),
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
    let n: u64 = digits.parse().ok()?;
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
    _call: Call,
    deadline: Pin<Box<tokio::time::Sleep>>,
}
impl http_body::Body for AdmittedBody {
    type Data = tonic::codegen::Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Status>>> {
        if self.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "managed response deadline",
            ))));
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
struct LimitedIo {
    socket: TcpStream,
    _permit: OwnedSemaphorePermit,
    stop: ConnectionStop,
    cancelled: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}
impl tonic::transport::server::Connected for LimitedIo {
    type ConnectInfo = ConnectionStop;
    fn connect_info(&self) -> ConnectionStop {
        self.stop.clone()
    }
}
impl AsyncRead for LimitedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(std::io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}
impl AsyncWrite for LimitedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(std::io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(std::io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
/// Managed internal gRPC listener. Startup uses the original readiness probe.
pub struct Server {
    address: SocketAddr,
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
    timeout: Duration,
}
impl Server {
    pub async fn bind(
        node: Arc<NodeHandle>,
        identity: Identity,
        address: SocketAddr,
        timeout: Duration,
    ) -> Result<Self, Error> {
        if identity.role == Role::Coordinator
            && identity.cohort.profile == CapacityProfile::ByteBound16MiBV1
        {
            return Err(Error::Identity);
        }
        Self::configured(node, identity, None, address, timeout).await
    }
    pub async fn bind_byte_bound_coordinator(
        node: Arc<NodeHandle>,
        identity: Identity,
        mut members: Vec<Client>,
        address: SocketAddr,
        timeout: Duration,
    ) -> Result<Self, Error> {
        validate_capacity_endpoints(&identity, &mut members)?;
        Self::configured(node, identity, Some(members.into()), address, timeout).await
    }
    async fn configured(
        node: Arc<NodeHandle>,
        identity: Identity,
        members: Option<Arc<[Client]>>,
        address: SocketAddr,
        timeout: Duration,
    ) -> Result<Self, Error> {
        identity.validate()?;
        valid_timeout(timeout)?;
        if identity.node != node.id || identity.group != node.group_id() {
            return Err(Error::Identity);
        }
        let probe = match identity.role {
            Role::Coordinator => Operation::Summary {
                authority: identity.authority.clone(),
                epoch: identity.target.epoch,
            },
            Role::Participant => Operation::Visibility {
                target: identity.target.clone(),
            },
        };
        let end = tokio::time::Instant::now() + timeout;
        tokio::time::timeout_at(
            end,
            dispatch(&node, Some(identity.clone()), members.clone(), probe, end),
        )
        .await
        .map_err(|_| Error::Unavailable)??;
        let listener = TcpListener::bind(address)
            .await
            .map_err(|_| Error::Unavailable)?;
        let address = listener.local_addr().map_err(|_| Error::Unavailable)?;
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let connections_stop = stop.clone();
        let task = tokio::spawn(async move {
            let slots = Arc::new(Semaphore::new(CONCURRENCY));
            let connections = Arc::new(Semaphore::new(CONCURRENCY + MAX_OUTBOUND_WAITERS));
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).filter_map(
                move |accepted| match accepted {
                    Err(error) => Some(Err(error)),
                    Ok(socket) => connections.clone().try_acquire_owned().ok().map(|permit| {
                        let token = connections_stop.child_token();
                        Ok(LimitedIo {
                            socket,
                            _permit: permit,
                            stop: ConnectionStop(token.clone()),
                            cancelled: Box::pin(token.cancelled_owned()),
                        })
                    }),
                },
            );
            let service = ManagedServiceServer::new(Handler {
                node,
                identity,
                members,
            })
            .max_decoding_message_size(MAX_FRAME_BYTES)
            .max_encoding_message_size(MAX_FRAME_BYTES);
            let _ = tonic::transport::Server::builder()
                .timeout(timeout)
                .max_concurrent_streams(Some(CONCURRENCY as u32))
                .add_service(Admitted {
                    inner: service,
                    slots,
                    timeout,
                })
                .serve_with_incoming_shutdown(incoming, shutdown.cancelled_owned())
                .await;
        });
        Ok(Self {
            address,
            stop,
            task: Some(task),
            timeout,
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub async fn shutdown(mut self) {
        self.stop.cancel();
        if let Some(mut task) = self.task.take() {
            if tokio::time::timeout(self.timeout, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> Identity {
        Identity {
            group: 9,
            node: 1,
            target: Target {
                shard: 9,
                epoch: 1,
                mode: [9; 16],
            },
            role: Role::Coordinator,
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: Cohort {
                profile: CapacityProfile::LegacyCountV1,
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![crate::transaction_decisions::Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
            },
        }
    }
    #[test]
    fn timeout_header_is_strict_and_bounded() {
        assert_eq!(grpc_timeout("10m"), Some(Duration::from_millis(10)));
        for value in ["", "1", "-1S", "+1S", "999999999S", "1x", "１S", "m"] {
            assert!(grpc_timeout(value).is_none(), "{value}");
        }
    }
    #[tokio::test]
    async fn watchdog_expires_without_body_polling_and_honors_shorter_budget() {
        let connection = CancellationToken::new();
        let (budget, guard) = watchdog(
            tokio::time::Instant::now() + Duration::from_secs(10),
            ConnectionStop(connection.clone()),
        );
        budget.send_replace(tokio::time::Instant::now());
        tokio::time::timeout(Duration::from_secs(1), connection.cancelled())
            .await
            .unwrap();
        drop(guard);
        let connection = CancellationToken::new();
        let (budget, guard) = watchdog(
            tokio::time::Instant::now() + Duration::from_secs(10),
            ConnectionStop(connection.clone()),
        );
        drop(guard);
        drop(budget);
        tokio::task::yield_now().await;
        assert!(
            !connection.is_cancelled(),
            "completed calls must keep a reusable channel alive"
        );
    }
    #[tokio::test]
    async fn byte_bound_coordinator_requires_complete_member_endpoints() {
        let cluster = crate::Cluster::new().in_group(9);
        let node = Arc::new(cluster.start(1).await.unwrap());
        let mut id = identity();
        id.cohort.profile = CapacityProfile::ByteBound16MiBV1;
        assert!(matches!(
            Server::bind(
                node.clone(),
                id.clone(),
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(1)
            )
            .await,
            Err(Error::Identity)
        ));
        assert!(matches!(
            Server::bind_byte_bound_coordinator(
                node.clone(),
                id,
                vec![],
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(1)
            )
            .await,
            Err(Error::Identity)
        ));
        node.raft.shutdown().await.unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_http2_window_releases_server_admission() {
        let _guard = test_resource_guard().await;
        let cluster = crate::Cluster::new().in_group(9);
        let node = Arc::new(cluster.start(1).await.unwrap());
        node.raft
            .initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while node.raft.ensure_linearizable().await.is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for command in [
            D1Command::Activate { shard: 9, epoch: 1 },
            D1Command::EnableManagedWrites {
                shard: 9,
                epoch: 1,
                expected_sequence: 0,
                mode_id: [9; 16],
            },
            D1Command::EnableCoordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
            },
            D1Command::Coordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
                command: Command::BindCohort {
                    cohort: identity().cohort,
                },
            },
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
        let server = Server::bind(
            node.clone(),
            identity(),
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let socket = TcpStream::connect(server.local_addr()).await.unwrap();
        let (mut sender, connection) = h2::client::Builder::new()
            .initial_window_size(0)
            .handshake::<_, prost::bytes::Bytes>(socket)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let message = codec::request_to_wire(
            Request {
                version: PROTOCOL_VERSION,
                identity: identity(),
                nonce: 1,
                operation: Operation::Summary {
                    authority: identity().authority,
                    epoch: 1,
                },
            },
            200,
        )
        .unwrap()
        .encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
        frame.extend_from_slice(&message);
        let mut responses = Vec::new();
        for _ in 0..CONCURRENCY {
            let request = http::Request::builder()
                .method("POST")
                .uri(format!(
                    "http://{}/velagraph.internal.managed.v1.ManagedService/Call",
                    server.local_addr()
                ))
                .header("content-type", "application/grpc")
                .header("te", "trailers")
                .header("grpc-timeout", "200m")
                .body(())
                .unwrap();
            let (response, mut body) = sender.send_request(request, false).unwrap();
            body.send_data(frame.clone().into(), true).unwrap();
            responses.push(response);
        }
        // Headers arrive, but stream DATA cannot be delivered with a zero receive window.
        let mut retained = Vec::new();
        for response in responses {
            retained.push(
                tokio::time::timeout(Duration::from_secs(1), response)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        for response in &retained {
            assert!(
                response.headers().get("grpc-status").is_none(),
                "RPC must be admitted, not rejected in headers"
            );
            assert!(
                !response.body().is_end_stream(),
                "response DATA must still be blocked"
            );
        }
        // h2 may report either a clean EOF or connection error; both must occur
        // within the budget while the response bodies remain deliberately unread.
        let _ = tokio::time::timeout(Duration::from_secs(1), driver)
            .await
            .unwrap()
            .unwrap();
        drop(retained);
        drop(sender);
        let client =
            Client::new_grpc(server.local_addr(), identity(), Duration::from_secs(1)).unwrap();
        assert_eq!(
            NodeRef::Remote(&client)
                .summary(&identity().authority, 1)
                .await
                .unwrap()
                .resolved_prefix,
            0
        );
        drop(client);
        server.shutdown().await;
        node.raft.shutdown().await.unwrap();
    }
    struct Peer {
        wrong: u8,
    }
    #[tonic::async_trait]
    impl ManagedService for Peer {
        async fn call(
            &self,
            request: tonic::Request<wire::Request>,
        ) -> Result<tonic::Response<wire::Reply>, Status> {
            let (request, _) = codec::request_from_wire(request.into_inner()).map_err(status)?;
            let mut reply = Reply {
                version: PROTOCOL_VERSION,
                operation: request.operation.tag(),
                identity: request.identity,
                nonce: request.nonce,
                result: Ok(Value::Summary(CoordinatorSummary {
                    authority: identity().authority,
                    cohort: identity().cohort,
                    resolved_prefix: 0,
                })),
            };
            match self.wrong {
                1 => reply.identity.node += 1,
                2 => reply.nonce += 1,
                3 => reply.operation += 1,
                4 => reply.version += 1,
                _ => {}
            }
            Ok(tonic::Response::new(
                codec::reply_to_wire(reply).map_err(status)?,
            ))
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_reuse_and_full_reply_binding() {
        let _guard = test_resource_guard().await;
        for wrong in 0..=4 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let connections = Arc::new(AtomicU64::new(0));
            let observed = connections.clone();
            let incoming =
                tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |socket| {
                    observed.fetch_add(1, Ordering::Relaxed);
                    socket
                });
            let stop = CancellationToken::new();
            let shutdown = stop.clone();
            let task = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(ManagedServiceServer::new(Peer { wrong }))
                    .serve_with_incoming_shutdown(incoming, shutdown.cancelled_owned())
                    .await
                    .unwrap();
            });
            let client = Client::new_grpc(address, identity(), Duration::from_secs(2)).unwrap();
            for _ in 0..3 {
                let result = NodeRef::Remote(&client.clone())
                    .summary(&identity().authority, 1)
                    .await;
                if wrong == 0 {
                    assert_eq!(result.unwrap().resolved_prefix, 0);
                } else {
                    assert_eq!(
                        result,
                        Err(if wrong == 4 {
                            Error::Protocol
                        } else {
                            Error::Identity
                        })
                    );
                }
            }
            assert_eq!(
                connections.load(Ordering::Relaxed),
                1,
                "clones must reuse channel"
            );
            drop(client);
            stop.cancel();
            task.await.unwrap();
        }
    }
}
