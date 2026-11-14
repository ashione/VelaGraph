//! Typed gRPC test peers. JSON, where used by callers, is a local fixture only.
use d2_raft::managed_rpc::{grpc::wire, MAX_FRAME_BYTES};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};

type ReplyFuture = Pin<Box<dyn Future<Output = Result<wire::Reply, Status>> + Send>>;
#[derive(Clone)]
struct Handler(Arc<dyn Fn(wire::Request) -> ReplyFuture + Send + Sync>);
#[tonic::async_trait]
impl wire::managed_service_server::ManagedService for Handler {
    async fn call(&self, request: Request<wire::Request>) -> Result<Response<wire::Reply>, Status> {
        (self.0)(request.into_inner()).await.map(Response::new)
    }
}
pub struct Peer {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Peer {
    pub async fn finish(mut self) {
        if let Some(stop) = self.stop.take() { let _ = stop.send(()); }
        if let Some(task) = self.task.take() { task.await.unwrap(); }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() { let _ = stop.send(()); }
        if let Some(task) = self.task.take() { task.abort(); }
    }
}
pub fn spawn<F, Fut>(listener: TcpListener, handler: F) -> Peer
where
    F: Fn(wire::Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<wire::Reply, Status>> + Send + 'static,
{
    let handler = Handler(Arc::new(move |request| Box::pin(handler(request))));
    let service = wire::managed_service_server::ManagedServiceServer::new(handler)
        .max_decoding_message_size(MAX_FRAME_BYTES)
        .max_encoding_message_size(MAX_FRAME_BYTES);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder().add_service(service)
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async { let _ = stopped.await; })
            .await.unwrap();
    });
    Peer { stop: Some(stop), task: Some(task) }
}
pub async fn forward(upstream: std::net::SocketAddr, mut request: wire::Request) -> Result<wire::Reply, Status> {
    let timeout = std::time::Duration::from_millis(request.timeout_millis);
    let end = tokio::time::Instant::now() + timeout;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{upstream}"))
        .map_err(|_| Status::invalid_argument("test endpoint"))?
        .connect_timeout(timeout).connect().await.map_err(|_| Status::unavailable("test upstream"))?;
    let mut client = wire::managed_service_client::ManagedServiceClient::new(channel)
        .max_decoding_message_size(MAX_FRAME_BYTES).max_encoding_message_size(MAX_FRAME_BYTES);
    let remaining = end.checked_duration_since(tokio::time::Instant::now())
        .ok_or_else(|| Status::deadline_exceeded("test forwarding budget"))?;
    request.timeout_millis = remaining.as_millis().try_into().map_err(|_| Status::invalid_argument("test forwarding budget"))?;
    if request.timeout_millis == 0 { return Err(Status::deadline_exceeded("test forwarding budget")); }
    let mut request = Request::new(request);
    request.set_timeout(remaining);
    client.call(request).await.map(Response::into_inner)
}
