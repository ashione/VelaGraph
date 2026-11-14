//! Test-only typed HTTP/2 peers. Domain messages stay in memory, never JSON on wire.
use super::*;
use crate::remote::paged::grpc::status;
use grpc::wire;
use tokio::sync::{mpsc, oneshot};
use tonic::{Response as RpcResponse, Status};

/// One RPC stream observation, not a physical TCP socket.
pub(super) struct Socket {
    request: Option<super::super::Request>,
    reply: Option<oneshot::Sender<Result<Reply, RemoteError>>>,
}
impl Socket {
    // Completion observes handler/stream cancellation, not connection closure.
    pub(super) async fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
        if let Some(reply) = &mut self.reply {
            reply.closed().await;
        }
        Ok(0)
    }
}
pub(super) struct Listener {
    address: SocketAddr,
    incoming: tokio::sync::Mutex<mpsc::Receiver<Socket>>,
    shutdown: tokio_util::sync::CancellationToken,
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
impl Listener {
    pub(super) async fn bind(address: &str) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let (tx, incoming) = mpsc::channel(32);
        let peer = Peer(tx);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let stopping = shutdown.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    wire::query_service_server::QueryServiceServer::new(peer.clone())
                        .max_encoding_message_size(MAX_FRAME_BYTES)
                        .max_decoding_message_size(MAX_FRAME_BYTES),
                )
                .add_service(
                    wire::fixed_query_service_server::FixedQueryServiceServer::new(peer)
                        .max_encoding_message_size(MAX_FRAME_BYTES)
                        .max_decoding_message_size(MAX_FRAME_BYTES),
                )
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    stopping.cancelled_owned(),
                )
                .await
                .unwrap();
        });
        Ok(Self {
            address,
            incoming: tokio::sync::Mutex::new(incoming),
            shutdown,
        })
    }
    pub(super) fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.address)
    }
    pub(super) async fn accept(&self) -> std::io::Result<(Socket, SocketAddr)> {
        self.incoming
            .lock()
            .await
            .recv()
            .await
            .map(|s| (s, self.address))
            .ok_or_else(|| std::io::Error::other("typed peer stopped"))
    }
}
pub(super) async fn read_frame(
    socket: &mut Socket,
    _end: Instant,
) -> Result<super::super::Request, RemoteError> {
    socket.request.take().ok_or(RemoteError::Protocol)
}
pub(super) async fn write_frame(
    socket: &mut Socket,
    response: super::super::Response,
    _end: Instant,
) -> Result<(), RemoteError> {
    socket
        .reply
        .take()
        .ok_or(RemoteError::Protocol)?
        .send(response.result)
        .map_err(|_| RemoteError::Unavailable)
}
#[derive(Clone)]
struct Peer(mpsc::Sender<Socket>);
impl Peer {
    async fn call(
        &self,
        context: Option<wire::Context>,
        operation: Operation,
    ) -> Result<Reply, Status> {
        let context = context.ok_or_else(|| status(RemoteError::InvalidArgument))?;
        let (reply, receive) = oneshot::channel();
        let request = super::super::Request {
            version: context.version,
            identity: context.identity.map(|i| RemoteIdentity {
                group: i.group,
                node: i.node,
            }),
            operation,
        };
        self.0
            .send(Socket {
                request: Some(request),
                reply: Some(reply),
            })
            .await
            .map_err(|_| status(RemoteError::Unavailable))?;
        receive
            .await
            .map_err(|_| status(RemoteError::Unavailable))?
            .map_err(status)
    }
}
#[tonic::async_trait]
impl wire::fixed_query_service_server::FixedQueryService for Peer {
    async fn open_fixed(
        &self,
        request: tonic::Request<wire::FixedOpenEnvelope>,
    ) -> Result<RpcResponse<wire::FixedOpened>, Status> {
        let r = request.into_inner();
        let value = r
            .request
            .ok_or_else(|| status(RemoteError::Protocol))?
            .try_into()
            .map_err(status)?;
        match self
            .call(r.context, Operation::OpenFixedQuery(value))
            .await?
        {
            Reply::OpenFixedQuery(value) => Ok(RpcResponse::new(value.into())),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn open_renewable(
        &self,
        request: tonic::Request<wire::RenewableOpenEnvelope>,
    ) -> Result<RpcResponse<wire::RenewableFixedOpened>, Status> {
        let r = request.into_inner();
        let value = r
            .request
            .ok_or_else(|| status(RemoteError::Protocol))?
            .try_into()
            .map_err(status)?;
        match self
            .call(r.context, Operation::OpenRenewableFixedQuery(value))
            .await?
        {
            Reply::OpenRenewableFixedQuery(value) => Ok(RpcResponse::new(value.into())),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn renew(
        &self,
        request: tonic::Request<wire::FixedRenewEnvelope>,
    ) -> Result<RpcResponse<wire::FixedLeaseGrant>, Status> {
        let r = request.into_inner();
        let value = r
            .request
            .ok_or_else(|| status(RemoteError::Protocol))?
            .try_into()
            .map_err(status)?;
        match self
            .call(r.context, Operation::RenewFixedQuery(value))
            .await?
        {
            Reply::RenewFixedQuery(value) => Ok(RpcResponse::new(value.into())),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn fragment_fixed(
        &self,
        request: tonic::Request<wire::FixedFragmentEnvelope>,
    ) -> Result<RpcResponse<wire::FixedFragmentResponse>, Status> {
        let r = request.into_inner();
        let value = r
            .request
            .ok_or_else(|| status(RemoteError::Protocol))?
            .try_into()
            .map_err(status)?;
        match self
            .call(r.context, Operation::FixedQueryFragment(value))
            .await?
        {
            Reply::FixedQueryFragment(value) => Ok(RpcResponse::new(wire::FixedFragmentResponse {
                result: Some(match value {
                    Ok(v) => wire::fixed_fragment_response::Result::Output(v.into()),
                    Err(e) => wire::fixed_fragment_response::Result::Failure(
                        wire::QueryFailure::from(e) as i32,
                    ),
                }),
            })),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn close_fixed(
        &self,
        request: tonic::Request<wire::FixedCloseEnvelope>,
    ) -> Result<RpcResponse<wire::Empty>, Status> {
        let r = request.into_inner();
        let value = r
            .descriptor
            .ok_or_else(|| status(RemoteError::Protocol))?
            .try_into()
            .map_err(status)?;
        match self
            .call(r.context, Operation::CloseFixedQuery { descriptor: value })
            .await?
        {
            Reply::CloseFixedQuery => Ok(RpcResponse::new(wire::Empty {})),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
}
#[tonic::async_trait]
impl wire::query_service_server::QueryService for Peer {
    async fn open(
        &self,
        request: tonic::Request<wire::OpenRequest>,
    ) -> Result<RpcResponse<wire::Descriptor>, Status> {
        let r = request.into_inner();
        let operation = Operation::OpenQuery {
            shard: r.shard,
            timeout_millis: r.context.as_ref().map_or(0, |c| c.timeout_millis),
        };
        match self.call(r.context, operation).await? {
            Reply::OpenQuery(value) => Ok(RpcResponse::new(value.into())),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn open_hierarchy(
        &self,
        request: tonic::Request<wire::OpenRequest>,
    ) -> Result<RpcResponse<wire::HierarchyDescriptor>, Status> {
        let r = request.into_inner();
        let operation = Operation::OpenHierarchyQuery { shard: r.shard };
        match self.call(r.context, operation).await? {
            Reply::OpenHierarchyQuery {
                descriptor,
                max_row_bytes,
            } => Ok(RpcResponse::new(wire::HierarchyDescriptor {
                descriptor: Some(descriptor.into()),
                max_row_bytes: max_row_bytes as u64,
            })),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn fragment(
        &self,
        request: tonic::Request<wire::FragmentRequest>,
    ) -> Result<RpcResponse<wire::FragmentResponse>, Status> {
        let mut r = request.into_inner();
        let context = r.context.take();
        let value = r.try_into().map_err(status)?;
        match self.call(context, Operation::QueryFragment(value)).await? {
            Reply::QueryFragment(value) => Ok(RpcResponse::new(wire::FragmentResponse {
                result: Some(match value {
                    Ok(v) => wire::fragment_response::Result::Output(v.into()),
                    Err(e) => {
                        wire::fragment_response::Result::Failure(wire::QueryFailure::from(e) as i32)
                    }
                }),
            })),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn close(
        &self,
        request: tonic::Request<wire::CloseRequest>,
    ) -> Result<RpcResponse<wire::Empty>, Status> {
        let r = request.into_inner();
        let descriptor = r
            .descriptor
            .ok_or_else(|| status(RemoteError::Protocol))?
            .try_into()
            .map_err(status)?;
        match self
            .call(r.context, Operation::CloseQuery { descriptor })
            .await?
        {
            Reply::CloseQuery => Ok(RpcResponse::new(wire::Empty {})),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
}
