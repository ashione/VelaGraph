//! Typed observed data-local query fragments. No snapshot download or JSON payload.
//! Includes hierarchy job shard sessions; coordinator ingress is separate.
use super::*;
use crate::remote::paged::grpc::{remote_status, response, status, Handler};
use tonic::{transport::Channel, Status};

#[path = "grpc/coordinator_codec.rs"]
mod coordinator_codec;
#[path = "grpc/coordinator_transport.rs"]
mod coordinator_transport;
pub use coordinator_transport::{CoordinatorClient, GrpcCoordinatorService};

pub mod wire {
    include!("grpc/query_proto.rs");
}
#[path = "grpc/bounded_codec.rs"]
pub(crate) mod bounded_codec;
#[path = "grpc/codec.rs"]
mod codec;
#[path = "grpc/fixed_codec.rs"]
mod fixed_codec;
#[path = "grpc/fixed_transport.rs"]
mod fixed_transport;
pub(in crate::remote) use fixed_transport::service as fixed_service;
pub(crate) use fixed_transport::FixedClient;
use wire::query_service_client::QueryServiceClient;
use wire::query_service_server::{QueryService, QueryServiceServer};

// Preserve existing wire ceilings, not a new expanded traversal allowance.
const REQUEST_LIMIT: usize = query::MAX_QUERY_RESPONSE_BYTES + 1024;
const RESPONSE_LIMIT: usize = query::MAX_QUERY_RESPONSE_BYTES + 1024;

pub(in crate::remote) fn service(handler: Handler) -> QueryServiceServer<Handler> {
    QueryServiceServer::new(handler)
        .max_decoding_message_size(REQUEST_LIMIT)
        .max_encoding_message_size(RESPONSE_LIMIT)
}
fn context(context: Option<wire::Context>) -> Result<crate::remote::grpc::wire::Context, Status> {
    let c = context.ok_or_else(|| status(RemoteError::InvalidArgument))?;
    if !(1..=MAX_TIMEOUT).contains(&c.timeout_millis) {
        return Err(status(RemoteError::InvalidArgument));
    }
    Ok(crate::remote::grpc::wire::Context {
        version: c.version,
        identity: c.identity.map(|i| crate::remote::grpc::wire::Identity {
            group: i.group,
            node: i.node,
        }),
        timeout_millis: c.timeout_millis,
    })
}
#[tonic::async_trait]
impl QueryService for Handler {
    async fn open(
        &self,
        request: tonic::Request<wire::OpenRequest>,
    ) -> Result<tonic::Response<wire::Descriptor>, Status> {
        let request = request.into_inner();
        let context = context(request.context)?;
        let timeout_millis = context.timeout_millis;
        match self
            .execute(
                Some(context),
                Operation::OpenQuery {
                    shard: request.shard,
                    timeout_millis,
                },
            )
            .await?
        {
            (Reply::OpenQuery(descriptor), end) => Ok(response(descriptor.into(), end)),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn open_hierarchy(
        &self,
        request: tonic::Request<wire::OpenRequest>,
    ) -> Result<tonic::Response<wire::HierarchyDescriptor>, Status> {
        let request = request.into_inner();
        match self
            .execute(
                Some(context(request.context)?),
                Operation::OpenHierarchyQuery {
                    shard: request.shard,
                },
            )
            .await?
        {
            (
                Reply::OpenHierarchyQuery {
                    descriptor,
                    max_row_bytes,
                },
                end,
            ) => Ok(response(
                wire::HierarchyDescriptor {
                    descriptor: Some(descriptor.into()),
                    max_row_bytes: max_row_bytes as u64,
                },
                end,
            )),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn fragment(
        &self,
        request: tonic::Request<wire::FragmentRequest>,
    ) -> Result<tonic::Response<wire::FragmentResponse>, Status> {
        let mut request = request.into_inner();
        let context = context(request.context.take())?;
        let request: FragmentRequest = request.try_into().map_err(status)?;
        request.fragment.validate().map_err(status)?;
        match self
            .execute(Some(context), Operation::QueryFragment(request))
            .await?
        {
            (Reply::QueryFragment(result), end) => {
                let result = match result {
                    Ok(output) => wire::fragment_response::Result::Output(output.into()),
                    Err(error) => wire::fragment_response::Result::Failure(
                        wire::QueryFailure::from(error) as i32,
                    ),
                };
                Ok(response(
                    wire::FragmentResponse {
                        result: Some(result),
                    },
                    end,
                ))
            }
            _ => Err(status(RemoteError::Protocol)),
        }
    }
    async fn close(
        &self,
        request: tonic::Request<wire::CloseRequest>,
    ) -> Result<tonic::Response<wire::Empty>, Status> {
        let request = request.into_inner();
        let context = context(request.context)?;
        let descriptor = request
            .descriptor
            .ok_or_else(|| status(RemoteError::InvalidArgument))?
            .try_into()
            .map_err(status)?;
        match self
            .execute(Some(context), Operation::CloseQuery { descriptor })
            .await?
        {
            (Reply::CloseQuery, end) => Ok(response(wire::Empty {}, end)),
            _ => Err(status(RemoteError::Protocol)),
        }
    }
}

#[derive(Clone)]
pub(super) struct QueryClient {
    client: QueryServiceClient<Channel>,
    identity: RemoteIdentity,
}
impl QueryClient {
    pub(super) async fn connect(
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
            client: QueryServiceClient::new(channel)
                .max_decoding_message_size(RESPONSE_LIMIT)
                .max_encoding_message_size(REQUEST_LIMIT),
            identity,
        })
    }
    fn context(&self, end: Instant) -> Result<wire::Context, RemoteError> {
        let millis = end
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(MAX_TIMEOUT as u128) as u64;
        if millis == 0 {
            return Err(RemoteError::Timeout { request_id: None });
        }
        Ok(wire::Context {
            version: VERSION,
            identity: Some(wire::Identity {
                group: self.identity.group,
                node: self.identity.node,
            }),
            timeout_millis: millis,
        })
    }
    fn request<T>(message: T, end: Instant) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(end.saturating_duration_since(Instant::now()));
        request
    }
    pub(super) async fn open(
        &self,
        shard: u64,
        deadline: Duration,
    ) -> Result<Descriptor, RemoteError> {
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
        let descriptor: Descriptor = response
            .into_inner()
            .try_into()
            .map_err(|_| RemoteError::Protocol)?;
        if descriptor.stamp.identity != self.identity || descriptor.stamp.shard != shard {
            return Err(RemoteError::Protocol);
        }
        Ok(descriptor)
    }
    pub(super) async fn open_hierarchy(
        &self,
        shard: u64,
        deadline: Duration,
    ) -> Result<(Descriptor, usize), RemoteError> {
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
        let result = tokio::time::timeout_at(end, self.client.clone().open_hierarchy(request))
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(remote_status)?
            .into_inner();
        let descriptor: Descriptor = result
            .descriptor
            .ok_or(RemoteError::Protocol)?
            .try_into()
            .map_err(|_| RemoteError::Protocol)?;
        if descriptor.stamp.identity != self.identity || descriptor.stamp.shard != shard {
            return Err(RemoteError::Protocol);
        }
        let max_row_bytes =
            usize::try_from(result.max_row_bytes).map_err(|_| RemoteError::Protocol)?;
        Ok((descriptor, max_row_bytes))
    }
    pub(super) async fn fragment(
        &self,
        request: FragmentRequest,
        deadline: Duration,
    ) -> Result<Result<FragmentOutput, QueryFailure>, RemoteError> {
        let end = checked_deadline(deadline, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        request.fragment.validate()?;
        let descriptor = request.descriptor.clone();
        let mut request: wire::FragmentRequest = request.into();
        request.context = Some(self.context(end)?);
        let response = tokio::time::timeout_at(
            end,
            self.client.clone().fragment(Self::request(request, end)),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
        .map_err(remote_status)?
        .into_inner();
        match response.result.ok_or(RemoteError::Protocol)? {
            wire::fragment_response::Result::Output(output) => {
                let output: FragmentOutput =
                    output.try_into().map_err(|_| RemoteError::Protocol)?;
                if output.descriptor != descriptor {
                    return Err(RemoteError::Protocol);
                }
                Ok(Ok(output))
            }
            wire::fragment_response::Result::Failure(error) => {
                let error =
                    wire::QueryFailure::try_from(error).map_err(|_| RemoteError::Protocol)?;
                Ok(Err(error.try_into().map_err(|_| RemoteError::Protocol)?))
            }
        }
    }
    pub(super) async fn close(
        &self,
        descriptor: Descriptor,
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
