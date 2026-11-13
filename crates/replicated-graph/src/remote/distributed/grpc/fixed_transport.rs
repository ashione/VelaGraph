//! Typed fixed-V query transport. Managed control and Raft are separate protocols.
use super::*;
use prost::Message;
use wire::fixed_query_service_client::FixedQueryServiceClient;
use wire::fixed_query_service_server::{FixedQueryService, FixedQueryServiceServer};

pub(in crate::remote) fn service(handler: Handler) -> FixedQueryServiceServer<Handler> {
    FixedQueryServiceServer::new(handler)
        .max_decoding_message_size(MAX_FRAME_BYTES)
        .max_encoding_message_size(MAX_FRAME_BYTES)
}
fn fixed_status(error: RemoteError) -> Status {
    match error {
        RemoteError::Application(error) => fixed_codec::application_status(error),
        other => status(other),
    }
}
fn fixed_error(error: Status) -> RemoteError {
    match fixed_codec::application_error(&error) {
        Ok(Some(error)) => RemoteError::Application(error),
        Ok(None) => remote_status(error),
        Err(error) => error,
    }
}
fn fixed_context(c: Option<wire::Context>) -> Result<crate::remote::grpc::wire::Context, Status> {
    let c = c.ok_or_else(|| fixed_status(RemoteError::InvalidArgument))?;
    if !(1..=60_000).contains(&c.timeout_millis) {
        return Err(fixed_status(RemoteError::InvalidArgument));
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
impl FixedQueryService for Handler {
    async fn open_fixed(
        &self,
        request: tonic::Request<wire::FixedOpenEnvelope>,
    ) -> Result<tonic::Response<wire::FixedOpened>, Status> {
        let r = request.into_inner();
        let context = fixed_context(r.context)?;
        let r = r
            .request
            .ok_or_else(|| fixed_status(RemoteError::InvalidArgument))?
            .try_into()
            .map_err(fixed_status)?;
        match self
            .execute_raw(Some(context), Operation::OpenFixedQuery(r))
            .await
            .map_err(fixed_status)?
        {
            (Reply::OpenFixedQuery(opened), end) => Ok(response(opened.into(), end)),
            _ => Err(fixed_status(RemoteError::Protocol)),
        }
    }
    async fn open_renewable(
        &self,
        request: tonic::Request<wire::RenewableOpenEnvelope>,
    ) -> Result<tonic::Response<wire::RenewableFixedOpened>, Status> {
        let r = request.into_inner();
        let context = fixed_context(r.context)?;
        let r = r
            .request
            .ok_or_else(|| fixed_status(RemoteError::InvalidArgument))?
            .try_into()
            .map_err(fixed_status)?;
        match self
            .execute_raw(Some(context), Operation::OpenRenewableFixedQuery(r))
            .await
            .map_err(fixed_status)?
        {
            (Reply::OpenRenewableFixedQuery(opened), end) => Ok(response(opened.into(), end)),
            _ => Err(fixed_status(RemoteError::Protocol)),
        }
    }
    async fn renew(
        &self,
        request: tonic::Request<wire::FixedRenewEnvelope>,
    ) -> Result<tonic::Response<wire::FixedLeaseGrant>, Status> {
        let r = request.into_inner();
        let context = fixed_context(r.context)?;
        let r = r
            .request
            .ok_or_else(|| fixed_status(RemoteError::InvalidArgument))?
            .try_into()
            .map_err(fixed_status)?;
        match self
            .execute_raw(Some(context), Operation::RenewFixedQuery(r))
            .await
            .map_err(fixed_status)?
        {
            (Reply::RenewFixedQuery(grant), end) => Ok(response(grant.into(), end)),
            _ => Err(fixed_status(RemoteError::Protocol)),
        }
    }
    async fn fragment_fixed(
        &self,
        request: tonic::Request<wire::FixedFragmentEnvelope>,
    ) -> Result<tonic::Response<wire::FixedFragmentResponse>, Status> {
        let r = request.into_inner();
        let context = fixed_context(r.context)?;
        let r = r
            .request
            .ok_or_else(|| fixed_status(RemoteError::InvalidArgument))?
            .try_into()
            .map_err(fixed_status)?;
        match self
            .execute_raw(Some(context), Operation::FixedQueryFragment(r))
            .await
            .map_err(fixed_status)?
        {
            (Reply::FixedQueryFragment(result), end) => {
                let result = match result {
                    Ok(output) => wire::fixed_fragment_response::Result::Output(output.into()),
                    Err(error) => wire::fixed_fragment_response::Result::Failure(
                        wire::QueryFailure::from(error) as i32,
                    ),
                };
                Ok(response(
                    wire::FixedFragmentResponse {
                        result: Some(result),
                    },
                    end,
                ))
            }
            _ => Err(fixed_status(RemoteError::Protocol)),
        }
    }
    async fn close_fixed(
        &self,
        request: tonic::Request<wire::FixedCloseEnvelope>,
    ) -> Result<tonic::Response<wire::Empty>, Status> {
        let r = request.into_inner();
        let context = fixed_context(r.context)?;
        let descriptor = r
            .descriptor
            .ok_or_else(|| fixed_status(RemoteError::InvalidArgument))?
            .try_into()
            .map_err(fixed_status)?;
        match self
            .execute_raw(Some(context), Operation::CloseFixedQuery { descriptor })
            .await
            .map_err(fixed_status)?
        {
            (Reply::CloseFixedQuery, end) => Ok(response(wire::Empty {}, end)),
            _ => Err(fixed_status(RemoteError::Protocol)),
        }
    }
}

#[derive(Clone)]
pub(crate) struct FixedClient {
    client: FixedQueryServiceClient<Channel>,
    identity: RemoteIdentity,
}
impl FixedClient {
    pub(crate) async fn connect(
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
            client: FixedQueryServiceClient::new(channel)
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES),
            identity,
        })
    }
    fn context(&self, end: Instant) -> Result<wire::Context, RemoteError> {
        let millis = end.saturating_duration_since(Instant::now()).as_millis();
        if millis == 0 {
            return Err(RemoteError::Timeout { request_id: None });
        }
        Ok(wire::Context {
            version: VERSION,
            identity: Some(self.identity.into()),
            timeout_millis: millis.min(60_000) as u64,
        })
    }
    fn request<T: Message>(message: T, end: Instant) -> Result<tonic::Request<T>, RemoteError> {
        if message.encoded_len() > MAX_FRAME_BYTES {
            return Err(RemoteError::ResourceExhausted);
        }
        let mut request = tonic::Request::new(message);
        request.set_timeout(end.saturating_duration_since(Instant::now()));
        Ok(request)
    }
    pub(crate) async fn open(
        &self,
        mut request: fixed::FixedOpenRequest,
        duration: Duration,
    ) -> Result<fixed::FixedOpened, RemoteError> {
        let started = Instant::now();
        let end = checked_deadline(duration, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        fixed::preflight(self.identity, &request, end)?;
        let now = Instant::now();
        fixed::narrow_open_deadlines(
            &mut request,
            now - started,
            end.saturating_duration_since(now),
        )?;
        let request = Self::request(
            wire::FixedOpenEnvelope {
                context: Some(self.context(end)?),
                request: Some(request.into()),
            },
            end,
        )?;
        tokio::time::timeout_at(end, self.client.clone().open_fixed(request))
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(fixed_error)?
            .into_inner()
            .try_into()
            .map_err(|_| RemoteError::Protocol)
    }
    pub(crate) async fn renewable_open(
        &self,
        mut request: fixed::RenewableFixedOpenRequest,
        duration: Duration,
    ) -> Result<fixed::RenewableFixedOpened, RemoteError> {
        let started = Instant::now();
        let end = checked_deadline(duration, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        fixed::renewable_preflight(self.identity, &request, end)?;
        let now = Instant::now();
        fixed::narrow_open_deadlines(
            &mut request.open,
            now - started,
            end.saturating_duration_since(now),
        )?;
        request.hard_lifetime_millis = Duration::from_millis(request.hard_lifetime_millis)
            .saturating_sub(now - started)
            .as_millis() as u64;
        let request = Self::request(
            wire::RenewableOpenEnvelope {
                context: Some(self.context(end)?),
                request: Some(request.into()),
            },
            end,
        )?;
        tokio::time::timeout_at(end, self.client.clone().open_renewable(request))
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(fixed_error)?
            .into_inner()
            .try_into()
            .map_err(|_| RemoteError::Protocol)
    }
    pub(crate) async fn renew(
        &self,
        request: fixed::FixedRenewRequest,
        duration: Duration,
    ) -> Result<fixed::FixedLeaseGrant, RemoteError> {
        let end = checked_deadline(duration, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let request = Self::request(
            wire::FixedRenewEnvelope {
                context: Some(self.context(end)?),
                request: Some(request.into()),
            },
            end,
        )?;
        tokio::time::timeout_at(end, self.client.clone().renew(request))
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(fixed_error)?
            .into_inner()
            .try_into()
            .map_err(|_| RemoteError::Protocol)
    }
    pub(crate) async fn fragment(
        &self,
        request: fixed::FixedFragmentRequest,
        duration: Duration,
    ) -> Result<Result<fixed::FixedFragmentOutput, QueryFailure>, RemoteError> {
        let end = checked_deadline(duration, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let descriptor = request.descriptor.clone();
        let sequence = request.request_sequence;
        let request = Self::request(
            wire::FixedFragmentEnvelope {
                context: Some(self.context(end)?),
                request: Some(request.into()),
            },
            end,
        )?;
        let response = tokio::time::timeout_at(
            end,
            self.client
                .clone()
                .max_decoding_message_size(crate::remote::query::MAX_QUERY_RESPONSE_BYTES + 1024)
                .fragment_fixed(request),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
        .map_err(fixed_error)?
        .into_inner();
        match response.result.ok_or(RemoteError::Protocol)? {
            wire::fixed_fragment_response::Result::Output(output) => {
                let output: fixed::FixedFragmentOutput =
                    output.try_into().map_err(|_| RemoteError::Protocol)?;
                if output.descriptor != descriptor || output.request_sequence != sequence {
                    return Err(RemoteError::Protocol);
                }
                Ok(Ok(output))
            }
            wire::fixed_fragment_response::Result::Failure(error) => {
                let error =
                    wire::QueryFailure::try_from(error).map_err(|_| RemoteError::Protocol)?;
                Ok(Err(error.try_into().map_err(|_| RemoteError::Protocol)?))
            }
        }
    }
    pub(crate) async fn close(
        &self,
        descriptor: fixed::FixedDescriptor,
        duration: Duration,
    ) -> Result<(), RemoteError> {
        let end = checked_deadline(duration, None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let request = Self::request(
            wire::FixedCloseEnvelope {
                context: Some(self.context(end)?),
                descriptor: Some(descriptor.into()),
            },
            end,
        )?;
        tokio::time::timeout_at(
            end,
            self.client
                .clone()
                .max_decoding_message_size(crate::remote::query::MAX_QUERY_RESPONSE_BYTES + 1024)
                .close_fixed(request),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
        .map_err(fixed_error)?;
        Ok(())
    }
}
