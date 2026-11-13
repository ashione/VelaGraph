//! Typed single-partition text query: the original executor, never a TCP proxy.
use super::{distributed::grpc::wire, paged::grpc, query::*, *};
use prost::Message;
use tonic::{transport::Channel, Status};
use wire::single_query_service_client::SingleQueryServiceClient;
use wire::single_query_service_server::{SingleQueryService, SingleQueryServiceServer};
const REQUEST_LIMIT: usize = MAX_QUERY_TEXT_BYTES + 1024;
const RESPONSE_LIMIT: usize = MAX_QUERY_RESPONSE_BYTES;
pub(in crate::remote) fn service(
    handler: grpc::Handler,
) -> SingleQueryServiceServer<grpc::Handler> {
    SingleQueryServiceServer::new(handler)
        .max_decoding_message_size(REQUEST_LIMIT)
        .max_encoding_message_size(RESPONSE_LIMIT)
}
fn mode(v: i32) -> Result<QueryMode, RemoteError> {
    match wire::SingleQueryMode::try_from(v) {
        Ok(wire::SingleQueryMode::V1) => Ok(QueryMode::V1),
        Ok(wire::SingleQueryMode::V2Observed) => Ok(QueryMode::V2Observed),
        _ => Err(RemoteError::Protocol),
    }
}
fn encode_mode(v: QueryMode) -> i32 {
    match v {
        QueryMode::V1 => 1,
        QueryMode::V2Observed => 2,
    }
}
fn encode(v: QueryResponse) -> wire::SingleQueryResponse {
    use wire::single_query_response::Result;
    wire::SingleQueryResponse {
        result: Some(match v {
            Ok(v) => Result::Output(wire::SingleQueryOutput {
                partition: v.partition,
                mode: encode_mode(v.mode),
                completeness: match v.completeness {
                    QueryCompleteness::SinglePartition => {
                        wire::Completeness::SinglePartition as i32
                    }
                    QueryCompleteness::Unverified => wire::Completeness::Unverified as i32,
                },
                snapshot: Some(v.snapshot.into()),
                rows: v.rows.into_iter().map(Into::into).collect(),
            }),
            Err(e) => Result::Failure(wire::QueryFailure::from(e) as i32),
        }),
    }
}
fn decode(v: wire::SingleQueryResponse) -> Result<QueryResponse, RemoteError> {
    use wire::single_query_response::Result as R;
    Ok(match v.result.ok_or(RemoteError::Protocol)? {
        R::Failure(e) => Err(wire::QueryFailure::try_from(e)
            .map_err(|_| RemoteError::Protocol)?
            .try_into()?),
        R::Output(v) => Ok(QueryOutput {
            partition: v.partition,
            mode: mode(v.mode)?,
            completeness: match wire::Completeness::try_from(v.completeness) {
                Ok(wire::Completeness::SinglePartition) => QueryCompleteness::SinglePartition,
                Ok(wire::Completeness::Unverified) => QueryCompleteness::Unverified,
                _ => return Err(RemoteError::Protocol),
            },
            snapshot: v.snapshot.ok_or(RemoteError::Protocol)?.try_into()?,
            rows: v
                .rows
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        }),
    })
}
#[tonic::async_trait]
impl SingleQueryService for grpc::Handler {
    async fn query(
        &self,
        request: tonic::Request<wire::SingleQueryRequest>,
    ) -> Result<tonic::Response<wire::SingleQueryResponse>, Status> {
        let end = request
            .extensions()
            .get::<grpc::RequestDeadline>()
            .ok_or_else(|| Status::internal("missing request deadline"))?
            .0;
        let v = request.into_inner();
        let context = v
            .context
            .ok_or_else(|| grpc::status(RemoteError::InvalidArgument))?;
        let request = QueryRequest {
            shard: v.shard,
            partition: v.partition,
            mode: mode(v.mode).map_err(grpc::status)?,
            text: v.text,
            timeout_millis: context.timeout_millis,
        };
        request.validate().map_err(grpc::status)?;
        let identity = context
            .identity
            .as_ref()
            .ok_or_else(|| grpc::status(RemoteError::InvalidArgument))?;
        let identity = RemoteIdentity {
            group: identity.group,
            node: identity.node,
        };
        let expected = request.clone();
        let context = grpc::wire::Context {
            version: context.version,
            identity: Some(identity.into()),
            timeout_millis: context.timeout_millis,
        };
        let (reply, end) = self
            .execute_until(Some(context), Operation::Query(request), end)
            .await
            .map_err(grpc::status)?;
        let Reply::Query(result) = reply else {
            return Err(grpc::status(RemoteError::Protocol));
        };
        validate_rpc_response(&expected, &result, identity, end).map_err(grpc::status)?;
        let encoded = encode(result);
        if encoded.encoded_len() > RESPONSE_LIMIT {
            return Err(grpc::status(RemoteError::ResourceExhausted));
        }
        Ok(grpc::response(encoded, end))
    }
}
/// One explicitly addressed call. Connect/decode/validation share one total budget.
pub async fn query(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    request: QueryRequest,
    deadline: Duration,
) -> Result<QueryResponse, RemoteError> {
    let end = checked_deadline(deadline, None)?;
    request.validate()?;
    let future = async {
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let channel = Channel::from_shared(format!("http://{endpoint}"))
            .map_err(|_| RemoteError::InvalidArgument)?
            .connect_timeout(deadline)
            .buffer_size(MAX_IN_FLIGHT)
            .concurrency_limit(MAX_IN_FLIGHT)
            .connect()
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let remaining = end.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(RemoteError::Timeout { request_id: None });
        }
        let mut wire_request = tonic::Request::new(wire::SingleQueryRequest {
            context: Some(wire::Context {
                version: VERSION,
                identity: Some(wire::Identity {
                    group: identity.group,
                    node: identity.node,
                }),
                timeout_millis: request.timeout_millis,
            }),
            shard: request.shard,
            partition: request.partition,
            mode: encode_mode(request.mode),
            text: request.text.clone(),
        });
        wire_request.set_timeout(remaining);
        let reply = SingleQueryServiceClient::new(channel)
            .max_encoding_message_size(REQUEST_LIMIT)
            .max_decoding_message_size(RESPONSE_LIMIT)
            .query(wire_request)
            .await
            .map_err(grpc::remote_status)?
            .into_inner();
        let result = decode(reply)?;
        validate_rpc_response(&request, &result, identity, end)?;
        Ok(result)
    };
    tokio::time::timeout_at(end, future)
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_roundtrip_and_strict_modes() {
        for mode in [QueryMode::V1, QueryMode::V2Observed] {
            let out = QueryOutput {
                partition: 4,
                mode,
                completeness: if mode == QueryMode::V1 {
                    QueryCompleteness::SinglePartition
                } else {
                    QueryCompleteness::Unverified
                },
                snapshot: QueryStamp {
                    identity: RemoteIdentity { group: 1, node: 2 },
                    shard: 7,
                    epoch: 3,
                    sequence: 0,
                },
                rows: vec![QueryRow::Count(0)],
            };
            assert_eq!(decode(encode(Ok(out.clone()))).unwrap(), Ok(out));
        }
        assert!(decode(wire::SingleQueryResponse { result: None }).is_err());
        assert!(mode(0).is_err());
        assert!(mode(99).is_err());
        for failure in [
            QueryFailure::Parse,
            QueryFailure::Unsupported,
            QueryFailure::Timeout,
            QueryFailure::ResourceExhausted,
        ] {
            assert_eq!(decode(encode(Err(failure))).unwrap(), Err(failure));
        }
    }
}
