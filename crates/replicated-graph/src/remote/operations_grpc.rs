//! Typed single-shard operations on the existing graph gRPC endpoint.
//! No forwarding, retry, legacy fallback or new consistency semantics.
use super::{
    paged::grpc, D1Result, D1WriteRequest, Operation, RemoteError, RemoteIdentity, Reply,
    MAX_DEADLINE, MAX_FRAME_BYTES, VERSION,
};
use prost::Message;
use std::{io, net::SocketAddr, time::Duration};
use tokio::time::{timeout_at, Instant};
use tonic::{transport::Channel, Status};
pub mod wire {
    include!("operations_grpc/operations_proto.rs");
}
pub(crate) mod bounded;
mod codec;
use wire::shard_operations_client::ShardOperationsClient;
use wire::shard_operations_server::{ShardOperations, ShardOperationsServer};

struct Counter {
    bytes: usize,
    end: Instant,
}
impl io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if Instant::now() >= self.end {
            return Err(io::ErrorKind::TimedOut.into());
        }
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_FRAME_BYTES)
            .ok_or(io::ErrorKind::OutOfMemory)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn logical(v: &impl serde::Serialize, end: Instant) -> Result<(), RemoteError> {
    let mut count = Counter { bytes: 0, end };
    serde_json::to_writer(&mut count, v).map_err(|_| {
        if Instant::now() >= end {
            RemoteError::Timeout { request_id: None }
        } else {
            RemoteError::ResourceExhausted
        }
    })?;
    if Instant::now() >= end {
        return Err(RemoteError::Timeout { request_id: None });
    }
    Ok(())
}
fn logical_request(
    identity: RemoteIdentity,
    op: &Operation,
    end: Instant,
) -> Result<(), RemoteError> {
    #[derive(serde::Serialize)]
    struct Request<'a> {
        version: u32,
        identity: Option<RemoteIdentity>,
        operation: &'a Operation,
    }
    logical(
        &Request {
            version: VERSION,
            identity: Some(identity),
            operation: op,
        },
        end,
    )
}
fn logical_response(result: &Result<Reply, RemoteError>, end: Instant) -> Result<(), RemoteError> {
    #[derive(serde::Serialize)]
    struct Response<'a> {
        version: u32,
        result: &'a Result<Reply, RemoteError>,
    }
    logical(
        &Response {
            version: VERSION,
            result,
        },
        end,
    )
}
pub(in crate::remote) fn service(handler: grpc::Handler) -> ShardOperationsServer<grpc::Handler> {
    ShardOperationsServer::new(handler)
        .max_decoding_message_size(MAX_FRAME_BYTES)
        .max_encoding_message_size(MAX_FRAME_BYTES)
}
#[tonic::async_trait]
impl ShardOperations for grpc::Handler {
    async fn call(
        &self,
        request: tonic::Request<wire::Request>,
    ) -> Result<tonic::Response<wire::Response>, Status> {
        let admitted = request
            .extensions()
            .get::<grpc::RequestDeadline>()
            .ok_or_else(|| Status::internal("missing request deadline"))?
            .0;
        let r = request.into_inner();
        if r.version != VERSION {
            return Err(grpc::status(RemoteError::Protocol));
        }
        if r.timeout_millis == 0 || r.timeout_millis > 60_000 {
            return Err(grpc::status(RemoteError::InvalidArgument));
        }
        let end = admitted.min(Instant::now() + Duration::from_millis(r.timeout_millis));
        let identity = r
            .identity
            .ok_or_else(|| grpc::status(RemoteError::InvalidArgument))?;
        let identity = RemoteIdentity {
            group: identity.group,
            node: identity.node,
        };
        let op = codec::operation(
            r.operation
                .ok_or_else(|| grpc::status(RemoteError::InvalidArgument))?,
        )
        .map_err(grpc::status)?;
        logical_request(identity, &op, end).map_err(grpc::status)?;
        let request_id = match &op {
            Operation::Write(w) => Some(w.request_id),
            _ => None,
        };
        let context = grpc::wire::Context {
            version: VERSION,
            identity: Some(identity.into()),
            timeout_millis: r.timeout_millis,
        };
        let result = self
            .execute_until(Some(context), op, end)
            .await
            .map(|(reply, _)| reply)
            .map_err(|e| retain_id(e, request_id));
        logical_response(&result, end).map_err(grpc::status)?;
        let response = codec::response(result).map_err(grpc::status)?;
        if response.encoded_len() > MAX_FRAME_BYTES {
            return Err(grpc::status(RemoteError::ResourceExhausted));
        }
        if Instant::now() >= end {
            return Err(grpc::status(RemoteError::Timeout { request_id }));
        }
        Ok(grpc::response(response, end))
    }
}
fn retain_id(e: RemoteError, id: Option<[u8; 16]>) -> RemoteError {
    match e {
        RemoteError::Timeout { .. } => RemoteError::Timeout { request_id: id },
        e => e,
    }
}
/// One RPC per operation; the original write identity survives ambiguous timeouts.
/// Connect and each operation have separate bounded budgets; operations never replay.
pub struct Client {
    channel: Channel,
    identity: RemoteIdentity,
    timeout: Duration,
}
impl Client {
    pub async fn connect(
        address: SocketAddr,
        identity: RemoteIdentity,
        timeout: Duration,
    ) -> Result<Self, RemoteError> {
        if timeout.is_zero() || timeout > MAX_DEADLINE {
            return Err(RemoteError::InvalidArgument);
        }
        let end = Instant::now() + timeout;
        let channel = timeout_at(
            end,
            Channel::from_shared(format!("http://{address}"))
                .map_err(|_| RemoteError::InvalidArgument)?
                .connect(),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
        .map_err(|_| RemoteError::Unavailable)?;
        Ok(Self {
            channel,
            identity,
            timeout,
        })
    }
    async fn call(&mut self, op: Operation) -> Result<Reply, RemoteError> {
        let end = Instant::now() + self.timeout;
        let id = match &op {
            Operation::Write(w) => Some(w.request_id),
            _ => None,
        };
        let future = async {
            let _permit = super::CLIENT_ADMISSION
                .try_acquire()
                .map_err(|_| RemoteError::ResourceExhausted)?;
            logical_request(self.identity, &op, end)?;
            let remaining = end.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RemoteError::Timeout { request_id: id });
            }
            let millis = remaining
                .as_millis()
                .max(1)
                .try_into()
                .map_err(|_| RemoteError::InvalidArgument)?;
            let request = codec::request(self.identity, millis, op)?;
            if request.encoded_len() > MAX_FRAME_BYTES {
                return Err(RemoteError::ResourceExhausted);
            }
            let mut request = tonic::Request::new(request);
            request.set_timeout(remaining);
            let mut client = ShardOperationsClient::new(self.channel.clone())
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES);
            let response = client
                .call(request)
                .await
                .map_err(grpc::remote_status)?
                .into_inner();
            let result = codec::reply(response)?;
            logical_response(&result, end)?;
            result
        };
        timeout_at(end, future)
            .await
            .map_err(|_| RemoteError::Timeout { request_id: id })?
            .map_err(|e| retain_id(e, id))
    }
    pub async fn probe(&mut self, shard: u64) -> Result<(), RemoteError> {
        match self.call(Operation::Probe { shard }).await? {
            Reply::Probe => Ok(()),
            _ => Err(RemoteError::Protocol),
        }
    }
    pub async fn metadata(&mut self, shard: u64) -> Result<(u64, u64, u64), RemoteError> {
        match self.call(Operation::Metadata { shard }).await? {
            Reply::Metadata(s, e, q) => Ok((s, e, q)),
            _ => Err(RemoteError::Protocol),
        }
    }
    pub async fn write(&mut self, request: D1WriteRequest) -> Result<D1Result, RemoteError> {
        match self.call(Operation::Write(request)).await? {
            Reply::Write(v) => Ok(v),
            _ => Err(RemoteError::Protocol),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_write_boundary_matches_legacy_request_not_compact_protobuf() {
        let identity = RemoteIdentity { group: 1, node: 2 };
        let make = |len| super::super::Request {
            version: VERSION,
            identity: Some(identity),
            operation: Operation::Write(D1WriteRequest {
                shard: 7,
                epoch: 1,
                request_id: [1; 16],
                expected_sequence: None,
                operations: vec![d2_raft::WriteOp::Put {
                    key: vec![],
                    value: vec![255; len],
                }],
            }),
        };
        let empty_len = serde_json::to_vec(&make(0)).unwrap().len();
        // [255,...] adds four bytes per value minus the first comma.
        let n = (MAX_FRAME_BYTES - empty_len + 1) / 4;
        for (len, fits) in [(n, true), (n + 1, false)] {
            let original = make(len);
            let original_len = serde_json::to_vec(&original).unwrap().len();
            assert_eq!(original_len <= MAX_FRAME_BYTES, fits);
            assert_eq!(
                logical_request(identity, &original.operation, Instant::now() + MAX_DEADLINE)
                    .is_ok(),
                fits
            );
            let encoded = codec::request(identity, 60_000, original.operation).unwrap();
            assert!(encoded.encoded_len() < MAX_FRAME_BYTES);
        }
    }
    #[test]
    fn logical_budget_and_deadline() {
        assert!(matches!(
            logical(&"x", Instant::now()),
            Err(RemoteError::Timeout { .. })
        ));
        let value = "x".repeat(MAX_FRAME_BYTES);
        assert!(matches!(
            logical(&value, Instant::now() + MAX_DEADLINE),
            Err(RemoteError::ResourceExhausted)
        ));
    }
}
