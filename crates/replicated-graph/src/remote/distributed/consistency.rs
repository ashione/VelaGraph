//! Explicit, fail-closed consistency negotiation over typed coordinator gRPC.
//!
//! This is admission, not MVCC or a distributed transaction implementation.
//! Independent shard snapshots never imply a global committed cut.
use super::*;

/// The visibility guarantee required for the entire query, including job pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadConsistency {
    /// Each shard is frozen independently. The vector need not be a global cut.
    IndependentShardSnapshots,
    /// One complete committed global version. Unsupported by observed queries,
    /// even for a catalog containing only one shard.
    GlobalCommittedSnapshot,
}
impl From<ReadConsistency> for i32 {
    fn from(value: ReadConsistency) -> Self {
        use grpc::wire::ConsistencyRequirement;
        match value {
            ReadConsistency::IndependentShardSnapshots => {
                ConsistencyRequirement::IndependentShardSnapshots as i32
            }
            ReadConsistency::GlobalCommittedSnapshot => {
                ConsistencyRequirement::GlobalCommittedSnapshot as i32
            }
        }
    }
}

/// Execute with an explicit visibility requirement. No downgrade or retry occurs.
/// Negotiated requests use protocol version 2 so older servers reject them before
/// executing work; a successful response must acknowledge the exact requirement.
pub async fn query_with_consistency(
    endpoint: SocketAddr,
    request: DistributedRequest,
    required: ReadConsistency,
    deadline: Duration,
) -> Result<DistributedResponse, RemoteError> {
    validate_request(&request)?;
    let deadline = deadline.min(Duration::from_millis(request.timeout_millis));
    let end = checked_deadline(deadline, None)?;
    tokio::time::timeout_at(end, async {
        let client = grpc::CoordinatorClient::connect_lazy(endpoint, deadline)?;
        client
            .query_with_consistency(
                request,
                required,
                end.saturating_duration_since(Instant::now()),
            )
            .await
    })
    .await
    .unwrap_or(Err(RemoteError::Timeout { request_id: None }))
}

/// Start, advance, read or cancel a job without silently weakening consistency.
/// An unsupported requirement is rejected before allocation or token lookup.
pub async fn job_with_consistency(
    endpoint: SocketAddr,
    request: jobs::Request,
    required: ReadConsistency,
    deadline: Duration,
) -> Result<jobs::Response, RemoteError> {
    if matches!(&request, jobs::Request::Start {text,..} | jobs::Request::StartRows {text,..} if text.len() > query::MAX_QUERY_TEXT_BYTES)
    {
        return Err(RemoteError::ResourceExhausted);
    }
    let deadline = deadline.min(Duration::from_millis(MAX_TIMEOUT));
    let end = checked_deadline(deadline, None)?;
    tokio::time::timeout_at(end, async {
        let client = grpc::CoordinatorClient::connect_lazy(endpoint, deadline)?;
        client
            .job_with_consistency(
                request,
                required,
                end.saturating_duration_since(Instant::now()),
            )
            .await
    })
    .await
    .unwrap_or(Err(RemoteError::Timeout { request_id: None }))
}
