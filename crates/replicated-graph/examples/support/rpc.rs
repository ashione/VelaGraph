//! Typed RPC clients shared by the executable acceptance examples.
//! One caller budget covers connection and operation; writes are never replayed.
use replicated_graph::remote::{
    self,
    distributed::{self, grpc::CoordinatorClient, jobs},
    RemoteError, RemoteIdentity,
};
use std::{net::SocketAddr, time::Duration};
use tokio::time::Instant;

pub async fn probe(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    shard: u64,
    budget: Duration,
) -> Result<(), RemoteError> {
    let end = Instant::now() + budget;
    tokio::time::timeout_at(end, async {
        remote::operations_grpc::Client::connect(endpoint, identity, budget)
            .await?
            .with_deadline(end)
            .probe(shard)
            .await
    })
    .await
    .map_err(|_| RemoteError::Timeout { request_id: None })?
}

pub async fn write(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    request: d2_raft::D1WriteRequest,
    budget: Duration,
) -> Result<d2_raft::D1Result, RemoteError> {
    let id = request.request_id;
    let end = Instant::now() + budget;
    tokio::time::timeout_at(end, async {
        remote::operations_grpc::Client::connect(endpoint, identity, budget)
            .await?
            .with_deadline(end)
            .write(request)
            .await
    })
    .await
    .map_err(|_| RemoteError::Timeout {
        request_id: Some(id),
    })?
}

pub async fn query(
    endpoint: SocketAddr,
    request: distributed::DistributedRequest,
    budget: Duration,
) -> Result<distributed::DistributedResponse, RemoteError> {
    let end = Instant::now() + budget;
    tokio::time::timeout_at(end, async {
        CoordinatorClient::connect(endpoint, budget)
            .await?
            .query(request, end.saturating_duration_since(Instant::now()))
            .await
    })
    .await
    .map_err(|_| RemoteError::Timeout { request_id: None })?
}

pub async fn job(
    endpoint: SocketAddr,
    request: jobs::Request,
    budget: Duration,
) -> Result<jobs::Response, RemoteError> {
    let end = Instant::now() + budget;
    tokio::time::timeout_at(end, async {
        CoordinatorClient::connect(endpoint, budget)
            .await?
            .job(request, end.saturating_duration_since(Instant::now()))
            .await
    })
    .await
    .map_err(|_| RemoteError::Timeout { request_id: None })?
}
