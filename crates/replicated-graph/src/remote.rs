//! Bounded typed gRPC D1 transport for trusted development networks only.
//! No authentication, TLS, forwarding or mutation replay is provided. Only read
//! quorum confirmation has bounded retry within the original deadline. Managed write activation
//! is an explicit irreversible protocol transition, not an authorization boundary.
//! HTTP/2 channels carry typed operations; no legacy protocol detection or fallback.
use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use d2_raft::{D1Command, D1Error, D1Result, D1WriteRequest, LogCommand, StateMachine, TypeConfig};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    task::JoinHandle,
    time::Instant,
};

/// Maximum encoded JSON body in either direction.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Snapshot copies are deliberately smaller than local D1 views.
pub const MAX_SNAPSHOT_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_SNAPSHOT_ROWS: usize = 100_000;
pub const MAX_IN_FLIGHT: usize = 16;
/// Total sockets, including dedicated metadata streams, unary RPC and snapshots.
pub const MAX_GRAPH_CONNECTIONS: usize = 2 * MAX_IN_FLIGHT;
const VERSION: u32 = 1;
/// Largest accepted client or server operation budget.
pub const MAX_DEADLINE: Duration = Duration::from_secs(60);
mod diagnostic;
pub mod distributed;
pub mod managed;
pub(crate) mod metadata_stream_client;
pub(crate) mod metadata_stream_server;
pub mod operations_grpc;
mod paged;
mod read_barrier;
pub use paged::grpc;
pub mod query;
pub mod single_query_grpc;
pub use paged::{
    close_snapshot, grpc_paged_snapshot, open_snapshot, paged_snapshot_identified, snapshot_page,
    SnapshotCursor, SnapshotDescriptor, SnapshotPage, SnapshotStats, MAX_PAGE_BYTES, MAX_PAGE_ROWS,
    MAX_RETAINED_BYTES, MAX_SESSIONS, MAX_SESSION_BYTES, MAX_SESSION_ROWS, SESSION_TTL,
};

static CLIENT_ADMISSION: Semaphore = Semaphore::const_new(MAX_IN_FLIGHT);

/// Transport errors never imply that an already dispatched write was aborted.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum RemoteError {
    #[error("not leader: {leader:?}")]
    NotLeader { leader: Option<u64> },
    #[error("D1 rejected request: {0:?}")]
    Application(D1Error),
    #[error("invalid argument")]
    InvalidArgument,
    #[error("remote resource limit exceeded")]
    ResourceExhausted,
    #[error("remote unavailable; a dispatched write may have committed")]
    Unavailable,
    #[error("deadline exceeded; retain original write identity: {request_id:?}")]
    Timeout { request_id: Option<[u8; 16]> },
    #[error("snapshot session unavailable (closed, expired, or previous server incarnation)")]
    SessionUnavailable,
    #[error("invalid remote protocol")]
    Protocol,
}

/// Expected graph endpoint identity, not authenticated Raft transport identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteIdentity {
    pub group: u64,
    pub node: u64,
}

/// Atomic state-machine copy taken after a leader read barrier.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSnapshot {
    pub shard: u64,
    pub epoch: u64,
    pub sequence: u64,
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity: Option<RemoteIdentity>,
    operation: Operation,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Operation {
    Probe {
        shard: u64,
    },
    Snapshot {
        shard: u64,
    },
    Metadata {
        shard: u64,
    },
    OpenSnapshot {
        shard: u64,
    },
    SnapshotPage {
        descriptor: SnapshotDescriptor,
        cursor: Option<SnapshotCursor>,
    },
    CloseSnapshot {
        descriptor: SnapshotDescriptor,
    },
    Query(query::QueryRequest),
    OpenFixedQuery(distributed::fixed::FixedOpenRequest),
    OpenRenewableFixedQuery(distributed::fixed::RenewableFixedOpenRequest),
    RenewFixedQuery(distributed::fixed::FixedRenewRequest),
    FixedQueryFragment(distributed::fixed::FixedFragmentRequest),
    CloseFixedQuery {
        descriptor: distributed::fixed::FixedDescriptor,
    },
    OpenQuery {
        shard: u64,
        timeout_millis: u64,
    },
    OpenHierarchyQuery {
        shard: u64,
    },
    QueryFragment(distributed::FragmentRequest),
    CloseQuery {
        descriptor: distributed::Descriptor,
    },
    Write(D1WriteRequest),
    Managed(managed::Request),
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    version: u32,
    result: Result<Reply, RemoteError>,
}
#[derive(Serialize, Deserialize)]
enum Reply {
    Probe,
    Snapshot(RemoteSnapshot),
    Metadata(u64, u64, u64),
    OpenSnapshot(SnapshotDescriptor),
    SnapshotPage(SnapshotPage),
    CloseSnapshot,
    Query(query::QueryResponse),
    OpenFixedQuery(distributed::fixed::FixedOpened),
    OpenRenewableFixedQuery(distributed::fixed::RenewableFixedOpened),
    RenewFixedQuery(distributed::fixed::FixedLeaseGrant),
    FixedQueryFragment(Result<distributed::fixed::FixedFragmentOutput, query::QueryFailure>),
    CloseFixedQuery,
    OpenQuery(distributed::Descriptor),
    OpenHierarchyQuery {
        descriptor: distributed::Descriptor,
        max_row_bytes: usize,
    },
    QueryFragment(Result<distributed::FragmentOutput, query::QueryFailure>),
    CloseQuery,
    Write(D1Result),
    Managed(managed::Output),
}

/// Transport-independent graph state. Construction opens no socket and spawns
/// no task. Each bound adapter owns its cleanup/stop loop; adapters share the
/// same session and admission budgets, not independent copies.
pub struct GraphState {
    raft: openraft::Raft<TypeConfig>,
    state: Arc<StateMachine>,
    identity: RemoteIdentity,
    shard: u64,
    deadline: Duration,
    admission: Arc<Semaphore>,
    metadata_streams: Arc<Semaphore>,
    sessions: Arc<paged::Sessions>,
    query_sessions: Arc<distributed::Sessions>,
}
impl GraphState {
    pub fn from_tcp(
        node: &d2_raft::tcp::TcpNode,
        group: u64,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        Self::from_parts(
            node.id,
            node.raft.clone(),
            node.state_machine.clone(),
            group,
            shard,
            deadline,
        )
    }
    pub fn from_managed(
        node: &d2_raft::NodeHandle,
        group: u64,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        if group != node.group_id() {
            return Err(RemoteError::InvalidArgument);
        }
        Self::from_parts(
            node.id,
            node.raft.clone(),
            node.state_machine.clone(),
            group,
            shard,
            deadline,
        )
    }
    fn from_parts(
        node: u64,
        raft: openraft::Raft<TypeConfig>,
        state: Arc<StateMachine>,
        group: u64,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        checked_deadline(deadline, None)?;
        Ok(Self {
            raft,
            state,
            identity: RemoteIdentity { group, node },
            shard,
            deadline,
            admission: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            metadata_streams: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            sessions: Arc::new(paged::Sessions::new()?),
            query_sessions: Arc::new(distributed::Sessions::new()?),
        })
    }
    /// Local diagnostics only; no remote administration endpoint.
    pub fn snapshot_stats(&self) -> SnapshotStats {
        self.sessions.stats()
    }
}

/// Typed graph RPC convenience adapter. No legacy listener or protocol fallback.
/// The shared state keeps the same session/admission budgets as explicit adapters.
pub struct GraphService {
    shared: GraphState,
    transport: grpc::GrpcSnapshotService,
}
impl std::ops::Deref for GraphService {
    type Target = GraphState;
    fn deref(&self) -> &GraphState {
        &self.shared
    }
}
impl GraphService {
    pub async fn start(
        node: &d2_raft::tcp::TcpNode,
        bind: SocketAddr,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        Self::start_identified(node, bind, 0, shard, deadline).await
    }
    pub async fn start_identified(
        node: &d2_raft::tcp::TcpNode,
        bind: SocketAddr,
        group: u64,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        let shared = GraphState::from_tcp(node, group, shard, deadline)?;
        let transport = grpc::GrpcSnapshotService::start(&shared, bind).await?;
        Ok(Self { shared, transport })
    }
    pub async fn start_managed(
        node: &d2_raft::NodeHandle,
        bind: SocketAddr,
        group: u64,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        let shared = GraphState::from_managed(node, group, shard, deadline)?;
        let transport = grpc::GrpcSnapshotService::start(&shared, bind).await?;
        Ok(Self { shared, transport })
    }
    pub fn snapshot_stats(&self) -> SnapshotStats {
        self.shared.snapshot_stats()
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.transport.local_addr()
    }
    /// Submitted Raft commands may still commit after shutdown.
    pub async fn shutdown(self) {
        self.transport.shutdown().await;
    }
}

async fn execute(
    raft: openraft::Raft<TypeConfig>,
    state: Arc<StateMachine>,
    identity: RemoteIdentity,
    shard: u64,
    request: Request,
    sessions: (Arc<paged::Sessions>, Arc<distributed::Sessions>),
    end: Instant,
) -> Result<Reply, RemoteError> {
    let (sessions, query_sessions) = sessions;
    if request.version != VERSION {
        return Err(RemoteError::Protocol);
    }
    if request
        .identity
        .map_or(identity.group != 0, |expected| expected != identity)
    {
        return Err(RemoteError::InvalidArgument);
    }
    if matches!(
        request.operation,
        Operation::OpenFixedQuery(_)
            | Operation::OpenRenewableFixedQuery(_)
            | Operation::RenewFixedQuery(_)
            | Operation::FixedQueryFragment(_)
            | Operation::CloseFixedQuery { .. }
    ) && request.identity != Some(identity)
    {
        return Err(RemoteError::InvalidArgument);
    }
    match request.operation {
        Operation::Managed(request) => managed::handle(raft, state, shard, request)
            .await
            .map(Reply::Managed),
        Operation::OpenFixedQuery(request) => {
            request.validate(shard)?;
            // Start the node lease before preflight/barrier/capture, never afterwards.
            let lease_end = Instant::now() + Duration::from_millis(request.ttl_millis);
            distributed::fixed::preflight(identity, &request, end.min(lease_end))?;
            read_barrier::confirm(&raft, end.min(lease_end), "read_barrier")
                .await
                .map_err(read_barrier::without_leader_hint)?;
            query_sessions
                .open_fixed(
                    &state,
                    identity,
                    shard,
                    request,
                    end.min(lease_end),
                    lease_end,
                )
                .await
                .map(Reply::OpenFixedQuery)
        }
        Operation::OpenRenewableFixedQuery(request) => {
            request.validate(shard)?;
            let admitted = Instant::now();
            let lease_end = admitted + Duration::from_millis(request.open.ttl_millis);
            let hard_end = admitted + Duration::from_millis(request.hard_lifetime_millis);
            let end = end
                .min(lease_end)
                .min(admitted + Duration::from_millis(request.open.acquire_timeout_millis));
            distributed::fixed::renewable_preflight(identity, &request, end)?;
            read_barrier::confirm(&raft, end, "read_barrier")
                .await
                .map_err(read_barrier::without_leader_hint)?;
            query_sessions
                .open_renewable_fixed(
                    &state,
                    identity,
                    shard,
                    request.open,
                    end,
                    lease_end,
                    hard_end,
                )
                .await
                .map(Reply::OpenRenewableFixedQuery)
        }
        Operation::RenewFixedQuery(request) => query_sessions
            .renew_fixed_checked(&state, identity, shard, request, end)
            .await
            .map(Reply::RenewFixedQuery),
        Operation::FixedQueryFragment(request) => {
            query_sessions
                .fragment_fixed(&state, identity, shard, request, end)
                .await
        }
        Operation::CloseFixedQuery { descriptor } => {
            query_sessions.close_fixed(identity, shard, &descriptor)?;
            Ok(Reply::CloseFixedQuery)
        }

        Operation::OpenQuery {
            shard: requested,
            timeout_millis,
        } => {
            if requested != shard || !(1..=5000).contains(&timeout_millis) {
                return Err(RemoteError::InvalidArgument);
            }
            let end = end.min(Instant::now() + Duration::from_millis(timeout_millis));
            read_barrier::confirm(&raft, end, "read_barrier")
                .await
                .map_err(read_barrier::without_leader_hint)?;
            query_sessions
                .open(&state, identity, shard, end)
                .await
                .map(Reply::OpenQuery)
        }
        Operation::OpenHierarchyQuery { shard: requested } => {
            if requested != shard {
                return Err(RemoteError::InvalidArgument);
            }
            let end = end.min(Instant::now() + Duration::from_secs(5));
            tokio::time::timeout_at(end, async {
                read_barrier::confirm(&raft, end, "read_barrier")
                    .await
                    .map_err(read_barrier::without_leader_hint)?;
                query_sessions
                    .open_hierarchy(&state, identity, shard, end)
                    .await
                    .map(|(descriptor, max_row_bytes)| Reply::OpenHierarchyQuery {
                        descriptor,
                        max_row_bytes,
                    })
            })
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
        }
        Operation::QueryFragment(request) => {
            query_sessions
                .fragment(&state, identity, shard, request, end)
                .await
        }
        Operation::CloseQuery { descriptor } => {
            query_sessions.close(identity, shard, &descriptor)?;
            Ok(Reply::CloseQuery)
        }
        Operation::Query(request) => {
            query::execute_query(raft, state, identity, shard, request, end).await
        }
        Operation::OpenSnapshot { shard: requested } => {
            if requested != shard {
                return Err(RemoteError::InvalidArgument);
            }
            let mut phase = diagnostic::Phase::new("open_snapshot_barrier", None, end);
            let barrier = read_barrier::confirm(&raft, end, "open_snapshot_barrier").await;
            if barrier.is_err() {
                phase.finish(&barrier);
            }
            barrier?;
            phase.stage("open_snapshot_capture");
            let result = sessions
                .open(&state, identity, shard, end)
                .await
                .map(Reply::OpenSnapshot);
            phase.finish(&result);
            result
        }
        Operation::SnapshotPage { descriptor, cursor } => {
            let mut phase = diagnostic::Phase::new("snapshot_page_materialize", None, end);
            let result = sessions
                .page(&state, identity, shard, descriptor, cursor, end)
                .await
                .map(Reply::SnapshotPage);
            phase.finish(&result);
            result
        }
        Operation::CloseSnapshot { descriptor } => {
            sessions.close(identity, shard, &descriptor)?;
            Ok(Reply::CloseSnapshot)
        }
        Operation::Probe { shard: requested } => {
            if requested != shard {
                return Err(RemoteError::InvalidArgument);
            }
            read_barrier::confirm(&raft, end, "read_barrier").await?;
            let (actual, _, _) = state
                .d1_metadata()
                .await
                .ok_or(RemoteError::Application(D1Error::NotActivated))?;
            if actual != shard {
                return Err(RemoteError::InvalidArgument);
            }
            Ok(Reply::Probe)
        }
        Operation::Metadata { shard: requested } => {
            #[cfg(test)]
            metadata_benchmark::record(identity.group);
            if requested != shard {
                return Err(RemoteError::InvalidArgument);
            }
            let (actual, epoch, sequence) = state
                .d1_metadata()
                .await
                .ok_or(RemoteError::Application(D1Error::NotActivated))?;
            if actual != shard {
                return Err(RemoteError::InvalidArgument);
            }
            Ok(Reply::Metadata(actual, epoch, sequence))
        }
        Operation::Snapshot { shard: requested } => {
            if requested != shard {
                return Err(RemoteError::InvalidArgument);
            }
            read_barrier::confirm(&raft, end, "read_barrier").await?;
            let view = state
                .d1_read_snapshot(MAX_SNAPSHOT_ROWS, MAX_SNAPSHOT_BYTES)
                .await
                .map_err(|_| RemoteError::ResourceExhausted)?
                .ok_or(RemoteError::Application(D1Error::NotActivated))?;
            if view.shard != shard {
                return Err(RemoteError::InvalidArgument);
            }
            Ok(Reply::Snapshot(RemoteSnapshot {
                shard: view.shard,
                epoch: view.epoch,
                sequence: view.sequence,
                entries: view.entries.into_iter().collect(),
            }))
        }
        Operation::Write(request) => {
            validate_write(&request)?;
            if request.shard != shard {
                return Err(RemoteError::InvalidArgument);
            }
            let response = raft
                .client_write(LogCommand::D1V1(D1Command::Write(request)))
                .await
                .map_err(|e| {
                    if let Some(hint) = e.forward_to_leader::<openraft::BasicNode>() {
                        RemoteError::NotLeader {
                            leader: hint.leader_id,
                        }
                    } else {
                        RemoteError::Unavailable
                    }
                })?;
            let result = response
                .data
                .d1
                .ok_or(RemoteError::Protocol)?
                .map_err(RemoteError::Application)?;
            if !matches!(result, D1Result::Write { .. }) {
                return Err(RemoteError::Protocol);
            }
            Ok(Reply::Write(result))
        }
    }
}
fn validate_write(request: &D1WriteRequest) -> Result<(), RemoteError> {
    if request.operations.is_empty() {
        return Err(RemoteError::Application(D1Error::EmptyBatch));
    }
    if request.epoch == 0 {
        return Err(RemoteError::Application(D1Error::InvalidEpoch));
    }
    let bytes = request.operations.iter().fold(0usize, |n, op| match op {
        d2_raft::WriteOp::Put { key, value } => {
            n.saturating_add(key.len()).saturating_add(value.len())
        }
        d2_raft::WriteOp::Delete { key } => n.saturating_add(key.len()),
    });
    if request.operations.len() > d2_raft::MAX_BATCH_OPERATIONS || bytes > d2_raft::MAX_BATCH_BYTES
    {
        return Err(RemoteError::ResourceExhausted);
    }
    Ok(())
}

/// Fetch a bounded whole-shard snapshot; does not follow leader hints.
/// Validate the configured group and node for a bounded whole-shard copy.
pub async fn snapshot_identified(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    shard: u64,
    deadline: Duration,
) -> Result<RemoteSnapshot, RemoteError> {
    snapshot_with_identity(endpoint, Some(identity), shard, deadline).await
}

async fn snapshot_with_identity(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    shard: u64,
    deadline: Duration,
) -> Result<RemoteSnapshot, RemoteError> {
    match typed_operation(
        endpoint,
        identity,
        Operation::Snapshot { shard },
        deadline,
        None,
    )
    .await?
    {
        Reply::Snapshot(view) => {
            let bytes = view.entries.iter().fold(0usize, |n, (k, v)| {
                n.saturating_add(k.len()).saturating_add(v.len())
            });
            if view.shard != shard
                || view.epoch == 0
                || view.entries.windows(2).any(|w| w[0].0 >= w[1].0)
            {
                return Err(RemoteError::Protocol);
            }
            if view.entries.len() > MAX_SNAPSHOT_ROWS || bytes > MAX_SNAPSHOT_BYTES {
                return Err(RemoteError::ResourceExhausted);
            }
            Ok(view)
        }
        _ => Err(RemoteError::Protocol),
    }
}
/// Check local applied metadata for a pinned view. This is not a leader read
/// barrier and provides the same local-epoch check as the in-process adapter.
/// Fetch metadata with the configured group and node.
pub async fn metadata_identified(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    shard: u64,
    deadline: Duration,
) -> Result<(u64, u64, u64), RemoteError> {
    metadata_with_identity(endpoint, Some(identity), shard, deadline).await
}

async fn metadata_with_identity(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    shard: u64,
    deadline: Duration,
) -> Result<(u64, u64, u64), RemoteError> {
    match typed_operation(
        endpoint,
        identity,
        Operation::Metadata { shard },
        deadline,
        None,
    )
    .await?
    {
        Reply::Metadata(actual, epoch, sequence) if actual == shard && epoch > 0 => {
            Ok((actual, epoch, sequence))
        }
        _ => Err(RemoteError::Protocol),
    }
}
/// Submit exact immutable D1 bytes/identity once. Preserve a copy before calling:
/// timeout, disconnect, or unavailable responses may have an unknown outcome.
/// Write with the configured group and node and original request identity.
pub async fn write_identified(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    request: D1WriteRequest,
    deadline: Duration,
) -> Result<D1Result, RemoteError> {
    write_with_identity(endpoint, Some(identity), request, deadline).await
}

async fn write_with_identity(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    request: D1WriteRequest,
    deadline: Duration,
) -> Result<D1Result, RemoteError> {
    validate_write(&request)?;
    let id = request.request_id;
    match typed_operation(
        endpoint,
        identity,
        Operation::Write(request),
        deadline,
        Some(id),
    )
    .await?
    {
        Reply::Write(result @ D1Result::Write { .. }) => Ok(result),
        _ => Err(RemoteError::Protocol),
    }
}
/// Perform a leader/quorum barrier without copying shard contents.
pub async fn probe_identified(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    shard: u64,
    deadline: Duration,
) -> Result<(), RemoteError> {
    match typed_operation(
        endpoint,
        Some(identity),
        Operation::Probe { shard },
        deadline,
        None,
    )
    .await?
    {
        Reply::Probe => Ok(()),
        _ => Err(RemoteError::Protocol),
    }
}

fn checked_deadline(duration: Duration, id: Option<[u8; 16]>) -> Result<Instant, RemoteError> {
    if duration > MAX_DEADLINE {
        return Err(RemoteError::InvalidArgument);
    }
    if duration.is_zero() {
        return Err(RemoteError::Timeout { request_id: id });
    }
    Instant::now()
        .checked_add(duration)
        .ok_or(RemoteError::InvalidArgument)
}
/// Single-operation typed client. Lazy connection ensures admission precedes
/// any handshake; an absolute cap covers construction, transport and decoding.
async fn typed_operation(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    operation: Operation,
    duration: Duration,
    id: Option<[u8; 16]>,
) -> Result<Reply, RemoteError> {
    let end = checked_deadline(duration, id)?;
    let identity = identity.ok_or(RemoteError::InvalidArgument)?;
    let mut client =
        operations_grpc::Client::connect_lazy(endpoint, identity, duration)?.with_deadline(end);
    Box::pin(client.call(operation))
        .await
        .map_err(|error| match error {
            RemoteError::Timeout { .. } => RemoteError::Timeout { request_id: id },
            other => other,
        })
}

// Preserve the caller's stack, deadline and permits while allowing the
// multithread runtime to replace its worker during bounded JSON CPU work.
fn blocking_json<T>(work: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::try_current().is_ok_and(|handle| {
        matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        )
    }) {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

#[cfg(test)]
mod raw_test_peer;
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn every_operation_checks_group_and_node_before_execution() {
        let dir = tempfile::tempdir().unwrap();
        let node = d2_raft::TcpNode::start(d2_raft::TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: dir.path().into(),
        })
        .await
        .unwrap();
        let service = GraphService::start_identified(
            &node,
            "127.0.0.1:0".parse().unwrap(),
            10,
            7,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let endpoint = service.local_addr();
        let identity = RemoteIdentity { group: 10, node: 1 };
        let request = D1WriteRequest {
            shard: 7,
            epoch: 1,
            request_id: [4; 16],
            expected_sequence: Some(0),
            operations: vec![d2_raft::WriteOp::Put {
                key: vec![1],
                value: vec![2],
            }],
        };
        // Even an uninitialized Raft node must reject identity before attempting
        // a read barrier, metadata lookup, or mutation.
        for wrong in [
            None,
            Some(RemoteIdentity { group: 11, node: 1 }),
            Some(RemoteIdentity { group: 10, node: 2 }),
        ] {
            for operation in [
                Operation::Snapshot { shard: 7 },
                Operation::Metadata { shard: 7 },
                Operation::Probe { shard: 7 },
                Operation::Write(request.clone()),
            ] {
                assert!(matches!(
                    typed_operation(endpoint, wrong, operation, Duration::from_secs(2), None).await,
                    Err(RemoteError::InvalidArgument)
                ));
            }
        }
        assert!(node.state_machine.d1_metadata().await.is_none());
        assert!(matches!(
            metadata_identified(endpoint, identity, 7, Duration::from_secs(2)).await,
            Err(RemoteError::Application(D1Error::NotActivated))
        ));
        service.shutdown().await;
        node.shutdown().await;
    }

    #[tokio::test]
    async fn probe_is_bounded_and_does_not_transfer_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let node = d2_raft::TcpNode::start(d2_raft::TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: dir.path().into(),
        })
        .await
        .unwrap();
        node.bootstrap(std::collections::BTreeMap::from([(
            1,
            node.raft_addr.clone(),
        )]))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if node.raft.ensure_linearizable().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        node.raft
            .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 1 }))
            .await
            .unwrap();
        node.raft
            .client_write(LogCommand::D1V1(D1Command::Write(D1WriteRequest {
                shard: 7,
                epoch: 1,
                request_id: [5; 16],
                expected_sequence: Some(0),
                operations: vec![d2_raft::WriteOp::Put {
                    key: vec![1],
                    value: vec![2; MAX_SNAPSHOT_BYTES + 1],
                }],
            })))
            .await
            .unwrap();
        let service = GraphService::start_identified(
            &node,
            "127.0.0.1:0".parse().unwrap(),
            10,
            7,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let identity = RemoteIdentity { group: 10, node: 1 };
        assert!(
            probe_identified(service.local_addr(), identity, 7, Duration::from_secs(5))
                .await
                .is_ok()
        );
        assert!(matches!(
            snapshot_identified(service.local_addr(), identity, 7, Duration::from_secs(5)).await,
            Err(RemoteError::ResourceExhausted)
        ));
        service.shutdown().await;
        node.shutdown().await;
    }

    #[tokio::test]
    async fn oversized_response_rejected_before_body() {
        let (address, seen, task) = raw_test_peer::spawn(Some(MAX_FRAME_BYTES + 1)).await;
        let result = metadata_identified(
            address,
            RemoteIdentity { group: 10, node: 1 },
            7,
            Duration::from_secs(2),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), seen)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(RemoteError::ResourceExhausted)));
        task.abort();
    }
    #[tokio::test]
    async fn stalled_response_keeps_original_write_identity() {
        let (address, seen, task) = raw_test_peer::spawn(None).await;
        let request = D1WriteRequest {
            shard: 7,
            epoch: 1,
            request_id: [9; 16],
            expected_sequence: Some(0),
            operations: vec![d2_raft::WriteOp::Put {
                key: vec![1],
                value: vec![2],
            }],
        };
        let result = write_identified(
            address,
            RemoteIdentity { group: 10, node: 1 },
            request,
            Duration::from_millis(500),
        )
        .await;
        let frame = tokio::time::timeout(Duration::from_secs(2), seen)
            .await
            .unwrap()
            .unwrap();
        use prost::Message;
        assert_eq!(frame[0], 0);
        assert_eq!(
            u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize,
            frame.len() - 5
        );
        let observed = operations_grpc::wire::Request::decode(&frame[5..]).unwrap();
        let Some(operations_grpc::wire::request::Operation::Write(observed)) = observed.operation
        else {
            panic!("expected typed Write");
        };
        assert_eq!(observed.request_id, vec![9; 16]);
        assert!(
            matches!(result, Err(RemoteError::Timeout { request_id: Some(id) }) if id == [9; 16])
        );
        task.abort();
    }
}
#[cfg(test)]
mod metadata_benchmark;
