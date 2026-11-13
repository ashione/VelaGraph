//! Bounded plaintext TCP D1 transport for trusted development networks only.
//! No authentication, TLS, forwarding or mutation replay is provided. Only read
//! quorum confirmation has bounded retry within the original deadline. Managed write activation
//! is an explicit irreversible protocol transition, not an authorization boundary.
//! Ordinary connections carry one request/response. Opt-in hierarchy channels
//! reuse a bounded, sequential connection tied to one fixed query session.
use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use d2_raft::{D1Command, D1Error, D1Result, D1WriteRequest, LogCommand, StateMachine, TypeConfig};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    task::{JoinHandle, JoinSet},
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
    close_snapshot, open_snapshot, paged_snapshot, paged_snapshot_identified, snapshot_page,
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
    OpenHierarchyChannel {
        version: u32,
        request: distributed::FragmentRequest,
    },
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

/// Explicit legacy TCP adapter for library/parity callers. Construct GraphState
/// and a typed gRPC adapter instead for a graph node without a legacy listener.
/// Shutdown cancels/joins handlers; submitted Raft writes may still commit.
pub struct GraphService {
    shared: GraphState,
    address: SocketAddr,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
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

    /// Bind graph RPC identity. Nonzero groups require identified requests.
    /// Separate Raft groups must still use independently configured TCP peers.
    pub async fn start_identified(
        node: &d2_raft::tcp::TcpNode,
        bind: SocketAddr,
        group: u64,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        Self::start_parts(
            node.id,
            node.raft.clone(),
            node.state_machine.clone(),
            bind,
            group,
            shard,
            deadline,
        )
        .await
    }

    /// Serve the exact same Raft/state handle as a managed-RPC endpoint. The
    /// caller fixes both ports and identities in trusted server configuration.
    /// This adds neither authentication nor automatic endpoint discovery.
    pub async fn start_managed(
        node: &d2_raft::NodeHandle,
        bind: SocketAddr,
        group: u64,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        if group != node.group_id() {
            return Err(RemoteError::InvalidArgument);
        }
        Self::start_parts(
            node.id,
            node.raft.clone(),
            node.state_machine.clone(),
            bind,
            group,
            shard,
            deadline,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_parts(
        node: u64,
        raft: openraft::Raft<TypeConfig>,
        state: Arc<StateMachine>,
        bind: SocketAddr,
        group: u64,
        shard: u64,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        let shared =
            GraphState::from_parts(node, raft.clone(), state.clone(), group, shard, deadline)?;
        let identity = shared.identity;
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let address = listener
            .local_addr()
            .map_err(|_| RemoteError::Unavailable)?;
        let worker_sessions = shared.sessions.clone();
        let query_sessions = shared.query_sessions.clone();
        let (stop, mut stopped) = watch::channel(false);
        let admission = shared.admission.clone();
        let task = tokio::spawn(async move {
            let sessions = worker_sessions;
            let mut cleanup = tokio::time::interval(Duration::from_millis(100));
            let mut handlers = JoinSet::new();
            loop {
                while let Some(result) = handlers.try_join_next() {
                    if let Err(error) = result {
                        diagnostic::failure(format_args!(
                            "phase=handler_join panic={} cancelled={}",
                            error.is_panic(),
                            error.is_cancelled()
                        ));
                    }
                }
                let accepted = tokio::select! {
                    _ = stopped.changed() => break,
                    _ = cleanup.tick() => { sessions.expire(); query_sessions.expire(); continue; },
                    result = handlers.join_next(), if !handlers.is_empty() => { if let Some(Err(error)) = result { diagnostic::failure(format_args!("phase=handler_join panic={} cancelled={}", error.is_panic(), error.is_cancelled())); } continue; },
                    result = listener.accept() => result,
                };
                let Ok((mut socket, peer)) = accepted else {
                    break;
                };
                let Ok(permit) = admission.clone().try_acquire_owned() else {
                    diagnostic::failure(format_args!(
                        "phase=server_admission peer={peer} reason=resource_exhausted"
                    ));
                    continue;
                };
                let raft = raft.clone();
                let state = state.clone();
                let sessions = sessions.clone();
                let query_sessions = query_sessions.clone();
                let end = Instant::now() + deadline;
                handlers.spawn(async move {
                    let _permit = permit;
                    let request =
                        match tokio::time::timeout_at(end, read_frame::<Request>(&mut socket, end))
                            .await
                        {
                            Ok(Ok(request)) => request,
                            _ => {
                                diagnostic::failure(format_args!(
                                    "phase=server_request_read peer={peer} expired={}",
                                    Instant::now() >= end
                                ));
                                return;
                            }
                        };
                    let op = diagnostic::operation(&request.operation);
                    let request = match request {
                        Request {
                            version: VERSION,
                            identity: Some(expected),
                            operation: Operation::OpenHierarchyChannel { version, request },
                        } if expected == identity => {
                            let _ = distributed::channel::serve(
                                socket,
                                state,
                                query_sessions,
                                (identity, shard),
                                version,
                                request,
                                (end, deadline),
                            )
                            .await;
                            return;
                        }
                        request => request,
                    };
                    // A query's shorter budget also governs response encoding and
                    // sending; it cannot regain the longer connection deadline.
                    let end = match &request.operation {
                        Operation::Query(query) if (1..=5000).contains(&query.timeout_millis) => {
                            end.min(Instant::now() + Duration::from_millis(query.timeout_millis))
                        }
                        Operation::OpenQuery { timeout_millis, .. }
                            if (1..=5000).contains(timeout_millis) =>
                        {
                            end.min(Instant::now() + Duration::from_millis(*timeout_millis))
                        }
                        Operation::OpenFixedQuery(request)
                            if (1..=5000).contains(&request.acquire_timeout_millis) =>
                        {
                            end.min(
                                Instant::now()
                                    + Duration::from_millis(request.acquire_timeout_millis),
                            )
                        }
                        Operation::OpenRenewableFixedQuery(request)
                            if (1..=5000).contains(&request.open.acquire_timeout_millis) =>
                        {
                            end.min(
                                Instant::now()
                                    + Duration::from_millis(request.open.acquire_timeout_millis),
                            )
                        }
                        Operation::OpenHierarchyQuery { .. } => {
                            end.min(Instant::now() + Duration::from_secs(5))
                        }
                        _ => end,
                    };
                    // Dropping the waiting future at the deadline does not undo
                    // a command already submitted to Raft.
                    if Instant::now() >= end {
                        diagnostic::failure(format_args!(
                            "phase=server_before_execute op={op} peer={peer} expired=true"
                        ));
                        return;
                    }
                    let result = match tokio::time::timeout_at(
                        end,
                        execute(
                            raft,
                            state,
                            identity,
                            shard,
                            request,
                            (sessions, query_sessions),
                            end,
                        ),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            diagnostic::failure(format_args!(
                                "phase=server_execute op={op} peer={peer} reason=deadline"
                            ));
                            return;
                        }
                    };
                    if let Err(error) = &result {
                        diagnostic::failure(format_args!(
                            "phase=server_execute op={op} peer={peer} reason={}",
                            diagnostic::kind(error)
                        ));
                    }
                    let response = Response {
                        version: VERSION,
                        result,
                    };
                    match tokio::time::timeout_at(end, write_frame(&mut socket, &response, end))
                        .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => diagnostic::failure(format_args!(
                            "phase=server_response op={op} peer={peer} reason={}",
                            diagnostic::kind(&error)
                        )),
                        Err(_) => diagnostic::failure(format_args!(
                            "phase=server_response op={op} peer={peer} reason=deadline"
                        )),
                    }
                });
            }
            handlers.abort_all();
            while handlers.join_next().await.is_some() {}
        });
        Ok(Self {
            shared,
            address,
            stop,
            task: Some(task),
        })
    }
    /// Local diagnostic counters; not a remote administration endpoint.
    pub fn snapshot_stats(&self) -> SnapshotStats {
        self.sessions.stats()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    /// Stop accepting and cancel/join connection handlers. Already submitted
    /// Raft commands may still commit; retain original write identities.
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for GraphService {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
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
        Operation::OpenHierarchyChannel { .. } => Err(RemoteError::InvalidArgument),
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
pub async fn snapshot(
    endpoint: SocketAddr,
    shard: u64,
    deadline: Duration,
) -> Result<RemoteSnapshot, RemoteError> {
    snapshot_with_identity(endpoint, None, shard, deadline).await
}

/// Identified variant of [`snapshot`]; validates the configured group and node.
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
    match rpc(
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
pub async fn metadata(
    endpoint: SocketAddr,
    shard: u64,
    deadline: Duration,
) -> Result<(u64, u64, u64), RemoteError> {
    metadata_with_identity(endpoint, None, shard, deadline).await
}

/// Identified variant of [`metadata`]; validates the configured group and node.
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
    match rpc(
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
pub async fn write(
    endpoint: SocketAddr,
    request: D1WriteRequest,
    deadline: Duration,
) -> Result<D1Result, RemoteError> {
    write_with_identity(endpoint, None, request, deadline).await
}

/// Identified variant of [`write`]; validates the configured group and node.
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
    match rpc(
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
    match rpc(
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
async fn rpc(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    operation: Operation,
    duration: Duration,
    id: Option<[u8; 16]>,
) -> Result<Reply, RemoteError> {
    let end = checked_deadline(duration, id)?;
    let op = diagnostic::operation(&operation);
    let response_limit = if matches!(
        &operation,
        Operation::Query(_)
            | Operation::OpenQuery { .. }
            | Operation::OpenHierarchyQuery { .. }
            | Operation::OpenHierarchyChannel { .. }
            | Operation::FixedQueryFragment(_)
            | Operation::CloseFixedQuery { .. }
            | Operation::QueryFragment(_)
            | Operation::CloseQuery { .. }
    ) {
        query::MAX_QUERY_RESPONSE_BYTES + 1024
    } else {
        MAX_FRAME_BYTES
    };
    let _permit = CLIENT_ADMISSION
        .try_acquire()
        .map_err(|_| RemoteError::ResourceExhausted)?;
    tokio::time::timeout_at(end, async {
        let mut stream = TcpStream::connect(endpoint).await.map_err(|e| {
            diagnostic::failure(format_args!(
                "phase=connect op={op} peer={endpoint} io_kind={:?} os={:?}",
                e.kind(),
                e.raw_os_error()
            ));
            RemoteError::Unavailable
        })?;
        write_frame(
            &mut stream,
            &Request {
                version: VERSION,
                identity,
                operation,
            },
            end,
        )
        .await?;
        let response: Response = read_frame_limited(&mut stream, end, response_limit).await?;
        if response.version != VERSION {
            return Err(RemoteError::Protocol);
        }
        response.result
    })
    .await
    .unwrap_or(Err(RemoteError::Timeout { request_id: id }))
    .map_err(|error| {
        diagnostic::failure(format_args!(
            "phase=client_rpc op={op} peer={endpoint} reason={}",
            diagnostic::kind(&error)
        ));
        match error {
            RemoteError::Timeout { .. } => RemoteError::Timeout { request_id: id },
            other => other,
        }
    })
}

struct BoundedWriter(Vec<u8>, Instant);
impl io::Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if Instant::now() >= self.1 {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline"));
        }
        if bytes.len() > MAX_FRAME_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("frame limit"));
        }
        let needed = self.0.len() + bytes.len();
        if needed > self.0.capacity() {
            let capacity = needed
                .max(self.0.capacity().saturating_mul(2))
                .min(MAX_FRAME_BYTES);
            self.0.reserve_exact(capacity - self.0.len());
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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

// Tiny frames have a fixed, conservative CPU-input bound. Avoid handing off a
// runtime worker for these short parses; larger frames retain the blocking
// boundary. This is a byte threshold, independent of protocol or graph shape.
const INLINE_JSON_BYTES: usize = 4096;
fn decode_frame_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    if bytes.len() <= INLINE_JSON_BYTES {
        serde_json::from_slice(bytes)
    } else {
        blocking_json(|| serde_json::from_slice(bytes))
    }
}

async fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
    end: Instant,
) -> Result<(), RemoteError> {
    let mut phase = diagnostic::Phase::new("encode", Some(stream), end);
    let buffer = blocking_json(|| {
        let mut buffer = BoundedWriter(Vec::new(), end);
        {
            let mut buffered = io::BufWriter::with_capacity(8192, &mut buffer);
            serde_json::to_writer(&mut buffered, value)
                .and_then(|()| io::Write::flush(&mut buffered).map_err(serde_json::Error::io))
                .map_err(|_| {
                    if Instant::now() >= end {
                        RemoteError::Timeout { request_id: None }
                    } else {
                        RemoteError::ResourceExhausted
                    }
                })?;
        }
        Ok::<_, RemoteError>(buffer)
    });
    if buffer.is_err() {
        phase.finish(&buffer);
    }
    let buffer = buffer?;
    if Instant::now() >= end {
        return Err(RemoteError::Timeout { request_id: None });
    }
    phase.stage("write_header");
    stream.write_u32(buffer.0.len() as u32).await.map_err(|e| {
        phase.io(&e);
        RemoteError::Unavailable
    })?;
    phase.stage("write_body");
    let result = stream.write_all(&buffer.0).await.map_err(|e| {
        phase.io(&e);
        RemoteError::Unavailable
    });
    phase.finish(&result);
    result
}
async fn read_frame<T: DeserializeOwned>(
    stream: &mut TcpStream,
    end: Instant,
) -> Result<T, RemoteError> {
    read_frame_limited(stream, end, MAX_FRAME_BYTES).await
}
async fn read_frame_limited<T: DeserializeOwned>(
    stream: &mut TcpStream,
    end: Instant,
    limit: usize,
) -> Result<T, RemoteError> {
    let mut phase = diagnostic::Phase::new("read_header", Some(stream), end);
    let size = stream.read_u32().await.map_err(|e| {
        phase.io(&e);
        RemoteError::Unavailable
    })? as usize;
    if size == 0 || size > limit {
        phase.finish(&Err::<(), _>(RemoteError::ResourceExhausted));
        return Err(RemoteError::ResourceExhausted);
    }
    let mut bytes = vec![0; size];
    phase.stage("read_body");
    stream.read_exact(&mut bytes).await.map_err(|e| {
        phase.io(&e);
        RemoteError::Unavailable
    })?;
    if Instant::now() >= end {
        return Err(RemoteError::Timeout { request_id: None });
    }
    phase.stage("decode");
    let result = decode_frame_json(&bytes).map_err(|_| RemoteError::Protocol);
    if Instant::now() >= end {
        return Err(RemoteError::Timeout { request_id: None });
    }
    phase.finish(&result);
    result
}

#[cfg(test)]
mod frame_decode_tests;
#[cfg(test)]
mod frame_scheduler_probe;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_encoding_and_strict_requests() {
        let mut writer = BoundedWriter(
            vec![0; MAX_FRAME_BYTES - 1],
            Instant::now() + Duration::from_secs(5),
        );
        assert!(io::Write::write_all(&mut writer, &[1, 2]).is_err());
        assert_eq!(writer.0.len(), MAX_FRAME_BYTES - 1);
        assert!(serde_json::from_str::<Request>(
            r#"{"version":1,"operation":{"Snapshot":{"shard":7,"extra":1}}}"#
        )
        .is_err());
        assert!(serde_json::from_str::<Request>(
            r#"{"version":1,"operation":{"Activate":{"shard":7,"epoch":1}}}"#
        )
        .is_err());
    }

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
                    rpc(endpoint, wrong, operation, Duration::from_secs(2), None).await,
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
    async fn oversized_frame_rejected_before_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.write_u32(MAX_FRAME_BYTES as u32 + 1).await.unwrap();
        });
        let mut stream = TcpStream::connect(address).await.unwrap();
        assert!(matches!(
            read_frame::<Response>(&mut stream, Instant::now() + Duration::from_secs(5)).await,
            Err(RemoteError::ResourceExhausted)
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn stalled_response_keeps_original_write_identity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _: Request = read_frame(&mut socket, Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            socket.write_all(&[0, 0]).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
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
        assert!(matches!(
            write(address, request, Duration::from_millis(100)).await,
            Err(RemoteError::Timeout {
                request_id: Some([9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9])
            })
        ));
        task.abort();
    }
    #[tokio::test]
    async fn service_bounds_stalled_inputs_and_validates_identity() {
        let dir = tempfile::tempdir().unwrap();
        let node = d2_raft::TcpNode::start(d2_raft::TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: dir.path().into(),
        })
        .await
        .unwrap();
        let service = GraphService::start(
            &node,
            "127.0.0.1:0".parse().unwrap(),
            7,
            Duration::from_millis(150),
        )
        .await
        .unwrap();
        let address = service.local_addr();
        for prefix in [vec![0], vec![0, 0, 0, 100, b'{']] {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(&prefix).await.unwrap();
            let mut byte = [0];
            let count = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(count, 0);
        }
        for (version, shard) in [(2, 7), (1, 8)] {
            let mut stream = TcpStream::connect(address).await.unwrap();
            write_frame(
                &mut stream,
                &Request {
                    version,
                    identity: None,
                    operation: Operation::Snapshot { shard },
                },
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
            let response: Response =
                read_frame(&mut stream, Instant::now() + Duration::from_secs(5))
                    .await
                    .unwrap();
            assert!(matches!(
                response.result,
                Err(RemoteError::Protocol | RemoteError::InvalidArgument)
            ));
        }
        service.shutdown().await;
        node.shutdown().await;
    }
}

#[cfg(test)]
mod scheduling_regression {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn graph_frame_encoding_keeps_async_worker_available() {
        struct GatedValue {
            entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl Serialize for GatedValue {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.entered
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .map_err(serde::ser::Error::custom)?;
                serializer.serialize_u64(42)
            }
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, receiver) = std::sync::mpsc::channel();
        let writer = tokio::spawn(async move {
            write_frame(
                &mut server,
                &GatedValue {
                    entered: std::sync::Mutex::new(Some(entered)),
                    release: std::sync::Mutex::new(receiver),
                },
                Instant::now() + Duration::from_secs(5),
            )
            .await
        });
        let releaser = tokio::spawn(async move {
            ready.await.unwrap();
            release.send(()).unwrap();
        });
        writer.await.unwrap().unwrap();
        releaser.await.unwrap();
        assert_eq!(
            read_frame::<u64>(&mut client, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap(),
            42
        );
    }
}

#[cfg(test)]
mod transport_boundary_tests {
    use super::*;
    async fn sockets() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }
    #[test]
    fn frame_decode_matches_json_across_inline_boundary() {
        for n in [
            0,
            1,
            INLINE_JSON_BYTES - 3,
            INLINE_JSON_BYTES - 2,
            INLINE_JSON_BYTES - 1,
            INLINE_JSON_BYTES,
            INLINE_JSON_BYTES + 1,
            8192,
        ] {
            let text = "x".repeat(n);
            let bytes = serde_json::to_vec(&text).unwrap();
            assert_eq!(decode_frame_json::<String>(&bytes).unwrap(), text);
            let mut corrupt = bytes.clone();
            corrupt.push(b'!');
            assert!(decode_frame_json::<String>(&corrupt).is_err());
        }
        for value in [
            serde_json::json!({"s":"\u{0000}\n雪🚀".repeat(1024)}),
            serde_json::json!([null, true, i64::MIN, u64::MAX, 0.125]),
        ] {
            let bytes = serde_json::to_vec(&value).unwrap();
            assert_eq!(
                decode_frame_json::<serde_json::Value>(&bytes).unwrap(),
                value
            );
        }
        for invalid in [b"".as_slice(), b"null trailing", b"[", b"\xff"] {
            assert!(decode_frame_json::<serde_json::Value>(invalid).is_err());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frame_decode_matches_json_in_multithread_runtime() {
        frame_decode_matches_json_across_inline_boundary();
    }

    #[test]
    fn no_runtime_fallback() {
        assert_eq!(blocking_json(|| 7), 7);
    }
    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_compatibility_roundtrip() {
        let (mut client, mut server) = sockets().await;
        write_frame(&mut server, &42u64, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            read_frame::<u64>(&mut client, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap(),
            42
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn exact_buffered_frame_cap_and_oversize_no_network_prefix() {
        let (mut client, mut server) = sockets().await;
        let payload = "x".repeat(MAX_FRAME_BYTES - 2);
        let write = tokio::spawn(async move {
            write_frame(
                &mut server,
                &payload,
                Instant::now() + Duration::from_secs(10),
            )
            .await
        });
        let received = read_frame::<String>(&mut client, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(received.len(), MAX_FRAME_BYTES - 2);
        write.await.unwrap().unwrap();
        let (mut client, mut server) = sockets().await;
        let result = write_frame(
            &mut server,
            &"x".repeat(MAX_FRAME_BYTES - 1),
            Instant::now() + Duration::from_secs(10),
        )
        .await;
        assert!(matches!(result, Err(RemoteError::ResourceExhausted)));
        drop(server);
        let mut byte = [0u8];
        assert_eq!(client.read(&mut byte).await.unwrap(), 0);
    }
    struct LateEncode;
    impl Serialize for LateEncode {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            std::thread::sleep(Duration::from_millis(30));
            s.serialize_u64(42)
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn late_small_buffer_encode_sends_no_prefix() {
        let (mut client, mut server) = sockets().await;
        assert!(matches!(
            write_frame(
                &mut server,
                &LateEncode,
                Instant::now() + Duration::from_millis(5)
            )
            .await,
            Err(RemoteError::Timeout { .. })
        ));
        drop(server);
        let mut byte = [0u8];
        assert_eq!(client.read(&mut byte).await.unwrap(), 0);
    }
    struct LateDecode;
    impl<'de> serde::Deserialize<'de> for LateDecode {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let _ = <u64 as serde::Deserialize>::deserialize(d)?;
            std::thread::sleep(Duration::from_millis(30));
            Ok(Self)
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn post_decode_deadline_rejects_late_success() {
        let (mut client, mut server) = sockets().await;
        write_frame(&mut server, &42u64, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(matches!(
            read_frame::<LateDecode>(&mut client, Instant::now() + Duration::from_millis(5)).await,
            Err(RemoteError::Timeout { .. })
        ));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn aborted_encoding_keeps_original_admission_until_work_finishes() {
        struct Gate {
            entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl Serialize for Gate {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.entered
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(3))
                    .map_err(serde::ser::Error::custom)?;
                s.serialize_u64(42)
            }
        }
        let (_client, mut server) = sockets().await;
        let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            let _permit = permit;
            write_frame(
                &mut server,
                &Gate {
                    entered: std::sync::Mutex::new(Some(entered)),
                    release: std::sync::Mutex::new(rx),
                },
                Instant::now() + Duration::from_secs(5),
            )
            .await
        });
        ready.await.unwrap();
        task.abort();
        assert_eq!(permits.available_permits(), 0);
        assert!(permits.try_acquire().is_err());
        release.send(()).unwrap();
        let _ = task.await;
        assert_eq!(permits.available_permits(), 1);
    }
}

#[cfg(test)]
mod metadata_benchmark;
