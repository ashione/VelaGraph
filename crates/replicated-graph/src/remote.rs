//! Bounded plaintext TCP D1 transport for trusted development networks only.
//! No authentication, TLS, forwarding, retry, or remote administration is provided.
//! Each connection carries exactly one request and one response.
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
const VERSION: u32 = 1;
/// Largest accepted client or server operation budget.
pub const MAX_DEADLINE: Duration = Duration::from_secs(60);
pub mod distributed;
mod paged;
pub mod query;
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
    OpenQuery {
        shard: u64,
        timeout_millis: u64,
    },
    QueryFragment(distributed::FragmentRequest),
    CloseQuery {
        descriptor: distributed::Descriptor,
    },
    Write(D1WriteRequest),
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
    OpenQuery(distributed::Descriptor),
    QueryFragment(Result<distributed::FragmentOutput, query::QueryFailure>),
    CloseQuery,
    Write(D1Result),
}

/// A separate graph port. Deadlines must be positive and at most 60 seconds. Dropping stops acceptance; in-flight work retains its
/// permit until its handler completes. Shutdown cancels and joins handlers;
/// cancellation never proves that an already submitted Raft write aborted.
pub struct GraphService {
    address: SocketAddr,
    sessions: Arc<paged::Sessions>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
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
        let identity = RemoteIdentity {
            group,
            node: node.id,
        };
        checked_deadline(deadline, None)?;
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let address = listener
            .local_addr()
            .map_err(|_| RemoteError::Unavailable)?;
        let raft = node.raft.clone();
        let state = node.state_machine.clone();
        let sessions = Arc::new(paged::Sessions::new()?);
        let worker_sessions = sessions.clone();
        let query_sessions = Arc::new(distributed::Sessions::new()?);
        let (stop, mut stopped) = watch::channel(false);
        let admission = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
        let task = tokio::spawn(async move {
            let sessions = worker_sessions;
            let mut cleanup = tokio::time::interval(Duration::from_millis(100));
            let mut handlers = JoinSet::new();
            loop {
                while handlers.try_join_next().is_some() {}
                let accepted = tokio::select! {
                    _ = stopped.changed() => break,
                    _ = cleanup.tick() => { sessions.expire(); query_sessions.expire(); continue; },
                    _ = handlers.join_next(), if !handlers.is_empty() => continue,
                    result = listener.accept() => result,
                };
                let Ok((mut socket, _)) = accepted else { break };
                let Ok(permit) = admission.clone().try_acquire_owned() else {
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
                            _ => return,
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
                        _ => end,
                    };
                    // Dropping the waiting future at the deadline does not undo
                    // a command already submitted to Raft.
                    if Instant::now() >= end {
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
                        Err(_) => return,
                    };
                    let response = Response {
                        version: VERSION,
                        result,
                    };
                    let _ = tokio::time::timeout_at(end, write_frame(&mut socket, &response, end))
                        .await;
                });
            }
            handlers.abort_all();
            while handlers.join_next().await.is_some() {}
        });
        Ok(Self {
            address,
            sessions,
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
    match request.operation {
        Operation::OpenQuery {
            shard: requested,
            timeout_millis,
        } => {
            if requested != shard || !(1..=5000).contains(&timeout_millis) {
                return Err(RemoteError::InvalidArgument);
            }
            let end = end.min(Instant::now() + Duration::from_millis(timeout_millis));
            raft.ensure_linearizable()
                .await
                .map_err(|_| RemoteError::Unavailable)?;
            query_sessions
                .open(&state, identity, shard, end)
                .await
                .map(Reply::OpenQuery)
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
            raft.ensure_linearizable().await.map_err(|e| {
                if let Some(hint) = e.forward_to_leader::<openraft::BasicNode>() {
                    RemoteError::NotLeader {
                        leader: hint.leader_id,
                    }
                } else {
                    RemoteError::Unavailable
                }
            })?;
            sessions
                .open(&state, identity, shard, end)
                .await
                .map(Reply::OpenSnapshot)
        }
        Operation::SnapshotPage { descriptor, cursor } => sessions
            .page(&state, identity, shard, descriptor, cursor, end)
            .await
            .map(Reply::SnapshotPage),
        Operation::CloseSnapshot { descriptor } => {
            sessions.close(identity, shard, &descriptor)?;
            Ok(Reply::CloseSnapshot)
        }
        Operation::Probe { shard: requested } => {
            if requested != shard {
                return Err(RemoteError::InvalidArgument);
            }
            raft.ensure_linearizable().await.map_err(|e| {
                if let Some(hint) = e.forward_to_leader::<openraft::BasicNode>() {
                    RemoteError::NotLeader {
                        leader: hint.leader_id,
                    }
                } else {
                    RemoteError::Unavailable
                }
            })?;
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
            raft.ensure_linearizable().await.map_err(|e| {
                if let Some(hint) = e.forward_to_leader::<openraft::BasicNode>() {
                    RemoteError::NotLeader {
                        leader: hint.leader_id,
                    }
                } else {
                    RemoteError::Unavailable
                }
            })?;
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
    let response_limit = if matches!(
        &operation,
        Operation::Query(_)
            | Operation::OpenQuery { .. }
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
        let mut stream = TcpStream::connect(endpoint)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
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
    .map_err(|error| match error {
        RemoteError::Timeout { .. } => RemoteError::Timeout { request_id: id },
        other => other,
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
async fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
    end: Instant,
) -> Result<(), RemoteError> {
    let mut buffer = BoundedWriter(Vec::new(), end);
    serde_json::to_writer(&mut buffer, value).map_err(|_| {
        if Instant::now() >= end {
            RemoteError::Timeout { request_id: None }
        } else {
            RemoteError::ResourceExhausted
        }
    })?;
    if Instant::now() >= end {
        return Err(RemoteError::Timeout { request_id: None });
    }
    stream
        .write_u32(buffer.0.len() as u32)
        .await
        .map_err(|_| RemoteError::Unavailable)?;
    stream
        .write_all(&buffer.0)
        .await
        .map_err(|_| RemoteError::Unavailable)
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
    let size = stream
        .read_u32()
        .await
        .map_err(|_| RemoteError::Unavailable)? as usize;
    if size == 0 || size > limit {
        return Err(RemoteError::ResourceExhausted);
    }
    let mut bytes = vec![0; size];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|_| RemoteError::Unavailable)?;
    if Instant::now() >= end {
        return Err(RemoteError::Timeout { request_id: None });
    }
    let result = serde_json::from_slice(&bytes).map_err(|_| RemoteError::Protocol);
    if Instant::now() >= end {
        return Err(RemoteError::Timeout { request_id: None });
    }
    result
}

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
