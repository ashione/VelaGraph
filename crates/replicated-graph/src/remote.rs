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
    #[error("invalid remote protocol")]
    Protocol,
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
    operation: Operation,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Operation {
    Snapshot { shard: u64 },
    Metadata { shard: u64 },
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
    Snapshot(RemoteSnapshot),
    Metadata(u64, u64, u64),
    Write(D1Result),
}

/// A separate graph port. Deadlines must be positive and at most 60 seconds. Dropping stops acceptance; in-flight work retains its
/// permit until its handler completes. Shutdown cancels and joins handlers;
/// cancellation never proves that an already submitted Raft write aborted.
pub struct GraphService {
    address: SocketAddr,
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
        checked_deadline(deadline, None)?;
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let address = listener
            .local_addr()
            .map_err(|_| RemoteError::Unavailable)?;
        let raft = node.raft.clone();
        let state = node.state_machine.clone();
        let (stop, mut stopped) = watch::channel(false);
        let admission = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
        let task = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                while handlers.try_join_next().is_some() {}
                let accepted = tokio::select! {
                    _ = stopped.changed() => break,
                    _ = handlers.join_next(), if !handlers.is_empty() => continue,
                    result = listener.accept() => result,
                };
                let Ok((mut socket, _)) = accepted else { break };
                let Ok(permit) = admission.clone().try_acquire_owned() else {
                    continue;
                };
                let raft = raft.clone();
                let state = state.clone();
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
                    // Dropping the waiting future at the deadline does not undo
                    // a command already submitted to Raft.
                    if Instant::now() >= end {
                        return;
                    }
                    let result =
                        match tokio::time::timeout_at(end, execute(raft, state, shard, request))
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
            stop,
            task: Some(task),
        })
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
    shard: u64,
    request: Request,
) -> Result<Reply, RemoteError> {
    if request.version != VERSION {
        return Err(RemoteError::Protocol);
    }
    match request.operation {
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
    match rpc(endpoint, Operation::Snapshot { shard }, deadline, None).await? {
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
    match rpc(endpoint, Operation::Metadata { shard }, deadline, None).await? {
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
    validate_write(&request)?;
    let id = request.request_id;
    match rpc(endpoint, Operation::Write(request), deadline, Some(id)).await? {
        Reply::Write(result @ D1Result::Write { .. }) => Ok(result),
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
    operation: Operation,
    duration: Duration,
    id: Option<[u8; 16]>,
) -> Result<Reply, RemoteError> {
    let end = checked_deadline(duration, id)?;
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
                operation,
            },
            end,
        )
        .await?;
        let response: Response = read_frame(&mut stream, end).await?;
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
    let size = stream
        .read_u32()
        .await
        .map_err(|_| RemoteError::Unavailable)? as usize;
    if size == 0 || size > MAX_FRAME_BYTES {
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
