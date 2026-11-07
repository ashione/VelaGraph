//! Real TCP transport for the D2 prototype.
//!
//! One Raft node per process; peers reach each other through newline-delimited
//! JSON RPC. The wire types are the openraft request/response values, so the
//! consensus decisions stay identical to the in-process tests.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, PayloadTooLarge, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{timeout_at, Instant};

use crate::blocking::blocking_section;
use crate::durable_log_store::DurableLogStore;
use crate::state_machine::StateMachine;
use crate::types::{LogCommand, NodeId, TypeConfig};

pub type Raft = openraft::Raft<TypeConfig>;

const RPC_TIMEOUT: Duration = Duration::from_secs(3);
/// Legacy newline JSON protocol, unchanged on the wire (no negotiation).
pub const TCP_PROTOCOL_VERSION: u16 = 0;
const MAX_LINE: usize = 64 * 1024 * 1024;
const CONNECTIONS_PER_LISTENER: usize = 4;
static OUTBOUND: Semaphore = Semaphore::const_new(16);

// The frame includes the newline. Check before extending, never read_line into
// an unbounded String. BufReader itself uses only its fixed-size read buffer.
async fn read_frame<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete JSON frame",
            ));
        }
        let newline = available.iter().position(|b| *b == b'\n');
        let count = newline.map_or(available.len(), |n| n + 1);
        if count > MAX_LINE - frame.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "JSON frame exceeds limit",
            ));
        }
        reserve_frame(&mut frame, count)?;
        frame.extend_from_slice(&available[..count]);
        reader.consume(count);
        if newline.is_some() {
            return Ok(frame);
        }
        if frame.len() == MAX_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "JSON frame missing delimiter within limit",
            ));
        }
    }
}

// Grow geometrically for small JSON tokens, but never request capacity beyond
// the frame cap (including the delimiter). Allocator bookkeeping is excluded.
fn reserve_frame(bytes: &mut Vec<u8>, additional: usize) -> io::Result<()> {
    let needed = bytes.len() + additional;
    if needed > bytes.capacity() {
        let capacity = needed.max(bytes.capacity().saturating_mul(2)).min(MAX_LINE);
        bytes
            .try_reserve_exact(capacity - bytes.len())
            .map_err(io::Error::other)?;
    }
    Ok(())
}

struct FrameWriter {
    bytes: Vec<u8>,
    deadline: Instant,
}
impl io::Write for FrameWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "JSON encoding deadline",
            ));
        }
        if bytes.len() > MAX_LINE - 1 - self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "JSON encoding exceeds limit",
            ));
        }
        reserve_frame(&mut self.bytes, bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode_frame<T: Serialize>(value: &T, deadline: Instant) -> anyhow::Result<Vec<u8>> {
    blocking_section(|| {
        let mut writer = FrameWriter {
            bytes: Vec::new(),
            deadline,
        };
        // Coalesce serde's tiny punctuation/numeric writes. The fixed buffer is
        // additional scratch space, not extra permitted frame bytes. Flush
        // through FrameWriter before accepting the final deadline or delimiter.
        {
            let mut buffered = std::io::BufWriter::with_capacity(8192, &mut writer);
            serde_json::to_writer(&mut buffered, value)?;
            std::io::Write::flush(&mut buffered).map_err(serde_json::Error::io)?;
        }
        if Instant::now() >= deadline {
            return Err(serde_json::Error::io(io::Error::new(
                io::ErrorKind::TimedOut,
                "JSON encoding deadline",
            ))
            .into());
        }
        reserve_frame(&mut writer.bytes, 1)?;
        writer.bytes.push(b'\n');
        Ok(writer.bytes)
    })
}

async fn exchange_json<Q: Serialize, R: DeserializeOwned>(
    addr: &str,
    request: &Q,
    budget: Duration,
) -> anyhow::Result<R> {
    let deadline = Instant::now() + budget.min(RPC_TIMEOUT);
    let _permit = OUTBOUND
        .try_acquire()
        .map_err(|_| anyhow::anyhow!("outbound RPC capacity exhausted"))?;
    let result = timeout_at(deadline, async {
        let payload = encode_frame(request, deadline)?;
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        stream.write_all(&payload).await?;
        let frame = read_frame(&mut BufReader::new(stream)).await?;
        let reply = blocking_section(|| serde_json::from_slice(&frame))?;
        anyhow::ensure!(
            Instant::now() < deadline,
            "RPC deadline expired during decode"
        );
        Ok::<R, anyhow::Error>(reply)
    })
    .await;
    result.map_err(|_| anyhow::anyhow!("{addr}: RPC deadline expired"))?
}

#[derive(Debug, Serialize, Deserialize)]
enum RaftRpc {
    Vote(VoteRequest<NodeId>),
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

#[derive(Debug, Serialize, Deserialize)]
enum RaftReply {
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ),
}

fn io_unreachable(addr: &str, message: impl Into<String>) -> Unreachable {
    Unreachable::new(&io::Error::new(
        io::ErrorKind::NotConnected,
        format!("{addr}: {}", message.into()),
    ))
}

async fn exchange(addr: &str, request: &RaftRpc, option: RPCOption) -> anyhow::Result<RaftReply> {
    exchange_json(addr, request, option.hard_ttl()).await
}

// Own every connection task. Shutdown stops admission, cancels active handlers,
// and joins them before the listener task finishes.
async fn serve(
    listener: TcpListener,
    raft: Raft,
    state: Option<Arc<StateMachine>>,
    mut stop: watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept(), if connections.len() < CONNECTIONS_PER_LISTENER => {
                let (stream, _) = match accepted {
                    Ok(value) => value,
                    Err(error) => { tracing::warn!(%error, "listener accept failed"); break; }
                };
                let raft = raft.clone();
                let state = state.clone();
                let deadline = Instant::now() + RPC_TIMEOUT;
                connections.spawn(async move {
                    let work = async {
                        match state {
                            Some(state) => handle_control_connection(stream, raft, state, deadline).await,
                            None => handle_raft_connection(stream, raft, deadline).await,
                        }
                    };
                    // Do not log attacker-controlled JSON or payloads.
                    let _ = timeout_at(deadline, work).await;
                });
            }
        }
    }
    connections.shutdown().await;
}

async fn handle_raft_connection(
    stream: TcpStream,
    raft: Raft,
    deadline: Instant,
) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream);
    let frame = read_frame(&mut reader).await?;
    let request: RaftRpc = blocking_section(|| serde_json::from_slice(&frame))?;
    anyhow::ensure!(Instant::now() < deadline, "request deadline expired");
    drop(frame);
    let mut stream = reader.into_inner();
    let reply = match request {
        RaftRpc::Vote(request) => RaftReply::Vote(raft.vote(request).await),
        RaftRpc::AppendEntries(request) => {
            RaftReply::AppendEntries(raft.append_entries(request).await)
        }
        RaftRpc::InstallSnapshot(request) => {
            RaftReply::InstallSnapshot(raft.install_snapshot(request).await)
        }
    };
    let payload = encode_frame(&reply, deadline)?;
    stream.write_all(&payload).await?;
    Ok(())
}

/// Factory used by a single node to dial every peer's advertised address.
#[derive(Clone, Copy, Default)]
pub struct TcpNetworkFactory;

#[derive(Clone)]
pub struct TcpNetwork {
    target: NodeId,
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for TcpNetworkFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        TcpNetwork {
            target,
            addr: node.addr.clone(),
        }
    }
}

fn remote<T>(
    target: NodeId,
    result: Result<T, RaftError<NodeId>>,
) -> Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
    result.map_err(|error| RPCError::RemoteError(RemoteError::new(target, error)))
}

fn remote_snapshot(
    target: NodeId,
    result: Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
) -> Result<
    InstallSnapshotResponse<NodeId>,
    RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
> {
    result.map_err(|error| RPCError::RemoteError(RemoteError::new(target, error)))
}

impl RaftNetwork<TypeConfig> for TcpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let can_split = rpc.entries.len() > 1;
        let entries_hint = (rpc.entries.len() / 2).max(1) as u64;
        let reply = exchange(&self.addr, &RaftRpc::AppendEntries(rpc), option)
            .await
            .map_err(|error| {
                // Openraft must split an oversized aggregate batch instead of
                // retrying the identical batch forever as an unreachable peer.
                // Encoding budget exhaustion also shrinks multi-entry batches;
                // network timeouts never do. A single entry cannot be split.
                if error.downcast_ref::<serde_json::Error>().is_some_and(|e| {
                    e.io_error_kind() == Some(io::ErrorKind::InvalidData)
                        || (can_split && e.io_error_kind() == Some(io::ErrorKind::TimedOut))
                }) {
                    RPCError::PayloadTooLarge(PayloadTooLarge::new_entries_hint(entries_hint))
                } else {
                    RPCError::Unreachable(io_unreachable(&self.addr, error.to_string()))
                }
            })?;
        match reply {
            RaftReply::AppendEntries(result) => remote(self.target, result),
            _ => Err(RPCError::Unreachable(io_unreachable(
                &self.addr,
                "unexpected reply variant",
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let reply = exchange(&self.addr, &RaftRpc::InstallSnapshot(rpc), option)
            .await
            .map_err(|error| {
                RPCError::Unreachable(io_unreachable(&self.addr, error.to_string()))
            })?;
        match reply {
            RaftReply::InstallSnapshot(result) => remote_snapshot(self.target, result),
            _ => Err(RPCError::Unreachable(io_unreachable(
                &self.addr,
                "unexpected reply variant",
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let reply = exchange(&self.addr, &RaftRpc::Vote(rpc), option)
            .await
            .map_err(|error| {
                RPCError::Unreachable(io_unreachable(&self.addr, error.to_string()))
            })?;
        match reply {
            RaftReply::Vote(result) => remote(self.target, result),
            _ => Err(RPCError::Unreachable(io_unreachable(
                &self.addr,
                "unexpected reply variant",
            ))),
        }
    }
}

/// Initialize a new cluster with the given voter addresses.
///
/// This is the same path used by `d2-node --bootstrap` and by the control API;
/// real deployments should call it from their startup coordinator.
pub async fn initialize_cluster(
    raft: &Raft,
    members: BTreeMap<NodeId, String>,
) -> anyhow::Result<()> {
    let members: BTreeMap<NodeId, BasicNode> = members
        .into_iter()
        .map(|(id, addr)| (id, BasicNode { addr }))
        .collect();
    raft.initialize(members).await?;
    Ok(())
}

/// Add one learner and return the committed log index.
pub async fn add_learner(
    raft: &Raft,
    id: NodeId,
    addr: String,
    blocking: bool,
) -> anyhow::Result<u64> {
    let response = raft.add_learner(id, BasicNode { addr }, blocking).await?;
    Ok(response.log_id.index)
}

/// Change the voter set and return the committed log index.
pub async fn change_membership(raft: &Raft, ids: BTreeSet<NodeId>) -> anyhow::Result<u64> {
    let response = raft.change_membership(ids, false).await?;
    Ok(response.log_id.index)
}

/// Driver commands used by the multi-process integration test and the node CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    D1Capacity {
        shard: u64,
        epoch: u64,
    },
    D1SetCapacity {
        shard: u64,
        epoch: u64,
        expected_bytes: usize,
        new_bytes: usize,
    },
    Initialize {
        members: BTreeMap<NodeId, String>,
    },
    AddLearner {
        id: NodeId,
        addr: String,
        blocking: bool,
    },
    ChangeMembership {
        ids: BTreeSet<NodeId>,
    },
    Write {
        key: String,
        value: String,
    },
    /// Linearizable read, only valid on the current leader.
    Read {
        key: String,
    },
    /// Local state-machine read; used by tests to verify every replica.
    Get {
        key: String,
    },
    Status,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub leader: Option<NodeId>,
    pub applied: u64,
    pub last_log: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    D1Capacity(Result<crate::D1Capacity, crate::D1Error>),
    D1Admin(Result<crate::D1Result, crate::D1Error>),
    Ack,
    Index(u64),
    Value(Option<String>),
    Status(NodeStatus),
    Err(String),
}

impl ControlResponse {
    pub fn into_result(self) -> anyhow::Result<Self> {
        match self {
            Self::Err(error) => anyhow::bail!("{error}"),
            other => Ok(other),
        }
    }
}

async fn exchange_control(addr: &str, request: &ControlRequest) -> anyhow::Result<ControlResponse> {
    exchange_json(addr, request, RPC_TIMEOUT).await
}

/// Send one control command and return the decoded response.
pub async fn send_control(addr: &str, request: ControlRequest) -> anyhow::Result<ControlResponse> {
    exchange_control(addr, &request).await?.into_result()
}

async fn handle_control_connection(
    stream: TcpStream,
    raft: Raft,
    state_machine: Arc<StateMachine>,
    deadline: Instant,
) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream);
    let frame = read_frame(&mut reader).await?;
    let request: ControlRequest = blocking_section(|| serde_json::from_slice(&frame))?;
    anyhow::ensure!(Instant::now() < deadline, "control deadline expired");
    drop(frame);
    let mut stream = reader.into_inner();
    let response = match request {
        ControlRequest::D1Capacity { shard, epoch } => match raft.ensure_linearizable().await {
            Ok(_) => ControlResponse::D1Capacity(state_machine.d1_capacity(shard, epoch).await),
            Err(error) => ControlResponse::Err(error.to_string()),
        },
        ControlRequest::D1SetCapacity {
            shard,
            epoch,
            expected_bytes,
            new_bytes,
        } => {
            match raft
                .client_write(LogCommand::D1V1(crate::D1Command::SetDedupCapacity {
                    shard,
                    epoch,
                    expected_bytes,
                    new_bytes,
                }))
                .await
            {
                Ok(reply) => match reply.data.d1 {
                    Some(result) => ControlResponse::D1Admin(result),
                    None => ControlResponse::Err("missing D1 capacity response".into()),
                },
                Err(error) => ControlResponse::Err(error.to_string()),
            }
        }
        ControlRequest::Initialize { members } => match initialize_cluster(&raft, members).await {
            Ok(()) => ControlResponse::Ack,
            Err(error) => ControlResponse::Err(error.to_string()),
        },
        ControlRequest::AddLearner { id, addr, blocking } => {
            match add_learner(&raft, id, addr, blocking).await {
                Ok(index) => ControlResponse::Index(index),
                Err(error) => ControlResponse::Err(error.to_string()),
            }
        }
        ControlRequest::ChangeMembership { ids } => match change_membership(&raft, ids).await {
            Ok(index) => ControlResponse::Index(index),
            Err(error) => ControlResponse::Err(error.to_string()),
        },
        ControlRequest::Write { key, value } => {
            // Each UTF-8 byte becomes at most four JSON bytes ("255,").
            // Keep 1 KiB for the fixed command/log/vote envelope. Raw public
            // Raft handles remain caller-managed and can submit larger entries.
            if key.len().saturating_add(value.len()) > (MAX_LINE - 1024) / 4 {
                let response =
                    ControlResponse::Err("control write exceeds replication payload budget".into());
                stream
                    .write_all(&encode_frame(&response, deadline)?)
                    .await?;
                return Ok(());
            }
            match raft.client_write(LogCommand::put(key, value)).await {
                Ok(response) => match response.data.d1 {
                    Some(Err(error)) => {
                        ControlResponse::Err(format!("D1 application rejected: {error:?}"))
                    }
                    _ => ControlResponse::Index(response.log_id.index),
                },
                Err(error) => ControlResponse::Err(error.to_string()),
            }
        }
        ControlRequest::Read { key } => match raft.ensure_linearizable().await {
            Ok(_) => ControlResponse::Value(state_machine.get(&key).await),
            Err(error) => ControlResponse::Err(error.to_string()),
        },
        ControlRequest::Get { key } => ControlResponse::Value(state_machine.get(&key).await),
        ControlRequest::Status => {
            let metrics = raft.metrics().borrow().clone();
            let applied = state_machine
                .applied()
                .await
                .map(|log_id| log_id.index)
                .unwrap_or(0);
            ControlResponse::Status(NodeStatus {
                id: metrics.id,
                leader: metrics.current_leader,
                applied,
                last_log: metrics.last_log_index.unwrap_or(0),
            })
        }
    };
    let payload = encode_frame(&response, deadline)?;
    stream.write_all(&payload).await?;
    Ok(())
}

fn create_data_dir(path: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(path)
        .map_err(|error| anyhow::anyhow!("create data dir {}: {error}", path.display()))
}

/// Configuration for one node process.
#[derive(Debug, Clone)]
pub struct TcpNodeConfig {
    pub id: NodeId,
    pub raft_addr: String,
    pub control_addr: String,
    /// Directory for the durable `raft.db` and `state.db` RocksDB stores.
    pub data_dir: PathBuf,
}

pub struct TcpNode {
    pub id: NodeId,
    pub raft_addr: String,
    pub control_addr: String,
    pub raft: Raft,
    pub state_machine: Arc<StateMachine>,
    tasks: Vec<JoinHandle<()>>,
    stop: watch::Sender<bool>,
}

impl TcpNode {
    pub async fn start(config: TcpNodeConfig) -> anyhow::Result<Self> {
        let raft_listener = TcpListener::bind(&config.raft_addr).await?;
        let control_listener = TcpListener::bind(&config.control_addr).await?;
        let raft_addr = raft_listener.local_addr()?.to_string();
        let control_addr = control_listener.local_addr()?.to_string();

        let raft_config = Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()?,
        );
        create_data_dir(&config.data_dir)?;
        let log_store = DurableLogStore::open(config.data_dir.join("raft.db"))?;
        let state_machine = Arc::new(StateMachine::open(config.data_dir.join("state.db"))?);
        let raft = Raft::new(
            config.id,
            raft_config,
            TcpNetworkFactory,
            log_store,
            state_machine.clone(),
        )
        .await?;

        let (stop, receiver) = watch::channel(false);
        let raft_task = tokio::spawn(serve(raft_listener, raft.clone(), None, receiver.clone()));
        let control_task = tokio::spawn(serve(
            control_listener,
            raft.clone(),
            Some(state_machine.clone()),
            receiver,
        ));
        Ok(Self {
            id: config.id,
            raft_addr,
            control_addr,
            raft,
            state_machine,
            tasks: vec![raft_task, control_task],
            stop,
        })
    }

    /// Initialize a new cluster with this node and the supplied voters.
    pub async fn bootstrap(&self, members: BTreeMap<NodeId, String>) -> anyhow::Result<()> {
        initialize_cluster(&self.raft, members).await
    }

    pub async fn add_learner(
        &self,
        id: NodeId,
        addr: String,
        blocking: bool,
    ) -> anyhow::Result<u64> {
        add_learner(&self.raft, id, addr, blocking).await
    }

    pub async fn change_membership(&self, ids: BTreeSet<NodeId>) -> anyhow::Result<u64> {
        change_membership(&self.raft, ids).await
    }

    pub async fn shutdown(mut self) {
        // Once polled, cleanup owns the node even if the caller cancels its
        // wait. Otherwise cancellation between joining listeners and stopping
        // Raft would leave a live core retaining the durable stores.
        let cleanup = tokio::spawn(async move {
            let _ = self.stop.send(true);
            for task in self.tasks.drain(..) {
                let _ = task.await;
            }
            let _ = self.raft.shutdown().await;
        });
        let _ = cleanup.await;
    }
}

#[cfg(test)]
mod framing_tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt};

    #[tokio::test]
    async fn oversize_without_newline_fails_before_consuming_extra_bytes() {
        let bytes = vec![b'x'; MAX_LINE + 32];
        let mut reader = BufReader::with_capacity(8192, bytes.as_slice());
        let error = read_frame(&mut reader).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let mut remaining = Vec::new();
        reader.read_to_end(&mut remaining).await.unwrap();
        assert_eq!(remaining, vec![b'x'; 32]);
    }

    #[tokio::test]
    async fn exact_frame_boundary_and_multiple_frames() {
        let mut bytes = vec![b' '; MAX_LINE];
        bytes[MAX_LINE - 1] = b'\n';
        bytes.extend_from_slice(b"\"Status\"\n");
        let mut reader = BufReader::new(bytes.as_slice());
        assert_eq!(read_frame(&mut reader).await.unwrap().len(), MAX_LINE);
        assert_eq!(read_frame(&mut reader).await.unwrap(), b"\"Status\"\n");
        assert_eq!(
            read_frame(&mut reader).await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        let mut reader = BufReader::new(&b"\"Status\""[..]);
        assert_eq!(
            read_frame(&mut reader).await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn encoding_is_bounded_for_requests_and_responses_including_escapes() {
        let deadline = Instant::now() + Duration::from_secs(30);
        let value = "x".repeat(MAX_LINE - 3);
        let encoded = encode_frame(&value, deadline).unwrap();
        assert_eq!(encoded.len(), MAX_LINE);
        assert!(encoded.capacity() <= MAX_LINE);
        let oversized = "x".repeat(MAX_LINE - 2);
        assert!(encode_frame(&oversized, deadline).is_err());
        // JSON escaping, not the input string length, determines the limit.
        let response = ControlResponse::Value(Some("\0".repeat(MAX_LINE / 6)));
        assert!(encode_frame(&response, deadline).is_err());
        assert!(encode_frame(&ControlRequest::Status, Instant::now()).is_err());
        assert_eq!(
            encode_frame(&ControlRequest::Status, deadline).unwrap(),
            b"\"Status\"\n"
        );
        assert_eq!(TCP_PROTOCOL_VERSION, 0);
    }

    #[test]
    fn buffered_encoding_preserves_wire_bytes_and_deadline_error_kind() {
        for size in [0, 1, 8191, 8192, 8193, 65536] {
            let value = vec![255_u8; size];
            let mut expected = serde_json::to_vec(&value).unwrap();
            expected.push(b'\n');
            assert_eq!(
                encode_frame(&value, Instant::now() + Duration::from_secs(5)).unwrap(),
                expected
            );
            let error = encode_frame(&value, Instant::now()).unwrap_err();
            assert_eq!(
                error
                    .downcast_ref::<serde_json::Error>()
                    .unwrap()
                    .io_error_kind(),
                Some(io::ErrorKind::TimedOut)
            );
        }
    }

    #[tokio::test]
    async fn slow_frame_uses_one_deadline_not_one_timeout_per_chunk() {
        let (mut sender, receiver) = duplex(64);
        let task = tokio::spawn(async move {
            for _ in 0..20 {
                if sender.write_all(b" ").await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let start = Instant::now();
        assert!(timeout_at(
            start + Duration::from_millis(50),
            read_frame(&mut BufReader::new(receiver))
        )
        .await
        .is_err());
        assert!(start.elapsed() < Duration::from_secs(1));
        task.abort();
        let _ = task.await;
    }
}
