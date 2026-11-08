//! Bounded, trusted internal managed-transaction transport. This is neither an
//! authenticated service nor a public graph query API. Journal evidence must
//! come from a leader observation by a trusted caller. Timeouts are uncertain
//! outcomes and never authorize abort, identity replacement, or fallback reads.
use crate::cohort_reads::VisibilityInfo;
use crate::participant_writes::{Authority, Status};
use crate::request_index::RequestLookup;
use crate::terminal_prefix::TerminalPrefix;
use crate::transaction_decisions::{
    CapacityProfile, Cohort, Command, HistoryCursor, HistoryPage, Journal, MemberCapacityGrant,
    Participant,
};
use crate::versioned_reads::{ReadLimit, ReadSnapshot, Target};
use crate::{D1Command, D1Error, D1Result, D1WriteRequest, LogCommand, NodeHandle};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

/// JSON framing limit, enforced before allocation and while serialization runs.
pub const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;
const PROTOCOL_VERSION: u32 = 5;
const CONCURRENCY: usize = 4;
const LOCAL_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    Application(D1Error),
    Identity,
    Capacity,
    Unavailable,
    Protocol,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "managed RPC: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Coordinator,
    Participant,
}

/// Fixed deployment identity, not a credential or a routing hint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub group: u64,
    pub node: u64,
    pub target: Target,
    pub role: Role,
    pub authority: Authority,
    pub cohort: Cohort,
}
impl Identity {
    fn validate(&self) -> Result<(), Error> {
        if self.cohort.validate().is_err()
            || self.authority.shard != self.cohort.coordinator_shard
            || self.authority.mode == [0; 16]
            || self.target.mode == [0; 16]
        {
            return Err(Error::Identity);
        }
        let matches = match self.role {
            Role::Coordinator => {
                self.target.shard == self.authority.shard
                    && self.target.mode == self.authority.mode
                    && self.target.epoch == self.cohort.coordinator_epoch
            }
            Role::Participant => self.cohort.members.iter().any(|m| {
                m.shard == self.target.shard
                    && m.epoch == self.target.epoch
                    && m.mode == self.target.mode
            }),
        };
        if matches {
            Ok(())
        } else {
            Err(Error::Identity)
        }
    }
}

/// Leader-observed coordinator metadata, not itself a read certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorSummary {
    pub authority: Authority,
    pub cohort: Cohort,
    pub resolved_prefix: u64,
}

/// Leader-observed durable history position and applied watermark from one lock.
/// The cursor is a consistency commitment, not an authenticated certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryStatus {
    pub cursor: HistoryCursor,
    pub applied_prefix: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u32,
    identity: Identity,
    nonce: u64,
    operation: Operation,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Operation {
    GrantStatus {
        authority: Authority,
        target: Target,
        cohort: Cohort,
    },
    ConfirmCapacity {
        authority: Authority,
        epoch: u64,
    },
    HistoryStatus {
        authority: Authority,
        target: Target,
        cohort: Cohort,
    },
    HistoryPage {
        authority: Authority,
        epoch: u64,
        base: HistoryCursor,
        cut: Option<HistoryCursor>,
    },
    SyncHistory {
        authority: Authority,
        target: Target,
        page: HistoryPage,
    },
    AdvanceRetained {
        authority: Authority,
        target: Target,
        ordinal: u64,
        cut: HistoryCursor,
    },
    LookupRequest {
        authority: Authority,
        epoch: u64,
        id: [u8; 16],
    },
    Summary {
        authority: Authority,
        epoch: u64,
    },
    Terminal {
        authority: Authority,
        epoch: u64,
        version: u64,
    },
    Journal {
        authority: Authority,
        epoch: u64,
    },
    Command {
        authority: Authority,
        epoch: u64,
        command: Command,
    },
    Visibility {
        target: Target,
    },
    Prepare {
        authority: Authority,
        participant: Participant,
        request: D1WriteRequest,
        journal: Journal,
    },
    Advance {
        authority: Authority,
        target: Target,
        ordinal: u64,
        journal: Journal,
    },
    Prefix {
        target: Target,
        proof: TerminalPrefix,
    },
    Read {
        target: Target,
        limit: ReadLimit,
        proof: TerminalPrefix,
    },
}
impl Operation {
    fn tag(&self) -> u8 {
        match self {
            Self::GrantStatus { .. } => 15,
            Self::ConfirmCapacity { .. } => 16,
            Self::LookupRequest { .. } => 10,
            Self::HistoryStatus { .. } => 11,
            Self::HistoryPage { .. } => 12,
            Self::SyncHistory { .. } => 13,
            Self::AdvanceRetained { .. } => 14,
            Self::Journal { .. } => 1,
            Self::Command { .. } => 2,
            Self::Visibility { .. } => 3,
            Self::Prepare { .. } => 4,
            Self::Advance { .. } => 5,
            Self::Prefix { .. } => 6,
            Self::Read { .. } => 7,
            Self::Summary { .. } => 8,
            Self::Terminal { .. } => 9,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    version: u32,
    operation: u8,
    identity: Identity,
    nonce: u64,
    result: Result<Value, Error>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Value {
    Grant(CapacityGrantStatus),
    Capacity(CohortCapacity),
    HistoryStatus(HistoryStatus),
    HistoryPage(HistoryPage),
    HistorySynced(HistoryStatus),
    RetainedPrefix(u64),
    Request(RequestLookup),
    Journal(Journal),
    Unit,
    Visibility(VisibilityInfo),
    Status(Status),
    Prefix(u64),
    Summary(CoordinatorSummary),
    Evidence(TerminalPrefix),
    Snapshot(Snapshot, TerminalPrefix),
}
// Responses bind the generation to the same complete identity as the endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapacityGrantStatus {
    authority: Authority,
    cohort: Cohort,
    grant: MemberCapacityGrant,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CohortCapacity {
    authority: Authority,
    cohort: Cohort,
    #[serde(deserialize_with = "deserialize_capacity_grants")]
    grants: Vec<MemberCapacityGrant>,
}
fn deserialize_capacity_grants<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<MemberCapacityGrant>, D::Error> {
    crate::capacity_policy::deserialize_bounded_vec(
        deserializer,
        crate::transaction_decisions::MAX_PARTICIPANTS,
    )
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    shard: u64,
    epoch: u64,
    authority: Authority,
    version: u64,
    baseline_sequence: u64,
    cohort: Option<Cohort>,
    applied_prefix: u64,
    local_sequence: u64,
    replay_operations: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}
impl From<ReadSnapshot> for Snapshot {
    fn from(s: ReadSnapshot) -> Self {
        Self {
            shard: s.shard,
            epoch: s.epoch,
            authority: s.authority,
            version: s.version,
            baseline_sequence: s.baseline_sequence,
            cohort: s.cohort,
            applied_prefix: s.applied_prefix,
            local_sequence: s.local_sequence,
            replay_operations: s.replay_operations,
            entries: s.entries.into_iter().collect(),
        }
    }
}
impl Snapshot {
    fn into_read(self) -> Result<ReadSnapshot, Error> {
        let mut entries = BTreeMap::new();
        for (k, v) in self.entries {
            if entries.insert(k, v).is_some() {
                return Err(Error::Protocol);
            }
        }
        Ok(ReadSnapshot {
            shard: self.shard,
            epoch: self.epoch,
            authority: self.authority,
            version: self.version,
            baseline_sequence: self.baseline_sequence,
            cohort: self.cohort,
            applied_prefix: self.applied_prefix,
            local_sequence: self.local_sequence,
            replay_operations: self.replay_operations,
            entries,
        })
    }
}

fn valid_timeout(timeout: Duration) -> Result<(), Error> {
    if timeout.is_zero() || timeout > MAX_TIMEOUT {
        Err(Error::Capacity)
    } else {
        Ok(())
    }
}
fn cpu_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(CONCURRENCY)))
}
fn outbound_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(CONCURRENCY)))
}
// The permit lives in the blocking closure, including after a caller times out.
async fn cpu<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    cpu_with_slots(cpu_slots().clone(), work).await
}
async fn cpu_with_slots<T: Send + 'static>(
    slots: Arc<Semaphore>,
    work: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    let permit = slots
        .acquire_owned()
        .await
        .map_err(|_| Error::Unavailable)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|_| Error::Unavailable)?
}
// A caller's timeout does not cancel a running spawn_blocking closure. Put
// the same absolute deadline inside that closure as well, so pending Raft or
// state-lock awaits are dropped and the CPU permit is released on expiration.
// Type-erase at the synchronous boundary: nested managed/cohort futures must
// not embed the complete execution future in every caller's async layout.
type BoundedWork<T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + 'static>>;
fn bounded_cpu<T: Send + 'static>(
    deadline: tokio::time::Instant,
    work: impl std::future::Future<Output = Result<T, Error>> + Send + 'static,
) -> BoundedWork<T> {
    bounded_cpu_with_slots(cpu_slots().clone(), deadline, work)
}
fn bounded_cpu_with_slots<T: Send + 'static>(
    slots: Arc<Semaphore>,
    deadline: tokio::time::Instant,
    work: impl std::future::Future<Output = Result<T, Error>> + Send + 'static,
) -> BoundedWork<T> {
    let work: BoundedWork<T> = Box::pin(work);
    Box::pin(async move {
        let runtime = tokio::runtime::Handle::current();
        tokio::time::timeout_at(
            deadline,
            cpu_with_slots(slots, move || {
                runtime.block_on(async {
                    let result = tokio::time::timeout_at(deadline, work)
                        .await
                        .map_err(|_| Error::Unavailable)?;
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Error::Unavailable);
                    }
                    result
                })
            }),
        )
        .await
        .map_err(|_| Error::Unavailable)?
    })
}
struct BoundedWriter(Vec<u8>);
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_FRAME_BYTES.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("frame capacity"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode_sync<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let mut writer = BoundedWriter(Vec::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| Error::Capacity)?;
    Ok(writer.0)
}
async fn encode<T: Serialize + Send + 'static>(value: T) -> Result<Vec<u8>, Error> {
    cpu(move || encode_sync(&value)).await
}
async fn receive<T: DeserializeOwned + Send + 'static>(stream: &mut TcpStream) -> Result<T, Error> {
    let length = stream.read_u32().await.map_err(|_| Error::Unavailable)? as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err(Error::Capacity);
    }
    let mut bytes = vec![0; length];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|_| Error::Unavailable)?;
    cpu(move || serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)).await
}
async fn send(stream: &mut TcpStream, bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() || bytes.len() > MAX_FRAME_BYTES {
        return Err(Error::Capacity);
    }
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|_| Error::Unavailable)?;
    stream
        .write_all(bytes)
        .await
        .map_err(|_| Error::Unavailable)
}

/// A fixed remote replica. No forwarding, discovery, or failover occurs.
#[derive(Clone)]
pub struct Client {
    address: SocketAddr,
    identity: Identity,
    timeout: Duration,
    slots: Arc<Semaphore>,
    nonce: Arc<AtomicU64>,
}
impl Client {
    pub fn new(address: SocketAddr, identity: Identity, timeout: Duration) -> Result<Self, Error> {
        identity.validate()?;
        valid_timeout(timeout)?;
        Ok(Self {
            address,
            identity,
            timeout,
            slots: outbound_slots().clone(),
            nonce: Arc::new(AtomicU64::new(1)),
        })
    }
    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    #[cfg(test)]
    async fn call(&self, operation: Operation) -> Result<Value, Error> {
        self.call_at(operation, tokio::time::Instant::now() + self.timeout)
            .await
    }
    // Fail before connecting: the response envelope cannot make a request for
    // another role, mode, cohort or participant valid on this fixed endpoint.
    fn validate_operation(&self, operation: &Operation) -> Result<(), Error> {
        check_public_operation(operation)?;
        let participant = |target: &Target| {
            self.identity.role == Role::Participant && self.identity.target == *target
        };
        let history = |journal: &Journal| {
            journal.authority() == self.identity.authority.mode
                && journal.cohort() == Some(&self.identity.cohort)
        };
        let valid = match operation {
            Operation::HistoryStatus {
                authority,
                target,
                cohort,
            }
            | Operation::GrantStatus {
                authority,
                target,
                cohort,
            } => {
                participant(target)
                    && *authority == self.identity.authority
                    && *cohort == self.identity.cohort
            }
            Operation::HistoryPage {
                authority,
                epoch,
                base,
                cut,
            } => {
                self.identity.role == Role::Coordinator
                    && self.identity.authority == *authority
                    && self.identity.target.epoch == *epoch
                    && base.authority == *authority
                    && base.cohort == self.identity.cohort
                    && cut.as_ref().is_none_or(|c| {
                        c.authority == *authority && c.cohort == self.identity.cohort
                    })
            }
            Operation::SyncHistory {
                authority,
                target,
                page,
            } => {
                participant(target)
                    && self.identity.authority == *authority
                    && page.base.authority == *authority
                    && page.base.cohort == self.identity.cohort
            }
            Operation::AdvanceRetained {
                authority,
                target,
                cut,
                ..
            } => {
                participant(target)
                    && self.identity.authority == *authority
                    && cut.authority == *authority
                    && cut.cohort == self.identity.cohort
            }
            Operation::Journal { authority, epoch }
            | Operation::ConfirmCapacity { authority, epoch }
            | Operation::LookupRequest {
                authority, epoch, ..
            }
            | Operation::Summary { authority, epoch }
            | Operation::Terminal {
                authority, epoch, ..
            }
            | Operation::Command {
                authority, epoch, ..
            } => {
                self.identity.role == Role::Coordinator
                    && self.identity.authority == *authority
                    && self.identity.target.epoch == *epoch
            }
            Operation::Visibility { target } => participant(target),
            Operation::Prepare {
                authority,
                participant: descriptor,
                request,
                journal,
            } => {
                participant(&Target {
                    shard: descriptor.shard,
                    epoch: descriptor.epoch,
                    mode: descriptor.mode,
                }) && self.identity.authority == *authority
                    && history(journal)
                    && request.shard == descriptor.shard
                    && request.epoch == descriptor.epoch
            }
            Operation::Advance {
                authority,
                target,
                journal,
                ..
            } => participant(target) && self.identity.authority == *authority && history(journal),
            Operation::Prefix { target, proof } | Operation::Read { target, proof, .. } => {
                participant(target)
                    && proof.authority() == self.identity.authority.mode
                    && proof.cohort() == &self.identity.cohort
            }
        };
        if valid {
            Ok(())
        } else {
            Err(Error::Identity)
        }
    }
    async fn call_at(
        &self,
        operation: Operation,
        deadline: tokio::time::Instant,
    ) -> Result<Value, Error> {
        self.validate_operation(&operation)?;
        let _permit = self.slots.try_acquire().map_err(|_| Error::Capacity)?;
        tokio::time::timeout_at(deadline, async {
            // Retain the documented Rust 1.95 API while newer toolchains rename it.
            #[allow(deprecated)]
            let nonce = self
                .nonce
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .map_err(|_| Error::Capacity)?;
            let operation_tag = operation.tag();
            let bytes = encode(Request {
                version: PROTOCOL_VERSION,
                identity: self.identity.clone(),
                nonce,
                operation,
            })
            .await?;
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Unavailable);
            }
            let mut stream = TcpStream::connect(self.address)
                .await
                .map_err(|_| Error::Unavailable)?;
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Unavailable);
            }
            send(&mut stream, &bytes).await?;
            let reply: Reply = receive(&mut stream).await?;
            if reply.version != PROTOCOL_VERSION
                || reply.operation != operation_tag
                || reply.identity != self.identity
                || reply.nonce != nonce
            {
                return Err(Error::Identity);
            }
            reply.result
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }
}

/// Borrowed local/remote handle shared by managed graph and cohort algorithms.
#[derive(Clone, Copy)]
pub enum NodeRef<'a> {
    Local(&'a NodeHandle),
    Remote(&'a Client),
}
impl NodeRef<'_> {
    // Cover preflight and post-reply page hashing with the same total budget.
    async fn bounded<T>(
        self,
        work: impl std::future::Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        let timeout = match self {
            Self::Local(_) => LOCAL_TIMEOUT,
            Self::Remote(client) => client.timeout,
        };
        let deadline = tokio::time::Instant::now() + timeout;
        let result = tokio::time::timeout_at(deadline, work)
            .await
            .map_err(|_| Error::Unavailable)??;
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unavailable);
        }
        Ok(result)
    }
    async fn call(self, build: impl FnOnce() -> Operation) -> Result<Value, Error> {
        let duration = match self {
            Self::Local(_) => LOCAL_TIMEOUT,
            Self::Remote(c) => c.timeout,
        };
        let deadline = tokio::time::Instant::now() + duration;
        let op = build();
        check_public_operation(&op)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unavailable);
        }
        let result = tokio::time::timeout_at(deadline, async {
            match self {
                Self::Local(node) => dispatch(node, None, None, op, deadline).await,
                Self::Remote(client) => client.call_at(op, deadline).await,
            }
        })
        .await
        .map_err(|_| Error::Unavailable)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unavailable);
        }
        result
    }
    /// Observe a member's immutable generation after its real leader barrier.
    pub async fn grant_status(
        self,
        authority: &Authority,
        target: &Target,
        cohort: &Cohort,
    ) -> Result<MemberCapacityGrant, Error> {
        match self
            .call(|| Operation::GrantStatus {
                authority: authority.clone(),
                target: target.clone(),
                cohort: cohort.clone(),
            })
            .await?
        {
            Value::Grant(status) => {
                check_grant_status(&status, authority, target, cohort)?;
                Ok(status.grant)
            }
            _ => Err(Error::Protocol),
        }
    }
    /// Ask the configured coordinator to obtain all member grants itself.
    /// Local handles have no trusted endpoint set and cannot confirm capacity.
    pub async fn confirm_capacity(
        self,
        authority: &Authority,
        epoch: u64,
    ) -> Result<Vec<MemberCapacityGrant>, Error> {
        let Self::Remote(client) = self else {
            return Err(Error::Identity);
        };
        match self
            .call(|| Operation::ConfirmCapacity {
                authority: authority.clone(),
                epoch,
            })
            .await?
        {
            Value::Capacity(value) => {
                if value.authority != *authority
                    || value.cohort != client.identity.cohort
                    || value.cohort.coordinator_epoch != epoch
                {
                    return Err(Error::Identity);
                }
                crate::capacity_policy::validate_grants(&value.cohort, &value.grants)
                    .map_err(|_| Error::Identity)?;
                Ok(value.grants)
            }
            _ => Err(Error::Protocol),
        }
    }
    /// Leader-barrier point recovery of an original durable admission and current transaction.
    pub async fn request(
        self,
        authority: &Authority,
        epoch: u64,
        id: [u8; 16],
    ) -> Result<RequestLookup, Error> {
        if id == [0; 16] {
            return Err(Error::Identity);
        }
        match self
            .call(|| Operation::LookupRequest {
                authority: authority.clone(),
                epoch,
                id,
            })
            .await?
        {
            Value::Request(value) => {
                check_request(&value, authority, epoch, id)?;
                if matches!(self, Self::Remote(c) if c.identity.cohort != value.cohort) {
                    return Err(Error::Identity);
                }
                Ok(value)
            }
            _ => Err(Error::Protocol),
        }
    }
    pub async fn journal(self, authority: &Authority, epoch: u64) -> Result<Journal, Error> {
        match self
            .call(|| Operation::Journal {
                authority: authority.clone(),
                epoch,
            })
            .await?
        {
            Value::Journal(v) => {
                if matches!(self, Self::Local(_)) && v.cohort().is_none() {
                    return Ok(v);
                }
                let cohort = evidence(authority, &v)?;
                if cohort.coordinator_epoch != epoch
                    || matches!(self, Self::Remote(c) if c.identity.role != Role::Coordinator
                        || c.identity.authority != *authority || c.identity.target.epoch != epoch
                        || c.identity.cohort != *cohort)
                {
                    return Err(Error::Identity);
                }
                Ok(v)
            }
            _ => Err(Error::Protocol),
        }
    }
    /// Observe only bounded membership/watermark metadata before selecting V.
    pub async fn summary(
        self,
        authority: &Authority,
        epoch: u64,
    ) -> Result<CoordinatorSummary, Error> {
        match self
            .call(|| Operation::Summary {
                authority: authority.clone(),
                epoch,
            })
            .await?
        {
            Value::Summary(value) => {
                check_summary(&value, authority, epoch)?;
                if matches!(self, Self::Remote(c) if c.identity.cohort != value.cohort) {
                    return Err(Error::Identity);
                }
                Ok(value)
            }
            _ => Err(Error::Protocol),
        }
    }
    /// Fetch exactly 1..=V after V is fixed. No full Journal is transferred.
    pub async fn terminal(
        self,
        authority: &Authority,
        epoch: u64,
        version: u64,
    ) -> Result<TerminalPrefix, Error> {
        match self
            .call(|| Operation::Terminal {
                authority: authority.clone(),
                epoch,
                version,
            })
            .await?
        {
            Value::Evidence(proof) => {
                if proof.authority() != authority.mode
                    || proof.version() != version
                    || proof.cohort().coordinator_shard != authority.shard
                    || proof.cohort().coordinator_epoch != epoch
                    || matches!(self, Self::Remote(c) if c.identity.cohort != *proof.cohort())
                {
                    return Err(Error::Identity);
                }
                Ok(proof)
            }
            _ => Err(Error::Protocol),
        }
    }
    pub async fn command(
        self,
        authority: &Authority,
        epoch: u64,
        command: Command,
    ) -> Result<(), Error> {
        match self
            .call(|| Operation::Command {
                authority: authority.clone(),
                epoch,
                command,
            })
            .await?
        {
            Value::Unit => Ok(()),
            _ => Err(Error::Protocol),
        }
    }
    pub async fn visibility(self, target: &Target) -> Result<VisibilityInfo, Error> {
        match self
            .call(|| Operation::Visibility {
                target: target.clone(),
            })
            .await?
        {
            Value::Visibility(v) => {
                let cohort = v.cohort.as_ref().ok_or(Error::Identity)?;
                if v.baseline_sequence != 0
                    || v.applied_prefix > cohort.profile.max_transactions() as u64
                    || v.authority.mode == [0; 16]
                    || cohort.coordinator_shard != v.authority.shard
                    || !cohort.members.iter().any(|m| {
                        m.shard == target.shard && m.epoch == target.epoch && m.mode == target.mode
                    })
                    || matches!(self, Self::Remote(c) if c.identity.role != Role::Participant
                        || c.identity.target != *target || c.identity.authority != v.authority
                        || c.identity.cohort != *cohort)
                {
                    return Err(Error::Identity);
                }
                Ok(v)
            }
            _ => Err(Error::Protocol),
        }
    }
    pub async fn prepare(
        self,
        authority: &Authority,
        participant: &Participant,
        request: D1WriteRequest,
        journal: &Journal,
    ) -> Result<Status, Error> {
        match self
            .call(|| Operation::Prepare {
                authority: authority.clone(),
                participant: participant.clone(),
                request,
                journal: journal.clone(),
            })
            .await?
        {
            Value::Status(v) => Ok(v),
            _ => Err(Error::Protocol),
        }
    }
    /// Observe the member's exact retained event position, independent of applied V.
    pub async fn history_status(
        self,
        authority: &Authority,
        target: &Target,
        cohort: &Cohort,
    ) -> Result<HistoryStatus, Error> {
        match self
            .call(|| Operation::HistoryStatus {
                authority: authority.clone(),
                target: target.clone(),
                cohort: cohort.clone(),
            })
            .await?
        {
            Value::HistoryStatus(status) => {
                check_history_status(&status, authority, target, cohort)?;
                Ok(status)
            }
            _ => Err(Error::Protocol),
        }
    }
    /// Pin a source cut once, then fetch bounded pages against that exact cut.
    pub async fn history_page(
        self,
        authority: &Authority,
        epoch: u64,
        base: &HistoryCursor,
        cut: Option<&HistoryCursor>,
    ) -> Result<HistoryPage, Error> {
        self.bounded(async {
            check_history_request(authority, epoch, base, cut)?;
            match self
                .call(|| Operation::HistoryPage {
                    authority: authority.clone(),
                    epoch,
                    base: base.clone(),
                    cut: cut.cloned(),
                })
                .await?
            {
                Value::HistoryPage(page) => {
                    let base = base.clone();
                    let cut = cut.cloned();
                    cpu(move || {
                        check_history_page(&page, &base, cut.as_ref())?;
                        Ok(page)
                    })
                    .await
                }
                _ => Err(Error::Protocol),
            }
        })
        .await
    }

    /// Replicate only observed history. This does not advance read visibility.
    pub async fn sync_history(
        self,
        authority: &Authority,
        target: &Target,
        page: &HistoryPage,
    ) -> Result<HistoryStatus, Error> {
        self.bounded(async {
            let page = page.clone();
            let page = cpu(move || {
                page.validate().map_err(|_| Error::Identity)?;
                Ok(page)
            })
            .await?;
            check_member_cursor(&page.base, authority, target, &page.base.cohort)?;
            let end = page.end.clone();
            match self
                .call(|| Operation::SyncHistory {
                    authority: authority.clone(),
                    target: target.clone(),
                    page,
                })
                .await?
            {
                Value::HistorySynced(status) => {
                    check_history_status(&status, authority, target, &end.cohort)?;
                    if status.cursor.event_count < end.event_count
                        || (status.cursor.event_count == end.event_count && status.cursor != end)
                    {
                        return Err(Error::Identity);
                    }
                    Ok(status)
                }
                _ => Err(Error::Protocol),
            }
        })
        .await
    }

    /// Consume one retained ordinal against an exact already-transferred cut.
    pub async fn advance_retained(
        self,
        authority: &Authority,
        target: &Target,
        ordinal: u64,
        cut: &HistoryCursor,
    ) -> Result<u64, Error> {
        check_member_cursor(cut, authority, target, &cut.cohort)?;
        if ordinal == 0
            || ordinal > cut.cohort.profile.max_transactions() as u64
            || ordinal > (cut.event_count - 1) / 2
        {
            return Err(Error::Identity);
        }
        match self
            .call(|| Operation::AdvanceRetained {
                authority: authority.clone(),
                target: target.clone(),
                ordinal,
                cut: cut.clone(),
            })
            .await?
        {
            Value::RetainedPrefix(applied)
                if applied >= ordinal
                    && applied <= cut.cohort.profile.max_transactions() as u64 =>
            {
                Ok(applied)
            }
            Value::RetainedPrefix(_) => Err(Error::Identity),
            _ => Err(Error::Protocol),
        }
    }
    /// Explicit full-archive compatibility helper. Managed catch-up uses
    /// history_status/history_page/sync_history/advance_retained instead.
    pub async fn advance(
        self,
        authority: &Authority,
        target: &Target,
        ordinal: u64,
        journal: &Journal,
    ) -> Result<u64, Error> {
        match self
            .call(|| Operation::Advance {
                authority: authority.clone(),
                target: target.clone(),
                ordinal,
                journal: journal.clone(),
            })
            .await?
        {
            Value::Prefix(v) => Ok(v),
            _ => Err(Error::Protocol),
        }
    }
    /// Compatibility helper for callers already holding the durable archive.
    /// Only the exact terminal projection crosses the transport.
    pub async fn prefix(
        self,
        target: &Target,
        version: u64,
        journal: &Journal,
    ) -> Result<(), Error> {
        let proof = journal
            .terminal_prefix(version)
            .map_err(|_| Error::Identity)?;
        self.prefix_terminal(target, &proof).await
    }
    pub async fn prefix_terminal(
        self,
        target: &Target,
        proof: &TerminalPrefix,
    ) -> Result<(), Error> {
        let timeout = match self {
            Self::Local(_) => LOCAL_TIMEOUT,
            Self::Remote(c) => c.timeout,
        };
        let deadline = tokio::time::Instant::now() + timeout;
        tokio::time::timeout_at(deadline, async {
            match self
                .call(|| Operation::Prefix {
                    target: target.clone(),
                    proof: proof.clone(),
                })
                .await?
            {
                Value::Evidence(retained) => {
                    let expected = proof.clone();
                    cpu(move || {
                        if retained == expected {
                            Ok(())
                        } else {
                            Err(Error::Identity)
                        }
                    })
                    .await
                }
                _ => Err(Error::Protocol),
            }
        })
        .await
        .map_err(|_| Error::Unavailable)??;
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unavailable);
        }
        Ok(())
    }
    /// Compatibility helper. Snapshot acquisition uses read_terminal directly.
    pub async fn read(
        self,
        target: &Target,
        version: u64,
        limit: ReadLimit,
        journal: &Journal,
    ) -> Result<ReadSnapshot, Error> {
        let proof = journal
            .terminal_prefix(version)
            .map_err(|_| Error::Identity)?;
        self.read_terminal(target, limit, &proof).await
    }
    pub async fn read_terminal(
        self,
        target: &Target,
        limit: ReadLimit,
        proof: &TerminalPrefix,
    ) -> Result<ReadSnapshot, Error> {
        let version = proof.version();
        let timeout = match self {
            Self::Local(_) => LOCAL_TIMEOUT,
            Self::Remote(c) => c.timeout,
        };
        let deadline = tokio::time::Instant::now() + timeout;
        let result = tokio::time::timeout_at(deadline, async {
            let expected = limit.clone();
            match self
                .call(|| Operation::Read {
                    target: target.clone(),
                    limit,
                    proof: proof.clone(),
                })
                .await?
            {
                Value::Snapshot(v, retained) => {
                    if v.shard != target.shard
                        || v.epoch != target.epoch
                        || v.version != version
                        || v.baseline_sequence != 0
                        || v.cohort.as_ref() != Some(proof.cohort())
                        || v.authority.mode != proof.authority()
                        || v.authority.shard != proof.cohort().coordinator_shard
                        || v.applied_prefix < version
                        || v.applied_prefix > proof.cohort().profile.max_transactions() as u64
                    {
                        return Err(Error::Identity);
                    }
                    if v.entries.len() > expected.rows
                        || v.replay_operations > expected.operations
                        || v.entries
                            .iter()
                            .try_fold(0usize, |sum, (k, v)| {
                                sum.checked_add(k.len())?.checked_add(v.len())
                            })
                            .is_none_or(|bytes| bytes > expected.bytes)
                    {
                        return Err(Error::Capacity);
                    }
                    let observed = proof.clone();
                    cpu(move || {
                        if retained != observed {
                            return Err(Error::Identity);
                        }
                        v.into_read()
                    })
                    .await
                }
                _ => Err(Error::Protocol),
            }
        })
        .await
        .map_err(|_| Error::Unavailable)??;
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unavailable);
        }
        Ok(result)
    }
}

fn check_public_operation(operation: &Operation) -> Result<(), Error> {
    if matches!(
        operation,
        Operation::Command {
            command: Command::BindCohort { .. } | Command::ConfirmCohortCapacity { .. },
            ..
        }
    ) {
        return Err(Error::Identity);
    }
    Ok(())
}
fn check_capacity_binding(
    authority: &Authority,
    target: &Target,
    cohort: &Cohort,
) -> Result<(), Error> {
    if cohort.validate().is_err()
        || cohort.profile != CapacityProfile::ByteBound16MiBV1
        || authority.mode == [0; 16]
        || authority.shard != cohort.coordinator_shard
        || !cohort.members.iter().any(|member| {
            member.shard == target.shard
                && member.epoch == target.epoch
                && member.mode == target.mode
        })
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn check_grant_status(
    status: &CapacityGrantStatus,
    authority: &Authority,
    target: &Target,
    cohort: &Cohort,
) -> Result<(), Error> {
    check_capacity_binding(authority, target, cohort)?;
    if status.authority != *authority
        || status.cohort != *cohort
        || status.grant.member.shard != target.shard
        || status.grant.member.epoch != target.epoch
        || status.grant.member.mode != target.mode
        || status.grant.activation.validate().is_err()
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn validate_capacity_endpoints(identity: &Identity, members: &mut [Client]) -> Result<(), Error> {
    identity.validate()?;
    if identity.role != Role::Coordinator
        || identity.cohort.profile != CapacityProfile::ByteBound16MiBV1
        || members.len() != identity.cohort.members.len()
    {
        return Err(Error::Identity);
    }
    members.sort_by_key(|client| client.identity.target.shard);
    let mut groups = BTreeSet::from([identity.group]);
    let mut addresses = BTreeSet::new();
    for (member, client) in identity.cohort.members.iter().zip(members) {
        client.identity.validate()?;
        if client.identity.role != Role::Participant
            || client.identity.authority != identity.authority
            || client.identity.cohort != identity.cohort
            || client.identity.target.shard != member.shard
            || client.identity.target.epoch != member.epoch
            || client.identity.target.mode != member.mode
            // One Raft group owns one shard. A group or address cannot stand
            // in for two members, even if the supplied node IDs differ.
            || !groups.insert(client.identity.group)
            || !addresses.insert(client.address)
        {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
async fn leader_barrier(node: &WorkNode, deadline: tokio::time::Instant) -> Result<(), Error> {
    if tokio::time::Instant::now() >= deadline {
        return Err(Error::Unavailable);
    }
    node.raft
        .ensure_linearizable()
        .await
        .map_err(|_| Error::Unavailable)?;
    if tokio::time::Instant::now() >= deadline {
        return Err(Error::Unavailable);
    }
    Ok(())
}
fn generate_activation_nonce(nonce: &mut [u8; 32]) -> Result<(), Error> {
    getrandom::fill(nonce).map_err(|_| Error::Unavailable)?;
    if *nonce == [0; 32] {
        return Err(Error::Unavailable);
    }
    Ok(())
}
/// Activate a genuinely empty member with a server-owned OS-random generation.
/// This trusted local control-plane helper accepts no caller generation. Retry
/// reads the first durable winner, including after an uncertain proposal result.
pub async fn activate_byte_bound_member(
    node: &NodeHandle,
    authority: &Authority,
    target: &Target,
    cohort: &Cohort,
) -> Result<MemberCapacityGrant, Error> {
    activate_member_with_entropy(node, authority, target, cohort, generate_activation_nonce).await
}
async fn activate_member_with_entropy(
    node: &NodeHandle,
    authority: &Authority,
    target: &Target,
    cohort: &Cohort,
    entropy: fn(&mut [u8; 32]) -> Result<(), Error>,
) -> Result<MemberCapacityGrant, Error> {
    check_capacity_binding(authority, target, cohort)?;
    let node = WorkNode {
        raft: node.raft.clone(),
        state_machine: node.state_machine.clone(),
    };
    let authority = authority.clone();
    let target = target.clone();
    let cohort = cohort.clone();
    let deadline = tokio::time::Instant::now() + LOCAL_TIMEOUT;
    bounded_cpu(deadline, async move {
        leader_barrier(&node, deadline).await?;
        match node
            .state_machine
            .participant_capacity_grant(
                target.shard,
                target.epoch,
                target.mode,
                &authority,
                &cohort,
            )
            .await
        {
            Ok(grant) => return Ok(grant),
            Err(D1Error::VisibilityNotEnabled) => {}
            Err(error) => return Err(application_error(error)),
        }
        // The source getter permits this only for the validated empty baseline.
        // All entropy is obtained before the deterministic activation proposal.
        let mut generation_nonce = [0; 32];
        entropy(&mut generation_nonce)?;
        if generation_nonce == [0; 32] {
            return Err(Error::Unavailable);
        }
        let mut journal = Journal::new(authority.mode).map_err(|_| Error::Identity)?;
        journal
            .apply(Command::BindCohort {
                cohort: cohort.clone(),
            })
            .map_err(|_| Error::Identity)?;
        let proposal = submit(
            &node,
            deadline,
            D1Command::EnableByteBoundReads {
                shard: target.shard,
                epoch: target.epoch,
                mode_id: target.mode,
                journal,
                generation_nonce,
            },
        )
        .await;
        leader_barrier(&node, deadline).await?;
        match node
            .state_machine
            .participant_capacity_grant(
                target.shard,
                target.epoch,
                target.mode,
                &authority,
                &cohort,
            )
            .await
        {
            // A competing first proposal can win. Its retained generation,
            // never this attempt's candidate, is the authoritative response.
            Ok(grant) => Ok(grant),
            Err(error) => Err(proposal.err().unwrap_or_else(|| application_error(error))),
        }
    })
    .await
}
// Keep network fan-out outside the bounded CPU pool: concurrent confirmations
// must not hold every worker permit while waiting for member RPC decoding.
async fn confirm_cohort_capacity(
    node: &NodeHandle,
    pinned: Option<Identity>,
    members: Option<Arc<[Client]>>,
    authority: Authority,
    epoch: u64,
    deadline: tokio::time::Instant,
) -> Result<Value, Error> {
    let identity = pinned.ok_or(Error::Identity)?;
    let members = members.ok_or(Error::Identity)?;
    if identity.role != Role::Coordinator
        || identity.authority != authority
        || identity.target.epoch != epoch
        || identity.cohort.profile != CapacityProfile::ByteBound16MiBV1
    {
        return Err(Error::Identity);
    }
    let Value::Journal(journal) = dispatch_operation(
        node,
        Some(identity.clone()),
        Operation::Journal {
            authority: authority.clone(),
            epoch,
        },
        deadline,
    )
    .await?
    else {
        return Err(Error::Protocol);
    };
    if let Some(grants) = journal.capacity_grants() {
        return Ok(Value::Capacity(CohortCapacity {
            authority,
            cohort: identity.cohort,
            grants: grants.to_vec(),
        }));
    }
    drop(journal);
    let mut grants = Vec::with_capacity(members.len());
    for member in members.iter() {
        // Client timeouts remain capped by the one original server deadline.
        let value = member
            .call_at(
                Operation::GrantStatus {
                    authority: authority.clone(),
                    target: member.identity.target.clone(),
                    cohort: identity.cohort.clone(),
                },
                deadline,
            )
            .await?;
        let Value::Grant(status) = value else {
            return Err(Error::Protocol);
        };
        check_grant_status(
            &status,
            &authority,
            &member.identity.target,
            &identity.cohort,
        )?;
        grants.push(status.grant);
    }
    crate::capacity_policy::validate_grants(&identity.cohort, &grants)
        .map_err(|_| Error::Identity)?;
    let node = WorkNode {
        raft: node.raft.clone(),
        state_machine: node.state_machine.clone(),
    };
    bounded_cpu(deadline, async move {
        leader_barrier(&node, deadline).await?;
        let current = checked_journal(&node, Some(&identity), &authority, epoch).await?;
        if let Some(retained) = current.capacity_grants() {
            if retained != grants.as_slice() {
                return Err(Error::Identity);
            }
            return Ok(Value::Capacity(CohortCapacity {
                authority,
                cohort: identity.cohort,
                grants,
            }));
        }
        let proposal = submit(
            &node,
            deadline,
            D1Command::Coordinator {
                shard: authority.shard,
                epoch,
                mode_id: authority.mode,
                command: Command::ConfirmCohortCapacity {
                    cohort: identity.cohort.clone(),
                    grants: grants.clone(),
                },
            },
        )
        .await;
        // A lost ACK or a concurrent identical proposal is recovered from the
        // durable source. No timeout can authorize replacing a generation.
        leader_barrier(&node, deadline).await?;
        let retained = checked_journal(&node, Some(&identity), &authority, epoch).await?;
        match retained.capacity_grants() {
            Some(saved) if saved == grants.as_slice() => Ok(Value::Capacity(CohortCapacity {
                authority,
                cohort: identity.cohort,
                grants,
            })),
            Some(_) => Err(Error::Identity),
            None => Err(proposal.err().unwrap_or(Error::Protocol)),
        }
    })
    .await
}
async fn retain_prepare_history(
    node: &WorkNode,
    authority: &Authority,
    target: &Target,
    journal: &Journal,
    deadline: tokio::time::Instant,
) -> Result<(), Error> {
    let cut = journal.history_cursor().map_err(|_| Error::Identity)?;
    let mut cursor = node
        .state_machine
        .participant_prepare_history(target.shard, target.epoch, target.mode, journal)
        .await
        .map_err(application_error)?;
    // The durable getter already proves exact prefix compatibility in either
    // direction. Preserve a later local history rather than rolling it back.
    if cursor.event_count >= cut.event_count {
        return Ok(());
    }
    // Every accepted page must move the position; the byte-derived event bound
    // and the original absolute deadline independently bound this loop.
    for _ in 0..journal.capacity_profile().max_history_events() {
        if cursor.event_count >= cut.event_count {
            return Ok(());
        }
        let page = journal
            .history_page(&cursor, Some(&cut))
            .map_err(|_| Error::Identity)?;
        let end = page.end.clone();
        if end.event_count <= cursor.event_count {
            return Err(Error::Identity);
        }
        match submit(
            node,
            deadline,
            D1Command::SyncHistory {
                authority: authority.clone(),
                shard: target.shard,
                epoch: target.epoch,
                mode_id: target.mode,
                page: Box::new(page),
            },
        )
        .await?
        {
            D1Result::HistorySynced {
                cursor: observed,
                applied_prefix,
            } => {
                let status = HistoryStatus {
                    cursor: observed,
                    applied_prefix,
                };
                check_history_status(&status, authority, target, &cut.cohort)?;
                if status.cursor.event_count < end.event_count
                    || (status.cursor.event_count == end.event_count && status.cursor != end)
                {
                    return Err(Error::Identity);
                }
                // Recheck the exact source against any concurrent longer local
                // history before allowing the subsequent prepare command.
                cursor = node
                    .state_machine
                    .participant_prepare_history(target.shard, target.epoch, target.mode, journal)
                    .await
                    .map_err(application_error)?;
                if cursor.event_count < end.event_count {
                    return Err(Error::Identity);
                }
            }
            _ => return Err(Error::Protocol),
        }
    }
    Err(Error::Capacity)
}

fn check_member_cursor(
    cursor: &HistoryCursor,
    authority: &Authority,
    target: &Target,
    cohort: &Cohort,
) -> Result<(), Error> {
    cursor.validate().map_err(|_| Error::Identity)?;
    if cursor.event_count == 0
        || cursor.authority != *authority
        || cursor.cohort != *cohort
        || !cohort
            .members
            .iter()
            .any(|m| m.shard == target.shard && m.epoch == target.epoch && m.mode == target.mode)
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn check_history_status(
    status: &HistoryStatus,
    authority: &Authority,
    target: &Target,
    cohort: &Cohort,
) -> Result<(), Error> {
    check_member_cursor(&status.cursor, authority, target, cohort)?;
    // Every consumed ordinal needs an admission/Begin and terminal decision,
    // in addition to the initial BindCohort. A cursor alone does not prove V.
    if status.applied_prefix > cohort.profile.max_transactions() as u64
        || status.applied_prefix > (status.cursor.event_count - 1) / 2
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn check_history_request(
    authority: &Authority,
    epoch: u64,
    base: &HistoryCursor,
    cut: Option<&HistoryCursor>,
) -> Result<(), Error> {
    base.validate().map_err(|_| Error::Identity)?;
    if base.authority != *authority || base.cohort.coordinator_epoch != epoch {
        return Err(Error::Identity);
    }
    if let Some(cut) = cut {
        cut.validate().map_err(|_| Error::Identity)?;
        if cut.authority != *authority
            || cut.cohort != base.cohort
            || cut.event_count < base.event_count
            || (cut.event_count == base.event_count && cut != base)
        {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
fn check_history_page(
    page: &HistoryPage,
    base: &HistoryCursor,
    cut: Option<&HistoryCursor>,
) -> Result<(), Error> {
    page.validate().map_err(|_| Error::Identity)?;
    if page.base != *base || cut.is_some_and(|cut| page.cut != *cut) {
        return Err(Error::Identity);
    }
    Ok(())
}

fn check_summary(
    value: &CoordinatorSummary,
    authority: &Authority,
    epoch: u64,
) -> Result<(), Error> {
    if value.authority != *authority
        || authority.mode == [0; 16]
        || value.cohort.validate().is_err()
        || value.cohort.coordinator_shard != authority.shard
        || value.cohort.coordinator_epoch != epoch
        || value.resolved_prefix > value.cohort.profile.max_transactions() as u64
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn check_request(
    value: &RequestLookup,
    authority: &Authority,
    epoch: u64,
    id: [u8; 16],
) -> Result<(), Error> {
    if value.authority != *authority || value.cohort.coordinator_epoch != epoch || value.id != id {
        return Err(Error::Identity);
    }
    value.validate().map_err(|_| Error::Protocol)
}

fn evidence<'a>(authority: &Authority, journal: &'a Journal) -> Result<&'a Cohort, Error> {
    let cohort = journal.cohort().ok_or(Error::Identity)?;
    if journal.authority() != authority.mode || cohort.coordinator_shard != authority.shard {
        return Err(Error::Identity);
    }
    Ok(cohort)
}
async fn visibility(
    node: &WorkNode,
    target: &Target,
    pinned: Option<&Identity>,
) -> Result<VisibilityInfo, Error> {
    if pinned.is_some_and(|p| p.role != Role::Participant || p.target != *target) {
        return Err(Error::Identity);
    }
    let info = node
        .state_machine
        .participant_visibility_info(target.shard, target.epoch, target.mode)
        .await
        .map_err(application_error)?;
    if info.baseline_sequence != 0
        || info.cohort.is_none()
        || pinned.is_some_and(|p| {
            p.authority != info.authority || info.cohort.as_ref() != Some(&p.cohort)
        })
    {
        return Err(Error::Identity);
    }
    Ok(info)
}
async fn checked_journal(
    node: &WorkNode,
    pinned: Option<&Identity>,
    authority: &Authority,
    epoch: u64,
) -> Result<Journal, Error> {
    if pinned.is_some_and(|p| {
        p.role != Role::Coordinator || p.authority != *authority || p.target.epoch != epoch
    }) {
        return Err(Error::Identity);
    }
    let journal = node
        .state_machine
        .coordinator_journal(authority.shard, epoch, authority.mode)
        .await
        .map_err(application_error)?;
    if pinned.is_none() && journal.cohort().is_none() {
        return Ok(journal);
    }
    let cohort = evidence(authority, &journal)?;
    if cohort.coordinator_epoch != epoch || pinned.is_some_and(|p| &p.cohort != cohort) {
        return Err(Error::Identity);
    }
    Ok(journal)
}
// Disk format errors do not expand the wire error enum. Peers already understand
// history mismatch and transient unavailability; the ledger stays private.
fn application_error(error: D1Error) -> Error {
    match error {
        D1Error::EventLedgerHistory => Error::Application(D1Error::VisibilityHistory),
        D1Error::EventLedgerUnavailable => Error::Unavailable,
        other => Error::Application(other),
    }
}
async fn submit(
    node: &WorkNode,
    deadline: tokio::time::Instant,
    command: D1Command,
) -> Result<D1Result, Error> {
    if tokio::time::Instant::now() >= deadline {
        return Err(Error::Unavailable);
    }
    node.raft
        .client_write(LogCommand::D1V1(command))
        .await
        .map_err(|_| Error::Unavailable)?
        .data
        .d1
        .ok_or(Error::Protocol)?
        .map_err(application_error)
}
struct WorkNode {
    raft: crate::network::Raft,
    state_machine: Arc<crate::StateMachine>,
}
async fn dispatch(
    node: &NodeHandle,
    pinned: Option<Identity>,
    members: Option<Arc<[Client]>>,
    op: Operation,
    deadline: tokio::time::Instant,
) -> Result<Value, Error> {
    if let Operation::ConfirmCapacity { authority, epoch } = op {
        return confirm_cohort_capacity(node, pinned, members, authority, epoch, deadline).await;
    }
    dispatch_operation(node, pinned, op, deadline).await
}
async fn dispatch_operation(
    node: &NodeHandle,
    pinned: Option<Identity>,
    op: Operation,
    deadline: tokio::time::Instant,
) -> Result<Value, Error> {
    let node = WorkNode {
        raft: node.raft.clone(),
        state_machine: node.state_machine.clone(),
    };
    bounded_cpu(deadline, async move {
        execute(&node, pinned.as_ref(), op, deadline).await
    })
    .await
}
async fn execute(
    node: &WorkNode,
    pinned: Option<&Identity>,
    op: Operation,
    deadline: tokio::time::Instant,
) -> Result<Value, Error> {
    leader_barrier(node, deadline).await?;
    match op {
        Operation::ConfirmCapacity { .. } => Err(Error::Identity),
        Operation::GrantStatus {
            authority,
            target,
            cohort,
        } => {
            let info = visibility(node, &target, pinned).await?;
            if info.authority != authority || info.cohort.as_ref() != Some(&cohort) {
                return Err(Error::Identity);
            }
            let grant = node
                .state_machine
                .participant_capacity_grant(
                    target.shard,
                    target.epoch,
                    target.mode,
                    &authority,
                    &cohort,
                )
                .await
                .map_err(application_error)?;
            let status = CapacityGrantStatus {
                authority: authority.clone(),
                cohort: cohort.clone(),
                grant,
            };
            check_grant_status(&status, &authority, &target, &cohort)?;
            Ok(Value::Grant(status))
        }
        Operation::HistoryStatus {
            authority,
            target,
            cohort,
        } => {
            let info = visibility(node, &target, pinned).await?;
            if info.authority != authority || info.cohort.as_ref() != Some(&cohort) {
                return Err(Error::Identity);
            }
            let (cursor, applied_prefix) = node
                .state_machine
                .participant_history_status(target.shard, target.epoch, target.mode)
                .await
                .map_err(application_error)?;
            let status = HistoryStatus {
                cursor,
                applied_prefix,
            };
            check_history_status(&status, &authority, &target, &cohort)?;
            Ok(Value::HistoryStatus(status))
        }
        Operation::HistoryPage {
            authority,
            epoch,
            base,
            cut,
        } => {
            check_history_request(&authority, epoch, &base, cut.as_ref())?;
            if pinned.is_some_and(|p| {
                p.role != Role::Coordinator
                    || p.authority != authority
                    || p.target.epoch != epoch
                    || p.cohort != base.cohort
            }) {
                return Err(Error::Identity);
            }
            let page = node
                .state_machine
                .coordinator_history_page(
                    authority.shard,
                    epoch,
                    authority.mode,
                    &base,
                    cut.as_ref(),
                )
                .await
                .map_err(application_error)?;
            check_history_page(&page, &base, cut.as_ref())?;
            Ok(Value::HistoryPage(page))
        }
        Operation::SyncHistory {
            authority,
            target,
            page,
        } => {
            page.validate().map_err(|_| Error::Identity)?;
            let info = visibility(node, &target, pinned).await?;
            check_member_cursor(&page.base, &authority, &target, &page.base.cohort)?;
            if info.authority != authority || info.cohort.as_ref() != Some(&page.base.cohort) {
                return Err(Error::Identity);
            }
            let end = page.end.clone();
            match submit(
                node,
                deadline,
                D1Command::SyncHistory {
                    authority: authority.clone(),
                    shard: target.shard,
                    epoch: target.epoch,
                    mode_id: target.mode,
                    page: Box::new(page),
                },
            )
            .await?
            {
                D1Result::HistorySynced {
                    cursor,
                    applied_prefix,
                } => {
                    let status = HistoryStatus {
                        cursor,
                        applied_prefix,
                    };
                    check_history_status(&status, &authority, &target, &end.cohort)?;
                    if status.cursor.event_count < end.event_count
                        || (status.cursor.event_count == end.event_count && status.cursor != end)
                    {
                        return Err(Error::Identity);
                    }
                    Ok(Value::HistorySynced(status))
                }
                _ => Err(Error::Protocol),
            }
        }
        Operation::AdvanceRetained {
            authority,
            target,
            ordinal,
            cut,
        } => {
            let info = visibility(node, &target, pinned).await?;
            check_member_cursor(&cut, &authority, &target, &cut.cohort)?;
            if info.authority != authority || info.cohort.as_ref() != Some(&cut.cohort) {
                return Err(Error::Identity);
            }
            let max_applied = (cut.event_count - 1) / 2;
            let max_transactions = cut.cohort.profile.max_transactions() as u64;
            if ordinal == 0 || ordinal > max_transactions || ordinal > max_applied {
                return Err(Error::Identity);
            }
            match submit(
                node,
                deadline,
                D1Command::AdvanceRetained {
                    authority,
                    shard: target.shard,
                    epoch: target.epoch,
                    mode_id: target.mode,
                    ordinal,
                    cut,
                },
            )
            .await?
            {
                D1Result::Visibility { applied_prefix }
                    if applied_prefix >= ordinal && applied_prefix <= max_transactions =>
                {
                    Ok(Value::RetainedPrefix(applied_prefix))
                }
                _ => Err(Error::Protocol),
            }
        }
        Operation::Summary { authority, epoch } => {
            let value = node
                .state_machine
                .coordinator_summary(authority.shard, epoch, authority.mode)
                .await
                .map_err(application_error)?;
            check_summary(&value, &authority, epoch)?;
            if pinned.is_some_and(|p| {
                p.role != Role::Coordinator
                    || p.authority != authority
                    || p.target.epoch != epoch
                    || p.cohort != value.cohort
            }) {
                return Err(Error::Identity);
            }
            Ok(Value::Summary(value))
        }
        Operation::Terminal {
            authority,
            epoch,
            version,
        } => {
            let proof = node
                .state_machine
                .coordinator_terminal_prefix(authority.shard, epoch, authority.mode, version)
                .await
                .map_err(application_error)?;
            if proof.authority() != authority.mode
                || proof.cohort().coordinator_shard != authority.shard
                || proof.cohort().coordinator_epoch != epoch
                || pinned.is_some_and(|p| {
                    p.role != Role::Coordinator
                        || p.authority != authority
                        || p.target.epoch != epoch
                        || &p.cohort != proof.cohort()
                })
            {
                return Err(Error::Identity);
            }
            Ok(Value::Evidence(proof))
        }
        Operation::LookupRequest {
            authority,
            epoch,
            id,
        } => {
            let value = node
                .state_machine
                .coordinator_request(authority.shard, epoch, authority.mode, id)
                .await
                .map_err(application_error)?;
            check_request(&value, &authority, epoch, id)?;
            if pinned.is_some_and(|p| {
                p.role != Role::Coordinator
                    || p.authority != authority
                    || p.target.epoch != epoch
                    || p.cohort != value.cohort
            }) {
                return Err(Error::Identity);
            }
            Ok(Value::Request(value))
        }
        Operation::Journal { authority, epoch } => Ok(Value::Journal(
            checked_journal(node, pinned, &authority, epoch).await?,
        )),
        Operation::Command {
            authority,
            epoch,
            command,
        } => {
            if pinned.is_some_and(|p| {
                p.role != Role::Coordinator || p.authority != authority || p.target.epoch != epoch
            }) {
                return Err(Error::Identity);
            }
            let cohort = node
                .state_machine
                .coordinator_command_identity(authority.shard, epoch, authority.mode)
                .await
                .map_err(application_error)?;
            if cohort
                .as_ref()
                .is_some_and(|cohort| cohort.coordinator_epoch != epoch)
                || pinned.is_some_and(|p| cohort.as_ref() != Some(&p.cohort))
            {
                return Err(Error::Identity);
            }
            if matches!(
                command,
                Command::BindCohort { .. } | Command::ConfirmCohortCapacity { .. }
            ) {
                return Err(Error::Identity);
            }
            match submit(
                node,
                deadline,
                D1Command::Coordinator {
                    shard: authority.shard,
                    epoch,
                    mode_id: authority.mode,
                    command,
                },
            )
            .await?
            {
                D1Result::Coordinator { .. } => Ok(Value::Unit),
                _ => Err(Error::Protocol),
            }
        }
        Operation::Visibility { target } => {
            Ok(Value::Visibility(visibility(node, &target, pinned).await?))
        }
        Operation::Prepare {
            authority,
            participant,
            request,
            journal,
        } => {
            let target = Target {
                shard: participant.shard,
                epoch: participant.epoch,
                mode: participant.mode,
            };
            let info = visibility(node, &target, pinned).await?;
            let cohort = evidence(&authority, &journal)?;
            if info.authority != authority
                || info.cohort.as_ref() != Some(cohort)
                || request.shard != participant.shard
                || request.epoch != participant.epoch
                || !cohort.contains(&participant)
            {
                return Err(Error::Identity);
            }
            let tx = journal
                .transaction(request.request_id)
                .ok_or(Error::Identity)?;
            if !tx.participants.contains(&participant) || tx.decision.is_some() {
                return Err(Error::Identity);
            }
            if let Some(record) = journal.request_for_transaction(request.request_id) {
                let payload = record.payload.clone();
                let plan =
                    crate::request_plans::Plan::decode(&payload).map_err(|_| Error::Identity)?;
                if record.transaction_id != request.request_id
                    || plan.id != request.request_id
                    || plan.authority != authority
                    || &plan.cohort != cohort
                    || !plan
                        .parts
                        .iter()
                        .any(|p| p.descriptor == participant && p.request == request)
                {
                    return Err(Error::Identity);
                }
            }
            if cohort.profile == CapacityProfile::ByteBound16MiBV1 {
                retain_prepare_history(node, &authority, &target, &journal, deadline).await?;
            }
            match submit(
                node,
                deadline,
                D1Command::PrepareParticipant {
                    authority,
                    participant,
                    request,
                },
            )
            .await?
            {
                D1Result::Participant(status) => Ok(Value::Status(status)),
                _ => Err(Error::Protocol),
            }
        }
        Operation::Advance {
            authority,
            target,
            ordinal,
            journal,
        } => {
            let info = visibility(node, &target, pinned).await?;
            if info.authority != authority
                || info.cohort.as_ref() != Some(evidence(&authority, &journal)?)
            {
                return Err(Error::Identity);
            }
            match submit(
                node,
                deadline,
                D1Command::AdvanceVisibility {
                    authority,
                    shard: target.shard,
                    epoch: target.epoch,
                    mode_id: target.mode,
                    ordinal,
                    journal,
                },
            )
            .await?
            {
                D1Result::Visibility { applied_prefix } => Ok(Value::Prefix(applied_prefix)),
                _ => Err(Error::Protocol),
            }
        }
        Operation::Prefix { target, proof } => {
            let info = visibility(node, &target, pinned).await?;
            if info.cohort.as_ref() != Some(proof.cohort())
                || info.authority.mode != proof.authority()
            {
                return Err(Error::Identity);
            }
            let retained = node
                .state_machine
                .participant_terminal_prefix(target.shard, target.epoch, target.mode, &proof)
                .await
                .map_err(application_error)?;
            Ok(Value::Evidence(retained))
        }
        Operation::Read {
            target,
            limit,
            proof,
        } => {
            let info = visibility(node, &target, pinned).await?;
            if info.cohort.as_ref() != Some(proof.cohort())
                || info.authority.mode != proof.authority()
            {
                return Err(Error::Identity);
            }
            let (snapshot, retained) = node
                .state_machine
                .certified_terminal_read(target.shard, target.epoch, target.mode, limit, &proof)
                .await
                .map_err(application_error)?;
            Ok(Value::Snapshot(snapshot.into(), retained))
        }
    }
}

/// Listener for exactly one configured node. Existing role activation is required.
/// Shutdown stops admission and waits for bounded in-flight handlers.
pub struct Server {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl Server {
    pub async fn bind(
        node: Arc<NodeHandle>,
        identity: Identity,
        address: SocketAddr,
        timeout: Duration,
    ) -> Result<Self, Error> {
        if identity.role == Role::Coordinator
            && identity.cohort.profile == CapacityProfile::ByteBound16MiBV1
        {
            return Err(Error::Identity);
        }
        Self::bind_configured(node, identity, None, address, timeout).await
    }
    /// Bind a byte-budget coordinator to its complete, immutable trusted endpoint set.
    /// Every member, including nonparticipants, is checked during confirmation.
    pub async fn bind_byte_bound_coordinator(
        node: Arc<NodeHandle>,
        identity: Identity,
        mut members: Vec<Client>,
        address: SocketAddr,
        timeout: Duration,
    ) -> Result<Self, Error> {
        validate_capacity_endpoints(&identity, &mut members)?;
        Self::bind_configured(node, identity, Some(members.into()), address, timeout).await
    }
    async fn bind_configured(
        node: Arc<NodeHandle>,
        identity: Identity,
        members: Option<Arc<[Client]>>,
        address: SocketAddr,
        timeout: Duration,
    ) -> Result<Self, Error> {
        identity.validate()?;
        valid_timeout(timeout)?;
        if identity.node != node.id || identity.group != node.group_id() {
            return Err(Error::Identity);
        }
        let probe = match identity.role {
            Role::Coordinator => Operation::Journal {
                authority: identity.authority.clone(),
                epoch: identity.target.epoch,
            },
            Role::Participant => Operation::Visibility {
                target: identity.target.clone(),
            },
        };
        let deadline = tokio::time::Instant::now() + timeout;
        tokio::time::timeout_at(
            deadline,
            dispatch(
                &node,
                Some(identity.clone()),
                members.clone(),
                probe,
                deadline,
            ),
        )
        .await
        .map_err(|_| Error::Unavailable)??;
        let listener = TcpListener::bind(address)
            .await
            .map_err(|_| Error::Unavailable)?;
        let address = listener.local_addr().map_err(|_| Error::Unavailable)?;
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let slots = Arc::new(Semaphore::new(CONCURRENCY));
            let mut handlers = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    Some(_) = handlers.join_next(), if !handlers.is_empty() => {},
                    accepted = listener.accept() => {
                        let Ok((mut stream,_)) = accepted else { break; };
                        let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                        let node = node.clone(); let identity = identity.clone(); let members = members.clone();
                        handlers.spawn(async move {
                            let _permit = permit;
                            let deadline = tokio::time::Instant::now() + timeout;
                            let _ = tokio::time::timeout_at(deadline, async {
                                let request: Request = receive(&mut stream).await?;
                                let operation = request.operation.tag();
                                let result = if request.version != PROTOCOL_VERSION { Err(Error::Protocol) } else if request.identity != identity { Err(Error::Identity) }
                                    else { dispatch(&node,Some(identity.clone()),members,request.operation,deadline).await };
                                let bytes = match encode(Reply { version:PROTOCOL_VERSION,operation,identity:identity.clone(),nonce:request.nonce,result }).await {
                                    Ok(bytes) => bytes,
                                    Err(error) => encode(Reply { version:PROTOCOL_VERSION,operation,identity,nonce:request.nonce,result:Err(error) }).await?,
                                };
                                if tokio::time::Instant::now() >= deadline { return Err(Error::Unavailable); }
                                send(&mut stream,&bytes).await
                            }).await;
                        });
                    }
                }
            }
            while handlers.join_next().await.is_some() {}
        });
        Ok(Self {
            address,
            stop: Some(stop),
            task: Some(task),
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::MAX_TRANSACTIONS;
    #[test]
    fn bounded_writer_and_strict_json() {
        let mut writer = BoundedWriter(vec![0; MAX_FRAME_BYTES - 1]);
        assert!(writer.write_all(&[1, 2]).is_err());
        assert_eq!(writer.0.len(), MAX_FRAME_BYTES - 1);
        assert!(serde_json::from_str::<Operation>(
            r#"{"Visibility":{"target":{"shard":1,"epoch":1,"mode":[0]},"extra":true}}"#
        )
        .is_err());
        assert_eq!(valid_timeout(Duration::ZERO), Err(Error::Capacity));
        assert_eq!(
            valid_timeout(MAX_TIMEOUT + Duration::from_secs(1)),
            Err(Error::Capacity)
        );
    }
    #[test]
    fn application_error_roundtrip() {
        let error = Error::Application(D1Error::VisibilityNotApplied);
        assert_eq!(
            serde_json::from_slice::<Error>(&encode_sync(&error).unwrap()).unwrap(),
            error
        );
    }
    #[tokio::test]
    async fn oversized_length_is_rejected_before_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_u32((MAX_FRAME_BYTES + 1) as u32)
                .await
                .unwrap();
        });
        let (mut stream, _) = listener.accept().await.unwrap();
        assert!(matches!(
            receive::<Request>(&mut stream).await,
            Err(Error::Capacity)
        ));
        writer.await.unwrap();
    }
    fn identity() -> Identity {
        Identity {
            group: 9,
            node: 1,
            target: Target {
                shard: 9,
                epoch: 1,
                mode: [9; 16],
            },
            role: Role::Coordinator,
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: Cohort {
                profile: CapacityProfile::LegacyCountV1,
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![crate::transaction_decisions::Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
            },
        }
    }
    #[test]
    fn rejects_inconsistent_role_target_and_duplicate_snapshot_keys() {
        let mut wrong = identity();
        wrong.role = Role::Participant;
        assert_eq!(wrong.validate(), Err(Error::Identity));
        let mut wrong = identity();
        wrong.target.epoch = 2;
        assert_eq!(wrong.validate(), Err(Error::Identity));
        let id = identity();
        let snapshot = Snapshot {
            shard: 7,
            epoch: 1,
            authority: id.authority,
            version: 0,
            baseline_sequence: 0,
            cohort: Some(id.cohort),
            applied_prefix: 0,
            local_sequence: 0,
            replay_operations: 0,
            entries: vec![(vec![1], vec![2]), (vec![1], vec![3])],
        };
        assert!(matches!(snapshot.into_read(), Err(Error::Protocol)));
    }
    #[tokio::test]
    async fn forged_replies_and_slow_input_fail_closed() {
        for wrong in 0..5 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = Client::new(
                listener.local_addr().unwrap(),
                identity(),
                Duration::from_secs(2),
            )
            .unwrap();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request: Request = receive(&mut stream).await.unwrap();
                let mut reply = Reply {
                    version: PROTOCOL_VERSION,
                    operation: request.operation.tag(),
                    identity: request.identity,
                    nonce: request.nonce,
                    result: Ok(Value::Unit),
                };
                match wrong {
                    0 => reply.identity.group += 1,
                    1 => reply.nonce += 1,
                    2 => reply.operation += 1,
                    3 => reply.version += 1,
                    _ => reply.version = 4,
                }
                send(&mut stream, &encode(reply).await.unwrap())
                    .await
                    .unwrap();
            });
            assert!(matches!(
                client
                    .call(Operation::Journal {
                        authority: identity().authority,
                        epoch: 1
                    })
                    .await,
                Err(Error::Identity)
            ));
            peer.await.unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Client::new(
            listener.local_addr().unwrap(),
            identity(),
            Duration::from_millis(50),
        )
        .unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _: Request = receive(&mut stream).await.unwrap();
            stream.write_u32(100).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        assert!(matches!(
            client
                .call(Operation::Journal {
                    authority: identity().authority,
                    epoch: 1
                })
                .await,
            Err(Error::Unavailable)
        ));
        peer.await.unwrap();
    }
    #[tokio::test]
    async fn server_rejects_unready_state_and_wrong_node_at_startup() {
        let cluster = crate::Cluster::new();
        let node = Arc::new(cluster.in_group(9).start(1).await.unwrap());
        let mut wrong = identity();
        wrong.group = 10;
        assert!(matches!(
            Server::bind(
                node.clone(),
                wrong,
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_millis(50)
            )
            .await,
            Err(Error::Identity)
        ));
        assert!(matches!(
            Server::bind(
                node.clone(),
                identity(),
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_millis(50)
            )
            .await,
            Err(Error::Unavailable)
        ));
        node.raft.shutdown().await.unwrap();
    }
    #[test]
    fn independent_clients_share_outbound_admission() {
        let a = Client::new(
            "127.0.0.1:1".parse().unwrap(),
            identity(),
            Duration::from_secs(1),
        )
        .unwrap();
        let b = Client::new(
            "127.0.0.1:2".parse().unwrap(),
            identity(),
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&a.slots, &b.slots));
    }
    #[test]
    fn terminal_proof_is_encoded_once_and_detects_other_histories() {
        let id = identity();
        let mut observed = Journal::new(id.authority.mode).unwrap();
        observed
            .apply(Command::BindCohort {
                cohort: id.cohort.clone(),
            })
            .unwrap();
        let participant = Participant {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
            intent: [1; 32],
        };
        observed
            .apply(Command::Begin {
                id: [1; 16],
                participants: vec![participant.clone()],
            })
            .unwrap();
        observed
            .apply(Command::Decide {
                id: [1; 16],
                decision: crate::transaction_decisions::Decision::Abort,
            })
            .unwrap();
        let mut other = Journal::new(id.authority.mode).unwrap();
        other
            .apply(Command::BindCohort { cohort: id.cohort })
            .unwrap();
        other
            .apply(Command::Begin {
                id: [2; 16],
                participants: vec![participant],
            })
            .unwrap();
        other
            .apply(Command::Decide {
                id: [2; 16],
                decision: crate::transaction_decisions::Decision::Abort,
            })
            .unwrap();
        assert!(!observed.same_terminal_prefix(&other, 1));
        let journal_bytes = serde_json::to_vec(&observed.terminal_prefix(1).unwrap()).unwrap();
        let reply = encode_sync(&Value::Evidence(observed.terminal_prefix(1).unwrap())).unwrap();
        assert!(reply
            .windows(journal_bytes.len())
            .any(|window| window == journal_bytes));
        assert!(reply.len() < journal_bytes.len() + 32);
        let Value::Evidence(decoded) = serde_json::from_slice::<Value>(&reply).unwrap() else {
            panic!("wrong reply");
        };
        assert_eq!(observed.terminal_prefix(1).unwrap(), decoded);
    }
    #[tokio::test]
    async fn canceled_cpu_caller_retains_permit_until_work_finishes() {
        let slots = Arc::new(Semaphore::new(1));
        let (entered, ready) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let work_slots = slots.clone();
        let work = tokio::spawn(async move {
            cpu_with_slots(work_slots, move || {
                let _ = entered.send(());
                blocked
                    .recv_timeout(Duration::from_secs(2))
                    .map_err(|_| Error::Unavailable)?;
                Ok(())
            })
            .await
        });
        ready.await.unwrap();
        work.abort();
        let _ = work.await;
        assert!(slots.clone().try_acquire_owned().is_err());
        release.send(()).unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(2), slots.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
    }
    #[test]
    fn frame_reserves_full_journal_and_admitted_payload() {
        // Direct Journal JSON is already capped at MAX_JOURNAL_BYTES. A
        // byte-array payload uses at most four JSON bytes per input byte;
        // fixed identity/envelope fields are comfortably below 64 KiB.
        let worst = crate::transaction_decisions::MAX_JOURNAL_BYTES
            + 4 * crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES
            + 64 * 1024;
        assert!(MAX_FRAME_BYTES > worst);
    }
    async fn fake_value(id: Identity, value: Value) -> (Client, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client =
            Client::new(listener.local_addr().unwrap(), id, Duration::from_secs(2)).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request: Request = receive(&mut stream).await.unwrap();
            let reply = Reply {
                version: PROTOCOL_VERSION,
                operation: request.operation.tag(),
                identity: request.identity,
                nonce: request.nonce,
                result: Ok(value),
            };
            send(&mut stream, &encode(reply).await.unwrap())
                .await
                .unwrap();
        });
        (client, peer)
    }
    fn request_lookup() -> RequestLookup {
        let journal =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        RequestLookup {
            authority: identity().authority,
            cohort: identity().cohort,
            resolved_prefix: journal.resolved_prefix(),
            id: [1; 16],
            record: Some(crate::request_index::RecoveredRequest {
                record: journal.request([1; 16]).unwrap().clone(),
                transaction: journal.transaction([2; 16]).unwrap().clone(),
            }),
        }
    }
    async fn fake_request_json(value: serde_json::Value) -> (Client, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Client::new(
            listener.local_addr().unwrap(),
            identity(),
            Duration::from_secs(2),
        )
        .unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request: Request = receive(&mut stream).await.unwrap();
            assert!(matches!(
                request.operation,
                Operation::LookupRequest { id: [1, ..], .. }
            ));
            let reply = serde_json::json!({
                "version": PROTOCOL_VERSION,
                "operation": request.operation.tag(),
                "identity": request.identity,
                "nonce": request.nonce,
                "result": {"Ok": {"Request": value}},
            });
            send(&mut stream, &encode(reply).await.unwrap())
                .await
                .unwrap();
        });
        (client, peer)
    }
    #[tokio::test]
    async fn point_request_rejects_invalid_outbound_identity_without_connecting() {
        let mut participant = identity();
        participant.role = Role::Participant;
        participant.target = Target {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
        };
        for (pinned, authority, epoch, id) in [
            (identity(), identity().authority, 1, [0; 16]),
            (identity(), identity().authority, 2, [1; 16]),
            (
                identity(),
                Authority {
                    shard: 9,
                    mode: [8; 16],
                },
                1,
                [1; 16],
            ),
            (participant, identity().authority, 1, [1; 16]),
        ] {
            let client = Client::new(
                "127.0.0.1:1".parse().unwrap(),
                pinned,
                Duration::from_secs(1),
            )
            .unwrap();
            assert_eq!(
                NodeRef::Remote(&client)
                    .request(&authority, epoch, id)
                    .await,
                Err(Error::Identity)
            );
        }
    }
    #[tokio::test]
    async fn point_request_returns_exact_record_or_absence_without_journal() {
        for present in [true, false] {
            let mut lookup = request_lookup();
            if !present {
                lookup.record = None;
            }
            let (client, peer) = fake_request_json(serde_json::to_value(&lookup).unwrap()).await;
            assert_eq!(
                NodeRef::Remote(&client)
                    .request(&identity().authority, 1, [1; 16])
                    .await
                    .unwrap(),
                lookup
            );
            peer.await.unwrap();
        }
    }
    #[tokio::test]
    async fn point_request_rejects_substituted_and_malformed_evidence() {
        for fault in 0..25 {
            let mut value = serde_json::to_value(request_lookup()).unwrap();
            match fault {
                0 => value["id"] = serde_json::json!(vec![2; 16]),
                1 => value["authority"]["shard"] = 8.into(),
                2 => value["cohort"]["coordinator_epoch"] = 2.into(),
                3 => value["cohort"]["members"][0]["epoch"] = 2.into(),
                4 => value["record"]["record"]["id"] = serde_json::json!(vec![2; 16]),
                5 => value["record"]["record"]["transaction_id"] = serde_json::json!(vec![3; 16]),
                6 => value["record"]["transaction"]["id"] = serde_json::json!(vec![3; 16]),
                7 => {
                    value["record"]["transaction"]["participants"][0]["intent"] =
                        serde_json::json!(vec![5; 32])
                }
                8 => value["record"]["transaction"]["prepared"] = serde_json::json!([]),
                9 => value["record"]["record"]["payload"] = serde_json::json!([0]),
                10..=12 => {
                    let mut plan: serde_json::Value = serde_json::from_slice(include_bytes!(
                        "../tests/fixtures/request-plan-v1.json"
                    ))
                    .unwrap();
                    match fault {
                        10 => plan["version"] = 2.into(),
                        11 => plan["id"] = serde_json::json!(vec![3; 16]),
                        _ => plan["extra"] = true.into(),
                    }
                    value["record"]["record"]["payload"] =
                        serde_json::json!(serde_json::to_vec(&plan).unwrap());
                }
                13 => value["extra"] = true.into(),
                14 => value["record"]["extra"] = true.into(),
                15 => value["record"]["record"]["extra"] = true.into(),
                16 => value["record"]["transaction"]["extra"] = true.into(),
                17 => {
                    value["record"]["record"]["request"] = serde_json::json!(vec![
                        1;
                        crate::transaction_decisions::MAX_REQUEST_BYTES
                            + 1
                    ])
                }
                18 => {
                    value["record"]["record"]["payload"] = serde_json::json!(vec![
                        1;
                        crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES
                            + 1
                    ])
                }
                19 => value["record"]["transaction"]["ordinal"] = 0.into(),
                20 => {
                    value["resolved_prefix"] =
                        (crate::transaction_decisions::MAX_TRANSACTIONS as u64 + 1).into()
                }
                21 => {
                    value["record"] = serde_json::Value::Null;
                    value["cohort"]["members"][0]["epoch"] = 2.into();
                }
                22 => value["authority"]["mode"] = serde_json::json!(vec![8; 16]),
                23 => {
                    let mut plan: serde_json::Value = serde_json::from_slice(include_bytes!(
                        "../tests/fixtures/request-plan-v1.json"
                    ))
                    .unwrap();
                    plan["validation_version"] = 1.into();
                    value["record"]["record"]["payload"] =
                        serde_json::json!(serde_json::to_vec(&plan).unwrap());
                }
                _ => {
                    // A terminal transaction may be behind the current watermark,
                    // but its original admission cut must precede its own ordinal.
                    let mut plan: serde_json::Value = serde_json::from_slice(include_bytes!(
                        "../tests/fixtures/request-plan-v1.json"
                    ))
                    .unwrap();
                    plan["validation_version"] = 128.into();
                    value["resolved_prefix"] = 128.into();
                    value["record"]["transaction"]["ordinal"] = 1.into();
                    value["record"]["transaction"]["decision"] = serde_json::json!("Abort");
                    value["record"]["record"]["payload"] =
                        serde_json::json!(serde_json::to_vec(&plan).unwrap());
                }
            }
            let (client, peer) = fake_request_json(value).await;
            assert!(
                matches!(
                    NodeRef::Remote(&client)
                        .request(&identity().authority, 1, [1; 16])
                        .await,
                    Err(Error::Identity | Error::Protocol)
                ),
                "fault {fault}"
            );
            peer.await.unwrap();
        }
    }
    #[test]
    fn point_request_nested_duplicate_fields_are_rejected() {
        let found = request_lookup().record.unwrap();
        let record = serde_json::to_string(&found.record).unwrap();
        let duplicate = record.replacen("\"request\":", "\"request\":[],\"request\":", 1);
        assert!(
            serde_json::from_str::<crate::transaction_decisions::RequestRecord>(&duplicate)
                .is_err()
        );
        let tx = serde_json::to_string(&found.transaction).unwrap();
        let duplicate = tx.replacen("\"ordinal\":", "\"ordinal\":1,\"ordinal\":", 1);
        assert!(
            serde_json::from_str::<crate::transaction_decisions::Transaction>(&duplicate).is_err()
        );
    }
    fn terminal_journal(tx: u8) -> Journal {
        let mut journal = Journal::new(identity().authority.mode).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: identity().cohort,
            })
            .unwrap();
        journal
            .apply(Command::Begin {
                id: [tx; 16],
                participants: vec![Participant {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                    intent: [1; 32],
                }],
            })
            .unwrap();
        journal
            .apply(Command::Decide {
                id: [tx; 16],
                decision: crate::transaction_decisions::Decision::Abort,
            })
            .unwrap();
        journal
    }
    fn participant_identity() -> Identity {
        let mut id = identity();
        id.role = Role::Participant;
        id.target = Target {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
        };
        id
    }
    #[test]
    fn history_wire_v5_operation_tags_are_explicit() {
        let journal = terminal_journal(1);
        let base = journal.history_cursor_at(1).unwrap();
        let page = journal.history_page(&base, None).unwrap();
        let id = participant_identity();
        let operations = [
            Operation::HistoryStatus {
                authority: id.authority.clone(),
                target: id.target.clone(),
                cohort: id.cohort.clone(),
            },
            Operation::HistoryPage {
                authority: id.authority.clone(),
                epoch: 1,
                base: base.clone(),
                cut: Some(page.cut.clone()),
            },
            Operation::SyncHistory {
                authority: id.authority.clone(),
                target: id.target.clone(),
                page: page.clone(),
            },
            Operation::AdvanceRetained {
                authority: id.authority.clone(),
                target: id.target.clone(),
                ordinal: 1,
                cut: page.cut,
            },
        ];
        assert_eq!(PROTOCOL_VERSION, 5);
        assert_eq!(operations.each_ref().map(Operation::tag), [11, 12, 13, 14]);
        for (operation, name) in operations.into_iter().zip([
            "HistoryStatus",
            "HistoryPage",
            "SyncHistory",
            "AdvanceRetained",
        ]) {
            let request = Request {
                version: PROTOCOL_VERSION,
                identity: id.clone(),
                nonce: 17,
                operation,
            };
            let bytes = encode_sync(&request).unwrap();
            let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(wire["version"], 5);
            assert_eq!(wire["nonce"], 17);
            assert_eq!(
                wire["operation"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .collect::<Vec<_>>(),
                vec![name]
            );
            let decoded: Request = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(decoded.operation.tag(), request.operation.tag());
        }
    }
    #[tokio::test]
    async fn matching_envelopes_cannot_substitute_history_status_or_sync_cursor() {
        let journal = terminal_journal(1);
        let base = journal.history_cursor_at(1).unwrap();
        let page = journal.history_page(&base, None).unwrap();
        let id = participant_identity();
        for fault in 0..7 {
            let mut status = HistoryStatus {
                cursor: page.end.clone(),
                applied_prefix: 1,
            };
            match fault {
                0 => status.cursor.authority.mode = [8; 16],
                1 => status.cursor.authority.shard = 8,
                2 => status.cursor.cohort.members[0].epoch = 2,
                3 => status.cursor.version = 2,
                4 => status.cursor.event_count = 0,
                5 => status.cursor.event_count = u64::MAX,
                _ => status.applied_prefix = MAX_TRANSACTIONS as u64 + 1,
            }
            let (client, peer) = fake_value(id.clone(), Value::HistoryStatus(status)).await;
            assert!(
                matches!(
                    NodeRef::Remote(&client)
                        .history_status(&id.authority, &id.target, &id.cohort)
                        .await,
                    Err(Error::Identity | Error::Protocol)
                ),
                "fault {fault}"
            );
            peer.await.unwrap();
        }
        for fault in 0..4 {
            let mut status = HistoryStatus {
                cursor: page.end.clone(),
                applied_prefix: 0,
            };
            match fault {
                0 => status.cursor.root[0] ^= 1,
                1 => status.cursor = base.clone(),
                2 => status.cursor.cohort.members[0].mode = [8; 16],
                _ => status.applied_prefix = 2,
            }
            let (client, peer) = fake_value(id.clone(), Value::HistorySynced(status)).await;
            assert!(
                matches!(
                    NodeRef::Remote(&client)
                        .sync_history(&id.authority, &id.target, &page)
                        .await,
                    Err(Error::Identity | Error::Protocol)
                ),
                "fault {fault}"
            );
            peer.await.unwrap();
        }
        let (client, peer) = fake_value(id.clone(), Value::RetainedPrefix(0)).await;
        assert_eq!(
            NodeRef::Remote(&client)
                .advance_retained(&id.authority, &id.target, 1, &page.cut)
                .await,
            Err(Error::Identity)
        );
        peer.await.unwrap();
    }
    #[tokio::test]
    async fn matching_envelopes_cannot_substitute_history_page_base_cut_or_chain() {
        let journal = terminal_journal(1);
        let base = journal.history_cursor_at(1).unwrap();
        let page = journal.history_page(&base, None).unwrap();
        for fault in 0..9 {
            let mut changed = page.clone();
            match fault {
                0 => changed.base.root[0] ^= 1,
                1 => changed.end.root[0] ^= 1,
                2 => changed.cut.root[0] ^= 1,
                3 => changed.end.event_count += 1,
                4 => changed.events.reverse(),
                5 => {
                    let _ = changed.events.pop();
                }
                6 => changed.cut.cohort.members[0].mode = [8; 16],
                7 => changed.version = 2,
                // This is a valid page for a different base, despite matching envelope.
                _ => changed = journal.history_page(&page.end, Some(&page.cut)).unwrap(),
            }
            let (client, peer) = fake_value(identity(), Value::HistoryPage(changed)).await;
            assert!(
                matches!(
                    NodeRef::Remote(&client)
                        .history_page(&identity().authority, 1, &base, Some(&page.cut))
                        .await,
                    Err(Error::Identity | Error::Protocol)
                ),
                "fault {fault}"
            );
            peer.await.unwrap();
        }
        // A valid longer cut must not replace the frozen source cut.
        let mut later = journal.clone();
        later
            .apply(Command::Begin {
                id: [2; 16],
                participants: vec![Participant {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                    intent: [2; 32],
                }],
            })
            .unwrap();
        let changed = later.history_page(&base, None).unwrap();
        let (client, peer) = fake_value(identity(), Value::HistoryPage(changed)).await;
        assert_eq!(
            NodeRef::Remote(&client)
                .history_page(&identity().authority, 1, &base, Some(&page.cut))
                .await,
            Err(Error::Identity)
        );
        peer.await.unwrap();
    }
    #[tokio::test]
    async fn matching_envelopes_cannot_substitute_journal_visibility_or_read_evidence() {
        let observed = terminal_journal(1);
        let mut wrong_cohort = identity().cohort;
        wrong_cohort.members[0].epoch = 2;
        let mut bad_journal = Journal::new(identity().authority.mode).unwrap();
        bad_journal
            .apply(Command::BindCohort {
                cohort: wrong_cohort,
            })
            .unwrap();
        let (client, peer) = fake_value(identity(), Value::Journal(bad_journal)).await;
        assert!(matches!(
            NodeRef::Remote(&client)
                .journal(&identity().authority, 1)
                .await,
            Err(Error::Identity)
        ));
        peer.await.unwrap();

        let mut participant_id = identity();
        participant_id.role = Role::Participant;
        participant_id.target = Target {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
        };
        let target = participant_id.target.clone();
        let info = VisibilityInfo {
            authority: identity().authority,
            baseline_sequence: 1,
            cohort: Some(identity().cohort),
            applied_prefix: 1,
        };
        let (client, peer) = fake_value(participant_id.clone(), Value::Visibility(info)).await;
        assert!(matches!(
            NodeRef::Remote(&client).visibility(&target).await,
            Err(Error::Identity)
        ));
        peer.await.unwrap();

        let (client, peer) = fake_value(
            participant_id.clone(),
            Value::Evidence(terminal_journal(2).terminal_prefix(1).unwrap()),
        )
        .await;
        assert!(matches!(
            NodeRef::Remote(&client).prefix(&target, 1, &observed).await,
            Err(Error::Identity)
        ));
        peer.await.unwrap();
        for fault in 0..5 {
            let mut snapshot = Snapshot {
                shard: 7,
                epoch: 1,
                authority: identity().authority,
                version: 1,
                baseline_sequence: 0,
                cohort: Some(identity().cohort),
                applied_prefix: 1,
                local_sequence: 0,
                replay_operations: 1,
                entries: vec![(vec![1], vec![2])],
            };
            let mut retained = observed.clone();
            match fault {
                0 => retained = terminal_journal(2),
                1 => snapshot.version = 0,
                2 => snapshot.entries.push((vec![3], vec![4])),
                3 => snapshot.entries[0].1 = vec![2; 10],
                _ => snapshot.replay_operations = 2,
            }
            let (client, peer) = fake_value(
                participant_id.clone(),
                Value::Snapshot(snapshot, retained.terminal_prefix(1).unwrap()),
            )
            .await;
            let result = NodeRef::Remote(&client)
                .read(
                    &target,
                    1,
                    ReadLimit {
                        rows: 1,
                        bytes: 2,
                        operations: 1,
                    },
                    &observed,
                )
                .await;
            if fault < 2 {
                assert!(matches!(result, Err(Error::Identity)));
            } else {
                assert!(matches!(result, Err(Error::Capacity)));
            }
            peer.await.unwrap();
        }
    }
    #[tokio::test]
    async fn acquisition_requests_only_summary_and_chosen_cut_not_full_archive() {
        async fn sequence(id: Identity, values: Vec<Value>) -> (Client, JoinHandle<Vec<u8>>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client =
                Client::new(listener.local_addr().unwrap(), id, Duration::from_secs(2)).unwrap();
            let server = tokio::spawn(async move {
                let mut tags = Vec::new();
                for value in values {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let request: Request = receive(&mut stream).await.unwrap();
                    tags.push(request.operation.tag());
                    match &request.operation {
                        Operation::Journal { .. } => panic!("acquisition fetched full archive"),
                        Operation::Terminal { version, .. } => assert_eq!(*version, 1),
                        Operation::Read { proof, .. } => assert_eq!(proof.version(), 1),
                        _ => {}
                    }
                    let reply = Reply {
                        version: PROTOCOL_VERSION,
                        operation: request.operation.tag(),
                        identity: request.identity,
                        nonce: request.nonce,
                        result: Ok(value),
                    };
                    send(&mut stream, &encode(reply).await.unwrap())
                        .await
                        .unwrap();
                }
                tags
            });
            (client, server)
        }
        let proof = terminal_journal(1).terminal_prefix(1).unwrap();
        let (coordinator, coordinator_calls) = sequence(
            identity(),
            vec![
                Value::Summary(CoordinatorSummary {
                    authority: identity().authority,
                    cohort: identity().cohort,
                    resolved_prefix: 128,
                }),
                Value::Evidence(proof.clone()),
            ],
        )
        .await;
        let mut member = identity();
        member.role = Role::Participant;
        member.target = Target {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
        };
        let (participant, participant_calls) = sequence(
            member.clone(),
            vec![
                Value::Visibility(VisibilityInfo {
                    authority: identity().authority,
                    baseline_sequence: 0,
                    cohort: Some(identity().cohort),
                    applied_prefix: 128,
                }),
                Value::Snapshot(
                    Snapshot {
                        shard: 7,
                        epoch: 1,
                        authority: identity().authority,
                        version: 1,
                        baseline_sequence: 0,
                        cohort: Some(identity().cohort),
                        applied_prefix: 128,
                        local_sequence: 0,
                        replay_operations: 0,
                        entries: vec![],
                    },
                    proof,
                ),
            ],
        )
        .await;
        let session = crate::cohort_reads::acquire_endpoints(
            NodeRef::Remote(&coordinator),
            identity().authority,
            1,
            vec![(NodeRef::Remote(&participant), member.target)],
            Some(1),
            ReadLimit {
                rows: 1,
                bytes: 1,
                operations: 1,
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(session.version(), Ok(1));
        assert_eq!(coordinator_calls.await.unwrap(), vec![8, 9]);
        assert_eq!(participant_calls.await.unwrap(), vec![3, 7]);
    }
    #[tokio::test]
    async fn summary_and_terminal_reply_identity_and_version_are_not_certificates() {
        for fault in 0..5 {
            let mut summary = CoordinatorSummary {
                authority: identity().authority,
                cohort: identity().cohort,
                resolved_prefix: 1,
            };
            match fault {
                0 => summary.authority.mode = [8; 16],
                1 => summary.cohort.coordinator_epoch = 2,
                2 => summary.cohort.members[0].mode = [8; 16],
                3 => summary.cohort.members.clear(),
                _ => {
                    summary.resolved_prefix =
                        crate::transaction_decisions::MAX_TRANSACTIONS as u64 + 1
                }
            }
            let (client, peer) = fake_value(identity(), Value::Summary(summary)).await;
            assert_eq!(
                NodeRef::Remote(&client)
                    .summary(&identity().authority, 1)
                    .await,
                Err(Error::Identity)
            );
            peer.await.unwrap();
        }
        // A correctly framed proof for another cut (including V=0) is rejected.
        for (requested, returned) in [(0, 1), (1, 0)] {
            let (client, peer) = fake_value(
                identity(),
                Value::Evidence(terminal_journal(1).terminal_prefix(returned).unwrap()),
            )
            .await;
            assert_eq!(
                NodeRef::Remote(&client)
                    .terminal(&identity().authority, 1, requested)
                    .await,
                Err(Error::Identity)
            );
            peer.await.unwrap();
        }
        // An empty proof still has a complete, exact authority/cohort binding.
        let mut other_cohort = identity().cohort;
        other_cohort.members[0].mode = [6; 16];
        let mut other = Journal::new(identity().authority.mode).unwrap();
        other
            .apply(Command::BindCohort {
                cohort: other_cohort,
            })
            .unwrap();
        let (client, peer) = fake_value(
            identity(),
            Value::Evidence(other.terminal_prefix(0).unwrap()),
        )
        .await;
        assert_eq!(
            NodeRef::Remote(&client)
                .terminal(&identity().authority, 1, 0)
                .await,
            Err(Error::Identity)
        );
        peer.await.unwrap();
        let mut wrong = Journal::new([8; 16]).unwrap();
        wrong
            .apply(Command::BindCohort {
                cohort: identity().cohort,
            })
            .unwrap();
        let (client, peer) = fake_value(
            identity(),
            Value::Evidence(wrong.terminal_prefix(0).unwrap()),
        )
        .await;
        assert_eq!(
            NodeRef::Remote(&client)
                .terminal(&identity().authority, 1, 0)
                .await,
            Err(Error::Identity)
        );
        peer.await.unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_server_bounds_incomplete_input_and_remains_available() {
        let cluster = crate::Cluster::new();
        let node = Arc::new(cluster.in_group(9).start(1).await.unwrap());
        node.raft
            .initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
            .await
            .unwrap();
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |m| m.current_leader == Some(1) && m.last_applied.is_some(),
                "leader",
            )
            .await
            .unwrap();
        for command in [
            D1Command::Activate { shard: 9, epoch: 1 },
            D1Command::EnableManagedWrites {
                shard: 9,
                epoch: 1,
                expected_sequence: 0,
                mode_id: [9; 16],
            },
            D1Command::EnableCoordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
            },
            D1Command::Coordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
                command: Command::BindCohort {
                    cohort: identity().cohort,
                },
            },
        ] {
            if matches!(command, D1Command::Coordinator { .. }) {
                let result = crate::cohort_reads::acquire(
                    &node,
                    identity().authority,
                    1,
                    Vec::new(),
                    None,
                    ReadLimit {
                        rows: 1,
                        bytes: 1,
                        operations: 1,
                    },
                    Duration::from_secs(1),
                )
                .await;
                assert!(matches!(
                    result,
                    Err(crate::cohort_reads::Error::UnboundCohort)
                ));
            }
            node.raft
                .client_write(LogCommand::D1V1(command))
                .await
                .unwrap()
                .data
                .d1
                .unwrap()
                .unwrap();
        }
        let server = Server::bind(
            node.clone(),
            identity(),
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_millis(200),
        )
        .await
        .unwrap();
        // Previous wire peers fail closed. There is no automatic archive or
        // weaker-read fallback even though durable journal formats are unchanged.
        let mut legacy = TcpStream::connect(server.local_addr()).await.unwrap();
        send(
            &mut legacy,
            &encode_sync(&Request {
                version: 4,
                identity: identity(),
                nonce: 1,
                operation: Operation::Journal {
                    authority: identity().authority,
                    epoch: 1,
                },
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let reply: Reply = receive(&mut legacy).await.unwrap();
        assert_eq!(reply.version, PROTOCOL_VERSION);
        assert!(matches!(reply.result, Err(Error::Protocol)));
        let mut slow = TcpStream::connect(server.local_addr()).await.unwrap();
        slow.write_u32(100).await.unwrap();
        slow.write_all(b"{").await.unwrap();
        let mut byte = [0u8; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), slow.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        let client = Client::new(server.local_addr(), identity(), Duration::from_secs(2)).unwrap();
        assert_eq!(
            NodeRef::Remote(&client)
                .journal(&identity().authority, 1)
                .await
                .unwrap()
                .transaction_count(),
            0
        );
        server.shutdown().await;
        node.raft.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn wrong_fixed_target_mode_role_and_history_never_connect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut id = identity();
        id.role = Role::Participant;
        id.target = Target {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
        };
        let client = Client::new(
            listener.local_addr().unwrap(),
            id.clone(),
            Duration::from_secs(1),
        )
        .unwrap();
        let node = NodeRef::Remote(&client);
        let journal = terminal_journal(1);
        let mut wrong = id.target.clone();
        wrong.mode = [8; 16];
        assert!(matches!(
            node.prefix(&wrong, 1, &journal).await,
            Err(Error::Identity)
        ));
        assert!(matches!(
            node.read(
                &wrong,
                1,
                ReadLimit {
                    rows: 1,
                    bytes: 1,
                    operations: 1
                },
                &journal
            )
            .await,
            Err(Error::Identity)
        ));
        assert!(matches!(
            node.advance(&id.authority, &wrong, 1, &journal).await,
            Err(Error::Identity)
        ));
        let descriptor = Participant {
            shard: wrong.shard,
            epoch: wrong.epoch,
            mode: wrong.mode,
            intent: [1; 32],
        };
        let request = D1WriteRequest {
            shard: 7,
            epoch: 1,
            request_id: [1; 16],
            expected_sequence: Some(0),
            operations: vec![crate::WriteOp::put("a", "b")],
        };
        assert!(matches!(
            node.prepare(&id.authority, &descriptor, request, &journal)
                .await,
            Err(Error::Identity)
        ));
        assert!(matches!(
            node.command(
                &id.authority,
                1,
                Command::Decide {
                    id: [1; 16],
                    decision: crate::transaction_decisions::Decision::Abort
                }
            )
            .await,
            Err(Error::Identity)
        ));
        let mut other = Journal::new([8; 16]).unwrap();
        other
            .apply(Command::BindCohort { cohort: id.cohort })
            .unwrap();
        assert!(matches!(
            node.prefix(&id.target, 0, &other).await,
            Err(Error::Identity)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }

    fn byte_identity() -> Identity {
        let mut id = identity();
        id.cohort.profile = CapacityProfile::ByteBound16MiBV1;
        id.cohort
            .members
            .push(crate::transaction_decisions::Member {
                shard: 8,
                epoch: 1,
                mode: [8; 16],
            });
        id
    }
    fn byte_member_identity(shard: u64) -> Identity {
        let mut id = byte_identity();
        let member = id
            .cohort
            .members
            .iter()
            .find(|member| member.shard == shard)
            .unwrap();
        id.group = shard;
        id.target = Target {
            shard,
            epoch: member.epoch,
            mode: member.mode,
        };
        id.role = Role::Participant;
        id
    }
    fn byte_grants() -> Vec<MemberCapacityGrant> {
        byte_identity()
            .cohort
            .members
            .into_iter()
            .map(|member| MemberCapacityGrant {
                activation: crate::transaction_decisions::CapacityActivation {
                    nonce: [member.shard as u8; 32],
                    log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 4),
                },
                member,
            })
            .collect()
    }
    async fn capacity_node(cluster: &crate::Cluster, identity: &Identity) -> Arc<NodeHandle> {
        let node = Arc::new(
            cluster
                .in_group(identity.group)
                .start(identity.node)
                .await
                .unwrap(),
        );
        node.raft
            .initialize(BTreeMap::from([(
                identity.node,
                openraft::BasicNode::default(),
            )]))
            .await
            .unwrap();
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    metrics.current_leader == Some(identity.node) && metrics.last_applied.is_some()
                },
                "capacity leader",
            )
            .await
            .unwrap();
        let mut commands = vec![
            D1Command::Activate {
                shard: identity.target.shard,
                epoch: identity.target.epoch,
            },
            D1Command::EnableManagedWrites {
                shard: identity.target.shard,
                epoch: identity.target.epoch,
                expected_sequence: 0,
                mode_id: identity.target.mode,
            },
        ];
        match identity.role {
            Role::Coordinator => {
                commands.push(D1Command::EnableCoordinator {
                    shard: identity.target.shard,
                    epoch: identity.target.epoch,
                    mode_id: identity.target.mode,
                });
                commands.push(D1Command::Coordinator {
                    shard: identity.target.shard,
                    epoch: identity.target.epoch,
                    mode_id: identity.target.mode,
                    command: Command::BindCohort {
                        cohort: identity.cohort.clone(),
                    },
                });
            }
            Role::Participant => commands.push(D1Command::EnableParticipant {
                shard: identity.target.shard,
                epoch: identity.target.epoch,
                mode_id: identity.target.mode,
                authority: identity.authority.clone(),
            }),
        }
        for command in commands {
            node.raft
                .client_write(LogCommand::D1V1(command))
                .await
                .unwrap()
                .data
                .d1
                .unwrap()
                .unwrap();
        }
        node
    }
    async fn capacity_client(node: Arc<NodeHandle>, identity: Identity) -> (Client, Server) {
        let server = Server::bind(
            node,
            identity.clone(),
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let client = Client::new(server.local_addr(), identity, Duration::from_secs(2)).unwrap();
        (client, server)
    }
    #[test]
    fn capacity_endpoint_set_is_complete_unique_and_profile_bound() {
        let id = byte_identity();
        let client = |shard: u64| {
            Client::new(
                format!("127.0.0.1:{}", 10000 + shard).parse().unwrap(),
                byte_member_identity(shard),
                Duration::from_secs(1),
            )
            .unwrap()
        };
        let original = vec![client(7), client(8)];
        for fault in 0..10 {
            let mut clients = original.clone();
            match fault {
                0 => {
                    clients.pop();
                }
                1 => clients.push(client(7)),
                2 => clients[1] = clients[0].clone(),
                3 => clients[1].identity.cohort.profile = CapacityProfile::LegacyCountV1,
                4 => clients[1].identity.authority.mode = [3; 16],
                5 => clients[1].identity.role = Role::Coordinator,
                6 => clients[1].identity.target.epoch += 1,
                7 => clients[1].identity.group = clients[0].identity.group,
                8 => clients[1].address = clients[0].address,
                _ => clients[1].identity.group = id.group,
            }
            assert_eq!(
                validate_capacity_endpoints(&id, &mut clients),
                Err(Error::Identity),
                "fault {fault}"
            );
        }
        let mut reverse = vec![client(8), client(7)];
        validate_capacity_endpoints(&id, &mut reverse).unwrap();
        assert_eq!(reverse[0].identity.target.shard, 7);
        assert!(serde_json::from_value::<Operation>(serde_json::json!({
            "ConfirmCapacity": { "authority": id.authority, "epoch": 1, "grants": byte_grants() },
        }))
        .is_err());
        assert!(serde_json::from_value::<Operation>(serde_json::json!({
            "GrantStatus": { "authority": id.authority, "target": byte_member_identity(7).target,
                "cohort": id.cohort, "generation_nonce": vec![7; 32] },
        }))
        .is_err());
    }
    #[tokio::test]
    async fn matching_envelopes_cannot_substitute_capacity_binding_or_generation_shape() {
        let id = byte_member_identity(7);
        for fault in 0..8 {
            let mut status = CapacityGrantStatus {
                authority: id.authority.clone(),
                cohort: id.cohort.clone(),
                grant: byte_grants().remove(0),
            };
            match fault {
                0 => status.authority.mode = [3; 16],
                1 => status.cohort.profile = CapacityProfile::LegacyCountV1,
                2 => status.cohort.coordinator_epoch += 1,
                3 => status.grant.member.shard = 8,
                4 => status.grant.member.mode = [8; 16],
                5 => status.grant.activation.nonce = [0; 32],
                6 => status.grant.activation.log_id.index = 0,
                _ => status.grant.activation.log_id.leader_id.term = 0,
            }
            let (client, peer) = fake_value(id.clone(), Value::Grant(status)).await;
            assert_eq!(
                NodeRef::Remote(&client)
                    .grant_status(&id.authority, &id.target, &id.cohort)
                    .await,
                Err(Error::Identity),
                "fault {fault}"
            );
            peer.await.unwrap();
        }
        for fault in 0..6 {
            let coordinator = byte_identity();
            let mut value = CohortCapacity {
                authority: coordinator.authority.clone(),
                cohort: coordinator.cohort.clone(),
                grants: byte_grants(),
            };
            match fault {
                0 => value.cohort.profile = CapacityProfile::LegacyCountV1,
                1 => {
                    value.grants.pop();
                }
                2 => value.grants.reverse(),
                3 => value.grants[1] = value.grants[0].clone(),
                4 => value.grants[0].activation.nonce = [0; 32],
                _ => value.authority.mode = [3; 16],
            }
            let (client, peer) = fake_value(coordinator.clone(), Value::Capacity(value)).await;
            assert_eq!(
                NodeRef::Remote(&client)
                    .confirm_capacity(&coordinator.authority, 1)
                    .await,
                Err(Error::Identity),
                "confirmation fault {fault}"
            );
            peer.await.unwrap();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn activation_entropy_failure_and_lost_ack_retry_preserve_first_generation() {
        let cluster = crate::Cluster::new();
        let id = byte_member_identity(7);
        let node = capacity_node(&cluster, &id).await;
        let before = node.raft.metrics().borrow().last_log_index;
        assert_eq!(
            activate_member_with_entropy(&node, &id.authority, &id.target, &id.cohort, |_| Err(
                Error::Unavailable
            ))
            .await,
            Err(Error::Unavailable)
        );
        assert_eq!(node.raft.metrics().borrow().last_log_index, before);
        assert_eq!(
            node.state_machine
                .participant_capacity_grant(
                    id.target.shard,
                    id.target.epoch,
                    id.target.mode,
                    &id.authority,
                    &id.cohort,
                )
                .await,
            Err(D1Error::VisibilityNotEnabled)
        );
        let mut journal = Journal::new(id.authority.mode).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: id.cohort.clone(),
            })
            .unwrap();
        // Apply an activation, then deliberately discard its ACK. The public
        // helper must recover the stored first generation without new entropy.
        node.raft
            .client_write(LogCommand::D1V1(D1Command::EnableByteBoundReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: journal.clone(),
                generation_nonce: [17; 32],
            }))
            .await
            .unwrap()
            .data
            .d1
            .unwrap()
            .unwrap();
        let first =
            activate_member_with_entropy(&node, &id.authority, &id.target, &id.cohort, |_| {
                panic!("retry generated a replacement incarnation")
            })
            .await
            .unwrap();
        assert_eq!(first.activation.nonce, [17; 32]);
        let rejected = node
            .raft
            .client_write(LogCommand::D1V1(D1Command::EnableByteBoundReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal,
                generation_nonce: [18; 32],
            }))
            .await
            .unwrap()
            .data
            .d1
            .unwrap();
        assert!(rejected.is_err());
        assert_eq!(
            activate_byte_bound_member(&node, &id.authority, &id.target, &id.cohort).await,
            Ok(first)
        );
        node.raft.shutdown().await.unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn actual_coordinator_owns_all_grants_and_retains_confirmation_offline() {
        let cluster = crate::Cluster::new();
        let id = byte_identity();
        let coordinator = capacity_node(&cluster, &id).await;
        let writer_id = byte_member_identity(7);
        let observer_id = byte_member_identity(8);
        let writer = capacity_node(&cluster, &writer_id).await;
        let observer = capacity_node(&cluster, &observer_id).await;
        let writer_grant = activate_byte_bound_member(
            &writer,
            &writer_id.authority,
            &writer_id.target,
            &writer_id.cohort,
        )
        .await
        .unwrap();
        let observer_grant = activate_byte_bound_member(
            &observer,
            &observer_id.authority,
            &observer_id.target,
            &observer_id.cohort,
        )
        .await
        .unwrap();
        let (writer_client, writer_server) =
            capacity_client(writer.clone(), writer_id.clone()).await;
        let (observer_client, observer_server) =
            capacity_client(observer.clone(), observer_id.clone()).await;
        assert!(matches!(
            Server::bind(
                coordinator.clone(),
                id.clone(),
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(2)
            )
            .await,
            Err(Error::Identity)
        ));
        assert!(matches!(
            Server::bind_byte_bound_coordinator(
                coordinator.clone(),
                id.clone(),
                vec![writer_client.clone()],
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(2)
            )
            .await,
            Err(Error::Identity)
        ));
        let server = Server::bind_byte_bound_coordinator(
            coordinator.clone(),
            id.clone(),
            vec![observer_client.clone(), writer_client.clone()],
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let client = Client::new(server.local_addr(), id.clone(), Duration::from_secs(2)).unwrap();
        assert_eq!(
            NodeRef::Local(&coordinator)
                .confirm_capacity(&id.authority, 1)
                .await,
            Err(Error::Identity)
        );
        let command = Command::ConfirmCohortCapacity {
            cohort: id.cohort.clone(),
            grants: byte_grants(),
        };
        assert_eq!(
            NodeRef::Remote(&client)
                .command(&id.authority, 1, command.clone())
                .await,
            Err(Error::Identity)
        );
        // Bypass client validation to prove the actual TCP server rejects forged
        // grants on the ordinary Command route as well.
        let mut stream = TcpStream::connect(server.local_addr()).await.unwrap();
        send(
            &mut stream,
            &encode_sync(&Request {
                version: PROTOCOL_VERSION,
                identity: id.clone(),
                nonce: 19,
                operation: Operation::Command {
                    authority: id.authority.clone(),
                    epoch: 1,
                    command,
                },
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let reply: Reply = receive(&mut stream).await.unwrap();
        assert_eq!(reply.nonce, 19);
        assert!(matches!(reply.result, Err(Error::Identity)));
        let offline_address = observer_server.local_addr();
        observer_server.shutdown().await;
        assert!(matches!(
            NodeRef::Remote(&client)
                .confirm_capacity(&id.authority, 1)
                .await,
            Err(Error::Unavailable)
        ));
        let unready = NodeRef::Remote(&client)
            .journal(&id.authority, 1)
            .await
            .unwrap();
        assert!(!unready.capacity_ready());
        assert_eq!(unready.history_cursor().unwrap().event_count, 1);
        let begin = Command::Begin {
            id: [1; 16],
            participants: vec![Participant {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
                intent: [1; 32],
            }],
        };
        assert!(NodeRef::Remote(&client)
            .command(&id.authority, 1, begin.clone())
            .await
            .is_err());
        assert_eq!(
            NodeRef::Remote(&client)
                .journal(&id.authority, 1)
                .await
                .unwrap()
                .transaction_count(),
            0
        );
        let observer_server = Server::bind(
            observer.clone(),
            observer_id.clone(),
            offline_address,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let expected = vec![writer_grant, observer_grant];
        assert_eq!(
            NodeRef::Remote(&client)
                .confirm_capacity(&id.authority, 1)
                .await
                .unwrap(),
            expected
        );
        let confirmed = NodeRef::Remote(&client)
            .journal(&id.authority, 1)
            .await
            .unwrap();
        assert_eq!(confirmed.history_cursor().unwrap().event_count, 2);
        assert_eq!(confirmed.transaction_count(), 0);
        let descriptor = Participant {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
            intent: [2; 32],
        };
        let request = D1WriteRequest {
            shard: 7,
            epoch: 1,
            request_id: [2; 16],
            expected_sequence: Some(0),
            operations: vec![crate::WriteOp::put("capacity-key", "value")],
        };
        let plan = crate::request_plans::Plan {
            version: 1,
            id: [2; 16],
            authority: id.authority.clone(),
            cohort: id.cohort.clone(),
            validation_version: 0,
            parts: vec![crate::request_plans::Part {
                descriptor: descriptor.clone(),
                request: request.clone(),
            }],
        };
        NodeRef::Remote(&client)
            .command(
                &id.authority,
                1,
                Command::AdmitRequest {
                    id: [3; 16],
                    request: vec![1],
                    payload: plan.encode().unwrap(),
                    transaction_id: [2; 16],
                    participants: vec![descriptor.clone()],
                },
            )
            .await
            .unwrap();
        let admission = NodeRef::Remote(&client)
            .journal(&id.authority, 1)
            .await
            .unwrap();
        // Privileged raw preparation still cannot reserve an identity until its
        // confirmed grant and exact original Plan have been retained locally.
        assert!(writer
            .raft
            .client_write(LogCommand::D1V1(D1Command::PrepareParticipant {
                authority: id.authority.clone(),
                participant: descriptor.clone(),
                request: request.clone(),
            }))
            .await
            .unwrap()
            .data
            .d1
            .unwrap()
            .is_err());
        assert_eq!(
            NodeRef::Remote(&writer_client)
                .prepare(&id.authority, &descriptor, request.clone(), &admission)
                .await
                .unwrap(),
            Status::Prepared
        );
        NodeRef::Remote(&client)
            .command(
                &id.authority,
                1,
                Command::Prepared {
                    id: [2; 16],
                    participant: descriptor.clone(),
                },
            )
            .await
            .unwrap();
        let later = NodeRef::Remote(&client)
            .journal(&id.authority, 1)
            .await
            .unwrap();
        let base = NodeRef::Remote(&writer_client)
            .history_status(&id.authority, &writer_id.target, &id.cohort)
            .await
            .unwrap()
            .cursor;
        let page = later.history_page(&base, None).unwrap();
        let retained = NodeRef::Remote(&writer_client)
            .sync_history(&id.authority, &writer_id.target, &page)
            .await
            .unwrap();
        assert_eq!(
            NodeRef::Remote(&writer_client)
                .prepare(&id.authority, &descriptor, request.clone(), &admission)
                .await
                .unwrap(),
            Status::Prepared
        );
        assert_eq!(
            NodeRef::Remote(&writer_client)
                .history_status(&id.authority, &writer_id.target, &id.cohort)
                .await
                .unwrap(),
            retained
        );
        let mut changed = request;
        changed.operations = vec![crate::WriteOp::put("capacity-key", "different")];
        assert_eq!(
            NodeRef::Remote(&writer_client)
                .prepare(&id.authority, &descriptor, changed, &admission)
                .await,
            Err(Error::Identity)
        );
        // The retained canonical grant survives missing members and exact retry;
        // confirmation never re-queries offline members or creates a new event.
        observer_server.shutdown().await;
        writer_server.shutdown().await;
        assert_eq!(
            NodeRef::Remote(&client)
                .confirm_capacity(&id.authority, 1)
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            NodeRef::Remote(&client)
                .journal(&id.authority, 1)
                .await
                .unwrap(),
            later
        );
        NodeRef::Remote(&client)
            .command(&id.authority, 1, begin)
            .await
            .unwrap();
        assert_eq!(
            NodeRef::Remote(&client)
                .journal(&id.authority, 1)
                .await
                .unwrap()
                .transaction_count(),
            2
        );
        server.shutdown().await;
        observer.raft.shutdown().await.unwrap();
        writer.raft.shutdown().await.unwrap();
        coordinator.raft.shutdown().await.unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actual_grant_status_checks_pinned_group_node_and_profile() {
        let cluster = crate::Cluster::new();
        let id = byte_member_identity(7);
        let node = capacity_node(&cluster, &id).await;
        let grant = activate_byte_bound_member(&node, &id.authority, &id.target, &id.cohort)
            .await
            .unwrap();
        let (client, server) = capacity_client(node.clone(), id.clone()).await;
        assert_eq!(
            NodeRef::Remote(&client)
                .grant_status(&id.authority, &id.target, &id.cohort)
                .await,
            Ok(grant)
        );
        for fault in 0..3 {
            let mut wrong = id.clone();
            match fault {
                0 => wrong.group += 1,
                1 => wrong.node += 1,
                _ => wrong.cohort.profile = CapacityProfile::LegacyCountV1,
            }
            let client =
                Client::new(server.local_addr(), wrong.clone(), Duration::from_secs(2)).unwrap();
            assert_eq!(
                NodeRef::Remote(&client)
                    .grant_status(&wrong.authority, &wrong.target, &wrong.cohort)
                    .await,
                Err(Error::Identity)
            );
        }
        server.shutdown().await;
        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_async_deadline_drops_pending_work_and_releases_permit_after_caller_cancel() {
        struct Dropped(Option<oneshot::Sender<()>>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        for cancel_caller in [false, true] {
            let slots = Arc::new(Semaphore::new(1));
            let (entered, ready) = oneshot::channel();
            let (dropped, canceled) = oneshot::channel();
            let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
            let work_slots = slots.clone();
            let work = tokio::spawn(async move {
                bounded_cpu_with_slots(work_slots, deadline, async move {
                    let _drop = Dropped(Some(dropped));
                    let _ = entered.send(());
                    std::future::pending::<Result<(), Error>>().await
                })
                .await
            });
            ready.await.unwrap();
            assert!(slots.clone().try_acquire_owned().is_err());
            if cancel_caller {
                work.abort();
                assert!(work.await.unwrap_err().is_cancelled());
            } else {
                assert_eq!(work.await.unwrap(), Err(Error::Unavailable));
            }
            // Releasing the caller is insufficient. The timer inside the
            // blocking closure must actually drop its still-pending future.
            tokio::time::timeout_at(deadline + Duration::from_secs(1), canceled)
                .await
                .unwrap()
                .unwrap();
            let permit = tokio::time::timeout(Duration::from_secs(1), slots.acquire())
                .await
                .unwrap()
                .unwrap();
            drop(permit);
            assert_eq!(slots.available_permits(), 1);
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn actual_quorum_loss_bounds_activation_submit_and_releases_cpu_permit() {
        let cluster = crate::Cluster::new();
        let id = byte_member_identity(7);
        let group = cluster.in_group(id.group);
        let leader = capacity_node(&cluster, &id).await;
        let second = group.start(2).await.unwrap();
        let third = group.start(3).await.unwrap();
        for follower in [2, 3] {
            leader
                .raft
                .add_learner(follower, openraft::BasicNode::default(), true)
                .await
                .unwrap();
        }
        leader
            .raft
            .change_membership(BTreeSet::from([1, 2, 3]), false)
            .await
            .unwrap();
        leader.raft.ensure_linearizable().await.unwrap();
        let mut journal = Journal::new(id.authority.mode).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: id.cohort.clone(),
            })
            .unwrap();
        for follower in [2, 3] {
            group.policy.deny(1, follower).await;
            group.policy.deny(follower, 1).await;
        }
        let node = WorkNode {
            raft: leader.raft.clone(),
            state_machine: leader.state_machine.clone(),
        };
        let slots = Arc::new(Semaphore::new(1));
        let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        let (entered, ready) = oneshot::channel();
        let work_slots = slots.clone();
        let work = tokio::spawn(async move {
            bounded_cpu_with_slots(work_slots, deadline, async move {
                let _ = entered.send(());
                // Quorum disappeared after the successful leader barrier. An
                // actual Raft proposal now remains pending, as can happen in
                // either activation or the verified confirmation commit phase.
                submit(
                    &node,
                    deadline,
                    D1Command::EnableByteBoundReads {
                        shard: 7,
                        epoch: 1,
                        mode_id: [7; 16],
                        journal,
                        generation_nonce: [29; 32],
                    },
                )
                .await
            })
            .await
        });
        ready.await.unwrap();
        assert_eq!(work.await.unwrap(), Err(Error::Unavailable));
        let permit = tokio::time::timeout(Duration::from_secs(1), slots.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        assert_eq!(slots.available_permits(), 1);
        // No missing quorum may be replaced by a fresh nonce or an optimistic
        // grant. The public activation path must finish within its own deadline.
        assert_eq!(
            tokio::time::timeout(
                LOCAL_TIMEOUT + Duration::from_secs(1),
                activate_member_with_entropy(
                    &leader,
                    &id.authority,
                    &id.target,
                    &id.cohort,
                    |_| panic!("lost quorum reached activation entropy")
                ),
            )
            .await
            .unwrap(),
            Err(Error::Unavailable)
        );
        assert_eq!(
            leader
                .state_machine
                .participant_capacity_grant(7, 1, [7; 16], &id.authority, &id.cohort,)
                .await,
            Err(D1Error::VisibilityNotEnabled)
        );
        // Do not heal and retry an uncertain proposal. Shutdown removes the
        // pending test group without asserting that timeout erased its log.
        leader.raft.shutdown().await.unwrap();
        second.raft.shutdown().await.unwrap();
        third.raft.shutdown().await.unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopped_leader_confirmation_fails_closed_with_bounded_deadline() {
        let cluster = crate::Cluster::new();
        let id = byte_identity();
        let coordinator = capacity_node(&cluster, &id).await;
        let members = [7, 8]
            .into_iter()
            .map(|shard| {
                Client::new(
                    format!("127.0.0.1:{}", 10000 + shard).parse().unwrap(),
                    byte_member_identity(shard),
                    Duration::from_secs(1),
                )
                .unwrap()
            })
            .collect();
        let server = Server::bind_byte_bound_coordinator(
            coordinator.clone(),
            id.clone(),
            members,
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_millis(250),
        )
        .await
        .unwrap();
        let client = Client::new(server.local_addr(), id.clone(), Duration::from_secs(1)).unwrap();
        coordinator.raft.shutdown().await.unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(2),
                NodeRef::Remote(&client).confirm_capacity(&id.authority, 1),
            )
            .await
            .unwrap(),
            Err(Error::Unavailable)
        );
        let journal = coordinator
            .state_machine
            .coordinator_journal(9, 1, [9; 16])
            .await
            .unwrap();
        assert!(!journal.capacity_ready());
        assert_eq!(journal.history_cursor().unwrap().event_count, 1);
        server.shutdown().await;
    }

    #[test]
    fn combined_snapshot_and_terminal_proof_obey_exact_whole_frame_boundary() {
        check_combined_frame_boundary(
            terminal_journal(1).terminal_prefix(1).unwrap(),
            participant_identity(),
        );
    }
    #[test]
    fn byte_profile_combined_snapshot_and_extended_proof_obey_exact_frame_boundary() {
        let id = byte_member_identity(7);
        let mut journal = Journal::new(id.authority.mode).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: id.cohort.clone(),
            })
            .unwrap();
        journal
            .apply(Command::ConfirmCohortCapacity {
                cohort: id.cohort.clone(),
                grants: byte_grants(),
            })
            .unwrap();
        for counter in 1..=1025u64 {
            let mut transaction = [0; 16];
            transaction[..8].copy_from_slice(&counter.to_be_bytes());
            journal
                .apply(Command::Begin {
                    id: transaction,
                    participants: vec![Participant {
                        shard: 7,
                        epoch: 1,
                        mode: [7; 16],
                        intent: [1; 32],
                    }],
                })
                .unwrap();
            journal
                .apply(Command::Decide {
                    id: transaction,
                    decision: crate::transaction_decisions::Decision::Abort,
                })
                .unwrap();
        }
        let proof = journal.terminal_prefix(1025).unwrap();
        let encoded = serde_json::to_value(&proof).unwrap();
        assert_eq!(encoded["version"], 2);
        check_summary(
            &CoordinatorSummary {
                authority: id.authority.clone(),
                cohort: id.cohort.clone(),
                resolved_prefix: 1025,
            },
            &id.authority,
            1,
        )
        .unwrap();
        check_history_status(
            &HistoryStatus {
                cursor: journal.history_cursor().unwrap(),
                applied_prefix: 1025,
            },
            &id.authority,
            &id.target,
            &id.cohort,
        )
        .unwrap();
        check_combined_frame_boundary(proof, id);
    }
    fn check_combined_frame_boundary(proof: TerminalPrefix, id: Identity) {
        id.validate().unwrap();
        let mut reply = Reply {
            version: PROTOCOL_VERSION,
            operation: 7,
            identity: id.clone(),
            nonce: u64::MAX,
            result: Ok(Value::Snapshot(
                Snapshot {
                    shard: 7,
                    epoch: 1,
                    authority: id.authority.clone(),
                    version: proof.version(),
                    baseline_sequence: 0,
                    cohort: Some(id.cohort.clone()),
                    applied_prefix: proof.version(),
                    local_sequence: 0,
                    replay_operations: 0,
                    entries: vec![(vec![1], vec![0])],
                },
                proof.clone(),
            )),
        };
        let base = encode_sync(&reply).unwrap().len();
        let growth = MAX_FRAME_BYTES - 1 - base;
        if let Ok(Value::Snapshot(snapshot, _)) = &mut reply.result {
            let value = &mut snapshot.entries[0].1;
            // Each appended 255 adds four JSON bytes. Keep the initial zero
            // for the later one-byte transitions 0 -> 10 -> 100.
            let appended = growth.div_ceil(4);
            value.extend(std::iter::repeat_n(255, appended));
            let reduction = 4 * appended - growth;
            if reduction >= 2 {
                value[appended] = 0;
            }
            if reduction % 2 == 1 {
                value[appended - 1] = 10;
            }
            assert!(value.len() + 1 < crate::versioned_reads::MAX_READ_BYTES);
        }

        // Actual Reply + Value::Snapshot, including both the rows and the
        // checked evidence. A proof-only frame measurement is insufficient.
        assert_eq!(encode_sync(&reply).unwrap().len(), MAX_FRAME_BYTES - 1);
        if let Ok(Value::Snapshot(snapshot, _)) = &mut reply.result {
            snapshot.entries[0].1[0] = 10;
        }
        assert_eq!(encode_sync(&reply).unwrap().len(), MAX_FRAME_BYTES);
        if let Ok(Value::Snapshot(snapshot, retained)) = &mut reply.result {
            snapshot.entries[0].1[0] = 100;
            // Both components individually fit, but their complete envelope
            // is one byte over the frame. Row capture's 64 MiB raw-data bound
            // does not promise that JSON rows plus proof fit a 32 MiB frame.
            assert!(encode_sync(snapshot).unwrap().len() < MAX_FRAME_BYTES);
            assert!(encode_sync(retained).unwrap().len() < MAX_FRAME_BYTES);
            assert_eq!(*retained, proof);
        }
        // The encoder owns its temporary writer: failure returns only the
        // typed error, never a partially encoded successful response.
        assert_eq!(encode_sync(&reply), Err(Error::Capacity));
        reply.result = Err(Error::Capacity);
        let fallback = encode_sync(&reply).unwrap();
        assert!(fallback.len() < 4096);
        let decoded: Reply = serde_json::from_slice(&fallback).unwrap();
        assert!(matches!(decoded.result, Err(Error::Capacity)));
    }
}
