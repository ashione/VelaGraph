//! Bounded, trusted internal managed-transaction transport. This is neither an
//! authenticated service nor a public graph query API. Journal evidence must
//! come from a leader observation by a trusted caller. Timeouts are uncertain
//! outcomes and never authorize abort, identity replacement, or fallback reads.
use crate::cohort_reads::VisibilityInfo;
use crate::participant_writes::{Authority, Status};
use crate::request_index::RequestLookup;
use crate::terminal_prefix::TerminalPrefix;
use crate::transaction_decisions::{
    Cohort, Command, HistoryCursor, HistoryPage, Journal, Participant, MAX_TRANSACTIONS,
};
use crate::versioned_reads::{ReadLimit, ReadSnapshot, Target};
use crate::{D1Command, D1Error, D1Result, D1WriteRequest, LogCommand, NodeHandle};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
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
const PROTOCOL_VERSION: u32 = 4;
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
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unavailable);
        }
        let result = tokio::time::timeout_at(deadline, async {
            match self {
                Self::Local(node) => dispatch(node, None, op, deadline).await,
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
        if ordinal == 0 || ordinal > MAX_TRANSACTIONS as u64 {
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
                if applied >= ordinal && applied <= MAX_TRANSACTIONS as u64 =>
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
    if status.applied_prefix > MAX_TRANSACTIONS as u64
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
        || value.resolved_prefix > crate::transaction_decisions::MAX_TRANSACTIONS as u64
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
// Disk format errors do not expand the v4 wire enum. Peers already understand
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
    op: Operation,
    deadline: tokio::time::Instant,
) -> Result<Value, Error> {
    let node = WorkNode {
        raft: node.raft.clone(),
        state_machine: node.state_machine.clone(),
    };
    let runtime = tokio::runtime::Handle::current();
    cpu(move || {
        runtime.block_on(async {
            let result =
                tokio::time::timeout_at(deadline, execute(&node, pinned.as_ref(), op, deadline))
                    .await
                    .map_err(|_| Error::Unavailable)?;
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Unavailable);
            }
            result
        })
    })
    .await
}
async fn execute(
    node: &WorkNode,
    pinned: Option<&Identity>,
    op: Operation,
    deadline: tokio::time::Instant,
) -> Result<Value, Error> {
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
    match op {
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
                    if applied_prefix >= ordinal && applied_prefix <= MAX_TRANSACTIONS as u64 =>
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
            checked_journal(node, pinned, &authority, epoch).await?;
            if matches!(command, Command::BindCohort { .. }) {
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
            dispatch(&node, Some(identity.clone()), probe, deadline),
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
                        let node = node.clone(); let identity = identity.clone();
                        handlers.spawn(async move {
                            let _permit = permit;
                            let deadline = tokio::time::Instant::now() + timeout;
                            let _ = tokio::time::timeout_at(deadline, async {
                                let request: Request = receive(&mut stream).await?;
                                let operation = request.operation.tag();
                                let result = if request.version != PROTOCOL_VERSION { Err(Error::Protocol) } else if request.identity != identity { Err(Error::Identity) }
                                    else { dispatch(&node,Some(identity.clone()),request.operation,deadline).await };
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
                    _ => reply.version = 3,
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
    fn history_wire_v4_operation_tags_are_explicit() {
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
        assert_eq!(PROTOCOL_VERSION, 4);
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
            assert_eq!(wire["version"], 4);
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
                version: 3,
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
}
