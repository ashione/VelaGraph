//! Fixed, bounded snapshot downloads. These are not on-demand remote ReadViews.
use super::*;
pub mod grpc;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};
use tokio::sync::OwnedSemaphorePermit;

pub const MAX_SESSION_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SESSION_ROWS: usize = 1_000_000;
pub const MAX_PAGE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_PAGE_ROWS: usize = 4096;
pub const MAX_SESSIONS: usize = 4;
pub const MAX_RETAINED_BYTES: usize = 128 * 1024 * 1024;
/// Fixed lifetime from admission; paging never renews it.
pub const SESSION_TTL: Duration = Duration::from_secs(60);
static DOWNLOAD_ADMISSION: Semaphore = Semaphore::const_new(MAX_IN_FLIGHT);
static DOWNLOAD_BYTES: Semaphore = Semaphore::const_new(MAX_RETAINED_BYTES);
static DOWNLOAD_ROWS: Semaphore = Semaphore::const_new(MAX_SESSION_ROWS * MAX_SESSIONS);

/// Exact frozen snapshot identity. Tokens are bearer capabilities on a trusted
/// plaintext network, not authenticated identities or cross-shard snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDescriptor {
    pub version: u32,
    pub identity: RemoteIdentity,
    pub incarnation: [u8; 16],
    pub session: [u8; 16],
    pub shard: u64,
    pub epoch: u64,
    pub sequence: u64,
    pub rows: usize,
    pub bytes: usize,
}
/// Server-issued non-consuming continuation, bound to one session and position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotCursor {
    incarnation: [u8; 16],
    session: [u8; 16],
    position: usize,
    token: [u8; 16],
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPage {
    pub descriptor: SnapshotDescriptor,
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub next: Option<SnapshotCursor>,
}
/// Payload/object admission counters, not a process RSS measurement.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotStats {
    pub sessions: usize,
    pub retained_bytes: usize,
    pub pages: u64,
    pub expired: u64,
}
struct Session {
    descriptor: SnapshotDescriptor,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    // Bounded by snapshot rows (plus the empty-snapshot boundary). Each
    // nonterminal page either meets the row cap or cannot fit its next row.
    starts: Vec<usize>,
    tokens: Vec<[u8; 16]>,
    expires: Instant,
    _slot: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
pub(super) struct Sessions {
    incarnation: [u8; 16],
    values: Mutex<BTreeMap<[u8; 16], Arc<Session>>>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    pages: AtomicU64,
    expired: AtomicU64,
}
fn token() -> Result<[u8; 16], RemoteError> {
    let mut value = [0; 16];
    getrandom::fill(&mut value).map_err(|_| RemoteError::Unavailable)?;
    Ok(value)
}
fn check_time(end: Instant) -> Result<(), RemoteError> {
    if Instant::now() >= end {
        Err(RemoteError::Timeout { request_id: None })
    } else {
        Ok(())
    }
}
impl Sessions {
    pub(super) fn new() -> Result<Self, RemoteError> {
        Ok(Self {
            incarnation: token()?,
            values: Mutex::new(BTreeMap::new()),
            slots: Arc::new(Semaphore::new(MAX_SESSIONS)),
            bytes: Arc::new(Semaphore::new(MAX_RETAINED_BYTES)),
            pages: AtomicU64::new(0),
            expired: AtomicU64::new(0),
        })
    }
    pub(super) fn expire(&self) {
        let now = Instant::now();
        self.values.lock().unwrap().retain(|_, session| {
            let keep = session.expires > now;
            if !keep {
                self.expired.fetch_add(1, Ordering::Relaxed);
            }
            keep
        });
    }
    pub(super) fn stats(&self) -> SnapshotStats {
        self.expire();
        SnapshotStats {
            sessions: MAX_SESSIONS - self.slots.available_permits(),
            retained_bytes: MAX_RETAINED_BYTES - self.bytes.available_permits(),
            pages: self.pages.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
        }
    }
    pub(super) async fn open(
        &self,
        state: &StateMachine,
        identity: RemoteIdentity,
        shard: u64,
        end: Instant,
    ) -> Result<SnapshotDescriptor, RemoteError> {
        self.expire();
        check_time(end)?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        // Reserve the worst permitted payload BEFORE the atomic state clone.
        // The permit remains held across cancellation and until all Arc holders drop.
        let mut reserved = self
            .bytes
            .clone()
            .try_acquire_many_owned(MAX_SESSION_BYTES as u32)
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let expires = Instant::now() + SESSION_TTL;
        let view = state
            .d1_read_snapshot(MAX_SESSION_ROWS, MAX_SESSION_BYTES)
            .await
            .map_err(|_| RemoteError::ResourceExhausted)?
            .ok_or(RemoteError::Application(D1Error::NotActivated))?;
        check_time(end)?;
        if view.shard != shard || view.epoch == 0 {
            return Err(RemoteError::InvalidArgument);
        }
        let mut entries = Vec::with_capacity(view.entries.len());
        let mut starts = vec![0];
        let mut page_bytes = 0usize;
        let mut page_rows = 0usize;
        let mut bytes = 0usize;
        for (key, value) in view.entries {
            check_time(end)?;
            let size = key.len().saturating_add(value.len());
            if size > MAX_PAGE_BYTES {
                return Err(RemoteError::ResourceExhausted);
            }
            if page_rows == MAX_PAGE_ROWS || page_bytes + size > MAX_PAGE_BYTES {
                starts.push(entries.len());
                page_bytes = 0;
                page_rows = 0;
            }
            bytes += size;
            page_bytes += size;
            page_rows += 1;
            entries.push((key, value));
        }
        let descriptor = SnapshotDescriptor {
            version: VERSION,
            identity,
            incarnation: self.incarnation,
            session: token()?,
            shard,
            epoch: view.epoch,
            sequence: view.sequence,
            rows: entries.len(),
            bytes,
        };
        let tokens = (0..starts.len())
            .map(|_| token())
            .collect::<Result<Vec<_>, _>>()?;
        let bytes_guard = reserved
            .split(bytes)
            .ok_or(RemoteError::ResourceExhausted)?;
        drop(reserved);
        let session = Arc::new(Session {
            descriptor: descriptor.clone(),
            entries,
            starts,
            tokens,
            expires,
            _slot: slot,
            _bytes: bytes_guard,
        });
        check_time(end.min(expires))?;
        let mut values = self.values.lock().unwrap();
        // Never replace an extant random ID even in the extraordinarily unlikely collision case.
        if values.contains_key(&descriptor.session) {
            return Err(RemoteError::Unavailable);
        }
        values.insert(descriptor.session, session);
        Ok(descriptor)
    }
    fn validate(
        &self,
        identity: RemoteIdentity,
        shard: u64,
        descriptor: &SnapshotDescriptor,
    ) -> Result<(), RemoteError> {
        if descriptor.identity != identity
            || descriptor.shard != shard
            || descriptor.version != VERSION
        {
            return Err(RemoteError::InvalidArgument);
        }
        if descriptor.incarnation != self.incarnation {
            return Err(RemoteError::SessionUnavailable);
        }
        Ok(())
    }
    fn lookup(
        &self,
        identity: RemoteIdentity,
        shard: u64,
        descriptor: &SnapshotDescriptor,
    ) -> Result<Arc<Session>, RemoteError> {
        self.validate(identity, shard, descriptor)?;
        self.expire();
        let session = self
            .values
            .lock()
            .unwrap()
            .get(&descriptor.session)
            .cloned()
            .ok_or(RemoteError::SessionUnavailable)?;
        if session.descriptor != *descriptor {
            return Err(RemoteError::InvalidArgument);
        }
        Ok(session)
    }
    pub(super) fn close(
        &self,
        identity: RemoteIdentity,
        shard: u64,
        descriptor: &SnapshotDescriptor,
    ) -> Result<(), RemoteError> {
        let session = self.lookup(identity, shard, descriptor)?;
        self.values
            .lock()
            .unwrap()
            .remove(&session.descriptor.session);
        Ok(())
    }
    pub(super) async fn page(
        &self,
        state: &StateMachine,
        identity: RemoteIdentity,
        shard: u64,
        descriptor: SnapshotDescriptor,
        cursor: Option<SnapshotCursor>,
        end: Instant,
    ) -> Result<SnapshotPage, RemoteError> {
        let session = self.lookup(identity, shard, &descriptor)?;
        let index = match cursor {
            None => 0,
            Some(cursor) => {
                if cursor.incarnation != self.incarnation || cursor.session != descriptor.session {
                    return Err(RemoteError::InvalidArgument);
                }
                let index = session
                    .starts
                    .binary_search(&cursor.position)
                    .map_err(|_| RemoteError::InvalidArgument)?;
                if index == 0 || cursor.token != session.tokens[index] {
                    return Err(RemoteError::InvalidArgument);
                }
                index
            }
        };
        let (actual, current, _) = state
            .d1_metadata()
            .await
            .ok_or(RemoteError::Application(D1Error::NotActivated))?;
        if actual != shard {
            return Err(RemoteError::InvalidArgument);
        }
        if current != descriptor.epoch {
            return Err(RemoteError::Application(D1Error::Fenced {
                requested: descriptor.epoch,
                current,
            }));
        }
        check_time(end)?;
        if Instant::now() >= session.expires {
            return Err(RemoteError::SessionUnavailable);
        }
        let start = session.starts[index];
        let stop = session
            .starts
            .get(index + 1)
            .copied()
            .unwrap_or(session.entries.len());
        let mut entries = Vec::with_capacity(stop - start);
        for entry in &session.entries[start..stop] {
            check_time(end)?;
            entries.push(entry.clone());
        }
        let next = session
            .starts
            .get(index + 1)
            .map(|position| SnapshotCursor {
                incarnation: self.incarnation,
                session: descriptor.session,
                position: *position,
                token: session.tokens[index + 1],
            });
        check_time(end)?;
        if Instant::now() >= session.expires {
            return Err(RemoteError::SessionUnavailable);
        }
        self.pages.fetch_add(1, Ordering::Relaxed);
        Ok(SnapshotPage {
            descriptor,
            entries,
            next,
        })
    }
}

/// Capture a fixed whole-shard session after a leader barrier. Caller must
/// Close it; lost responses/disconnected callers are reclaimed by fixed TTL.
pub async fn open_snapshot(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    shard: u64,
    deadline: Duration,
) -> Result<SnapshotDescriptor, RemoteError> {
    match rpc(
        endpoint,
        identity,
        Operation::OpenSnapshot { shard },
        deadline,
        None,
    )
    .await?
    {
        Reply::OpenSnapshot(descriptor) => {
            validate_descriptor(&descriptor, identity, shard)?;
            Ok(descriptor)
        }
        _ => Err(RemoteError::Protocol),
    }
}
fn validate_descriptor(
    descriptor: &SnapshotDescriptor,
    identity: Option<RemoteIdentity>,
    shard: u64,
) -> Result<(), RemoteError> {
    if descriptor.version != VERSION
        || descriptor.shard != shard
        || descriptor.epoch == 0
        || identity.is_some_and(|id| id != descriptor.identity)
    {
        return Err(RemoteError::Protocol);
    }
    if descriptor.rows > MAX_SESSION_ROWS || descriptor.bytes > MAX_SESSION_BYTES {
        return Err(RemoteError::ResourceExhausted);
    }
    Ok(())
}
/// Fetch one bounded non-consuming page; repeat a cursor to repeat its page.
pub async fn snapshot_page(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    descriptor: SnapshotDescriptor,
    cursor: Option<SnapshotCursor>,
    deadline: Duration,
) -> Result<SnapshotPage, RemoteError> {
    match rpc(
        endpoint,
        identity,
        Operation::SnapshotPage {
            descriptor: descriptor.clone(),
            cursor: cursor.clone(),
        },
        deadline,
        None,
    )
    .await?
    {
        Reply::SnapshotPage(page) => {
            validate_page(&descriptor, cursor.as_ref(), &page)?;
            Ok(page)
        }
        _ => Err(RemoteError::Protocol),
    }
}
fn validate_page(
    descriptor: &SnapshotDescriptor,
    cursor: Option<&SnapshotCursor>,
    page: &SnapshotPage,
) -> Result<(), RemoteError> {
    let position = cursor.map_or(0, |c| c.position);
    let bytes = page.entries.iter().fold(0usize, |n, (k, v)| {
        n.saturating_add(k.len()).saturating_add(v.len())
    });
    if page.descriptor != *descriptor
        || page.entries.len() > MAX_PAGE_ROWS
        || bytes > MAX_PAGE_BYTES
        || page.entries.windows(2).any(|p| p[0].0 >= p[1].0)
    {
        return Err(RemoteError::Protocol);
    }
    let next_position = position
        .checked_add(page.entries.len())
        .ok_or(RemoteError::Protocol)?;
    if next_position > descriptor.rows || bytes > descriptor.bytes {
        return Err(RemoteError::Protocol);
    }
    match &page.next {
        Some(next)
            if !page.entries.is_empty()
                && next.incarnation == descriptor.incarnation
                && next.session == descriptor.session
                && next.position == next_position
                && next.position < descriptor.rows => {}
        None if next_position == descriptor.rows => (),
        _ => return Err(RemoteError::Protocol),
    }
    Ok(())
}
/// Release a session. Unknown/closed/expired sessions fail explicitly.
pub async fn close_snapshot(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    descriptor: SnapshotDescriptor,
    deadline: Duration,
) -> Result<(), RemoteError> {
    match rpc(
        endpoint,
        identity,
        Operation::CloseSnapshot { descriptor },
        deadline,
        None,
    )
    .await?
    {
        Reply::CloseSnapshot => Ok(()),
        _ => Err(RemoteError::Protocol),
    }
}
/// Download a complete frozen shard through bounded pages, with one deadline.
/// No partial view, automatic retry, leader failover, or lazy remote reads.
pub async fn paged_snapshot(
    endpoint: SocketAddr,
    shard: u64,
    deadline: Duration,
) -> Result<RemoteSnapshot, RemoteError> {
    download(endpoint, None, shard, deadline).await
}
/// Identified variant of [`paged_snapshot`].
pub async fn paged_snapshot_identified(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    shard: u64,
    deadline: Duration,
) -> Result<RemoteSnapshot, RemoteError> {
    download(endpoint, Some(identity), shard, deadline).await
}
async fn download(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    shard: u64,
    deadline: Duration,
) -> Result<RemoteSnapshot, RemoteError> {
    let end = checked_deadline(deadline, None)?;
    let _download = DOWNLOAD_ADMISSION
        .try_acquire()
        .map_err(|_| RemoteError::ResourceExhausted)?;
    download_complete(DownloadTransport::Legacy { endpoint, identity }, shard, end).await
}
/// Typed full snapshot download with the same aggregate limits and validator as
/// the legacy explicit transport. No fallback or protocol detection.
pub async fn grpc_paged_snapshot(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    shard: u64,
    deadline: Duration,
) -> Result<RemoteSnapshot, RemoteError> {
    let end = checked_deadline(deadline, None)?;
    let _download = DOWNLOAD_ADMISSION
        .try_acquire()
        .map_err(|_| RemoteError::ResourceExhausted)?;
    let client = grpc::SnapshotClient::connect(
        endpoint,
        identity,
        end.saturating_duration_since(Instant::now()),
    )
    .await?;
    download_complete(DownloadTransport::Grpc(client), shard, end).await
}
enum DownloadTransport {
    Legacy {
        endpoint: SocketAddr,
        identity: Option<RemoteIdentity>,
    },
    Grpc(grpc::SnapshotClient),
}
impl DownloadTransport {
    async fn open(&self, shard: u64, end: Instant) -> Result<SnapshotDescriptor, RemoteError> {
        check_time(end)?;
        let budget = end.saturating_duration_since(Instant::now());
        match self {
            Self::Legacy { endpoint, identity } => {
                open_snapshot(*endpoint, *identity, shard, budget).await
            }
            Self::Grpc(client) => client.open(shard, budget).await,
        }
    }
    async fn page(
        &self,
        descriptor: SnapshotDescriptor,
        cursor: Option<SnapshotCursor>,
        end: Instant,
    ) -> Result<SnapshotPage, RemoteError> {
        check_time(end)?;
        let budget = end.saturating_duration_since(Instant::now());
        match self {
            Self::Legacy { endpoint, identity } => {
                snapshot_page(*endpoint, *identity, descriptor, cursor, budget).await
            }
            Self::Grpc(client) => client.page(descriptor, cursor, budget).await,
        }
    }
    async fn close(&self, descriptor: SnapshotDescriptor, end: Instant) -> Result<(), RemoteError> {
        check_time(end)?;
        let budget = end.saturating_duration_since(Instant::now());
        match self {
            Self::Legacy { endpoint, identity } => {
                close_snapshot(*endpoint, *identity, descriptor, budget).await
            }
            Self::Grpc(client) => client.close(descriptor, budget).await,
        }
    }
}
async fn download_complete(
    transport: DownloadTransport,
    shard: u64,
    end: Instant,
) -> Result<RemoteSnapshot, RemoteError> {
    let descriptor = transport.open(shard, end).await.inspect_err(|error| {
        diagnostic::failure(format_args!(
            "phase=download_open reason={}",
            diagnostic::kind(error)
        ));
    })?;
    // Keep aggregate memory admission until Close has finished and the complete
    // snapshot is handed to the caller, not merely until assembly completes.
    let memory = DOWNLOAD_BYTES
        .try_acquire_many(descriptor.bytes as u32)
        .and_then(|bytes| {
            DOWNLOAD_ROWS
                .try_acquire_many(descriptor.rows as u32)
                .map(|rows| (bytes, rows))
        });
    let result = async {
        memory
            .as_ref()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        let mut cursor = None;
        let mut ordinal = 0usize;
        loop {
            check_time(end)?;
            let page = transport
                .page(descriptor.clone(), cursor, end)
                .await
                .inspect_err(|error| {
                    diagnostic::failure(format_args!(
                        "phase=download_page ordinal={ordinal} reason={}",
                        diagnostic::kind(error)
                    ));
                })?;
            ordinal += 1;
            for (key, value) in page.entries {
                check_time(end)?;
                if entries.last().is_some_and(|(last, _)| last >= &key) {
                    return Err(RemoteError::Protocol);
                }
                bytes = bytes
                    .checked_add(key.len())
                    .and_then(|n| n.checked_add(value.len()))
                    .ok_or(RemoteError::Protocol)?;
                if entries.len() >= descriptor.rows || bytes > descriptor.bytes {
                    return Err(RemoteError::Protocol);
                }
                entries.push((key, value));
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        if entries.len() != descriptor.rows || bytes != descriptor.bytes {
            return Err(RemoteError::Protocol);
        }
        check_time(end)?;
        Ok(RemoteSnapshot {
            shard,
            epoch: descriptor.epoch,
            sequence: descriptor.sequence,
            entries,
        })
    }
    .await;
    // Best effort within the original budget, never masking a primary error.
    // Cancellation or an expired deadline leaves only a fixed-TTL resource.
    if Instant::now() < end {
        let _ = transport.close(descriptor, end).await;
    }
    result.and_then(|view| {
        check_time(end)?;
        Ok(view)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn descriptor(rows: usize, bytes: usize) -> SnapshotDescriptor {
        SnapshotDescriptor {
            version: VERSION,
            identity: RemoteIdentity { group: 1, node: 2 },
            incarnation: [3; 16],
            session: [4; 16],
            shard: 7,
            epoch: 1,
            sequence: 8,
            rows,
            bytes,
        }
    }
    #[test]
    fn rejects_malformed_pages_and_metadata() {
        let d = descriptor(2, 4);
        let mut page = SnapshotPage {
            descriptor: d.clone(),
            entries: vec![(vec![1], vec![1])],
            next: Some(SnapshotCursor {
                incarnation: d.incarnation,
                session: d.session,
                position: 1,
                token: [5; 16],
            }),
        };
        validate_page(&d, None, &page).unwrap();
        page.descriptor.sequence += 1;
        assert!(validate_page(&d, None, &page).is_err());
        page.descriptor = d.clone();
        page.next.as_mut().unwrap().position = 0;
        assert!(validate_page(&d, None, &page).is_err());
        page.next.as_mut().unwrap().position = 1;
        page.next.as_mut().unwrap().session = [6; 16];
        assert!(validate_page(&d, None, &page).is_err());
        page.next.as_mut().unwrap().session = d.session;
        page.entries.clear();
        assert!(validate_page(&d, None, &page).is_err());
        page.entries = vec![(vec![1], vec![1]), (vec![1], vec![2])];
        page.next = None;
        assert!(validate_page(&d, None, &page).is_err());
        page.entries[1].0 = vec![2];
        validate_page(&d, None, &page).unwrap();
        page.entries[0].1 = vec![0; MAX_PAGE_BYTES];
        assert!(validate_page(&d, None, &page).is_err());
        assert!(validate_descriptor(&descriptor(MAX_SESSION_ROWS + 1, 1), None, 7).is_err());
        assert!(validate_descriptor(&descriptor(1, MAX_SESSION_BYTES + 1), None, 7).is_err());
        let empty = descriptor(0, 0);
        validate_page(
            &empty,
            None,
            &SnapshotPage {
                descriptor: empty.clone(),
                entries: vec![],
                next: None,
            },
        )
        .unwrap();
    }
    #[tokio::test]
    async fn registry_admission_cursors_epoch_close_and_expiry() {
        let state = Arc::new(StateMachine::default());
        // Populate via the public Raft state-machine apply contract.
        use openraft::storage::RaftStateMachine;
        use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
        let mut state_writer = state.clone();
        let commands = [
            D1Command::Activate { shard: 7, epoch: 1 },
            D1Command::Write(D1WriteRequest {
                shard: 7,
                epoch: 1,
                request_id: [1; 16],
                expected_sequence: Some(0),
                operations: (0..3)
                    .map(|i| d2_raft::WriteOp::Put {
                        key: vec![i],
                        value: vec![i; MAX_PAGE_BYTES / 2],
                    })
                    .collect(),
            }),
        ];
        for (index, command) in commands.into_iter().enumerate() {
            state_writer
                .apply([Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), index as u64 + 1),
                    payload: EntryPayload::Normal(LogCommand::D1V1(command)),
                }])
                .await
                .unwrap();
        }
        let sessions = Sessions::new().unwrap();
        let id = RemoteIdentity { group: 1, node: 2 };
        let end = Instant::now() + Duration::from_secs(5);
        let d = sessions.open(&state, id, 7, end).await.unwrap();
        assert_eq!(d.rows, 3);
        let page = sessions
            .page(&state, id, 7, d.clone(), None, end)
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 1);
        let cursor = page.next.unwrap();
        let next = sessions
            .page(&state, id, 7, d.clone(), Some(cursor.clone()), end)
            .await
            .unwrap();
        let replay = sessions
            .page(&state, id, 7, d.clone(), Some(cursor.clone()), end)
            .await
            .unwrap();
        assert_eq!(next.entries, replay.entries);
        let other = sessions.open(&state, id, 7, end).await.unwrap();
        assert!(matches!(
            sessions
                .page(&state, id, 7, other.clone(), Some(cursor), end)
                .await,
            Err(RemoteError::InvalidArgument)
        ));
        let held = sessions.lookup(id, 7, &d).unwrap();
        sessions.close(id, 7, &d).unwrap();
        assert_eq!(sessions.stats().sessions, 2); // Arc-held handler still owns admission.
        assert!(matches!(
            sessions.page(&state, id, 7, d.clone(), None, end).await,
            Err(RemoteError::SessionUnavailable)
        ));
        drop(held);
        assert_eq!(sessions.stats().sessions, 1);
        // Reserve all remaining payload. Admission must reject before copying.
        let reserved = sessions
            .bytes
            .clone()
            .try_acquire_many_owned((MAX_RETAINED_BYTES - other.bytes) as u32)
            .unwrap();
        assert!(matches!(
            sessions.open(&state, id, 7, end).await,
            Err(RemoteError::ResourceExhausted)
        ));
        drop(reserved);
        for _ in 0..3 {
            sessions.open(&state, id, 7, end).await.unwrap();
        }
        assert!(matches!(
            sessions.open(&state, id, 7, end).await,
            Err(RemoteError::ResourceExhausted)
        ));
        for s in sessions.values.lock().unwrap().values_mut() {
            Arc::get_mut(s).unwrap().expires = Instant::now();
        }
        sessions.expire();
        assert_eq!(sessions.stats().sessions, 0);
        assert_eq!(sessions.stats().retained_bytes, 0);
        assert_eq!(sessions.stats().expired, 4);
        assert!(matches!(
            sessions.page(&state, id, 7, other, None, end).await,
            Err(RemoteError::SessionUnavailable)
        ));
        let d = sessions.open(&state, id, 7, end).await.unwrap();
        state_writer
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 3),
                payload: EntryPayload::Normal(LogCommand::D1V1(D1Command::SetEpoch {
                    shard: 7,
                    expected_epoch: 1,
                    new_epoch: 2,
                })),
            }])
            .await
            .unwrap();
        assert!(matches!(
            sessions.page(&state, id, 7, d.clone(), None, end).await,
            Err(RemoteError::Application(D1Error::Fenced {
                requested: 1,
                current: 2
            }))
        ));
        sessions.close(id, 7, &d).unwrap();
        assert!(matches!(
            sessions.open(&state, id, 7, Instant::now()).await,
            Err(RemoteError::Timeout { request_id: None })
        ));
    }
    #[tokio::test]
    async fn exact_page_and_session_payload_caps_release_all_reservations() {
        use openraft::storage::RaftStateMachine;
        use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
        async fn fixture(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Arc<StateMachine> {
            let mut state = Arc::new(StateMachine::default());
            state
                .apply([Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                    payload: EntryPayload::Normal(LogCommand::D1V1(D1Command::Activate {
                        shard: 7,
                        epoch: 1,
                    })),
                }])
                .await
                .unwrap();
            state
                .apply([Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
                    payload: EntryPayload::Normal(LogCommand::D1V1(D1Command::SetDedupCapacity {
                        shard: 7,
                        epoch: 1,
                        expected_bytes: d2_raft::MAX_DEDUP_BYTES,
                        new_bytes: d2_raft::EXTENDED_DEDUP_BYTES,
                    })),
                }])
                .await
                .unwrap();
            for (i, chunk) in entries.chunks(4).enumerate() {
                let operations = chunk
                    .iter()
                    .map(|(key, value)| d2_raft::WriteOp::Put {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect();
                let result = state
                    .apply([Entry {
                        log_id: LogId::new(CommittedLeaderId::new(1, 1), i as u64 + 3),
                        payload: EntryPayload::Normal(LogCommand::D1V1(D1Command::Write(
                            d2_raft::D1WriteRequest {
                                shard: 7,
                                epoch: 1,
                                request_id: (i as u128).to_be_bytes(),
                                expected_sequence: None,
                                operations,
                            },
                        ))),
                    }])
                    .await
                    .unwrap();
                assert!(
                    matches!(result[0].d1, Some(Ok(D1Result::Write { .. }))),
                    "{result:?}"
                );
            }
            state
        }
        let sessions = Sessions::new().unwrap();
        let id = RemoteIdentity { group: 1, node: 2 };
        let end = Instant::now() + Duration::from_secs(20);
        let state = fixture(vec![(vec![0], vec![0; MAX_PAGE_BYTES - 1])]).await;
        let d = sessions.open(&state, id, 7, end).await.unwrap();
        let page = sessions
            .page(&state, id, 7, d.clone(), None, end)
            .await
            .unwrap();
        assert_eq!(
            page.entries[0].0.len() + page.entries[0].1.len(),
            MAX_PAGE_BYTES
        );
        assert!(page.next.is_none());
        sessions.close(id, 7, &d).unwrap();
        let state = fixture(vec![(vec![0], vec![0; MAX_PAGE_BYTES])]).await;
        assert!(matches!(
            sessions.open(&state, id, 7, end).await,
            Err(RemoteError::ResourceExhausted)
        ));
        assert_eq!(sessions.stats().retained_bytes, 0);
        let state = fixture(
            (0..MAX_PAGE_ROWS + 1)
                .map(|n| ((n as u64).to_be_bytes().to_vec(), vec![]))
                .collect(),
        )
        .await;
        let d = sessions.open(&state, id, 7, end).await.unwrap();
        let page = sessions
            .page(&state, id, 7, d.clone(), None, end)
            .await
            .unwrap();
        assert_eq!(page.entries.len(), MAX_PAGE_ROWS);
        assert!(page.next.is_some());
        let mut forged = d.clone();
        forged.incarnation[0] ^= 1;
        assert!(matches!(
            sessions.page(&state, id, 7, forged, None, end).await,
            Err(RemoteError::SessionUnavailable)
        ));
        sessions.close(id, 7, &d).unwrap();
        let state = fixture(
            (0..32u8)
                .map(|n| (vec![n], vec![0; MAX_PAGE_BYTES - 1]))
                .collect(),
        )
        .await;
        let first = sessions.open(&state, id, 7, end).await.unwrap();
        let second = sessions.open(&state, id, 7, end).await.unwrap();
        assert_eq!(first.bytes, MAX_SESSION_BYTES);
        assert_eq!(sessions.stats().retained_bytes, MAX_RETAINED_BYTES);
        assert!(matches!(
            sessions.open(&state, id, 7, end).await,
            Err(RemoteError::ResourceExhausted)
        ));
        sessions.close(id, 7, &first).unwrap();
        sessions.close(id, 7, &second).unwrap();
        let state = fixture(
            (0..32u8)
                .map(|n| (vec![n], vec![0; MAX_PAGE_BYTES - 1]))
                .chain(std::iter::once((vec![32], vec![])))
                .collect(),
        )
        .await;
        assert!(matches!(
            sessions.open(&state, id, 7, end).await,
            Err(RemoteError::ResourceExhausted)
        ));
        assert_eq!(sessions.stats().retained_bytes, 0);
        assert_eq!(sessions.stats().sessions, 0);
    }
}

#[cfg(test)]
mod adversarial_tests;
