use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::blocking::blocking_section;
use crate::d1::{D1State, DedupRecord, MAX_DEDUP_REQUESTS};
use crate::types::{ApplyResult, LogCommand, NodeId, TypeConfig, WriteOp};
use crate::{D1Command, D1Error, D1Result, D1WriteStatus};

const D1_KEY: &[u8] = b"sm/d1";
const DEDUP_PREFIX: &[u8] = b"sm/dedup/";
const DEDUP_PREFIX_END: &[u8] = b"sm/dedup0";

// Disk-only v2. Logical snapshots retain their complete v1 D1 state. Old
// binaries reject this header rather than silently losing retry identities.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct D1MetadataV2 {
    version: u32,
    shard: u64,
    epoch: u64,
    sequence: u64,
}

impl D1MetadataV2 {
    fn of(state: &D1State) -> Self {
        Self {
            version: 2,
            shard: state.shard,
            epoch: state.epoch,
            sequence: state.sequence,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct D1MetadataV3 {
    version: u32,
    shard: u64,
    epoch: u64,
    sequence: u64,
    dedup_bytes_limit: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct D1MetadataV4 {
    version: u32,
    shard: u64,
    epoch: u64,
    sequence: u64,
    dedup_bytes_limit: Option<usize>,
    managed: crate::D1ManagedMode,
}

fn d1_metadata_bytes(state: &D1State) -> Result<Vec<u8>, serde_json::Error> {
    if state.visibility.is_some() {
        #[derive(Serialize)]
        struct Metadata {
            version: u32,
            state: D1State,
        }
        return serde_json::to_vec(&Metadata {
            version: 7,
            state: state.stage(),
        });
    }
    if state.participant.is_some() {
        #[derive(Serialize)]
        struct Metadata {
            version: u32,
            state: D1State,
        }
        return serde_json::to_vec(&Metadata {
            version: 6,
            state: state.stage(),
        });
    }
    if state.coordinator.is_some() {
        // Dedicated format: old binaries must not ignore coordinator authority.
        #[derive(Serialize)]
        struct Metadata<'a> {
            version: u32,
            state: &'a D1State,
        }
        return serde_json::to_vec(&Metadata { version: 5, state });
    }
    if let Some(managed) = &state.managed {
        return serde_json::to_vec(&D1MetadataV4 {
            version: 4,
            shard: state.shard,
            epoch: state.epoch,
            sequence: state.sequence,
            dedup_bytes_limit: state.dedup_bytes_limit,
            managed: managed.clone(),
        });
    }
    match state.dedup_bytes_limit {
        None => serde_json::to_vec(&D1MetadataV2::of(state)),
        Some(dedup_bytes_limit) => serde_json::to_vec(&D1MetadataV3 {
            version: 3,
            shard: state.shard,
            epoch: state.epoch,
            sequence: state.sequence,
            dedup_bytes_limit,
        }),
    }
}

fn dedup_key(sequence: u64) -> Vec<u8> {
    let mut key = DEDUP_PREFIX.to_vec();
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn put_dedup(batch: &mut WriteBatch, records: &[DedupRecord]) -> Result<(), StorageError<NodeId>> {
    for record in records {
        batch.put(
            dedup_key(record.sequence),
            serde_json::to_vec(record).map_err(write_error)?,
        );
    }
    Ok(())
}
const DATA_PREFIX: &[u8] = b"sm/data/";
const DATA_PREFIX_END: &[u8] = b"sm/data0";
const APPLIED_KEY: &[u8] = b"sm/applied";
const MEMBERSHIP_KEY: &[u8] = b"sm/membership";
const SNAPSHOT_KEY: &[u8] = b"sm/snapshot";
const SNAPSHOT_DATA_KEY: &[u8] = b"sm/snapshot-data";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    pub data: Vec<u8>,
}

/// Disk-only snapshot envelope. Logical snapshot/transport bytes stay unchanged.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshotV2 {
    version: u32,
    meta: SnapshotMeta<NodeId, BasicNode>,
    data_len: u64,
}

fn put_snapshot(
    batch: &mut WriteBatch,
    snapshot: &StoredSnapshot,
) -> Result<(), StorageError<NodeId>> {
    let header = StoredSnapshotV2 {
        version: 2,
        meta: snapshot.meta.clone(),
        data_len: snapshot.data.len() as u64,
    };
    batch.put(
        SNAPSHOT_KEY,
        serde_json::to_vec(&header).map_err(write_error)?,
    );
    batch.put(SNAPSHOT_DATA_KEY, &snapshot.data);
    Ok(())
}

fn load_snapshot(db: &DB) -> anyhow::Result<Option<StoredSnapshot>> {
    let header = db.get(SNAPSHOT_KEY)?;
    let payload = db.get(SNAPSHOT_DATA_KEY)?;
    let Some(header) = header else {
        anyhow::ensure!(payload.is_none(), "orphan snapshot payload");
        return Ok(None);
    };
    #[derive(Deserialize)]
    struct Version {
        version: Option<u32>,
    }
    match serde_json::from_slice::<Version>(&header)?.version {
        None => {
            anyhow::ensure!(payload.is_none(), "legacy snapshot has separate payload");
            Ok(Some(serde_json::from_slice(&header)?))
        }
        Some(2) => {
            let header: StoredSnapshotV2 = serde_json::from_slice(&header)?;
            let data = payload.ok_or_else(|| anyhow::anyhow!("snapshot payload missing"))?;
            anyhow::ensure!(
                header.data_len == data.len() as u64,
                "snapshot payload length mismatch"
            );
            Ok(Some(StoredSnapshot {
                meta: header.meta,
                data,
            }))
        }
        _ => anyhow::bail!("unsupported stored snapshot version"),
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
    #[serde(default)]
    pub(crate) d1: Option<D1State>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotV1 {
    version: u32,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    d1: D1State,
}

// Borrow the locked state while preserving the exact sorted array wire format.
struct SnapshotEntries<'a>(&'a BTreeMap<Vec<u8>, Vec<u8>>);
impl Serialize for SnapshotEntries<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for pair in self.0.iter() {
            sequence.serialize_element(&pair)?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct BorrowedSnapshotV1<'a> {
    version: u32,
    entries: SnapshotEntries<'a>,
    d1: &'a D1State,
}

struct DecodedSnapshot {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    d1: Option<D1State>,
}

// Legacy snapshots are arrays; D1 snapshots are a mandatory versioned object.
fn decode_snapshot(bytes: &[u8]) -> anyhow::Result<DecodedSnapshot> {
    let first = bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace());
    let (entries, d1) = if first == Some(b'[') {
        (
            serde_json::from_slice::<Vec<(Vec<u8>, Vec<u8>)>>(bytes)?,
            None,
        )
    } else {
        // Deserialize directly into typed byte vectors, not one Value per byte.
        // This keeps strict fields/version checks and rejects duplicate fields.
        let snapshot: SnapshotV1 = serde_json::from_slice(bytes)?;
        anyhow::ensure!(snapshot.version == 1, "unsupported snapshot version");
        snapshot.d1.validate()?;
        (snapshot.entries, Some(snapshot.d1))
    };
    let mut data = BTreeMap::new();
    for (key, value) in entries {
        anyhow::ensure!(data.insert(key, value).is_none(), "duplicate snapshot key");
    }
    if let Some(state) = &d1 {
        anyhow::ensure!(
            state.managed.is_none() || state.matches_data(&data),
            "managed snapshot data differs from committed history"
        );
    }
    Ok(DecodedSnapshot { data, d1 })
}

#[derive(Debug)]
struct StateMachineStorage {
    db: DB,
    legacy_d1: AtomicBool,
    #[cfg(test)]
    fail_next_write: AtomicBool,
}

impl StateMachineStorage {
    fn write_sync(&self, batch: WriteBatch) -> Result<(), StorageError<NodeId>> {
        #[cfg(test)]
        if self.fail_next_write.swap(false, Ordering::Relaxed) {
            return Err(write_error(std::io::Error::other("injected sync failure")));
        }
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db
            .write_opt(batch, &options)
            .map_err(|error| StorageIOError::write_state_machine(&error).into())
    }
}

fn write_error<E>(error: E) -> StorageError<NodeId>
where
    E: std::error::Error + Send + Sync + 'static,
{
    StorageIOError::write_state_machine(&error).into()
}

fn data_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(DATA_PREFIX.len() + key.len());
    encoded.extend_from_slice(DATA_PREFIX);
    encoded.extend_from_slice(key);
    encoded
}

/// Atomic local D1 snapshot, not itself a distributed consistency guarantee.
#[derive(Debug)]
pub struct D1ReadSnapshot {
    pub shard: u64,
    pub epoch: u64,
    pub sequence: u64,
    pub entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// Requested snapshot bounds were exceeded; no partial data is returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct D1ReadLimit;

#[cfg(test)]
#[derive(Debug)]
struct ApplyPublishGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

/// Opaque publication pause for deterministic diagnostics. It exposes no data
/// or mutation methods; dropping it resumes state-machine work.
pub struct StateMachinePause<'a> {
    _guard: tokio::sync::RwLockWriteGuard<'a, StateMachineData>,
}

#[derive(Debug, Default)]
pub struct StateMachine {
    state: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    storage: Option<Arc<StateMachineStorage>>,
    #[cfg(test)]
    apply_publish_gate: std::sync::Mutex<Option<ApplyPublishGate>>,
    #[cfg(test)]
    snapshot_publish_gate: std::sync::Mutex<Option<ApplyPublishGate>>,
}

impl StateMachine {
    /// Pause state reads/publication for diagnostics without exposing mutable data.
    /// Do not retain this guard in normal serving code.
    pub async fn pause_state(&self) -> StateMachinePause<'_> {
        StateMachinePause {
            _guard: self.state.write().await,
        }
    }

    /// Open a state machine backed by RocksDB at `path`.
    ///
    /// The persisted keys, applied index, membership and latest snapshot are
    /// loaded eagerly so a restarted node observes the committed state.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        let db = DB::open(&options, path.as_ref())?;
        let storage = Arc::new(StateMachineStorage {
            db,
            legacy_d1: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_write: AtomicBool::new(false),
        });

        let mut d1 = None;
        if let Some(bytes) = storage.db.get(D1_KEY)? {
            // Inspect only the discriminator; deserialize directly from bytes so
            // large legacy requests do not also become a serde_json::Value tree.
            #[derive(Deserialize)]
            struct Version {
                version: u32,
            }
            match serde_json::from_slice::<Version>(&bytes)?.version {
                1 => {
                    d1 = Some(serde_json::from_slice::<D1State>(&bytes)?);
                    storage.legacy_d1.store(true, Ordering::Relaxed);
                }
                2 => {
                    let meta: D1MetadataV2 = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        meta.sequence <= MAX_DEDUP_REQUESTS as u64,
                        "D1 dedup capacity exceeded"
                    );
                    let mut state = D1State::new(meta.shard, meta.epoch);
                    state.sequence = meta.sequence;
                    d1 = Some(state);
                }
                3 => {
                    let meta: D1MetadataV3 = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        meta.dedup_bytes_limit == crate::EXTENDED_DEDUP_BYTES,
                        "unsupported D1 capacity"
                    );
                    anyhow::ensure!(
                        meta.sequence <= MAX_DEDUP_REQUESTS as u64,
                        "D1 dedup capacity exceeded"
                    );
                    let mut state = D1State::new(meta.shard, meta.epoch);
                    state
                        .set_capacity(
                            meta.shard,
                            meta.epoch,
                            crate::MAX_DEDUP_BYTES,
                            meta.dedup_bytes_limit,
                        )
                        .map_err(|e| anyhow::anyhow!("invalid capacity: {e:?}"))?;
                    state.sequence = meta.sequence;
                    d1 = Some(state);
                }
                4 => {
                    let meta: D1MetadataV4 = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        meta.sequence <= MAX_DEDUP_REQUESTS as u64,
                        "D1 dedup capacity exceeded"
                    );
                    let mut state = D1State::new(meta.shard, meta.epoch);
                    state.version = 3;
                    state.sequence = meta.sequence;
                    state.dedup_bytes_limit = meta.dedup_bytes_limit;
                    state.managed = Some(meta.managed);
                    d1 = Some(state);
                }
                5 => {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Metadata {
                        version: u32,
                        state: D1State,
                    }
                    let meta: Metadata = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        meta.version == 5 && meta.state.coordinator.is_some(),
                        "invalid coordinator metadata"
                    );
                    d1 = Some(meta.state);
                }
                6 => {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Metadata {
                        version: u32,
                        state: D1State,
                    }
                    let meta: Metadata = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        meta.version == 6
                            && meta.state.participant.is_some()
                            && meta.state.dedup.is_empty(),
                        "invalid participant metadata"
                    );
                    d1 = Some(meta.state);
                }
                7 => {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Metadata {
                        version: u32,
                        state: D1State,
                    }
                    let meta: Metadata = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        meta.version == 7
                            && meta.state.visibility.is_some()
                            && meta.state.dedup.is_empty(),
                        "invalid visibility metadata"
                    );
                    d1 = Some(meta.state);
                }
                _ => anyhow::bail!("unsupported D1 disk version"),
            }
        }
        for item in storage
            .db
            .iterator(IteratorMode::From(DEDUP_PREFIX, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(DEDUP_PREFIX) {
                break;
            }
            anyhow::ensure!(
                !storage.legacy_d1.load(Ordering::Relaxed),
                "legacy D1 state has unexpected incremental retry records"
            );
            let state = d1
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("orphan D1 retry record"))?;
            anyhow::ensure!(
                state.dedup.len() < MAX_DEDUP_REQUESTS,
                "D1 dedup capacity exceeded"
            );
            let record: DedupRecord = serde_json::from_slice(&value)?;
            anyhow::ensure!(
                key.as_ref() == dedup_key(record.sequence),
                "D1 retry key/sequence mismatch"
            );
            state.dedup.push(record);
        }
        if let Some(d1) = &d1 {
            d1.validate()?;
        }
        let last_applied_log: Option<LogId<NodeId>> = match storage.db.get(APPLIED_KEY)? {
            Some(value) => serde_json::from_slice(&value)?,
            None => None,
        };
        anyhow::ensure!(
            d1.is_none() || last_applied_log.is_some(),
            "D1 state is missing its applied marker"
        );
        let last_membership = match storage.db.get(MEMBERSHIP_KEY)? {
            Some(value) => serde_json::from_slice(&value)?,
            None => StoredMembership::default(),
        };
        let mut data = BTreeMap::new();
        let mut iter = storage
            .db
            .iterator(IteratorMode::From(DATA_PREFIX, Direction::Forward));
        for item in &mut iter {
            let (key, value) = item?;
            if !key.starts_with(DATA_PREFIX) {
                break;
            }
            data.insert(key[DATA_PREFIX.len()..].to_vec(), value.to_vec());
        }
        drop(iter);
        if let Some(d1) = &d1 {
            anyhow::ensure!(
                d1.managed.is_none() || d1.matches_data(&data),
                "managed disk data differs from committed history"
            );
        }
        let current_snapshot = load_snapshot(&storage.db)?;

        let snapshot_idx = current_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.meta.snapshot_id.rsplit('-').next())
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(Self {
            state: RwLock::new(StateMachineData {
                last_applied_log,
                last_membership,
                data,
                d1,
            }),
            snapshot_idx: AtomicU64::new(snapshot_idx),
            current_snapshot: RwLock::new(current_snapshot),
            storage: Some(storage),
            #[cfg(test)]
            apply_publish_gate: Default::default(),
            #[cfg(test)]
            snapshot_publish_gate: Default::default(),
        })
    }

    /// Local capacity capture; remote callers must first establish a read barrier.
    pub async fn d1_capacity(&self, shard: u64, epoch: u64) -> Result<crate::D1Capacity, D1Error> {
        let state = self.state.read().await;
        let d1 = state.d1.as_ref().ok_or(D1Error::NotActivated)?;
        d1.check_shard(shard)?;
        d1.check_epoch(epoch)?;
        Ok(d1.capacity())
    }

    /// Local capture only; network callers must first establish a leader read barrier.
    pub async fn d1_managed_mode(
        &self,
        shard: u64,
        epoch: u64,
    ) -> Result<Option<crate::D1ManagedMode>, D1Error> {
        let state = self.state.read().await;
        let d1 = state.d1.as_ref().ok_or(D1Error::NotActivated)?;
        d1.check_shard(shard)?;
        d1.check_epoch(epoch)?;
        Ok(d1.managed.clone())
    }

    /// Identity status is shard-local. Unknown is not an abort decision.
    pub async fn d1_managed_status(
        &self,
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        request_id: [u8; 16],
    ) -> Result<crate::D1ManagedStatus, D1Error> {
        let state = self.state.read().await;
        state
            .d1
            .as_ref()
            .ok_or(D1Error::NotActivated)?
            .managed_status(shard, epoch, mode_id, request_id)
    }

    /// Capture D1 metadata and bytes under one lock, checking bounds before cloning.
    /// This is a local snapshot; callers must first complete a Raft read barrier.
    pub async fn d1_read_snapshot(
        &self,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Option<D1ReadSnapshot>, D1ReadLimit> {
        let state = self.state.read().await;
        let Some(d1) = &state.d1 else { return Ok(None) };
        if state.data.len() > max_rows {
            return Err(D1ReadLimit);
        }
        let mut bytes = 0usize;
        for (key, value) in &state.data {
            bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
            if bytes > max_bytes {
                return Err(D1ReadLimit);
            }
        }
        Ok(Some(D1ReadSnapshot {
            shard: d1.shard,
            epoch: d1.epoch,
            sequence: d1.sequence,
            entries: state.data.clone(),
        }))
    }

    /// Local historical capture. Distributed callers must establish a leader
    /// barrier and verify authority/routing. This is not global admission.
    pub async fn participant_read_at(
        &self,
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        version: u64,
        limit: crate::versioned_reads::ReadLimit,
    ) -> Result<crate::versioned_reads::ReadSnapshot, D1Error> {
        let state = self.state.read().await;
        let d1 = state.d1.as_ref().ok_or(D1Error::NotActivated)?;
        d1.check_shard(shard)?;
        d1.check_epoch(epoch)?;
        if d1.managed.as_ref().is_none_or(|m| m.mode_id != mode_id) {
            return Err(D1Error::ManagedModeConflict);
        }
        d1.read_at(version, limit)
    }

    /// Local diagnostic participant status. Establish a leader read barrier
    /// before using this for distributed recovery; Unknown never means Aborted.
    pub async fn participant_status(
        &self,
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        id: [u8; 16],
    ) -> Result<crate::participant_writes::Status, D1Error> {
        let state = self.state.read().await;
        let d1 = state.d1.as_ref().ok_or(D1Error::NotActivated)?;
        d1.check_shard(shard)?;
        d1.check_epoch(epoch)?;
        if d1.managed.as_ref().is_none_or(|m| m.mode_id != mode_id) {
            return Err(D1Error::ManagedModeConflict);
        }
        let participant = d1
            .participant
            .as_ref()
            .ok_or(D1Error::ParticipantNotEnabled)?;
        if let Some(visibility) = &d1.visibility {
            if visibility.journal.transaction(id).is_some_and(|tx| {
                tx.ordinal <= visibility.applied
                    && tx.decision == Some(crate::transaction_decisions::Decision::Abort)
                    && tx.participants.iter().any(|p| p.shard == shard)
            }) {
                return Ok(crate::participant_writes::Status::Aborted);
            }
        }
        Ok(participant.status(id))
    }

    /// Local decision status only. Distributed callers must establish a leader
    /// read barrier first; absence never proves abort and is not a read version.
    pub async fn coordinator_journal(
        &self,
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
    ) -> Result<crate::transaction_decisions::Journal, D1Error> {
        let state = self.state.read().await;
        let d1 = state.d1.as_ref().ok_or(D1Error::NotActivated)?;
        d1.check_shard(shard)?;
        d1.check_epoch(epoch)?;
        if d1
            .managed
            .as_ref()
            .is_none_or(|mode| mode.mode_id != mode_id)
        {
            return Err(D1Error::ManagedModeConflict);
        }
        d1.coordinator.clone().ok_or(D1Error::CoordinatorNotEnabled)
    }

    /// Local diagnostic metadata, not a linearizable distributed read.
    /// Call `raft.ensure_linearizable()` before sampling on a leader.
    pub async fn d1_metadata(&self) -> Option<(u64, u64, u64)> {
        self.state
            .read()
            .await
            .d1
            .as_ref()
            .map(|d1| (d1.shard, d1.epoch, d1.sequence))
    }

    /// Read a UTF-8 key/value pair from the D2 compatibility surface.
    pub async fn get(&self, key: &str) -> Option<String> {
        self.get_raw(key.as_bytes())
            .await
            .and_then(|value| String::from_utf8(value).ok())
    }

    /// Read opaque D1 bytes.
    pub async fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.state.read().await.data.get(key).cloned()
    }

    /// Dump the whole D1 partition as opaque bytes.
    pub async fn scan_raw(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        self.state.read().await.data.clone()
    }

    /// UTF-8 view used by the D2 smoke tests and control port.
    pub async fn scan(&self) -> BTreeMap<String, String> {
        self.state
            .read()
            .await
            .data
            .iter()
            .filter_map(|(key, value)| {
                let key = String::from_utf8(key.clone()).ok()?;
                let value = String::from_utf8(value.clone()).ok()?;
                Some((key, value))
            })
            .collect()
    }

    pub async fn applied(&self) -> Option<LogId<NodeId>> {
        self.state.read().await.last_applied_log
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.state.read().await;
        let data = match &sm.d1 {
            Some(d1) => serde_json::to_vec(&BorrowedSnapshotV1 {
                version: 1,
                entries: SnapshotEntries(&sm.data),
                d1,
            }),
            None => serde_json::to_vec(&SnapshotEntries(&sm.data)),
        }
        .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();
        let mut current = self.current_snapshot.write().await;
        drop(sm);

        let idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match last_applied_log {
            Some(log) => format!("{}-{}-{}", log.leader_id, log.index, idx),
            None => format!("--{idx}"),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        if let Some(storage) = &self.storage {
            let mut batch = WriteBatch::default();
            put_snapshot(&mut batch, &stored)?;
            storage.write_sync(batch)?;
        }
        #[cfg(test)]
        if let Some(gate) = self.snapshot_publish_gate.lock().unwrap().take() {
            let _ = gate.entered.send(());
            gate.release
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("release snapshot publication");
        }
        *current = Some(stored);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let sm = self.state.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut sm = self.state.write().await;
        blocking_section(|| {
            let mut next = ApplyDelta::new(&sm);
            let mut out = Vec::new();
            let mut batch = WriteBatch::default();

            for entry in entries {
                next.last_applied_log = Some(entry.log_id);
                match entry.payload {
                    EntryPayload::Blank => out.push(ApplyResult {
                        value: None,
                        d1: None,
                    }),
                    EntryPayload::Membership(mem) => {
                        next.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                        out.push(ApplyResult {
                            value: None,
                            d1: None,
                        });
                    }
                    EntryPayload::Normal(cmd) => {
                        let mut result = ApplyResult {
                            value: None,
                            d1: None,
                        };
                        match cmd {
                            LogCommand::Noop => {}
                            LogCommand::TxnBatch(operations) => {
                                if next.d1.is_some() {
                                    result.d1 = Some(Err(D1Error::LegacyWriteDisabled));
                                } else {
                                    apply_operations(&sm, &mut next, &mut batch, operations);
                                }
                            }
                            LogCommand::D1V1(command) => {
                                result.d1 = Some(apply_d1(&sm, &mut next, &mut batch, command));
                            }
                        }
                        out.push(result);
                    }
                }
            }

            if let Some(storage) = &self.storage {
                let applied = serde_json::to_vec(&next.last_applied_log).map_err(write_error)?;
                let membership = serde_json::to_vec(&next.last_membership).map_err(write_error)?;
                if let Some(d1) = &next.d1 {
                    batch.put(D1_KEY, d1_metadata_bytes(d1).map_err(write_error)?);
                    if storage.legacy_d1.load(Ordering::Relaxed) {
                        if let Some(previous) = &sm.d1 {
                            put_dedup(&mut batch, &previous.dedup)?;
                        }
                    }
                    put_dedup(&mut batch, &d1.dedup)?;
                } else {
                    batch.delete(D1_KEY);
                }
                batch.put(APPLIED_KEY, applied);
                batch.put(MEMBERSHIP_KEY, membership);
                storage.write_sync(batch)?;
                storage.legacy_d1.store(false, Ordering::Relaxed);
            }
            #[cfg(test)]
            if let Some(gate) = self.apply_publish_gate.lock().unwrap().take() {
                let _ = gate.entered.send(());
                gate.release
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("test must release apply publication gate");
            }
            next.publish(&mut sm);
            Ok(out)
        })
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let DecodedSnapshot { data, d1 } = decode_snapshot(&bytes).map_err(|e| {
            StorageIOError::read_snapshot(
                Some(meta.signature()),
                &std::io::Error::other(e.to_string()),
            )
        })?;
        if d1.is_some() && meta.last_log_id.is_none() {
            return Err(StorageIOError::read_snapshot(
                Some(meta.signature()),
                &std::io::Error::other("D1 snapshot is missing its applied marker"),
            )
            .into());
        }
        // Serialize persistence and publication with apply and snapshot build.
        let mut sm = self.state.write().await;
        if sm.last_applied_log.is_some_and(|current| {
            meta.last_log_id.is_none_or(|incoming| {
                incoming.index < current.index
                    || (incoming.index == current.index && incoming != current)
            })
        }) {
            return Err(StorageIOError::read_snapshot(
                Some(meta.signature()),
                &std::io::Error::other("snapshot would regress applied state"),
            )
            .into());
        }
        if sm.d1.is_some() && d1.is_none() {
            return Err(StorageIOError::read_snapshot(
                Some(meta.signature()),
                &std::io::Error::other("cannot downgrade D1 snapshot to legacy"),
            )
            .into());
        }
        if let (Some(current), Some(incoming)) = (&sm.d1, &d1) {
            if current.managed.is_some()
                && (current.managed != incoming.managed
                    || incoming.epoch < current.epoch
                    || incoming.sequence < current.sequence
                    || incoming.dedup.get(..current.dedup.len()) != Some(current.dedup.as_slice()))
            {
                return Err(StorageIOError::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(
                        "snapshot would regress managed write fence or transaction history",
                    ),
                )
                .into());
            }
            if incoming.coordinator.is_some() && current.sequence != 0 {
                return Err(StorageIOError::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other("coordinator snapshot replaces nonempty data history"),
                )
                .into());
            }
            if let Some(journal) = &current.coordinator {
                let valid = incoming
                    .coordinator
                    .as_ref()
                    .is_some_and(|next| journal.validate_successor(next).is_ok());
                if !valid {
                    return Err(StorageIOError::read_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other("snapshot would rewrite coordinator history"),
                    )
                    .into());
                }
            }
            if current.participant.is_none()
                && incoming
                    .participant
                    .as_ref()
                    .is_some_and(|p| p.activated_sequence < current.sequence)
            {
                return Err(StorageIOError::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other("snapshot retroactively certifies participant writes"),
                )
                .into());
            }
            if let Some(participant) = &current.participant {
                if !incoming
                    .participant
                    .as_ref()
                    .is_some_and(|next| participant.validate_successor(next))
                {
                    return Err(StorageIOError::read_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other("snapshot would erase participant intent history"),
                    )
                    .into());
                }
            }
            if let Some(visibility) = &current.visibility {
                if incoming.epoch != current.epoch
                    || !incoming
                        .visibility
                        .as_ref()
                        .is_some_and(|next| visibility.validate_successor(next))
                {
                    return Err(StorageIOError::read_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other(
                            "snapshot regresses version history/watermark/epoch",
                        ),
                    )
                    .into());
                }
            } else if incoming.visibility.is_some()
                && current
                    .participant
                    .as_ref()
                    .is_some_and(|p| !p.records.is_empty())
            {
                return Err(StorageIOError::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other("snapshot retroactively versions participant history"),
                )
                .into());
            }
            if incoming.bytes_limit() < current.bytes_limit() {
                return Err(StorageIOError::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other("snapshot would reduce D1 capacity"),
                )
                .into());
            }
            if current.shard != incoming.shard {
                return Err(StorageIOError::read_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other("snapshot shard mismatch"),
                )
                .into());
            }
        }
        let next = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data,
            d1,
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        };
        // Acquire both locks before the durable write: there is no cancellation
        // point between a successful disk commit and publishing the new state.
        let mut current = self.current_snapshot.write().await;

        if let Some(storage) = &self.storage {
            let mut batch = WriteBatch::default();
            batch.delete_range(DATA_PREFIX, DATA_PREFIX_END);
            for (key, value) in &next.data {
                batch.put(data_key(key), value);
            }
            let applied = serde_json::to_vec(&next.last_applied_log).map_err(write_error)?;
            let membership = serde_json::to_vec(&next.last_membership).map_err(write_error)?;
            batch.delete_range(DEDUP_PREFIX, DEDUP_PREFIX_END);
            if let Some(d1) = &next.d1 {
                batch.put(D1_KEY, d1_metadata_bytes(d1).map_err(write_error)?);
                put_dedup(&mut batch, &d1.dedup)?;
            } else {
                batch.delete(D1_KEY);
            }
            batch.put(APPLIED_KEY, applied);
            batch.put(MEMBERSHIP_KEY, membership);
            put_snapshot(&mut batch, &stored)?;
            storage.write_sync(batch)?;
            storage.legacy_d1.store(false, Ordering::Relaxed);
        }

        #[cfg(test)]
        if let Some(gate) = self.snapshot_publish_gate.lock().unwrap().take() {
            let _ = gate.entered.send(());
            gate.release
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("release snapshot publication");
        }
        *sm = next;
        *current = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

/// An apply-call overlay. Only changed keys and newly accepted retry identities
/// are owned here; the published state remains untouched until sync succeeds.
struct ApplyDelta {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    d1: Option<D1State>,
    data: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    data_len: usize,
}

impl ApplyDelta {
    fn new(state: &StateMachineData) -> Self {
        Self {
            last_applied_log: state.last_applied_log,
            last_membership: state.last_membership.clone(),
            d1: state.d1.as_ref().map(D1State::stage),
            data: BTreeMap::new(),
            data_len: state.data.len(),
        }
    }

    fn publish(self, state: &mut StateMachineData) {
        for (key, value) in self.data {
            match value {
                Some(value) => {
                    state.data.insert(key, value);
                }
                None => {
                    state.data.remove(&key);
                }
            }
        }
        if let Some(mut next) = self.d1 {
            if let Some(previous) = state.d1.as_mut() {
                previous.version = next.version;
                previous.dedup_bytes_limit = next.dedup_bytes_limit;
                previous.managed = next.managed;
                previous.coordinator = next.coordinator;
                previous.participant = next.participant;
                previous.visibility = next.visibility;
                previous.epoch = next.epoch;
                previous.sequence = next.sequence;
                previous.dedup.append(&mut next.dedup);
            } else {
                state.d1 = Some(next);
            }
        }
        state.last_applied_log = self.last_applied_log;
        state.last_membership = self.last_membership;
    }
}

fn apply_operations(
    base: &StateMachineData,
    next: &mut ApplyDelta,
    batch: &mut WriteBatch,
    operations: Vec<WriteOp>,
) {
    for operation in operations {
        let (key, value) = match operation {
            WriteOp::Put { key, value } => (key, Some(value)),
            WriteOp::Delete { key } => (key, None),
        };
        let existed = next
            .data
            .get(&key)
            .map_or_else(|| base.data.contains_key(&key), Option::is_some);
        match (&value, existed) {
            (Some(_), false) => next.data_len += 1,
            (None, true) => next.data_len -= 1,
            _ => {}
        }
        match &value {
            Some(value) => batch.put(data_key(&key), value),
            None => batch.delete(data_key(&key)),
        }
        next.data.insert(key, value);
    }
}

fn apply_d1(
    base: &StateMachineData,
    next: &mut ApplyDelta,
    batch: &mut WriteBatch,
    command: D1Command,
) -> Result<D1Result, D1Error> {
    match command {
        D1Command::EnableVersionedReads {
            shard,
            epoch,
            mode_id,
            journal,
        } => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            d1.check_shard(shard)?;
            d1.check_epoch(epoch)?;
            if d1.managed.as_ref().is_none_or(|m| m.mode_id != mode_id) {
                return Err(D1Error::ManagedModeConflict);
            }
            let participant = d1
                .participant
                .as_ref()
                .ok_or(D1Error::ParticipantNotEnabled)?;
            if journal.authority() != participant.authority.mode || journal.transaction_count() != 0
            {
                return Err(D1Error::VisibilityHistory);
            }
            if d1.visibility.is_none() {
                if !participant.records.is_empty() {
                    return Err(D1Error::VisibilityHistory);
                }
                d1.visibility = Some(crate::versioned_reads::State {
                    baseline_sequence: d1.sequence,
                    epoch: d1.epoch,
                    applied: 0,
                    journal,
                    commits: Vec::new(),
                });
                d1.version = 6;
            }
            Ok(D1Result::Visibility {
                applied_prefix: d1.visibility.as_ref().unwrap().applied,
            })
        }
        D1Command::AdvanceVisibility {
            authority,
            shard,
            epoch,
            mode_id,
            ordinal,
            journal,
        } => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            d1.check_shard(shard)?;
            d1.check_epoch(epoch)?;
            if d1.managed.as_ref().is_none_or(|m| m.mode_id != mode_id) {
                return Err(D1Error::ManagedModeConflict);
            }
            if d1
                .participant
                .as_ref()
                .is_none_or(|p| p.authority != authority)
            {
                return Err(D1Error::ParticipantAuthority);
            }
            // All fallible validation and resolution happen on an unpublished
            // candidate; a rejected prefix cannot leave a partial terminal status.
            let mut candidate = d1.clone();
            let (result, request) =
                candidate.advance_visibility(ordinal, journal, base.d1.as_ref())?;
            *d1 = candidate;
            if let Some(request) = request {
                apply_operations(base, next, batch, request.operations);
            }
            Ok(result)
        }
        D1Command::EnableParticipant {
            shard,
            epoch,
            mode_id,
            authority,
        } => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            d1.check_shard(shard)?;
            d1.check_epoch(epoch)?;
            if d1.managed.as_ref().is_none_or(|m| m.mode_id != mode_id) {
                return Err(D1Error::ManagedModeConflict);
            }
            if d1.coordinator.is_some() {
                return Err(D1Error::CoordinatorOnly);
            }
            if authority.mode == [0; 16] || authority.shard == shard {
                return Err(D1Error::ParticipantAuthority);
            }
            if let Some(current) = &d1.participant {
                if current.authority != authority {
                    return Err(D1Error::ParticipantAuthority);
                }
            } else {
                d1.participant = Some(crate::participant_writes::State {
                    authority,
                    activated_sequence: d1.sequence,
                    records: Vec::new(),
                });
                d1.version = 5;
            }
            Ok(D1Result::Participant(
                crate::participant_writes::Status::Unknown,
            ))
        }
        D1Command::PrepareParticipant {
            authority,
            participant,
            request,
        } => next
            .d1
            .as_mut()
            .ok_or(D1Error::NotActivated)?
            .prepare_participant(&authority, participant, request, base.d1.as_ref()),
        D1Command::ResolveParticipant {
            authority,
            participant,
            id,
            decision,
        } => {
            if next.d1.as_ref().is_some_and(|d1| d1.visibility.is_some()) {
                return Err(D1Error::VisibilityRequired);
            }
            let (result, request) = next
                .d1
                .as_mut()
                .ok_or(D1Error::NotActivated)?
                .resolve_participant(&authority, participant, id, decision, base.d1.as_ref())?;
            if let Some(request) = request {
                apply_operations(base, next, batch, request.operations);
            }
            Ok(result)
        }
        D1Command::EnableCoordinator {
            shard,
            epoch,
            mode_id,
        } => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            d1.check_shard(shard)?;
            d1.check_epoch(epoch)?;
            let mode = d1
                .managed
                .as_ref()
                .ok_or(D1Error::ManagedWritesNotEnabled)?;
            if mode.mode_id != mode_id {
                return Err(D1Error::ManagedModeConflict);
            }
            if d1.participant.is_some() {
                return Err(D1Error::ParticipantOnly);
            }
            if d1.sequence != 0 || next.data_len != 0 {
                return Err(D1Error::CoordinatorRequiresEmptyShard);
            }
            if d1.coordinator.is_none() {
                d1.coordinator = Some(
                    crate::transaction_decisions::Journal::new(mode_id)
                        .map_err(D1Error::CoordinatorRejected)?,
                );
                d1.version = 4;
            }
            Ok(D1Result::Coordinator {
                resolved_prefix: d1.coordinator.as_ref().unwrap().resolved_prefix(),
            })
        }
        D1Command::Coordinator {
            shard,
            epoch,
            mode_id,
            command,
        } => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            d1.check_shard(shard)?;
            d1.check_epoch(epoch)?;
            if d1
                .managed
                .as_ref()
                .is_none_or(|mode| mode.mode_id != mode_id)
            {
                return Err(D1Error::ManagedModeConflict);
            }
            let journal = d1
                .coordinator
                .as_mut()
                .ok_or(D1Error::CoordinatorNotEnabled)?;
            journal
                .apply(command)
                .map_err(D1Error::CoordinatorRejected)?;
            Ok(D1Result::Coordinator {
                resolved_prefix: journal.resolved_prefix(),
            })
        }
        D1Command::Activate { shard, epoch } => {
            if let Some(d1) = &next.d1 {
                d1.check_shard(shard)?;
                d1.check_epoch(epoch)?;
            } else {
                if next.data_len != 0 {
                    return Err(D1Error::LegacyDataPresent);
                }
                next.d1 = Some(D1State::new(shard, epoch));
            }
            Ok(D1Result::Activated { shard, epoch })
        }
        D1Command::SetEpoch {
            shard,
            expected_epoch,
            new_epoch,
        } => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            d1.check_shard(shard)?;
            if d1.visibility.is_some() && new_epoch != d1.epoch {
                return Err(D1Error::VisibilityRequired);
            }
            if d1.participant.as_ref().is_some_and(|s| s.pending()) && new_epoch != d1.epoch {
                return Err(D1Error::ParticipantBusy);
            }
            if new_epoch < expected_epoch {
                return Err(D1Error::InvalidEpoch);
            }
            // An exact administrative retry after losing the reply is harmless.
            if d1.epoch != new_epoch {
                d1.check_epoch(expected_epoch)?;
            }
            d1.epoch = new_epoch;
            Ok(D1Result::Epoch { epoch: new_epoch })
        }
        D1Command::SetDedupCapacity {
            shard,
            epoch,
            expected_bytes,
            new_bytes,
        } => next.d1.as_mut().ok_or(D1Error::NotActivated)?.set_capacity(
            shard,
            epoch,
            expected_bytes,
            new_bytes,
        ),
        D1Command::EnableManagedWrites {
            shard,
            epoch,
            expected_sequence,
            mode_id,
        } => {
            // All D1 data must have a replayable identity before opting in,
            // including legacy history and earlier writes in this apply batch.
            let d1 = next.d1.as_ref().ok_or(D1Error::NotActivated)?;
            d1.check_shard(shard)?;
            d1.check_epoch(epoch)?;
            let expected = d1.replayed_data(base.d1.as_ref());
            let mismatch = expected.len() != next.data_len
                || base
                    .data
                    .iter()
                    .filter(|(key, _)| !next.data.contains_key(*key))
                    .any(|(key, value)| {
                        expected.get(key.as_slice()).copied() != Some(value.as_slice())
                    })
                || next
                    .data
                    .iter()
                    .any(|(key, value)| expected.get(key.as_slice()).copied() != value.as_deref());
            if mismatch {
                return Err(D1Error::UnverifiedWriteHistory);
            }
            next.d1
                .as_mut()
                .unwrap()
                .enable_managed(shard, epoch, expected_sequence, mode_id)
        }
        D1Command::ManagedWrite { mode_id, request } => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            let result = d1.managed_write(mode_id, &request, base.d1.as_ref())?;
            if matches!(
                result,
                D1Result::ManagedWrite {
                    status: D1WriteStatus::Committed,
                    ..
                }
            ) {
                apply_operations(base, next, batch, request.operations);
            }
            Ok(result)
        }
        D1Command::Write(request) => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            let result = d1.write_with_history(&request, base.d1.as_ref())?;
            if matches!(
                result,
                D1Result::Write {
                    status: D1WriteStatus::Committed,
                    ..
                }
            ) {
                apply_operations(base, next, batch, request.operations);
            }
            Ok(result)
        }
    }
}

#[cfg(test)]
mod apply_publication_tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use std::time::Duration;

    struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    fn entry(index: u64, value: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LogCommand::put("key", value)),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn abort_during_persisted_apply_keeps_publication_and_fences_old_install(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut state = Arc::new(StateMachine::open(dir.path())?);
        state.apply([entry(1, "old")]).await?;
        let old = state.build_snapshot().await?;
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let release = ReleaseOnDrop(Some(release));
        *state.apply_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
            entered,
            release: gate,
        });
        let mut writer = state.clone();
        let apply = tokio::spawn(async move { writer.apply([entry(2, "new")]).await });
        tokio::time::timeout(Duration::from_secs(5), ready).await??;
        // The actual durable write has completed, but the original write lock
        // must still exclude snapshots until publication on the same stack.
        let persisted = state
            .storage
            .as_ref()
            .unwrap()
            .db
            .get(APPLIED_KEY)?
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Option<LogId<NodeId>>>(&persisted)?,
            Some(entry(2, "").log_id)
        );
        assert!(state.state.try_read().is_err());
        apply.abort();
        let (started, installing) = tokio::sync::oneshot::channel();
        let mut installer = state.clone();
        let install = tokio::spawn(async move {
            started.send(()).unwrap();
            installer.install_snapshot(&old.meta, old.snapshot).await
        });
        tokio::time::timeout(Duration::from_secs(5), installing).await??;
        assert!(
            !install.is_finished(),
            "snapshot install bypassed apply lock"
        );
        drop(release); // Also released automatically on every earlier error/panic.
                       // Abort may be observed after the non-yielding section, or completion
                       // may win the race. Neither outcome implies rollback of the disk write.
        match apply.await {
            Ok(result) => {
                result?;
            }
            Err(error) => assert!(error.is_cancelled()),
        }
        assert!(
            install.await?.is_err(),
            "old install regressed applied state"
        );
        assert_eq!(state.applied().await, Some(entry(2, "").log_id));
        assert_eq!(state.get("key").await.as_deref(), Some("new"));
        drop(state);
        let reopened = StateMachine::open(dir.path())?;
        assert_eq!(reopened.applied().await, Some(entry(2, "").log_id));
        assert_eq!(reopened.get("key").await.as_deref(), Some("new"));
        Ok(())
    }
}

#[cfg(test)]
mod incremental_apply_tests {
    use super::*;
    use crate::D1WriteRequest;
    use openraft::CommittedLeaderId;

    fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LogCommand::D1V1(command)),
        }
    }

    fn write(id: u8, expected: u64) -> D1Command {
        D1Command::Write(D1WriteRequest {
            shard: 7,
            epoch: 3,
            request_id: [id; 16],
            expected_sequence: Some(expected),
            operations: vec![
                WriteOp::put(b"key", [id]),
                WriteOp::put(b"gone", [id]),
                WriteOp::delete(b"gone"),
            ],
        })
    }

    fn activate() -> Entry<TypeConfig> {
        entry(1, D1Command::Activate { shard: 7, epoch: 3 })
    }

    // Construct the exact old layout, including graph bytes and the applied
    // marker, without calling any new persistence helpers.
    async fn legacy_fixture(path: &Path) -> anyhow::Result<()> {
        let mut memory = Arc::new(StateMachine::default());
        memory.apply([activate(), entry(2, write(1, 0))]).await?;
        let state = memory.state.read().await;
        let db = DB::open_default(path)?;
        let mut batch = WriteBatch::default();
        batch.put(D1_KEY, serde_json::to_vec(state.d1.as_ref().unwrap())?);
        batch.put(APPLIED_KEY, serde_json::to_vec(&state.last_applied_log)?);
        batch.put(MEMBERSHIP_KEY, serde_json::to_vec(&state.last_membership)?);
        for (key, value) in &state.data {
            batch.put(data_key(key), value);
        }
        let mut options = WriteOptions::default();
        options.set_sync(true);
        db.write_opt(batch, &options)?;
        Ok(())
    }

    fn disk_records(sm: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
        sm.storage
            .as_ref()
            .unwrap()
            .db
            .iterator(IteratorMode::From(DEDUP_PREFIX, Direction::Forward))
            .map(Result::unwrap)
            .take_while(|(k, _)| k.starts_with(DEDUP_PREFIX))
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect()
    }

    #[tokio::test]
    async fn migration_is_lazy_atomic_and_keeps_historical_retries() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        legacy_fixture(dir.path()).await?;
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        let old = sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap();
        assert!(serde_json::from_slice::<D1State>(&old).is_ok());
        assert!(disk_records(&sm).is_empty());
        let before = sm.state.read().await.clone();
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm.apply([entry(3, write(2, 1))]).await.is_err());
        {
            let after = sm.state.read().await;
            assert_eq!(after.data, before.data);
            assert_eq!(after.d1, before.d1);
            assert_eq!(after.last_applied_log, before.last_applied_log);
            assert_eq!(after.last_membership, before.last_membership);
        }
        assert_eq!(sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap(), old);
        assert!(disk_records(&sm).is_empty());
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        let result = sm
            .apply([
                entry(3, write(1, 0)),
                entry(4, write(2, 1)),
                entry(5, write(2, 1)),
            ])
            .await?;
        assert!(matches!(
            result[0].d1,
            Some(Ok(D1Result::Write {
                sequence: 1,
                status: D1WriteStatus::Duplicate,
                ..
            }))
        ));
        assert!(matches!(
            result[1].d1,
            Some(Ok(D1Result::Write {
                sequence: 2,
                status: D1WriteStatus::Committed,
                ..
            }))
        ));
        assert!(matches!(
            result[2].d1,
            Some(Ok(D1Result::Write {
                sequence: 2,
                status: D1WriteStatus::Duplicate,
                ..
            }))
        ));
        assert_eq!(disk_records(&sm).len(), 2);
        let header = sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap();
        assert_eq!(header, br#"{"version":2,"shard":7,"epoch":3,"sequence":2}"#);
        // This is precisely the old binary's D1 decoder: reject rather than
        // treating separated records as empty or accepting the newer version.
        assert!(serde_json::from_slice::<D1State>(&header).is_err());
        assert_eq!(dedup_key(1), b"sm/dedup/\0\0\0\0\0\0\0\x01");
        assert_eq!(
            disk_records(&sm)[0].1,
            serde_json::to_vec(&serde_json::from_slice::<D1State>(&old)?.dedup[0])?
        );
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        assert_eq!(sm.d1_metadata().await, Some((7, 3, 2)));
        assert_eq!(sm.applied().await.unwrap().index, 5);
        assert_eq!(sm.get_raw(b"key").await, Some(vec![2]));
        assert_eq!(sm.get_raw(b"gone").await, None);
        assert!(matches!(
            sm.apply([entry(6, write(1, 0))]).await?[0].d1,
            Some(Ok(D1Result::Write {
                sequence: 1,
                status: D1WriteStatus::Duplicate,
                ..
            }))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn incremental_apply_does_not_clone_untouched_values_or_rewrite_history(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        sm.apply([activate(), entry(2, write(1, 0))]).await?;
        let value_pointer = sm.state.read().await.data[b"key".as_slice()].as_ptr();
        let records = disk_records(&sm);
        let mut request = match write(2, 1) {
            D1Command::Write(r) => r,
            _ => unreachable!(),
        };
        request.operations = vec![WriteOp::put(b"another", vec![42; 1024])];
        let disk_sequence = sm.storage.as_ref().unwrap().db.latest_sequence_number();
        sm.apply([entry(3, D1Command::Write(request))]).await?;
        // Exactly one data key, one new retry record, and three metadata keys.
        // Rewriting any retained record would increase this RocksDB sequence.
        assert_eq!(
            sm.storage.as_ref().unwrap().db.latest_sequence_number() - disk_sequence,
            5
        );
        assert_eq!(
            sm.state.read().await.data[b"key".as_slice()].as_ptr(),
            value_pointer
        );
        assert_eq!(disk_records(&sm)[0], records[0]);
        let storage = sm.storage.as_ref().unwrap();
        let header = storage.db.get(D1_KEY)?.unwrap();
        assert!(header.len() < 100);
        // A rejected command advances only the applied marker; all historical
        // retry identity bytes stay the same across a failed CAS and a noop.
        let before = disk_records(&sm);
        sm.apply([entry(4, write(3, 0))]).await?;
        assert_eq!(disk_records(&sm), before);
        assert_eq!(sm.d1_metadata().await, Some((7, 3, 2)));
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_replaces_incremental_retry_range_and_reopens() -> anyhow::Result<()> {
        let mut source = Arc::new(StateMachine::default());
        source.apply([activate(), entry(10, write(1, 0))]).await?;
        let snapshot = source.build_snapshot().await?;
        let decoded: SnapshotV1 = serde_json::from_slice(snapshot.snapshot.get_ref())?;
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.d1.dedup.len(), 1);
        let dir = tempfile::tempdir()?;
        let mut target = Arc::new(StateMachine::open(dir.path())?);
        target
            .apply([activate(), entry(2, write(2, 0)), entry(3, write(3, 1))])
            .await?;
        assert_eq!(disk_records(&target).len(), 2);
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await?;
        assert_eq!(disk_records(&target).len(), 1);
        drop(target);
        let mut target = Arc::new(StateMachine::open(dir.path())?);
        assert_eq!(target.d1_metadata().await, Some((7, 3, 1)));
        assert!(matches!(
            target.apply([entry(11, write(1, 0))]).await?[0].d1,
            Some(Ok(D1Result::Write {
                status: D1WriteStatus::Duplicate,
                ..
            }))
        ));
        assert!(matches!(
            target.apply([entry(12, write(2, 1))]).await?[0].d1,
            Some(Ok(D1Result::Write {
                status: D1WriteStatus::Committed,
                sequence: 2,
                ..
            }))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn missing_mismatched_or_orphan_retry_records_fail_closed() -> anyhow::Result<()> {
        for corruption in 0..4 {
            let dir = tempfile::tempdir()?;
            let mut sm = Arc::new(StateMachine::open(dir.path())?);
            sm.apply([activate(), entry(2, write(1, 0))]).await?;
            let db = &sm.storage.as_ref().unwrap().db;
            match corruption {
                0 => db.delete(dedup_key(1))?,
                1 => {
                    let record = db.get(dedup_key(1))?.unwrap();
                    db.put(dedup_key(2), record)?;
                }
                2 => db.delete(D1_KEY)?,
                _ => {
                    let legacy = serde_json::to_vec(sm.state.read().await.d1.as_ref().unwrap())?;
                    db.put(D1_KEY, legacy)?;
                }
            }
            drop(sm);
            assert!(
                StateMachine::open(dir.path()).is_err(),
                "corruption {corruption}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn legacy_overlay_activation_observes_ordered_deletes() -> anyhow::Result<()> {
        let mut sm = Arc::new(StateMachine::default());
        let mut put = activate();
        put.payload = EntryPayload::Normal(LogCommand::put("key", "old"));
        sm.apply([put]).await?;
        let rejected = sm
            .apply([entry(2, D1Command::Activate { shard: 7, epoch: 3 })])
            .await?;
        assert!(matches!(
            rejected[0].d1,
            Some(Err(D1Error::LegacyDataPresent))
        ));
        let mut remove = activate();
        remove.log_id.index = 3;
        remove.payload = EntryPayload::Normal(LogCommand::txn(vec![
            WriteOp::put(b"key", b"new"),
            WriteOp::delete(b"key"),
            WriteOp::delete(b"key"),
        ]));
        sm.apply([
            remove,
            entry(4, D1Command::Activate { shard: 7, epoch: 3 }),
            entry(5, write(1, 0)),
        ])
        .await?;
        assert_eq!(sm.d1_metadata().await, Some((7, 3, 1)));
        assert_eq!(sm.get_raw(b"key").await, Some(vec![1]));
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn migration_crash_child() -> anyhow::Result<()> {
        let Some(path) = std::env::var_os("D2_MIGRATION_CRASH_DIRECTORY") else {
            return Ok(());
        };
        let path = std::path::PathBuf::from(path);
        legacy_fixture(&path.join("state.db")).await?;
        let mut sm = Arc::new(StateMachine::open(path.join("state.db"))?);
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (_release, gate) = std::sync::mpsc::channel();
        *sm.apply_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
            entered,
            release: gate,
        });
        let writer = tokio::spawn(async move { sm.apply([entry(3, write(2, 1))]).await });
        ready.await?;
        std::fs::write(path.join("synced"), b"ready")?;
        // Parent kills this entire process after observing the fsync boundary.
        writer.await??;
        anyhow::bail!("parent did not kill migration child before publication")
    }

    #[tokio::test]
    async fn process_death_after_migration_sync_recovers_complete_retry_identity(
    ) -> anyhow::Result<()> {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let dir = tempfile::tempdir()?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "state_machine::incremental_apply_tests::migration_crash_child",
                    "--nocapture",
                ])
                .env("D2_MIGRATION_CRASH_DIRECTORY", dir.path())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?,
        );
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            while !dir.path().join("synced").exists() {
                anyhow::ensure!(
                    child.0.try_wait()?.is_none(),
                    "migration child exited before sync"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            anyhow::Ok(())
        })
        .await??;
        child.0.kill()?;
        child.0.wait()?;
        let mut sm = Arc::new(StateMachine::open(dir.path().join("state.db"))?);
        assert_eq!(sm.d1_metadata().await, Some((7, 3, 2)));
        assert_eq!(sm.applied().await.unwrap().index, 3);
        assert_eq!(sm.get_raw(b"key").await, Some(vec![2]));
        assert_eq!(disk_records(&sm).len(), 2);
        let results = sm
            .apply([entry(4, write(1, 0)), entry(5, write(2, 1))])
            .await?;
        for (index, result) in results.iter().enumerate() {
            assert!(
                matches!(result.d1, Some(Ok(D1Result::Write { sequence, status: D1WriteStatus::Duplicate, .. })) if sequence == index as u64 + 1)
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod typed_snapshot_tests {
    use super::*;

    #[test]
    fn borrowed_snapshot_is_byte_identical_to_owned_v1_and_legacy() {
        let entries = BTreeMap::from([(vec![0, 255], vec![128, 1]), (vec![3], vec![])]);
        let d1 = D1State::new(7, 3);
        let owned = SnapshotV1 {
            version: 1,
            entries: entries.clone().into_iter().collect(),
            d1: d1.clone(),
        };
        let borrowed = BorrowedSnapshotV1 {
            version: 1,
            entries: SnapshotEntries(&entries),
            d1: &d1,
        };
        let old = serde_json::to_vec(&owned).unwrap();
        assert_eq!(serde_json::to_vec(&borrowed).unwrap(), old);
        let decoded = decode_snapshot(&old).unwrap();
        assert_eq!(decoded.data, entries);
        assert_eq!(decoded.d1, Some(d1));
        let legacy = serde_json::to_vec(&owned.entries).unwrap();
        assert_eq!(
            serde_json::to_vec(&SnapshotEntries(&entries)).unwrap(),
            legacy
        );
        assert_eq!(decode_snapshot(&legacy).unwrap().data, entries);
        let mut whitespace = b" \n\t\r".to_vec();
        whitespace.extend_from_slice(&legacy);
        assert_eq!(decode_snapshot(&whitespace).unwrap().data, entries);
        assert!(decode_snapshot(&whitespace).unwrap().d1.is_none());
    }

    #[test]
    fn typed_snapshot_rejects_malformed_and_duplicate_fields() {
        for bad in ["", "null", "42", "{}", "[] []", "[[[256],[]]]", "[[[1.0],[]]]", "[[[1],[]],[[1],[2]]]", "{\"version\":1,\"version\":1,\"entries\":[],\"d1\":{\"version\":1,\"shard\":7,\"epoch\":1,\"sequence\":0,\"dedup\":[]}}", "{\"version\":2,\"entries\":[],\"d1\":{\"version\":1,\"shard\":7,\"epoch\":1,\"sequence\":0,\"dedup\":[]}}", "{\"version\":1,\"entries\":[],\"unknown\":0,\"d1\":{\"version\":1,\"shard\":7,\"epoch\":1,\"sequence\":0,\"dedup\":[]}}", "{\"version\":1,\"entries\":[],\"d1\":{\"version\":1,\"shard\":7,\"epoch\":1,\"sequence\":1,\"dedup\":[]}}"] {
            assert!(decode_snapshot(bad.as_bytes()).is_err(), "accepted {bad}");
        }
    }
}

#[cfg(test)]
mod raw_snapshot_tests {
    use super::*;
    fn empty_snapshot() -> StoredSnapshot {
        StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "--1".into(),
            },
            data: b"[]".to_vec(),
        }
    }
    fn crash_snapshot() -> (Vec<u8>, crate::D1WriteRequest) {
        let request = crate::D1WriteRequest {
            shard: 7,
            epoch: 3,
            request_id: [1; 16],
            expected_sequence: Some(0),
            operations: vec![WriteOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        };
        let mut d1 = D1State::new(7, 3);
        d1.write(&request).unwrap();
        (
            serde_json::to_vec(&SnapshotV1 {
                version: 1,
                entries: vec![(b"k".to_vec(), b"v".to_vec())],
                d1,
            })
            .unwrap(),
            request,
        )
    }
    #[test]
    fn raw_snapshot_envelopes_reject_orphan_mixed_missing_and_bad_lengths() -> anyhow::Result<()> {
        for case in 0..9 {
            let dir = tempfile::tempdir()?;
            let db = DB::open_default(dir.path())?;
            let old = empty_snapshot();
            let header = StoredSnapshotV2 {
                version: 2,
                meta: old.meta.clone(),
                data_len: 2,
            };
            match case {
                0 => db.put(SNAPSHOT_DATA_KEY, b"[]")?,
                1 => {
                    db.put(SNAPSHOT_KEY, serde_json::to_vec(&old)?)?;
                    db.put(SNAPSHOT_DATA_KEY, b"[]")?;
                }
                2 => db.put(SNAPSHOT_KEY, serde_json::to_vec(&header)?)?,
                3 => {
                    db.put(SNAPSHOT_KEY, serde_json::to_vec(&header)?)?;
                    db.put(SNAPSHOT_DATA_KEY, b"[")?;
                }
                4 => {
                    db.put(
                        SNAPSHOT_KEY,
                        serde_json::to_vec(&StoredSnapshotV2 {
                            version: 3,
                            ..header
                        })?,
                    )?;
                    db.put(SNAPSHOT_DATA_KEY, b"[]")?;
                }
                6 => {
                    let mut value = serde_json::to_value(&old)?;
                    value["version"] = serde_json::Value::Null;
                    db.put(SNAPSHOT_KEY, serde_json::to_vec(&value)?)?;
                }
                7 => {
                    let mut value = serde_json::to_value(&old)?;
                    value["extra"] = true.into();
                    db.put(SNAPSHOT_KEY, serde_json::to_vec(&value)?)?;
                }
                8 => {
                    let encoded = serde_json::to_string(&header)?.replacen(
                        "\"version\":2",
                        "\"version\":2,\"version\":2",
                        1,
                    );
                    db.put(SNAPSHOT_KEY, encoded.as_bytes())?;
                    db.put(SNAPSHOT_DATA_KEY, b"[]")?;
                }
                _ => {
                    let mut value = serde_json::to_value(header)?;
                    value["extra"] = true.into();
                    db.put(SNAPSHOT_KEY, serde_json::to_vec(&value)?)?;
                    db.put(SNAPSHOT_DATA_KEY, b"[]")?;
                }
            }
            assert!(load_snapshot(&db).is_err(), "accepted case {case}");
        }
        Ok(())
    }
    #[tokio::test]
    async fn legacy_empty_snapshot_migrates_only_after_successful_sync() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let old = empty_snapshot();
        let bytes = serde_json::to_vec(&old)?;
        {
            let db = DB::open_default(dir.path())?;
            db.put(SNAPSHOT_KEY, &bytes)?;
        }
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        assert_eq!(
            sm.storage.as_ref().unwrap().db.get(SNAPSHOT_KEY)?.unwrap(),
            bytes
        );
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm.build_snapshot().await.is_err());
        assert_eq!(
            sm.storage.as_ref().unwrap().db.get(SNAPSHOT_KEY)?.unwrap(),
            bytes
        );
        assert!(sm
            .storage
            .as_ref()
            .unwrap()
            .db
            .get(SNAPSHOT_DATA_KEY)?
            .is_none());
        assert_eq!(
            sm.current_snapshot
                .read()
                .await
                .as_ref()
                .unwrap()
                .meta
                .snapshot_id,
            old.meta.snapshot_id
        );
        let fresh = sm.build_snapshot().await?;
        assert_eq!(fresh.snapshot.get_ref(), b"[]");
        let disk = sm.storage.as_ref().unwrap().db.get(SNAPSHOT_KEY)?.unwrap();
        assert!(
            serde_json::from_slice::<StoredSnapshot>(&disk).is_err(),
            "old decoder must fail closed"
        );
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        assert_eq!(
            sm.get_current_snapshot().await?.unwrap().snapshot.get_ref(),
            b"[]"
        );
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn snapshot_crash_child() -> anyhow::Result<()> {
        let Some(path) = std::env::var_os("D2_SNAPSHOT_CRASH_DIRECTORY") else {
            return Ok(());
        };
        let path = std::path::PathBuf::from(path);
        let mut sm = Arc::new(StateMachine::open(path.join("state.db"))?);
        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 7)),
            last_membership: StoredMembership::default(),
            snapshot_id: "1-7-1".into(),
        };
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (_release, gate) = std::sync::mpsc::channel();
        *sm.snapshot_publish_gate.lock().unwrap() = Some(ApplyPublishGate {
            entered,
            release: gate,
        });
        let writer = tokio::spawn(async move {
            sm.install_snapshot(&meta, Box::new(Cursor::new(crash_snapshot().0)))
                .await
        });
        ready.await?;
        std::fs::write(path.join("synced"), b"ready")?;
        writer.await??;
        anyhow::bail!("parent must kill child after sync")
    }
    #[tokio::test]
    async fn process_death_after_snapshot_sync_recovers_metadata_payload_and_state(
    ) -> anyhow::Result<()> {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let dir = tempfile::tempdir()?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "state_machine::raw_snapshot_tests::snapshot_crash_child",
                    "--nocapture",
                ])
                .env("D2_SNAPSHOT_CRASH_DIRECTORY", dir.path())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?,
        );
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            while !dir.path().join("synced").exists() {
                anyhow::ensure!(child.0.try_wait()?.is_none(), "child exited before sync");
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            anyhow::Ok(())
        })
        .await??;
        child.0.kill()?;
        child.0.wait()?;
        let mut sm = Arc::new(StateMachine::open(dir.path().join("state.db"))?);
        assert_eq!(sm.applied().await.unwrap().index, 7);
        assert_eq!(sm.get_raw(b"k").await, Some(b"v".to_vec()));
        let snapshot = sm.get_current_snapshot().await?.unwrap();
        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 7);
        assert_eq!(snapshot.snapshot.get_ref(), &crash_snapshot().0);
        assert_eq!(sm.d1_metadata().await, Some((7, 3, 1)));
        let reply = sm
            .apply([Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 8),
                payload: EntryPayload::Normal(LogCommand::D1V1(D1Command::Write(
                    crash_snapshot().1,
                ))),
            }])
            .await?;
        assert!(matches!(
            reply[0].d1,
            Some(Ok(D1Result::Write {
                sequence: 1,
                status: D1WriteStatus::Duplicate,
                ..
            }))
        ));
        Ok(())
    }
}

#[cfg(test)]
mod capacity_persistence_tests {
    use super::*;
    fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LogCommand::D1V1(command)),
        }
    }
    fn expand() -> D1Command {
        D1Command::SetDedupCapacity {
            shard: 7,
            epoch: 3,
            expected_bytes: crate::MAX_DEDUP_BYTES,
            new_bytes: crate::EXTENDED_DEDUP_BYTES,
        }
    }
    #[tokio::test]
    async fn capacity_sync_failure_reopen_snapshot_and_retry() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        let request = crate::D1WriteRequest {
            shard: 7,
            epoch: 3,
            request_id: [1; 16],
            expected_sequence: Some(0),
            operations: vec![WriteOp::put(b"k", b"v")],
        };
        sm.apply([
            entry(1, D1Command::Activate { shard: 7, epoch: 3 }),
            entry(2, D1Command::Write(request.clone())),
        ])
        .await?;
        // Reconstruct an actual v1 single-key fixture to exercise expansion
        // together with lazy incremental-history migration in one sync batch.
        let legacy = serde_json::to_vec(sm.state.read().await.d1.as_ref().unwrap())?;
        let mut legacy_batch = WriteBatch::default();
        legacy_batch.put(D1_KEY, legacy);
        legacy_batch.delete_range(DEDUP_PREFIX, DEDUP_PREFIX_END);
        sm.storage.as_ref().unwrap().write_sync(legacy_batch)?;
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        let old = sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap();
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm.apply([entry(3, expand())]).await.is_err());
        assert_eq!(sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap(), old);
        assert_eq!(
            sm.d1_capacity(7, 3).await.unwrap().bytes_limit,
            crate::MAX_DEDUP_BYTES
        );
        assert_eq!(sm.applied().await.unwrap().index, 2);
        sm.apply([entry(3, expand()), entry(4, expand())]).await?;
        assert_eq!(sm.d1_metadata().await, Some((7, 3, 1)));
        assert_eq!(
            sm.d1_capacity(7, 3).await.unwrap().bytes_limit,
            crate::EXTENDED_DEDUP_BYTES
        );
        let header = sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap();
        let parsed: D1MetadataV3 = serde_json::from_slice(&header)?;
        assert_eq!(parsed.version, 3);
        assert!(serde_json::from_slice::<D1MetadataV2>(&header).is_err());
        let snapshot = sm.build_snapshot().await?;
        let decoded: SnapshotV1 = serde_json::from_slice(snapshot.snapshot.get_ref())?;
        assert_eq!(decoded.d1.version, 2);
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        assert_eq!(
            sm.d1_capacity(7, 3).await.unwrap().bytes_limit,
            crate::EXTENDED_DEDUP_BYTES
        );
        let mut old = Arc::new(StateMachine::default());
        old.apply([entry(9, D1Command::Activate { shard: 7, epoch: 3 })])
            .await?;
        let downgrade = old.build_snapshot().await?;
        assert!(sm
            .install_snapshot(&downgrade.meta, downgrade.snapshot)
            .await
            .is_err());
        assert_eq!(
            sm.d1_capacity(7, 3).await.unwrap().bytes_limit,
            crate::EXTENDED_DEDUP_BYTES
        );
        let target_dir = tempfile::tempdir()?;
        let mut target = Arc::new(StateMachine::open(target_dir.path())?);
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await?;
        drop(target);
        let mut target = Arc::new(StateMachine::open(target_dir.path())?);
        for node in [&mut sm, &mut target] {
            assert_eq!(
                node.d1_capacity(7, 3).await.unwrap().bytes_limit,
                crate::EXTENDED_DEDUP_BYTES
            );
            let result = node
                .apply([entry(5, D1Command::Write(request.clone()))])
                .await?;
            assert!(matches!(
                result[0].d1,
                Some(Ok(D1Result::Write {
                    sequence: 1,
                    status: D1WriteStatus::Duplicate,
                    ..
                }))
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "managed_state_tests.rs"]
mod managed_state_tests;

#[cfg(test)]
mod coordinator_tests {
    use super::*;
    use crate::transaction_decisions::{Command, Decision, Participant};
    use openraft::CommittedLeaderId;

    fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LogCommand::D1V1(command)),
        }
    }
    fn command(command: Command) -> D1Command {
        D1Command::Coordinator {
            shard: 7,
            epoch: 3,
            mode_id: [1; 16],
            command,
        }
    }
    fn begin() -> D1Command {
        command(Command::Begin {
            id: [2; 16],
            participants: vec![Participant {
                shard: 8,
                epoch: 1,
                mode: [3; 16],
                intent: [4; 32],
            }],
        })
    }
    async fn setup(sm: &mut Arc<StateMachine>) {
        let results = sm
            .apply([
                entry(1, D1Command::Activate { shard: 7, epoch: 3 }),
                entry(
                    2,
                    D1Command::EnableManagedWrites {
                        shard: 7,
                        epoch: 3,
                        expected_sequence: 0,
                        mode_id: [1; 16],
                    },
                ),
                entry(
                    3,
                    D1Command::EnableCoordinator {
                        shard: 7,
                        epoch: 3,
                        mode_id: [1; 16],
                    },
                ),
            ])
            .await
            .unwrap();
        assert!(results.iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn coordinator_reopens_pending_decision_and_blocks_data_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm).await;
        sm.apply([entry(4, begin())]).await.unwrap();
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        let snapshot = sm
            .state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .coordinator
            .clone()
            .unwrap();
        assert_eq!(snapshot.resolved_prefix(), 0);
        assert_eq!(snapshot.transaction([2; 16]).unwrap().decision, None);
        let request = crate::D1WriteRequest {
            shard: 7,
            epoch: 3,
            request_id: [5; 16],
            expected_sequence: Some(0),
            operations: vec![WriteOp::put("bad", "data")],
        };
        let rejected = sm
            .apply([entry(
                5,
                D1Command::ManagedWrite {
                    mode_id: [1; 16],
                    request,
                },
            )])
            .await
            .unwrap();
        assert_eq!(rejected[0].d1, Some(Err(D1Error::CoordinatorOnly)));
        sm.apply([entry(
            6,
            command(Command::Decide {
                id: [2; 16],
                decision: Decision::Abort,
            }),
        )])
        .await
        .unwrap();
        let image = sm.build_snapshot().await.unwrap();
        let target = tempfile::tempdir().unwrap();
        let mut restored = Arc::new(StateMachine::open(target.path()).unwrap());
        restored
            .install_snapshot(&image.meta, image.snapshot)
            .await
            .unwrap();
        drop(restored);
        let restored = StateMachine::open(target.path()).unwrap();
        let state = restored.state.read().await;
        assert!(state.data.is_empty());
        assert_eq!(
            state
                .d1
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .transaction([2; 16])
                .unwrap()
                .decision,
            Some(Decision::Abort)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn coordinator_sync_failure_does_not_publish_or_forget_pending() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm).await;
        sm.apply([entry(4, begin())]).await.unwrap();
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm
            .apply([entry(
                5,
                command(Command::Decide {
                    id: [2; 16],
                    decision: Decision::Abort
                })
            )])
            .await
            .is_err());
        assert_eq!(sm.state.read().await.last_applied_log.unwrap().index, 4);
        drop(sm);
        let sm = StateMachine::open(dir.path()).unwrap();
        assert_eq!(
            sm.state
                .read()
                .await
                .d1
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .transaction([2; 16])
                .unwrap()
                .decision,
            None
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn coordinator_snapshot_cannot_erase_history_even_with_newer_marker() {
        let mut sm = Arc::new(StateMachine::default());
        setup(&mut sm).await;
        let mut old = sm.build_snapshot().await.unwrap();
        sm.apply([entry(4, begin())]).await.unwrap();
        old.meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 10));
        assert!(sm.install_snapshot(&old.meta, old.snapshot).await.is_err());
        assert!(sm
            .state
            .read()
            .await
            .d1
            .as_ref()
            .unwrap()
            .coordinator
            .as_ref()
            .unwrap()
            .transaction([2; 16])
            .is_some());
    }
}

#[cfg(test)]
#[path = "participant_state_tests.rs"]
mod participant_state_tests;

#[cfg(test)]
#[path = "versioned_state_tests.rs"]
mod versioned_state_tests;
