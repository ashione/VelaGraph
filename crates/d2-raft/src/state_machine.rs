use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::d1::D1State;
use crate::types::{ApplyResult, LogCommand, NodeId, TypeConfig, WriteOp};
use crate::{D1Command, D1Error, D1Result, D1WriteStatus};

const D1_KEY: &[u8] = b"sm/d1";
const DATA_PREFIX: &[u8] = b"sm/data/";
const DATA_PREFIX_END: &[u8] = b"sm/data0";
const APPLIED_KEY: &[u8] = b"sm/applied";
const MEMBERSHIP_KEY: &[u8] = b"sm/membership";
const SNAPSHOT_KEY: &[u8] = b"sm/snapshot";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    pub data: Vec<u8>,
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

struct DecodedSnapshot {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    d1: Option<D1State>,
}

// Legacy snapshots are arrays; D1 snapshots are a mandatory versioned object.
fn decode_snapshot(bytes: &[u8]) -> anyhow::Result<DecodedSnapshot> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    let (entries, d1) = if value.is_array() {
        (
            serde_json::from_value::<Vec<(Vec<u8>, Vec<u8>)>>(value)?,
            None,
        )
    } else {
        let snapshot: SnapshotV1 = serde_json::from_value(value)?;
        anyhow::ensure!(snapshot.version == 1, "unsupported snapshot version");
        snapshot.d1.validate()?;
        (snapshot.entries, Some(snapshot.d1))
    };
    let mut data = BTreeMap::new();
    for (key, value) in entries {
        anyhow::ensure!(data.insert(key, value).is_none(), "duplicate snapshot key");
    }
    Ok(DecodedSnapshot { data, d1 })
}

#[derive(Debug)]
struct StateMachineStorage {
    db: DB,
}

impl StateMachineStorage {
    fn write_sync(&self, batch: WriteBatch) -> Result<(), StorageError<NodeId>> {
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

#[derive(Debug, Default)]
pub struct StateMachine {
    pub state: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    storage: Option<Arc<StateMachineStorage>>,
}

impl StateMachine {
    /// Open a state machine backed by RocksDB at `path`.
    ///
    /// The persisted keys, applied index, membership and latest snapshot are
    /// loaded eagerly so a restarted node observes the committed state.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        let db = DB::open(&options, path.as_ref())?;
        let storage = Arc::new(StateMachineStorage { db });

        let d1: Option<D1State> = storage
            .db
            .get(D1_KEY)?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()?;
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
        let current_snapshot: Option<StoredSnapshot> = match storage.db.get(SNAPSHOT_KEY)? {
            Some(value) => Some(serde_json::from_slice(&value)?),
            None => None,
        };

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
        })
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
        let entries: Vec<(Vec<u8>, Vec<u8>)> = sm
            .data
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let data = match &sm.d1 {
            Some(d1) => serde_json::to_vec(&SnapshotV1 {
                version: 1,
                entries,
                d1: d1.clone(),
            }),
            None => serde_json::to_vec(&entries),
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
            let value = serde_json::to_vec(&stored).map_err(write_error)?;
            let mut batch = WriteBatch::default();
            batch.put(SNAPSHOT_KEY, value);
            storage.write_sync(batch)?;
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
        let mut next = sm.clone();
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
                                apply_operations(&mut next, &mut batch, operations);
                            }
                        }
                        LogCommand::D1V1(command) => {
                            result.d1 = Some(apply_d1(&mut next, &mut batch, command));
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
                batch.put(D1_KEY, serde_json::to_vec(d1).map_err(write_error)?);
            } else {
                batch.delete(D1_KEY);
            }
            batch.put(APPLIED_KEY, applied);
            batch.put(MEMBERSHIP_KEY, membership);
            storage.write_sync(batch)?;
        }
        *sm = next;
        Ok(out)
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
            let snapshot = serde_json::to_vec(&stored).map_err(write_error)?;
            if let Some(d1) = &next.d1 {
                batch.put(D1_KEY, serde_json::to_vec(d1).map_err(write_error)?);
            } else {
                batch.delete(D1_KEY);
            }
            batch.put(APPLIED_KEY, applied);
            batch.put(MEMBERSHIP_KEY, membership);
            batch.put(SNAPSHOT_KEY, snapshot);
            storage.write_sync(batch)?;
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

fn apply_operations(next: &mut StateMachineData, batch: &mut WriteBatch, operations: Vec<WriteOp>) {
    for operation in operations {
        match operation {
            WriteOp::Put { key, value } => {
                next.data.insert(key.clone(), value.clone());
                batch.put(data_key(&key), value);
            }
            WriteOp::Delete { key } => {
                next.data.remove(&key);
                batch.delete(data_key(&key));
            }
        }
    }
}

fn apply_d1(
    next: &mut StateMachineData,
    batch: &mut WriteBatch,
    command: D1Command,
) -> Result<D1Result, D1Error> {
    match command {
        D1Command::Activate { shard, epoch } => {
            if let Some(d1) = &next.d1 {
                d1.check_shard(shard)?;
                d1.check_epoch(epoch)?;
            } else {
                if !next.data.is_empty() {
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
        D1Command::Write(request) => {
            let d1 = next.d1.as_mut().ok_or(D1Error::NotActivated)?;
            let result = d1.write(&request)?;
            if matches!(
                result,
                D1Result::Write {
                    status: D1WriteStatus::Committed,
                    ..
                }
            ) {
                apply_operations(next, batch, request.operations);
            }
            Ok(result)
        }
    }
}
