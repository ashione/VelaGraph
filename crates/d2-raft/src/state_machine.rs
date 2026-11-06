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

use crate::types::{ApplyResult, LogCommand, NodeId, TypeConfig};

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
    pub data: BTreeMap<String, String>,
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

fn data_key(key: &str) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(DATA_PREFIX.len() + key.len());
    encoded.extend_from_slice(DATA_PREFIX);
    encoded.extend_from_slice(key.as_bytes());
    encoded
}

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

        let last_applied_log = match storage.db.get(APPLIED_KEY)? {
            Some(value) => serde_json::from_slice(&value)?,
            None => None,
        };
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
            let key = String::from_utf8(key[DATA_PREFIX.len()..].to_vec())?;
            let value = String::from_utf8(value.to_vec())?;
            data.insert(key, value);
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
            }),
            snapshot_idx: AtomicU64::new(snapshot_idx),
            current_snapshot: RwLock::new(current_snapshot),
            storage: Some(storage),
        })
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.state.read().await.data.get(key).cloned()
    }

    pub async fn scan(&self) -> BTreeMap<String, String> {
        self.state.read().await.data.clone()
    }

    pub async fn applied(&self) -> Option<LogId<NodeId>> {
        self.state.read().await.last_applied_log
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.state.read().await;
        let data =
            serde_json::to_vec(&sm.data).map_err(|e| StorageIOError::read_state_machine(&e))?;
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
                EntryPayload::Blank => out.push(ApplyResult { value: None }),
                EntryPayload::Membership(mem) => {
                    next.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    out.push(ApplyResult { value: None });
                }
                EntryPayload::Normal(cmd) => match cmd {
                    LogCommand::Noop => out.push(ApplyResult { value: None }),
                    LogCommand::Put(key, value) => {
                        next.data.insert(key.clone(), value.clone());
                        batch.put(data_key(&key), value.as_bytes());
                        out.push(ApplyResult { value: Some(value) });
                    }
                    LogCommand::Delete(key) => {
                        next.data.remove(&key);
                        batch.delete(data_key(&key));
                        out.push(ApplyResult { value: None });
                    }
                },
            }
        }

        if let Some(storage) = &self.storage {
            let applied = serde_json::to_vec(&next.last_applied_log).map_err(write_error)?;
            let membership = serde_json::to_vec(&next.last_membership).map_err(write_error)?;
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
        let data = serde_json::from_slice(&bytes)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let next = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data,
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        };

        if let Some(storage) = &self.storage {
            let mut batch = WriteBatch::default();
            batch.delete_range(DATA_PREFIX, DATA_PREFIX_END);
            for (key, value) in &next.data {
                batch.put(data_key(key), value.as_bytes());
            }
            let applied = serde_json::to_vec(&next.last_applied_log).map_err(write_error)?;
            let membership = serde_json::to_vec(&next.last_membership).map_err(write_error)?;
            let snapshot = serde_json::to_vec(&stored).map_err(write_error)?;
            batch.put(APPLIED_KEY, applied);
            batch.put(MEMBERSHIP_KEY, membership);
            batch.put(SNAPSHOT_KEY, snapshot);
            storage.write_sync(batch)?;
        }

        let mut sm = self.state.write().await;
        *sm = next;
        drop(sm);
        let mut current = self.current_snapshot.write().await;
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
