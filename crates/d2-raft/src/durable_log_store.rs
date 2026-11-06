//! Durable Raft log storage.
//!
//! Raft log entries and vote/commit metadata live in a dedicated RocksDB
//! directory, separate from the state machine. Every append, truncate, purge
//! and vote write uses a synchronous write so acknowledged Raft state survives
//! process restart.

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    LogId, LogState, RaftLogReader, RaftTypeConfig, StorageError, StorageIOError, Vote,
};
use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use tokio::sync::Mutex;

use crate::types::{NodeId, TypeConfig};

const LOG_PREFIX: &[u8] = b"raft/log/";
const LOG_PREFIX_END: &[u8] = b"raft/log0";
const VOTE_KEY: &[u8] = b"raft/vote";
const COMMITTED_KEY: &[u8] = b"raft/committed";
const LAST_PURGED_KEY: &[u8] = b"raft/last_purged";

fn log_key(index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(LOG_PREFIX.len() + 8);
    key.extend_from_slice(LOG_PREFIX);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn log_index(key: &[u8]) -> Option<u64> {
    if key.len() != LOG_PREFIX.len() + 8 || !key.starts_with(LOG_PREFIX) {
        return None;
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&key[LOG_PREFIX.len()..]);
    Some(u64::from_be_bytes(bytes))
}

fn read_error<E>(error: E) -> StorageError<NodeId>
where
    E: std::error::Error + Send + Sync + 'static,
{
    StorageIOError::read(&error).into()
}

fn write_error<E>(error: E) -> StorageError<NodeId>
where
    E: std::error::Error + Send + Sync + 'static,
{
    StorageIOError::write(&error).into()
}

/// RocksDB-backed `RaftLogStorage` for the D2 type configuration.
#[derive(Clone)]
pub struct DurableLogStore {
    inner: Arc<Mutex<DurableLogStoreInner>>,
}

struct DurableLogStoreInner {
    db: DB,
}

impl DurableLogStore {
    /// Open or create the log database at `path`.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        let db = DB::open(&options, path.as_ref())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(DurableLogStoreInner { db })),
        })
    }
}

impl DurableLogStoreInner {
    fn write_batch(&self, batch: WriteBatch) -> Result<(), StorageError<NodeId>> {
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db.write_opt(batch, &options).map_err(write_error)
    }

    fn last_log_id(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let mut iter = self
            .db
            .iterator(IteratorMode::From(LOG_PREFIX_END, Direction::Reverse));
        for item in &mut iter {
            let (key, value) = item.map_err(read_error)?;
            if log_index(&key).is_none() {
                if key.as_ref() < LOG_PREFIX {
                    break;
                }
                continue;
            }
            let entry: openraft::Entry<TypeConfig> =
                serde_json::from_slice(&value).map_err(read_error)?;
            return Ok(Some(entry.log_id));
        }
        Ok(None)
    }

    fn last_purged(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        match self.db.get(LAST_PURGED_KEY).map_err(read_error)? {
            Some(value) => Ok(Some(serde_json::from_slice(&value).map_err(read_error)?)),
            None => Ok(None),
        }
    }

    fn read_entries<RB: RangeBounds<u64>>(
        &self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<NodeId>> {
        let mut entries = Vec::new();
        let mut iter = self
            .db
            .iterator(IteratorMode::From(LOG_PREFIX, Direction::Forward));
        for item in &mut iter {
            let (key, value) = item.map_err(read_error)?;
            let Some(index) = log_index(&key) else {
                if !key.starts_with(LOG_PREFIX) {
                    break;
                }
                continue;
            };
            if !range.contains(&index) {
                continue;
            }
            let entry: openraft::Entry<TypeConfig> =
                serde_json::from_slice(&value).map_err(read_error)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogReader<TypeConfig> for DurableLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<TypeConfig as RaftTypeConfig>::Entry>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        inner.read_entries(range)
    }
}

impl RaftLogStorage<TypeConfig> for DurableLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let last_purged = inner.last_purged()?;
        let last_log = inner.last_log_id()?.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last_log,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let value = serde_json::to_vec(&committed).map_err(write_error)?;
        let mut batch = WriteBatch::default();
        batch.put(COMMITTED_KEY, value);
        inner.write_batch(batch)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        match inner.db.get(COMMITTED_KEY).map_err(read_error)? {
            Some(value) => Ok(serde_json::from_slice(&value).map_err(read_error)?),
            None => Ok(None),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let value = serde_json::to_vec(vote).map_err(write_error)?;
        let mut batch = WriteBatch::default();
        batch.put(VOTE_KEY, value);
        inner.write_batch(batch)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        match inner.db.get(VOTE_KEY).map_err(read_error)? {
            Some(value) => Ok(Some(serde_json::from_slice(&value).map_err(read_error)?)),
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = <TypeConfig as RaftTypeConfig>::Entry>,
    {
        let inner = self.inner.lock().await;
        let mut batch = WriteBatch::default();
        for entry in entries {
            let key = log_key(entry.log_id.index);
            let value = serde_json::to_vec(&entry).map_err(write_error)?;
            batch.put(key, value);
        }
        inner.write_batch(batch)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let mut batch = WriteBatch::default();
        batch.delete_range(log_key(log_id.index), LOG_PREFIX_END.to_vec());
        inner.write_batch(batch)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let end = log_key(log_id.index.saturating_add(1));
        let value = serde_json::to_vec(&log_id).map_err(write_error)?;
        let mut batch = WriteBatch::default();
        batch.delete_range(LOG_PREFIX.to_vec(), end);
        batch.put(LAST_PURGED_KEY, value);
        inner.write_batch(batch)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
