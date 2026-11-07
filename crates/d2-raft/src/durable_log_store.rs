//! Durable Raft log storage.
//!
//! Raft log entries and vote/commit metadata live in a dedicated RocksDB
//! directory, separate from the state machine. Every append, truncate, purge
//! and vote write uses a synchronous write so acknowledged Raft state survives
//! process restart.

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    LogId, LogState, RaftLogReader, RaftTypeConfig, StorageError, StorageIOError, Vote,
};
use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use tokio::sync::Mutex;

use crate::blocking::blocking_section;
use crate::types::{NodeId, TypeConfig};

// Soft encoded-byte cap: an oversized first entry remains indivisible.
const CATCHUP_MAX_ENTRIES: usize = 256;
const CATCHUP_MAX_BYTES: usize = 1024 * 1024;

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

    /// Copy one bounded prefix under the lock, but leave JSON decoding to the
    /// caller after unlocking. Raw iteration avoids copying a rejected next value.
    fn read_catchup_bytes(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Vec<u8>>, StorageError<NodeId>> {
        let mut entries = Vec::new();
        if start >= end {
            return Ok(entries);
        }
        let mut bytes = 0_usize;
        let mut iter = self.db.raw_iterator();
        let end_key = log_key(end);
        iter.seek(log_key(start));
        while iter.valid() {
            let key = iter.key().expect("valid iterator has key");
            if key >= end_key.as_slice() {
                break;
            }
            let Some(index) = log_index(key) else {
                if !key.starts_with(LOG_PREFIX) {
                    break;
                }
                return Err(read_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid Raft log key",
                )));
            };
            if index >= end {
                break;
            }
            let value = iter.value().expect("valid iterator has value");
            // Always make progress with the first complete available entry.
            // Subtraction avoids overflow, even for an oversized stored value.
            if !entries.is_empty() && value.len() > CATCHUP_MAX_BYTES - bytes {
                break;
            }
            bytes = bytes.saturating_add(value.len());
            entries.push(value.to_vec());
            if entries.len() == CATCHUP_MAX_ENTRIES || bytes >= CATCHUP_MAX_BYTES {
                break;
            }
            iter.next();
        }
        iter.status().map_err(read_error)?;
        Ok(entries)
    }

    fn read_entries<RB: RangeBounds<u64>>(
        &self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => match index.checked_add(1) {
                Some(next) => next,
                None => return Ok(Vec::new()),
            },
            Bound::Unbounded => 0,
        };
        let beyond_end = |index: u64| match range.end_bound() {
            Bound::Included(end) => index > *end,
            Bound::Excluded(end) => index >= *end,
            Bound::Unbounded => false,
        };
        if beyond_end(start) {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        let start_key = log_key(start);
        let mut iter = self
            .db
            .iterator(IteratorMode::From(&start_key, Direction::Forward));
        for item in &mut iter {
            let (key, value) = item.map_err(read_error)?;
            let Some(index) = log_index(&key) else {
                if !key.starts_with(LOG_PREFIX) {
                    break;
                }
                continue;
            };
            if beyond_end(index) {
                break;
            }
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
        blocking_section(|| inner.read_entries(range))
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<<TypeConfig as RaftTypeConfig>::Entry>, StorageError<NodeId>> {
        let encoded = {
            let inner = self.inner.lock().await;
            blocking_section(|| inner.read_catchup_bytes(start, end))?
        };
        // Entries are owned, immutable copies of the selected log prefix. The
        // snapshot boundary is the locked read above; decoding needs no lock.
        blocking_section(|| {
            encoded
                .into_iter()
                .map(|value| serde_json::from_slice(&value).map_err(read_error))
                .collect()
        })
    }
}

impl RaftLogStorage<TypeConfig> for DurableLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        blocking_section(|| {
            let last_purged = inner.last_purged()?;
            let last_log = inner.last_log_id()?.or(last_purged);
            Ok(LogState {
                last_purged_log_id: last_purged,
                last_log_id: last_log,
            })
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        blocking_section(|| {
            let value = serde_json::to_vec(&committed).map_err(write_error)?;
            let mut batch = WriteBatch::default();
            batch.put(COMMITTED_KEY, value);
            inner.write_batch(batch)
        })
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        blocking_section(|| match inner.db.get(COMMITTED_KEY).map_err(read_error)? {
            Some(value) => Ok(serde_json::from_slice(&value).map_err(read_error)?),
            None => Ok(None),
        })
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        blocking_section(|| {
            let value = serde_json::to_vec(vote).map_err(write_error)?;
            let mut batch = WriteBatch::default();
            batch.put(VOTE_KEY, value);
            inner.write_batch(batch)
        })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        blocking_section(|| match inner.db.get(VOTE_KEY).map_err(read_error)? {
            Some(value) => Ok(Some(serde_json::from_slice(&value).map_err(read_error)?)),
            None => Ok(None),
        })
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
        blocking_section(|| {
            let mut batch = WriteBatch::default();
            for entry in entries {
                let key = log_key(entry.log_id.index);
                let value = serde_json::to_vec(&entry).map_err(write_error)?;
                batch.put(key, value);
            }
            inner.write_batch(batch)?;
            callback.log_io_completed(Ok(()));
            Ok(())
        })
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        blocking_section(|| {
            let mut batch = WriteBatch::default();
            batch.delete_range(log_key(log_id.index), LOG_PREFIX_END.to_vec());
            inner.write_batch(batch)
        })
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        blocking_section(|| {
            let end = log_key(log_id.index.saturating_add(1));
            let value = serde_json::to_vec(&log_id).map_err(write_error)?;
            let mut batch = WriteBatch::default();
            batch.delete_range(LOG_PREFIX.to_vec(), end);
            batch.put(LAST_PURGED_KEY, value);
            inner.write_batch(batch)
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_log_reads_seek_and_stop_without_decoding_unrelated_values() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let inner = DurableLogStoreInner {
            db: DB::open(&options, directory.path()).unwrap(),
        };
        // Malformed adjacent rows make accidental out-of-range decoding observable.
        inner.db.put(log_key(0), b"invalid").unwrap();
        inner.db.put(log_key(4), b"invalid").unwrap();
        for index in [1, 2, 3, u64::MAX] {
            let entry = openraft::Entry::<TypeConfig> {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
                payload: openraft::EntryPayload::Blank,
            };
            inner
                .db
                .put(log_key(index), serde_json::to_vec(&entry).unwrap())
                .unwrap();
        }
        for (bounds, expected) in [
            ((Bound::Included(1), Bound::Included(3)), vec![1, 2, 3]),
            ((Bound::Excluded(1), Bound::Excluded(3)), vec![2]),
            ((Bound::Included(2), Bound::Excluded(2)), vec![]),
            ((Bound::Included(3), Bound::Included(1)), vec![]),
            ((Bound::Excluded(u64::MAX), Bound::Unbounded), vec![]),
            (
                (Bound::Included(u64::MAX), Bound::Unbounded),
                vec![u64::MAX],
            ),
            ((Bound::Unbounded, Bound::Excluded(0)), vec![]),
        ] {
            let entries = inner.read_entries(bounds).unwrap();
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| entry.log_id.index)
                    .collect::<Vec<_>>(),
                expected
            );
        }
        inner.db.delete(log_key(0)).unwrap();
        inner.db.delete(log_key(4)).unwrap();
        assert_eq!(inner.read_entries(..).unwrap().len(), 4);
        assert_eq!(inner.read_entries(..=2).unwrap().len(), 2);
    }

    fn blank(index: u64) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: openraft::EntryPayload::Blank,
        }
    }

    async fn put_entry(store: &DurableLogStore, index: u64, bytes: &[u8]) {
        store
            .inner
            .lock()
            .await
            .db
            .put(log_key(index), bytes)
            .unwrap();
    }

    #[tokio::test]
    async fn catchup_range_boundaries_and_unrestricted_reads() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = DurableLogStore::open(directory.path()).unwrap();
        for index in [0, 1, 2, u64::MAX - 1, u64::MAX] {
            put_entry(&store, index, &serde_json::to_vec(&blank(index)).unwrap()).await;
        }
        for (start, end, expected) in [
            (0, 3, vec![0, 1, 2]),
            (1, 2, vec![1]),
            (2, 2, vec![]),
            (3, 1, vec![]),
            (u64::MAX - 1, u64::MAX, vec![u64::MAX - 1]),
            (u64::MAX, u64::MAX, vec![]),
        ] {
            let entries = store.limited_get_log_entries(start, end).await.unwrap();
            assert_eq!(
                entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
                expected
            );
        }
        assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn catchup_byte_cap_exact_boundary_and_oversized_entry_progress() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = DurableLogStore::open(directory.path()).unwrap();
        for index in 0..3 {
            let mut encoded = serde_json::to_vec(&blank(index)).unwrap();
            encoded.resize(CATCHUP_MAX_BYTES / 2, b' ');
            put_entry(&store, index, &encoded).await;
        }
        assert_eq!(store.limited_get_log_entries(0, 3).await.unwrap().len(), 2);
        // A malformed next entry is never decoded after an exact-full batch.
        put_entry(&store, 2, b"invalid").await;
        assert_eq!(store.limited_get_log_entries(0, 3).await.unwrap().len(), 2);
        assert!(store.limited_get_log_entries(2, 3).await.is_err());
        assert!(store.try_get_log_entries(0..3).await.is_err());
        // One legal complete entry can exceed the soft encoded-byte cap. It
        // must survive intact, never be split, dropped or returned as empty.
        let mut large = blank(0);
        large.payload = openraft::EntryPayload::Normal(crate::LogCommand::put(
            b"large",
            vec![255; CATCHUP_MAX_BYTES + 1],
        ));
        let encoded = serde_json::to_vec(&large).unwrap();
        assert!(encoded.len() > CATCHUP_MAX_BYTES);
        put_entry(&store, 0, &encoded).await;
        let entries = store.limited_get_log_entries(0, 3).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(serde_json::to_vec(&entries[0]).unwrap(), encoded);
        // Stop before copying/decoding a next entry that would exceed the cap.
        let mut first = serde_json::to_vec(&blank(0)).unwrap();
        first.resize(CATCHUP_MAX_BYTES / 2 + 1, b' ');
        put_entry(&store, 0, &first).await;
        assert_eq!(store.limited_get_log_entries(0, 2).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn catchup_small_entry_backlog_batches_and_reopens_without_loss() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLogStore::open(directory.path()).unwrap();
        let total = CATCHUP_MAX_ENTRIES * 3 + 7;
        for index in 0..total as u64 {
            put_entry(&store, index, &serde_json::to_vec(&blank(index)).unwrap()).await;
        }
        drop(store);
        let mut store = DurableLogStore::open(directory.path()).unwrap();
        let mut start = 0;
        let mut batches = Vec::new();
        while start < total as u64 {
            let entries = store
                .limited_get_log_entries(start, total as u64)
                .await
                .unwrap();
            assert!(!entries.is_empty());
            for (offset, entry) in entries.iter().enumerate() {
                assert_eq!(entry.log_id.index, start + offset as u64);
            }
            batches.push(entries.len());
            start += entries.len() as u64;
        }
        assert_eq!(
            batches,
            vec![
                CATCHUP_MAX_ENTRIES,
                CATCHUP_MAX_ENTRIES,
                CATCHUP_MAX_ENTRIES,
                7
            ]
        );
        assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), total);
    }

    #[tokio::test]
    async fn catchup_owned_prefix_survives_later_log_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLogStore::open(directory.path()).unwrap();
        let encoded = serde_json::to_vec(&blank(1)).unwrap();
        put_entry(&store, 1, &encoded).await;
        let copied = store.inner.lock().await.read_catchup_bytes(1, 2).unwrap();
        put_entry(&store, 1, b"invalid replacement").await;
        assert_eq!(copied, vec![encoded]);
        let entry: openraft::Entry<TypeConfig> = serde_json::from_slice(&copied[0]).unwrap();
        assert_eq!(entry.log_id.index, 1);
    }

    #[tokio::test]
    async fn catchup_malformed_log_key_fails_instead_of_unbounded_skipping() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = DurableLogStore::open(directory.path()).unwrap();
        let mut invalid_key = log_key(0);
        invalid_key.push(0);
        store
            .inner
            .lock()
            .await
            .db
            .put(invalid_key, b"invalid")
            .unwrap();
        assert!(store.limited_get_log_entries(0, 1).await.is_err());
        let encoded = serde_json::to_vec(&blank(1)).unwrap();
        put_entry(&store, 1, &encoded).await;
        let mut outside = log_key(2);
        outside.push(0);
        store
            .inner
            .lock()
            .await
            .db
            .put(outside, b"invalid")
            .unwrap();
        assert_eq!(store.limited_get_log_entries(1, 2).await.unwrap().len(), 1);
    }
}
