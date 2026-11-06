//! D1 local storage-node adapter v0.
//!
//! This is the byte-oriented boundary from [`docs/STORAGE_NODE_INTERFACE.md`]:
//! the node owns one shard and one opaque key space. It knows nothing about
//! vertices, edges, or the `graph/v1/...` prefix.
//!
//! The RocksDB database has two column families: `data` for caller keys and
//! `meta` for the epoch, applied sequence, and idempotency records. A write
//! batch updates the data, sequence, and dedup record in one atomic RocksDB
//! write, so a retry cannot observe a half-applied batch.
//!
//! The local adapter copies the data partition when [`LocalStorageNode::read_view`]
//! is called so the returned view is stable across concurrent writes. That is
//! deliberately a prototype trade-off: it keeps snapshot semantics correct
//! without exposing a borrowed RocksDB `Snapshot` through the byte-oriented
//! interface, but it is bounded by [`MAX_VIEW_BYTES`] and
//! [`MAX_VIEW_ROWS`], and a production adapter must pin a real snapshot instead.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use rocksdb::{
    ColumnFamily, DBCompressionType, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const INTERFACE_VERSION: u32 = 0;

pub type ShardId = u64;
pub type Epoch = u64;
pub type Sequence = u64;
pub type RequestId = [u8; 16];

const DATA_CF: &str = "data";
const META_CF: &str = "meta";
const META_EPOCH: &[u8] = b"epoch";
const META_SEQUENCE: &[u8] = b"sequence";
const META_SHARD: &[u8] = b"shard";
const DEDUP_PREFIX: &[u8] = b"dedup/";
const DEDUP_PREFIX_END: &[u8] = b"dedup0";

/// A bounded read view of one shard at a logical sequence.
#[derive(Clone)]
pub struct ReadView {
    pub shard: ShardId,
    pub epoch: Epoch,
    pub sequence: Sequence,
    data: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl fmt::Debug for ReadView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadView")
            .field("shard", &self.shard)
            .field("epoch", &self.epoch)
            .field("sequence", &self.sequence)
            .field("rows", &self.data.len())
            .finish()
    }
}

/// A half-open byte range. `None` for `end` means unbounded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Result<Self> {
        let range = Self {
            start: start.into(),
            end: Some(end.into()),
        };
        range.validate()?;
        Ok(range)
    }

    pub fn all() -> Self {
        Self {
            start: Vec::new(),
            end: None,
        }
    }

    fn validate(&self) -> Result<()> {
        if let Some(end) = &self.end {
            if end < &self.start {
                return Err(StorageNodeError::InvalidArgument(
                    "scan end must not be before start".into(),
                ));
            }
        }
        Ok(())
    }

    fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        match &self.end {
            Some(end) => (
                Bound::Included(self.start.as_slice()),
                Bound::Excluded(end.as_slice()),
            ),
            None => (Bound::Included(self.start.as_slice()), Bound::Unbounded),
        }
    }
}

/// Minimum/maximum row and byte limits for one scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanLimit {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl ScanLimit {
    pub fn new(max_rows: usize, max_bytes: usize) -> Result<Self> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(StorageNodeError::InvalidArgument(
                "scan limits must be non-zero".into(),
            ));
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }
}

impl Default for ScanLimit {
    fn default() -> Self {
        Self {
            max_rows: 1_000,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WriteOp {
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self::Delete { key: key.into() }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteRequest {
    pub shard: ShardId,
    pub epoch: Epoch,
    pub request_id: RequestId,
    pub operations: Vec<WriteOp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteStatus {
    Committed,
    Duplicate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteResult {
    pub sequence: Sequence,
    pub status: WriteStatus,
    pub applied_operations: usize,
}

/// A complete export of one local shard. Dedup records are not part of the
/// export; a fresh target has no prior requests to deduplicate.
#[derive(Clone, PartialEq, Eq)]
pub struct Export {
    pub shard: ShardId,
    pub epoch: Epoch,
    pub sequence: Sequence,
    pub entries: Vec<KeyValue>,
}

impl fmt::Debug for Export {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Export")
            .field("shard", &self.shard)
            .field("epoch", &self.epoch)
            .field("sequence", &self.sequence)
            .field("entries", &self.entries.len())
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum StorageNodeError {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("shard {shard} epoch {epoch} is fenced; current epoch is {current}")]
    Fenced {
        shard: ShardId,
        epoch: Epoch,
        current: Epoch,
    },

    /// The shard changed after the caller's read view. Re-read before retrying.
    #[error("write conflict: expected sequence {expected}, current sequence is {current}")]
    Conflict {
        expected: Sequence,
        current: Sequence,
    },

    #[error("idempotency conflict for request {0:?}")]
    IdempotencyConflict(RequestId),

    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("corrupt storage: {0}")]
    Corrupt(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl From<rocksdb::Error> for StorageNodeError {
    fn from(error: rocksdb::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StorageNodeError>;

/// The local adapter's small administrative surface.
pub trait StorageAdmin {
    fn export(&self) -> Result<Export>;
    fn import(&self, export: Export) -> Result<()>;
    fn set_epoch(&self, epoch: Epoch) -> Result<()>;
}

/// The D1 byte-oriented boundary.
pub trait StorageNode: Send + Sync {
    fn interface_version(&self) -> u32 {
        INTERFACE_VERSION
    }

    fn read_view(&self) -> Result<ReadView>;
    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>>;
    fn scan(&self, range: KeyRange, limit: ScanLimit, view: &ReadView) -> Result<Vec<KeyValue>>;
    fn batch_write(&self, request: WriteRequest) -> Result<WriteResult>;

    /// Atomically commit only if the shard still has `expected_sequence`.
    ///
    /// `None` preserves unconditional writes. Implementations must reject
    /// unsupported conditions rather than silently dropping them. After epoch
    /// validation, an identical committed request is returned as `Duplicate`
    /// before checking the sequence; changing the condition for an existing
    /// request id is an `IdempotencyConflict`. A conflict writes nothing.
    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        expected_sequence: Option<Sequence>,
    ) -> Result<WriteResult> {
        if expected_sequence.is_some() {
            return Err(StorageNodeError::Unsupported(
                "conditional batch writes".into(),
            ));
        }
        self.batch_write(request)
    }
    fn admin(&self) -> &dyn StorageAdmin;
}

#[derive(Serialize, Deserialize)]
struct DedupRecord {
    // Absent in v0 records. Omitting None preserves legacy metadata bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_sequence: Option<Sequence>,
    operations: Vec<u8>,
    sequence: Sequence,
    applied_operations: usize,
}

const MAX_BATCH_OPERATIONS: usize = 10_000;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_VIEW_ROWS: usize = 1_000_000;
const MAX_VIEW_BYTES: usize = 64 * 1024 * 1024;

/// A single-shard local RocksDB implementation of [`StorageNode`].
pub struct LocalStorageNode {
    shard: ShardId,
    db: DB,
    write_lock: Mutex<()>,
}

impl LocalStorageNode {
    /// Open or create a local single-shard node at `path`.
    pub fn open(path: impl AsRef<Path>, shard: ShardId) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_compression_type(DBCompressionType::None);
        let db = DB::open_cf(&options, path, [DATA_CF, META_CF])?;
        let node = Self {
            shard,
            db,
            write_lock: Mutex::new(()),
        };

        let has_data = node.has_data()?;
        let mut batch = WriteBatch::default();
        let mut initialized = false;
        match node.get_meta(META_SHARD)? {
            Some(existing) if existing != shard => {
                return Err(StorageNodeError::Corrupt(format!(
                    "database belongs to shard {existing}, not requested shard {shard}"
                )));
            }
            Some(_) => {}
            None if has_data => {
                return Err(StorageNodeError::Corrupt(
                    "non-empty database has no shard identity".into(),
                ));
            }
            None => {
                batch.put_cf(node.meta_cf(), META_SHARD, shard.to_be_bytes());
                initialized = true;
            }
        }
        if node.get_meta(META_EPOCH)?.is_none() {
            if has_data {
                return Err(StorageNodeError::Corrupt(
                    "non-empty database has no epoch metadata".into(),
                ));
            }
            batch.put_cf(node.meta_cf(), META_EPOCH, 0_u64.to_be_bytes());
            initialized = true;
        }
        if node.get_meta(META_SEQUENCE)?.is_none() {
            if has_data {
                return Err(StorageNodeError::Corrupt(
                    "non-empty database has no sequence metadata".into(),
                ));
            }
            batch.put_cf(node.meta_cf(), META_SEQUENCE, 0_u64.to_be_bytes());
            initialized = true;
        }
        if initialized {
            node.write_batch(batch)?;
        }
        Ok(node)
    }

    fn lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.write_lock
            .lock()
            .map_err(|_| StorageNodeError::Corrupt("storage write lock poisoned".into()))
    }

    fn data_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(DATA_CF)
            .expect("data column family opened at startup")
    }

    fn meta_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(META_CF)
            .expect("meta column family opened at startup")
    }

    fn has_data(&self) -> Result<bool> {
        Ok(self
            .db
            .iterator_cf(self.data_cf(), IteratorMode::Start)
            .next()
            .is_some())
    }

    fn get_meta(&self, key: &[u8]) -> Result<Option<u64>> {
        match self.db.get_cf(self.meta_cf(), key)? {
            Some(bytes) => {
                let bytes: [u8; 8] = bytes.as_slice().try_into().map_err(|_| {
                    StorageNodeError::Corrupt(format!("metadata {key:?} is not 8 bytes"))
                })?;
                Ok(Some(u64::from_be_bytes(bytes)))
            }
            None => Ok(None),
        }
    }

    fn epoch(&self) -> Result<Epoch> {
        self.get_meta(META_EPOCH)?
            .ok_or_else(|| StorageNodeError::Corrupt("missing epoch metadata".into()))
    }

    fn sequence(&self) -> Result<Sequence> {
        self.get_meta(META_SEQUENCE)?
            .ok_or_else(|| StorageNodeError::Corrupt("missing sequence metadata".into()))
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db.write_opt(batch, &options)?;
        Ok(())
    }

    fn load_view(&self) -> Result<ReadView> {
        let epoch = self.epoch()?;
        let sequence = self.sequence()?;
        let mut data = BTreeMap::new();
        let mut bytes = 0_usize;
        for item in self.db.iterator_cf(self.data_cf(), IteratorMode::Start) {
            let (key, value) = item?;
            if data.len() >= MAX_VIEW_ROWS {
                return Err(StorageNodeError::ResourceExhausted(format!(
                    "read view exceeds {MAX_VIEW_ROWS} rows"
                )));
            }
            bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
            if bytes > MAX_VIEW_BYTES {
                return Err(StorageNodeError::ResourceExhausted(format!(
                    "read view exceeds {MAX_VIEW_BYTES} bytes"
                )));
            }
            data.insert(key.to_vec(), value.to_vec());
        }
        Ok(ReadView {
            shard: self.shard,
            epoch,
            sequence,
            data: Arc::new(data),
        })
    }

    fn check_view(&self, view: &ReadView) -> Result<()> {
        if view.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(format!(
                "view shard {} does not match local shard {}",
                view.shard, self.shard
            )));
        }
        let current = self.epoch()?;
        if view.epoch != current {
            return Err(StorageNodeError::Fenced {
                shard: self.shard,
                epoch: view.epoch,
                current,
            });
        }
        Ok(())
    }

    fn validate_write(&self, request: &WriteRequest) -> Result<Vec<u8>> {
        if request.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(format!(
                "request shard {} does not match local shard {}",
                request.shard, self.shard
            )));
        }
        let current = self.epoch()?;
        if request.epoch != current {
            return Err(StorageNodeError::Fenced {
                shard: request.shard,
                epoch: request.epoch,
                current,
            });
        }
        if request.operations.is_empty() {
            return Err(StorageNodeError::InvalidArgument(
                "write batch must not be empty".into(),
            ));
        }
        if request.operations.len() > MAX_BATCH_OPERATIONS {
            return Err(StorageNodeError::ResourceExhausted(format!(
                "write batch exceeds {MAX_BATCH_OPERATIONS} operations"
            )));
        }
        let mut bytes = 0_usize;
        for operation in &request.operations {
            bytes = bytes.saturating_add(operation.key().len());
            if let WriteOp::Put { value, .. } = operation {
                bytes = bytes.saturating_add(value.len());
            }
        }
        if bytes > MAX_BATCH_BYTES {
            return Err(StorageNodeError::ResourceExhausted(format!(
                "write batch exceeds {MAX_BATCH_BYTES} bytes"
            )));
        }
        serde_json::to_vec(&request.operations)
            .map_err(|error| StorageNodeError::Corrupt(error.to_string()))
    }

    fn dedup_key(request_id: &RequestId) -> Vec<u8> {
        let mut key = Vec::with_capacity(DEDUP_PREFIX.len() + request_id.len());
        key.extend_from_slice(DEDUP_PREFIX);
        key.extend_from_slice(request_id);
        key
    }

    fn decode_dedup(bytes: &[u8]) -> Result<DedupRecord> {
        serde_json::from_slice(bytes)
            .map_err(|error| StorageNodeError::Corrupt(format!("dedup record: {error}")))
    }
}

impl StorageNode for LocalStorageNode {
    fn read_view(&self) -> Result<ReadView> {
        let _guard = self.lock()?;
        self.load_view()
    }

    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>> {
        self.check_view(view)?;
        Ok(view.data.get(key).cloned())
    }

    fn scan(&self, range: KeyRange, limit: ScanLimit, view: &ReadView) -> Result<Vec<KeyValue>> {
        range.validate()?;
        if limit.max_rows == 0 || limit.max_bytes == 0 {
            return Err(StorageNodeError::InvalidArgument(
                "scan limits must be non-zero".into(),
            ));
        }
        self.check_view(view)?;
        let (start, end) = range.bounds();
        let mut out = Vec::new();
        let mut bytes = 0_usize;
        for (key, value) in view.data.range::<[u8], _>((start, end)) {
            if out.len() >= limit.max_rows {
                return Err(StorageNodeError::ResourceExhausted(
                    "scan row limit reached".into(),
                ));
            }
            let entry_bytes = key.len().saturating_add(value.len());
            if bytes.saturating_add(entry_bytes) > limit.max_bytes {
                return Err(StorageNodeError::ResourceExhausted(
                    "scan byte limit reached".into(),
                ));
            }
            bytes += entry_bytes;
            out.push(KeyValue {
                key: key.clone(),
                value: value.clone(),
            });
        }
        Ok(out)
    }

    fn batch_write(&self, request: WriteRequest) -> Result<WriteResult> {
        self.batch_write_conditional(request, None)
    }

    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        expected_sequence: Option<Sequence>,
    ) -> Result<WriteResult> {
        let canonical = self.validate_write(&request)?;
        let _guard = self.lock()?;
        let current_epoch = self.epoch()?;
        if request.epoch != current_epoch {
            return Err(StorageNodeError::Fenced {
                shard: self.shard,
                epoch: request.epoch,
                current: current_epoch,
            });
        }
        let dedup_key = Self::dedup_key(&request.request_id);
        if let Some(bytes) = self.db.get_cf(self.meta_cf(), &dedup_key)? {
            let record = Self::decode_dedup(&bytes)?;
            if record.operations != canonical || record.expected_sequence != expected_sequence {
                return Err(StorageNodeError::IdempotencyConflict(request.request_id));
            }
            return Ok(WriteResult {
                sequence: record.sequence,
                status: WriteStatus::Duplicate,
                applied_operations: record.applied_operations,
            });
        }

        let current = self.sequence()?;
        if let Some(expected) = expected_sequence {
            if expected != current {
                return Err(StorageNodeError::Conflict { expected, current });
            }
        }
        let sequence = current
            .checked_add(1)
            .ok_or_else(|| StorageNodeError::ResourceExhausted("sequence overflow".into()))?;
        let dedup = DedupRecord {
            expected_sequence,
            operations: canonical,
            sequence,
            applied_operations: request.operations.len(),
        };
        let dedup_bytes = serde_json::to_vec(&dedup)
            .map_err(|error| StorageNodeError::Corrupt(error.to_string()))?;
        let mut batch = WriteBatch::default();
        for operation in &request.operations {
            match operation {
                WriteOp::Put { key, value } => {
                    batch.put_cf(self.data_cf(), key, value);
                }
                WriteOp::Delete { key } => {
                    batch.delete_cf(self.data_cf(), key);
                }
            }
        }
        batch.put_cf(self.meta_cf(), META_SEQUENCE, sequence.to_be_bytes());
        batch.put_cf(self.meta_cf(), &dedup_key, dedup_bytes);
        self.write_batch(batch)?;

        Ok(WriteResult {
            sequence,
            status: WriteStatus::Committed,
            applied_operations: request.operations.len(),
        })
    }

    fn admin(&self) -> &dyn StorageAdmin {
        self
    }
}

impl StorageAdmin for LocalStorageNode {
    fn export(&self) -> Result<Export> {
        let _guard = self.lock()?;
        let epoch = self.epoch()?;
        let sequence = self.sequence()?;
        let mut entries = Vec::new();
        for item in self.db.iterator_cf(self.data_cf(), IteratorMode::Start) {
            let (key, value) = item?;
            entries.push(KeyValue {
                key: key.to_vec(),
                value: value.to_vec(),
            });
        }
        Ok(Export {
            shard: self.shard,
            epoch,
            sequence,
            entries,
        })
    }

    fn import(&self, export: Export) -> Result<()> {
        if export.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(format!(
                "import shard {} does not match local shard {}",
                export.shard, self.shard
            )));
        }
        if !export.entries.is_empty() && export.sequence == 0 {
            return Err(StorageNodeError::InvalidArgument(
                "entries require a positive sequence".into(),
            ));
        }
        let mut previous: Option<&[u8]> = None;
        let mut bytes = 0_usize;
        for entry in &export.entries {
            if previous.is_some_and(|previous| entry.key.as_slice() <= previous) {
                return Err(StorageNodeError::InvalidArgument(
                    "import entries must have strictly increasing keys".into(),
                ));
            }
            previous = Some(&entry.key);
            bytes = bytes.saturating_add(entry.key.len().saturating_add(entry.value.len()));
        }
        if export.entries.len() > MAX_VIEW_ROWS || bytes > MAX_VIEW_BYTES {
            return Err(StorageNodeError::ResourceExhausted(
                "import exceeds the local read-view limit".into(),
            ));
        }

        let _guard = self.lock()?;
        if self
            .db
            .iterator_cf(self.data_cf(), IteratorMode::Start)
            .next()
            .is_some()
        {
            return Err(StorageNodeError::InvalidArgument(
                "import target must be empty".into(),
            ));
        }
        let current_epoch = self.epoch()?;
        if export.epoch < current_epoch {
            return Err(StorageNodeError::InvalidArgument(
                "import epoch must not move backwards".into(),
            ));
        }
        if self.sequence()? != 0 {
            return Err(StorageNodeError::InvalidArgument(
                "import target must be pristine: sequence is not zero".into(),
            ));
        }

        let mut batch = WriteBatch::default();
        for entry in &export.entries {
            batch.put_cf(self.data_cf(), &entry.key, &entry.value);
        }
        batch.delete_range_cf(self.meta_cf(), DEDUP_PREFIX, DEDUP_PREFIX_END);
        batch.put_cf(self.meta_cf(), META_EPOCH, export.epoch.to_be_bytes());
        batch.put_cf(self.meta_cf(), META_SEQUENCE, export.sequence.to_be_bytes());
        self.write_batch(batch)
    }

    fn set_epoch(&self, epoch: Epoch) -> Result<()> {
        let _guard = self.lock()?;
        let current = self.epoch()?;
        if epoch < current {
            return Err(StorageNodeError::InvalidArgument(format!(
                "epoch must not decrease from {current} to {epoch}"
            )));
        }
        let mut batch = WriteBatch::default();
        batch.put_cf(self.meta_cf(), META_EPOCH, epoch.to_be_bytes());
        self.write_batch(batch)
    }
}
