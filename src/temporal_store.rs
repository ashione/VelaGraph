//! Experimental single-partition durable temporal assertions (#196).
//! This is storage plumbing, not a query language or certified distributed V.
use std::path::Path;
use std::sync::Mutex;

use rocksdb::{Direction, IteratorMode, Options, Snapshot, WriteBatch, WriteOptions, DB};

use crate::graph::{codec, validate_edge, validate_vertex};
use crate::temporal_reference::{Interval, Key};
use crate::{Edge, Vertex};

const FORMAT_KEY: &[u8] = b"graph/temporal/v1/format";
const HEAD_KEY: &[u8] = b"graph/temporal/v1/head";
const RECORD_PREFIX: &[u8] = b"graph/temporal/v1/revision/";
const MAX_BATCH: usize = 256;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_READ_REVISIONS: usize = 4096;
const MAX_READ_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum TemporalStoreError {
    #[error("storage error: {0}")]
    Storage(#[from] rocksdb::Error),
    #[error("graph record error: {0}")]
    Graph(#[from] crate::Error),
    #[error("invalid valid-time interval")]
    InvalidInterval,
    #[error("overlapping changes for one identity in a batch")]
    AmbiguousBatch,
    #[error("temporal capacity limit")]
    Limit,
    #[error("requested local system version is unavailable")]
    UnavailableVersion,
    #[error("database is not a temporal v1 store")]
    IncompatibleStore,
    #[error("corrupt temporal record or metadata")]
    Corrupt,
}
pub type Result<T> = std::result::Result<T, TemporalStoreError>;

/// An assertion replaces the selected logical record only over `valid`.
/// Absence does not purge old revisions or alter v1 permanent tombstones.
#[derive(Clone, Debug)]
pub enum TemporalMutation {
    Vertex { valid: Interval, vertex: Vertex },
    Edge { valid: Interval, edge: Edge },
    Absent { valid: Interval, key: Key },
}
impl TemporalMutation {
    fn key(&self) -> Key {
        match self {
            Self::Vertex { vertex, .. } => Key::Vertex(vertex.id),
            Self::Edge { edge, .. } => Key::Edge(edge.id),
            Self::Absent { key, .. } => *key,
        }
    }
    fn interval(&self) -> Interval {
        match self {
            Self::Vertex { valid, .. } | Self::Edge { valid, .. } | Self::Absent { valid, .. } => {
                *valid
            }
        }
    }
    fn encode(&self) -> Result<Vec<u8>> {
        let payload = match self {
            Self::Vertex { vertex, .. } => {
                validate_vertex(vertex)?;
                Some(codec::encode_vertex(vertex)?)
            }
            Self::Edge { edge, .. } => {
                validate_edge(edge)?;
                Some(codec::encode_edge(edge)?)
            }
            Self::Absent { .. } => None,
        };
        encode_assertion(self.interval(), payload.as_deref())
    }
}

/// Owns a dedicated RocksDB database. No v1/v2 namespace migration or reuse.
/// Local versions are consecutive synced batches, not Raft/cohort versions.
pub struct TemporalStore {
    db: DB,
    write: Mutex<()>,
}
impl TemporalStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path)?;
        match db.get(FORMAT_KEY)? {
            Some(value) if value == [1] => {
                decode_version(db.get(HEAD_KEY)?.as_deref())?;
            }
            Some(_) => return Err(TemporalStoreError::IncompatibleStore),
            None => {
                if let Some(first) = db.iterator(IteratorMode::Start).next() {
                    first?;
                    return Err(TemporalStoreError::IncompatibleStore);
                }
                let mut batch = WriteBatch::default();
                batch.put(FORMAT_KEY, [1]);
                batch.put(HEAD_KEY, 0u64.to_be_bytes());
                let mut write = WriteOptions::default();
                write.set_sync(true);
                db.write_opt(batch, &write)?;
            }
        }
        Ok(Self {
            db,
            write: Mutex::new(()),
        })
    }

    /// Commit one atomic, WAL-synced batch and return its local system version.
    /// An empty batch advances the local prefix without changing any assertion.
    /// An uncertain RocksDB error has no idempotent retry guarantee in this slice.
    pub fn commit(&self, mutations: &[TemporalMutation]) -> Result<u64> {
        if mutations.len() > MAX_BATCH {
            return Err(TemporalStoreError::Limit);
        }
        let mut encoded = Vec::with_capacity(mutations.len());
        let mut bytes = 0usize;
        for (i, mutation) in mutations.iter().enumerate() {
            let interval = mutation.interval();
            if interval.to.is_some_and(|to| to <= interval.from) {
                return Err(TemporalStoreError::InvalidInterval);
            }
            for prior in &mutations[..i] {
                let p = prior.interval();
                if prior.key() == mutation.key()
                    && p.to.is_none_or(|to| interval.from < to)
                    && interval.to.is_none_or(|to| p.from < to)
                {
                    return Err(TemporalStoreError::AmbiguousBatch);
                }
            }
            let value = mutation.encode()?;
            bytes = bytes
                .checked_add(value.len() + RECORD_PREFIX.len() + 21)
                .ok_or(TemporalStoreError::Limit)?;
            if bytes > MAX_BATCH_BYTES {
                return Err(TemporalStoreError::Limit);
            }
            encoded.push(value);
        }
        let _guard = self.write.lock().map_err(|_| TemporalStoreError::Corrupt)?;
        let head = decode_version(self.db.get(HEAD_KEY)?.as_deref())?;
        let version = head.checked_add(1).ok_or(TemporalStoreError::Limit)?;
        let mut batch = WriteBatch::default();
        for (i, (mutation, value)) in mutations.iter().zip(encoded).enumerate() {
            batch.put(revision_key(mutation.key(), version, i as u32), value);
        }
        batch.put(HEAD_KEY, version.to_be_bytes());
        let mut write = WriteOptions::default();
        write.set_sync(true);
        self.db.write_opt(batch, &write)?;
        Ok(version)
    }

    /// Pin RocksDB and both temporal coordinates once. None selects this local
    /// store's latest synced prefix, not a distributed latest/linearizable read.
    pub fn view(&self, version: Option<u64>, valid_time: i64) -> Result<TemporalStoreView<'_>> {
        let snapshot = self.db.snapshot();
        let head = decode_version(snapshot.get(HEAD_KEY)?.as_deref())?;
        let version = version.unwrap_or(head);
        if version > head {
            return Err(TemporalStoreError::UnavailableVersion);
        }
        Ok(TemporalStoreView {
            snapshot,
            version,
            valid_time,
        })
    }
}

/// Borrowed snapshot; historical lookup reads an identity's revision index,
/// not a full graph copy. All returned endpoints share this exact V/T.
pub struct TemporalStoreView<'a> {
    snapshot: Snapshot<'a>,
    version: u64,
    valid_time: i64,
}
impl TemporalStoreView<'_> {
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn valid_time(&self) -> i64 {
        self.valid_time
    }
    fn assertion(&self, key: Key) -> Result<Option<Vec<u8>>> {
        let prefix = identity_prefix(key);
        let start = revision_key(key, self.version, 0);
        let mut bytes = 0usize;
        for (i, row) in self
            .snapshot
            .iterator(IteratorMode::From(&start, Direction::Forward))
            .enumerate()
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            if i >= MAX_READ_REVISIONS || key.len() != RECORD_PREFIX.len() + 21 {
                return Err(if i >= MAX_READ_REVISIONS {
                    TemporalStoreError::Limit
                } else {
                    TemporalStoreError::Corrupt
                });
            }
            let stored_version = !u64::from_be_bytes(
                key[RECORD_PREFIX.len() + 9..RECORD_PREFIX.len() + 17]
                    .try_into()
                    .map_err(|_| TemporalStoreError::Corrupt)?,
            );
            let ordinal = u32::from_be_bytes(
                key[RECORD_PREFIX.len() + 17..]
                    .try_into()
                    .map_err(|_| TemporalStoreError::Corrupt)?,
            );
            if stored_version == 0
                || stored_version > self.version
                || ordinal as usize >= MAX_BATCH
                || value.len() > MAX_BATCH_BYTES
            {
                return Err(TemporalStoreError::Corrupt);
            }
            bytes = bytes
                .checked_add(key.len() + value.len())
                .ok_or(TemporalStoreError::Limit)?;
            if bytes > MAX_READ_BYTES {
                return Err(TemporalStoreError::Limit);
            }
            let (valid, payload) = decode_assertion(&value)?;
            if valid.from <= self.valid_time && valid.to.is_none_or(|to| self.valid_time < to) {
                return Ok(payload.map(|p| p.to_vec()));
            }
        }
        Ok(None)
    }
    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        let Some(payload) = self.assertion(Key::Vertex(id))? else {
            return Ok(None);
        };
        let vertex = codec::decode_vertex(&payload)?;
        if vertex.id != id {
            return Err(TemporalStoreError::Corrupt);
        }
        validate_vertex(&vertex)?;
        Ok(Some(vertex))
    }
    /// Assertions may be recorded before endpoints. Visibility requires both
    /// endpoints at V/T; this never weakens v1 create-edge validation.
    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        let Some(payload) = self.assertion(Key::Edge(id))? else {
            return Ok(None);
        };
        let edge = codec::decode_edge(&payload)?;
        if edge.id != id {
            return Err(TemporalStoreError::Corrupt);
        }
        validate_edge(&edge)?;
        if self.get_vertex(edge.from)?.is_none() || self.get_vertex(edge.to)?.is_none() {
            return Ok(None);
        }
        Ok(Some(edge))
    }
}
fn decode_version(value: Option<&[u8]>) -> Result<u64> {
    Ok(u64::from_be_bytes(
        value
            .ok_or(TemporalStoreError::Corrupt)?
            .try_into()
            .map_err(|_| TemporalStoreError::Corrupt)?,
    ))
}
fn identity_prefix(key: Key) -> Vec<u8> {
    let mut bytes = RECORD_PREFIX.to_vec();
    let (kind, id) = match key {
        Key::Vertex(id) => (1, id),
        Key::Edge(id) => (2, id),
    };
    bytes.push(kind);
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes
}
fn revision_key(key: Key, version: u64, ordinal: u32) -> Vec<u8> {
    let mut bytes = identity_prefix(key);
    bytes.extend_from_slice(&(!version).to_be_bytes());
    bytes.extend_from_slice(&ordinal.to_be_bytes());
    bytes
}
fn encode_assertion(valid: Interval, payload: Option<&[u8]>) -> Result<Vec<u8>> {
    if valid.to.is_some_and(|to| to <= valid.from) {
        return Err(TemporalStoreError::InvalidInterval);
    }
    let mut bytes = Vec::new();
    bytes.push(1);
    bytes.push(u8::from(valid.to.is_some()));
    bytes.extend_from_slice(&valid.from.to_be_bytes());
    bytes.extend_from_slice(&valid.to.unwrap_or(0).to_be_bytes());
    bytes.push(u8::from(payload.is_some()));
    if let Some(payload) = payload {
        bytes.extend_from_slice(payload)
    }
    Ok(bytes)
}
fn decode_assertion(bytes: &[u8]) -> Result<(Interval, Option<&[u8]>)> {
    if bytes.len() < 19 || bytes[0] != 1 || bytes[1] > 1 || bytes[18] > 1 {
        return Err(TemporalStoreError::Corrupt);
    }
    let from = i64::from_be_bytes(
        bytes[2..10]
            .try_into()
            .map_err(|_| TemporalStoreError::Corrupt)?,
    );
    let end = i64::from_be_bytes(
        bytes[10..18]
            .try_into()
            .map_err(|_| TemporalStoreError::Corrupt)?,
    );
    if bytes[1] == 0 && end != 0 {
        return Err(TemporalStoreError::Corrupt);
    }
    let to = if bytes[1] == 1 { Some(end) } else { None };
    if to.is_some_and(|to| to <= from)
        || (bytes[18] == 0 && bytes.len() != 19)
        || (bytes[18] == 1 && bytes.len() == 19)
    {
        return Err(TemporalStoreError::Corrupt);
    }
    Ok((
        Interval { from, to },
        if bytes[18] == 1 {
            Some(&bytes[19..])
        } else {
            None
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    fn interval(from: i64, to: Option<i64>) -> Interval {
        Interval { from, to }
    }
    fn vertex(id: u64, from: i64, to: Option<i64>, value: i64) -> TemporalMutation {
        let mut vertex = Vertex::new(id);
        vertex.labels.push("Node".into());
        vertex
            .properties
            .insert("value".into(), Value::Integer(value));
        TemporalMutation::Vertex {
            valid: interval(from, to),
            vertex,
        }
    }
    fn number(view: &TemporalStoreView<'_>, id: u64) -> Option<i64> {
        view.get_vertex(id)
            .unwrap()
            .map(|v| match v.properties["value"] {
                Value::Integer(n) => n,
                _ => panic!("wrong type"),
            })
    }
    #[test]
    fn golden_keys_and_envelopes() {
        let key = revision_key(Key::Vertex(7), 2, 3);
        let mut expected = b"graph/temporal/v1/revision/".to_vec();
        expected.extend([
            1, 0, 0, 0, 0, 0, 0, 0, 7, 255, 255, 255, 255, 255, 255, 255, 253, 0, 0, 0, 3,
        ]);
        assert_eq!(key, expected);
        let bytes = encode_assertion(interval(-2, Some(4)), None).unwrap();
        assert_eq!(
            bytes,
            vec![1, 1, 255, 255, 255, 255, 255, 255, 255, 254, 0, 0, 0, 0, 0, 0, 0, 4, 0]
        );
        assert_eq!(
            decode_assertion(&bytes).unwrap(),
            (interval(-2, Some(4)), None)
        );
        let vertex = Vertex::new(1);
        let payload = codec::encode_vertex(&vertex).unwrap();
        let bytes = encode_assertion(interval(0, None), Some(&payload)).unwrap();
        assert_eq!(
            &bytes[..19],
            &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        assert_eq!(
            decode_assertion(&bytes).unwrap().1,
            Some(payload.as_slice())
        );
        for invalid in [vec![], vec![1; 18], vec![2; 19], vec![1; 20]] {
            assert!(decode_assertion(&invalid).is_err());
        }
    }
    #[test]
    fn reopen_corrections_future_absence_and_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = TemporalStore::open(dir.path()).unwrap();
            assert_eq!(store.view(None, 0).unwrap().version(), 0);
            assert_eq!(
                store
                    .commit(&[
                        vertex(1, i64::MIN, None, 1),
                        vertex(2, 0, None, 2),
                        TemporalMutation::Edge {
                            valid: interval(0, None),
                            edge: Edge::new(1, 1, 2, "LINK")
                        }
                    ])
                    .unwrap(),
                1
            );
            let pinned = store.view(None, 7).unwrap();
            assert_eq!(
                store
                    .commit(&[vertex(1, 5, Some(10), 3), vertex(1, 20, None, 4)])
                    .unwrap(),
                2
            );
            assert_eq!(number(&pinned, 1), Some(1));
            assert_eq!(pinned.version(), 1);
            assert_eq!(number(&store.view(Some(2), 7).unwrap(), 1), Some(3));
            assert_eq!(number(&store.view(Some(2), 10).unwrap(), 1), Some(1));
            assert_eq!(number(&store.view(Some(2), 20).unwrap(), 1), Some(4));
            store
                .commit(&[TemporalMutation::Absent {
                    valid: interval(6, Some(9)),
                    key: Key::Vertex(2),
                }])
                .unwrap();
            assert!(store.view(None, 7).unwrap().get_edge(1).unwrap().is_none());
            assert!(pinned.get_edge(1).unwrap().is_some());
            assert!(store.view(None, 9).unwrap().get_edge(1).unwrap().is_some());
            assert_eq!(store.commit(&[]).unwrap(), 4);
        }
        let store = TemporalStore::open(dir.path()).unwrap();
        assert_eq!(store.view(None, 0).unwrap().version(), 4);
        for v in 1..=4 {
            assert_eq!(
                number(&store.view(Some(v), 7).unwrap(), 1),
                Some(if v == 1 { 1 } else { 3 })
            );
        }
        assert_eq!(number(&store.view(Some(2), i64::MIN).unwrap(), 1), Some(1));
        assert_eq!(number(&store.view(Some(2), i64::MAX).unwrap(), 1), Some(4));
        assert!(matches!(
            store.view(Some(5), 0),
            Err(TemporalStoreError::UnavailableVersion)
        ));
    }
    #[test]
    fn failed_write_does_not_advance_version_or_write_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store.commit(&[vertex(1, 0, None, 1)]).unwrap();
        assert!(matches!(
            store.commit(&[vertex(2, 0, None, 2), vertex(1, 5, Some(5), 3)]),
            Err(TemporalStoreError::InvalidInterval)
        ));
        assert!(matches!(
            store.commit(&[vertex(1, 0, Some(5), 2), vertex(1, 4, None, 3)]),
            Err(TemporalStoreError::AmbiguousBatch)
        ));
        assert_eq!(store.view(None, 0).unwrap().version(), 1);
        assert!(store
            .view(None, 0)
            .unwrap()
            .get_vertex(2)
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .commit(&[vertex(1, 0, Some(5), 2), vertex(1, 5, None, 3)])
                .unwrap(),
            2
        );
        assert_eq!(number(&store.view(None, 5).unwrap(), 1), Some(3));
        assert!(matches!(
            store.commit(&vec![vertex(1, 0, None, 4); MAX_BATCH + 1]),
            Err(TemporalStoreError::Limit)
        ));
    }
    #[test]
    fn existing_graph_or_unknown_format_is_not_initialized() {
        let dir = tempfile::tempdir().unwrap();
        {
            let graph = crate::GraphEngine::open(dir.path()).unwrap();
            graph.create_vertex(&Vertex::new(1)).unwrap();
        }
        assert!(matches!(
            TemporalStore::open(dir.path()),
            Err(TemporalStoreError::IncompatibleStore)
        ));
        {
            let graph = crate::GraphEngine::open(dir.path()).unwrap();
            assert!(graph.get_vertex(1).unwrap().is_some());
        }
        let other = tempfile::tempdir().unwrap();
        {
            let store = TemporalStore::open(other.path()).unwrap();
            store.db.put(FORMAT_KEY, [2]).unwrap();
        }
        assert!(matches!(
            TemporalStore::open(other.path()),
            Err(TemporalStoreError::IncompatibleStore)
        ));
    }
    #[test]
    fn dense_oracle_survives_reopen_and_empty_commits() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = Vec::new();
        let mut cells = [None; 16];
        {
            let store = TemporalStore::open(dir.path()).unwrap();
            for v in 1..=40i64 {
                let begin = (v * 7 % 15) as usize;
                let end = begin + 1 + (v * 11 % (16 - begin) as i64) as usize;
                let batch = if v % 7 == 0 {
                    vec![]
                } else if v % 5 == 0 {
                    cells[begin..end].fill(None);
                    vec![TemporalMutation::Absent {
                        valid: interval(begin as i64, Some(end as i64)),
                        key: Key::Vertex(1),
                    }]
                } else {
                    cells[begin..end].fill(Some(v));
                    vec![vertex(1, begin as i64, Some(end as i64), v)]
                };
                assert_eq!(store.commit(&batch).unwrap(), v as u64);
                rows.push(cells);
            }
        }
        let store = TemporalStore::open(dir.path()).unwrap();
        for (i, row) in rows.iter().enumerate() {
            for (t, expected) in row.iter().enumerate() {
                assert_eq!(
                    number(&store.view(Some(i as u64 + 1), t as i64).unwrap(), 1),
                    *expected,
                    "V={} T={t}",
                    i + 1
                );
            }
        }
    }
    #[test]
    fn concurrent_commits_and_edge_revisions_keep_local_prefixes() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        let mut versions = std::thread::scope(|scope| {
            let handles = (0..4)
                .map(|worker| {
                    let store = &store;
                    scope.spawn(move || {
                        (0..8)
                            .map(|n| store.commit(&[vertex(worker, 0, None, n)]).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        versions.sort_unstable();
        assert_eq!(versions, (1..=32).collect::<Vec<_>>());
        let v1 = store
            .commit(&[TemporalMutation::Edge {
                valid: interval(0, None),
                edge: Edge::new(9, 0, 1, "LINK"),
            }])
            .unwrap();
        let old = store.view(Some(v1), 7).unwrap();
        let v2 = store
            .commit(&[TemporalMutation::Edge {
                valid: interval(5, Some(10)),
                edge: Edge::new(9, 0, 2, "LINK"),
            }])
            .unwrap();
        let v3 = store
            .commit(&[TemporalMutation::Absent {
                valid: interval(6, Some(8)),
                key: Key::Edge(9),
            }])
            .unwrap();
        assert_eq!(old.get_edge(9).unwrap().unwrap().to, 1);
        assert_eq!(
            store
                .view(Some(v2), 7)
                .unwrap()
                .get_edge(9)
                .unwrap()
                .unwrap()
                .to,
            2
        );
        assert!(store
            .view(Some(v3), 7)
            .unwrap()
            .get_edge(9)
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .view(Some(v3), 8)
                .unwrap()
                .get_edge(9)
                .unwrap()
                .unwrap()
                .to,
            2
        );
        drop(old);
        drop(store);
        let store = TemporalStore::open(dir.path()).unwrap();
        assert_eq!(
            store
                .view(Some(v1), 7)
                .unwrap()
                .get_edge(9)
                .unwrap()
                .unwrap()
                .to,
            1
        );
        assert_eq!(
            store
                .view(Some(v2), 7)
                .unwrap()
                .get_edge(9)
                .unwrap()
                .unwrap()
                .to,
            2
        );
    }
    #[test]
    fn missing_metadata_and_wrong_payload_kind_are_errors() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = TemporalStore::open(dir.path()).unwrap();
            store.commit(&[vertex(1, 0, None, 1)]).unwrap();
            let payload = codec::encode_edge(&Edge::new(1, 1, 1, "LINK")).unwrap();
            store
                .db
                .put(
                    revision_key(Key::Vertex(1), 1, 0),
                    encode_assertion(interval(0, None), Some(&payload)).unwrap(),
                )
                .unwrap();
            assert!(matches!(
                store.view(None, 0).unwrap().get_vertex(1),
                Err(TemporalStoreError::Graph(crate::Error::CorruptData(_)))
            ));
            store.db.delete(HEAD_KEY).unwrap();
        }
        assert!(matches!(
            TemporalStore::open(dir.path()),
            Err(TemporalStoreError::Corrupt)
        ));
    }
    #[test]
    fn corruption_and_history_work_limits_fail_not_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store.commit(&[vertex(1, 0, None, 1)]).unwrap();
        let wrong = codec::encode_vertex(&Vertex::new(2)).unwrap();
        store
            .db
            .put(
                revision_key(Key::Vertex(1), 1, 0),
                encode_assertion(interval(0, None), Some(&wrong)).unwrap(),
            )
            .unwrap();
        assert!(matches!(
            store.view(None, 0).unwrap().get_vertex(1),
            Err(TemporalStoreError::Corrupt)
        ));
        let mut batch = WriteBatch::default();
        for v in 1..=MAX_READ_REVISIONS + 1 {
            batch.put(
                revision_key(Key::Vertex(3), v as u64, 0),
                encode_assertion(interval(0, Some(1)), None).unwrap(),
            );
        }
        batch.put(HEAD_KEY, ((MAX_READ_REVISIONS + 1) as u64).to_be_bytes());
        store.db.write(batch).unwrap();
        assert!(store
            .view(Some(MAX_READ_REVISIONS as u64), 2)
            .unwrap()
            .get_vertex(3)
            .unwrap()
            .is_none());
        assert!(matches!(
            store.view(None, 2).unwrap().get_vertex(3),
            Err(TemporalStoreError::Limit)
        ));
    }
}
