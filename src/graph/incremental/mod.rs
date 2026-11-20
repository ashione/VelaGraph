//! Experimental bounded in-memory property graph with atomic incremental batches.
//! See `docs/INCREMENTAL_MEMORY_GRAPH.md` for ownership, durability and limits.
#![deny(missing_docs)]
use super::*;
use crate::{GremlinBindings, GremlinQuery, GremlinResult, PreparedGremlinQuery};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, Weak};
use std::time::{Duration, Instant};
mod adjacency;
mod checkpoint;
mod publication;
mod query;
mod table;
mod transaction;
use adjacency::Adj;
use table::Table;
const SHARDS: usize = 256;
const INDEX_FEE: usize = 256;
const BASE_FEE: usize = SHARDS * 256;
const MAX_METADATA: usize = 64 * 1024;
const MAX_IDENTITY: usize = 1024;
const MAX_QUERY_OPERATIONS: usize = 65_536;
const POINT_WORK_BYTES: usize = std::mem::size_of::<u64>();
const SCAN_HEAP_ENTRY_FEE: usize = 64;
const BATCH_DEADLINE: Duration = Duration::from_secs(5);
fn limit(message: &str) -> Error {
    Error::TransactionTooLarge(format!("incremental graph: {message}"))
}
fn bad(message: &str) -> Error {
    Error::IncrementalMemory(Box::new(IncrementalMemoryError::InvalidState(
        message.into(),
    )))
}
/// Typed errors specific to the optional memory graph and its checkpoint files.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IncrementalMemoryError {
    /// Invalid checkpoint contents or violated in-memory state invariants.
    #[error("invalid state: {0}")]
    InvalidState(String),
    /// Local checkpoint filesystem failure; distinct from malformed contents.
    #[error("checkpoint I/O: {0}")]
    Io(#[source] std::io::Error),
}

/// Per-query logical work limits for incremental memory reads.
/// These are independent of retained graph limits and the existing 64 MiB
/// query-owned memory budget; values may only tighten the defaults.
#[derive(Clone, Copy, Debug)]
pub struct IncrementalQueryLimits {
    /// Point resolutions and visited adjacency sources/candidates.
    pub max_operations: usize,
    /// Logical metadata and complete-record work, including repeated reads.
    pub max_work_bytes: usize,
}
impl Default for IncrementalQueryLimits {
    fn default() -> Self {
        Self {
            max_operations: MAX_QUERY_OPERATIONS,
            max_work_bytes: crate::MAX_GREMLIN_ACCOUNTED_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Read ownership strategy for an incremental graph.
pub enum IncrementalMode {
    /// A read handle holds a lock; publication waits until readers release it.
    Locked,
    /// A read handle retains an immutable version; updates copy touched shards.
    Cow,
}
#[derive(Clone, Debug)]
/// Positive logical limits, each of which may only tighten its default.
/// They are not process RSS limits or a global budget across graph instances.
pub struct IncrementalLimits {
    /// Combined live vertex and edge records (maximum 1,000,000).
    pub max_records: usize,
    /// Current graph logical retention (default 256 MiB).
    pub max_accounted_bytes: usize,
    /// Mutation/read/control work units per transaction, not just API calls.
    pub max_batch_operations: usize,
    /// Conservative transaction record, metadata and copy work (16 MiB).
    pub max_batch_bytes: usize,
    /// Maximum retained old COW versions (default eight).
    pub max_pinned_versions: usize,
    /// Conservative current, pinned and COW construction-peak fees (512 MiB).
    pub max_pinned_accounted_bytes: usize,
}
impl Default for IncrementalLimits {
    fn default() -> Self {
        Self {
            max_records: 1_000_000,
            max_accounted_bytes: 256 * 1024 * 1024,
            max_batch_operations: 10_000,
            max_batch_bytes: 16 * 1024 * 1024,
            max_pinned_versions: 8,
            max_pinned_accounted_bytes: 512 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Caller-scoped optimistic token, not authentication or a distributed fence.
pub struct IncrementalToken {
    /// Caller-assigned source/generation identity.
    pub identity: String,
    /// Local successful publication sequence.
    pub sequence: u64,
}
#[derive(Clone, Debug)]
/// Ordered transaction operations. Put replaces an existing record.
pub enum IncrementalMutation {
    /// Insert or replace properties/labels while preserving incident edges.
    PutVertex(Vertex),
    /// Delete a vertex and all incident edges; absent IDs are no-ops.
    DeleteVertex(u64),
    /// Insert or replace an edge and both indexes; endpoints must exist now.
    PutEdge(Edge),
    /// Delete an edge and both indexes; absent IDs are no-ops.
    DeleteEdge(u64),
}
#[derive(Clone, Debug, Default, Serialize)]
/// Logical commit work and COW copy counters, not allocator or CPU measurements.
pub struct IncrementalCommitStats {
    /// Local successful publication sequence.
    pub sequence: u64,
    /// Vertex entries installed, including changed adjacency owners.
    pub changed_vertices: usize,
    /// Canonical edge entries installed or removed.
    pub changed_edges: usize,
    /// Existing table entries cloned by Arc copy-on-write.
    pub copied_index_entries: usize,
    /// Existing edge IDs cloned from shared adjacency chunks.
    pub copied_adjacency_ids: usize,
    /// Charged transaction work units, including control checks.
    pub work_operations: usize,
    /// Conservative transaction work bytes.
    pub work_bytes: usize,
    /// Logical size of the newly published graph.
    pub retained_accounted_bytes: usize,
}
struct Cached<T> {
    value: T,
    bytes: usize,
}
impl<T: RecordBytes> Cached<T> {
    fn new(value: T) -> Self {
        let bytes = value.accounted_bytes();
        Self { value, bytes }
    }
}
impl<T> Cached<T> {
    fn value_ref(&self) -> &T {
        &self.value
    }
}
impl<T> std::ops::Deref for Cached<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T> RecordBytes for Cached<T> {
    fn accounted_bytes(&self) -> usize {
        self.bytes
    }
}
#[derive(Clone)]
struct VEntry {
    record: Arc<Cached<Vertex>>,
    incoming: Arc<Adj>,
    outgoing: Arc<Adj>,
}
impl VEntry {
    fn bytes(&self) -> usize {
        self.record.accounted_bytes() + INDEX_FEE + self.incoming.bytes() + self.outgoing.bytes()
    }
}
#[derive(Clone)]
struct State {
    identity: String,
    sequence: u64,
    vertices: Table<VEntry>,
    edges: Table<Arc<Cached<Edge>>>,
    vertex_count: usize,
    edge_count: usize,
    bytes: usize,
    metadata: Vec<u8>,
}
struct Inner {
    current: Arc<State>,
    retired: Vec<Weak<State>>,
}
/// Optional single-process memory graph; writes are not individually durable.
/// Existing RocksDB engines are not replaced.
pub struct IncrementalGraph {
    mode: IncrementalMode,
    limits: IncrementalLimits,
    inner: RwLock<Inner>,
    // Readers never acquire this lock. Only one COW candidate is prepared at a time.
    cow_prepare: Mutex<()>,
}
enum Handle<'a> {
    Locked(RwLockReadGuard<'a, Inner>),
    Cow(Arc<State>),
}
/// One repeatable view borrowing its graph handle.
/// This handle is not Send, including in COW mode; acquire it in its reader
/// thread. Locked views must be released before the same thread commits.
pub struct IncrementalReadView<'a> {
    handle: Handle<'a>,
}
impl IncrementalGraph {
    /// Create an empty graph with a nonempty caller-managed identity.
    /// Use distinct identities for unrelated sources or generations.
    pub fn new(
        identity: impl Into<String>,
        mode: IncrementalMode,
        limits: IncrementalLimits,
    ) -> Result<Self> {
        let identity = identity.into();
        let cap = IncrementalLimits::default();
        if identity.is_empty()
            || identity.len() > MAX_IDENTITY
            || limits.max_records == 0
            || limits.max_records > cap.max_records
            || limits.max_accounted_bytes < BASE_FEE
            || limits.max_accounted_bytes > cap.max_accounted_bytes
            || limits.max_batch_operations == 0
            || limits.max_batch_operations > cap.max_batch_operations
            || limits.max_batch_bytes == 0
            || limits.max_batch_bytes > cap.max_batch_bytes
            || limits.max_pinned_versions == 0
            || limits.max_pinned_versions > cap.max_pinned_versions
            || limits.max_pinned_accounted_bytes < limits.max_accounted_bytes
            || limits.max_pinned_accounted_bytes > cap.max_pinned_accounted_bytes
        {
            return Err(limit("invalid identity or limits"));
        }
        Ok(Self {
            mode,
            limits,
            cow_prepare: Mutex::new(()),
            inner: RwLock::new(Inner {
                current: Arc::new(State {
                    identity,
                    sequence: 0,
                    vertices: Table::new(),
                    edges: Table::new(),
                    vertex_count: 0,
                    edge_count: 0,
                    bytes: BASE_FEE,
                    metadata: vec![],
                }),
                retired: vec![],
            }),
        })
    }
    /// Acquire the latest published version. Locked handles retain the read lock.
    pub fn read(&self) -> Result<IncrementalReadView<'_>> {
        let g = self.inner.read().map_err(|_| bad("poisoned lock"))?;
        Ok(IncrementalReadView {
            handle: match self.mode {
                IncrementalMode::Locked => Handle::Locked(g),
                IncrementalMode::Cow => Handle::Cow(g.current.clone()),
            },
        })
    }
    /// Return this graph or view's identity and local publication sequence.
    pub fn token(&self) -> Result<IncrementalToken> {
        Ok(self.read()?.token())
    }
    /// Pin a view and start a private, optimistic transaction.
    pub fn begin(&self) -> Result<IncrementalTransaction<'_>> {
        transaction::begin(self)
    }
    /// Apply an ordered batch only if the full expected token still matches.
    /// Metadata and records publish together; failures publish nothing.
    pub fn apply(
        &self,
        expected: &IncrementalToken,
        mutations: &[IncrementalMutation],
        metadata: &[u8],
    ) -> Result<IncrementalCommitStats> {
        let mut tx = self.begin()?;
        if &tx.token() != expected {
            return Err(Error::TransactionConflict);
        };
        for m in mutations {
            match m {
                IncrementalMutation::PutVertex(v) => {
                    tx.preflight_record_copy(v.accounted_bytes() + INDEX_FEE)?;
                    validate_vertex(v)?;
                    tx.put_vertex(v.clone())?;
                }
                IncrementalMutation::DeleteVertex(id) => tx.delete_vertex(*id)?,
                IncrementalMutation::PutEdge(e) => {
                    tx.preflight_record_copy(e.accounted_bytes() + INDEX_FEE)?;
                    validate_edge(e)?;
                    tx.put_edge(e.clone())?;
                }
                IncrementalMutation::DeleteEdge(id) => tx.delete_edge(*id)?,
            }
        }
        tx.set_metadata(metadata)?;
        tx.commit()
    }
}
impl IncrementalReadView<'_> {
    fn state(&self) -> &State {
        match &self.handle {
            Handle::Locked(g) => &g.current,
            Handle::Cow(s) => s,
        }
    }
    /// Return this graph or view's identity and local publication sequence.
    pub fn token(&self) -> IncrementalToken {
        IncrementalToken {
            identity: self.state().identity.clone(),
            sequence: self.state().sequence,
        }
    }
    /// Read caller metadata from exactly this version.
    pub fn metadata(&self) -> &[u8] {
        &self.state().metadata
    }
    /// Conservative reserved table-capacity fees, excluding record payloads.
    pub fn index_reserved_bytes(&self) -> usize {
        self.state().vertices.reserved_bytes() + self.state().edges.reserved_bytes()
    }
    /// Logical current-version retention, not RSS or all pinned versions.
    pub fn accounted_bytes(&self) -> usize {
        self.state().bytes
    }
    /// Borrow a vertex; a missing ID returns None.
    pub fn get_vertex(&self, id: u64) -> Option<&Vertex> {
        self.state().vertices.get(id).map(|v| &v.record.value)
    }
    /// Borrow an edge; a missing ID returns None.
    pub fn get_edge(&self, id: u64) -> Option<&Edge> {
        self.state().edges.get(id).map(|e| &e.value)
    }
    /// Visit vertices in ascending ID order. Allocates a bounded ID buffer.
    pub fn vertices(&self) -> impl Iterator<Item = &Vertex> {
        self.state()
            .vertices
            .sorted_ids(self.state().vertex_count)
            .into_iter()
            .map(|id| {
                &self
                    .state()
                    .vertices
                    .get(id)
                    .expect("validated vertex index")
                    .record
                    .value
            })
    }
    /// Visit edges in ascending ID order. Allocates a bounded ID buffer.
    pub fn edges(&self) -> impl Iterator<Item = &Edge> {
        self.state()
            .edges
            .sorted_ids(self.state().edge_count)
            .into_iter()
            .map(|id| {
                &self
                    .state()
                    .edges
                    .get(id)
                    .expect("validated edge index")
                    .value
            })
    }
    /// Borrow adjacent edges in ascending edge-ID order. Missing starts error.
    pub fn adjacent(&self, id: u64, incoming: bool) -> Result<impl Iterator<Item = &Edge>> {
        let v = self
            .state()
            .vertices
            .get(id)
            .ok_or(Error::VertexNotFound(id))?;
        let a = if incoming { &v.incoming } else { &v.outgoing };
        Ok(a.ids().map(|id| {
            self.state()
                .edges
                .get(id)
                .expect("validated adjacency invariant")
                .value_ref()
        }))
    }
}
pub use transaction::IncrementalTransaction;
trait RecordBytes {
    fn accounted_bytes(&self) -> usize;
}
fn property_bytes(props: &BTreeMap<String, crate::Value>) -> usize {
    props.iter().fold(0usize, |n, (k, v)| {
        n.saturating_add(crate::query_memory::MAP_ENTRY)
            .saturating_add(k.capacity())
            .saturating_add(match v {
                crate::Value::Text(s) => s.capacity(),
                _ => 0,
            })
    })
}
impl RecordBytes for Vertex {
    fn accounted_bytes(&self) -> usize {
        crate::query_memory::RECORD_BASE
            .saturating_add(self.labels.capacity() * std::mem::size_of::<String>())
            .saturating_add(self.labels.iter().map(String::capacity).sum::<usize>())
            .saturating_add(property_bytes(&self.properties))
    }
}
impl RecordBytes for Edge {
    fn accounted_bytes(&self) -> usize {
        crate::query_memory::RECORD_BASE
            .saturating_add(self.label.capacity())
            .saturating_add(property_bytes(&self.properties))
    }
}
