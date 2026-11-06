//! Single-node property graph storage.
//!
//! This is the storage kernel for the graph direction. It deliberately keeps
//! the first milestone small: vertices, directed labelled edges, properties,
//! adjacency indexes, and WAL-backed RocksDB writes. Query planning,
//! transactions, and distributed placement build on this layer.
//!
//! Key space (one physical partition per `GraphEngine`):
//!
//! ```text
//! graph/v1/p/<partition>/vertex/<vertex-id>            -> binary vertex
//! graph/v1/p/<partition>/edge/<edge-id>                -> binary edge
//! graph/v1/p/<partition>/out/<source>/<label>/<edge>   -> empty
//! graph/v1/p/<partition>/in/<target>/<label>/<edge>    -> empty
//! ```
//!
//! `v1` versions the on-disk layout, `partition` is a big-endian `u32`, and
//! ids are big-endian `u64`. Labels are validated ASCII identifiers, so `/` is
//! an unambiguous separator. Vertices, edges, and adjacency entries use a
//! deterministic protobuf-compatible binary format (see [`codec`]). Edge
//! mutations write the edge record and both adjacency entries in one RocksDB
//! `WriteBatch`.

pub(crate) mod codec;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::Mutex;

use rocksdb::{
    DBCompressionType, Direction, IteratorMode, Options, Snapshot, WriteBatch, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::value::Value;

const GRAPH_PREFIX: &[u8] = b"graph/v1/p/";
const VERTEX_SUFFIX: &[u8] = b"vertex/";
const EDGE_SUFFIX: &[u8] = b"edge/";
const OUT_SUFFIX: &[u8] = b"out/";
const IN_SUFFIX: &[u8] = b"in/";
const MAX_LABEL_LEN: usize = 64;
const MAX_TRANSACTION_OPERATIONS: usize = 10_000;
const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;

/// A property-graph vertex. `id` must be globally unique for a distributed
/// deployment; the engine never generates ids.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vertex {
    pub id: u64,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

impl Vertex {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            labels: Vec::new(),
            properties: BTreeMap::new(),
        }
    }
}

/// A directed, labelled edge between two vertices.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub id: u64,
    pub from: u64,
    pub to: u64,
    pub label: String,
    pub properties: BTreeMap<String, Value>,
}

impl Edge {
    pub fn new(id: u64, from: u64, to: u64, label: impl Into<String>) -> Self {
        Self {
            id,
            from,
            to,
            label: label.into(),
            properties: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum EdgeDirection {
    Out,
    In,
}

/// A single-node property graph backed by RocksDB, scoped to one partition.
///
/// Mutations are serialized by an in-process lock and committed as one batch,
/// so an edge and both of its adjacency entries become visible together. Reads
/// observe only committed batches. Writes use RocksDB's default WAL behavior
/// (no per-statement fsync); a process crash may lose the most recent writes,
/// while a clean reopen recovers any WAL entries that reached the OS.
///
/// `partition` is part of every key, so a future distributed layer can place
/// adjacent vertices and edges in the same partition. A batch is atomic only
/// within this partition; cross-partition transactions need the transaction
/// layer that K3 adds.
pub struct GraphEngine {
    db: DB,
    write_lock: Mutex<()>,
    partition: u32,
}

/// A stable, read-only view of one graph partition.
///
/// The view pins a RocksDB snapshot, so it continues to observe the state at
/// [`GraphEngine::read_view`] even while newer writes commit. It exposes point
/// and adjacency reads only; writes go through [`GraphEngine`] or a future
/// transaction. Snapshot creation copies no graph data.
pub struct GraphReadView<'a> {
    snapshot: Snapshot<'a>,
    partition: u32,
}

/// A single-partition, snapshot-isolated write transaction.
///
/// Mutations are buffered until [`GraphTransaction::commit`], which validates
/// the final state, checks that no other write committed after [`begin`], and
/// writes one synced `WriteBatch`. Dropping the transaction aborts it. Reads
/// see the transaction's own buffered writes over its pinned snapshot.
///
/// [`begin`]: GraphEngine::begin
pub struct GraphTransaction<'a> {
    engine: &'a GraphEngine,
    snapshot: Snapshot<'a>,
    read_sequence: u64,
    vertices: BTreeMap<u64, TxVertex>,
    edges: BTreeMap<u64, TxEdge>,
    deleted_adjacency: BTreeSet<Vec<u8>>,
    operations: usize,
}

struct TxVertex {
    original: Option<Vertex>,
    current: Option<Vertex>,
}

struct TxEdge {
    original: Option<Edge>,
    current: Option<Edge>,
}

impl GraphEngine {
    /// Open partition `0`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        Self::open_partition_with_options(path, 0, options)
    }

    /// Open partition `0` with caller-supplied RocksDB options.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        Self::open_partition_with_options(path, 0, options)
    }

    /// Open a specific partition with default options.
    pub fn open_partition(path: impl AsRef<Path>, partition: u32) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        Self::open_partition_with_options(path, partition, options)
    }

    /// Open a specific partition with caller-supplied RocksDB options.
    pub fn open_partition_with_options(
        path: impl AsRef<Path>,
        partition: u32,
        options: Options,
    ) -> Result<Self> {
        let db = DB::open(&options, path.as_ref())?;
        Ok(Self {
            db,
            write_lock: Mutex::new(()),
            partition,
        })
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    /// Pin a stable read view at the current RocksDB sequence.
    pub fn read_view(&self) -> GraphReadView<'_> {
        GraphReadView {
            snapshot: self.db.snapshot(),
            partition: self.partition,
        }
    }

    /// Start a single-partition transaction at the current RocksDB sequence.
    pub fn begin(&self) -> Result<GraphTransaction<'_>> {
        let snapshot = self.db.snapshot();
        let read_sequence = snapshot.sequence_number();
        Ok(GraphTransaction {
            engine: self,
            snapshot,
            read_sequence,
            vertices: BTreeMap::new(),
            edges: BTreeMap::new(),
            deleted_adjacency: BTreeSet::new(),
            operations: 0,
        })
    }

    /// Insert a new vertex. Returns `VertexAlreadyExists` if the id is taken.
    pub fn create_vertex(&self, vertex: &Vertex) -> Result<()> {
        validate_vertex(vertex)?;
        let encoded = codec::encode_vertex(vertex)?;
        let key = vertex_key(self.partition, vertex.id);
        let _guard = self.lock();
        if self.db.get(&key)?.is_some() {
            return Err(Error::VertexAlreadyExists(vertex.id));
        }
        self.db.put(key, encoded)?;
        Ok(())
    }

    /// Replace an existing vertex's labels and properties.
    pub fn update_vertex(&self, vertex: &Vertex) -> Result<()> {
        validate_vertex(vertex)?;
        let encoded = codec::encode_vertex(vertex)?;
        let key = vertex_key(self.partition, vertex.id);
        let _guard = self.lock();
        if self.db.get(&key)?.is_none() {
            return Err(Error::VertexNotFound(vertex.id));
        }
        self.db.put(key, encoded)?;
        Ok(())
    }

    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        let Some(bytes) = self.db.get(vertex_key(self.partition, id))? else {
            return Ok(None);
        };
        let vertex = codec::decode_vertex(&bytes)?;
        if vertex.id != id {
            return Err(Error::CorruptData(format!(
                "vertex key {id} contains vertex {}",
                vertex.id
            )));
        }
        Ok(Some(vertex))
    }

    /// Delete a vertex and every incident edge, including both endpoints'
    /// adjacency entries. Returns `false` when the vertex does not exist.
    pub fn delete_vertex(&self, id: u64) -> Result<bool> {
        let key = vertex_key(self.partition, id);
        let _guard = self.lock();
        if self.db.get(&key)?.is_none() {
            return Ok(false);
        }

        let mut edge_ids = HashSet::new();
        let mut adjacency_keys = Vec::new();
        for prefix in [
            adjacency_prefix(self.partition, EdgeDirection::Out, id, None),
            adjacency_prefix(self.partition, EdgeDirection::In, id, None),
        ] {
            for item in self
                .db
                .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
            {
                let (stored_key, _) = item?;
                if !stored_key.starts_with(&prefix) {
                    break;
                }
                edge_ids.insert(edge_id_from_adjacency_key(&stored_key)?);
                adjacency_keys.push(stored_key);
            }
        }

        let mut batch = WriteBatch::default();
        for edge_id in edge_ids {
            if let Some(edge) = self.get_edge_inner(edge_id)? {
                batch.delete(edge_key(self.partition, edge.id));
                batch.delete(adjacency_key(
                    self.partition,
                    EdgeDirection::Out,
                    edge.from,
                    &edge.label,
                    edge.id,
                ));
                batch.delete(adjacency_key(
                    self.partition,
                    EdgeDirection::In,
                    edge.to,
                    &edge.label,
                    edge.id,
                ));
            }
        }
        // Remove orphaned adjacency entries left by older data or a partially
        // repaired database, not just the entries reachable from the edges.
        for stored_key in adjacency_keys {
            batch.delete(stored_key);
        }
        batch.delete(key);
        self.db.write(batch)?;
        Ok(true)
    }

    /// Insert a directed edge. Both endpoint vertices must exist and the edge
    /// id must be unused.
    pub fn create_edge(&self, edge: &Edge) -> Result<()> {
        validate_edge(edge)?;
        let encoded = codec::encode_edge(edge)?;
        let key = edge_key(self.partition, edge.id);
        let _guard = self.lock();
        if self.db.get(&key)?.is_some() {
            return Err(Error::EdgeAlreadyExists(edge.id));
        }
        if self
            .db
            .get(vertex_key(self.partition, edge.from))?
            .is_none()
        {
            return Err(Error::EdgeEndpointNotFound(edge.from));
        }
        if self.db.get(vertex_key(self.partition, edge.to))?.is_none() {
            return Err(Error::EdgeEndpointNotFound(edge.to));
        }

        let mut batch = WriteBatch::default();
        batch.put(key, encoded);
        batch.put(
            adjacency_key(
                self.partition,
                EdgeDirection::Out,
                edge.from,
                &edge.label,
                edge.id,
            ),
            Vec::new(),
        );
        batch.put(
            adjacency_key(
                self.partition,
                EdgeDirection::In,
                edge.to,
                &edge.label,
                edge.id,
            ),
            Vec::new(),
        );
        self.db.write(batch)?;
        Ok(())
    }

    /// Read an edge by id.
    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        self.get_edge_inner(id)
    }

    /// Delete an edge and both adjacency entries. Returns `false` when absent.
    pub fn delete_edge(&self, id: u64) -> Result<bool> {
        let _guard = self.lock();
        let Some(edge) = self.get_edge_inner(id)? else {
            return Ok(false);
        };
        let mut batch = WriteBatch::default();
        batch.delete(edge_key(self.partition, edge.id));
        batch.delete(adjacency_key(
            self.partition,
            EdgeDirection::Out,
            edge.from,
            &edge.label,
            edge.id,
        ));
        batch.delete(adjacency_key(
            self.partition,
            EdgeDirection::In,
            edge.to,
            &edge.label,
            edge.id,
        ));
        self.db.write(batch)?;
        Ok(true)
    }

    /// Outgoing edges for a vertex, optionally filtered by label. Results are
    /// ordered by edge id.
    pub fn out_edges(&self, vertex_id: u64, label: Option<&str>) -> Result<Vec<Edge>> {
        self.edges_by_direction(vertex_id, EdgeDirection::Out, label)
    }

    /// Incoming edges for a vertex, optionally filtered by label. Results are
    /// ordered by edge id.
    pub fn in_edges(&self, vertex_id: u64, label: Option<&str>) -> Result<Vec<Edge>> {
        self.edges_by_direction(vertex_id, EdgeDirection::In, label)
    }

    /// All vertices in this partition, ordered by id.
    pub fn vertices(&self) -> Result<Vec<Vertex>> {
        let prefix = vertex_prefix(self.partition);
        let mut vertices: Vec<Vertex> = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
        {
            let (key, bytes) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let vertex = codec::decode_vertex(&bytes)?;
            let stored_id = id_from_record_key(&key, &prefix)?;
            if vertex.id != stored_id {
                return Err(Error::CorruptData(format!(
                    "vertex key {stored_id} contains vertex {}",
                    vertex.id
                )));
            }
            vertices.push(vertex);
        }
        vertices.sort_by_key(|vertex| vertex.id);
        Ok(vertices)
    }

    /// All edges in this partition, ordered by id.
    pub fn edges(&self) -> Result<Vec<Edge>> {
        let prefix = edge_prefix(self.partition);
        let mut edges: Vec<Edge> = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
        {
            let (key, bytes) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let edge = codec::decode_edge(&bytes)?;
            let stored_id = id_from_record_key(&key, &prefix)?;
            if edge.id != stored_id {
                return Err(Error::CorruptData(format!(
                    "edge key {stored_id} contains edge {}",
                    edge.id
                )));
            }
            edges.push(edge);
        }
        edges.sort_by_key(|edge| edge.id);
        Ok(edges)
    }

    fn edges_by_direction(
        &self,
        vertex_id: u64,
        direction: EdgeDirection,
        label: Option<&str>,
    ) -> Result<Vec<Edge>> {
        if let Some(label) = label {
            validate_label(label)?;
        }
        if self
            .db
            .get(vertex_key(self.partition, vertex_id))?
            .is_none()
        {
            return Err(Error::VertexNotFound(vertex_id));
        }

        let prefix = adjacency_prefix(self.partition, direction, vertex_id, label);
        let mut edge_ids = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
        {
            let (stored_key, _) = item?;
            if !stored_key.starts_with(&prefix) {
                break;
            }
            edge_ids.push(edge_id_from_adjacency_key(&stored_key)?);
        }
        edge_ids.sort_unstable();
        edge_ids.dedup();

        let mut edges = Vec::with_capacity(edge_ids.len());
        for edge_id in edge_ids {
            match self.get_edge_inner(edge_id)? {
                Some(edge) => edges.push(edge),
                None => {
                    return Err(Error::CorruptData(format!(
                        "adjacency entry references missing edge {edge_id}"
                    )))
                }
            }
        }
        Ok(edges)
    }

    fn get_edge_inner(&self, id: u64) -> Result<Option<Edge>> {
        let Some(bytes) = self.db.get(edge_key(self.partition, id))? else {
            return Ok(None);
        };
        let edge = codec::decode_edge(&bytes)?;
        if edge.id != id {
            return Err(Error::CorruptData(format!(
                "edge key {id} contains edge {}",
                edge.id
            )));
        }
        Ok(Some(edge))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl GraphReadView<'_> {
    /// The RocksDB sequence pinned by this view.
    pub fn sequence(&self) -> u64 {
        self.snapshot.sequence_number()
    }

    /// Read a vertex at this view's pinned sequence.
    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        let Some(bytes) = self.snapshot.get(vertex_key(self.partition, id))? else {
            return Ok(None);
        };
        let vertex = codec::decode_vertex(&bytes)?;
        if vertex.id != id {
            return Err(Error::CorruptData(format!(
                "vertex key {id} contains vertex {}",
                vertex.id
            )));
        }
        Ok(Some(vertex))
    }

    /// Read an edge at this view's pinned sequence.
    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        self.get_edge_inner(id)
    }

    /// Outgoing edges for a vertex in this view, ordered by edge id.
    pub fn out_edges(&self, vertex_id: u64, label: Option<&str>) -> Result<Vec<Edge>> {
        self.edges_by_direction(vertex_id, EdgeDirection::Out, label)
    }

    /// Incoming edges for a vertex in this view, ordered by edge id.
    pub fn in_edges(&self, vertex_id: u64, label: Option<&str>) -> Result<Vec<Edge>> {
        self.edges_by_direction(vertex_id, EdgeDirection::In, label)
    }

    fn edges_by_direction(
        &self,
        vertex_id: u64,
        direction: EdgeDirection,
        label: Option<&str>,
    ) -> Result<Vec<Edge>> {
        if let Some(label) = label {
            validate_label(label)?;
        }
        if self
            .snapshot
            .get(vertex_key(self.partition, vertex_id))?
            .is_none()
        {
            return Err(Error::VertexNotFound(vertex_id));
        }

        let prefix = adjacency_prefix(self.partition, direction, vertex_id, label);
        let mut edge_ids = Vec::new();
        for item in self
            .snapshot
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
        {
            let (stored_key, _) = item?;
            if !stored_key.starts_with(&prefix) {
                break;
            }
            edge_ids.push(edge_id_from_adjacency_key(&stored_key)?);
        }
        edge_ids.sort_unstable();
        edge_ids.dedup();

        let mut edges = Vec::with_capacity(edge_ids.len());
        for edge_id in edge_ids {
            match self.get_edge_inner(edge_id)? {
                Some(edge) => edges.push(edge),
                None => {
                    return Err(Error::CorruptData(format!(
                        "adjacency entry references missing edge {edge_id}"
                    )))
                }
            }
        }
        Ok(edges)
    }

    fn get_edge_inner(&self, id: u64) -> Result<Option<Edge>> {
        let Some(bytes) = self.snapshot.get(edge_key(self.partition, id))? else {
            return Ok(None);
        };
        let edge = codec::decode_edge(&bytes)?;
        if edge.id != id {
            return Err(Error::CorruptData(format!(
                "edge key {id} contains edge {}",
                edge.id
            )));
        }
        Ok(Some(edge))
    }
}

impl GraphTransaction<'_> {
    /// Read a vertex from the transaction's snapshot and own write overlay.
    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        self.view_vertex(id)
    }

    /// Read an edge from the transaction's snapshot and own write overlay.
    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        self.view_edge(id)
    }

    /// Buffer a new vertex.
    pub fn create_vertex(&mut self, vertex: &Vertex) -> Result<()> {
        validate_vertex(vertex)?;
        self.note_operation()?;
        if self.view_vertex(vertex.id)?.is_some() {
            return Err(Error::VertexAlreadyExists(vertex.id));
        }
        self.touch_vertex(vertex.id)?.current = Some(vertex.clone());
        Ok(())
    }

    /// Buffer a replacement for an existing vertex.
    pub fn update_vertex(&mut self, vertex: &Vertex) -> Result<()> {
        validate_vertex(vertex)?;
        self.note_operation()?;
        if self.view_vertex(vertex.id)?.is_none() {
            return Err(Error::VertexNotFound(vertex.id));
        }
        self.touch_vertex(vertex.id)?.current = Some(vertex.clone());
        Ok(())
    }

    /// Buffer a vertex deletion, including every incident edge.
    pub fn delete_vertex(&mut self, id: u64) -> Result<bool> {
        self.note_operation()?;
        if self.view_vertex(id)?.is_none() {
            return Ok(false);
        }

        let (edge_ids, adjacency_keys) = self.incident_edge_ids(id)?;
        self.deleted_adjacency.extend(adjacency_keys);
        for edge_id in edge_ids {
            let entry = self.touch_edge(edge_id)?;
            entry.current = None;
        }
        self.touch_vertex(id)?.current = None;
        Ok(true)
    }

    /// Buffer a new edge. Both endpoints must exist in the transaction view.
    pub fn create_edge(&mut self, edge: &Edge) -> Result<()> {
        validate_edge(edge)?;
        self.note_operation()?;
        if self.view_edge(edge.id)?.is_some() {
            return Err(Error::EdgeAlreadyExists(edge.id));
        }
        if self.view_vertex(edge.from)?.is_none() {
            return Err(Error::EdgeEndpointNotFound(edge.from));
        }
        if edge.from != edge.to && self.view_vertex(edge.to)?.is_none() {
            return Err(Error::EdgeEndpointNotFound(edge.to));
        }
        self.touch_edge(edge.id)?.current = Some(edge.clone());
        Ok(())
    }

    /// Buffer an edge deletion. Returns `false` when it is already absent.
    pub fn delete_edge(&mut self, id: u64) -> Result<bool> {
        self.note_operation()?;
        if self.view_edge(id)?.is_none() {
            return Ok(false);
        }
        self.touch_edge(id)?.current = None;
        Ok(true)
    }

    /// Validate and atomically commit the buffered writes.
    ///
    /// The commit conflicts if any write landed after this transaction's
    /// snapshot. A read-only transaction commits as a no-op.
    pub fn commit(self) -> Result<()> {
        if !self.has_writes() {
            return Ok(());
        }

        let _guard = self.engine.lock();
        if self.engine.db.latest_sequence_number() != self.read_sequence {
            return Err(Error::TransactionConflict);
        }
        self.validate_final_state()?;

        let batch = self.build_batch()?;
        if batch.size_in_bytes() > MAX_TRANSACTION_BYTES {
            return Err(Error::TransactionTooLarge(format!(
                "encoded write set exceeds {MAX_TRANSACTION_BYTES} bytes"
            )));
        }

        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.engine.db.write_opt(batch, &options)?;
        Ok(())
    }

    /// Discard the transaction. Dropping it has the same effect.
    pub fn abort(self) {}

    fn view_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        if let Some(entry) = self.vertices.get(&id) {
            return Ok(entry.current.clone());
        }
        self.read_vertex_snapshot(id)
    }

    fn view_edge(&self, id: u64) -> Result<Option<Edge>> {
        if let Some(entry) = self.edges.get(&id) {
            return Ok(entry.current.clone());
        }
        self.read_edge_snapshot(id)
    }

    fn read_vertex_snapshot(&self, id: u64) -> Result<Option<Vertex>> {
        let Some(bytes) = self.snapshot.get(vertex_key(self.engine.partition, id))? else {
            return Ok(None);
        };
        let vertex = codec::decode_vertex(&bytes)?;
        if vertex.id != id {
            return Err(Error::CorruptData(format!(
                "vertex key {id} contains vertex {}",
                vertex.id
            )));
        }
        Ok(Some(vertex))
    }

    fn read_edge_snapshot(&self, id: u64) -> Result<Option<Edge>> {
        let Some(bytes) = self.snapshot.get(edge_key(self.engine.partition, id))? else {
            return Ok(None);
        };
        let edge = codec::decode_edge(&bytes)?;
        if edge.id != id {
            return Err(Error::CorruptData(format!(
                "edge key {id} contains edge {}",
                edge.id
            )));
        }
        Ok(Some(edge))
    }

    fn touch_vertex(&mut self, id: u64) -> Result<&mut TxVertex> {
        if !self.vertices.contains_key(&id) {
            let original = self.read_vertex_snapshot(id)?;
            self.vertices.insert(
                id,
                TxVertex {
                    current: original.clone(),
                    original,
                },
            );
        }
        Ok(self
            .vertices
            .get_mut(&id)
            .expect("vertex entry inserted above"))
    }

    fn touch_edge(&mut self, id: u64) -> Result<&mut TxEdge> {
        if !self.edges.contains_key(&id) {
            let original = self.read_edge_snapshot(id)?;
            self.edges.insert(
                id,
                TxEdge {
                    current: original.clone(),
                    original,
                },
            );
        }
        Ok(self.edges.get_mut(&id).expect("edge entry inserted above"))
    }

    fn note_operation(&mut self) -> Result<()> {
        self.operations += 1;
        if self.operations > MAX_TRANSACTION_OPERATIONS {
            return Err(Error::TransactionTooLarge(format!(
                "more than {MAX_TRANSACTION_OPERATIONS} operations"
            )));
        }
        Ok(())
    }

    fn incident_edge_ids(&self, vertex_id: u64) -> Result<(Vec<u64>, Vec<Vec<u8>>)> {
        let mut snapshot_edge_ids = BTreeSet::new();
        let mut adjacency_keys = BTreeSet::new();
        for prefix in [
            adjacency_prefix(self.engine.partition, EdgeDirection::Out, vertex_id, None),
            adjacency_prefix(self.engine.partition, EdgeDirection::In, vertex_id, None),
        ] {
            for item in self
                .snapshot
                .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
            {
                let (stored_key, _) = item?;
                if !stored_key.starts_with(&prefix) {
                    break;
                }
                snapshot_edge_ids.insert(edge_id_from_adjacency_key(&stored_key)?);
                adjacency_keys.insert(stored_key.to_vec());
            }
        }

        // An edge may have been deleted or moved to another endpoint in this
        // transaction. Only the edge's current form decides whether deleting
        // the vertex still cascades to it.
        let mut edge_ids = BTreeSet::new();
        for edge_id in snapshot_edge_ids {
            if let Some(edge) = self.view_edge(edge_id)? {
                if edge.from == vertex_id || edge.to == vertex_id {
                    edge_ids.insert(edge_id);
                }
            }
        }
        for (id, entry) in &self.edges {
            if let Some(edge) = &entry.current {
                if edge.from == vertex_id || edge.to == vertex_id {
                    edge_ids.insert(*id);
                }
            }
        }
        Ok((
            edge_ids.into_iter().collect(),
            adjacency_keys.into_iter().collect(),
        ))
    }

    fn has_writes(&self) -> bool {
        !self.deleted_adjacency.is_empty()
            || self
                .vertices
                .values()
                .any(|entry| entry.original.as_ref() != entry.current.as_ref())
            || self
                .edges
                .values()
                .any(|entry| entry.original.as_ref() != entry.current.as_ref())
    }

    fn validate_final_state(&self) -> Result<()> {
        for (id, entry) in &self.vertices {
            if let Some(vertex) = &entry.current {
                validate_vertex(vertex)?;
                if vertex.id != *id {
                    return Err(Error::CorruptData(format!(
                        "transaction vertex key {id} contains vertex {}",
                        vertex.id
                    )));
                }
            }
        }
        for (id, entry) in &self.edges {
            let Some(edge) = &entry.current else {
                continue;
            };
            validate_edge(edge)?;
            if edge.id != *id {
                return Err(Error::CorruptData(format!(
                    "transaction edge key {id} contains edge {}",
                    edge.id
                )));
            }
            if self.view_vertex(edge.from)?.is_none() {
                return Err(Error::EdgeEndpointNotFound(edge.from));
            }
            if edge.from != edge.to && self.view_vertex(edge.to)?.is_none() {
                return Err(Error::EdgeEndpointNotFound(edge.to));
            }
        }
        Ok(())
    }

    fn build_batch(&self) -> Result<WriteBatch> {
        let partition = self.engine.partition;
        let mut batch = WriteBatch::default();

        // Orphaned adjacency entries from a deleted vertex must go first so a
        // recreated edge can put the same key back later in this batch.
        for key in &self.deleted_adjacency {
            batch.delete(key);
        }

        for (id, entry) in &self.vertices {
            match (&entry.original, &entry.current) {
                (Some(_), None) => batch.delete(vertex_key(partition, *id)),
                (Some(original), Some(current)) if original != current => {
                    batch.put(vertex_key(partition, *id), codec::encode_vertex(current)?);
                }
                (None, Some(current)) => {
                    batch.put(vertex_key(partition, *id), codec::encode_vertex(current)?);
                }
                _ => {}
            }
        }

        for entry in self.edges.values() {
            match (&entry.original, &entry.current) {
                (Some(original), None) => {
                    delete_edge_batch(&mut batch, partition, original);
                }
                (Some(original), Some(current)) => {
                    if original != current {
                        delete_edge_batch(&mut batch, partition, original);
                    }
                    put_edge_batch(&mut batch, partition, current)?;
                }
                (None, Some(current)) => put_edge_batch(&mut batch, partition, current)?,
                (None, None) => {}
            }
        }

        Ok(batch)
    }
}

fn put_edge_batch(batch: &mut WriteBatch, partition: u32, edge: &Edge) -> Result<()> {
    batch.put(edge_key(partition, edge.id), codec::encode_edge(edge)?);
    batch.put(
        adjacency_key(
            partition,
            EdgeDirection::Out,
            edge.from,
            &edge.label,
            edge.id,
        ),
        Vec::new(),
    );
    batch.put(
        adjacency_key(partition, EdgeDirection::In, edge.to, &edge.label, edge.id),
        Vec::new(),
    );
    Ok(())
}

fn delete_edge_batch(batch: &mut WriteBatch, partition: u32, edge: &Edge) {
    batch.delete(edge_key(partition, edge.id));
    batch.delete(adjacency_key(
        partition,
        EdgeDirection::Out,
        edge.from,
        &edge.label,
        edge.id,
    ));
    batch.delete(adjacency_key(
        partition,
        EdgeDirection::In,
        edge.to,
        &edge.label,
        edge.id,
    ));
}

fn partition_prefix(partition: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(GRAPH_PREFIX.len() + 5);
    key.extend_from_slice(GRAPH_PREFIX);
    key.extend_from_slice(&partition.to_be_bytes());
    key.push(b'/');
    key
}

pub(crate) fn vertex_prefix(partition: u32) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    key.extend_from_slice(VERTEX_SUFFIX);
    key
}

pub(crate) fn edge_prefix(partition: u32) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    key.extend_from_slice(EDGE_SUFFIX);
    key
}

pub(crate) fn vertex_key(partition: u32, id: u64) -> Vec<u8> {
    key_with_id(&vertex_prefix(partition), id)
}

pub(crate) fn edge_key(partition: u32, id: u64) -> Vec<u8> {
    key_with_id(&edge_prefix(partition), id)
}

fn key_with_id(prefix: &[u8], id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

pub(crate) fn adjacency_prefix(
    partition: u32,
    direction: EdgeDirection,
    vertex_id: u64,
    label: Option<&str>,
) -> Vec<u8> {
    let suffix = match direction {
        EdgeDirection::Out => OUT_SUFFIX,
        EdgeDirection::In => IN_SUFFIX,
    };
    let mut key = partition_prefix(partition);
    key.extend_from_slice(suffix);
    key.extend_from_slice(&vertex_id.to_be_bytes());
    key.push(b'/');
    if let Some(label) = label {
        key.extend_from_slice(label.as_bytes());
        key.push(b'/');
    }
    key
}

pub(crate) fn adjacency_key(
    partition: u32,
    direction: EdgeDirection,
    vertex_id: u64,
    label: &str,
    edge_id: u64,
) -> Vec<u8> {
    let mut key = adjacency_prefix(partition, direction, vertex_id, Some(label));
    key.extend_from_slice(&edge_id.to_be_bytes());
    key
}

pub(crate) fn edge_id_from_adjacency_key(key: &[u8]) -> Result<u64> {
    let bytes = key
        .get(key.len().saturating_sub(8)..)
        .ok_or_else(|| Error::CorruptData("adjacency key is too short".into()))?;
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        Error::CorruptData("adjacency key does not end with a u64".into())
    })?))
}

pub(crate) fn validate_vertex(vertex: &Vertex) -> Result<()> {
    for label in &vertex.labels {
        validate_label(label)?;
    }
    validate_properties(&vertex.properties)
}

pub(crate) fn validate_edge(edge: &Edge) -> Result<()> {
    validate_label(&edge.label)?;
    validate_properties(&edge.properties)
}

fn validate_properties(properties: &BTreeMap<String, Value>) -> Result<()> {
    for (name, value) in properties {
        if let Value::Real(value) = value {
            if !value.is_finite() {
                return Err(Error::InvalidGraphProperty(format!(
                    "property `{name}` cannot store a non-finite REAL"
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn id_from_record_key(key: &[u8], prefix: &[u8]) -> Result<u64> {
    let suffix = key
        .strip_prefix(prefix)
        .ok_or_else(|| Error::CorruptData("record key has the wrong prefix".into()))?;
    if suffix.len() != 8 {
        return Err(Error::CorruptData(format!(
            "record key has {} id bytes, expected 8",
            suffix.len()
        )));
    }
    Ok(u64::from_be_bytes(suffix.try_into().map_err(|_| {
        Error::CorruptData("record key does not end with a u64".into())
    })?))
}

pub(crate) fn validate_label(label: &str) -> Result<()> {
    let valid_length = !label.is_empty() && label.len() <= MAX_LABEL_LEN;
    let valid_bytes = label.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphabetic() || byte == b'_' || (index > 0 && byte.is_ascii_digit())
    });
    if valid_length && valid_bytes {
        Ok(())
    } else {
        Err(Error::InvalidGraphLabel(label.to_string()))
    }
}

impl crate::gremlin::QueryReadBackend for GraphReadView<'_> {
    fn get_vertex(
        &self,
        id: u64,
        budget: &crate::query_memory::Budget,
    ) -> Result<Option<crate::query_memory::Accounted<Vertex>>> {
        let key = vertex_key(self.partition, id);
        let Some(bytes) = self.snapshot.get(&key)? else {
            return Ok(None);
        };
        // RocksDB returns its buffer before the caller knows its length. Charge
        // immediately on return, before any query-owned decoding allocation.
        let _raw = budget.reserve(crate::query_memory::add(key.len(), bytes.len())?)?;
        let vertex = codec::decode_vertex_accounted(&bytes, budget)?;
        if vertex.id != id {
            return Err(Error::CorruptData("vertex id does not match key".into()));
        }
        Ok(Some(vertex))
    }
    fn get_edge(
        &self,
        id: u64,
        budget: &crate::query_memory::Budget,
    ) -> Result<Option<crate::query_memory::Accounted<Edge>>> {
        let key = edge_key(self.partition, id);
        let Some(bytes) = self.snapshot.get(&key)? else {
            return Ok(None);
        };
        let _raw = budget.reserve(crate::query_memory::add(key.len(), bytes.len())?)?;
        let edge = codec::decode_edge_accounted(&bytes, budget)?;
        if edge.id != id {
            return Err(Error::CorruptData("edge id does not match key".into()));
        }
        Ok(Some(edge))
    }
    fn scan_vertices_checked(
        &self,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        budget: &crate::query_memory::Budget,
    ) -> Result<(
        crate::query_memory::AccountedVec<crate::query_memory::Accounted<Vertex>>,
        bool,
    )> {
        check()?;
        let prefix = vertex_prefix(self.partition);
        let mut vertices = crate::query_memory::AccountedVec::new(budget);
        let mut truncated = false;
        for item in self
            .snapshot
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
        {
            check()?;
            let (key, bytes) = item?;
            let _raw = budget.reserve(crate::query_memory::add(key.len(), bytes.len())?)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if vertices.len() >= limit {
                truncated = true;
                break;
            }
            let vertex = codec::decode_vertex_accounted(&bytes, budget)?;
            check()?;
            let stored_id = id_from_record_key(&key, &prefix)?;
            if vertex.id != stored_id {
                return Err(Error::CorruptData(format!(
                    "vertex key {stored_id} contains vertex {}",
                    vertex.id
                )));
            }
            vertices.push(vertex)?;
        }
        check()?;
        Ok((vertices, truncated))
    }

    /// Scan at most `limit` edges in id order. See [`Self::scan_vertices_checked`].
    fn scan_edges_checked(
        &self,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        budget: &crate::query_memory::Budget,
    ) -> Result<(
        crate::query_memory::AccountedVec<crate::query_memory::Accounted<Edge>>,
        bool,
    )> {
        check()?;
        let prefix = edge_prefix(self.partition);
        let mut edges = crate::query_memory::AccountedVec::new(budget);
        let mut truncated = false;
        for item in self
            .snapshot
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
        {
            check()?;
            let (key, bytes) = item?;
            let _raw = budget.reserve(crate::query_memory::add(key.len(), bytes.len())?)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if edges.len() >= limit {
                truncated = true;
                break;
            }
            let edge = codec::decode_edge_accounted(&bytes, budget)?;
            check()?;
            let stored_id = id_from_record_key(&key, &prefix)?;
            if edge.id != stored_id {
                return Err(Error::CorruptData(format!(
                    "edge key {stored_id} contains edge {}",
                    edge.id
                )));
            }
            edges.push(edge)?;
        }
        check()?;
        Ok((edges, truncated))
    }

    /// Visit adjacency records one at a time without collecting ids or properties.
    /// The caller bounds retained rows and restores edge-id ordering (the index
    /// groups records by label). Checks bracket storage reads and decoding, and
    /// run after the visitor and before successful return, including empty scans.
    fn visit_adjacent_edges(
        &self,
        vertex_id: u64,
        incoming: bool,
        label: Option<&str>,
        check: &mut impl FnMut() -> Result<()>,
        visit: &mut impl FnMut(crate::query_memory::Accounted<Edge>) -> Result<()>,
        budget: &crate::query_memory::Budget,
    ) -> Result<()> {
        check()?;
        if let Some(label) = label {
            validate_label(label)?;
        }
        let vertex_exists = {
            let bytes = self.snapshot.get(vertex_key(self.partition, vertex_id))?;
            let _raw = budget.reserve(bytes.as_ref().map_or(0, |b| b.len()))?;
            bytes.is_some()
        };
        check()?;
        if !vertex_exists {
            return Err(Error::VertexNotFound(vertex_id));
        }
        let direction = if incoming {
            EdgeDirection::In
        } else {
            EdgeDirection::Out
        };
        let prefix = adjacency_prefix(self.partition, direction, vertex_id, label);
        for item in self
            .snapshot
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward))
        {
            check()?;
            let (stored_key, value) = item?;
            let _adjacency_raw =
                budget.reserve(crate::query_memory::add(stored_key.len(), value.len())?)?;
            if !stored_key.starts_with(&prefix) {
                break;
            }
            let edge_id = edge_id_from_adjacency_key(&stored_key)?;
            let bytes = self.snapshot.get(edge_key(self.partition, edge_id))?;
            check()?;
            let Some(bytes) = bytes else {
                return Err(Error::CorruptData(format!(
                    "adjacency entry references missing edge {edge_id}"
                )));
            };
            let _raw = budget.reserve(bytes.len())?;
            let edge = codec::decode_edge_accounted(&bytes, budget)?;
            check()?;
            if edge.id != edge_id {
                return Err(Error::CorruptData(format!(
                    "edge key {edge_id} contains edge {}",
                    edge.id
                )));
            }
            visit(edge)?;
            check()?;
        }
        check()?;
        Ok(())
    }
}

#[cfg(test)]
mod query_scan_tests {
    use std::cell::Cell;

    use super::*;
    use crate::gremlin::{QueryReadBackend, MAX_GREMLIN_ACCOUNTED_BYTES};
    use crate::query_memory::Budget;

    fn exhausted() -> Error {
        Error::TraversalLimitExceeded("test budget exhausted".into())
    }

    #[test]
    fn checked_scans_recheck_empty_results_before_returning() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        for vertices in [true, false] {
            let mut checks = 0;
            let mut check = || {
                checks += 1;
                if checks == 2 {
                    Err(exhausted())
                } else {
                    Ok(())
                }
            };
            let result = if vertices {
                graph
                    .read_view()
                    .scan_vertices_checked(
                        10,
                        &mut check,
                        &Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES),
                    )
                    .map(|_| ())
            } else {
                graph
                    .read_view()
                    .scan_edges_checked(10, &mut check, &Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES))
                    .map(|_| ())
            };
            assert!(matches!(result, Err(Error::TraversalLimitExceeded(_))));
            assert_eq!(checks, 2);
        }
    }

    #[test]
    fn checked_scans_stop_after_record_decode() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        graph.create_vertex(&Vertex::new(1)).unwrap();
        graph.create_edge(&Edge::new(2, 1, 1, "LOOP")).unwrap();
        for vertices in [true, false] {
            let mut checks = 0;
            let mut check = || {
                checks += 1;
                if checks == 3 {
                    Err(exhausted())
                } else {
                    Ok(())
                }
            };
            let result = if vertices {
                graph
                    .read_view()
                    .scan_vertices_checked(
                        10,
                        &mut check,
                        &Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES),
                    )
                    .map(|_| ())
            } else {
                graph
                    .read_view()
                    .scan_edges_checked(10, &mut check, &Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES))
                    .map(|_| ())
            };
            assert!(matches!(result, Err(Error::TraversalLimitExceeded(_))));
            assert_eq!(checks, 3);
        }
    }

    #[test]
    fn adjacency_visitor_checks_budget_after_visiting_last_edge() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        graph.create_vertex(&Vertex::new(1)).unwrap();
        graph.create_edge(&Edge::new(2, 1, 1, "LOOP")).unwrap();
        let visited = Cell::new(false);
        let result = graph.read_view().visit_adjacent_edges(
            1,
            false,
            None,
            &mut || {
                if visited.get() {
                    Err(exhausted())
                } else {
                    Ok(())
                }
            },
            &mut |_| {
                visited.set(true);
                Ok(())
            },
            &Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES),
        );
        assert!(visited.get());
        assert!(matches!(result, Err(Error::TraversalLimitExceeded(_))));
    }

    #[test]
    fn adjacency_visitor_stops_without_reading_later_corrupt_record() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        graph.create_vertex(&Vertex::new(1)).unwrap();
        for id in 2..=3 {
            graph.create_edge(&Edge::new(id, 1, 1, "LOOP")).unwrap();
        }
        graph.db.put(edge_key(graph.partition, 3), b"bad").unwrap();
        let mut visited = Vec::new();
        let result = graph.read_view().visit_adjacent_edges(
            1,
            false,
            None,
            &mut || Ok(()),
            &mut |edge| {
                visited.push(edge.id);
                Err(exhausted())
            },
            &Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES),
        );
        assert_eq!(visited, vec![2]);
        assert!(matches!(result, Err(Error::TraversalLimitExceeded(_))));
    }

    #[test]
    fn gremlin_expansion_hits_row_cap_before_later_corrupt_edge() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        graph.create_vertex(&Vertex::new(1)).unwrap();
        graph.create_vertex(&Vertex::new(2)).unwrap();
        for id in 1..=10_002 {
            graph.create_edge(&Edge::new(id, 1, 2, "LINK")).unwrap();
        }
        graph
            .db
            .put(edge_key(graph.partition, 10_002), b"bad")
            .unwrap();

        // The explicit limit cannot hide an overflowing intermediate expansion.
        // Eagerly loading every adjacency record would instead hit CorruptData.
        let result = graph.gremlin("g.V(1).out().limit(1)");
        assert!(matches!(result, Err(Error::TraversalLimitExceeded(_))));
        assert!(matches!(graph.get_edge(10_002), Err(Error::CorruptData(_))));
    }

    #[test]
    fn gremlin_both_deduplicates_self_loops_at_exact_row_cap() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        graph.create_vertex(&Vertex::new(1)).unwrap();
        for id in 1..=10_000 {
            graph.create_edge(&Edge::new(id, 1, 1, "LOOP")).unwrap();
        }
        let result = graph.gremlin("g.V(1).both().count()").unwrap();
        assert_eq!(result.rows, vec![crate::gremlin::GremlinRow::Count(10_000)]);
    }
}
