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

mod codec;

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Mutex;

use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::value::Value;

const GRAPH_PREFIX: &[u8] = b"graph/v1/p/";
const VERTEX_SUFFIX: &[u8] = b"vertex/";
const EDGE_SUFFIX: &[u8] = b"edge/";
const OUT_SUFFIX: &[u8] = b"out/";
const IN_SUFFIX: &[u8] = b"in/";
const MAX_LABEL_LEN: usize = 64;

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
enum EdgeDirection {
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

fn partition_prefix(partition: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(GRAPH_PREFIX.len() + 5);
    key.extend_from_slice(GRAPH_PREFIX);
    key.extend_from_slice(&partition.to_be_bytes());
    key.push(b'/');
    key
}

fn vertex_prefix(partition: u32) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    key.extend_from_slice(VERTEX_SUFFIX);
    key
}

fn edge_prefix(partition: u32) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    key.extend_from_slice(EDGE_SUFFIX);
    key
}

fn vertex_key(partition: u32, id: u64) -> Vec<u8> {
    key_with_id(&vertex_prefix(partition), id)
}

fn edge_key(partition: u32, id: u64) -> Vec<u8> {
    key_with_id(&edge_prefix(partition), id)
}

fn key_with_id(prefix: &[u8], id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn adjacency_prefix(
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

fn adjacency_key(
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

fn edge_id_from_adjacency_key(key: &[u8]) -> Result<u64> {
    let bytes = key
        .get(key.len().saturating_sub(8)..)
        .ok_or_else(|| Error::CorruptData("adjacency key is too short".into()))?;
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        Error::CorruptData("adjacency key does not end with a u64".into())
    })?))
}

fn validate_vertex(vertex: &Vertex) -> Result<()> {
    for label in &vertex.labels {
        validate_label(label)?;
    }
    validate_properties(&vertex.properties)
}

fn validate_edge(edge: &Edge) -> Result<()> {
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

fn id_from_record_key(key: &[u8], prefix: &[u8]) -> Result<u64> {
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

fn validate_label(label: &str) -> Result<()> {
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
