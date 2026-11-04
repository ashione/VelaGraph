//! Single-node property graph storage.
//!
//! This is the storage kernel for the graph direction. It deliberately keeps
//! the first milestone small: vertices, directed labelled edges, properties,
//! adjacency indexes, and crash-safe RocksDB writes. Query planning,
//! transactions, and distributed placement build on this layer.
//!
//! Key space:
//!
//! ```text
//! graph/v/<vertex-id>                                  -> JSON vertex
//! graph/e/<edge-id>                                    -> JSON edge
//! graph/out/<source-id>/<label>/<edge-id>              -> empty
//! graph/in/<target-id>/<label>/<edge-id>               -> empty
//! ```
//!
//! All arrays are length-prefixed only where needed; ids are big-endian i64
//! and labels are validated ASCII identifiers, so `/` is an unambiguous
//! separator. Edge mutations write the edge record and both adjacency entries
//! in one RocksDB `WriteBatch`.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Mutex;

use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::value::Value;

const VERTEX_PREFIX: &[u8] = b"graph/v/";
const EDGE_PREFIX: &[u8] = b"graph/e/";
const OUT_PREFIX: &[u8] = b"graph/out/";
const IN_PREFIX: &[u8] = b"graph/in/";
const MAX_LABEL_LEN: usize = 64;

/// A property-graph vertex.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vertex {
    pub id: i64,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

impl Vertex {
    pub fn new(id: i64) -> Self {
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
    pub id: i64,
    pub from: i64,
    pub to: i64,
    pub label: String,
    pub properties: BTreeMap<String, Value>,
}

impl Edge {
    pub fn new(id: i64, from: i64, to: i64, label: impl Into<String>) -> Self {
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

/// A single-node property graph backed by RocksDB.
///
/// Mutations are serialized by an in-process lock and committed as one batch,
/// so an edge and both of its adjacency entries become visible together. Reads
/// observe only committed batches. Crash recovery is RocksDB's responsibility.
pub struct GraphEngine {
    db: DB,
    write_lock: Mutex<()>,
}

impl GraphEngine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        Self::open_with_options(path, options)
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let db = DB::open(&options, path.as_ref())?;
        Ok(Self {
            db,
            write_lock: Mutex::new(()),
        })
    }

    /// Insert a new vertex. Returns `VertexAlreadyExists` if the id is taken.
    pub fn create_vertex(&self, vertex: &Vertex) -> Result<()> {
        validate_vertex(vertex)?;
        let key = vertex_key(vertex.id);
        let _guard = self.lock();
        if self.db.get(&key)?.is_some() {
            return Err(Error::VertexAlreadyExists(vertex.id));
        }
        self.db.put(key, serde_json::to_vec(vertex)?)?;
        Ok(())
    }

    /// Replace an existing vertex's labels and properties.
    pub fn update_vertex(&self, vertex: &Vertex) -> Result<()> {
        validate_vertex(vertex)?;
        let key = vertex_key(vertex.id);
        let _guard = self.lock();
        if self.db.get(&key)?.is_none() {
            return Err(Error::VertexNotFound(vertex.id));
        }
        self.db.put(key, serde_json::to_vec(vertex)?)?;
        Ok(())
    }

    pub fn get_vertex(&self, id: i64) -> Result<Option<Vertex>> {
        let Some(bytes) = self.db.get(vertex_key(id))? else {
            return Ok(None);
        };
        let vertex: Vertex = serde_json::from_slice(&bytes)?;
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
    pub fn delete_vertex(&self, id: i64) -> Result<bool> {
        let key = vertex_key(id);
        let _guard = self.lock();
        if self.db.get(&key)?.is_none() {
            return Ok(false);
        }

        let mut edge_ids = HashSet::new();
        let mut adjacency_keys = Vec::new();
        for prefix in [
            adjacency_prefix(EdgeDirection::Out, id, None),
            adjacency_prefix(EdgeDirection::In, id, None),
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
                batch.delete(edge_key(edge.id));
                batch.delete(adjacency_key(
                    EdgeDirection::Out,
                    edge.from,
                    &edge.label,
                    edge.id,
                ));
                batch.delete(adjacency_key(
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
        validate_label(&edge.label)?;
        let key = edge_key(edge.id);
        let _guard = self.lock();
        if self.db.get(&key)?.is_some() {
            return Err(Error::EdgeAlreadyExists(edge.id));
        }
        if self.db.get(vertex_key(edge.from))?.is_none() {
            return Err(Error::EdgeEndpointNotFound(edge.from));
        }
        if self.db.get(vertex_key(edge.to))?.is_none() {
            return Err(Error::EdgeEndpointNotFound(edge.to));
        }

        let mut batch = WriteBatch::default();
        batch.put(key, serde_json::to_vec(edge)?);
        batch.put(
            adjacency_key(EdgeDirection::Out, edge.from, &edge.label, edge.id),
            Vec::new(),
        );
        batch.put(
            adjacency_key(EdgeDirection::In, edge.to, &edge.label, edge.id),
            Vec::new(),
        );
        self.db.write(batch)?;
        Ok(())
    }

    pub fn get_edge(&self, id: i64) -> Result<Option<Edge>> {
        self.get_edge_inner(id)
    }

    /// Delete an edge and both adjacency entries. Returns `false` when absent.
    pub fn delete_edge(&self, id: i64) -> Result<bool> {
        let _guard = self.lock();
        let Some(edge) = self.get_edge_inner(id)? else {
            return Ok(false);
        };
        let mut batch = WriteBatch::default();
        batch.delete(edge_key(edge.id));
        batch.delete(adjacency_key(
            EdgeDirection::Out,
            edge.from,
            &edge.label,
            edge.id,
        ));
        batch.delete(adjacency_key(
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
    pub fn out_edges(&self, vertex_id: i64, label: Option<&str>) -> Result<Vec<Edge>> {
        self.edges_by_direction(vertex_id, EdgeDirection::Out, label)
    }

    /// Incoming edges for a vertex, optionally filtered by label. Results are
    /// ordered by edge id.
    pub fn in_edges(&self, vertex_id: i64, label: Option<&str>) -> Result<Vec<Edge>> {
        self.edges_by_direction(vertex_id, EdgeDirection::In, label)
    }

    /// All vertices, ordered by id.
    pub fn vertices(&self) -> Result<Vec<Vertex>> {
        let mut vertices: Vec<Vertex> = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(VERTEX_PREFIX, Direction::Forward))
        {
            let (key, bytes) = item?;
            if !key.starts_with(VERTEX_PREFIX) {
                break;
            }
            vertices.push(serde_json::from_slice(&bytes)?);
        }
        vertices.sort_by_key(|vertex| vertex.id);
        Ok(vertices)
    }

    /// All edges, ordered by id.
    pub fn edges(&self) -> Result<Vec<Edge>> {
        let mut edges: Vec<Edge> = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(EDGE_PREFIX, Direction::Forward))
        {
            let (key, bytes) = item?;
            if !key.starts_with(EDGE_PREFIX) {
                break;
            }
            edges.push(serde_json::from_slice(&bytes)?);
        }
        edges.sort_by_key(|edge| edge.id);
        Ok(edges)
    }

    fn edges_by_direction(
        &self,
        vertex_id: i64,
        direction: EdgeDirection,
        label: Option<&str>,
    ) -> Result<Vec<Edge>> {
        if let Some(label) = label {
            validate_label(label)?;
        }
        if self.db.get(vertex_key(vertex_id))?.is_none() {
            return Err(Error::VertexNotFound(vertex_id));
        }

        let prefix = adjacency_prefix(direction, vertex_id, label);
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

    fn get_edge_inner(&self, id: i64) -> Result<Option<Edge>> {
        let Some(bytes) = self.db.get(edge_key(id))? else {
            return Ok(None);
        };
        let edge: Edge = serde_json::from_slice(&bytes)?;
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

fn vertex_key(id: i64) -> Vec<u8> {
    key_with_id(VERTEX_PREFIX, id)
}

fn edge_key(id: i64) -> Vec<u8> {
    key_with_id(EDGE_PREFIX, id)
}

fn key_with_id(prefix: &[u8], id: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn adjacency_prefix(direction: EdgeDirection, vertex_id: i64, label: Option<&str>) -> Vec<u8> {
    let prefix = match direction {
        EdgeDirection::Out => OUT_PREFIX,
        EdgeDirection::In => IN_PREFIX,
    };
    let mut key = Vec::with_capacity(prefix.len() + 8 + 1 + MAX_LABEL_LEN + 1);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&vertex_id.to_be_bytes());
    key.push(b'/');
    if let Some(label) = label {
        key.extend_from_slice(label.as_bytes());
        key.push(b'/');
    }
    key
}

fn adjacency_key(direction: EdgeDirection, vertex_id: i64, label: &str, edge_id: i64) -> Vec<u8> {
    let mut key = adjacency_prefix(direction, vertex_id, Some(label));
    key.extend_from_slice(&edge_id.to_be_bytes());
    key
}

fn edge_id_from_adjacency_key(key: &[u8]) -> Result<i64> {
    let bytes = key
        .get(key.len().saturating_sub(8)..)
        .ok_or_else(|| Error::CorruptData("adjacency key is too short".into()))?;
    Ok(i64::from_be_bytes(bytes.try_into().map_err(|_| {
        Error::CorruptData("adjacency key does not end with an i64".into())
    })?))
}

fn validate_vertex(vertex: &Vertex) -> Result<()> {
    for label in &vertex.labels {
        validate_label(label)?;
    }
    Ok(())
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
