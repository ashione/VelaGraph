//! D1 graph-record adapter.
//!
//! This module connects the property-graph record format to the byte-oriented
//! [`StorageNode`](crate::storage_node::StorageNode) boundary. It reuses the
//! existing `graph/v1` key layout and codec, but owns no RocksDB handle: every
//! mutation is one `batch_write`, and every read comes from a [`ReadView`].
//!
//! `GraphEngine` still uses its own RocksDB handle and remains the K1 kernel
//! path. This adapter is the seam that lets a future distributed store persist
//! the same vertices, edges, and adjacency entries as opaque D1 bytes. It does
//! not implement transactions, routing, or replication by itself.
//!
//! Mutations through one store are serialized by an internal mutex. Each
//! mutation validates against one read view and submits its sequence as an
//! atomic precondition, so separate stores sharing a node cannot commit stale
//! validation. A concurrent write returns a typed storage conflict; callers
//! must retry the entire mutation to re-read and revalidate, never just its
//! batch. Nodes without conditional-write support reject mutations explicitly.
//! Each invocation uses a fresh random request id. The high-level methods do
//! not expose caller-supplied ids, so exactly-once retry after an unknown commit
//! remains a D1 caller responsibility.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::graph::{
    adjacency_key, adjacency_prefix, codec, edge_id_from_adjacency_key, edge_key, validate_edge,
    validate_vertex, vertex_key, Edge, EdgeDirection, Vertex,
};
use crate::storage_node::{
    KeyRange, ReadView, ScanLimit, ShardId, StorageNode, StorageNodeError, WriteOp, WriteRequest,
    WriteStatus,
};

/// Maximum number of edges a single adjacency read may return.
pub const MAX_ADJACENCY_EDGES: usize = 1_000;
/// Maximum number of operations one graph mutation may produce.
pub const MAX_GRAPH_BATCH_OPERATIONS: usize = 10_000;

const SCAN_BYTES: usize = 64 * 1024 * 1024;

/// A graph-record view over one D1 shard.
///
/// The store writes the same `graph/v1/p/<partition>/...` keys as
/// `GraphEngine`, so the two representations stay byte-compatible. It is
/// constructed over any [`StorageNode`], including the local adapter and a
/// future replicated state machine.
pub struct GraphRecordStore<S: StorageNode> {
    node: S,
    shard: ShardId,
    partition: u32,
    write_lock: Mutex<()>,
}

impl<S: StorageNode> GraphRecordStore<S> {
    /// Wrap an existing node. `shard` must match the node's shard identity.
    pub fn new(node: S, shard: ShardId, partition: u32) -> Self {
        Self {
            node,
            shard,
            partition,
            write_lock: Mutex::new(()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn node(&self) -> &S {
        &self.node
    }

    pub fn into_inner(self) -> S {
        self.node
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        let view = self.node.read_view()?;
        self.get_vertex_in(&view, id)
    }

    /// Insert a vertex. The id must not exist.
    pub fn create_vertex(&self, vertex: &Vertex) -> Result<()> {
        let _guard = self.lock();
        validate_vertex(vertex)?;
        let view = self.node.read_view()?;
        if self.get_vertex_in(&view, vertex.id)?.is_some() {
            return Err(Error::VertexAlreadyExists(vertex.id));
        }
        let encoded = codec::encode_vertex(vertex)?;
        self.write(
            &view,
            vec![WriteOp::put(vertex_key(self.partition, vertex.id), encoded)],
        )
    }

    /// Replace an existing vertex. The id must exist.
    pub fn update_vertex(&self, vertex: &Vertex) -> Result<()> {
        let _guard = self.lock();
        validate_vertex(vertex)?;
        let view = self.node.read_view()?;
        if self.get_vertex_in(&view, vertex.id)?.is_none() {
            return Err(Error::VertexNotFound(vertex.id));
        }
        let encoded = codec::encode_vertex(vertex)?;
        self.write(
            &view,
            vec![WriteOp::put(vertex_key(self.partition, vertex.id), encoded)],
        )
    }

    /// Delete a vertex and every incident edge and adjacency entry atomically.
    /// Corrupt scanned adjacency entries fail before any write is submitted.
    pub fn delete_vertex(&self, id: u64) -> Result<()> {
        let _guard = self.lock();
        let view = self.node.read_view()?;
        if self.get_vertex_in(&view, id)?.is_none() {
            return Err(Error::VertexNotFound(id));
        }
        let mut edges = BTreeMap::new();
        for direction in [EdgeDirection::Out, EdgeDirection::In] {
            for edge in self.adjacency_edges_in(&view, direction, id, None)? {
                edges.insert(edge.id, edge);
            }
        }
        let mut operations = Vec::with_capacity(edges.len() * 3 + 1);
        for edge in edges.values() {
            self.delete_edge_ops(edge, &mut operations);
        }
        operations.push(WriteOp::delete(vertex_key(self.partition, id)));
        self.write(&view, operations)
    }

    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        let view = self.node.read_view()?;
        self.get_edge_in(&view, id)
    }

    /// Insert an edge and both adjacency entries atomically.
    pub fn create_edge(&self, edge: &Edge) -> Result<()> {
        let _guard = self.lock();
        validate_edge(edge)?;
        let view = self.node.read_view()?;
        if self.get_edge_in(&view, edge.id)?.is_some() {
            return Err(Error::EdgeAlreadyExists(edge.id));
        }
        for endpoint in [edge.from, edge.to] {
            if self.get_vertex_in(&view, endpoint)?.is_none() {
                return Err(Error::EdgeEndpointNotFound(endpoint));
            }
        }
        self.write(&view, self.put_edge_ops(edge)?)
    }

    /// Replace an existing edge and its adjacency entries atomically.
    pub fn update_edge(&self, edge: &Edge) -> Result<()> {
        let _guard = self.lock();
        validate_edge(edge)?;
        let view = self.node.read_view()?;
        let previous = self
            .get_edge_in(&view, edge.id)?
            .ok_or(Error::EdgeNotFound(edge.id))?;
        for endpoint in [edge.from, edge.to] {
            if self.get_vertex_in(&view, endpoint)?.is_none() {
                return Err(Error::EdgeEndpointNotFound(endpoint));
            }
        }
        let mut operations = vec![WriteOp::put(
            edge_key(self.partition, edge.id),
            codec::encode_edge(edge)?,
        )];
        let old_out = adjacency_key(
            self.partition,
            EdgeDirection::Out,
            previous.from,
            &previous.label,
            previous.id,
        );
        let new_out = adjacency_key(
            self.partition,
            EdgeDirection::Out,
            edge.from,
            &edge.label,
            edge.id,
        );
        if old_out != new_out {
            operations.push(WriteOp::delete(old_out));
        }
        operations.push(WriteOp::put(new_out, Vec::new()));
        let old_in = adjacency_key(
            self.partition,
            EdgeDirection::In,
            previous.to,
            &previous.label,
            previous.id,
        );
        let new_in = adjacency_key(
            self.partition,
            EdgeDirection::In,
            edge.to,
            &edge.label,
            edge.id,
        );
        if old_in != new_in {
            operations.push(WriteOp::delete(old_in));
        }
        operations.push(WriteOp::put(new_in, Vec::new()));
        self.write(&view, operations)
    }

    /// Delete an edge and both adjacency entries atomically.
    pub fn delete_edge(&self, id: u64) -> Result<()> {
        let _guard = self.lock();
        let view = self.node.read_view()?;
        let edge = self
            .get_edge_in(&view, id)?
            .ok_or(Error::EdgeNotFound(id))?;
        let mut operations = Vec::new();
        self.delete_edge_ops(&edge, &mut operations);
        self.write(&view, operations)
    }

    /// Read outgoing edges, optionally filtered by label, ordered by edge id.
    /// Every scanned entry is validated before applying the result limit.
    pub fn out_edges(
        &self,
        vertex_id: u64,
        label: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        self.edges_in_direction(vertex_id, EdgeDirection::Out, label, limit)
    }

    /// Read incoming edges, optionally filtered by label, ordered by edge id.
    /// Every scanned entry is validated before applying the result limit.
    pub fn in_edges(&self, vertex_id: u64, label: Option<&str>, limit: usize) -> Result<Vec<Edge>> {
        self.edges_in_direction(vertex_id, EdgeDirection::In, label, limit)
    }

    fn edges_in_direction(
        &self,
        vertex_id: u64,
        direction: EdgeDirection,
        label: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Edge>> {
        if limit == 0 || limit > MAX_ADJACENCY_EDGES {
            return Err(Error::InvalidGraphProperty(format!(
                "edge limit must be between 1 and {MAX_ADJACENCY_EDGES}"
            )));
        }
        if let Some(label) = label {
            crate::graph::validate_label(label)?;
        }
        let view = self.node.read_view()?;
        if self.get_vertex_in(&view, vertex_id)?.is_none() {
            return Err(Error::VertexNotFound(vertex_id));
        }
        let mut edges = self.adjacency_edges_in(&view, direction, vertex_id, label)?;
        edges.truncate(limit);
        Ok(edges)
    }

    fn get_vertex_in(&self, view: &ReadView, id: u64) -> Result<Option<Vertex>> {
        let Some(bytes) = self.node.get(&vertex_key(self.partition, id), view)? else {
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

    fn get_edge_in(&self, view: &ReadView, id: u64) -> Result<Option<Edge>> {
        let Some(bytes) = self.node.get(&edge_key(self.partition, id), view)? else {
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

    // Validate every scanned entry before sorting, deduplicating, truncating,
    // or building a mutation. Otherwise a forged key can hide behind another
    // entry for the same edge, or point a cascade at an unrelated edge.
    fn adjacency_edges_in(
        &self,
        view: &ReadView,
        direction: EdgeDirection,
        vertex_id: u64,
        label: Option<&str>,
    ) -> Result<Vec<Edge>> {
        let prefix = adjacency_prefix(self.partition, direction, vertex_id, label);
        let range = prefix_range(&prefix)?;
        let values = self.node.scan(
            range,
            ScanLimit::new(MAX_ADJACENCY_EDGES, SCAN_BYTES)?,
            view,
        )?;
        let mut edges = Vec::with_capacity(values.len());
        for value in values {
            let edge_id = edge_id_from_adjacency_key(&value.key)?;
            let edge = self.get_edge_in(view, edge_id)?.ok_or_else(|| {
                Error::CorruptData(format!("adjacency points to missing edge {edge_id}"))
            })?;
            let endpoint = match direction {
                EdgeDirection::Out => edge.from,
                EdgeDirection::In => edge.to,
            };
            let expected = adjacency_key(self.partition, direction, endpoint, &edge.label, edge.id);
            if value.key != expected || !value.value.is_empty() {
                return Err(Error::CorruptData(format!(
                    "adjacency for vertex {vertex_id} does not match edge {edge_id}"
                )));
            }
            edges.push(edge);
        }
        edges.sort_by_key(|edge| edge.id);
        Ok(edges)
    }

    fn put_edge_ops(&self, edge: &Edge) -> Result<Vec<WriteOp>> {
        let encoded = codec::encode_edge(edge)?;
        Ok(vec![
            WriteOp::put(edge_key(self.partition, edge.id), encoded),
            WriteOp::put(
                adjacency_key(
                    self.partition,
                    EdgeDirection::Out,
                    edge.from,
                    &edge.label,
                    edge.id,
                ),
                Vec::new(),
            ),
            WriteOp::put(
                adjacency_key(
                    self.partition,
                    EdgeDirection::In,
                    edge.to,
                    &edge.label,
                    edge.id,
                ),
                Vec::new(),
            ),
        ])
    }

    fn delete_edge_ops(&self, edge: &Edge, operations: &mut Vec<WriteOp>) {
        operations.push(WriteOp::delete(edge_key(self.partition, edge.id)));
        operations.push(WriteOp::delete(adjacency_key(
            self.partition,
            EdgeDirection::Out,
            edge.from,
            &edge.label,
            edge.id,
        )));
        operations.push(WriteOp::delete(adjacency_key(
            self.partition,
            EdgeDirection::In,
            edge.to,
            &edge.label,
            edge.id,
        )));
    }

    fn write(&self, view: &ReadView, operations: Vec<WriteOp>) -> Result<()> {
        if operations.len() > MAX_GRAPH_BATCH_OPERATIONS {
            return Err(Error::TransactionTooLarge(format!(
                "graph mutation has {} operations, limit is {MAX_GRAPH_BATCH_OPERATIONS}",
                operations.len()
            )));
        }
        // Sequence-derived ids conflate independent writers from the same view,
        // including two identical creates. Use a distinct identity per attempt.
        let mut request_id = [0_u8; 16];
        getrandom::fill(&mut request_id).map_err(|error| {
            StorageNodeError::Storage(format!("request id entropy unavailable: {error}"))
        })?;
        let result = self.node.batch_write_conditional(
            WriteRequest {
                shard: self.shard,
                epoch: view.epoch,
                request_id,
                operations,
            },
            Some(view.sequence),
        )?;
        match result.status {
            WriteStatus::Committed | WriteStatus::Duplicate => Ok(()),
        }
    }
}

fn prefix_range(prefix: &[u8]) -> Result<KeyRange> {
    let mut end = prefix.to_vec();
    loop {
        match end.last_mut() {
            Some(last) if *last < u8::MAX => {
                *last += 1;
                return KeyRange::new(prefix.to_vec(), end).map_err(Error::from);
            }
            Some(_) => {
                end.pop();
            }
            None => return Ok(KeyRange::all()),
        }
    }
}
