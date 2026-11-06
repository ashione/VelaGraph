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
//! validation. A definite storage conflict requires preparing a new mutation
//! to re-read and revalidate. For an uncertain commit outcome, retain a
//! [`PreparedGraphMutation`] and retry it unchanged, including on another store
//! for the same logical shard and partition. The storage node determines dedup
//! retention and epoch fencing; this adapter never refreshes either the epoch
//! or sequence condition. Nodes without conditional writes reject mutations.
//! Convenience CRUD calls prepare a fresh identity per invocation and therefore
//! cannot recover an uncertain outcome by simply repeating the CRUD call.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::graph::{
    adjacency_key, adjacency_prefix, codec, edge_id_from_adjacency_key, edge_key, edge_prefix,
    id_from_record_key, validate_edge, validate_vertex, vertex_key, vertex_prefix, Edge,
    EdgeDirection, Vertex,
};
use crate::query_memory::{self, Accounted, AccountedVec, Budget, Reservation};
use crate::storage_node::{
    Epoch, KeyRange, ReadView, RequestId, ScanLimit, Sequence, ShardId, StorageNode,
    StorageNodeError, WriteOp, WriteRequest, WriteStatus,
};

/// Maximum number of edges a single adjacency read may return.
pub const MAX_ADJACENCY_EDGES: usize = 1_000;
/// Maximum number of operations one graph mutation may produce.
pub const MAX_GRAPH_BATCH_OPERATIONS: usize = 10_000;

const SCAN_BYTES: usize = 64 * 1024 * 1024;

/// An immutable, validated graph mutation with a stable retry identity.
///
/// Preparation does not write data. Keep this value until the outcome is known;
/// cloning it preserves its identity. Commit retries submit the exact original
/// bytes, epoch and sequence condition without re-reading or revalidating.
/// It may be submitted to another store for the same logical shard/partition,
/// such as after a leader change. It is not a durable retry journal and does not
/// bypass epoch fencing or the underlying node's deduplication retention limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedGraphMutation {
    request: WriteRequest,
    expected_sequence: Sequence,
    partition: u32,
}

impl PreparedGraphMutation {
    /// Logical shard to which this mutation is bound.
    pub fn shard(&self) -> ShardId {
        self.request.shard
    }

    /// Graph partition to which this mutation is bound.
    pub fn partition(&self) -> u32 {
        self.partition
    }

    /// Original ownership epoch; retries never refresh it.
    pub fn epoch(&self) -> Epoch {
        self.request.epoch
    }

    /// Stable idempotency identity shared by every retry of this value.
    pub fn request_id(&self) -> RequestId {
        self.request.request_id
    }

    /// Original validation sequence; retries never refresh it.
    pub fn expected_sequence(&self) -> Sequence {
        self.expected_sequence
    }
}

/// A graph-record view over one D1 shard.
///
/// The store writes the same `graph/v1/p/<partition>/...` keys as
/// `GraphEngine`, so the two representations stay byte-compatible. It is
/// constructed over any [`StorageNode`], including the local adapter and the
/// external in-process Raft adapter.
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

    fn lock(&self) -> Result<MutexGuard<'_, ()>> {
        // Reject incompatible executor threads before they could wait on a
        // mutation already using the blocking store lock.
        self.node.check_execution_context()?;
        Ok(self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()))
    }

    /// Pin one storage-node view. Replicated nodes acquire a leader read barrier.
    /// All subsequent query reads retain this sequence and use the node's checks.
    pub fn read_view(&self) -> Result<GraphRecordReadView<'_, S>> {
        self.node.check_execution_context()?;
        let view = self.node.read_view()?;
        if view.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(
                "view belongs to a different shard".into(),
            )
            .into());
        }
        Ok(GraphRecordReadView { store: self, view })
    }

    /// Parse and execute standard Gremlin against one newly pinned D1 view.
    pub fn execute_gremlin(&self, query: &str) -> Result<crate::GremlinResult> {
        crate::GremlinQuery::parse(query)?.execute_records(self)
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
        let _guard = self.lock()?;
        let prepared = self.prepare_create_vertex(vertex)?;
        self.commit_prepared_unlocked(&prepared)
    }

    /// Validate and prepare this mutation without writing it.
    /// Retain the returned value for retry after an uncertain commit outcome.
    pub fn prepare_create_vertex(&self, vertex: &Vertex) -> Result<PreparedGraphMutation> {
        validate_vertex(vertex)?;
        let view = self.node.read_view()?;
        if self.get_vertex_in(&view, vertex.id)?.is_some() {
            return Err(Error::VertexAlreadyExists(vertex.id));
        }
        let encoded = codec::encode_vertex(vertex)?;
        self.prepare_write(
            &view,
            vec![WriteOp::put(vertex_key(self.partition, vertex.id), encoded)],
        )
    }

    /// Replace an existing vertex. The id must exist.
    pub fn update_vertex(&self, vertex: &Vertex) -> Result<()> {
        let _guard = self.lock()?;
        let prepared = self.prepare_update_vertex(vertex)?;
        self.commit_prepared_unlocked(&prepared)
    }

    /// Validate and prepare this mutation without writing it.
    /// Retain the returned value for retry after an uncertain commit outcome.
    pub fn prepare_update_vertex(&self, vertex: &Vertex) -> Result<PreparedGraphMutation> {
        validate_vertex(vertex)?;
        let view = self.node.read_view()?;
        if self.get_vertex_in(&view, vertex.id)?.is_none() {
            return Err(Error::VertexNotFound(vertex.id));
        }
        let encoded = codec::encode_vertex(vertex)?;
        self.prepare_write(
            &view,
            vec![WriteOp::put(vertex_key(self.partition, vertex.id), encoded)],
        )
    }

    /// Delete a vertex and every incident edge and adjacency entry atomically.
    /// Corrupt scanned adjacency entries fail before any write is submitted.
    pub fn delete_vertex(&self, id: u64) -> Result<()> {
        let _guard = self.lock()?;
        let prepared = self.prepare_delete_vertex(id)?;
        self.commit_prepared_unlocked(&prepared)
    }

    /// Validate and prepare this mutation without writing it.
    /// Retain the returned value for retry after an uncertain commit outcome.
    pub fn prepare_delete_vertex(&self, id: u64) -> Result<PreparedGraphMutation> {
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
        self.prepare_write(&view, operations)
    }

    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        let view = self.node.read_view()?;
        self.get_edge_in(&view, id)
    }

    /// Insert an edge and both adjacency entries atomically.
    pub fn create_edge(&self, edge: &Edge) -> Result<()> {
        let _guard = self.lock()?;
        let prepared = self.prepare_create_edge(edge)?;
        self.commit_prepared_unlocked(&prepared)
    }

    /// Validate and prepare this mutation without writing it.
    /// Retain the returned value for retry after an uncertain commit outcome.
    pub fn prepare_create_edge(&self, edge: &Edge) -> Result<PreparedGraphMutation> {
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
        self.prepare_write(&view, self.put_edge_ops(edge)?)
    }

    /// Replace an existing edge and its adjacency entries atomically.
    pub fn update_edge(&self, edge: &Edge) -> Result<()> {
        let _guard = self.lock()?;
        let prepared = self.prepare_update_edge(edge)?;
        self.commit_prepared_unlocked(&prepared)
    }

    /// Validate and prepare this mutation without writing it.
    /// Retain the returned value for retry after an uncertain commit outcome.
    pub fn prepare_update_edge(&self, edge: &Edge) -> Result<PreparedGraphMutation> {
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
        self.prepare_write(&view, operations)
    }

    /// Delete an edge and both adjacency entries atomically.
    pub fn delete_edge(&self, id: u64) -> Result<()> {
        let _guard = self.lock()?;
        let prepared = self.prepare_delete_edge(id)?;
        self.commit_prepared_unlocked(&prepared)
    }

    /// Validate and prepare this mutation without writing it.
    /// Retain the returned value for retry after an uncertain commit outcome.
    pub fn prepare_delete_edge(&self, id: u64) -> Result<PreparedGraphMutation> {
        let view = self.node.read_view()?;
        let edge = self
            .get_edge_in(&view, id)?
            .ok_or(Error::EdgeNotFound(id))?;
        let mut operations = Vec::new();
        self.delete_edge_ops(&edge, &mut operations);
        self.prepare_write(&view, operations)
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

    fn prepare_write(
        &self,
        view: &ReadView,
        operations: Vec<WriteOp>,
    ) -> Result<PreparedGraphMutation> {
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
        Ok(PreparedGraphMutation {
            request: WriteRequest {
                shard: self.shard,
                epoch: view.epoch,
                request_id,
                operations,
            },
            expected_sequence: view.sequence,
            partition: self.partition,
        })
    }

    /// Commit or retry an unchanged prepared mutation for this shard/partition.
    ///
    /// No validation reads are repeated and no retry is automatic. A definite
    /// conflict requires a newly prepared mutation; an uncertain outcome must
    /// be retried with this same value. Epoch fencing remains authoritative.
    pub fn commit_prepared(&self, prepared: &PreparedGraphMutation) -> Result<()> {
        let _guard = self.lock()?;
        self.commit_prepared_unlocked(prepared)
    }

    fn commit_prepared_unlocked(&self, prepared: &PreparedGraphMutation) -> Result<()> {
        if prepared.shard() != self.shard || prepared.partition != self.partition {
            return Err(StorageNodeError::InvalidArgument(
                "prepared graph mutation targets a different shard or partition".into(),
            )
            .into());
        }
        let result = self
            .node
            .batch_write_conditional(prepared.request.clone(), Some(prepared.expected_sequence))?;
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

/// One immutable D1 graph sequence, retaining its originating store and adapter.
/// Reads still perform context and epoch checks; no raw-view shortcut is exposed.
/// Retaining this value does not guarantee that a new leader read is available.
pub struct GraphRecordReadView<'a, S: StorageNode> {
    store: &'a GraphRecordStore<S>,
    view: ReadView,
}

/// Byte budget for a D1 query page or individual graph record (key plus value).
pub const GRAPH_QUERY_PAGE_BYTES: usize = 16 * 1024 * 1024;
const QUERY_PAGE_ROWS: usize = 256;

impl<S: StorageNode> GraphRecordReadView<'_, S> {
    /// Sequence pinned when the view was acquired.
    pub fn sequence(&self) -> Sequence {
        self.view.sequence
    }
    /// Ownership epoch pinned when the view was acquired.
    pub fn epoch(&self) -> Epoch {
        self.view.epoch
    }
    /// Parse and execute a query on this retained sequence.
    pub fn execute_gremlin(&self, query: &str) -> Result<crate::GremlinResult> {
        crate::GremlinQuery::parse(query)?.execute_record_view(self)
    }
    /// Read a vertex through the originating adapter.
    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        self.record(
            &vertex_key(self.store.partition, id),
            codec::decode_vertex,
            |v| v.id,
            id,
        )
    }
    /// Read an edge through the originating adapter.
    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        self.record(
            &edge_key(self.store.partition, id),
            codec::decode_edge,
            |e| e.id,
            id,
        )
    }
    fn record<T>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> Result<T>,
        id_of: impl FnOnce(&T) -> u64,
        id: u64,
    ) -> Result<Option<T>> {
        let Some(bytes) = self.store.node.get(key, &self.view)? else {
            return Ok(None);
        };
        if key.len().saturating_add(bytes.len()) > GRAPH_QUERY_PAGE_BYTES {
            return Err(StorageNodeError::ResourceExhausted(
                "graph query record byte limit exceeded".into(),
            )
            .into());
        }
        let record = decode(&bytes)?;
        if id_of(&record) != id {
            return Err(Error::CorruptData(
                "graph record id does not match key".into(),
            ));
        }
        Ok(Some(record))
    }
    fn record_accounted<T>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8], &Budget) -> Result<Accounted<T>>,
        id_of: impl FnOnce(&T) -> u64,
        id: u64,
        budget: &Budget,
    ) -> Result<Option<Accounted<T>>> {
        let Some(bytes) = self.store.node.get(key, &self.view)? else {
            return Ok(None);
        };
        let bytes_len = query_memory::add(key.len(), bytes.len())?;
        if bytes_len > GRAPH_QUERY_PAGE_BYTES {
            return Err(StorageNodeError::ResourceExhausted(
                "graph query record byte limit exceeded".into(),
            )
            .into());
        }
        // Existing get cannot preflight the adapter's return allocation. Charge
        // on receipt, before decoding or retaining another copy in the query.
        let _raw = budget.reserve(bytes_len)?;
        let record = decode(&bytes, budget)?;
        if id_of(&record) != id {
            return Err(Error::CorruptData(
                "graph record id does not match key".into(),
            ));
        }
        Ok(Some(record))
    }
    fn query_page(
        &self,
        range: &KeyRange,
        rows: usize,
        cursor: Option<&crate::ScanCursor>,
        budget: &Budget,
    ) -> Result<QueryPage> {
        let slots = query_memory::mul(rows, std::mem::size_of::<crate::KeyValue>())?;
        let mut charge = budget.reserve(slots)?;
        // Reserve both returned key/value bytes and the possible copied cursor
        // key before dispatch. Adapter-private allocations remain out of scope.
        let bytes = GRAPH_QUERY_PAGE_BYTES.min(budget.available() / 2);
        if bytes == 0 {
            return Err(Error::TraversalLimitExceeded(
                "query accounted byte budget exceeded".into(),
            ));
        }
        charge.grow(query_memory::mul(bytes, 2)?)?;
        let page = self
            .store
            .node
            .scan_page(
                range.clone(),
                ScanLimit::new(rows, bytes)?,
                &self.view,
                cursor,
            )
            .map_err(|error| match error {
                StorageNodeError::ResourceExhausted(_) if bytes < GRAPH_QUERY_PAGE_BYTES => {
                    Error::TraversalLimitExceeded(
                        "query accounted byte budget cannot fit D1 page".into(),
                    )
                }
                other => Error::from(other),
            })?;
        let raw = page.rows.iter().try_fold(0, |sum, row| {
            query_memory::add(sum, query_memory::add(row.key.len(), row.value.len())?)
        })?;
        let cursor_bytes = page
            .next_cursor
            .as_ref()
            .map_or(0, |c| c.query_resume_bytes());
        // A conforming scan_page cannot exceed its requested bounds. Reject a
        // broken adapter rather than accepting unaccounted returned memory.
        if page.rows.len() > rows || raw > bytes || cursor_bytes > bytes {
            return Err(Error::TraversalLimitExceeded(
                "D1 page exceeded query byte reservation".into(),
            ));
        }
        charge.shrink(query_memory::add(
            slots,
            query_memory::add(raw, cursor_bytes)?,
        )?);
        let next_cursor = page
            .next_cursor
            .map(|cursor| Accounted::new(cursor, charge.split(cursor_bytes)));
        Ok(QueryPage {
            rows: page.rows,
            _charge: charge,
            next_cursor,
        })
    }
    fn scan<T>(
        &self,
        prefix: Vec<u8>,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        decode: impl Fn(&[u8], &Budget) -> Result<Accounted<T>>,
        id_of: impl Fn(&T) -> u64,
        budget: &Budget,
    ) -> Result<(AccountedVec<Accounted<T>>, bool)> {
        let range = prefix_range(&prefix)?;
        let mut cursor: Option<Accounted<crate::ScanCursor>> = None;
        let mut records = AccountedVec::new(budget);
        loop {
            check()?;
            let remaining = limit.saturating_sub(records.len());
            let page = self.query_page(
                &range,
                remaining.clamp(1, QUERY_PAGE_ROWS),
                cursor.as_deref(),
                budget,
            )?;
            check()?;
            for row in page.rows {
                check()?;
                if records.len() == limit {
                    return Ok((records, true));
                }
                let record = decode(&row.value, budget)?;
                check()?;
                if id_of(&record) != id_from_record_key(&row.key, &prefix)? {
                    return Err(Error::CorruptData(
                        "graph record id does not match key".into(),
                    ));
                }
                records.push(record)?;
            }
            if page.next_cursor.is_none() {
                check()?;
                return Ok((records, false));
            }
            if records.len() == limit {
                check()?;
                return Ok((records, true));
            }
            cursor = page.next_cursor;
        }
    }
}

struct QueryPage {
    rows: Vec<crate::KeyValue>,
    _charge: Reservation,
    next_cursor: Option<Accounted<crate::ScanCursor>>,
}

impl<S: StorageNode> crate::gremlin::QueryReadBackend for GraphRecordReadView<'_, S> {
    fn get_vertex(&self, id: u64, budget: &Budget) -> Result<Option<Accounted<Vertex>>> {
        self.record_accounted(
            &vertex_key(self.store.partition, id),
            codec::decode_vertex_accounted,
            |v| v.id,
            id,
            budget,
        )
    }
    fn get_edge(&self, id: u64, budget: &Budget) -> Result<Option<Accounted<Edge>>> {
        self.record_accounted(
            &edge_key(self.store.partition, id),
            codec::decode_edge_accounted,
            |e| e.id,
            id,
            budget,
        )
    }
    fn scan_vertices_checked(
        &self,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        budget: &Budget,
    ) -> Result<(AccountedVec<Accounted<Vertex>>, bool)> {
        self.scan(
            vertex_prefix(self.store.partition),
            limit,
            check,
            codec::decode_vertex_accounted,
            |v| v.id,
            budget,
        )
    }
    fn scan_edges_checked(
        &self,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        budget: &Budget,
    ) -> Result<(AccountedVec<Accounted<Edge>>, bool)> {
        self.scan(
            edge_prefix(self.store.partition),
            limit,
            check,
            codec::decode_edge_accounted,
            |e| e.id,
            budget,
        )
    }
    fn visit_adjacent_edges(
        &self,
        vertex_id: u64,
        incoming: bool,
        label: Option<&str>,
        check: &mut impl FnMut() -> Result<()>,
        visit: &mut impl FnMut(Accounted<Edge>) -> Result<()>,
        budget: &Budget,
    ) -> Result<()> {
        check()?;
        if let Some(label) = label {
            crate::graph::validate_label(label)?;
        }
        if crate::gremlin::QueryReadBackend::get_vertex(self, vertex_id, budget)?.is_none() {
            return Err(Error::VertexNotFound(vertex_id));
        }
        let direction = if incoming {
            EdgeDirection::In
        } else {
            EdgeDirection::Out
        };
        let range = prefix_range(&adjacency_prefix(
            self.store.partition,
            direction,
            vertex_id,
            label,
        ))?;
        let mut cursor: Option<Accounted<crate::ScanCursor>> = None;
        loop {
            check()?;
            let page = self.query_page(&range, QUERY_PAGE_ROWS, cursor.as_deref(), budget)?;
            check()?;
            for row in page.rows {
                check()?;
                let edge_id = edge_id_from_adjacency_key(&row.key)?;
                let edge = crate::gremlin::QueryReadBackend::get_edge(self, edge_id, budget)?
                    .ok_or_else(|| {
                        Error::CorruptData("adjacency references missing edge".into())
                    })?;
                check()?;
                // Stored payloads may be corrupt. Bound the reconstructed key
                // before allocation; the fixed control allowance covers only
                // valid (at most 64-byte) graph labels.
                crate::graph::validate_label(&edge.label)
                    .map_err(|_| Error::CorruptData("invalid edge label in adjacency".into()))?;
                let endpoint = if incoming { edge.to } else { edge.from };
                if endpoint != vertex_id
                    || row.key
                        != adjacency_key(
                            self.store.partition,
                            direction,
                            endpoint,
                            &edge.label,
                            edge.id,
                        )
                    || !row.value.is_empty()
                {
                    return Err(Error::CorruptData("adjacency does not match edge".into()));
                }
                visit(edge)?;
                check()?;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                check()?;
                return Ok(());
            }
        }
    }
}
