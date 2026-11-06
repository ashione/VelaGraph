//! Isolated graph/v2 experiment with append-only creation and property patches.
//! No v1 API reads these records.
//! Batches are shard-local; composing them is explicitly NOT a transaction.
use crate::graph::{codec, validate_edge, validate_label, validate_vertex};
use crate::{
    Edge, KeyRange, ReadView, RequestId, ScanLimit, StorageNode, StorageNodeError, Value, Vertex,
    WriteOp, WriteRequest,
};
use std::collections::{BTreeMap, BTreeSet};

pub type Result<T> = std::result::Result<T, ExperimentalError>;
#[derive(Debug, thiserror::Error)]
pub enum ExperimentalError {
    #[error("storage: {0}")]
    Storage(#[from] StorageNodeError),
    #[error("graph: {0}")]
    Graph(#[from] crate::Error),
    #[error("invalid experimental graph argument: {0}")]
    Invalid(&'static str),
    #[error("experimental entity already exists")]
    AlreadyExists,
    #[error("corrupt experimental graph data: {0}")]
    Corrupt(&'static str),
    #[error("experimental endpoint missing: {0:?}")]
    MissingEndpoint(VertexRef),
    #[error("incomplete or inconsistent experimental edge: {0:?}")]
    Incomplete(EdgeRef),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct VertexRef {
    pub partition: u32,
    pub id: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EdgeRef {
    pub partition: u32,
    pub id: u64,
}
#[derive(Clone, Debug, PartialEq)]
pub struct CrossEdge {
    pub id: u64,
    pub from: VertexRef,
    pub to: VertexRef,
    pub label: String,
    pub properties: BTreeMap<String, Value>,
}
impl CrossEdge {
    pub fn identity(&self) -> EdgeRef {
        EdgeRef {
            partition: self.from.partition,
            id: self.id,
        }
    }
    fn local(&self) -> Edge {
        Edge {
            id: self.id,
            from: self.from.id,
            to: self.to.id,
            label: self.label.clone(),
            properties: self.properties.clone(),
        }
    }
}
/// A properties-only patch. Null is a stored value; removal is explicit.
/// Empty patches and names appearing in both sets are rejected.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EdgePropertyPatch {
    pub set: BTreeMap<String, Value>,
    pub remove: BTreeSet<String>,
}
impl EdgePropertyPatch {
    fn apply(&self, edge: &mut CrossEdge) -> Result<()> {
        if self.set.is_empty() && self.remove.is_empty() {
            return Err(ExperimentalError::Invalid("empty edge property patch"));
        }
        if self.set.keys().any(|key| self.remove.contains(key)) {
            return Err(ExperimentalError::Invalid("overlapping set/remove keys"));
        }
        for key in &self.remove {
            edge.properties.remove(key);
        }
        edge.properties.extend(self.set.clone());
        validate_edge(&edge.local())?;
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
}
/// Immutable exact retry identity. This is not a durable coordinator journal.
#[derive(Clone, Debug)]
pub struct PreparedBatch {
    request: WriteRequest,
    sequence: u64,
}
impl PreparedBatch {
    pub fn request_id(&self) -> RequestId {
        self.request.request_id
    }
    pub fn shard(&self) -> u64 {
        self.request.shard
    }
    pub fn epoch(&self) -> u64 {
        self.request.epoch
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn submit(&self, node: &impl StorageNode) -> Result<()> {
        node.batch_write_conditional(self.request.clone(), Some(self.sequence))?;
        Ok(())
    }
    /// Combine only requests validated against the exact same shard sequence.
    pub fn combine(mut self, other: Self) -> Result<Self> {
        if (self.shard(), self.epoch(), self.sequence)
            != (other.shard(), other.epoch(), other.sequence)
        {
            return Err(ExperimentalError::Invalid("different shard snapshots"));
        }
        let mut keys = std::collections::BTreeSet::new();
        for op in self
            .request
            .operations
            .iter()
            .chain(other.request.operations.iter())
        {
            let key = match op {
                WriteOp::Put { key, .. } | WriteOp::Delete { key } => key,
            };
            if !keys.insert(key) {
                return Err(ExperimentalError::Invalid(
                    "combined batch contains duplicate keys",
                ));
            }
        }
        self.request.operations.extend(other.request.operations);
        Ok(self)
    }
}
/// Isolated participant primitives. Creates remain append-only. The additive
/// properties preparation changes only canonical edge properties; no delete,
/// v1 migration, label/endpoint update, or unqualified cross-edge API exists.
pub struct AppendOnlyStore<S> {
    node: S,
    shard: u64,
    partition: u32,
}
/// A bounded page of v2 vertices in local ID order.
/// `raw_bytes` accounts for the encoded page alongside the decoded vertices.
/// Pass `next_after` back to continue; None means the range was exhausted.
#[derive(Debug)]
pub struct VertexPage {
    pub vertices: Vec<Vertex>,
    pub next_after: Option<u64>,
    pub raw_bytes: usize,
}
/// A bounded canonical edge page in local ID order. Index and endpoint
/// completeness is not certified here; callers must verify each candidate.
#[derive(Debug)]
pub struct EdgePage {
    pub edges: Vec<CrossEdge>,
    pub next_after: Option<u64>,
    pub raw_bytes: usize,
}
impl<S: StorageNode> AppendOnlyStore<S> {
    pub fn new(node: S, shard: u64, partition: u32) -> Self {
        Self {
            node,
            shard,
            partition,
        }
    }
    fn view(&self) -> Result<ReadView> {
        let v = self.node.read_view()?;
        if v.shard != self.shard {
            return Err(ExperimentalError::Invalid("shard mismatch"));
        }
        Ok(v)
    }
    fn batch(&self, v: &ReadView, operations: Vec<WriteOp>) -> Result<PreparedBatch> {
        let mut request_id = [0; 16];
        getrandom::fill(&mut request_id)
            .map_err(|_| ExperimentalError::Invalid("request entropy unavailable"))?;
        Ok(PreparedBatch {
            request: WriteRequest {
                shard: self.shard,
                epoch: v.epoch,
                request_id,
                operations,
            },
            sequence: v.sequence,
        })
    }
    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        let v = self.view()?;
        self.vertex_in(id, &v)
    }
    /// Reads a bounded ordered page, without an all-or-error scan fallback.
    /// Each call uses this node's read view. Callers requiring stable pages must
    /// use a pinned node, as ExperimentalReadSession does; a plain live node
    /// does not promise a snapshot across calls.
    pub fn vertex_page(&self, after: Option<u64>, limit: ScanLimit) -> Result<VertexPage> {
        if limit.max_rows > 128 || limit.max_bytes > 1024 * 1024 + 64 {
            return Err(ExperimentalError::Invalid(
                "vertex page exceeds 128 rows or 1 MiB + 64 bytes",
            ));
        }
        let view = self.view()?;
        let mut prefix = record_key(self.partition, b"vertex/", 0);
        prefix.truncate(prefix.len() - 8);
        let mut end = prefix.clone();
        *end.last_mut().unwrap() += 1;
        let start = after.map_or_else(
            || prefix.clone(),
            |id| {
                let mut key = record_key(self.partition, b"vertex/", id);
                // Exclusive canonical key, without skipping malformed suffixes.
                key.push(0);
                key
            },
        );
        let page = self
            .node
            .scan_page(KeyRange::new(start, end)?, limit, &view, None)?;
        let mut vertices = Vec::new();
        let mut raw_bytes = 0usize;
        let mut last = after;
        for row in page.rows {
            self.node.check_execution_context()?;
            raw_bytes = raw_bytes.saturating_add(row.key.len() + row.value.len());
            if !row.key.starts_with(&prefix) || row.key.len() != prefix.len() + 8 {
                return Err(ExperimentalError::Corrupt("vertex key encoding"));
            }
            let id = u64::from_be_bytes(row.key[prefix.len()..].try_into().unwrap());
            if last.is_some_and(|previous| id <= previous) {
                return Err(ExperimentalError::Corrupt("vertex page order"));
            }
            let (partition, bytes) = unwrap_record(&row.value, 1)?;
            let vertex = codec::decode_vertex(bytes)?;
            if partition != self.partition || vertex.id != id {
                return Err(ExperimentalError::Corrupt("vertex key identity"));
            }
            last = Some(id);
            vertices.push(vertex);
        }
        if page.next_cursor.is_some() && vertices.is_empty() {
            return Err(ExperimentalError::Corrupt("empty continuing vertex page"));
        }
        Ok(VertexPage {
            vertices,
            next_after: page.next_cursor.and(last),
            raw_bytes,
        })
    }
    /// Reads source-owned canonical edges from one bounded page. Stable
    /// pagination requires a pinned node. No all-or-error scan fallback is used.
    pub fn edge_page(&self, after: Option<u64>, limit: ScanLimit) -> Result<EdgePage> {
        if limit.max_rows == 0
            || limit.max_bytes == 0
            || limit.max_rows > 128
            || limit.max_bytes > 1024 * 1024 + 64
        {
            return Err(ExperimentalError::Invalid("edge page limit"));
        }
        let view = self.view()?;
        let mut prefix = record_key(self.partition, b"edge/", 0);
        prefix.truncate(prefix.len() - 8);
        let mut end = prefix.clone();
        *end.last_mut().unwrap() += 1;
        let start = after.map_or_else(
            || prefix.clone(),
            |id| {
                let mut key = record_key(self.partition, b"edge/", id);
                key.push(0);
                key
            },
        );
        let range = KeyRange::new(start, end)?;
        let page = self.node.scan_page(range.clone(), limit, &view, None)?;
        if page.rows.len() > limit.max_rows {
            return Err(ExperimentalError::Corrupt("edge page row limit"));
        }
        let mut edges = Vec::new();
        let mut raw_bytes = 0usize;
        let mut last = after;
        let mut last_key = Vec::new();
        for row in page.rows {
            self.node.check_execution_context()?;
            raw_bytes = raw_bytes.saturating_add(row.key.len() + row.value.len());
            if raw_bytes > limit.max_bytes {
                return Err(ExperimentalError::Corrupt("edge page byte limit"));
            }
            if !row.key.starts_with(&prefix) || row.key.len() != prefix.len() + 8 {
                return Err(ExperimentalError::Corrupt("edge key encoding"));
            }
            let id = u64::from_be_bytes(row.key[prefix.len()..].try_into().unwrap());
            if last.is_some_and(|previous| id <= previous) {
                return Err(ExperimentalError::Corrupt("edge page order"));
            }
            let (to_partition, bytes) = unwrap_record(&row.value, 2)?;
            let local = codec::decode_edge(bytes)?;
            if local.id != id {
                return Err(ExperimentalError::Corrupt("edge key identity"));
            }
            edges.push(CrossEdge {
                id,
                from: VertexRef {
                    partition: self.partition,
                    id: local.from,
                },
                to: VertexRef {
                    partition: to_partition,
                    id: local.to,
                },
                label: local.label,
                properties: local.properties,
            });
            last = Some(id);
            last_key = row.key;
        }
        if page
            .next_cursor
            .as_ref()
            .is_some_and(|c| edges.is_empty() || !c.matches_page(&view, &range, &last_key))
        {
            return Err(ExperimentalError::Corrupt("edge page continuation"));
        }
        Ok(EdgePage {
            edges,
            next_after: page.next_cursor.and(last),
            raw_bytes,
        })
    }
    fn vertex_in(&self, id: u64, v: &ReadView) -> Result<Option<Vertex>> {
        self.node
            .get(&record_key(self.partition, b"vertex/", id), v)?
            .map(|bytes| {
                let (p, rest) = unwrap_record(&bytes, 1)?;
                if p != self.partition {
                    return Err(ExperimentalError::Corrupt("vertex partition encoding"));
                }
                let vertex = codec::decode_vertex(rest)?;
                if vertex.id != id {
                    return Err(ExperimentalError::Corrupt("vertex key identity"));
                }
                Ok(vertex)
            })
            .transpose()
    }
    pub fn prepare_vertex(&self, vertex: &Vertex) -> Result<PreparedBatch> {
        validate_vertex(vertex)?;
        let v = self.view()?;
        if self.vertex_in(vertex.id, &v)?.is_some() {
            return Err(ExperimentalError::AlreadyExists);
        }
        let bytes = wrap_record(1, self.partition, &codec::encode_vertex(vertex)?);
        check_record_size(&bytes)?;
        self.batch(
            &v,
            vec![WriteOp::put(
                record_key(self.partition, b"vertex/", vertex.id),
                bytes,
            )],
        )
    }
    pub fn prepare_source(&self, edge: &CrossEdge) -> Result<PreparedBatch> {
        validate_edge(&edge.local())?;
        if edge.from.partition != self.partition {
            return Err(ExperimentalError::Invalid("source owner mismatch"));
        }
        let v = self.view()?;
        if self.vertex_in(edge.from.id, &v)?.is_none() {
            return Err(ExperimentalError::MissingEndpoint(edge.from));
        }
        if self
            .node
            .get(&record_key(self.partition, b"edge/", edge.id), &v)?
            .is_some()
        {
            return Err(ExperimentalError::AlreadyExists);
        }
        let bytes = wrap_record(2, edge.to.partition, &codec::encode_edge(&edge.local())?);
        check_record_size(&bytes)?;
        self.batch(
            &v,
            vec![
                WriteOp::put(record_key(self.partition, b"edge/", edge.id), bytes),
                WriteOp::put(index_key(edge, Direction::Out), Vec::new()),
            ],
        )
    }
    pub fn prepare_reverse(&self, edge: &CrossEdge) -> Result<PreparedBatch> {
        validate_edge(&edge.local())?;
        if edge.to.partition != self.partition {
            return Err(ExperimentalError::Invalid("target partition mismatch"));
        }
        let v = self.view()?;
        if self.vertex_in(edge.to.id, &v)?.is_none() {
            return Err(ExperimentalError::MissingEndpoint(edge.to));
        }
        if self
            .node
            .get(&index_key(edge, Direction::In), &v)?
            .is_some()
        {
            return Err(ExperimentalError::AlreadyExists);
        }
        self.batch(
            &v,
            vec![WriteOp::put(index_key(edge, Direction::In), Vec::new())],
        )
    }
    pub fn get_edge(&self, id: u64) -> Result<Option<CrossEdge>> {
        let v = self.view()?;
        self.edge_in(id, &v)
    }
    fn edge_in(&self, id: u64, v: &ReadView) -> Result<Option<CrossEdge>> {
        let Some(bytes) = self
            .node
            .get(&record_key(self.partition, b"edge/", id), v)?
        else {
            return Ok(None);
        };
        let (to_partition, rest) = unwrap_record(&bytes, 2)?;
        let local = codec::decode_edge(rest)?;
        let edge = CrossEdge {
            id: local.id,
            from: VertexRef {
                partition: self.partition,
                id: local.from,
            },
            to: VertexRef {
                partition: to_partition,
                id: local.to,
            },
            label: local.label,
            properties: local.properties,
        };
        if edge.id != id
            || self.node.get(&index_key(&edge, Direction::Out), v)? != Some(Vec::new())
            || self.vertex_in(edge.from.id, v)?.is_none()
        {
            return Err(ExperimentalError::Incomplete(edge.identity()));
        }
        Ok(Some(edge))
    }
    /// Prepare one canonical Put against a single source view. The expected
    /// edge must match its exact canonical source encoding. The full routing API
    /// additionally verifies the reverse index and target endpoint in its
    /// frozen session before invoking this participant primitive.
    pub fn prepare_source_properties(
        &self,
        expected: &CrossEdge,
        patch: &EdgePropertyPatch,
    ) -> Result<PreparedBatch> {
        if expected.from.partition != self.partition {
            return Err(ExperimentalError::Invalid("source owner mismatch"));
        }
        let mut updated = expected.clone();
        patch.apply(&mut updated)?;
        let v = self.view()?;
        let current = self
            .edge_in(expected.id, &v)?
            .ok_or(ExperimentalError::Incomplete(expected.identity()))?;
        // Compare bytes rather than f64 PartialEq: signed zeros must not hide
        // a changed source record. NaN is already rejected by validation.
        if current.to != expected.to
            || codec::encode_edge(&current.local())? != codec::encode_edge(&expected.local())?
        {
            return Err(ExperimentalError::Incomplete(expected.identity()));
        }
        let bytes = wrap_record(
            2,
            updated.to.partition,
            &codec::encode_edge(&updated.local())?,
        );
        check_record_size(&bytes)?;
        self.batch(
            &v,
            vec![WriteOp::put(
                record_key(self.partition, b"edge/", updated.id),
                bytes,
            )],
        )
    }
    pub fn verify_reverse(&self, edge: &CrossEdge) -> Result<()> {
        if edge.to.partition != self.partition {
            return Err(ExperimentalError::Invalid("target partition mismatch"));
        }
        let v = self.view()?;
        if self.node.get(&index_key(edge, Direction::In), &v)? != Some(Vec::new())
            || self.vertex_in(edge.to.id, &v)?.is_none()
        {
            return Err(ExperimentalError::Incomplete(edge.identity()));
        }
        Ok(())
    }
    /// Only observed index entries, not proof that no index entries are missing.
    pub fn observed_index(&self, id: u64, direction: Direction) -> Result<Vec<ObservedIndex>> {
        let v = self.view()?;
        if self.vertex_in(id, &v)?.is_none() {
            return Err(ExperimentalError::MissingEndpoint(VertexRef {
                partition: self.partition,
                id,
            }));
        }
        let prefix = index_prefix(self.partition, direction, id);
        let mut end = prefix.clone();
        *end.last_mut().unwrap() += 1;
        let rows = self.node.scan(
            KeyRange::new(prefix.clone(), end)?,
            ScanLimit::new(1000, 1024 * 1024)?,
            &v,
        )?;
        let mut refs = Vec::with_capacity(rows.len());
        for row in rows {
            let tail = &row.key[prefix.len()..];
            if tail.len() < 14 || !row.value.is_empty() || tail[tail.len() - 13] != b'/' {
                return Err(ExperimentalError::Corrupt("index encoding"));
            }
            let n = tail.len();
            let label = std::str::from_utf8(&tail[..n - 13])
                .map_err(|_| ExperimentalError::Corrupt("index label"))?
                .to_owned();
            let partition = u32::from_be_bytes(tail[n - 12..n - 8].try_into().unwrap());
            let edge_id = u64::from_be_bytes(tail[n - 8..].try_into().unwrap());
            validate_label(&label).map_err(|_| ExperimentalError::Corrupt("index label"))?;
            if direction == Direction::Out && partition != self.partition {
                return Err(ExperimentalError::Corrupt("outgoing owner mismatch"));
            }
            refs.push(ObservedIndex {
                edge: EdgeRef {
                    partition,
                    id: edge_id,
                },
                label,
            });
        }
        refs.sort_by_key(|r| (r.edge.id, r.edge.partition));
        Ok(refs)
    }
}
#[derive(Clone, Debug)]
pub struct ObservedIndex {
    pub edge: EdgeRef,
    pub label: String,
}
fn record_key(p: u32, kind: &[u8], id: u64) -> Vec<u8> {
    let mut k = b"graph/v2/p/".to_vec();
    k.extend(p.to_be_bytes());
    k.push(b'/');
    k.extend(kind);
    k.extend(id.to_be_bytes());
    k
}
fn index_prefix(p: u32, d: Direction, id: u64) -> Vec<u8> {
    record_key(p, if d == Direction::Out { b"out/" } else { b"in/" }, id)
        .into_iter()
        .chain(*b"/")
        .collect()
}
fn index_key(e: &CrossEdge, d: Direction) -> Vec<u8> {
    let endpoint = if d == Direction::Out { e.from } else { e.to };
    let mut k = index_prefix(endpoint.partition, d, endpoint.id);
    k.extend(e.label.as_bytes());
    k.push(b'/');
    k.extend(e.from.partition.to_be_bytes());
    k.extend(e.id.to_be_bytes());
    k
}
// v2 protobuf envelope: version=2, kind, fixed32 partition field 3,
// bytes field 4 containing the unchanged canonical v1 Vertex/Edge record.
fn wrap_record(kind: u8, p: u32, record: &[u8]) -> Vec<u8> {
    let mut b = vec![8, 2, 16, kind, 29];
    b.extend(p.to_le_bytes());
    b.push(34);
    let mut n = record.len();
    while n >= 128 {
        b.push((n as u8 & 127) | 128);
        n >>= 7;
    }
    b.push(n as u8);
    b.extend(record);
    b
}
fn unwrap_record(b: &[u8], kind: u8) -> Result<(u32, &[u8])> {
    if b.len() < 11 || b[..5] != [8, 2, 16, kind, 29] || b[9] != 34 || b.len() > 1024 * 1024 {
        return Err(ExperimentalError::Corrupt("v2 record envelope"));
    }
    let p = u32::from_le_bytes(b[5..9].try_into().unwrap());
    let mut n = 0usize;
    let mut at = 10;
    for shift in (0..28).step_by(7) {
        let byte = *b
            .get(at)
            .ok_or(ExperimentalError::Corrupt("v2 record length"))?;
        at += 1;
        n |= ((byte & 127) as usize) << shift;
        if byte < 128 {
            if n != b.len() - at || (shift > 0 && byte == 0) {
                return Err(ExperimentalError::Corrupt("v2 record length"));
            }
            return Ok((p, &b[at..]));
        }
    }
    Err(ExperimentalError::Corrupt("v2 record length"))
}

fn check_record_size(bytes: &[u8]) -> Result<()> {
    if bytes.len() > 1024 * 1024 {
        return Err(StorageNodeError::ResourceExhausted("v2 record exceeds 1 MiB".into()).into());
    }
    Ok(())
}
