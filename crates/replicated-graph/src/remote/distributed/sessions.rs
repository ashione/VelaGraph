//! Data-local fixed snapshots and bounded typed fragment execution.
use super::*;
use rocksdb_sql_table::{
    experimental_graph::{
        AppendOnlyStore, CrossEdge, Direction, EdgeRecord, ExperimentalError, VertexRef,
    },
    Export, KeyRange, KeyValue, ReadView, ScanCursor, ScanLimit, ScanPage, StorageAdmin,
    StorageNode, StorageNodeError, Vertex, WriteRequest, WriteResult,
};
use std::sync::Mutex;

static SESSION_SLOTS: Semaphore = Semaphore::const_new(8);
static SESSION_BYTES: Semaphore = Semaphore::const_new(128 * 1024 * 1024);
static FRAGMENT_SLOTS: Semaphore = Semaphore::const_new(8);

struct Session {
    descriptor: Descriptor,
    hierarchy: bool,
    node: query::FrozenNode,
    used: Mutex<Cost>,
    _slot: tokio::sync::SemaphorePermit<'static>,
    _bytes: tokio::sync::SemaphorePermit<'static>,
}
pub(in crate::remote) struct Sessions {
    incarnation: [u8; 16],
    values: Mutex<BTreeMap<[u8; 16], Arc<Session>>>,
}
fn token() -> Result<[u8; 16], RemoteError> {
    let mut token = [0; 16];
    getrandom::fill(&mut token).map_err(|_| RemoteError::Unavailable)?;
    Ok(token)
}
impl Sessions {
    pub(in crate::remote) fn new() -> Result<Self, RemoteError> {
        Ok(Self {
            incarnation: token()?,
            values: Mutex::new(BTreeMap::new()),
        })
    }
    pub(in crate::remote) fn expire(&self) {
        let now = std::time::Instant::now();
        self.values.lock().unwrap().retain(|_, s| now < s.node.end);
    }
    pub(in crate::remote) async fn open(
        &self,
        state: &StateMachine,
        identity: RemoteIdentity,
        shard: u64,
        end: Instant,
    ) -> Result<Descriptor, RemoteError> {
        self.open_with_lease(state, identity, shard, (end, end), false)
            .await
            .map(|(descriptor, _)| descriptor)
    }
    pub(in crate::remote) async fn open_hierarchy(
        &self,
        state: &StateMachine,
        identity: RemoteIdentity,
        shard: u64,
        end: Instant,
    ) -> Result<(Descriptor, usize), RemoteError> {
        let now = Instant::now();
        let end = end.min(now + Duration::from_millis(MAX_TIMEOUT));
        let lease_end = now + Duration::from_millis(jobs::LEASE_MILLIS);
        tokio::time::timeout_at(
            end,
            self.open_with_lease(state, identity, shard, (end, lease_end), true),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
    }
    async fn open_with_lease(
        &self,
        state: &StateMachine,
        identity: RemoteIdentity,
        shard: u64,
        deadlines: (Instant, Instant),
        hierarchy: bool,
    ) -> Result<(Descriptor, usize), RemoteError> {
        let (end, lease_end) = deadlines;
        check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
        self.expire();
        let slot = SESSION_SLOTS
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let mut bytes = SESSION_BYTES
            .try_acquire_many(MAX_SESSION_BYTES as u32)
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let snapshot = state
            .d1_read_snapshot(MAX_SESSION_ROWS, MAX_SESSION_BYTES)
            .await
            .map_err(|_| RemoteError::ResourceExhausted)?
            .ok_or(RemoteError::Application(D1Error::NotActivated))?;
        if snapshot.shard != shard || snapshot.epoch == 0 {
            return Err(RemoteError::Protocol);
        }
        check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
        // Record dimensions belong to this immutable snapshot. They bound read
        // accounting before independent shards receive disjoint budget leases.
        let mut max_row_bytes = 0;
        if hierarchy {
            for (index, (key, value)) in snapshot.entries.iter().enumerate() {
                if index % 128 == 0 {
                    check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
                }
                max_row_bytes = max_row_bytes.max(key.len().saturating_add(value.len()));
            }
        }
        let descriptor = Descriptor {
            stamp: QueryStamp {
                identity,
                shard,
                epoch: snapshot.epoch,
                sequence: snapshot.sequence,
            },
            incarnation: self.incarnation,
            token: token()?,
        };
        let view =
            ReadView::from_entries(shard, snapshot.epoch, snapshot.sequence, snapshot.entries)
                .map_err(|_| RemoteError::ResourceExhausted)?;
        if let Some(unused) = bytes.split(MAX_SESSION_BYTES - view.payload_bytes()) {
            drop(unused);
        }
        check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
        let session = Session {
            descriptor: descriptor.clone(),
            hierarchy,
            node: query::FrozenNode {
                view,
                end: lease_end.into_std(),
            },
            used: Mutex::new(Cost::default()),
            _slot: slot,
            _bytes: bytes,
        };
        let mut values = self.values.lock().unwrap();
        if values.contains_key(&descriptor.token) {
            return Err(RemoteError::Unavailable);
        }
        values.insert(descriptor.token, Arc::new(session));
        Ok((descriptor, max_row_bytes))
    }
    fn lookup(
        &self,
        identity: RemoteIdentity,
        shard: u64,
        descriptor: &Descriptor,
    ) -> Result<Arc<Session>, RemoteError> {
        self.expire();
        if descriptor.stamp.identity != identity || descriptor.stamp.shard != shard {
            return Err(RemoteError::InvalidArgument);
        }
        if descriptor.incarnation != self.incarnation {
            return Err(RemoteError::SessionUnavailable);
        }
        let session = self
            .values
            .lock()
            .unwrap()
            .get(&descriptor.token)
            .cloned()
            .ok_or(RemoteError::SessionUnavailable)?;
        if &session.descriptor != descriptor {
            return Err(RemoteError::InvalidArgument);
        }
        Ok(session)
    }
    pub(in crate::remote) fn close(
        &self,
        identity: RemoteIdentity,
        shard: u64,
        descriptor: &Descriptor,
    ) -> Result<(), RemoteError> {
        let session = self.lookup(identity, shard, descriptor)?;
        self.values
            .lock()
            .unwrap()
            .remove(&session.descriptor.token);
        Ok(())
    }
    pub(in crate::remote) async fn fragment(
        &self,
        state: &StateMachine,
        identity: RemoteIdentity,
        shard: u64,
        request: FragmentRequest,
        end: Instant,
    ) -> Result<Reply, RemoteError> {
        request.fragment.validate()?;
        let session = self.lookup(identity, shard, &request.descriptor)?;
        let is_batch = matches!(request.fragment, Fragment::HierarchyBatch { .. });
        if session.hierarchy != is_batch {
            return Err(RemoteError::InvalidArgument);
        }
        let current = state
            .d1_metadata()
            .await
            .ok_or(RemoteError::SessionUnavailable)?;
        if (current.0, current.1) != (shard, session.descriptor.stamp.epoch) {
            return Err(RemoteError::SessionUnavailable);
        }
        let budget = if is_batch {
            jobs::JOB_CEILING
        } else if matches!(request.fragment, Fragment::Scan(_)) {
            scan::SCAN_CEILING
        } else {
            CEILING
        };
        if request.remaining.bytes > budget.bytes || request.remaining.work > budget.work {
            return Err(RemoteError::InvalidArgument);
        }
        let end = end.min(Instant::from_std(session.node.end));
        // Independent shard waves may briefly fill the process-wide worker pool.
        // Wait asynchronously within the existing deadline rather than rejecting
        // a job solely because its siblings occupied the available workers.
        let permit = if is_batch {
            tokio::time::timeout_at(end, FRAGMENT_SLOTS.acquire())
                .await
                .map_err(|_| RemoteError::Timeout { request_id: None })?
                .map_err(|_| RemoteError::ResourceExhausted)?
        } else {
            FRAGMENT_SLOTS
                .try_acquire()
                .map_err(|_| RemoteError::ResourceExhausted)?
        };
        let end = if is_batch {
            end.min(Instant::now() + Duration::from_secs(5))
        } else {
            end
        };
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // Try-lock prevents a forged concurrent client from blocking a worker indefinitely.
            let Ok(mut used) = session.used.try_lock() else {
                return Err(QueryFailure::ResourceExhausted);
            };
            let ceiling = Cost {
                bytes: request
                    .remaining
                    .bytes
                    .min(budget.bytes.saturating_sub(used.bytes)),
                work: request
                    .remaining
                    .work
                    .min(budget.work.saturating_sub(used.work)),
            };
            let node = MeteredNode {
                frozen: query::FrozenNode {
                    view: session.node.view.clone(),
                    end: end.into_std(),
                },
                cost: Arc::new(Mutex::new(Cost::default())),
                ceiling,
            };
            let run = || {
                check(end)?;
                node.charge(512, 1).map_err(query::storage_error)?;
                let value = run_fragment(&node, shard, request.fragment)?;
                let mut output = FragmentOutput {
                    descriptor: session.descriptor.clone(),
                    // Reserve maximum counter widths so replacing them with actual
                    // cost cannot grow the already-admitted encoded result.
                    cost: budget,
                    value,
                };
                let bytes = bounded_encode(&output, end)?;
                // Two encoded/decode copies and fixed overhead are charged before returning.
                node.charge(bytes.len().saturating_mul(2), 0)
                    .map_err(query::storage_error)?;
                output.cost = *node.cost.lock().unwrap();
                check(end)?;
                Ok(output)
            };
            let result = run();
            let cost = *node.cost.lock().unwrap();
            used.bytes = used.bytes.saturating_add(cost.bytes);
            used.work = used.work.saturating_add(cost.work);
            result
        })
        .await
        .map_err(|_| RemoteError::Unavailable)?;
        // Epoch changes while the worker ran invalidate the result too.
        let current = state
            .d1_metadata()
            .await
            .ok_or(RemoteError::SessionUnavailable)?;
        if (current.0, current.1) != (shard, request.descriptor.stamp.epoch) {
            return Err(RemoteError::SessionUnavailable);
        }
        Ok(Reply::QueryFragment(result))
    }
}

fn graph_error(error: ExperimentalError) -> QueryFailure {
    match error {
        ExperimentalError::Storage(e) => query::storage_error(e),
        ExperimentalError::Graph(e) => query::graph_error(e),
        _ => QueryFailure::IncompleteOrCorrupt,
    }
}
fn vertex(node: &MeteredNode, shard: u64, vertex: VertexId) -> Result<Vertex, QueryFailure> {
    AppendOnlyStore::new(node.clone(), shard, vertex.partition)
        .get_vertex(vertex.id)
        .map_err(graph_error)?
        .ok_or(QueryFailure::IncompleteOrCorrupt)
}
fn run_fragment(
    node: &MeteredNode,
    shard: u64,
    fragment: Fragment,
) -> Result<FragmentValue, QueryFailure> {
    match fragment {
        Fragment::HierarchyBatch { fragments, .. } => {
            // Validation bounds the batch and excludes nested batches. All leaves
            // share this worker's meter and the session's cumulative allowance.
            node.charge(fragments.len().saturating_mul(128), fragments.len())
                .map_err(query::storage_error)?;
            let mut values = Vec::with_capacity(fragments.len());
            let mut entries = 0usize;
            for fragment in fragments {
                node.check_execution_context()
                    .map_err(query::storage_error)?;
                let value = run_fragment(node, shard, fragment)?;
                entries += match &value {
                    FragmentValue::Edges(e) => e.len(),
                    FragmentValue::Incoming(e) => e.len(),
                    _ => 1,
                };
                if entries > jobs::MAX_PAGE_EDGES {
                    return Err(QueryFailure::ResourceExhausted);
                }
                values.push(value);
            }
            Ok(FragmentValue::HierarchyBatch(values))
        }
        Fragment::Scan(request) => run_scan_fragment(node, shard, request).map(FragmentValue::Scan),
        Fragment::Exists(v) => Ok(FragmentValue::Bool(
            AppendOnlyStore::new(node.clone(), shard, v.partition)
                .get_vertex(v.id)
                .map_err(graph_error)?
                .is_some(),
        )),
        Fragment::Out(v) => {
            let store = AppendOnlyStore::new(node.clone(), shard, v.partition);
            let entries = store
                .observed_index(v.id, Direction::Out)
                .map_err(graph_error)?;
            let mut edges = Vec::with_capacity(entries.len());
            for entry in entries {
                node.charge(256 + entry.label.len(), 1)
                    .map_err(query::storage_error)?;
                let Some(EdgeRecord::Live(edge)) =
                    store.get_edge_record(entry.edge.id).map_err(graph_error)?
                else {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                };
                if edge.from
                    != (VertexRef {
                        partition: v.partition,
                        id: v.id,
                    })
                    || edge.label != entry.label
                {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                }
                edges.push(EdgeHop {
                    id: edge.id,
                    from: v,
                    to: VertexId {
                        partition: edge.to.partition,
                        id: edge.to.id,
                    },
                    label: edge.label,
                });
            }
            Ok(FragmentValue::Edges(edges))
        }
        Fragment::In(v) => {
            let entries = AppendOnlyStore::new(node.clone(), shard, v.partition)
                .observed_index(v.id, Direction::In)
                .map_err(graph_error)?;
            let mut incoming = Vec::with_capacity(entries.len());
            for entry in entries {
                node.charge(256 + entry.label.len(), 1)
                    .map_err(query::storage_error)?;
                incoming.push(IncomingEdge {
                    id: entry.edge.id,
                    partition: entry.edge.partition,
                    label: entry.label,
                });
            }
            Ok(FragmentValue::Incoming(incoming))
        }
        Fragment::ResolveIncoming {
            target,
            edge: observed,
        } => {
            // Canonical source record validation also checks the outgoing index
            // and source endpoint. A tombstone with an observed reverse index is
            // an incomplete deletion, never an absent neighbor.
            let store = AppendOnlyStore::new(node.clone(), shard, observed.partition);
            let Some(EdgeRecord::Live(edge)) =
                store.get_edge_record(observed.id).map_err(graph_error)?
            else {
                return Err(QueryFailure::IncompleteOrCorrupt);
            };
            if edge.to
                != (VertexRef {
                    partition: target.partition,
                    id: target.id,
                })
                || edge.label != observed.label
            {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            Ok(FragmentValue::Edge(EdgeHop {
                id: edge.id,
                from: VertexId {
                    partition: edge.from.partition,
                    id: edge.from.id,
                },
                to: target,
                label: edge.label,
            }))
        }
        Fragment::VerifyReverse(e) => {
            let edge = CrossEdge {
                id: e.id,
                from: VertexRef {
                    partition: e.from.partition,
                    id: e.from.id,
                },
                to: VertexRef {
                    partition: e.to.partition,
                    id: e.to.id,
                },
                label: e.label,
                properties: BTreeMap::new(),
            };
            AppendOnlyStore::new(node.clone(), shard, edge.to.partition)
                .verify_reverse(&edge)
                .map_err(graph_error)?;
            Ok(FragmentValue::Bool(true))
        }
        Fragment::Has {
            vertex: id,
            key,
            value,
        } => {
            let v = vertex(node, shard, id)?;
            Ok(FragmentValue::Bool(match value {
                Some(value) => v.properties.get(&key) == Some(&value),
                None => v.properties.contains_key(&key),
            }))
        }
        Fragment::HasLabel { vertex: id, labels } => {
            let v = vertex(node, shard, id)?;
            Ok(FragmentValue::Bool(
                v.labels.iter().any(|l| labels.contains(l)),
            ))
        }
        Fragment::Values { vertex: id, key } => {
            let mut v = vertex(node, shard, id)?;
            Ok(FragmentValue::Value(v.properties.remove(&key)))
        }
    }
}

fn run_scan_fragment(
    node: &MeteredNode,
    shard: u64,
    mut request: scan::ScanRequest,
) -> Result<scan::ScanResult, QueryFailure> {
    let page = AppendOnlyStore::new(node.clone(), shard, request.partition)
        .vertex_page(
            request.after,
            ScanLimit {
                max_rows: scan::PAGE_ROWS,
                max_bytes: scan::PAGE_BYTES,
            },
        )
        .map_err(graph_error)?;
    let scanned = page.vertices.len();
    let mut matched = 0;
    let mut ids = Vec::new();
    for vertex in page.vertices {
        node.check_execution_context()
            .map_err(query::storage_error)?;
        if request.stopped() {
            break;
        }
        let mut keep = true;
        for step in &mut request.steps {
            node.charge(0, 1).map_err(query::storage_error)?;
            keep = match step {
                scan::ScanStep::Has { key, value } => match value {
                    Some(value) => vertex.properties.get(key) == Some(value),
                    None => vertex.properties.contains_key(key),
                },
                scan::ScanStep::HasLabel(labels) => {
                    vertex.labels.iter().any(|l| labels.contains(l))
                }
                scan::ScanStep::Limit(n) => {
                    *n -= 1;
                    true
                }
            };
            if !keep {
                break;
            }
        }
        if keep {
            matched += 1;
            if !request.count {
                node.charge(16, 0).map_err(query::storage_error)?;
                ids.push(vertex.id);
            }
        }
    }
    Ok(scan::ScanResult {
        next_after: page.next_after,
        scanned,
        matched,
        ids,
        limits: request.limits(),
    })
}

#[derive(Clone)]
struct MeteredNode {
    frozen: query::FrozenNode,
    cost: Arc<Mutex<Cost>>,
    ceiling: Cost,
}
impl MeteredNode {
    fn charge(&self, bytes: usize, work: usize) -> Result<(), StorageNodeError> {
        self.frozen.check_execution_context()?;
        self.cost
            .lock()
            .unwrap()
            .charge(bytes, work, self.ceiling)
            .map_err(|_| StorageNodeError::ResourceExhausted("distributed query budget".into()))
    }
}
impl StorageNode for MeteredNode {
    fn check_execution_context(&self) -> Result<(), StorageNodeError> {
        self.frozen.check_execution_context()
    }
    fn read_view(&self) -> Result<ReadView, StorageNodeError> {
        self.frozen.read_view()
    }
    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>, StorageNodeError> {
        self.charge(128 + key.len(), 1)?;
        let value = self.frozen.get(key, view)?;
        self.charge(value.as_ref().map_or(0, |v| v.len().saturating_mul(4)), 0)?;
        Ok(value)
    }
    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
    ) -> Result<Vec<KeyValue>, StorageNodeError> {
        self.charge(128, 1)?;
        let used = *self.cost.lock().unwrap();
        let limit = ScanLimit {
            max_bytes: limit
                .max_bytes
                .min(self.ceiling.bytes.saturating_sub(used.bytes) / 4),
            max_rows: limit
                .max_rows
                .min(self.ceiling.work.saturating_sub(used.work)),
        };
        if limit.max_bytes == 0 || limit.max_rows == 0 {
            return Err(StorageNodeError::ResourceExhausted(
                "distributed query budget".into(),
            ));
        }
        let rows = self.frozen.scan(range, limit, view)?;
        self.charge(
            rows.iter()
                .map(|r| 128 + (r.key.len() + r.value.len()) * 4)
                .sum(),
            rows.len(),
        )?;
        Ok(rows)
    }
    fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
        cursor: Option<&ScanCursor>,
    ) -> Result<ScanPage, StorageNodeError> {
        self.charge(128, 1)?;
        let used = *self.cost.lock().unwrap();
        let limit = ScanLimit {
            max_rows: limit
                .max_rows
                .min(scan::PAGE_ROWS)
                .min(self.ceiling.work.saturating_sub(used.work)),
            max_bytes: limit
                .max_bytes
                .min(scan::PAGE_BYTES)
                .min(self.ceiling.bytes.saturating_sub(used.bytes) / 4),
        };
        if limit.max_rows == 0 || limit.max_bytes == 0 {
            return Err(StorageNodeError::ResourceExhausted(
                "distributed scan budget".into(),
            ));
        }
        let page = self.frozen.scan_page(range, limit, view, cursor)?;
        self.charge(
            page.rows
                .iter()
                .map(|r| 128 + (r.key.len() + r.value.len()) * 4)
                .sum(),
            page.rows.len(),
        )?;
        Ok(page)
    }
    fn batch_write(&self, _: WriteRequest) -> Result<WriteResult, StorageNodeError> {
        Err(StorageNodeError::Unsupported("query is read only".into()))
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self
    }
}
impl StorageAdmin for MeteredNode {
    fn set_epoch(&self, _: u64) -> Result<(), StorageNodeError> {
        Err(StorageNodeError::Unsupported("query is read only".into()))
    }
    fn export(&self) -> Result<Export, StorageNodeError> {
        Err(StorageNodeError::Unsupported("query is read only".into()))
    }
    fn import(&self, _: Export) -> Result<(), StorageNodeError> {
        Err(StorageNodeError::Unsupported("query is read only".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(entries: BTreeMap<Vec<u8>, Vec<u8>>, ceiling: Cost) -> MeteredNode {
        MeteredNode {
            frozen: query::FrozenNode {
                view: ReadView::from_entries(1, 1, 1, entries).unwrap(),
                end: std::time::Instant::now() + Duration::from_secs(5),
            },
            cost: Arc::new(Mutex::new(Cost::default())),
            ceiling,
        }
    }
    #[test]
    fn pages_continue_without_truncation_and_budget_or_large_entry_fails() {
        let entries = (0..300u64)
            .map(|n| (n.to_be_bytes().to_vec(), vec![1; 10]))
            .collect();
        let node = node(entries, scan::SCAN_CEILING);
        let range = KeyRange::new(vec![0], vec![255]).unwrap();
        let limit = ScanLimit::new(128, 1024).unwrap();
        let mut cursor = None;
        let mut seen = Vec::new();
        loop {
            let page = node
                .scan_page(range.clone(), limit, &node.frozen.view, cursor.as_ref())
                .unwrap();
            assert!(page.rows.len() <= 128);
            seen.extend(
                page.rows
                    .iter()
                    .map(|r| u64::from_be_bytes(r.key.as_slice().try_into().unwrap())),
            );
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, (0..300).collect::<Vec<_>>());
        let denied = MeteredNode {
            ceiling: Cost {
                bytes: 1024,
                work: 1,
            },
            ..node.clone()
        };
        assert!(matches!(
            denied.scan_page(range.clone(), limit, &denied.frozen.view, None),
            Err(StorageNodeError::ResourceExhausted(_))
        ));
        let tiny = ScanLimit::new(128, 1).unwrap();
        assert!(matches!(
            node.scan_page(range, tiny, &node.frozen.view, None),
            Err(StorageNodeError::ResourceExhausted(_))
        ));
    }
    #[test]
    fn scan_checks_corruption_before_filters_and_later_limit() {
        let mut key = b"graph/v2/p/".to_vec();
        key.extend(1u32.to_be_bytes());
        key.extend(b"/vertex/");
        key.extend(1u64.to_be_bytes());
        for key in [key.clone(), [key.as_slice(), &[0]].concat()] {
            let node = node(BTreeMap::from([(key, vec![0])]), scan::SCAN_CEILING);
            let request = scan::ScanRequest {
                partition: 1,
                after: None,
                count: true,
                steps: vec![
                    scan::ScanStep::Has {
                        key: "missing".into(),
                        value: None,
                    },
                    scan::ScanStep::Limit(1),
                ],
            };
            assert!(matches!(
                run_scan_fragment(&node, 1, request),
                Err(QueryFailure::IncompleteOrCorrupt)
            ));
        }
    }
    #[test]
    fn reduced_scan_allowance_is_error_never_partial_success() {
        let entries = BTreeMap::from([(b"a1".to_vec(), vec![1]), (b"a2".to_vec(), vec![2])]);
        let node = node(
            entries,
            Cost {
                bytes: 10000,
                work: 2,
            },
        );
        let result = node.scan(
            KeyRange::new(b"a".to_vec(), b"b".to_vec()).unwrap(),
            ScanLimit::new(1000, 1024 * 1024).unwrap(),
            &node.frozen.view,
        );
        assert!(matches!(
            result,
            Err(StorageNodeError::ResourceExhausted(_))
        ));
    }
    #[test]
    fn repeated_reads_spend_same_lease_and_data_is_fixed() {
        let node = node(
            BTreeMap::from([(b"key".to_vec(), vec![1])]),
            Cost {
                bytes: 10000,
                work: 2,
            },
        );
        for _ in 0..2 {
            assert_eq!(node.get(b"key", &node.frozen.view).unwrap(), Some(vec![1]));
        }
        assert!(matches!(
            node.get(b"key", &node.frozen.view),
            Err(StorageNodeError::ResourceExhausted(_))
        ));
    }
    #[test]
    fn expired_or_wrong_descriptor_never_refreshes() {
        let sessions = Sessions::new().unwrap();
        let identity = RemoteIdentity { group: 1, node: 1 };
        let descriptor = Descriptor {
            stamp: QueryStamp {
                identity,
                shard: 1,
                epoch: 1,
                sequence: 1,
            },
            incarnation: sessions.incarnation,
            token: [1; 16],
        };
        assert!(matches!(
            sessions.lookup(identity, 1, &descriptor),
            Err(RemoteError::SessionUnavailable)
        ));
        let mut wrong = descriptor.clone();
        wrong.stamp.shard = 2;
        assert!(matches!(
            sessions.lookup(identity, 1, &wrong),
            Err(RemoteError::InvalidArgument)
        ));
        let mut wrong = descriptor;
        wrong.incarnation = [0; 16];
        assert!(matches!(
            sessions.lookup(identity, 1, &wrong),
            Err(RemoteError::SessionUnavailable)
        ));
    }
}

#[cfg(test)]
mod lifetime_tests {
    use super::*;
    #[test]
    fn token_bound_view_expires_closes_and_retains_admission_while_in_use() {
        let sessions = Sessions::new().unwrap();
        let identity = RemoteIdentity { group: 2, node: 1 };
        let descriptor = Descriptor {
            stamp: QueryStamp {
                identity,
                shard: 7,
                epoch: 3,
                sequence: 9,
            },
            incarnation: sessions.incarnation,
            token: [9; 16],
        };
        let session = Arc::new(Session {
            descriptor: descriptor.clone(),
            hierarchy: false,
            node: query::FrozenNode {
                view: ReadView::from_entries(7, 3, 9, BTreeMap::new()).unwrap(),
                end: std::time::Instant::now() + Duration::from_secs(1),
            },
            used: Mutex::new(Cost::default()),
            _slot: SESSION_SLOTS.try_acquire().unwrap(),
            _bytes: SESSION_BYTES.try_acquire_many(0).unwrap(),
        });
        sessions
            .values
            .lock()
            .unwrap()
            .insert(descriptor.token, session.clone());
        let mut wrong = descriptor.clone();
        wrong.stamp.sequence += 1;
        assert!(matches!(
            sessions.lookup(identity, 7, &wrong),
            Err(RemoteError::InvalidArgument)
        ));
        let retained = sessions.lookup(identity, 7, &descriptor).unwrap();
        sessions.close(identity, 7, &descriptor).unwrap();
        assert!(matches!(
            sessions.lookup(identity, 7, &descriptor),
            Err(RemoteError::SessionUnavailable)
        ));
        // Closing removes discoverability but cannot free the worker's retained snapshot/permits.
        let weak = Arc::downgrade(&session);
        drop(session);
        assert!(weak.upgrade().is_some());
        drop(retained);
        assert!(weak.upgrade().is_none());
        let expired = Arc::new(Session {
            descriptor: descriptor.clone(),
            hierarchy: false,
            node: query::FrozenNode {
                view: ReadView::from_entries(7, 3, 9, BTreeMap::new()).unwrap(),
                end: std::time::Instant::now() - Duration::from_millis(1),
            },
            used: Mutex::new(Cost::default()),
            _slot: SESSION_SLOTS.try_acquire().unwrap(),
            _bytes: SESSION_BYTES.try_acquire_many(0).unwrap(),
        });
        sessions
            .values
            .lock()
            .unwrap()
            .insert(descriptor.token, expired);
        assert!(matches!(
            sessions.lookup(identity, 7, &descriptor),
            Err(RemoteError::SessionUnavailable)
        ));
        assert!(sessions.values.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod epoch_tests {
    use super::*;
    use openraft::{storage::RaftStateMachine, CommittedLeaderId, Entry, EntryPayload, LogId};
    async fn apply(state: &mut Arc<StateMachine>, index: u64, command: D1Command) {
        state
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
                payload: EntryPayload::Normal(LogCommand::D1V1(command)),
            }])
            .await
            .unwrap()[0]
            .d1
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap();
    }
    #[tokio::test]
    async fn hierarchy_lease_is_fixed_and_batches_share_cumulative_budget() {
        let mut state = Arc::new(StateMachine::default());
        apply(&mut state, 1, D1Command::Activate { shard: 7, epoch: 1 }).await;
        let identity = RemoteIdentity { group: 2, node: 1 };
        let sessions = Sessions::new().unwrap();
        let start = Instant::now();
        let end = start + Duration::from_secs(3);
        let (descriptor, _) = sessions
            .open_hierarchy(&state, identity, 7, end)
            .await
            .unwrap();
        let session = sessions.lookup(identity, 7, &descriptor).unwrap();
        let lease_end = session.node.end;
        assert!(lease_end >= (start + Duration::from_millis(jobs::LEASE_MILLIS)).into_std());
        assert!(lease_end > end.into_std());
        let leaf = Fragment::Exists(VertexId {
            partition: 0,
            id: 1,
        });
        let batch = Fragment::HierarchyBatch {
            partition: 0,
            fragments: vec![leaf.clone(), leaf.clone()],
        };
        let request = FragmentRequest {
            descriptor: descriptor.clone(),
            fragment: batch.clone(),
            remaining: jobs::JOB_CEILING,
        };
        let mut previous_work = 0;
        for _ in 0..2 {
            let reply = sessions
                .fragment(&state, identity, 7, request.clone(), end)
                .await
                .unwrap();
            let Reply::QueryFragment(Ok(FragmentOutput {
                value: FragmentValue::HierarchyBatch(values),
                ..
            })) = reply
            else {
                panic!("expected successful hierarchy batch");
            };
            assert_eq!(values.len(), 2);
            assert!(values
                .iter()
                .all(|v| matches!(v, FragmentValue::Bool(false))));
            let work = session.used.lock().unwrap().work;
            assert!(work > previous_work);
            previous_work = work;
            assert_eq!(session.node.end, lease_end);
        }
        // Neither a different request kind nor a generous allowance can turn
        // an ordinary token into a hierarchy token (or vice versa).
        let ordinary = sessions.open(&state, identity, 7, end).await.unwrap();
        assert_eq!(
            sessions.lookup(identity, 7, &ordinary).unwrap().node.end,
            end.into_std()
        );
        for request in [
            FragmentRequest {
                fragment: leaf,
                ..request.clone()
            },
            FragmentRequest {
                descriptor: ordinary.clone(),
                ..request.clone()
            },
            FragmentRequest {
                fragment: Fragment::HierarchyBatch {
                    partition: 0,
                    fragments: vec![],
                },
                ..request.clone()
            },
            FragmentRequest {
                fragment: Fragment::HierarchyBatch {
                    partition: 0,
                    fragments: vec![batch],
                },
                ..request.clone()
            },
            FragmentRequest {
                fragment: Fragment::HierarchyBatch {
                    partition: 1,
                    fragments: vec![Fragment::Exists(VertexId {
                        partition: 0,
                        id: 1,
                    })],
                },
                ..request.clone()
            },
        ] {
            assert!(matches!(
                sessions.fragment(&state, identity, 7, request, end).await,
                Err(RemoteError::InvalidArgument)
            ));
        }
        // Exhaustion remains cumulative even if the caller resubmits the full allowance.
        *session.used.lock().unwrap() = jobs::JOB_CEILING;
        assert!(matches!(
            sessions
                .fragment(&state, identity, 7, request, end)
                .await
                .unwrap(),
            Reply::QueryFragment(Err(QueryFailure::ResourceExhausted))
        ));
        assert_eq!(session.node.end, lease_end);
        drop(session);
        {
            let mut values = sessions.values.lock().unwrap();
            Arc::get_mut(values.get_mut(&descriptor.token).unwrap())
                .unwrap()
                .node
                .end = std::time::Instant::now() - Duration::from_millis(1);
        }
        assert!(matches!(
            sessions.lookup(identity, 7, &descriptor),
            Err(RemoteError::SessionUnavailable)
        ));
        assert!(matches!(
            sessions
                .open_hierarchy(
                    &state,
                    identity,
                    7,
                    Instant::now() - Duration::from_millis(1)
                )
                .await,
            Err(RemoteError::Timeout { .. })
        ));
        sessions.close(identity, 7, &ordinary).unwrap();
    }
    #[tokio::test]
    async fn fragment_lease_fencing_and_previous_incarnation_fail_closed() {
        let mut state = Arc::new(StateMachine::default());
        apply(&mut state, 1, D1Command::Activate { shard: 7, epoch: 1 }).await;
        let identity = RemoteIdentity { group: 2, node: 1 };
        let sessions = Sessions::new().unwrap();
        let end = Instant::now() + Duration::from_secs(3);
        let descriptor = sessions.open(&state, identity, 7, end).await.unwrap();
        let request = FragmentRequest {
            descriptor: descriptor.clone(),
            fragment: Fragment::Exists(VertexId {
                partition: 0,
                id: 1,
            }),
            remaining: CEILING,
        };
        let reply = sessions
            .fragment(&state, identity, 7, request.clone(), end)
            .await
            .unwrap();
        assert!(matches!(
            reply,
            Reply::QueryFragment(Ok(FragmentOutput {
                value: FragmentValue::Bool(false),
                ..
            }))
        ));
        let exhausted = FragmentRequest {
            remaining: Cost { bytes: 0, work: 0 },
            ..request.clone()
        };
        assert!(matches!(
            sessions
                .fragment(&state, identity, 7, exhausted, end)
                .await
                .unwrap(),
            Reply::QueryFragment(Err(QueryFailure::ResourceExhausted))
        ));
        let restarted = Sessions::new().unwrap();
        assert!(matches!(
            restarted
                .fragment(&state, identity, 7, request.clone(), end)
                .await,
            Err(RemoteError::SessionUnavailable)
        ));
        apply(
            &mut state,
            2,
            D1Command::SetEpoch {
                shard: 7,
                expected_epoch: 1,
                new_epoch: 2,
            },
        )
        .await;
        assert!(matches!(
            sessions.fragment(&state, identity, 7, request, end).await,
            Err(RemoteError::SessionUnavailable)
        ));
        sessions.close(identity, 7, &descriptor).unwrap();
    }
    #[tokio::test]
    async fn hierarchy_five_leaf_actual_costs_fit_snapshot_reservations() {
        // Build valid graph bytes off the async executor, using a disposable local DB.
        // Adapter exists only to let AppendOnlyStore share ownership of that DB.
        #[derive(Clone)]
        struct FixtureNode(Arc<rocksdb_sql_table::LocalStorageNode>);
        impl StorageNode for FixtureNode {
            fn read_view(&self) -> Result<ReadView, StorageNodeError> {
                self.0.read_view()
            }
            fn get(
                &self,
                key: &[u8],
                view: &ReadView,
            ) -> Result<Option<Vec<u8>>, StorageNodeError> {
                self.0.get(key, view)
            }
            fn scan(
                &self,
                range: KeyRange,
                limit: ScanLimit,
                view: &ReadView,
            ) -> Result<Vec<KeyValue>, StorageNodeError> {
                self.0.scan(range, limit, view)
            }
            fn batch_write(&self, request: WriteRequest) -> Result<WriteResult, StorageNodeError> {
                self.0.batch_write(request)
            }
            fn batch_write_conditional(
                &self,
                request: WriteRequest,
                sequence: Option<u64>,
            ) -> Result<WriteResult, StorageNodeError> {
                self.0.batch_write_conditional(request, sequence)
            }
            fn admin(&self) -> &dyn StorageAdmin {
                self.0.admin()
            }
        }
        let (mut entries, label) = tokio::task::spawn_blocking(|| {
            let dir = tempfile::tempdir().unwrap();
            let node = FixtureNode(Arc::new(
                rocksdb_sql_table::LocalStorageNode::open(dir.path(), 7).unwrap(),
            ));
            node.admin().set_epoch(1).unwrap();
            let store = AppendOnlyStore::new(node.clone(), 7, 0);
            let mut source = Vertex::new(1);
            source.properties.insert(
                "payload".into(),
                rocksdb_sql_table::Value::Text("v".repeat(16 * 1024)),
            );
            for vertex in [source, Vertex::new(2)] {
                store
                    .prepare_vertex(&vertex)
                    .unwrap()
                    .submit(&node)
                    .unwrap();
            }
            let label = "L".repeat(64); // maximum legal graph label, not an invalid giant label
            for id in 10..18 {
                let edge = CrossEdge {
                    id,
                    from: VertexRef {
                        partition: 0,
                        id: 1,
                    },
                    to: VertexRef {
                        partition: 0,
                        id: 2,
                    },
                    label: label.clone(),
                    properties: BTreeMap::new(),
                };
                store.prepare_source(&edge).unwrap().submit(&node).unwrap();
                store.prepare_reverse(&edge).unwrap().submit(&node).unwrap();
            }
            let entries: BTreeMap<_, _> = node
                .admin()
                .export()
                .unwrap()
                .entries
                .into_iter()
                .map(|row| (row.key, row.value))
                .collect();
            // All DB owners are dropped before TempDir cleanup.
            drop(store);
            drop(node);
            (entries, label)
        })
        .await
        .unwrap();

        // The maximum is deliberately an uninterpreted/malformed raw row outside
        // graph prefixes. OpenHierarchy must include it, not decode/filter it away.
        let raw_key = b"uninterpreted/invalid-record".to_vec();
        entries.insert(raw_key.clone(), vec![0xff; 32 * 1024]);
        let expected_row_bound = entries
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .max()
            .unwrap();
        assert_eq!(expected_row_bound, raw_key.len() + 32 * 1024);

        let mut state = Arc::new(StateMachine::default());
        apply(&mut state, 1, D1Command::Activate { shard: 7, epoch: 1 }).await;
        apply(
            &mut state,
            2,
            D1Command::Write(d2_raft::D1WriteRequest {
                shard: 7,
                epoch: 1,
                request_id: [42; 16],
                expected_sequence: None,
                operations: entries
                    .iter()
                    .map(|(k, v)| d2_raft::WriteOp::put(k, v))
                    .collect(),
            }),
        )
        .await;
        let identity = RemoteIdentity { group: 2, node: 1 };
        let sessions = Sessions::new().unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        let (descriptor, row_bound) = sessions
            .open_hierarchy(&state, identity, 7, end)
            .await
            .unwrap();
        assert_eq!(row_bound, expected_row_bound);
        let pinned = sessions.lookup(identity, 7, &descriptor).unwrap();

        // A subsequent oversized live row must not alter the advertised bound or
        // any read performed through the already-open session.
        apply(
            &mut state,
            3,
            D1Command::Write(d2_raft::D1WriteRequest {
                shard: 7,
                epoch: 1,
                request_id: [43; 16],
                expected_sequence: None,
                operations: vec![d2_raft::WriteOp::put(b"later-live-row", vec![1; 64 * 1024])],
            }),
        )
        .await;
        assert!(pinned.node.view.get(b"later-live-row").is_none());
        let source = VertexId {
            partition: 0,
            id: 1,
        };
        let target = VertexId {
            partition: 0,
            id: 2,
        };
        let edge = EdgeHop {
            id: 10,
            from: source,
            to: target,
            label: label.clone(),
        };
        let leaves = [
            Fragment::Exists(source),
            Fragment::In(target),
            Fragment::Out(source),
            Fragment::VerifyReverse(edge),
            Fragment::ResolveIncoming {
                target,
                edge: IncomingEdge {
                    id: 10,
                    partition: 0,
                    label,
                },
            },
        ];
        for (kind, leaf) in leaves.into_iter().enumerate() {
            let fragment = Fragment::HierarchyBatch {
                partition: 0,
                fragments: vec![leaf],
            };
            let bound =
                jobs::hierarchy_reservation_for_test(fragment.clone(), row_bound, &descriptor)
                    .expect("finite reservation");
            assert!(bound.bytes <= jobs::JOB_CEILING.bytes);
            assert!(bound.work <= jobs::JOB_CEILING.work);
            // Reproduce Execution's actual pre-transport charges, using the bound
            // as the local ceiling. This catches missing request overhead too.
            let mut total = Cost::default();
            total.charge(512, 1, bound).unwrap();
            let mut request = FragmentRequest {
                descriptor: descriptor.clone(),
                fragment,
                remaining: Cost {
                    bytes: bound.bytes - total.bytes,
                    work: bound.work - total.work,
                },
            };
            let bytes = serde_json::to_vec(&request).unwrap();
            total.charge(bytes.len() * 2, 0, bound).unwrap();
            request.remaining = Cost {
                bytes: bound.bytes - total.bytes,
                work: bound.work - total.work,
            };
            let before = *pinned.used.lock().unwrap();
            let Reply::QueryFragment(Ok(output)) = sessions
                .fragment(&state, identity, 7, request, end)
                .await
                .unwrap()
            else {
                panic!("leaf {kind} must succeed with its reservation");
            };
            assert_eq!(output.descriptor, descriptor);
            assert!(output.cost.bytes > 0 && output.cost.work > 0);
            total
                .charge(output.cost.bytes, output.cost.work, bound)
                .unwrap();
            let after = *pinned.used.lock().unwrap();
            assert_eq!(after.bytes - before.bytes, output.cost.bytes);
            assert_eq!(after.work - before.work, output.cost.work);
            let FragmentValue::HierarchyBatch(values) = output.value else {
                panic!("batch expected");
            };
            assert_eq!(values.len(), 1);
            match (&values[0], kind) {
                (FragmentValue::Bool(true), 0 | 3) => {}
                (FragmentValue::Incoming(edges), 1) => assert_eq!(edges.len(), 8),
                (FragmentValue::Edges(edges), 2) => assert_eq!(edges.len(), 8),
                (FragmentValue::Edge(edge), 4) => assert_eq!(edge.id, 10),
                _ => panic!("wrong result for leaf {kind}"),
            }
        }
        sessions.close(identity, 7, &descriptor).unwrap();
    }
}
