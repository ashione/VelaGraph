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
            node: query::FrozenNode {
                view,
                end: end.into_std(),
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
        Ok(descriptor)
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
        let current = state
            .d1_metadata()
            .await
            .ok_or(RemoteError::SessionUnavailable)?;
        if (current.0, current.1) != (shard, session.descriptor.stamp.epoch) {
            return Err(RemoteError::SessionUnavailable);
        }
        if request.remaining.bytes > MAX_BYTES || request.remaining.work > MAX_WORK {
            return Err(RemoteError::InvalidArgument);
        }
        let permit = FRAGMENT_SLOTS
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let end = end.min(Instant::from_std(session.node.end));
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
                    .min(MAX_BYTES.saturating_sub(used.bytes)),
                work: request
                    .remaining
                    .work
                    .min(MAX_WORK.saturating_sub(used.work)),
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
                    cost: CEILING,
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
        _: KeyRange,
        _: ScanLimit,
        _: &ReadView,
        _: Option<&ScanCursor>,
    ) -> Result<ScanPage, StorageNodeError> {
        Err(StorageNodeError::Unsupported("fragment scan paging".into()))
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
}
