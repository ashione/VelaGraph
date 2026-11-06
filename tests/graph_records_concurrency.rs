//! Deterministic races between independently locked graph stores sharing a node.
//! Gates stop each mutation after its snapshot validation but before submission.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use rocksdb_sql_table::{
    Edge, Error, Export, GraphRecordStore, KeyRange, KeyValue, LocalStorageNode, ReadView,
    RequestId, ScanLimit, Sequence, StorageAdmin, StorageNode, StorageNodeError, Value, Vertex,
    WriteOp, WriteRequest, WriteResult,
};
use tempfile::TempDir;

// Only a deadlock guard: channels, not elapsed time, determine race ordering.
const GATE_TIMEOUT: Duration = Duration::from_secs(30);

type NodeResult<T> = std::result::Result<T, StorageNodeError>;

struct Gate {
    arrived: mpsc::Sender<(Option<Sequence>, RequestId)>,
    release: mpsc::Receiver<()>,
}

struct ControlledNode {
    inner: Arc<LocalStorageNode>,
    gate: Mutex<Option<Gate>>,
}

impl ControlledNode {
    fn plain(inner: &Arc<LocalStorageNode>) -> Self {
        Self {
            inner: Arc::clone(inner),
            gate: Mutex::new(None),
        }
    }
}

impl StorageNode for ControlledNode {
    fn read_view(&self) -> NodeResult<ReadView> {
        self.inner.read_view()
    }

    fn get(&self, key: &[u8], view: &ReadView) -> NodeResult<Option<Vec<u8>>> {
        self.inner.get(key, view)
    }

    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
    ) -> NodeResult<Vec<KeyValue>> {
        self.inner.scan(range, limit, view)
    }

    fn batch_write(&self, request: WriteRequest) -> NodeResult<WriteResult> {
        self.inner.batch_write(request)
    }

    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        expected_sequence: Option<Sequence>,
    ) -> NodeResult<WriteResult> {
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.arrived
                .send((expected_sequence, request.request_id))
                .unwrap();
            gate.release.recv_timeout(GATE_TIMEOUT).unwrap();
        }
        self.inner
            .batch_write_conditional(request, expected_sequence)
    }

    fn admin(&self) -> &dyn StorageAdmin {
        self.inner.admin()
    }
}

#[derive(Clone)]
enum Mutation {
    CreateVertex(Vertex),
    CreateEdge(Edge),
    UpdateEdge(Edge),
    DeleteVertex(u64),
}

impl Mutation {
    fn apply(&self, store: &GraphRecordStore<ControlledNode>) -> rocksdb_sql_table::Result<()> {
        match self {
            Self::CreateVertex(vertex) => store.create_vertex(vertex),
            Self::CreateEdge(edge) => store.create_edge(edge),
            Self::UpdateEdge(edge) => store.update_edge(edge),
            Self::DeleteVertex(id) => store.delete_vertex(*id),
        }
    }
}

fn store(node: &Arc<LocalStorageNode>) -> GraphRecordStore<ControlledNode> {
    GraphRecordStore::new(ControlledNode::plain(node), 7, 0)
}

fn assert_conflict(error: Error, expected: Sequence) {
    assert!(
        matches!(
            error,
            Error::StorageNode(StorageNodeError::Conflict { expected: seen, current })
                if seen == expected && current == expected + 1
        ),
        "expected sequence conflict at {expected}, got {error:?}"
    );
}

struct Race {
    winner: Export,
    retry: rocksdb_sql_table::Result<()>,
    after_retry: Export,
}

/// Both calls finish validation before either commits. Only the designated
/// winner is released first; the loser cannot submit until that commit finishes.
fn race(node: &Arc<LocalStorageNode>, mutations: [Mutation; 2], winner: usize) -> Race {
    let before = node.admin().export().unwrap();
    let stores = [store(node), store(node)];
    let mut arrivals = Vec::new();
    let mut releases = Vec::new();
    for store in &stores {
        let (arrived, arrival) = mpsc::channel();
        let (release, released) = mpsc::channel();
        *store.node().gate.lock().unwrap() = Some(Gate {
            arrived,
            release: released,
        });
        arrivals.push(arrival);
        releases.push(release);
    }
    let winner_export = std::thread::scope(|scope| {
        let first = scope.spawn(|| mutations[0].apply(&stores[0]));
        let second = scope.spawn(|| mutations[1].apply(&stores[1]));
        let first_submission = arrivals[0].recv_timeout(GATE_TIMEOUT).unwrap();
        let second_submission = arrivals[1].recv_timeout(GATE_TIMEOUT).unwrap();
        let (winning_call, losing_call) = if winner == 0 {
            (first, second)
        } else {
            (second, first)
        };
        releases[winner].send(()).unwrap();
        let winning_result = winning_call.join();
        let committed = node.admin().export();
        releases[1 - winner].send(()).unwrap();
        let losing_result = losing_call.join();
        // Release both workers before assertions so a failure cannot strand one.
        winning_result.unwrap().unwrap();
        let committed = committed.unwrap();
        assert_eq!(first_submission.0, Some(before.sequence));
        assert_eq!(second_submission.0, Some(before.sequence));
        // Identical mutations from different stores must not deduplicate each other.
        assert_ne!(first_submission.1, second_submission.1);
        assert_eq!(committed.sequence, before.sequence + 1);
        assert_conflict(losing_result.unwrap().unwrap_err(), before.sequence);
        assert_eq!(node.admin().export().unwrap(), committed);
        committed
    });
    // Retry the same high-level call on the same store, forcing fresh validation.
    let retry = mutations[1 - winner].apply(&stores[1 - winner]);
    let after_retry = node.admin().export().unwrap();
    Race {
        winner: winner_export,
        retry,
        after_retry,
    }
}

#[test]
fn endpoint_delete_and_edge_create_conflict_in_both_orders() {
    for winner in 0..2 {
        let dir = TempDir::new().unwrap();
        let node = Arc::new(LocalStorageNode::open(dir.path(), 7).unwrap());
        let graph = store(&node);
        for id in 1..=2 {
            graph.create_vertex(&Vertex::new(id)).unwrap();
        }
        let result = race(
            &node,
            [
                Mutation::DeleteVertex(1),
                Mutation::CreateEdge(Edge::new(10, 1, 2, "knows")),
            ],
            winner,
        );
        if winner == 0 {
            assert!(matches!(result.retry, Err(Error::EdgeEndpointNotFound(1))));
            assert_eq!(result.after_retry, result.winner);
        } else {
            result.retry.unwrap();
            assert_eq!(result.after_retry.sequence, result.winner.sequence + 1);
        }
        assert!(graph.get_vertex(1).unwrap().is_none());
        assert!(graph.get_vertex(2).unwrap().is_some());
        assert!(graph.get_edge(10).unwrap().is_none());
        assert!(graph.out_edges(2, None, 10).unwrap().is_empty());
        assert!(graph.in_edges(2, None, 10).unwrap().is_empty());
        assert_eq!(result.after_retry.entries.len(), 1);
    }
}

#[test]
fn same_vertex_id_creations_conflict_even_with_identical_values() {
    for identical in [true, false] {
        for winner in 0..2 {
            let dir = TempDir::new().unwrap();
            let node = Arc::new(LocalStorageNode::open(dir.path(), 7).unwrap());
            let first = Vertex::new(1);
            let mut second = first.clone();
            if !identical {
                second.properties.insert("value".into(), Value::Integer(2));
            }
            let vertices = [first, second];
            let result = race(&node, vertices.clone().map(Mutation::CreateVertex), winner);
            assert!(matches!(result.retry, Err(Error::VertexAlreadyExists(1))));
            assert_eq!(result.after_retry, result.winner);
            assert_eq!(
                store(&node).get_vertex(1).unwrap(),
                Some(vertices[winner].clone())
            );
            assert_eq!(result.after_retry.entries.len(), 1);
        }
    }
}

#[test]
fn same_edge_id_creations_conflict_even_with_identical_values() {
    for identical in [true, false] {
        for winner in 0..2 {
            let dir = TempDir::new().unwrap();
            let node = Arc::new(LocalStorageNode::open(dir.path(), 7).unwrap());
            let graph = store(&node);
            for id in 1..=3 {
                graph.create_vertex(&Vertex::new(id)).unwrap();
            }
            let first = Edge::new(10, 1, 2, "knows");
            let second = if identical {
                first.clone()
            } else {
                Edge::new(10, 1, 3, "likes")
            };
            let edges = [first, second];
            let result = race(&node, edges.clone().map(Mutation::CreateEdge), winner);
            assert!(matches!(result.retry, Err(Error::EdgeAlreadyExists(10))));
            assert_eq!(result.after_retry, result.winner);
            assert_eq!(graph.get_edge(10).unwrap(), Some(edges[winner].clone()));
            assert_eq!(
                graph.out_edges(1, None, 10).unwrap(),
                vec![edges[winner].clone()]
            );
            for endpoint in [2, 3] {
                let expected = if edges[winner].to == endpoint {
                    vec![edges[winner].clone()]
                } else {
                    vec![]
                };
                assert_eq!(graph.in_edges(endpoint, None, 10).unwrap(), expected);
            }
            assert_eq!(result.after_retry.entries.len(), 6);
        }
    }
}

#[test]
fn edge_label_and_endpoint_updates_conflict_with_cascade_in_both_orders() {
    // A label change remains incident; moving the endpoint away must survive a
    // retried cascade. Together these catch stale deletion and stale adjacency.
    for move_endpoint in [false, true] {
        for winner in 0..2 {
            let dir = TempDir::new().unwrap();
            let node = Arc::new(LocalStorageNode::open(dir.path(), 7).unwrap());
            let graph = store(&node);
            for id in 1..=3 {
                graph.create_vertex(&Vertex::new(id)).unwrap();
            }
            graph.create_edge(&Edge::new(10, 1, 2, "knows")).unwrap();
            let updated = Edge::new(10, if move_endpoint { 3 } else { 1 }, 2, "likes");
            let result = race(
                &node,
                [
                    Mutation::DeleteVertex(1),
                    Mutation::UpdateEdge(updated.clone()),
                ],
                winner,
            );
            if winner == 0 {
                assert!(matches!(result.retry, Err(Error::EdgeNotFound(10))));
                assert_eq!(result.after_retry, result.winner);
            } else {
                result.retry.unwrap();
                assert_eq!(result.after_retry.sequence, result.winner.sequence + 1);
            }
            assert!(graph.get_vertex(1).unwrap().is_none());
            let remaining = if winner == 1 && move_endpoint {
                vec![updated]
            } else {
                vec![]
            };
            assert_eq!(graph.get_edge(10).unwrap(), remaining.first().cloned());
            assert_eq!(graph.in_edges(2, None, 10).unwrap(), remaining);
            assert_eq!(graph.out_edges(3, None, 10).unwrap(), remaining);
            assert_eq!(result.after_retry.entries.len(), 2 + remaining.len() * 3);
        }
    }
}

#[test]
fn unrelated_raw_writer_conflicts_and_graph_retry_rereads() {
    let dir = TempDir::new().unwrap();
    let node = Arc::new(LocalStorageNode::open(dir.path(), 7).unwrap());
    let graph = store(&node);
    let (arrived, arrival) = mpsc::channel();
    let (release, released) = mpsc::channel();
    *graph.node().gate.lock().unwrap() = Some(Gate {
        arrived,
        release: released,
    });
    let before = node.read_view().unwrap();
    std::thread::scope(|scope| {
        let mutation = scope.spawn(|| graph.create_vertex(&Vertex::new(1)));
        let (expected, _) = arrival.recv_timeout(GATE_TIMEOUT).unwrap();
        let raw_result = node.batch_write(WriteRequest {
            shard: 7,
            epoch: before.epoch,
            request_id: [42; 16],
            operations: vec![WriteOp::put(b"unrelated".to_vec(), b"bytes".to_vec())],
        });
        let committed = node.admin().export();
        release.send(()).unwrap();
        let result = mutation.join();
        raw_result.unwrap();
        let committed = committed.unwrap();
        assert_eq!(expected, Some(before.sequence));
        assert_conflict(result.unwrap().unwrap_err(), before.sequence);
        assert_eq!(node.admin().export().unwrap(), committed);
        assert!(graph.get_vertex(1).unwrap().is_none());
    });
    graph.create_vertex(&Vertex::new(1)).unwrap();
    let view = node.read_view().unwrap();
    assert_eq!(view.sequence, before.sequence + 2);
    assert_eq!(
        node.get(b"unrelated", &view).unwrap(),
        Some(b"bytes".to_vec())
    );
    assert_eq!(node.admin().export().unwrap().entries.len(), 2);
    assert_eq!(graph.get_vertex(1).unwrap(), Some(Vertex::new(1)));
}

// Deliberately does not override the new conditional method. Existing adapters
// must fail closed, rather than silently doing an unconditional graph write.
struct LegacyNode {
    inner: Arc<LocalStorageNode>,
    unconditional_calls: AtomicUsize,
}

impl StorageNode for LegacyNode {
    fn read_view(&self) -> NodeResult<ReadView> {
        self.inner.read_view()
    }
    fn get(&self, key: &[u8], view: &ReadView) -> NodeResult<Option<Vec<u8>>> {
        self.inner.get(key, view)
    }
    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
    ) -> NodeResult<Vec<KeyValue>> {
        self.inner.scan(range, limit, view)
    }
    fn batch_write(&self, request: WriteRequest) -> NodeResult<WriteResult> {
        self.unconditional_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.batch_write(request)
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self.inner.admin()
    }
}

#[test]
fn legacy_adapter_rejects_graph_write_without_unconditional_fallback() {
    let dir = TempDir::new().unwrap();
    let node = Arc::new(LocalStorageNode::open(dir.path(), 7).unwrap());
    let before = node.admin().export().unwrap();
    let graph = GraphRecordStore::new(
        LegacyNode {
            inner: Arc::clone(&node),
            unconditional_calls: AtomicUsize::new(0),
        },
        7,
        0,
    );
    assert!(matches!(
        graph.create_vertex(&Vertex::new(1)),
        Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
    ));
    assert_eq!(node.admin().export().unwrap(), before);
    assert_eq!(graph.node().unconditional_calls.load(Ordering::SeqCst), 0);

    // Existing callers retain unconditional behavior through the additive API.
    let result = graph
        .node()
        .batch_write_conditional(
            WriteRequest {
                shard: 7,
                epoch: before.epoch,
                request_id: [73; 16],
                operations: vec![WriteOp::put(b"legacy".to_vec(), b"value".to_vec())],
            },
            None,
        )
        .unwrap();
    assert_eq!(graph.node().unconditional_calls.load(Ordering::SeqCst), 1);
    assert_eq!(result.status, rocksdb_sql_table::WriteStatus::Committed);
    assert_eq!(result.sequence, before.sequence + 1);
    assert_eq!(result.applied_operations, 1);
    let after = node.admin().export().unwrap();
    assert_eq!(after.sequence, result.sequence);
    assert_eq!(
        after.entries,
        vec![KeyValue {
            key: b"legacy".to_vec(),
            value: b"value".to_vec()
        }]
    );
}
