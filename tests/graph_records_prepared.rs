use rocksdb_sql_table::{
    Edge, Error, GraphRecordStore, LocalStorageNode, StorageNode, StorageNodeError, Value, Vertex,
};
use tempfile::TempDir;

fn store(dir: &TempDir) -> GraphRecordStore<LocalStorageNode> {
    GraphRecordStore::new(LocalStorageNode::open(dir.path(), 7).unwrap(), 7, 0)
}

#[test]
fn preparation_is_read_only_and_retry_survives_reopen_and_later_writes() {
    let dir = TempDir::new().unwrap();
    let graph = store(&dir);
    let before = graph.node().read_view().unwrap().sequence;
    let prepared = graph.prepare_create_vertex(&Vertex::new(1)).unwrap();
    assert_eq!(prepared.shard(), 7);
    assert_eq!(prepared.partition(), 0);
    assert_eq!(prepared.epoch(), graph.node().read_view().unwrap().epoch);
    assert_eq!(prepared.expected_sequence(), before);
    assert!(graph.get_vertex(1).unwrap().is_none());
    assert_eq!(graph.node().read_view().unwrap().sequence, before);
    graph.commit_prepared(&prepared).unwrap();
    graph.create_vertex(&Vertex::new(2)).unwrap();
    let committed = graph.node().read_view().unwrap().sequence;
    drop(graph);

    let graph = store(&dir);
    let retry = prepared.clone();
    assert_eq!(retry.request_id(), prepared.request_id());
    assert_eq!(retry, prepared);
    graph.commit_prepared(&retry).unwrap();
    assert_eq!(graph.node().read_view().unwrap().sequence, committed);
    assert_eq!(graph.get_vertex(1).unwrap(), Some(Vertex::new(1)));
}

#[test]
fn conflict_preserves_original_condition_and_does_not_mutate() {
    let dir = TempDir::new().unwrap();
    let graph = store(&dir);
    let prepared = graph.prepare_create_vertex(&Vertex::new(1)).unwrap();
    let independent = graph.prepare_create_vertex(&Vertex::new(1)).unwrap();
    assert_ne!(prepared.request_id(), independent.request_id());
    graph.create_vertex(&Vertex::new(2)).unwrap();
    let before = graph.node().admin().export().unwrap();
    for _ in 0..2 {
        assert!(matches!(
            graph.commit_prepared(&prepared),
            Err(Error::StorageNode(StorageNodeError::Conflict { expected, current }))
                if expected == prepared.expected_sequence() && current == before.sequence
        ));
    }
    let after = graph.node().admin().export().unwrap();
    assert_eq!(before, after);
    assert!(graph.get_vertex(1).unwrap().is_none());
    let refreshed = graph.prepare_create_vertex(&Vertex::new(1)).unwrap();
    assert_ne!(refreshed.request_id(), prepared.request_id());
    graph.commit_prepared(&refreshed).unwrap();
}

#[test]
fn rejects_wrong_shard_or_partition_without_writing() {
    let dir = TempDir::new().unwrap();
    let graph = store(&dir);
    let prepared = graph.prepare_create_vertex(&Vertex::new(1)).unwrap();
    for (shard, partition) in [(7, 1), (8, 0)] {
        let target_dir = TempDir::new().unwrap();
        let target = GraphRecordStore::new(
            LocalStorageNode::open(target_dir.path(), shard).unwrap(),
            shard,
            partition,
        );
        let before = target.node().admin().export().unwrap();
        assert!(matches!(
            target.commit_prepared(&prepared),
            Err(Error::StorageNode(StorageNodeError::InvalidArgument(_)))
        ));
        assert_eq!(target.node().admin().export().unwrap(), before);
    }
}

#[test]
fn retry_never_refreshes_epoch_even_after_prior_commit() {
    let dir = TempDir::new().unwrap();
    let graph = store(&dir);
    let prepared = graph.prepare_create_vertex(&Vertex::new(1)).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    graph
        .node()
        .admin()
        .set_epoch(prepared.epoch() + 1)
        .unwrap();
    let before = graph.node().admin().export().unwrap();
    assert!(matches!(
        graph.commit_prepared(&prepared),
        Err(Error::StorageNode(StorageNodeError::Fenced { epoch, current, .. }))
            if epoch == prepared.epoch() && current == epoch + 1
    ));
    assert_eq!(graph.node().admin().export().unwrap(), before);
}

#[test]
fn all_prepared_crud_operations_preserve_adjacency_and_deduplicate() {
    let dir = TempDir::new().unwrap();
    let graph = store(&dir);
    for id in 1..=3 {
        let prepared = graph.prepare_create_vertex(&Vertex::new(id)).unwrap();
        graph.commit_prepared(&prepared).unwrap();
        graph.commit_prepared(&prepared).unwrap();
    }
    let mut updated = Vertex::new(1);
    updated
        .properties
        .insert("name".into(), Value::Text("one".into()));
    let prepared = graph.prepare_update_vertex(&updated).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    assert_eq!(graph.get_vertex(1).unwrap(), Some(updated));

    let edge = Edge::new(10, 1, 2, "knows");
    let prepared = graph.prepare_create_edge(&edge).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    assert_eq!(graph.out_edges(1, None, 10).unwrap(), vec![edge]);
    let edge = Edge::new(10, 3, 1, "likes");
    let prepared = graph.prepare_update_edge(&edge).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    assert!(graph.out_edges(1, None, 10).unwrap().is_empty());
    assert!(graph.in_edges(2, None, 10).unwrap().is_empty());
    assert_eq!(graph.out_edges(3, None, 10).unwrap(), vec![edge.clone()]);
    assert_eq!(graph.in_edges(1, None, 10).unwrap(), vec![edge]);
    let prepared = graph.prepare_delete_edge(10).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    assert!(graph.get_edge(10).unwrap().is_none());
    assert!(graph.out_edges(3, None, 10).unwrap().is_empty());
    assert!(graph.in_edges(1, None, 10).unwrap().is_empty());

    graph.create_edge(&Edge::new(11, 1, 2, "knows")).unwrap();
    graph.create_edge(&Edge::new(12, 1, 1, "self")).unwrap();
    let prepared = graph.prepare_delete_vertex(1).unwrap();
    graph.commit_prepared(&prepared).unwrap();
    let committed = graph.node().read_view().unwrap().sequence;
    graph.commit_prepared(&prepared).unwrap();
    assert_eq!(graph.node().read_view().unwrap().sequence, committed);
    assert!(graph.get_vertex(1).unwrap().is_none());
    assert!(graph.get_edge(11).unwrap().is_none());
    assert!(graph.get_edge(12).unwrap().is_none());
    assert!(graph.in_edges(2, None, 10).unwrap().is_empty());
}

#[test]
fn prepared_edge_cannot_resurrect_deleted_endpoint() {
    let dir = TempDir::new().unwrap();
    let graph = store(&dir);
    graph.create_vertex(&Vertex::new(1)).unwrap();
    graph.create_vertex(&Vertex::new(2)).unwrap();
    let prepared = graph
        .prepare_create_edge(&Edge::new(10, 1, 2, "knows"))
        .unwrap();
    graph.delete_vertex(2).unwrap();
    assert!(matches!(
        graph.commit_prepared(&prepared),
        Err(Error::StorageNode(StorageNodeError::Conflict { .. }))
    ));
    assert!(graph.get_edge(10).unwrap().is_none());
    assert!(graph.out_edges(1, None, 10).unwrap().is_empty());
    assert!(matches!(
        graph.prepare_create_edge(&Edge::new(10, 1, 2, "knows")),
        Err(Error::EdgeEndpointNotFound(2))
    ));
}

// Model a reply lost after commit, then an adapter switch. The retry adapter
// deliberately cannot read: commit must use the retained validation and bytes.
struct RetryNode {
    inner: std::sync::Arc<LocalStorageNode>,
    lose_reply: bool,
    allow_reads: bool,
    submitted: std::sync::Mutex<Vec<(rocksdb_sql_table::WriteRequest, Option<u64>)>>,
}

impl StorageNode for RetryNode {
    fn read_view(&self) -> Result<rocksdb_sql_table::ReadView, StorageNodeError> {
        if !self.allow_reads {
            return Err(StorageNodeError::Storage(
                "validation reads unavailable".into(),
            ));
        }
        self.inner.read_view()
    }

    fn get(
        &self,
        key: &[u8],
        view: &rocksdb_sql_table::ReadView,
    ) -> Result<Option<Vec<u8>>, StorageNodeError> {
        self.inner.get(key, view)
    }

    fn scan(
        &self,
        range: rocksdb_sql_table::KeyRange,
        limit: rocksdb_sql_table::ScanLimit,
        view: &rocksdb_sql_table::ReadView,
    ) -> Result<Vec<rocksdb_sql_table::KeyValue>, StorageNodeError> {
        self.inner.scan(range, limit, view)
    }

    fn batch_write(
        &self,
        request: rocksdb_sql_table::WriteRequest,
    ) -> Result<rocksdb_sql_table::WriteResult, StorageNodeError> {
        self.batch_write_conditional(request, None)
    }

    fn batch_write_conditional(
        &self,
        request: rocksdb_sql_table::WriteRequest,
        condition: Option<u64>,
    ) -> Result<rocksdb_sql_table::WriteResult, StorageNodeError> {
        self.submitted
            .lock()
            .unwrap()
            .push((request.clone(), condition));
        let result = self.inner.batch_write_conditional(request, condition)?;
        if self.lose_reply {
            return Err(StorageNodeError::Storage("reply lost after commit".into()));
        }
        Ok(result)
    }

    fn admin(&self) -> &dyn rocksdb_sql_table::StorageAdmin {
        self.inner.admin()
    }
}

#[test]
fn uncertain_commit_retries_exact_request_through_another_adapter_without_reads() {
    let dir = TempDir::new().unwrap();
    let inner = std::sync::Arc::new(LocalStorageNode::open(dir.path(), 7).unwrap());
    let original = GraphRecordStore::new(
        RetryNode {
            inner: inner.clone(),
            lose_reply: true,
            allow_reads: true,
            submitted: std::sync::Mutex::new(Vec::new()),
        },
        7,
        0,
    );
    let prepared = original.prepare_create_vertex(&Vertex::new(1)).unwrap();
    assert!(matches!(
        original.commit_prepared(&prepared),
        Err(Error::StorageNode(StorageNodeError::Storage(_)))
    ));
    let before = inner.admin().export().unwrap();
    let replacement = GraphRecordStore::new(
        RetryNode {
            inner,
            lose_reply: false,
            allow_reads: false,
            submitted: std::sync::Mutex::new(Vec::new()),
        },
        7,
        0,
    );
    replacement.commit_prepared(&prepared).unwrap();
    assert_eq!(
        *original.node().submitted.lock().unwrap(),
        *replacement.node().submitted.lock().unwrap()
    );
    assert_eq!(replacement.node().admin().export().unwrap(), before);
    assert_eq!(original.get_vertex(1).unwrap(), Some(Vertex::new(1)));
}
