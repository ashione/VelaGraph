//! Regressions for rejecting a Tokio worker before taking the graph mutex.
use d2_raft::{Cluster, D1Command, LogCommand};
use replicated_graph::{run_blocking, ReplicatedStorageNode};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use velagraph::{
    Error, GraphRecordStore, KeyRange, KeyValue, ReadView, ScanLimit, Sequence, StorageAdmin,
    StorageNode, StorageNodeError, Vertex, WriteRequest, WriteResult,
};

type Result<T> = std::result::Result<T, StorageNodeError>;
struct ObservedNode {
    inner: ReplicatedStorageNode,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}
impl StorageNode for ObservedNode {
    fn check_execution_context(&self) -> Result<()> {
        self.inner.check_execution_context()
    }
    fn read_view(&self) -> Result<ReadView> {
        // GraphRecordStore has already acquired its write mutex here. The
        // signal removes a timing/scheduling assumption from the regression.
        if let Some(sender) = self.entered.lock().unwrap().take() {
            let _ = sender.send(());
        }
        self.inner.read_view()
    }
    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>> {
        self.inner.get(key, view)
    }
    fn scan(&self, range: KeyRange, limit: ScanLimit, view: &ReadView) -> Result<Vec<KeyValue>> {
        self.inner.scan(range, limit, view)
    }
    fn batch_write(&self, request: WriteRequest) -> Result<WriteResult> {
        self.inner.batch_write(request)
    }
    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        expected: Option<Sequence>,
    ) -> Result<WriteResult> {
        self.inner.batch_write_conditional(request, expected)
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self.inner.admin()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_direct_crud_and_prepared_commit_do_not_block_worker() {
    let cluster = Cluster::new();
    let node = cluster.start(1).await.unwrap();
    node.raft
        .initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while node.raft.ensure_linearizable().await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    node.raft
        .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
        .await
        .unwrap();
    let adapter = ReplicatedStorageNode::new(&node, 7, Duration::from_secs(2)).unwrap();
    let prepare_store = GraphRecordStore::new(adapter.clone(), 7, 0);
    let prepared = run_blocking(move || prepare_store.prepare_create_vertex(&Vertex::new(3)))
        .await
        .unwrap()
        .unwrap();
    let (entered, signal) = tokio::sync::oneshot::channel();
    let graph = Arc::new(GraphRecordStore::new(
        ObservedNode {
            inner: adapter,
            entered: Mutex::new(Some(entered)),
        },
        7,
        0,
    ));
    // Freeze snapshot acquisition while a permitted blocking caller owns the
    // graph mutex. Incorrect direct calls would wait until that caller times out.
    let state_guard = node.state_machine.pause_state().await;
    let g = graph.clone();
    let pending = tokio::spawn(run_blocking(move || g.create_vertex(&Vertex::new(1))));
    tokio::time::timeout(Duration::from_secs(5), signal)
        .await
        .unwrap()
        .unwrap();
    let start = std::time::Instant::now();
    let direct = graph.create_vertex(&Vertex::new(2));
    let retry = graph.commit_prepared(&prepared);
    assert!(matches!(
        direct,
        Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
    ));
    assert!(matches!(
        retry,
        Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
    ));
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "worker waited for graph mutation mutex"
    );
    drop(state_guard);
    // Unlike the broken path, the legitimate mutation must not time out while
    // the single runtime worker is blocked behind it.
    pending.await.unwrap().unwrap().unwrap();
    cluster.shutdown(&node).await;
}
