use rocksdb_sql_table::{Edge, GraphRecordStore, LocalStorageNode, StorageNode, Value, Vertex};
use tempfile::TempDir;

fn open_store(dir: &TempDir) -> GraphRecordStore<LocalStorageNode> {
    let node = LocalStorageNode::open(dir.path(), 7).expect("open node");
    GraphRecordStore::new(node, 7, 0)
}

fn vertex(id: u64, labels: &[&str]) -> Vertex {
    let mut vertex = Vertex::new(id);
    vertex.labels = labels.iter().map(|label| label.to_string()).collect();
    vertex
}

#[test]
fn creates_reads_and_traverses_graph_records() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);

    store.create_vertex(&vertex(1, &["person"])).unwrap();
    store.create_vertex(&vertex(2, &["person"])).unwrap();
    let mut edge = Edge::new(10, 1, 2, "knows");
    edge.properties.insert("since".into(), Value::Integer(2020));
    store.create_edge(&edge).unwrap();

    assert_eq!(store.get_vertex(1).unwrap().unwrap().labels, vec!["person"]);
    assert_eq!(store.get_edge(10).unwrap().unwrap(), edge);
    assert_eq!(
        store.out_edges(1, Some("knows"), 10).unwrap(),
        vec![edge.clone()]
    );
    assert_eq!(store.in_edges(2, Some("knows"), 10).unwrap(), vec![edge]);
    assert!(store.out_edges(1, Some("other"), 10).unwrap().is_empty());
    assert!(store.in_edges(1, None, 10).unwrap().is_empty());
}

#[test]
fn delete_vertex_cascades_edges_and_adjacency() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    store.create_vertex(&vertex(1, &[])).unwrap();
    store.create_vertex(&vertex(2, &[])).unwrap();
    store.create_edge(&Edge::new(10, 1, 2, "knows")).unwrap();
    store.create_edge(&Edge::new(11, 2, 1, "likes")).unwrap();
    store.create_edge(&Edge::new(12, 1, 1, "self")).unwrap();

    store.delete_vertex(1).unwrap();

    assert!(store.get_vertex(1).unwrap().is_none());
    assert!(store.get_vertex(2).unwrap().is_some());
    assert!(store.get_edge(10).unwrap().is_none());
    assert!(store.get_edge(11).unwrap().is_none());
    assert!(store.get_edge(12).unwrap().is_none());
    assert!(store.out_edges(2, None, 10).unwrap().is_empty());
    assert!(store.in_edges(2, None, 10).unwrap().is_empty());
}

#[test]
fn missing_endpoint_rejects_the_whole_edge() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    store.create_vertex(&vertex(1, &[])).unwrap();
    let before = store.node().admin().export().unwrap();

    let error = store
        .create_edge(&Edge::new(10, 1, 99, "knows"))
        .unwrap_err();
    assert!(error.to_string().contains("endpoint vertex 99"));

    let after = store.node().admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
    assert!(store.get_edge(10).unwrap().is_none());
    assert!(store.out_edges(1, None, 10).unwrap().is_empty());
}

#[test]
fn updates_replace_edge_adjacency() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    for id in 1..=3 {
        store.create_vertex(&vertex(id, &[])).unwrap();
    }
    store.create_edge(&Edge::new(10, 1, 2, "knows")).unwrap();
    store.update_edge(&Edge::new(10, 1, 3, "likes")).unwrap();

    assert!(store.out_edges(1, Some("knows"), 10).unwrap().is_empty());
    assert!(store.in_edges(2, Some("knows"), 10).unwrap().is_empty());
    assert_eq!(
        store.out_edges(1, Some("likes"), 10).unwrap(),
        vec![Edge::new(10, 1, 3, "likes")]
    );
    assert_eq!(
        store.in_edges(3, None, 10).unwrap(),
        vec![Edge::new(10, 1, 3, "likes")]
    );
}

#[test]
fn records_survive_reopen() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    store.create_vertex(&vertex(1, &["person"])).unwrap();
    store.create_vertex(&vertex(2, &["person"])).unwrap();
    store.create_edge(&Edge::new(10, 1, 2, "knows")).unwrap();
    drop(store.into_inner());

    let store = open_store(&dir);
    assert_eq!(store.get_vertex(1).unwrap().unwrap().labels, vec!["person"]);
    assert_eq!(
        store.out_edges(1, None, 10).unwrap(),
        vec![Edge::new(10, 1, 2, "knows")]
    );
}

#[test]
fn invalid_limits_and_labels_are_rejected() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    store.create_vertex(&vertex(1, &[])).unwrap();

    assert!(store.out_edges(1, None, 0).is_err());
    assert!(store.out_edges(1, None, 1_001).is_err());
    assert!(store.create_vertex(&vertex(2, &["not a label"])).is_err());
}

#[test]
fn duplicate_and_missing_records_are_rejected() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    store.create_vertex(&vertex(1, &[])).unwrap();
    store.create_vertex(&vertex(2, &[])).unwrap();

    assert!(store.create_vertex(&vertex(1, &[])).is_err());
    assert!(store.update_vertex(&vertex(3, &[])).is_err());
    store.create_edge(&Edge::new(10, 1, 2, "knows")).unwrap();
    assert!(store.create_edge(&Edge::new(10, 1, 2, "knows")).is_err());
    assert!(store.delete_edge(99).is_err());
    assert!(store.delete_vertex(99).is_err());
}
