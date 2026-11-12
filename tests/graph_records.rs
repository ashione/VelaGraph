use tempfile::TempDir;
use velagraph::{Edge, GraphRecordStore, LocalStorageNode, StorageNode, Value, Vertex};

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

#[test]
fn property_only_update_preserves_adjacency() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    store.create_vertex(&vertex(1, &[])).unwrap();
    store.create_vertex(&vertex(2, &[])).unwrap();
    let mut edge = Edge::new(10, 1, 2, "knows");
    store.create_edge(&edge).unwrap();
    edge.properties.insert("since".into(), Value::Integer(2024));

    store.update_edge(&edge).unwrap();

    assert_eq!(store.get_edge(10).unwrap().unwrap(), edge);
    assert_eq!(
        store.out_edges(1, Some("knows"), 10).unwrap(),
        vec![edge.clone()]
    );
    assert_eq!(store.in_edges(2, Some("knows"), 10).unwrap(), vec![edge]);
}

#[test]
fn concurrent_mutations_from_one_store_are_serialized() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);

    std::thread::scope(|scope| {
        let first = scope.spawn(|| store.create_vertex(&vertex(1, &[])));
        let second = scope.spawn(|| store.create_vertex(&vertex(2, &[])));
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
    });

    assert!(store.get_vertex(1).unwrap().is_some());
    assert!(store.get_vertex(2).unwrap().is_some());
}

fn raw_adjacency(direction: &str, vertex: u64, label: &str, edge: u64) -> Vec<u8> {
    let mut key = b"graph/v1/p/".to_vec();
    key.extend_from_slice(&0_u32.to_be_bytes());
    key.extend_from_slice(format!("/{direction}/").as_bytes());
    key.extend_from_slice(&vertex.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(label.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&edge.to_be_bytes());
    key
}

fn inject_adjacency(store: &GraphRecordStore<LocalStorageNode>, key: Vec<u8>, value: Vec<u8>) {
    use velagraph::{WriteOp, WriteRequest};
    let view = store.node().read_view().unwrap();
    let mut request_id = [255; 16];
    request_id[8..].copy_from_slice(&view.sequence.to_be_bytes());
    store
        .node()
        .batch_write(WriteRequest {
            shard: 7,
            epoch: view.epoch,
            request_id,
            operations: vec![WriteOp::put(key, value)],
        })
        .unwrap();
}

#[test]
fn corrupt_adjacency_rejects_reads_and_cascade_without_writes() {
    use velagraph::Error;
    // Check both directions, including incident edges indexed under the wrong
    // direction, aliases hidden by deduplication, missing records and bad keys.
    for direction in ["out", "in"] {
        for corruption in [
            "unrelated",
            "direction",
            "label",
            "missing",
            "malformed",
            "value",
            "self_label",
        ] {
            let dir = TempDir::new().unwrap();
            let store = open_store(&dir);
            for id in 1..=3 {
                store.create_vertex(&Vertex::new(id)).unwrap();
            }
            let (from, to) = if direction == "out" { (1, 2) } else { (2, 1) };
            // Lower-id valid entries must not allow a partial successful read.
            for id in [1, 2, 3] {
                store
                    .create_edge(&Edge::new(id, from, to, "knows"))
                    .unwrap();
            }
            let edge = match corruption {
                "unrelated" => Edge::new(10, 2, 3, "knows"),
                "direction" => Edge::new(10, to, from, "knows"),
                "self_label" => Edge::new(10, 1, 1, "knows"),
                _ => Edge::new(10, from, to, "knows"),
            };
            if corruption != "missing" {
                store.create_edge(&edge).unwrap();
            }
            let label = if matches!(corruption, "label" | "self_label") {
                "likes"
            } else {
                "knows"
            };
            let mut key = raw_adjacency(direction, 1, label, 10);
            if corruption == "malformed" {
                key.insert(key.len() - 8, b'/');
            }
            let value = if corruption == "value" {
                vec![1]
            } else {
                vec![]
            };
            inject_adjacency(&store, key, value);
            let before = store.node().admin().export().unwrap();
            for filter in [None, Some(label)] {
                let result = if direction == "out" {
                    store.out_edges(1, filter, 1)
                } else {
                    store.in_edges(1, filter, 1)
                };
                assert!(
                    matches!(result, Err(Error::CorruptData(_))),
                    "{direction}/{corruption}/{filter:?}: {result:?}"
                );
            }
            assert!(
                matches!(store.delete_vertex(1), Err(Error::CorruptData(_))),
                "{direction}/{corruption}"
            );
            let after = store.node().admin().export().unwrap();
            assert_eq!(before.entries, after.entries, "{direction}/{corruption}");
            assert_eq!(before.sequence, after.sequence, "{direction}/{corruption}");
        }
    }
}

#[test]
fn validated_adjacency_preserves_id_order_label_boundaries_and_self_loops() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    store.create_vertex(&Vertex::new(1)).unwrap();
    let edges = [
        Edge::new(30, 1, 1, "A"),
        Edge::new(10, 1, 1, "AB"),
        Edge::new(20, 1, 1, "A"),
    ];
    for edge in &edges {
        store.create_edge(edge).unwrap();
    }
    assert_eq!(store.out_edges(1, None, 1).unwrap(), vec![edges[1].clone()]);
    assert_eq!(
        store.in_edges(1, Some("A"), 10).unwrap(),
        vec![edges[2].clone(), edges[0].clone()]
    );
    let before = store.node().read_view().unwrap().sequence;
    store.delete_vertex(1).unwrap();
    let after = store.node().admin().export().unwrap();
    assert!(after.entries.is_empty());
    assert_eq!(after.sequence, before + 1);
}

#[test]
fn adjacency_capacity_errors_preserve_all_records() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    store.create_vertex(&Vertex::new(1)).unwrap();
    store.create_vertex(&Vertex::new(2)).unwrap();
    for id in 0..1_001 {
        store.create_edge(&Edge::new(id, 1, 2, "knows")).unwrap();
    }
    let before = store.node().admin().export().unwrap();
    assert!(store.out_edges(1, None, 1).is_err());
    assert!(store.in_edges(2, Some("knows"), 1).is_err());
    assert!(store.delete_vertex(1).is_err());
    assert!(store.delete_vertex(2).is_err());
    let after = store.node().admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
}

#[test]
fn exact_capacity_both_directions_is_one_atomic_batch() {
    let d = TempDir::new().unwrap();
    let s = GraphRecordStore::new(LocalStorageNode::open(d.path(), 7).unwrap(), 7, 0);
    for id in 1..=3 {
        s.create_vertex(&Vertex::new(id)).unwrap();
    }
    for id in 0..1000 {
        s.create_edge(&Edge::new(id, 1, 2, if id % 2 == 0 { "A" } else { "AB" }))
            .unwrap();
        s.create_edge(&Edge::new(id + 1000, 3, 1, "Z")).unwrap();
    }
    let out = s.out_edges(1, None, 1000).unwrap();
    assert_eq!(out.len(), 1000);
    assert_eq!(
        out.iter().map(|x| x.id).collect::<Vec<_>>(),
        (0..1000).collect::<Vec<_>>()
    );
    assert_eq!(s.in_edges(1, None, 1000).unwrap().len(), 1000);
    assert_eq!(s.out_edges(1, Some("A"), 1000).unwrap().len(), 500);
    let before = s.node().read_view().unwrap().sequence;
    s.delete_vertex(1).unwrap();
    let after = s.node().admin().export().unwrap();
    assert_eq!(after.sequence, before + 1);
    assert_eq!(after.entries.len(), 2);
    assert!(s.in_edges(2, None, 1000).unwrap().is_empty());
    assert!(s.out_edges(3, None, 1000).unwrap().is_empty());
}
