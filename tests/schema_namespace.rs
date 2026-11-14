use tempfile::TempDir;
use velagraph::{Edge, GraphEngine, Value, Vertex};

#[test]
fn declarative_namespace_keeps_the_v1_field_contract() {
    let schema = include_str!("../proto/graph.proto");
    assert_eq!(
        schema.lines().find(|line| line.starts_with("package ")),
        Some("package velagraph.graph.v1;")
    );
    let fields: String = schema
        .lines()
        .filter(|line| !line.starts_with("package "))
        .map(|line| line.split("//").next().unwrap())
        .collect();
    let compact = |s: &str| s.split_whitespace().collect::<String>();
    assert_eq!(
        compact(&fields),
        compact(include_str!("fixtures/graph_v1_schema.contract"))
    );
}

#[test]
fn historical_record_bytes_open_without_namespace_migration() {
    // Literal pre-rename GRAPH_FORMAT.md vectors, not bytes produced by the
    // current encoder. This fixture covers records, not a full adjacency graph.
    let vertex_bytes = [
        0x08, 0x01, 0x10, 0x01, 0x1a, 0x0e, 0x08, 0x01, 0x12, 0x01, 0x41, 0x1a, 0x07, 0x0a,
        0x01, 0x78, 0x12, 0x02, 0x10, 0x02,
    ];
    let edge_bytes = [
        0x08, 0x01, 0x10, 0x02, 0x1a, 0x12, 0x08, 0x07, 0x10, 0x01, 0x18, 0x02, 0x22, 0x01,
        0x4b, 0x2a, 0x07, 0x0a, 0x01, 0x78, 0x12, 0x02, 0x28, 0x01,
    ];
    let key = |kind: &[u8], id: u64| {
        let mut key = b"graph/v1/p/".to_vec();
        key.extend_from_slice(&0_u32.to_be_bytes());
        key.push(b'/');
        key.extend_from_slice(kind);
        key.extend_from_slice(&id.to_be_bytes());
        key
    };
    let vertex_key = key(b"vertex/", 1);
    let edge_key = key(b"edge/", 7);
    let dir = TempDir::new().unwrap();
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.set_compression_type(rocksdb::DBCompressionType::None);
    {
        let db = rocksdb::DB::open(&options, dir.path()).unwrap();
        db.put(&vertex_key, vertex_bytes).unwrap();
        db.put(&edge_key, edge_bytes).unwrap();
        db.flush().unwrap();
    }
    let mut expected_vertex = Vertex::new(1);
    expected_vertex.labels.push("A".into());
    expected_vertex
        .properties
        .insert("x".into(), Value::Integer(1));
    let mut expected_edge = Edge::new(7, 1, 2, "K");
    expected_edge
        .properties
        .insert("x".into(), Value::Boolean(true));
    for _ in 0..2 {
        let graph = GraphEngine::open(dir.path()).unwrap();
        assert_eq!(graph.get_vertex(1).unwrap(), Some(expected_vertex.clone()));
        assert_eq!(graph.get_edge(7).unwrap(), Some(expected_edge.clone()));
    }
    let db = rocksdb::DB::open(&options, dir.path()).unwrap();
    assert_eq!(db.get(vertex_key).unwrap().unwrap(), vertex_bytes);
    assert_eq!(db.get(edge_key).unwrap().unwrap(), edge_bytes);
}
