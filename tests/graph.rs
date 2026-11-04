use std::collections::BTreeMap;
use std::path::Path;

use rocksdb_sql_table::{Edge, Error, GraphEngine, Value, Vertex};
use tempfile::TempDir;

fn open(path: &Path) -> GraphEngine {
    GraphEngine::open(path).expect("open graph")
}

fn vertex(id: i64, label: &str, name: &str) -> Vertex {
    let mut vertex = Vertex::new(id);
    vertex.labels.push(label.to_string());
    vertex
        .properties
        .insert("name".to_string(), Value::Text(name.to_string()));
    vertex
}

fn edge(id: i64, from: i64, to: i64, label: &str) -> Edge {
    Edge::new(id, from, to, label)
}

#[test]
fn vertices_edges_and_adjacency_round_trip() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());

    let mut ada = vertex(1, "Person", "Ada");
    ada.properties.insert("age".to_string(), Value::Integer(37));
    let grace = vertex(2, "Person", "Grace");
    let paris = vertex(3, "City", "Paris");
    graph.create_vertex(&ada).unwrap();
    graph.create_vertex(&grace).unwrap();
    graph.create_vertex(&paris).unwrap();

    let mut knows = edge(10, 1, 2, "KNOWS");
    knows
        .properties
        .insert("since".to_string(), Value::Integer(1843));
    graph.create_edge(&knows).unwrap();
    graph.create_edge(&edge(11, 1, 3, "LIVES_IN")).unwrap();
    graph.create_edge(&edge(12, 2, 3, "VISITED")).unwrap();

    assert_eq!(graph.get_vertex(1).unwrap(), Some(ada));
    assert_eq!(graph.get_vertex(2).unwrap(), Some(grace));
    assert_eq!(graph.get_vertex(99).unwrap(), None);
    assert_eq!(graph.get_edge(10).unwrap(), Some(knows.clone()));
    assert_eq!(graph.get_edge(99).unwrap(), None);

    let out = graph.out_edges(1, None).unwrap();
    assert_eq!(out.iter().map(|edge| edge.id).collect::<Vec<_>>(), [10, 11]);
    assert_eq!(graph.out_edges(1, Some("KNOWS")).unwrap(), [knows.clone()]);
    assert!(graph.out_edges(1, Some("VISITED")).unwrap().is_empty());
    assert_eq!(
        graph
            .in_edges(3, None)
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [11, 12]
    );
    assert_eq!(
        graph
            .in_edges(3, Some("VISITED"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [12]
    );
    assert!(graph
        .in_edges(2, None)
        .unwrap()
        .iter()
        .any(|edge| edge.id == 10));

    assert_eq!(
        graph
            .vertices()
            .unwrap()
            .iter()
            .map(|vertex| vertex.id)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        graph
            .edges()
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10, 11, 12]
    );
}

#[test]
fn graph_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let graph = open(dir.path());
        graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();
        graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
        graph.create_edge(&edge(7, 1, 2, "KNOWS")).unwrap();
    }

    let graph = open(dir.path());
    assert_eq!(
        graph.get_vertex(1).unwrap().unwrap().properties["name"],
        Value::Text("Ada".into())
    );
    let out = graph.out_edges(1, Some("KNOWS")).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].to, 2);
}

#[test]
fn rejects_duplicates_missing_endpoints_and_invalid_labels() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    let ada = vertex(1, "Person", "Ada");
    graph.create_vertex(&ada).unwrap();

    assert!(matches!(
        graph.create_vertex(&ada),
        Err(Error::VertexAlreadyExists(1))
    ));

    let mut invalid = Vertex::new(2);
    invalid.labels.push("bad/label".to_string());
    assert!(matches!(
        graph.create_vertex(&invalid),
        Err(Error::InvalidGraphLabel(_))
    ));

    let mut too_long = Vertex::new(3);
    too_long.labels.push("a".repeat(65));
    assert!(matches!(
        graph.create_vertex(&too_long),
        Err(Error::InvalidGraphLabel(_))
    ));

    assert!(matches!(
        graph.create_edge(&edge(10, 1, 2, "KNOWS")),
        Err(Error::EdgeEndpointNotFound(2))
    ));
    assert!(matches!(
        graph.create_edge(&edge(10, 2, 1, "KNOWS")),
        Err(Error::EdgeEndpointNotFound(2))
    ));
    assert!(matches!(
        graph.create_edge(&edge(10, 1, 1, "bad/label")),
        Err(Error::InvalidGraphLabel(_))
    ));

    graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
    graph.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();
    assert!(matches!(
        graph.create_edge(&edge(10, 2, 1, "KNOWS")),
        Err(Error::EdgeAlreadyExists(10))
    ));
}

#[test]
fn update_preserves_edges_and_delete_cascades() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();
    graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
    graph.create_vertex(&vertex(3, "City", "Paris")).unwrap();
    graph.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();
    graph.create_edge(&edge(11, 1, 3, "LIVES_IN")).unwrap();
    graph.create_edge(&edge(12, 2, 3, "VISITED")).unwrap();

    let mut updated = vertex(1, "Person", "Ada Lovelace");
    updated
        .properties
        .insert("age".to_string(), Value::Integer(37));
    graph.update_vertex(&updated).unwrap();
    assert_eq!(graph.get_vertex(1).unwrap(), Some(updated));
    assert_eq!(graph.out_edges(1, None).unwrap().len(), 2);

    assert!(graph.delete_edge(10).unwrap());
    assert!(!graph.delete_edge(10).unwrap());
    assert!(graph.out_edges(1, Some("KNOWS")).unwrap().is_empty());
    assert!(graph.in_edges(2, None).unwrap().is_empty());

    assert!(graph.delete_vertex(1).unwrap());
    assert!(!graph.delete_vertex(1).unwrap());
    assert!(graph.get_vertex(1).unwrap().is_none());
    assert!(graph.out_edges(1, None).is_err());
    assert_eq!(
        graph
            .edges()
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [12]
    );
    assert_eq!(
        graph
            .in_edges(3, None)
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [12]
    );
}

#[test]
fn self_loop_is_stored_once_and_deleted_with_its_vertex() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Node", "one")).unwrap();
    graph.create_edge(&edge(5, 1, 1, "SELF")).unwrap();

    assert_eq!(graph.out_edges(1, None).unwrap().len(), 1);
    assert_eq!(graph.in_edges(1, None).unwrap().len(), 1);
    assert!(graph.delete_vertex(1).unwrap());
    assert!(graph.vertices().unwrap().is_empty());
    assert!(graph.edges().unwrap().is_empty());
}

#[test]
fn empty_properties_round_trip() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    let mut plain = Vertex::new(1);
    plain.labels.push("Thing".to_string());
    graph.create_vertex(&plain).unwrap();
    let loaded = graph.get_vertex(1).unwrap().unwrap();
    assert_eq!(loaded.labels, ["Thing"]);
    assert_eq!(loaded.properties, BTreeMap::new());
}

#[test]
fn label_prefixes_do_not_bleed() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    for id in 1..=3 {
        graph
            .create_vertex(&vertex(id, "Node", &format!("n{id}")))
            .unwrap();
    }
    graph.create_edge(&edge(10, 1, 2, "A")).unwrap();
    graph.create_edge(&edge(11, 1, 3, "AB")).unwrap();

    assert_eq!(
        graph
            .out_edges(1, Some("A"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
    assert_eq!(
        graph
            .out_edges(1, Some("AB"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [11]
    );
    assert_eq!(
        graph
            .in_edges(2, Some("A"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
}

#[test]
fn extreme_ids_round_trip() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    for id in [i64::MIN, -1, 0, i64::MAX] {
        graph.create_vertex(&vertex(id, "Node", "n")).unwrap();
    }
    graph
        .create_edge(&edge(i64::MIN, i64::MIN, i64::MAX, "EDGE"))
        .unwrap();
    graph.create_edge(&edge(i64::MAX, -1, 0, "EDGE")).unwrap();

    assert!(graph.get_vertex(i64::MIN).unwrap().is_some());
    assert!(graph.get_vertex(i64::MAX).unwrap().is_some());
    assert_eq!(
        graph
            .out_edges(i64::MIN, None)
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [i64::MIN]
    );
    assert_eq!(
        graph
            .in_edges(i64::MAX, None)
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [i64::MIN]
    );
    assert!(graph.delete_edge(i64::MIN).unwrap());
    assert!(graph.delete_vertex(i64::MIN).unwrap());
}

#[test]
fn property_values_are_typed_and_nonfinite_reals_are_rejected() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    let mut node = Vertex::new(1);
    node.labels.push("Node".to_string());
    node.properties.insert("null".to_string(), Value::Null);
    node.properties
        .insert("int".to_string(), Value::Integer(-7));
    node.properties.insert("real".to_string(), Value::Real(1.5));
    node.properties
        .insert("text".to_string(), Value::Text("x".to_string()));
    node.properties
        .insert("bool".to_string(), Value::Boolean(true));
    graph.create_vertex(&node).unwrap();
    assert_eq!(graph.get_vertex(1).unwrap(), Some(node));

    let mut bad = Vertex::new(2);
    bad.properties
        .insert("bad".to_string(), Value::Real(f64::NAN));
    assert!(matches!(
        graph.create_vertex(&bad),
        Err(Error::InvalidGraphProperty(_))
    ));

    graph.create_vertex(&vertex(3, "Node", "n3")).unwrap();
    let mut bad_edge = edge(10, 1, 3, "EDGE");
    bad_edge
        .properties
        .insert("bad".to_string(), Value::Real(f64::INFINITY));
    assert!(matches!(
        graph.create_edge(&bad_edge),
        Err(Error::InvalidGraphProperty(_))
    ));
}

#[test]
fn cascade_delete_from_target_endpoint_removes_both_sides() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    for id in 1..=3 {
        graph
            .create_vertex(&vertex(id, "Node", &format!("n{id}")))
            .unwrap();
    }
    graph.create_edge(&edge(10, 1, 2, "EDGE")).unwrap();
    graph.create_edge(&edge(12, 2, 3, "EDGE")).unwrap();

    assert!(graph.delete_vertex(2).unwrap());
    assert!(graph.get_edge(10).unwrap().is_none());
    assert!(graph.get_edge(12).unwrap().is_none());
    assert!(graph.out_edges(1, None).unwrap().is_empty());
    assert!(graph.in_edges(3, None).unwrap().is_empty());
    assert_eq!(graph.vertices().unwrap().len(), 2);
    assert!(graph.edges().unwrap().is_empty());
}

#[test]
fn failed_edge_create_leaves_no_partial_state() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Node", "n1")).unwrap();
    assert!(matches!(
        graph.create_edge(&edge(10, 1, 2, "EDGE")),
        Err(Error::EdgeEndpointNotFound(2))
    ));
    assert!(graph.edges().unwrap().is_empty());
    assert!(graph.get_edge(10).unwrap().is_none());
    assert!(graph.out_edges(1, None).unwrap().is_empty());

    graph.create_vertex(&vertex(2, "Node", "n2")).unwrap();
    graph.create_edge(&edge(10, 1, 2, "EDGE")).unwrap();
    assert_eq!(graph.out_edges(1, None).unwrap().len(), 1);
}

#[test]
fn traversal_and_update_require_an_existing_vertex() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    assert!(matches!(
        graph.out_edges(99, None),
        Err(Error::VertexNotFound(99))
    ));
    assert!(matches!(
        graph.in_edges(99, None),
        Err(Error::VertexNotFound(99))
    ));
    assert!(matches!(
        graph.update_vertex(&Vertex::new(99)),
        Err(Error::VertexNotFound(99))
    ));
}

#[test]
fn label_length_boundary_is_enforced() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    let mut boundary = Vertex::new(1);
    boundary.labels.push("a".repeat(64));
    graph.create_vertex(&boundary).unwrap();
    assert_eq!(graph.get_vertex(1).unwrap().unwrap().labels[0].len(), 64);

    let mut too_long = Vertex::new(2);
    too_long.labels.push("a".repeat(65));
    assert!(matches!(
        graph.create_vertex(&too_long),
        Err(Error::InvalidGraphLabel(_))
    ));
    let mut empty = Vertex::new(3);
    empty.labels.push(String::new());
    assert!(matches!(
        graph.create_vertex(&empty),
        Err(Error::InvalidGraphLabel(_))
    ));
}

#[test]
fn empty_graph_lists_are_empty() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    assert!(graph.vertices().unwrap().is_empty());
    assert!(graph.edges().unwrap().is_empty());
}
