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
