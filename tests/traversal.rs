use std::path::Path;

use rocksdb_sql_table::{Edge, Error, GraphEngine, TraversalDirection, TraversalOptions, Vertex};
use tempfile::TempDir;

fn open(path: &Path) -> GraphEngine {
    GraphEngine::open(path).expect("open graph")
}

fn vertex(id: u64) -> Vertex {
    let mut vertex = Vertex::new(id);
    vertex.labels.push("Node".to_string());
    vertex
}

fn edge(id: u64, from: u64, to: u64, label: &str) -> Edge {
    Edge::new(id, from, to, label)
}

fn chain(graph: &GraphEngine) {
    for id in 1..=4 {
        graph.create_vertex(&vertex(id)).unwrap();
    }
    graph.create_edge(&edge(10, 1, 2, "NEXT")).unwrap();
    graph.create_edge(&edge(11, 2, 3, "NEXT")).unwrap();
    graph.create_edge(&edge(12, 3, 4, "NEXT")).unwrap();
}

#[test]
fn outgoing_paths_are_bounded_and_deterministic() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    chain(&graph);

    let result = graph
        .traverse(
            1,
            &TraversalOptions {
                max_hops: 3,
                direction: TraversalDirection::Out,
                label: Some("NEXT".to_string()),
                limit: 10,
            },
        )
        .unwrap();
    assert!(!result.truncated);
    assert_eq!(result.paths.len(), 3);
    assert_eq!(result.paths[0].vertices, [1, 2]);
    assert_eq!(result.paths[0].edges, [10]);
    assert_eq!(result.paths[1].vertices, [1, 2, 3]);
    assert_eq!(result.paths[1].edges, [10, 11]);
    assert_eq!(result.paths[2].vertices, [1, 2, 3, 4]);
    assert_eq!(result.paths[2].edges, [10, 11, 12]);
}

#[test]
fn path_limit_is_reported_as_truncated() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1)).unwrap();
    graph.create_vertex(&vertex(2)).unwrap();
    graph.create_vertex(&vertex(3)).unwrap();
    graph.create_edge(&edge(10, 1, 2, "NEXT")).unwrap();
    graph.create_edge(&edge(11, 1, 3, "NEXT")).unwrap();

    let result = graph
        .traverse(
            1,
            &TraversalOptions {
                max_hops: 1,
                direction: TraversalDirection::Out,
                label: None,
                limit: 1,
            },
        )
        .unwrap();
    assert!(result.truncated);
    assert_eq!(result.paths.len(), 1);
    assert_eq!(result.paths[0].vertices, [1, 2]);
}

#[test]
fn incoming_outgoing_and_both_directions() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    for id in 1..=3 {
        graph.create_vertex(&vertex(id)).unwrap();
    }
    graph.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();
    graph.create_edge(&edge(20, 3, 2, "KNOWS")).unwrap();

    let incoming = graph
        .traverse(
            2,
            &TraversalOptions {
                max_hops: 1,
                direction: TraversalDirection::In,
                label: Some("KNOWS".to_string()),
                limit: 10,
            },
        )
        .unwrap();
    assert_eq!(incoming.paths.len(), 2);
    assert_eq!(incoming.paths[0].vertices, [2, 1]);
    assert_eq!(incoming.paths[1].vertices, [2, 3]);

    let both = graph
        .traverse(
            2,
            &TraversalOptions {
                max_hops: 1,
                direction: TraversalDirection::Both,
                label: None,
                limit: 10,
            },
        )
        .unwrap();
    assert_eq!(both.paths.len(), 2);
    assert_eq!(both.paths[0].vertices, [2, 1]);
    assert_eq!(both.paths[1].vertices, [2, 3]);
}

#[test]
fn label_filter_and_cycles_terminate() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    for id in 1..=3 {
        graph.create_vertex(&vertex(id)).unwrap();
    }
    graph.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();
    graph.create_edge(&edge(11, 1, 3, "LIKES")).unwrap();
    graph.create_edge(&edge(12, 2, 1, "KNOWS")).unwrap();

    let result = graph
        .traverse(
            1,
            &TraversalOptions {
                max_hops: 3,
                direction: TraversalDirection::Out,
                label: Some("KNOWS".to_string()),
                limit: 10,
            },
        )
        .unwrap();
    assert_eq!(result.paths.len(), 1);
    assert_eq!(result.paths[0].vertices, [1, 2]);
    assert_eq!(result.paths[0].edges, [10]);
}

#[test]
fn invalid_traversal_options_and_missing_start_fail() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1)).unwrap();

    for max_hops in [0, 4] {
        assert!(matches!(
            graph.traverse(
                1,
                &TraversalOptions {
                    max_hops,
                    ..TraversalOptions::default()
                }
            ),
            Err(Error::InvalidTraversal(_))
        ));
    }
    assert!(matches!(
        graph.traverse(
            1,
            &TraversalOptions {
                limit: 0,
                ..TraversalOptions::default()
            }
        ),
        Err(Error::InvalidTraversal(_))
    ));
    assert!(matches!(
        graph.traverse(99, &TraversalOptions::default()),
        Err(Error::VertexNotFound(99))
    ));
    assert!(matches!(
        graph.traverse(
            1,
            &TraversalOptions {
                label: Some("bad-label".to_string()),
                ..TraversalOptions::default()
            }
        ),
        Err(Error::InvalidGraphLabel(_))
    ));
}
