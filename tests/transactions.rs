use std::collections::BTreeMap;
use std::path::Path;

use tempfile::TempDir;
use velagraph::{Edge, Error, GraphEngine, Value, Vertex};

fn open(path: &Path) -> GraphEngine {
    GraphEngine::open(path).expect("open graph")
}

fn vertex(id: u64, label: &str, name: &str) -> Vertex {
    let mut vertex = Vertex::new(id);
    vertex.labels.push(label.to_string());
    vertex
        .properties
        .insert("name".to_string(), Value::Text(name.to_string()));
    vertex
}

fn edge(id: u64, from: u64, to: u64, label: &str) -> Edge {
    Edge::new(id, from, to, label)
}

#[test]
fn transaction_reads_its_own_writes_and_commits_atomically() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());

    let mut transaction = graph.begin().unwrap();
    transaction
        .create_vertex(&vertex(1, "Person", "Ada"))
        .unwrap();
    transaction
        .create_vertex(&vertex(2, "Person", "Grace"))
        .unwrap();
    transaction.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();

    assert_eq!(
        transaction.get_vertex(1).unwrap().unwrap().properties["name"],
        Value::Text("Ada".into())
    );
    assert_eq!(transaction.get_edge(10).unwrap().unwrap().to, 2);
    assert!(graph.get_vertex(1).unwrap().is_none());

    transaction.commit().unwrap();

    assert_eq!(
        graph.get_vertex(1).unwrap().unwrap().properties["name"],
        Value::Text("Ada".into())
    );
    assert_eq!(graph.get_edge(10).unwrap().unwrap().to, 2);
    assert_eq!(
        graph
            .out_edges(1, Some("KNOWS"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
    assert_eq!(
        graph
            .in_edges(2, Some("KNOWS"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
}

#[test]
fn transaction_conflicts_with_a_write_after_its_snapshot() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();

    let mut transaction = graph.begin().unwrap();
    transaction
        .create_vertex(&vertex(2, "Person", "Grace"))
        .unwrap();

    graph
        .create_vertex(&vertex(3, "Person", "Katherine"))
        .unwrap();
    graph
        .update_vertex(&vertex(1, "Person", "Ada Lovelace"))
        .unwrap();

    assert_eq!(
        transaction.get_vertex(1).unwrap().unwrap().properties["name"],
        Value::Text("Ada".into())
    );
    let error = transaction.commit().unwrap_err();
    assert!(matches!(error, Error::TransactionConflict));
    assert_eq!(
        graph.get_vertex(1).unwrap().unwrap().properties["name"],
        Value::Text("Ada Lovelace".into())
    );
    assert!(graph.get_vertex(2).unwrap().is_none());
    assert!(graph.get_vertex(3).unwrap().is_some());
}

#[test]
fn dropping_a_transaction_aborts_it() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());

    {
        let mut transaction = graph.begin().unwrap();
        transaction
            .create_vertex(&vertex(1, "Person", "Ada"))
            .unwrap();
    }

    assert!(graph.get_vertex(1).unwrap().is_none());
}

#[test]
fn transaction_delete_vertex_cascades_to_edges() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();
    graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
    graph.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();
    graph.create_edge(&edge(11, 1, 1, "SELF")).unwrap();

    let mut transaction = graph.begin().unwrap();
    assert!(transaction.delete_vertex(1).unwrap());
    assert!(transaction.get_vertex(1).unwrap().is_none());
    assert!(transaction.get_edge(10).unwrap().is_none());
    assert!(transaction.get_edge(11).unwrap().is_none());
    transaction.commit().unwrap();

    assert!(graph.get_vertex(1).unwrap().is_none());
    assert!(graph.get_edge(10).unwrap().is_none());
    assert!(graph.get_edge(11).unwrap().is_none());
    assert!(graph.out_edges(1, None).is_err());
    assert!(graph.in_edges(2, None).unwrap().is_empty());
}

#[test]
fn transaction_validation_failure_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());

    let mut transaction = graph.begin().unwrap();
    transaction
        .create_vertex(&vertex(2, "Person", "Grace"))
        .unwrap();
    let error = transaction
        .create_edge(&edge(10, 2, 99, "KNOWS"))
        .unwrap_err();
    assert!(matches!(error, Error::EdgeEndpointNotFound(99)));
    drop(transaction);

    assert!(graph.get_vertex(2).unwrap().is_none());
    assert!(graph.get_edge(10).unwrap().is_none());
}

#[test]
fn transaction_recreating_an_edge_replaces_both_adjacency_indexes() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();
    graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
    graph
        .create_vertex(&vertex(3, "Person", "Katherine"))
        .unwrap();
    graph.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();

    let mut transaction = graph.begin().unwrap();
    assert!(transaction.delete_edge(10).unwrap());
    transaction
        .create_edge(&edge(10, 1, 3, "WORKS_WITH"))
        .unwrap();
    transaction.commit().unwrap();

    assert_eq!(graph.get_edge(10).unwrap().unwrap().to, 3);
    assert_eq!(
        graph
            .out_edges(1, None)
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
    assert!(graph.in_edges(2, None).unwrap().is_empty());
    assert_eq!(
        graph
            .in_edges(3, Some("WORKS_WITH"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
}

#[test]
fn read_only_transactions_do_not_conflict() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();

    let transaction = graph.begin().unwrap();
    graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
    transaction.commit().unwrap();

    assert!(graph.get_vertex(2).unwrap().is_some());
}

#[test]
fn transaction_update_and_delete_are_visible_after_commit() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();
    graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();

    let mut transaction = graph.begin().unwrap();
    transaction
        .update_vertex(&vertex(1, "Person", "Ada Lovelace"))
        .unwrap();
    assert!(transaction.delete_vertex(2).unwrap());
    assert_eq!(
        transaction.get_vertex(1).unwrap().unwrap().properties["name"],
        Value::Text("Ada Lovelace".into())
    );
    transaction.commit().unwrap();

    assert_eq!(
        graph.get_vertex(1).unwrap().unwrap().properties,
        BTreeMap::from([("name".to_string(), Value::Text("Ada Lovelace".into()))])
    );
    assert!(graph.get_vertex(2).unwrap().is_none());
}

#[test]
fn deleting_a_vertex_keeps_an_edge_moved_away_in_the_same_transaction() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();
    graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
    graph
        .create_vertex(&vertex(3, "Person", "Katherine"))
        .unwrap();
    graph.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();

    let mut transaction = graph.begin().unwrap();
    assert!(transaction.delete_edge(10).unwrap());
    transaction.create_edge(&edge(10, 3, 2, "KNOWS")).unwrap();
    assert!(transaction.delete_vertex(1).unwrap());
    transaction.commit().unwrap();

    assert!(graph.get_vertex(1).unwrap().is_none());
    assert_eq!(graph.get_edge(10).unwrap().unwrap().from, 3);
    assert!(graph.out_edges(1, None).is_err());
    assert_eq!(
        graph
            .out_edges(3, None)
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
}

#[test]
fn recreating_a_deleted_vertex_and_edge_restores_its_adjacency() {
    let dir = TempDir::new().unwrap();
    let graph = open(dir.path());
    let ada = vertex(1, "Person", "Ada");
    graph.create_vertex(&ada).unwrap();
    graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
    graph.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();

    let mut transaction = graph.begin().unwrap();
    assert!(transaction.delete_vertex(1).unwrap());
    transaction.create_vertex(&ada).unwrap();
    transaction.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();
    transaction.commit().unwrap();

    assert_eq!(graph.get_vertex(1).unwrap(), Some(ada));
    assert_eq!(graph.get_edge(10).unwrap().unwrap().to, 2);
    assert_eq!(
        graph
            .out_edges(1, Some("KNOWS"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
    assert_eq!(
        graph
            .in_edges(2, Some("KNOWS"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
}

#[test]
fn committed_transaction_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let graph = open(dir.path());
        graph.create_vertex(&vertex(1, "Person", "Ada")).unwrap();
        graph.create_vertex(&vertex(2, "Person", "Grace")).unwrap();
        let mut transaction = graph.begin().unwrap();
        transaction.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();
        transaction.commit().unwrap();
    }

    let graph = open(dir.path());
    assert_eq!(graph.get_vertex(1).unwrap().unwrap().id, 1);
    assert_eq!(graph.get_edge(10).unwrap().unwrap().to, 2);
    assert_eq!(graph.out_edges(1, Some("KNOWS")).unwrap()[0].id, 10);
    assert_eq!(graph.in_edges(2, Some("KNOWS")).unwrap()[0].id, 10);
}
