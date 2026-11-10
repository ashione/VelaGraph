//! Bounded both() expectations from input relationships, never engine output.
use super::{partition, Model};
use serde_json::{json, Value};
use std::collections::BTreeSet;

fn both(model: &Model, roots: &[u64]) -> Vec<u64> {
    roots
        .iter()
        .flat_map(|root| {
            let mut edges: Vec<_> = model
                .edges
                .iter()
                .enumerate()
                .filter(|(_, e)| e.label == "reports_to" && (e.from == *root || e.to == *root))
                .collect();
            // The fixture writer assigns canonical edge IDs as input index + 1.
            edges.sort_by_key(|(index, edge)| (index + 1, partition(edge.from)));
            edges.into_iter().map(move |(_, edge)| {
                if edge.from == *root {
                    edge.to
                } else {
                    edge.from
                }
            })
        })
        .collect()
}
fn unique(ids: Vec<u64>) -> Vec<u64> {
    let mut seen = BTreeSet::new();
    ids.into_iter()
        .filter(|id| seen.insert((partition(*id), *id)))
        .collect()
}
fn vertices(ids: &[u64]) -> Value {
    json!({"vertices":ids.iter().map(|id| json!([partition(*id), id])).collect::<Vec<_>>()})
}
pub(super) fn queries(model: &Model) -> Vec<Value> {
    let named = both(model, &[20]);
    let mut limited = unique(both(model, &named));
    limited.truncate(12);
    let mut cases = vec![
        json!({"name":"both_named_ordered_neighbors","partition":partition(20),
            "query":"g.V(20).both('reports_to')", "expected":vertices(&named),"server_supported":true}),
        json!({"name":"both_batch_repeated_roots","partition":partition(20),
            "query":"g.V(20,23,20).both('reports_to')", "expected":vertices(&both(model,&[20,23,20])),"server_supported":true}),
        json!({"name":"global_both_named_unique_count","partition":1,
            "query":"g.V().has('name','Employee-000020').limit(1).both('reports_to').both('reports_to').dedup().limit(12).count()",
            "expected":{"count":limited.len()},"server_supported":true}),
        json!({"name":"global_both_named_names","partition":1,
            "query":"g.V().has('name','Employee-000020').limit(1).both('reports_to').dedup().limit(4).values('name')",
            "expected":{"values":unique(named).into_iter().take(4).map(|id| json!({"Text":format!("Employee-{id:06}")})).collect::<Vec<_>>()},"server_supported":true}),
        json!({"name":"global_both_empty_roots","partition":1,
            "query":"g.V().has('name','NoSuchEmployee').both('reports_to').count()",
            "expected":{"count":0},"server_supported":true}),
    ];
    // Server scans qualified partition order; the observed client scans local
    // ID order. Preserve both contracts rather than sorting result vectors.
    let mut server_roots: Vec<_> = (1..=model.people).collect();
    server_roots.sort_by_key(|id| (partition(*id), *id));
    server_roots.truncate(2);
    cases.push(json!({"name":"global_both_bounded_root_order","partition":1,
        "query":"g.V().hasLabel('person').limit(2).both('reports_to').dedup().limit(12)",
        "expected":vertices(&unique(both(model,&server_roots)).into_iter().take(12).collect::<Vec<_>>()),
        "client_expected":vertices(&unique(both(model,&[1,2])).into_iter().take(12).collect::<Vec<_>>()),
        "server_supported":true}));
    cases
}

#[test]
fn bidirectional_fixture_vectors_follow_input_edges_and_root_order() {
    for (people, departments) in [(2000, 50), (200000, 5000)] {
        let model = Model::new(people, departments);
        let rows = queries(&model);
        assert_eq!(rows.len(), 6);
        let neighbors: Vec<_> = std::iter::once(3).chain(154..=161).collect();
        assert_eq!(both(&model, &[20]), neighbors);
        assert_eq!(rows[0]["expected"], vertices(&neighbors));
        let mut batch = neighbors.clone();
        batch.extend(std::iter::once(3).chain(178..=185));
        batch.extend(neighbors);
        assert_eq!(rows[1]["expected"], vertices(&batch));
        assert_eq!(rows[2]["expected"], json!({"count":12}));
        assert_eq!(
            rows[3]["expected"],
            json!({"values":[{"Text":"Employee-000003"},{"Text":"Employee-000154"},{"Text":"Employee-000155"},{"Text":"Employee-000156"}]})
        );
        assert_eq!(rows[4]["expected"], json!({"count":0}));
        assert_eq!(
            rows[5]["expected"],
            vertices(&[1, 18, 19, 20, 21, 22, 23, 24, 25, 42, 43, 44])
        );
        assert_eq!(
            rows[5]["client_expected"],
            vertices(&[2, 3, 4, 5, 6, 7, 8, 9, 1, 10, 11, 12])
        );
        assert!(rows.iter().all(|row| row["server_supported"] == true));
    }
}

#[test]
fn bidirectional_oracle_preserves_parallel_edges_and_counts_self_loop_once() {
    use super::Edge;
    let model = Model {
        people: 100,
        departments: 25,
        edges: vec![
            Edge {
                from: 20,
                to: 23,
                label: "reports_to",
            },
            Edge {
                from: 26,
                to: 20,
                label: "reports_to",
            },
            Edge {
                from: 20,
                to: 20,
                label: "reports_to",
            },
            Edge {
                from: 20,
                to: 23,
                label: "reports_to",
            },
            Edge {
                from: 20,
                to: 29,
                label: "member_of",
            },
        ],
    };
    assert_eq!(
        both(&model, &[20, 20]),
        vec![23, 26, 20, 23, 23, 26, 20, 23]
    );
    assert_eq!(unique(both(&model, &[20])), vec![23, 26, 20]);
}
