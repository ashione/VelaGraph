//! Compare synthetic wide bulk loads and warm reads over overlapping revisions.
//! Timings are elapsed microseconds. Each query row covers 10,000 reads.
use std::time::Instant;
use velagraph::{
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, GremlinQuery, GremlinRow, IncrementalGraph, IncrementalLimits, IncrementalMode, Value,
    Vertex,
};
fn bulk_load() {
    for count in [2048u64, 65536] {
        let vertices = vec![Vertex::new(1), Vertex::new(2)];
        let edges = (0..count).rev().map(|i| Edge::new(i, 1, 2, "x")).collect();
        let start = Instant::now();
        let g = IncrementalGraph::from_records(
            "bench",
            IncrementalMode::Cow,
            IncrementalLimits::default(),
            vertices,
            edges,
            vec![],
        )
        .unwrap();
        println!(
            "bulk {count} {} {}",
            start.elapsed().as_micros(),
            g.read().unwrap().accounted_bytes()
        );
    }
}

fn temporal_reads() {
    let d = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(d.path()).unwrap();
    for n in 0..512 {
        let mut v = Vertex::new(1);
        v.properties.insert("n".into(), Value::Integer(n));
        store
            .commit(&[TemporalMutation::Vertex {
                vertex: v,
                valid: Interval {
                    from: 0,
                    to: Some(if n == 511 { 200 } else { 300 }),
                },
            }])
            .unwrap();
    }
    let q = GremlinQuery::parse("g.V(1).values('n')").unwrap();
    assert!(store
        .view(None, -1)
        .unwrap()
        .execute_query(&q)
        .unwrap()
        .rows
        .is_empty());
    // Cold miss populated a revision prefix up to the cache block size.
    for (t, expected) in [(255, 510), (150, 511)] {
        let start = Instant::now();
        for _ in 0..10000 {
            let result = store.view(None, t).unwrap().execute_query(&q).unwrap();
            assert_eq!(
                result.rows,
                vec![GremlinRow::Value(Value::Integer(expected))]
            );
        }
        println!("query {t} {}", start.elapsed().as_micros());
    }
}

fn main() {
    bulk_load();
    temporal_reads();
}
