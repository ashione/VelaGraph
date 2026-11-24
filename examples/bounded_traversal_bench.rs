//! Run with `cargo run --release --example bounded_traversal_bench`.
use std::{hint::black_box, time::Instant};

use velagraph::{Edge, GraphEngine, TraversalOptions, Vertex};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphEngine::open(dir.path()).unwrap();
    for id in 0..=260 {
        graph.create_vertex(&Vertex::new(id)).unwrap();
    }
    let mut edge_id = 0;
    let mut connect = |from, to| {
        edge_id += 1;
        graph
            .create_edge(&Edge::new(edge_id, from, to, "link"))
            .unwrap();
    };
    connect(1, 2);
    connect(2, 3);
    connect(3, 1);
    for to in 5..=260 {
        connect(4, to);
    }
    for (name, start, hops, limit, count, truncated, iterations) in [
        ("isolated", 0, 1, 16, 0, false, 50_000),
        ("cycle", 1, 3, 16, 2, false, 20_000),
        ("fan-out", 4, 1, 256, 256, false, 500),
        ("limited fan-out", 4, 1, 8, 8, true, 500),
    ] {
        let opts = TraversalOptions {
            max_hops: hops,
            limit,
            ..Default::default()
        };
        let query = || graph.traverse(start, &opts).unwrap();
        let warm = query();
        assert_eq!((warm.paths.len(), warm.truncated), (count, truncated));
        let start_time = Instant::now();
        for _ in 0..iterations {
            black_box(query());
        }
        println!(
            "{name}: {} ns/traversal",
            start_time.elapsed().as_nanos() / iterations
        );
    }
}
