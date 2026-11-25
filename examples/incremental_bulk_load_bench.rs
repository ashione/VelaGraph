//! Time synthetic bulk loads into the incremental memory graph.
//! Output columns are edge count, elapsed microseconds and accounted bytes.
use std::time::Instant;
use velagraph::{Edge, IncrementalGraph, IncrementalLimits, IncrementalMode, Vertex};

fn main() {
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
