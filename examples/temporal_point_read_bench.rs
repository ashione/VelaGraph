//! Warm direct point reads on a deterministic synthetic graph.
//! Run with cargo run --release --locked --example temporal_point_read_bench.
use std::{hint::black_box, time::Instant};
use velagraph::{
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, Vertex,
};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let valid = Interval { from: 0, to: None };
    const N: u64 = 128;
    let vertices: Vec<_> = (0..N)
        .map(|id| TemporalMutation::Vertex {
            valid,
            vertex: Vertex::new(id),
        })
        .collect();
    store.commit(&vertices).unwrap();
    let mut edges = Vec::new();
    for id in 0..N {
        for (edge_id, to) in [(id, (id + 1) % N), (id + N, id)] {
            edges.push(TemporalMutation::Edge {
                valid,
                edge: Edge::new(edge_id, id, to, "link"),
            });
        }
    }
    store.commit(&edges).unwrap();
    // Reopen so the timed reads exercise the on-disk revision index.
    drop(store);
    let store = TemporalStore::open(dir.path()).unwrap();
    let view = store.view(None, 5).unwrap();
    for (name, offset) in [("ordinary edge", 0), ("self-loop", N)] {
        let mut rounds = Vec::new();
        for round in 0..10 {
            let started = Instant::now();
            for i in 0..16_384 {
                let id = offset + (i * 73 % N);
                let edge = view.get_edge(id).unwrap().unwrap();
                assert_eq!(black_box(edge).id, id);
            }
            if round != 0 {
                rounds.push(started.elapsed().as_nanos() / 16_384);
            }
        }
        rounds.sort_unstable();
        println!("{name}: {} ns/read (median of nine round means)", rounds[4]);
    }
}
