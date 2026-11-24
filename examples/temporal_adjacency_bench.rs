//! Warm synthetic adjacency reads on a fixed snapshot. Compare the same example
//! in release builds of the baseline and candidate, alternating process order.
use std::time::Instant;
use velagraph::{
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, GremlinQuery, GremlinRow, Vertex,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let store = TemporalStore::open(dir.path())?;
    let valid = Interval { from: 0, to: None };
    let roots = 48;
    let target = roots + 1;
    let mut mutations: Vec<_> = (1..=target)
        .map(|id| TemporalMutation::Vertex {
            vertex: Vertex::new(id),
            valid,
        })
        .collect();
    // Leave gaps between nonempty adjacency lists.
    for id in (1..=roots).step_by(4) {
        mutations.push(TemporalMutation::Edge {
            edge: Edge::new(id, id, target, "link"),
            valid,
        });
    }
    store.commit(&mutations)?;
    drop(store);
    let store = TemporalStore::open(dir.path())?;
    let view = store.view(None, 0)?;
    let sorted: Vec<_> = (1..=roots).collect();
    let scattered: Vec<_> = (0..roots).map(|n| (n * 29) % roots + 1).collect();
    for (name, ids) in [
        ("ordered", sorted),
        ("scattered", scattered),
        ("single", vec![1]),
    ] {
        let expected = ids.iter().filter(|&&id| (id - 1) % 4 == 0).count() as u64;
        let ids = ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let query = GremlinQuery::parse(&format!("g.V({ids}).out('link').count()"))?;
        let mut samples = Vec::new();
        for round in 0..1100 {
            let start = Instant::now();
            let result = view.execute_query(&query)?;
            let elapsed = start.elapsed().as_nanos();
            assert_eq!(result.rows, [GremlinRow::Count(expected)]);
            if round >= 100 {
                samples.push(elapsed);
            }
        }
        samples.sort_unstable();
        println!("{name} p50_ns={} p99_ns={}", samples[499], samples[989]);
    }
    Ok(())
}
