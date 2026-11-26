//! Warm local Gremlin traversals with a single expansion or a wide second hop.
use std::time::Instant;
use velagraph::{Edge, GraphEngine, GremlinQuery, GremlinRow, Vertex};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const WIDTH: u64 = 512;
    let dir = tempfile::tempdir()?;
    let graph = GraphEngine::open(dir.path())?;
    graph.create_vertex(&Vertex::new(0))?;
    for id in 1..=WIDTH {
        graph.create_vertex(&Vertex::new(id))?;
        graph.create_edge(&Edge::new(id, 0, id, "link"))?;
        if id % 8 == 0 {
            graph.create_edge(&Edge::new(WIDTH + id, id, 0, "back"))?;
        }
    }

    for (name, text, count) in [
        ("one_hop", "g.V(0).out('link').count()", WIDTH),
        (
            "empty_second_hop",
            "g.V(0).out('link').out('link').count()",
            0,
        ),
        (
            "sparse_second_hop",
            "g.V(0).out('link').out('back').count()",
            WIDTH / 8,
        ),
    ] {
        let query = GremlinQuery::parse(text)?;
        let mut samples = Vec::new();
        for round in 0..110 {
            let start = Instant::now();
            let result = query.execute(&graph)?;
            let elapsed = start.elapsed().as_micros();
            assert_eq!(result.rows, [GremlinRow::Count(count)]);
            if round >= 10 {
                samples.push(elapsed);
            }
        }
        samples.sort_unstable();
        println!("{name} p50_us={} p95_us={}", samples[49], samples[94]);
    }
    Ok(())
}
