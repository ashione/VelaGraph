//! Reproducible warm local smoke measurements, not a service/RSS capacity claim.
use std::time::Instant;
use velagraph::{Edge, Error, GraphEngine, GremlinQuery, Value, Vertex};

fn measure(graph: &GraphEngine, name: &str, text: &str, repetitions: usize) {
    let query = GremlinQuery::parse(text).unwrap();
    let mut times = Vec::new();
    let mut rejected = 0;
    for _ in 0..repetitions {
        let now = Instant::now();
        match query.execute(graph) {
            Ok(_) => {}
            Err(Error::TraversalLimitExceeded(_)) => rejected += 1,
            Err(error) => panic!("{name}: {error}"),
        }
        times.push(now.elapsed().as_micros());
    }
    times.sort_unstable();
    println!(
        "{name} iterations={repetitions} p50_us={} p95_us={} rejected={rejected}",
        times[times.len() / 2],
        times[(times.len() * 95 / 100).min(times.len() - 1)]
    );
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let graph = GraphEngine::open(dir.path())?;
    for id in 0..10_000 {
        let mut vertex = Vertex::new(id);
        vertex.labels.push("V".into());
        vertex
            .properties
            .insert("n".into(), Value::Integer(id as i64));
        graph.create_vertex(&vertex)?;
    }
    measure(&graph, "scan_10k_count", "g.V().count()", 50);
    measure(&graph, "result_100", "g.V().limit(100).values('n')", 100);
    measure(&graph, "result_1000", "g.V().limit(1000).values('n')", 100);
    for id in 0..8 {
        graph.create_edge(&Edge::new(id, 0, 0, "E"))?;
    }
    measure(
        &graph,
        "three_hop_512",
        "g.V(0).out().out().out().path()",
        100,
    );
    let mut short = Vertex::new(10_000);
    for id in 0..1_000 {
        short.properties.insert(format!("p{id}"), Value::Null);
    }
    graph.create_vertex(&short)?;
    measure(&graph, "short_properties_1000", "g.V(10000).count()", 100);
    let mut big = Vertex::new(10_001);
    big.properties
        .insert("big".into(), Value::Text("x".repeat(1024 * 1024)));
    graph.create_vertex(&big)?;
    for id in 100..200 {
        graph.create_edge(&Edge::new(id, 1, 10_001, "BIG"))?;
    }
    measure(&graph, "parallel_100_1mib", "g.V(1).out().count()", 20);
    Ok(())
}
