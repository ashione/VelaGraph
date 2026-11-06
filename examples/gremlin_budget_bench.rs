//! Reproducible in-process Gremlin budget smoke benchmark, not a capacity claim.
use rocksdb_sql_table::{Edge, Error, GraphEngine, GremlinQuery, Vertex};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let graph = GraphEngine::open(dir.path())?;
    graph.create_vertex(&Vertex::new(1))?;
    graph.create_vertex(&Vertex::new(2))?;
    for id in 0..100 {
        graph.create_edge(&Edge::new(id, 1, 2, "E"))?;
    }
    let query = GremlinQuery::parse("g.V(1).out().count()")?;
    let started = Instant::now();
    for _ in 0..1_000 {
        query.execute(&graph)?;
    }
    println!(
        "fanout=100 iterations=1000 mean_us={:.2}",
        started.elapsed().as_micros() as f64 / 1_000.0
    );
    for id in 100..100_000 {
        graph.create_edge(&Edge::new(id, 1, 2, "E"))?;
    }
    let started = Instant::now();
    for _ in 0..20 {
        assert!(matches!(
            query.execute(&graph),
            Err(Error::TraversalLimitExceeded(_))
        ));
    }
    println!(
        "fanout=100000 rejected_iterations=20 mean_ms={:.2}",
        started.elapsed().as_secs_f64() * 1_000.0 / 20.0
    );
    Ok(())
}
