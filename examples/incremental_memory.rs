//! Generated-data comparison of full reconstruction and incremental publication.
use std::{error::Error, time::Instant};
use velagraph::{
    Edge, GremlinQuery, GremlinRow, IncrementalGraph, IncrementalLimits, IncrementalMode,
    IncrementalMutation, Vertex,
};
const VERTICES: u64 = 4096;
const BATCHES: usize = 40;
const CHANGES_PER_BATCH: usize = 32;
const WARMUP: usize = 4;
const IDENTITY: &str = "generated-memory-example";
fn main() -> Result<(), Box<dyn Error>> {
    for mode in [IncrementalMode::Locked, IncrementalMode::Cow] {
        run(mode)?;
    }
    Ok(())
}
fn run(mode: IncrementalMode) -> Result<(), Box<dyn Error>> {
    let vertices: Vec<_> = (1..=VERTICES).map(Vertex::new).collect();
    let mut edges: Vec<_> = (3..=VERTICES)
        .map(|id| Edge::new(id, id, 1, "LINK"))
        .collect();
    let mut graph = IncrementalGraph::from_records(
        IDENTITY,
        mode,
        IncrementalLimits::default(),
        vertices.clone(),
        edges.clone(),
        vec![],
    )?;
    let query = GremlinQuery::parse("g.V(1).in('LINK').count()")?;
    let mut rebuild_ns = Vec::new();
    let mut update_ns = Vec::new();
    let mut copied_entries = 0usize;
    let mut copied_ids = 0usize;
    let dir = tempfile::tempdir()?;
    let checkpoint = dir.path().join("model.bin");
    for batch in 0..WARMUP + BATCHES {
        let mut changes = Vec::new();
        for n in 0..CHANGES_PER_BATCH {
            let index = (batch * CHANGES_PER_BATCH + n) % edges.len();
            let e = &mut edges[index];
            e.to = if e.to == 1 { 2 } else { 1 };
            changes.push(IncrementalMutation::PutEdge(e.clone()));
        }
        let metadata = (batch as u64).to_le_bytes();
        let expected = vec![GremlinRow::Count(
            edges.iter().filter(|e| e.to == 1).count() as u64,
        )];
        let rebuild = || -> Result<(_, u128), Box<dyn Error>> {
            let start = Instant::now();
            let model = IncrementalGraph::from_records(
                IDENTITY,
                mode,
                IncrementalLimits::default(),
                vertices.clone(),
                edges.clone(),
                metadata.to_vec(),
            )?;
            Ok((model, start.elapsed().as_nanos()))
        };
        let update = || -> Result<(_, u128), Box<dyn Error>> {
            let start = Instant::now();
            let stats = graph.apply(&graph.token()?, &changes, &metadata)?;
            Ok((stats, start.elapsed().as_nanos()))
        };
        let (rebuilt, build_time, stats, update_time) = if batch.is_multiple_of(2) {
            let (a, t) = rebuild()?;
            let (b, u) = update()?;
            (a, t, b, u)
        } else {
            let (b, u) = update()?;
            let (a, t) = rebuild()?;
            (a, t, b, u)
        };
        assert_eq!(rebuilt.read()?.execute_query(&query)?.rows, expected);
        assert_eq!(graph.read()?.execute_query(&query)?.rows, expected);
        assert_eq!(graph.read()?.metadata(), metadata);
        copied_entries += stats.copied_index_entries;
        copied_ids += stats.copied_adjacency_ids;
        if batch >= WARMUP {
            rebuild_ns.push(build_time);
            update_ns.push(update_time);
        }
        if batch == (WARMUP + BATCHES) / 2 {
            let token = graph.token()?;
            graph.checkpoint(&checkpoint)?;
            drop(graph);
            graph = IncrementalGraph::restore(
                &checkpoint,
                IDENTITY,
                mode,
                IncrementalLimits::default(),
            )?;
            assert_eq!(graph.token()?, token);
            assert_eq!(graph.read()?.metadata(), metadata);
        }
    }
    rebuild_ns.sort_unstable();
    update_ns.sort_unstable();
    println!("mode={mode:?}; vertices={VERTICES}; samples={BATCHES}; changes_per_batch={CHANGES_PER_BATCH}; rebuild_p50_us={:.3}; incremental_p50_us={:.3}; copied_index_entries={copied_entries}; copied_adjacency_ids={copied_ids}",rebuild_ns[BATCHES/2]as f64/1000.,update_ns[BATCHES/2]as f64/1000.);
    println!("Both paths are memory-only; input generation, correctness queries and checkpoint I/O are outside update timers. No production SLA or per-commit durability claim.");
    Ok(())
}
