//! Reproducible generated-data comparison; construction is reported separately.
use std::error::Error;
use std::time::Instant;
use velagraph::{
    temporal_reference::Interval,
    temporal_store::{materialized::MaterializationLimits, TemporalMutation, TemporalStore},
    Edge, GremlinQuery, GremlinRow, Vertex,
};
const EDGES: u64 = 1024;
const ACTIVE_STRIDE: u64 = 4;
const BATCH_SIZE: usize = 256;
const WARMUP: usize = 10;
const SAMPLES: usize = 200;
fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let store = TemporalStore::open(dir.path())?;
    let mut changes: Vec<_> = (1..=EDGES + 1)
        .map(|id| TemporalMutation::Vertex {
            valid: Interval { from: 0, to: None },
            vertex: Vertex::new(id),
        })
        .collect();
    changes.extend((1..=EDGES).map(|id| TemporalMutation::Edge {
        valid: Interval {
            from: 0,
            to: if id.is_multiple_of(ACTIVE_STRIDE) {
                None
            } else {
                Some(1)
            },
        },
        edge: Edge::new(id, 1, id + 1, "LINK"),
    }));
    for batch in changes.chunks(BATCH_SIZE) {
        store.commit(batch)?;
    }
    let started = Instant::now();
    let model = store.materialize(None, MaterializationLimits::default())?;
    let build_us = started.elapsed().as_micros();
    let disk = store.view(Some(model.version()), 2)?;
    let memory = model.view(2);
    let query = GremlinQuery::parse("g.V(1).out('LINK').count()")?;
    let expected = vec![GremlinRow::Count(EDGES / ACTIVE_STRIDE)];
    let mut disk_ns = Vec::new();
    let mut model_ns = Vec::new();
    for round in 0..WARMUP + SAMPLES {
        for materialized in if round.is_multiple_of(2) {
            [false, true]
        } else {
            [true, false]
        } {
            let start = Instant::now();
            let result = if materialized {
                memory.execute_query(&query)?
            } else {
                disk.execute_query(&query)?
            };
            let elapsed = start.elapsed().as_nanos();
            assert_eq!(result.rows, expected);
            if round >= WARMUP {
                if materialized {
                    model_ns.push(elapsed);
                } else {
                    disk_ns.push(elapsed);
                }
            }
        }
    }
    disk_ns.sort_unstable();
    model_ns.sort_unstable();
    println!(
        "generated edges={EDGES}; visible={}; samples={SAMPLES}",
        EDGES / ACTIVE_STRIDE
    );
    println!(
        "materialization_us={build_us}; retained_accounted_bytes={}",
        model.stats().retained_accounted_bytes
    );
    println!(
        "disk_p50_us={:.3}; model_p50_us={:.3}",
        disk_ns[SAMPLES / 2] as f64 / 1000.0,
        model_ns[SAMPLES / 2] as f64 / 1000.0
    );
    println!(
        "Warm single-process query timing excludes parse/build; logical accounting is not RSS."
    );
    Ok(())
}
