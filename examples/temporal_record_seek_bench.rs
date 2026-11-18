//! Generated temporal edge runs; compare the same example across revisions.
use std::hint::black_box;
use std::time::Instant;
use velagraph::{
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, GremlinQuery, GremlinRow, Vertex,
};

const CHILDREN: u64 = 256;
const PERIODS: u64 = 4;
const INTERVAL_WIDTH: i64 = 5;
const QUERY_TIME: i64 = 20;
const BATCH_SIZE: usize = 256;
const WARMUP: usize = 10;
const SAMPLES: usize = 200;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    {
        let store = TemporalStore::open(dir.path())?;
        let mut changes: Vec<_> = (1..=CHILDREN + 1)
            .map(|id| TemporalMutation::Vertex {
                valid: Interval { from: 0, to: None },
                vertex: Vertex::new(id),
            })
            .collect();
        for child in 0..CHILDREN {
            for period in 0..PERIODS {
                changes.push(TemporalMutation::Edge {
                    valid: Interval {
                        from: period as i64 * INTERVAL_WIDTH,
                        to: (period + 1 < PERIODS).then_some((period as i64 + 1) * INTERVAL_WIDTH),
                    },
                    edge: Edge::new(child * PERIODS + period + 1, 1, child + 2, "LINK"),
                });
            }
        }
        for batch in changes.chunks(BATCH_SIZE) {
            store.commit(batch)?;
        }
    }
    // Reopen so the benchmark includes the persisted store read path.
    let store = TemporalStore::open(dir.path())?;
    let view = store.view(None, QUERY_TIME)?;
    let query = GremlinQuery::parse("g.V(1).out('LINK').count()")?;
    let mut samples = Vec::with_capacity(SAMPLES);
    for round in 0..WARMUP + SAMPLES {
        let start = Instant::now();
        let result = view.execute_query(&query)?;
        black_box(&result);
        let elapsed = start.elapsed().as_nanos() as u64;
        assert_eq!(result.rows, vec![GremlinRow::Count(CHILDREN)]);
        if round >= WARMUP {
            samples.push(elapsed);
        }
    }
    samples.sort_unstable();
    let percentile = |p: usize| samples[(samples.len() - 1) * p / 100] as f64 / 1000.0;
    println!(
        "{}",
        serde_json::json!({"children": CHILDREN, "temporal_edges": CHILDREN * PERIODS,
        "warmup": WARMUP, "samples": SAMPLES,
        "mean_us": samples.iter().map(|v| *v as f64).sum::<f64>() / SAMPLES as f64 / 1000.0,
        "p50_us": percentile(50), "p95_us": percentile(95), "p99_us": percentile(99)})
    );
    Ok(())
}
