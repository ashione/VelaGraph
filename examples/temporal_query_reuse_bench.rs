//! Synthetic, local query-reuse comparison. No external data or service needed.
//! All lanes use the current read backend; only plan reuse differs.
use std::hint::black_box;
use std::time::Instant;
use velagraph::{
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    Edge, GremlinBindings, GremlinParameters, GremlinQuery, GremlinRow, Vertex,
};

const ROOTS: u64 = 1280; // Exceeds the text-plan cache's entry capacity.
const FIXED_CASES: usize = 64;
const MUTATIONS_PER_COMMIT: usize = 256;
const TRIALS: usize = 3;
const WARMUP_ROUNDS: usize = 1;
const MEASURED_ROUNDS: usize = 3;

#[derive(Clone, Copy)]
enum Lane {
    Parse,
    Text,
    Prepared,
}
impl Lane {
    fn name(self) -> &'static str {
        match self {
            Self::Parse => "parse_each_call",
            Self::Text => "cached_text",
            Self::Prepared => "typed_prepared",
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let store = TemporalStore::open(dir.path())?;
    let valid = Interval { from: 0, to: None };
    let mut mutations = vec![TemporalMutation::Vertex {
        valid,
        vertex: Vertex::new(0),
    }];
    for id in 1..=ROOTS {
        mutations.push(TemporalMutation::Vertex {
            valid,
            vertex: Vertex::new(id),
        });
        mutations.push(TemporalMutation::Edge {
            valid,
            edge: Edge::new(id, id, 0, "LINK"),
        });
    }
    for batch in mutations.chunks(MUTATIONS_PER_COMMIT) {
        store.commit(batch)?;
    }
    let view = store.view(None, 0)?;
    let prepared =
        GremlinQuery::parse("g.V(0).out('LINK').count()")?.prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![],
        })?;
    let mut output = Vec::new();
    for (workload, ids) in [
        ("fixed_id", vec![1; FIXED_CASES]),
        ("changing_ids", (1..=ROOTS).collect()),
    ] {
        let texts: Vec<_> = ids
            .iter()
            .map(|id| format!("g.V({id}).out('LINK').count()"))
            .collect();
        for trial in 0..TRIALS {
            // Rotate which lane executes first, with the same data and V/T.
            let lanes = [Lane::Parse, Lane::Text, Lane::Prepared];
            for offset in 0..lanes.len() {
                let lane = lanes[(trial + offset) % lanes.len()];
                let mut samples = Vec::with_capacity(ids.len() * MEASURED_ROUNDS);
                for round in 0..WARMUP_ROUNDS + MEASURED_ROUNDS {
                    for (id, text) in ids.iter().zip(&texts) {
                        let start = Instant::now();
                        let result = match lane {
                            Lane::Parse => view.execute_query(&GremlinQuery::parse(text)?)?,
                            Lane::Text => view.execute_gremlin(text)?,
                            Lane::Prepared => view.execute_prepared(
                                &prepared,
                                GremlinBindings {
                                    start_ids: Some(&[*id]),
                                    values: &[],
                                },
                            )?,
                        };
                        black_box(&result);
                        let elapsed = start.elapsed().as_nanos() as u64;
                        assert_eq!(result.rows, vec![GremlinRow::Count(1)]);
                        if round >= WARMUP_ROUNDS {
                            samples.push(elapsed);
                        }
                    }
                }
                samples.sort_unstable();
                let percentile = |p: usize| samples[(samples.len() - 1) * p / 100] as f64 / 1000.0;
                output.push(serde_json::json!({
                    "workload": workload, "lane": lane.name(), "trial": trial + 1,
                    "samples": samples.len(),
                    "mean_us": samples.iter().map(|n| *n as f64).sum::<f64>() / samples.len() as f64 / 1000.0,
                    "p50_us": percentile(50), "p95_us": percentile(95), "p99_us": percentile(99),
                }));
            }
        }
    }
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
