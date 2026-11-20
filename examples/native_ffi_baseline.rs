//! Generated counterpart of sdk/go/native/example; setup is outside timings.
use std::{
    hint::black_box,
    time::{Duration, Instant},
};
use velagraph::{
    CancellationToken, Edge, ExecutionControl, GremlinQuery, GremlinRow, IncrementalGraph,
    IncrementalLimits, IncrementalMode, QueryBatchLimits, Value, Vertex,
};
const CHILDREN: u64 = 256;
const WARMUP: usize = 100;
const ROUNDS: usize = 1000;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let vertices = (1..=CHILDREN + 1)
        .map(|id| {
            let mut v = Vertex::new(id);
            v.properties.insert("n".into(), Value::Integer(id as i64));
            v
        })
        .collect();
    let edges = (2..=CHILDREN + 1)
        .map(|id| Edge::new(id, 1, id, "LINK"))
        .collect();
    let graph = IncrementalGraph::from_records(
        "native-example",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vertices,
        edges,
        vec![],
    )?;
    for (name, texts, expected) in [
        ("point", vec!["g.V(1).values('n')".to_string()], 1),
        (
            "batch16",
            (1..=16)
                .map(|id| format!("g.V({id}).values('n')"))
                .collect(),
            16,
        ),
        (
            "fanout256",
            vec!["g.V(1).out('LINK').values('n').limit(256)".to_string()],
            256,
        ),
    ] {
        let queries: Vec<_> = texts
            .iter()
            .map(|q| GremlinQuery::parse(q).unwrap())
            .collect();
        let mut samples = Vec::with_capacity(ROUNDS);
        for i in 0..WARMUP + ROUNDS {
            let start = Instant::now();
            let control =
                ExecutionControl::new(CancellationToken::default(), Duration::from_secs(5))?;
            let view = graph.read()?;
            let results = black_box(view.execute_query_batch(
                &queries,
                &control,
                QueryBatchLimits::default(),
            )?);
            let elapsed = start.elapsed().as_nanos() as u64;
            let actual: Vec<_> = results.iter().flat_map(|r| r.rows.iter()).collect();
            assert_eq!(actual.len(), expected);
            for (i, row) in actual.iter().enumerate() {
                let id = if name == "fanout256" { i + 2 } else { i + 1 };
                assert_eq!(**row, GremlinRow::Value(Value::Integer(id as i64)));
            }
            if i >= WARMUP {
                samples.push(elapsed);
            }
        }
        samples.sort_unstable();
        println!(
            "{}",
            serde_json::json!({"lane":"rust-direct", "workload":name,
            "rounds":ROUNDS,"rows":expected,"p50_ns":samples[ROUNDS/2],
            "p95_ns":samples[ROUNDS*95/100],"mean_ns":samples.iter().sum::<u64>()/ROUNDS as u64})
        );
    }
    Ok(())
}
