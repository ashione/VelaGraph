//! Synthetic, local query-path comparison. No service, dataset or network needed.
//! Run this identical example on the baseline and candidate release builds.
use std::time::{Duration, Instant};
use velagraph::{
    temporal_reference::Interval,
    temporal_store::{TemporalMutation, TemporalStore},
    GremlinQuery, GremlinRow, Value, Vertex,
};

fn mutation(id: u64, from: i64, to: Option<i64>, value: i64) -> TemporalMutation {
    let mut vertex = Vertex::new(id);
    vertex
        .properties
        .insert("value".into(), Value::Integer(value));
    TemporalMutation::Vertex {
        valid: Interval { from, to },
        vertex,
    }
}
fn report(shape: usize, phase: &str, mut ns: Vec<u128>) {
    let mean = ns.iter().sum::<u128>() as f64 / ns.len() as f64 / 1000.0;
    ns.sort_unstable();
    let percentile = |p: usize| ns[(ns.len() * p).div_ceil(100) - 1] as f64 / 1000.0;
    println!(
        "{shape},{phase},{},{mean:.3},{:.3},{:.3},{:.3}",
        ns.len(),
        percentile(50),
        percentile(95),
        percentile(99)
    );
}
fn read(store: &TemporalStore, query: &GremlinQuery, time: i64, expected: i64) -> Duration {
    let start = Instant::now();
    let result = store
        .view(None, time)
        .unwrap()
        .execute_query(query)
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(
        result.rows,
        vec![GremlinRow::Value(Value::Integer(expected))]
    );
    elapsed
}
fn main() {
    let samples: usize = std::env::args()
        .nth(1)
        .map(|s| s.parse().unwrap())
        .unwrap_or(10000);
    assert!((100..=1_000_000).contains(&samples));
    println!("revisions,phase,samples,mean_us,p50_us,p95_us,p99_us");
    for count in [1, 32, 256] {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        let changes: Vec<_> = (0..count)
            .map(|i| mutation(1, (i * 10) as i64, Some((i * 10 + 10) as i64), i as i64))
            .collect();
        store.commit(&changes).unwrap();
        let query = GremlinQuery::parse("g.V(1).values('value')").unwrap();
        report(
            count,
            "cold_first",
            vec![read(&store, &query, (count * 10 - 1) as i64, (count - 1) as i64).as_nanos()],
        );
        let mut warm = Vec::with_capacity(samples);
        for i in 0..samples {
            let value = (i * 17 % count) as i64;
            warm.push(read(&store, &query, value * 10 + 5, value).as_nanos());
        }
        report(count, "warm_varying_time", warm);
        let mut commits = Vec::new();
        let mut after = Vec::new();
        for i in 0..100 {
            let start = Instant::now();
            store.commit(&[mutation(2, 0, None, i)]).unwrap();
            commits.push(start.elapsed().as_nanos());
            let value = (i as usize * 17 % count) as i64;
            after.push(read(&store, &query, value * 10 + 5, value).as_nanos());
        }
        report(count, "unrelated_commit", commits);
        report(count, "read_after_unrelated_commit", after);
    }
}
