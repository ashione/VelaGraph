//! Generated contention workload: version acquisition vs complete read/update.
use std::sync::Barrier;
use std::time::{Duration, Instant};
use velagraph::{IncrementalGraph, IncrementalLimits, IncrementalMode, Value, Vertex};

const VERTICES: u64 = 65_536;
const BATCH_WIDTH: u64 = 32;
const BATCHES: u64 = 120;
const READERS: usize = 4;
const READS_PER_READER: usize = 4_000;
const WARMUP_READS: usize = 100;
const UPDATE_INTERVAL: Duration = Duration::from_millis(1);
const READ_INTERVAL: Duration = Duration::from_micros(50);
const PROPERTY: &str = "generation";
struct Sample {
    start: Instant,
    acquire_ns: u128,
    read_ns: u128,
    generation: u64,
}
fn vertex(id: u64, generation: u64) -> Vertex {
    let mut v = Vertex::new(id);
    v.properties
        .insert(PROPERTY.into(), Value::Integer(generation as i64));
    v
}
fn read(g: &IncrementalGraph) -> Sample {
    let start = Instant::now();
    let view = g.read().unwrap();
    let acquired = Instant::now();
    let generation = u64::from_le_bytes(view.metadata().try_into().unwrap());
    let mut values = Vec::with_capacity(BATCH_WIDTH as usize);
    for id in 0..BATCH_WIDTH {
        values.push(view.get_vertex(id).unwrap().properties[PROPERTY].clone());
    }
    drop(view);
    let end = Instant::now();
    assert!(values
        .iter()
        .all(|v| *v == Value::Integer(generation as i64)));
    Sample {
        start,
        acquire_ns: acquired.duration_since(start).as_nanos(),
        read_ns: end.duration_since(start).as_nanos(),
        generation,
    }
}
fn report(name: &str, samples: &mut [u128]) {
    if samples.is_empty() {
        println!("{name}: samples=0");
        return;
    }
    samples.sort_unstable();
    let at = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1] as f64 / 1000.0;
    println!(
        "{name}: samples={}; p50_us={:.3}; p95_us={:.3}; p99_us={:.3}; max_us={:.3}",
        samples.len(),
        at(50),
        at(95),
        at(99),
        *samples.last().unwrap() as f64 / 1000.0
    );
}
fn main() {
    let g = IncrementalGraph::from_records(
        "generated-publication",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        (0..VERTICES).map(|id| vertex(id, 0)).collect(),
        vec![],
        0_u64.to_le_bytes().to_vec(),
    )
    .unwrap();
    let barrier = Barrier::new(READERS + 1);
    let (samples, mut writes, write_start, write_end) = std::thread::scope(|scope| {
        let readers: Vec<_> = (0..READERS)
            .map(|_| {
                let (g, barrier) = (&g, &barrier);
                scope.spawn(move || {
                    for _ in 0..WARMUP_READS {
                        read(g);
                    }
                    let mut samples = Vec::with_capacity(READS_PER_READER);
                    barrier.wait();
                    for _ in 0..READS_PER_READER {
                        samples.push(read(g));
                        // Pacing and validation are excluded from each read timer.
                        std::thread::sleep(READ_INTERVAL);
                    }
                    samples
                })
            })
            .collect();
        barrier.wait();
        let write_start = Instant::now();
        let mut writes = Vec::with_capacity(BATCHES as usize);
        for generation in 1..=BATCHES {
            let due = write_start + UPDATE_INTERVAL * generation as u32;
            if let Some(wait) = due.checked_duration_since(Instant::now()) {
                std::thread::sleep(wait);
            }
            let start = Instant::now();
            let mut tx = g.begin().unwrap();
            for id in 0..BATCH_WIDTH {
                tx.put_vertex(vertex(id, generation)).unwrap();
            }
            tx.set_metadata(&generation.to_le_bytes()).unwrap();
            tx.commit().unwrap();
            writes.push(start.elapsed().as_nanos());
        }
        let write_end = Instant::now();
        let samples = readers
            .into_iter()
            .flat_map(|reader| reader.join().unwrap())
            .collect::<Vec<_>>();
        (samples, writes, write_start, write_end)
    });
    let mut acquire = Vec::new();
    let mut query = Vec::new();
    let mut idle = Vec::new();
    let mut versions = std::collections::BTreeSet::new();
    for sample in samples {
        if sample.start >= write_start && sample.start <= write_end {
            acquire.push(sample.acquire_ns);
            query.push(sample.read_ns);
            versions.insert(sample.generation);
        } else {
            idle.push(sample.acquire_ns);
        }
    }
    assert!(!acquire.is_empty());
    assert_eq!(g.token().unwrap().sequence, BATCHES);
    println!("vertices={VERTICES}; readers={READERS}; batches={BATCHES}; width={BATCH_WIDTH}; observed_active_versions={}; write_window_ms={:.3}", versions.len(),write_end.duration_since(write_start).as_secs_f64()*1000.0);
    report("active_acquire", &mut acquire);
    report("active_read_32_properties_including_acquire", &mut query);
    report("idle_acquire", &mut idle);
    report("update", &mut writes);
    println!("Active samples START within the writer start/end window (including paced gaps); reader warmup finishes before the start barrier. Local direct record reads, not RPC/Gremlin or throughput. Repeat with alternating revision order; scheduler and allocator influence tails.");
}
