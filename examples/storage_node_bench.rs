//! Reproducible local benchmark for the D1 storage-node adapter.
//!
//! Run with:
//!
//! ```text
//! cargo run --release --example storage_node_bench --locked
//! ```
//!
//! The benchmark uses a fresh temporary database, 100,000 opaque keys with
//! 100-byte values, and 100-operation batches. Every batch is fsynced because
//! the adapter uses `WriteOptions::set_sync(true)`, so the write number includes
//! the durability cost of this prototype. Read numbers are taken from a stable
//! `ReadView`; the view itself copies the partition and is measured separately.
//! Read operations report the fastest of five trials.
//!
//! These numbers describe one local run on the machine printed at the end; they
//! are not a production capacity claim.

use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use velagraph::{KeyRange, LocalStorageNode, ScanLimit, StorageNode, WriteOp, WriteRequest};

const KEYS: usize = 100_000;
const VALUE_BYTES: usize = 100;
const BATCH_OPS: usize = 100;
const READ_TRIALS: usize = 5;

fn key(index: usize) -> Vec<u8> {
    format!("bench/{index:08}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    let mut value = vec![0_u8; VALUE_BYTES];
    value[..8].copy_from_slice(&(index as u64).to_be_bytes());
    value
}

fn request(sequence: u64, operations: Vec<WriteOp>) -> WriteRequest {
    let mut request_id = [0_u8; 16];
    request_id[..8].copy_from_slice(&sequence.to_be_bytes());
    request_id[8..].copy_from_slice(&sequence.to_be_bytes());
    WriteRequest {
        shard: 7,
        epoch: 0,
        request_id,
        operations,
    }
}

fn measure(mut run: impl FnMut(), trials: usize) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..trials {
        let start = Instant::now();
        run();
        best = best.min(start.elapsed());
    }
    best
}

fn rate(operations: usize, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn latency_us(operations: usize, elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * 1_000_000.0 / operations as f64
}

fn main() {
    let directory = std::env::temp_dir().join(format!(
        "velagraph-storage-node-bench-{}",
        std::process::id()
    ));
    if directory.exists() {
        std::fs::remove_dir_all(&directory).expect("clean old benchmark directory");
    }
    std::fs::create_dir_all(&directory).expect("create benchmark directory");
    let path: PathBuf = directory.join("node");

    let node = LocalStorageNode::open(&path, 7).expect("open storage node");
    let keys = (0..KEYS).map(key).collect::<Vec<_>>();
    let write_start = Instant::now();
    let mut batches = 0;
    for chunk in (0..KEYS).step_by(BATCH_OPS) {
        let operations = (chunk..(chunk + BATCH_OPS).min(KEYS))
            .map(|index| WriteOp::put(key(index), value(index)))
            .collect::<Vec<_>>();
        node.batch_write(request(batches as u64 + 1, operations))
            .expect("write batch");
        batches += 1;
    }
    let write_elapsed = write_start.elapsed();

    let view_start = Instant::now();
    let view = node.read_view().expect("read view");
    let view_elapsed = view_start.elapsed();
    assert_eq!(view.sequence, batches as u64);

    let get_elapsed = measure(
        || {
            let mut checksum = 0_usize;
            for key in &keys {
                checksum += black_box(node.get(black_box(key), &view))
                    .expect("get")
                    .expect("present")
                    .len();
            }
            black_box(checksum);
        },
        READ_TRIALS,
    );

    let scan_limit = ScanLimit::new(KEYS, 64 * 1024 * 1024).expect("scan limit");
    let scan_elapsed = measure(
        || {
            let rows = node
                .scan(black_box(KeyRange::all()), scan_limit, &view)
                .expect("scan");
            black_box(rows);
        },
        READ_TRIALS,
    );
    let scan_rows = node
        .scan(KeyRange::all(), scan_limit, &view)
        .expect("scan")
        .len();

    drop(view);
    drop(node);

    let reopen_start = Instant::now();
    let reopened = LocalStorageNode::open(&path, 7).expect("reopen storage node");
    let reopen_elapsed = reopen_start.elapsed();
    let reopened_view = reopened.read_view().expect("read view after reopen");
    assert_eq!(reopened_view.sequence, batches as u64);
    assert_eq!(
        reopened
            .get(&key(KEYS - 1), &reopened_view)
            .expect("get after reopen"),
        Some(value(KEYS - 1))
    );
    drop(reopened);

    println!("D1 StorageNode local benchmark");
    println!("keys={KEYS} value_bytes={VALUE_BYTES} batch_ops={BATCH_OPS} sync=true");
    println!(
        "batch_write: {:.0} ops/s, {:.3} ms/batch ({} batches)",
        rate(KEYS, write_elapsed),
        write_elapsed.as_secs_f64() * 1000.0 / batches as f64,
        batches
    );
    println!(
        "read_view: {:.3} ms, {:.0} rows/s",
        view_elapsed.as_secs_f64() * 1000.0,
        rate(KEYS, view_elapsed)
    );
    println!(
        "get: {:.0} ops/s, {:.3} us/op",
        rate(KEYS, get_elapsed),
        latency_us(KEYS, get_elapsed)
    );
    println!(
        "scan: {:.0} rows/s, {:.3} ms/scan ({} rows)",
        rate(scan_rows, scan_elapsed),
        scan_elapsed.as_secs_f64() * 1000.0,
        scan_rows
    );
    println!("reopen: {:.3} ms", reopen_elapsed.as_secs_f64() * 1000.0);
    println!("host: {}", std::env::consts::OS);
    let _ = std::fs::remove_dir_all(&directory);
}
