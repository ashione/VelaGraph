//! Diagnostic attempt-local work accounting, never a correctness or latency gate.
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Default)]
pub(super) struct Metrics(Arc<Mutex<BTreeMap<String, Sample>>>);

#[derive(Default, Serialize)]
struct Sample {
    calls: u64,
    bytes: u64,
    maximum_bytes: u64,
    elapsed_ns: u128,
}

pub(super) struct Span {
    metrics: Metrics,
    name: String,
    bytes: u64,
    started: Instant,
}

impl Metrics {
    pub(super) fn span(&self, name: impl Into<String>) -> Span {
        Span {
            metrics: self.clone(),
            name: name.into(),
            bytes: 0,
            started: Instant::now(),
        }
    }

    pub(super) fn work(&self, name: &str, bytes: usize) {
        self.record(name, bytes as u64, 0);
    }

    fn record(&self, name: &str, bytes: u64, elapsed_ns: u128) {
        let mut samples = self.0.lock().expect("lifecycle metrics mutex");
        let sample = samples.entry(name.to_owned()).or_default();
        sample.calls += 1;
        sample.bytes += bytes;
        sample.maximum_bytes = sample.maximum_bytes.max(bytes);
        sample.elapsed_ns += elapsed_ns;
    }

    #[cfg(test)]
    pub(super) fn count(&self, name: &str) -> u64 {
        self.0
            .lock()
            .expect("lifecycle metrics mutex")
            .get(name)
            .map_or(0, |sample| sample.calls)
    }

    pub(super) fn emit(&self, source: &str, elf: &str, ordinal: u64, completed: bool) {
        // Attempts and nested phase spans are not disjoint wall-time totals.
        // stderr is retained on errors/timeouts too. No extra fsync is added to
        // an observed operation, and a metric never changes a production budget.
        let samples = self.0.lock().expect("lifecycle metrics mutex");
        eprintln!(
            "byte-bound lifecycle metrics: {}",
            serde_json::json!({
                "format": 1,
                "source_sha256": source,
                "elf_sha256": elf,
                "verified_ordinal": ordinal,
                "completed": completed,
                "scope": "this attempt; nested spans must not be summed",
                "samples": &*samples,
            })
        );
    }
}

impl Span {
    pub(super) fn bytes(&mut self, bytes: usize) {
        self.bytes = bytes as u64;
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        self.metrics
            .record(&self.name, self.bytes, self.started.elapsed().as_nanos());
    }
}
