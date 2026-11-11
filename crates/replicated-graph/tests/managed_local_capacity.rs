//! Fresh mixed-API managed capacity acceptance in independent OS processes.
//! This ordinary target is deliberately separate from managed_process: adding
//! this fixture must not serialize or alter that target's original scenarios.
include!("support/managed_process_fixture.rs");

#[path = "support/managed_local_capacity.rs"]
mod managed_local_capacity;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expanded_local_read_crosses_original_capture_boundary() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(180), managed_local_capacity::scenario()).await?
}
