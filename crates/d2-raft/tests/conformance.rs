//! Openraft storage conformance tests for the durable RocksDB stores.

use std::sync::Arc;

use d2_raft::{DurableLogStore, StateMachine, TypeConfig};
use openraft::testing::StoreBuilder;
use openraft::{AnyError, StorageError, StorageIOError};
use tempfile::TempDir;

#[derive(Clone, Copy, Default)]
struct RocksStoreBuilder;

fn storage_error(error: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::read(AnyError::error(error.to_string())).into()
}

impl StoreBuilder<TypeConfig, DurableLogStore, Arc<StateMachine>, TempDir> for RocksStoreBuilder {
    async fn build(
        &self,
    ) -> Result<(TempDir, DurableLogStore, Arc<StateMachine>), StorageError<u64>> {
        let dir = tempfile::tempdir().map_err(storage_error)?;
        let log = DurableLogStore::open(dir.path().join("raft.db")).map_err(storage_error)?;
        let state_machine =
            Arc::new(StateMachine::open(dir.path().join("state.db")).map_err(storage_error)?);
        Ok((dir, log, state_machine))
    }
}

#[test]
fn durable_stores_pass_the_openraft_storage_suite() {
    openraft::testing::Suite::<
        TypeConfig,
        DurableLogStore,
        Arc<StateMachine>,
        RocksStoreBuilder,
        TempDir,
    >::test_all(RocksStoreBuilder)
    .expect("durable RocksDB stores must satisfy openraft's storage contract");
}
