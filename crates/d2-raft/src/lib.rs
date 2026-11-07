//! D2 Raft prototype.
//!
//! This crate proves the openraft integration before D1 wiring lands. It has
//! two transports: an in-process network for deterministic tests, and a TCP
//! transport that runs one node per OS process. The TCP node and the durable
//! cluster tests persist Raft log and state-machine data in RocksDB.
//!
#![allow(clippy::result_large_err)] // openraft trait signatures fix the error type; boxing would break the trait contract.

//! The Raft log store is adapted from openraft's MIT/Apache-2.0 `memstore`
//! example (https://github.com/databendlabs/openraft); see NOTICE.

mod blocking;
mod d1;
mod durable_log_store;
mod log_store;
mod network;
mod node;
mod state_machine;
pub mod tcp;
pub mod transaction_decisions;
mod types;

pub use d1::{
    D1Capacity, D1Command, D1Error, D1ManagedMode, D1ManagedStatus, D1Result, D1WriteRequest,
    D1WriteStatus, EXTENDED_DEDUP_BYTES, MAX_BATCH_BYTES, MAX_BATCH_OPERATIONS, MAX_DEDUP_BYTES,
    MAX_DEDUP_REQUESTS,
};
pub use durable_log_store::DurableLogStore;
pub use log_store::LogStore;
pub use network::{DropPolicy, GroupId, NetworkFactory, Registry};
pub use node::{Cluster, Node, NodeHandle};
pub use state_machine::{
    D1ReadLimit, D1ReadSnapshot, StateMachine, StateMachineData, StateMachinePause,
};
pub use tcp::{
    add_learner, change_membership, initialize_cluster, send_control, ControlRequest,
    ControlResponse, NodeStatus, TcpNode, TcpNodeConfig,
};
pub use types::{ApplyResult, LogCommand, NodeId, TypeConfig, WriteOp};
