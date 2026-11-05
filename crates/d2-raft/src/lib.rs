//! D2 in-process Raft prototype.
//!
//! This crate proves the openraft integration before a durable RocksDB log
//! adapter and D1 wiring land. It uses an in-memory log store and an in-memory
//! key/value state machine; no HTTP or network ports are involved.
//!
//! The Raft log store is adapted from openraft's MIT/Apache-2.0 `memstore`
//! example (https://github.com/databendlabs/openraft); see NOTICE.

mod log_store;
mod network;
mod node;
mod state_machine;
mod types;

pub use log_store::LogStore;
pub use network::{DropPolicy, NetworkFactory, Registry};
pub use node::{Cluster, Node, NodeHandle};
pub use state_machine::{StateMachine, StateMachineData};
pub use types::{ApplyResult, LogCommand, NodeId, TypeConfig};
