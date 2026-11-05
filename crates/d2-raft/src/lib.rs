//! D2 Raft prototype.
//!
//! This crate proves the openraft integration before a durable RocksDB log
//! adapter and D1 wiring land. It has two transports: an in-process network
//! for deterministic tests, and a TCP transport that runs one node per OS
//! process. Storage is still in memory.
//!
#![allow(clippy::result_large_err)] // openraft trait signatures fix the error type; boxing would break the trait contract.

//! The Raft log store is adapted from openraft's MIT/Apache-2.0 `memstore`
//! example (https://github.com/databendlabs/openraft); see NOTICE.

mod log_store;
mod network;
mod node;
mod state_machine;
pub mod tcp;
mod types;

pub use log_store::LogStore;
pub use network::{DropPolicy, NetworkFactory, Registry};
pub use node::{Cluster, Node, NodeHandle};
pub use state_machine::{StateMachine, StateMachineData};
pub use tcp::{send_control, ControlRequest, ControlResponse, NodeStatus, TcpNode, TcpNodeConfig};
pub use types::{ApplyResult, LogCommand, NodeId, TypeConfig};
