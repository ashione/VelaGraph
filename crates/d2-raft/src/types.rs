use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogCommand {
    Noop,
    Put(String, String),
    Delete(String),
}

impl LogCommand {
    pub fn put(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Put(key.into(), value.into())
    }

    pub fn delete(key: impl Into<String>) -> Self {
        Self::Delete(key.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyResult {
    pub value: Option<String>,
}

openraft::declare_raft_types!(
    /// Raft type configuration for the D2 prototype.
    pub TypeConfig:
        D = LogCommand,
        R = ApplyResult,
        NodeId = NodeId,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);
