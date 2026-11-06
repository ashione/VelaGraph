use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

/// One opaque key/value mutation inside a single-shard batch.
///
/// D1 owns the key format; the consensus layer never interprets these bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WriteOp {
    pub fn put(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        Self::Put {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        }
    }

    pub fn delete(key: impl AsRef<[u8]>) -> Self {
        Self::Delete {
            key: key.as_ref().to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogCommand {
    Noop,
    /// Version 1 D1 ownership, retry and conditional-write contract.
    D1V1(crate::D1Command),
    /// An ordered, atomic batch of D1 key/value mutations.
    TxnBatch(Vec<WriteOp>),
}

impl LogCommand {
    pub fn txn(operations: Vec<WriteOp>) -> Self {
        Self::TxnBatch(operations)
    }

    pub fn put(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        Self::txn(vec![WriteOp::put(key, value)])
    }

    pub fn delete(key: impl AsRef<[u8]>) -> Self {
        Self::txn(vec![WriteOp::delete(key)])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyResult {
    pub value: Option<String>,
    /// None for legacy/no-op entries. Application rejections still advance applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub d1: Option<Result<crate::D1Result, crate::D1Error>>,
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
