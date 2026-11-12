//! VelaGraph property-graph storage and Gremlin query kernel.
//!
//! Provides typed vertices and edges, deterministic graph records, bounded
//! traversals, snapshot reads, and single-partition write transactions.

mod error;
mod global_scan;
mod graph;
mod graph_records;
mod gremlin;
mod query_memory;
mod storage_node;
mod traversal;
mod value;

pub use error::{Error, Result};
pub use global_scan::{GlobalScanOutput, GlobalScanPlan, QualifiedGremlinRow};
pub use graph::{Edge, GraphEngine, GraphReadView, GraphTransaction, Vertex};
pub use graph_records::{
    GraphRecordReadView, GraphRecordStore, PreparedGraphMutation, GRAPH_QUERY_PAGE_BYTES,
    MAX_ADJACENCY_EDGES, MAX_GRAPH_BATCH_OPERATIONS,
};
pub use gremlin::{
    GremlinQuery, GremlinResult, GremlinRow, GremlinStart, GremlinStep, ObservedGremlinQuery,
    ObservedGremlinStep, PathElement, DEFAULT_GREMLIN_LIMIT, GREMLIN_DEADLINE,
    MAX_GREMLIN_ACCOUNTED_BYTES, MAX_GREMLIN_EDGE_STEPS, MAX_GREMLIN_LIMIT,
    MAX_GREMLIN_QUERY_BYTES, MAX_GREMLIN_ROWS, MAX_GREMLIN_STRUCTURAL_MARKS,
    MAX_OBSERVED_GREMLIN_REPEAT_TIMES,
};
pub use storage_node::{
    Epoch, Export, KeyRange, KeyValue, LocalStorageNode, ReadView, RequestId, ScanCursor,
    ScanLimit, ScanPage, Sequence, ShardId, StorageAdmin, StorageNode, StorageNodeError, WriteOp,
    WriteRequest, WriteResult, WriteStatus, INTERFACE_VERSION, MAX_VIEW_BYTES, MAX_VIEW_ROWS,
};
pub use traversal::{
    TraversalDirection, TraversalOptions, TraversalPath, TraversalResult, MAX_TRAVERSAL_HOPS,
};
pub use value::Value;

/// Isolated, explicitly non-atomic append-only cross-partition experiment.
pub mod experimental_graph;

/// Experimental bounded semantic reference; not a durable temporal API.
pub mod temporal_reference;

/// Experimental durable local temporal assertions; no distributed certification.
pub mod temporal_store;
