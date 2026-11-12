use thiserror::Error;

/// Errors returned by the graph kernel.
#[derive(Debug, Error)]
pub enum Error {
    #[error("temporal store error: {0}")]
    TemporalStore(Box<crate::temporal_store::TemporalStoreError>),

    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("JSON encoding error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("vertex {0} already exists")]
    VertexAlreadyExists(u64),

    #[error("vertex {0} does not exist")]
    VertexNotFound(u64),

    #[error("edge {0} already exists")]
    EdgeAlreadyExists(u64),

    #[error("edge {0} does not exist")]
    EdgeNotFound(u64),

    #[error("edge endpoint vertex {0} does not exist")]
    EdgeEndpointNotFound(u64),

    #[error("invalid graph label `{0}`: labels must match [A-Za-z_][A-Za-z0-9_]* and be at most 64 bytes")]
    InvalidGraphLabel(String),

    #[error("invalid graph property: {0}")]
    InvalidGraphProperty(String),

    #[error("invalid traversal: {0}")]
    InvalidTraversal(String),

    #[error("invalid Gremlin: {0}")]
    GremlinParse(String),

    /// Raw structural-mark admission failed before the recursive parser ran.
    #[error("Gremlin parse complexity limit exceeded: {marks} structural marks; limit is {limit}")]
    GremlinComplexityLimit { marks: usize, limit: usize },

    #[error("unsupported Gremlin step `{0}`")]
    UnsupportedGremlinStep(String),

    #[error("unsupported Gremlin argument: {0}")]
    UnsupportedGremlinArgument(String),

    #[error("unsupported Gremlin value: {0}")]
    UnsupportedGremlinValue(String),

    #[error("invalid Gremlin limit {0}: limit must be between 1 and 1000")]
    InvalidGremlinLimit(i64),

    #[error("Gremlin traversal limit exceeded: {0}")]
    TraversalLimitExceeded(String),

    #[error("invalid Gremlin traverser: {0}")]
    InvalidGremlinTraverser(String),

    #[error("transaction conflict: another write committed after the transaction snapshot")]
    TransactionConflict,

    #[error("transaction write set is too large: {0}")]
    TransactionTooLarge(String),

    #[error("corrupt data in RocksDB: {0}")]
    CorruptData(String),

    #[error("storage node error: {0}")]
    StorageNode(#[from] crate::storage_node::StorageNodeError),
}

pub type Result<T> = std::result::Result<T, Error>;
