use thiserror::Error;

/// Errors returned by the SQL table engine.
#[derive(Debug, Error)]
pub enum Error {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("SQL parse error: {0}")]
    SqlParse(#[from] sqlparser::parser::ParserError),

    #[error("JSON encoding error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("table `{0}` does not exist")]
    TableNotFound(String),

    #[error("table `{0}` already exists")]
    TableAlreadyExists(String),

    #[error("column `{column}` does not exist in table `{table}`")]
    ColumnNotFound { table: String, column: String },

    #[error("column `{0}` is specified more than once")]
    DuplicateColumn(String),

    #[error("table `{0}` must declare exactly one primary key")]
    MissingPrimaryKey(String),

    #[error("table `{0}` declares more than one primary key")]
    MultiplePrimaryKeys(String),

    #[error("primary key must be a single column; composite primary keys are not supported")]
    CompositePrimaryKey,

    #[error("primary key column `{0}` must use INT or TEXT")]
    InvalidPrimaryKeyType(String),

    #[error("invalid primary key value: {0}")]
    InvalidPrimaryKeyValue(String),

    #[error("NOT NULL constraint failed for column `{0}`")]
    NotNullConstraint(String),

    #[error("primary key column `{0}` cannot be NULL")]
    NullPrimaryKey(String),

    #[error("duplicate primary key value {0}")]
    DuplicatePrimaryKey(String),

    #[error("index `{0}` already exists")]
    IndexAlreadyExists(String),

    #[error("index `{0}` does not exist")]
    IndexNotFound(String),

    #[error("duplicate key for unique index `{index}`: {value}")]
    DuplicateIndexValue { index: String, value: String },

    #[error("type mismatch for column `{column}`: expected {expected}, got {actual}")]
    TypeMismatch {
        column: String,
        expected: String,
        actual: String,
    },

    #[error("cannot compare {left} with {right}")]
    IncomparableTypes { left: String, right: String },

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("invalid literal: {0}")]
    InvalidLiteral(String),

    #[error("invalid identifier `{0}`: identifiers must match [A-Za-z_][A-Za-z0-9_]* and be at most 64 bytes")]
    InvalidIdentifier(String),

    #[error("invalid row: expected {expected} values, got {actual}")]
    InvalidRow { expected: usize, actual: usize },

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

    #[error("unsupported SQL: {0}")]
    Unsupported(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("storage node error: {0}")]
    StorageNode(#[from] crate::storage_node::StorageNodeError),
}

pub type Result<T> = std::result::Result<T, Error>;
