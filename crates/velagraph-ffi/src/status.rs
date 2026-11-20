use std::fmt;
use velagraph::{temporal_store::TemporalStoreError, Error, IncrementalMemoryError};
pub const OK: u32 = 0;
pub const INVALID: u32 = 1;
pub const HANDLE: u32 = 2;
pub const UNSUPPORTED: u32 = 3;
pub const LIMIT: u32 = 4;
pub const CONFLICT: u32 = 5;
pub const CANCELLED: u32 = 6;
pub const DEADLINE: u32 = 7;
pub const IO: u32 = 8;
pub const INTERNAL: u32 = 9;
pub const UNKNOWN: u32 = 10;
pub const SMALL: u32 = 11;
pub const BUSY: u32 = 12;
pub const CORRUPT: u32 = 13;
#[derive(Debug)]
pub enum Failure {
    Simple(u32, &'static str),
    Core(Error),
    Temporal(TemporalStoreError),
    Json(serde_json::Error),
}
pub type Result<T> = std::result::Result<T, Failure>;
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Core(e)
    }
}
impl From<TemporalStoreError> for Failure {
    fn from(e: TemporalStoreError) -> Self {
        Self::Temporal(e)
    }
}
impl From<serde_json::Error> for Failure {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simple(_, s) => f.write_str(s),
            Self::Core(e) => fmt::Display::fmt(e, f),
            Self::Temporal(e) => fmt::Display::fmt(e, f),
            Self::Json(e) => fmt::Display::fmt(e, f),
        }
    }
}
fn temporal(e: &TemporalStoreError, write: bool) -> u32 {
    match e {
        TemporalStoreError::Graph(e) => core(e, write),
        TemporalStoreError::Limit => LIMIT,
        TemporalStoreError::UnavailableVersion => CONFLICT,
        TemporalStoreError::Storage(_) | TemporalStoreError::Node(_) => {
            if write {
                UNKNOWN
            } else {
                IO
            }
        }
        TemporalStoreError::Corrupt | TemporalStoreError::IncompatibleStore => CORRUPT,
        _ => INVALID,
    }
}
fn core(e: &Error, write: bool) -> u32 {
    match e {
        Error::ExecutionCancelled => CANCELLED,
        Error::ExecutionDeadlineExceeded => DEADLINE,
        Error::TransactionConflict => CONFLICT,
        Error::TraversalLimitExceeded(_)
        | Error::TransactionTooLarge(_)
        | Error::GremlinComplexityLimit { .. } => LIMIT,
        Error::UnsupportedGremlinStep(_)
        | Error::UnsupportedGremlinArgument(_)
        | Error::UnsupportedGremlinValue(_) => UNSUPPORTED,
        Error::TemporalStore(e) => temporal(e, write),
        Error::RocksDb(_) | Error::StorageNode(_) => {
            if write {
                UNKNOWN
            } else {
                IO
            }
        }
        Error::CorruptData(_) => CORRUPT,
        Error::IncrementalMemory(e) => match e.as_ref() {
            IncrementalMemoryError::Io(_) => {
                if write {
                    UNKNOWN
                } else {
                    IO
                }
            }
            _ => CORRUPT,
        },
        _ => INVALID,
    }
}
impl Failure {
    pub fn code(&self, write: bool) -> u32 {
        match self {
            Self::Simple(c, _) => *c,
            Self::Core(e) => core(e, write),
            Self::Temporal(e) => temporal(e, write),
            Self::Json(_) => INVALID,
        }
    }
}
pub fn invalid(s: &'static str) -> Failure {
    Failure::Simple(INVALID, s)
}
pub fn limit(s: &'static str) -> Failure {
    Failure::Simple(LIMIT, s)
}
pub fn unsupported(s: &'static str) -> Failure {
    Failure::Simple(UNSUPPORTED, s)
}
