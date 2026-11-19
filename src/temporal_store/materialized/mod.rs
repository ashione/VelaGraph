//! Bounded, owned temporal read models at a fixed local system version.
//!
//! Construction validates one source snapshot before publishing a model. A model
//! owns its records and remains usable after later commits or source closure.
//! It never refreshes implicitly. Reuse parsed/prepared queries for hot paths.
use super::*;
use crate::query_memory::{Accounted, AccountedVec, Budget};
use crate::{GremlinBindings, GremlinQuery, GremlinResult, GremlinStart, PreparedGremlinQuery};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

mod build;
mod query;
#[cfg(test)]
mod tests;

const MAX_ITEMS: usize = 1_000_000;
const MAX_BUILD_BYTES: usize = 256 * 1024 * 1024;
const BUILD_DEADLINE: Duration = Duration::from_secs(10);
const MAX_QUERY_OPERATIONS: usize = 65_536;
const ROW_WORK_BYTES: usize = 48;
const REVISION_SCRATCH_FEE: usize = 1024;
const SLOT_FEE: usize = 256;
const CANDIDATE_FEE: usize = 256;
const WORK_MESSAGE: &str = "materialized temporal query work limit";
const GLOBAL_MESSAGE: &str = "materialized temporal global start is unsupported";

/// Construction ceilings. Values may tighten, but not exceed, the defaults.
/// Accounting is conservative logical admission, not a process RSS limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaterializationLimits {
    /// Maximum combined revision and adjacency entries read (default 1,000,000).
    pub max_source_rows: usize,
    /// Maximum combined source key/value bytes read (default 64 MiB).
    pub max_raw_bytes: usize,
    /// Maximum cumulative construction and retained logical charges (256 MiB).
    pub max_accounted_bytes: usize,
}
impl Default for MaterializationLimits {
    fn default() -> Self {
        Self {
            max_source_rows: MAX_ITEMS,
            max_raw_bytes: MAX_READ_BYTES,
            max_accounted_bytes: MAX_BUILD_BYTES,
        }
    }
}

/// Per-query work ceilings, separate from the executor's live-memory budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaterializedQueryLimits {
    /// Point resolutions, endpoint checks and visited candidates (maximum 65,536).
    pub max_operations: usize,
    /// Metadata visits plus accounted copied-record bytes (maximum 64 MiB).
    pub max_work_bytes: usize,
}
impl Default for MaterializedQueryLimits {
    fn default() -> Self {
        Self {
            max_operations: MAX_QUERY_OPERATIONS,
            max_work_bytes: MAX_READ_BYTES,
        }
    }
}

/// Construction diagnostics. Counts cover all valid times at the selected V.
/// Neither accounted-byte field measures heap allocation or RSS.
#[derive(Clone, Debug, Serialize)]
pub struct MaterializationStats {
    /// Selected local system version; not globally unique or a cohort version.
    pub version: u64,
    /// Source snapshot's local head, which can be later than the selected V.
    pub source_head: u64,
    /// Canonical revision entries validated at the source head.
    pub scanned_revisions: usize,
    /// Adjacency entries checked against canonical positive assertions.
    pub scanned_candidates: usize,
    /// Combined key/value bytes of those source entries.
    pub scanned_raw_bytes: usize,
    /// Distinct selected positive payloads retained by compiled time segments.
    pub payloads: usize,
    /// Positive disjoint time segments, across vertices and edges.
    pub resolved_segments: usize,
    /// Vertex identities, including absent identities and referenced placeholders.
    pub vertex_slots: usize,
    /// Edge identities, including those with no selected positive segment.
    pub edge_slots: usize,
    /// Covering rows, one per selected positive edge segment.
    pub adjacency_rows: usize,
    /// References to covering rows in both directional lists.
    pub adjacency_references: usize,
    /// Allocated covering-row capacity in bytes.
    pub row_storage_bytes: usize,
    /// Conservative retained payload, container-capacity and map-entry charges.
    pub retained_accounted_bytes: usize,
    /// Cumulative construction admission charges, including released scratch.
    pub build_accounted_bytes: usize,
}

struct Meter {
    limits: MaterializationLimits,
    charged: usize,
    rows: usize,
    revisions: usize,
    candidates: usize,
    raw: usize,
    deadline: Instant,
}
impl Meter {
    fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            Err(TemporalStoreError::Limit)
        } else {
            Ok(())
        }
    }
    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.check()?;
        self.charged = self
            .charged
            .checked_add(bytes)
            .ok_or(TemporalStoreError::Limit)?;
        if self.charged > self.limits.max_accounted_bytes {
            return Err(TemporalStoreError::Limit);
        }
        Ok(())
    }
    fn scanned(&mut self, bytes: usize, revision: bool) -> Result<()> {
        self.rows = self.rows.checked_add(1).ok_or(TemporalStoreError::Limit)?;
        self.raw = self
            .raw
            .checked_add(bytes)
            .ok_or(TemporalStoreError::Limit)?;
        if self.rows > self.limits.max_source_rows || self.raw > self.limits.max_raw_bytes {
            return Err(TemporalStoreError::Limit);
        }
        if revision {
            self.revisions += 1;
        } else {
            self.candidates += 1;
        }
        self.charge(
            bytes
                .checked_add(if revision { REVISION_SCRATCH_FEE } else { 0 })
                .ok_or(TemporalStoreError::Limit)?,
        )
    }
}
enum Record {
    Vertex(Vertex),
    Edge(Edge),
}
struct Cached {
    record: Record,
    bytes: usize,
}
#[derive(Clone, Copy, Debug)]
struct Piece {
    from: i64,
    to: Option<i64>,
    payload: u32,
}
impl Piece {
    fn contains(&self, time: i64) -> bool {
        self.from <= time && self.to.is_none_or(|end| time < end)
    }
}
struct Revision {
    version: u64,
    ordinal: u32,
    valid: Interval,
    payload: Option<u32>,
}
struct Slot {
    id: u64,
    pieces: Vec<Piece>,
}
impl Slot {
    fn at(&self, time: i64) -> Option<&Piece> {
        let end = self.pieces.partition_point(|p| p.from <= time);
        end.checked_sub(1)
            .and_then(|i| self.pieces.get(i))
            .filter(|p| p.contains(time))
    }
}
#[derive(Clone, Copy, Debug)]
struct Row {
    edge: u64,
    from: u32,
    to: u32,
    label: u32,
    payload: u32,
    start: i64,
    end: i64,
    has_end: bool,
}
impl Row {
    fn contains(self, time: i64) -> bool {
        self.start <= time && (!self.has_end || time < self.end)
    }
}

/// An immutable, owned read model for one store's fixed local V and all T.
///
/// Share with `Arc` when needed. It has no database handle, implicit refresh or
/// global cache. To serve a newer V, explicitly build and publish a new model;
/// dropping the last owner releases this model. V alone is not a cache key for
/// different stores, tenants, or restored store generations.
pub struct TemporalReadModel {
    version: u64,
    vertices: Vec<Slot>,
    edges: Vec<Slot>,
    vertex_index: HashMap<u64, u32>,
    edge_index: HashMap<u64, u32>,
    payloads: Vec<Cached>,
    labels: Vec<String>,
    rows: Vec<Row>,
    outgoing: Vec<Vec<u32>>,
    incoming: Vec<Vec<u32>>,
    stats: MaterializationStats,
}
/// A borrowed business-time selection on an owned fixed-V model.
pub struct TemporalReadModelView<'a> {
    graph: &'a TemporalReadModel,
    time: i64,
}
impl TemporalReadModel {
    /// The selected local version, never refreshed implicitly.
    pub fn version(&self) -> u64 {
        self.version
    }
    /// Construction counts and conservative accounting.
    pub fn stats(&self) -> &MaterializationStats {
        &self.stats
    }
    /// Select UTC microseconds T at this model's unchanged local V.
    pub fn view(&self, time: i64) -> TemporalReadModelView<'_> {
        TemporalReadModelView { graph: self, time }
    }
}
