//! Caller-owned cooperative controls and aggregate query-batch limits.
use crate::{Error, Result};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

/// Thread-safe, monotonic cancellation signal. Cancellation cannot be reset.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);
impl CancellationToken {
    /// Request cancellation of all executions sharing this token.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
/// A cooperative deadline and cancellation token; native waits are not preempted.
#[derive(Clone, Debug)]
pub struct ExecutionControl {
    token: CancellationToken,
    deadline: Instant,
}
impl ExecutionControl {
    /// Start a control budget now, including time spent before engine execution.
    pub fn new(token: CancellationToken, timeout: Duration) -> Result<Self> {
        Ok(Self {
            token,
            deadline: Instant::now()
                .checked_add(timeout)
                .ok_or(Error::ExecutionDeadlineExceeded)?,
        })
    }
    /// Fail when cancelled or expired. Call again at safe execution boundaries.
    pub fn check(&self) -> Result<()> {
        if self.token.is_cancelled() {
            return Err(Error::ExecutionCancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(Error::ExecutionDeadlineExceeded);
        }
        Ok(())
    }
}
/// Positive, caller-tightened limits shared by all queries in one fixed view.
#[derive(Clone, Copy, Debug)]
pub struct QueryBatchLimits {
    /// Maximum query count (64).
    pub max_queries: usize,
    /// Maximum cumulative result rows (10,000).
    pub max_rows: usize,
    /// Maximum cumulative backend work operations (65,536).
    pub max_work_operations: usize,
    /// Maximum cumulative backend logical work bytes (64 MiB).
    pub max_work_bytes: usize,
    /// Maximum live query and retained result accounting (64 MiB).
    pub max_live_bytes: usize,
}
impl Default for QueryBatchLimits {
    fn default() -> Self {
        Self {
            max_queries: 64,
            max_rows: crate::MAX_GREMLIN_ROWS,
            max_work_operations: 65_536,
            max_work_bytes: crate::MAX_GREMLIN_ACCOUNTED_BYTES,
            max_live_bytes: crate::MAX_GREMLIN_ACCOUNTED_BYTES,
        }
    }
}
impl QueryBatchLimits {
    pub(crate) fn validate(&self, queries: usize) -> Result<()> {
        let d = Self::default();
        if queries == 0
            || queries > self.max_queries
            || self.max_queries == 0
            || self.max_queries > d.max_queries
            || self.max_rows == 0
            || self.max_rows > d.max_rows
            || self.max_work_operations == 0
            || self.max_work_operations > d.max_work_operations
            || self.max_work_bytes == 0
            || self.max_work_bytes > d.max_work_bytes
            || self.max_live_bytes == 0
            || self.max_live_bytes > d.max_live_bytes
        {
            return Err(Error::TraversalLimitExceeded(
                "invalid query batch limits".into(),
            ));
        }
        Ok(())
    }
}
pub(crate) fn execute_batch(
    queries: &[crate::GremlinQuery],
    backend: &impl crate::gremlin::QueryReadBackend,
    check: &dyn Fn() -> Result<()>,
    limits: QueryBatchLimits,
    budget: &crate::query_memory::Budget,
) -> Result<Vec<crate::GremlinResult>> {
    use crate::{GremlinRow, PathElement, Value};
    use std::mem::size_of;
    check()?;
    limits.validate(queries.len())?;
    let _control = budget.reserve(queries.len() * size_of::<crate::GremlinResult>())?;
    let mut results = Vec::with_capacity(queries.len());
    let mut retained = Vec::with_capacity(queries.len());
    let mut rows = 0usize;
    for q in queries {
        check()?;
        let result = q.execute_on_read_budget(backend, check, budget)?;
        rows = rows
            .checked_add(result.rows.len())
            .ok_or_else(|| Error::TraversalLimitExceeded("batch row overflow".into()))?;
        if rows > limits.max_rows {
            return Err(Error::TraversalLimitExceeded(
                "batch result row limit".into(),
            ));
        }
        let mut bytes = crate::query_memory::mul(result.rows.capacity(), size_of::<GremlinRow>())?;
        for row in &result.rows {
            let extra = match row {
                GremlinRow::Value(Value::Text(s)) => s.capacity(),
                GremlinRow::Path(p) => {
                    crate::query_memory::mul(p.capacity(), size_of::<PathElement>())?
                }
                _ => 0,
            };
            bytes = crate::query_memory::add(bytes, extra)?;
        }
        retained.push(budget.reserve(bytes)?);
        results.push(result);
    }
    check()?;
    Ok(results)
}
