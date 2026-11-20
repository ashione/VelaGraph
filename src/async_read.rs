//! Bounded local reads for embedding a graph in a Tokio service.

use crate::{GraphEngine, GremlinResult, Result, StorageNodeError};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{timeout_at, Instant},
};

const EXECUTIONS: usize = 32;
const WAITERS: usize = 4;
const WAIT_BUDGET: Duration = Duration::from_millis(2);

/// A shareable read-only entry point for a local graph on a Tokio runtime.
///
/// Poll queries on a Tokio runtime with its time driver enabled. When building
/// the runtime manually, call [`tokio::runtime::Builder::enable_time`] or
/// [`tokio::runtime::Builder::enable_all`]. Reads that acquire a slot
/// immediately do not use the timer; queued reads use it for the 2 ms deadline.
///
/// Construct one reader per admission domain and clone it into request handlers.
/// Clones share 32 execution slots and four waiting slots. A fifth waiter or a
/// wait of 2 ms returns [`StorageNodeError::ResourceExhausted`] through
/// [`crate::Error::StorageNode`]. Each separately constructed reader has its
/// own limits. Direct calls on the engine remain outside this reader's budget.
///
/// The waiting budget ends when the reader reserves an execution slot. The
/// blocking pool can schedule the admitted work later. Cancelling the caller
/// releases a waiting slot immediately when its future drops. A submitted
/// worker retains its execution slot until the query returns or unwinds.
///
/// ```
/// use std::sync::Arc;
/// use velagraph::{AsyncGraphReader, GraphEngine, GremlinRow, Vertex};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let directory = tempfile::tempdir()?;
/// let graph = Arc::new(GraphEngine::open(directory.path())?);
/// graph.create_vertex(&Vertex::new(7))?;
/// let reader = AsyncGraphReader::new(graph);
/// let runtime = tokio::runtime::Builder::new_current_thread()
///     .enable_time()
///     .build()?;
/// runtime.block_on(async {
///     let client = reader.clone();
///     let result = client.gremlin("g.V(7).count()").await?;
///     assert_eq!(result.rows, [GremlinRow::Count(1)]);
///     Ok::<_, velagraph::Error>(())
/// })?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct AsyncGraphReader {
    graph: Arc<GraphEngine>,
    executions: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
}

impl AsyncGraphReader {
    /// Create a new admission domain for this engine.
    pub fn new(graph: Arc<GraphEngine>) -> Self {
        Self {
            graph,
            executions: Arc::new(Semaphore::new(EXECUTIONS)),
            waiters: Arc::new(Semaphore::new(WAITERS)),
        }
    }

    /// Run a standard local Gremlin query on the Tokio blocking pool.
    ///
    /// # Panics
    ///
    /// Can panic if polled outside a Tokio runtime. If a query enters the
    /// wait queue on a runtime without a time driver, [`tokio::time::timeout_at`]
    /// panics while setting the deadline.
    pub async fn gremlin(&self, text: &str) -> Result<GremlinResult> {
        crate::gremlin::validate_query_bytes(text.len())?;
        let permit = self.reserve().await?;
        let text = text.to_owned();
        self.run(permit, move |graph| graph.gremlin(&text)).await
    }

    async fn reserve(&self) -> Result<OwnedSemaphorePermit> {
        if let Ok(permit) = self.executions.clone().try_acquire_owned() {
            return Ok(permit);
        }
        let deadline = Instant::now() + WAIT_BUDGET;
        let exhausted = || StorageNodeError::ResourceExhausted("local read admission".into());
        let _waiting = self.waiters.try_acquire().map_err(|_| exhausted())?;
        let permit = timeout_at(deadline, self.executions.clone().acquire_owned())
            .await
            .map_err(|_| exhausted())?
            .map_err(|_| exhausted())?;
        // A delayed poll can observe both a ready permit and an expired timer.
        if Instant::now() >= deadline {
            return Err(exhausted().into());
        }
        Ok(permit)
    }

    async fn run(
        &self,
        permit: OwnedSemaphorePermit,
        work: impl FnOnce(&GraphEngine) -> Result<GremlinResult> + Send + 'static,
    ) -> Result<GremlinResult> {
        let graph = self.graph.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work(&graph)
        })
        .await
        .map_err(|_| StorageNodeError::Unavailable("local read worker failed".into()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, GremlinRow, Vertex};
    use std::{future::Future, pin::Pin, task::Poll};

    fn setup() -> (tempfile::TempDir, AsyncGraphReader) {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        (dir, AsyncGraphReader::new(Arc::new(graph)))
    }

    fn assert_exhausted<T>(result: Result<T>) {
        assert!(matches!(
            result,
            Err(Error::StorageNode(StorageNodeError::ResourceExhausted(_)))
        ));
    }

    // Register a waiter without yielding to the runtime's paused clock.
    async fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn burst_shares_bounds_and_recovers_after_cancellation_and_expiry() {
        let (_dir, reader) = setup();
        let other = reader.clone();
        let mut running = Vec::new();
        for _ in 0..32 {
            running.push(reader.reserve().await.unwrap());
        }

        let mut waiting = Vec::new();
        for _ in 0..4 {
            let mut query = Box::pin(other.reserve());
            poll_pending(query.as_mut()).await;
            waiting.push(query);
        }
        assert_exhausted(reader.gremlin("g.V(1)").await);
        let oversized = " ".repeat(crate::MAX_GREMLIN_QUERY_BYTES + 1);
        assert!(matches!(
            other.gremlin(&oversized).await,
            Err(Error::GremlinParse(_))
        ));
        // Writes still use the underlying engine while this reader is full.
        reader.graph.create_vertex(&Vertex::new(1)).unwrap();

        drop(waiting.remove(0));
        let mut replacement = Box::pin(reader.reserve());
        poll_pending(replacement.as_mut()).await;
        tokio::time::advance(Duration::from_millis(1)).await;
        drop(running.pop());
        running.push(waiting.remove(0).await.unwrap());
        // A permit that becomes ready at expiry must be relinquished.
        tokio::time::advance(Duration::from_millis(1)).await;
        drop(running.pop());
        for waiter in waiting {
            assert_exhausted(waiter.await);
        }
        assert_exhausted(replacement.await);
        // A waiter with no release through its deadline also fails.
        running.push(other.reserve().await.unwrap());
        let mut timeout = Box::pin(other.gremlin("g.V(1)"));
        poll_pending(timeout.as_mut()).await;
        tokio::time::advance(Duration::from_millis(3)).await;
        assert_exhausted(timeout.await);
        drop(running);
        assert_eq!(reader.waiters.available_permits(), 4);
        assert_eq!(other.executions.available_permits(), 32);
        assert_eq!(
            other.gremlin("g.V(1).count()").await.unwrap().rows,
            [GremlinRow::Count(1)]
        );
    }

    #[tokio::test]
    async fn cancelled_caller_cannot_release_a_running_workers_slot() {
        let (_dir, reader) = setup();
        let client = reader.clone();
        let (started, entered) = tokio::sync::oneshot::channel();
        let (finish, wait) = std::sync::mpsc::channel();
        let call = tokio::spawn(async move {
            client
                .run(client.reserve().await?, move |graph| {
                    started.send(()).unwrap();
                    wait.recv().unwrap();
                    graph.gremlin("g.V(1).count()")
                })
                .await
        });
        entered.await.unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert_eq!(reader.executions.available_permits(), 31);
        finish.send(()).unwrap();
        let all = tokio::time::timeout(Duration::from_secs(5), reader.executions.acquire_many(32))
            .await
            .unwrap()
            .unwrap();
        drop(all);
        assert!(reader.gremlin("invalid").await.is_err());
        assert_eq!(reader.executions.available_permits(), 32);
    }
}
