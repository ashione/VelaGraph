//! Explicit single-partition server execution on one immutable shard snapshot.
//! Plaintext trusted-network experiment: no retry, failover or global guarantee.
use super::*;
use rocksdb_sql_table::{GremlinRow, PathElement, Value};
use serde::{Deserialize, Serialize};

pub const MAX_QUERY_TEXT_BYTES: usize = 16 * 1024;
pub const MAX_QUERY_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_QUERY_ROWS: usize = 10_000;
pub const MAX_QUERY_IN_FLIGHT: usize = 2;
pub const MAX_QUERY_DEADLINE: Duration = Duration::from_secs(5);
static QUERY_ADMISSION: Semaphore = Semaphore::const_new(MAX_QUERY_IN_FLIGHT);

/// V2 observations never certify adjacency completeness, even on one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryMode {
    V1,
    V2Observed,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub shard: u64,
    pub partition: u32,
    pub mode: QueryMode,
    pub text: String,
    /// Server cooperative budget, capped again by its connection deadline.
    pub timeout_millis: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryCompleteness {
    SinglePartition,
    Unverified,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QualifiedElement {
    Vertex { partition: u32, id: u64 },
    Edge { partition: u32, id: u64 },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryRow {
    Element(QualifiedElement),
    Path(Vec<QualifiedElement>),
    Count(u64),
    /// V1's existing scalar result has no provenance. V2 always supplies it.
    Value {
        source: Option<QualifiedElement>,
        value: Value,
    },
}
/// Actual snapshot sampled after the contacted node's leader read barrier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryStamp {
    pub identity: RemoteIdentity,
    pub shard: u64,
    pub epoch: u64,
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryOutput {
    pub partition: u32,
    pub mode: QueryMode,
    pub completeness: QueryCompleteness,
    pub snapshot: QueryStamp,
    pub rows: Vec<QueryRow>,
}
/// Typed failures carry no user data and never carry partial results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum QueryFailure {
    #[error("invalid Gremlin syntax")]
    Parse,
    #[error("unsupported Gremlin feature")]
    Unsupported,
    #[error("invalid query argument or traverser")]
    InvalidArgument,
    #[error("query resource limit exceeded")]
    ResourceExhausted,
    #[error("query deadline exceeded")]
    Timeout,
    #[error("query references a partition outside the explicit single-partition scope")]
    UnsupportedScope,
    #[error("corrupt or incomplete graph observation")]
    IncompleteOrCorrupt,
    #[error("query execution unavailable")]
    Unavailable,
}
/// Transport failures are separate from typed query failures.
pub type QueryResponse = Result<QueryOutput, QueryFailure>;

impl QueryRequest {
    fn validate(&self) -> Result<(), RemoteError> {
        if self.text.len() > MAX_QUERY_TEXT_BYTES {
            return Err(RemoteError::ResourceExhausted);
        }
        if self.timeout_millis == 0 || self.timeout_millis > MAX_QUERY_DEADLINE.as_millis() as u64 {
            return Err(RemoteError::InvalidArgument);
        }
        Ok(())
    }
}

/// Execute exactly once at the explicitly selected replica; never follow hints.
/// Only this partition is in scope; v2 cross-partition references fail closed.
pub async fn query(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    request: QueryRequest,
    deadline: Duration,
) -> Result<QueryResponse, RemoteError> {
    let end = checked_deadline(deadline, None)?;
    request.validate()?;
    let expected = (request.shard, request.partition, request.mode);
    match rpc(
        endpoint,
        identity,
        Operation::Query(request),
        end.saturating_duration_since(Instant::now()),
        None,
    )
    .await?
    {
        Reply::Query(response) => {
            if let Ok(output) = &response {
                validate_output(output, expected, identity, end)?;
            }
            if Instant::now() >= end {
                return Err(RemoteError::Timeout { request_id: None });
            }
            Ok(response)
        }
        _ => Err(RemoteError::Protocol),
    }
}

fn validate_output(
    output: &QueryOutput,
    expected: (u64, u32, QueryMode),
    identity: Option<RemoteIdentity>,
    end: Instant,
) -> Result<(), RemoteError> {
    if (output.snapshot.shard, output.partition, output.mode) != expected
        || output.snapshot.epoch == 0
        || identity.is_some_and(|id| id != output.snapshot.identity)
        || output.rows.len() > MAX_QUERY_ROWS
        || output.completeness
            != match output.mode {
                QueryMode::V1 => QueryCompleteness::SinglePartition,
                QueryMode::V2Observed => QueryCompleteness::Unverified,
            }
    {
        return Err(RemoteError::Protocol);
    }
    let valid_element = |e: &QualifiedElement| match e {
        QualifiedElement::Vertex { partition, .. } | QualifiedElement::Edge { partition, .. } => {
            *partition == output.partition
        }
    };
    for row in &output.rows {
        if Instant::now() >= end {
            return Err(RemoteError::Timeout { request_id: None });
        }
        let valid = match row {
            QueryRow::Element(e) => valid_element(e),
            QueryRow::Path(path) => {
                !path.is_empty() && path.len() <= 33 && path.iter().all(valid_element)
            }
            QueryRow::Count(_) => true,
            QueryRow::Value { source, value } => {
                source.as_ref().is_none_or(valid_element)
                    && source.is_some() == (output.mode == QueryMode::V2Observed)
                    && !matches!(value, Value::Real(n) if !n.is_finite())
            }
        };
        if !valid {
            return Err(RemoteError::Protocol);
        }
    }
    let mut writer = QueryWriter(Vec::new(), end);
    serde_json::to_writer(&mut writer, output).map_err(|_| {
        if Instant::now() >= end {
            RemoteError::Timeout { request_id: None }
        } else {
            RemoteError::ResourceExhausted
        }
    })?;
    Ok(())
}

pub(super) async fn execute_query(
    raft: openraft::Raft<TypeConfig>,
    state: Arc<StateMachine>,
    identity: RemoteIdentity,
    shard: u64,
    request: QueryRequest,
    end: Instant,
) -> Result<Reply, RemoteError> {
    request.validate()?;
    if request.shard != shard {
        return Err(RemoteError::InvalidArgument);
    }
    let end = end.min(Instant::now() + Duration::from_millis(request.timeout_millis));
    tokio::time::timeout_at(
        end,
        execute_query_inner(raft, state, identity, shard, request, end),
    )
    .await
    .unwrap_or(Ok(Reply::Query(Err(QueryFailure::Timeout))))
}

async fn execute_query_inner(
    raft: openraft::Raft<TypeConfig>,
    state: Arc<StateMachine>,
    identity: RemoteIdentity,
    shard: u64,
    request: QueryRequest,
    end: Instant,
) -> Result<Reply, RemoteError> {
    // Held through detached blocking work too: cancellation never frees its slot early.
    let permit = QUERY_ADMISSION
        .try_acquire()
        .map_err(|_| RemoteError::ResourceExhausted)?;
    raft.ensure_linearizable().await.map_err(|e| {
        if let Some(hint) = e.forward_to_leader::<openraft::BasicNode>() {
            RemoteError::NotLeader {
                leader: hint.leader_id,
            }
        } else {
            RemoteError::Unavailable
        }
    })?;
    if Instant::now() >= end {
        return Ok(Reply::Query(Err(QueryFailure::Timeout)));
    }
    let snapshot = state
        .d1_read_snapshot(MAX_SESSION_ROWS, MAX_SESSION_BYTES)
        .await
        .map_err(|_| RemoteError::ResourceExhausted)?
        .ok_or(RemoteError::Application(D1Error::NotActivated))?;
    if snapshot.shard != shard || snapshot.epoch == 0 {
        return Err(RemoteError::Protocol);
    }
    let stamp = QueryStamp {
        identity,
        shard,
        epoch: snapshot.epoch,
        sequence: snapshot.sequence,
    };
    let result = run_query_worker(permit, move || {
        let run = || -> QueryResponse {
            check(end)?;
            let view = rocksdb_sql_table::ReadView::from_entries(
                snapshot.shard,
                snapshot.epoch,
                snapshot.sequence,
                snapshot.entries,
            )
            .map_err(storage_error)?;
            let node = FrozenNode {
                view,
                end: end.into_std(),
            };
            let rows = match request.mode {
                QueryMode::V1 => {
                    let graph =
                        rocksdb_sql_table::GraphRecordStore::new(node, shard, request.partition);
                    let result = graph.execute_gremlin(&request.text).map_err(graph_error)?;
                    result
                        .rows
                        .into_iter()
                        .map(|row| v1_row(request.partition, row))
                        .collect()
                }
                QueryMode::V2Observed => crate::routing::experimental::execute_frozen_query(
                    node,
                    identity,
                    request.partition,
                    &request.text,
                )?,
            };
            check(end)?;
            let output = QueryOutput {
                partition: request.partition,
                mode: request.mode,
                completeness: match request.mode {
                    QueryMode::V1 => QueryCompleteness::SinglePartition,
                    QueryMode::V2Observed => QueryCompleteness::Unverified,
                },
                snapshot: stamp,
                rows,
            };
            let mut writer = QueryWriter(Vec::new(), end);
            serde_json::to_writer(&mut writer, &output).map_err(|_| {
                if Instant::now() >= end {
                    QueryFailure::Timeout
                } else {
                    QueryFailure::ResourceExhausted
                }
            })?;
            check(end)?;
            Ok(output)
        };
        let result = run();
        if Instant::now() >= end {
            Err(QueryFailure::Timeout)
        } else {
            result
        }
    })
    .await?;
    Ok(Reply::Query(result))
}
async fn run_query_worker(
    permit: tokio::sync::SemaphorePermit<'static>,
    work: impl FnOnce() -> QueryResponse + Send + 'static,
) -> Result<QueryResponse, RemoteError> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|_| RemoteError::Unavailable)
}
fn check(end: Instant) -> Result<(), QueryFailure> {
    if Instant::now() >= end {
        Err(QueryFailure::Timeout)
    } else {
        Ok(())
    }
}
struct QueryWriter(Vec<u8>, Instant);
impl io::Write for QueryWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if Instant::now() >= self.1
            || bytes.len() > MAX_QUERY_RESPONSE_BYTES.saturating_sub(self.0.len())
        {
            return Err(io::Error::other("query response limit"));
        }
        let required = self.0.len() + bytes.len();
        if required > self.0.capacity() {
            let capacity = required
                .max(self.0.capacity().saturating_mul(2))
                .min(MAX_QUERY_RESPONSE_BYTES);
            self.0.reserve_exact(capacity - self.0.len());
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn graph_error(e: rocksdb_sql_table::Error) -> QueryFailure {
    use rocksdb_sql_table::Error::*;
    match e {
        GremlinParse(_) => QueryFailure::Parse,
        UnsupportedGremlinStep(_)
        | UnsupportedGremlinArgument(_)
        | UnsupportedGremlinValue(_)
        | Unsupported(_) => QueryFailure::Unsupported,
        GremlinComplexityLimit { .. } | TraversalLimitExceeded(_) => {
            QueryFailure::ResourceExhausted
        }
        InvalidGremlinLimit(_)
        | InvalidGremlinTraverser(_)
        | InvalidTraversal(_)
        | InvalidValue(_)
        | InvalidLiteral(_) => QueryFailure::InvalidArgument,
        StorageNode(e) => storage_error(e),
        _ => QueryFailure::IncompleteOrCorrupt,
    }
}
pub(crate) fn storage_error(e: rocksdb_sql_table::StorageNodeError) -> QueryFailure {
    use rocksdb_sql_table::StorageNodeError::*;
    match e {
        Timeout { .. } => QueryFailure::Timeout,
        ResourceExhausted(_) => QueryFailure::ResourceExhausted,
        InvalidArgument(_) => QueryFailure::InvalidArgument,
        Corrupt(_) => QueryFailure::IncompleteOrCorrupt,
        _ => QueryFailure::Unavailable,
    }
}
fn v1_row(partition: u32, row: GremlinRow) -> QueryRow {
    match row {
        GremlinRow::Vertex(id) => QueryRow::Element(QualifiedElement::Vertex { partition, id }),
        GremlinRow::Edge(id) => QueryRow::Element(QualifiedElement::Edge { partition, id }),
        GremlinRow::Path(path) => QueryRow::Path(
            path.into_iter()
                .map(|e| match e {
                    PathElement::Vertex(id) => QualifiedElement::Vertex { partition, id },
                    PathElement::Edge(id) => QualifiedElement::Edge { partition, id },
                })
                .collect(),
        ),
        GremlinRow::Count(n) => QueryRow::Count(n),
        GremlinRow::Value(value) => QueryRow::Value {
            source: None,
            value,
        },
    }
}

/// Internal read-only adapter: no live metadata reads or snapshot replacement.
#[derive(Clone)]
pub(crate) struct FrozenNode {
    pub(crate) view: rocksdb_sql_table::ReadView,
    pub(crate) end: std::time::Instant,
}
use rocksdb_sql_table::{
    Export, KeyRange, KeyValue, ReadView, ScanCursor, ScanLimit, ScanPage, StorageAdmin,
    StorageNode, StorageNodeError, WriteRequest, WriteResult,
};
impl FrozenNode {
    pub(crate) fn check_view(&self, view: &ReadView) -> crate::Result<()> {
        self.check_execution_context()?;
        if (view.shard, view.epoch, view.sequence)
            != (self.view.shard, self.view.epoch, self.view.sequence)
        {
            return Err(StorageNodeError::InvalidArgument(
                "frozen snapshot mismatch".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn probe_leader(&self) -> crate::Result<()> {
        self.check_execution_context()
    }
}
impl StorageNode for FrozenNode {
    fn check_execution_context(&self) -> crate::Result<()> {
        if std::time::Instant::now() >= self.end {
            Err(StorageNodeError::Timeout { request_id: None })
        } else {
            Ok(())
        }
    }
    fn read_view(&self) -> crate::Result<ReadView> {
        self.check_execution_context()?;
        Ok(self.view.clone())
    }
    fn get(&self, key: &[u8], view: &ReadView) -> crate::Result<Option<Vec<u8>>> {
        self.check_view(view)?;
        Ok(self.view.get(key))
    }
    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
    ) -> crate::Result<Vec<KeyValue>> {
        self.check_view(view)?;
        self.view.scan(range, limit)
    }
    fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
        cursor: Option<&ScanCursor>,
    ) -> crate::Result<ScanPage> {
        self.check_view(view)?;
        self.view.scan_page(range, limit, cursor)
    }
    fn batch_write(&self, _: WriteRequest) -> crate::Result<WriteResult> {
        Err(StorageNodeError::Unsupported(
            "frozen query is read only".into(),
        ))
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self
    }
}
impl StorageAdmin for FrozenNode {
    fn export(&self) -> crate::Result<Export> {
        Err(StorageNodeError::Unsupported(
            "frozen query is read only".into(),
        ))
    }
    fn import(&self, _: Export) -> crate::Result<()> {
        Err(StorageNodeError::Unsupported(
            "frozen query is read only".into(),
        ))
    }
    fn set_epoch(&self, _: u64) -> crate::Result<()> {
        Err(StorageNodeError::Unsupported(
            "frozen query is read only".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    fn output() -> QueryOutput {
        QueryOutput {
            partition: 10,
            mode: QueryMode::V2Observed,
            completeness: QueryCompleteness::Unverified,
            snapshot: QueryStamp {
                identity: RemoteIdentity { group: 2, node: 3 },
                shard: 7,
                epoch: 1,
                sequence: 8,
            },
            rows: vec![QueryRow::Value {
                source: Some(QualifiedElement::Vertex {
                    partition: 10,
                    id: 1,
                }),
                value: Value::Text("value".into()),
            }],
        }
    }
    #[test]
    fn rejects_wrong_scope_stamp_mode_and_missing_provenance() {
        let good = output();
        let end = Instant::now() + MAX_QUERY_DEADLINE;
        let validate = |out: &QueryOutput| {
            validate_output(
                out,
                (7, 10, QueryMode::V2Observed),
                Some(good.snapshot.identity),
                end,
            )
        };
        validate(&good).unwrap();
        for change in 0..9 {
            let mut bad = good.clone();
            match change {
                0 => bad.snapshot.epoch = 0,
                1 => bad.snapshot.shard = 8,
                2 => bad.snapshot.identity.node = 4,
                3 => bad.partition = 11,
                4 => bad.mode = QueryMode::V1,
                5 => bad.completeness = QueryCompleteness::SinglePartition,
                6 => {
                    bad.rows = vec![QueryRow::Value {
                        source: None,
                        value: Value::Null,
                    }]
                }
                7 => {
                    bad.rows = vec![QueryRow::Element(QualifiedElement::Vertex {
                        partition: 11,
                        id: 1,
                    })]
                }
                _ => bad.rows = vec![QueryRow::Path(vec![])],
            }
            assert!(
                matches!(validate(&bad), Err(RemoteError::Protocol)),
                "{change}"
            );
        }
    }
    #[test]
    fn response_encoding_and_deadline_are_bounded() {
        use std::io::Write;
        let mut writer = QueryWriter(Vec::new(), Instant::now() + MAX_QUERY_DEADLINE);
        writer
            .write_all(&vec![0; MAX_QUERY_RESPONSE_BYTES])
            .unwrap();
        assert!(writer.write_all(&[0]).is_err());
        assert_eq!(writer.0.len(), MAX_QUERY_RESPONSE_BYTES);
        let mut expired = QueryWriter(Vec::new(), Instant::now());
        assert!(expired.write_all(b"a").is_err());
        let mut out = output();
        out.rows = vec![QueryRow::Value {
            source: Some(QualifiedElement::Vertex {
                partition: 10,
                id: 1,
            }),
            value: Value::Text("\u{0000}".repeat(MAX_QUERY_RESPONSE_BYTES / 6)),
        }];
        assert!(matches!(
            validate_output(
                &out,
                (7, 10, QueryMode::V2Observed),
                None,
                Instant::now() + MAX_QUERY_DEADLINE
            ),
            Err(RemoteError::ResourceExhausted)
        ));
    }
    #[test]
    fn frozen_reads_never_replace_the_snapshot_and_reject_writes() {
        let view = ReadView::from_entries(
            7,
            1,
            9,
            BTreeMap::from([(b"key".to_vec(), b"original".to_vec())]),
        )
        .unwrap();
        let node = FrozenNode {
            view: view.clone(),
            end: std::time::Instant::now() + MAX_QUERY_DEADLINE,
        };
        let later = ReadView::from_entries(
            7,
            1,
            10,
            BTreeMap::from([(b"key".to_vec(), b"new".to_vec())]),
        )
        .unwrap();
        assert_eq!(node.get(b"key", &view).unwrap(), Some(b"original".to_vec()));
        assert_eq!(node.read_view().unwrap().sequence, 9);
        assert!(node.get(b"key", &later).is_err());
        assert!(node.admin().set_epoch(2).is_err());
        let expired = FrozenNode {
            view,
            end: std::time::Instant::now(),
        };
        assert!(matches!(
            expired.read_view(),
            Err(StorageNodeError::Timeout { .. })
        ));
    }
    #[tokio::test]
    async fn query_response_length_is_rejected_before_body_allocation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _: Request = read_frame(&mut socket, Instant::now() + MAX_QUERY_DEADLINE)
                .await
                .unwrap();
            socket
                .write_u32((MAX_QUERY_RESPONSE_BYTES + 1025) as u32)
                .await
                .unwrap();
        });
        let result = query(
            endpoint,
            None,
            QueryRequest {
                shard: 7,
                partition: 10,
                mode: QueryMode::V1,
                text: "g.V().count()".into(),
                timeout_millis: 1000,
            },
            MAX_QUERY_DEADLINE,
        )
        .await;
        assert!(matches!(result, Err(RemoteError::ResourceExhausted)));
        server.await.unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_waiters_keeps_worker_admission_until_work_stops() {
        let mut stops = Vec::new();
        let mut waiters = Vec::new();
        for _ in 0..MAX_QUERY_IN_FLIGHT {
            let permit = QUERY_ADMISSION.try_acquire().unwrap();
            let (ready, started) = tokio::sync::oneshot::channel();
            let (stop, stopped) = std::sync::mpsc::channel();
            stops.push(stop);
            waiters.push(tokio::spawn(run_query_worker(permit, move || {
                let _ = ready.send(());
                let _ = stopped.recv();
                Err(QueryFailure::Timeout)
            })));
            started.await.unwrap();
        }
        for waiter in waiters {
            waiter.abort();
            let _ = waiter.await;
        }
        assert!(QUERY_ADMISSION.try_acquire().is_err());
        for stop in stops {
            stop.send(()).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while QUERY_ADMISSION.available_permits() != MAX_QUERY_IN_FLIGHT {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
