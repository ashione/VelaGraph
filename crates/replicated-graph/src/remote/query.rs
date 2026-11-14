//! Explicit single-partition server execution on one immutable shard snapshot.
//! Plaintext trusted-network experiment: no retry, failover or global guarantee.
use super::*;
use serde::{Deserialize, Serialize};
use velagraph::{GremlinRow, PathElement, Value};

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
    /// Selected vertex properties, each represented as a Gremlin singleton list.
    /// Only supported distributed terminal valueMap surfaces emit this.
    PropertyMap {
        source: QualifiedElement,
        properties: std::collections::BTreeMap<String, Vec<Value>>,
    },
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
    pub(super) fn validate(&self) -> Result<(), RemoteError> {
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
    super::single_query_grpc::query(endpoint, identity.ok_or(RemoteError::InvalidArgument)?, request, deadline).await
}

struct MapShape {
    keys: Vec<String>,
    // None after a graph transition; an empty ID list denotes a scan.
    start_ids: Option<Vec<u64>>,
}
fn admitted_extensions(request: &QueryRequest) -> (bool, Option<MapShape>) {
    use velagraph::{ObservedGremlinQuery, ObservedGremlinStep as Step};
    if request.mode != QueryMode::V2Observed {
        return (false, None);
    }
    let Ok(parsed) = ObservedGremlinQuery::parse(&request.text) else {
        return (false, None);
    };
    if crate::routing::experimental::validate_traversal(&parsed).is_err() {
        return (false, None);
    }
    let repeated = parsed
        .steps()
        .iter()
        .any(|step| matches!(step, Step::Repeat { .. }));
    let keys = match parsed.steps().last() {
        Some(Step::ValueMap(keys)) => Some(MapShape {
            keys: keys.clone(),
            start_ids: match parsed.start() {
                velagraph::GremlinStart::Vertices(ids)
                    if parsed.steps().iter().all(|step| {
                        matches!(
                            step,
                            Step::ValueMap(_)
                                | Step::Range { .. }
                                | Step::SimplePath
                                | Step::CyclicPath
                                | Step::Dedup
                                | Step::Existing(
                                    velagraph::GremlinStep::HasLabel(_)
                                        | velagraph::GremlinStep::Has { .. }
                                        | velagraph::GremlinStep::Limit(_)
                                )
                        )
                    }) =>
                {
                    Some(ids.clone())
                }
                _ => None,
            },
        }),
        _ => None,
    };
    (repeated, keys)
}

fn validate_output(
    output: &QueryOutput,
    expected: (u64, u32, QueryMode),
    identity: Option<RemoteIdentity>,
    repeated_path: bool,
    map_keys: Option<&MapShape>,
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
                let maximum = if output.mode == QueryMode::V2Observed && repeated_path {
                    // 32 expanded edges plus an optional edge-start endpoint.
                    2 * velagraph::MAX_OBSERVED_GREMLIN_REPEAT_TIMES + 2
                } else {
                    33
                };
                !path.is_empty() && path.len() <= maximum && path.iter().all(valid_element)
            }
            QueryRow::Count(_) => true,
            QueryRow::PropertyMap { source, properties } => {
                output.mode == QueryMode::V2Observed
                    && matches!(source, QualifiedElement::Vertex { .. })
                    && valid_element(source)
                    && map_keys.is_some_and(|shape| {
                        crate::selected_properties::valid_map(properties, &shape.keys)
                            && shape.start_ids.as_ref().is_none_or(|ids| ids.is_empty() || matches!(source, QualifiedElement::Vertex { id, .. } if ids.contains(id)))
                    })
            }
            QueryRow::Value { source, value } => {
                source.as_ref().is_none_or(valid_element)
                    && source.is_some() == (output.mode == QueryMode::V2Observed)
                    && !matches!(value, Value::Real(n) if !n.is_finite())
            }
        };
        if !valid || (map_keys.is_some() && !matches!(row, QueryRow::PropertyMap { .. })) {
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

fn query_barrier_failure(error: RemoteError) -> Result<Reply, RemoteError> {
    match error {
        RemoteError::Timeout { .. } => Ok(Reply::Query(Err(QueryFailure::Timeout))),
        other => Err(other),
    }
}

#[cfg(test)]
#[test]
fn query_barrier_timeout_preserves_query_failure_contract() {
    assert!(matches!(
        query_barrier_failure(RemoteError::Timeout { request_id: None }),
        Ok(Reply::Query(Err(QueryFailure::Timeout)))
    ));
    assert!(matches!(
        query_barrier_failure(RemoteError::NotLeader { leader: Some(2) }),
        Err(RemoteError::NotLeader { leader: Some(2) })
    ));
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
    if let Err(error) = super::read_barrier::confirm(&raft, end, "query_barrier").await {
        return query_barrier_failure(error);
    }
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
            let view = velagraph::ReadView::from_entries(
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
                    let graph = velagraph::GraphRecordStore::new(node, shard, request.partition);
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
    .map_err(|e| {
        let reason = if e.is_panic() { "panic" } else { "cancelled" };
        diagnostic::failure(format_args!("phase=query_worker reason={reason}"));
        RemoteError::Unavailable
    })
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
pub(crate) fn graph_error(e: velagraph::Error) -> QueryFailure {
    use velagraph::Error::*;
    match e {
        GremlinParse(_) => QueryFailure::Parse,
        UnsupportedGremlinStep(_) | UnsupportedGremlinArgument(_) | UnsupportedGremlinValue(_) => {
            QueryFailure::Unsupported
        }
        GremlinComplexityLimit { .. } | TraversalLimitExceeded(_) => {
            QueryFailure::ResourceExhausted
        }
        InvalidGremlinLimit(_) | InvalidGremlinTraverser(_) | InvalidTraversal(_) => {
            QueryFailure::InvalidArgument
        }
        StorageNode(e) => storage_error(e),
        _ => QueryFailure::IncompleteOrCorrupt,
    }
}
pub(crate) fn storage_error(e: velagraph::StorageNodeError) -> QueryFailure {
    use velagraph::StorageNodeError::*;
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
    pub(crate) view: velagraph::ReadView,
    pub(crate) end: std::time::Instant,
}
use velagraph::{
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

// Both legacy and typed clients validate exactly the same complete result shape.
pub(super) fn validate_rpc_response(
    request: &QueryRequest,
    response: &QueryResponse,
    identity: RemoteIdentity,
    end: Instant,
) -> Result<(), RemoteError> {
    let (repeated, keys) = admitted_extensions(request);
    if let Ok(output) = response {
        validate_output(
            output,
            (request.shard, request.partition, request.mode),
            Some(identity),
            repeated,
            keys.as_ref(),
            end,
        )?;
    }
    if Instant::now() >= end {
        return Err(RemoteError::Timeout { request_id: None });
    }
    Ok(())
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
                false,
                None,
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
    fn long_path_admission_is_specific_to_supported_repeat_requests() {
        for mode in [QueryMode::V1, QueryMode::V2Observed] {
            for (text, supported) in [
                ("g.V(1).out().path()", false),
                ("g.V(1).repeat(out()).times(32).path()", true),
                ("g.E(1).inV().repeat(in()).emit().times(32).path()", true),
                ("g.E(1).repeat(out()).times(32).path()", false),
                ("g.V(1).repeat(out()).times(32).out().path()", false),
                ("g.V(1).repeat(both()).times(1).path()", false),
            ] {
                let request = QueryRequest {
                    shard: 7,
                    partition: 10,
                    mode,
                    text: text.into(),
                    timeout_millis: 1000,
                };
                assert_eq!(
                    admitted_extensions(&request).0,
                    mode == QueryMode::V2Observed && supported,
                    "{text}"
                );
            }
        }
    }

    #[test]
    fn observed_long_paths_keep_scope_and_mode_specific_envelope() {
        let mut out = output();
        let end = Instant::now() + MAX_QUERY_DEADLINE;
        for (mode, repeated_path, maximum) in [
            (QueryMode::V1, false, 33),
            (QueryMode::V2Observed, false, 33),
            (QueryMode::V2Observed, true, 66),
        ] {
            out.mode = mode;
            out.completeness = if mode == QueryMode::V1 {
                QueryCompleteness::SinglePartition
            } else {
                QueryCompleteness::Unverified
            };
            for length in [maximum, maximum + 1] {
                out.rows = vec![QueryRow::Path(vec![
                    QualifiedElement::Vertex {
                        partition: 10,
                        id: 1
                    };
                    length
                ])];
                assert_eq!(
                    validate_output(&out, (7, 10, mode), None, repeated_path, None, end).is_ok(),
                    length == maximum
                );
            }
            out.rows = vec![QueryRow::Path(vec![
                QualifiedElement::Vertex {
                    partition: 11,
                    id: 1
                };
                maximum
            ])];
            assert!(matches!(
                validate_output(&out, (7, 10, mode), None, repeated_path, None, end),
                Err(RemoteError::Protocol)
            ));
        }
    }

    #[test]
    fn selected_map_replies_require_admitted_vertex_shape_and_bounded_values() {
        let request = |text: &str, mode| QueryRequest {
            shard: 7,
            partition: 10,
            mode,
            text: text.into(),
            timeout_millis: 1000,
        };
        let (_, shape) = admitted_extensions(&request(
            "g.V(1).valueMap('name','absent')",
            QueryMode::V2Observed,
        ));
        let validate = |out: &QueryOutput, shape: Option<&MapShape>| {
            validate_output(
                out,
                (7, 10, QueryMode::V2Observed),
                None,
                false,
                shape,
                Instant::now() + MAX_QUERY_DEADLINE,
            )
        };
        let source = QualifiedElement::Vertex {
            partition: 10,
            id: 1,
        };
        let mut out = output();
        out.rows = vec![QueryRow::PropertyMap {
            source,
            properties: BTreeMap::from([("name".into(), vec![Value::Null])]),
        }];
        assert!(validate(&out, shape.as_ref()).is_ok());
        assert!(validate(&out, None).is_err());
        // Range selects positions but cannot admit an unrelated source ID.
        let (_, ranged_shape) = admitted_extensions(&request(
            "g.V(1,1).range(1,2).valueMap('name','absent')",
            QueryMode::V2Observed,
        ));
        assert_eq!(ranged_shape.as_ref().unwrap().start_ids, Some(vec![1, 1]));
        assert!(validate(&out, ranged_shape.as_ref()).is_ok());
        let mut wrong_source = out.clone();
        let QueryRow::PropertyMap {
            source: bad_source, ..
        } = &mut wrong_source.rows[0]
        else {
            unreachable!()
        };
        *bad_source = QualifiedElement::Vertex {
            partition: 10,
            id: 2,
        };
        assert!(validate(&wrong_source, ranged_shape.as_ref()).is_err());
        for filter in ["simplePath", "cyclicPath"] {
            let (_, shape) = admitted_extensions(&request(
                &format!("g.V(1).{filter}().valueMap('name','absent')"),
                QueryMode::V2Observed,
            ));
            assert_eq!(shape.as_ref().unwrap().start_ids, Some(vec![1]));
            assert!(validate(&wrong_source, shape.as_ref()).is_err());
        }
        for (text, mode) in [
            ("g.V(1)", QueryMode::V2Observed),
            ("g.E(1).valueMap('name')", QueryMode::V2Observed),
            ("g.V(1).valueMap('name')", QueryMode::V1),
        ] {
            assert!(admitted_extensions(&request(text, mode)).1.is_none());
        }
        for change in 0..8 {
            let mut bad = out.clone();
            let QueryRow::PropertyMap { source, properties } = &mut bad.rows[0] else {
                unreachable!()
            };
            match change {
                0 => {
                    *source = QualifiedElement::Vertex {
                        partition: 11,
                        id: 1,
                    }
                }
                1 => {
                    *source = QualifiedElement::Edge {
                        partition: 10,
                        id: 1,
                    }
                }
                2 => {
                    *source = QualifiedElement::Vertex {
                        partition: 10,
                        id: 2,
                    }
                }
                3 => {
                    properties.insert("unselected".into(), vec![Value::Null]);
                }
                4 => {
                    properties.insert("name".into(), vec![]);
                }
                5 => {
                    properties.insert("name".into(), vec![Value::Null, Value::Null]);
                }
                6 => {
                    properties.insert("name".into(), vec![Value::Real(f64::NAN)]);
                }
                _ => {
                    properties.insert("name".into(), vec![Value::Text("x".repeat(2048))]);
                }
            }
            assert!(validate(&bad, shape.as_ref()).is_err(), "{change}");
        }
        out.rows = vec![QueryRow::Element(source)];
        assert!(validate(&out, shape.as_ref()).is_err());
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
                false,
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
        let (endpoint, seen, server) = raw_test_peer::spawn(Some(MAX_QUERY_RESPONSE_BYTES + 1)).await;
        let result = query(
            endpoint,
            Some(RemoteIdentity { group: 10, node: 1 }),
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
        tokio::time::timeout(Duration::from_secs(2), seen).await.unwrap().unwrap();
        server.abort();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_panic_keeps_unavailable_and_releases_its_permit() {
        static ADMISSION: Semaphore = Semaphore::const_new(1);
        let permit = ADMISSION.try_acquire().unwrap();
        let result = run_query_worker(permit, || panic!("synthetic worker failure")).await;
        assert!(matches!(result, Err(RemoteError::Unavailable)));
        assert_eq!(ADMISSION.available_permits(), 1);
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
