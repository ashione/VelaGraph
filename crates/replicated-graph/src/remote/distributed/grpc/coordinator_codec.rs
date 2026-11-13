//! Lossless observed coordinator DTOs. No JSON payloads or alternate executor.
use super::super::jobs;
use super::*;

fn required<T>(v: Option<T>) -> Result<T, RemoteError> {
    v.ok_or(RemoteError::Protocol)
}
fn size(v: u64) -> Result<usize, RemoteError> {
    v.try_into().map_err(|_| RemoteError::Protocol)
}
fn token(v: Vec<u8>) -> Result<[u8; 16], RemoteError> {
    v.try_into().map_err(|_| RemoteError::Protocol)
}
fn observed(v: i32) -> Result<QueryCompleteness, RemoteError> {
    if v == wire::Completeness::Unverified as i32 {
        Ok(QueryCompleteness::Unverified)
    } else {
        Err(RemoteError::Protocol)
    }
}
fn completeness(v: QueryCompleteness) -> i32 {
    match v {
        QueryCompleteness::Unverified => wire::Completeness::Unverified as i32,
        QueryCompleteness::SinglePartition => wire::Completeness::SinglePartition as i32,
    }
}
fn timeout(version: u32, millis: u64) -> Result<Duration, RemoteError> {
    if version != 1 {
        return Err(RemoteError::Protocol);
    }
    if !(1..=MAX_TIMEOUT).contains(&millis) {
        return Err(RemoteError::InvalidArgument);
    }
    Ok(Duration::from_millis(millis))
}
fn failure(v: i32) -> Result<QueryFailure, RemoteError> {
    wire::QueryFailure::try_from(v)
        .map_err(|_| RemoteError::Protocol)?
        .try_into()
}
impl From<QualifiedElement> for wire::QualifiedElement {
    fn from(v: QualifiedElement) -> Self {
        use wire::qualified_element::Kind;
        Self {
            kind: Some(match v {
                QualifiedElement::Vertex { partition, id } => {
                    Kind::Vertex(wire::VertexId { partition, id })
                }
                QualifiedElement::Edge { partition, id } => {
                    Kind::Edge(wire::VertexId { partition, id })
                }
            }),
        }
    }
}
impl TryFrom<wire::QualifiedElement> for QualifiedElement {
    type Error = RemoteError;
    fn try_from(v: wire::QualifiedElement) -> Result<Self, Self::Error> {
        use wire::qualified_element::Kind;
        Ok(match required(v.kind)? {
            Kind::Vertex(v) => Self::Vertex {
                partition: v.partition,
                id: v.id,
            },
            Kind::Edge(v) => Self::Edge {
                partition: v.partition,
                id: v.id,
            },
        })
    }
}
fn encode_map(
    source: QualifiedElement,
    properties: BTreeMap<String, Vec<Value>>,
) -> wire::PropertyMap {
    wire::PropertyMap {
        source: Some(source.into()),
        properties: properties
            .into_iter()
            .map(|(key, values)| wire::PropertyValues {
                key,
                values: values.into_iter().map(Into::into).collect(),
            })
            .collect(),
    }
}
fn decode_map(
    v: wire::PropertyMap,
) -> Result<(QualifiedElement, BTreeMap<String, Vec<Value>>), RemoteError> {
    let source = required(v.source)?.try_into()?;
    let mut properties = BTreeMap::new();
    for p in v.properties {
        let values = p
            .values
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        if properties.insert(p.key, values).is_some() {
            return Err(RemoteError::Protocol);
        }
    }
    Ok((source, properties))
}
impl From<QueryRow> for wire::QueryRow {
    fn from(v: QueryRow) -> Self {
        use wire::query_row::Kind;
        Self {
            kind: Some(match v {
                QueryRow::Element(v) => Kind::Element(v.into()),
                QueryRow::Path(v) => Kind::Path(wire::QueryPath {
                    elements: v.into_iter().map(Into::into).collect(),
                }),
                QueryRow::Count(v) => Kind::Count(v),
                QueryRow::PropertyMap { source, properties } => {
                    Kind::PropertyMap(encode_map(source, properties))
                }
                QueryRow::Value { source, value } => Kind::Value(wire::ScalarValue {
                    source: source.map(Into::into),
                    value: Some(value.into()),
                }),
            }),
        }
    }
}
impl TryFrom<wire::QueryRow> for QueryRow {
    type Error = RemoteError;
    fn try_from(v: wire::QueryRow) -> Result<Self, Self::Error> {
        use wire::query_row::Kind;
        Ok(match required(v.kind)? {
            Kind::Element(v) => Self::Element(v.try_into()?),
            Kind::Path(v) => Self::Path(
                v.elements
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            ),
            Kind::Count(v) => Self::Count(v),
            Kind::PropertyMap(v) => {
                let (source, properties) = decode_map(v)?;
                Self::PropertyMap { source, properties }
            }
            Kind::Value(v) => Self::Value {
                source: v.source.map(TryInto::try_into).transpose()?,
                value: required(v.value)?.try_into()?,
            },
        })
    }
}
pub(super) fn encode_query_request(v: DistributedRequest) -> wire::CoordinatorQueryRequest {
    wire::CoordinatorQueryRequest {
        version: 1,
        timeout_millis: v.timeout_millis,
        start_partition: v.start_partition,
        text: v.text,
    }
}
pub(super) fn decode_query_request(
    v: wire::CoordinatorQueryRequest,
) -> Result<DistributedRequest, RemoteError> {
    timeout(v.version, v.timeout_millis)?;
    let r = DistributedRequest {
        start_partition: v.start_partition,
        text: v.text,
        timeout_millis: v.timeout_millis,
    };
    validate_request(&r)?;
    Ok(r)
}
pub(super) fn encode_query_response(v: DistributedResponse) -> wire::CoordinatorQueryResponse {
    use wire::coordinator_query_response::Result;
    wire::CoordinatorQueryResponse {
        result: Some(match v {
            Ok(v) => Result::Output(wire::CoordinatorQueryOutput {
                completeness: completeness(v.completeness),
                snapshots: v.snapshots.into_iter().map(Into::into).collect(),
                rows: v.rows.into_iter().map(Into::into).collect(),
            }),
            Err(e) => Result::Failure(wire::QueryFailure::from(e) as i32),
        }),
    }
}
pub(super) fn decode_query_response(
    v: wire::CoordinatorQueryResponse,
) -> Result<DistributedResponse, RemoteError> {
    use wire::coordinator_query_response::Result as R;
    Ok(match required(v.result)? {
        R::Output(v) => Ok(DistributedOutput {
            completeness: observed(v.completeness)?,
            snapshots: v
                .snapshots
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            rows: v
                .rows
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        }),
        R::Failure(e) => Err(failure(e)?),
    })
}
pub(super) fn encode_job_request(
    v: jobs::Request,
    duration: Duration,
) -> wire::CoordinatorJobRequest {
    use wire::coordinator_job_request::Request as R;
    wire::CoordinatorJobRequest {
        version: 1,
        timeout_millis: duration.as_millis().min(u64::MAX as u128) as u64,
        request: Some(match v {
            jobs::Request::Start {
                start_partition,
                text,
            } => R::Start(wire::JobStart {
                start_partition,
                text,
            }),
            jobs::Request::StartRows {
                start_partition,
                text,
                page_size,
            } => R::StartRows(wire::JobStartRows {
                start_partition,
                text,
                page_size: page_size as u64,
            }),
            jobs::Request::Read { token, cursor } => R::Read(wire::JobRead {
                token: token.to_vec(),
                cursor: cursor as u64,
            }),
            jobs::Request::Advance { token, sequence } => R::Advance(wire::JobAdvance {
                token: token.to_vec(),
                sequence,
            }),
            jobs::Request::Cancel { token } => R::Cancel(wire::JobCancel {
                token: token.to_vec(),
            }),
        }),
    }
}
pub(super) fn decode_job_request(
    v: wire::CoordinatorJobRequest,
) -> Result<(jobs::Request, Duration), RemoteError> {
    use wire::coordinator_job_request::Request as R;
    let duration = timeout(v.version, v.timeout_millis)?;
    let request = match required(v.request)? {
        R::Start(v) => jobs::Request::Start {
            start_partition: v.start_partition,
            text: v.text,
        },
        R::StartRows(v) => jobs::Request::StartRows {
            start_partition: v.start_partition,
            text: v.text,
            page_size: size(v.page_size)?,
        },
        R::Read(v) => jobs::Request::Read {
            token: token(v.token)?,
            cursor: size(v.cursor)?,
        },
        R::Advance(v) => jobs::Request::Advance {
            token: token(v.token)?,
            sequence: v.sequence,
        },
        R::Cancel(v) => jobs::Request::Cancel {
            token: token(v.token)?,
        },
    };
    if matches!(&request,jobs::Request::Start{text,..}|jobs::Request::StartRows{text,..} if text.len()>query::MAX_QUERY_TEXT_BYTES)
    {
        return Err(RemoteError::ResourceExhausted);
    }
    Ok((request, duration))
}
pub(super) fn encode_job_response(v: jobs::Response) -> wire::CoordinatorJobResponse {
    use wire::coordinator_job_output::Kind;
    use wire::coordinator_job_response::Result as R;
    let result = match v {
        Err(e) => R::Failure(wire::QueryFailure::from(e) as i32),
        Ok(v) => R::Output(wire::CoordinatorJobOutput {
            kind: Some(match v {
                jobs::Output::ProjectedPage {
                    token,
                    cursor,
                    next_cursor,
                    done,
                    rows,
                    completeness: c,
                    snapshots,
                } => Kind::ProjectedPage(wire::JobProjectedPage {
                    token: token.to_vec(),
                    cursor: cursor as u64,
                    next_cursor: next_cursor.map(|v| v as u64),
                    done,
                    rows: rows
                        .into_iter()
                        .map(|v| encode_map(v.source, v.properties))
                        .collect(),
                    completeness: completeness(c),
                    snapshots: snapshots.into_iter().map(Into::into).collect(),
                }),
                jobs::Output::Preparing {
                    token,
                    next_sequence,
                    ready,
                    completeness: c,
                    snapshots,
                } => Kind::Preparing(wire::JobPreparing {
                    token: token.to_vec(),
                    next_sequence,
                    ready,
                    completeness: completeness(c),
                    snapshots: snapshots.into_iter().map(Into::into).collect(),
                }),
                jobs::Output::Page {
                    token,
                    cursor,
                    next_cursor,
                    done,
                    rows,
                    completeness: c,
                    snapshots,
                } => Kind::Page(wire::JobPage {
                    token: token.to_vec(),
                    cursor: cursor as u64,
                    next_cursor: next_cursor.map(|v| v as u64),
                    done,
                    rows: rows.into_iter().map(Into::into).collect(),
                    completeness: completeness(c),
                    snapshots: snapshots.into_iter().map(Into::into).collect(),
                }),
                jobs::Output::Progress {
                    token,
                    next_sequence,
                    complete,
                    count,
                    completeness: c,
                    snapshots,
                } => Kind::Progress(wire::JobProgress {
                    token: token.to_vec(),
                    next_sequence,
                    complete,
                    count,
                    completeness: completeness(c),
                    snapshots: snapshots.into_iter().map(Into::into).collect(),
                }),
                jobs::Output::Cancelled => Kind::Cancelled(wire::Empty {}),
            }),
        }),
    };
    wire::CoordinatorJobResponse {
        result: Some(result),
    }
}
pub(super) fn decode_job_response(
    v: wire::CoordinatorJobResponse,
) -> Result<jobs::Response, RemoteError> {
    use wire::coordinator_job_output::Kind;
    use wire::coordinator_job_response::Result as R;
    Ok(match required(v.result)? {
        R::Failure(e) => Err(failure(e)?),
        R::Output(v) => Ok(match required(v.kind)? {
            Kind::ProjectedPage(v) => jobs::Output::ProjectedPage {
                token: token(v.token)?,
                cursor: size(v.cursor)?,
                next_cursor: v.next_cursor.map(size).transpose()?,
                done: v.done,
                rows: v
                    .rows
                    .into_iter()
                    .map(|v| {
                        let (source, properties) = decode_map(v)?;
                        Ok(jobs::ProjectedRow { source, properties })
                    })
                    .collect::<Result<_, RemoteError>>()?,
                completeness: observed(v.completeness)?,
                snapshots: v
                    .snapshots
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            },
            Kind::Preparing(v) => jobs::Output::Preparing {
                token: token(v.token)?,
                next_sequence: v.next_sequence,
                ready: v.ready,
                completeness: observed(v.completeness)?,
                snapshots: v
                    .snapshots
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            },
            Kind::Page(v) => jobs::Output::Page {
                token: token(v.token)?,
                cursor: size(v.cursor)?,
                next_cursor: v.next_cursor.map(size).transpose()?,
                done: v.done,
                rows: v
                    .rows
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
                completeness: observed(v.completeness)?,
                snapshots: v
                    .snapshots
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            },
            Kind::Progress(v) => jobs::Output::Progress {
                token: token(v.token)?,
                next_sequence: v.next_sequence,
                complete: v.complete,
                count: v.count,
                completeness: observed(v.completeness)?,
                snapshots: v
                    .snapshots
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            },
            Kind::Cancelled(_) => jobs::Output::Cancelled,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    fn element() -> QualifiedElement {
        QualifiedElement::Vertex {
            partition: 7,
            id: u64::MAX,
        }
    }
    fn stamps() -> Vec<QueryStamp> {
        vec![QueryStamp {
            identity: RemoteIdentity { group: 7, node: 1 },
            shard: 7,
            epoch: 1,
            sequence: u64::MAX,
        }]
    }
    fn properties() -> BTreeMap<String, Vec<Value>> {
        BTreeMap::from([
            ("int".into(), vec![Value::Integer(1)]),
            ("real".into(), vec![Value::Real(1.0)]),
            ("null".into(), vec![Value::Null]),
            ("text".into(), vec![Value::Text("中文".into())]),
            ("bool".into(), vec![Value::Boolean(false)]),
        ])
    }
    #[test]
    fn query_request_golden_and_all_rows_roundtrip() {
        let request = DistributedRequest {
            start_partition: 7,
            text: "g.V()".into(),
            timeout_millis: 1000,
        };
        let wire = encode_query_request(request);
        assert_eq!(
            wire.encode_to_vec(),
            vec![8, 1, 16, 232, 7, 24, 7, 34, 5, 103, 46, 86, 40, 41]
        );
        let request = decode_query_request(wire).unwrap();
        assert_eq!(request.start_partition, 7);
        assert_eq!(request.text, "g.V()");
        let output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: stamps(),
            rows: vec![
                QueryRow::Element(element()),
                QueryRow::Path(vec![
                    element(),
                    QualifiedElement::Edge {
                        partition: 8,
                        id: 0,
                    },
                ]),
                QueryRow::Count(u64::MAX),
                QueryRow::PropertyMap {
                    source: element(),
                    properties: properties(),
                },
                QueryRow::Value {
                    source: Some(element()),
                    value: Value::Integer(i64::MIN),
                },
                QueryRow::Value {
                    source: None,
                    value: Value::Real(-0.0),
                },
            ],
        };
        let wire = encode_query_response(Ok(output.clone()));
        let bytes = wire.encode_to_vec();
        assert_eq!(
            decode_query_response(
                wire::CoordinatorQueryResponse::decode(bytes.as_slice()).unwrap()
            )
            .unwrap(),
            Ok(output)
        );
    }
    #[test]
    fn all_job_variants_and_failures_roundtrip() {
        for request in [
            jobs::Request::Start {
                start_partition: 7,
                text: "g.V()".into(),
            },
            jobs::Request::StartRows {
                start_partition: 7,
                text: "g.V()".into(),
                page_size: 128,
            },
            jobs::Request::Read {
                token: [1; 16],
                cursor: usize::MAX,
            },
            jobs::Request::Advance {
                token: [2; 16],
                sequence: u64::MAX,
            },
            jobs::Request::Cancel { token: [3; 16] },
        ] {
            let (decoded, deadline) = decode_job_request(encode_job_request(
                request.clone(),
                Duration::from_millis(1000),
            ))
            .unwrap();
            assert_eq!(
                serde_json::to_value(request).unwrap(),
                serde_json::to_value(decoded).unwrap()
            );
            assert_eq!(deadline, Duration::from_secs(1));
        }
        let c = QueryCompleteness::Unverified;
        for output in [
            jobs::Output::ProjectedPage {
                token: [1; 16],
                cursor: 3,
                next_cursor: Some(4),
                done: false,
                rows: vec![jobs::ProjectedRow {
                    source: element(),
                    properties: properties(),
                }],
                completeness: c,
                snapshots: stamps(),
            },
            jobs::Output::Preparing {
                token: [1; 16],
                next_sequence: u64::MAX,
                ready: true,
                completeness: c,
                snapshots: stamps(),
            },
            jobs::Output::Page {
                token: [2; 16],
                cursor: 0,
                next_cursor: None,
                done: true,
                rows: vec![element()],
                completeness: c,
                snapshots: stamps(),
            },
            jobs::Output::Progress {
                token: [3; 16],
                next_sequence: 0,
                complete: true,
                count: Some(0),
                completeness: c,
                snapshots: stamps(),
            },
            jobs::Output::Cancelled,
        ] {
            let wire = encode_job_response(Ok(output.clone()));
            let decoded = decode_job_response(wire).unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(output).unwrap(),
                serde_json::to_value(decoded).unwrap()
            );
        }
        for e in [
            QueryFailure::Parse,
            QueryFailure::Unsupported,
            QueryFailure::InvalidArgument,
            QueryFailure::ResourceExhausted,
            QueryFailure::Timeout,
            QueryFailure::UnsupportedScope,
            QueryFailure::IncompleteOrCorrupt,
            QueryFailure::Unavailable,
        ] {
            assert_eq!(
                decode_query_response(encode_query_response(Err(e))).unwrap(),
                Err(e)
            );
            assert!(
                matches!(decode_job_response(encode_job_response(Err(e))),Ok(Err(actual)) if actual==e)
            );
        }
    }
    #[test]
    fn malformed_versions_tokens_enums_values_and_duplicate_properties_fail() {
        let mut request = wire::CoordinatorQueryRequest {
            version: 1,
            timeout_millis: 1,
            start_partition: 7,
            text: "g.V()".into(),
        };
        request.version = 2;
        assert!(decode_query_request(request.clone()).is_err());
        request.version = 1;
        request.timeout_millis = 0;
        assert!(decode_query_request(request).is_err());
        assert!(decode_query_response(wire::CoordinatorQueryResponse { result: None }).is_err());
        for e in [0, 999] {
            assert!(decode_query_response(wire::CoordinatorQueryResponse {
                result: Some(wire::coordinator_query_response::Result::Failure(e))
            })
            .is_err());
        }
        let mut job = encode_job_request(
            jobs::Request::Cancel { token: [1; 16] },
            Duration::from_secs(1),
        );
        job.request = Some(wire::coordinator_job_request::Request::Cancel(
            wire::JobCancel { token: vec![1; 15] },
        ));
        assert!(decode_job_request(job).is_err());
        let mut map = encode_map(element(), properties());
        map.properties.push(map.properties[0].clone());
        assert!(decode_map(map).is_err());
        assert!(QueryRow::try_from(wire::QueryRow { kind: None }).is_err());
        assert!(QueryRow::try_from(wire::QueryRow {
            kind: Some(wire::query_row::Kind::Value(wire::ScalarValue {
                source: None,
                value: Some(wire::Value {
                    kind: Some(wire::value::Kind::Real(f64::NAN))
                })
            }))
        })
        .is_err());
        let output = DistributedOutput {
            completeness: QueryCompleteness::SinglePartition,
            snapshots: stamps(),
            rows: vec![],
        };
        assert!(decode_query_response(encode_query_response(Ok(output))).is_err());
    }
}
