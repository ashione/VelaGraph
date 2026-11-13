//! Typed public gRPC/domain conversion. The durable command remains the existing
//! managed canonical encoding, never protobuf bytes.
use super::public_proto as pb;
use super::*;
use prost::Message;
#[path = "managed_gateway_public_bounded.rs"]
pub(crate) mod bounded;

pub(super) fn context(c: &Option<pb::Context>) -> Result<Duration> {
    let c = c.as_ref().ok_or(Error::Protocol)?;
    if c.version != 1 || c.timeout_millis == 0 || c.timeout_millis > 60_000 {
        return Err(Error::Protocol);
    }
    Ok(Duration::from_millis(c.timeout_millis))
}
fn required<T>(x: Option<T>) -> Result<T> {
    x.ok_or(Error::Protocol)
}
fn token<const N: usize>(x: Vec<u8>) -> Result<[u8; N]> {
    let x: [u8; N] = x.try_into().map_err(|_| Error::Identity)?;
    if x == [0; N] {
        return Err(Error::Identity);
    }
    Ok(x)
}
fn scalar(s: pb::Scalar) -> Result<Value> {
    use pb::scalar::Kind;
    Ok(match required(s.kind)? {
        Kind::Null(_) => Value::Null,
        Kind::Integer(x) => Value::Integer(x),
        Kind::Real(x) if x.is_finite() => Value::Real(x),
        Kind::Real(_) => return Err(Error::Protocol),
        Kind::Text(x) => Value::Text(x),
        Kind::Boolean(x) => Value::Boolean(x),
    })
}
fn properties(input: Vec<pb::Property>) -> Result<BTreeMap<String, Value>> {
    let mut out = BTreeMap::new();
    for p in input {
        if out.insert(p.key, scalar(required(p.value)?)?).is_some() {
            return Err(Error::Protocol);
        }
    }
    Ok(out)
}
fn entity(e: pb::Entity) -> Entity {
    Entity {
        partition: e.partition,
        id: e.id,
    }
}
fn edge(e: pb::Edge) -> Result<EdgeData> {
    Ok(EdgeData {
        id: e.id,
        from: entity(required(e.from)?),
        to: entity(required(e.to)?),
        label: e.label,
        properties: properties(e.properties)?,
    })
}
fn patch(p: pb::Patch) -> Result<Patch> {
    let n = p.remove.len();
    let remove: BTreeSet<_> = p.remove.into_iter().collect();
    if remove.len() != n {
        return Err(Error::Protocol);
    }
    Ok(Patch {
        set: properties(p.set)?,
        remove,
    })
}
fn conditions(input: Vec<pb::PropertyCondition>) -> Result<PropertyConditions> {
    let mut out = BTreeMap::new();
    for c in input {
        let value = match required(required(c.condition)?.kind)? {
            pb::condition::Kind::Missing(_) => PropertyCondition::Missing,
            pb::condition::Kind::Value(v) => PropertyCondition::Value(scalar(v)?),
        };
        if out.insert(c.key, value).is_some() {
            return Err(Error::Protocol);
        }
    }
    Ok(out)
}
fn mutation(m: pb::Mutation) -> Result<Mutation> {
    use pb::mutation::Kind;
    let m = match required(m.kind)? {
        Kind::CreateVertex(v) => {
            let vertex = required(v.vertex)?;
            Mutation::CreateVertex {
                partition: v.partition,
                vertex: VertexData {
                    id: vertex.id,
                    labels: vertex.labels,
                    properties: properties(vertex.properties)?,
                },
            }
        }
        Kind::CreateEdge(e) => Mutation::CreateEdge {
            edge: edge(required(e.edge)?)?,
        },
        Kind::PatchVertex(p) => Mutation::PatchVertex {
            vertex: entity(required(p.entity)?),
            patch: patch(required(p.patch)?)?,
        },
        Kind::PatchEdge(p) => Mutation::PatchEdge {
            edge: entity(required(p.entity)?),
            patch: patch(required(p.patch)?)?,
        },
        Kind::PatchVertexIf(p) => Mutation::PatchVertexIf {
            vertex: entity(required(p.entity)?),
            patch: patch(required(p.patch)?)?,
            conditions: conditions(p.conditions)?,
        },
        Kind::PatchEdgeIf(p) => Mutation::PatchEdgeIf {
            edge: entity(required(p.entity)?),
            patch: patch(required(p.patch)?)?,
            conditions: conditions(p.conditions)?,
        },
        Kind::DeleteEdge(e) => Mutation::DeleteEdge { edge: entity(e) },
        Kind::ReplaceEdge(e) => Mutation::ReplaceEdge {
            edge: entity(required(e.edge)?),
            replacement: edge(required(e.replacement)?)?,
        },
    };
    m.validate()?;
    Ok(m)
}
fn request_size<M: Message>(m: &M) -> Result<()> {
    if m.encoded_len() > MAX_REQUEST_BYTES {
        Err(Error::Capacity)
    } else {
        Ok(())
    }
}
fn write(r: pb::WriteRequest) -> Result<Write> {
    request_size(&r)?;
    context(&r.context)?;
    Ok(Write {
        external_id: token(r.external_id)?,
        mutation: mutation(required(r.mutation)?)?,
    })
}
pub(super) fn submit(r: pb::WriteRequest) -> Result<Operation> {
    Ok(Operation::Submit(write(r)?))
}
pub(super) fn status(r: pb::WriteRequest) -> Result<Operation> {
    Ok(Operation::Status(write(r)?))
}
pub(super) fn abort(r: pb::WriteRequest) -> Result<Operation> {
    Ok(Operation::Abort(write(r)?))
}
fn query(q: pb::Query) -> Result<Query> {
    if q.text.len() > crate::remote::query::MAX_QUERY_TEXT_BYTES {
        return Err(Error::Capacity);
    }
    Ok(Query {
        partition: q.partition,
        text: q.text,
    })
}
fn size(x: u32) -> Result<u16> {
    let size = u16::try_from(x).map_err(|_| Error::Protocol)?;
    if !(1..=MAX_PAGE_SIZE).contains(&size) {
        return Err(Error::Protocol);
    }
    Ok(size)
}
fn session(s: pb::Session) -> Result<LocalSession> {
    Ok(LocalSession {
        incarnation: token(s.incarnation)?,
        token: token(s.token)?,
        version: s.version,
    })
}
pub(super) fn open(r: pb::OpenRequest) -> Result<Operation> {
    request_size(&r)?;
    context(&r.context)?;
    if r.ttl_millis == 0 || r.ttl_millis > MAX_TTL.as_millis() as u64 {
        return Err(Error::Protocol);
    }
    let query = query(required(r.query)?)?;
    let page_size = size(r.page_size)?;
    Ok(match required(required(r.read)?.kind)? {
        pb::read_selector::Kind::Latest(_) => Operation::OpenLocalQuery {
            version: None,
            ttl_millis: r.ttl_millis,
            query,
            page_size,
        },
        pb::read_selector::Kind::Exact(v) => Operation::OpenLocalQuery {
            version: Some(v),
            ttl_millis: r.ttl_millis,
            query,
            page_size,
        },
        pb::read_selector::Kind::Minimum(v) => Operation::OpenLocalQueryAfter {
            minimum_version: v,
            ttl_millis: r.ttl_millis,
            query,
            page_size,
        },
    })
}
pub(super) fn fetch(r: pb::FetchRequest) -> Result<Operation> {
    request_size(&r)?;
    context(&r.context)?;
    Ok(Operation::FetchLocalQuery {
        session: session(required(r.session)?)?,
        query: query(required(r.query)?)?,
        cursor: r
            .cursor
            .map(|c| {
                Ok::<_, Error>(PageCursor {
                    token: token(c.token)?,
                })
            })
            .transpose()?,
        page_size: size(r.page_size)?,
    })
}
pub(super) fn close(r: pb::CloseRequest) -> Result<Operation> {
    request_size(&r)?;
    context(&r.context)?;
    Ok(Operation::CloseLocalQuery {
        session: session(required(r.session)?)?,
    })
}
fn out_scalar(s: &Value) -> Result<pb::Scalar> {
    use pb::scalar::Kind;
    Ok(pb::Scalar {
        kind: Some(match s {
            Value::Null => Kind::Null(pb::Empty {}),
            Value::Integer(x) => Kind::Integer(*x),
            Value::Real(x) if x.is_finite() => Kind::Real(*x),
            Value::Real(_) => return Err(Error::Protocol),
            Value::Text(x) => Kind::Text(x.clone()),
            Value::Boolean(x) => Kind::Boolean(*x),
        }),
    })
}
fn out_element(e: &crate::remote::query::QualifiedElement) -> pb::QualifiedElement {
    use crate::remote::query::QualifiedElement as Q;
    use pb::qualified_element::Kind;
    pb::QualifiedElement {
        kind: Some(match e {
            Q::Vertex { partition, id } => Kind::Vertex(pb::Entity {
                partition: *partition,
                id: *id,
            }),
            Q::Edge { partition, id } => Kind::Edge(pb::Entity {
                partition: *partition,
                id: *id,
            }),
        }),
    }
}
fn out_row(r: &QueryRow) -> Result<pb::Row> {
    use pb::row::Kind;
    Ok(pb::Row {
        kind: Some(match r {
            QueryRow::Element(e) => Kind::Element(out_element(e)),
            QueryRow::Path(p) => Kind::Path(pb::Path {
                elements: p.iter().map(out_element).collect(),
            }),
            QueryRow::Count(n) => Kind::Count(*n),
            QueryRow::Value { source, value } => Kind::Value(pb::ValueRow {
                source: source.as_ref().map(out_element),
                value: Some(out_scalar(value)?),
            }),
            QueryRow::PropertyMap { source, properties } => Kind::PropertyMap(pb::PropertyMap {
                source: Some(out_element(source)),
                properties: properties
                    .iter()
                    .map(|(key, values)| {
                        Ok(pb::SelectedProperty {
                            key: key.clone(),
                            values: values.iter().map(out_scalar).collect::<Result<_>>()?,
                        })
                    })
                    .collect::<Result<_>>()?,
            }),
        }),
    })
}
fn out_session(s: &LocalSession) -> pb::Session {
    pb::Session {
        incarnation: s.incarnation.to_vec(),
        token: s.token.to_vec(),
        version: s.version,
    }
}
fn out_query(q: &Query) -> pb::Query {
    pb::Query {
        partition: q.partition,
        text: q.text.clone(),
    }
}
fn out_cursor(c: &PageCursor) -> pb::Cursor {
    pb::Cursor {
        token: c.token.to_vec(),
    }
}
fn out_page(p: &QueryPage) -> Result<pb::Page> {
    Ok(pb::Page {
        rows: p.data.rows.iter().map(out_row).collect::<Result<_>>()?,
        completeness: match p.data.completeness {
            QueryCompleteness::SinglePartition => pb::Completeness::SinglePartition as i32,
            QueryCompleteness::Unverified => pb::Completeness::Unverified as i32,
        },
        next: p.next.as_ref().map(out_cursor),
        offset: p.offset,
        total: p.total,
    })
}
fn response_size<M: Message + bounded::BoundedMessage>(m: M) -> Result<M> {
    if m.encoded_len() > MAX_RESPONSE_BYTES {
        Err(Error::Capacity)
    } else {
        // Do not admit a response the checked generated client would reject.
        bounded::validate_bytes::<M>(&m.encode_to_vec()).map_err(|e| {
            if e.code() == tonic::Code::ResourceExhausted {
                Error::Capacity
            } else {
                Error::Protocol
            }
        })?;
        Ok(m)
    }
}
pub(super) fn encode_write(r: &Reply) -> Result<pb::WriteResponse> {
    let Reply::Write {
        external_id,
        transaction_id,
        status,
    } = r
    else {
        return Err(Error::Protocol);
    };
    use pb::write_response::Status;
    response_size(pb::WriteResponse {
        external_id: external_id.to_vec(),
        transaction_id: transaction_id.as_ref().map(|x| x.to_vec()),
        status: Some(match status {
            WriteStatus::Absent => Status::Absent(pb::Empty {}),
            WriteStatus::Pending => Status::Pending(pb::Empty {}),
            WriteStatus::Committed { ordinal, visible } => {
                Status::Committed(pb::TerminalDecision {
                    ordinal: *ordinal,
                    visible: *visible,
                })
            }
            WriteStatus::Aborted { ordinal, visible } => Status::Aborted(pb::TerminalDecision {
                ordinal: *ordinal,
                visible: *visible,
            }),
        }),
    })
}
pub(super) fn encode_open(r: &Reply) -> Result<pb::OpenResponse> {
    let Reply::OpenLocalQuery {
        session,
        query,
        page_size,
        page,
    } = r
    else {
        return Err(Error::Protocol);
    };
    response_size(pb::OpenResponse {
        session: Some(out_session(session)),
        query: Some(out_query(query)),
        page_size: u32::from(*page_size),
        page: Some(out_page(page)?),
    })
}
pub(super) fn encode_fetch(r: &Reply) -> Result<pb::FetchResponse> {
    let Reply::FetchLocalQuery {
        session,
        query,
        cursor,
        page_size,
        page,
    } = r
    else {
        return Err(Error::Protocol);
    };
    response_size(pb::FetchResponse {
        session: Some(out_session(session)),
        query: Some(out_query(query)),
        cursor: cursor.as_ref().map(out_cursor),
        page_size: u32::from(*page_size),
        page: Some(out_page(page)?),
    })
}
pub(super) fn encode_close(r: &Reply) -> Result<pb::CloseResponse> {
    let Reply::CloseLocalQuery(session) = r else {
        return Err(Error::Protocol);
    };
    response_size(pb::CloseResponse {
        session: Some(out_session(session)),
    })
}
pub(super) fn error(e: Error) -> tonic::Status {
    use pb::error_detail::Reason as R;
    use tonic::Code as C;
    let (code, reason) = match e {
        Error::Protocol => (C::InvalidArgument, R::InvalidArgument),
        Error::Identity => (C::FailedPrecondition, R::IdentityMismatch),
        Error::Capacity => (C::ResourceExhausted, R::ResourceExhausted),
        Error::Rejected => (C::Unavailable, R::RejectedOutcomeUncertain),
        Error::PreconditionFailed => (C::FailedPrecondition, R::PreconditionFailed),
        Error::Unknown => (C::Unavailable, R::OutcomeUnknown),
        Error::SessionUnavailable => (C::FailedPrecondition, R::SessionUnavailable),
        Error::JobBusy => (C::FailedPrecondition, R::JobBusy),
        Error::Query(q) => match q {
            QueryFailure::Parse => (C::InvalidArgument, R::QuerySyntax),
            QueryFailure::Unsupported | QueryFailure::UnsupportedScope => {
                (C::Unimplemented, R::QueryUnsupported)
            }
            QueryFailure::InvalidArgument => (C::InvalidArgument, R::QueryInvalidArgument),
            QueryFailure::ResourceExhausted => (C::ResourceExhausted, R::QueryResourceExhausted),
            QueryFailure::Timeout => (C::DeadlineExceeded, R::QueryDeadline),
            QueryFailure::IncompleteOrCorrupt => (C::Unavailable, R::QueryIncompleteOrCorrupt),
            QueryFailure::Unavailable => (C::Unavailable, R::QueryUnavailable),
        },
    };
    tonic::Status::with_details(
        code,
        reason.as_str_name(),
        pb::ErrorDetail {
            reason: reason as i32,
        }
        .encode_to_vec()
        .into(),
    )
}

// Fixed snapshot sessions deliberately use a distinct public DTO from local
// query sessions. Both are checked by their original state registry.
fn fixed_session(s: pb::FixedSession) -> Result<Session> {
    Ok(Session {
        incarnation: token(s.incarnation)?,
        token: token(s.token)?,
        version: s.version,
    })
}
pub(super) fn fixed_open(r: pb::FixedOpenRequest) -> Result<Operation> {
    request_size(&r)?;
    context(&r.context)?;
    if r.ttl_millis == 0 || r.ttl_millis > MAX_TTL.as_millis() as u64 {
        return Err(Error::Protocol);
    }
    Ok(Operation::OpenSession { version: r.version, ttl_millis: r.ttl_millis })
}
pub(super) fn fixed_read(r: pb::FixedQueryRequest) -> Result<Operation> {
    request_size(&r)?;
    context(&r.context)?;
    Ok(Operation::Read { session: fixed_session(required(r.session)?)?, query: query(required(r.query)?)? })
}
pub(super) fn fixed_traverse(r: pb::FixedQueryRequest) -> Result<Operation> {
    request_size(&r)?;
    context(&r.context)?;
    Ok(Operation::Traverse { session: fixed_session(required(r.session)?)?, query: query(required(r.query)?)? })
}
pub(super) fn fixed_page(r: pb::FixedPageRequest) -> Result<Operation> {
    request_size(&r)?;
    context(&r.context)?;
    Ok(Operation::TraversePage {
        session: fixed_session(required(r.session)?)?,
        query: query(required(r.query)?)?,
        cursor: r.cursor.map(|c| Ok::<_, Error>(PageCursor { token: token(c.token)? })).transpose()?,
        page_size: size(r.page_size)?,
    })
}
pub(super) fn fixed_close(r: pb::FixedCloseRequest) -> Result<Operation> {
    request_size(&r)?;
    context(&r.context)?;
    Ok(Operation::Close { session: fixed_session(required(r.session)?)? })
}
fn out_fixed_session(s: &Session) -> pb::FixedSession {
    pb::FixedSession { incarnation: s.incarnation.to_vec(), token: s.token.to_vec(), version: s.version }
}
pub(super) fn encode_fixed_open(r: &Reply) -> Result<pb::FixedOpenResponse> {
    let Reply::OpenSession(session) = r else { return Err(Error::Protocol); };
    response_size(pb::FixedOpenResponse { session: Some(out_fixed_session(session)) })
}
pub(super) fn encode_fixed_query(r: &Reply) -> Result<pb::FixedQueryResponse> {
    let (session, query, data) = match r {
        Reply::Read { session, query, data } | Reply::Traverse { session, query, data } => (session, query, data),
        _ => return Err(Error::Protocol),
    };
    response_size(pb::FixedQueryResponse {
        session: Some(out_fixed_session(session)), query: Some(out_query(query)),
        data: Some(pb::QueryData { rows: data.rows.iter().map(out_row).collect::<Result<_>>()?, completeness: match data.completeness {
            QueryCompleteness::SinglePartition => pb::Completeness::SinglePartition as i32,
            QueryCompleteness::Unverified => pb::Completeness::Unverified as i32,
        } }),
    })
}
pub(super) fn encode_fixed_page(r: &Reply) -> Result<pb::FixedPageResponse> {
    let Reply::TraversePage { session, query, cursor, page_size, page } = r else { return Err(Error::Protocol); };
    response_size(pb::FixedPageResponse {
        session: Some(out_fixed_session(session)), query: Some(out_query(query)), cursor: cursor.as_ref().map(out_cursor), page_size: u32::from(*page_size), page: Some(out_page(page)?),
    })
}
pub(super) fn encode_fixed_close(r: &Reply) -> Result<pb::FixedCloseResponse> {
    let Reply::Close(session) = r else { return Err(Error::Protocol); };
    response_size(pb::FixedCloseResponse { session: Some(out_fixed_session(session)) })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_session_requests_preserve_exact_zero_and_reject_invalid_identity() {
        let request = pb::FixedOpenRequest { context: ctx(), version: Some(0), ttl_millis: 500 };
        assert_eq!(fixed_open(request.clone()).unwrap(), Operation::OpenSession { version: Some(0), ttl_millis: 500 });
        assert_eq!(fixed_open(pb::FixedOpenRequest { version: None, ..request.clone() }).unwrap(), Operation::OpenSession { version: None, ttl_millis: 500 });
        assert!(fixed_open(pb::FixedOpenRequest { context: None, ..request.clone() }).is_err());
        assert!(fixed_open(pb::FixedOpenRequest { ttl_millis: 0, ..request.clone() }).is_err());
        assert!(fixed_open(pb::FixedOpenRequest { ttl_millis: MAX_TTL.as_millis() as u64 + 1, ..request }).is_err());
        let session = pb::FixedSession { incarnation: vec![1; 16], token: vec![2; 32], version: 7 };
        let query = pb::Query { partition: 7, text: "g.V(1)".into() };
        let request = pb::FixedQueryRequest { context: ctx(), session: Some(session.clone()), query: Some(query.clone()) };
        let expected_session = Session { incarnation: [1; 16], token: [2; 32], version: 7 };
        let expected_query = Query { partition: 7, text: "g.V(1)".into() };
        assert_eq!(fixed_read(request.clone()).unwrap(), Operation::Read { session: expected_session.clone(), query: expected_query.clone() });
        assert_eq!(fixed_traverse(request.clone()).unwrap(), Operation::Traverse { session: expected_session, query: expected_query });
        assert!(fixed_read(pb::FixedQueryRequest { session: None, ..request.clone() }).is_err());
        assert!(fixed_traverse(pb::FixedQueryRequest { query: None, ..request.clone() }).is_err());
        for n in [0, 15, 17] {
            assert!(fixed_traverse(pb::FixedQueryRequest { session: Some(pb::FixedSession { incarnation: vec![1; n], ..session.clone() }), ..request.clone() }).is_err());
        }
        for n in [0, 31, 33] {
            assert!(fixed_read(pb::FixedQueryRequest { session: Some(pb::FixedSession { token: vec![2; n], ..session.clone() }), ..request.clone() }).is_err());
        }
        for n in [0, u32::from(MAX_PAGE_SIZE) + 1, u32::MAX] {
            assert!(fixed_page(pb::FixedPageRequest { context: ctx(), session: Some(session.clone()), query: Some(query.clone()), cursor: None, page_size: n }).is_err());
        }
        assert!(fixed_page(pb::FixedPageRequest { context: ctx(), session: Some(session.clone()), query: Some(query), cursor: Some(pb::Cursor { token: vec![1; 31] }), page_size: 1 }).is_err());
        assert_eq!(fixed_close(pb::FixedCloseRequest { context: ctx(), session: Some(session) }).unwrap(), Operation::Close { session: Session { incarnation: [1; 16], token: [2; 32], version: 7 } });
    }
    #[test]
    fn fixed_response_preserves_rows_and_rejects_wrong_reply() {
        let session = Session { incarnation: [1; 16], token: [2; 32], version: 0 };
        let opened = encode_fixed_open(&Reply::OpenSession(session.clone())).unwrap();
        assert_eq!(opened.session.unwrap().version, 0);
        let query = Query { partition: 7, text: "g.V().count()".into() };
        let reply = Reply::Traverse { session: session.clone(), query: query.clone(), data: QueryData { rows: vec![QueryRow::Count(u64::MAX)], completeness: QueryCompleteness::Unverified } };
        let encoded = encode_fixed_query(&reply).unwrap();
        assert_eq!(encoded.data.unwrap().rows[0].kind, Some(pb::row::Kind::Count(u64::MAX)));
        assert!(encode_fixed_open(&reply).is_err());
        assert!(encode_fixed_page(&reply).is_err());
        assert_eq!(encode_fixed_close(&Reply::Close(session)).unwrap().session.unwrap().version, 0);
    }
    fn ctx() -> Option<pb::Context> {
        Some(pb::Context {
            version: 1,
            timeout_millis: 500,
        })
    }
    fn ent() -> Option<pb::Entity> {
        Some(pb::Entity {
            partition: 7,
            id: u64::MAX,
        })
    }
    fn edge() -> Option<pb::Edge> {
        Some(pb::Edge {
            id: 9,
            from: ent(),
            to: Some(pb::Entity {
                partition: 8,
                id: 2,
            }),
            label: "rel".into(),
            properties: vec![],
        })
    }
    fn patch() -> Option<pb::Patch> {
        Some(pb::Patch {
            set: vec![pb::Property {
                key: "x".into(),
                value: Some(pb::Scalar {
                    kind: Some(pb::scalar::Kind::Null(pb::Empty {})),
                }),
            }],
            remove: vec!["y".into()],
        })
    }
    fn write_request(kind: pb::mutation::Kind) -> pb::WriteRequest {
        pb::WriteRequest {
            context: ctx(),
            external_id: vec![1; 16],
            mutation: Some(pb::Mutation { kind: Some(kind) }),
        }
    }
    #[test]
    fn all_mutations_share_exact_submit_status_abort_domain() {
        use pb::mutation::Kind as K;
        for kind in [
            K::CreateVertex(pb::CreateVertex {
                partition: 7,
                vertex: Some(pb::Vertex {
                    id: u64::MAX,
                    labels: vec![],
                    properties: vec![],
                }),
            }),
            K::CreateEdge(pb::CreateEdge { edge: edge() }),
            K::PatchVertex(pb::PatchEntity {
                entity: ent(),
                patch: patch(),
            }),
            K::PatchEdge(pb::PatchEntity {
                entity: ent(),
                patch: patch(),
            }),
            K::PatchVertexIf(pb::ConditionalPatch {
                entity: ent(),
                patch: patch(),
                conditions: vec![pb::PropertyCondition {
                    key: "x".into(),
                    condition: Some(pb::Condition {
                        kind: Some(pb::condition::Kind::Missing(pb::Empty {})),
                    }),
                }],
            }),
            K::PatchEdgeIf(pb::ConditionalPatch {
                entity: ent(),
                patch: patch(),
                conditions: vec![pb::PropertyCondition {
                    key: "x".into(),
                    condition: Some(pb::Condition {
                        kind: Some(pb::condition::Kind::Value(pb::Scalar {
                            kind: Some(pb::scalar::Kind::Null(pb::Empty {})),
                        })),
                    }),
                }],
            }),
            K::DeleteEdge(ent().unwrap()),
            K::ReplaceEdge(pb::ReplaceEdge {
                edge: ent(),
                replacement: edge(),
            }),
        ] {
            let req = write_request(kind);
            let Operation::Submit(a) = submit(req.clone()).unwrap() else {
                panic!()
            };
            let Operation::Status(b) = status(req.clone()).unwrap() else {
                panic!()
            };
            let Operation::Abort(c) = abort(req).unwrap() else {
                panic!()
            };
            assert_eq!(a, b);
            assert_eq!(a, c);
        }
    }
    #[test]
    fn scalar_presence_null_width_and_finite_are_strict() {
        assert_eq!(scalar(pb::Scalar { kind: None }), Err(Error::Protocol));
        assert_eq!(
            scalar(pb::Scalar {
                kind: Some(pb::scalar::Kind::Integer(i64::MIN))
            }),
            Ok(Value::Integer(i64::MIN))
        );
        assert_eq!(
            scalar(pb::Scalar {
                kind: Some(pb::scalar::Kind::Real(f64::INFINITY))
            }),
            Err(Error::Protocol)
        );
        assert_eq!(
            scalar(pb::Scalar {
                kind: Some(pb::scalar::Kind::Null(pb::Empty {}))
            }),
            Ok(Value::Null)
        );
        let p = pb::Property {
            key: "x".into(),
            value: Some(pb::Scalar {
                kind: Some(pb::scalar::Kind::Boolean(false)),
            }),
        };
        assert_eq!(properties(vec![p.clone(), p]), Err(Error::Protocol));
        assert_eq!(token::<16>(vec![0; 16]), Err(Error::Identity));
        assert_eq!(token::<16>(vec![1; 15]), Err(Error::Identity));
    }
    #[test]
    fn read_selector_and_context_never_silently_default() {
        let req = pb::OpenRequest {
            context: ctx(),
            read: Some(pb::ReadSelector {
                kind: Some(pb::read_selector::Kind::Exact(0)),
            }),
            ttl_millis: 1000,
            query: Some(pb::Query {
                partition: 7,
                text: "g.V(1)".into(),
            }),
            page_size: 1,
        };
        assert!(matches!(
            open(req.clone()).unwrap(),
            Operation::OpenLocalQuery {
                version: Some(0),
                ..
            }
        ));
        let mut bad = req.clone();
        bad.read = None;
        assert_eq!(open(bad), Err(Error::Protocol));
        let mut bad = req.clone();
        bad.context = None;
        assert_eq!(open(bad), Err(Error::Protocol));
        let mut minimum = req;
        minimum.read = Some(pb::ReadSelector {
            kind: Some(pb::read_selector::Kind::Minimum(u64::MAX)),
        });
        assert!(matches!(
            open(minimum).unwrap(),
            Operation::OpenLocalQueryAfter {
                minimum_version: u64::MAX,
                ..
            }
        ));
    }
    #[test]
    fn result_rows_visibility_and_error_details_preserve_meaning() {
        use crate::remote::query::QualifiedElement as Q;
        let source = Q::Vertex {
            partition: 7,
            id: u64::MAX,
        };
        let reply = Reply::FetchLocalQuery {
            session: LocalSession {
                incarnation: [1; 16],
                token: [2; 32],
                version: 3,
            },
            query: Query {
                partition: 7,
                text: "g.V(1)".into(),
            },
            cursor: None,
            page_size: 10,
            page: QueryPage {
                data: QueryData {
                    completeness: QueryCompleteness::Unverified,
                    rows: vec![
                        QueryRow::Element(source),
                        QueryRow::Path(vec![source]),
                        QueryRow::Count(u64::MAX),
                        QueryRow::Value {
                            source: None,
                            value: Value::Integer(i64::MIN),
                        },
                        QueryRow::PropertyMap {
                            source,
                            properties: BTreeMap::from([("x".into(), vec![Value::Null])]),
                        },
                    ],
                },
                next: None,
                offset: 0,
                total: 5,
            },
        };
        let out = encode_fetch(&reply).unwrap();
        assert_eq!(out.page.unwrap().rows.len(), 5);
        let out = encode_write(&Reply::Write {
            external_id: [1; 16],
            transaction_id: Some([2; 16]),
            status: WriteStatus::Committed {
                ordinal: u64::MAX,
                visible: false,
            },
        })
        .unwrap();
        assert!(matches!(
            out.status,
            Some(pb::write_response::Status::Committed(
                pb::TerminalDecision { visible: false, .. }
            ))
        ));
        let err = error(Error::Unknown);
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(
            pb::ErrorDetail::decode(err.details()).unwrap().reason,
            pb::error_detail::Reason::OutcomeUnknown as i32
        );
    }
}
