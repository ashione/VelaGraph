//! Preview HTTP JSON contract. Integer identities/values are canonical decimal
//! strings; tokens are lowercase hex. This is not protobuf JSON or a raw
//! managed Operation tunnel. Both are deliberate, versioned preview boundaries.
use super::*;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde_json::{json, Value as Json};
use std::fmt;

/// Reject duplicate object keys at every depth before DTO conversion. The JSON
/// parser retains its normal recursion limit and the outer frame is bounded.
struct Unique(Json);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("strict JSON")
            }
            fn visit_bool<E: de::Error>(self, x: bool) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(x)))
            }
            fn visit_i64<E: de::Error>(self, x: i64) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(x)))
            }
            fn visit_u64<E: de::Error>(self, x: u64) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(x)))
            }
            fn visit_f64<E: de::Error>(self, x: f64) -> std::result::Result<Unique, E> {
                serde_json::Number::from_f64(x)
                    .map(|n| Unique(Json::Number(n)))
                    .ok_or_else(|| E::custom("nonfinite"))
            }
            fn visit_str<E: de::Error>(self, x: &str) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(x)))
            }
            fn visit_string<E: de::Error>(self, x: String) -> std::result::Result<Unique, E> {
                Ok(Unique(Json::String(x)))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Json::Null))
            }
            fn visit_none<E: de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Json::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut values = Vec::new();
                while let Some(Unique(v)) = a.next_element()? {
                    values.push(v);
                }
                Ok(Unique(Json::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some((k, Unique(v))) = a.next_entry::<String, Unique>()? {
                    if values.insert(k, v).is_some() {
                        return Err(de::Error::custom("duplicate key"));
                    }
                }
                Ok(Unique(Json::Object(values)))
            }
        }
        d.deserialize_any(V)
    }
}
fn dto<T: DeserializeOwned>(input: &[u8]) -> Result<T> {
    if input.len() > MAX_REQUEST_BYTES {
        return Err(Error::Capacity);
    }
    let Unique(value) = serde_json::from_slice(input).map_err(|_| Error::Protocol)?;
    serde_json::from_value(value).map_err(|_| Error::Protocol)
}
fn uint(s: &str) -> Result<u64> {
    let n = s.parse::<u64>().map_err(|_| Error::Protocol)?;
    if n.to_string() != s {
        return Err(Error::Protocol);
    }
    Ok(n)
}
fn sint(s: &str) -> Result<i64> {
    let n = s.parse::<i64>().map_err(|_| Error::Protocol)?;
    if n.to_string() != s {
        return Err(Error::Protocol);
    }
    Ok(n)
}
fn unhex<const N: usize>(s: &str) -> Result<[u8; N]> {
    if s.len() != N * 2 {
        return Err(Error::Protocol);
    }
    let mut out = [0; N];
    for (i, c) in s.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let nibble = |c| match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            _ => Err(Error::Protocol),
        };
        out[i] = (nibble(c[0])? << 4) | nibble(c[1])?;
    }
    Ok(out)
}
fn hex(bytes: &[u8]) -> String {
    const H: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 15) as usize] as char);
    }
    s
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Scalar {
    Null {},
    Integer { value: String },
    Real { value: f64 },
    Text { value: String },
    Boolean { value: bool },
}
impl Scalar {
    fn domain(self) -> Result<Value> {
        Ok(match self {
            Self::Null {} => Value::Null,
            Self::Integer { value } => Value::Integer(sint(&value)?),
            Self::Real { value } if value.is_finite() => Value::Real(value),
            Self::Real { .. } => return Err(Error::Protocol),
            Self::Text { value } => Value::Text(value),
            Self::Boolean { value } => Value::Boolean(value),
        })
    }
}
fn properties(p: BTreeMap<String, Scalar>) -> Result<BTreeMap<String, Value>> {
    p.into_iter().map(|(k, v)| Ok((k, v.domain()?))).collect()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityDto {
    partition: u32,
    id: String,
}
impl EntityDto {
    fn domain(self) -> Result<Entity> {
        Ok(Entity {
            partition: self.partition,
            id: uint(&self.id)?,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VertexDto {
    id: String,
    labels: Vec<String>,
    properties: BTreeMap<String, Scalar>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EdgeDto {
    id: String,
    from: EntityDto,
    to: EntityDto,
    label: String,
    properties: BTreeMap<String, Scalar>,
}
impl EdgeDto {
    fn domain(self) -> Result<EdgeData> {
        Ok(EdgeData {
            id: uint(&self.id)?,
            from: self.from.domain()?,
            to: self.to.domain()?,
            label: self.label,
            properties: properties(self.properties)?,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchDto {
    set: BTreeMap<String, Scalar>,
    remove: Vec<String>,
}
impl PatchDto {
    fn domain(self) -> Result<Patch> {
        let remove: BTreeSet<_> = self.remove.iter().cloned().collect();
        if remove.len() != self.remove.len() {
            return Err(Error::Protocol);
        }
        Ok(Patch {
            set: properties(self.set)?,
            remove,
        })
    }
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Condition {
    Missing {},
    Value { value: Scalar },
}
fn conditions(c: BTreeMap<String, Condition>) -> Result<PropertyConditions> {
    c.into_iter()
        .map(|(k, v)| {
            Ok((
                k,
                match v {
                    Condition::Missing {} => PropertyCondition::Missing,
                    Condition::Value { value } => PropertyCondition::Value(value.domain()?),
                },
            ))
        })
        .collect()
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum MutationDto {
    CreateVertex {
        partition: u32,
        vertex: VertexDto,
    },
    CreateEdge {
        edge: EdgeDto,
    },
    PatchVertex {
        vertex: EntityDto,
        patch: PatchDto,
    },
    PatchEdge {
        edge: EntityDto,
        patch: PatchDto,
    },
    PatchVertexIf {
        vertex: EntityDto,
        conditions: BTreeMap<String, Condition>,
        patch: PatchDto,
    },
    PatchEdgeIf {
        edge: EntityDto,
        conditions: BTreeMap<String, Condition>,
        patch: PatchDto,
    },
    DeleteEdge {
        edge: EntityDto,
    },
    ReplaceEdge {
        edge: EntityDto,
        replacement: EdgeDto,
    },
}
impl MutationDto {
    fn domain(self) -> Result<Mutation> {
        Ok(match self {
            Self::CreateVertex { partition, vertex } => Mutation::CreateVertex {
                partition,
                vertex: VertexData {
                    id: uint(&vertex.id)?,
                    labels: vertex.labels,
                    properties: properties(vertex.properties)?,
                },
            },
            Self::CreateEdge { edge } => Mutation::CreateEdge {
                edge: edge.domain()?,
            },
            Self::PatchVertex { vertex, patch } => Mutation::PatchVertex {
                vertex: vertex.domain()?,
                patch: patch.domain()?,
            },
            Self::PatchEdge { edge, patch } => Mutation::PatchEdge {
                edge: edge.domain()?,
                patch: patch.domain()?,
            },
            Self::PatchVertexIf {
                vertex,
                conditions: c,
                patch,
            } => Mutation::PatchVertexIf {
                vertex: vertex.domain()?,
                conditions: conditions(c)?,
                patch: patch.domain()?,
            },
            Self::PatchEdgeIf {
                edge,
                conditions: c,
                patch,
            } => Mutation::PatchEdgeIf {
                edge: edge.domain()?,
                conditions: conditions(c)?,
                patch: patch.domain()?,
            },
            Self::DeleteEdge { edge } => Mutation::DeleteEdge {
                edge: edge.domain()?,
            },
            Self::ReplaceEdge { edge, replacement } => Mutation::ReplaceEdge {
                edge: edge.domain()?,
                replacement: replacement.domain()?,
            },
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteDto {
    external_id: String,
    mutation: MutationDto,
}
impl WriteDto {
    fn domain(self) -> Result<Write> {
        let external_id = unhex(&self.external_id)?;
        if external_id == [0; 16] {
            return Err(Error::Identity);
        }
        let mutation = self.mutation.domain()?;
        mutation.validate()?;
        Ok(Write {
            external_id,
            mutation,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryDto {
    partition: u32,
    text: String,
}
impl QueryDto {
    fn domain(self) -> Result<Query> {
        if self.text.len() > crate::remote::query::MAX_QUERY_TEXT_BYTES {
            return Err(Error::Capacity);
        }
        Ok(Query {
            partition: self.partition,
            text: self.text,
        })
    }
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ReadSelector {
    Latest {},
    Exact { version: String },
    Minimum { version: String },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenDto {
    read: ReadSelector,
    ttl_millis: u64,
    query: QueryDto,
    page_size: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionDto {
    incarnation: String,
    token: String,
    version: String,
}
impl SessionDto {
    fn domain(self) -> Result<LocalSession> {
        Ok(LocalSession {
            incarnation: unhex(&self.incarnation)?,
            token: unhex(&self.token)?,
            version: uint(&self.version)?,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchDto {
    session: SessionDto,
    query: QueryDto,
    cursor: Option<String>,
    page_size: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CloseDto {
    session: SessionDto,
}

pub(super) fn decode(path: &str, input: &[u8]) -> Result<Operation> {
    Ok(match path {
        "/preview1/writes:submit" => Operation::Submit(dto::<WriteDto>(input)?.domain()?),
        "/preview1/writes:status" => Operation::Status(dto::<WriteDto>(input)?.domain()?),
        "/preview1/writes:abort" => Operation::Abort(dto::<WriteDto>(input)?.domain()?),
        "/preview1/queries:open" => {
            let r: OpenDto = dto(input)?;
            if r.ttl_millis == 0
                || r.ttl_millis > MAX_TTL.as_millis() as u64
                || r.page_size == 0
                || r.page_size > MAX_PAGE_SIZE
            {
                return Err(Error::Protocol);
            }
            let query = r.query.domain()?;
            match r.read {
                ReadSelector::Latest {} => Operation::OpenLocalQuery {
                    version: None,
                    ttl_millis: r.ttl_millis,
                    query,
                    page_size: r.page_size,
                },
                ReadSelector::Exact { version } => Operation::OpenLocalQuery {
                    version: Some(uint(&version)?),
                    ttl_millis: r.ttl_millis,
                    query,
                    page_size: r.page_size,
                },
                ReadSelector::Minimum { version } => Operation::OpenLocalQueryAfter {
                    minimum_version: uint(&version)?,
                    ttl_millis: r.ttl_millis,
                    query,
                    page_size: r.page_size,
                },
            }
        }
        "/preview1/queries:fetch" => {
            let r: FetchDto = dto(input)?;
            if r.page_size == 0 || r.page_size > MAX_PAGE_SIZE {
                return Err(Error::Protocol);
            }
            Operation::FetchLocalQuery {
                session: r.session.domain()?,
                query: r.query.domain()?,
                cursor: r
                    .cursor
                    .map(|s| Ok::<_, Error>(PageCursor { token: unhex(&s)? }))
                    .transpose()?,
                page_size: r.page_size,
            }
        }
        "/preview1/queries:close" => Operation::CloseLocalQuery {
            session: dto::<CloseDto>(input)?.session.domain()?,
        },
        _ => return Err(Error::Protocol),
    })
}
fn scalar(v: &Value) -> Result<Json> {
    Ok(match v {
        Value::Null => json!({"type":"null"}),
        Value::Integer(v) => json!({"type":"integer","value":v.to_string()}),
        Value::Real(v) if v.is_finite() => json!({"type":"real","value":v}),
        Value::Real(_) => return Err(Error::Protocol),
        Value::Text(v) => json!({"type":"text","value":v}),
        Value::Boolean(v) => json!({"type":"boolean","value":v}),
    })
}
fn element(e: &crate::remote::query::QualifiedElement) -> Json {
    use crate::remote::query::QualifiedElement::*;
    match e {
        Vertex { partition, id } => {
            json!({"type":"vertex","partition":partition,"id":id.to_string()})
        }
        Edge { partition, id } => json!({"type":"edge","partition":partition,"id":id.to_string()}),
    }
}
fn row(r: &QueryRow) -> Result<Json> {
    Ok(match r {
        QueryRow::Element(e) => json!({"type":"element","element":element(e)}),
        QueryRow::Path(p) => {
            json!({"type":"path","elements":p.iter().map(element).collect::<Vec<_>>()})
        }
        QueryRow::Count(n) => json!({"type":"count","value":n.to_string()}),
        QueryRow::Value { source, value } => {
            json!({"type":"value","source":source.as_ref().map(element),"value":scalar(value)?})
        }
        QueryRow::PropertyMap { source, properties } => {
            let values: Result<BTreeMap<_, _>> = properties
                .iter()
                .map(|(k, v)| Ok((k, v.iter().map(scalar).collect::<Result<Vec<_>>>()?)))
                .collect();
            json!({"type":"property_map","source":element(source),"properties":values?})
        }
    })
}
fn session(s: &LocalSession) -> Json {
    json!({"incarnation":hex(&s.incarnation),"token":hex(&s.token),"version":s.version.to_string()})
}
fn query(q: &Query) -> Json {
    json!({"partition":q.partition,"text":q.text})
}
fn page(p: &QueryPage) -> Result<Json> {
    Ok(
        json!({"rows":p.data.rows.iter().map(row).collect::<Result<Vec<_>>>()?,"completeness":match p.data.completeness{QueryCompleteness::SinglePartition=>"single_partition",QueryCompleteness::Unverified=>"unverified"},"next":p.next.as_ref().map(|c|hex(&c.token)),"offset":p.offset,"total":p.total}),
    )
}
fn reply(r: &Reply) -> Result<Json> {
    Ok(match r {
        Reply::Write {
            external_id,
            transaction_id,
            status,
        } => {
            let status = match status {
                WriteStatus::Absent => json!({"type":"absent"}),
                WriteStatus::Pending => json!({"type":"pending"}),
                WriteStatus::Committed { ordinal, visible } => {
                    json!({"type":"committed","ordinal":ordinal.to_string(),"visible":visible})
                }
                WriteStatus::Aborted { ordinal, visible } => {
                    json!({"type":"aborted","ordinal":ordinal.to_string(),"visible":visible})
                }
            };
            json!({"type":"write","external_id":hex(external_id),"transaction_id":transaction_id.as_ref().map(|id|hex(id)),"status":status})
        }
        Reply::OpenLocalQuery {
            session: s,
            query: q,
            page_size,
            page: p,
        } => {
            json!({"type":"query_opened","session":session(s),"query":query(q),"page_size":page_size,"page":page(p)?})
        }
        Reply::FetchLocalQuery {
            session: s,
            query: q,
            cursor,
            page_size,
            page: p,
        } => {
            json!({"type":"query_page","session":session(s),"query":query(q),"cursor":cursor.as_ref().map(|c|hex(&c.token)),"page_size":page_size,"page":page(p)?})
        }
        Reply::CloseLocalQuery(s) => json!({"type":"query_closed","session":session(s)}),
        _ => return Err(Error::Protocol),
    })
}
pub(super) fn error_code(e: &Error) -> (u16, &'static str) {
    match e {
        Error::Protocol => (400, "invalid_argument"),
        Error::Identity => (409, "identity_mismatch"),
        Error::Capacity => (429, "resource_exhausted"),
        Error::Rejected => (503, "rejected_outcome_uncertain"),
        Error::PreconditionFailed => (409, "precondition_failed"),
        Error::Unknown => (503, "outcome_unknown"),
        Error::SessionUnavailable => (410, "session_unavailable"),
        Error::JobBusy => (409, "job_busy"),
        Error::Query(q) => match q {
            QueryFailure::Parse => (400, "query_syntax"),
            QueryFailure::Unsupported | QueryFailure::UnsupportedScope => {
                (501, "query_unsupported")
            }
            QueryFailure::InvalidArgument => (400, "query_invalid_argument"),
            QueryFailure::ResourceExhausted => (429, "query_resource_exhausted"),
            QueryFailure::Timeout => (504, "query_deadline"),
            QueryFailure::IncompleteOrCorrupt => (503, "query_incomplete_or_corrupt"),
            QueryFailure::Unavailable => (503, "query_unavailable"),
        },
    }
}
/// Encoding is bounded even when JSON escaping expands a typed result.
pub(super) fn encode(result: &Result<Reply>) -> Result<(u16, Vec<u8>)> {
    let (status, value) = match result {
        Ok(r) => (
            200,
            json!({"api":"velagraph.http.preview1","result":reply(r)?}),
        ),
        Err(e) => {
            let (status, code) = error_code(e);
            (
                status,
                json!({"api":"velagraph.http.preview1","error":{"code":code}}),
            )
        }
    };
    struct Bounded(Vec<u8>);
    impl std::io::Write for Bounded {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            if b.len() > MAX_RESPONSE_BYTES.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("response limit"));
            }
            self.0.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut out = Bounded(Vec::new());
    serde_json::to_writer(&mut out, &value).map_err(|_| Error::Capacity)?;
    Ok((status, out.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    const OPEN: &str = r#"{"read":{"type":"exact","version":"0"},"ttl_millis":1000,"query":{"partition":1,"text":"g.V().count()"},"page_size":5}"#;
    #[test]
    fn exact_zero_and_latest_are_distinct() {
        assert!(matches!(
            decode("/preview1/queries:open", OPEN.as_bytes()).unwrap(),
            Operation::OpenLocalQuery {
                version: Some(0),
                ..
            }
        ));
        let latest = OPEN.replace(r#"{"type":"exact","version":"0"}"#, r#"{"type":"latest"}"#);
        assert!(matches!(
            decode("/preview1/queries:open", latest.as_bytes()).unwrap(),
            Operation::OpenLocalQuery { version: None, .. }
        ));
    }
    #[test]
    fn duplicates_unknown_and_wrong_shape_reject() {
        for s in [
            OPEN.replace("\"partition\":1", "\"partition\":1,\"partition\":2"),
            OPEN.replace("\"partition\":1", "\"partition\":1,\"unknown\":2"),
            OPEN.replace("\"version\":\"0\"", "\"version\":0"),
            OPEN.replace("\"version\":\"0\"", "\"version\":\"00\""),
        ] {
            assert!(decode("/preview1/queries:open", s.as_bytes()).is_err());
        }
        assert!(decode("/v1/internal:prepare", b"{}").is_err());
    }
    #[test]
    fn integer_and_tokens_are_strict() {
        assert_eq!(uint("18446744073709551615").unwrap(), u64::MAX);
        assert_eq!(sint("-9223372036854775808").unwrap(), i64::MIN);
        for s in ["+1", "01", "-0", "18446744073709551616"] {
            assert!(uint(s).is_err());
        }
        assert_eq!(unhex::<2>("00ff").unwrap(), [0, 255]);
        assert!(unhex::<2>("00FF").is_err());
    }
    #[test]
    fn typed_output_preserves_values_and_visibility() {
        let value = scalar(&Value::Integer(i64::MAX)).unwrap();
        assert_eq!(value["value"], i64::MAX.to_string());
        assert_ne!(
            scalar(&Value::Null).unwrap(),
            scalar(&Value::Text("null".into())).unwrap()
        );
        let (_, bytes) = encode(&Ok(Reply::Write {
            external_id: [1; 16],
            transaction_id: None,
            status: WriteStatus::Committed {
                ordinal: u64::MAX,
                visible: false,
            },
        }))
        .unwrap();
        let v: Json = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["result"]["status"]["visible"], false);
        assert_eq!(v["result"]["status"]["ordinal"], u64::MAX.to_string());
    }
    #[test]
    fn every_write_variant_and_cross_action_identity() {
        let entity = json!({"partition": 1, "id": "2"});
        let edge = json!({"id": "9", "from": entity, "to": {"partition":2,"id":"3"}, "label":"knows", "properties":{}});
        let patch = json!({"set":{"nullable":{"type":"null"}},"remove":["old"]});
        let conditions =
            json!({"old":{"type":"missing"},"nullable":{"type":"value","value":{"type":"null"}}});
        let mutations = [
            json!({"type":"create_vertex","partition":1,"vertex":{"id":"2","labels":[],"properties":{}}}),
            json!({"type":"create_edge","edge":edge}),
            json!({"type":"patch_vertex","vertex":entity,"patch":patch}),
            json!({"type":"patch_edge","edge":entity,"patch":patch}),
            json!({"type":"patch_vertex_if","vertex":entity,"patch":patch,"conditions":conditions}),
            json!({"type":"patch_edge_if","edge":entity,"patch":patch,"conditions":conditions}),
            json!({"type":"delete_edge","edge":entity}),
            json!({"type":"replace_edge","edge":entity,"replacement":edge}),
        ];
        for mutation in mutations {
            let bytes = serde_json::to_vec(
                &json!({"external_id":"01010101010101010101010101010101","mutation":mutation}),
            )
            .unwrap();
            let Operation::Submit(submit) = decode("/preview1/writes:submit", &bytes).unwrap()
            else {
                panic!("submit");
            };
            let Operation::Status(status) = decode("/preview1/writes:status", &bytes).unwrap()
            else {
                panic!("status");
            };
            let Operation::Abort(abort) = decode("/preview1/writes:abort", &bytes).unwrap() else {
                panic!("abort");
            };
            assert_eq!(submit, status);
            assert_eq!(submit, abort);
        }
    }
    #[test]
    fn duplicate_property_keys_and_extra_scalar_fields_reject() {
        let prefix = r#"{"external_id":"01010101010101010101010101010101","mutation":{"type":"create_vertex","partition":1,"vertex":{"id":"2","labels":[],"properties":{"x":{"type":"null"},"x":{"type":"text","value":"second"}}}}}"#;
        assert_eq!(
            decode("/preview1/writes:submit", prefix.as_bytes()).unwrap_err(),
            Error::Protocol
        );
        let extra = prefix.replace(
            r#""x":{"type":"null"},"x":{"type":"text","value":"second"}"#,
            r#""x":{"type":"null","value":"ignored"}"#,
        );
        assert_eq!(
            decode("/preview1/writes:submit", extra.as_bytes()).unwrap_err(),
            Error::Protocol
        );
    }
    #[test]
    fn size_and_nonfinite_fail_closed() {
        assert_eq!(
            decode("/preview1/queries:open", &vec![b' '; MAX_REQUEST_BYTES + 1]).unwrap_err(),
            Error::Capacity
        );
        assert!(scalar(&Value::Real(f64::NAN)).is_err());
        let r = Reply::FetchLocalQuery {
            session: LocalSession {
                incarnation: [0; 16],
                token: [0; 32],
                version: 0,
            },
            query: Query {
                partition: 1,
                text: String::new(),
            },
            cursor: None,
            page_size: 1,
            page: QueryPage {
                data: QueryData {
                    rows: vec![QueryRow::Value {
                        source: None,
                        value: Value::Text("x".repeat(MAX_RESPONSE_BYTES)),
                    }],
                    completeness: QueryCompleteness::Unverified,
                },
                next: None,
                offset: 0,
                total: 1,
            },
        };
        assert_eq!(encode(&Ok(r)).unwrap_err(), Error::Capacity);
    }
}
