//! Schema-aware allocation-free prescan. Rejects unknown fields, duplicate
//! singular/oneof members and pathological collection overhead before prost.
//! Version 1 is deliberately strict; evolution requires explicit support.
use super::pb;
use prost::{bytes::Buf, Message};
use std::marker::PhantomData;
use tonic::{
    codec::{Codec, DecodeBuf, Decoder},
    Status,
};

pub struct BoundedCodec<T, U>(PhantomData<(T, U)>);
impl<T, U> Default for BoundedCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
pub trait BoundedMessage {
    fn schema() -> Schema;
    fn request() -> bool;
}
pub struct BoundedDecoder<U>(PhantomData<U>);
impl<T, U> Codec for BoundedCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + BoundedMessage + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = tonic_prost::ProstEncoder<T>;
    type Decoder = BoundedDecoder<U>;
    fn encoder(&mut self) -> Self::Encoder {
        Default::default()
    }
    fn decoder(&mut self) -> Self::Decoder {
        BoundedDecoder(PhantomData)
    }
}
impl<U: Message + Default + BoundedMessage> Decoder for BoundedDecoder<U> {
    type Item = U;
    type Error = Status;
    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<U>, Status> {
        decode_checked(buf).map(Some)
    }
}
#[cfg(test)]
thread_local! {static DECODES:std::cell::Cell<usize>=const{std::cell::Cell::new(0)};}
fn decode_checked<U: Message + Default + BoundedMessage, B: Buf>(buf: &mut B) -> Result<U, Status> {
    let request = U::request();
    if buf.chunk().len() != buf.remaining() {
        return Err(bad());
    }
    if buf.remaining() > if request { 64 * 1024 } else { 1024 * 1024 } {
        return Err(limit());
    }
    validate_bytes::<U>(buf.chunk())?;
    #[cfg(test)]
    DECODES.with(|n| n.set(n.get() + 1));
    U::decode(buf).map_err(|_| bad())
}
pub(super) fn validate_bytes<U: BoundedMessage>(bytes: &[u8]) -> Result<(), Status> {
    let request = U::request();
    if bytes.len() > if request { 64 * 1024 } else { 1024 * 1024 } {
        return Err(limit());
    }
    let mut budget = Budget {
        fields: if request { 32768 } else { 262144 },
        messages: if request { 16384 } else { 131072 },
    };
    scan(U::schema(), bytes, 0, &mut budget)
}
fn bad() -> Status {
    Status::invalid_argument("invalid public protobuf message")
}
fn limit() -> Status {
    Status::resource_exhausted("public protobuf structural budget exceeded")
}
#[derive(Clone, Copy)]
pub enum Schema {
    Empty,
    Context,
    Scalar,
    Property,
    Entity,
    Vertex,
    Edge,
    Patch,
    Condition,
    PropertyCondition,
    CreateVertex,
    CreateEdge,
    PatchEntity,
    ConditionalPatch,
    ReplaceEdge,
    Mutation,
    WriteRequest,
    TerminalDecision,
    WriteResponse,
    Query,
    ReadSelector,
    Session,
    Cursor,
    OpenRequest,
    FetchRequest,
    CloseRequest,
    QualifiedElement,
    Path,
    SelectedProperty,
    PropertyMap,
    ValueRow,
    Row,
    Page,
    OpenResponse,
    FetchResponse,
    CloseResponse,
    FixedOpenRequest,
    FixedQueryRequest,
    FixedPageRequest,
    FixedCloseRequest,
    FixedOpenResponse,
    FixedQueryResponse,
    FixedPageResponse,
    FixedCloseResponse,
    QueryData,
    JobHandle,
    JobPolicy,
    JobStatus,
    JobCursor,
    JobPage,
    JobStartRequest,
    JobStartResponse,
    JobGetRequest,
    JobCancelRequest,
    JobStatusResponse,
    JobRenewRequest,
    JobRenewResponse,
    JobFetchRequest,
    JobFetchResponse,
}
#[derive(Clone, Copy)]
enum Kind {
    Var(u64),
    Fixed64,
    Bytes,
    Message(Schema),
}
#[derive(Clone, Copy)]
struct Field {
    kind: Kind,
    repeated: bool,
    oneof: bool,
}
fn field(s: Schema, t: u32) -> Option<Field> {
    use Kind as K;
    use Schema as S;
    if matches!(s, S::JobPage) {
        return if t == 3 {
            Some(Field { kind: K::Message(S::JobCursor), repeated: false, oneof: false })
        } else {
            field(S::Page, t)
        };
    }
    let (kind, repeated, oneof) = match (s, t) {
        (S::JobHandle, 1 | 2) | (S::JobCursor, 1)
        | (S::JobStartRequest, 2) | (S::JobStartResponse, 1) => (K::Bytes, false, false),
        (S::JobPolicy, 1..=3) | (S::JobStatus, 2)
        | (S::JobStartRequest, 3) | (S::JobRenewRequest, 3)
        | (S::JobRenewResponse, 2 | 3) | (S::JobFetchResponse, 5) => (K::Var(u64::MAX), false, false),
        (S::JobStatus, 1) => (K::Var(7), false, false),
        (S::JobStatus, 3) => (K::Var(super::super::pages::MAX_ROWS as u64), false, false),
        (S::JobStatus, 4) => (K::Var(16), false, false),
        (S::JobStartRequest, 1) | (S::JobGetRequest, 1)
        | (S::JobCancelRequest, 1) | (S::JobRenewRequest, 1)
        | (S::JobFetchRequest, 1) => (K::Message(S::Context), false, false),
        (S::JobStartResponse, 2) | (S::JobGetRequest, 2)
        | (S::JobCancelRequest, 2) | (S::JobRenewRequest, 2)
        | (S::JobFetchRequest, 2) | (S::JobStatusResponse, 1)
        | (S::JobRenewResponse, 1) | (S::JobFetchResponse, 1) => (K::Message(S::JobHandle), false, false),
        (S::JobStartRequest, 4) | (S::JobFetchRequest, 3)
        | (S::JobFetchResponse, 2) => (K::Message(S::Query), false, false),
        (S::JobStartRequest, 5) | (S::JobFetchRequest, 5)
        | (S::JobFetchResponse, 4) => (K::Var(u32::MAX.into()), false, false),
        (S::JobStartRequest, 6) => (K::Message(S::JobPolicy), false, false),
        (S::JobStatusResponse, 2) => (K::Message(S::JobStatus), false, false),
        (S::JobFetchRequest, 4) | (S::JobFetchResponse, 3) => (K::Message(S::JobCursor), false, false),
        (S::JobFetchResponse, 6) => (K::Message(S::JobPage), false, false),
        (S::Context, 1) => (K::Var(u32::MAX.into()), false, false),
        (S::Context, 2) => (K::Var(u64::MAX), false, false),
        (S::Scalar, 1) => (K::Message(S::Empty), false, true),
        (S::Scalar, 2) => (K::Var(u64::MAX), false, true),
        (S::Scalar, 3) => (K::Fixed64, false, true),
        (S::Scalar, 4) => (K::Bytes, false, true),
        (S::Scalar, 5) => (K::Var(1), false, true),
        (S::Property, 1) | (S::PropertyCondition, 1) => (K::Bytes, false, false),
        (S::Property, 2) => (K::Message(S::Scalar), false, false),
        (S::PropertyCondition, 2) => (K::Message(S::Condition), false, false),
        (S::Entity, 1) => (K::Var(u32::MAX.into()), false, false),
        (S::Entity, 2) => (K::Var(u64::MAX), false, false),
        (S::Vertex, 1) | (S::Edge, 1) => (K::Var(u64::MAX), false, false),
        (S::Vertex, 2) => (K::Bytes, true, false),
        (S::Vertex, 3) => (K::Message(S::Property), true, false),
        (S::Edge, 2 | 3) => (K::Message(S::Entity), false, false),
        (S::Edge, 4) => (K::Bytes, false, false),
        (S::Edge, 5) => (K::Message(S::Property), true, false),
        (S::Patch, 1) => (K::Message(S::Property), true, false),
        (S::Patch, 2) => (K::Bytes, true, false),
        (S::Condition, 1) => (K::Message(S::Empty), false, true),
        (S::Condition, 2) => (K::Message(S::Scalar), false, true),
        (S::CreateVertex, 1) => (K::Var(u32::MAX.into()), false, false),
        (S::CreateVertex, 2) => (K::Message(S::Vertex), false, false),
        (S::CreateEdge, 1) => (K::Message(S::Edge), false, false),
        (S::PatchEntity, 1) | (S::ConditionalPatch, 1) | (S::ReplaceEdge, 1) => {
            (K::Message(S::Entity), false, false)
        }
        (S::PatchEntity, 2) | (S::ConditionalPatch, 2) => (K::Message(S::Patch), false, false),
        (S::ConditionalPatch, 3) => (K::Message(S::PropertyCondition), true, false),
        (S::ReplaceEdge, 2) => (K::Message(S::Edge), false, false),
        (S::Mutation, 1) => (K::Message(S::CreateVertex), false, true),
        (S::Mutation, 2) => (K::Message(S::CreateEdge), false, true),
        (S::Mutation, 3 | 4) => (K::Message(S::PatchEntity), false, true),
        (S::Mutation, 5 | 6) => (K::Message(S::ConditionalPatch), false, true),
        (S::Mutation, 7) => (K::Message(S::Entity), false, true),
        (S::Mutation, 8) => (K::Message(S::ReplaceEdge), false, true),
        (S::WriteRequest, 1)
        | (S::OpenRequest, 1)
        | (S::FetchRequest, 1)
        | (S::CloseRequest, 1) => (K::Message(S::Context), false, false),
        (S::WriteRequest, 2) => (K::Bytes, false, false),
        (S::WriteRequest, 3) => (K::Message(S::Mutation), false, false),
        (S::TerminalDecision, 1) => (K::Var(u64::MAX), false, false),
        (S::TerminalDecision, 2) => (K::Var(1), false, false),
        (S::WriteResponse, 1 | 2) => (K::Bytes, false, false),
        (S::WriteResponse, 3 | 4) => (K::Message(S::Empty), false, true),
        (S::WriteResponse, 5 | 6) => (K::Message(S::TerminalDecision), false, true),
        (S::Query, 1) => (K::Var(u32::MAX.into()), false, false),
        (S::Query, 2) => (K::Bytes, false, false),
        (S::ReadSelector, 1) => (K::Message(S::Empty), false, true),
        (S::ReadSelector, 2 | 3) => (K::Var(u64::MAX), false, true),
        (S::Session, 1 | 2) | (S::Cursor, 1) => (K::Bytes, false, false),
        (S::Session, 3) => (K::Var(u64::MAX), false, false),
        (S::OpenRequest, 2) => (K::Message(S::ReadSelector), false, false),
        (S::OpenRequest, 3) => (K::Var(u64::MAX), false, false),
        (S::OpenRequest, 4) | (S::FetchRequest, 3) => (K::Message(S::Query), false, false),
        (S::OpenRequest, 5) | (S::FetchRequest, 5) => (K::Var(u32::MAX.into()), false, false),
        (S::FetchRequest, 2) | (S::CloseRequest, 2) => (K::Message(S::Session), false, false),
        (S::FetchRequest, 4) => (K::Message(S::Cursor), false, false),
        (S::QualifiedElement, 1 | 2) => (K::Message(S::Entity), false, true),
        (S::Path, 1) => (K::Message(S::QualifiedElement), true, false),
        (S::SelectedProperty, 1) => (K::Bytes, false, false),
        (S::SelectedProperty, 2) => (K::Message(S::Scalar), true, false),
        (S::PropertyMap, 1) | (S::ValueRow, 1) => (K::Message(S::QualifiedElement), false, false),
        (S::PropertyMap, 2) => (K::Message(S::SelectedProperty), true, false),
        (S::ValueRow, 2) => (K::Message(S::Scalar), false, false),
        (S::Row, 1) => (K::Message(S::QualifiedElement), false, true),
        (S::Row, 2) => (K::Message(S::Path), false, true),
        (S::Row, 3) => (K::Var(u64::MAX), false, true),
        (S::Row, 4) => (K::Message(S::PropertyMap), false, true),
        (S::Row, 5) => (K::Message(S::ValueRow), false, true),
        (S::Page, 1) => (K::Message(S::Row), true, false),
        (S::Page, 2) => (K::Var(2), false, false),
        (S::Page, 3) => (K::Message(S::Cursor), false, false),
        (S::Page, 4 | 5) => (K::Var(u32::MAX.into()), false, false),
        (S::OpenResponse, 1) | (S::FetchResponse, 1) | (S::CloseResponse, 1) => {
            (K::Message(S::Session), false, false)
        }
        (S::OpenResponse, 2) | (S::FetchResponse, 2) => (K::Message(S::Query), false, false),
        (S::OpenResponse, 3) | (S::FetchResponse, 4) => (K::Var(u32::MAX.into()), false, false),
        (S::OpenResponse, 4) | (S::FetchResponse, 5) => (K::Message(S::Page), false, false),
        (S::FetchResponse, 3) => (K::Message(S::Cursor), false, false),
        (S::FixedOpenRequest, 1)
        | (S::FixedQueryRequest, 1)
        | (S::FixedPageRequest, 1)
        | (S::FixedCloseRequest, 1) => (K::Message(S::Context), false, false),
        (S::FixedOpenRequest, 2 | 3) => (K::Var(u64::MAX), false, false),
        (S::FixedQueryRequest, 2)
        | (S::FixedPageRequest, 2)
        | (S::FixedCloseRequest, 2)
        | (S::FixedOpenResponse, 1)
        | (S::FixedQueryResponse, 1)
        | (S::FixedPageResponse, 1)
        | (S::FixedCloseResponse, 1) => (K::Message(S::Session), false, false),
        (S::FixedQueryRequest, 3)
        | (S::FixedPageRequest, 3)
        | (S::FixedQueryResponse, 2)
        | (S::FixedPageResponse, 2) => (K::Message(S::Query), false, false),
        (S::FixedPageRequest, 4) | (S::FixedPageResponse, 3) => {
            (K::Message(S::Cursor), false, false)
        }
        (S::FixedPageRequest, 5) | (S::FixedPageResponse, 4) => {
            (K::Var(u32::MAX.into()), false, false)
        }
        (S::FixedPageResponse, 5) => (K::Message(S::Page), false, false),
        (S::FixedQueryResponse, 3) => (K::Message(S::QueryData), false, false),
        (S::QueryData, 1) => (K::Message(S::Row), true, false),
        (S::QueryData, 2) => (K::Var(2), false, false),
        _ => return None,
    };
    Some(Field {
        kind,
        repeated,
        oneof,
    })
}
struct Budget {
    fields: usize,
    messages: usize,
}
fn var(bytes: &[u8], at: &mut usize) -> Result<u64, Status> {
    let mut value = 0u64;
    for i in 0..10 {
        let b = *bytes.get(*at).ok_or_else(bad)?;
        *at += 1;
        if i == 9 && b > 1 {
            return Err(bad());
        }
        value |= u64::from(b & 127) << (i * 7);
        if b < 128 {
            return Ok(value);
        }
    }
    Err(bad())
}
fn scan(schema: Schema, bytes: &[u8], depth: usize, budget: &mut Budget) -> Result<(), Status> {
    if depth > 16 {
        return Err(limit());
    }
    budget.messages = budget.messages.checked_sub(1).ok_or_else(limit)?;
    let mut at = 0;
    let mut seen = 0u64;
    let mut oneof = false;
    let mut job_phase = 0;
    while at < bytes.len() {
        budget.fields = budget.fields.checked_sub(1).ok_or_else(limit)?;
        let key = var(bytes, &mut at)?;
        let tag = u32::try_from(key >> 3).map_err(|_| bad())?;
        let wire = key & 7;
        let field = field(schema, tag).ok_or_else(bad)?;
        let bit = 1u64.checked_shl(tag).ok_or_else(bad)?;
        if !field.repeated && seen & bit != 0 {
            return Err(bad());
        }
        seen |= bit;
        if field.oneof {
            if oneof {
                return Err(bad());
            }
            oneof = true;
        }
        match field.kind {
            Kind::Var(max) => {
                if wire != 0 {
                    return Err(bad());
                }
                let value = var(bytes, &mut at)?;
                if value > max || (matches!(schema, Schema::JobStatus) && matches!(tag, 1 | 4) && value == 0) {
                    return Err(bad());
                }
                if matches!(schema, Schema::JobStatus) && tag == 1 {
                    job_phase = value;
                }
            }
            Kind::Fixed64 => {
                if wire != 1 {
                    return Err(bad());
                }
                at = at
                    .checked_add(8)
                    .filter(|n| *n <= bytes.len())
                    .ok_or_else(bad)?;
            }
            Kind::Bytes | Kind::Message(_) => {
                if wire != 2 {
                    return Err(bad());
                }
                let len = usize::try_from(var(bytes, &mut at)?).map_err(|_| bad())?;
                let end = at
                    .checked_add(len)
                    .filter(|n| *n <= bytes.len())
                    .ok_or_else(bad)?;
                let exact_width = match (schema, tag) {
                    (Schema::JobHandle, 1) | (Schema::JobStartRequest, 2)
                    | (Schema::JobStartResponse, 1) => Some(16),
                    (Schema::JobHandle, 2) | (Schema::JobCursor, 1) => Some(32),
                    _ => None,
                };
                if exact_width.is_some_and(|width| len != width) {
                    return Err(bad());
                }
                if let Kind::Message(child) = field.kind {
                    scan(child, &bytes[at..end], depth + 1, budget)?;
                }
                at = end;
            }
        }
    }
    let required = match schema {
        Schema::JobHandle | Schema::JobStartResponse | Schema::JobGetRequest
        | Schema::JobCancelRequest | Schema::JobStatusResponse | Schema::JobRenewRequest => (1 << 1) | (1 << 2),
        Schema::JobCursor | Schema::JobRenewResponse => 1 << 1,
        Schema::JobStatus => 1 << 1,
        Schema::JobStartRequest => (1 << 1) | (1 << 2) | (1 << 4) | (1 << 6),
        Schema::JobFetchRequest => (1 << 1) | (1 << 2) | (1 << 3),
        Schema::JobFetchResponse => (1 << 1) | (1 << 2) | (1 << 6),
        _ => 0,
    };
    if seen & required != required {
        return Err(bad());
    }
    if matches!(schema, Schema::JobStatus) {
        let version_and_total = (1 << 2) | (1 << 3);
        let failure = 1 << 4;
        let valid = match job_phase {
            1..=3 => seen & (version_and_total | failure) == 0,
            4 => seen & version_and_total == version_and_total && seen & failure == 0,
            5..=7 => seen & version_and_total == 0,
            _ => false,
        };
        if !valid {
            return Err(bad());
        }
    }
    if matches!(
        schema,
        Schema::Scalar
            | Schema::Condition
            | Schema::Mutation
            | Schema::ReadSelector
            | Schema::QualifiedElement
            | Schema::Row
            | Schema::WriteResponse
    ) && !oneof
    {
        return Err(bad());
    }
    Ok(())
}
macro_rules! message {
    ($t:ident,$req:expr) => {
        impl BoundedMessage for pb::$t {
            fn schema() -> Schema {
                Schema::$t
            }
            fn request() -> bool {
                $req
            }
        }
    };
}
message!(WriteRequest, true);
message!(OpenRequest, true);
message!(FetchRequest, true);
message!(CloseRequest, true);
message!(WriteResponse, false);
message!(OpenResponse, false);
message!(FetchResponse, false);
message!(CloseResponse, false);
message!(FixedOpenRequest, true);
message!(FixedQueryRequest, true);
message!(FixedPageRequest, true);
message!(FixedCloseRequest, true);
message!(FixedOpenResponse, false);
message!(FixedQueryResponse, false);
message!(FixedPageResponse, false);
message!(FixedCloseResponse, false);
message!(JobStartRequest, true);
message!(JobGetRequest, true);
message!(JobCancelRequest, true);
message!(JobRenewRequest, true);
message!(JobFetchRequest, true);
message!(JobStartResponse, false);
message!(JobStatusResponse, false);
message!(JobRenewResponse, false);
message!(JobFetchResponse, false);

#[cfg(test)]
mod tests {
    use super::*;
    fn job_handle() -> pb::JobHandle {
        pb::JobHandle { incarnation: vec![1; 16], token: vec![2; 32] }
    }
    fn job_start() -> pb::JobStartRequest {
        pb::JobStartRequest {
            context: Some(pb::Context { version: 1, timeout_millis: 50 }),
            operation_id: vec![3; 16],
            version: Some(0),
            query: Some(pb::Query { partition: 7, text: "g.V(1)".into() }),
            page_size: 1,
            policy: Some(pb::JobPolicy { idle_millis: 1000, execution_millis: 1000, lifetime_millis: 2000 }),
        }
    }
    fn checked<T: BoundedMessage + Message + Default + std::fmt::Debug + PartialEq>(message: T) {
        let bytes = message.encode_to_vec();
        assert_eq!(decode_checked::<T, _>(&mut bytes.as_slice()).unwrap(), message);
        // Unknown fields and duplicate required messages must fail before prost.
        let mut unknown = bytes;
        unknown.extend_from_slice(&[120, 1]);
        let before = DECODES.with(|n| n.get());
        assert!(decode_checked::<T, _>(&mut unknown.as_slice()).is_err());
        assert_eq!(DECODES.with(|n| n.get()), before);
    }
    fn rejected<T: BoundedMessage + Message + Default>(message: T) {
        let bytes = message.encode_to_vec();
        let before = DECODES.with(|n| n.get());
        assert!(decode_checked::<T, _>(&mut bytes.as_slice()).is_err());
        assert_eq!(DECODES.with(|n| n.get()), before);
    }
    #[test]
    fn job_messages_roundtrip_and_reject_unknown_before_prost() {
        for version in [None, Some(0), Some(u64::MAX)] {
            let mut start = job_start();
            start.version = version;
            checked(start);
        }
        checked(pb::JobStartResponse { operation_id: vec![3; 16], job: Some(job_handle()) });
        checked(pb::JobGetRequest { context: job_start().context, job: Some(job_handle()) });
        checked(pb::JobCancelRequest { context: job_start().context, job: Some(job_handle()) });
        checked(pb::JobRenewRequest { context: job_start().context, job: Some(job_handle()), expected_generation: u64::MAX });
        checked(pb::JobRenewResponse { job: Some(job_handle()), generation: u64::MAX, remaining_millis: 1 });
        for cursor in [None, Some(pb::Cursor { token: vec![4; 32] })] {
            checked(pb::JobFetchRequest { context: job_start().context, job: Some(job_handle()), query: job_start().query, cursor: cursor.clone(), page_size: 1 });
            checked(pb::JobFetchResponse { job: Some(job_handle()), query: job_start().query, cursor, page_size: 1, version: 0,
                page: Some(pb::Page { rows: vec![], completeness: 1, next: None, offset: 0, total: 0 }) });
        }
        for phase in 1..=7 {
            let ready = phase == 4;
            checked(pb::JobStatusResponse { job: Some(job_handle()), status: Some(pb::JobStatus { phase, version: ready.then_some(0), total: ready.then_some(0), failure: None }) });
        }
        for failure in 1..=16 {
            checked(pb::JobStatusResponse { job: Some(job_handle()), status: Some(pb::JobStatus { phase: 7, version: None, total: None, failure: Some(failure) }) });
        }
    }
    #[test]
    fn job_required_fields_tokens_and_duplicates_fail_before_prost() {
        let mut start = job_start();
        start.policy = None;
        rejected(start);
        let mut start = job_start();
        start.query = None;
        rejected(start);
        let mut start = job_start();
        start.context = None;
        rejected(start);
        for len in [0, 15, 17] {
            let mut start = job_start();
            start.operation_id = vec![3; len];
            rejected(start);
        }
        for len in [0, 31, 33] {
            let mut handle = job_handle();
            handle.token = vec![2; len];
            rejected(pb::JobGetRequest { context: job_start().context, job: Some(handle) });
        }
        for len in [0, 15, 17] {
            let mut handle = job_handle();
            handle.incarnation = vec![1; len];
            rejected(pb::JobGetRequest { context: job_start().context, job: Some(handle) });
        }
        rejected(pb::JobGetRequest { context: job_start().context, job: None });
        rejected(pb::JobStatusResponse { job: Some(job_handle()), status: None });
        rejected(pb::JobFetchRequest { context: job_start().context, job: Some(job_handle()), query: job_start().query, cursor: Some(pb::Cursor { token: vec![] }), page_size: 1 });
        rejected(pb::JobFetchResponse { job: Some(job_handle()), query: job_start().query, cursor: None, page_size: 1, version: 0,
            page: Some(pb::Page { rows: vec![], completeness: 1, next: Some(pb::Cursor { token: vec![] }), offset: 0, total: 0 }) });
        let mut bytes = job_start().encode_to_vec();
        bytes.extend_from_slice(&[24, 0]); // repeated optional exact V=0
        let before = DECODES.with(|n| n.get());
        assert!(decode_checked::<pb::JobStartRequest, _>(&mut bytes.as_slice()).is_err());
        assert_eq!(DECODES.with(|n| n.get()), before);
    }
    #[test]
    fn job_status_invalid_combinations_fail_before_prost() {
        for status in [
            pb::JobStatus { phase: 0, version: None, total: None, failure: None },
            pb::JobStatus { phase: 8, version: None, total: None, failure: None },
            pb::JobStatus { phase: 4, version: None, total: Some(0), failure: None },
            pb::JobStatus { phase: 4, version: Some(0), total: None, failure: None },
            pb::JobStatus { phase: 4, version: Some(0), total: Some(0), failure: Some(1) },
            pb::JobStatus { phase: 4, version: Some(0), total: Some(10001), failure: None },
            pb::JobStatus { phase: 2, version: Some(0), total: None, failure: None },
            pb::JobStatus { phase: 3, version: None, total: None, failure: Some(1) },
            pb::JobStatus { phase: 7, version: Some(0), total: None, failure: Some(1) },
            pb::JobStatus { phase: 7, version: None, total: None, failure: Some(0) },
            pb::JobStatus { phase: 7, version: None, total: None, failure: Some(17) },
        ] {
            rejected(pb::JobStatusResponse { job: Some(job_handle()), status: Some(status) });
        }
    }
    #[test]
    fn fixed_requests_are_prescanned_and_preserve_optional_zero() {
        for bytes in [vec![10, 0, 10, 0], vec![120, 1], vec![16, 0, 16, 1]] {
            let before = DECODES.with(|n| n.get());
            assert!(decode_checked::<pb::FixedOpenRequest, _>(&mut bytes.as_slice()).is_err());
            assert_eq!(DECODES.with(|n| n.get()), before);
        }
        for version in [None, Some(0), Some(u64::MAX)] {
            let r = pb::FixedOpenRequest {
                context: Some(pb::Context {
                    version: 1,
                    timeout_millis: 50,
                }),
                version,
                ttl_millis: 500,
            };
            let bytes = r.encode_to_vec();
            assert_eq!(
                decode_checked::<pb::FixedOpenRequest, _>(&mut bytes.as_slice()).unwrap(),
                r
            );
        }
        let bytes = [18, 4, 10, 0, 10, 0]; // duplicate incarnation inside fixed session
        let before = DECODES.with(|n| n.get());
        assert!(decode_checked::<pb::FixedQueryRequest, _>(&mut bytes.as_slice()).is_err());
        assert_eq!(DECODES.with(|n| n.get()), before);
    }
    #[test]
    fn duplicate_unknown_and_oneof_fail_before_prost() {
        for bytes in [vec![10, 0, 10, 0], vec![120, 1], vec![18, 4, 10, 0, 16, 0]] {
            let before = DECODES.with(|n| n.get());
            assert!(decode_checked::<pb::OpenRequest, _>(&mut bytes.as_slice()).is_err());
            assert_eq!(DECODES.with(|n| n.get()), before);
        }
    }
    #[test]
    fn valid_exact_zero_roundtrip() {
        let r = pb::OpenRequest {
            context: Some(pb::Context {
                version: 1,
                timeout_millis: 50,
            }),
            read: Some(pb::ReadSelector {
                kind: Some(pb::read_selector::Kind::Exact(0)),
            }),
            ttl_millis: 500,
            query: Some(pb::Query {
                partition: 7,
                text: "g.V(1)".into(),
            }),
            page_size: 1,
        };
        let b = r.encode_to_vec();
        assert_eq!(
            decode_checked::<pb::OpenRequest, _>(&mut b.as_slice()).unwrap(),
            r
        );
    }
    #[test]
    fn byte_limit_before_prost() {
        let bytes = vec![0; 65537];
        let before = DECODES.with(|n| n.get());
        assert_eq!(
            decode_checked::<pb::WriteRequest, _>(&mut bytes.as_slice())
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(DECODES.with(|n| n.get()), before);
    }
    #[test]
    fn structural_budget_before_allocation() {
        // A legal repeated empty label has nonzero Vec/String overhead. Reject
        // the field-count attack without allocating one Rust String per label.
        let body = [18u8, 0].repeat(32768);
        let mut budget = Budget {
            fields: 100,
            messages: 100,
        };
        assert_eq!(
            scan(Schema::Vertex, &body, 0, &mut budget)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
    }
}
