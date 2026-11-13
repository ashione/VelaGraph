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
    let (kind, repeated, oneof) = match (s, t) {
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
        (S::FixedOpenRequest, 1) | (S::FixedQueryRequest, 1)
        | (S::FixedPageRequest, 1) | (S::FixedCloseRequest, 1) => (K::Message(S::Context), false, false),
        (S::FixedOpenRequest, 2 | 3) => (K::Var(u64::MAX), false, false),
        (S::FixedQueryRequest, 2) | (S::FixedPageRequest, 2) | (S::FixedCloseRequest, 2)
        | (S::FixedOpenResponse, 1) | (S::FixedQueryResponse, 1)
        | (S::FixedPageResponse, 1) | (S::FixedCloseResponse, 1) => (K::Message(S::Session), false, false),
        (S::FixedQueryRequest, 3) | (S::FixedPageRequest, 3)
        | (S::FixedQueryResponse, 2) | (S::FixedPageResponse, 2) => (K::Message(S::Query), false, false),
        (S::FixedPageRequest, 4) | (S::FixedPageResponse, 3) => (K::Message(S::Cursor), false, false),
        (S::FixedPageRequest, 5) | (S::FixedPageResponse, 4) => (K::Var(u32::MAX.into()), false, false),
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
                if var(bytes, &mut at)? > max {
                    return Err(bad());
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
                if let Kind::Message(child) = field.kind {
                    scan(child, &bytes[at..end], depth + 1, budget)?;
                }
                at = end;
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_requests_are_prescanned_and_preserve_optional_zero() {
        for bytes in [vec![10, 0, 10, 0], vec![120, 1], vec![16, 0, 16, 1]] {
            let before = DECODES.with(|n| n.get());
            assert!(decode_checked::<pb::FixedOpenRequest, _>(&mut bytes.as_slice()).is_err());
            assert_eq!(DECODES.with(|n| n.get()), before);
        }
        for version in [None, Some(0), Some(u64::MAX)] {
            let r = pb::FixedOpenRequest { context: Some(pb::Context { version: 1, timeout_millis: 50 }), version, ttl_millis: 500 };
            let bytes = r.encode_to_vec();
            assert_eq!(decode_checked::<pb::FixedOpenRequest, _>(&mut bytes.as_slice()).unwrap(), r);
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
