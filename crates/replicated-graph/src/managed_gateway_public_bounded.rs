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
            Some(Field {
                kind: K::Message(S::JobCursor),
                repeated: false,
                oneof: false,
            })
        } else {
            field(S::Page, t)
        };
    }
    let (kind, repeated, oneof) = match (s, t) {
        (S::JobHandle, 1 | 2)
        | (S::JobCursor, 1)
        | (S::JobStartRequest, 2)
        | (S::JobStartResponse, 1) => (K::Bytes, false, false),
        (S::JobPolicy, 1..=3)
        | (S::JobStatus, 2)
        | (S::JobStartRequest, 3)
        | (S::JobRenewRequest, 3)
        | (S::JobRenewResponse, 2 | 3)
        | (S::JobFetchResponse, 5) => (K::Var(u64::MAX), false, false),
        (S::JobStatus, 1) => (K::Var(7), false, false),
        (S::JobStatus, 3) => (K::Var(super::super::pages::MAX_ROWS as u64), false, false),
        (S::JobStatus, 4) => (K::Var(16), false, false),
        (S::JobStartRequest, 1)
        | (S::JobGetRequest, 1)
        | (S::JobCancelRequest, 1)
        | (S::JobRenewRequest, 1)
        | (S::JobFetchRequest, 1) => (K::Message(S::Context), false, false),
        (S::JobStartResponse, 2)
        | (S::JobGetRequest, 2)
        | (S::JobCancelRequest, 2)
        | (S::JobRenewRequest, 2)
        | (S::JobFetchRequest, 2)
        | (S::JobStatusResponse, 1)
        | (S::JobRenewResponse, 1)
        | (S::JobFetchResponse, 1) => (K::Message(S::JobHandle), false, false),
        (S::JobStartRequest, 4) | (S::JobFetchRequest, 3) | (S::JobFetchResponse, 2) => {
            (K::Message(S::Query), false, false)
        }
        (S::JobStartRequest, 5) | (S::JobFetchRequest, 5) | (S::JobFetchResponse, 4) => {
            (K::Var(u32::MAX.into()), false, false)
        }
        (S::JobStartRequest, 6) => (K::Message(S::JobPolicy), false, false),
        (S::JobStatusResponse, 2) => (K::Message(S::JobStatus), false, false),
        (S::JobFetchRequest, 4) | (S::JobFetchResponse, 3) => {
            (K::Message(S::JobCursor), false, false)
        }
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
        (S::ReadSelector, 1) => (K::Message(S::Em…30110 tokens truncated…ifecycle tests on current main. This avoids competing edits to shared public
   proto/generated/handler files while preserving a single endpoint.
3. Record exact candidate evidence and update this document plus public transport
   inventories. Only then assess legacy retirement in its own verified slice;
   do not remove unconverted operations or declare #218 complete.

### Typed lifecycle integration evidence

`grpc_job_process.rs` exercises the public typed client against independent
service and managed node processes using a fixture-owned qualified-path oracle.
It covers fixed results across later deletion, Start argument replay, failure
precedence, immutable pages, lease replay, expiration without implicit renewal,
shared-slot exhaustion, incarnation fencing and dropped control replies.
The existing deterministic lifecycle tests remain the evidence for running
renewal races and last-owner resource retention. This candidate must pass
independent review and exact-head CI before #268 acceptance.
