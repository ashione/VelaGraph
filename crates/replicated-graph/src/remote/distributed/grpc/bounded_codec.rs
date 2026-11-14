//! Allocation-free schema prescan before prost allocates repeated messages.
//! Unknown fields are skipped; duplicate singular fields and protobuf groups are
//! rejected deliberately (no merge semantics across independently bounded parts).
use super::wire;
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
thread_local! {static PROST_DECODES:std::cell::Cell<usize>=const{std::cell::Cell::new(0)};}
fn decode_checked<U: Message + Default + BoundedMessage, B: Buf>(buf: &mut B) -> Result<U, Status> {
    // tonic 0.14 DecodeBuf wraps contiguous BytesMut. Fail closed if that changes.
    if buf.chunk().len() != buf.remaining() {
        return Err(bad());
    }
    let schema = U::schema();
    if matches!(schema, Schema::FixedFragmentResponse | Schema::Empty)
        && buf.remaining() > crate::remote::query::MAX_QUERY_RESPONSE_BYTES + 1024
    {
        return Err(limit());
    }
    let ingress_limit = match schema {
        Schema::SingleQueryRequest
        | Schema::CoordinatorQueryRequest
        | Schema::CoordinatorJobRequest => Some(crate::remote::query::MAX_QUERY_TEXT_BYTES + 1024),
        Schema::SingleQueryResponse
        | Schema::CoordinatorQueryResponse
        | Schema::CoordinatorJobResponse => Some(super::super::MAX_WIRE),
        _ => None,
    };
    if ingress_limit.is_some_and(|max| buf.remaining() > max) {
        return Err(limit());
    }
    scan(schema, buf.chunk(), 0, 0)?;
    #[cfg(test)]
    PROST_DECODES.with(|n| n.set(n.get() + 1));
    U::decode(buf).map_err(|_| bad())
}
fn bad() -> Status {
    Status::invalid_argument("invalid bounded protobuf message")
}
fn limit() -> Status {
    Status::resource_exhausted("protobuf collection limit exceeded")
}
#[derive(Clone, Copy)]
pub enum Schema {
    SingleQueryRequest,
    SingleQueryOutput,
    SingleQueryResponse,
    CoordinatorValue,
    CoordinatorQueryRequest,
    QualifiedElement,
    QueryPath,
    PropertyValues,
    PropertyMap,
    ScalarValue,
    QueryRow,
    CoordinatorQueryOutput,
    CoordinatorQueryResponse,
    JobStart,
    JobStartRows,
    JobRead,
    JobAdvance,
    JobCancel,
    CoordinatorJobRequest,
    JobProjectedPage,
    JobPreparing,
    JobPage,
    JobProgress,
    CoordinatorJobOutput,
    CoordinatorJobResponse,

    Empty,
    Identity,
    Context,
    QueryStamp,
    Cost,
    VertexId,
    EdgeHop,
    IncomingEdge,
    Value,
    Filter,
    Labels,
    ScanStep,
    ScanRequest,
    ScanResult,
    HierarchyBatch,
    ResolveIncoming,
    Has,
    HasLabel,
    Values,
    Project,
    Fragment,
    BatchValues,
    Edges,
    Incoming,
    OptionalValue,
    Property,
    Projected,
    FragmentValue,
    Target,
    Authority,
    Member,
    Cohort,
    Participant,
    TerminalRecord,
    TerminalSegment,
    TerminalPrefix,
    ReadLimit,
    FixedOpenRequest,
    RenewableFixedOpenRequest,
    FixedDescriptor,
    CaptureCost,
    FixedOpened,
    RenewableFixedOpened,
    FixedRenewRequest,
    FixedLeaseGrant,
    FixedFragmentRequest,
    FixedFragmentOutput,
    FixedOpenEnvelope,
    RenewableOpenEnvelope,
    FixedRenewEnvelope,
    FixedFragmentEnvelope,
    FixedCloseEnvelope,
    FixedFragmentResponse,
}
#[derive(Clone, Copy)]
enum Kind {
    Varint,
    Fixed64,
    Bytes(Option<usize>),
    Child(Schema),
}
#[derive(Clone, Copy)]
struct Field {
    tag: u32,
    kind: Kind,
    max: usize,
    repeated: bool,
}
// Match the existing serde proof visitor's profile-derived segment bound.
const MAX_TERMINAL_SEGMENTS: usize =
    d2_raft::transaction_decisions::CapacityProfile::ByteBound16MiBV1
        .max_transactions()
        .div_ceil(d2_raft::terminal_prefix::SEGMENT_TRANSACTIONS);
#[rustfmt::skip]
fn fields(s: Schema) -> &'static [Field] {
 match s {
 Schema::SingleQueryRequest => &[Field {tag:1,kind:Kind::Child(Schema::Context),max:1,repeated:false}, Field {tag:2,kind:Kind::Varint,max:1,repeated:false}, Field {tag:3,kind:Kind::Varint,max:1,repeated:false}, Field {tag:4,kind:Kind::Varint,max:1,repeated:false}, Field {tag:5,kind:Kind::Bytes(None),max:1,repeated:false}],
 Schema::SingleQueryOutput => &[Field {tag:1,kind:Kind::Varint,max:1,repeated:false},Field {tag:2,kind:Kind::Varint,max:1,repeated:false},Field {tag:3,kind:Kind::Varint,max:1,repeated:false},Field {tag:4,kind:Kind::Child(Schema::QueryStamp),max:1,repeated:false},Field {tag:5,kind:Kind::Child(Schema::QueryRow),max:crate::remote::query::MAX_QUERY_ROWS,repeated:true}],
 Schema::SingleQueryResponse => &[Field {tag:1,kind:Kind::Child(Schema::SingleQueryOutput),max:1,repeated:false},Field {tag:2,kind:Kind::Varint,max:1,repeated:false}],
 Schema::CoordinatorQueryRequest => &[Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 4, kind: Kind::Bytes(None), max: 1, repeated: false }, Field { tag: 5, kind: Kind::Varint, max: 1, repeated: false }],
 Schema::QualifiedElement => &[Field { tag: 1, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false }],
 Schema::QueryPath => &[Field { tag: 1, kind: Kind::Child(Schema::QualifiedElement), max: super::super::MAX_WIRE, repeated: true }],
 Schema::PropertyValues => &[Field { tag: 1, kind: Kind::Bytes(None), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Child(Schema::CoordinatorValue), max: 1, repeated: true }],
 Schema::PropertyMap => &[Field { tag: 1, kind: Kind::Child(Schema::QualifiedElement), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Child(Schema::PropertyValues), max: 8, repeated: true }],
 Schema::ScalarValue => &[Field { tag: 1, kind: Kind::Child(Schema::QualifiedElement), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Child(Schema::CoordinatorValue), max: 1, repeated: false }],
 Schema::QueryRow => &[Field { tag: 1, kind: Kind::Child(Schema::QualifiedElement), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Child(Schema::QueryPath), max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 4, kind: Kind::Child(Schema::PropertyMap), max: 1, repeated: false }, Field { tag: 5, kind: Kind::Child(Schema::ScalarValue), max: 1, repeated: false }],
 Schema::CoordinatorQueryOutput => &[Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 2, kind: Kind::Child(Schema::QueryStamp), max: 8, repeated: true }, Field { tag: 3, kind: Kind::Child(Schema::QueryRow), max: super::super::MAX_ROWS, repeated: true }],
 Schema::CoordinatorQueryResponse => &[Field { tag: 1, kind: Kind::Child(Schema::CoordinatorQueryOutput), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }],
 Schema::JobStart => &[Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 2, kind: Kind::Bytes(None), max: 1, repeated: false }],
 Schema::JobStartRows => &[Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 2, kind: Kind::Bytes(None), max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }],
 Schema::JobRead => &[Field { tag: 1, kind: Kind::Bytes(Some(16)), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }],
 Schema::JobAdvance => &[Field { tag: 1, kind: Kind::Bytes(Some(16)), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }],
 Schema::JobCancel => &[Field { tag: 1, kind: Kind::Bytes(Some(16)), max: 1, repeated: false }],
 Schema::CoordinatorJobRequest => &[Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 3, kind: Kind::Child(Schema::JobStart), max: 1, repeated: false }, Field { tag: 4, kind: Kind::Child(Schema::JobStartRows), max: 1, repeated: false }, Field { tag: 5, kind: Kind::Child(Schema::JobRead), max: 1, repeated: false }, Field { tag: 6, kind: Kind::Child(Schema::JobAdvance), max: 1, repeated: false }, Field { tag: 7, kind: Kind::Child(Schema::JobCancel), max: 1, repeated: false }, Field { tag: 8, kind: Kind::Varint, max: 1, repeated: false }],
 Schema::JobProjectedPage => &[Field { tag: 1, kind: Kind::Bytes(Some(16)), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 4, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 5, kind: Kind::Child(Schema::PropertyMap), max: 128, repeated: true }, Field { tag: 6, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 7, kind: Kind::Child(Schema::QueryStamp), max: 8, repeated: true }],
 Schema::JobPreparing => &[Field { tag: 1, kind: Kind::Bytes(Some(16)), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 4, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 5, kind: Kind::Child(Schema::QueryStamp), max: 8, repeated: true }],
 Schema::JobPage => &[Field { tag: 1, kind: Kind::Bytes(Some(16)), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 4, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 5, kind: Kind::Child(Schema::QualifiedElement), max: 1000, repeated: true }, Field { tag: 6, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 7, kind: Kind::Child(Schema::QueryStamp), max: 8, repeated: true }],
 Schema::JobProgress => &[Field { tag: 1, kind: Kind::Bytes(Some(16)), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 4, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 5, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 6, kind: Kind::Child(Schema::QueryStamp), max: 8, repeated: true }],
 Schema::CoordinatorJobOutput => &[Field { tag: 1, kind: Kind::Child(Schema::JobProjectedPage), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Child(Schema::JobPreparing), max: 1, repeated: false }, Field { tag: 3, kind: Kind::Child(Schema::JobPage), max: 1, repeated: false }, Field { tag: 4, kind: Kind::Child(Schema::JobProgress), max: 1, repeated: false }, Field { tag: 5, kind: Kind::Child(Schema::Empty), max: 1, repeated: false }],
 Schema::CoordinatorJobResponse => &[Field { tag: 1, kind: Kind::Child(Schema::CoordinatorJobOutput), max: 1, repeated: false }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false }],

 Schema::Empty => &[],
 Schema::Identity => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::Context => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::Identity), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::QueryStamp => &[ Field { tag: 1, kind: Kind::Child(Schema::Identity), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::Cost => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::VertexId => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::EdgeHop => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Bytes(None), max: 1, repeated: false, }, ],
 Schema::IncomingEdge => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Bytes(None), max: 1, repeated: false, }, ],
 Schema::Value | Schema::CoordinatorValue => &[ Field { tag: 1, kind: Kind::Child(Schema::Empty), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Fixed64, max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Bytes(None), max: 1, repeated: false, }, Field { tag: 5, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::Filter => &[ Field { tag: 1, kind: Kind::Bytes(None), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::Value), max: 1, repeated: false, }, ],
 Schema::Labels => &[Field { tag: 1, kind: Kind::Bytes(None), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }],
 Schema::ScanStep => &[ Field { tag: 1, kind: Kind::Child(Schema::Filter), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::Labels), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::ScanRequest => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::ScanStep), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }, Field { tag: 4, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::ScanResult => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Varint, max: crate::remote::MAX_FRAME_BYTES, repeated: true, }, Field { tag: 5, kind: Kind::Varint, max: crate::remote::MAX_FRAME_BYTES, repeated: true, }, ],
 Schema::HierarchyBatch => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::Fragment), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }, ],
 Schema::ResolveIncoming => &[ Field { tag: 1, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::IncomingEdge), max: 1, repeated: false, }, ],
 Schema::Has => &[ Field { tag: 1, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Bytes(None), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::Value), max: 1, repeated: false, }, ],
 Schema::HasLabel => &[ Field { tag: 1, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Bytes(None), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }, ],
 Schema::Values => &[ Field { tag: 1, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Bytes(None), max: 1, repeated: false, }, ],
 Schema::Project => &[ Field { tag: 1, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Bytes(None), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }, ],
 Schema::Fragment => &[ Field { tag: 1, kind: Kind::Child(Schema::HierarchyBatch), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::ScanRequest), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 5, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 6, kind: Kind::Child(Schema::ResolveIncoming), max: 1, repeated: false, }, Field { tag: 7, kind: Kind::Child(Schema::EdgeHop), max: 1, repeated: false, }, Field { tag: 8, kind: Kind::Child(Schema::Has), max: 1, repeated: false, }, Field { tag: 9, kind: Kind::Child(Schema::HasLabel), max: 1, repeated: false, }, Field { tag: 10, kind: Kind::Child(Schema::Values), max: 1, repeated: false, }, Field { tag: 11, kind: Kind::Child(Schema::Project), max: 1, repeated: false, }, ],
 Schema::BatchValues => &[Field { tag: 1, kind: Kind::Child(Schema::FragmentValue), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }],
 Schema::Edges => &[Field { tag: 1, kind: Kind::Child(Schema::EdgeHop), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }],
 Schema::Incoming => &[Field { tag: 1, kind: Kind::Child(Schema::IncomingEdge), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }],
 Schema::OptionalValue => &[Field { tag: 1, kind: Kind::Child(Schema::Value), max: 1, repeated: false, }],
 Schema::Property => &[ Field { tag: 1, kind: Kind::Bytes(None), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::Value), max: 1, repeated: false, }, ],
 Schema::Projected => &[ Field { tag: 1, kind: Kind::Child(Schema::VertexId), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::Property), max: crate::remote::MAX_FRAME_BYTES, repeated: true, }, ],
 Schema::FragmentValue => &[ Field { tag: 1, kind: Kind::Child(Schema::BatchValues), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::ScanResult), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Child(Schema::Edges), max: 1, repeated: false, }, Field { tag: 5, kind: Kind::Child(Schema::Incoming), max: 1, repeated: false, }, Field { tag: 6, kind: Kind::Child(Schema::EdgeHop), max: 1, repeated: false, }, Field { tag: 7, kind: Kind::Child(Schema::OptionalValue), max: 1, repeated: false, }, Field { tag: 8, kind: Kind::Child(Schema::Projected), max: 1, repeated: false, }, ],
 Schema::Target => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, ],
 Schema::Authority => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, ],
 Schema::Member => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, ],
 Schema::Cohort => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::Member), max: d2_raft::transaction_decisions::MAX_PARTICIPANTS, repeated: true, }, Field { tag: 4, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::Participant => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Bytes(Some(32)), max: 1, repeated: false, }, ],
 Schema::TerminalRecord => &[ Field { tag: 1, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::Participant), max: d2_raft::transaction_decisions::MAX_PARTICIPANTS, repeated: true, }, Field { tag: 4, kind: Kind::Varint, max: d2_raft::transaction_decisions::MAX_PARTICIPANTS, repeated: true, }, Field { tag: 5, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::TerminalSegment => &[Field { tag: 1, kind: Kind::Child(Schema::TerminalRecord), max: d2_raft::terminal_prefix::SEGMENT_TRANSACTIONS, repeated: true, }],
 Schema::TerminalPrefix => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::Cohort), max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 5, kind: Kind::Child(Schema::TerminalSegment), max: MAX_TERMINAL_SEGMENTS, repeated: true, }, ],
 Schema::ReadLimit => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::FixedOpenRequest => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::Target), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::TerminalPrefix), max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Child(Schema::ReadLimit), max: 1, repeated: false, }, Field { tag: 5, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 6, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::RenewableFixedOpenRequest => &[ Field { tag: 1, kind: Kind::Child(Schema::FixedOpenRequest), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::FixedDescriptor => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::QueryStamp), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, Field { tag: 5, kind: Kind::Child(Schema::Authority), max: 1, repeated: false, }, Field { tag: 6, kind: Kind::Child(Schema::Cohort), max: 1, repeated: false, }, Field { tag: 7, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 8, kind: Kind::Bytes(Some(16)), max: 1, repeated: false, }, ],
 Schema::CaptureCost => &[ Field { tag: 1, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::FixedOpened => &[ Field { tag: 1, kind: Kind::Child(Schema::FixedDescriptor), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::TerminalPrefix), max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::CaptureCost), max: 1, repeated: false, }, ],
 Schema::RenewableFixedOpened => &[ Field { tag: 1, kind: Kind::Child(Schema::FixedOpened), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::FixedLeaseGrant), max: 1, repeated: false, }, ],
 Schema::FixedRenewRequest => &[ Field { tag: 1, kind: Kind::Child(Schema::FixedDescriptor), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::FixedLeaseGrant => &[ Field { tag: 1, kind: Kind::Child(Schema::FixedDescriptor), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Varint, max: 1, repeated: false, }, ],
 Schema::FixedFragmentRequest => &[ Field { tag: 1, kind: Kind::Child(Schema::FixedDescriptor), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::Fragment), max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Child(Schema::Cost), max: 1, repeated: false, }, ],
 Schema::FixedFragmentOutput => &[ Field { tag: 1, kind: Kind::Child(Schema::FixedDescriptor), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, Field { tag: 3, kind: Kind::Child(Schema::Cost), max: 1, repeated: false, }, Field { tag: 4, kind: Kind::Child(Schema::FragmentValue), max: 1, repeated: false, }, ],
 Schema::FixedOpenEnvelope => &[ Field { tag: 1, kind: Kind::Child(Schema::Context), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::FixedOpenRequest), max: 1, repeated: false, }, ],
 Schema::RenewableOpenEnvelope => &[ Field { tag: 1, kind: Kind::Child(Schema::Context), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::RenewableFixedOpenRequest), max: 1, repeated: false, }, ],
 Schema::FixedRenewEnvelope => &[ Field { tag: 1, kind: Kind::Child(Schema::Context), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::FixedRenewRequest), max: 1, repeated: false, }, ],
 Schema::FixedFragmentEnvelope => &[ Field { tag: 1, kind: Kind::Child(Schema::Context), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::FixedFragmentRequest), max: 1, repeated: false, }, ],
 Schema::FixedCloseEnvelope => &[ Field { tag: 1, kind: Kind::Child(Schema::Context), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Child(Schema::FixedDescriptor), max: 1, repeated: false, }, ],
 Schema::FixedFragmentResponse => &[ Field { tag: 1, kind: Kind::Child(Schema::FixedFragmentOutput), max: 1, repeated: false, }, Field { tag: 2, kind: Kind::Varint, max: 1, repeated: false, }, ],
 }
}
fn varint(data: &mut &[u8]) -> Result<u64, Status> {
    let mut result = 0;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = data.split_first().ok_or_else(bad)?;
        *data = rest;
        if shift == 63 && byte > 1 {
            return Err(bad());
        }
        result |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(result);
        }
    }
    Err(bad())
}
fn take<'a>(data: &mut &'a [u8], len: usize) -> Result<&'a [u8], Status> {
    if len > data.len() {
        return Err(bad());
    }
    let (head, tail) = data.split_at(len);
    *data = tail;
    Ok(head)
}
fn delimited<'a>(data: &mut &'a [u8]) -> Result<&'a [u8], Status> {
    let len = usize::try_from(varint(data)?).map_err(|_| bad())?;
    take(data, len)
}
// Exact legacy serde canonical-size accounting for proof records. This is
// arithmetic on borrowed protobuf fields, not JSON encoding or a payload bridge.
fn digits(n: u64) -> usize {
    if n == 0 {
        1
    } else {
        n.ilog10() as usize + 1
    }
}
fn json_array(sum: usize, count: usize) -> usize {
    2 + sum + count.saturating_sub(1)
}
fn scan(schema: Schema, mut data: &[u8], depth: usize, batches: usize) -> Result<usize, Status> {
    if depth > 32 {
        return Err(limit());
    }
    let batches = batches
        + usize::from(matches!(
            schema,
            Schema::HierarchyBatch | Schema::BatchValues
        ));
    if batches > 1 {
        return Err(bad());
    }
    let spec = fields(schema);
    let mut counts = [0usize; 64];
    let mut sizes = [0usize; 64];
    while !data.is_empty() {
        let key = varint(&mut data)?;
        let tag = u32::try_from(key >> 3).map_err(|_| bad())?;
        let wt = key & 7;
        if tag == 0 || tag > 0x1fff_ffff {
            return Err(bad());
        }
        let field = spec.iter().enumerate().find(|(_, f)| f.tag == tag);
        let Some((index, field)) = field else {
            match wt {
                0 => {
                    varint(&mut data)?;
                }
                1 => {
                    take(&mut data, 8)?;
                }
                2 => {
                    delimited(&mut data)?;
                }
                5 => {
                    take(&mut data, 4)?;
                }
                _ => return Err(bad()),
            };
            continue;
        };
        let mut count = 1;
        let size = match (field.kind, wt) {
            (Kind::Varint, 0) => {
                let n = varint(&mut data)?;
                if ((matches!(schema, Schema::SingleQueryRequest) && matches!(tag, 3 | 4))
                    || (matches!(schema, Schema::SingleQueryOutput) && matches!(tag, 1..=3))
                    || (matches!(schema, Schema::SingleQueryResponse) && tag == 2)
                    || (matches!(schema, Schema::Context) && tag == 1))
                    && n > u32::MAX as u64
                {
                    return Err(bad());
                }

                if matches!(schema, Schema::TerminalRecord) && tag == 5 {
                    match n {
                        1 => 8,
                        2 => 7,
                        _ => return Err(bad()),
                    }
                } else if matches!(schema, Schema::TerminalRecord) && tag == 4 {
                    if n == 0 {
                        5
                    } else {
                        4
                    }
                } else {
                    digits(n)
                }
            }
            (Kind::Varint, 2) if field.repeated => {
                let mut packed = delimited(&mut data)?;
                count = 0;
                let mut size = 0;
                while !packed.is_empty() {
                    let n = varint(&mut packed)?;
                    size += if matches!(schema, Schema::TerminalRecord) && tag == 4 {
                        if n == 0 {
                            5
                        } else {
                            4
                        }
                    } else {
                        digits(n)
                    };
                    count += 1;
                    if count > field.max {
                        return Err(limit());
                    }
                }
                size
            }
            (Kind::Fixed64, 1) => {
                take(&mut data, 8)?;
                0
            }
            (Kind::Bytes(exact), 2) => {
                let value = delimited(&mut data)?;
                if exact.is_some_and(|n| value.len() != n) {
                    return Err(bad());
                }
                if matches!(schema, Schema::TerminalRecord | Schema::Participant) {
                    json_array(
                        value.iter().map(|b| digits(u64::from(*b))).sum(),
                        value.len(),
                    )
                } else {
                    0
                }
            }
            (Kind::Child(child), 2) => {
                let value = delimited(&mut data)?;
                if counts[index] >= field.max {
                    return Err(limit());
                }
                scan(child, value, depth + 1, batches)?
            }
            _ => return Err(bad()),
        };
        counts[index] = counts[index].checked_add(count).ok_or_else(limit)?;
        sizes[index] = sizes[index].checked_add(size).ok_or_else(limit)?;
        if counts[index] > field.max {
            return Err(limit());
        }
        if matches!(schema, Schema::TerminalPrefix)
            && tag == 5
            && sizes[index] > d2_raft::capacity_policy::MAX_BYTE_PROFILE_TERMINAL_BYTES
        {
            return Err(limit());
        }
    }
    let oneof_count = match schema {
        Schema::CoordinatorValue
        | Schema::QualifiedElement
        | Schema::QueryRow
        | Schema::SingleQueryResponse
        | Schema::CoordinatorJobOutput
        => Some(counts.iter().sum::<usize>()),
        Schema::CoordinatorQueryResponse | Schema::CoordinatorJobResponse => Some(counts[..2].iter().sum::<usize>()),
        Schema::CoordinatorJobRequest => Some(counts[2..7].iter().sum::<usize>()),
        _ => None,
    };
    if oneof_count.is_some_and(|n| n != 1) {
        return Err(bad());
    }
    // Missing byte arrays would default to empty vectors. Reject before allocating
    // enclosing records. Zero scalar fields are legal protobuf defaults and count
    // as a one-digit JSON number even when omitted from the frame.
    match schema {
        Schema::Participant => {
            if counts[2] != 1 || counts[3] != 1 {
                return Err(bad());
            }
            Ok(b"{\"shard\":,\"epoch\":,\"mode\":,\"intent\":}".len()
                + sizes[0].max(1)
                + sizes[1].max(1)
                + sizes[2]
                + sizes[3])
        }
        Schema::TerminalRecord => {
            if counts[0] != 1 || counts[4] != 1 {
                return Err(bad());
            }
            Ok(
                b"{\"id\":,\"ordinal\":,\"participants\":,\"prepared\":,\"decision\":}".len()
                    + sizes[0]
                    + sizes[1].max(1)
                    + json_array(sizes[2], counts[2])
                    + json_array(sizes[3], counts[3])
                    + sizes[4],
            )
        }
        Schema::TerminalSegment => Ok(json_array(sizes[0], counts[0])),
        _ => Ok(0),
    }
}
impl BoundedMessage for wire::FixedOpenEnvelope {
    fn schema() -> Schema {
        Schema::FixedOpenEnvelope
    }
}
impl BoundedMessage for wire::FixedOpened {
    fn schema() -> Schema {
        Schema::FixedOpened
    }
}
impl BoundedMessage for wire::RenewableOpenEnvelope {
    fn schema() -> Schema {
        Schema::RenewableOpenEnvelope
    }
}
impl BoundedMessage for wire::RenewableFixedOpened {
    fn schema() -> Schema {
        Schema::RenewableFixedOpened
    }
}
impl BoundedMessage for wire::FixedRenewEnvelope {
    fn schema() -> Schema {
        Schema::FixedRenewEnvelope
    }
}
impl BoundedMessage for wire::FixedLeaseGrant {
    fn schema() -> Schema {
        Schema::FixedLeaseGrant
    }
}
impl BoundedMessage for wire::FixedFragmentEnvelope {
    fn schema() -> Schema {
        Schema::FixedFragmentEnvelope
    }
}
impl BoundedMessage for wire::FixedFragmentResponse {
    fn schema() -> Schema {
        Schema::FixedFragmentResponse
    }
}
impl BoundedMessage for wire::FixedCloseEnvelope {
    fn schema() -> Schema {
        Schema::FixedCloseEnvelope
    }
}
impl BoundedMessage for wire::Empty {
    fn schema() -> Schema {
        Schema::Empty
    }
}

impl BoundedMessage for wire::CoordinatorQueryRequest {
    fn schema() -> Schema {
        Schema::CoordinatorQueryRequest
    }
}
impl BoundedMessage for wire::CoordinatorQueryResponse {
    fn schema() -> Schema {
        Schema::CoordinatorQueryResponse
    }
}
impl BoundedMessage for wire::CoordinatorJobRequest {
    fn schema() -> Schema {
        Schema::CoordinatorJobRequest
    }
}
impl BoundedMessage for wire::CoordinatorJobResponse {
    fn schema() -> Schema {
        Schema::CoordinatorJobResponse
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn lenfield(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag << 3 | 2];
        prost::encoding::encode_varint(body.len() as u64, &mut out);
        out.extend_from_slice(body);
        out
    }
    fn envelope(proof: &[u8]) -> Vec<u8> {
        lenfield(2, &lenfield(3, proof))
    }
    fn record() -> wire::TerminalRecord {
        wire::TerminalRecord {
            id: vec![255; 16],
            ordinal: u64::MAX,
            participants: vec![wire::Participant {
                shard: u64::MAX,
                epoch: 0,
                mode: vec![255; 16],
                intent: vec![255; 32],
            }],
            prepared: vec![true],
            decision: wire::TerminalDecision::Commit as i32,
        }
    }
    fn reject_before_prost(bytes: Vec<u8>) {
        let before = PROST_DECODES.with(|n| n.get());
        assert!(decode_checked::<wire::FixedOpenEnvelope, _>(&mut bytes.as_slice()).is_err());
        assert_eq!(PROST_DECODES.with(|n| n.get()), before);
    }
    #[test]
    fn empty_collection_bombs_rejected_before_prost() {
        // Millions of empty messages fit a small body, but must not allocate structs.
        let records = [0x0a, 0].repeat(1_000_000);
        reject_before_prost(envelope(&lenfield(5, &records)));
        reject_before_prost(envelope(&[0x2a, 0].repeat(1_000_000)));
        let participants = [0x1a, 0].repeat(1_000_000);
        reject_before_prost(envelope(&lenfield(5, &lenfield(1, &participants))));
    }
    #[test]
    fn repeated_singular_and_mixed_packed_counts_cannot_reset_quotas() {
        let mut twice = lenfield(2, &[]);
        twice.extend(lenfield(2, &[]));
        reject_before_prost(twice);
        let mut raw = record().encode_to_vec();
        raw.extend([0x20, 1].repeat(64));
        reject_before_prost(envelope(&lenfield(5, &lenfield(1, &raw))));
        let mut raw = record().encode_to_vec();
        raw.extend(lenfield(4, &[1; 64]));
        reject_before_prost(envelope(&lenfield(5, &lenfield(1, &raw))));
    }
    #[test]
    fn proof_canonical_budget_checked_without_allocating_decoded_records() {
        let mut rec = record();
        rec.participants = vec![rec.participants[0].clone(); 64];
        rec.prepared = vec![true; 64];
        let segment = wire::TerminalSegment {
            records: vec![rec; 32],
        }
        .encode_to_vec();
        let field = lenfield(5, &segment);
        let proof = field.repeat(100);
        assert!(proof.len() < 16 * 1024 * 1024);
        reject_before_prost(envelope(&proof));
    }
    #[test]
    fn canonical_accounting_matches_legacy_serde_exactly() {
        for byte in [0, 9, 10, 99, 100, 255] {
            for prepared in [false, true] {
                let mut rec = record();
                rec.id = vec![byte; 16];
                rec.ordinal = 0;
                rec.participants[0].mode = vec![byte; 16];
                rec.participants[0].intent = vec![byte; 32];
                rec.prepared = vec![prepared];
                let expected = serde_json::json!({"id":rec.id,"ordinal":rec.ordinal,"participants":[{"shard":rec.participants[0].shard,"epoch":rec.participants[0].epoch,"mode":rec.participants[0].mode,"intent":rec.participants[0].intent}],"prepared":rec.prepared,"decision":"Commit"});
                let size = scan(Schema::TerminalRecord, &rec.encode_to_vec(), 0, 0).unwrap();
                assert_eq!(size, serde_json::to_vec(&expected).unwrap().len());
                let segment = wire::TerminalSegment {
                    records: vec![rec.clone(), rec],
                };
                assert_eq!(
                    scan(Schema::TerminalSegment, &segment.encode_to_vec(), 0, 0).unwrap(),
                    size * 2 + 3
                );
            }
        }
    }
    #[test]
    fn normal_generated_encoding_and_unknown_fields_preserve_values() {
        let request = wire::FixedOpenEnvelope {
            context: Some(wire::Context {
                version: 1,
                identity: Some(wire::Identity { group: 1, node: 1 }),
                timeout_millis: 1000,
            }),
            request: Some(wire::FixedOpenRequest {
                profile: 1,
                target: Some(wire::Target {
                    shard: 7,
                    epoch: 1,
                    mode: vec![7; 16],
                }),
                proof: Some(wire::TerminalPrefix {
                    format_version: 1,
                    authority: vec![9; 16],
                    cohort: Some(wire::Cohort {
                        coordinator_shard: 9,
                        coordinator_epoch: 1,
                        members: vec![wire::Member {
                            shard: 7,
                            epoch: 1,
                            mode: vec![7; 16],
                        }],
                        profile: 1,
                    }),
                    through: 1,
                    segments: vec![wire::TerminalSegment {
                        records: vec![record()],
                    }],
                }),
                capture_limit: Some(wire::ReadLimit {
                    rows: 1,
                    bytes: 100,
                    operations: 1,
                }),
                ttl_millis: 1000,
                acquire_timeout_millis: 1000,
            }),
        };
        let mut encoded = request.encode_to_vec();
        encoded.extend_from_slice(&[0x78, 1]);
        assert_eq!(
            decode_checked::<wire::FixedOpenEnvelope, _>(&mut encoded.as_slice()).unwrap(),
            request
        );
    }
    #[test]
    fn prescan_does_not_invent_nonproof_collection_limits() {
        let labels = wire::Labels {
            labels: vec![String::new(); 65],
        };
        assert!(scan(Schema::Labels, &labels.encode_to_vec(), 0, 0).is_ok());
        let edges = wire::Edges {
            edges: vec![wire::EdgeHop::default(); 10001],
        };
        assert!(scan(Schema::Edges, &edges.encode_to_vec(), 0, 0).is_ok());
    }
    #[test]
    fn fragment_reply_retains_legacy_response_frame_limit_before_decode() {
        let bytes = vec![0u8; crate::remote::query::MAX_QUERY_RESPONSE_BYTES + 1025];
        PROST_DECODES.with(|n| n.set(0));
        assert_eq!(
            decode_checked::<wire::FixedFragmentResponse, _>(&mut bytes.as_slice())
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            decode_checked::<wire::Empty, _>(&mut bytes.as_slice())
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        PROST_DECODES.with(|n| assert_eq!(n.get(), 0));
    }
}

#[cfg(test)]
mod coordinator_tests {
    use super::*;
    fn reject<U: Message + Default + BoundedMessage>(bytes: Vec<u8>) {
        PROST_DECODES.with(|n| n.set(0));
        assert!(decode_checked::<U, _>(&mut bytes.as_slice()).is_err());
        PROST_DECODES.with(|n| assert_eq!(n.get(), 0));
    }
    #[test]
    fn coordinator_prescan_rejects_bad_oneofs_tokens_and_collections_before_allocation() {
        reject::<wire::CoordinatorJobRequest>(vec![8, 1, 16, 1]); // no request
        reject::<wire::CoordinatorQueryResponse>(vec![0x0a, 0, 0x10, 1]); // both results
        let mut job = wire::CoordinatorJobRequest {
            required_consistency: Some(1),
            version: 1,
            timeout_millis: 1,
            request: Some(wire::coordinator_job_request::Request::Cancel(
                wire::JobCancel { token: vec![1; 15] },
            )),
        };
        reject::<wire::CoordinatorJobRequest>(job.encode_to_vec());
        job.request = Some(wire::coordinator_job_request::Request::Cancel(
            wire::JobCancel { token: vec![1; 16] },
        ));
        let mut duplicate = job.encode_to_vec();
        duplicate.extend([8, 1]);
        reject::<wire::CoordinatorJobRequest>(duplicate);
        let output = wire::CoordinatorQueryOutput {
            completeness: 2,
            snapshots: vec![wire::QueryStamp::default(); 9],
            rows: vec![],
        };
        reject::<wire::CoordinatorQueryResponse>(
            wire::CoordinatorQueryResponse {
            accepted_consistency: Some(1),
                result: Some(wire::coordinator_query_response::Result::Output(output)),
            }
            .encode_to_vec(),
        );
        let output = wire::CoordinatorQueryOutput {
            completeness: 2,
            snapshots: vec![],
            rows: vec![
                wire::QueryRow {
                    kind: Some(wire::query_row::Kind::Count(0))
                };
                super::super::super::MAX_ROWS + 1
            ],
        };
        reject::<wire::CoordinatorQueryResponse>(
            wire::CoordinatorQueryResponse {
            accepted_consistency: Some(1),
                result: Some(wire::coordinator_query_response::Result::Output(output)),
            }
            .encode_to_vec(),
        );
    }
    #[test]
    fn coordinator_prescan_accepts_valid_typed_failures_and_cancel() {
        let response = wire::CoordinatorQueryResponse {
            accepted_consistency: Some(1),
            result: Some(wire::coordinator_query_response::Result::Failure(
                wire::QueryFailure::Unavailable as i32,
            )),
        };
        let bytes = response.encode_to_vec();
        assert_eq!(
            decode_checked::<wire::CoordinatorQueryResponse, _>(&mut bytes.as_slice()).unwrap(),
            response
        );
        let request = wire::CoordinatorJobRequest {
            required_consistency: Some(1),
            version: 1,
            timeout_millis: 1,
            request: Some(wire::coordinator_job_request::Request::Cancel(
                wire::JobCancel { token: vec![1; 16] },
            )),
        };
        let bytes = request.encode_to_vec();
        assert_eq!(
            decode_checked::<wire::CoordinatorJobRequest, _>(&mut bytes.as_slice()).unwrap(),
            request
        );
    }
}

impl BoundedMessage for wire::SingleQueryRequest {
    fn schema() -> Schema {
        Schema::SingleQueryRequest
    }
}
impl BoundedMessage for wire::SingleQueryResponse {
    fn schema() -> Schema {
        Schema::SingleQueryResponse
    }
}

#[cfg(test)]
#[test]
fn single_query_prescan_rejects_duplicate_result_overflow_and_oversize() {
    for bytes in [vec![0x10, 1, 0x10, 2], vec![0x0a, 0, 0x10, 1]] {
        assert!(decode_checked::<wire::SingleQueryResponse, _>(&mut bytes.as_slice()).is_err());
    }
    // Mode 2^32+1 must not truncate to V1.
    let overflow = [0x20, 0x81, 0x80, 0x80, 0x80, 0x10];
    assert!(decode_checked::<wire::SingleQueryRequest, _>(&mut overflow.as_slice()).is_err());
    let request = wire::SingleQueryRequest {
        context: None,
        shard: 7,
        partition: 0,
        mode: 1,
        text: "x".repeat(crate::remote::query::MAX_QUERY_TEXT_BYTES + 1024),
    }
    .encode_to_vec();
    assert!(decode_checked::<wire::SingleQueryRequest, _>(&mut request.as_slice()).is_err());
}
