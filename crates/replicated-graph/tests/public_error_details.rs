//! Independent wire vectors for the public google.rpc.Status contract.
//! Definitions here follow the standard envelope, not the production encoder.
use prost::Message;
use replicated_graph::managed_gateway::{decode_grpc_error_detail, public_proto as pb};

const TYPE_URL: &str = "type.googleapis.com/velagraph.public.managed.v1.ErrorDetail";

#[derive(Clone, PartialEq, Message)]
struct Any {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
struct RichStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

fn detail() -> pb::ErrorDetail {
    pb::ErrorDetail {
        reason: pb::error_detail::Reason::QueryUnsupported as i32,
        identity: None,
    }
}
fn envelope(detail: &pb::ErrorDetail) -> RichStatus {
    RichStatus {
        code: tonic::Code::Unimplemented as i32,
        message: "QUERY_UNSUPPORTED".into(),
        details: vec![Any {
            type_url: TYPE_URL.into(),
            value: detail.encode_to_vec(),
        }],
    }
}
fn status(envelope: &RichStatus) -> tonic::Status {
    tonic::Status::with_details(
        tonic::Code::Unimplemented,
        "QUERY_UNSUPPORTED",
        envelope.encode_to_vec().into(),
    )
}

#[test]
fn standard_error_envelope_roundtrips_and_preserves_identity() {
    let original = detail();
    assert_eq!(
        decode_grpc_error_detail(&status(&envelope(&original))).unwrap(),
        original
    );
    let mut identified = detail();
    identified.identity = Some(pb::CallIdentity {
        request_id: vec![3; 16],
        binding: Some(pb::Binding {
            authority_shard: 9,
            authority_mode: vec![9; 16],
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: [7, 8]
                .into_iter()
                .map(|shard| pb::BindingMember {
                    shard,
                    epoch: 1,
                    mode: vec![shard as u8; 16],
                })
                .collect(),
            capacity_profile: pb::CapacityProfile::LegacyCountV1 as i32,
            mapping: pb::Mapping::PartitionEqualsShardV1 as i32,
        }),
    });
    assert_eq!(
        decode_grpc_error_detail(&status(&envelope(&identified))).unwrap(),
        identified
    );
    let mut short_nonce = identified.clone();
    short_nonce.identity.as_mut().unwrap().request_id.pop();
    assert!(decode_grpc_error_detail(&status(&envelope(&short_nonce))).is_err());
    let mut short_mode = identified.clone();
    short_mode
        .identity
        .as_mut()
        .unwrap()
        .binding
        .as_mut()
        .unwrap()
        .authority_mode
        .pop();
    assert!(decode_grpc_error_detail(&status(&envelope(&short_mode))).is_err());
    let mut missing_binding = identified;
    missing_binding.identity.as_mut().unwrap().binding = None;
    assert!(decode_grpc_error_detail(&status(&envelope(&missing_binding))).is_err());
}

#[test]
fn error_envelope_rejects_code_message_type_and_cardinality_changes() {
    let good = envelope(&detail());
    let mut changed = good.clone();
    changed.code = tonic::Code::Internal as i32;
    assert!(decode_grpc_error_detail(&status(&changed)).is_err());
    let mut changed = good.clone();
    changed.message = "another message".into();
    assert!(decode_grpc_error_detail(&status(&changed)).is_err());
    let mut changed = good.clone();
    changed.details[0].type_url = "type.googleapis.com/other.ErrorDetail".into();
    assert!(decode_grpc_error_detail(&status(&changed)).is_err());
    let mut changed = good.clone();
    changed.details.push(changed.details[0].clone());
    assert!(decode_grpc_error_detail(&status(&changed)).is_err());
    let mut changed = good.clone();
    changed.details.clear();
    assert!(decode_grpc_error_detail(&status(&changed)).is_err());
    let mut wrong_reason = detail();
    wrong_reason.reason = pb::error_detail::Reason::OutcomeUnknown as i32;
    assert!(decode_grpc_error_detail(&status(&envelope(&wrong_reason))).is_err());
}

#[test]
fn error_envelope_rejects_legacy_malformed_and_oversized_payloads() {
    let naked = tonic::Status::with_details(
        tonic::Code::Unimplemented,
        "QUERY_UNSUPPORTED",
        detail().encode_to_vec().into(),
    );
    assert!(decode_grpc_error_detail(&naked).is_err());
    assert!(decode_grpc_error_detail(&tonic::Status::unavailable("lost transport")).is_err());
    let malformed = tonic::Status::with_details(
        tonic::Code::Unimplemented,
        "QUERY_UNSUPPORTED",
        vec![0xff].into(),
    );
    assert!(decode_grpc_error_detail(&malformed).is_err());
    let mut malformed_detail = envelope(&detail());
    malformed_detail.details[0].value = vec![0xff];
    assert!(decode_grpc_error_detail(&status(&malformed_detail)).is_err());
    let mut oversized = envelope(&detail());
    oversized.details[0].value = vec![0; 1024 * 1024 + 1];
    assert!(decode_grpc_error_detail(&status(&oversized)).is_err());
}

#[test]
fn error_envelope_rejects_duplicate_and_unknown_outer_fields() {
    for suffix in [vec![0x08, tonic::Code::Unimplemented as u8], vec![0x20, 0]] {
        let mut bytes = envelope(&detail()).encode_to_vec();
        bytes.extend(suffix);
        let wire = tonic::Status::with_details(
            tonic::Code::Unimplemented,
            "QUERY_UNSUPPORTED",
            bytes.into(),
        );
        assert!(decode_grpc_error_detail(&wire).is_err());
    }
}
