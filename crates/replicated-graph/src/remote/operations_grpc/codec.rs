use super::{wire, RemoteError};
use crate::remote::{Operation, RemoteIdentity, Reply};
use d2_raft::{D1Result, D1WriteStatus};
pub(super) fn operation(v: wire::request::Operation) -> Result<Operation, RemoteError> {
    Ok(match v {
        wire::request::Operation::Probe(shard) => Operation::Probe { shard },
        wire::request::Operation::Metadata(shard) => Operation::Metadata { shard },
        wire::request::Operation::Write(v) => {
            Operation::Write(v.try_into().map_err(|_| RemoteError::InvalidArgument)?)
        }
    })
}
pub(super) fn request(
    identity: RemoteIdentity,
    timeout_millis: u64,
    op: Operation,
) -> Result<wire::Request, RemoteError> {
    Ok(wire::Request {
        version: 1,
        identity: Some(wire::Identity {
            group: identity.group,
            node: identity.node,
        }),
        timeout_millis,
        operation: Some(match op {
            Operation::Probe { shard } => wire::request::Operation::Probe(shard),
            Operation::Metadata { shard } => wire::request::Operation::Metadata(shard),
            Operation::Write(v) => wire::request::Operation::Write(v.into()),
            _ => return Err(RemoteError::Protocol),
        }),
    })
}
pub(in crate::remote) fn failure(v: RemoteError) -> wire::RemoteFailure {
    use wire::remote_failure::Kind as K;
    wire::RemoteFailure {
        kind: Some(match v {
            RemoteError::NotLeader { leader } => K::NotLeader(wire::NotLeader { leader }),
            RemoteError::Application(v) => K::Application(v.into()),
            RemoteError::InvalidArgument => K::InvalidArgument(wire::Empty {}),
            RemoteError::ResourceExhausted => K::ResourceExhausted(wire::Empty {}),
            RemoteError::Unavailable => K::Unavailable(wire::Empty {}),
            RemoteError::Timeout { request_id } => K::Timeout(wire::Timeout {
                request_id: request_id.map(|x| x.to_vec()),
            }),
            RemoteError::SessionUnavailable => K::SessionUnavailable(wire::Empty {}),
            RemoteError::Protocol => K::Protocol(wire::Empty {}),
        }),
    }
}
pub(in crate::remote) fn unfailure(v: wire::RemoteFailure) -> Result<RemoteError, RemoteError> {
    use wire::remote_failure::Kind as K;
    Ok(match v.kind.ok_or(RemoteError::Protocol)? {
        K::NotLeader(v) => RemoteError::NotLeader { leader: v.leader },
        K::Application(v) => {
            RemoteError::Application(v.try_into().map_err(|_| RemoteError::Protocol)?)
        }
        K::InvalidArgument(_) => RemoteError::InvalidArgument,
        K::ResourceExhausted(_) => RemoteError::ResourceExhausted,
        K::Unavailable(_) => RemoteError::Unavailable,
        K::Timeout(v) => RemoteError::Timeout {
            request_id: v
                .request_id
                .map(|x| x.try_into().map_err(|_| RemoteError::Protocol))
                .transpose()?,
        },
        K::SessionUnavailable(_) => RemoteError::SessionUnavailable,
        K::Protocol(_) => RemoteError::Protocol,
    })
}
pub(super) fn response(v: Result<Reply, RemoteError>) -> Result<wire::Response, RemoteError> {
    use wire::response::Result as R;
    Ok(wire::Response {
        version: 1,
        result: Some(match v {
            Ok(Reply::Probe) => R::Probe(wire::Empty {}),
            Ok(Reply::Metadata(shard, epoch, sequence)) => R::Metadata(wire::Metadata {
                shard,
                epoch,
                sequence,
            }),
            Ok(Reply::Write(D1Result::Write {
                sequence,
                status,
                applied_operations,
            })) => R::Write(wire::WriteResult {
                sequence,
                status: match status {
                    D1WriteStatus::Committed => 1,
                    D1WriteStatus::Duplicate => 2,
                },
                applied_operations: applied_operations as u64,
            }),
            Ok(_) => return Err(RemoteError::Protocol),
            Err(v) => R::Error(failure(v)),
        }),
    })
}
pub(super) fn reply(v: wire::Response) -> Result<Result<Reply, RemoteError>, RemoteError> {
    if v.version != 1 {
        return Err(RemoteError::Protocol);
    }
    use wire::response::Result as R;
    Ok(match v.result.ok_or(RemoteError::Protocol)? {
        R::Probe(_) => Ok(Reply::Probe),
        R::Metadata(v) => Ok(Reply::Metadata(v.shard, v.epoch, v.sequence)),
        R::Write(v) => Ok(Reply::Write(D1Result::Write {
            sequence: v.sequence,
            status: match v.status {
                1 => D1WriteStatus::Committed,
                2 => D1WriteStatus::Duplicate,
                _ => return Err(RemoteError::Protocol),
            },
            applied_operations: v
                .applied_operations
                .try_into()
                .map_err(|_| RemoteError::Protocol)?,
        })),
        R::Error(v) => Err(unfailure(v)?),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    #[test]
    fn golden_and_strict() {
        let v = request(
            RemoteIdentity { group: 1, node: 2 },
            3,
            Operation::Probe { shard: 4 },
        )
        .unwrap();
        assert_eq!(
            v.encode_to_vec(),
            vec![8, 1, 18, 4, 8, 1, 16, 2, 24, 3, 32, 4]
        );
        assert!(matches!(
            operation(v.operation.unwrap()).unwrap(),
            Operation::Probe { shard: 4 }
        ));
        assert!(reply(wire::Response {
            version: 2,
            result: Some(wire::response::Result::Probe(wire::Empty {}))
        })
        .is_err());
        assert!(reply(wire::Response {
            version: 1,
            result: Some(wire::response::Result::Write(wire::WriteResult {
                sequence: 1,
                status: 0,
                applied_operations: 1
            }))
        })
        .is_err());
    }
    #[test]
    fn write_request_preserves_bytes_and_optional_sequence() {
        for expected_sequence in [None, Some(0), Some(u64::MAX)] {
            let write = d2_raft::D1WriteRequest {
                shard: 7,
                epoch: 2,
                request_id: [0xff; 16],
                expected_sequence,
                operations: vec![
                    d2_raft::WriteOp::Put {
                        key: vec![0, 255],
                        value: vec![],
                    },
                    d2_raft::WriteOp::Delete { key: vec![128] },
                ],
            };
            let before = serde_json::to_vec(&write).unwrap();
            let encoded = request(
                RemoteIdentity { group: 1, node: 2 },
                1,
                Operation::Write(write),
            )
            .unwrap();
            let decoded = wire::Request::decode(encoded.encode_to_vec().as_slice()).unwrap();
            let Operation::Write(after) = operation(decoded.operation.unwrap()).unwrap() else {
                panic!("wrong operation")
            };
            assert_eq!(before, serde_json::to_vec(&after).unwrap());
        }
        let malformed = d2_raft::managed_rpc::grpc::wire::D1WriteRequest {
            request_id: vec![1; 15],
            ..Default::default()
        };
        assert!(operation(wire::request::Operation::Write(malformed)).is_err());
    }
    #[test]
    fn result_and_errors_roundtrip() {
        for e in [
            RemoteError::NotLeader { leader: Some(3) },
            RemoteError::Application(d2_raft::D1Error::Fenced {
                requested: 2,
                current: 3,
            }),
            RemoteError::InvalidArgument,
            RemoteError::ResourceExhausted,
            RemoteError::Unavailable,
            RemoteError::Timeout {
                request_id: Some([9; 16]),
            },
            RemoteError::SessionUnavailable,
            RemoteError::Protocol,
        ] {
            let before = serde_json::to_vec(&e).unwrap();
            let out = reply(response(Err(e)).unwrap()).unwrap().err().unwrap();
            assert_eq!(before, serde_json::to_vec(&out).unwrap());
        }
        for r in [
            Reply::Probe,
            Reply::Metadata(1, 2, 3),
            Reply::Write(D1Result::Write {
                sequence: 4,
                status: D1WriteStatus::Duplicate,
                applied_operations: 5,
            }),
        ] {
            let before = serde_json::to_vec(&r).unwrap();
            let out = reply(response(Ok(r)).unwrap()).unwrap().unwrap();
            assert_eq!(before, serde_json::to_vec(&out).unwrap());
        }
    }
}
