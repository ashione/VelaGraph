use super::{wire, RemoteError};
use crate::remote::{Operation, RemoteIdentity, Reply};
use d2_raft::{D1Result, D1WriteStatus};
use crate::remote::{managed, RemoteSnapshot};
fn id(v: Vec<u8>) -> Result<[u8;16], RemoteError> { v.try_into().map_err(|_| RemoteError::Protocol) }
pub(super) fn operation(v: wire::request::Operation) -> Result<Operation, RemoteError> {
    Ok(match v {
        wire::request::Operation::Snapshot(shard) => Operation::Snapshot { shard },
        wire::request::Operation::ManagedInspect(v) => Operation::Managed(managed::Request::Inspect { shard:v.shard, epoch:v.epoch }),
        wire::request::Operation::ManagedEnable(v) => Operation::Managed(managed::Request::Enable { shard:v.shard, epoch:v.epoch, expected_sequence:v.expected_sequence, mode_id:id(v.mode_id)? }),
        wire::request::Operation::ManagedWrite(v) => Operation::Managed(managed::Request::Write { mode_id:id(v.mode_id)?, request:v.request.ok_or(RemoteError::Protocol)?.try_into().map_err(|_| RemoteError::InvalidArgument)? }),
        wire::request::Operation::ManagedStatus(v) => Operation::Managed(managed::Request::Status { shard:v.shard, epoch:v.epoch, mode_id:id(v.mode_id)?, request_id:id(v.request_id)? }),
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
            Operation::Snapshot { shard } => wire::request::Operation::Snapshot(shard),
            Operation::Managed(v) => match v {
                managed::Request::Inspect{shard,epoch} => wire::request::Operation::ManagedInspect(wire::ManagedInspect{shard,epoch}),
                managed::Request::Enable{shard,epoch,expected_sequence,mode_id} => wire::request::Operation::ManagedEnable(wire::ManagedEnable{shard,epoch,expected_sequence,mode_id:mode_id.to_vec()}),
                managed::Request::Write{mode_id,request} => wire::request::Operation::ManagedWrite(wire::ManagedWrite{mode_id:mode_id.to_vec(),request:Some(request.into())}),
                managed::Request::Status{shard,epoch,mode_id,request_id} => wire::request::Operation::ManagedStatus(wire::ManagedStatusRequest{shard,epoch,mode_id:mode_id.to_vec(),request_id:request_id.to_vec()}),
            },
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
            Ok(Reply::Snapshot(v)) => R::Snapshot(wire::Snapshot{shard:v.shard,epoch:v.epoch,sequence:v.sequence,entries:v.entries.into_iter().map(|(key,value)|wire::SnapshotEntry{key,value}).collect()}),
            Ok(Reply::Managed(v)) => match v {
                managed::Output::Mode(mode) => R::ManagedMode(wire::ManagedModeResult{mode:mode.map(|v|wire::ManagedMode{mode_id:v.mode_id.to_vec(),activated_sequence:v.activated_sequence})}),
                managed::Output::Enabled{mode_id,activated_sequence} => R::ManagedEnabled(wire::ManagedEnabled{mode_id:mode_id.to_vec(),activated_sequence}),
                managed::Output::Written{mode_id,sequence,status,applied_operations} => R::ManagedWritten(wire::ManagedWritten{mode_id:mode_id.to_vec(),result:Some(wire::WriteResult{sequence,status:match status {D1WriteStatus::Committed=>1,D1WriteStatus::Duplicate=>2},applied_operations:applied_operations as u64})}),
                managed::Output::Status(status) => R::ManagedStatus(wire::ManagedStatusResult{status:Some(match status {d2_raft::D1ManagedStatus::Unknown => wire::managed_status_result::Status::Unknown(wire::Empty{}), d2_raft::D1ManagedStatus::Committed{sequence} => wire::managed_status_result::Status::CommittedSequence(sequence)})}),
            },
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
        R::Snapshot(v) => {
            let bytes=v.entries.iter().fold(0usize,|n,e|n.saturating_add(e.key.len()).saturating_add(e.value.len()));
            if v.entries.len()>crate::remote::MAX_SNAPSHOT_ROWS || bytes>crate::remote::MAX_SNAPSHOT_BYTES {return Err(RemoteError::ResourceExhausted);}
            Ok(Reply::Snapshot(RemoteSnapshot{shard:v.shard,epoch:v.epoch,sequence:v.sequence,entries:v.entries.into_iter().map(|e|(e.key,e.value)).collect()}))
        },
        R::ManagedMode(v) => Ok(Reply::Managed(managed::Output::Mode(v.mode.map(|v| Ok::<_,RemoteError>(d2_raft::D1ManagedMode{mode_id:id(v.mode_id)?,activated_sequence:v.activated_sequence})).transpose()?))),
        R::ManagedEnabled(v) => Ok(Reply::Managed(managed::Output::Enabled{mode_id:id(v.mode_id)?,activated_sequence:v.activated_sequence})),
        R::ManagedWritten(v) => {
            let w=v.result.ok_or(RemoteError::Protocol)?;
            Ok(Reply::Managed(managed::Output::Written{mode_id:id(v.mode_id)?,sequence:w.sequence,status:match w.status{1=>D1WriteStatus::Committed,2=>D1WriteStatus::Duplicate,_=>return Err(RemoteError::Protocol)},applied_operations:w.applied_operations.try_into().map_err(|_|RemoteError::Protocol)?}))
        },
        R::ManagedStatus(v) => Ok(Reply::Managed(managed::Output::Status(match v.status.ok_or(RemoteError::Protocol)?{wire::managed_status_result::Status::Unknown(_)=>d2_raft::D1ManagedStatus::Unknown,wire::managed_status_result::Status::CommittedSequence(sequence)=>d2_raft::D1ManagedStatus::Committed{sequence}}))),
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
    #[test]
    fn managed_operations_and_single_snapshot_are_lossless() {
        let identity=RemoteIdentity{group:1,node:2};
        let write=d2_raft::D1WriteRequest{shard:7,epoch:1,request_id:[4;16],expected_sequence:Some(0),operations:vec![d2_raft::WriteOp::put([0,255],[128])]};
        for op in [
            Operation::Snapshot{shard:7},
            Operation::Managed(managed::Request::Inspect{shard:7,epoch:1}),
            Operation::Managed(managed::Request::Enable{shard:7,epoch:1,expected_sequence:0,mode_id:[3;16]}),
            Operation::Managed(managed::Request::Write{mode_id:[3;16],request:write}),
            Operation::Managed(managed::Request::Status{shard:7,epoch:1,mode_id:[3;16],request_id:[4;16]}),
        ] {
            let before=serde_json::to_vec(&op).unwrap();
            let encoded=request(identity,10,op).unwrap().encode_to_vec();
            super::super::bounded::check::<wire::Request>(&encoded).unwrap();
            let decoded=wire::Request::decode(encoded.as_slice()).unwrap();
            assert_eq!(before,serde_json::to_vec(&operation(decoded.operation.unwrap()).unwrap()).unwrap());
        }
        for value in [
            Reply::Snapshot(RemoteSnapshot{shard:7,epoch:1,sequence:0,entries:vec![(vec![0,255],vec![128])]}),
            Reply::Managed(managed::Output::Mode(None)),
            Reply::Managed(managed::Output::Mode(Some(d2_raft::D1ManagedMode{mode_id:[3;16],activated_sequence:0}))),
            Reply::Managed(managed::Output::Enabled{mode_id:[3;16],activated_sequence:0}),
            Reply::Managed(managed::Output::Written{mode_id:[3;16],sequence:1,status:D1WriteStatus::Duplicate,applied_operations:1}),
            Reply::Managed(managed::Output::Status(d2_raft::D1ManagedStatus::Unknown)),
            Reply::Managed(managed::Output::Status(d2_raft::D1ManagedStatus::Committed{sequence:1})),
        ] {
            let before=serde_json::to_vec(&value).unwrap();
            let encoded=response(Ok(value)).unwrap().encode_to_vec();
            super::super::bounded::check::<wire::Response>(&encoded).unwrap();
            let decoded=wire::Response::decode(encoded.as_slice()).unwrap();
            assert_eq!(before,serde_json::to_vec(&reply(decoded).unwrap().unwrap()).unwrap());
        }
    }

}
