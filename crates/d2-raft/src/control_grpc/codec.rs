use super::*;
use crate::{D1Capacity, D1Result, D1WriteStatus};
fn req<T>(v: Option<T>) -> Result<T, Status> {
    v.ok_or_else(|| Status::invalid_argument("missing control field"))
}
fn size(v: u64) -> Result<usize, Status> {
    v.try_into()
        .map_err(|_| Status::invalid_argument("control size overflow"))
}
fn bad() -> Status {
    Status::invalid_argument("invalid control value")
}
fn managed(e: crate::managed_rpc::Error) -> Status {
    let _ = e;
    bad()
}
pub(super) fn encode_request(v: ControlRequest, timeout_millis: u64) -> wire::ControlRequest {
    use wire::control_request::Command as C;
    wire::ControlRequest {
        version: 1,
        timeout_millis,
        command: Some(match v {
            ControlRequest::D1Capacity { shard, epoch } => {
                C::Capacity(wire::CapacityRequest { shard, epoch })
            }
            ControlRequest::D1SetCapacity {
                shard,
                epoch,
                expected_bytes,
                new_bytes,
            } => C::SetCapacity(wire::SetCapacityRequest {
                shard,
                epoch,
                expected_bytes: expected_bytes as u64,
                new_bytes: new_bytes as u64,
            }),
            ControlRequest::Initialize { members } => C::Initialize(wire::Initialize {
                members: members
                    .into_iter()
                    .map(|(id, addr)| wire::Member { id, addr })
                    .collect(),
            }),
            ControlRequest::AddLearner { id, addr, blocking } => {
                C::AddLearner(wire::AddLearner { id, addr, blocking })
            }
            ControlRequest::ChangeMembership { ids } => {
                C::ChangeMembership(wire::ChangeMembership {
                    ids: ids.into_iter().collect(),
                })
            }
            ControlRequest::Write { key, value } => C::Write(wire::Write { key, value }),
            ControlRequest::Read { key } => C::Read(wire::Key { key }),
            ControlRequest::Get { key } => C::Get(wire::Key { key }),
            ControlRequest::Status => C::Status(wire::Empty {}),
        }),
    }
}
pub(super) fn decode_request(
    v: wire::ControlRequest,
) -> Result<(ControlRequest, Duration), Status> {
    use wire::control_request::Command as C;
    if v.version != 1 || !(1..=RPC_TIMEOUT.as_millis() as u64).contains(&v.timeout_millis) {
        return Err(bad());
    }
    let r = match req(v.command)? {
        C::Capacity(v) => ControlRequest::D1Capacity {
            shard: v.shard,
            epoch: v.epoch,
        },
        C::SetCapacity(v) => ControlRequest::D1SetCapacity {
            shard: v.shard,
            epoch: v.epoch,
            expected_bytes: size(v.expected_bytes)?,
            new_bytes: size(v.new_bytes)?,
        },
        C::Initialize(v) => {
            let mut members = BTreeMap::new();
            for m in v.members {
                if members.insert(m.id, m.addr).is_some() {
                    return Err(bad());
                }
            }
            ControlRequest::Initialize { members }
        }
        C::AddLearner(v) => ControlRequest::AddLearner {
            id: v.id,
            addr: v.addr,
            blocking: v.blocking,
        },
        C::ChangeMembership(v) => {
            let mut ids = BTreeSet::new();
            for id in v.ids {
                if !ids.insert(id) {
                    return Err(bad());
                }
            }
            ControlRequest::ChangeMembership { ids }
        }
        C::Write(v) => ControlRequest::Write {
            key: v.key,
            value: v.value,
        },
        C::Read(v) => ControlRequest::Read { key: v.key },
        C::Get(v) => ControlRequest::Get { key: v.key },
        C::Status(_) => ControlRequest::Status,
    };
    Ok((r, Duration::from_millis(v.timeout_millis)))
}
fn capacity(v: D1Capacity) -> wire::Capacity {
    wire::Capacity {
        bytes_limit: v.bytes_limit as u64,
        retained_bytes: v.retained_bytes as u64,
        retained_requests: v.retained_requests as u64,
        max_requests: v.max_requests as u64,
        max_bytes: v.max_bytes as u64,
    }
}
fn uncapacity(v: wire::Capacity) -> Result<D1Capacity, Status> {
    Ok(D1Capacity {
        bytes_limit: size(v.bytes_limit)?,
        retained_bytes: size(v.retained_bytes)?,
        retained_requests: size(v.retained_requests)?,
        max_requests: size(v.max_requests)?,
        max_bytes: size(v.max_bytes)?,
    })
}
fn write_status(v: D1WriteStatus) -> i32 {
    match v {
        D1WriteStatus::Committed => 1,
        D1WriteStatus::Duplicate => 2,
    }
}
fn unwrite_status(v: i32) -> Result<D1WriteStatus, Status> {
    match v {
        1 => Ok(D1WriteStatus::Committed),
        2 => Ok(D1WriteStatus::Duplicate),
        _ => Err(bad()),
    }
}
fn application(v: D1Result) -> wire::ApplicationResult {
    use wire::application_result::Result as R;
    wire::ApplicationResult {
        result: Some(match v {
            D1Result::HistorySynced {
                cursor,
                applied_prefix,
            } => R::HistorySynced(wire::HistorySynced {
                cursor: Some(cursor.into()),
                applied_prefix,
            }),
            D1Result::Visibility { applied_prefix } => R::Visibility(applied_prefix),
            D1Result::Participant(v) => R::Participant(v.into()),
            D1Result::Coordinator { resolved_prefix } => R::Coordinator(resolved_prefix),
            D1Result::Activated { shard, epoch } => R::Activated(wire::Activated { shard, epoch }),
            D1Result::Epoch { epoch } => R::Epoch(epoch),
            D1Result::DedupCapacity { bytes_limit } => R::DedupCapacity(bytes_limit as u64),
            D1Result::ManagedWritesEnabled {
                mode_id,
                activated_sequence,
            } => R::ManagedEnabled(wire::ManagedWritesEnabled {
                mode_id: mode_id.to_vec(),
                activated_sequence,
            }),
            D1Result::ManagedWrite {
                mode_id,
                sequence,
                status,
                applied_operations,
            } => R::ManagedWrite(wire::ManagedWrite {
                mode_id: mode_id.to_vec(),
                sequence,
                status: write_status(status),
                applied_operations: applied_operations as u64,
            }),
            D1Result::Write {
                sequence,
                status,
                applied_operations,
            } => R::Write(wire::WriteResult {
                sequence,
                status: write_status(status),
                applied_operations: applied_operations as u64,
            }),
        }),
    }
}
fn unapplication(v: wire::ApplicationResult) -> Result<D1Result, Status> {
    use wire::application_result::Result as R;
    Ok(match req(v.result)? {
        R::HistorySynced(v) => D1Result::HistorySynced {
            cursor: req(v.cursor)?.try_into().map_err(managed)?,
            applied_prefix: v.applied_prefix,
        },
        R::Visibility(applied_prefix) => D1Result::Visibility { applied_prefix },
        R::Participant(v) => D1Result::Participant(v.try_into().map_err(managed)?),
        R::Coordinator(resolved_prefix) => D1Result::Coordinator { resolved_prefix },
        R::Activated(v) => D1Result::Activated {
            shard: v.shard,
            epoch: v.epoch,
        },
        R::Epoch(epoch) => D1Result::Epoch { epoch },
        R::DedupCapacity(v) => D1Result::DedupCapacity {
            bytes_limit: size(v)?,
        },
        R::ManagedEnabled(v) => D1Result::ManagedWritesEnabled {
            mode_id: v.mode_id.try_into().map_err(|_| bad())?,
            activated_sequence: v.activated_sequence,
        },
        R::ManagedWrite(v) => D1Result::ManagedWrite {
            mode_id: v.mode_id.try_into().map_err(|_| bad())?,
            sequence: v.sequence,
            status: unwrite_status(v.status)?,
            applied_operations: size(v.applied_operations)?,
        },
        R::Write(v) => D1Result::Write {
            sequence: v.sequence,
            status: unwrite_status(v.status)?,
            applied_operations: size(v.applied_operations)?,
        },
    })
}
pub(super) fn encode_response(v: ControlResponse) -> wire::ControlResponse {
    use wire::control_response::Result as R;
    wire::ControlResponse {
        result: Some(match v {
            ControlResponse::D1Capacity(v) => R::Capacity(wire::CapacityResponse {
                result: Some(match v {
                    Ok(v) => wire::capacity_response::Result::Capacity(capacity(v)),
                    Err(e) => wire::capacity_response::Result::Error(e.into()),
                }),
            }),
            ControlResponse::D1Admin(v) => R::Admin(wire::AdminResponse {
                result: Some(match v {
                    Ok(v) => wire::admin_response::Result::Value(application(v)),
                    Err(e) => wire::admin_response::Result::Error(e.into()),
                }),
            }),
            ControlResponse::Ack => R::Ack(wire::Empty {}),
            ControlResponse::Index(v) => R::Index(v),
            ControlResponse::Value(v) => R::Value(wire::OptionalValue { value: v }),
            ControlResponse::Status(v) => R::Status(wire::NodeStatus {
                id: v.id,
                leader: v.leader,
                applied: v.applied,
                last_log: v.last_log,
            }),
            ControlResponse::Err(e) => R::Error(e),
        }),
    }
}
pub(super) fn decode_response(v: wire::ControlResponse) -> Result<ControlResponse, Status> {
    use wire::control_response::Result as R;
    Ok(match req(v.result)? {
        R::Capacity(v) => ControlResponse::D1Capacity(match req(v.result)? {
            wire::capacity_response::Result::Capacity(v) => Ok(uncapacity(v)?),
            wire::capacity_response::Result::Error(e) => Err(e.try_into().map_err(managed)?),
        }),
        R::Admin(v) => ControlResponse::D1Admin(match req(v.result)? {
            wire::admin_response::Result::Value(v) => Ok(unapplication(v)?),
            wire::admin_response::Result::Error(e) => Err(e.try_into().map_err(managed)?),
        }),
        R::Ack(_) => ControlResponse::Ack,
        R::Index(v) => ControlResponse::Index(v),
        R::Value(v) => ControlResponse::Value(v.value),
        R::Status(v) => ControlResponse::Status(NodeStatus {
            id: v.id,
            leader: v.leader,
            applied: v.applied,
            last_log: v.last_log,
        }),
        R::Error(e) => ControlResponse::Err(e),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    fn equal<T: serde::Serialize>(a: &T, b: &T) {
        assert_eq!(
            serde_json::to_value(a).unwrap(),
            serde_json::to_value(b).unwrap()
        );
    }
    #[test]
    fn request_golden_all_commands_and_strict_fields() {
        assert_eq!(
            encode_request(ControlRequest::Status, 1).encode_to_vec(),
            vec![8, 1, 16, 1, 90, 0]
        );
        let requests = [
            ControlRequest::Status,
            ControlRequest::D1Capacity { shard: 7, epoch: 1 },
            ControlRequest::D1SetCapacity {
                shard: 7,
                epoch: 1,
                expected_bytes: 64,
                new_bytes: 128,
            },
            ControlRequest::Initialize {
                members: BTreeMap::from([(1, "127.0.0.1:1".into()), (2, "127.0.0.1:2".into())]),
            },
            ControlRequest::AddLearner {
                id: 2,
                addr: "127.0.0.1:2".into(),
                blocking: true,
            },
            ControlRequest::ChangeMembership {
                ids: BTreeSet::from([1, 2]),
            },
            ControlRequest::Write {
                key: "\0中文".into(),
                value: "\"\\\n".into(),
            },
            ControlRequest::Read { key: "k".into() },
            ControlRequest::Get { key: "k".into() },
        ];
        for request in requests {
            let (actual, d) = decode_request(encode_request(request.clone(), 1000)).unwrap();
            equal(&request, &actual);
            assert_eq!(d, Duration::from_secs(1));
        }
        let mut v = encode_request(ControlRequest::Status, 1);
        v.version = 0;
        assert!(decode_request(v).is_err());
        assert!(decode_request(encode_request(ControlRequest::Status, 0)).is_err());
        assert!(decode_request(encode_request(ControlRequest::Status, 3001)).is_err());
        let mut v = encode_request(ControlRequest::Status, 1);
        v.command = None;
        assert!(decode_request(v).is_err());
        let members = vec![
            wire::Member {
                id: 1,
                addr: "a".into(),
            },
            wire::Member {
                id: 1,
                addr: "b".into(),
            },
        ];
        let mut v = encode_request(ControlRequest::Status, 1);
        v.command = Some(wire::control_request::Command::Initialize(
            wire::Initialize { members },
        ));
        assert!(decode_request(v).is_err());
        let mut v = encode_request(ControlRequest::Status, 1);
        v.command = Some(wire::control_request::Command::ChangeMembership(
            wire::ChangeMembership { ids: vec![1, 1] },
        ));
        assert!(decode_request(v).is_err());
    }
    #[test]
    fn responses_preserve_capacity_errors_optional_values_and_application_results() {
        let responses = [
            ControlResponse::Ack,
            ControlResponse::Index(u64::MAX),
            ControlResponse::Value(None),
            ControlResponse::Value(Some(String::new())),
            ControlResponse::Value(Some("中文\0".into())),
            ControlResponse::Status(NodeStatus {
                id: 1,
                leader: None,
                applied: 2,
                last_log: 3,
            }),
            ControlResponse::Err("unknown outcome".into()),
            ControlResponse::D1Capacity(Ok(D1Capacity {
                bytes_limit: usize::MAX,
                retained_bytes: 1,
                retained_requests: 2,
                max_requests: 3,
                max_bytes: 4,
            })),
            ControlResponse::D1Capacity(Err(crate::D1Error::CapacityConflict {
                expected: usize::MAX,
                current: 0,
            })),
            ControlResponse::D1Admin(Err(crate::D1Error::CoordinatorRejected(
                crate::transaction_decisions::Error::InvalidJournal,
            ))),
        ];
        for response in responses {
            let actual = decode_response(encode_response(response.clone())).unwrap();
            equal(&response, &actual);
        }
        let results = [
            D1Result::Visibility {
                applied_prefix: u64::MAX,
            },
            D1Result::Participant(crate::participant_writes::Status::Committed { sequence: 9 }),
            D1Result::Coordinator { resolved_prefix: 9 },
            D1Result::Activated { shard: 7, epoch: 1 },
            D1Result::Epoch { epoch: 2 },
            D1Result::DedupCapacity {
                bytes_limit: usize::MAX,
            },
            D1Result::ManagedWritesEnabled {
                mode_id: [7; 16],
                activated_sequence: 8,
            },
            D1Result::ManagedWrite {
                mode_id: [7; 16],
                sequence: 9,
                status: D1WriteStatus::Duplicate,
                applied_operations: usize::MAX,
            },
            D1Result::Write {
                sequence: 9,
                status: D1WriteStatus::Committed,
                applied_operations: 0,
            },
        ];
        for result in results {
            assert_eq!(unapplication(application(result.clone())).unwrap(), result);
        }
        assert!(decode_response(wire::ControlResponse { result: None }).is_err());
        assert!(unapplication(wire::ApplicationResult {
            result: Some(wire::application_result::Result::Write(wire::WriteResult {
                sequence: 1,
                status: 0,
                applied_operations: 1
            }))
        })
        .is_err());
        assert!(unapplication(wire::ApplicationResult {
            result: Some(wire::application_result::Result::ManagedEnabled(
                wire::ManagedWritesEnabled {
                    mode_id: vec![0; 15],
                    activated_sequence: 0
                }
            ))
        })
        .is_err());
    }
}
