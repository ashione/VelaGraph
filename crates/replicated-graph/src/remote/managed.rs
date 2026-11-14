//! Opt-in shard-local write fencing. No distributed transaction/MVCC guarantee.
use super::*;
use d2_raft::{D1ManagedMode, D1ManagedStatus};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Request {
    Inspect {
        shard: u64,
        epoch: u64,
    },
    Enable {
        shard: u64,
        epoch: u64,
        expected_sequence: u64,
        mode_id: [u8; 16],
    },
    Write {
        mode_id: [u8; 16],
        request: D1WriteRequest,
    },
    Status {
        shard: u64,
        epoch: u64,
        mode_id: [u8; 16],
        request_id: [u8; 16],
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Output {
    Mode(Option<D1ManagedMode>),
    Enabled {
        mode_id: [u8; 16],
        activated_sequence: u64,
    },
    Written {
        mode_id: [u8; 16],
        sequence: u64,
        status: d2_raft::D1WriteStatus,
        applied_operations: usize,
    },
    Status(D1ManagedStatus),
}
impl Request {
    fn validate(&self) -> Result<(u64, Option<[u8; 16]>), RemoteError> {
        let (shard, epoch, mode, id) = match self {
            Self::Inspect { shard, epoch } => (*shard, *epoch, None, None),
            Self::Enable {
                shard,
                epoch,
                mode_id,
                ..
            } => (*shard, *epoch, Some(mode_id), None),
            Self::Status {
                shard,
                epoch,
                mode_id,
                ..
            } => (*shard, *epoch, Some(mode_id), None),
            Self::Write { mode_id, request } => {
                validate_write(request)?;
                (
                    request.shard,
                    request.epoch,
                    Some(mode_id),
                    Some(request.request_id),
                )
            }
        };
        if epoch == 0 {
            return Err(RemoteError::Application(D1Error::InvalidEpoch));
        }
        if mode.is_some_and(|id| *id == [0; 16]) {
            return Err(RemoteError::Application(D1Error::InvalidManagedMode));
        }
        Ok((shard, id))
    }
}

pub(super) async fn handle(
    raft: openraft::Raft<TypeConfig>,
    state: Arc<StateMachine>,
    shard: u64,
    request: Request,
) -> Result<Output, RemoteError> {
    if request.validate()?.0 != shard {
        return Err(RemoteError::InvalidArgument);
    }
    let command = match request {
        Request::Inspect { shard, epoch } => {
            raft.ensure_linearizable()
                .await
                .map_err(|_| RemoteError::Unavailable)?;
            return state
                .d1_managed_mode(shard, epoch)
                .await
                .map(Output::Mode)
                .map_err(RemoteError::Application);
        }
        Request::Status {
            shard,
            epoch,
            mode_id,
            request_id,
        } => {
            raft.ensure_linearizable()
                .await
                .map_err(|_| RemoteError::Unavailable)?;
            return state
                .d1_managed_status(shard, epoch, mode_id, request_id)
                .await
                .map(Output::Status)
                .map_err(RemoteError::Application);
        }
        Request::Enable {
            shard,
            epoch,
            expected_sequence,
            mode_id,
        } => D1Command::EnableManagedWrites {
            shard,
            epoch,
            expected_sequence,
            mode_id,
        },
        Request::Write { mode_id, request } => D1Command::ManagedWrite { mode_id, request },
    };
    let response = raft
        .client_write(LogCommand::D1V1(command))
        .await
        .map_err(|e| {
            if let Some(hint) = e.forward_to_leader::<openraft::BasicNode>() {
                RemoteError::NotLeader {
                    leader: hint.leader_id,
                }
            } else {
                RemoteError::Unavailable
            }
        })?;
    match response
        .data
        .d1
        .ok_or(RemoteError::Protocol)?
        .map_err(RemoteError::Application)?
    {
        D1Result::ManagedWritesEnabled {
            mode_id,
            activated_sequence,
        } => Ok(Output::Enabled {
            mode_id,
            activated_sequence,
        }),
        D1Result::ManagedWrite {
            mode_id,
            sequence,
            status,
            applied_operations,
        } => Ok(Output::Written {
            mode_id,
            sequence,
            status,
            applied_operations,
        }),
        _ => Err(RemoteError::Protocol),
    }
}

/// No retry/fallback. Enable is irreversible for a running shard. A timed-out
/// write remains unknown until checked/retried with the exact identity and body.
pub async fn request(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    request: Request,
    deadline: Duration,
) -> Result<Output, RemoteError> {
    let (_, id) = request.validate()?;
    let mut client = super::operations_grpc::Client::connect_lazy(endpoint, identity, deadline)?;
    let reply = client.call(Operation::Managed(request.clone())).await.map_err(|e| match e {
        RemoteError::Timeout { .. } => RemoteError::Timeout { request_id: id },
        e => e,
    })?;
    let Reply::Managed(output) = reply else {
        return Err(RemoteError::Protocol);
    };
    let valid = match (&request, &output) {
        (Request::Inspect { .. }, Output::Mode(None)) => true,
        (Request::Inspect { .. }, Output::Mode(Some(mode))) => mode.mode_id != [0; 16],
        (
            Request::Enable {
                mode_id,
                expected_sequence,
                ..
            },
            Output::Enabled {
                mode_id: actual,
                activated_sequence,
            },
        ) => mode_id == actual && expected_sequence == activated_sequence,
        (
            Request::Write { mode_id, request },
            Output::Written {
                mode_id: actual,
                sequence,
                applied_operations,
                ..
            },
        ) => {
            mode_id == actual
                && *sequence > 0
                && *applied_operations == request.operations.len()
                && request
                    .expected_sequence
                    .is_none_or(|expected| expected.checked_add(1) == Some(*sequence))
        }
        (Request::Status { .. }, Output::Status(D1ManagedStatus::Unknown)) => true,
        (Request::Status { .. }, Output::Status(D1ManagedStatus::Committed { sequence })) => {
            *sequence > 0
        }
        _ => false,
    };
    if !valid {
        return Err(RemoteError::Protocol);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2_raft::{D1WriteStatus, TcpNode, TcpNodeConfig, WriteOp};
    const MODE: [u8; 16] = [9; 16];
    const TIME: Duration = Duration::from_secs(5);
    fn write(id: u8, seq: u64) -> D1WriteRequest {
        D1WriteRequest {
            shard: 7,
            epoch: 1,
            request_id: [id; 16],
            expected_sequence: Some(seq),
            operations: vec![WriteOp::put(b"key", [id])],
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn managed_activation_orders_with_old_writes_and_cas_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let node = TcpNode::start(TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: dir.path().into(),
        })
        .await
        .unwrap();
        node.bootstrap(std::collections::BTreeMap::from([(
            1,
            node.raft_addr.clone(),
        )]))
        .await
        .unwrap();
        tokio::time::timeout(TIME, async {
            loop {
                if node.raft.ensure_linearizable().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        node.raft
            .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 1 }))
            .await
            .unwrap();
        let service =
            GraphService::start_identified(&node, "127.0.0.1:0".parse().unwrap(), 10, 7, TIME)
                .await
                .unwrap();
        let endpoint = service.local_addr();
        let identity = RemoteIdentity { group: 10, node: 1 };
        let enable = |sequence| Request::Enable {
            shard: 7,
            epoch: 1,
            expected_sequence: sequence,
            mode_id: MODE,
        };
        let (old, activation) = tokio::join!(
            write_identified(endpoint, identity, write(1, 0), TIME),
            request(endpoint, identity, enable(0), TIME),
        );
        let baseline = match (old, activation) {
            (
                Ok(D1Result::Write { sequence: 1, .. }),
                Err(RemoteError::Application(D1Error::Conflict {
                    expected: 0,
                    current: 1,
                })),
            ) => {
                request(endpoint, identity, enable(1), TIME).await.unwrap();
                1
            }
            (
                Err(RemoteError::Application(D1Error::ManagedWriteRequired)),
                Ok(Output::Enabled {
                    activated_sequence: 0,
                    ..
                }),
            ) => 0,
            other => panic!("invalid activation/write ordering: {other:?}"),
        };
        assert!(matches!(
            write_identified(endpoint, identity, write(2, baseline), TIME).await,
            Err(RemoteError::Application(D1Error::ManagedWriteRequired))
        ));
        let mut handles = Vec::new();
        for id in 2..=5 {
            handles.push(tokio::spawn(request(
                endpoint,
                identity,
                Request::Write {
                    mode_id: MODE,
                    request: write(id, baseline),
                },
                TIME,
            )));
        }
        let mut committed = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(Output::Written {
                    sequence,
                    status: D1WriteStatus::Committed,
                    applied_operations: 1,
                    ..
                }) => {
                    assert_eq!(sequence, baseline + 1);
                    committed += 1;
                }
                Err(RemoteError::Application(D1Error::Conflict { expected, current })) => {
                    assert_eq!(expected, baseline);
                    assert_eq!(current, baseline + 1);
                }
                other => panic!("unexpected managed transaction result: {other:?}"),
            }
        }
        assert_eq!(committed, 1);
        let mut known = 0;
        for id in 2..=5 {
            match request(
                endpoint,
                identity,
                Request::Status {
                    shard: 7,
                    epoch: 1,
                    mode_id: MODE,
                    request_id: [id; 16],
                },
                TIME,
            )
            .await
            .unwrap()
            {
                Output::Status(D1ManagedStatus::Committed { sequence }) => {
                    assert_eq!(sequence, baseline + 1);
                    known += 1;
                }
                Output::Status(D1ManagedStatus::Unknown) => {}
                other => panic!("unexpected status: {other:?}"),
            }
        }
        assert_eq!(known, 1);
        assert_eq!(
            request(
                endpoint,
                identity,
                Request::Inspect { shard: 7, epoch: 1 },
                TIME
            )
            .await
            .unwrap(),
            Output::Mode(Some(D1ManagedMode {
                mode_id: MODE,
                activated_sequence: baseline
            }))
        );
        service.shutdown().await;
        node.shutdown().await;
    }
}
