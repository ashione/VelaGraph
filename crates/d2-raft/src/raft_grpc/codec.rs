//! Wire-only conversion. Durable JSON log/checkpoint encodings are unchanged.
use super::wire as w;
use crate::{
    managed_rpc::grpc_codec::pb as m, D1Command, D1WriteRequest, LogCommand, TypeConfig, WriteOp,
};
use openraft::{BasicNode, Entry, EntryPayload, LogId, Membership, StoredMembership, Vote};
use std::collections::{BTreeMap, BTreeSet};
use tonic::Status;
fn bad() -> Status {
    Status::invalid_argument("invalid raft protobuf")
}
fn req<T>(v: Option<T>) -> Result<T, Status> {
    v.ok_or_else(bad)
}
fn cv<T, U: TryFrom<T>>(v: T) -> Result<U, Status> {
    U::try_from(v).map_err(|_| bad())
}
fn arr<const N: usize>(v: Vec<u8>) -> Result<[u8; N], Status> {
    v.try_into().map_err(|_| bad())
}
impl From<Vote<u64>> for w::Vote {
    fn from(v: Vote<u64>) -> Self {
        Self {
            term: v.leader_id.term,
            node: v.leader_id.node_id,
            committed: v.committed,
        }
    }
}
impl From<w::Vote> for Vote<u64> {
    fn from(v: w::Vote) -> Self {
        Self {
            leader_id: openraft::LeaderId::new(v.term, v.node),
            committed: v.committed,
        }
    }
}
impl From<LogId<u64>> for w::LogId {
    fn from(v: LogId<u64>) -> Self {
        Self {
            term: v.leader_id.term,
            node: v.leader_id.node_id,
            index: v.index,
        }
    }
}
impl From<w::LogId> for LogId<u64> {
    fn from(v: w::LogId) -> Self {
        Self::new(openraft::CommittedLeaderId::new(v.term, v.node), v.index)
    }
}
impl From<Membership<u64, BasicNode>> for w::Membership {
    fn from(v: Membership<u64, BasicNode>) -> Self {
        Self {
            configurations: v
                .get_joint_config()
                .iter()
                .map(|c| w::Voters {
                    ids: c.iter().copied().collect(),
                })
                .collect(),
            nodes: v
                .nodes()
                .map(|(&id, n)| w::Node {
                    id,
                    address: n.addr.clone(),
                })
                .collect(),
        }
    }
}
impl TryFrom<w::Membership> for Membership<u64, BasicNode> {
    type Error = Status;
    fn try_from(v: w::Membership) -> Result<Self, Status> {
        let mut nodes = BTreeMap::new();
        for n in v.nodes {
            if nodes.insert(n.id, BasicNode { addr: n.address }).is_some() {
                return Err(bad());
            }
        }
        let mut cfg = Vec::new();
        for c in v.configurations {
            let count = c.ids.len();
            let c: BTreeSet<_> = c.ids.into_iter().collect();
            if c.len() != count || c.iter().any(|id| !nodes.contains_key(id)) {
                return Err(bad());
            }
            cfg.push(c);
        }
        Ok(Membership::new(cfg, nodes))
    }
}
// Raft transports application commands, including commands rejected during apply.
// Do not run D1WriteRequest::validate here: that would change consensus log semantics.
fn write(v: m::D1WriteRequest) -> Result<D1WriteRequest, Status> {
    Ok(D1WriteRequest {
        shard: v.shard,
        epoch: v.epoch,
        request_id: arr(v.request_id)?,
        expected_sequence: v.expected_sequence,
        operations: v.operations.into_iter().map(op).collect::<Result<_, _>>()?,
    })
}
fn op(v: m::WriteOp) -> Result<WriteOp, Status> {
    Ok(match req(v.kind)? {
        m::write_op::Kind::Put(p) => WriteOp::Put {
            key: p.key,
            value: p.value,
        },
        m::write_op::Kind::Delete(key) => WriteOp::Delete { key },
    })
}
fn target(shard: u64, epoch: u64, mode: [u8; 16]) -> Option<m::Target> {
    Some(m::Target {
        shard,
        epoch,
        mode: mode.to_vec(),
    })
}
impl From<LogCommand> for w::LogCommand {
    fn from(v: LogCommand) -> Self {
        use w::log_command::Kind;
        Self {
            kind: Some(match v {
                LogCommand::Noop => Kind::Noop(w::Empty {}),
                LogCommand::TxnBatch(ops) => Kind::Batch(w::Batch {
                    operations: ops.into_iter().map(Into::into).collect(),
                }),
                LogCommand::D1V1(v) => Kind::D1(v.into()),
            }),
        }
    }
}
impl TryFrom<w::LogCommand> for LogCommand {
    type Error = Status;
    fn try_from(v: w::LogCommand) -> Result<Self, Status> {
        use w::log_command::Kind;
        Ok(match req(v.kind)? {
            Kind::Noop(_) => Self::Noop,
            Kind::Batch(b) => {
                Self::TxnBatch(b.operations.into_iter().map(op).collect::<Result<_, _>>()?)
            }
            Kind::D1(v) => Self::D1V1(v.try_into()?),
        })
    }
}
impl From<D1Command> for w::D1Command {
    fn from(v: D1Command) -> Self {
        use w::d1_command::Kind as K;
        use D1Command::*;
        Self {
            kind: Some(match v {
                Activate { shard, epoch } => K::Activate(m::Target {
                    shard,
                    epoch,
                    mode: vec![],
                }),
                SetEpoch {
                    shard,
                    expected_epoch,
                    new_epoch,
                } => K::SetEpoch(w::EpochChange {
                    shard,
                    expected_epoch,
                    new_epoch,
                }),
                SetDedupCapacity {
                    shard,
                    epoch,
                    expected_bytes,
                    new_bytes,
                } => K::SetCapacity(w::CapacityChange {
                    shard,
                    epoch,
                    expected_bytes: expected_bytes as u64,
                    new_bytes: new_bytes as u64,
                }),
                Write(v) => K::Write(v.into()),
                EnableVersionedReads {
                    shard,
                    epoch,
                    mode_id,
                    journal,
                } => K::EnableReads(w::EnableReads {
                    target: target(shard, epoch, mode_id),
                    journal: Some(journal.into()),
                }),
                EnableByteBoundReads {
                    shard,
                    epoch,
                    mode_id,
                    journal,
                    generation_nonce,
                } => K::EnableByteReads(w::EnableByteReads {
                    target: target(shard, epoch, mode_id),
                    journal: Some(journal.into()),
                    generation_nonce: generation_nonce.to_vec(),
                }),
                AdvanceVisibility {
                    authority,
                    shard,
                    epoch,
                    mode_id,
                    ordinal,
                    journal,
                } => K::AdvanceVisibility(m::Advance {
                    authority: Some(authority.into()),
                    target: target(shard, epoch, mode_id),
                    ordinal,
                    journal: Some(journal.into()),
                }),
                SyncHistory {
                    authority,
                    shard,
                    epoch,
                    mode_id,
                    page,
                } => K::SyncHistory(m::SyncHistory {
                    authority: Some(authority.into()),
                    target: target(shard, epoch, mode_id),
                    page: Some((*page).into()),
                }),
                AdvanceRetained {
                    authority,
                    shard,
                    epoch,
                    mode_id,
                    ordinal,
                    cut,
                } => K::AdvanceRetained(m::AdvanceRetained {
                    authority: Some(authority.into()),
                    target: target(shard, epoch, mode_id),
                    ordinal,
                    cut: Some(cut.into()),
                }),
                EnableParticipant {
                    shard,
                    epoch,
                    mode_id,
                    authority,
                } => K::EnableParticipant(w::EnableParticipant {
                    target: target(shard, epoch, mode_id),
                    authority: Some(authority.into()),
                }),
                PrepareRetained(v) => K::PrepareRetained((*v).into()),
                PrepareParticipant {
                    authority,
                    participant,
                    request,
                } => K::PrepareParticipant(w::Prepare {
                    authority: Some(authority.into()),
                    participant: Some(participant.into()),
                    request: Some(request.into()),
                }),
                ResolveParticipant {
                    authority,
                    participant,
                    id,
                    decision,
                } => K::ResolveParticipant(w::Resolve {
                    authority: Some(authority.into()),
                    participant: Some(participant.into()),
                    id: id.to_vec(),
                    decision: match decision {
                        crate::transaction_decisions::Decision::Commit => 1,
                        crate::transaction_decisions::Decision::Abort => 2,
                    },
                }),
                EnableCoordinator {
                    shard,
                    epoch,
                    mode_id,
                } => K::EnableCoordinator(target(shard, epoch, mode_id).unwrap()),
                Coordinator {
                    shard,
                    epoch,
                    mode_id,
                    command,
                } => K::Coordinator(w::Coordinator {
                    target: target(shard, epoch, mode_id),
                    command: Some(command.into()),
                }),
                EnableManagedWrites {
                    shard,
                    epoch,
                    expected_sequence,
                    mode_id,
                } => K::EnableManaged(w::EnableManaged {
                    target: target(shard, epoch, mode_id),
                    expected_sequence,
                }),
                ManagedWrite { mode_id, request } => K::ManagedWrite(w::ManagedWrite {
                    mode: mode_id.to_vec(),
                    request: Some(request.into()),
                }),
            }),
        }
    }
}
impl TryFrom<w::D1Command> for D1Command {
    type Error = Status;
    fn try_from(v: w::D1Command) -> Result<Self, Status> {
        use w::d1_command::Kind as K;
        Ok(match req(v.kind)? {
            K::Activate(t) => Self::Activate {
                shard: t.shard,
                epoch: t.epoch,
            },
            K::SetEpoch(v) => Self::SetEpoch {
                shard: v.shard,
                expected_epoch: v.expected_epoch,
                new_epoch: v.new_epoch,
            },
            K::SetCapacity(v) => Self::SetDedupCapacity {
                shard: v.shard,
                epoch: v.epoch,
                expected_bytes: cv(v.expected_bytes)?,
                new_bytes: cv(v.new_bytes)?,
            },
            K::Write(v) => Self::Write(write(v)?),
            K::EnableReads(v) => {
                let t = req(v.target)?;
                Self::EnableVersionedReads {
                    shard: t.shard,
                    epoch: t.epoch,
                    mode_id: arr(t.mode)?,
                    journal: cv(req(v.journal)?)?,
                }
            }
            K::EnableByteReads(v) => {
                let t = req(v.target)?;
                Self::EnableByteBoundReads {
                    shard: t.shard,
                    epoch: t.epoch,
                    mode_id: arr(t.mode)?,
                    journal: cv(req(v.journal)?)?,
                    generation_nonce: arr(v.generation_nonce)?,
                }
            }
            K::AdvanceVisibility(v) => {
                let t = req(v.target)?;
                Self::AdvanceVisibility {
                    shard: t.shard,
                    epoch: t.epoch,
                    mode_id: arr(t.mode)?,
                    authority: cv(req(v.authority)?)?,
                    ordinal: v.ordinal,
                    journal: cv(req(v.journal)?)?,
                }
            }
            K::SyncHistory(v) => {
                let t = req(v.target)?;
                Self::SyncHistory {
                    shard: t.shard,
                    epoch: t.epoch,
                    mode_id: arr(t.mode)?,
                    authority: cv(req(v.authority)?)?,
                    page: Box::new(history_page(req(v.page)?)?),
                }
            }
            K::AdvanceRetained(v) => {
                let t = req(v.target)?;
                Self::AdvanceRetained {
                    shard: t.shard,
                    epoch: t.epoch,
                    mode_id: arr(t.mode)?,
                    authority: cv(req(v.authority)?)?,
                    ordinal: v.ordinal,
                    cut: cv(req(v.cut)?)?,
                }
            }
            K::EnableParticipant(v) => {
                let t = req(v.target)?;
                Self::EnableParticipant {
                    shard: t.shard,
                    epoch: t.epoch,
                    mode_id: arr(t.mode)?,
                    authority: cv(req(v.authority)?)?,
                }
            }
            K::PrepareRetained(v) => Self::PrepareRetained(Box::new(cv(v)?)),
            K::PrepareParticipant(v) => Self::PrepareParticipant {
                authority: cv(req(v.authority)?)?,
                participant: cv(req(v.participant)?)?,
                request: write(req(v.request)?)?,
            },
            K::ResolveParticipant(v) => Self::ResolveParticipant {
                authority: cv(req(v.authority)?)?,
                participant: cv(req(v.participant)?)?,
                id: arr(v.id)?,
                decision: match v.decision {
                    1 => crate::transaction_decisions::Decision::Commit,
                    2 => crate::transaction_decisions::Decision::Abort,
                    _ => return Err(bad()),
                },
            },
            K::EnableCoordinator(t) => Self::EnableCoordinator {
                shard: t.shard,
                epoch: t.epoch,
                mode_id: arr(t.mode)?,
            },
            K::Coordinator(v) => {
                let t = req(v.target)?;
                Self::Coordinator {
                    shard: t.shard,
                    epoch: t.epoch,
                    mode_id: arr(t.mode)?,
                    command: command(req(v.command)?)?,
                }
            }
            K::EnableManaged(v) => {
                let t = req(v.target)?;
                Self::EnableManagedWrites {
                    shard: t.shard,
                    epoch: t.epoch,
                    mode_id: arr(t.mode)?,
                    expected_sequence: v.expected_sequence,
                }
            }
            K::ManagedWrite(v) => Self::ManagedWrite {
                mode_id: arr(v.mode)?,
                request: write(req(v.request)?)?,
            },
        })
    }
}
impl From<Entry<TypeConfig>> for w::Entry {
    fn from(v: Entry<TypeConfig>) -> Self {
        use w::entry::Payload;
        Self {
            log_id: Some(v.log_id.into()),
            payload: Some(match v.payload {
                EntryPayload::Blank => Payload::Blank(w::Empty {}),
                EntryPayload::Normal(v) => Payload::Command(v.into()),
                EntryPayload::Membership(v) => Payload::Membership(v.into()),
            }),
        }
    }
}
impl TryFrom<w::Entry> for Entry<TypeConfig> {
    type Error = Status;
    fn try_from(v: w::Entry) -> Result<Self, Status> {
        use w::entry::Payload;
        Ok(Self {
            log_id: req(v.log_id)?.into(),
            payload: match req(v.payload)? {
                Payload::Blank(_) => EntryPayload::Blank,
                Payload::Command(v) => EntryPayload::Normal(v.try_into()?),
                Payload::Membership(v) => EntryPayload::Membership(v.try_into()?),
            },
        })
    }
}
impl From<openraft::SnapshotMeta<u64, BasicNode>> for w::SnapshotMeta {
    fn from(v: openraft::SnapshotMeta<u64, BasicNode>) -> Self {
        Self {
            last_log: v.last_log_id.map(Into::into),
            membership_log: v.last_membership.log_id().map(Into::into),
            membership: Some(v.last_membership.membership().clone().into()),
            id: v.snapshot_id,
        }
    }
}
impl TryFrom<w::SnapshotMeta> for openraft::SnapshotMeta<u64, BasicNode> {
    type Error = Status;
    fn try_from(v: w::SnapshotMeta) -> Result<Self, Status> {
        Ok(Self {
            last_log_id: v.last_log.map(Into::into),
            last_membership: StoredMembership::new(
                v.membership_log.map(Into::into),
                req(v.membership)?.try_into()?,
            ),
            snapshot_id: v.id,
        })
    }
}

// Unlike the public managed operation, a replicated coordinator command may
// legitimately fail at apply. Preserve its exact shape rather than prevalidate.
fn cohort(v: m::Cohort) -> Result<crate::transaction_decisions::Cohort, Status> {
    if v.members.len() > crate::transaction_decisions::MAX_PARTICIPANTS {
        return Err(bad());
    }
    Ok(crate::transaction_decisions::Cohort {
        coordinator_shard: v.coordinator_shard,
        coordinator_epoch: v.coordinator_epoch,
        members: v.members.into_iter().map(cv).collect::<Result<_, _>>()?,
        profile: cv(m::CapacityProfile::try_from(v.profile).map_err(|_| bad())?)?,
    })
}
fn command(v: m::Command) -> Result<crate::transaction_decisions::Command, Status> {
    use crate::transaction_decisions::Command as C;
    use m::command::Kind as K;
    Ok(match req(v.kind)? {
        K::BindCohort(v) => C::BindCohort { cohort: cohort(v)? },
        K::ConfirmCohortCapacity(v) => C::ConfirmCohortCapacity {
            cohort: cohort(req(v.cohort)?)?,
            grants: limited(v.grants)?
                .into_iter()
                .map(grant)
                .collect::<Result<_, _>>()?,
        },
        K::AdmitRequest(v) => C::AdmitRequest {
            id: arr(v.id)?,
            request: v.request,
            payload: v.payload,
            transaction_id: arr(v.transaction_id)?,
            participants: limited(v.participants)?
                .into_iter()
                .map(cv)
                .collect::<Result<_, _>>()?,
        },
        K::Begin(v) => C::Begin {
            id: arr(v.id)?,
            participants: limited(v.participants)?
                .into_iter()
                .map(cv)
                .collect::<Result<_, _>>()?,
        },
        K::Prepared(v) => C::Prepared {
            id: arr(v.id)?,
            participant: cv(req(v.participant)?)?,
        },
        K::Decide(v) => C::Decide {
            id: arr(v.id)?,
            decision: match v.decision {
                1 => crate::transaction_decisions::Decision::Commit,
                2 => crate::transaction_decisions::Decision::Abort,
                _ => return Err(bad()),
            },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_application_commands_remain_replicable() {
        let request = D1WriteRequest {
            shard: 0,
            epoch: 0,
            request_id: [0; 16],
            expected_sequence: None,
            operations: vec![],
        };
        let cmds = vec![
            D1Command::Write(request.clone()),
            D1Command::ManagedWrite {
                mode_id: [0; 16],
                request,
            },
            D1Command::Coordinator {
                shard: 0,
                epoch: 0,
                mode_id: [0; 16],
                command: crate::transaction_decisions::Command::BindCohort {
                    cohort: crate::transaction_decisions::Cohort {
                        coordinator_shard: 0,
                        coordinator_epoch: 0,
                        members: vec![],
                        profile: crate::transaction_decisions::CapacityProfile::LegacyCountV1,
                    },
                },
            },
        ];
        for cmd in cmds {
            let wire = w::D1Command::from(cmd.clone());
            assert_eq!(D1Command::try_from(wire).unwrap(), cmd)
        }
    }
    #[test]
    fn membership_preserves_joint_voters_and_learners() {
        let m = Membership::new(
            vec![BTreeSet::from([1, 2]), BTreeSet::from([2, 3])],
            BTreeMap::from([
                (1, BasicNode::new("a")),
                (2, BasicNode::new("b")),
                (3, BasicNode::new("c")),
                (4, BasicNode::new("d")),
            ]),
        );
        assert_eq!(
            Membership::try_from(w::Membership::from(m.clone())).unwrap(),
            m
        );
    }
    #[test]
    fn missing_entry_payload_is_not_blank() {
        assert!(Entry::<TypeConfig>::try_from(w::Entry {
            log_id: Some(w::LogId::default()),
            payload: None
        })
        .is_err());
    }
}
#[cfg(test)]
mod full_command_tests {
    use super::*;
    use crate::{
        participant_writes::Authority,
        transaction_decisions::{
            CapacityProfile, Cohort, Command, Decision, HistoryCursor, HistoryPage, Journal,
            Member, Participant,
        },
    };
    #[test]
    fn every_d1_variant_roundtrips_without_changing_durable_identity() {
        let authority = Authority {
            shard: 10,
            mode: [1; 16],
        };
        let cohort = Cohort {
            coordinator_shard: 10,
            coordinator_epoch: 1,
            members: vec![Member {
                shard: 20,
                epoch: 1,
                mode: [2; 16],
            }],
            profile: CapacityProfile::LegacyCountV1,
        };
        let cut = HistoryCursor::empty(authority.clone(), cohort.clone()).unwrap();
        let journal = Journal::new([1; 16]).unwrap();
        let participant = Participant {
            shard: 20,
            epoch: 1,
            mode: [2; 16],
            intent: [3; 32],
        };
        let request = D1WriteRequest {
            shard: 20,
            epoch: 1,
            request_id: [4; 16],
            expected_sequence: Some(0),
            operations: vec![WriteOp::put([0, 255], [1, 2]), WriteOp::delete([7, 8])],
        };
        let variants = vec![
            D1Command::Activate {
                shard: 20,
                epoch: 1,
            },
            D1Command::SetEpoch {
                shard: 20,
                expected_epoch: 1,
                new_epoch: 2,
            },
            D1Command::SetDedupCapacity {
                shard: 20,
                epoch: 1,
                expected_bytes: 1024,
                new_bytes: 2048,
            },
            D1Command::Write(request.clone()),
            D1Command::EnableVersionedReads {
                shard: 20,
                epoch: 1,
                mode_id: [2; 16],
                journal: journal.clone(),
            },
            D1Command::EnableByteBoundReads {
                shard: 20,
                epoch: 1,
                mode_id: [2; 16],
                journal: journal.clone(),
                generation_nonce: [9; 32],
            },
            D1Command::AdvanceVisibility {
                authority: authority.clone(),
                shard: 20,
                epoch: 1,
                mode_id: [2; 16],
                ordinal: 0,
                journal,
            },
            D1Command::SyncHistory {
                authority: authority.clone(),
                shard: 20,
                epoch: 1,
                mode_id: [2; 16],
                page: Box::new(HistoryPage {
                    version: 1,
                    base: cut.clone(),
                    end: cut.clone(),
                    cut: cut.clone(),
                    events: vec![],
                }),
            },
            D1Command::AdvanceRetained {
                authority: authority.clone(),
                shard: 20,
                epoch: 1,
                mode_id: [2; 16],
                ordinal: 0,
                cut: cut.clone(),
            },
            D1Command::EnableParticipant {
                shard: 20,
                epoch: 1,
                mode_id: [2; 16],
                authority: authority.clone(),
            },
            D1Command::PrepareRetained(Box::new(crate::retained_prepare::RetainedPrepare {
                version: 1,
                authority: authority.clone(),
                participant: participant.clone(),
                request: request.clone(),
                cut,
            })),
            D1Command::PrepareParticipant {
                authority: authority.clone(),
                participant: participant.clone(),
                request: request.clone(),
            },
            D1Command::ResolveParticipant {
                authority,
                participant,
                id: [4; 16],
                decision: Decision::Abort,
            },
            D1Command::EnableCoordinator {
                shard: 10,
                epoch: 1,
                mode_id: [1; 16],
            },
            D1Command::Coordinator {
                shard: 10,
                epoch: 1,
                mode_id: [1; 16],
                command: Command::Decide {
                    id: [4; 16],
                    decision: Decision::Commit,
                },
            },
            D1Command::EnableManagedWrites {
                shard: 20,
                epoch: 1,
                expected_sequence: 0,
                mode_id: [2; 16],
            },
            D1Command::ManagedWrite {
                mode_id: [2; 16],
                request,
            },
        ];
        for v in variants {
            let bytes = serde_json::to_vec(&v).unwrap();
            let encoded = prost::Message::encode_to_vec(&w::D1Command::from(v.clone()));
            let dto = <w::D1Command as prost::Message>::decode(encoded.as_slice()).unwrap();
            let next = D1Command::try_from(dto).unwrap();
            assert_eq!(next, v);
            assert_eq!(serde_json::to_vec(&next).unwrap(), bytes)
        }
    }
}

fn limited<T>(v: Vec<T>) -> Result<Vec<T>, Status> {
    if v.len() > crate::transaction_decisions::MAX_PARTICIPANTS {
        Err(bad())
    } else {
        Ok(v)
    }
}
fn grant(
    v: m::MemberCapacityGrant,
) -> Result<crate::transaction_decisions::MemberCapacityGrant, Status> {
    let a = req(v.activation)?;
    let l = req(a.log_id)?;
    Ok(crate::transaction_decisions::MemberCapacityGrant {
        member: cv(req(v.member)?)?,
        activation: crate::transaction_decisions::CapacityActivation {
            nonce: arr(a.nonce)?,
            log_id: LogId::new(openraft::CommittedLeaderId::new(l.term, l.node), l.index),
        },
    })
}
fn history_page(v: m::HistoryPage) -> Result<crate::transaction_decisions::HistoryPage, Status> {
    let result = crate::transaction_decisions::HistoryPage {
        version: v.version,
        base: cv(req(v.base)?)?,
        end: cv(req(v.end)?)?,
        cut: cv(req(v.cut)?)?,
        events: v
            .events
            .into_iter()
            .map(command)
            .collect::<Result<_, _>>()?,
    };
    result.validate().map_err(|_| bad())?;
    Ok(result)
}
#[cfg(test)]
mod legacy_acceptance_tests {
    use super::*;
    use crate::{participant_writes::Authority, transaction_decisions as t};
    fn check(v: D1Command) {
        let json = serde_json::to_vec(&v).unwrap();
        let old: Result<D1Command, _> = serde_json::from_slice(&json);
        let new = D1Command::try_from(w::D1Command::from(v));
        assert_eq!(old.is_ok(), new.is_ok());
        if let Ok(old) = old {
            assert_eq!(old, new.unwrap());
        }
    }
    #[test]
    fn invalid_capacity_activation_is_an_application_rejection() {
        let cohort = t::Cohort {
            coordinator_shard: 10,
            coordinator_epoch: 1,
            members: vec![],
            profile: t::CapacityProfile::LegacyCountV1,
        };
        check(D1Command::Coordinator {
            shard: 10,
            epoch: 1,
            mode_id: [1; 16],
            command: t::Command::ConfirmCohortCapacity {
                cohort,
                grants: vec![t::MemberCapacityGrant {
                    member: t::Member {
                        shard: 20,
                        epoch: 1,
                        mode: [2; 16],
                    },
                    activation: t::CapacityActivation {
                        nonce: [0; 32],
                        log_id: LogId::new(openraft::CommittedLeaderId::new(0, 0), 0),
                    },
                }],
            },
        });
    }
    #[test]
    fn history_page_retains_structurally_valid_but_unreplayed_events() {
        let authority = Authority {
            shard: 10,
            mode: [1; 16],
        };
        let cohort = t::Cohort {
            coordinator_shard: 10,
            coordinator_epoch: 1,
            members: vec![t::Member {
                shard: 20,
                epoch: 1,
                mode: [2; 16],
            }],
            profile: t::CapacityProfile::LegacyCountV1,
        };
        let base = t::HistoryCursor::empty(authority.clone(), cohort).unwrap();
        let invalid = t::Command::BindCohort {
            cohort: t::Cohort {
                coordinator_shard: 10,
                coordinator_epoch: 1,
                members: vec![],
                profile: t::CapacityProfile::LegacyCountV1,
            },
        };
        let end = base.extend(std::slice::from_ref(&invalid)).unwrap();
        check(D1Command::SyncHistory {
            authority,
            shard: 20,
            epoch: 1,
            mode_id: [2; 16],
            page: Box::new(t::HistoryPage {
                version: 1,
                base,
                end: end.clone(),
                cut: end,
                events: vec![invalid],
            }),
        });
    }
}
