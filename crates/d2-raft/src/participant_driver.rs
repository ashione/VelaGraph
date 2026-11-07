//! Trusted in-process decision relay. No global visibility or authenticated RPC.
//! Timeouts are uncertain outcomes: never infer abort or choose a new identity.
use crate::participant_writes::{Authority, Status};
use crate::transaction_decisions::{Command, Decision, Participant};
use crate::{D1Command, D1Error, D1Result, D1WriteRequest, LogCommand, NodeHandle};
use std::time::Duration;

#[derive(Debug)]
pub enum Error {
    Unavailable,
    Application(D1Error),
    Unknown,
    Unresolved,
    Identity,
    Unexpected,
}
const DEADLINE: Duration = Duration::from_secs(5);

async fn submit(node: &NodeHandle, command: D1Command) -> Result<D1Result, Error> {
    let response =
        tokio::time::timeout(DEADLINE, node.raft.client_write(LogCommand::D1V1(command)))
            .await
            .map_err(|_| Error::Unavailable)?
            .map_err(|_| Error::Unavailable)?;
    response
        .data
        .d1
        .ok_or(Error::Unexpected)?
        .map_err(Error::Application)
}

async fn transaction(
    coordinator: &NodeHandle,
    authority: &Authority,
    epoch: u64,
    id: [u8; 16],
) -> Result<crate::transaction_decisions::Transaction, Error> {
    tokio::time::timeout(DEADLINE, coordinator.raft.ensure_linearizable())
        .await
        .map_err(|_| Error::Unavailable)?
        .map_err(|_| Error::Unavailable)?;
    let journal = coordinator
        .state_machine
        .coordinator_journal(authority.shard, epoch, authority.mode)
        .await
        .map_err(Error::Application)?;
    journal.transaction(id).cloned().ok_or(Error::Unknown)
}

/// The coordinator Begin must already exist with the exact participant intent.
/// A Prepared response is acknowledged in the coordinator only after Raft applies
/// the full participant intent. Caller must resolve after any uncertain response.
pub async fn prepare(
    coordinator: &NodeHandle,
    participant_node: &NodeHandle,
    authority: Authority,
    coordinator_epoch: u64,
    participant: Participant,
    request: D1WriteRequest,
) -> Result<Status, Error> {
    let tx = transaction(
        coordinator,
        &authority,
        coordinator_epoch,
        request.request_id,
    )
    .await?;
    if !tx.participants.contains(&participant) {
        return Err(Error::Identity);
    }
    if tx.decision == Some(Decision::Abort) {
        resolve(
            coordinator,
            participant_node,
            authority.clone(),
            coordinator_epoch,
            participant.clone(),
            request.request_id,
        )
        .await?;
    }
    if tx.decision == Some(Decision::Commit)
        && status(participant_node, &participant, request.request_id).await? == Status::Unknown
    {
        return Err(Error::Unknown);
    }
    let status = match submit(
        participant_node,
        D1Command::PrepareParticipant {
            authority: authority.clone(),
            participant: participant.clone(),
            request: request.clone(),
        },
    )
    .await?
    {
        D1Result::Participant(status) => status,
        _ => return Err(Error::Unexpected),
    };
    if tx.decision.is_some() {
        return resolve(
            coordinator,
            participant_node,
            authority,
            coordinator_epoch,
            participant,
            request.request_id,
        )
        .await;
    }
    if status != Status::Prepared {
        return Err(Error::Unexpected);
    }
    submit(
        coordinator,
        D1Command::Coordinator {
            shard: authority.shard,
            epoch: coordinator_epoch,
            mode_id: authority.mode,
            command: Command::Prepared {
                id: request.request_id,
                participant,
            },
        },
    )
    .await?;
    Ok(status)
}

/// Query the authoritative decision using a leader barrier. Unknown/unresolved
/// leaves the durable intent locked. No timer, absence or peer reply can abort it.
pub async fn resolve(
    coordinator: &NodeHandle,
    participant_node: &NodeHandle,
    authority: Authority,
    coordinator_epoch: u64,
    participant: Participant,
    id: [u8; 16],
) -> Result<Status, Error> {
    let tx = transaction(coordinator, &authority, coordinator_epoch, id).await?;
    if !tx.participants.contains(&participant) {
        return Err(Error::Identity);
    }
    let decision = tx.decision.ok_or(Error::Unresolved)?;
    if decision == Decision::Commit && !tx.prepared.iter().all(|p| *p) {
        return Err(Error::Unresolved);
    }
    match submit(
        participant_node,
        D1Command::ResolveParticipant {
            authority,
            participant,
            id,
            decision,
        },
    )
    .await?
    {
        D1Result::Participant(status) => Ok(status),
        _ => Err(Error::Unexpected),
    }
}

/// Linearizable participant status for recovery. Unknown is not Aborted;
/// exact-body Prepare retry is necessary to validate an intended request body.
pub async fn status(
    node: &NodeHandle,
    participant: &Participant,
    id: [u8; 16],
) -> Result<Status, Error> {
    tokio::time::timeout(DEADLINE, node.raft.ensure_linearizable())
        .await
        .map_err(|_| Error::Unavailable)?
        .map_err(|_| Error::Unavailable)?;
    node.state_machine
        .participant_status(participant.shard, participant.epoch, participant.mode, id)
        .await
        .map_err(Error::Application)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cluster, WriteOp};
    use std::collections::BTreeMap;
    async fn start(
        cluster: &Cluster,
        shard: u64,
        mode: [u8; 16],
        authority: &Authority,
        coordinator: bool,
    ) -> NodeHandle {
        let node = cluster.in_group(shard).start(1).await.unwrap();
        node.raft
            .initialize(BTreeMap::from([(
                1,
                openraft::BasicNode {
                    addr: format!("inproc://{shard}/1"),
                },
            )]))
            .await
            .unwrap();
        node.raft
            .wait(Some(DEADLINE))
            .metrics(
                |m| m.current_leader == Some(1) && m.last_applied.is_some(),
                "leader ready",
            )
            .await
            .unwrap();
        submit(&node, D1Command::Activate { shard, epoch: 1 })
            .await
            .unwrap();
        if !coordinator {
            submit(
                &node,
                D1Command::Write(D1WriteRequest {
                    shard,
                    epoch: 1,
                    request_id: [1; 16],
                    expected_sequence: Some(0),
                    operations: vec![WriteOp::put("x", "old")],
                }),
            )
            .await
            .unwrap();
        }
        submit(
            &node,
            D1Command::EnableManagedWrites {
                shard,
                epoch: 1,
                expected_sequence: u64::from(!coordinator),
                mode_id: mode,
            },
        )
        .await
        .unwrap();
        let enable = if coordinator {
            D1Command::EnableCoordinator {
                shard,
                epoch: 1,
                mode_id: mode,
            }
        } else {
            D1Command::EnableParticipant {
                shard,
                epoch: 1,
                mode_id: mode,
                authority: authority.clone(),
            }
        };
        submit(&node, enable).await.unwrap();
        node
    }
    fn descriptor(shard: u64) -> Participant {
        Participant {
            shard,
            epoch: 1,
            mode: [shard as u8; 16],
            intent: [shard as u8; 32],
        }
    }
    fn request(shard: u64) -> D1WriteRequest {
        D1WriteRequest {
            shard,
            epoch: 1,
            request_id: [6; 16],
            expected_sequence: Some(1),
            operations: vec![WriteOp::put("x", "new")],
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_participants_relay_committed_decision_without_claiming_atomic_visibility() {
        let cluster = Cluster::new();
        let authority = Authority {
            shard: 9,
            mode: [9; 16],
        };
        let coordinator = start(&cluster, 9, [9; 16], &authority, true).await;
        let first = start(&cluster, 7, [7; 16], &authority, false).await;
        let second = start(&cluster, 8, [8; 16], &authority, false).await;
        submit(
            &coordinator,
            D1Command::Coordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
                command: Command::Begin {
                    id: [6; 16],
                    participants: vec![descriptor(7), descriptor(8)],
                },
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            resolve(
                &coordinator,
                &first,
                authority.clone(),
                1,
                descriptor(7),
                [6; 16]
            )
            .await,
            Err(Error::Unresolved)
        ));
        let old = first
            .state_machine
            .d1_read_snapshot(10, 100)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            prepare(
                &coordinator,
                &first,
                authority.clone(),
                1,
                descriptor(7),
                request(7)
            )
            .await
            .unwrap(),
            Status::Prepared
        );
        assert_eq!(
            prepare(
                &coordinator,
                &second,
                authority.clone(),
                1,
                descriptor(8),
                request(8)
            )
            .await
            .unwrap(),
            Status::Prepared
        );
        assert_eq!(first.state_machine.get("x").await.as_deref(), Some("old"));
        submit(
            &coordinator,
            D1Command::Coordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
                command: Command::Decide {
                    id: [6; 16],
                    decision: Decision::Commit,
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(
            resolve(
                &coordinator,
                &first,
                authority.clone(),
                1,
                descriptor(7),
                [6; 16]
            )
            .await
            .unwrap(),
            Status::Committed { sequence: 2 }
        );
        assert_eq!(first.state_machine.get("x").await.as_deref(), Some("new"));
        assert_eq!(second.state_machine.get("x").await.as_deref(), Some("old")); // no global visibility guarantee
        assert_eq!(old.entries.get(b"x".as_slice()).unwrap(), b"old");
        assert_eq!(
            resolve(
                &coordinator,
                &second,
                authority.clone(),
                1,
                descriptor(8),
                [6; 16]
            )
            .await
            .unwrap(),
            Status::Committed { sequence: 2 }
        );
        let mut changed = request(7);
        changed.operations = vec![WriteOp::put("x", "wrong")];
        assert!(matches!(
            prepare(
                &coordinator,
                &first,
                authority.clone(),
                1,
                descriptor(7),
                changed
            )
            .await,
            Err(Error::Application(D1Error::IdempotencyConflict))
        ));
        assert_eq!(
            prepare(
                &coordinator,
                &first,
                authority.clone(),
                1,
                descriptor(7),
                request(7)
            )
            .await
            .unwrap(),
            Status::Committed { sequence: 2 }
        );
        assert_eq!(
            status(&first, &descriptor(7), [6; 16]).await.unwrap(),
            Status::Committed { sequence: 2 }
        );
        for node in [&first, &second, &coordinator] {
            node.raft.shutdown().await.unwrap();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_conditional_prepares_have_one_winner_without_blocking_reads() {
        let cluster = Cluster::new();
        let authority = Authority {
            shard: 9,
            mode: [9; 16],
        };
        let coordinator = start(&cluster, 9, [9; 16], &authority, true).await;
        let node = start(&cluster, 7, [7; 16], &authority, false).await;
        for id in [6, 7] {
            submit(
                &coordinator,
                D1Command::Coordinator {
                    shard: 9,
                    epoch: 1,
                    mode_id: [9; 16],
                    command: Command::Begin {
                        id: [id; 16],
                        participants: vec![descriptor(7)],
                    },
                },
            )
            .await
            .unwrap();
        }
        let first = request(7);
        let mut second = first.clone();
        second.request_id = [7; 16];
        let (a, b) = tokio::join!(
            prepare(
                &coordinator,
                &node,
                authority.clone(),
                1,
                descriptor(7),
                first
            ),
            prepare(
                &coordinator,
                &node,
                authority.clone(),
                1,
                descriptor(7),
                second
            )
        );
        let winner = match (a, b) {
            (Ok(Status::Prepared), Err(Error::Application(D1Error::ParticipantBusy))) => 6,
            (Err(Error::Application(D1Error::ParticipantBusy)), Ok(Status::Prepared)) => 7,
            outcomes => panic!("unexpected prepare outcomes: {outcomes:?}"),
        };
        assert_eq!(node.state_machine.get("x").await.as_deref(), Some("old"));
        let loser = if winner == 6 { 7 } else { 6 };
        submit(
            &coordinator,
            D1Command::Coordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
                command: Command::Decide {
                    id: [loser; 16],
                    decision: Decision::Abort,
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(
            resolve(
                &coordinator,
                &node,
                authority.clone(),
                1,
                descriptor(7),
                [loser; 16]
            )
            .await
            .unwrap(),
            Status::Aborted
        );
        submit(
            &coordinator,
            D1Command::Coordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
                command: Command::Decide {
                    id: [winner; 16],
                    decision: Decision::Commit,
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(
            resolve(
                &coordinator,
                &node,
                authority,
                1,
                descriptor(7),
                [winner; 16]
            )
            .await
            .unwrap(),
            Status::Committed { sequence: 2 }
        );
        node.raft.shutdown().await.unwrap();
        coordinator.raft.shutdown().await.unwrap();
    }
}
