//! Real managed v5 TCP member operations over durable Raft, with the member's
//! actual server/apply workers forbidding local full-Journal materialization.
use super::*;
use crate::managed_rpc::{Client, Error, Identity, NodeRef, Role, Server};
use crate::participant_writes::{Authority, Status};
use crate::request_plans::{Part, Plan};
use crate::transaction_decisions::{
    CapacityProfile, Cohort, Command, Decision, Journal, Member, Participant,
};
use crate::versioned_reads::Target;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn actual_tcp_byte_member_uses_compact_history_and_original_plan() {
    let _resources = crate::managed_rpc::test_resource_guard().await;
    let authority = Authority {
        shard: 9,
        mode: [9; 16],
    };
    let target = Target {
        shard: 7,
        epoch: 1,
        mode: [7; 16],
    };
    let cohort = Cohort {
        profile: CapacityProfile::ByteBound16MiBV1,
        coordinator_shard: 9,
        coordinator_epoch: 1,
        members: vec![Member {
            shard: target.shard,
            epoch: target.epoch,
            mode: target.mode,
        }],
    };
    let identity = Identity {
        group: 702,
        node: 1,
        target: target.clone(),
        role: Role::Participant,
        authority: authority.clone(),
        cohort: cohort.clone(),
    };
    let dir = tempfile::tempdir().unwrap();
    let cluster = crate::Cluster::new().in_group(identity.group);
    let node = Arc::new(
        cluster
            .start_durable(identity.node, dir.path())
            .await
            .unwrap(),
    );
    node.raft
        .initialize(BTreeMap::from([(identity.node, BasicNode::default())]))
        .await
        .unwrap();
    node.raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics.current_leader == Some(identity.node) && metrics.last_applied.is_some()
            },
            "member checkpoint TCP leader",
        )
        .await
        .unwrap();
    for command in [
        D1Command::Activate {
            shard: target.shard,
            epoch: target.epoch,
        },
        D1Command::EnableManagedWrites {
            shard: target.shard,
            epoch: target.epoch,
            expected_sequence: 0,
            mode_id: target.mode,
        },
        D1Command::EnableParticipant {
            shard: target.shard,
            epoch: target.epoch,
            mode_id: target.mode,
            authority: authority.clone(),
        },
    ] {
        node.raft
            .client_write(LogCommand::D1V1(command))
            .await
            .unwrap()
            .data
            .d1
            .unwrap()
            .unwrap();
    }
    let original_grant =
        crate::managed_rpc::activate_byte_bound_member(&node, &authority, &target, &cohort)
            .await
            .unwrap();
    {
        let state = node.state_machine.state.read().await;
        let visibility = state.d1.as_ref().unwrap().visibility.as_ref().unwrap();
        assert!(visibility.journal.full().is_none());
        assert!(Arc::ptr_eq(
            visibility.journal.active_arc().unwrap(),
            state.archive_cut.as_ref().unwrap().checkpoint_arc()
        ));
    }
    node.state_machine.forbid_coordinator_journal(true);
    // Use TCP only, retaining the existing test transport deadlines and limits.
    let server = Server::bind(
        node.clone(),
        identity.clone(),
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let client = Client::new(server.local_addr(), identity, Duration::from_secs(2)).unwrap();
    let remote = NodeRef::Remote(&client);
    assert_eq!(
        remote
            .grant_status(&authority, &target, &cohort)
            .await
            .unwrap(),
        original_grant
    );
    assert_eq!(remote.visibility(&target).await.unwrap().applied_prefix, 0);
    let initial = remote
        .history_status(&authority, &target, &cohort)
        .await
        .unwrap();
    assert_eq!(initial.applied_prefix, 0);
    assert_eq!(initial.cursor.event_count, 1);

    let participant = Participant {
        shard: target.shard,
        epoch: target.epoch,
        mode: target.mode,
        intent: [4; 32],
    };
    let request = crate::D1WriteRequest {
        shard: target.shard,
        epoch: target.epoch,
        request_id: [1; 16],
        expected_sequence: Some(0),
        operations: vec![WriteOp::put("network-member", "committed")],
    };
    let mut journal = Journal::new(authority.mode).unwrap();
    journal
        .apply(Command::BindCohort {
            cohort: cohort.clone(),
        })
        .unwrap();
    journal
        .apply(Command::ConfirmCohortCapacity {
            cohort: cohort.clone(),
            grants: vec![original_grant.clone()],
        })
        .unwrap();
    let plan = Plan {
        version: 1,
        id: request.request_id,
        authority: authority.clone(),
        cohort: cohort.clone(),
        validation_version: 0,
        parts: vec![Part {
            descriptor: participant.clone(),
            request: request.clone(),
        }],
    };
    journal
        .apply(Command::AdmitRequest {
            id: [99; 16],
            request: b"original network request".to_vec(),
            payload: plan.encode().unwrap(),
            transaction_id: request.request_id,
            participants: vec![participant.clone()],
        })
        .unwrap();
    let page = journal.history_page(&initial.cursor, None).unwrap();
    let marker = node.state_machine.applied().await.unwrap();
    let synced = remote
        .sync_history(&authority, &target, &page)
        .await
        .unwrap();
    assert_eq!(synced.cursor, page.end);
    assert_eq!(synced.applied_prefix, 0);
    assert!(node.state_machine.applied().await.unwrap().index > marker.index);
    let admitted = journal.clone(); // Explicit original wire evidence, not member materialization.
    assert_eq!(
        remote
            .prepare(&authority, &participant, request.clone(), &admitted)
            .await
            .unwrap(),
        Status::Prepared
    );
    assert_eq!(
        node.state_machine
            .participant_status(target.shard, target.epoch, target.mode, request.request_id)
            .await
            .unwrap(),
        Status::Prepared
    );

    journal
        .apply(Command::Prepared {
            id: request.request_id,
            participant: participant.clone(),
        })
        .unwrap();
    let before_decision = journal.history_cursor().unwrap();
    journal
        .apply(Command::Decide {
            id: request.request_id,
            decision: Decision::Commit,
        })
        .unwrap();
    let page = journal.history_page(&synced.cursor, None).unwrap();
    let cut = page.cut.clone();
    let synced = remote
        .sync_history(&authority, &target, &page)
        .await
        .unwrap();
    assert_eq!(synced.applied_prefix, 0);
    assert_eq!(remote.visibility(&target).await.unwrap().applied_prefix, 0);
    assert_eq!(node.state_machine.get_raw(b"network-member").await, None);
    assert_eq!(
        remote
            .advance_retained(&authority, &target, 1, &cut)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        node.state_machine.get_raw(b"network-member").await,
        Some(b"committed".to_vec())
    );
    assert_eq!(
        remote
            .advance_retained(&authority, &target, 1, &before_decision)
            .await,
        Err(Error::Application(D1Error::VisibilityUnresolved))
    );
    let status = remote
        .history_status(&authority, &target, &cohort)
        .await
        .unwrap();
    assert_eq!(status.cursor, cut);
    assert_eq!(status.applied_prefix, 1);
    assert_eq!(remote.visibility(&target).await.unwrap().applied_prefix, 1);
    // Managed v5 has no separate participant-status operation. The exact old
    // admission retry returns terminal status through its existing Prepare RPC.
    assert_eq!(
        remote
            .prepare(&authority, &participant, request.clone(), &admitted)
            .await
            .unwrap(),
        Status::Committed { sequence: 1 }
    );
    assert_eq!(
        node.state_machine
            .participant_status(target.shard, target.epoch, target.mode, request.request_id)
            .await
            .unwrap(),
        Status::Committed { sequence: 1 }
    );
    let mut changed = request;
    changed.operations = vec![WriteOp::put("network-member", "wrong plan")];
    // Preserve the existing wire preflight precedence: altered Plan parts fail
    // Identity before they can obtain an idempotent local status.
    assert_eq!(
        remote
            .prepare(&authority, &participant, changed, &admitted)
            .await,
        Err(Error::Identity)
    );
    assert_eq!(
        remote
            .grant_status(&authority, &target, &cohort)
            .await
            .unwrap(),
        original_grant
    );
    assert_eq!(
        node.state_machine.get_raw(b"network-member").await,
        Some(b"committed".to_vec())
    );
    {
        let state = node.state_machine.state.read().await;
        let visibility = state.d1.as_ref().unwrap().visibility.as_ref().unwrap();
        assert!(visibility.journal.full().is_none());
        assert_eq!(visibility.journal.resolved_prefix(), 1);
        assert!(visibility
            .journal
            .active()
            .unwrap()
            .active_transactions()
            .is_empty());
    }
    node.state_machine.forbid_coordinator_journal(false);
    server.shutdown().await;
    let closing = node.state_machine.clone();
    cluster.shutdown(&node).await;
    drop(node);
    close_after_background_owners(closing).await;
}

async fn close_after_background_owners(mut state: Arc<StateMachine>) {
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            match Arc::try_unwrap(state) {
                Ok(state) => {
                    drop(state);
                    return;
                }
                Err(shared) => {
                    state = shared;
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .expect("background state-machine owners must release before fixture teardown");
}
