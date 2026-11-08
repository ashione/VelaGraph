use super::*;
use crate::participant_writes::{State, Status};
use crate::transaction_decisions::{
    CapacityActivation, CapacityProfile, Cohort, Command, Decision, Journal, Member,
    MemberCapacityGrant,
};
use crate::D1Command;

fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn participant() -> Participant {
    Participant {
        shard: 7,
        epoch: 1,
        mode: [7; 16],
        intent: [4; 32],
    }
}
fn request() -> D1WriteRequest {
    D1WriteRequest {
        shard: 7,
        epoch: 1,
        request_id: [2; 16],
        expected_sequence: Some(0),
        operations: vec![WriteOp::put("x", "value")],
    }
}
fn fixture(profile: CapacityProfile, admitted: bool) -> (D1State, Journal, RetainedPrepare) {
    let cohort = Cohort {
        coordinator_shard: 9,
        coordinator_epoch: 1,
        profile,
        members: vec![Member {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
        }],
    };
    let activation = CapacityActivation {
        nonce: [8; 32],
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
    };
    let mut journal = Journal::new([9; 16]).unwrap();
    journal
        .apply(Command::BindCohort {
            cohort: cohort.clone(),
        })
        .unwrap();
    if !profile.is_legacy() {
        journal
            .apply(Command::ConfirmCohortCapacity {
                cohort: cohort.clone(),
                grants: vec![MemberCapacityGrant {
                    member: cohort.members[0].clone(),
                    activation: activation.clone(),
                }],
            })
            .unwrap();
    }
    let request = request();
    let command = if admitted {
        let plan = crate::request_plans::Plan {
            version: 1,
            id: request.request_id,
            authority: authority(),
            cohort,
            validation_version: 0,
            parts: vec![crate::request_plans::Part {
                descriptor: participant(),
                request: request.clone(),
            }],
        };
        Command::AdmitRequest {
            id: [3; 16],
            request: b"original external request".to_vec(),
            payload: plan.encode().unwrap(),
            transaction_id: request.request_id,
            participants: vec![participant()],
        }
    } else {
        Command::Begin {
            id: request.request_id,
            participants: vec![participant()],
        }
    };
    journal.apply(command).unwrap();
    let prepare = RetainedPrepare {
        version: 1,
        authority: authority(),
        participant: participant(),
        request,
        cut: journal.history_cursor().unwrap(),
    };
    let mut state = D1State::new(7, 1);
    state.enable_managed(7, 1, 0, [7; 16]).unwrap();
    state.participant = Some(State {
        authority: authority(),
        activated_sequence: 0,
        records: vec![],
    });
    state.version = if profile.is_legacy() { 6 } else { 7 };
    state.visibility = Some(crate::versioned_reads::State {
        baseline_sequence: 0,
        epoch: 1,
        applied: 0,
        journal: journal.clone().into(),
        commits: vec![],
        activation: (!profile.is_legacy()).then_some(activation),
    });
    (state, journal, prepare)
}
fn observe(state: &mut D1State, journal: &Journal) {
    state.visibility.as_mut().unwrap().journal = journal.clone().into();
}

#[test]
fn strict_prepare_checks_old_cut_before_original_plan_and_local_retry() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        for admitted in [false, true] {
            let (mut state, mut journal, prepare) = fixture(profile, admitted);
            assert_eq!(
                state.prepare_retained(&prepare, None),
                Ok(D1Result::Participant(Status::Prepared))
            );
            let before = state.clone();
            for bad in [
                RetainedPrepare {
                    cut: journal.history_cursor_at(1).unwrap(),
                    ..prepare.clone()
                },
                RetainedPrepare {
                    cut: HistoryCursor {
                        root: [99; 32],
                        ..prepare.cut.clone()
                    },
                    ..prepare.clone()
                },
                RetainedPrepare {
                    participant: Participant {
                        intent: [5; 32],
                        ..participant()
                    },
                    ..prepare.clone()
                },
            ] {
                assert!(state.prepare_retained(&bad, None).is_err());
                assert_eq!(state, before);
            }
            let changed = RetainedPrepare {
                request: D1WriteRequest {
                    operations: vec![WriteOp::put("x", "changed")],
                    ..request()
                },
                ..prepare.clone()
            };
            assert_eq!(
                state.prepare_retained(&changed, None),
                Err(D1Error::IdempotencyConflict)
            );
            assert_eq!(state, before);
            journal
                .apply(Command::Decide {
                    id: request().request_id,
                    decision: Decision::Abort,
                })
                .unwrap();
            observe(&mut state, &journal);
            // Observation E advanced, but A and the resident Intent have not.
            assert_eq!(
                state.prepare_retained(&prepare, None),
                Ok(D1Result::Participant(Status::Prepared))
            );
            let terminal_cut = RetainedPrepare {
                cut: journal.history_cursor().unwrap(),
                ..prepare.clone()
            };
            let before = state.clone();
            assert_eq!(
                state.prepare_retained(&terminal_cut, None),
                Err(D1Error::VisibilityUnresolved)
            );
            assert_eq!(state, before);
            state.advance_retained(1, &terminal_cut.cut, None).unwrap();
            assert_eq!(
                state.prepare_retained(&prepare, None),
                Ok(D1Result::Participant(Status::Aborted))
            );
            assert_eq!(
                state.prepare_retained(&changed, None),
                Err(D1Error::IdempotencyConflict)
            );
            assert_eq!(
                state.prepare_retained(&terminal_cut, None),
                Err(D1Error::VisibilityUnresolved)
            );
        }
    }
}

#[test]
fn strict_prepare_original_plan_checks_precede_absent_and_terminal_intents() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let (mut state, mut journal, prepare) = fixture(profile, true);
        let changed = RetainedPrepare {
            request: D1WriteRequest {
                operations: vec![WriteOp::put("x", "changed")],
                ..request()
            },
            ..prepare.clone()
        };
        let before = state.clone();
        assert_eq!(
            state.prepare_retained(&changed, None),
            Err(D1Error::IdempotencyConflict)
        );
        assert_eq!(state, before);
        journal
            .apply(Command::Decide {
                id: request().request_id,
                decision: Decision::Abort,
            })
            .unwrap();
        observe(&mut state, &journal);
        assert_eq!(
            state.prepare_retained(&prepare, None),
            Err(D1Error::VisibilityUnresolved)
        );
        state
            .advance_retained(1, &journal.history_cursor().unwrap(), None)
            .unwrap();
        assert!(state.participant.as_ref().unwrap().records.is_empty());
        assert_eq!(
            state.prepare_retained(&changed, None),
            Err(D1Error::IdempotencyConflict)
        );
        assert_eq!(
            state.prepare_retained(&prepare, None),
            Ok(D1Result::Participant(Status::Aborted))
        );
        assert!(state.participant.as_ref().unwrap().records.is_empty());
    }
}

#[test]
fn new_command_codec_is_strict_without_changing_legacy_operation_bytes() {
    let (_, _, prepare) = fixture(CapacityProfile::LegacyCountV1, false);
    let old = D1Command::PrepareParticipant {
        authority: authority(),
        participant: participant(),
        request: request(),
    };
    let expected = format!(
        "{{\"PrepareParticipant\":{{\"authority\":{},\"participant\":{},\"request\":{}}}}}",
        serde_json::to_string(&authority()).unwrap(),
        serde_json::to_string(&participant()).unwrap(),
        serde_json::to_string(&request()).unwrap()
    );
    assert_eq!(serde_json::to_vec(&old).unwrap(), expected.as_bytes());
    assert_eq!(serde_json::from_str::<D1Command>(&expected).unwrap(), old);
    let new = D1Command::PrepareRetained(Box::new(prepare.clone()));
    let expected = format!("{{\"PrepareRetained\":{{\"version\":1,\"authority\":{},\"participant\":{},\"request\":{},\"cut\":{}}}}}",
        serde_json::to_string(&prepare.authority).unwrap(), serde_json::to_string(&prepare.participant).unwrap(),
        serde_json::to_string(&prepare.request).unwrap(), serde_json::to_string(&prepare.cut).unwrap());
    assert_eq!(serde_json::to_vec(&new).unwrap(), expected.as_bytes());
    assert_eq!(serde_json::from_str::<D1Command>(&expected).unwrap(), new);
    let value = serde_json::to_value(&prepare).unwrap();
    for path in [
        vec![],
        vec!["authority"],
        vec!["participant"],
        vec!["request"],
        vec!["cut"],
        vec!["cut", "cohort"],
    ] {
        let mut bad = value.clone();
        let mut object = &mut bad;
        for key in path {
            object = &mut object[key];
        }
        object
            .as_object_mut()
            .unwrap()
            .insert("extra".into(), true.into());
        assert!(serde_json::from_value::<RetainedPrepare>(bad).is_err());
    }
    for operation in [
        serde_json::json!({"Put":{"key":[1],"value":[2],"extra":0}}),
        serde_json::json!({"Delete":{"key":[1],"extra":0}}),
    ] {
        let mut bad = value.clone();
        bad["request"]["operations"] = serde_json::json!([operation]);
        assert!(serde_json::from_value::<RetainedPrepare>(bad).is_err());
    }
    let mut missing = value.clone();
    missing["request"]
        .as_object_mut()
        .unwrap()
        .remove("expected_sequence");
    assert!(serde_json::from_value::<RetainedPrepare>(missing).is_err());
    let mut explicit_null = value.clone();
    explicit_null["request"]["expected_sequence"] = serde_json::Value::Null;
    let null: RetainedPrepare = serde_json::from_value(explicit_null).unwrap();
    assert_eq!(null.request.expected_sequence, None);
    let (mut state, _, _) = fixture(CapacityProfile::LegacyCountV1, false);
    assert_eq!(
        state.prepare_retained(&null, None),
        Err(D1Error::Conflict {
            expected: u64::MAX,
            current: 0
        })
    );
    let encoded = serde_json::to_string(&prepare).unwrap();
    for bad in [
        encoded.replacen("\"version\":1", "\"version\":2", 1),
        encoded.replacen("\"version\":1", "\"version\":1,\"version\":1", 1),
        encoded.replacen("\"key\":[120]", "\"key\":[120],\"key\":[120]", 1),
    ] {
        assert!(serde_json::from_str::<RetainedPrepare>(&bad).is_err());
    }
    // Preserve the old nested WriteOp acceptance contract as well as bytes.
    let mut old_extra = serde_json::to_value(&old).unwrap();
    old_extra["PrepareParticipant"]["request"]["operations"][0]["Put"]["extra"] = true.into();
    assert_eq!(serde_json::from_value::<D1Command>(old_extra).unwrap(), old);
}

#[test]
fn strict_direct_prepare_keeps_d1_limits_instead_of_plan_or_external_request_limits() {
    let (mut state, _, mut prepare) = fixture(CapacityProfile::LegacyCountV1, false);
    prepare.request.operations = vec![WriteOp::put(
        "x",
        vec![5; crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES + 1],
    )];
    let encoded = serde_json::to_vec(&prepare).unwrap();
    let decoded: RetainedPrepare = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, prepare);
    assert_eq!(
        state.prepare_retained(&decoded, None),
        Ok(D1Result::Participant(Status::Prepared))
    );
    let mut bad = prepare.clone();
    bad.request.operations = vec![WriteOp::delete([]); MAX_BATCH_OPERATIONS + 1];
    assert!(serde_json::from_slice::<RetainedPrepare>(&serde_json::to_vec(&bad).unwrap()).is_err());
    assert_eq!(bad.validate(), Err(D1Error::BatchTooLarge));
    bad.request.operations = vec![WriteOp::put([], vec![0; MAX_BATCH_BYTES + 1])];
    assert_eq!(bad.validate(), Err(D1Error::BatchTooLarge));
    assert!(serde_json::from_slice::<RetainedPrepare>(&serde_json::to_vec(&bad).unwrap()).is_err());
}

#[tokio::test]
async fn durable_retained_prepare_same_batch_old_cut_reopen_and_snapshot() {
    use crate::{LogCommand, StateMachine, TypeConfig};
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use std::sync::Arc;
    fn entry(n: u64, command: D1Command) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), n),
            payload: openraft::EntryPayload::Normal(LogCommand::D1V1(command)),
        }
    }
    async fn apply(
        sm: &mut Arc<StateMachine>,
        commands: Vec<D1Command>,
    ) -> Vec<Result<D1Result, D1Error>> {
        let first = sm.applied().await.map_or(1, |id| id.index + 1);
        sm.apply(
            commands
                .into_iter()
                .enumerate()
                .map(|(i, c)| entry(first + i as u64, c)),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.d1.unwrap())
        .collect()
    }
    fn sync(page: crate::transaction_decisions::HistoryPage) -> D1Command {
        D1Command::SyncHistory {
            authority: authority(),
            shard: 7,
            epoch: 1,
            mode_id: [7; 16],
            page: Box::new(page),
        }
    }
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        let (_, _, template) = fixture(profile, true);
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: template.cut.cohort.clone(),
            })
            .unwrap();
        let initialization = if profile.is_legacy() {
            D1Command::EnableVersionedReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: journal.clone(),
            }
        } else {
            D1Command::EnableByteBoundReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: journal.clone(),
                generation_nonce: [8; 32],
            }
        };
        for result in apply(
            &mut sm,
            vec![
                D1Command::Activate { shard: 7, epoch: 1 },
                D1Command::EnableManagedWrites {
                    shard: 7,
                    epoch: 1,
                    expected_sequence: 0,
                    mode_id: [7; 16],
                },
                D1Command::EnableParticipant {
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    authority: authority(),
                },
                initialization,
            ],
        )
        .await
        {
            result.unwrap();
        }
        sm.forbid_coordinator_journal(true);
        if !profile.is_legacy() {
            let grant = sm
                .participant_capacity_grant(7, 1, [7; 16], &authority(), &template.cut.cohort)
                .await
                .unwrap();
            journal
                .apply(Command::ConfirmCohortCapacity {
                    cohort: template.cut.cohort.clone(),
                    grants: vec![grant],
                })
                .unwrap();
        }
        let old_cut = journal.history_cursor().unwrap();
        let plan = crate::request_plans::Plan {
            version: 1,
            id: request().request_id,
            authority: authority(),
            cohort: template.cut.cohort.clone(),
            validation_version: 0,
            parts: vec![crate::request_plans::Part {
                descriptor: participant(),
                request: request(),
            }],
        };
        journal
            .apply(Command::AdmitRequest {
                id: [3; 16],
                request: b"original".to_vec(),
                payload: plan.encode().unwrap(),
                transaction_id: request().request_id,
                participants: vec![participant()],
            })
            .unwrap();
        let prepare = RetainedPrepare {
            cut: journal.history_cursor().unwrap(),
            ..template
        };
        let base = sm
            .participant_history_status(7, 1, [7; 16])
            .await
            .unwrap()
            .0;
        let page = journal.history_page(&base, None).unwrap();
        let mut rejected_page = page.clone();
        rejected_page.events.push(Command::Prepared {
            id: [99; 16],
            participant: participant(),
        });
        rejected_page.end = rejected_page.base.extend(&rejected_page.events).unwrap();
        rejected_page.cut = rejected_page.end.clone();
        let marker_before_rejection = sm.applied().await.unwrap();
        let rejected = apply(
            &mut sm,
            vec![
                sync(rejected_page),
                D1Command::PrepareRetained(Box::new(prepare.clone())),
            ],
        )
        .await;
        assert!(rejected[0].is_err());
        assert_eq!(
            rejected[1],
            Err(if profile.is_legacy() {
                D1Error::VisibilityHistory
            } else {
                // The rejected page included the first confirmation. Its rollback
                // must retain the earlier unconfirmed checkpoint and fail readiness.
                D1Error::CoordinatorRejected(
                    crate::transaction_decisions::Error::CapacityNotConfirmed,
                )
            })
        );
        assert_eq!(
            sm.applied().await.unwrap().index,
            marker_before_rejection.index + 2
        );
        assert_eq!(
            sm.applied().await.unwrap().leader_id,
            marker_before_rejection.leader_id
        );
        assert_eq!(
            sm.participant_status(7, 1, [7; 16], request().request_id)
                .await
                .unwrap(),
            Status::Unknown
        );
        let data = sm.d1_read_snapshot(10, 1024).await.unwrap().unwrap();
        assert_eq!(data.sequence, 0);
        assert!(data.entries.is_empty());
        assert_eq!(
            sm.participant_history_status(7, 1, [7; 16]).await.unwrap(),
            (base.clone(), 0)
        );
        let results = apply(
            &mut sm,
            vec![
                sync(page),
                D1Command::PrepareRetained(Box::new(RetainedPrepare {
                    cut: old_cut,
                    ..prepare.clone()
                })),
                D1Command::PrepareRetained(Box::new(prepare.clone())),
            ],
        )
        .await;
        assert!(results[0].is_ok());
        assert_eq!(results[1], Err(D1Error::VisibilityHistory));
        assert_eq!(results[2], Ok(D1Result::Participant(Status::Prepared)));
        assert_eq!(
            sm.participant_history_status(7, 1, [7; 16])
                .await
                .unwrap()
                .1,
            0
        );
        assert_eq!(sm.get_raw(b"x").await, None);
        // Same batch: observe terminal, preserve still-Prepared status, consume A,
        // then return Aborted against the same independent old pending cut.
        journal
            .apply(Command::Decide {
                id: request().request_id,
                decision: Decision::Abort,
            })
            .unwrap();
        let terminal_cut = journal.history_cursor().unwrap();
        let page = journal.history_page(&prepare.cut, None).unwrap();
        let results = apply(
            &mut sm,
            vec![
                sync(page),
                D1Command::PrepareRetained(Box::new(prepare.clone())),
                D1Command::AdvanceRetained {
                    authority: authority(),
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    ordinal: 1,
                    cut: terminal_cut.clone(),
                },
                D1Command::PrepareRetained(Box::new(prepare.clone())),
                D1Command::PrepareRetained(Box::new(RetainedPrepare {
                    cut: terminal_cut,
                    ..prepare.clone()
                })),
            ],
        )
        .await;
        assert!(results[0].is_ok());
        assert_eq!(results[1], Ok(D1Result::Participant(Status::Prepared)));
        assert!(results[2].is_ok());
        assert_eq!(results[3], Ok(D1Result::Participant(Status::Aborted)));
        assert_eq!(results[4], Err(D1Error::VisibilityUnresolved));
        let snapshot = sm
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        sm.forbid_coordinator_journal(true);
        assert_eq!(
            apply(
                &mut sm,
                vec![D1Command::PrepareRetained(Box::new(prepare.clone()))]
            )
            .await,
            vec![Ok(D1Result::Participant(Status::Aborted))]
        );
        let target_dir = tempfile::tempdir().unwrap();
        let mut target = Arc::new(StateMachine::open(target_dir.path()).unwrap());
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        target.forbid_coordinator_journal(true);
        assert_eq!(
            apply(
                &mut target,
                vec![D1Command::PrepareRetained(Box::new(prepare))]
            )
            .await,
            vec![Ok(D1Result::Participant(Status::Aborted))]
        );
        assert_eq!(
            target
                .participant_history_status(7, 1, [7; 16])
                .await
                .unwrap()
                .1,
            1
        );
        assert_eq!(target.get_raw(b"x").await, None);
    }
}
