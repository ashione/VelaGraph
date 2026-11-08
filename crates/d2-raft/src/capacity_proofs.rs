//! Quantitative gates for legacy and byte-bound cohort codecs and admission.
//! These use the actual Command, Plan, Journal, history and terminal codecs.
use super::*;
use crate::participant_writes::Authority;
use crate::request_plans::{Part, Plan};
use crate::terminal_prefix::{Terminal, TerminalPrefix, SEGMENT_TRANSACTIONS};
use crate::{D1WriteRequest, WriteOp};

const EVENT_FLOOR: usize = 70;
const BEGIN_FLOOR: usize = 205;
const PROOF_OVERHEAD: usize = 64 * 1024;
// Reserved space in the proposed inequality only. No new profile/tag exists in
// these baseline codecs: its actual encoded delta still needs a separate gate.
const UNMEASURED_TAG_ALLOWANCE: usize = 1024;

fn size<T: Serialize>(value: &T) -> usize {
    serde_json::to_vec(value).unwrap().len()
}

fn identity<const N: usize>(wide: bool) -> [u8; N] {
    if wide {
        [255; N]
    } else {
        let mut value = [0; N];
        value[0] = 1;
        value
    }
}

fn participants(count: usize, numbers: bool, mode: bool, intent: bool) -> Vec<Participant> {
    (0..count)
        .map(|n| Participant {
            shard: if numbers {
                u64::MAX - MAX_PARTICIPANTS as u64 + n as u64
            } else {
                n as u64
            },
            epoch: if numbers { u64::MAX } else { 0 },
            mode: identity(mode),
            intent: identity(intent),
        })
        .collect()
}

fn cohort(participants: &[Participant]) -> Cohort {
    let cohort = Cohort {
        coordinator_shard: u64::MAX,
        coordinator_epoch: u64::MAX,
        profile: CapacityProfile::LegacyCountV1,
        members: participants
            .iter()
            .map(|p| Member {
                shard: p.shard,
                epoch: p.epoch,
                mode: p.mode,
            })
            .collect(),
    };
    cohort.validate().unwrap();
    cohort
}

fn admit(
    authority: [u8; 16],
    id: [u8; 16],
    participants: &[Participant],
    cohort: &Cohort,
    value: Vec<u8>,
) -> Command {
    let plan = Plan {
        version: 1,
        id,
        authority: Authority {
            shard: cohort.coordinator_shard,
            mode: authority,
        },
        cohort: cohort.clone(),
        validation_version: 0,
        parts: participants
            .iter()
            .map(|descriptor| Part {
                descriptor: descriptor.clone(),
                request: D1WriteRequest {
                    shard: descriptor.shard,
                    epoch: descriptor.epoch,
                    request_id: id,
                    expected_sequence: Some(0),
                    operations: vec![WriteOp::put(vec![0], value.clone())],
                },
            })
            .collect(),
    };
    let payload = plan.encode().unwrap();
    assert_eq!(Plan::decode(&payload).unwrap().encode().unwrap(), payload);
    Command::AdmitRequest {
        id,
        request: vec![0],
        payload,
        transaction_id: id,
        participants: participants.to_vec(),
    }
}

fn assert_event_floor(event: &Command) {
    assert!(size(event) >= EVENT_FLOOR);
    // Deliberately exhaustive: adding a new event requires extending this gate.
    match event {
        Command::Begin { .. } => assert!(size(event) >= BEGIN_FLOOR),
        Command::AdmitRequest {
            transaction_id,
            participants,
            ..
        } => {
            let equivalent = Command::Begin {
                id: *transaction_id,
                participants: participants.clone(),
            };
            assert!(size(event) > size(&equivalent));
            assert!(size(&equivalent) >= BEGIN_FLOOR);
        }
        Command::BindCohort { .. }
        | Command::ConfirmCohortCapacity { .. }
        | Command::Prepared { .. }
        | Command::Decide { .. } => {}
    }
}

#[test]
fn baseline_valid_command_minima_and_plan_growth_are_conservative() {
    let id = identity(false);
    let parts = participants(1, false, false, false);
    let begin = Command::Begin {
        id,
        participants: parts.clone(),
    };
    let abort = Command::Decide {
        id,
        decision: Decision::Abort,
    };
    assert_eq!(size(&begin), BEGIN_FLOOR);
    assert_eq!(size(&abort), EVENT_FLOOR);
    let mut minimal = Journal::new(id).unwrap();
    minimal.apply(begin.clone()).unwrap();
    minimal.apply(abort.clone()).unwrap();
    for event in [
        begin,
        abort,
        Command::Prepared {
            id,
            participant: parts[0].clone(),
        },
        Command::Decide {
            id,
            decision: Decision::Commit,
        },
    ] {
        assert_event_floor(&event);
    }

    let mut binding = cohort(&parts);
    binding.coordinator_shard = 1;
    binding.coordinator_epoch = 0;
    let bind = Command::BindCohort {
        cohort: binding.clone(),
    };
    assert_event_floor(&bind);
    let mut bound = Journal::new(id).unwrap();
    bound.apply(bind).unwrap();
    let mut previous = 0;
    // The actual Plan byte vector is nested in AdmitRequest. Growing its one
    // operation, or its request byte vector, cannot shorten canonical JSON.
    for length in [0, 1, 9, 10, 99, 100, 1024] {
        let mut event = admit(id, id, &parts, &binding, vec![0; length]);
        assert_event_floor(&event);
        assert!(size(&event) > previous);
        previous = size(&event);
        bound.clone().apply(event.clone()).unwrap();
        for byte in [9, 10, 99, 100, 255] {
            if let Command::AdmitRequest { request, .. } = &mut event {
                request.push(byte);
            }
            assert!(size(&event) > previous);
            previous = size(&event);
            bound.clone().apply(event.clone()).unwrap();
        }
        // Compare successive Plan lengths without the extra request padding.
        previous = size(&admit(id, id, &parts, &binding, vec![0; length]));
    }
}

#[test]
fn baseline_terminal_records_fit_four_thirds_of_begin_for_all_width_extremes() {
    for count in 1..=MAX_PARTICIPANTS {
        // Independently widen id, shard/epoch, mode and intent. Those fields
        // appear identically in both real codecs; any intermediate decimal
        // width adds the same bytes to each side and improves the ratio.
        for widths in 0..16 {
            let id = identity(widths & 1 != 0);
            let parts = participants(count, widths & 2 != 0, widths & 4 != 0, widths & 8 != 0);
            let begin = size(&Command::Begin {
                id,
                participants: parts.clone(),
            });
            assert!(begin >= 67 + 138 * count);
            for (decision, prepared) in [
                (Decision::Abort, false),
                (Decision::Abort, true),
                (Decision::Commit, true),
            ] {
                let terminal = Terminal {
                    id,
                    ordinal: u64::MAX,
                    participants: parts.clone(),
                    prepared: vec![prepared; count],
                    decision,
                };
                let charged = size(&terminal) + 1; // includes a record separator
                assert!(charged <= begin + 54 + 6 * count);
                assert!(
                    3 * charged < 4 * begin,
                    "participants={count}, widths={widths}"
                );
            }
        }
        // This algebra, and the identical encoding of all shared fields, is
        // the general bound rather than an extrapolation from a few records.
        assert!(3 * (54 + 6 * count) < 67 + 138 * count);
    }
}

#[test]
fn baseline_complete_proof_punctuation_and_derived_byte_ceilings_fit() {
    let parts = participants(MAX_PARTICIPANTS, true, true, true);
    let binding = cohort(&parts);
    let authority = identity(true);
    let empty = TerminalPrefix::from_segments(authority, binding.clone(), 0, vec![]).unwrap();
    let envelope = size(&empty);
    for count in [1usize, 31, 32, 33, MAX_TRANSACTIONS] {
        let terminals: Vec<_> = (1..=count)
            .map(|ordinal| {
                let mut id = [255; 16];
                id[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
                Terminal {
                    id,
                    ordinal: ordinal as u64,
                    participants: vec![parts[0].clone()],
                    prepared: vec![false],
                    decision: Decision::Abort,
                }
            })
            .collect();
        let charged: usize = terminals.iter().map(|record| size(record) + 1).sum();
        let segments: Vec<_> = terminals
            .chunks(SEGMENT_TRANSACTIONS)
            .map(|chunk| chunk.to_vec())
            .collect();
        let segment_count = segments.len();
        let proof =
            TerminalPrefix::from_segments(authority, binding.clone(), count as u64, segments)
                .unwrap();
        assert_eq!(
            size(&proof) - charged,
            envelope + count.to_string().len() - 1 + 2 * segment_count - 1
        );
        assert_eq!(
            serde_json::from_slice::<TerminalPrefix>(&serde_json::to_vec(&proof).unwrap()).unwrap(),
            proof
        );
    }

    let transactions = MAX_JOURNAL_BYTES / BEGIN_FLOOR;
    let events = MAX_JOURNAL_BYTES / EVENT_FLOOR;
    assert_eq!((transactions, events), (81_840, 239_674));
    let segment_count = transactions.div_ceil(SEGMENT_TRANSACTIONS);
    // Keep even more punctuation than the measured exact 2*S-1 above, plus
    // maximum u64 width and a separate, as-yet-unmeasured profile allowance.
    let overhead =
        envelope + u64::MAX.to_string().len() - 1 + 3 * segment_count + UNMEASURED_TAG_ALLOWANCE;
    assert!(overhead <= PROOF_OVERHEAD);
    let proof_upper = (4 * MAX_JOURNAL_BYTES).div_ceil(3) + PROOF_OVERHEAD;
    assert_eq!(proof_upper, 22_435_158);
    assert!(proof_upper < crate::managed_rpc::MAX_FRAME_BYTES);

    // Derived geometry only. Actual row/key codecs are independently gated by
    // event_ledger's maximum-width and staged-key tests; a new domain/version
    // must extend those tests rather than infer it from this arithmetic.
    let rows = events.div_ceil(MAX_HISTORY_PAGE_EVENTS)
        + MAX_JOURNAL_BYTES / (MAX_HISTORY_PAGE_BYTES - MAX_REQUEST_EVENT_BYTES)
        + 1;
    assert_eq!(rows, 7_499);
    assert_eq!(MAX_JOURNAL_BYTES + rows * 16 * 1024, 139_640_832);
}

fn assert_exact_reservation(journal: &Journal) {
    let pending: usize = journal
        .transactions
        .iter()
        .filter(|tx| tx.decision.is_none())
        .map(|tx| {
            let decision = size(&Command::Decide {
                id: tx.id,
                decision: Decision::Commit,
            }) + 1;
            decision
                + tx.participants
                    .iter()
                    .zip(&tx.prepared)
                    .filter(|(_, prepared)| !**prepared)
                    .map(|(participant, _)| {
                        size(&Command::Prepared {
                            id: tx.id,
                            participant: participant.clone(),
                        }) + 1
                    })
                    .sum::<usize>()
        })
        .sum();
    assert_eq!(
        journal.reserved_bytes,
        journal.encode().unwrap().len() + pending
    );
    assert_eq!(
        Journal::decode(&journal.encode().unwrap()).unwrap(),
        *journal
    );
}

#[test]
fn baseline_versions_charge_exact_envelope_and_every_pending_resolution_byte() {
    for wide in [false, true] {
        let authority = identity(wide);
        let envelope = Journal::new(authority).unwrap().encode().unwrap().len();
        for version in 1..=3 {
            // Actual private persisted Encoding, not a hand-copied JSON schema.
            assert_eq!(
                size(&Encoding {
                    version,
                    authority,
                    events: vec![],
                }),
                envelope
            );
            for count in [1, MAX_PARTICIPANTS] {
                let parts = participants(count, wide, wide, wide);
                let binding = cohort(&parts);
                let mut journal = Journal::new(authority).unwrap();
                if version >= 2 {
                    journal
                        .apply(Command::BindCohort {
                            cohort: binding.clone(),
                        })
                        .unwrap();
                    assert_exact_reservation(&journal);
                }
                let admission = if version == 3 {
                    admit(authority, identity(wide), &parts, &binding, vec![])
                } else {
                    Command::Begin {
                        id: identity(wide),
                        participants: parts.clone(),
                    }
                };
                let before = journal.clone();
                journal.apply(admission.clone()).unwrap();
                assert_eq!(journal.encoding_version(), version);
                assert_exact_reservation(&journal);
                let reserved = journal.reserved_bytes;
                let mut exact = before.clone();
                exact
                    .replay_event_with_limit(admission.clone(), reserved)
                    .unwrap();
                assert_eq!(exact, journal);
                // Failed unpublished replay is discarded, exactly as recovery
                // requires. The published predecessor is untouched.
                assert_eq!(
                    before
                        .clone()
                        .replay_event_with_limit(admission, reserved - 1),
                    Err(Error::Capacity)
                );
                for participant in parts {
                    journal
                        .apply(Command::Prepared {
                            id: identity(wide),
                            participant,
                        })
                        .unwrap();
                    assert_eq!(journal.reserved_bytes, reserved);
                    assert_exact_reservation(&journal);
                }
                journal
                    .apply(Command::Decide {
                        id: identity(wide),
                        decision: Decision::Commit,
                    })
                    .unwrap();
                assert_eq!(journal.encode().unwrap().len(), reserved);
                assert_exact_reservation(&journal);
            }
        }
    }
}

fn byte_cohort(parts: &[Participant]) -> Cohort {
    Cohort {
        profile: CapacityProfile::ByteBound16MiBV1,
        ..cohort(parts)
    }
}

fn grants(binding: &Cohort, wide: bool) -> Vec<MemberCapacityGrant> {
    binding
        .members
        .iter()
        .map(|member| MemberCapacityGrant {
            member: member.clone(),
            activation: CapacityActivation {
                nonce: identity(wide),
                log_id: openraft::LogId::new(
                    openraft::CommittedLeaderId::new(
                        if wide { u64::MAX } else { 1 },
                        if wide { u64::MAX } else { 0 },
                    ),
                    if wide { u64::MAX } else { 1 },
                ),
            },
        })
        .collect()
}

fn confirmed(parts: &[Participant]) -> Journal {
    let binding = byte_cohort(parts);
    let mut journal = Journal::new([9; 16]).unwrap();
    journal
        .apply(Command::BindCohort {
            cohort: binding.clone(),
        })
        .unwrap();
    journal
        .apply(Command::ConfirmCohortCapacity {
            grants: grants(&binding, false),
            cohort: binding,
        })
        .unwrap();
    journal
}

#[test]
fn byte_profile_tag_and_formats_are_explicit_legacy_encodings_remain_exact() {
    let parts = participants(1, false, false, false);
    let old = cohort(&parts);
    let legacy = serde_json::to_vec(&old).unwrap();
    assert!(!String::from_utf8_lossy(&legacy).contains("profile"));
    assert_eq!(serde_json::from_slice::<Cohort>(&legacy).unwrap(), old);
    let new = byte_cohort(&parts);
    let new_bytes = serde_json::to_vec(&new).unwrap();
    assert!(String::from_utf8_lossy(&new_bytes).ends_with(r#","profile":"ByteBound16MiBV1"}"#));
    assert_eq!(serde_json::from_slice::<Cohort>(&new_bytes).unwrap(), new);
    for tag in [
        serde_json::Value::Null,
        serde_json::json!("LegacyCountV1"),
        serde_json::json!("ByteBound16MiBV2"),
        serde_json::json!(2),
    ] {
        let mut invalid = serde_json::to_value(&old).unwrap();
        invalid["profile"] = tag;
        assert!(serde_json::from_value::<Cohort>(invalid).is_err());
    }
    let mut journal = Journal::new([9; 16]).unwrap();
    journal.apply(Command::BindCohort { cohort: old }).unwrap();
    let before = journal.clone();
    assert_eq!(
        journal.apply(Command::BindCohort { cohort: new }),
        Err(Error::IdentityConflict)
    );
    assert_eq!(journal, before);
    assert_eq!(CapacityProfile::LegacyCountV1.max_transactions(), 1024);
    assert_eq!(CapacityProfile::LegacyCountV1.max_requests(), 256);
    assert_eq!(
        CapacityProfile::ByteBound16MiBV1.max_transactions(),
        MAX_JOURNAL_BYTES / BEGIN_FLOOR
    );
    assert_eq!(
        CapacityProfile::ByteBound16MiBV1.max_history_events(),
        (MAX_JOURNAL_BYTES / EVENT_FLOOR) as u64
    );
    assert_eq!(
        CapacityProfile::ByteBound16MiBV1.max_intents(),
        crate::participant_writes::MAX_INTENT_BYTES / crate::capacity_policy::INTENT_FIXED_BYTES
    );
}

#[test]
fn byte_profile_complete_grants_are_immutable_and_precede_every_new_admission() {
    let parts = participants(2, false, false, false);
    let binding = byte_cohort(&parts);
    let mut journal = Journal::new([9; 16]).unwrap();
    journal
        .apply(Command::BindCohort {
            cohort: binding.clone(),
        })
        .unwrap();
    let bound = journal.clone();
    let begin = Command::Begin {
        id: identity(false),
        participants: parts.clone(),
    };
    let admission = admit([9; 16], identity(false), &parts, &binding, vec![]);
    for event in [begin.clone(), admission.clone()] {
        assert_eq!(journal.apply(event), Err(Error::CapacityNotConfirmed));
        assert_eq!(journal, bound);
        assert_eq!(journal.transaction_count(), 0);
        assert!(journal.request(identity(false)).is_none());
    }
    assert!(!journal.capacity_ready());
    assert!(journal.capacity_grants().is_none());
    let complete = grants(&binding, false);
    for case in 0..9 {
        let mut invalid = complete.clone();
        match case {
            0 => {
                invalid.pop();
            }
            1 => invalid.push(invalid[0].clone()),
            2 => invalid.swap(0, 1),
            3 => invalid[0].member.epoch += 1,
            4 => invalid[0].member.mode[0] += 1,
            5 => invalid[0].activation.nonce = [0; 32],
            6 => invalid[0].activation.log_id.index = 0,
            7 => invalid[0].activation.log_id.leader_id.term = 0,
            _ => invalid[1].member = invalid[0].member.clone(),
        }
        assert!(
            journal
                .apply(Command::ConfirmCohortCapacity {
                    cohort: binding.clone(),
                    grants: invalid
                })
                .is_err(),
            "case {case}"
        );
        assert_eq!(journal, bound);
    }
    let confirm = Command::ConfirmCohortCapacity {
        cohort: binding.clone(),
        grants: complete.clone(),
    };
    journal.apply(confirm.clone()).unwrap();
    assert!(journal.capacity_ready());
    assert_eq!(journal.capacity_grants(), Some(complete.as_slice()));
    let first = journal.clone();
    journal.apply(confirm.clone()).unwrap();
    assert_eq!(journal, first);
    for change_nonce in [false, true] {
        let mut changed = complete.clone();
        if change_nonce {
            changed[0].activation.nonce[0] += 1;
        } else {
            changed[0].activation.log_id.index += 1;
        }
        assert_eq!(
            journal.apply(Command::ConfirmCohortCapacity {
                cohort: binding.clone(),
                grants: changed
            }),
            Err(Error::IdentityConflict)
        );
        assert_eq!(journal, first);
    }
    journal.apply(admission).unwrap();
    assert_exact_reservation(&journal);
    let mut encoding: Encoding = serde_json::from_slice(&journal.encode().unwrap()).unwrap();
    assert_eq!(encoding.version, 4);
    encoding.events.swap(1, 2);
    assert_eq!(
        Journal::decode(&serde_json::to_vec(&encoding).unwrap()),
        Err(Error::CapacityNotConfirmed)
    );
    encoding.events.swap(1, 2);
    encoding.events.push(confirm);
    assert_eq!(
        Journal::decode(&serde_json::to_vec(&encoding).unwrap()),
        Err(Error::InvalidJournal)
    );
    encoding.events.pop();
    for version in 1..=3 {
        encoding.version = version;
        assert_eq!(
            Journal::decode(&serde_json::to_vec(&encoding).unwrap()),
            Err(Error::InvalidJournal)
        );
    }
    let mut unbound = Journal::new([9; 16]).unwrap();
    unbound.apply(begin).unwrap();
    assert_eq!(
        unbound.apply(Command::BindCohort { cohort: binding }),
        Err(Error::IncompatibleHistory)
    );
}

#[test]
fn byte_profile_actual_grant_envelope_minima_and_terminal_overhead_are_proven() {
    for wide in [false, true] {
        let authority = identity(wide);
        for count in [1, MAX_PARTICIPANTS] {
            let parts = participants(count, wide, wide, wide);
            let binding = byte_cohort(&parts);
            let mut journal = Journal::new(authority).unwrap();
            assert_eq!(
                size(&Encoding {
                    version: 4,
                    authority,
                    events: vec![]
                }),
                journal.envelope_bytes().unwrap()
            );
            for command in [
                Command::BindCohort {
                    cohort: binding.clone(),
                },
                Command::ConfirmCohortCapacity {
                    cohort: binding.clone(),
                    grants: grants(&binding, wide),
                },
            ] {
                assert_event_floor(&command);
                assert!(size(&command) < MAX_REQUEST_EVENT_BYTES);
                let predecessor = journal.clone();
                journal.apply(command.clone()).unwrap();
                assert_exact_reservation(&journal);
                let exact = journal.reserved_bytes;
                assert!(predecessor.clone().append(command.clone(), exact).unwrap());
                assert_eq!(
                    predecessor.clone().append(command, exact - 1),
                    Err(Error::Capacity)
                );
            }
            let zero = journal.terminal_prefix(0).unwrap();
            assert_eq!(serde_json::to_value(&zero).unwrap()["version"], 2);
            let envelope = size(&zero);
            if wide && count == MAX_PARTICIPANTS {
                let largest_legacy_record = Terminal {
                    id: [255; 16],
                    ordinal: u64::MAX,
                    participants: parts.clone(),
                    prepared: vec![false; MAX_PARTICIPANTS],
                    decision: Decision::Commit,
                };
                // Even all structurally valid legacy records at maximum width
                // fit the shared incremental decoder byte bound.
                assert!(
                    (size(&largest_legacy_record) + 1) * MAX_TRANSACTIONS
                        + envelope
                        + 3 * MAX_TRANSACTIONS.div_ceil(SEGMENT_TRANSACTIONS)
                        < crate::capacity_policy::MAX_BYTE_PROFILE_TERMINAL_BYTES
                );
            }
            let legacy =
                TerminalPrefix::from_segments(authority, cohort(&parts), 0, vec![]).unwrap();
            // The actual new tag is measured; no estimated profile allowance.
            let tag_delta = size(&binding) - size(&cohort(&parts));
            assert_eq!(envelope - size(&legacy), tag_delta);
            let segments = CapacityProfile::ByteBound16MiBV1
                .max_transactions()
                .div_ceil(SEGMENT_TRANSACTIONS);
            let overhead = envelope + u64::MAX.to_string().len() - 1 + 3 * segments;
            assert!(overhead <= PROOF_OVERHEAD);
            assert_eq!(
                crate::capacity_policy::MAX_BYTE_PROFILE_TERMINAL_BYTES,
                (4 * MAX_JOURNAL_BYTES).div_ceil(3) + PROOF_OVERHEAD
            );
            let event = admit(authority, identity(wide), &parts, &binding, vec![]);
            assert_event_floor(&event);
            journal.apply(event).unwrap();
            assert_exact_reservation(&journal);
            let reserved = journal.reserved_bytes;
            for participant in &parts {
                journal
                    .apply(Command::Prepared {
                        id: identity(wide),
                        participant: participant.clone(),
                    })
                    .unwrap();
                assert_eq!(journal.reserved_bytes, reserved);
                assert_exact_reservation(&journal);
            }
            journal
                .apply(Command::Decide {
                    id: identity(wide),
                    decision: Decision::Commit,
                })
                .unwrap();
            assert_eq!(journal.encode().unwrap().len(), reserved);
            assert_eq!(journal.terminal_prefix(0).unwrap(), zero);
            let proof = journal.terminal_prefix(1).unwrap();
            assert!(size(&proof) < crate::capacity_policy::MAX_BYTE_PROFILE_TERMINAL_BYTES);
            assert_eq!(
                serde_json::from_slice::<TerminalPrefix>(&serde_json::to_vec(&proof).unwrap())
                    .unwrap(),
                proof
            );
        }
    }
}

#[test]
fn byte_profile_crosses_legacy_counts_and_keeps_holes_original_plans_and_old_cuts() {
    let parts = participants(1, false, false, false);
    let binding = byte_cohort(&parts);
    let mut journal = confirmed(&parts);
    let zero = journal.terminal_prefix(0).unwrap();
    let grant_cut = journal.history_cursor().unwrap();
    let mut old = Vec::new();
    // Raw replay here tests the policy itself. Real 1025 committed graph writes
    // are independently required through the managed gateway acceptance path.
    for ordinal in 1..=1025u128 {
        let id = ordinal.to_le_bytes();
        journal
            .replay_event(admit([9; 16], id, &parts, &binding, vec![]))
            .unwrap();
        if ordinal != 1024 {
            journal
                .replay_event(Command::Decide {
                    id,
                    decision: Decision::Abort,
                })
                .unwrap();
        }
        if [1, 256, 1023].contains(&ordinal) {
            old.push((
                ordinal as u64,
                journal.terminal_prefix(ordinal as u64).unwrap(),
            ));
        }
    }
    assert_eq!(journal.transaction_count(), 1025);
    assert_eq!(journal.requests().len(), 1025);
    assert_eq!(journal.resolved_prefix(), 1023);
    assert_eq!(journal.terminal_prefix(1025), Err(Error::NotPrepared));
    journal
        .replay_event(Command::Decide {
            id: 1024u128.to_le_bytes(),
            decision: Decision::Abort,
        })
        .unwrap();
    assert_eq!(journal.resolved_prefix(), 1025);
    let restored = Journal::decode(&journal.encode().unwrap()).unwrap();
    assert_eq!(restored, journal);
    for (version, proof) in old {
        assert_eq!(restored.terminal_prefix(version).unwrap(), proof);
    }
    assert_eq!(restored.terminal_prefix(0).unwrap(), zero);
    assert_eq!(
        restored.history_prefix(&grant_cut).unwrap(),
        confirmed(&parts)
    );
    for ordinal in [1u128, 256, 1024] {
        let record = journal.request(ordinal.to_le_bytes()).unwrap().clone();
        let plan = Plan::decode(&record.payload).unwrap();
        assert_eq!(plan.version, 1);
        assert_eq!(plan.cohort, binding);
        let retry = admit([9; 16], 2000u128.to_le_bytes(), &parts, &binding, vec![3]);
        let mut retry = match retry {
            Command::AdmitRequest {
                request,
                payload,
                transaction_id,
                participants,
                ..
            } => Command::AdmitRequest {
                id: ordinal.to_le_bytes(),
                request,
                payload,
                transaction_id,
                participants,
            },
            _ => unreachable!(),
        };
        journal.apply(retry.clone()).unwrap();
        assert_eq!(journal, restored);
        if let Command::AdmitRequest { request, .. } = &mut retry {
            request.push(1);
        }
        assert_eq!(journal.apply(retry), Err(Error::IdentityConflict));
        assert_eq!(journal.request(ordinal.to_le_bytes()).unwrap(), &record);
    }
    let proof = restored.terminal_prefix(1025).unwrap();
    assert_eq!(
        serde_json::from_slice::<TerminalPrefix>(&serde_json::to_vec(&proof).unwrap()).unwrap(),
        proof
    );
    let mut wrong = serde_json::to_value(&proof).unwrap();
    wrong["version"] = 1.into();
    assert!(serde_json::from_value::<TerminalPrefix>(wrong).is_err());
    let mut wrong = serde_json::to_value(&proof).unwrap();
    wrong["cohort"].as_object_mut().unwrap().remove("profile");
    assert!(serde_json::from_value::<TerminalPrefix>(wrong).is_err());
}

#[test]
fn byte_profile_history_v2_zero_roots_overlap_and_prefix_replay_never_fall_back() {
    let parts = participants(1, false, false, false);
    let binding = byte_cohort(&parts);
    let mut source = confirmed(&parts);
    let zero = source.history_cursor_at(0).unwrap();
    assert_eq!(zero.version, 2);
    let empty = source.history_prefix(&zero).unwrap();
    assert!(empty.cohort().is_none());
    let first = source
        .history_prefix(&source.history_cursor_at(1).unwrap())
        .unwrap();
    assert_eq!(first.capacity_profile(), CapacityProfile::ByteBound16MiBV1);
    assert!(!first.capacity_ready());
    assert_eq!(
        first.history_cursor().unwrap(),
        source.history_cursor_at(1).unwrap()
    );
    let mut legacy = Journal::new([9; 16]).unwrap();
    legacy
        .apply(Command::BindCohort {
            cohort: cohort(&parts),
        })
        .unwrap();
    assert_ne!(zero.root, legacy.history_cursor_at(0).unwrap().root);
    let mut wrong = zero.clone();
    wrong.version = 1;
    assert_eq!(wrong.validate(), Err(Error::InvalidJournal));
    let mut wrong = zero.clone();
    wrong.cohort.profile = CapacityProfile::LegacyCountV1;
    assert_eq!(wrong.validate(), Err(Error::InvalidJournal));
    let old_cut = source.history_cursor().unwrap();
    let page = source
        .history_page(&first.history_cursor().unwrap(), Some(&old_cut))
        .unwrap();
    assert_eq!(page.version, 2);
    assert_eq!(first.merge_history_page(&page).unwrap(), source);
    for ordinal in 1..=33u128 {
        source
            .replay_event(Command::Begin {
                id: ordinal.to_le_bytes(),
                participants: parts.clone(),
            })
            .unwrap();
        source
            .replay_event(Command::Decide {
                id: ordinal.to_le_bytes(),
                decision: Decision::Abort,
            })
            .unwrap();
    }
    assert_eq!(source.merge_history_page(&page).unwrap(), source);
    assert_eq!(source.history_cursor_at(0).unwrap(), zero);
    assert_eq!(
        source.history_prefix(&old_cut).unwrap().capacity_grants(),
        confirmed(&parts).capacity_grants()
    );
    assert!(legacy.merge_history_page(&page).is_err());
    let mut receiver = first;
    let cut = source.history_cursor().unwrap();
    while receiver.history_cursor().unwrap() != cut {
        let page = source
            .history_page(&receiver.history_cursor().unwrap(), Some(&cut))
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<HistoryPage>(&serde_json::to_vec(&page).unwrap()).unwrap(),
            page
        );
        receiver = receiver.merge_history_page(&page).unwrap();
    }
    assert_eq!(receiver, source);
    let mut too_large = cut;
    too_large.event_count = binding.profile.max_history_events() + 1;
    assert_eq!(too_large.validate(), Err(Error::InvalidJournal));
}

#[test]
fn byte_profile_exact_sixteen_mib_reserves_complete_resolution_and_rejects_one_more_byte() {
    let parts = participants(1, false, false, false);
    let binding = byte_cohort(&parts);
    let mut journal = confirmed(&parts);
    let mut ordinal = 1u128;
    // Large original Plans reach the byte ceiling with few records. Each event
    // remains below 2 MiB and each Plan below its unchanged 256 KiB cap.
    loop {
        let command = admit(
            [9; 16],
            ordinal.to_le_bytes(),
            &parts,
            &binding,
            vec![0; 100_000],
        );
        let mut candidate = journal.clone();
        match candidate.append(command, MAX_JOURNAL_BYTES) {
            Ok(true) => {
                journal = candidate;
                ordinal += 1;
            }
            Err(Error::Capacity) => break,
            result => panic!("unexpected admission: {result:?}"),
        }
    }
    let id = ordinal.to_le_bytes();
    let empty = admit([9; 16], id, &parts, &binding, vec![]);
    let mut minimum = journal.clone();
    minimum.append(empty, MAX_JOURNAL_BYTES).unwrap();
    let remaining = MAX_JOURNAL_BYTES - minimum.reserved_bytes;
    // In this actual nested Plan encoding, n nonempty zero bytes add 6*n-3
    // outer JSON bytes. Measure the exact remainder before request padding.
    let value_len = (remaining + 3) / 6;
    let mut final_event = admit([9; 16], id, &parts, &binding, vec![0; value_len]);
    let mut candidate = journal.clone();
    candidate
        .append(final_event.clone(), MAX_JOURNAL_BYTES)
        .unwrap();
    let padding = MAX_JOURNAL_BYTES - candidate.reserved_bytes;
    assert!(padding < 6);
    if let Command::AdmitRequest { request, .. } = &mut final_event {
        request.extend(std::iter::repeat_n(0, padding / 2));
        if !padding.is_multiple_of(2) {
            request[0] = 10;
        }
    }
    let mut over = final_event.clone();
    if let Command::AdmitRequest { request, .. } = &mut over {
        // Widen exactly one one-digit byte by one decimal digit.
        let index = request.iter().position(|byte| *byte < 10).unwrap_or(0);
        request[index] = if request[index] < 10 { 10 } else { 100 };
    }
    assert_eq!(size(&over), size(&final_event) + 1);
    assert_eq!(
        journal.clone().append(over, MAX_JOURNAL_BYTES),
        Err(Error::Capacity)
    );
    journal.append(final_event, MAX_JOURNAL_BYTES).unwrap();
    assert_eq!(journal.reserved_bytes, MAX_JOURNAL_BYTES);
    assert_exact_reservation(&journal);
    let at_limit = journal.clone();
    assert_eq!(
        journal.apply(Command::Begin {
            id: (ordinal + 1).to_le_bytes(),
            participants: parts.clone()
        }),
        Err(Error::Capacity)
    );
    assert_eq!(journal, at_limit);
    for n in 1..=ordinal {
        journal
            .replay_event(Command::Prepared {
                id: n.to_le_bytes(),
                participant: parts[0].clone(),
            })
            .unwrap();
        journal
            .replay_event(Command::Decide {
                id: n.to_le_bytes(),
                decision: Decision::Commit,
            })
            .unwrap();
        assert_eq!(journal.reserved_bytes, MAX_JOURNAL_BYTES);
    }
    assert_eq!(journal.encode().unwrap().len(), MAX_JOURNAL_BYTES);
    assert_exact_reservation(&journal);
}

#[test]
fn bounded_vectors_reject_excess_before_deserializing_invalid_next_item() {
    fn excess_array(value: &serde_json::Value, count: usize) -> String {
        format!(
            "[{},false]",
            std::iter::repeat_n(value.to_string(), count)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
    let parts = participants(1, false, false, false);
    let member = serde_json::to_value(&cohort(&parts).members[0]).unwrap();
    let text = format!(
        r#"{{"coordinator_shard":9,"coordinator_epoch":1,"members":{}}}"#,
        excess_array(&member, MAX_PARTICIPANTS)
    );
    let error = serde_json::from_str::<Cohort>(&text)
        .unwrap_err()
        .to_string();
    assert!(error.contains("too many retained items"), "{error}");
    let smallest = serde_json::to_value(Command::Decide {
        id: identity(false),
        decision: Decision::Abort,
    })
    .unwrap();
    let text = format!(
        r#"{{"version":4,"authority":{},"events":{}}}"#,
        serde_json::to_string(&identity::<16>(false)).unwrap(),
        excess_array(&smallest, crate::capacity_policy::BYTE_MAX_HISTORY_EVENTS)
    );
    let error = serde_json::from_str::<Encoding>(&text)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("too many retained items"), "{error}");
    let mut malformed =
        serde_json::to_value(confirmed(&parts).terminal_prefix(0).unwrap()).unwrap();
    malformed["through"] = u64::MAX.into();
    assert!(serde_json::from_value::<TerminalPrefix>(malformed).is_err());
}

#[test]
fn byte_profile_journal_history_and_terminal_zero_goldens_are_frozen() {
    let binding = Cohort {
        coordinator_shard: 9,
        coordinator_epoch: 1,
        members: vec![Member {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
        }],
        profile: CapacityProfile::ByteBound16MiBV1,
    };
    let mut journal = Journal::new([9; 16]).unwrap();
    journal
        .apply(Command::BindCohort {
            cohort: binding.clone(),
        })
        .unwrap();
    let zero = journal.terminal_prefix(0).unwrap();
    journal
        .apply(Command::ConfirmCohortCapacity {
            grants: vec![MemberCapacityGrant {
                member: binding.members[0].clone(),
                activation: CapacityActivation {
                    nonce: [1; 32],
                    log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 1),
                },
            }],
            cohort: binding,
        })
        .unwrap();
    let journal_golden = r#"{"version":4,"authority":[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9],"events":[{"BindCohort":{"cohort":{"coordinator_shard":9,"coordinator_epoch":1,"members":[{"shard":7,"epoch":1,"mode":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]}],"profile":"ByteBound16MiBV1"}}},{"ConfirmCohortCapacity":{"cohort":{"coordinator_shard":9,"coordinator_epoch":1,"members":[{"shard":7,"epoch":1,"mode":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]}],"profile":"ByteBound16MiBV1"},"grants":[{"member":{"shard":7,"epoch":1,"mode":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]},"activation":{"nonce":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],"log_id":{"leader_id":{"term":1,"node_id":0},"index":1}}}]}}]}"#;
    let cursor_golden = r#"{"version":2,"authority":{"shard":9,"mode":[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9]},"cohort":{"coordinator_shard":9,"coordinator_epoch":1,"members":[{"shard":7,"epoch":1,"mode":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]}],"profile":"ByteBound16MiBV1"},"event_count":0,"root":[90,144,21,56,75,128,229,95,54,151,106,239,166,80,249,88,244,129,191,23,23,247,53,250,133,146,64,172,186,81,41,92]}"#;
    let terminal_golden = r#"{"version":2,"authority":[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9],"cohort":{"coordinator_shard":9,"coordinator_epoch":1,"members":[{"shard":7,"epoch":1,"mode":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]}],"profile":"ByteBound16MiBV1"},"through":0,"segments":[]}"#;
    assert_eq!(journal.encode().unwrap(), journal_golden.as_bytes());
    assert_eq!(Journal::decode(journal_golden.as_bytes()).unwrap(), journal);
    assert_eq!(
        serde_json::to_vec(&journal.history_cursor_at(0).unwrap()).unwrap(),
        cursor_golden.as_bytes()
    );
    assert_eq!(
        serde_json::from_str::<HistoryCursor>(cursor_golden).unwrap(),
        journal.history_cursor_at(0).unwrap()
    );
    assert_eq!(
        serde_json::to_vec(&zero).unwrap(),
        terminal_golden.as_bytes()
    );
    assert_eq!(
        serde_json::from_str::<TerminalPrefix>(terminal_golden).unwrap(),
        zero
    );
    assert_eq!(journal.terminal_prefix(0).unwrap(), zero);
}

#[test]
fn byte_profile_terminal_bytes_are_bounded_independently_of_structural_record_count() {
    let parts = participants(MAX_PARTICIPANTS, true, true, true);
    let binding = byte_cohort(&parts);
    let one = Terminal {
        id: [255; 16],
        ordinal: 1,
        participants: parts,
        prepared: vec![false; MAX_PARTICIPANTS],
        decision: Decision::Abort,
    };
    let count = crate::capacity_policy::MAX_BYTE_PROFILE_TERMINAL_BYTES / size(&one) + 1;
    assert!(count < binding.profile.max_transactions());
    let mut records = Vec::new();
    for ordinal in 1..=count {
        let mut record = one.clone();
        record.id[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
        record.ordinal = ordinal as u64;
        records.push(record);
    }
    // Use an extra complete segment to dominate any narrower ID digits.
    for ordinal in count + 1..=count + SEGMENT_TRANSACTIONS {
        let mut record = one.clone();
        record.id[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
        record.ordinal = ordinal as u64;
        records.push(record);
    }
    let count = records.len();
    let segments = records
        .chunks(SEGMENT_TRANSACTIONS)
        .map(|segment| segment.to_vec())
        .collect();
    assert_eq!(
        TerminalPrefix::from_segments([255; 16], binding, count as u64, segments),
        Err(Error::InvalidJournal)
    );
}

#[test]
fn grant_generation_decode_rejects_missing_unknown_and_malformed_fields() {
    let binding = byte_cohort(&participants(1, false, false, false));
    let activation = grants(&binding, false)[0].activation.clone();
    let original = serde_json::to_value(&activation).unwrap();
    for path in [
        "/nonce",
        "/log_id",
        "/log_id/leader_id",
        "/log_id/leader_id/term",
        "/log_id/leader_id/node_id",
        "/log_id/index",
    ] {
        let mut invalid = original.clone();
        *invalid.pointer_mut(path).unwrap() = serde_json::Value::Null;
        assert!(
            serde_json::from_value::<CapacityActivation>(invalid).is_err(),
            "{path}"
        );
    }
    for path in ["", "/log_id", "/log_id/leader_id"] {
        let mut invalid = original.clone();
        invalid
            .pointer_mut(path)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("future".to_string(), true.into());
        assert!(
            serde_json::from_value::<CapacityActivation>(invalid).is_err(),
            "{path}"
        );
    }
    assert_eq!(
        serde_json::from_value::<CapacityActivation>(original).unwrap(),
        activation
    );
}

#[test]
fn private_boxed_cohort_preserves_clone_equality_and_all_encoding_bytes() {
    let parts = participants(1, false, false, false);
    let mut legacy = Journal::new([9; 16]).unwrap();
    legacy
        .apply(Command::BindCohort {
            cohort: cohort(&parts),
        })
        .unwrap();
    for original in [legacy, confirmed(&parts)] {
        let before = original.encode().unwrap();
        let mut cloned = original.clone();
        assert_eq!(cloned, original);
        assert_eq!(cloned.encode().unwrap(), before);
        assert_eq!(Journal::decode(&before).unwrap(), original);
        assert!(!std::ptr::eq(
            original.cohort().unwrap(),
            cloned.cohort().unwrap()
        ));
        cloned
            .apply(Command::Begin {
                id: [17; 16],
                participants: parts.clone(),
            })
            .unwrap();
        assert_ne!(cloned, original);
        assert_eq!(original.encode().unwrap(), before);
        assert_eq!(original.transaction_count(), 0);
        assert_eq!(cloned.cohort(), original.cohort());
    }
}
