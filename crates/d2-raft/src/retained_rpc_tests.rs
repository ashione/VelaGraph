// Included in managed_rpc::tests so these exercise the real Request/Reply codec.
fn retained_target_fixture() -> TargetEvidence {
    let journal =
        Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
    TargetEvidence::from_journal(&journal, [2; 16]).unwrap()
}
async fn fake_target_json(value: serde_json::Value) -> (Client, JoinHandle<()>) {
    let result = serde_json::from_value(value).map(Value::Target).map_err(|_| Error::Protocol);
    typed_peer(identity(), Duration::from_secs(2), 1, move |request| {
        assert!(matches!(request.operation, Operation::LookupTarget { id: [2, ..], .. }));
        let result = result.clone();
        Box::pin(async move { wire_reply(request, result) })
    }).await
}

#[tokio::test]
async fn matching_target_envelopes_reject_structural_and_original_binding_substitution() {
    let _resources = test_resource_guard().await;
    let original = retained_target_fixture();
    let value = serde_json::to_value(&original).unwrap();
    let mut bad = Vec::new();
    let mut v = value.clone();
    v["id"] = serde_json::to_value([3; 16]).unwrap();
    bad.push(v);
    let mut v = value.clone();
    v["cut"]["cohort"]["members"][0]["mode"] = serde_json::to_value([8; 16]).unwrap();
    bad.push(v);
    let mut v = value.clone();
    v["record"]["transaction"]["ordinal"] = 0.into();
    bad.push(v);
    let mut v = value.clone();
    v["record"]["transaction"]["ordinal"] = u64::MAX.into();
    bad.push(v);
    let mut v = value.clone();
    v["record"]["transaction"]["prepared"] = serde_json::json!([]);
    bad.push(v);
    let mut v = value.clone();
    v["record"]["transaction"]["prepared"] = serde_json::json!([false]);
    v["record"]["transaction"]["decision"] = "Commit".into();
    bad.push(v);
    let mut v = value.clone();
    v["record"]["request"]["transaction_id"] = serde_json::to_value([4; 16]).unwrap();
    bad.push(v);
    let mut v = value.clone();
    v["record"]["request"]["id"] = serde_json::to_value([0; 16]).unwrap();
    bad.push(v);
    let mut v = value.clone();
    v.as_object_mut().unwrap().remove("record");
    bad.push(v);
    let mut v = value.clone();
    v["record"].as_object_mut().unwrap().remove("request");
    bad.push(v);
    let mut v = value.clone();
    v["record"]["transaction"]
        .as_object_mut()
        .unwrap()
        .remove("decision");
    bad.push(v);
    let mut plan = crate::request_plans::Plan::decode(
        &original
            .record
            .as_ref()
            .unwrap()
            .request
            .as_ref()
            .unwrap()
            .payload,
    )
    .unwrap();
    plan.authority.mode = [8; 16];
    let mut v = value.clone();
    v["record"]["request"]["payload"] = serde_json::to_value(plan.encode().unwrap()).unwrap();
    bad.push(v);
    for (index, value) in bad.into_iter().enumerate() {
        let (client, peer) = fake_target_json(value).await;
        let result = NodeRef::Remote(&client)
            .target(&identity().authority, 1, [2; 16])
            .await;
        assert!(
            matches!(result, Err(Error::Identity | Error::Protocol)),
            "case {index}: {result:?}"
        );
        peer.await.unwrap();
    }
    // Explicit null is a source-provided absence result, not a client inclusion
    // proof. Missing record must not be treated as this valid null result.
    let mut absent = original.clone();
    absent.record = None;
    let (client, peer) = fake_value(identity(), Value::Target(absent.clone())).await;
    assert_eq!(
        NodeRef::Remote(&client)
            .target(&identity().authority, 1, [2; 16])
            .await
            .unwrap(),
        absent
    );
    peer.await.unwrap();
    let (client, peer) = fake_value(identity(), Value::Target(original.clone())).await;
    assert_eq!(
        NodeRef::Remote(&client)
            .target(&identity().authority, 1, [2; 16])
            .await
            .unwrap(),
        original
    );
    peer.await.unwrap();
}

fn widest_retained_identity() -> Identity {
    let max = u64::MAX;
    Identity {
        group: max,
        node: max,
        role: Role::Coordinator,
        authority: Authority {
            shard: max,
            mode: [255; 16],
        },
        target: Target {
            shard: max,
            epoch: max,
            mode: [255; 16],
        },
        cohort: Cohort {
            coordinator_shard: max,
            coordinator_epoch: max,
            profile: CapacityProfile::ByteBound16MiBV1,
            members: (1..=64)
                .rev()
                .map(|offset| crate::transaction_decisions::Member {
                    shard: max - offset,
                    epoch: max,
                    mode: [255; 16],
                })
                .collect(),
        },
    }
}

#[test]
fn complete_target_reply_fits_maximum_member_request_and_plan_envelopes() {
    let identity = widest_retained_identity();
    identity.validate().unwrap();
    let parts: Vec<_> = identity
        .cohort
        .members
        .iter()
        .map(|member| crate::request_plans::Part {
            descriptor: Participant {
                shard: member.shard,
                epoch: member.epoch,
                mode: member.mode,
                intent: [255; 32],
            },
            request: D1WriteRequest {
                shard: member.shard,
                epoch: member.epoch,
                request_id: [255; 16],
                expected_sequence: Some(u64::MAX),
                operations: vec![crate::WriteOp::put([255], [255])],
            },
        })
        .collect();
    let mut payload = crate::request_plans::Plan {
        version: 1,
        id: [255; 16],
        authority: identity.authority.clone(),
        cohort: identity.cohort.clone(),
        validation_version: 0,
        parts: parts.clone(),
    }
    .encode()
    .unwrap();
    // Historical Plan decoding accepts trailing JSON whitespace. Preserve the
    // exact original bytes while exercising the actual full 256 KiB ceiling.
    payload.resize(
        crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES,
        b' ',
    );
    crate::request_plans::Plan::decode(&payload).unwrap();
    let mut journal = Journal::new(identity.authority.mode).unwrap();
    journal
        .apply(Command::BindCohort {
            cohort: identity.cohort.clone(),
        })
        .unwrap();
    journal
        .apply(Command::ConfirmCohortCapacity {
            cohort: identity.cohort.clone(),
            grants: identity
                .cohort
                .members
                .iter()
                .map(|member| MemberCapacityGrant {
                    member: member.clone(),
                    activation: crate::transaction_decisions::CapacityActivation {
                        nonce: [255; 32],
                        log_id: openraft::LogId::new(
                            openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
                            u64::MAX,
                        ),
                    },
                })
                .collect(),
        })
        .unwrap();
    journal
        .apply(Command::AdmitRequest {
            id: [254; 16],
            request: vec![255; crate::transaction_decisions::MAX_REQUEST_BYTES],
            payload,
            transaction_id: [255; 16],
            participants: parts.iter().map(|part| part.descriptor.clone()).collect(),
        })
        .unwrap();
    let target = TargetEvidence::from_journal(&journal, [255; 16]).unwrap();
    let lookup = Request {
        version: PROTOCOL_VERSION,
        identity: identity.clone(),
        nonce: u64::MAX,
        operation: Operation::LookupTarget {
            authority: identity.authority.clone(),
            epoch: u64::MAX,
            id: [255; 16],
        },
    };
    let lookup_bytes = encode_sync(&lookup).unwrap();
    let decoded: Request = serde_json::from_slice(&lookup_bytes).unwrap();
    assert_eq!(decoded.operation.tag(), 17);
    let reply = Reply {
        version: PROTOCOL_VERSION,
        operation: 17,
        identity,
        nonce: u64::MAX,
        result: Ok(Value::Target(target.clone())),
    };
    let bytes = encode_sync(&reply).unwrap();
    assert!(bytes.len() < MAX_FRAME_BYTES);
    // This bound covers the full Reply, duplicated endpoint and cursor cohorts,
    // all 64 descriptors, raw external request and opaque original Plan arrays.
    assert!(
        bytes.len() < 2 * 1024 * 1024,
        "full target reply {}",
        bytes.len()
    );
    let decoded: Reply = serde_json::from_slice(&bytes).unwrap();
    let Ok(Value::Target(decoded)) = decoded.result else {
        panic!("wrong reply")
    };
    assert_eq!(decoded, target);
}

#[test]
fn strict_direct_prepare_complete_request_obeys_actual_frame_boundary() {
    let mut identity = widest_retained_identity();
    let member = identity.cohort.members[0].clone();
    identity.role = Role::Participant;
    identity.target = Target {
        shard: member.shard,
        epoch: member.epoch,
        mode: member.mode,
    };
    let participant = Participant {
        shard: member.shard,
        epoch: member.epoch,
        mode: member.mode,
        intent: [255; 32],
    };
    let mut journal = Journal::new(identity.authority.mode).unwrap();
    journal
        .apply(Command::BindCohort {
            cohort: identity.cohort.clone(),
        })
        .unwrap();
    let cut = journal.history_cursor().unwrap();
    let prepare = RetainedPrepare {
        version: 1,
        authority: identity.authority.clone(),
        participant,
        request: D1WriteRequest {
            shard: member.shard,
            epoch: member.epoch,
            request_id: [255; 16],
            expected_sequence: Some(u64::MAX),
            operations: vec![crate::WriteOp::put([], vec![255])],
        },
        cut,
    };
    let mut request = Request {
        version: PROTOCOL_VERSION,
        identity,
        nonce: u64::MAX,
        operation: Operation::PrepareRetained(Box::new(prepare)),
    };
    let baseline = encode_sync(&request).unwrap().len();
    let available = MAX_FRAME_BYTES - baseline;
    let Operation::PrepareRetained(prepare) = &mut request.operation else {
        unreachable!()
    };
    let crate::WriteOp::Put { value, .. } = &mut prepare.request.operations[0] else {
        unreachable!()
    };
    value.resize(1 + available / 4, 255);
    match available % 4 {
        0 => {}
        1 => {
            value[0] = 25;
            value.push(0);
        }
        2 => value.push(0),
        3 => {
            value[0] = 25;
            value.push(255);
        }
        _ => unreachable!(),
    }
    assert!(value.len() > crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES);
    assert!(value.len() < crate::MAX_BATCH_BYTES);
    prepare.validate().unwrap();
    let bytes = encode_sync(&request).unwrap();
    assert_eq!(bytes.len(), MAX_FRAME_BYTES);
    let decoded: Request = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.operation.tag(), 18);
    drop(decoded);
    drop(bytes);
    let Operation::PrepareRetained(prepare) = &mut request.operation else {
        unreachable!()
    };
    let crate::WriteOp::Put { value, .. } = &mut prepare.request.operations[0] else {
        unreachable!()
    };
    value.push(0);
    prepare.validate().unwrap();
    assert!(matches!(encode_sync(&request), Err(Error::Capacity)));
}

#[tokio::test]
async fn retained_prepare_pinned_identity_rejects_before_connecting() {
    let _resources = test_resource_guard().await;
    let evidence = retained_target_fixture();
    let record = evidence.record.as_ref().unwrap();
    let plan =
        crate::request_plans::Plan::decode(&record.request.as_ref().unwrap().payload).unwrap();
    let part = &plan.parts[0];
    let mut member = identity();
    member.role = Role::Participant;
    member.target = Target {
        shard: part.descriptor.shard,
        epoch: part.descriptor.epoch,
        mode: part.descriptor.mode,
    };
    for pinned in [
        identity(),
        Identity {
            authority: Authority {
                shard: 9,
                mode: [8; 16],
            },
            ..member
        },
    ] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let client = Client::new(
            listener.local_addr().unwrap(),
            pinned,
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            NodeRef::Remote(&client)
                .prepare_retained(
                    &identity().authority,
                    &part.descriptor,
                    part.request.clone(),
                    &evidence.cut
                )
                .await,
            Err(Error::Identity)
        );
        assert!(listener
            .accept()
            .is_err_and(|e| e.kind() == std::io::ErrorKind::WouldBlock));
    }
}
