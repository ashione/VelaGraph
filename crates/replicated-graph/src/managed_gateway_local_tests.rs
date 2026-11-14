use super::*;
use crate::remote::{GraphService, RemoteIdentity};
use d2_raft::managed_rpc::{Identity, Role, Server};

fn identity(shard: u64) -> Identity {
    Identity {
        group: shard,
        node: 1,
        target: target(shard),
        role: if shard == 9 {
            Role::Coordinator
        } else {
            Role::Participant
        },
        authority: authority(),
        cohort: binding(),
    }
}
async fn local_gateway(f: &Fixture) -> (Gateway, Vec<Server>, Vec<GraphService>) {
    let mut servers = Vec::new();
    let mut graphs = Vec::new();
    let mut clients = Vec::new();
    let mut routes = Vec::new();
    for (node, shard) in f.nodes.iter().zip([9, 7, 8]) {
        let server = Server::bind(
            node.clone(),
            identity(shard),
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(15),
        )
        .await
        .unwrap();
        clients.push(
            Client::new(
                server.local_addr(),
                identity(shard),
                Duration::from_secs(15),
            )
            .unwrap(),
        );
        servers.push(server);
        if shard != 9 {
            let graph = GraphService::start_managed(
                node,
                "127.0.0.1:0".parse().unwrap(),
                shard,
                shard,
                Duration::from_secs(15),
            )
            .await
            .unwrap();
            routes.push(LocalGraphRoute {
                endpoint: graph.local_addr(),
                identity: RemoteIdentity {
                    group: shard,
                    node: 1,
                },
                managed_identity: identity(shard),
            });
            graphs.push(graph);
        }
    }
    let gateway = Gateway::bind_remote_local_queries(
        RemoteConfig {
            coordinator: clients.remove(0),
            binding: bound(),
            members: clients,
        },
        routes,
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(15),
    )
    .await
    .unwrap();
    (gateway, servers, graphs)
}
fn local_open(version: Option<u64>, text: &str, size: u16) -> Operation {
    Operation::OpenLocalQuery {
        version,
        ttl_millis: 60_000,
        query: Query {
            partition: 7,
            text: text.into(),
        },
        page_size: size,
    }
}
fn ready(reply: Reply) -> (LocalSession, Query, QueryPage) {
    match reply {
        Reply::OpenLocalQuery {
            session,
            query,
            page,
            ..
        } => (session, query, page),
        other => panic!("{other:?}"),
    }
}
#[test]
fn explicit_profile_reply_binding_and_legacy_wire_shapes() {
    let query = Query {
        partition: 7,
        text: "g.V(1).count()".into(),
    };
    let session = LocalSession {
        incarnation: [1; 16],
        token: [2; 32],
        version: 7,
    };
    let page = QueryPage {
        data: QueryData {
            rows: vec![QueryRow::Count(1)],
            completeness: QueryCompleteness::Unverified,
        },
        next: None,
        offset: 0,
        total: 1,
    };
    let legacy_op = Operation::OpenLocalQuery {
        version: Some(7),
        ttl_millis: 60_000,
        query: query.clone(),
        page_size: 1,
    };
    assert_eq!(
        serde_json::to_string(&legacy_op).unwrap(),
        r#"{"OpenLocalQuery":{"version":7,"ttl_millis":60000,"query":{"partition":7,"text":"g.V(1).count()"},"page_size":1}}"#
    );
    let session_wire = serde_json::to_value(&session).unwrap();
    assert_eq!(session_wire.as_object().unwrap().len(), 3);
    assert_eq!(
        serde_json::from_value::<LocalSession>(session_wire).unwrap(),
        session
    );
    let legacy = Reply::OpenLocalQuery {
        session: session.clone(),
        query: query.clone(),
        page_size: 1,
        page: page.clone(),
    };
    let response = |reply| Response {
        version: VERSION,
        request_id: [9; 16],
        binding: bound(),
        result: Ok(reply),
    };
    let mut causal_request = Request {
        version: VERSION,
        request_id: [9; 16],
        binding: bound(),
        operation: Operation::OpenLocalQueryAfter {
            minimum_version: 7,
            ttl_millis: 60_000,
            query: query.clone(),
            page_size: 1,
        },
    };
    assert_eq!(
        validate_reply(&causal_request, response(legacy.clone())),
        Ok(legacy.clone())
    );
    if let Operation::OpenLocalQueryAfter {
        minimum_version, ..
    } = &mut causal_request.operation
    {
        *minimum_version = 8;
    }
    assert_eq!(
        validate_reply(&causal_request, response(legacy.clone())),
        Err(Error::Identity)
    );
    assert_eq!(
        serde_json::from_str::<Operation>(
            &serde_json::to_string(&causal_request.operation).unwrap()
        )
        .unwrap(),
        causal_request.operation
    );
    for profile in [
        LocalReadProfile::Parity2MiBV1,
        LocalReadProfile::LocalRead8MiBV1,
    ] {
        let operation = Operation::OpenLocalQueryWithProfile {
            profile,
            version: Some(7),
            ttl_millis: 60_000,
            query: query.clone(),
            page_size: 1,
        };
        let request = Request {
            version: VERSION,
            request_id: [9; 16],
            binding: bound(),
            operation: operation.clone(),
        };
        let reply = Reply::OpenLocalQueryWithProfile {
            profile,
            session: session.clone(),
            query: query.clone(),
            page_size: 1,
            page: page.clone(),
        };
        assert_eq!(
            validate_reply(&request, response(reply.clone())),
            Ok(reply.clone())
        );
        assert_eq!(
            validate_reply(&request, response(legacy.clone())),
            Err(Error::Identity)
        );
        let mut altered = reply.clone();
        let Reply::OpenLocalQueryWithProfile {
            profile: returned, ..
        } = &mut altered
        else {
            unreachable!()
        };
        *returned = if profile == LocalReadProfile::Parity2MiBV1 {
            LocalReadProfile::LocalRead8MiBV1
        } else {
            LocalReadProfile::Parity2MiBV1
        };
        assert_eq!(
            validate_reply(&request, response(altered)),
            Err(Error::Identity)
        );
        let old_request = Request {
            version: VERSION,
            request_id: [9; 16],
            binding: bound(),
            operation: legacy_op.clone(),
        };
        assert_eq!(
            validate_reply(&old_request, response(reply.clone())),
            Err(Error::Identity)
        );
        let mut stripped = serde_json::to_value(reply).unwrap();
        stripped["OpenLocalQueryWithProfile"]
            .as_object_mut()
            .unwrap()
            .remove("profile");
        assert!(serde_json::from_value::<Reply>(stripped).is_err());
        for value in [
            serde_json::Value::Null,
            serde_json::json!("FutureUnlimitedV2"),
        ] {
            let mut invalid = serde_json::to_value(&operation).unwrap();
            invalid["OpenLocalQueryWithProfile"]["profile"] = value;
            assert!(serde_json::from_value::<Operation>(invalid).is_err());
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_result_replays_original_v_after_writes_and_node_loss() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    let (zero, _, page) = ready(
        call(&g, local_open(Some(0), "g.V(1).count()", 1))
            .await
            .unwrap(),
    );
    assert_eq!(page.data.rows, vec![QueryRow::Count(0)]);
    call(&g, Operation::CloseLocalQuery { session: zero })
        .await
        .unwrap();
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    committed(&call(&g, Operation::Submit(create(2, 8, 1))).await.unwrap());
    committed(
        &call(
            &g,
            Operation::Submit(Write {
                external_id: [3; 16],
                mutation: Mutation::CreateEdge { edge: edge() },
            }),
        )
        .await
        .unwrap(),
    );
    let (session, query, first) = ready(
        call(&g, local_open(None, "g.V(1,1).out().path()", 1))
            .await
            .unwrap(),
    );
    assert_eq!(session.version, 3);
    assert_eq!(first.total, 2);
    let expected = QueryRow::Path(vec![
        crate::remote::query::QualifiedElement::Vertex {
            partition: 7,
            id: 1,
        },
        crate::remote::query::QualifiedElement::Edge {
            partition: 7,
            id: 10,
        },
        crate::remote::query::QualifiedElement::Vertex {
            partition: 8,
            id: 1,
        },
    ]);
    assert_eq!(first.data.rows, vec![expected.clone()]);
    let old = match call(
        &g,
        Operation::OpenSession {
            version: Some(3),
            ttl_millis: 60_000,
        },
    )
    .await
    .unwrap()
    {
        Reply::OpenSession(s) => s,
        _ => panic!(),
    };
    let Reply::Traverse { data, .. } = call(
        &g,
        Operation::Traverse {
            session: old.clone(),
            query: query.clone(),
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(data.rows, vec![expected.clone(), expected.clone()]);
    call(&g, Operation::Close { session: old }).await.unwrap();
    committed(&call(&g, Operation::Submit(create(4, 7, 2))).await.unwrap());
    let fetch = Operation::FetchLocalQuery {
        session: session.clone(),
        query: query.clone(),
        cursor: first.next.clone(),
        page_size: 1,
    };
    let original = call(&g, fetch.clone()).await.unwrap();
    for graph in graphs {
        graph.shutdown().await;
    }
    assert_eq!(call(&g, fetch).await.unwrap(), original);
    let Reply::FetchLocalQuery { page, .. } = call(
        &g,
        Operation::FetchLocalQuery {
            session: session.clone(),
            query: query.clone(),
            cursor: None,
            page_size: 1,
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(page, first);
    // Missing roots do not skip full-cohort certification/capture.
    assert!(call(&g, local_open(None, "g.V(999).count()", 1))
        .await
        .is_err());
    assert_eq!(
        call(
            &g,
            Operation::FetchLocalQuery {
                session: session.clone(),
                query,
                cursor: None,
                page_size: 2
            }
        )
        .await,
        Err(Error::Identity)
    );
    call(
        &g,
        Operation::CloseLocalQuery {
            session: session.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        call(&g, Operation::CloseLocalQuery { session }).await,
        Err(Error::SessionUnavailable)
    );
    g.shutdown().await;
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_unsupported_and_shared_gateway_reservations() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    for text in ["g.V()", "g.V(1).both()", "g.V(1).limit(1)"] {
        assert!(matches!(
            call(&g, local_open(None, text, 1)).await,
            Err(Error::Query(_))
        ));
        assert_eq!(g.retained_payload_charge(), 0);
    }
    let mut old = Vec::new();
    for _ in 0..3 {
        let Reply::OpenSession(session) = call(
            &g,
            Operation::OpenSession {
                version: Some(0),
                ttl_millis: 60_000,
            },
        )
        .await
        .unwrap() else {
            panic!()
        };
        old.push(session);
    }
    let (local, query, _) = ready(
        call(&g, local_open(Some(0), "g.V(1).count()", 1))
            .await
            .unwrap(),
    );
    assert_eq!(g.retained_payload_charge(), MAX_RETAINED_BYTES);
    assert_eq!(
        call(&g, local_open(Some(0), "g.V(1).count()", 1)).await,
        Err(Error::Capacity)
    );
    let held = g.state.get_local(&local).unwrap();
    call(&g, Operation::CloseLocalQuery { session: local })
        .await
        .unwrap();
    assert_eq!(g.retained_payload_charge(), MAX_RETAINED_BYTES);
    drop(held);
    assert_eq!(g.retained_payload_charge(), 3 * SESSION_CHARGE);
    let (local, _, _) = ready(
        call(
            &g,
            Operation::OpenLocalQuery {
                version: Some(0),
                ttl_millis: 500,
                query: query.clone(),
                page_size: 1,
            },
        )
        .await
        .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(550)).await;
    assert_eq!(
        call(
            &g,
            Operation::FetchLocalQuery {
                session: local,
                query,
                cursor: None,
                page_size: 1
            }
        )
        .await,
        Err(Error::SessionUnavailable)
    );
    for session in old {
        call(&g, Operation::Close { session }).await.unwrap();
    }
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

#[tokio::test]
async fn fixed_graph_routes_bind_complete_managed_identity() {
    let client = |shard| {
        Client::new(
            "127.0.0.1:12345".parse().unwrap(),
            identity(shard),
            Duration::from_secs(1),
        )
        .unwrap()
    };
    let config = RemoteConfig {
        coordinator: client(9),
        binding: bound(),
        members: vec![client(7), client(8)],
    };
    let routes: Vec<_> = [7u64, 8]
        .into_iter()
        .map(|shard| LocalGraphRoute {
            endpoint: format!("127.0.0.1:{}", 10000 + shard).parse().unwrap(),
            identity: RemoteIdentity {
                group: shard,
                node: 1,
            },
            managed_identity: identity(shard),
        })
        .collect();
    local::validate_routes(&config, &routes).unwrap();
    let mut duplicate_group = routes.clone();
    duplicate_group[1].managed_identity.group = 7;
    duplicate_group[1].managed_identity.node = 2;
    duplicate_group[1].identity = RemoteIdentity { group: 7, node: 2 };
    let conflicting = RemoteConfig {
        coordinator: client(9),
        binding: bound(),
        members: vec![
            client(7),
            Client::new(
                "127.0.0.1:12345".parse().unwrap(),
                duplicate_group[1].managed_identity.clone(),
                Duration::from_secs(1),
            )
            .unwrap(),
        ],
    };
    assert_eq!(
        local::validate_routes(&conflicting, &duplicate_group),
        Err(Error::Identity)
    );
    // Group identity is independently configured, not inferred from shard.
    let mut different_group = routes.clone();
    different_group[1].managed_identity.group = 80;
    different_group[1].identity.group = 80;
    let valid_mapping = RemoteConfig {
        coordinator: client(9),
        binding: bound(),
        members: vec![
            client(7),
            Client::new(
                "127.0.0.1:12345".parse().unwrap(),
                different_group[1].managed_identity.clone(),
                Duration::from_secs(1),
            )
            .unwrap(),
        ],
    };
    local::validate_routes(&valid_mapping, &different_group).unwrap();
    for kind in 0..7 {
        let mut bad = routes.clone();
        match kind {
            0 => bad[0].identity.node = 2,
            1 => bad[0].managed_identity.target.mode = [99; 16],
            2 => {
                bad[0].managed_identity.cohort.profile =
                    d2_raft::transaction_decisions::CapacityProfile::ByteBound16MiBV1
            }
            3 => bad[0].managed_identity.authority.mode = [99; 16],
            4 => bad[0].endpoint = bad[1].endpoint,
            5 => bad.swap(0, 1),
            _ => bad[0].managed_identity.role = Role::Coordinator,
        }
        assert_eq!(local::validate_routes(&config, &bad), Err(Error::Identity));
    }
}

#[tokio::test]
async fn fixed_graph_protocol_member_limit_is_checked_before_admission() {
    let mut binding = bound();
    binding.cohort.members = (10..74)
        .map(|shard| d2_raft::transaction_decisions::Member {
            shard,
            epoch: 1,
            mode: [shard as u8; 16],
        })
        .collect();
    let mut coordinator = identity(9);
    coordinator.cohort = binding.cohort.clone();
    let mut members = Vec::new();
    let mut routes = Vec::new();
    for member in &binding.cohort.members {
        let id = Identity {
            group: member.shard,
            node: 1,
            target: d2_raft::versioned_reads::Target {
                shard: member.shard,
                epoch: member.epoch,
                mode: member.mode,
            },
            role: Role::Participant,
            authority: binding.authority.clone(),
            cohort: binding.cohort.clone(),
        };
        members.push(
            Client::new(
                "127.0.0.1:12345".parse().unwrap(),
                id.clone(),
                Duration::from_secs(1),
            )
            .unwrap(),
        );
        routes.push(LocalGraphRoute {
            endpoint: format!("127.0.0.1:{}", 10000 + member.shard)
                .parse()
                .unwrap(),
            identity: RemoteIdentity {
                group: id.group,
                node: id.node,
            },
            managed_identity: id,
        });
    }
    let config = RemoteConfig {
        coordinator: Client::new(
            "127.0.0.1:12345".parse().unwrap(),
            coordinator,
            Duration::from_secs(1),
        )
        .unwrap(),
        binding,
        members,
    };
    local::validate_routes(&config, &routes).unwrap();
    let mut oversized = config.coordinator.identity().clone();
    oversized
        .cohort
        .members
        .push(d2_raft::transaction_decisions::Member {
            shard: 74,
            epoch: 1,
            mode: [74; 16],
        });
    assert!(Client::new(
        "127.0.0.1:12345".parse().unwrap(),
        oversized,
        Duration::from_secs(1)
    )
    .is_err());
    // A caller cannot bypass the gateway bound with an extra configured route.
    routes.push(routes[0].clone());
    assert_eq!(
        local::validate_routes(&config, &routes),
        Err(Error::Identity)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_qualified_bags_match_input_model() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    let mut external = 1u8;
    for (partition, id) in [(7, 1), (7, 2), (8, 1)] {
        committed(
            &call(&g, Operation::Submit(create(external, partition, id)))
                .await
                .unwrap(),
        );
        external += 1;
    }
    // Input-only reference records: same IDs across partitions, parallel edges,
    // a self-loop and a cycle. The oracle never reads production graph storage.
    let inputs = [
        (10, (7, 1), (8, 1), "x"),
        (11, (7, 1), (8, 1), "x"),
        (12, (7, 1), (7, 1), "self"),
        (13, (8, 1), (7, 1), "x"),
        (14, (7, 2), (8, 1), "x"),
    ];
    for (id, from, to, label) in inputs {
        committed(
            &call(
                &g,
                Operation::Submit(Write {
                    external_id: [external; 16],
                    mutation: Mutation::CreateEdge {
                        edge: EdgeData {
                            id,
                            from: entity(from.0, from.1),
                            to: entity(to.0, to.1),
                            label: label.into(),
                            properties: BTreeMap::new(),
                        },
                    },
                }),
            )
            .await
            .unwrap(),
        );
        external += 1;
    }
    use crate::remote::query::QualifiedElement as E;
    let v = |p: u32, id: u64| E::Vertex { partition: p, id };
    for (text, directions, label) in [
        ("g.V(1,1).out().path()", vec![false], None),
        ("g.V(1,1).in().path()", vec![true], None),
        (
            "g.V(1,1).out('x').in('x').path()",
            vec![false, true],
            Some("x"),
        ),
        (
            "g.V(1,1).out().out().out().path()",
            vec![false, false, false],
            None,
        ),
    ] {
        let mut model = vec![((7, 1), vec![v(7, 1)]); 2];
        for incoming in directions {
            let mut next = Vec::new();
            for (vertex, path) in model {
                for (id, from, to, edge_label) in inputs {
                    if (if incoming { to } else { from }) == vertex
                        && label.is_none_or(|l| l == edge_label)
                    {
                        let dest = if incoming { from } else { to };
                        let mut path = path.clone();
                        path.push(E::Edge {
                            partition: from.0,
                            id,
                        });
                        path.push(v(dest.0, dest.1));
                        next.push((dest, path));
                    }
                }
            }
            model = next;
        }
        model.truncate(100);
        let expected: Vec<_> = model
            .into_iter()
            .map(|(_, path)| QueryRow::Path(path))
            .collect();
        let (session, query, page) =
            ready(call(&g, local_open(Some(8), text, 1000)).await.unwrap());
        assert_eq!(page.data.rows, expected, "{text}");
        for profile in [
            LocalReadProfile::Parity2MiBV1,
            LocalReadProfile::LocalRead8MiBV1,
        ] {
            let Reply::OpenLocalQueryWithProfile {
                profile: returned,
                session: selected,
                page: selected_page,
                ..
            } = call(
                &g,
                Operation::OpenLocalQueryWithProfile {
                    profile,
                    version: Some(8),
                    ttl_millis: 60_000,
                    query: query.clone(),
                    page_size: 1000,
                },
            )
            .await
            .unwrap()
            else {
                panic!("expected selected profile reply")
            };
            assert_eq!(returned, profile);
            assert_eq!(selected_page, page, "{text}: {profile:?}");
            call(&g, Operation::CloseLocalQuery { session: selected })
                .await
                .unwrap();
        }
        let Reply::OpenSession(old) = call(
            &g,
            Operation::OpenSession {
                version: Some(8),
                ttl_millis: 60_000,
            },
        )
        .await
        .unwrap() else {
            panic!()
        };
        let Reply::Traverse { data, .. } = call(
            &g,
            Operation::Traverse {
                session: old.clone(),
                query,
            },
        )
        .await
        .unwrap() else {
            panic!()
        };
        assert_eq!(page.data, data, "{text}");
        call(&g, Operation::Close { session: old }).await.unwrap();
        call(&g, Operation::CloseLocalQuery { session })
            .await
            .unwrap();
    }
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_rejects_same_v_actual_retained_proof_substitution() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    for downgrade_profile in [false, true] {
        let mut routes = g.state.config.local_routes.clone().unwrap();
        let real = routes[0].endpoint;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        routes[0].endpoint = listener.local_addr().unwrap();
        let proxy = tokio::spawn(run_fixed_script(listener, move |mut events| async move {
            let event = events.recv().await.unwrap();
            let FixedRpc::Open(mut open) = event.request else { panic!("expected OpenFixed") };
            if downgrade_profile {
                let request = open.request.as_mut().unwrap();
                assert_eq!(request.profile, fixed_wire::LocalReadProfile::LocalRead8MibV1 as i32);
                request.profile = fixed_wire::LocalReadProfile::Parity2MibV1 as i32;
                request.capture_limit.as_mut().unwrap().bytes = 2 * 1024 * 1024;
            }
            let FixedRpcReply::Open(mut opened) = forward_fixed(real, FixedRpc::Open(open)).await else { panic!() };
            let descriptor = opened.descriptor.clone().unwrap();
            let proof = opened.retained_proof.as_mut().unwrap();
            assert_eq!(proof.through, 0);
            if !downgrade_profile {
                // Independently valid V0 evidence, but from another authority.
                proof.authority = vec![66; 16];
            } else {
                assert_eq!(descriptor.profile, fixed_wire::LocalReadProfile::Parity2MibV1 as i32);
            }
            assert!(event.reply.send(Ok(FixedRpcReply::Open(opened))).is_ok());
            // Proof/profile rejection still closes the known safe descriptor.
            let event = events.recv().await.unwrap();
            let FixedRpc::Close(close) = &event.request else { panic!("expected CloseFixed") };
            assert_eq!(close.descriptor.as_ref(), Some(&descriptor));
            let reply = forward_fixed(real, event.request).await;
            assert!(event.reply.send(Ok(reply)).is_ok());
        }));
        let client = |node: &OwnedNode| match node {
            OwnedNode::Remote(c) => c.clone(),
            _ => panic!(),
        };
        let bad = Gateway::bind_remote_local_queries(
            RemoteConfig {
                coordinator: client(&g.state.config.coordinator),
                binding: bound(),
                members: g.state.config.members.iter().map(client).collect(),
            },
            routes,
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(15),
        )
        .await
        .unwrap();
        assert_eq!(
            call(
                &bad,
                if downgrade_profile {
                    Operation::OpenLocalQueryWithProfile {
                        profile: LocalReadProfile::LocalRead8MiBV1,
                        version: Some(0),
                        ttl_millis: 60_000,
                        query: Query {
                            partition: 7,
                            text: "g.V(999).count()".into(),
                        },
                        page_size: 1,
                    }
                } else {
                    local_open(Some(0), "g.V(999).count()", 1)
                }
            )
            .await,
            Err(Error::Identity)
        );
        tokio::time::timeout(Duration::from_secs(15), proxy)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bad.retained_payload_charge(), 0);
        bad.shutdown().await;
    }
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

// Test-only typed scripts inspect real fixed RPCs and withhold actual upstream
// responses at deterministic boundaries. No JSON, fake views, or engine changes.
use crate::remote::distributed::grpc::wire as fixed_wire;
#[derive(Clone)]
enum FixedRpc {
    Open(fixed_wire::FixedOpenEnvelope),
    Fragment(fixed_wire::FixedFragmentEnvelope),
    Close(fixed_wire::FixedCloseEnvelope),
}
impl FixedRpc {
    fn fragment(&self) -> Option<&fixed_wire::FixedFragmentRequest> {
        match self { Self::Fragment(v) => v.request.as_ref(), _ => None }
    }
    fn is_close(&self) -> bool { matches!(self, Self::Close(_)) }
}
enum FixedRpcReply {
    Open(fixed_wire::FixedOpened),
    Fragment(fixed_wire::FixedFragmentResponse),
    Close(fixed_wire::Empty),
}
struct FixedEvent {
    request: FixedRpc,
    reply: tokio::sync::oneshot::Sender<Result<FixedRpcReply, tonic::Status>>,
}
#[derive(Clone)]
struct FixedScript(tokio::sync::mpsc::Sender<FixedEvent>);
impl FixedScript {
    async fn dispatch(&self, request: FixedRpc) -> Result<FixedRpcReply, tonic::Status> {
        let (reply, response) = tokio::sync::oneshot::channel();
        self.0.send(FixedEvent { request, reply }).await
            .map_err(|_| tonic::Status::unavailable("fault script finished"))?;
        response.await.map_err(|_| tonic::Status::unavailable("fault script dropped response"))?
    }
}
#[tonic::async_trait]
impl fixed_wire::fixed_query_service_server::FixedQueryService for FixedScript {
    async fn open_fixed(&self, request: tonic::Request<fixed_wire::FixedOpenEnvelope>) -> Result<tonic::Response<fixed_wire::FixedOpened>, tonic::Status> {
        match self.dispatch(FixedRpc::Open(request.into_inner())).await? {
            FixedRpcReply::Open(v) => Ok(tonic::Response::new(v)), _ => panic!("wrong scripted reply"),
        }
    }
    async fn fragment_fixed(&self, request: tonic::Request<fixed_wire::FixedFragmentEnvelope>) -> Result<tonic::Response<fixed_wire::FixedFragmentResponse>, tonic::Status> {
        match self.dispatch(FixedRpc::Fragment(request.into_inner())).await? {
            FixedRpcReply::Fragment(v) => Ok(tonic::Response::new(v)), _ => panic!("wrong scripted reply"),
        }
    }
    async fn close_fixed(&self, request: tonic::Request<fixed_wire::FixedCloseEnvelope>) -> Result<tonic::Response<fixed_wire::Empty>, tonic::Status> {
        match self.dispatch(FixedRpc::Close(request.into_inner())).await? {
            FixedRpcReply::Close(v) => Ok(tonic::Response::new(v)), _ => panic!("wrong scripted reply"),
        }
    }
    async fn open_renewable(&self, _: tonic::Request<fixed_wire::RenewableOpenEnvelope>) -> Result<tonic::Response<fixed_wire::RenewableFixedOpened>, tonic::Status> {
        panic!("fixed fixture must not open a renewable session")
    }
    async fn renew(&self, _: tonic::Request<fixed_wire::FixedRenewEnvelope>) -> Result<tonic::Response<fixed_wire::FixedLeaseGrant>, tonic::Status> {
        panic!("fixed fixture must not renew or refresh its view")
    }
}
async fn forward_fixed(real: SocketAddr, request: FixedRpc) -> FixedRpcReply {
    let mut client = fixed_wire::fixed_query_service_client::FixedQueryServiceClient::connect(format!("http://{real}")).await.unwrap()
        .max_decoding_message_size(crate::remote::MAX_FRAME_BYTES)
        .max_encoding_message_size(crate::remote::MAX_FRAME_BYTES);
    match request {
        FixedRpc::Open(v) => FixedRpcReply::Open(client.open_fixed(v).await.unwrap().into_inner()),
        FixedRpc::Fragment(v) => FixedRpcReply::Fragment(client.fragment_fixed(v).await.unwrap().into_inner()),
        FixedRpc::Close(v) => FixedRpcReply::Close(client.close_fixed(v).await.unwrap().into_inner()),
    }
}
async fn run_fixed_script<F, Fut, T>(listener: TcpListener, script: F) -> T
where
    F: FnOnce(tokio::sync::mpsc::Receiver<FixedEvent>) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let (events, receiver) = tokio::sync::mpsc::channel(16);
    let stopped = events.clone();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        tonic::transport::Server::builder()
            .timeout(Duration::from_secs(15))
            .add_service(fixed_wire::fixed_query_service_server::FixedQueryServiceServer::new(FixedScript(events))
                .max_decoding_message_size(crate::remote::MAX_FRAME_BYTES)
                .max_encoding_message_size(crate::remote::MAX_FRAME_BYTES))
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), stopped.closed())
            .await.unwrap();
    });
    let result = script(receiver).await;
    // Dropping the receiver starts graceful shutdown, flushing the last response.
    // JoinSet owns the server if an assertion or caller cancellation interrupts us.
    tasks.join_next().await.unwrap().unwrap();
    result
}

fn remote_config(g: &Gateway) -> RemoteConfig {
    let client = |node: &OwnedNode| match node {
        OwnedNode::Remote(c) => c.clone(),
        _ => panic!(),
    };
    RemoteConfig {
        coordinator: client(&g.state.config.coordinator),
        binding: bound(),
        members: g.state.config.members.iter().map(client).collect(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_fault_timing_respects_last_required_access() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    for after_last_read in [false, true] {
        let f = Fixture::new().await;
        let (g, servers, mut graphs) = local_gateway(&f).await;
        committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
        let mut routes = g.state.config.local_routes.clone().unwrap();
        let real = routes[0].endpoint;
        let graph = graphs.remove(0);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        routes[0].endpoint = listener.local_addr().unwrap();
        let proxy = tokio::spawn(run_fixed_script(listener, move |mut events| async move {
            let event = events.recv().await.unwrap();
            assert!(matches!(event.request, FixedRpc::Open(_)));
            let reply = forward_fixed(real, event.request).await;
            assert!(event.reply.send(Ok(reply)).is_ok());
            let event = events.recv().await.unwrap();
            assert!(matches!(event.request, FixedRpc::Fragment(_)));
            if !after_last_read {
                graph.shutdown().await;
                drop(event); // No required result was obtained: never Ready.
                return;
            }
            let reply = forward_fixed(real, event.request).await;
            // Hold the complete actual result until the participant is gone.
            // The gateway cannot have observed it or sealed before shutdown.
            graph.shutdown().await;
            assert!(event.reply.send(Ok(reply)).is_ok());
        }));
        let tested = Gateway::bind_remote_local_queries(
            remote_config(&g),
            routes,
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(15),
        )
        .await
        .unwrap();
        let response = call(&tested, local_open(Some(1), "g.V(1).count()", 1)).await;
        if after_last_read {
            let (session, _, page) = ready(response.unwrap());
            assert_eq!(page.data.rows, vec![QueryRow::Count(1)]);
            call(&tested, Operation::CloseLocalQuery { session })
                .await
                .unwrap();
        } else {
            assert!(response.is_err());
        }
        tokio::time::timeout(Duration::from_secs(15), proxy)
            .await
            .unwrap()
            .unwrap();
        tested.shutdown().await;
        g.shutdown().await;
        for graph in graphs {
            graph.shutdown().await;
        }
        for server in servers {
            server.shutdown().await;
        }
        f.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_concurrent_write_never_refreshes_descriptor_or_recaptures() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    let mut routes = g.state.config.local_routes.clone().unwrap();
    let real = routes[0].endpoint;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    routes[0].endpoint = listener.local_addr().unwrap();
    let write_address = g.local_addr();
    let proxy = tokio::spawn(run_fixed_script(listener, move |mut events| async move {
        let mut descriptor = None;
        for index in 0..4 {
            let event = events.recv().await.unwrap();
            if index == 0 {
                assert!(matches!(event.request, FixedRpc::Open(_)));
            } else if index < 3 {
                let fragment = event.request.fragment().expect("expected fragment");
                assert_eq!(fragment.request_sequence, index - 1);
                if let Some(original) = &descriptor {
                    assert_eq!(fragment.descriptor.as_ref(), Some(original));
                } else {
                    descriptor = fragment.descriptor.clone();
                    assert!(descriptor.is_some());
                }
            } else {
                assert!(event.request.is_close());
            }
            let reply = forward_fixed(real, event.request).await;
            if index == 1 {
                // Commit after first Exists at V1 but before the second.
                super::request(write_address, bound(), Operation::Submit(create(2, 7, 2)), Duration::from_secs(15)).await.unwrap();
            }
            assert!(event.reply.send(Ok(reply)).is_ok());
        }
        }));
    let tested = Gateway::bind_remote_local_queries(
        remote_config(&g),
        routes,
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(15),
    )
    .await
    .unwrap();
    let (session, _, page) = ready(
        call(&tested, local_open(Some(1), "g.V(1,2).count()", 1))
            .await
            .unwrap(),
    );
    assert_eq!(session.version, 1);
    assert_eq!(page.data.rows, vec![QueryRow::Count(1)]);
    tokio::time::timeout(Duration::from_secs(15), proxy)
        .await
        .unwrap()
        .unwrap();
    let (new, _, page) = ready(
        call(&g, local_open(Some(2), "g.V(1,2).count()", 1))
            .await
            .unwrap(),
    );
    assert_eq!(page.data.rows, vec![QueryRow::Count(2)]);
    call(&g, Operation::CloseLocalQuery { session: new })
        .await
        .unwrap();
    call(&tested, Operation::CloseLocalQuery { session })
        .await
        .unwrap();
    tested.shutdown().await;
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_replacement_keeps_captured_paths_and_matches_lazy_traverse() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    for (external, partition, id) in [(1, 7, 1), (2, 7, 2), (3, 8, 1)] {
        committed(
            &call(&g, Operation::Submit(create(external, partition, id)))
                .await
                .unwrap(),
        );
    }
    let edge_data = |id, from, to, label: &str| EdgeData {
        id,
        from,
        to,
        label: label.into(),
        properties: BTreeMap::new(),
    };
    let mut old_v = 0;
    for (external, edge) in [
        (4, edge_data(10, entity(7, 1), entity(8, 1), "reports")),
        (5, edge_data(11, entity(7, 1), entity(8, 1), "parallel")),
        (6, edge_data(12, entity(8, 1), entity(8, 1), "self")),
    ] {
        old_v = committed(
            &call(
                &g,
                Operation::Submit(Write {
                    external_id: [external; 16],
                    mutation: Mutation::CreateEdge { edge },
                }),
            )
            .await
            .unwrap(),
        )
        .1;
    }
    let replacement = Write {
        external_id: [7; 16],
        mutation: Mutation::ReplaceEdge {
            edge: entity(7, 10),
            replacement: edge_data(10, entity(8, 1), entity(7, 2), "manages"),
        },
    };
    let mut routes = g.state.config.local_routes.clone().unwrap();
    let real = routes[0].endpoint;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    routes[0].endpoint = listener.local_addr().unwrap();
    let write_address = g.local_addr();
    let proxy = tokio::spawn(run_fixed_script(listener, move |mut events| async move {
        let mut descriptor = None;
        let (mut opens, mut sequence, mut new_v) = (0, 0u64, None);
        loop {
            let event = events.recv().await.unwrap();
            let fragment = event.request.fragment();
            let is_fragment = fragment.is_some();
            let close = event.request.is_close();
            if matches!(event.request, FixedRpc::Open(_)) {
                opens += 1;
                assert_eq!(opens, 1);
            } else if let Some(fragment) = fragment {
                assert_eq!(fragment.request_sequence, sequence);
                sequence += 1;
                if let Some(original) = &descriptor {
                    assert_eq!(fragment.descriptor.as_ref(), Some(original));
                } else {
                    descriptor = fragment.descriptor.clone();
                    assert!(descriptor.is_some());
                }
            } else { assert!(close); }
            let reply = forward_fixed(real, event.request).await;
            if is_fragment && new_v.is_none() {
                // Both captures preceded this required read. Replace the actual
                // edge while its old-view response is withheld.
                let result = super::request(write_address, bound(), Operation::Submit(replacement.clone()), Duration::from_secs(15)).await.unwrap();
                new_v = Some(committed(&result).1);
            }
            assert!(event.reply.send(Ok(reply)).is_ok());
            if close {
                assert_eq!(opens, 1);
                assert!(sequence > 1);
                return new_v.unwrap();
            }
        }
        }));
    let tested = Gateway::bind_remote_local_queries(
        remote_config(&g),
        routes,
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(15),
    )
    .await
    .unwrap();
    let old_query = query(7, "g.V(1,1).out('reports').out('self').path()");
    let (session, _, page) = ready(
        call(
            &tested,
            Operation::OpenLocalQuery {
                version: Some(old_v),
                ttl_millis: 60_000,
                query: old_query.clone(),
                page_size: 1,
            },
        )
        .await
        .unwrap(),
    );
    let new_v = tokio::time::timeout(Duration::from_secs(15), proxy)
        .await
        .unwrap()
        .unwrap();
    assert!(new_v > old_v);
    use crate::remote::query::QualifiedElement as E;
    let vertex = |partition, id| E::Vertex { partition, id };
    let edge = |partition, id| E::Edge { partition, id };
    let old_path = QueryRow::Path(vec![
        vertex(7, 1),
        edge(7, 10),
        vertex(8, 1),
        edge(8, 12),
        vertex(8, 1),
    ]);
    assert_eq!(page.total, 2);
    assert_eq!(page.data.rows, vec![old_path.clone()]);
    let Reply::FetchLocalQuery { page: replay, .. } = call(
        &tested,
        Operation::FetchLocalQuery {
            session: session.clone(),
            query: old_query.clone(),
            cursor: page.next,
            page_size: 1,
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(replay.data.rows, vec![old_path.clone()]);

    async fn parity(g: &Gateway, version: u64, query: Query, expected: Vec<QueryRow>) {
        let (local, _, page) = ready(
            call(
                g,
                Operation::OpenLocalQuery {
                    version: Some(version),
                    ttl_millis: 60_000,
                    query: query.clone(),
                    page_size: 1000,
                },
            )
            .await
            .unwrap(),
        );
        assert_eq!(page.data.rows, expected, "V={version} {}", query.text);
        let old = open(g, Some(version)).await;
        let Reply::Traverse { data, .. } = call(
            g,
            Operation::Traverse {
                session: old.clone(),
                query,
            },
        )
        .await
        .unwrap() else {
            panic!()
        };
        assert_eq!(data, page.data);
        close(g, old).await;
        call(g, Operation::CloseLocalQuery { session: local })
            .await
            .unwrap();
    }
    // The old path is independently captured again after replacement, then
    // compared with the separately optimized materialized traversal engine.
    parity(
        &g,
        old_v,
        old_query.clone(),
        vec![old_path.clone(), old_path],
    )
    .await;
    parity(&g, new_v, old_query, vec![]).await;
    parity(
        &g,
        new_v,
        query(7, "g.V(1).out('reports').count()"),
        vec![QueryRow::Count(0)],
    )
    .await;
    parity(
        &g,
        new_v,
        query(7, "g.V(1).out('parallel')"),
        vec![QueryRow::Element(vertex(8, 1))],
    )
    .await;
    let new_path = QueryRow::Path(vec![vertex(8, 1), edge(8, 10), vertex(7, 2)]);
    parity(
        &g,
        new_v,
        query(8, "g.V(1,1).out('manages').path()"),
        vec![new_path.clone(), new_path.clone()],
    )
    .await;
    parity(
        &g,
        new_v,
        query(8, "g.V(1,1).out('manages').count()"),
        vec![QueryRow::Count(2)],
    )
    .await;
    parity(
        &g,
        new_v,
        query(7, "g.V(2).in('manages')"),
        vec![QueryRow::Element(vertex(8, 1))],
    )
    .await;
    parity(
        &g,
        new_v,
        query(7, "g.V(2,2).in('manages').count()"),
        vec![QueryRow::Count(2)],
    )
    .await;
    parity(
        &g,
        new_v,
        query(7, "g.V(2).in('manages').in('parallel').path()"),
        vec![QueryRow::Path(vec![
            vertex(7, 2),
            edge(8, 10),
            vertex(8, 1),
            edge(7, 11),
            vertex(7, 1),
        ])],
    )
    .await;
    let self_v = committed(
        &call(
            &g,
            Operation::Submit(Write {
                external_id: [8; 16],
                mutation: Mutation::ReplaceEdge {
                    edge: entity(8, 10),
                    replacement: edge_data(21, entity(7, 2), entity(7, 2), "manages"),
                },
            }),
        )
        .await
        .unwrap(),
    )
    .1;
    parity(
        &g,
        new_v,
        query(8, "g.V(1,1).out('manages').path()"),
        vec![new_path.clone(), new_path],
    )
    .await;
    parity(
        &g,
        self_v,
        query(8, "g.V(1).out('manages').count()"),
        vec![QueryRow::Count(0)],
    )
    .await;
    parity(
        &g,
        self_v,
        query(7, "g.V(2).out('manages').in('manages').path()"),
        vec![QueryRow::Path(vec![
            vertex(7, 2),
            edge(7, 21),
            vertex(7, 2),
            edge(7, 21),
            vertex(7, 2),
        ])],
    )
    .await;
    call(&tested, Operation::CloseLocalQuery { session })
        .await
        .unwrap();
    tested.shutdown().await;
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_parallel_validation_uses_real_views_and_never_hides_filtered_corruption() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    for corrupt in [false, true] {
        let f = Fixture::new().await;
        let (g, servers, graphs) = local_gateway(&f).await;
        for (external, shard) in [(1, 7), (2, 8)] {
            committed(
                &call(&g, Operation::Submit(create(external, shard, 1)))
                    .await
                    .unwrap(),
            );
        }
        for (external, target) in [(3, 7), (4, 8)] {
            committed(
                &call(
                    &g,
                    Operation::Submit(Write {
                        external_id: [external; 16],
                        mutation: Mutation::CreateEdge {
                            edge: EdgeData {
                                id: u64::from(external),
                                from: entity(7, 1),
                                to: entity(target, 1),
                                label: if target == 7 { "selected" } else { "filtered" }.into(),
                                properties: BTreeMap::new(),
                            },
                        },
                    }),
                )
                .await
                .unwrap(),
            );
        }
        let mut routes = g.state.config.local_routes.clone().unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut proxies = Vec::new();
        for route in &mut routes {
            let real = route.endpoint;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            route.endpoint = listener.local_addr().unwrap();
            let barrier = barrier.clone();
            let shard = route.identity.group;
            proxies.push(tokio::spawn(run_fixed_script(listener, move |mut events| async move {
                let mut sequence = 0;
                let mut outgoing = 0;
                let mut descriptor = None;
                loop {
                    let event = events.recv().await.unwrap();
                    let leaf = event.request.fragment();
                    let kind = leaf.and_then(|v| v.fragment.as_ref()).and_then(|v| v.kind.as_ref());
                    let verify = matches!(kind, Some(fixed_wire::fragment::Kind::VerifyReverse(_)));
                    let out = matches!(kind, Some(fixed_wire::fragment::Kind::Out(_)));
                    if let Some(leaf) = leaf {
                        assert_eq!(leaf.request_sequence, sequence);
                        sequence += 1;
                        assert_eq!(leaf.descriptor.as_ref().unwrap().version, 4);
                        if let Some(original) = &descriptor {
                            assert_eq!(leaf.descriptor.as_ref(), Some(original));
                        } else { descriptor = leaf.descriptor.clone(); }
                    }
                    if out { outgoing += 1; }
                    // Real retained lanes must reach this barrier together.
                    let parallel_out = !corrupt && out && (shard == 8 || outgoing > 1);
                    if verify || parallel_out {
                        tokio::time::timeout(Duration::from_secs(4), barrier.wait()).await.unwrap();
                    }
                    let close = event.request.is_close();
                    let mut reply = forward_fixed(real, event.request).await;
                    if corrupt && verify && shard == 8 {
                        let FixedRpcReply::Fragment(response) = &mut reply else { panic!() };
                        let Some(fixed_wire::fixed_fragment_response::Result::Output(output)) = response.result.as_mut() else { panic!("expected real successful verification") };
                        assert!(matches!(output.value.as_ref().and_then(|v| v.kind.as_ref()), Some(fixed_wire::fragment_value::Kind::Boolean(_))));
                        output.value.as_mut().unwrap().kind = Some(fixed_wire::fragment_value::Kind::Boolean(false));
                    }
                    let sent = event.reply.send(Ok(reply));
                    // Corrupt peer may cancel the healthy lane's response.
                    if !(corrupt && verify && shard == 7) { assert!(sent.is_ok()); }
                    if close { break; }
                }
            })));
        }
        let tested = Gateway::bind_remote_local_queries(
            remote_config(&g),
            routes,
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(15),
        )
        .await
        .unwrap();
        let result = call(
            &tested,
            local_open(
                Some(4),
                if corrupt {
                    "g.V(1).out('selected').count()"
                } else {
                    "g.V(1).out().out('selected').count()"
                },
                1,
            ),
        )
        .await;
        if corrupt {
            assert_eq!(result, Err(Error::Query(QueryFailure::IncompleteOrCorrupt)));
            assert!(tested.state.local_sessions.lock().unwrap().is_empty());
        } else {
            let (session, _, page) = ready(result.unwrap());
            assert_eq!(page.data.rows, vec![QueryRow::Count(1)]);
            call(&tested, Operation::CloseLocalQuery { session })
                .await
                .unwrap();
        }
        for proxy in proxies {
            tokio::time::timeout(Duration::from_secs(5), proxy)
                .await
                .unwrap()
                .unwrap();
        }
        tested.shutdown().await;
        g.shutdown().await;
        for graph in graphs {
            graph.shutdown().await;
        }
        for server in servers {
            server.shutdown().await;
        }
        f.stop().await;
    }
}

// Same harness can be applied unchanged to serial main. No artificial delays,
// bypassed writes, revised budgets, or special query implementation are used.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual serial/candidate end-to-end measurement, not a timing assertion"]
async fn fixed_local_parallel_query_measurement() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    for (external, shard) in [(1, 7), (2, 8)] {
        committed(
            &call(&g, Operation::Submit(create(external, shard, 1)))
                .await
                .unwrap(),
        );
    }
    for (external, from, to) in [(3, 7, 7), (4, 7, 8), (5, 8, 7)] {
        committed(
            &call(
                &g,
                Operation::Submit(Write {
                    external_id: [external; 16],
                    mutation: Mutation::CreateEdge {
                        edge: EdgeData {
                            id: u64::from(external),
                            from: entity(from, 1),
                            to: entity(to, 1),
                            label: "link".into(),
                            properties: BTreeMap::new(),
                        },
                    },
                }),
            )
            .await
            .unwrap(),
        );
    }
    for text in [
        "g.V(1,1,1,1,1,1,1,1).out().out().out().count()",
        "g.V(1,1,1,1,1,1,1,1).in().in().in().count()",
    ] {
        let mut samples = Vec::new();
        for round in 0..11 {
            let start = std::time::Instant::now();
            let (session, _, page) = ready(call(&g, local_open(Some(5), text, 1)).await.unwrap());
            let micros = start.elapsed().as_micros();
            assert_eq!(session.version, 5);
            assert_eq!(page.data.rows, vec![QueryRow::Count(40)]);
            call(&g, Operation::CloseLocalQuery { session })
                .await
                .unwrap();
            if round >= 2 {
                samples.push(micros);
            }
        }
        eprintln!(
            "PARALLEL_QUERY_MEASUREMENT {}",
            serde_json::json!({
                "query":text,"version":5,"rows":40,"open_micros":samples,
                "scope":"same-host TCP, in-process real Raft participants, debug build, no injected delay"
            })
        );
    }
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

#[path = "managed_gateway_scaling_tests.rs"]
mod scaling;

#[tokio::test]
async fn fixed_graph_membership_accepts_protocol_bound_and_checks_late_routes() {
    use d2_raft::transaction_decisions::Member;
    for count in [9, 64] {
        let mut cohort = binding();
        cohort.members = (20..20 + count)
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .collect();
        let id = |shard| Identity {
            cohort: cohort.clone(),
            ..identity(shard)
        };
        let client = |shard| {
            Client::new(
                format!("127.0.0.1:{}", 20000 + shard).parse().unwrap(),
                id(shard),
                Duration::from_secs(1),
            )
            .unwrap()
        };
        let config = RemoteConfig {
            coordinator: client(9),
            binding: Binding {
                cohort: cohort.clone(),
                ..bound()
            },
            members: (20..20 + count).map(client).collect(),
        };
        let routes: Vec<_> = (20..20 + count)
            .map(|shard| LocalGraphRoute {
                endpoint: format!("127.0.0.1:{}", 10000 + shard).parse().unwrap(),
                identity: RemoteIdentity {
                    group: shard,
                    node: 1,
                },
                managed_identity: id(shard),
            })
            .collect();
        local::validate_routes(&config, &routes).unwrap();
        let mut wrong = routes.clone();
        wrong.last_mut().unwrap().managed_identity.target.mode = [255; 16];
        assert_eq!(
            local::validate_routes(&config, &wrong),
            Err(Error::Identity)
        );
        let mut duplicate = routes.clone();
        duplicate.last_mut().unwrap().endpoint = routes[0].endpoint;
        assert_eq!(
            local::validate_routes(&config, &duplicate),
            Err(Error::Identity)
        );
        assert_eq!(
            local::validate_routes(&config, &routes[..routes.len() - 1]),
            Err(Error::Identity)
        );
    }
}

#[tokio::test]
async fn member_width_rejects_before_endpoint_or_route_validation() {
    let config = || RemoteConfig {
        coordinator: Client::new(
            "127.0.0.1:1".parse().unwrap(),
            identity(9),
            Duration::from_secs(1),
        )
        .unwrap(),
        binding: bound(),
        members: Vec::new(),
    };
    for width in [0, 5, 8, usize::MAX] {
        assert!(matches!(
            Gateway::bind_remote_with_member_parallelism(
                config(),
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(1),
                width
            )
            .await,
            Err(Error::Protocol)
        ));
        assert!(matches!(
            Gateway::bind_remote_with_member_parallelism_and_timing(
                config(),
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(1),
                width
            )
            .await,
            Err(Error::Protocol)
        ));
        assert!(matches!(
            Gateway::bind_remote_local_queries_with_concurrency(
                config(),
                Vec::new(),
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(1),
                8,
                width
            )
            .await,
            Err(Error::Protocol)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn causal_lower_bound_local_query_round_trip_and_timeout() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    let write = call(&g, Operation::Submit(create(71, 7, 1))).await.unwrap();
    let (_, floor) = committed(&write);
    let after = |minimum_version, ttl_millis| Operation::OpenLocalQueryAfter {
        minimum_version,
        ttl_millis,
        query: Query {
            partition: 7,
            text: "g.V(1).count()".into(),
        },
        page_size: 1,
    };
    let (session, _, page) = ready(call(&g, after(floor, 60_000)).await.unwrap());
    assert!(session.version >= floor);
    assert_eq!(page.data.rows, vec![QueryRow::Count(1)]);
    call(&g, Operation::CloseLocalQuery { session })
        .await
        .unwrap();
    let charge_before_wait = g.retained_payload_charge();
    assert_eq!(
        call(&g, after(floor + 1, 100)).await,
        Err(Error::SessionUnavailable)
    );
    assert_eq!(g.retained_payload_charge(), charge_before_wait);
    let (caught_up, _, page) = {
        let waiting = call(&g, after(floor + 1, 60_000));
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut waiting)
                .await
                .is_err()
        );
        committed(&call(&g, Operation::Submit(create(72, 8, 2))).await.unwrap());
        ready(waiting.await.unwrap())
    };
    assert!(caught_up.version > floor);
    assert_eq!(page.data.rows, vec![QueryRow::Count(1)]);
    call(&g, Operation::CloseLocalQuery { session: caught_up })
        .await
        .unwrap();
    // A failed wait must not leave a local result/session reservation behind.
    let (session, _, _) = ready(call(&g, after(floor, 60_000)).await.unwrap());
    call(&g, Operation::CloseLocalQuery { session })
        .await
        .unwrap();
    for server in servers {
        server.shutdown().await;
    }
    assert!(call(&g, after(floor, 60_000)).await.is_err());
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    f.stop().await;
}

async fn http_post(
    address: SocketAddr,
    path: &str,
    value: serde_json::Value,
) -> (u16, serde_json::Value) {
    use axum::body::{to_bytes, Body};
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .unwrap();
    let connection = tokio::spawn(connection);
    let request = axum::http::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", "localhost")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&value).unwrap()))
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    let status = response.status().as_u16();
    let bytes = to_bytes(Body::new(response.into_body()), MAX_RESPONSE_BYTES)
        .await
        .unwrap();
    connection.await.unwrap().unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_http_shared_writes_queries_and_tcp_fixed_view() {
    use serde_json::json;
    let _test = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    assert!(matches!(
        g.bind_http("0.0.0.0:0".parse().unwrap(), Duration::from_secs(15))
            .await,
        Err(Error::Rejected)
    ));
    let http = g
        .bind_http("127.0.0.1:0".parse().unwrap(), Duration::from_secs(15))
        .await
        .unwrap();
    let write = json!({"external_id":"01010101010101010101010101010101","mutation":{"type":"create_vertex","partition":7,"vertex":{"id":"1","labels":["person"],"properties":{"name":{"type":"text","value":"Alice"}}}}});
    let (status, first) =
        http_post(http.local_addr(), "/preview1/writes:submit", write.clone()).await;
    assert_eq!(status, 200, "{first}");
    let (_, replay) = http_post(http.local_addr(), "/preview1/writes:status", write.clone()).await;
    assert_eq!(first, replay);
    let tcp = call(
        &g,
        Operation::Status(Write {
            external_id: [1; 16],
            mutation: Mutation::CreateVertex {
                partition: 7,
                vertex: VertexData {
                    id: 1,
                    labels: vec!["person".into()],
                    properties: BTreeMap::from([("name".into(), Value::Text("Alice".into()))]),
                },
            },
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        tcp,
        Reply::Write {
            status: WriteStatus::Committed { visible: true, .. },
            ..
        }
    ));
    let query = json!({"partition":7,"text":"g.V(1,2)"});
    let open = json!({"read":{"type":"latest"},"ttl_millis":60000,"query":query,"page_size":1});
    let (status, result) = http_post(http.local_addr(), "/preview1/queries:open", open).await;
    assert_eq!(status, 200, "{result}");
    let session = result["result"]["session"].clone();
    assert!(session["version"].is_string(), "{result}");
    let write2 = json!({"external_id":"02020202020202020202020202020202","mutation":{"type":"create_vertex","partition":7,"vertex":{"id":"2","labels":["person"],"properties":{"name":{"type":"text","value":"Bob"}}}}});
    assert_eq!(
        http_post(http.local_addr(), "/preview1/writes:submit", write2)
            .await
            .0,
        200
    );
    let (status, old) = http_post(
        http.local_addr(),
        "/preview1/queries:fetch",
        json!({"session":session,"query":query,"cursor":null,"page_size":1}),
    )
    .await;
    assert_eq!(status, 200, "{old}");
    assert_eq!(old["result"]["session"], session);
    assert_eq!(old["result"]["page"]["total"], 1);
    assert_eq!(old["result"]["page"]["rows"][0]["element"]["id"], "1");
    let (status, fresh) = http_post(http.local_addr(), "/preview1/queries:open", json!({"read":{"type":"minimum","version":"2"},"ttl_millis":60000,"query":query,"page_size":1})).await;
    assert_eq!(status, 200, "{fresh}");
    assert_eq!(fresh["result"]["page"]["total"], 2);
    assert_eq!(
        http_post(
            http.local_addr(),
            "/preview1/queries:close",
            json!({"session":fresh["result"]["session"]})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        http_post(
            http.local_addr(),
            "/preview1/queries:close",
            json!({"session":session})
        )
        .await
        .0,
        200
    );
    assert_ne!(
        http_post(
            http.local_addr(),
            "/preview1/queries:fetch",
            json!({"session":session,"query":query,"cursor":null,"page_size":1})
        )
        .await
        .0,
        200
    );
    http.shutdown().await;
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_grpc_zero_window_deadlines_release_shared_work_and_new_open() {
    use prost::Message;
    use public_proto as pb;
    let _test = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    committed(&call(&g, Operation::Submit(create(1, 7, 1))).await.unwrap());
    let grpc = g
        .bind_grpc("127.0.0.1:0".parse().unwrap(), Duration::from_secs(15))
        .await
        .unwrap();
    // Different causes: first context deadline, then retained query lease.
    for (rpc_ms, ttl_ms) in [(200, 60_000), (5_000, 750)] {
        let stream = TcpStream::connect(grpc.local_addr()).await.unwrap();
        let (mut sender, connection) = h2::client::Builder::new()
            .initial_window_size(0)
            .handshake::<_, axum::body::Bytes>(stream)
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(format!(
                "http://{}/velagraph.public.managed.v1.QueryService/Open",
                grpc.local_addr()
            ))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(())
            .unwrap();
        let (response, mut send) = sender.send_request(request, false).unwrap();
        let payload = pb::OpenRequest {
            profile: None,
            context: Some(pb::Context {
                identity: None,
                version: 1,
                timeout_millis: rpc_ms,
            }),
            read: Some(pb::ReadSelector {
                kind: Some(pb::read_selector::Kind::Latest(pb::Empty {})),
            }),
            ttl_millis: ttl_ms,
            query: Some(pb::Query {
                partition: 7,
                text: "g.V(1).count()".into(),
            }),
            page_size: 1,
        }
        .encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        send.send_data(frame.into(), true).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(2), response)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(
            response.headers().get("grpc-status").is_none(),
            "{:?}",
            response.headers()
        );
        let mut unread = response.into_body(); // keep zero stream window; never release capacity
        assert_eq!(g.retained_payload_charge(), SESSION_CHARGE);
        tokio::time::timeout(Duration::from_secs(2), async {
            while g.state.connections.available_permits() != MAX_CONNECTIONS
                || g.retained_payload_charge() != 0
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("watchdog must reclaim flow-controlled work and undelivered open");
        let result = tokio::time::timeout(Duration::from_secs(1), unread.data())
            .await
            .unwrap();
        assert!(
            !matches!(result, Some(Ok(_))),
            "zero window cannot deliver a data frame"
        );
        drop(sender);
        drop(unread);
        let _ = tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .unwrap();
        // Connection cancellation is not a transaction abort or state reset.
        assert!(matches!(
            call(&g, Operation::Status(create(1, 7, 1))).await.unwrap(),
            Reply::Write {
                status: WriteStatus::Committed { visible: true, .. },
                ..
            }
        ));
    }
    grpc.shutdown().await;
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_profile_captures_large_values_and_paginates_supported_paths() {
    use crate::remote::query::QualifiedElement as E;
    use prost::Message;
    let _test = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = Fixture::new().await;
    let (g, servers, graphs) = local_gateway(&f).await;
    // Large stored properties exercise capture, not unsupported values() output.
    // Exact response-size boundaries are covered by synthetic Retained pages.
    let config = OwnedConfig::from(f.config());
    let graph = config.graph();
    let payload = "x".repeat(128 * 1024);
    for id in [1, 2] {
        let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
        let mut vertex = Vertex::new(id);
        vertex
            .properties
            .insert("payload".into(), Value::Text(payload.clone()));
        let plan = snapshot.prepare_vertex(7, &vertex).unwrap();
        graph.execute(&plan).await.unwrap();
    }
    let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
    let plan = snapshot.prepare_vertex(7, &Vertex::new(3)).unwrap();
    graph.execute(&plan).await.unwrap();
    for index in 0..6u64 {
        let edge = EdgeData {
            id: 10 + index,
            from: entity(7, 3),
            to: entity(7, 1 + index % 2),
            label: "payload".into(),
            properties: BTreeMap::new(),
        };
        let snapshot = graph.snapshot(None, limit(), MAX_TTL).await.unwrap();
        let plan = snapshot.prepare_edge(&edge.graph()).unwrap();
        graph.execute(&plan).await.unwrap();
    }
    let first = call(
        &g,
        Operation::OpenLocalQueryWithProfile {
            profile: LocalReadProfile::LocalRead8MiBV1,
            version: None,
            ttl_millis: 60_000,
            query: Query {
                partition: 7,
                text: "g.V(3).out('payload').path()".into(),
            },
            page_size: 3,
        },
    )
    .await
    .unwrap();
    let Reply::OpenLocalQueryWithProfile {
        profile,
        session,
        query,
        page_size,
        page,
    } = first.clone()
    else {
        panic!("explicit profile reply was erased");
    };
    assert_eq!(profile, LocalReadProfile::LocalRead8MiBV1);
    assert_eq!(page_size, 3);
    assert_eq!(page.total, 6);
    assert_eq!(page.offset, 0);
    assert!(page.next.is_some());
    let identity = Some(public_proto::CallIdentity {
        request_id: vec![255; 16],
        binding: Some(grpc_client::encode_binding(&bound())),
    });
    let mut encoded = grpc_codec::encode_open(&first).unwrap();
    encoded.identity = identity.clone();
    assert!(encoded.encoded_len() <= MAX_RESPONSE_BYTES);
    assert_eq!(encoded.profile, Some(grpc_codec::encode_profile(profile)));
    let expected = |start: u64| {
        (start..start + 3)
            .map(|index| {
                QueryRow::Path(vec![
                    E::Vertex {
                        partition: 7,
                        id: 3,
                    },
                    E::Edge {
                        partition: 7,
                        id: 10 + index,
                    },
                    E::Vertex {
                        partition: 7,
                        id: 1 + index % 2,
                    },
                ])
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(page.data.rows, expected(0));
    let second = call(
        &g,
        Operation::FetchLocalQuery {
            session: session.clone(),
            query: query.clone(),
            cursor: page.next.clone(),
            page_size: 3,
        },
    )
    .await
    .unwrap();
    let Reply::FetchLocalQuery {
        session: returned,
        page: next,
        ..
    } = &second
    else {
        panic!("fetch reply");
    };
    assert_eq!(returned, &session);
    assert_eq!(next.offset, 3);
    assert_eq!(next.total, 6);
    assert_eq!(next.next, None);
    assert_eq!(next.data.rows, expected(3));
    let mut encoded = grpc_codec::encode_fetch(&second).unwrap();
    encoded.identity = identity;
    assert!(encoded.encoded_len() <= MAX_RESPONSE_BYTES);
    call(&g, Operation::CloseLocalQuery { session })
        .await
        .unwrap();
    assert!(g.state.local_sessions.lock().unwrap().is_empty());
    assert_eq!(g.retained_payload_charge(), 0);
    g.shutdown().await;
    for graph in graphs {
        graph.shutdown().await;
    }
    for server in servers {
        server.shutdown().await;
    }
    drop(graph);
    drop(config);
    f.stop().await;
}
