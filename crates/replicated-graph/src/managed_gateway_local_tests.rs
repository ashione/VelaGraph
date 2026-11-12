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

#[test]
fn fixed_graph_routes_bind_complete_managed_identity() {
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

#[test]
fn fixed_graph_protocol_member_limit_is_checked_before_admission() {
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
        let proxy = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await.unwrap();
            let mut length = client.read_u32().await.unwrap();
            assert!(length as usize <= crate::remote::MAX_FRAME_BYTES);
            let mut body = vec![0; length as usize];
            client.read_exact(&mut body).await.unwrap();
            if downgrade_profile {
                let mut request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let open = &mut request["operation"]["OpenFixedQuery"];
                assert_eq!(open["profile"], "LocalRead8MiBV1");
                open["profile"] = serde_json::json!("Parity2MiBV1");
                open["capture_limit"]["bytes"] = serde_json::json!(2 * 1024 * 1024);
                body = serde_json::to_vec(&request).unwrap();
                length = body.len() as u32;
            }
            let mut upstream = TcpStream::connect(real).await.unwrap();
            upstream.write_u32(length).await.unwrap();
            upstream.write_all(&body).await.unwrap();
            let length = upstream.read_u32().await.unwrap();
            assert!(length as usize <= crate::remote::MAX_FRAME_BYTES);
            let mut body = vec![0; length as usize];
            upstream.read_exact(&mut body).await.unwrap();
            let mut reply: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let descriptor = reply["result"]["Ok"]["OpenFixedQuery"]["descriptor"].clone();
            let proof = &mut reply["result"]["Ok"]["OpenFixedQuery"]["retained_proof"];
            assert_eq!(proof["through"], 0);
            // Independently valid exact V0 evidence, but from a different authority.
            if !downgrade_profile {
                proof["authority"] = serde_json::to_value([66u8; 16]).unwrap();
            } else {
                assert!(descriptor.get("profile").is_none());
            }
            let body = serde_json::to_vec(&reply).unwrap();
            client.write_u32(body.len() as u32).await.unwrap();
            client.write_all(&body).await.unwrap();
            // A known identity-safe descriptor is closed even though its proof
            // failed validation, instead of consuming the shared pool until TTL.
            let (mut client, _) = listener.accept().await.unwrap();
            let length = client.read_u32().await.unwrap();
            let mut body = vec![0; length as usize];
            client.read_exact(&mut body).await.unwrap();
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(request["operation"]["CloseFixedQuery"].is_object());
            assert_eq!(
                request["operation"]["CloseFixedQuery"]["descriptor"],
                descriptor
            );
            let mut upstream = TcpStream::connect(real).await.unwrap();
            upstream.write_u32(length).await.unwrap();
            upstream.write_all(&body).await.unwrap();
            let length = upstream.read_u32().await.unwrap();
            let mut body = vec![0; length as usize];
            upstream.read_exact(&mut body).await.unwrap();
            let closed: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(closed["result"]["Ok"], "CloseFixedQuery");
            client.write_u32(length).await.unwrap();
            client.write_all(&body).await.unwrap();
        });
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

async fn wire_read(socket: &mut TcpStream) -> Vec<u8> {
    let length = socket.read_u32().await.unwrap() as usize;
    assert!(length <= crate::remote::MAX_FRAME_BYTES);
    let mut body = vec![0; length];
    socket.read_exact(&mut body).await.unwrap();
    body
}
async fn wire_write(socket: &mut TcpStream, body: &[u8]) {
    socket.write_u32(body.len() as u32).await.unwrap();
    socket.write_all(body).await.unwrap();
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
        let proxy = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await.unwrap();
            let body = wire_read(&mut client).await;
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(request["operation"]["OpenFixedQuery"].is_object());
            let mut upstream = TcpStream::connect(real).await.unwrap();
            wire_write(&mut upstream, &body).await;
            let reply = wire_read(&mut upstream).await;
            wire_write(&mut client, &reply).await;
            let (mut client, _) = listener.accept().await.unwrap();
            let body = wire_read(&mut client).await;
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(request["operation"]["FixedQueryFragment"].is_object());
            if !after_last_read {
                graph.shutdown().await;
                return; // Required Exists response is unknown: never Ready.
            }
            let mut upstream = TcpStream::connect(real).await.unwrap();
            wire_write(&mut upstream, &body).await;
            let reply = wire_read(&mut upstream).await;
            // The required response is complete, but the gateway cannot have
            // observed it or sealed Ready until we forward it after shutdown.
            graph.shutdown().await;
            wire_write(&mut client, &reply).await;
        });
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
    let proxy = tokio::spawn(async move {
        let mut descriptor = None;
        for index in 0..4 {
            let (mut client, _) = listener.accept().await.unwrap();
            let body = wire_read(&mut client).await;
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
            if index == 0 {
                assert!(request["operation"]["OpenFixedQuery"].is_object());
            } else if index < 3 {
                let fragment = &request["operation"]["FixedQueryFragment"];
                assert_eq!(fragment["request_sequence"], index - 1);
                if let Some(original) = &descriptor {
                    assert_eq!(&fragment["descriptor"], original);
                } else {
                    descriptor = Some(fragment["descriptor"].clone());
                }
            } else {
                assert!(request["operation"]["CloseFixedQuery"].is_object());
            }
            let mut upstream = TcpStream::connect(real).await.unwrap();
            wire_write(&mut upstream, &body).await;
            let reply = wire_read(&mut upstream).await;
            if index == 1 {
                // Commit after the first Exists at V1 but before the second.
                super::request(
                    write_address,
                    bound(),
                    Operation::Submit(create(2, 7, 2)),
                    Duration::from_secs(15),
                )
                .await
                .unwrap();
            }
            wire_write(&mut client, &reply).await;
        }
    });
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
    let proxy = tokio::spawn(async move {
        let mut descriptor = None;
        let (mut opens, mut sequence, mut new_v) = (0, 0u64, None);
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            let body = wire_read(&mut client).await;
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let operation = &request["operation"];
            let fragment = operation["FixedQueryFragment"].is_object();
            let close = operation["CloseFixedQuery"].is_object();
            if operation["OpenFixedQuery"].is_object() {
                opens += 1;
                assert_eq!(opens, 1);
            } else if fragment {
                let fragment = &operation["FixedQueryFragment"];
                assert_eq!(fragment["request_sequence"], sequence);
                sequence += 1;
                if let Some(original) = &descriptor {
                    assert_eq!(&fragment["descriptor"], original);
                } else {
                    descriptor = Some(fragment["descriptor"].clone());
                }
            } else {
                assert!(close);
            }
            let mut upstream = TcpStream::connect(real).await.unwrap();
            wire_write(&mut upstream, &body).await;
            let reply = wire_read(&mut upstream).await;
            if fragment && new_v.is_none() {
                // Both members were captured before the first required read.
                // Change source owner, target and label while retaining the edge ID before traversal resumes.
                let reply = super::request(
                    write_address,
                    bound(),
                    Operation::Submit(replacement.clone()),
                    Duration::from_secs(15),
                )
                .await
                .unwrap();
                new_v = Some(committed(&reply).1);
            }
            wire_write(&mut client, &reply).await;
            if close {
                assert_eq!(opens, 1);
                assert!(sequence > 1);
                return new_v.unwrap();
            }
        }
    });
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
            proxies.push(tokio::spawn(async move {
                let mut sequence = 0;
                let mut outgoing = 0;
                let mut descriptor = None;
                loop {
                    let (mut client, _) = listener.accept().await.unwrap();
                    let body = wire_read(&mut client).await;
                    let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let leaf = &request["operation"]["FixedQueryFragment"];
                    let verify = leaf["fragment"]["VerifyReverse"].is_object();
                    if leaf.is_object() {
                        assert_eq!(leaf["request_sequence"], sequence);
                        sequence += 1;
                        assert_eq!(leaf["descriptor"]["version"], 4);
                        if let Some(original) = &descriptor {
                            assert_eq!(&leaf["descriptor"], original);
                        } else {
                            descriptor = Some(leaf["descriptor"].clone());
                        }
                    }
                    let out = leaf["fragment"]["Out"].is_object();
                    if out {
                        outgoing += 1;
                    }
                    // The initial root exists only on shard 7. Its first Out
                    // creates the cross-shard frontier. The second hop must
                    // dispatch Out to both actual retained participant views.
                    let parallel_out = !corrupt && out && (shard == 8 || outgoing > 1);
                    if verify || parallel_out {
                        // Both actual participant lanes must be in flight. The
                        // barrier is synchronization, not a performance claim.
                        tokio::time::timeout(Duration::from_secs(4), barrier.wait())
                            .await
                            .unwrap();
                    }
                    let mut upstream = TcpStream::connect(real).await.unwrap();
                    wire_write(&mut upstream, &body).await;
                    let mut reply = wire_read(&mut upstream).await;
                    if corrupt && verify && shard == 8 {
                        let mut value: serde_json::Value = serde_json::from_slice(&reply).unwrap();
                        value["result"]["Ok"]["FixedQueryFragment"]["Ok"]["value"]["Bool"] =
                            false.into();
                        reply = serde_json::to_vec(&value).unwrap();
                    }
                    if corrupt && verify && shard == 7 {
                        // The failing peer may cancel this healthy lane before
                        // its response finishes. That is expected cancellation.
                        if client.write_u32(reply.len() as u32).await.is_ok() {
                            let _ = client.write_all(&reply).await;
                        }
                    } else {
                        wire_write(&mut client, &reply).await;
                    }
                    if request["operation"]["CloseFixedQuery"].is_object() {
                        break;
                    }
                }
            }));
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

#[test]
fn fixed_graph_membership_accepts_protocol_bound_and_checks_late_routes() {
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
