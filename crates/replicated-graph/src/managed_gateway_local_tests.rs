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
fn fixed_graph_nine_matching_routes_are_rejected_before_admission() {
    let mut binding = bound();
    binding.cohort.members = (10..19)
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
    let mut routes = g.state.config.local_routes.clone().unwrap();
    let real = routes[0].endpoint;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    routes[0].endpoint = listener.local_addr().unwrap();
    let proxy = tokio::spawn(async move {
        let (mut client, _) = listener.accept().await.unwrap();
        let length = client.read_u32().await.unwrap();
        assert!(length as usize <= crate::remote::MAX_FRAME_BYTES);
        let mut body = vec![0; length as usize];
        client.read_exact(&mut body).await.unwrap();
        let mut upstream = TcpStream::connect(real).await.unwrap();
        upstream.write_u32(length).await.unwrap();
        upstream.write_all(&body).await.unwrap();
        let length = upstream.read_u32().await.unwrap();
        assert!(length as usize <= crate::remote::MAX_FRAME_BYTES);
        let mut body = vec![0; length as usize];
        upstream.read_exact(&mut body).await.unwrap();
        let mut reply: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let proof = &mut reply["result"]["Ok"]["OpenFixedQuery"]["retained_proof"];
        assert_eq!(proof["through"], 0);
        // Independently valid exact V0 evidence, but from a different authority.
        proof["authority"] = serde_json::to_value([66u8; 16]).unwrap();
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
        let mut upstream = TcpStream::connect(real).await.unwrap();
        upstream.write_u32(length).await.unwrap();
        upstream.write_all(&body).await.unwrap();
        let length = upstream.read_u32().await.unwrap();
        let mut body = vec![0; length as usize];
        upstream.read_exact(&mut body).await.unwrap();
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
        call(&bad, local_open(Some(0), "g.V(999).count()", 1)).await,
        Err(Error::Identity)
    );
    tokio::time::timeout(Duration::from_secs(15), proxy)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bad.retained_payload_charge(), 0);
    bad.shutdown().await;
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
