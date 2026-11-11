//! Four independent Raft participant groups, exercised only through managed writes
//! and the public TCP gateway. Fixture IDs have no production significance.
use super::*;

const SHARDS: [u64; 4] = [20, 21, 22, 23];
const DEADLINE: Duration = Duration::from_secs(15);

struct ScalingFixture {
    cluster: Cluster,
    nodes: Vec<Arc<NodeHandle>>,
    servers: Vec<Server>,
    graphs: Vec<GraphService>,
    config: RemoteConfig,
    routes: Vec<LocalGraphRoute>,
}

fn scaling_binding() -> Binding {
    Binding {
        authority: authority(),
        cohort: Cohort {
            profile: Default::default(),
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: SHARDS
                .into_iter()
                .map(|shard| Member {
                    shard,
                    epoch: 1,
                    mode: [shard as u8; 16],
                })
                .collect(),
        },
        mapping: Mapping::PartitionEqualsShardV1,
    }
}

fn scaling_identity(shard: u64) -> Identity {
    Identity {
        cohort: scaling_binding().cohort,
        ..identity(shard)
    }
}

impl ScalingFixture {
    async fn new() -> Self {
        let cluster = Cluster::new();
        let mut nodes = Vec::new();
        let mut servers = Vec::new();
        let mut graphs = Vec::new();
        let mut clients = Vec::new();
        let mut routes = Vec::new();
        for shard in std::iter::once(9).chain(SHARDS) {
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
                .wait(Some(Duration::from_secs(5)))
                .metrics(
                    |m| m.current_leader == Some(1) && m.last_applied.is_some(),
                    "ready",
                )
                .await
                .unwrap();
            submit(&node, D1Command::Activate { shard, epoch: 1 })
                .await
                .unwrap();
            submit(
                &node,
                D1Command::EnableManagedWrites {
                    shard,
                    epoch: 1,
                    expected_sequence: 0,
                    mode_id: [shard as u8; 16],
                },
            )
            .await
            .unwrap();
            if shard == 9 {
                submit(
                    &node,
                    D1Command::EnableCoordinator {
                        shard,
                        epoch: 1,
                        mode_id: [9; 16],
                    },
                )
                .await
                .unwrap();
                submit(
                    &node,
                    D1Command::Coordinator {
                        shard,
                        epoch: 1,
                        mode_id: [9; 16],
                        command: Command::BindCohort {
                            cohort: scaling_binding().cohort,
                        },
                    },
                )
                .await
                .unwrap();
            } else {
                submit(
                    &node,
                    D1Command::EnableParticipant {
                        shard,
                        epoch: 1,
                        mode_id: [shard as u8; 16],
                        authority: authority(),
                    },
                )
                .await
                .unwrap();
                let mut journal = Journal::new([9; 16]).unwrap();
                journal
                    .apply(Command::BindCohort {
                        cohort: scaling_binding().cohort,
                    })
                    .unwrap();
                submit(
                    &node,
                    D1Command::EnableVersionedReads {
                        shard,
                        epoch: 1,
                        mode_id: [shard as u8; 16],
                        journal,
                    },
                )
                .await
                .unwrap();
            }
            let node = Arc::new(node);
            let server = Server::bind(
                node.clone(),
                scaling_identity(shard),
                "127.0.0.1:0".parse().unwrap(),
                DEADLINE,
            )
            .await
            .unwrap();
            clients
                .push(Client::new(server.local_addr(), scaling_identity(shard), DEADLINE).unwrap());
            servers.push(server);
            if shard != 9 {
                let graph = GraphService::start_managed(
                    &node,
                    "127.0.0.1:0".parse().unwrap(),
                    shard,
                    shard,
                    DEADLINE,
                )
                .await
                .unwrap();
                routes.push(LocalGraphRoute {
                    endpoint: graph.local_addr(),
                    identity: RemoteIdentity {
                        group: shard,
                        node: 1,
                    },
                    managed_identity: scaling_identity(shard),
                });
                graphs.push(graph);
            }
            nodes.push(node);
        }
        Self {
            cluster,
            nodes,
            servers,
            graphs,
            config: RemoteConfig {
                coordinator: clients.remove(0),
                binding: scaling_binding(),
                members: clients,
            },
            routes,
        }
    }

    async fn gateway(&self, parallelism: usize) -> Gateway {
        self.gateway_with_routes(parallelism, self.routes.clone())
            .await
    }

    async fn gateway_with_routes(
        &self,
        parallelism: usize,
        routes: Vec<LocalGraphRoute>,
    ) -> Gateway {
        Gateway::bind_remote_local_queries_with_parallelism(
            RemoteConfig {
                coordinator: self.config.coordinator.clone(),
                binding: self.config.binding.clone(),
                members: self.config.members.clone(),
            },
            routes,
            "127.0.0.1:0".parse().unwrap(),
            DEADLINE,
            parallelism,
        )
        .await
        .unwrap()
    }

    async fn stop(self) {
        for graph in self.graphs {
            graph.shutdown().await;
        }
        for server in self.servers {
            server.shutdown().await;
        }
        for (node, shard) in self.nodes.iter().zip(std::iter::once(9).chain(SHARDS)) {
            self.cluster.in_group(shard).shutdown(node).await;
        }
    }
}

async fn scaling_call(g: &Gateway, operation: Operation) -> Result<Reply> {
    request(g.local_addr(), scaling_binding(), operation, DEADLINE).await
}

async fn scaling_seed(g: &Gateway) -> u64 {
    let mut external = 1;
    let mut version = 0;
    for shard in SHARDS {
        committed(
            &scaling_call(g, Operation::Submit(create(external, shard as u32, 1)))
                .await
                .unwrap(),
        );
        external += 1;
    }
    for shard in SHARDS {
        for (id, from, label) in [(100 + shard, 20, "spread"), (200 + shard, shard, "stay")] {
            version = committed(
                &scaling_call(
                    g,
                    Operation::Submit(Write {
                        external_id: [external; 16],
                        mutation: Mutation::CreateEdge {
                            edge: EdgeData {
                                id,
                                from: entity(from as u32, 1),
                                to: entity(shard as u32, 1),
                                label: label.into(),
                                properties: BTreeMap::new(),
                            },
                        },
                    }),
                )
                .await
                .unwrap(),
            )
            .1;
            external += 1;
        }
    }
    version
}

fn scaling_open(version: u64, text: &str) -> Operation {
    Operation::OpenLocalQuery {
        version: Some(version),
        ttl_millis: 60_000,
        query: query(20, text),
        page_size: 1000,
    }
}

async fn scaling_rows(g: &Gateway, version: u64, text: &str) -> Vec<QueryRow> {
    let (session, _, page) = ready(scaling_call(g, scaling_open(version, text)).await.unwrap());
    assert_eq!(session.version, version);
    assert!(page.next.is_none());
    scaling_call(g, Operation::CloseLocalQuery { session })
        .await
        .unwrap();
    page.data.rows
}

fn scaling_paths(shards: &[u64]) -> Vec<QueryRow> {
    use crate::remote::query::QualifiedElement as E;
    (0..2)
        .flat_map(|_| {
            shards.iter().map(|&shard| {
                QueryRow::Path(vec![
                    E::Vertex {
                        partition: 20,
                        id: 1,
                    },
                    E::Edge {
                        partition: 20,
                        id: 100 + shard,
                    },
                    E::Vertex {
                        partition: shard as u32,
                        id: 1,
                    },
                    E::Edge {
                        partition: shard as u32,
                        id: 200 + shard,
                    },
                    E::Vertex {
                        partition: shard as u32,
                        id: 1,
                    },
                ])
            })
        })
        .collect()
}

const PATH_QUERY: &str = "g.V(1,1).out('spread').out('stay').path()";
const COUNT_QUERY: &str = "g.V(1,1).out('spread').out('stay').count()";
const IN_PATH_QUERY: &str = "g.V(1,1).out('spread').in('stay').path()";
const IN_COUNT_QUERY: &str = "g.V(1,1).out('spread').in('stay').count()";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_scaling_four_shard_widths_preserve_ordered_paths_counts_and_versions() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = ScalingFixture::new().await;
    let serial = f.gateway(1).await;
    let old_v = scaling_seed(&serial).await;
    let baseline = scaling_rows(&serial, old_v, PATH_QUERY).await;
    assert_eq!(baseline, scaling_paths(&SHARDS));
    // The selected stay edges are self-loops: incoming and outgoing paths
    // share this input-only oracle while exercising separate executor stages.
    for (path, count) in [(PATH_QUERY, COUNT_QUERY), (IN_PATH_QUERY, IN_COUNT_QUERY)] {
        assert_eq!(scaling_rows(&serial, old_v, path).await, baseline);
        assert_eq!(
            scaling_rows(&serial, old_v, count).await,
            vec![QueryRow::Count(8)]
        );
    }
    for width in 2..=8 {
        let g = f.gateway(width).await;
        for (path, count) in [(PATH_QUERY, COUNT_QUERY), (IN_PATH_QUERY, IN_COUNT_QUERY)] {
            assert_eq!(scaling_rows(&g, old_v, path).await, baseline);
            assert_eq!(
                scaling_rows(&g, old_v, count).await,
                vec![QueryRow::Count(8)]
            );
        }
        g.shutdown().await;
    }
    let new_v = committed(
        &scaling_call(
            &serial,
            Operation::Submit(Write {
                external_id: [100; 16],
                mutation: Mutation::DeleteEdge {
                    edge: entity(23, 223),
                },
            }),
        )
        .await
        .unwrap(),
    )
    .1;
    assert!(new_v > old_v);
    for width in [1, 3, 4, 8] {
        let g = f.gateway(width).await;
        for (path, count) in [(PATH_QUERY, COUNT_QUERY), (IN_PATH_QUERY, IN_COUNT_QUERY)] {
            assert_eq!(scaling_rows(&g, old_v, path).await, baseline);
            assert_eq!(
                scaling_rows(&g, old_v, count).await,
                vec![QueryRow::Count(8)]
            );
            assert_eq!(
                scaling_rows(&g, new_v, path).await,
                scaling_paths(&SHARDS[..3])
            );
            assert_eq!(
                scaling_rows(&g, new_v, count).await,
                vec![QueryRow::Count(6)]
            );
        }
        g.shutdown().await;
    }
    serial.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_local_scaling_four_real_retained_views_overlap_during_managed_write() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = ScalingFixture::new().await;
    let writer = f.gateway(1).await;
    let old_v = scaling_seed(&writer).await;
    let mut routes = f.routes.clone();
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let mut proxies = Vec::new();
    for route in &mut routes {
        let real = route.endpoint;
        let shard = route.identity.group;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        route.endpoint = listener.local_addr().unwrap();
        let barrier = barrier.clone();
        let address = writer.local_addr();
        proxies.push(tokio::spawn(async move {
            let mut sequence = 0;
            let mut descriptor = None;
            let mut outs = 0;
            let mut overlaps = 0;
            loop {
                let (mut client, _) = listener.accept().await.unwrap();
                let body = wire_read(&mut client).await;
                let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let operation = &value["operation"];
                let fragment = &operation["FixedQueryFragment"];
                if fragment.is_object() {
                    assert_eq!(fragment["request_sequence"], sequence);
                    sequence += 1;
                    assert_eq!(fragment["descriptor"]["version"], old_v);
                    if let Some(original) = &descriptor {
                        assert_eq!(&fragment["descriptor"], original);
                    } else {
                        descriptor = Some(fragment["descriptor"].clone());
                    }
                }
                if fragment["fragment"]["Out"].is_object() {
                    outs += 1;
                }
                let overlap = fragment["fragment"]["Out"].is_object() && (shard != 20 || outs == 2);
                let mut upstream = TcpStream::connect(real).await.unwrap();
                wire_write(&mut upstream, &body).await;
                let reply = wire_read(&mut upstream).await;
                if overlap {
                    overlaps += 1;
                    // Responses from all four actual retained views are held
                    // concurrently. A serial implementation cannot pass this
                    // barrier; no injected delay is used as timing evidence.
                    let leader = tokio::time::timeout(DEADLINE, barrier.wait())
                        .await
                        .unwrap()
                        .is_leader();
                    if leader {
                        let result = request(
                            address,
                            scaling_binding(),
                            Operation::Submit(Write {
                                external_id: [101; 16],
                                mutation: Mutation::DeleteEdge {
                                    edge: entity(23, 223),
                                },
                            }),
                            DEADLINE,
                        )
                        .await
                        .unwrap();
                        assert!(committed(&result).1 > old_v);
                    }
                    tokio::time::timeout(DEADLINE, barrier.wait())
                        .await
                        .unwrap();
                }
                wire_write(&mut client, &reply).await;
                if operation["CloseFixedQuery"].is_object() {
                    assert_eq!(overlaps, 1);
                    assert!(descriptor.is_some());
                    break;
                }
            }
        }));
    }
    let tested = f.gateway_with_routes(4, routes).await;
    assert_eq!(
        scaling_rows(&tested, old_v, "g.V(1).out('spread').out('stay').path()").await,
        scaling_paths(&SHARDS)[..SHARDS.len()]
    );
    for proxy in proxies {
        tokio::time::timeout(DEADLINE, proxy)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(
        scaling_rows(&writer, old_v, COUNT_QUERY).await,
        vec![QueryRow::Count(8)]
    );
    assert_eq!(
        scaling_rows(&writer, old_v + 1, COUNT_QUERY).await,
        vec![QueryRow::Count(6)]
    );
    tested.shutdown().await;
    writer.shutdown().await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual interleaved real four-shard TCP measurement, no timing assertion"]
async fn fixed_local_scaling_four_shard_parallelism_measurement() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let f = ScalingFixture::new().await;
    let mut gateways = Vec::new();
    let widths = [1, 3, 4, 8];
    for width in widths {
        gateways.push(f.gateway(width).await);
    }
    let version = scaling_seed(&gateways[0]).await;
    let mut samples = vec![Vec::new(); widths.len()];
    for text in [COUNT_QUERY, PATH_QUERY] {
        let expected = scaling_rows(&gateways[0], version, text).await;
        for sample in &mut samples {
            sample.clear();
        }
        for round in 0..14 {
            for offset in 0..widths.len() {
                let lane = (round + offset) % widths.len();
                let start = std::time::Instant::now();
                let (session, _, page) = ready(
                    scaling_call(&gateways[lane], scaling_open(version, text))
                        .await
                        .unwrap(),
                );
                let micros = start.elapsed().as_micros();
                assert_eq!(page.data.rows, expected);
                assert_eq!(session.version, version);
                scaling_call(&gateways[lane], Operation::CloseLocalQuery { session })
                    .await
                    .unwrap();
                if round >= 2 {
                    samples[lane].push(micros);
                }
            }
        }
        eprintln!(
            "FOUR_SHARD_PARALLELISM_MEASUREMENT {}",
            serde_json::json!({
                "query": text, "version": version, "widths": widths,
                "open_micros": samples, "warmup_rounds": 2,
                "scope": "same-host TCP; four independent in-process Raft participants; rotated interleaving; no injected delay; no timing assertion"
            })
        );
    }
    for gateway in gateways {
        gateway.shutdown().await;
    }
    f.stop().await;
}

#[tokio::test]
async fn fixed_local_scaling_invalid_widths_fail_before_routes_or_network() {
    let _gate = TEST_GATE.lock().await;
    for width in [0, 9, usize::MAX] {
        let client = |shard| {
            Client::new(
                "127.0.0.1:1".parse().unwrap(),
                scaling_identity(shard),
                DEADLINE,
            )
            .unwrap()
        };
        let result = Gateway::bind_remote_local_queries_with_parallelism(
            RemoteConfig {
                coordinator: client(9),
                binding: scaling_binding(),
                members: SHARDS.into_iter().map(client).collect(),
            },
            vec![],
            "127.0.0.1:0".parse().unwrap(),
            DEADLINE,
            width,
        )
        .await;
        assert!(matches!(result, Err(Error::Protocol)), "width={width}");
    }
}
