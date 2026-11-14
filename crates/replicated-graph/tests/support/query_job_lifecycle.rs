//! Injected transport-delay functional evidence, not compute/performance evidence.
//! The graph and every managed mutation live in independent child processes.
use super::*;
use gateway::{JobHandle, JobPhase, JobPolicy, JobStatus};
use replicated_graph::remote::distributed::grpc::wire as delay_wire;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;

const ROOT: u64 = 2_000_001;
const EDGE: u64 = 2_000_010;
const MIDDLE: u64 = ROOT + 1;
const COPIES: usize = 12;
const REQUIRED_FRAGMENTS: u64 = (COPIES * 8) as u64;

#[derive(Default)]
struct FragmentTraffic {
    active: std::sync::Mutex<BTreeMap<u64, usize>>,
    peak_shards: AtomicU64,
}
impl FragmentTraffic {
    fn enter(self: &Arc<Self>, shard: u64) -> ActiveFragment {
        let mut active = self.active.lock().unwrap();
        *active.entry(shard).or_default() += 1;
        self.peak_shards
            .fetch_max(active.len() as u64, Ordering::SeqCst);
        ActiveFragment {
            traffic: self.clone(),
            shard,
        }
    }
    fn peak(&self) -> u64 {
        self.peak_shards.load(Ordering::SeqCst)
    }
}
struct ActiveFragment {
    traffic: Arc<FragmentTraffic>,
    shard: u64,
}
impl Drop for ActiveFragment {
    fn drop(&mut self) {
        let mut active = self.traffic.active.lock().unwrap();
        let count = active.get_mut(&self.shard).unwrap();
        *count -= 1;
        if *count == 0 {
            active.remove(&self.shard);
        }
    }
}

/// Typed fixed-query proxy: only fragment traffic is delayed/counted. Every
/// request goes to the real independent child exactly once; lifecycle/control
/// RPCs retain their own paths and do not sleep. This is not performance data.
#[derive(Clone)]
struct DelayService {
    client:
        delay_wire::fixed_query_service_client::FixedQueryServiceClient<tonic::transport::Channel>,
    shard: u64,
    traffic: Arc<FragmentTraffic>,
    fragments: Arc<AtomicU64>,
    delay_millis: Arc<AtomicU64>,
}
#[tonic::async_trait]
impl delay_wire::fixed_query_service_server::FixedQueryService for DelayService {
    async fn open_fixed(
        &self,
        request: tonic::Request<delay_wire::FixedOpenEnvelope>,
    ) -> Result<tonic::Response<delay_wire::FixedOpened>, tonic::Status> {
        self.client.clone().open_fixed(request).await
    }
    async fn open_renewable(
        &self,
        request: tonic::Request<delay_wire::RenewableOpenEnvelope>,
    ) -> Result<tonic::Response<delay_wire::RenewableFixedOpened>, tonic::Status> {
        self.client.clone().open_renewable(request).await
    }
    async fn renew(
        &self,
        request: tonic::Request<delay_wire::FixedRenewEnvelope>,
    ) -> Result<tonic::Response<delay_wire::FixedLeaseGrant>, tonic::Status> {
        self.client.clone().renew(request).await
    }
    async fn fragment_fixed(
        &self,
        request: tonic::Request<delay_wire::FixedFragmentEnvelope>,
    ) -> Result<tonic::Response<delay_wire::FixedFragmentResponse>, tonic::Status> {
        let _active = self.traffic.enter(self.shard);
        self.fragments.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(
            self.delay_millis.load(Ordering::SeqCst),
        ))
        .await;
        self.client.clone().fragment_fixed(request).await
    }
    async fn close_fixed(
        &self,
        request: tonic::Request<delay_wire::FixedCloseEnvelope>,
    ) -> Result<tonic::Response<delay_wire::Empty>, tonic::Status> {
        self.client.clone().close_fixed(request).await
    }
}
struct FragmentDelay {
    endpoint: SocketAddr,
    fragments: Arc<AtomicU64>,
    delay_millis: Arc<AtomicU64>,
    shutdown: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}
impl FragmentDelay {
    async fn start(
        upstream: SocketAddr,
        shard: u64,
        traffic: Arc<FragmentTraffic>,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let fragments = Arc::new(AtomicU64::new(0));
        let delay_millis = Arc::new(AtomicU64::new(0));
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{upstream}"))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .connect()
            .await?;
        let service = DelayService {
            client: delay_wire::fixed_query_service_client::FixedQueryServiceClient::new(channel)
                .max_decoding_message_size(4 * 1024 * 1024)
                .max_encoding_message_size(4 * 1024 * 1024),
            shard,
            traffic,
            fragments: fragments.clone(),
            delay_millis: delay_millis.clone(),
        };
        let shutdown = tokio_util::sync::CancellationToken::new();
        let stopped = shutdown.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .concurrency_limit_per_connection(32)
                .timeout(Duration::from_secs(10))
                .add_service(
                    delay_wire::fixed_query_service_server::FixedQueryServiceServer::new(service)
                        .max_decoding_message_size(4 * 1024 * 1024)
                        .max_encoding_message_size(4 * 1024 * 1024),
                )
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    stopped.cancelled(),
                )
                .await?;
            Ok(())
        });
        Ok(Self {
            endpoint,
            fragments,
            delay_millis,
            shutdown,
            task,
        })
    }
    fn delay(&self, millis: u64) {
        self.delay_millis.store(millis, Ordering::SeqCst);
    }
    fn count(&self) -> u64 {
        self.fragments.load(Ordering::SeqCst)
    }
    async fn stop(mut self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(15), &mut self.task)
            .await
            .expect("typed delay proxy shutdown deadline")
            .unwrap()
            .unwrap();
    }
}
impl Drop for FragmentDelay {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

async fn route(p: &Process) -> anyhow::Result<gateway::LocalGraphRoute> {
    fs::write(p.directory.path().join("graph.request"), b"start")?;
    let endpoint = tokio::time::timeout(BUDGET, async {
        loop {
            if let Ok(bytes) = fs::read(p.directory.path().join("graph-ready.json")) {
                let (pid, endpoint): (u32, SocketAddr) = serde_json::from_slice(&bytes)?;
                if pid == p.child.id() {
                    return Ok::<_, anyhow::Error>(endpoint);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(gateway::LocalGraphRoute {
        endpoint,
        identity: replicated_graph::remote::RemoteIdentity {
            group: p.shard,
            node: 1,
        },
        managed_identity: identity(p.shard),
    })
}
fn query() -> Query {
    Query {
        partition: 7,
        text: format!(
            "g.V({}).out().out().path()",
            vec![ROOT.to_string(); COPIES].join(",")
        ),
    }
}
fn expected() -> Vec<QueryRow> {
    // Fixture-owned ordered bag with qualified identities, independent of engine output.
    let mut rows = Vec::new();
    for _ in 0..COPIES {
        for (middle_partition, first_edge, last_partition, last_edge) in
            [(7, EDGE, 8, EDGE + 2), (8, EDGE + 1, 7, EDGE + 3)]
        {
            rows.push(QueryRow::Path(vec![
                QualifiedElement::Vertex {
                    partition: 7,
                    id: ROOT,
                },
                QualifiedElement::Edge {
                    partition: 7,
                    id: first_edge,
                },
                QualifiedElement::Vertex {
                    partition: middle_partition,
                    id: MIDDLE,
                },
                QualifiedElement::Edge {
                    partition: middle_partition,
                    id: last_edge,
                },
                QualifiedElement::Vertex {
                    partition: last_partition,
                    id: ROOT,
                },
            ]));
        }
    }
    rows
}

async fn write(g: &Gateway, sequence: u8, mutation: Mutation) -> anyhow::Result<u64> {
    let mut external_id = [b'J'; 16];
    external_id[15] = sequence;
    Ok(committed(
        call(
            g,
            Operation::Submit(Write {
                external_id,
                mutation,
            }),
        )
        .await?,
    )
    .1)
}
async fn start(g: &Gateway, id: u8, version: u64, idle_millis: u64) -> anyhow::Result<JobHandle> {
    start_with_policy(
        g,
        id,
        version,
        JobPolicy {
            idle_millis,
            execution_millis: 120_000,
            lifetime_millis: 180_000,
        },
    )
    .await
}
async fn start_with_policy(
    g: &Gateway,
    id: u8,
    version: u64,
    policy: JobPolicy,
) -> anyhow::Result<JobHandle> {
    let operation = Operation::StartLocalQueryJob {
        operation_id: [id; 16],
        version: Some(version),
        query: query(),
        page_size: 7,
        policy,
    };
    let reply = call(g, operation.clone()).await?;
    assert_eq!(
        call(g, operation).await?,
        reply,
        "admission replay must retain identity"
    );
    let Reply::StartLocalQueryJob { job, .. } = reply else {
        anyhow::bail!("expected job")
    };
    Ok(job)
}
async fn status(g: &Gateway, job: &JobHandle) -> anyhow::Result<JobStatus> {
    let Reply::GetLocalQueryJob { status, .. } =
        call(g, Operation::GetLocalQueryJob { job: job.clone() }).await?
    else {
        anyhow::bail!("expected status")
    };
    Ok(status)
}
async fn phase(g: &Gateway, job: &JobHandle, wanted: JobPhase) -> anyhow::Result<JobStatus> {
    tokio::time::timeout(BUDGET, async {
        loop {
            let current = status(g, job).await?;
            if current.phase == wanted {
                return Ok(current);
            }
            anyhow::ensure!(
                matches!(
                    current.phase,
                    JobPhase::Capturing | JobPhase::Running | JobPhase::Sealing
                ),
                "unexpected phase {current:?}, wanted {wanted:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}
async fn fetch(
    g: &Gateway,
    job: &JobHandle,
    cursor: Option<PageCursor>,
) -> anyhow::Result<(u64, QueryPage)> {
    let Reply::FetchLocalQueryJob { version, page, .. } = call(
        g,
        Operation::FetchLocalQueryJob {
            job: job.clone(),
            query: query(),
            cursor,
            page_size: 7,
        },
    )
    .await?
    else {
        anyhow::bail!("expected page")
    };
    Ok((version, page))
}
async fn cancel(g: &Gateway, job: &JobHandle) -> anyhow::Result<()> {
    let Reply::CancelLocalQueryJob { status, .. } =
        call(g, Operation::CancelLocalQueryJob { job: job.clone() }).await?
    else {
        anyhow::bail!("expected cancel")
    };
    assert_eq!(status.phase, JobPhase::Cancelled);
    assert!(fetch(g, job, None).await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewable_query_runs_past_sixty_seconds_with_real_writes() -> anyhow::Result<()> {
    let _exclusive = RETAINED_FIXTURE_PHASE.lock().await;
    let coordinator = Process::start(9).await?;
    let mut a = Process::start(7).await?;
    let mut b = Process::start(8).await?;
    let mut ar = route(&a).await?;
    let mut br = route(&b).await?;
    let traffic = Arc::new(FragmentTraffic::default());
    let ap = FragmentDelay::start(ar.endpoint, 7, traffic.clone()).await?;
    let bp = FragmentDelay::start(br.endpoint, 8, traffic.clone()).await?;
    ar.endpoint = ap.endpoint;
    br.endpoint = bp.endpoint;
    let g = Gateway::bind_remote_local_queries(
        RemoteConfig {
            coordinator: coordinator.client()?,
            binding: binding(),
            members: vec![a.client()?, b.client()?],
        },
        vec![ar, br],
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    for (index, (partition, id)) in [(7, ROOT), (8, ROOT), (7, MIDDLE), (8, MIDDLE)]
        .into_iter()
        .enumerate()
    {
        write(
            &g,
            (index + 1) as u8,
            Mutation::CreateVertex {
                partition,
                vertex: VertexData {
                    id,
                    labels: vec!["person".into()],
                    properties: BTreeMap::new(),
                },
            },
        )
        .await?;
    }
    let mut fixed_version = 0;
    for (index, (source_partition, source, target_partition, target)) in [
        (7, ROOT, 7, MIDDLE),
        (7, ROOT, 8, MIDDLE),
        (7, MIDDLE, 8, ROOT),
        (8, MIDDLE, 7, ROOT),
    ]
    .into_iter()
    .enumerate()
    {
        fixed_version = write(
            &g,
            (index + 5) as u8,
            Mutation::CreateEdge {
                edge: EdgeData {
                    id: EDGE + index as u64,
                    from: Entity {
                        partition: source_partition,
                        id: source,
                    },
                    to: Entity {
                        partition: target_partition,
                        id: target,
                    },
                    label: "knows".into(),
                    properties: BTreeMap::new(),
                },
            },
        )
        .await?;
    }
    // Un-delayed control: same API, query, real child nodes, and independent oracle.
    let control_started = tokio::time::Instant::now();
    let control = start(&g, 11, fixed_version, 60_000).await?;
    phase(&g, &control, JobPhase::Ready).await?;
    let mut rows = Vec::new();
    let mut cursor = None;
    loop {
        let (version, page) = fetch(&g, &control, cursor).await?;
        assert_eq!(version, fixed_version);
        rows.extend(page.data.rows);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(rows, expected());
    eprintln!(
        "query job un-delayed TCP control: {} exact paths at V={}, elapsed={:?}",
        rows.len(),
        fixed_version,
        control_started.elapsed()
    );
    cancel(&g, &control).await?;

    // The ordinary default width=3 scheduler must overlap eligible work on
    // distinct shards. Twelve repeated roots generate 96 required leaves;
    // dependencies and same-shard lanes retain 68 one-second transport waves.
    traffic.peak_shards.store(0, Ordering::SeqCst);
    ap.delay(1_000);
    bp.delay(1_000);
    let base = ap.count() + bp.count();
    let job = start(&g, 12, fixed_version, 60_000).await?;
    phase(&g, &job, JobPhase::Running).await?;
    let running_since = tokio::time::Instant::now();
    // A received required fragment proves captures finished and traversal remains live.
    tokio::time::timeout(BUDGET, async {
        while ap.count() + bp.count() == base {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(
        write(
            &g,
            20,
            Mutation::PatchVertex {
                vertex: Entity {
                    partition: 8,
                    id: MIDDLE
                },
                patch: Patch {
                    set: BTreeMap::from([(
                        "name".into(),
                        Value::Text("changed while running".into())
                    )]),
                    remove: BTreeSet::new(),
                }
            }
        )
        .await?
            > fixed_version
    );
    assert_eq!(status(&g, &job).await?.phase, JobPhase::Running);
    for (sequence, edge) in [(21, EDGE), (22, EDGE + 1)] {
        assert!(
            write(
                &g,
                sequence,
                Mutation::DeleteEdge {
                    edge: Entity {
                        partition: 7,
                        id: edge
                    }
                },
            )
            .await?
                > fixed_version
        );
    }
    let latest = open(&g, None).await?;
    assert!(latest.version > fixed_version);
    assert_eq!(
        traverse(&g, &latest, 7, &format!("g.V({ROOT}).out().count()")).await?,
        vec![QueryRow::Count(0)]
    );
    assert_eq!(
        traverse(&g, &latest, 8, &format!("g.V({MIDDLE}).values('name')")).await?,
        vec![QueryRow::Value {
            source: Some(QualifiedElement::Vertex {
                partition: 8,
                id: MIDDLE
            }),
            value: Value::Text("changed while running".into()),
        }]
    );
    call(&g, Operation::Close { session: latest }).await?;
    eprintln!("query job genuine concurrent patch/delete committed while Running; required fragments started={}", ap.count() + bp.count() - base);
    for (generation, seconds) in [(0, 20), (1, 40), (2, 61)] {
        tokio::time::sleep_until(running_since + Duration::from_secs(seconds)).await;
        let state = status(&g, &job).await?;
        assert_eq!(state.phase, JobPhase::Running);
        assert!(
            state.total.is_none(),
            "Running must not publish partial rows"
        );
        assert!(fetch(&g, &job, None).await.is_err());
        let Reply::RenewLocalQueryJob { grant, .. } = call(
            &g,
            Operation::RenewLocalQueryJob {
                job: job.clone(),
                expected_generation: generation,
            },
        )
        .await?
        else {
            anyhow::bail!("expected renewal")
        };
        assert_eq!(grant.generation, generation + 1);
        eprintln!(
            "query job Running elapsed={:?}, required fragments started={}, renewal generation={}",
            running_since.elapsed(),
            ap.count() + bp.count() - base,
            grant.generation
        );
    }
    assert!(running_since.elapsed() > Duration::from_secs(60));
    assert!(
        ap.count() + bp.count() - base < REQUIRED_FRAGMENTS,
        "required reads still outstanding after 60s"
    );
    let ready = phase(&g, &job, JobPhase::Ready).await?;
    assert_eq!(ap.count() + bp.count() - base, REQUIRED_FRAGMENTS);
    assert_eq!(
        traffic.peak(),
        2,
        "required leaves must overlap on both real shards"
    );
    eprintln!(
        "query job Ready elapsed={:?}, all 96 required TCP fragments finished at original V={}, peak active shards={}",
        running_since.elapsed(),
        fixed_version,
        traffic.peak(),
    );
    assert_eq!(ready.version, Some(fixed_version));
    assert_eq!(ready.total, Some((2 * COPIES) as u32));
    let mut rows = Vec::new();
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let result = fetch(&g, &job, cursor.clone()).await?;
        assert_eq!(result.0, fixed_version);
        assert_eq!(fetch(&g, &job, cursor.clone()).await?, result);
        rows.extend(result.1.data.rows.clone());
        pages.push((cursor, result.clone()));
        cursor = result.1.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(rows, expected());

    // Independent cancellation and omitted-renewal paths while reads are required.
    let cancelled = start(&g, 13, fixed_version, 60_000).await?;
    phase(&g, &cancelled, JobPhase::Running).await?;
    let reserved = g.retained_payload_charge();
    cancel(&g, &cancelled).await?;
    assert_eq!(status(&g, &cancelled).await?.total, None);
    // The admission reservation deliberately includes its bounded replay tombstone.
    assert_eq!(g.retained_payload_charge(), reserved);
    let expired = start_with_policy(
        &g,
        14,
        fixed_version,
        JobPolicy {
            idle_millis: 1_000,
            execution_millis: 2_000,
            lifetime_millis: 3_000,
        },
    )
    .await?;
    assert!(g.retained_payload_charge() > reserved);
    phase(&g, &expired, JobPhase::Expired).await?;
    assert!(call(
        &g,
        Operation::RenewLocalQueryJob {
            job: expired.clone(),
            expected_generation: 0,
        }
    )
    .await
    .is_err());
    assert!(fetch(&g, &expired, None).await.is_err());
    tokio::time::timeout(BUDGET, async {
        while g.retained_payload_charge() != reserved {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    assert!(
        status(&g, &expired).await.is_err(),
        "hard-expired identity is gone"
    );
    // Kill a real required participant before the final read. No partial Ready.
    let failed = start(&g, 15, fixed_version, 60_000).await?;
    phase(&g, &failed, JobPhase::Running).await?;
    a.stop()?;
    let failure = phase(&g, &failed, JobPhase::Failed).await?;
    assert!(failure.total.is_none());
    assert!(fetch(&g, &failed, None).await.is_err());
    b.stop()?;
    // Sealed pages no longer depend on either graph process.
    for (cursor, result) in pages {
        assert_eq!(fetch(&g, &job, cursor.clone()).await?, result);
        assert_eq!(fetch(&g, &job, cursor).await?, result);
    }
    cancel(&g, &job).await?;
    g.shutdown().await;
    ap.stop().await;
    bp.stop().await;
    Ok(())
}
