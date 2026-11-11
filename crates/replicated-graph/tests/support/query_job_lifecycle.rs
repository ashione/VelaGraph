//! Injected transport-delay functional evidence, not compute/performance evidence.
//! The graph and every managed mutation live in independent child processes.
use super::*;
use gateway::{JobHandle, JobPhase, JobPolicy, JobStatus};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};

const ROOT: u64 = 2_000_001;
const EDGE: u64 = 2_000_010;
const COPIES: usize = 24;

/// Bounded, transparent one-request TCP proxy. Control traffic never sleeps.
/// Each accepted task has a deadline; JoinSet owns and aborts all child tasks.
struct FragmentDelay {
    endpoint: SocketAddr,
    fragments: Arc<AtomicU64>,
    delay_millis: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}
impl FragmentDelay {
    async fn start(upstream: SocketAddr) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let fragments = Arc::new(AtomicU64::new(0));
        let delay_millis = Arc::new(AtomicU64::new(0));
        let count = fragments.clone();
        let delay = delay_millis.clone();
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut incoming, _) = accepted?;
                        anyhow::ensure!(children.len() < 32, "proxy task bound");
                        let count = count.clone();
                        let delay = delay.clone();
                        children.spawn(async move {
                            tokio::time::timeout(Duration::from_secs(10), async move {
                                let length = incoming.read_u32().await?;
                                anyhow::ensure!(length <= 4 * 1024 * 1024, "proxy frame bound");
                                let mut frame = vec![0; length as usize];
                                incoming.read_exact(&mut frame).await?;
                                let request: serde_json::Value = serde_json::from_slice(&frame)?;
                                if request["operation"].get("FixedQueryFragment").is_some() {
                                    count.fetch_add(1, Ordering::SeqCst);
                                    tokio::time::sleep(Duration::from_millis(delay.load(Ordering::SeqCst))).await;
                                }
                                let mut outgoing = TcpStream::connect(upstream).await?;
                                outgoing.write_u32(length).await?;
                                outgoing.write_all(&frame).await?;
                                let length = outgoing.read_u32().await?;
                                anyhow::ensure!(length <= 4 * 1024 * 1024, "proxy response bound");
                                let mut response = vec![0; length as usize];
                                outgoing.read_exact(&mut response).await?;
                                incoming.write_u32(length).await?;
                                incoming.write_all(&response).await?;
                                Ok::<_, anyhow::Error>(())
                            }).await
                        });
                    }
                    Some(result) = children.join_next(), if !children.is_empty() => {
                        // Socket failures are intentional during cancel and node death.
                        let _ = result?;
                    }
                }
            }
        });
        Ok(Self {
            endpoint,
            fragments,
            delay_millis,
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
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for FragmentDelay {
    fn drop(&mut self) {
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
            "g.V({}).out().path()",
            vec![ROOT.to_string(); COPIES].join(",")
        ),
    }
}
fn expected() -> Vec<QueryRow> {
    // Independent semantic oracle, not copied from any engine output.
    vec![
        QueryRow::Path(vec![
            QualifiedElement::Vertex {
                partition: 7,
                id: ROOT
            },
            QualifiedElement::Edge {
                partition: 7,
                id: EDGE
            },
            QualifiedElement::Vertex {
                partition: 8,
                id: ROOT
            },
        ]);
        COPIES
    ]
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
    let ap = FragmentDelay::start(ar.endpoint).await?;
    let bp = FragmentDelay::start(br.endpoint).await?;
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
    for (sequence, partition) in [(1, 7), (2, 8)] {
        write(
            &g,
            sequence,
            Mutation::CreateVertex {
                partition,
                vertex: VertexData {
                    id: ROOT,
                    labels: vec!["person".into()],
                    properties: BTreeMap::new(),
                },
            },
        )
        .await?;
    }
    let fixed_version = write(
        &g,
        3,
        Mutation::CreateEdge {
            edge: EdgeData {
                id: EDGE,
                from: Entity {
                    partition: 7,
                    id: ROOT,
                },
                to: Entity {
                    partition: 8,
                    id: ROOT,
                },
                label: "knows".into(),
                properties: BTreeMap::new(),
            },
        },
    )
    .await?;
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
            4,
            Mutation::PatchVertex {
                vertex: Entity {
                    partition: 8,
                    id: ROOT
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
    assert!(
        write(
            &g,
            5,
            Mutation::DeleteEdge {
                edge: Entity {
                    partition: 7,
                    id: EDGE
                }
            }
        )
        .await?
            > fixed_version
    );
    let latest = open(&g, None).await?;
    assert!(latest.version > fixed_version);
    assert_eq!(
        traverse(&g, &latest, 7, &format!("g.V({ROOT}).out().count()")).await?,
        vec![QueryRow::Count(0)]
    );
    assert_eq!(
        traverse(&g, &latest, 8, &format!("g.V({ROOT}).values('name')")).await?,
        vec![QueryRow::Value {
            source: Some(QualifiedElement::Vertex {
                partition: 8,
                id: ROOT
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
        ap.count() + bp.count() - base < 72,
        "required reads still outstanding after 60s"
    );
    let ready = phase(&g, &job, JobPhase::Ready).await?;
    assert_eq!(ap.count() + bp.count() - base, 72);
    eprintln!(
        "query job Ready elapsed={:?}, all 72 required TCP fragments finished at original V={}",
        running_since.elapsed(),
        fixed_version
    );
    assert_eq!(ready.version, Some(fixed_version));
    assert_eq!(ready.total, Some(COPIES as u32));
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
