//! Typed job lifecycle across independent public-service and managed-node processes.
//! Shared fixture helpers also serve other targets; this is not a throughput test.
#![allow(dead_code)]
include!("support/public_service_fixture.rs");

type Jobs =
    pb::managed_query_job_service_client::ManagedQueryJobServiceClient<tonic::transport::Channel>;
type Writes = pb::write_service_client::WriteServiceClient<tonic::transport::Channel>;

fn start_job(id: u8, version: Option<u64>) -> pb::JobStartRequest {
    pb::JobStartRequest {
        context: context(),
        operation_id: vec![id; 16],
        version,
        query: Some(pb::Query {
            partition: 7,
            text: "g.V(1,1).out('reports_to').path()".into(),
        }),
        page_size: 1,
        policy: Some(pb::JobPolicy {
            idle_millis: 60_000,
            execution_millis: 120_000,
            lifetime_millis: 180_000,
        }),
    }
}
fn get_job(job: &pb::JobHandle) -> pb::JobGetRequest {
    pb::JobGetRequest {
        context: context(),
        job: Some(job.clone()),
    }
}
fn fetch_job(job: &pb::JobHandle, start: &pb::JobStartRequest) -> pb::JobFetchRequest {
    pb::JobFetchRequest {
        context: context(),
        job: Some(job.clone()),
        query: start.query.clone(),
        cursor: None,
        page_size: start.page_size,
    }
}
async fn ready(jobs: &mut Jobs, job: &pb::JobHandle) -> anyhow::Result<pb::JobStatus> {
    tokio::time::timeout(BUDGET, async {
        loop {
            let reply = jobs.get(get_job(job)).await?.into_inner();
            assert_eq!(reply.job.as_ref(), Some(job));
            let status = reply.status.context("missing job status")?;
            if status.phase == pb::JobPhase::Ready as i32 {
                assert!(
                    status.version.is_some() && status.total.is_some() && status.failure.is_none()
                );
                return Ok::<_, anyhow::Error>(status);
            }
            anyhow::ensure!(
                matches!(
                    pb::JobPhase::try_from(status.phase)?,
                    pb::JobPhase::Capturing | pb::JobPhase::Running | pb::JobPhase::Sealing
                ),
                "job did not reach Ready: {status:?}"
            );
            assert!(status.version.is_none() && status.total.is_none() && status.failure.is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}
async fn write(
    writes: &mut Writes,
    external_id: u8,
    kind: pb::mutation::Kind,
) -> anyhow::Result<u64> {
    let reply = writes
        .submit(pb::WriteRequest {
            context: context(),
            external_id: vec![external_id; 16],
            mutation: Some(pb::Mutation { kind: Some(kind) }),
        })
        .await?
        .into_inner();
    let Some(pb::write_response::Status::Committed(decision)) = reply.status else {
        anyhow::bail!("write not committed: {reply:?}");
    };
    assert!(decision.visible);
    Ok(decision.ordinal)
}
fn expected_paths() -> Vec<pb::Row> {
    // Fixture-owned oracle: qualified IDs, parallel edges and duplicate starts.
    (0..2)
        .flat_map(|_| [(10, 2), (20, 3), (30, 3)])
        .map(|(edge, id)| pb::Row {
            kind: Some(pb::row::Kind::Path(pb::Path {
                elements: vec![
                    pb::QualifiedElement {
                        kind: Some(pb::qualified_element::Kind::Vertex(pb::Entity {
                            partition: 7,
                            id: 1,
                        })),
                    },
                    pb::QualifiedElement {
                        kind: Some(pb::qualified_element::Kind::Edge(pb::Entity {
                            partition: 7,
                            id: edge,
                        })),
                    },
                    pb::QualifiedElement {
                        kind: Some(pb::qualified_element::Kind::Vertex(pb::Entity {
                            partition: 8,
                            id,
                        })),
                    },
                ],
            })),
        })
        .collect()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_jobs_preserve_replay_fixed_results_lease_and_restart() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(180), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    use pb::error_detail::Reason;
    let (coordinator, a, b) = tokio::try_join!(
        NodeProcess::start(9, true),
        NodeProcess::start(7, true),
        NodeProcess::start(8, true)
    )?;
    let directory = tempfile::tempdir()?;
    let config = json!({"binding":binding(),"coordinator":{"endpoint":coordinator.ready.control,"identity":identity(9)},"members":[{"endpoint":a.ready.control,"identity":identity(7),"graph_endpoint":a.ready.graph},{"endpoint":b.ready.control,"identity":identity(8),"graph_endpoint":b.ready.graph}]});
    let config_path = directory.path().join("service.json");
    fs::write(&config_path, serde_json::to_vec(&config)?)?;
    let mut service =
        ServiceProcess::start(&config_path, directory.path().join("jobs1.stdout")).await?;
    assert_eq!(
        [
            std::process::id(),
            service.child.id(),
            coordinator.child.id(),
            a.child.id(),
            b.child.id()
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len(),
        5
    );
    let channel = grpc_channel(service.grpc).await?;
    let mut jobs = Jobs::new(channel.clone());
    let mut writes = Writes::new(channel);
    for (external, partition, id) in [(1, 7, 1), (2, 8, 2), (3, 8, 3)] {
        write(
            &mut writes,
            external,
            pb::mutation::Kind::CreateVertex(pb::CreateVertex {
                partition,
                vertex: Some(pb::Vertex {
                    id,
                    labels: vec!["person".into()],
                    properties: vec![],
                }),
            }),
        )
        .await?;
    }
    let mut version = 0;
    for (external, id, to) in [(4, 10, 2), (5, 20, 3), (6, 30, 3)] {
        version = write(
            &mut writes,
            external,
            pb::mutation::Kind::CreateEdge(pb::CreateEdge {
                edge: Some(pb::Edge {
                    id,
                    from: Some(pb::Entity {
                        partition: 7,
                        id: 1,
                    }),
                    to: Some(pb::Entity {
                        partition: 8,
                        id: to,
                    }),
                    label: "reports_to".into(),
                    properties: vec![],
                }),
            }),
        )
        .await?;
    }
    let request = start_job(40, Some(version));
    let admitted = jobs.start(request.clone()).await?.into_inner();
    assert_eq!(admitted.operation_id, request.operation_id);
    let job = admitted.job.clone().context("admitted handle")?;
    assert_eq!(jobs.start(request.clone()).await?.into_inner(), admitted);
    for change in 0..4 {
        let mut conflicting = request.clone();
        match change {
            0 => conflicting.version = None,
            1 => conflicting.page_size = 2,
            2 => conflicting.query.as_mut().unwrap().text = "g.V(1).count()".into(),
            _ => conflicting.policy.as_mut().unwrap().idle_millis -= 1,
        }
        assert_reason(
            &jobs.start(conflicting).await.unwrap_err(),
            Reason::IdentityMismatch,
        );
    }
    let mut invalid = request.clone();
    invalid.page_size = 0;
    assert_reason(
        &jobs.start(invalid).await.unwrap_err(),
        Reason::InvalidArgument,
    );
    let status = ready(&mut jobs, &job).await?;
    assert_eq!((status.version, status.total), (Some(version), Some(6)));
    let first_request = fetch_job(&job, &request);
    let first = jobs.fetch(first_request.clone()).await?.into_inner();
    assert_eq!(first.version, version);
    // Mutation after admission cannot change the sealed original bag or its V.
    let later = write(
        &mut writes,
        7,
        pb::mutation::Kind::DeleteEdge(pb::Entity {
            partition: 7,
            id: 20,
        }),
    )
    .await?;
    assert!(later > version);
    assert_eq!(jobs.fetch(first_request.clone()).await?.into_inner(), first);
    let mut page_request = first_request.clone();
    let mut rows = Vec::new();
    let mut offset = 0;
    loop {
        let response = jobs.fetch(page_request.clone()).await?.into_inner();
        assert_eq!(
            jobs.fetch(page_request.clone()).await?.into_inner(),
            response
        );
        assert_eq!(response.job.as_ref(), Some(&job));
        assert_eq!(response.version, version);
        assert_eq!(response.query, request.query);
        let page = response.page.context("page")?;
        assert_eq!((page.offset, page.total), (offset, 6));
        assert_eq!(page.rows.len(), 1);
        rows.extend(page.rows);
        page_request.cursor = page.next;
        if page_request.cursor.is_none() {
            break;
        }
        offset += 1;
    }
    assert_eq!(rows, expected_paths());
    let mut tampered = first_request.clone();
    tampered.query.as_mut().unwrap().text = "g.V(1).count()".into();
    assert_reason(
        &jobs.fetch(tampered).await.unwrap_err(),
        Reason::IdentityMismatch,
    );
    let mut tampered = first_request.clone();
    tampered.cursor = Some(pb::Cursor { token: vec![] });
    assert!(
        jobs.fetch(tampered).await.is_err(),
        "present empty cursor is not first page"
    );
    for wrong_incarnation in [true, false] {
        let mut wrong = job.clone();
        if wrong_incarnation {
            wrong.incarnation[0] ^= 1;
        } else {
            wrong.token[0] ^= 1;
        }
        assert_reason(
            &jobs.get(get_job(&wrong)).await.unwrap_err(),
            Reason::SessionUnavailable,
        );
    }
    let renewal = pb::JobRenewRequest {
        context: context(),
        job: Some(job.clone()),
        expected_generation: 0,
    };
    let grant = jobs.renew(renewal.clone()).await?.into_inner();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let replay = jobs.renew(renewal).await?.into_inner();
    assert_eq!(grant.generation, 1);
    assert_eq!(replay.generation, grant.generation);
    assert!(replay.remaining_millis <= grant.remaining_millis);
    let error = jobs
        .renew(pb::JobRenewRequest {
            context: context(),
            job: Some(job.clone()),
            expected_generation: u64::MAX,
        })
        .await
        .unwrap_err();
    assert_reason(&error, Reason::IdentityMismatch);
    let cancelled = jobs
        .cancel(pb::JobCancelRequest {
            context: context(),
            job: Some(job.clone()),
        })
        .await?
        .into_inner();
    assert_eq!(
        cancelled.status.unwrap().phase,
        pb::JobPhase::Cancelled as i32
    );
    assert_eq!(
        jobs.start(request.clone()).await?.into_inner(),
        admitted,
        "retained terminal replay must not start a new job"
    );
    assert_reason(
        &jobs.fetch(first_request).await.unwrap_err(),
        Reason::SessionUnavailable,
    );

    // Exact zero and latest remain distinct on the actual typed service.
    let zero_request = start_job(41, Some(0));
    let zero = jobs
        .start(zero_request.clone())
        .await?
        .into_inner()
        .job
        .context("zero job")?;
    assert_eq!(ready(&mut jobs, &zero).await?.version, Some(0));
    assert_eq!(
        jobs.fetch(fetch_job(&zero, &zero_request))
            .await?
            .into_inner()
            .page
            .unwrap()
            .total,
        0
    );
    let latest_request = start_job(42, None);
    let latest = jobs
        .start(latest_request.clone())
        .await?
        .into_inner()
        .job
        .context("latest job")?;
    assert_eq!(ready(&mut jobs, &latest).await?.version, Some(later));
    assert_eq!(
        jobs.fetch(fetch_job(&latest, &latest_request))
            .await?
            .into_inner()
            .page
            .unwrap()
            .total,
        4
    );

    // A short-idle fourth job cannot be kept alive by polling Get/Fetch.
    let mut short = start_job(43, Some(later));
    short.policy = Some(pb::JobPolicy {
        idle_millis: 1000,
        execution_millis: 5000,
        lifetime_millis: 6000,
    });
    let short_job = jobs
        .start(short.clone())
        .await?
        .into_inner()
        .job
        .context("short job")?;
    let exhausted = jobs.start(start_job(44, Some(later))).await.unwrap_err();
    assert_reason(&exhausted, Reason::ResourceExhausted);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let state = jobs
                .get(get_job(&short_job))
                .await?
                .into_inner()
                .status
                .context("short status")?;
            if state.phase == pb::JobPhase::Expired as i32 {
                break;
            }
            anyhow::ensure!(
                matches!(
                    pb::JobPhase::try_from(state.phase)?,
                    pb::JobPhase::Capturing
                        | pb::JobPhase::Running
                        | pb::JobPhase::Sealing
                        | pb::JobPhase::Ready
                ),
                "unexpected short phase {state:?}"
            );
            // Fetch may be Busy before sealing, but must not renew in either case.
            let _ = jobs.fetch(fetch_job(&short_job, &short)).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_reason(
        &jobs
            .renew(pb::JobRenewRequest {
                context: context(),
                job: Some(short_job.clone()),
                expected_generation: 0,
            })
            .await
            .unwrap_err(),
        Reason::SessionUnavailable,
    );
    assert_reason(
        &jobs.fetch(fetch_job(&short_job, &short)).await.unwrap_err(),
        Reason::SessionUnavailable,
    );
    service.graceful_stop().await?;
    drop((jobs, writes));
    let mut restarted =
        ServiceProcess::start(&config_path, directory.path().join("jobs2.stdout")).await?;
    let mut jobs = Jobs::new(grpc_channel(restarted.grpc).await?);
    for old in [&job, &zero, &latest, &short_job] {
        assert_reason(
            &jobs.get(get_job(old)).await.unwrap_err(),
            Reason::SessionUnavailable,
        );
    }
    let fresh = jobs
        .start(request)
        .await?
        .into_inner()
        .job
        .context("post restart job")?;
    assert_ne!(fresh.incarnation, job.incarnation);
    assert_eq!(ready(&mut jobs, &fresh).await?.version, Some(version));
    interrupted_delivery(&mut jobs, restarted.grpc, version).await?;
    restarted.graceful_stop().await?;
    Ok(())
}

// A response-only TCP gate is test instrumentation, never a product fallback.
// Only a complete, decoded gRPC DATA response proves business completion.
// HTTP/2 control frames and response headers are forwarded, not counted as proof.
async fn interrupted_delivery(
    jobs: &mut Jobs,
    endpoint: SocketAddr,
    version: u64,
) -> anyhow::Result<()> {
    let mut proxy = ResponseGate::start(endpoint).await?;
    let mut gated = Jobs::new(grpc_channel(proxy.address).await?);
    let unknown = pb::JobHandle {
        incarnation: vec![0; 16],
        token: vec![0; 32],
    };
    assert!(gated.get(get_job(&unknown)).await.is_err());
    let start = start_job(70, Some(version));
    proxy.paused.send(true)?;
    let mut pending_client = gated.clone();
    let pending_start = start.clone();
    let pending = tokio::spawn(async move { pending_client.start(pending_start).await });
    let response = tokio::time::timeout(BUDGET, proxy.responses.recv())
        .await?
        .context("gate closed before Start response DATA")?;
    let admitted = <pb::JobStartResponse as prost::Message>::decode(response.as_slice())?;
    assert_eq!(admitted.operation_id, start.operation_id);
    let handle = admitted.job.clone().context("admitted interrupted job")?;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    drop(gated);
    drop(proxy);
    assert_eq!(jobs.start(start.clone()).await?.into_inner(), admitted);
    ready(jobs, &handle).await?;
    let original = jobs.fetch(fetch_job(&handle, &start)).await?.into_inner();
    // An independent polling connection disappearing does not own the job.
    let mut transient = Jobs::new(grpc_channel(endpoint).await?);
    transient.get(get_job(&handle)).await?;
    transient.fetch(fetch_job(&handle, &start)).await?;
    drop(transient);
    assert_eq!(
        jobs.fetch(fetch_job(&handle, &start)).await?.into_inner(),
        original
    );
    let mut proxy = ResponseGate::start(endpoint).await?;
    let mut gated = Jobs::new(grpc_channel(proxy.address).await?);
    gated.get(get_job(&handle)).await?;
    proxy.paused.send(true)?;
    let renewal = pb::JobRenewRequest {
        context: context(),
        job: Some(handle.clone()),
        expected_generation: 0,
    };
    let pending_request = renewal.clone();
    let pending = tokio::spawn(async move { gated.renew(pending_request).await });
    let response = tokio::time::timeout(BUDGET, proxy.responses.recv())
        .await?
        .context("gate closed before Renew response DATA")?;
    let lost_renewal = <pb::JobRenewResponse as prost::Message>::decode(response.as_slice())?;
    assert_eq!(lost_renewal.job.as_ref(), Some(&handle));
    assert_eq!(lost_renewal.generation, 1);
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    drop(proxy);
    // Ready has no remote grants. Lost delivery must neither strand its round
    // nor advance the same committed renewal more than once.
    let committed = jobs.renew(renewal.clone()).await?.into_inner();
    let replayed = jobs.renew(renewal).await?.into_inner();
    assert_eq!(committed.generation, lost_renewal.generation);
    assert!(committed.remaining_millis <= lost_renewal.remaining_millis);
    assert_eq!(replayed.generation, committed.generation);
    assert!(replayed.remaining_millis <= committed.remaining_millis);
    Ok(())
}
struct ResponseGate {
    address: SocketAddr,
    paused: tokio::sync::watch::Sender<bool>,
    responses: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for ResponseGate {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl ResponseGate {
    async fn start(upstream: SocketAddr) -> anyhow::Result<Self> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (paused, pause) = tokio::sync::watch::channel(false);
        let (signal, responses) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break };
                        let mut pause = pause.clone();
                        let signal = signal.clone();
                        tasks.spawn(async move {
                            let server = tokio::net::TcpStream::connect(upstream).await?;
                            let (mut cr, mut cw) = client.into_split();
                            let (mut sr, mut sw) = server.into_split();
                            let requests = tokio::io::copy(&mut cr, &mut sw);
                            let responses = async {
                                // A server HTTP/2 connection begins with frames (no preface).
                                // Parse framing so SETTINGS/PING/WINDOW_UPDATE cannot be
                                // mistaken for completion of the pending business RPC.
                                let mut message = Vec::new();
                                let mut message_stream = None;
                                loop {
                                    let mut header = [0u8; 9];
                                    sr.read_exact(&mut header).await?;
                                    let length = ((header[0] as usize) << 16)
                                        | ((header[1] as usize) << 8)
                                        | header[2] as usize;
                                    let mut payload = vec![0; length];
                                    sr.read_exact(&mut payload).await?;
                                    let stream = u32::from_be_bytes([
                                        header[5] & 0x7f, header[6], header[7], header[8],
                                    ]);
                                    let capturing = *pause.borrow_and_update();
                                    if header[3] == 0 && stream != 0 && capturing {
                                        if message_stream.is_some_and(|id| id != stream) {
                                            return Err::<(), std::io::Error>(std::io::Error::other("unexpected concurrent DATA stream"));
                                        }
                                        message_stream = Some(stream);
                                        let data = if header[4] & 0x08 != 0 {
                                            let padding = *payload.first().ok_or_else(|| std::io::Error::other("missing DATA padding length"))? as usize;
                                            if padding + 1 > payload.len() {
                                                return Err(std::io::Error::other("invalid DATA padding"));
                                            }
                                            &payload[1..payload.len() - padding]
                                        } else {
                                            payload.as_slice()
                                        };
                                        message.extend_from_slice(data);
                                        if message.len() >= 5 {
                                            if message[0] != 0 {
                                                return Err(std::io::Error::other("unexpected compressed gRPC message"));
                                            }
                                            let size = u32::from_be_bytes(message[1..5].try_into().unwrap()) as usize;
                                            if message.len() >= 5 + size {
                                                // No response DATA has reached the caller. Decode
                                                // this captured message in the test before aborting.
                                                let _ = signal.send(message[5..5 + size].to_vec());
                                                // Intentionally discard all response DATA. This
                                                // gate is single-use; its owner aborts the proxy.
                                                std::future::pending::<()>().await;
                                            }
                                        }
                                        continue;
                                    }
                                    cw.write_all(&header).await?;
                                    cw.write_all(&payload).await?;
                                }
                            };
                            tokio::select! { result = requests => { result?; }, result = responses => { result?; } }
                            Ok::<(), std::io::Error>(())
                        });
                    },
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Ok(Self {
            address,
            paused,
            responses,
            task,
        })
    }
}
