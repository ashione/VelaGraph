//! HTTP/gRPC differential against one public-service job registry and three
//! independent managed-node processes. This is correctness, not throughput evidence.
#![allow(dead_code)]
include!("support/public_service_fixture.rs");

type Jobs =
    pb::managed_query_job_service_client::ManagedQueryJobServiceClient<tonic::transport::Channel>;
fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn handle(value: &Value) -> pb::JobHandle {
    pb::JobHandle {
        incarnation: decode_hex(&value["incarnation"]),
        token: decode_hex(&value["token"]),
    }
}
fn start_body(id: u8, version: Option<u64>) -> Value {
    json!({"operation_id":format!("{id:02x}").repeat(16),
        "read":version.map_or(json!({"type":"latest"}), |v| json!({"type":"exact","version":v.to_string()})),
        "query":{"partition":7,"text":"g.V(1,1).out('reports_to').path()"},"page_size":1,
        "policy":{"idle_millis":60000,"execution_millis":120000,"lifetime_millis":180000}})
}
fn start_proto(body: &Value) -> pb::JobStartRequest {
    pb::JobStartRequest {
        context: context(),
        operation_id: decode_hex(&body["operation_id"]),
        version: body["read"]["version"].as_str().map(|s| s.parse().unwrap()),
        query: Some(pb::Query {
            partition: 7,
            text: body["query"]["text"].as_str().unwrap().into(),
        }),
        page_size: body["page_size"].as_u64().unwrap() as u32,
        policy: Some(pb::JobPolicy {
            idle_millis: body["policy"]["idle_millis"].as_u64().unwrap(),
            execution_millis: body["policy"]["execution_millis"].as_u64().unwrap(),
            lifetime_millis: body["policy"]["lifetime_millis"].as_u64().unwrap(),
        }),
    }
}
async fn ok(address: SocketAddr, method: &str, body: Value) -> anyhow::Result<Value> {
    let (code, response) = post(address, &format!("/preview1/jobs:{method}"), body).await?;
    assert_eq!(code, 200, "{method}: {response}");
    assert_eq!(response["api"], "velagraph.http.preview1");
    Ok(response["result"].clone())
}
async fn error(
    address: SocketAddr,
    method: &str,
    body: Value,
    status: u16,
    reason: &str,
) -> anyhow::Result<()> {
    let (code, response) = post(address, &format!("/preview1/jobs:{method}"), body).await?;
    assert_eq!(code, status, "{response}");
    assert_eq!(response["error"]["code"], reason);
    Ok(())
}
async fn ready_http(address: SocketAddr, job: &Value) -> anyhow::Result<Value> {
    tokio::time::timeout(BUDGET, async {
        loop {
            let reply = ok(address, "get", json!({"job":job})).await?;
            assert_eq!(reply["job"], *job);
            assert_eq!(reply["type"], "job_status");
            let status = &reply["status"];
            if status["phase"] == "ready" {
                return Ok::<_, anyhow::Error>(status.clone());
            }
            anyhow::ensure!(
                matches!(
                    status["phase"].as_str(),
                    Some("capturing" | "running" | "sealing")
                ),
                "unexpected status {status}"
            );
            assert!(
                status["version"].is_null()
                    && status["total"].is_null()
                    && status["failure"].is_null()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}
fn row_json(row: &pb::Row) -> Value {
    let Some(pb::row::Kind::Path(path)) = &row.kind else {
        panic!("fixture must return path")
    };
    let elements: Vec<_> = path
        .elements
        .iter()
        .map(|element| {
            let (kind, entity) = match element.kind.as_ref().unwrap() {
                pb::qualified_element::Kind::Vertex(e) => ("vertex", e),
                pb::qualified_element::Kind::Edge(e) => ("edge", e),
            };
            json!({"type":kind,"partition":entity.partition,"id":entity.id.to_string()})
        })
        .collect();
    json!({"type":"path","elements":elements})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_jobs_share_typed_lifecycle_fixed_version_and_restart_boundary() -> anyhow::Result<()>
{
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
        ServiceProcess::start(&config_path, directory.path().join("http-jobs1.stdout")).await?;
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
    for write in [vertex(1, 7, "1"), vertex(2, 8, "2")] {
        let (code, reply) = post(service.address, "/preview1/writes:submit", write).await?;
        assert_eq!(code, 200, "{reply}");
    }
    let mut writes =
        pb::write_service_client::WriteServiceClient::new(grpc_channel(service.grpc).await?);
    let reply = writes.submit(edge_proto()).await?.into_inner();
    let Some(pb::write_response::Status::Committed(commit)) = reply.status else {
        anyhow::bail!("fixture write not committed")
    };
    assert!(commit.visible);
    let version = commit.ordinal;
    let mut jobs = Jobs::new(grpc_channel(service.grpc).await?);
    let start = start_body(40, Some(version));
    let admitted = ok(service.address, "start", start.clone()).await?;
    assert_eq!(admitted["type"], "job_started");
    assert_eq!(admitted["operation_id"], start["operation_id"]);
    let job = admitted["job"].clone();
    let typed_job = handle(&job);
    let typed = jobs.start(start_proto(&start)).await?.into_inner();
    assert_eq!(typed.job.as_ref(), Some(&typed_job));
    assert_eq!(typed.operation_id, decode_hex(&start["operation_id"]));
    assert_eq!(ok(service.address, "start", start.clone()).await?, admitted);
    let status = ready_http(service.address, &job).await?;
    let typed_status = jobs
        .get(pb::JobGetRequest {
            context: context(),
            job: Some(typed_job.clone()),
        })
        .await?
        .into_inner()
        .status
        .unwrap();
    assert_eq!(
        status,
        json!({"phase":"ready","version":version.to_string(),"total":2,"failure":null})
    );
    assert_eq!(
        (typed_status.phase, typed_status.version, typed_status.total),
        (pb::JobPhase::Ready as i32, Some(version), Some(2))
    );
    let fetch = json!({"job":job,"query":start["query"],"cursor":null,"page_size":1});
    let first = ok(service.address, "fetch", fetch.clone()).await?;
    let (code, deleted) = post(service.address, "/preview1/writes:submit", json!({"external_id":"04040404040404040404040404040404","mutation":{"type":"delete_edge","edge":{"partition":7,"id":"10"}}})).await?;
    assert_eq!(code, 200, "{deleted}");
    let later: u64 = deleted["result"]["status"]["ordinal"]
        .as_str()
        .unwrap()
        .parse()?;
    assert!(later > version);
    assert_eq!(ok(service.address, "fetch", fetch.clone()).await?, first);
    let mut request = fetch.clone();
    let mut rows = Vec::new();
    loop {
        let page = ok(service.address, "fetch", request.clone()).await?;
        assert_eq!(page["type"], "job_page");
        assert_eq!(page["job"], job);
        assert_eq!(page["version"], version.to_string());
        assert_eq!(page["query"], start["query"]);
        assert_eq!(page["cursor"], request["cursor"]);
        assert_eq!(page["page_size"], 1);
        assert_eq!(ok(service.address, "fetch", request.clone()).await?, page);
        let typed = jobs
            .fetch(pb::JobFetchRequest {
                context: context(),
                job: Some(typed_job.clone()),
                query: start_proto(&start).query,
                cursor: request["cursor"].as_str().map(|_| pb::Cursor {
                    token: decode_hex(&request["cursor"]),
                }),
                page_size: 1,
            })
            .await?
            .into_inner();
        assert_eq!(typed.version, version);
        assert_eq!(typed.job.as_ref(), Some(&typed_job));
        let typed_page = typed.page.unwrap();
        assert_eq!(page["page"]["offset"], typed_page.offset);
        assert_eq!(page["page"]["total"], typed_page.total);
        assert_eq!(typed_page.completeness, pb::Completeness::Unverified as i32);
        assert_eq!(page["page"]["completeness"], "unverified");
        assert_eq!(
            page["page"]["next"],
            json!(typed_page.next.as_ref().map(|c| hex_bytes(&c.token)))
        );
        assert_eq!(
            page["page"]["rows"],
            json!(typed_page.rows.iter().map(row_json).collect::<Vec<_>>())
        );
        rows.extend(page["page"]["rows"].as_array().unwrap().iter().cloned());
        request["cursor"] = page["page"]["next"].clone();
        if request["cursor"].is_null() {
            break;
        }
    }
    let expected = json!({"type":"path","elements":[{"type":"vertex","partition":7,"id":"1"},{"type":"edge","partition":7,"id":"10"},{"type":"vertex","partition":8,"id":"2"}]});
    assert_eq!(rows, vec![expected.clone(), expected]);
    let renewal = json!({"job":job,"expected_generation":"0"});
    let grant = ok(service.address, "renew", renewal.clone()).await?;
    assert_eq!(grant["type"], "job_renewed");
    let replay = jobs
        .renew(pb::JobRenewRequest {
            context: context(),
            job: Some(typed_job.clone()),
            expected_generation: 0,
        })
        .await?
        .into_inner();
    assert_eq!(replay.generation, 1);
    assert_eq!(grant["grant"]["generation"], "1");
    assert!(replay.remaining_millis <= grant["grant"]["remaining_millis"].as_u64().unwrap());
    let replay_http = ok(service.address, "renew", renewal).await?;
    assert_eq!(replay_http["grant"]["generation"], "1");
    assert!(replay_http["grant"]["remaining_millis"].as_u64().unwrap() <= replay.remaining_millis);
    let mut conflict = start.clone();
    conflict["read"] = json!({"type":"latest"});
    error(
        service.address,
        "start",
        conflict.clone(),
        409,
        "identity_mismatch",
    )
    .await?;
    assert_reason(
        &jobs.start(start_proto(&conflict)).await.unwrap_err(),
        Reason::IdentityMismatch,
    );
    let mut wrong_query = fetch.clone();
    wrong_query["query"]["text"] = json!("g.V(1).count()");
    error(
        service.address,
        "fetch",
        wrong_query,
        409,
        "identity_mismatch",
    )
    .await?;
    error(
        service.address,
        "renew",
        json!({"job":job,"expected_generation":u64::MAX.to_string()}),
        409,
        "identity_mismatch",
    )
    .await?;
    assert_reason(
        &jobs
            .renew(pb::JobRenewRequest {
                context: context(),
                job: Some(typed_job.clone()),
                expected_generation: u64::MAX,
            })
            .await
            .unwrap_err(),
        Reason::IdentityMismatch,
    );
    for incarnation in [true, false] {
        let mut bad = typed_job.clone();
        if incarnation {
            bad.incarnation[0] ^= 1;
        } else {
            bad.token[0] ^= 1;
        }
        error(service.address, "get", json!({"job":{"incarnation":hex_bytes(&bad.incarnation),"token":hex_bytes(&bad.token)}}), 410, "session_unavailable").await?;
        assert_reason(
            &jobs
                .get(pb::JobGetRequest {
                    context: context(),
                    job: Some(bad),
                })
                .await
                .unwrap_err(),
            Reason::SessionUnavailable,
        );
    }
    // Structural decoding rejects an empty protobuf cursor before the handler,
    // so this transport error has no domain ErrorDetail.
    let mut bad_cursor = fetch.clone();
    bad_cursor["cursor"] = json!("");
    error(
        service.address,
        "fetch",
        bad_cursor,
        400,
        "invalid_argument",
    )
    .await?;
    let malformed = jobs
        .fetch(pb::JobFetchRequest {
            context: context(),
            job: Some(typed_job.clone()),
            query: start_proto(&start).query,
            cursor: Some(pb::Cursor { token: vec![] }),
            page_size: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
    assert!(malformed.details().is_empty());
    // A correctly sized but unknown cursor reaches the shared domain handler.
    let next_token = decode_hex(&first["page"]["next"]);
    let mut unknown_token = vec![1; 32];
    if unknown_token == next_token {
        unknown_token[0] = 2;
    }
    assert_ne!(unknown_token, next_token);
    let mut unknown_cursor = fetch.clone();
    unknown_cursor["cursor"] = json!(hex_bytes(&unknown_token));
    error(
        service.address,
        "fetch",
        unknown_cursor,
        409,
        "identity_mismatch",
    )
    .await?;
    assert_reason(
        &jobs
            .fetch(pb::JobFetchRequest {
                context: context(),
                job: Some(typed_job.clone()),
                query: start_proto(&start).query,
                cursor: Some(pb::Cursor {
                    token: unknown_token,
                }),
                page_size: 1,
            })
            .await
            .unwrap_err(),
        Reason::IdentityMismatch,
    );
    let mut invalid = start.clone();
    invalid["page_size"] = json!(0);
    error(service.address, "start", invalid, 400, "invalid_argument").await?;
    let mut cancelled = jobs
        .cancel(pb::JobCancelRequest {
            context: context(),
            job: Some(typed_job.clone()),
        })
        .await?
        .into_inner();
    assert_eq!(
        cancelled.status.take().unwrap().phase,
        pb::JobPhase::Cancelled as i32
    );
    let cancel = ok(service.address, "cancel", json!({"job":job})).await?;
    assert_eq!(cancel["status"]["phase"], "cancelled");
    assert_eq!(
        ok(service.address, "cancel", json!({"job":job})).await?,
        cancel
    );
    assert_eq!(ok(service.address, "start", start.clone()).await?, admitted);
    error(
        service.address,
        "fetch",
        fetch.clone(),
        410,
        "session_unavailable",
    )
    .await?;
    assert_reason(
        &jobs
            .fetch(pb::JobFetchRequest {
                context: context(),
                job: Some(typed_job.clone()),
                query: start_proto(&start).query,
                cursor: None,
                page_size: 1,
            })
            .await
            .unwrap_err(),
        Reason::SessionUnavailable,
    );
    // Exact zero is not latest; admission works in both transport directions.
    for (id, selected, expected_version) in [(41, Some(0), 0), (42, None, later)] {
        let body = start_body(id, selected);
        let typed = jobs
            .start(start_proto(&body))
            .await?
            .into_inner()
            .job
            .unwrap();
        let http = ok(service.address, "start", body).await?;
        assert_eq!(handle(&http["job"]), typed);
        let status = ready_http(service.address, &http["job"]).await?;
        assert_eq!(status["version"], expected_version.to_string());
        assert_eq!(status["total"], 0);
    }
    // Get is observational: frequent HTTP polling must not extend idle expiry.
    let mut short = start_body(43, Some(later));
    short["policy"] = json!({"idle_millis":1000,"execution_millis":5000,"lifetime_millis":6000});
    let short_job = ok(service.address, "start", short).await?["job"].clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = ok(service.address, "get", json!({"job":short_job})).await?;
            if response["status"]["phase"] == "expired" {
                break;
            }
            anyhow::ensure!(
                matches!(
                    response["status"]["phase"].as_str(),
                    Some("capturing" | "running" | "sealing" | "ready")
                ),
                "unexpected status {response}"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    error(
        service.address,
        "renew",
        json!({"job":short_job,"expected_generation":"0"}),
        410,
        "session_unavailable",
    )
    .await?;
    service.graceful_stop().await?;
    drop((jobs, writes));
    let mut restarted =
        ServiceProcess::start(&config_path, directory.path().join("http-jobs2.stdout")).await?;
    error(
        restarted.address,
        "get",
        json!({"job":job}),
        410,
        "session_unavailable",
    )
    .await?;
    let mut jobs = Jobs::new(grpc_channel(restarted.grpc).await?);
    assert_reason(
        &jobs
            .get(pb::JobGetRequest {
                context: context(),
                job: Some(typed_job),
            })
            .await
            .unwrap_err(),
        Reason::SessionUnavailable,
    );
    let fresh = ok(restarted.address, "start", start).await?;
    assert_ne!(fresh["job"]["incarnation"], job["incarnation"]);
    assert_eq!(
        ready_http(restarted.address, &fresh["job"]).await?["version"],
        version.to_string()
    );
    lost_start_ack(restarted.address, &mut jobs, version).await?;
    restarted.graceful_stop().await?;
    Ok(())
}

// Test-only response gate. It forwards the real HTTP request upstream and
// captures a complete decoded business response, but sends no response bytes
// back to the original caller. A TCP connect/header alone is not admission proof.
async fn lost_start_ack(address: SocketAddr, jobs: &mut Jobs, version: u64) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let proxy_address = listener.local_addr()?;
    let (captured, mut responses) = tokio::sync::mpsc::unbounded_channel();
    let proxy = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let handler =
            hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let captured = captured.clone();
                async move {
                    let path = request.uri().path().to_string();
                    let bytes = to_bytes(Body::new(request.into_body()), 1024 * 1024).await?;
                    let body: Value = serde_json::from_slice(&bytes)?;
                    let response = post(address, &path, body).await?;
                    captured
                        .send(response)
                        .map_err(|_| anyhow!("response gate receiver closed"))?;
                    std::future::pending::<anyhow::Result<hyper::Response<Body>>>().await
                }
            });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(hyper_util::rt::TokioIo::new(stream), handler)
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    let start = start_body(90, Some(version));
    let request = start.clone();
    let pending =
        tokio::spawn(async move { post(proxy_address, "/preview1/jobs:start", request).await });
    let captured = tokio::time::timeout(BUDGET, responses.recv()).await;
    // Always stop the client and proxy, including failures, so no process test
    // leaves a detached pending response future running.
    pending.abort();
    proxy.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let _ = proxy.await;
    let (code, response) = captured?.context("gate closed before complete Start response")?;
    assert_eq!(code, 200, "{response}");
    let admitted = &response["result"];
    assert_eq!(admitted["type"], "job_started");
    assert_eq!(admitted["operation_id"], start["operation_id"]);
    let job = handle(&admitted["job"]);
    // The original client received no ACK. Replay must recover the same retained
    // handle through either API, not start another task or change its fixed V.
    assert_eq!(ok(address, "start", start.clone()).await?, *admitted);
    let typed = jobs.start(start_proto(&start)).await?.into_inner();
    assert_eq!(typed.job.as_ref(), Some(&job));
    assert_eq!(typed.operation_id, decode_hex(&start["operation_id"]));
    assert_eq!(
        ready_http(address, &admitted["job"]).await?["version"],
        version.to_string()
    );
    Ok(())
}
