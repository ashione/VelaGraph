//! Actual process boundary: three managed node processes, one public service
//! process and this HTTP client. Not multi-host or global recovery evidence.
include!("support/public_service_fixture.rs");

async fn verify_fixed_session_after_delete(
    client: &mut pb::fixed_session_service_client::FixedSessionServiceClient<
        tonic::transport::Channel,
    >,
    session: &pb::FixedSession,
) -> anyhow::Result<()> {
    use pb::error_detail::Reason;
    let element = |partition, id, edge| pb::QualifiedElement {
        kind: Some(if edge {
            pb::qualified_element::Kind::Edge(pb::Entity { partition, id })
        } else {
            pb::qualified_element::Kind::Vertex(pb::Entity { partition, id })
        }),
    };
    let expected = pb::Row {
        kind: Some(pb::row::Kind::Path(pb::Path {
            elements: vec![
                element(7, 1, false),
                element(7, 10, true),
                element(8, 2, false),
            ],
        })),
    };
    let old = client
        .traverse(fixed_request(session, "g.V(1).out().path()"))
        .await?
        .into_inner();
    assert_eq!(old.session.as_ref(), Some(session));
    assert_eq!(old.query.unwrap().text, "g.V(1).out().path()");
    let data = old.data.unwrap();
    assert_eq!(data.rows, vec![expected.clone()]);
    assert_eq!(data.completeness, pb::Completeness::Unverified as i32);
    let read = client
        .read(fixed_request(session, "g.V(1).outE()"))
        .await?
        .into_inner();
    assert_eq!(read.session.as_ref(), Some(session));
    assert_eq!(
        read.data.unwrap().rows,
        vec![pb::Row {
            kind: Some(pb::row::Kind::Element(element(7, 10, true))),
        }]
    );
    let unsupported = client
        .traverse(fixed_request(session, "g.V(1).inV()"))
        .await
        .unwrap_err();
    assert_reason(&unsupported, Reason::QueryUnsupported);
    let mut changed = session.clone();
    changed.version += 1;
    let wrong_version = client
        .traverse(fixed_request(&changed, "g.V(1)"))
        .await
        .unwrap_err();
    assert_reason(&wrong_version, Reason::IdentityMismatch);
    changed = session.clone();
    changed.incarnation[0] ^= 1;
    let wrong_owner = client
        .read(fixed_request(&changed, "g.V(1)"))
        .await
        .unwrap_err();
    assert_reason(&wrong_owner, Reason::SessionUnavailable);
    // Duplicate starts preserve bags; replay preserves the complete envelope.
    let first_request = pb::FixedPageRequest {
        context: context(),
        session: Some(session.clone()),
        query: Some(pb::Query {
            partition: 7,
            text: "g.V(1,1).out().path()".into(),
        }),
        cursor: None,
        page_size: 1,
    };
    let first = client.page(first_request.clone()).await?.into_inner();
    let page = first.page.as_ref().unwrap();
    assert_eq!((page.offset, page.total), (0, 2));
    assert_eq!(page.rows, vec![expected.clone()]);
    assert_eq!(
        client.page(first_request.clone()).await?.into_inner(),
        first
    );
    let mut next_request = first_request.clone();
    next_request.cursor = page.next.clone();
    assert!(next_request.cursor.is_some());
    let last = client.page(next_request.clone()).await?.into_inner();
    let page = last.page.as_ref().unwrap();
    assert_eq!((page.offset, page.total), (1, 2));
    assert_eq!(page.rows, vec![expected]);
    assert_eq!(page.next, None);
    assert_eq!(client.page(next_request.clone()).await?.into_inner(), last);
    next_request.cursor.as_mut().unwrap().token[0] ^= 1;
    let bad_cursor = client.page(next_request).await.unwrap_err();
    assert_reason(&bad_cursor, Reason::IdentityMismatch);
    let mut changed_query = first_request;
    changed_query.query.as_mut().unwrap().text = "g.V(1)".into();
    let bad_query = client.page(changed_query).await.unwrap_err();
    assert_reason(&bad_query, Reason::IdentityMismatch);
    let fresh = client
        .open(pb::FixedOpenRequest {
            context: context(),
            version: Some(4),
            ttl_millis: 60_000,
        })
        .await?
        .into_inner()
        .session
        .unwrap();
    assert_eq!(fresh.version, 4);
    let current = client
        .traverse(fixed_request(&fresh, "g.V(1).out().path()"))
        .await?
        .into_inner();
    assert!(current.data.unwrap().rows.is_empty());
    let closed = client
        .close(pb::FixedCloseRequest {
            context: context(),
            session: Some(fresh.clone()),
        })
        .await?
        .into_inner();
    assert_eq!(closed.session, Some(fresh.clone()));
    let closed_read = client
        .read(fixed_request(&fresh, "g.V(1)"))
        .await
        .unwrap_err();
    assert_reason(&closed_read, Reason::SessionUnavailable);
    let zero = client
        .open(pb::FixedOpenRequest {
            context: context(),
            version: Some(0),
            ttl_millis: 60_000,
        })
        .await?
        .into_inner()
        .session
        .unwrap();
    assert_eq!(zero.version, 0);
    assert!(client
        .traverse(fixed_request(&zero, "g.V()"))
        .await?
        .into_inner()
        .data
        .unwrap()
        .rows
        .is_empty());
    client
        .close(pb::FixedCloseRequest {
            context: context(),
            session: Some(zero),
        })
        .await?;
    // An unavailable exact future V must fail, never substitute current V.
    let unavailable = client
        .open(pb::FixedOpenRequest {
            context: context(),
            version: Some(100),
            ttl_millis: 60_000,
        })
        .await
        .unwrap_err();
    assert_reason(&unavailable, Reason::RejectedOutcomeUncertain);
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_process_public_http_grpc_consistency_and_restart() -> anyhow::Result<()> {
    public_scenario().await
}
async fn public_scenario() -> anyhow::Result<()> {
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
        ServiceProcess::start(&config_path, directory.path().join("service1.stdout")).await?;
    let pids = [
        std::process::id(),
        service.child.id(),
        coordinator.child.id(),
        a.child.id(),
        b.child.id(),
    ];
    anyhow::ensure!(
        pids.into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == 5,
        "client/service/nodes must be independent processes"
    );
    for write in [vertex(1, 7, "1"), vertex(2, 8, "2")] {
        let (status, body) = post(service.address, "/preview1/writes:submit", write).await?;
        assert_eq!(status, 200, "{body}");
    }
    let edge = json!({"external_id":"03030303030303030303030303030303","mutation":{"type":"create_edge","edge":{"id":"10","from":{"partition":7,"id":"1"},"to":{"partition":8,"id":"2"},"label":"reports_to","properties":{}}}});
    let (status, written) = post(service.address, "/preview1/writes:submit", edge.clone()).await?;
    assert_eq!(status, 200, "{written}");
    let (status, replayed) = post(service.address, "/preview1/writes:submit", edge.clone()).await?;
    assert_eq!(status, 200, "{replayed}");
    assert_eq!(replayed, written);
    let channel = grpc_channel(service.grpc).await?;
    let mut grpc_writes = pb::write_service_client::WriteServiceClient::new(channel.clone());
    let mut grpc_queries = pb::query_service_client::QueryServiceClient::new(channel.clone());
    let mut fixed_queries =
        pb::fixed_session_service_client::FixedSessionServiceClient::new(channel);
    // HTTP then gRPC replay use exactly one canonical managed command/decision.
    let grpc_replay = grpc_writes.submit(edge_proto()).await?.into_inner();
    assert_eq!(grpc_replay.external_id, vec![3; 16]);
    assert_eq!(
        grpc_replay.transaction_id,
        Some(decode_hex(&written["result"]["transaction_id"]))
    );
    assert_eq!(
        grpc_replay.status,
        Some(pb::write_response::Status::Committed(
            pb::TerminalDecision {
                ordinal: 3,
                visible: true
            }
        ))
    );
    let query = json!({"partition":7,"text":"g.V(1).out().path()"});
    let(status,opened)=post(service.address,"/preview1/queries:open",json!({"read":{"type":"minimum","version":"3"},"ttl_millis":60000,"query":query,"page_size":1})).await?;
    assert_eq!(status, 200, "{opened}");
    let session = opened["result"]["session"].clone();
    assert_eq!(session["version"], "3");
    assert_eq!(
        opened["result"]["page"]["rows"],
        json!([{"type":"path","elements":[{"type":"vertex","partition":7,"id":"1"},{"type":"edge","partition":7,"id":"10"},{"type":"vertex","partition":8,"id":"2"}]}])
    );
    // FixedSession is a separate contract: capture once, then run multiple
    // bounded queries at that V. It is not the data-local query token above.
    let fixed_session = fixed_queries
        .open(pb::FixedOpenRequest {
            context: context(),
            version: None,
            ttl_millis: 60_000,
        })
        .await?
        .into_inner()
        .session
        .unwrap();
    assert_eq!(fixed_session.version, 3);
    let(status,deleted)=post(service.address,"/preview1/writes:submit",json!({"external_id":"04040404040404040404040404040404","mutation":{"type":"delete_edge","edge":{"partition":7,"id":"10"}}})).await?;
    assert_eq!(status, 200, "{deleted}");
    verify_fixed_session_after_delete(&mut fixed_queries, &fixed_session).await?;
    let fetch = json!({"session":session,"query":query,"cursor":null,"page_size":1});
    let (status, old) = post(service.address, "/preview1/queries:fetch", fetch.clone()).await?;
    assert_eq!(status, 200, "{old}");
    assert_eq!(old["result"]["page"], opened["result"]["page"]);
    let old_grpc = grpc_queries
        .fetch(pb::FetchRequest {
            context: context(),
            session: Some(session_proto(&session)),
            query: Some(grpc_query()),
            cursor: None,
            page_size: 1,
        })
        .await?
        .into_inner();
    assert_eq!(old_grpc.session, Some(session_proto(&session)));
    let old_page = old_grpc.page.unwrap();
    assert_eq!(old_page.total, 1);
    assert!(
        matches!(old_page.rows[0].kind.as_ref(),Some(pb::row::Kind::Path(path)) if path.elements.len()==3)
    );
    // gRPC-opened session is the same owner-bound state seen by HTTP fetch/close.
    let grpc_open = grpc_queries
        .open(pb::OpenRequest {
            profile: None,
            context: context(),
            read: Some(pb::ReadSelector {
                kind: Some(pb::read_selector::Kind::Minimum(4)),
            }),
            ttl_millis: 60_000,
            query: Some(grpc_query()),
            page_size: 1,
        })
        .await?
        .into_inner();
    let grpc_session = grpc_open.session.unwrap();
    assert_eq!(grpc_open.page.unwrap().total, 0);
    let cross_fetch =
        json!({"session":session_json(&grpc_session),"query":query,"cursor":null,"page_size":1});
    let (status, cross) = post(service.address, "/preview1/queries:fetch", cross_fetch).await?;
    assert_eq!(status, 200, "{cross}");
    assert_eq!(cross["result"]["page"]["total"], 0);
    assert_eq!(
        post(
            service.address,
            "/preview1/queries:close",
            json!({"session":session_json(&grpc_session)})
        )
        .await?
        .0,
        200
    );
    let closed = grpc_queries
        .fetch(pb::FetchRequest {
            context: context(),
            session: Some(grpc_session),
            query: Some(grpc_query()),
            cursor: None,
            page_size: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(
        pb::ErrorDetail::decode(closed.details())?.reason,
        pb::error_detail::Reason::SessionUnavailable as i32
    );
    let fresh = json!({"read":{"type":"minimum","version":"4"},"ttl_millis":60000,"query":query,"page_size":1});
    let (status, new) = post(service.address, "/preview1/queries:open", fresh.clone()).await?;
    assert_eq!(status, 200, "{new}");
    assert_eq!(new["result"]["page"]["total"], 0);
    // Conversely, gRPC close invalidates a session opened by HTTP.
    grpc_queries
        .close(pb::CloseRequest {
            context: context(),
            session: Some(session_proto(&new["result"]["session"])),
        })
        .await?;
    let (status, closed) = post(
        service.address,
        "/preview1/queries:fetch",
        json!({"session":new["result"]["session"],"query":query,"cursor":null,"page_size":1}),
    )
    .await?;
    assert_eq!(status, 410, "{closed}");
    drop(grpc_writes);
    drop(grpc_queries);
    drop(fixed_queries);
    service.graceful_stop().await?;
    let mut service =
        ServiceProcess::start(&config_path, directory.path().join("service2.stdout")).await?;
    let (status, stale) = post(service.address, "/preview1/queries:fetch", fetch).await?;
    assert_eq!(status, 410, "{stale}");
    let channel = grpc_channel(service.grpc).await?;
    let mut grpc_queries = pb::query_service_client::QueryServiceClient::new(channel.clone());
    let stale_rpc = grpc_queries
        .fetch(pb::FetchRequest {
            context: context(),
            session: Some(session_proto(&session)),
            query: Some(grpc_query()),
            cursor: None,
            page_size: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(
        pb::ErrorDetail::decode(stale_rpc.details())?.reason,
        pb::error_detail::Reason::SessionUnavailable as i32
    );
    let mut fixed_queries =
        pb::fixed_session_service_client::FixedSessionServiceClient::new(channel.clone());
    let stale_fixed = fixed_queries
        .traverse(fixed_request(&fixed_session, "g.V(1).out().path()"))
        .await
        .unwrap_err();
    assert_reason(&stale_fixed, pb::error_detail::Reason::SessionUnavailable);
    drop(fixed_queries);
    let mut grpc_writes = pb::write_service_client::WriteServiceClient::new(channel);
    assert_eq!(
        grpc_writes.status(edge_proto()).await?.into_inner(),
        grpc_replay
    );

    let (status, after_restart) = post(service.address, "/preview1/queries:open", fresh).await?;
    assert_eq!(status, 200, "{after_restart}");
    assert_eq!(after_restart["result"]["page"]["total"], 0);
    let (status, decision) = post(service.address, "/preview1/writes:status", edge).await?;
    assert_eq!(status, 200, "{decision}");
    assert_eq!(decision, written);
    drop(grpc_writes);
    drop(grpc_queries);
    service.graceful_stop().await?;
    // Deliberately wrong topology is rejected before publishing any listener.
    let mut invalid = config;
    invalid["binding"]["cohort"]["coordinator_epoch"] = json!(2);
    let bad_path = directory.path().join("bad.json");
    fs::write(&bad_path, serde_json::to_vec(&invalid)?)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_graph-service"));
    let failure = command
        .args([
            "--allow-insecure-development",
            "--config",
            bad_path.to_str().unwrap(),
            "--http",
            "127.0.0.1:0",
            "--timeout-ms",
            "1000",
        ])
        .output()?;
    assert!(!failure.status.success());
    assert!(failure.stdout.is_empty());
    // The HTTP listener must not be announced if the optional second bind fails.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0")?;
    let grpc_address = occupied.local_addr()?.to_string();
    let mut command = Command::new(env!("CARGO_BIN_EXE_graph-service"));
    let failure = command
        .args([
            "--allow-insecure-development",
            "--config",
            config_path.to_str().unwrap(),
            "--http",
            "127.0.0.1:0",
            "--grpc",
            &grpc_address,
            "--timeout-ms",
            "1000",
        ])
        .output()?;
    assert!(!failure.status.success());
    assert!(failure.stdout.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_never_falls_back_to_legacy_internal_endpoints() -> anyhow::Result<()> {
    let (coordinator, a, b) = tokio::try_join!(
        NodeProcess::start(9, false),
        NodeProcess::start(7, false),
        NodeProcess::start(8, false)
    )?;
    let (grpc_coordinator, grpc_a, grpc_b) = tokio::try_join!(
        NodeProcess::start(9, true),
        NodeProcess::start(7, true),
        NodeProcess::start(8, true)
    )?;
    // The shipped legacy managed listener is gone. This isolated protocol
    // rejection fixture speaks only the old frame prefix and records any
    // attempted fallback; genuine graph-side legacy coverage remains below.
    let old_control = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let old_control_addr = old_control.local_addr()?;
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = attempts.clone();
    let rejection_peer = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let (mut socket, _) = old_control.accept().await.unwrap();
            let mut first = [0; 4];
            if socket.read_exact(&mut first).await.is_err() {
                continue;
            }
            assert_eq!(
                &first, b"PRI ",
                "must not retry using the retired JSON framing"
            );
            observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = socket.write_all(&[0, 0, 0, 2, b'{', b'}']).await;
        }
    });
    let directory = tempfile::tempdir()?;
    // Independently reject legacy control, legacy graph, and both. A failure
    // on one path must not conceal fallback on the other.
    for (legacy_control, legacy_graph) in [(true, true), (true, false), (false, true)] {
        let (c, ca, cb) = if legacy_control {
            (&coordinator, &a, &b)
        } else {
            (&grpc_coordinator, &grpc_a, &grpc_b)
        };
        let (ga, gb) = if legacy_graph {
            (&a, &b)
        } else {
            (&grpc_a, &grpc_b)
        };
        let config = json!({"binding":binding(),"coordinator":{"endpoint":if legacy_control { old_control_addr } else { c.ready.control },"identity":identity(9)},"members":[{"endpoint":if legacy_control { old_control_addr } else { ca.ready.control },"identity":identity(7),"graph_endpoint":ga.ready.graph},{"endpoint":if legacy_control { old_control_addr } else { cb.ready.control },"identity":identity(8),"graph_endpoint":gb.ready.graph}]});
        let path = directory.path().join("legacy.json");
        fs::write(&path, serde_json::to_vec(&config)?)?;
        if !legacy_control {
            // Connecting a Channel only proves a transport connection, not a
            // typed graph service. The first graph operation must fail rather
            // than retrying the legacy protocol. No fake read/write probe is
            // added to startup merely to test readiness.
            let service =
                ServiceProcess::start(&path, directory.path().join("mixed.stdout")).await?;
            let (status, body) = post(service.address, "/preview1/queries:open",
                json!({"read":{"type":"latest"},"ttl_millis":1000,"query":{"partition":7,"text":"g.V(1)"},"page_size":1})).await?;
            assert_eq!(
                status, 503,
                "wrong graph transport must be unavailable: {body}"
            );
            drop(service);
            continue;
        }
        let mut command = Command::new(env!("CARGO_BIN_EXE_graph-service"));
        command.args([
            "--allow-insecure-development",
            "--config",
            path.to_str().context("path")?,
            "--http",
            "127.0.0.1:0",
            "--grpc",
            "127.0.0.1:0",
            "--timeout-ms",
            "1000",
        ]);
        let output = directory.path().join("rejected.stdout");
        let mut child = command
            .stdout(Stdio::from(fs::File::create(&output)?))
            .stderr(Stdio::inherit())
            .spawn()?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                other => {
                    let _ = child.kill();
                    let _ = child.wait();
                    anyhow::bail!("legacy service did not exit within budget: {other:?}");
                }
            }
        };
        assert!(!status.success());
        assert!(
            fs::read(&output)?.is_empty(),
            "legacy endpoints must never announce serving"
        );
    }
    assert!(attempts.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(
        !rejection_peer.is_finished(),
        "legacy protocol fixture must observe no fallback"
    );
    rejection_peer.abort();
    let _ = rejection_peer.await;
    Ok(())
}
