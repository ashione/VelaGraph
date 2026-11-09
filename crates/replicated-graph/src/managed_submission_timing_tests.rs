use super::*;
use d2_raft::managed_rpc::{self, Identity, Role, Server};

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

async fn remote_config(f: &Fixture) -> (Vec<Server>, RemoteConfig) {
    let mut servers = Vec::new();
    let mut clients = Vec::new();
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
    }
    let coordinator = clients.remove(0);
    (
        servers,
        RemoteConfig {
            coordinator,
            binding: bound(),
            members: clients,
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_submission_timing_preserves_success_and_original_id_retry() {
    let _test = TEST_GATE.lock().await;
    let mut observed = Vec::new();
    for enabled in [false, true] {
        let f = Fixture::new().await;
        let (servers, config) = remote_config(&f).await;
        let (g, reports) = if enabled {
            let (g, reports) = Gateway::bind_remote_with_timing(
                config,
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(15),
            )
            .await
            .unwrap();
            (g, Some(reports))
        } else {
            let g = Gateway::bind_remote(
                config,
                "127.0.0.1:0".parse().unwrap(),
                Duration::from_secs(15),
            )
            .await
            .unwrap();
            assert!(g.state.timing.is_none());
            (g, None)
        };
        let old = open(&g, Some(0)).await;
        let write = create(61, 7, 1);
        let first = call(&g, Operation::Submit(write.clone())).await.unwrap();
        let (id, ordinal) = committed(&first);
        let record = journal(&f)
            .await
            .request(write.external_id)
            .unwrap()
            .clone();
        assert_eq!(record.transaction_id, id);
        assert_eq!(
            call(&g, Operation::Submit(write.clone())).await.unwrap(),
            first
        );
        assert_eq!(
            journal(&f).await.request(write.external_id).unwrap(),
            &record
        );
        assert_eq!(
            call(&g, Operation::Submit(create(61, 7, 2))).await,
            Err(Error::Identity)
        );
        let current = open(&g, Some(ordinal)).await;
        observed.push((
            ordinal,
            read(&g, &old, query(7, "g.V(1)")).await,
            read(&g, &current, query(7, "g.V(1)")).await,
            journal(&f).await.resolved_prefix(),
        ));
        if let Some(reports) = reports {
            let reports: Vec<_> = reports.try_iter().collect();
            assert_eq!(reports.len(), 3);
            assert_eq!(reports[0].completion, Completion::Returned);
            assert_eq!(reports[0].phases[Phase::Admission as usize].calls, 1);
            assert_eq!(reports[0].phases[Phase::Prepare as usize].calls, 1);
            assert_eq!(reports[0].phases[Phase::Visibility as usize].calls, 2);
            assert_eq!(reports[1].external_id, write.external_id);
            assert_eq!(reports[1].completion, Completion::Returned);
            assert_eq!(reports[1].phases[Phase::Recovery as usize].calls, 1);
            for phase in [
                Phase::Snapshot,
                Phase::Validation,
                Phase::Admission,
                Phase::Prepare,
            ] {
                assert_eq!(reports[1].phases[phase as usize].calls, 0);
            }
            assert_eq!(reports[2].completion, Completion::Error);
        }
        g.shutdown().await;
        for server in servers {
            server.shutdown().await;
        }
        f.stop().await;
    }
    assert_eq!(observed[0], observed[1]);
    assert_eq!(observed[0], (1, vec![], vec![v(7, 1)], 1));
}

// Forward the actual bounded managed frames to the real durable coordinator.
// Only the first LookupTarget is held: admission and its recovery lookup have
// already returned, but no preparation or decision command can be sent.
// The finite test request sequence ends with an explicit stop signal.
async fn hold_first_target(
    upstream: SocketAddr,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
    listener: TcpListener,
) {
    let mut gate = Some((reached, release));
    loop {
        let (mut incoming, _) = tokio::select! {
            _ = &mut stop => break,
            accepted = listener.accept() => accepted.unwrap(),
        };
        let length = incoming.read_u32().await.unwrap() as usize;
        assert!(length <= managed_rpc::MAX_FRAME_BYTES);
        let mut bytes = vec![0; length];
        incoming.read_exact(&mut bytes).await.unwrap();
        let request: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        if request["operation"].get("LookupTarget").is_some() {
            if let Some((reached, release)) = gate.take() {
                reached.send(()).unwrap();
                release.await.unwrap();
                // The test has canceled its handler; discard this held read.
                continue;
            }
        }
        let mut outgoing = TcpStream::connect(upstream).await.unwrap();
        outgoing.write_u32(length as u32).await.unwrap();
        outgoing.write_all(&bytes).await.unwrap();
        let length = outgoing.read_u32().await.unwrap() as usize;
        assert!(length <= managed_rpc::MAX_FRAME_BYTES);
        let mut bytes = vec![0; length];
        outgoing.read_exact(&mut bytes).await.unwrap();
        incoming.write_u32(length as u32).await.unwrap();
        incoming.write_all(&bytes).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submission_timing_cancellation_after_durable_admit_preserves_original_plan() {
    let _test = TEST_GATE.lock().await;
    let cluster = Cluster::new();
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let mut nodes = Vec::new();
    for (shard, dir) in [9, 7, 8].into_iter().zip(&dirs) {
        nodes.push(Arc::new(
            initialize(
                cluster
                    .in_group(shard)
                    .start_durable(1, dir.path())
                    .await
                    .unwrap(),
                shard,
            )
            .await,
        ));
    }
    let f = Fixture { cluster, nodes };
    let (servers, mut config) = remote_config(&f).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.coordinator = Client::new(
        listener.local_addr().unwrap(),
        identity(9),
        Duration::from_secs(15),
    )
    .unwrap();
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let (stop, stop_rx) = tokio::sync::oneshot::channel();
    let proxy = tokio::spawn(hold_first_target(
        servers[0].local_addr(),
        reached,
        release_rx,
        stop_rx,
        listener,
    ));
    let (g, reports) = Gateway::bind_remote_with_timing(
        config,
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(15),
    )
    .await
    .unwrap();
    let write = create(62, 7, 1);
    let state = g.state.clone();
    let submitted = write.clone();
    let handler = tokio::spawn(async move {
        let work = Arc::new(state.connections.clone().try_acquire_owned().unwrap());
        state
            .execute(
                Operation::Submit(submitted),
                work,
                Instant::now() + Duration::from_secs(15),
            )
            .await
    });
    // Event synchronization, not a sleep or a shortened production deadline.
    tokio::time::timeout(Duration::from_secs(15), reached_rx)
        .await
        .unwrap()
        .unwrap();
    let record = journal(&f)
        .await
        .request(write.external_id)
        .unwrap()
        .clone();
    let local = OwnedConfig::from(f.config());
    let original = local
        .graph()
        .recover_request(write.external_id, &record.request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.id(), record.transaction_id);
    handler.abort();
    assert!(handler.await.unwrap_err().is_cancelled());
    let report = reports.try_recv().unwrap();
    assert_eq!(report.external_id, write.external_id);
    assert_eq!(report.completion, Completion::Interrupted);
    for phase in [
        Phase::Queue,
        Phase::Canonical,
        Phase::Recovery,
        Phase::Snapshot,
        Phase::Validation,
        Phase::Admission,
        Phase::Execute,
        Phase::Decision,
    ] {
        assert_eq!(report.phases[phase as usize].calls, 1, "{phase:?}");
    }
    for phase in [
        Phase::Prepare,
        Phase::HistorySync,
        Phase::Cohort,
        Phase::Consume,
        Phase::Visibility,
    ] {
        assert_eq!(report.phases[phase as usize].calls, 0, "{phase:?}");
    }
    assert_eq!(
        journal(&f).await.request(write.external_id).unwrap(),
        &record
    );
    assert_eq!(
        local
            .graph()
            .recover_request(write.external_id, &record.request)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    release.send(()).unwrap();
    assert!(
        matches!(call(&g, Operation::Status(write.clone())).await.unwrap(),
        Reply::Write { transaction_id: Some(id), status: WriteStatus::Pending, .. } if id == original.id())
    );
    let reply = call(&g, Operation::Submit(write.clone())).await.unwrap();
    assert_eq!(committed(&reply), (original.id(), 1));
    assert_eq!(
        journal(&f).await.request(write.external_id).unwrap(),
        &record
    );
    let retry = reports.try_recv().unwrap();
    assert_eq!(retry.completion, Completion::Returned);
    assert_eq!(retry.phases[Phase::Admission as usize].calls, 0);
    assert_eq!(retry.phases[Phase::Snapshot as usize].calls, 0);
    assert_eq!(retry.phases[Phase::Visibility as usize].calls, 2);
    g.shutdown().await;
    stop.send(()).unwrap();
    proxy.await.unwrap();
    for server in servers {
        server.shutdown().await;
    }
    drop(local);
    f.stop().await;
}
