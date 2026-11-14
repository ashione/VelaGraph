//! Go generated-client interoperability against the existing independent-process
//! managed fixture. This target is opt-in locally, mandatory in go-interop CI.
// The shared fixture also contains HTTP/Rust-client helpers used by its other target.
#![allow(dead_code)]
include!("support/public_service_fixture.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VELAGRAPH_GO_INTEROP_BIN; mandatory in the Go interoperability job"]
async fn go_client_against_real_managed_service() -> anyhow::Result<()> {
    let binary = std::env::var_os("VELAGRAPH_GO_INTEROP_BIN")
        .context("VELAGRAPH_GO_INTEROP_BIN is required, never skip this acceptance")?;
    let binary = fs::canonicalize(binary).context("Go interoperability binary must exist")?;
    let (coordinator, a, b) = tokio::try_join!(
        NodeProcess::start(9, true),
        NodeProcess::start(7, true),
        NodeProcess::start(8, true)
    )?;
    let directory = tempfile::tempdir()?;
    let config = json!({
        "binding": binding(),
        "coordinator": {"endpoint": coordinator.ready.control, "identity": identity(9)},
        "members": [
            {"endpoint": a.ready.control, "identity": identity(7), "graph_endpoint": a.ready.graph},
            {"endpoint": b.ready.control, "identity": identity(8), "graph_endpoint": b.ready.graph}
        ]
    });
    let config_path = directory.path().join("service.json");
    fs::write(&config_path, serde_json::to_vec(&config)?)?;
    let mut service =
        ServiceProcess::start(&config_path, directory.path().join("service.stdout")).await?;
    let expected_binding = binding();
    anyhow::ensure!(
        expected_binding.cohort.profile.is_legacy(),
        "fixture capacity profile changed"
    );
    let binding_message = pb::Binding {
        authority_shard: expected_binding.authority.shard,
        authority_mode: expected_binding.authority.mode.to_vec(),
        coordinator_shard: expected_binding.cohort.coordinator_shard,
        coordinator_epoch: expected_binding.cohort.coordinator_epoch,
        members: expected_binding
            .cohort
            .members
            .iter()
            .map(|member| pb::BindingMember {
                shard: member.shard,
                epoch: member.epoch,
                mode: member.mode.to_vec(),
            })
            .collect(),
        capacity_profile: pb::CapacityProfile::LegacyCountV1 as i32,
        mapping: pb::Mapping::PartitionEqualsShardV1 as i32,
    };
    let binding_path = directory.path().join("binding.pb");
    fs::write(&binding_path, binding_message.encode_to_vec())?;
    let stdout_path = directory.path().join("go.stdout");
    let stderr_path = directory.path().join("go.stderr");
    let mut client = Command::new(binary)
        .args([
            "-endpoint",
            &service.grpc.to_string(),
            "-binding-proto",
            binding_path.to_str().context("binding path")?,
            "-partition",
            "7",
            "-first-id",
            "900001",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::from(fs::File::create(&stdout_path)?))
        .stderr(Stdio::from(fs::File::create(&stderr_path)?))
        .spawn()
        .context("start the real Go generated-client executable")?;
    let pids = [
        std::process::id(),
        client.id(),
        service.child.id(),
        coordinator.child.id(),
        a.child.id(),
        b.child.id(),
    ];
    let distinct = pids
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if distinct != 6 {
        let _ = client.kill();
        let _ = client.wait();
        anyhow::bail!("Go client, Rust harness, service and nodes must be six distinct processes");
    }
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Some(status) = client.try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    // Always reap the Go process before its service/nodes are dropped.
    let status = match result {
        Ok(Ok(status)) => status,
        error => {
            let _ = client.kill();
            let _ = client.wait();
            eprintln!("Go stdout:\n{}", fs::read_to_string(&stdout_path)?);
            eprintln!("Go stderr:\n{}", fs::read_to_string(&stderr_path)?);
            anyhow::bail!("Go interoperability deadline/process failure: {error:?}");
        }
    };
    println!("Go stdout:\n{}", fs::read_to_string(&stdout_path)?);
    eprintln!("Go stderr:\n{}", fs::read_to_string(&stderr_path)?);
    anyhow::ensure!(status.success(), "Go interoperability failed: {status}");
    service.graceful_stop().await?;
    Ok(())
}
