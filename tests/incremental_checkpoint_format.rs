use velagraph::{Edge, IncrementalGraph, IncrementalLimits, IncrementalMode, Value, Vertex};
#[test]
fn checkpoint_v1_has_a_fixed_golden_and_reopens() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("model.bin");
    let mut v = Vertex::new(1);
    v.labels = vec!["A".into()];
    v.properties.insert("score".into(), Value::Integer(7));
    let g = IncrementalGraph::from_records(
        "format-demo",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vec![v],
        vec![],
        vec![1, 2],
    )
    .unwrap();
    g.checkpoint(&p).unwrap();
    assert_eq!(
        std::fs::read(&p).unwrap(),
        include_bytes!("fixtures/incremental_checkpoint_v1.bin")
    );
    let restored = IncrementalGraph::restore(
        &p,
        "format-demo",
        IncrementalMode::Locked,
        IncrementalLimits::default(),
    )
    .unwrap();
    assert_eq!(restored.token().unwrap(), g.token().unwrap());
    assert_eq!(restored.read().unwrap().metadata(), &[1, 2]);
}
#[test]
fn checkpoints_and_raw_iteration_are_id_ordered() {
    let vertices: Vec<_> = (1..200).rev().map(Vertex::new).collect();
    let edges: Vec<_> = (2..200)
        .rev()
        .map(|id| Edge::new(id, 1, id, "LINK"))
        .collect();
    let a = IncrementalGraph::from_records(
        "same",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vertices.clone(),
        edges.clone(),
        vec![],
    )
    .unwrap();
    let b = IncrementalGraph::from_records(
        "same",
        IncrementalMode::Locked,
        IncrementalLimits::default(),
        vertices.into_iter().rev().collect(),
        edges.into_iter().rev().collect(),
        vec![],
    )
    .unwrap();
    let d = tempfile::tempdir().unwrap();
    let pa = d.path().join("a");
    let pb = d.path().join("b");
    a.checkpoint(&pa).unwrap();
    b.checkpoint(&pb).unwrap();
    assert_eq!(std::fs::read(pa).unwrap(), std::fs::read(pb).unwrap());
    assert_eq!(
        a.read()
            .unwrap()
            .vertices()
            .map(|v| v.id)
            .collect::<Vec<_>>(),
        (1..200).collect::<Vec<_>>()
    );
    assert_eq!(
        b.read().unwrap().edges().map(|e| e.id).collect::<Vec<_>>(),
        (2..200).collect::<Vec<_>>()
    );
}
#[test]
fn restore_rejects_non_files_and_other_format_versions() {
    let d = tempfile::tempdir().unwrap();
    assert!(IncrementalGraph::restore(
        d.path(),
        "format-demo",
        IncrementalMode::Cow,
        IncrementalLimits::default()
    )
    .is_err());
    let p = d.path().join("bad");
    let mut bytes = include_bytes!("fixtures/incremental_checkpoint_v1.bin").to_vec();
    bytes[7] = b'2';
    std::fs::write(&p, bytes).unwrap();
    assert!(IncrementalGraph::restore(
        &p,
        "format-demo",
        IncrementalMode::Cow,
        IncrementalLimits::default()
    )
    .is_err());
    #[cfg(unix)]
    {
        let link = d.path().join("symlink");
        std::os::unix::fs::symlink(&p, &link).unwrap();
        assert!(IncrementalGraph::restore(
            link,
            "format-demo",
            IncrementalMode::Cow,
            IncrementalLimits::default()
        )
        .is_err());
    }
}

#[test]
fn checkpoint_preserves_finite_real_bits_in_standalone_core() {
    let values = [
        51.248178375505404,
        -51.248178375505404,
        1.0000000000000002,
        f64::from_bits(1),
        f64::MAX,
        -0.0,
    ];
    let mut vertex = Vertex::new(1);
    for (i, value) in values.iter().enumerate() {
        vertex
            .properties
            .insert(format!("real_{i}"), Value::Real(*value));
    }
    let graph = IncrementalGraph::from_records(
        "real-bits",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vec![vertex],
        vec![],
        vec![],
    )
    .unwrap();
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("reals.bin");
    graph.checkpoint(&p).unwrap();
    let restored = IncrementalGraph::restore(
        &p,
        "real-bits",
        IncrementalMode::Locked,
        IncrementalLimits::default(),
    )
    .unwrap();
    let view = restored.read().unwrap();
    for (i, expected) in values.iter().enumerate() {
        let Value::Real(actual) = view.get_vertex(1).unwrap().properties[&format!("real_{i}")]
        else {
            panic!("wrong property type")
        };
        assert_eq!(actual.to_bits(), expected.to_bits(), "real_{i}");
    }
}

#[test]
fn checkpoint_rejects_older_or_forked_versions_at_same_path() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("model.bin");
    let old = IncrementalGraph::new("source", IncrementalMode::Cow, IncrementalLimits::default())
        .unwrap();
    old.checkpoint(&path).unwrap();
    let newer = IncrementalGraph::restore(
        &path,
        "source",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
    )
    .unwrap();
    newer.apply(&newer.token().unwrap(), &[], b"new").unwrap();
    newer.checkpoint(&path).unwrap();
    let expected = std::fs::read(&path).unwrap();
    assert!(matches!(
        old.checkpoint(&path),
        Err(velagraph::Error::TransactionConflict)
    ));
    old.apply(&old.token().unwrap(), &[], b"fork").unwrap();
    assert!(matches!(
        old.checkpoint(&path),
        Err(velagraph::Error::TransactionConflict)
    ));
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    newer.checkpoint(&path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), expected);
}

#[test]
fn checkpoint_protects_foreign_corrupt_and_reserved_destinations() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("model.bin");
    let graph = IncrementalGraph::new("source", IncrementalMode::Cow, IncrementalLimits::default())
        .unwrap();
    let foreign = IncrementalGraph::new(
        "different",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
    )
    .unwrap();
    foreign.checkpoint(&path).unwrap();
    let original = std::fs::read(&path).unwrap();
    assert!(matches!(
        graph.checkpoint(&path),
        Err(velagraph::Error::TransactionConflict)
    ));
    assert_eq!(std::fs::read(&path).unwrap(), original);
    std::fs::write(&path, b"corrupt").unwrap();
    assert!(graph.checkpoint(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"corrupt");
    for name in [
        ".velagraph-checkpoint.lock",
        ".VELAGRAPH-CHECKPOINT.LOCK.backup",
        ".incremental-example.tmp",
    ] {
        assert!(graph.checkpoint(d.path().join(name)).is_err());
    }
    assert!(!std::fs::read_dir(d.path()).unwrap().any(|e| e
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".incremental-")));
}

#[test]
fn checkpoint_rejects_occupied_lock_namespace() {
    let d = tempfile::tempdir().unwrap();
    let lock = d.path().join(".velagraph-checkpoint.lock");
    std::fs::write(&lock, b"existing user data").unwrap();
    let graph = IncrementalGraph::new("source", IncrementalMode::Cow, IncrementalLimits::default())
        .unwrap();
    assert!(graph.checkpoint(d.path().join("model.bin")).is_err());
    assert_eq!(std::fs::read(&lock).unwrap(), b"existing user data");
    assert!(!d.path().join("model.bin").exists());
}

#[test]
fn checkpoint_waiting_for_disk_publication_releases_locked_graph_view() {
    use std::time::{Duration, Instant};
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("model.bin");
    let lock = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(d.path().join(".velagraph-checkpoint.lock"))
        .unwrap();
    lock.lock().unwrap();
    let graph = IncrementalGraph::new(
        "locked",
        IncrementalMode::Locked,
        IncrementalLimits::default(),
    )
    .unwrap();
    let before = graph.token().unwrap();
    std::thread::scope(|scope| {
        let checkpoint = scope.spawn(|| graph.checkpoint(&path));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !std::fs::read_dir(d.path()).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".incremental-")
        }) {
            assert!(
                Instant::now() < deadline,
                "checkpoint did not finish serialization"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
        let graph = &graph;
        let update = scope.spawn(move || {
            graph.apply(&before, &[], b"new").unwrap();
            let _ = done_tx.send(());
        });
        let updated_while_disk_blocked = done_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        drop(lock);
        checkpoint.join().unwrap().unwrap();
        update.join().unwrap();
        assert!(
            updated_while_disk_blocked,
            "disk publication retained the graph read lock"
        );
    });
    assert_eq!(graph.read().unwrap().metadata(), b"new");
}

#[test]
fn independent_checkpoint_process_obeys_publication_lock() {
    use std::io::Write;
    use std::time::{Duration, Instant};
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("model.bin");
    let lock = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(d.path().join(".velagraph-checkpoint.lock"))
        .unwrap();
    lock.lock().unwrap();
    let newer = IncrementalGraph::new(
        "process",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
    )
    .unwrap();
    newer.apply(&newer.token().unwrap(), &[], b"new").unwrap();
    let source = tempfile::tempdir().unwrap();
    newer.checkpoint(source.path().join("new.bin")).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "checkpoint_process_writer",
            "--ignored",
            "--nocapture",
        ])
        .env("VELAGRAPH_CHECKPOINT_CHILD_PATH", &path)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::read_dir(d.path()).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".incremental-")
        }) {
            break;
        }
        if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not reach checkpoint publication");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    // Complete the newer publication while holding the protocol's lock.
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&std::fs::read(source.path().join("new.bin")).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    std::fs::File::open(d.path()).unwrap().sync_all().unwrap();
    drop(lock);
    assert!(child.wait().unwrap().success());
    let restored = IncrementalGraph::restore(
        &path,
        "process",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
    )
    .unwrap();
    assert_eq!(restored.token().unwrap(), newer.token().unwrap());
    assert_eq!(restored.read().unwrap().metadata(), b"new");
}

#[test]
#[ignore = "child-process entrypoint, exercised by parent checkpoint test"]
fn checkpoint_process_writer() {
    let path = std::env::var_os("VELAGRAPH_CHECKPOINT_CHILD_PATH").unwrap();
    let old = IncrementalGraph::new(
        "process",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        old.checkpoint(path),
        Err(velagraph::Error::TransactionConflict)
    ));
}
