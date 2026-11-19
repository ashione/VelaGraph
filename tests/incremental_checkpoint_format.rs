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
