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
