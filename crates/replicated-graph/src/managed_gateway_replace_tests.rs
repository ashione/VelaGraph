use super::*;

fn replacement() -> Mutation {
    let mut replacement = edge();
    replacement.id = 11;
    Mutation::ReplaceEdge {
        edge: entity(7, 10),
        replacement,
    }
}
#[test]
fn replacement_canonical_identity_strict_wire_and_bounds() {
    let original = replacement();
    assert_eq!(
        serde_json::to_string(&original).unwrap(),
        r#"{"ReplaceEdge":{"edge":{"partition":7,"id":10},"replacement":{"id":11,"from":{"partition":7,"id":1},"to":{"partition":8,"id":1},"label":"knows","properties":{}}}}"#
    );
    let bytes = canonical(&bound(), &original).unwrap();
    let decoded: Mutation =
        serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
    assert_eq!(canonical(&bound(), &decoded).unwrap(), bytes);
    for path in [
        vec!["ReplaceEdge"],
        vec!["ReplaceEdge", "edge"],
        vec!["ReplaceEdge", "replacement"],
        vec!["ReplaceEdge", "replacement", "from"],
        vec!["ReplaceEdge", "replacement", "to"],
    ] {
        let mut wire = serde_json::to_value(&original).unwrap();
        let mut field = &mut wire;
        for key in path {
            field = &mut field[key];
        }
        field["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Mutation>(wire).is_err());
    }
    let mut commands = BTreeSet::new();
    for value in [
        Value::Null,
        Value::Integer(0),
        Value::Real(0.0),
        Value::Real(-0.0),
        Value::Boolean(false),
        Value::Text("0".into()),
    ] {
        let mut candidate = replacement();
        if let Mutation::ReplaceEdge { replacement, .. } = &mut candidate {
            replacement.properties.insert("v".into(), value);
        }
        commands.insert(canonical(&bound(), &candidate).unwrap());
    }
    assert_eq!(commands.len(), 6);
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut candidate = replacement();
        if let Mutation::ReplaceEdge { replacement, .. } = &mut candidate {
            replacement
                .properties
                .insert("v".into(), Value::Real(invalid));
        }
        assert_eq!(canonical(&bound(), &candidate), Err(Error::Protocol));
    }
    let mut oversized = replacement();
    if let Mutation::ReplaceEdge { replacement, .. } = &mut oversized {
        replacement
            .properties
            .insert("v".into(), Value::Text("x".repeat(MAX_REQUEST_BYTES)));
    }
    assert_eq!(canonical(&bound(), &oversized), Err(Error::Capacity));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_tcp_invalid_candidates_remain_absent() {
    let _test = TEST_GATE.lock().await;
    let f = Fixture::new().await;
    let g = f.gateway().await;
    for (id, partition) in [(1, 7), (2, 8)] {
        committed(
            &call(&g, Operation::Submit(create(id, partition, 1)))
                .await
                .unwrap(),
        );
    }
    committed(
        &call(
            &g,
            Operation::Submit(Write {
                external_id: [3; 16],
                mutation: Mutation::CreateEdge { edge: edge() },
            }),
        )
        .await
        .unwrap(),
    );
    let original = open(&g, None).await;
    let mut candidates = Vec::new();
    candidates.push(Mutation::ReplaceEdge {
        edge: entity(7, 10),
        replacement: edge(),
    });
    candidates.push(Mutation::ReplaceEdge {
        edge: entity(7, 999),
        replacement: edge(),
    });
    let mut missing = edge();
    missing.id = 11;
    missing.to.id = 999;
    candidates.push(Mutation::ReplaceEdge {
        edge: entity(7, 10),
        replacement: missing,
    });
    for (index, mutation) in candidates.into_iter().enumerate() {
        let write = Write {
            external_id: [10 + index as u8; 16],
            mutation,
        };
        assert_eq!(
            call(&g, Operation::Submit(write.clone())).await,
            Err(Error::Rejected)
        );
        assert!(matches!(
            call(&g, Operation::Status(write)).await.unwrap(),
            Reply::Write {
                transaction_id: None,
                status: WriteStatus::Absent,
                ..
            }
        ));
    }
    assert_eq!(journal(&f).await.transaction_count(), 3);
    assert_eq!(
        read(&g, &original, query(7, "g.E(10).count()")).await,
        vec![QueryRow::Count(1)]
    );
    close(&g, original).await;
    g.shutdown().await;
    f.stop().await;
}
