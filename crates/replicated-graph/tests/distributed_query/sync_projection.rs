use super::*;
fn projected(id: u64) -> QueryRow {
    let mut properties = BTreeMap::from([("active".into(), vec![Value::Boolean(id != 2)])]);
    if id != 2 {
        properties.insert("name".into(), vec![Value::Text(format!("person-{id}"))]);
    }
    QueryRow::PropertyMap {
        source: vertex(1, id),
        properties,
    }
}
pub(super) async fn check(endpoint: SocketAddr) -> anyhow::Result<()> {
    assert_eq!(
        rows(
            endpoint,
            "g.V(1).out('knows').valueMap('name','active','absent')"
        )
        .await?,
        [2, 1, 3, 1].map(projected)
    );
    assert_eq!(
        rows(
            endpoint,
            "g.V(1,1).out('knows').dedup().has('active',true).limit(1).valueMap('name','active')"
        )
        .await?,
        vec![projected(1)]
    );
    assert_eq!(
        rows(endpoint, "g.V(7).out('knows').valueMap('name','active')").await?,
        vec![projected(1); 100]
    );
    assert_eq!(
        rows(
            endpoint,
            "g.V(7).out('knows').limit(101).valueMap('name','active')"
        )
        .await?,
        vec![projected(1); 101]
    );
    let expected = QueryRow::PropertyMap {
        source: vertex(0, 11),
        properties: BTreeMap::from([
            ("nullable".into(), vec![Value::Null]),
            ("unicode".into(), vec![Value::Text("姓名\n\"".into())]),
            ("signed".into(), vec![Value::Integer(i64::MIN)]),
            ("real".into(), vec![Value::Real(0.5)]),
            ("enabled".into(), vec![Value::Boolean(true)]),
        ]),
    };
    assert_eq!(
        rows(
            endpoint,
            "g.V(11,11).valueMap('nullable','unicode','signed','real','enabled','absent')"
        )
        .await?,
        vec![expected.clone(), expected]
    );
    assert_eq!(
        rows(endpoint, "g.V(11).valueMap('absent')").await?,
        vec![QueryRow::PropertyMap {
            source: vertex(0, 11),
            properties: BTreeMap::new()
        }]
    );
    assert!(rows(endpoint, "g.V(9999).valueMap('name')")
        .await?
        .is_empty());
    for text in [
        "g.V().valueMap('large')",
        "g.V(11).valueMap('large')",
        "g.V(1,11).valueMap('large')",
        "g.V(8).out('knows').out('returns').valueMap('name')",
    ] {
        let result = distributed::query(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: text.into(),
                timeout_millis: 5000,
            },
            BUDGET,
        )
        .await?;
        ensure!(
            result.is_err(),
            "projection returned partial success: {text}"
        );
    }
    for text in [
        "g.V().limit(0).valueMap('name')",
        "g.V(9999).valueMap()",
        "g.V(9999).valueMap('name','name')",
        "g.V(9999).valueMap('name').count()",
        "g.V(9999).values('name').valueMap('name')",
        "g.V().valueMap('name').count()",
    ] {
        let result = distributed::query(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: text.into(),
                timeout_millis: 5000,
            },
            BUDGET,
        )
        .await?;
        ensure!(
            result.is_err(),
            "invalid projection accepted on empty stream: {text}"
        );
    }
    Ok(())
}
