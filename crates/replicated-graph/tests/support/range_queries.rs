//! Small additive range phase over the existing managed fixture after recovery.
use super::*;

pub(super) async fn historical(g: &Gateway, version: u64) -> anyhow::Result<()> {
    let old = open(g, Some(version)).await?;
    // The original recovery setup also owns 7:1, 8:1, and 7:2. Global
    // source order is (local ID, partition), before the personnel fixture IDs.
    assert_eq!(
        traverse(g, &old, 7, "g.V().range(1,3)").await?,
        vec![row(8, 1), row(7, 2)]
    );
    let text = "g.E(101,101).bothV().range(1,3).values('name')";
    let expected = vec![value(8, 101, "Bob"), value(7, 101, "Alice")];
    assert_eq!(traverse(g, &old, 7, text).await?, expected);
    let first = page(g, &old, 7, text, None, 1).await?;
    assert_eq!((first.offset, first.total), (0, 2));
    assert_eq!(first.data.rows, expected[..1]);
    let cursor = first.next.clone().context("range continuation")?;
    // This final fixture phase has its own free session capacity. A real
    // commit separates cursor creation from consumption without renewing old V.
    let (_, written_version) = committed(
        call(
            g,
            Operation::Submit(Write {
                external_id: [80; 16],
                mutation: Mutation::PatchVertex {
                    vertex: Entity {
                        partition: 8,
                        id: 101,
                    },
                    patch: Patch {
                        set: BTreeMap::from([("name".into(), Value::Text("RangeUpdated".into()))]),
                        remove: BTreeSet::new(),
                    },
                },
            }),
        )
        .await?,
    );
    assert!(written_version > old.version);
    let last = page(g, &old, 7, text, Some(cursor.clone()), 1).await?;
    assert_eq!((last.offset, last.total), (1, 2));
    assert_eq!(last.data.rows, expected[1..]);
    assert!(last.next.is_none());
    assert_eq!(page(g, &old, 7, text, Some(cursor.clone()), 1).await?, last);
    assert_eq!(page(g, &old, 7, text, None, 1).await?, first);
    // The exact range/query is bound, not merely session and page size.
    assert!(page(
        g,
        &old,
        7,
        "g.E(101,101).bothV().range(1,4).values('name')",
        Some(cursor),
        1
    )
    .await
    .is_err());
    assert_eq!(
        traverse(g, &old, 7, "g.V(101).out('member').range(1,2)").await?,
        vec![row(8, 102)]
    );
    let path = QueryRow::Path(vec![
        element(7, 101),
        QualifiedElement::Edge {
            partition: 7,
            id: 102,
        },
        element(8, 101),
        QualifiedElement::Edge {
            partition: 8,
            id: 101,
        },
        element(7, 101),
    ]);
    assert_eq!(
        traverse(
            g,
            &old,
            7,
            "g.V(101).out('reports').range(1,2).out('reports').path()"
        )
        .await?,
        vec![path]
    );
    assert_eq!(
        traverse(g, &old, 7, "g.V(101,101,102).range(1,3).dedup().count()").await?,
        vec![QueryRow::Count(2)]
    );
    assert_eq!(
        traverse(g, &old, 7, "g.V(101,101,102).dedup().range(1,3).count()").await?,
        vec![QueryRow::Count(1)]
    );
    assert_eq!(
        traverse(g, &old, 7, "g.V(101,102).range(1,2).valueMap('name')").await?,
        vec![QueryRow::PropertyMap {
            source: element(7, 102),
            properties: BTreeMap::from([("name".into(), vec![Value::Text("Engineering".into())])])
        }]
    );
    for text in ["g.V(101).range(0,0)", "g.V(101).range(2,3)"] {
        assert!(traverse(g, &old, 7, text).await?.is_empty());
    }
    call(g, Operation::Close { session: old }).await?;
    let fresh = open(g, None).await?;
    assert_eq!(fresh.version, written_version);
    assert_eq!(
        traverse(g, &fresh, 7, text).await?,
        vec![value(8, 101, "RangeUpdated"), value(7, 101, "Alice")]
    );
    call(g, Operation::Close { session: fresh }).await?;
    Ok(())
}
