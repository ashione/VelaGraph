use super::*;

fn vertex(partition: u32, id: u64) -> ObservedPathElement {
    ObservedPathElement::Vertex(VertexRef { partition, id })
}
fn edge(partition: u32, id: u64) -> ObservedPathElement {
    ObservedPathElement::Edge(EdgeRef { partition, id })
}
fn row(path: Vec<ObservedPathElement>, objects: u128) -> ElementRow {
    let element = match path.last().unwrap() {
        ObservedPathElement::Vertex(v) => ObservedElementRef::Vertex(*v),
        ObservedPathElement::Edge(e) => ObservedElementRef::Edge(*e),
    };
    ElementRow {
        element,
        path: ElementPath::Retained(path),
        objects,
        value: None,
    }
}
fn fresh_work() -> Work {
    Work {
        visits: 0,
        bytes: 0,
    }
}

#[test]
fn path_filters_preflight_empty_streams_before_any_reads() {
    for text in [
        "g.V(999).simplePath().outE().cyclicPath().inV().path()",
        "g.E(999).simplePath().bothV().cyclicPath().count()",
        "g.V().repeat(out()).times(32).simplePath().valueMap('x')",
        "g.E(999).inV().repeat(out()).times(32).emit().cyclicPath().path()",
        "g.V(999).simplePath().id().is(1).dedup().range(0,0).count()",
    ] {
        validate_steps(&rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap()).unwrap();
    }
    for text in [
        "g.V(999).id().simplePath()",
        "g.E(999).values('missing').cyclicPath()",
        "g.V(999).label().range(0,0).simplePath().count()",
        "g.V(999).simplePath().inV()",
        "g.E(999).cyclicPath().out()",
        "g.V(999).simplePath().repeat(out()).times(32).out()",
    ] {
        assert!(
            validate_steps(&rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap()).is_err(),
            "{text}"
        );
    }
}

#[test]
fn path_filters_compare_typed_qualified_actual_objects_not_implicit_edges() {
    let cases = [
        // The traverser saw A, B, e, not the implicit edge in out().
        (
            vec![vertex(7, 1), edge(7, 1), vertex(8, 1), edge(7, 1)],
            0b1101,
            true,
        ),
        // In outE().inV().inE(), both e entries are actual objects.
        (
            vec![vertex(7, 1), edge(7, 1), vertex(8, 1), edge(7, 1)],
            0b1111,
            false,
        ),
        (vec![vertex(7, 1), edge(7, 1), vertex(8, 1)], 0b111, true),
        (vec![edge(7, 1), vertex(7, 1), edge(8, 1)], 0b111, true),
        (vec![vertex(7, 1), edge(7, 2), vertex(7, 1)], 0b101, false),
        (vec![vertex(7, 1)], 1, true),
    ];
    for (path, objects, expected_simple) in cases {
        for simple in [false, true] {
            let rows = vec![row(path.clone(), objects), row(path.clone(), objects)];
            let mut work = fresh_work();
            let kept = filter_paths(rows, simple, &mut work, || Ok(())).unwrap();
            assert_eq!(kept.len(), if simple == expected_simple { 2 } else { 0 });
            for kept in kept {
                assert_eq!(kept.path.retained().unwrap(), path);
                assert_eq!(kept.objects, objects);
                assert!(kept.value.is_none());
            }
        }
    }
}

#[test]
fn path_filters_match_independent_set_oracle_and_preserve_bags() {
    let mut seed = 3u64;
    for length in 1..=66 {
        let mut path = Vec::new();
        let mut objects = 0u128;
        let mut identities = std::collections::BTreeSet::new();
        let mut simple = true;
        for index in 0..length {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = (
                (seed >> 32) % 2,
                ((seed >> 33) % 3) as u32,
                (seed >> 35) % 23,
            );
            path.push(if key.0 == 0 {
                vertex(key.1, key.2)
            } else {
                edge(key.1, key.2)
            });
            if index == 0 || index + 1 == length || (seed >> 44) % 2 == 1 {
                objects |= 1u128 << index;
                simple &= identities.insert(key);
            }
        }
        let mut work = fresh_work();
        assert_eq!(
            path_is_simple(&row(path, objects), &mut work, &mut || Ok(())).unwrap(),
            simple
        );
    }
    // The admitted maximum provenance shape, with all identities distinct.
    let path = (0..66).map(|id| vertex(7, id)).collect::<Vec<_>>();
    let mut work = fresh_work();
    assert!(path_is_simple(&row(path, (1u128 << 66) - 1), &mut work, &mut || Ok(())).unwrap());
    assert_eq!(work.visits, 66 + 66 * 65 / 2);
    assert_eq!(work.bytes, 0);
}

#[test]
fn path_filters_charge_rejected_comparisons_retention_and_cancellation() {
    assert!(std::mem::size_of::<ElementRow>() <= 128);
    let cyclic = || row(vec![vertex(7, 1), edge(7, 1), vertex(7, 1)], 0b101);
    for simple in [false, true] {
        let mut work = fresh_work();
        let kept = filter_paths(vec![cyclic()], simple, &mut work, || Ok(())).unwrap();
        assert_eq!(work.visits, 4); // input + two objects + one equality
        assert_eq!(kept.len(), usize::from(!simple));
        assert_eq!(
            work.bytes,
            if simple {
                0
            } else {
                128 + 3 * std::mem::size_of::<ObservedPathElement>()
            }
        );
        let mut work = Work {
            visits: 9_997,
            bytes: 0,
        };
        assert!(filter_paths(vec![cyclic()], simple, &mut work, || Ok(())).is_err());
        assert_eq!(work.visits, 10_001);
        assert_eq!(work.bytes, 0);
    }
    let mut work = Work {
        visits: 0,
        bytes: 64 * 1024 * 1024 - 1,
    };
    assert!(filter_paths(vec![cyclic()], false, &mut work, || Ok(())).is_err());
    assert_eq!(work.visits, 4);
    for fail_after in 0..4 {
        let mut checks = 0;
        let mut work = fresh_work();
        assert!(filter_paths(vec![cyclic()], true, &mut work, || {
            checks += 1;
            if checks > fail_after {
                Err(limit("expired"))
            } else {
                Ok(())
            }
        })
        .is_err());
        assert_eq!(work.visits, fail_after);
    }
    assert_eq!(mark_object(1, 65).unwrap(), 1 | (1u128 << 65));
    assert_eq!(mark_object(0, 127).unwrap(), 1u128 << 127);
    assert!(mark_object(0, 128).is_err());
    assert!(mark_object(0, usize::MAX).is_err());
    let mut work = fresh_work();
    assert!(path_is_simple(&row(vec![vertex(7, 1); 129], 1), &mut work, &mut || Ok(())).is_err());
}
