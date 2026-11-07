//! Exact tagged-JSON byte admission; row count alone is not a wire-size bound.
use super::*;
use std::io::{self, Write};

fn encoded_bytes(fragment: &Fragment) -> Result<usize, QueryFailure> {
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > query::MAX_QUERY_TEXT_BYTES.saturating_sub(self.0) {
                return Err(io::Error::other("hierarchy fragment byte limit"));
            }
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, fragment).map_err(|_| QueryFailure::ResourceExhausted)?;
    Ok(counter.0)
}

pub(super) fn pack(
    partition: u32,
    shard: u64,
    group: Vec<(usize, Fragment)>,
    cost: &mut Cost,
    ceiling: Cost,
    end: Instant,
) -> Result<Vec<parallel::Batch>, QueryFailure> {
    check(end)?;
    let empty = encoded_bytes(&Fragment::HierarchyBatch {
        partition,
        fragments: Vec::new(),
    })?;
    cost.charge(empty, 1, ceiling)?;
    let mut pending = Vec::new();
    let mut indices = Vec::new();
    let mut fragments = Vec::new();
    let mut bytes = empty;
    for (index, fragment) in group {
        check(end)?;
        // Count the actual representation, including escapes and integer width,
        // without allocating an encoded copy or repeatedly cloning growing batches.
        let leaf = encoded_bytes(&fragment)?;
        cost.charge(leaf, 1, ceiling)?;
        if leaf > query::MAX_QUERY_TEXT_BYTES.saturating_sub(empty) {
            return Err(QueryFailure::ResourceExhausted);
        }
        let comma = usize::from(!fragments.is_empty());
        if fragments.len() == BATCH_ROWS
            || leaf + comma > query::MAX_QUERY_TEXT_BYTES.saturating_sub(bytes)
        {
            pending.push(parallel::Batch {
                shard,
                indices: std::mem::take(&mut indices),
                fragment: Fragment::HierarchyBatch {
                    partition,
                    fragments: std::mem::take(&mut fragments),
                },
            });
            bytes = empty;
        }
        bytes += leaf + usize::from(!fragments.is_empty());
        indices.push(index);
        fragments.push(fragment);
    }
    if !fragments.is_empty() {
        pending.push(parallel::Batch {
            shard,
            indices,
            fragment: Fragment::HierarchyBatch {
                partition,
                fragments,
            },
        });
    }
    check(end)?;
    Ok(pending)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reverse(i: u64) -> Fragment {
        Fragment::VerifyReverse(EdgeHop {
            id: u64::MAX - i,
            from: VertexId {
                partition: u32::MAX - 1,
                id: u64::MAX - i,
            },
            to: VertexId {
                partition: u32::MAX,
                id: u64::MAX - 1 - i,
            },
            label: "x".repeat(64),
        })
    }
    fn verify(group: Vec<(usize, Fragment)>) -> Vec<parallel::Batch> {
        let expected: Vec<_> = group
            .iter()
            .map(|(index, f)| (*index, serde_json::to_value(f).unwrap()))
            .collect();
        let mut cost = Cost::default();
        let batches = pack(
            u32::MAX,
            7,
            group,
            &mut cost,
            JOB_CEILING,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let mut actual = Vec::new();
        for b in &batches {
            assert_eq!(b.shard, 7);
            assert!(serde_json::to_vec(&b.fragment).unwrap().len() <= query::MAX_QUERY_TEXT_BYTES);
            b.fragment.validate().unwrap();
            let Fragment::HierarchyBatch { fragments, .. } = &b.fragment else {
                unreachable!()
            };
            assert!(!fragments.is_empty() && fragments.len() <= BATCH_ROWS);
            assert_eq!(b.indices.len(), fragments.len());
            actual.extend(
                b.indices
                    .iter()
                    .zip(fragments)
                    .map(|(i, f)| (*i, serde_json::to_value(f).unwrap())),
            );
        }
        assert_eq!(
            actual, expected,
            "batching must preserve every original index and leaf"
        );
        for pair in batches.windows(2) {
            let Fragment::HierarchyBatch {
                partition,
                fragments,
            } = &pair[0].fragment
            else {
                unreachable!()
            };
            let Fragment::HierarchyBatch {
                fragments: next, ..
            } = &pair[1].fragment
            else {
                unreachable!()
            };
            if fragments.len() < BATCH_ROWS {
                let mut grown = fragments.clone();
                grown.push(next[0].clone());
                assert!(
                    serde_json::to_vec(&Fragment::HierarchyBatch {
                        partition: *partition,
                        fragments: grown
                    })
                    .unwrap()
                    .len()
                        > query::MAX_QUERY_TEXT_BYTES
                );
            }
        }
        assert!(cost.bytes > 0 && cost.work > 0);
        batches
    }
    #[test]
    fn exact_encoded_bytes_split_wide_reverse_and_incoming_leaves() {
        let reverse: Vec<_> = (0..128).map(reverse).collect();
        let old = Fragment::HierarchyBatch {
            partition: u32::MAX,
            fragments: reverse.clone(),
        };
        assert!(serde_json::to_vec(&old).unwrap().len() > query::MAX_QUERY_TEXT_BYTES);
        assert!(matches!(
            old.validate(),
            Err(RemoteError::ResourceExhausted)
        ));
        assert!(
            verify(
                reverse
                    .into_iter()
                    .enumerate()
                    .map(|(i, f)| (i * 3, f))
                    .collect()
            )
            .len()
                > 1
        );
        let incoming = (0..256)
            .map(|i| {
                (
                    i as usize * 2,
                    Fragment::ResolveIncoming {
                        edge: IncomingEdge {
                            partition: u32::MAX,
                            id: u64::MAX - i,
                            label: "x".repeat(64),
                        },
                        target: VertexId {
                            partition: u32::MAX - 1,
                            id: u64::MAX - i,
                        },
                    },
                )
            })
            .collect();
        assert!(verify(incoming).len() > 1);
    }
    #[test]
    fn byte_sizes_cover_utf8_json_escapes_and_row_limit_without_schema_rules() {
        let keys = (0usize..8)
            .map(|i| {
                format!(
                    "{i}{}",
                    if i.is_multiple_of(2) {
                        "界".repeat(20)
                    } else {
                        "\"\\\n".repeat(20)
                    }
                )
            })
            .collect::<Vec<_>>();
        projection::validate_keys(&keys).unwrap();
        let group = (0..128)
            .map(|i| {
                (
                    i,
                    Fragment::Project {
                        vertex: VertexId {
                            partition: u32::MAX,
                            id: u64::MAX - i as u64,
                        },
                        keys: keys.clone(),
                    },
                )
            })
            .collect();
        assert!(verify(group).len() > 1);
        let small = verify(
            (0..257)
                .map(|i| {
                    (
                        i,
                        Fragment::Exists(VertexId {
                            partition: u32::MAX,
                            id: i as u64,
                        }),
                    )
                })
                .collect(),
        );
        assert_eq!(
            small.iter().map(|b| b.indices.len()).collect::<Vec<_>>(),
            [128, 128, 1]
        );
    }
    #[test]
    fn single_oversized_leaf_budget_and_expiry_fail_without_a_partial_batch() {
        let mut cost = Cost::default();
        let too_big = Fragment::Project {
            vertex: VertexId {
                partition: u32::MAX,
                id: 1,
            },
            keys: vec!["x".repeat(query::MAX_QUERY_TEXT_BYTES)],
        };
        assert!(matches!(
            pack(
                u32::MAX,
                7,
                vec![(0, reverse(0)), (1, too_big)],
                &mut cost,
                JOB_CEILING,
                Instant::now() + Duration::from_secs(1)
            ),
            Err(QueryFailure::ResourceExhausted)
        ));
        assert!(matches!(
            pack(
                u32::MAX,
                7,
                vec![(0, reverse(0))],
                &mut Cost::default(),
                Cost { bytes: 1, work: 1 },
                Instant::now() + Duration::from_secs(1)
            ),
            Err(QueryFailure::ResourceExhausted)
        ));
        assert!(matches!(
            pack(
                u32::MAX,
                7,
                vec![],
                &mut Cost::default(),
                JOB_CEILING,
                Instant::now() - Duration::from_millis(1)
            ),
            Err(QueryFailure::Timeout)
        ));
    }
    #[test]
    fn exact_byte_boundary_and_measurement_cost_are_not_rounded() {
        let empty = encoded_bytes(&Fragment::HierarchyBatch {
            partition: u32::MAX,
            fragments: vec![],
        })
        .unwrap();
        let fixed_leaf = encoded_bytes(&reverse(0)).unwrap() - 64;
        // Four legal ASCII labels provide a contiguous range of byte lengths;
        // find an exactly full batch without relying on one hard-coded encoding.
        let (full, sum) = (0..=BATCH_ROWS - 4)
            .find_map(|full| {
                let n = full + 4;
                let fixed = empty + n * fixed_leaf + (n - 1) + full * 64;
                query::MAX_QUERY_TEXT_BYTES
                    .checked_sub(fixed)
                    .filter(|sum| (4..=256).contains(sum))
                    .map(|sum| (full, sum))
            })
            .expect("legal labels can fill the exact boundary");
        let mut sizes = vec![64; full];
        let mut left = sum - 4;
        for _ in 0..4 {
            let extra = left.min(63);
            sizes.push(1 + extra);
            left -= extra;
        }
        assert_eq!(left, 0);
        let leaves: Vec<_> = sizes
            .into_iter()
            .enumerate()
            .map(|(i, n)| {
                let Fragment::VerifyReverse(mut edge) = reverse(i as u64) else {
                    unreachable!()
                };
                edge.label = "x".repeat(n);
                Fragment::VerifyReverse(edge)
            })
            .collect();
        let exact = Fragment::HierarchyBatch {
            partition: u32::MAX,
            fragments: leaves.clone(),
        };
        assert_eq!(
            serde_json::to_vec(&exact).unwrap().len(),
            query::MAX_QUERY_TEXT_BYTES
        );
        exact.validate().unwrap();
        let extra = Fragment::Exists(VertexId {
            partition: u32::MAX,
            id: 0,
        });
        let expected_bytes = empty
            + leaves
                .iter()
                .map(|f| encoded_bytes(f).unwrap())
                .sum::<usize>()
            + encoded_bytes(&extra).unwrap();
        let expected_work = leaves.len() + 2;
        let mut group: Vec<_> = leaves.into_iter().enumerate().collect();
        let n = group.len();
        group.push((n, extra));
        let mut cost = Cost::default();
        let batches = pack(
            u32::MAX,
            7,
            group,
            &mut cost,
            JOB_CEILING,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            serde_json::to_vec(&batches[0].fragment).unwrap().len(),
            query::MAX_QUERY_TEXT_BYTES
        );
        assert_eq!(batches[1].indices, vec![n]);
        assert_eq!((cost.bytes, cost.work), (expected_bytes, expected_work));
    }
}
