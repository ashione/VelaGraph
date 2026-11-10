//! Bounded directional adjacency over the original retained shard sessions.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Direction {
    Out,
    In,
    Both,
}
impl Direction {
    pub(super) fn from_step(step: &Step) -> Option<Self> {
        match step {
            Step::Existing(GremlinStep::Out(_)) => Some(Self::Out),
            Step::Existing(GremlinStep::In(_)) => Some(Self::In),
            Step::Existing(GremlinStep::Both(_)) => Some(Self::Both),
            _ => None,
        }
    }
    pub(super) fn from_incoming(incoming: bool) -> Self {
        if incoming {
            Self::In
        } else {
            Self::Out
        }
    }
    pub(super) fn admits_owner(self, owner: u32, source: u32, target: u32) -> bool {
        match self {
            Self::Out => owner == source,
            Self::In => owner == target,
            Self::Both => owner == source || owner == target,
        }
    }
}
fn edge_key(edge: &EdgeHop) -> (u64, u32) {
    (edge.id, edge.from.partition)
}
type EdgeIter = std::iter::Peekable<std::vec::IntoIter<EdgeHop>>;
fn next_hop(
    outgoing: &mut EdgeIter,
    incoming: &mut EdgeIter,
) -> Result<Option<(EdgeHop, bool)>, QueryFailure> {
    match (outgoing.peek(), incoming.peek()) {
        (Some(out), Some(input)) if edge_key(out) == edge_key(input) => {
            // A canonical identity can occur in both directions only for the
            // same exact self-loop. Never coalesce inconsistent fragment data.
            if out != input || out.from != out.to {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            incoming.next();
            Ok(outgoing.next().map(|edge| (edge, false)))
        }
        (Some(out), Some(input)) if edge_key(out) > edge_key(input) => {
            Ok(incoming.next().map(|edge| (edge, true)))
        }
        (Some(_), _) => Ok(outgoing.next().map(|edge| (edge, false))),
        (None, _) => Ok(incoming.next().map(|edge| (edge, true))),
    }
}

impl Execution<'_> {
    async fn outgoing(&mut self, source: VertexId) -> Result<Vec<EdgeHop>, QueryFailure> {
        let FragmentValue::Edges(edges) = self.fragment(Fragment::Out(source)).await? else {
            return Err(QueryFailure::IncompleteOrCorrupt);
        };
        if edges.len() > 1000 || edges.windows(2).any(|w| edge_key(&w[0]) >= edge_key(&w[1])) {
            return Err(QueryFailure::IncompleteOrCorrupt);
        }
        for edge in &edges {
            if edge.from != source || !self.boolean(Fragment::VerifyReverse(edge.clone())).await? {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
        }
        Ok(edges)
    }

    pub(super) async fn expand(
        &mut self,
        rows: Vec<Traverser>,
        direction: Direction,
        label: Option<&str>,
    ) -> Result<Vec<Traverser>, QueryFailure> {
        let mut next = Vec::new();
        for row in rows {
            // Validate both complete directions before selecting rows. A later
            // label/limit must not hide a corrupt half or replenish its budget.
            let outgoing = if direction != Direction::In {
                self.outgoing(row.vertex).await?
            } else {
                Vec::new()
            };
            let incoming = if direction != Direction::Out {
                self.incoming(
                    row.vertex,
                    if direction == Direction::Both {
                        None
                    } else {
                        label
                    },
                )
                .await?
            } else {
                Vec::new()
            };
            let mut outgoing = outgoing.into_iter().peekable();
            let mut incoming = incoming.into_iter().peekable();
            // Each iteration consumes at least one of two <=1,000-entry lists.
            // Merge in place: no cloned/concatenated adjacency or identity set.
            while let Some((edge, reverse)) = next_hop(&mut outgoing, &mut incoming)? {
                check(self.end)?;
                if direction == Direction::Both {
                    self.cost.charge(0, 1, self.ceiling)?;
                }
                if label.is_some_and(|l| l != edge.label) {
                    continue;
                }
                self.cost.charge(row.path.len() * 32, 0, self.ceiling)?;
                let mut path = row.path.clone();
                path.push(QualifiedElement::Edge {
                    partition: edge.from.partition,
                    id: edge.id,
                });
                let vertex = if reverse { edge.from } else { edge.to };
                path.push(vertex.element());
                push(
                    &mut next,
                    Traverser {
                        vertex,
                        path,
                        value: None,
                    },
                    &mut self.cost,
                )?;
            }
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(id: u64, from: u32, to: u32) -> EdgeHop {
        EdgeHop {
            id,
            from: VertexId {
                partition: from,
                id: 1,
            },
            to: VertexId {
                partition: to,
                id: 1,
            },
            label: "edge".into(),
        }
    }

    #[test]
    fn ordered_merge_preserves_owner_ties_and_only_coalesces_exact_self_loops() {
        let mut outgoing = vec![edge(3, 2, 1), edge(5, 2, 2), edge(8, 2, 3)]
            .into_iter()
            .peekable();
        let mut incoming = vec![edge(3, 1, 2), edge(5, 2, 2), edge(8, 4, 2)]
            .into_iter()
            .peekable();
        let mut result = Vec::new();
        while let Some((edge, reverse)) = next_hop(&mut outgoing, &mut incoming).unwrap() {
            result.push((edge_key(&edge), reverse));
        }
        assert_eq!(
            result,
            vec![
                ((3, 1), true),
                ((3, 2), false),
                ((5, 2), false),
                ((8, 2), false),
                ((8, 4), true)
            ]
        );
        for altered in [
            edge(5, 2, 3),
            EdgeHop {
                label: "other".into(),
                ..edge(5, 2, 2)
            },
        ] {
            let mut out = vec![edge(5, 2, 2)].into_iter().peekable();
            let mut input = vec![altered].into_iter().peekable();
            assert_eq!(
                next_hop(&mut out, &mut input),
                Err(QueryFailure::IncompleteOrCorrupt)
            );
        }
        // Equal canonical identity on two non-loop directions is not dedup.
        let mut out = vec![edge(5, 2, 3)].into_iter().peekable();
        let mut input = vec![edge(5, 2, 3)].into_iter().peekable();
        assert_eq!(
            next_hop(&mut out, &mut input),
            Err(QueryFailure::IncompleteOrCorrupt)
        );
    }

    #[test]
    fn client_checks_each_mixed_direction_without_expanding_direction_combinations() {
        let request = DistributedRequest {
            start_partition: 1,
            text: "g.V(1).both().in().both().path()".into(),
            timeout_millis: 5000,
        };
        let v = |partition| QualifiedElement::Vertex { partition, id: 1 };
        let e = |partition| QualifiedElement::Edge { partition, id: 3 };
        let mut output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![QueryStamp {
                identity: RemoteIdentity { group: 1, node: 1 },
                shard: 1,
                epoch: 1,
                sequence: 1,
            }],
            rows: Vec::new(),
        };
        for first in [1, 2] {
            for last in [3, 4] {
                let path = vec![v(1), e(first), v(2), e(3), v(3), e(last), v(4)];
                output.rows = vec![QueryRow::Path(path.clone())];
                assert!(validate_output(&output, &request).is_ok());
                for at in [1, 3, 5] {
                    let mut bad = path.clone();
                    bad[at] = e(99);
                    output.rows = vec![QueryRow::Path(bad)];
                    assert!(validate_output(&output, &request).is_err());
                }
                let mut bad = path;
                bad[3] = e(2); // The ordinary in hop still requires its source owner.
                output.rows = vec![QueryRow::Path(bad)];
                assert!(validate_output(&output, &request).is_err());
            }
        }
        let query = plan(&DistributedRequest {
            text: "g.V(1).both().both().repeat(__.in()).times(29).emit().both().path()".into(),
            ..request
        })
        .unwrap();
        let directions = hierarchy::path_directions(&query);
        assert_eq!(directions.len(), 29); // No 2^both Cartesian materialization.
        assert_eq!(directions.last().unwrap().len(), 32);
    }
}
