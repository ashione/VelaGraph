//! Native fixed-depth hierarchy execution. No retries, refreshed sessions or
//! unbounded termination predicates; bags and cycles retain ordinary path identity.
use super::*;
const MAX_DEPTH: usize = rocksdb_sql_table::MAX_OBSERVED_GREMLIN_REPEAT_TIMES;

pub(super) fn validate_depth(query: &GremlinQuery) -> Result<(), QueryFailure> {
    let mut depth = 0usize;
    let mut repeats = 0;
    for step in query.steps() {
        match step {
            Step::Existing(GremlinStep::In(_) | GremlinStep::Out(_)) => depth += 1,
            Step::Repeat { times, .. } => {
                repeats += 1;
                if *times == 0 {
                    return Err(QueryFailure::Unsupported);
                }
                depth = depth
                    .checked_add(*times)
                    .ok_or(QueryFailure::ResourceExhausted)?;
            }
            _ => {}
        }
    }
    if repeats > 1 {
        return Err(QueryFailure::Unsupported);
    }
    if depth > MAX_DEPTH {
        return Err(QueryFailure::ResourceExhausted);
    }
    Ok(())
}

pub(super) fn path_directions(query: &GremlinQuery) -> Vec<Vec<bool>> {
    let mut paths = vec![Vec::new()];
    for step in query.steps() {
        match step {
            Step::Existing(GremlinStep::Out(_) | GremlinStep::In(_)) => {
                let incoming = matches!(step, Step::Existing(GremlinStep::In(_)));
                for path in &mut paths {
                    path.push(incoming);
                }
            }
            Step::Repeat {
                incoming,
                times,
                emit,
                ..
            } => {
                let mut expanded = Vec::new();
                for path in paths {
                    for depth in if *emit { 1..=*times } else { *times..=*times } {
                        let mut next = path.clone();
                        next.extend(std::iter::repeat_n(*incoming, depth));
                        expanded.push(next);
                    }
                }
                paths = expanded;
            }
            _ => {}
        }
    }
    paths
}

impl Execution<'_> {
    pub(super) async fn repeat(
        &mut self,
        mut rows: Vec<Traverser>,
        incoming: bool,
        label: Option<&str>,
        times: usize,
        emit: bool,
    ) -> Result<Vec<Traverser>, QueryFailure> {
        let mut emitted = Vec::new();
        for depth in 1..=times {
            check(self.end)?;
            self.cost.charge(0, 1, CEILING)?;
            rows = self.expand(rows, incoming, label).await?;
            if rows.is_empty() {
                break;
            }
            if emit {
                if depth == times {
                    for row in rows {
                        push(&mut emitted, row, &mut self.cost)?;
                    }
                    return Ok(emitted);
                }
                for row in &rows {
                    // Charge before cloning paths while keeping the next frontier.
                    self.cost.charge(row.path.len() * 32, 0, CEILING)?;
                    push(
                        &mut emitted,
                        Traverser {
                            vertex: row.vertex,
                            path: row.path.clone(),
                            value: None,
                        },
                        &mut self.cost,
                    )?;
                }
            }
        }
        if emit {
            Ok(emitted)
        } else {
            Ok(rows)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn req(text: &str) -> DistributedRequest {
        DistributedRequest {
            start_partition: 1,
            text: text.into(),
            timeout_millis: 5000,
        }
    }
    #[test]
    fn repetition_is_bounded_and_validated_before_any_empty_stream() {
        for text in [
            "g.V(999).repeat(__.out()).times(0)",
            "g.V(999).repeat(__.out())",
            "g.V(999).values('x').repeat(__.out()).times(2)",
            "g.E(1).repeat(__.out()).times(2)",
        ] {
            assert!(plan(&req(text)).is_err(), "{text}");
        }
        for text in [
            "g.V(999).repeat(__.out()).times(33)",
            "g.V(999).repeat(__.out()).times(32).out()",
        ] {
            assert_eq!(
                plan(&req(text)).unwrap_err(),
                QueryFailure::ResourceExhausted,
                "{text}"
            );
        }
        assert!(plan(&req(
            "g.V(999).repeat(__.in()).emit().times(32).dedup().count()"
        ))
        .is_ok());
    }
    #[test]
    fn global_repeat_validates_entire_shape_before_empty_scan() {
        for text in [
            "g.V().hasLabel('missing').repeat(__.out()).times(0)",
            "g.V().hasLabel('missing').repeat(__.out()).times(2).repeat(__.in()).times(2)",
            "g.V().hasLabel('missing').repeat(__.out()).times(2).path()",
            "g.V().hasLabel('missing').repeat(__.out()).times(2).values('x')",
        ] {
            assert_eq!(
                plan(&req(text)).unwrap_err(),
                QueryFailure::Unsupported,
                "{text}"
            );
        }
        for text in [
            "g.V().hasLabel('missing').repeat(__.out()).times(33)",
            "g.V().hasLabel('missing').out().repeat(__.in()).times(32)",
        ] {
            assert_eq!(
                plan(&req(text)).unwrap_err(),
                QueryFailure::ResourceExhausted,
                "{text}"
            );
        }
        assert!(plan(&req(
            "g.V().repeat(__.out()).times(32).emit().dedup().count()"
        ))
        .is_ok());
    }
    #[test]
    fn emitted_paths_have_each_bounded_depth_and_preserve_direction() {
        let query = plan(&req(
            "g.V(1).out().repeat(__.in()).emit().times(3).out().path()",
        ))
        .unwrap();
        assert_eq!(
            path_directions(&query),
            vec![
                vec![false, true, false],
                vec![false, true, true, false],
                vec![false, true, true, true, false]
            ]
        );
        let vertex = |p| QualifiedElement::Vertex {
            partition: p,
            id: 1,
        };
        let edge = |p| QualifiedElement::Edge {
            partition: p,
            id: 7,
        };
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
        let request = req("g.V(1).repeat(__.in()).emit().times(3).path()");
        let mut path = vec![vertex(1)];
        for p in 2..=4 {
            path.extend([edge(p), vertex(p)]);
            output.rows = vec![QueryRow::Path(path.clone())];
            assert!(validate_output(&output, &request).is_ok());
        }
        output.rows = vec![QueryRow::Path(vec![vertex(1)])];
        assert!(validate_output(&output, &request).is_err());
        path[1] = edge(99);
        output.rows = vec![QueryRow::Path(path)];
        assert!(validate_output(&output, &request).is_err());
    }
}
