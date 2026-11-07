//! Bounded vertex scans over the same fixed sessions as explicit-ID traversals.
use super::*;

pub(super) const SCAN_CEILING: Cost = Cost {
    bytes: 256 * 1024 * 1024,
    work: 1_000_000,
};
pub(super) const PAGE_ROWS: usize = 128;
pub(super) const PAGE_BYTES: usize = 1024 * 1024 + 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum ScanStep {
    Has { key: String, value: Option<Value> },
    HasLabel(Vec<String>),
    Limit(usize),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScanRequest {
    pub partition: u32,
    pub after: Option<u64>,
    pub steps: Vec<ScanStep>,
    pub count: bool,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScanResult {
    pub next_after: Option<u64>,
    pub scanned: usize,
    pub matched: usize,
    pub ids: Vec<u64>,
    pub limits: Vec<usize>,
}
pub(super) fn is_global(query: &GremlinQuery) -> bool {
    matches!(query.start(), GremlinStart::Vertices(ids) if ids.is_empty())
}
// The first adjacency or repeat step separates scan filters from the bounded
// traversal. A preceding limit belongs to the scan, never to the final output.
pub(super) fn join_at(query: &GremlinQuery) -> Result<Option<usize>, QueryFailure> {
    if !is_global(query) {
        return Ok(None);
    }
    let Some(at) = query.steps().iter().position(|step| {
        matches!(
            step,
            Step::Existing(GremlinStep::Out(_))
                | Step::Existing(GremlinStep::In(_))
                | Step::Repeat { .. }
        )
    }) else {
        return Ok(None);
    };
    hierarchy::validate_depth(query)?;
    for step in &query.steps()[..at] {
        if !matches!(
            step,
            Step::Existing(GremlinStep::Has { .. })
                | Step::Existing(GremlinStep::HasLabel(_))
                | Step::Existing(GremlinStep::Limit(_))
        ) {
            return Err(QueryFailure::Unsupported);
        }
    }
    for (index, step) in query.steps().iter().enumerate().skip(at) {
        match step {
            Step::Existing(GremlinStep::Out(_))
            | Step::Existing(GremlinStep::In(_))
            | Step::Existing(GremlinStep::Has { .. })
            | Step::Existing(GremlinStep::HasLabel(_))
            | Step::Existing(GremlinStep::Limit(_))
            | Step::Dedup
            | Step::Repeat { .. } => {}
            Step::Existing(GremlinStep::Count) if index + 1 == query.steps().len() => {}
            Step::ValueMap(keys) if index + 1 == query.steps().len() => {
                projection::validate_keys(keys)?
            }
            _ => return Err(QueryFailure::Unsupported),
        }
    }
    Ok(Some(at))
}
fn filter_steps(steps: &[Step]) -> Result<Vec<ScanStep>, QueryFailure> {
    steps
        .iter()
        .map(|step| match step {
            Step::Existing(GremlinStep::Has { key, value }) => Ok(ScanStep::Has {
                key: key.clone(),
                value: value.clone(),
            }),
            Step::Existing(GremlinStep::HasLabel(labels)) => Ok(ScanStep::HasLabel(labels.clone())),
            Step::Existing(GremlinStep::Limit(n)) => Ok(ScanStep::Limit(*n)),
            _ => Err(QueryFailure::Unsupported),
        })
        .collect()
}
pub(super) fn steps(query: &GremlinQuery) -> Result<Vec<ScanStep>, QueryFailure> {
    let mut result = Vec::new();
    let mut has_limit = false;
    let mut count = false;
    for step in query.steps() {
        match step {
            Step::Existing(GremlinStep::Has { key, value }) => result.push(ScanStep::Has {
                key: key.clone(),
                value: value.clone(),
            }),
            Step::Existing(GremlinStep::HasLabel(labels)) => {
                result.push(ScanStep::HasLabel(labels.clone()))
            }
            Step::Existing(GremlinStep::Limit(n)) => {
                has_limit = true;
                result.push(ScanStep::Limit(*n));
            }
            Step::Existing(GremlinStep::Count) => count = true,
            _ => return Err(QueryFailure::Unsupported),
        }
    }
    if !count && !has_limit {
        result.push(ScanStep::Limit(100));
    }
    Ok(result)
}
impl ScanRequest {
    pub(super) fn validate(&self) -> Result<(), RemoteError> {
        if self.steps.len() > 32
            || self
                .steps
                .iter()
                .any(|s| matches!(s, ScanStep::Limit(n) if *n > 1000))
        {
            return Err(RemoteError::InvalidArgument);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| RemoteError::InvalidArgument)?;
        if bytes.len() > query::MAX_QUERY_TEXT_BYTES {
            return Err(RemoteError::ResourceExhausted);
        }
        Ok(())
    }
    pub(super) fn limits(&self) -> Vec<usize> {
        self.steps
            .iter()
            .filter_map(|s| match s {
                ScanStep::Limit(n) => Some(*n),
                _ => None,
            })
            .collect()
    }
    pub(super) fn stopped(&self) -> bool {
        self.steps.iter().any(|s| matches!(s, ScanStep::Limit(0)))
    }
}
impl Execution<'_> {
    pub(super) async fn scan_rows(
        &mut self,
        query: &GremlinQuery,
        join: Option<usize>,
    ) -> Result<Vec<QueryRow>, QueryFailure> {
        let count = join.is_none()
            && matches!(
                query.steps().last(),
                Some(Step::Existing(GremlinStep::Count))
            );
        let mut request = ScanRequest {
            partition: 0,
            after: None,
            steps: if let Some(at) = join {
                filter_steps(&query.steps()[..at])?
            } else {
                steps(query)?
            },
            count,
        };
        let mut partitions: Vec<_> = self
            .catalog
            .partitions
            .iter()
            .map(|p| p.partition)
            .collect();
        partitions.sort_unstable();
        let mut rows = Vec::new();
        let mut total = 0u64;
        for partition in partitions {
            request.partition = partition;
            request.after = None;
            while !request.stopped() {
                check(self.end)?;
                let FragmentValue::Scan(page) =
                    self.fragment(Fragment::Scan(request.clone())).await?
                else {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                };
                let before = request.limits();
                if page.scanned > PAGE_ROWS
                    || page.matched > page.scanned
                    || page.limits.len() != before.len()
                    || page.limits.iter().zip(&before).any(|(after, before)| {
                        after > before
                            || before - after > page.scanned
                            || before - after < page.matched
                    })
                    || page
                        .next_after
                        .is_some_and(|n| request.after.is_some_and(|a| n <= a))
                    || (page.next_after.is_some() && page.scanned == 0)
                    || (count && !page.ids.is_empty())
                    || (!count && page.ids.len() != page.matched)
                    || page.ids.windows(2).any(|w| w[0] >= w[1])
                    || page.ids.iter().any(|id| {
                        request.after.is_some_and(|a| *id <= a)
                            || page.next_after.is_some_and(|n| *id > n)
                    })
                {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                }
                let mut limits = page.limits.into_iter();
                for step in &mut request.steps {
                    if let ScanStep::Limit(n) = step {
                        *n = limits.next().unwrap();
                    }
                }
                total = total
                    .checked_add(page.matched as u64)
                    .ok_or(QueryFailure::ResourceExhausted)?;
                if total > SCAN_CEILING.work as u64 {
                    return Err(QueryFailure::ResourceExhausted);
                }
                for id in page.ids {
                    if rows.len() >= if join.is_some() { MAX_ROWS } else { 1000 } {
                        return Err(QueryFailure::ResourceExhausted);
                    }
                    self.scan_cost.charge(128, 1, SCAN_CEILING)?;
                    rows.push(QueryRow::Element(VertexId { partition, id }.element()));
                }
                request.after = page.next_after;
                if request.after.is_none() {
                    break;
                }
            }
            if request.stopped() {
                break;
            }
        }
        if count {
            rows.push(QueryRow::Count(total));
        }
        Ok(rows)
    }
    pub(super) async fn run_scan(&mut self, query: &GremlinQuery) -> DistributedResponse {
        let rows = self.scan_rows(query, None).await?;
        let output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: self
                .sessions
                .values()
                .map(|(_, d)| d.stamp.clone())
                .collect(),
            rows,
        };
        let bytes = bounded_encode(&output, self.end)?;
        self.scan_cost
            .charge(bytes.len().saturating_mul(2), 0, SCAN_CEILING)?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn joined_plan_preserves_prefix_and_has_no_implicit_limit() {
        for text in [
            "g.V().out().count()",
            "g.V().out().repeat(__.in()).times(2)",
            "g.V().has('x').repeat(__.out()).times(32).emit().count()",
            "g.V().has('x').limit(3).in('r').dedup().valueMap('other')",
            "g.V().limit(2).has('x').out().limit(1).in().count()",
        ] {
            let q = GremlinQuery::parse(text).unwrap();
            let at = join_at(&q).unwrap().unwrap();
            let prefix = filter_steps(&q.steps()[..at]).unwrap();
            assert_eq!(prefix.len(), at);
            assert!(plan(&DistributedRequest {
                start_partition: 0,
                text: text.into(),
                timeout_millis: 5000
            })
            .is_ok());
        }
        let q = GremlinQuery::parse("g.V().out()").unwrap();
        assert!(filter_steps(&q.steps()[..join_at(&q).unwrap().unwrap()])
            .unwrap()
            .is_empty());
        for text in [
            "g.V().dedup().out()",
            "g.V().out().values('x')",
            "g.V().out().path()",
        ] {
            let q = GremlinQuery::parse(text).unwrap();
            assert!(join_at(&q).is_err(), "{text}");
        }
    }
    #[test]
    fn joined_results_are_bags_not_sorted_global_scans() {
        let request = DistributedRequest {
            start_partition: 0,
            text: "g.V().has('a').out().valueMap('name')".into(),
            timeout_millis: 5000,
        };
        let source = VertexId {
            partition: 2,
            id: 9,
        }
        .element();
        let row = QueryRow::PropertyMap {
            source,
            properties: BTreeMap::from([("name".into(), vec![Value::Null])]),
        };
        let mut output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![QueryStamp {
                identity: RemoteIdentity { group: 1, node: 1 },
                shard: 1,
                epoch: 1,
                sequence: 1,
            }],
            rows: vec![row.clone(); 101],
        };
        assert!(validate_output(&output, &request).is_ok());
        output.rows = vec![QueryRow::Count(MAX_ROWS as u64 + 1)];
        let mut count = request;
        count.text = "g.V().out().count()".into();
        assert!(validate_output(&output, &count).is_err());
    }
    #[test]
    fn global_plan_is_narrow_and_limits_keep_their_positions() {
        for text in [
            "g.V()",
            "g.V().count()",
            "g.V().limit(9).has('x').limit(3).count()",
        ] {
            let q = GremlinQuery::parse(text).unwrap();
            assert!(is_global(&q));
            assert!(steps(&q).is_ok());
        }
        for text in [
            "g.V().out()",
            "g.V().in()",
            "g.V().dedup()",
            "g.V().values('x')",
            "g.V().path()",
        ] {
            assert!(matches!(
                steps(&GremlinQuery::parse(text).unwrap()),
                Err(QueryFailure::Unsupported)
            ));
        }
        let q = GremlinQuery::parse("g.V().limit(9).has('x').limit(3).count()").unwrap();
        let request = ScanRequest {
            partition: 0,
            after: None,
            steps: steps(&q).unwrap(),
            count: true,
        };
        assert_eq!(request.limits(), vec![9, 3]);
        assert!(!request.stopped());
        assert_eq!(
            steps(&GremlinQuery::parse("g.V()").unwrap()).unwrap().len(),
            1
        );
        assert!(steps(&GremlinQuery::parse("g.V().count()").unwrap())
            .unwrap()
            .is_empty());
    }
    #[test]
    fn scan_output_is_qualified_ordered_and_count_can_exceed_traversal_cap() {
        let req = |text: &str| DistributedRequest {
            start_partition: 1,
            text: text.into(),
            timeout_millis: 5000,
        };
        let mut result = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![QueryStamp {
                identity: RemoteIdentity { group: 1, node: 1 },
                shard: 1,
                epoch: 1,
                sequence: 1,
            }],
            rows: vec![QueryRow::Count(205000)],
        };
        assert!(validate_output(&result, &req("g.V().count()")).is_ok());
        assert!(validate_output(&result, &req("g.V(1).count()")).is_err());
        result.rows = vec![QueryRow::Count(1_000_001)];
        assert!(validate_output(&result, &req("g.V().count()")).is_err());
        result.rows = vec![
            QueryRow::Element(
                VertexId {
                    partition: 1,
                    id: 9,
                }
                .element(),
            ),
            QueryRow::Element(
                VertexId {
                    partition: 2,
                    id: 1,
                }
                .element(),
            ),
        ];
        assert!(validate_output(&result, &req("g.V()")).is_ok());
        result.rows.reverse();
        assert!(validate_output(&result, &req("g.V()")).is_err());
        result.rows = vec![result.rows[0].clone(); 2];
        assert!(validate_output(&result, &req("g.V()")).is_err());
    }
}
