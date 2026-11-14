//! Ordered, materialized hierarchy vertex results. No result is visible until
//! every requested expansion and validation has succeeded.
use super::*;
use std::collections::VecDeque;

const MAX_LIVE_ROWS: usize = 750_000;
const MAX_RESULT_ROWS: usize = 250_000;
const MAX_RESULT_PAGE_ROWS: usize = 1000;
// Reusable delivery scratch: two typed buffers and two encoded buffers, each
// allowing geometric capacity growth. Vertex JSON is at most 61 bytes, plus
// one separator. Snapshot-vector allowances are already charged by Job::open.
// This reserves one operation's workspace, not all concurrent replies or RSS.
// Read releases the job lock before encoding; the coordinator's separate
// 16-handler admission bound limits simultaneous delivery buffers. Replays,
// like count-response replays, do not increase the traversal cost ledger.
const DELIVERY_ROW_BYTES: usize = 512;
const DELIVERY_ENVELOPE_BYTES: usize = 4096;
// Covers queue/vector capacity growth and identity-set nodes conservatively;
// this is logical accounting, not a process RSS guarantee.
const ROW_BYTES: usize = 128;

pub(super) struct Rows {
    frontier: VecDeque<VertexId>,
    next: VecDeque<VertexId>,
    seen: BTreeSet<VertexId>,
    start_partition: u32,
    start_cursor: usize,
    initialized: bool,
    delivery_reserved: bool,
    depth: usize,
    pub(super) complete: bool,
    pub(super) results: Vec<VertexId>,
    pub(super) page_size: usize,
    traversal_finished: bool,
    pub(super) projection: Vec<String>,
    pub(super) projected: Vec<BTreeMap<String, Value>>,
    retained_projection_bytes: usize,
}

impl Rows {
    pub(super) fn new(plan: &Plan, partition: u32, page_size: usize) -> Result<Self, QueryFailure> {
        if !(1..=MAX_RESULT_PAGE_ROWS).contains(&page_size) || plan.ids.is_empty() {
            return Err(QueryFailure::InvalidArgument);
        }
        if !plan.projection.is_empty() {
            projection::validate_keys(&plan.projection)?;
            if page_size > projection::MAX_PAGE_ROWS {
                return Err(QueryFailure::InvalidArgument);
            }
        }
        // Starts are read from the plan only inside a charged, bounded advance.
        Ok(Self {
            frontier: VecDeque::new(),
            next: VecDeque::new(),
            seen: BTreeSet::new(),
            start_partition: partition,
            start_cursor: 0,
            initialized: false,
            delivery_reserved: false,
            depth: 0,
            complete: false,
            results: Vec::new(),
            page_size,
            traversal_finished: false,
            projection: plan.projection.clone(),
            projected: Vec::new(),
            retained_projection_bytes: 0,
        })
    }

    fn live_rows(&self) -> usize {
        self.frontier.len() + self.next.len() + self.results.len() + self.seen.len()
    }

    fn reserve(&self, additional: usize, cost: &mut Cost) -> Result<(), QueryFailure> {
        if self.live_rows().saturating_add(additional) > MAX_LIVE_ROWS {
            return Err(QueryFailure::ResourceExhausted);
        }
        cost.charge(ROW_BYTES * additional, 1, JOB_CEILING)
    }

    fn seed(&mut self, vertex: VertexId, cost: &mut Cost) -> Result<(), QueryFailure> {
        self.reserve(1, cost)?;
        self.frontier.push_back(vertex);
        Ok(())
    }

    fn accept(
        &mut self,
        plan: &Plan,
        vertex: VertexId,
        label: &str,
        cost: &mut Cost,
    ) -> Result<(), QueryFailure> {
        // The caller has validated the canonical edge, endpoints and reverse
        // observation, even when this label does not match.
        if plan
            .label
            .as_ref()
            .is_some_and(|expected| expected != label)
        {
            return Ok(());
        }
        let terminal = self.depth + 1 == plan.times;
        let output = (plan.emit || terminal) && (!plan.dedup || !self.seen.contains(&vertex));
        if output && self.results.len() >= MAX_RESULT_ROWS {
            return Err(QueryFailure::ResourceExhausted);
        }
        let additional =
            usize::from(!terminal) + usize::from(output) + usize::from(output && plan.dedup);
        self.reserve(additional, cost)?;
        // Dedup affects only the terminal result stream, never future expansion.
        if !terminal {
            self.next.push_back(vertex);
        }
        if output {
            if plan.dedup {
                self.seen.insert(vertex);
            }
            self.results.push(vertex);
        }
        Ok(())
    }

    fn finish_depth(&mut self, plan: &Plan) {
        if !self.frontier.is_empty() {
            return;
        }
        self.depth += 1;
        if self.depth == plan.times || self.next.is_empty() {
            self.traversal_finished = true;
            self.complete = self.projection.is_empty();
            self.seen.clear();
        } else {
            self.frontier = std::mem::take(&mut self.next);
        }
    }

    /// One bounded micro-step. The parent removes the job from the registry
    /// before calling this and exposes results only once `complete` is true.
    pub(super) async fn advance(
        &mut self,
        job: &mut Job,
        end: Instant,
    ) -> Result<(), QueryFailure> {
        let end = end.min(job.expires);
        check(end)?;
        if self.complete {
            return Ok(());
        }
        if self.traversal_finished {
            return self.project(job, end).await;
        }
        job.charge(256, 1)?;
        if !self.delivery_reserved {
            // Pay once for bounded reusable page scratch, not once per result
            // or replay. Read is immutable and performs no new graph work.
            if self.projection.is_empty() {
                job.charge(
                    DELIVERY_ENVELOPE_BYTES + self.page_size * DELIVERY_ROW_BYTES,
                    1,
                )?;
            }
            self.delivery_reserved = true;
        }
        if !self.initialized {
            let stop = (self.start_cursor + ADVANCE_ROWS).min(job.plan.ids.len());
            job.charge(
                (stop - self.start_cursor) * ROW_BYTES,
                stop - self.start_cursor,
            )?;
            // The original start partition is retained separately from each
            // routed adjacency target; never derive it from current frontier.
            let partition = self.start_partition;
            let starts: Vec<_> = job.plan.ids[self.start_cursor..stop]
                .iter()
                .map(|id| VertexId { partition, id: *id })
                .collect();
            let values = job
                .batch(starts.iter().copied().map(Fragment::Exists).collect(), end)
                .await?;
            if values.len() != starts.len() {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            for (vertex, value) in starts.into_iter().zip(values) {
                check(end)?;
                match value {
                    FragmentValue::Bool(true) => self.seed(vertex, &mut job.cost)?,
                    FragmentValue::Bool(false) => {}
                    _ => return Err(QueryFailure::IncompleteOrCorrupt),
                }
            }
            self.start_cursor = stop;
            self.initialized = stop == job.plan.ids.len();
            if self.initialized && self.frontier.is_empty() {
                self.traversal_finished = true;
                self.complete = self.projection.is_empty();
            }
            return check(end);
        }
        let count = self.frontier.len().min(ADVANCE_ROWS);
        job.charge(count * ROW_BYTES, count)?;
        let current: Vec<_> = self.frontier.drain(..count).collect();
        let leaves = current
            .iter()
            .map(|v| {
                if job.plan.incoming {
                    Fragment::In(*v)
                } else {
                    Fragment::Out(*v)
                }
            })
            .collect();
        let observations = job.batch(leaves, end).await?;
        if observations.len() != current.len() {
            return Err(QueryFailure::IncompleteOrCorrupt);
        }
        let mut pending = Vec::new();
        let mut incoming = Vec::new();
        let mut outgoing = Vec::new();
        for (vertex, value) in current.into_iter().zip(observations) {
            check(end)?;
            match value {
                FragmentValue::Incoming(entries) if job.plan.incoming => {
                    if entries.len() > 1000
                        || entries
                            .windows(2)
                            .any(|w| (w[0].id, w[0].partition) >= (w[1].id, w[1].partition))
                    {
                        return Err(QueryFailure::IncompleteOrCorrupt);
                    }
                    for edge in entries {
                        check(end)?;
                        if pending.len() >= MAX_PAGE_EDGES {
                            return Err(QueryFailure::ResourceExhausted);
                        }
                        job.charge(256 + edge.label.len(), 1)?;
                        incoming.push((vertex, edge.clone()));
                        pending.push(Fragment::ResolveIncoming {
                            target: vertex,
                            edge,
                        });
                    }
                }
                FragmentValue::Edges(edges) if !job.plan.incoming => {
                    if edges.len() > 1000 || edges.windows(2).any(|w| w[0].id >= w[1].id) {
                        return Err(QueryFailure::IncompleteOrCorrupt);
                    }
                    for edge in edges {
                        check(end)?;
                        if edge.from != vertex {
                            return Err(QueryFailure::IncompleteOrCorrupt);
                        }
                        if pending.len() >= MAX_PAGE_EDGES {
                            return Err(QueryFailure::ResourceExhausted);
                        }
                        job.charge(256 + edge.label.len(), 1)?;
                        pending.push(Fragment::VerifyReverse(edge.clone()));
                        outgoing.push(edge);
                    }
                }
                _ => return Err(QueryFailure::IncompleteOrCorrupt),
            }
        }
        let verified = job.batch(pending, end).await?;
        if verified.len() != incoming.len() + outgoing.len() {
            return Err(QueryFailure::IncompleteOrCorrupt);
        }
        if job.plan.incoming {
            for ((target, observed), value) in incoming.into_iter().zip(verified) {
                check(end)?;
                let FragmentValue::Edge(edge) = value else {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                };
                if edge.to != target
                    || edge.id != observed.id
                    || edge.from.partition != observed.partition
                    || edge.label != observed.label
                {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                }
                self.accept(&job.plan, edge.from, &edge.label, &mut job.cost)?;
            }
        } else {
            for (edge, value) in outgoing.into_iter().zip(verified) {
                check(end)?;
                if !matches!(value, FragmentValue::Bool(true)) {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                }
                self.accept(&job.plan, edge.to, &edge.label, &mut job.cost)?;
            }
        }
        self.finish_depth(&job.plan);
        check(end)
    }
    async fn project(&mut self, job: &mut Job, end: Instant) -> Result<(), QueryFailure> {
        check(end)?;
        if self.projection.is_empty() {
            return Err(QueryFailure::InvalidArgument);
        }
        if job.completed_traversal_cost.is_none() {
            // This is an explicit new phase with its own allowance, not a reset
            // of the original traversal or any data-session cumulative meter.
            job.completed_traversal_cost = Some(job.cost);
            job.cost = Cost::default();
            // Two typed and encoded buffers with geometric-growth allowance.
            // Per row: map <=2048 tagged bytes, <=8 nodes/keys, source metadata.
            job.charge(4096 + self.page_size * 32768, 1)?;
        }
        job.charge(256, 1)?;
        let start = self.projected.len();
        let stop = (start + projection::batch_rows(&self.projection)?).min(self.results.len());
        job.charge(
            (stop - start)
                * (128
                    + self.projection.len() * 64
                    + self.projection.iter().map(String::len).sum::<usize>() * 2),
            stop - start,
        )?;
        let fragments = self.results[start..stop]
            .iter()
            .map(|vertex| Fragment::Project {
                vertex: *vertex,
                keys: self.projection.clone(),
            })
            .collect();
        let values = job.batch(fragments, end).await?;
        if values.len() != stop - start {
            return Err(QueryFailure::IncompleteOrCorrupt);
        }
        for (expected, value) in self.results[start..stop].iter().zip(values) {
            check(end)?;
            let FragmentValue::Projected { vertex, properties } = value else {
                return Err(QueryFailure::IncompleteOrCorrupt);
            };
            if vertex != *expected || properties.keys().any(|k| !self.projection.contains(k)) {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            let size = projection::properties_bytes(&properties)?;
            let retained = self
                .retained_projection_bytes
                .checked_add(size)
                .filter(|n| *n <= projection::MAX_RETAINED_BYTES)
                .ok_or(QueryFailure::ResourceExhausted)?;
            job.charge(64 + properties.len() * 128 + size * 2, 1)?;
            self.projected.push(properties);
            self.retained_projection_bytes = retained;
        }
        if stop == self.results.len() {
            self.complete = true;
        }
        check(end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vertex(partition: u32, id: u64) -> VertexId {
        VertexId { partition, id }
    }

    fn plan(emit: bool, dedup: bool) -> Plan {
        Plan {
            ids: vec![2, 1, 2],
            incoming: false,
            label: Some("reports".into()),
            times: 3,
            emit,
            dedup,
            projection: Vec::new(),
        }
    }

    fn edges(v: VertexId) -> Vec<VertexId> {
        match v.id {
            // Parallel edges to 1, followed by a cross-partition vertex with
            // the same local ID; one later edge cycles to the original root.
            2 => vec![vertex(1, 1), vertex(1, 1), vertex(2, 1)],
            1 if v.partition == 1 => vec![vertex(1, 2)],
            _ => vec![],
        }
    }

    #[test]
    fn ordered_bags_match_independent_bfs_with_cycles_parallel_edges_and_dedup() {
        for emit in [false, true] {
            for dedup in [false, true] {
                let plan = plan(emit, dedup);
                let mut rows = Rows::new(&plan, 1, 2).unwrap();
                let mut cost = Cost::default();
                for id in &plan.ids {
                    rows.seed(vertex(1, *id), &mut cost).unwrap();
                }
                let mut expected = Vec::new();
                let mut frontier: Vec<_> = plan.ids.iter().map(|id| vertex(1, *id)).collect();
                for depth in 1..=plan.times {
                    frontier = frontier.into_iter().flat_map(edges).collect();
                    if emit || depth == plan.times {
                        expected.extend(frontier.iter().copied());
                    }
                }
                if dedup {
                    let mut seen = BTreeSet::new();
                    expected.retain(|v| seen.insert(*v));
                }
                while !rows.complete {
                    // Two parents per step exercises a mid-depth boundary.
                    for _ in 0..2 {
                        let Some(v) = rows.frontier.pop_front() else {
                            break;
                        };
                        for child in edges(v) {
                            rows.accept(&plan, child, "reports", &mut cost).unwrap();
                        }
                    }
                    rows.finish_depth(&plan);
                }
                assert_eq!(rows.results, expected, "emit={emit}, dedup={dedup}");
            }
        }
    }

    #[test]
    fn terminal_dedup_never_suppresses_reexpansion_or_partition_identity() {
        let plan = plan(true, true);
        let mut rows = Rows::new(&plan, 1, 128).unwrap();
        let mut cost = Cost::default();
        rows.accept(&plan, vertex(1, 2), "reports", &mut cost)
            .unwrap();
        rows.accept(&plan, vertex(1, 2), "reports", &mut cost)
            .unwrap();
        rows.accept(&plan, vertex(2, 2), "reports", &mut cost)
            .unwrap();
        assert_eq!(rows.next.len(), 3);
        assert_eq!(rows.results, vec![vertex(1, 2), vertex(2, 2)]);
        rows.finish_depth(&plan);
        assert_eq!(rows.frontier.len(), 3);
        assert!(!rows.complete);
    }

    #[test]
    fn dead_ends_and_label_filter_preserve_exact_and_emit_semantics() {
        for emit in [false, true] {
            let plan = plan(emit, false);
            let mut rows = Rows::new(&plan, 1, 1).unwrap();
            let mut cost = Cost::default();
            rows.accept(&plan, vertex(1, 99), "unrelated", &mut cost)
                .unwrap();
            assert!(rows.next.is_empty());
            rows.accept(&plan, vertex(1, 1), "reports", &mut cost)
                .unwrap();
            rows.finish_depth(&plan);
            rows.frontier.clear(); // The next expansion has no matching edges.
            rows.finish_depth(&plan);
            assert!(rows.complete);
            assert_eq!(rows.results, if emit { vec![vertex(1, 1)] } else { vec![] });
        }
    }

    #[test]
    fn admissions_and_cost_fail_before_mutating_result_state() {
        let plan = plan(true, false);
        assert!(matches!(
            Rows::new(&plan, 1, 0),
            Err(QueryFailure::InvalidArgument)
        ));
        assert!(matches!(
            Rows::new(&plan, 1, 1001),
            Err(QueryFailure::InvalidArgument)
        ));
        let mut rows = Rows::new(&plan, 1, 128).unwrap();
        let mut cost = JOB_CEILING;
        assert_eq!(
            rows.accept(&plan, vertex(1, 1), "reports", &mut cost),
            Err(QueryFailure::ResourceExhausted)
        );
        assert!(rows.results.is_empty());
        assert!(rows.next.is_empty());
        assert!(rows.seen.is_empty());
        rows.results = vec![vertex(1, 1); MAX_RESULT_ROWS];
        let mut cost = Cost::default();
        assert_eq!(
            rows.accept(&plan, vertex(1, 2), "reports", &mut cost),
            Err(QueryFailure::ResourceExhausted)
        );
        assert_eq!(cost.bytes, 0);
        assert_eq!(cost.work, 0);
        assert!(rows.next.is_empty());
    }

    #[test]
    fn live_frontier_budget_is_separate_from_the_output_budget() {
        let plan = plan(false, false);
        let mut rows = Rows::new(&plan, 1, 128).unwrap();
        rows.frontier = std::iter::repeat_n(vertex(1, 1), MAX_LIVE_ROWS).collect();
        let mut cost = Cost::default();
        assert_eq!(
            rows.accept(&plan, vertex(1, 2), "reports", &mut cost),
            Err(QueryFailure::ResourceExhausted)
        );
        assert!(rows.next.is_empty());
        assert!(rows.results.is_empty());
        assert_eq!(cost.bytes, 0);
        assert_eq!(cost.work, 0);
    }

    #[test]
    fn maximum_width_vertex_delivery_fits_reserved_page_scratch() {
        let row = QualifiedElement::Vertex {
            partition: u32::MAX,
            id: u64::MAX,
        };
        let encoded = serde_json::to_vec(&row).unwrap();
        assert_eq!(encoded.len(), 61);
        // Two copies of each buffer with a factor of two for capacity growth.
        let typed_and_encoded =
            4 * std::mem::size_of::<QualifiedElement>() + 4 * (encoded.len() + 1);
        assert!(typed_and_encoded <= DELIVERY_ROW_BYTES);
        assert!(2 * std::mem::size_of::<VertexId>() <= ROW_BYTES);
        let page = vec![row; MAX_RESULT_PAGE_ROWS];
        let encoded_page = serde_json::to_vec(&page).unwrap();
        assert_eq!(encoded_page.len(), 62 * MAX_RESULT_PAGE_ROWS + 1);
        assert!(encoded_page.len() < MAX_WIRE);
        let plan = plan(false, false);
        assert!(Rows::new(&plan, 1, MAX_RESULT_PAGE_ROWS).is_ok());
        assert_eq!(
            DELIVERY_ENVELOPE_BYTES + MAX_RESULT_PAGE_ROWS * DELIVERY_ROW_BYTES,
            516_096
        );
    }
}

#[cfg(test)]
mod projection_phase_tests {
    use super::*;
    async fn fixture() -> (Rows, Job) {
        let plan = parse_mode("g.V(1).repeat(__.out()).times(1).valueMap('name')", true).unwrap();
        let mut rows = Rows::new(&plan, 0, 128).unwrap();
        rows.traversal_finished = true;
        let job = Job {
            grpc_clients: None,
            catalog: Arc::new(StaticCatalog { partitions: vec![] }),
            sessions: BTreeMap::new(),
            row_bounds: BTreeMap::new(),
            rows: None,
            expires: Instant::now() + Duration::from_secs(5),
            cost: Cost {
                bytes: JOB_CEILING.bytes - 1,
                work: JOB_CEILING.work - 1,
            },
            completed_traversal_cost: None,
            plan,
            compact: None,
            frontier: BTreeMap::new(),
            next: BTreeMap::new(),
            seen: BTreeSet::new(),
            depth: 0,
            total: 0,
            next_mass: 0,
            initialized: true,
            sequence: 0,
            last: None,
            completed: false,
            _permit: None,
            _retained: RETAINED_SLOTS.acquire().await.unwrap(),
        };
        (rows, job)
    }
    #[tokio::test]
    async fn projection_has_explicit_separate_allowance_and_cannot_reset_exhausted_stage() {
        let (mut rows, mut job) = fixture().await;
        let old = job.cost;
        rows.project(&mut job, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(rows.complete);
        let saved = job.completed_traversal_cost.unwrap();
        assert_eq!((saved.bytes, saved.work), (old.bytes, old.work));
        assert!(job.cost.bytes < projection::STAGE_CEILING.bytes);
        assert!(
            saved.bytes + job.cost.bytes <= JOB_CEILING.bytes + projection::STAGE_CEILING.bytes
        );
        rows.complete = false;
        job.cost = projection::STAGE_CEILING;
        assert_eq!(
            rows.project(&mut job, Instant::now() + Duration::from_secs(1))
                .await,
            Err(QueryFailure::ResourceExhausted)
        );
        assert!(!rows.complete);
        assert_eq!(job.completed_traversal_cost.unwrap().bytes, old.bytes);
    }
    #[tokio::test]
    async fn expired_projection_does_not_start_or_renew_phase() {
        let (mut rows, mut job) = fixture().await;
        let old = job.cost;
        assert_eq!(
            rows.project(&mut job, Instant::now() - Duration::from_millis(1))
                .await,
            Err(QueryFailure::Timeout)
        );
        assert!(job.completed_traversal_cost.is_none() && !rows.complete);
        assert_eq!((job.cost.bytes, job.cost.work), (old.bytes, old.work));
    }
    #[test]
    fn maximal_projected_rows_and_pages_fit_fixed_delivery_bounds() {
        let mut map = BTreeMap::from([("name".into(), Value::Text(String::new()))]);
        let overhead = serde_json::to_vec(&map).unwrap().len();
        map.insert(
            "name".into(),
            Value::Text("x".repeat(projection::MAX_MAP_BYTES - overhead)),
        );
        assert_eq!(
            projection::properties_bytes(&map).unwrap(),
            projection::MAX_MAP_BYTES
        );
        let row = ProjectedRow {
            source: QualifiedElement::Vertex {
                partition: u32::MAX,
                id: u64::MAX,
            },
            properties: map.into_iter().map(|(k, v)| (k, vec![v])).collect(),
        };
        assert!(serde_json::to_vec(&row).unwrap().len() <= 4096);
        let snapshots = (1..=8)
            .map(|shard| QueryStamp {
                identity: RemoteIdentity {
                    group: u64::MAX,
                    node: u64::MAX,
                },
                shard,
                epoch: u64::MAX,
                sequence: u64::MAX,
            })
            .collect();
        let page = Output::ProjectedPage {
            token: [255; 16],
            cursor: 250000,
            next_cursor: Some(250128),
            done: false,
            rows: vec![row; projection::MAX_PAGE_ROWS],
            completeness: QueryCompleteness::Unverified,
            snapshots,
        };
        assert!(
            serde_json::to_vec(&page).unwrap().len() <= 4096 + 4096 * projection::MAX_PAGE_ROWS
        );
        const { assert!(4096 + 4096 * projection::MAX_PAGE_ROWS < MAX_WIRE) };
    }
}
