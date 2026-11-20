use super::*;
use std::collections::{BTreeSet, HashMap};
/// Private staged changes over one pinned version. Mutation errors poison
/// the transaction; commit cannot publish a partially failed batch.
pub struct IncrementalTransaction<'a> {
    graph: &'a IncrementalGraph,
    base: IncrementalReadView<'a>,
    vertices: HashMap<u64, Option<VEntry>>,
    edges: HashMap<u64, Option<Arc<Cached<Edge>>>>,
    metadata: Vec<u8>,
    stats: IncrementalCommitStats,
    deadline: Instant,
    poisoned: bool,
}
pub(super) fn begin(graph: &IncrementalGraph) -> Result<IncrementalTransaction<'_>> {
    let base = graph.read()?;
    let initial_bytes = base.metadata().len();
    if initial_bytes > graph.limits.max_batch_bytes {
        return Err(limit("initial metadata copy"));
    }
    let metadata = base.metadata().to_vec();
    Ok(IncrementalTransaction {
        graph,
        base,
        vertices: HashMap::new(),
        edges: HashMap::new(),
        metadata,
        stats: IncrementalCommitStats {
            work_bytes: initial_bytes,
            ..Default::default()
        },
        deadline: Instant::now() + BATCH_DEADLINE,
        poisoned: false,
    })
}
impl IncrementalTransaction<'_> {
    /// Token of the transaction's original read view.
    pub fn token(&self) -> IncrementalToken {
        self.base.token()
    }
    /// Read staged metadata, initially copied from the original view.
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }
    fn projected_work(&self, bytes: usize) -> Result<(usize, usize)> {
        let operations = self
            .stats
            .work_operations
            .checked_add(1)
            .ok_or_else(|| limit("work overflow"))?;
        let total = self
            .stats
            .work_bytes
            .checked_add(bytes)
            .ok_or_else(|| limit("work overflow"))?;
        if Instant::now() >= self.deadline
            || operations > self.graph.limits.max_batch_operations
            || total > self.graph.limits.max_batch_bytes
        {
            return Err(limit("batch work budget"));
        }
        Ok((operations, total))
    }
    pub(super) fn preflight_record_copy(&mut self, bytes: usize) -> Result<()> {
        let result = self.projected_work(bytes).map(|_| ());
        if result.is_err() {
            self.poisoned = true
        }
        result
    }
    fn note(&mut self, bytes: usize) -> Result<()> {
        let (operations, total) = self.projected_work(bytes)?;
        self.stats.work_operations = operations;
        self.stats.work_bytes = total;
        Ok(())
    }
    fn entry(&self, id: u64) -> Option<&VEntry> {
        match self.vertices.get(&id) {
            Some(v) => v.as_ref(),
            None => self.base.state().vertices.get(id),
        }
    }
    /// Read the transaction overlay before the pinned view.
    pub fn get_vertex(&self, id: u64) -> Option<&Vertex> {
        self.entry(id).map(|v| &v.record.value)
    }
    /// Read the transaction overlay before the pinned view.
    pub fn get_edge(&self, id: u64) -> Option<&Edge> {
        match self.edges.get(&id) {
            Some(e) => e.as_ref().map(|e| &e.value),
            None => self.base.get_edge(id),
        }
    }
    fn touch(&mut self, id: u64) -> Result<&mut VEntry> {
        if !self.vertices.contains_key(&id) {
            let v = self.entry(id).ok_or(Error::VertexNotFound(id))?.clone();
            self.note(v.bytes())?;
            self.vertices.insert(id, Some(v));
        }
        self.vertices
            .get_mut(&id)
            .and_then(Option::as_mut)
            .ok_or(Error::VertexNotFound(id))
    }
    fn change_adjacency(
        &mut self,
        vertex: u64,
        edge: u64,
        incoming: bool,
        insert: bool,
    ) -> Result<()> {
        let fee = {
            let v = self.touch(vertex)?;
            let a = if incoming { &v.incoming } else { &v.outgoing };
            a.header_bytes().saturating_mul(3) + adjacency::CHUNK * 4 * 8
        };
        self.note(fee)?;
        let v = self.touch(vertex)?;
        let a = if incoming {
            &mut v.incoming
        } else {
            &mut v.outgoing
        };
        let copied = Arc::make_mut(a).change(edge, insert);
        self.stats.copied_adjacency_ids += copied;
        Ok(())
    }
    /// Copy ordered incident records under the transaction work budget.
    pub fn adjacent(&mut self, id: u64, incoming: bool) -> Result<Vec<Edge>> {
        let result = self.adjacent_inner(id, incoming);
        if result.is_err() {
            self.poisoned = true
        }
        result
    }
    fn adjacent_inner(&mut self, id: u64, incoming: bool) -> Result<Vec<Edge>> {
        let e = self.entry(id).ok_or(Error::VertexNotFound(id))?;
        let adj = if incoming {
            e.incoming.clone()
        } else {
            e.outgoing.clone()
        };
        if adj.len()
            > self
                .graph
                .limits
                .max_batch_operations
                .saturating_sub(self.stats.work_operations)
        {
            return Err(limit("adjacency read budget"));
        };
        // Admit the complete copy before allocating its vector or any payload.
        for id in adj.ids() {
            let bytes = self
                .get_edge(id)
                .ok_or_else(|| bad("missing staged edge"))?
                .accounted_bytes();
            self.note(bytes + INDEX_FEE)?;
        }
        let mut out = Vec::with_capacity(adj.len());
        for id in adj.ids() {
            if Instant::now() >= self.deadline {
                return Err(limit("batch work budget"));
            }
            out.push(
                self.get_edge(id)
                    .ok_or_else(|| bad("missing staged edge"))?
                    .clone(),
            );
        }
        Ok(out)
    }
    /// Upsert a vertex while preserving its incident edges.
    pub fn put_vertex(&mut self, v: Vertex) -> Result<()> {
        let r = self.put_vertex_inner(v);
        if r.is_err() {
            self.poisoned = true
        }
        r
    }
    fn put_vertex_inner(&mut self, v: Vertex) -> Result<()> {
        self.note(v.accounted_bytes() + INDEX_FEE)?;
        validate_vertex(&v)?;
        let id = v.id;
        let record = Cached::new(v);
        let record = Arc::new(record);
        let entry = if let Some(old) = self.entry(id) {
            VEntry {
                record,
                incoming: old.incoming.clone(),
                outgoing: old.outgoing.clone(),
            }
        } else {
            VEntry {
                record,
                incoming: Arc::new(Adj::default()),
                outgoing: Arc::new(Adj::default()),
            }
        };
        self.vertices.insert(id, Some(entry));
        Ok(())
    }
    /// Upsert an edge and both indexes; endpoints must already exist in this view.
    pub fn put_edge(&mut self, e: Edge) -> Result<()> {
        let r = self.put_edge_inner(e);
        if r.is_err() {
            self.poisoned = true
        }
        r
    }
    fn put_edge_inner(&mut self, e: Edge) -> Result<()> {
        self.note(e.accounted_bytes() + INDEX_FEE)?;
        validate_edge(&e)?;
        if self.get_vertex(e.from).is_none() {
            return Err(Error::EdgeEndpointNotFound(e.from));
        }
        if self.get_vertex(e.to).is_none() {
            return Err(Error::EdgeEndpointNotFound(e.to));
        }
        self.delete_edge_inner(e.id)?;
        self.change_adjacency(e.from, e.id, false, true)?;
        self.change_adjacency(e.to, e.id, true, true)?;
        self.edges.insert(e.id, Some(Arc::new(Cached::new(e))));
        Ok(())
    }
    /// Delete an edge and both index entries; absence is a charged no-op.
    pub fn delete_edge(&mut self, id: u64) -> Result<()> {
        let r = self.delete_edge_inner(id);
        if r.is_err() {
            self.poisoned = true
        }
        r
    }
    fn delete_edge_inner(&mut self, id: u64) -> Result<()> {
        self.note(INDEX_FEE)?;
        if let Some((from, to)) = self.get_edge(id).map(|e| (e.from, e.to)) {
            self.change_adjacency(from, id, false, false)?;
            self.change_adjacency(to, id, true, false)?;
            self.edges.insert(id, None);
        }
        Ok(())
    }
    /// Delete a vertex and every incident edge, deduplicating self-loops.
    pub fn delete_vertex(&mut self, id: u64) -> Result<()> {
        let r = self.delete_vertex_inner(id);
        if r.is_err() {
            self.poisoned = true
        }
        r
    }
    fn delete_vertex_inner(&mut self, id: u64) -> Result<()> {
        self.note(INDEX_FEE)?;
        let Some(v) = self.entry(id).cloned() else {
            return Ok(());
        };
        if v.incoming.len() + v.outgoing.len()
            > self
                .graph
                .limits
                .max_batch_operations
                .saturating_sub(self.stats.work_operations)
        {
            return Err(limit("cascade budget"));
        };
        let ids: BTreeSet<_> = v.incoming.ids().chain(v.outgoing.ids()).collect();
        for edge in ids {
            self.delete_edge_inner(edge)?
        }
        self.vertices.insert(id, None);
        Ok(())
    }
    /// Replace opaque caller progress, at most 64 KiB, in this same transaction.
    pub fn set_metadata(&mut self, metadata: &[u8]) -> Result<()> {
        let r = (|| {
            if metadata.len() > MAX_METADATA {
                return Err(limit("metadata"));
            }
            self.note(metadata.len())?;
            self.metadata = metadata.to_vec();
            Ok(())
        })();
        if r.is_err() {
            self.poisoned = true
        }
        r
    }
    /// Publish all staged changes and metadata after validating the token.
    /// Every successful commit advances sequence, including metadata-only commits.
    /// Preparation has a cooperative deadline; waiting for a Locked reader
    /// is not preempted. Release same-thread read handles before calling this.
    pub fn commit(self) -> Result<IncrementalCommitStats> {
        self.commit_checked(&|| Ok(()))
    }
    /// Commit with caller control; cancellation after the final check may lose
    /// to atomic publication. Locked installation is never interrupted halfway.
    pub fn commit_with_control(
        self,
        control: &crate::ExecutionControl,
    ) -> Result<IncrementalCommitStats> {
        self.commit_checked(&|| control.check())
    }
    pub(super) fn commit_checked(
        mut self,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<IncrementalCommitStats> {
        check()?;
        if self.poisoned {
            return Err(limit("poisoned transaction"));
        }
        self.note(0)?;
        let token = self.token();
        let base = self.base.state();
        let mut bytes = base.bytes - base.metadata.len() + self.metadata.len();
        // A candidate starts as the full base. Changes are installed in hash
        // order, so reserve every positive delta before crediting any shrink.
        let mut preparation_bytes = base
            .bytes
            .checked_add(self.metadata.len().saturating_sub(base.metadata.len()))
            .ok_or_else(|| limit("preparation accounting"))?;
        let mut nv = base.vertex_count;
        let mut ne = base.edge_count;
        for (id, v) in &self.vertices {
            check()?;
            let old = base.vertices.get(*id);
            let old_bytes = old.map_or(0, VEntry::bytes);
            let new_bytes = v.as_ref().map_or(0, VEntry::bytes);
            preparation_bytes = preparation_bytes
                .checked_add(new_bytes.saturating_sub(old_bytes))
                .ok_or_else(|| limit("preparation accounting"))?;
            if old.is_some() {
                bytes = bytes
                    .checked_sub(old_bytes)
                    .ok_or_else(|| bad("accounting"))?;
                nv -= 1
            }
            if v.is_some() {
                bytes = bytes
                    .checked_add(new_bytes)
                    .ok_or_else(|| limit("accounting"))?;
                nv += 1
            }
        }
        for (id, e) in &self.edges {
            check()?;
            let old = base.edges.get(*id);
            let old_bytes = old.map_or(0, |e| e.bytes + INDEX_FEE);
            let new_bytes = e.as_ref().map_or(0, |e| e.bytes + INDEX_FEE);
            preparation_bytes = preparation_bytes
                .checked_add(new_bytes.saturating_sub(old_bytes))
                .ok_or_else(|| limit("preparation accounting"))?;
            if old.is_some() {
                bytes = bytes
                    .checked_sub(old_bytes)
                    .ok_or_else(|| bad("accounting"))?;
                ne -= 1
            }
            if e.is_some() {
                bytes = bytes
                    .checked_add(new_bytes)
                    .ok_or_else(|| limit("accounting"))?;
                ne += 1
            }
        }
        if nv + ne > self.graph.limits.max_records || bytes > self.graph.limits.max_accounted_bytes
        {
            return Err(limit("retained graph budget"));
        }
        let sequence = token
            .sequence
            .checked_add(1)
            .ok_or_else(|| limit("version overflow"))?;
        let shrink_work = base
            .vertices
            .mutation_entries(self.vertices.iter().map(|(id, v)| (*id, v.is_some())))
            + base
                .edges
                .mutation_entries(self.edges.iter().map(|(id, e)| (*id, e.is_some())));
        let cow_work = if self.graph.mode == IncrementalMode::Cow {
            base.vertices.copy_entries(self.vertices.keys().copied())
                + base.edges.copy_entries(self.edges.keys().copied())
        } else {
            0
        };
        self.note(
            shrink_work
                .checked_add(cow_work)
                .and_then(|n| n.checked_mul(INDEX_FEE))
                .ok_or_else(|| limit("copy accounting"))?,
        )?;

        self.stats.changed_vertices = self.vertices.len();
        self.stats.changed_edges = self.edges.len();
        self.stats.retained_accounted_bytes = bytes;
        self.stats.sequence = sequence;
        let Self {
            graph,
            base,
            vertices,
            edges,
            metadata,
            mut stats,
            deadline,
            ..
        } = self;
        if graph.mode == IncrementalMode::Cow {
            return publication::commit(
                graph,
                base,
                publication::Changes {
                    vertices,
                    edges,
                    metadata,
                    vertex_count: nv,
                    edge_count: ne,
                    preparation_bytes,
                    stats,
                },
                deadline,
                check,
            );
        }
        drop(base);
        let mut guard = graph.inner.write().map_err(|_| bad("poisoned lock"))?;
        if guard.current.sequence != token.sequence || guard.current.identity != token.identity {
            return Err(Error::TransactionConflict);
        }
        check()?;
        let s = Arc::get_mut(&mut guard.current)
            .ok_or_else(|| bad("locked state unexpectedly shared"))?;
        for (id, v) in vertices {
            stats.copied_index_entries += s.vertices.set(id, v)
        }
        for (id, e) in edges {
            stats.copied_index_entries += s.edges.set(id, e)
        }
        s.sequence = sequence;
        s.metadata = metadata;
        s.bytes = bytes;
        s.vertex_count = nv;
        s.edge_count = ne;
        Ok(stats)
    }
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn every_commit_checkpoint_is_atomic_and_late_cancel_preserves_success() {
        for mode in [IncrementalMode::Cow, IncrementalMode::Locked] {
            let make = || {
                IncrementalGraph::from_records(
                    "control",
                    mode,
                    IncrementalLimits::default(),
                    vec![Vertex::new(1), Vertex::new(3)],
                    vec![Edge::new(1, 1, 3, "link")],
                    b"old".to_vec(),
                )
                .unwrap()
            };
            fn stage(g: &IncrementalGraph) -> IncrementalTransaction<'_> {
                let mut tx = begin(g).unwrap();
                tx.delete_vertex(1).unwrap();
                tx.put_vertex(Vertex::new(2)).unwrap();
                tx.put_edge(Edge::new(2, 2, 3, "link")).unwrap();
                tx.set_metadata(b"new").unwrap();
                tx
            }
            let graph = make();
            let count = Cell::new(0);
            stage(&graph)
                .commit_checked(&|| {
                    count.set(count.get() + 1);
                    Ok(())
                })
                .unwrap();
            assert!(count.get() >= 4);
            let late_graph = make();
            let late_calls = Cell::new(0);
            let late_token = crate::CancellationToken::default();
            let late_control =
                crate::ExecutionControl::new(late_token.clone(), Duration::from_secs(30)).unwrap();
            stage(&late_graph)
                .commit_checked(&|| {
                    late_control.check()?;
                    late_calls.set(late_calls.get() + 1);
                    if late_calls.get() == count.get() {
                        late_token.cancel();
                    }
                    Ok(())
                })
                .unwrap();
            assert!(late_token.is_cancelled());
            assert_eq!(late_graph.token().unwrap().sequence, 1);

            for stop in 1..=count.get() + 1 {
                let graph = make();
                let before = graph.token().unwrap();
                let calls = Cell::new(0);
                let result = stage(&graph).commit_checked(&|| {
                    calls.set(calls.get() + 1);
                    if calls.get() == stop {
                        Err(Error::ExecutionCancelled)
                    } else {
                        Ok(())
                    }
                });
                let view = graph.read().unwrap();
                if stop <= count.get() {
                    assert!(matches!(result, Err(Error::ExecutionCancelled)));
                    assert_eq!(view.token(), before);
                    assert!(view.get_vertex(1).is_some());
                    assert!(view.get_vertex(2).is_none());
                    assert_eq!(view.metadata(), b"old");
                    assert!(view.get_edge(1).is_some());
                    assert!(view.get_edge(2).is_none());
                    assert_eq!(
                        view.execute_gremlin("g.V(3).in('link')").unwrap().rows,
                        vec![crate::GremlinRow::Vertex(1)]
                    );
                } else {
                    assert_eq!(result.unwrap().sequence, 1);
                    assert!(view.get_vertex(1).is_none());
                    assert!(view.get_vertex(2).is_some());
                    assert_eq!(view.metadata(), b"new");
                    assert!(view.get_edge(1).is_none());
                    assert!(view.get_edge(2).is_some());
                    assert_eq!(
                        view.execute_gremlin("g.V(3).in('link')").unwrap().rows,
                        vec![crate::GremlinRow::Vertex(2)]
                    );
                }
                drop(view);
                // Every rejection must release writer serialization and its source view.
                graph.apply(&graph.token().unwrap(), &[], b"next").unwrap();
            }
        }
    }

    #[test]
    fn cancellation_observed_after_cow_serialization_wait() {
        let graph =
            IncrementalGraph::new("wait", IncrementalMode::Cow, IncrementalLimits::default())
                .unwrap();
        let held = graph.cow_prepare.lock().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let token = crate::CancellationToken::default();
        let control = crate::ExecutionControl::new(token.clone(), Duration::from_secs(30)).unwrap();
        std::thread::scope(|scope| {
            let graph = &graph;
            let control = &control;
            let join = scope.spawn(move || {
                let staged = begin(graph).unwrap();
                staged.commit_checked(&|| {
                    tx.send(()).unwrap();
                    control.check()
                })
            });
            // First check has run. Publication is blocked by the held prepare lock.
            rx.recv().unwrap();
            token.cancel();
            drop(held);
            assert!(matches!(
                join.join().unwrap(),
                Err(Error::ExecutionCancelled)
            ));
        });
        assert_eq!(graph.token().unwrap().sequence, 0);
        graph.apply(&graph.token().unwrap(), &[], b"next").unwrap();
    }
}

#[cfg(test)]
mod locked_control_tests {
    use super::*;
    #[test]
    fn cancellation_after_locked_reader_wait_leaves_state_unchanged() {
        let graph = IncrementalGraph::new(
            "locked-wait",
            IncrementalMode::Locked,
            IncrementalLimits::default(),
        )
        .unwrap();
        let held = graph.read().unwrap();
        let token = crate::CancellationToken::default();
        let control = crate::ExecutionControl::new(token.clone(), Duration::from_secs(30)).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let graph = &graph;
            let control = &control;
            let join = scope.spawn(move || {
                let calls = std::cell::Cell::new(0);
                begin(graph).unwrap().commit_checked(&|| {
                    calls.set(calls.get() + 1);
                    if calls.get() == 1 {
                        tx.send(()).unwrap();
                        Ok(())
                    } else {
                        control.check()
                    }
                })
            });
            rx.recv().unwrap();
            token.cancel();
            drop(held);
            assert!(matches!(
                join.join().unwrap(),
                Err(Error::ExecutionCancelled)
            ));
        });
        assert_eq!(graph.token().unwrap().sequence, 0);
        graph.apply(&graph.token().unwrap(), &[], b"next").unwrap();
    }
}
