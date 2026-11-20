//! COW preparation and reclamation must not hold the reader publication lock.
use super::*;
use std::collections::HashMap;

pub(super) struct Changes {
    pub vertices: HashMap<u64, Option<VEntry>>,
    pub edges: HashMap<u64, Option<Arc<Cached<Edge>>>>,
    pub metadata: Vec<u8>,
    pub vertex_count: usize,
    pub edge_count: usize,
    pub preparation_bytes: usize,
    pub stats: IncrementalCommitStats,
}

fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(limit("batch preparation deadline"));
    }
    Ok(())
}

fn check_token(state: &State, token: &IncrementalToken) -> Result<()> {
    if state.sequence != token.sequence || state.identity != token.identity {
        return Err(Error::TransactionConflict);
    }
    Ok(())
}

fn version_bytes(candidate: usize, current: usize, alive: &[Arc<State>]) -> Result<usize> {
    alive
        .iter()
        .fold(candidate.checked_add(current), |sum, state| {
            sum.and_then(|n| n.checked_add(state.bytes))
        })
        .ok_or_else(|| limit("pinned accounting"))
}

pub(super) fn commit(
    graph: &IncrementalGraph,
    source: IncrementalReadView<'_>,
    staged: Changes,
    deadline: Instant,
) -> Result<IncrementalCommitStats> {
    // Keep serialization through candidate destruction/reclamation, including
    // all early returns. Existing transaction overlays have their own bounds.
    let _prepare = graph
        .cow_prepare
        .lock()
        .map_err(|_| bad("poisoned preparation lock"))?;
    // Function parameters otherwise drop after local guards on early returns.
    let base = source;
    let changes = staged;
    check_deadline(deadline)?;
    let token = base.token();
    let bytes = changes.stats.retained_accounted_bytes;
    let alive = {
        let guard = graph.inner.read().map_err(|_| bad("poisoned lock"))?;
        check_token(&guard.current, &token)?;
        guard
            .retired
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>()
    };
    // Both versions coexist while preparing, even without an external reader.
    // The writer mutex admits only one such candidate per graph.
    if version_bytes(changes.preparation_bytes, base.state().bytes, &alive)?
        > graph.limits.max_pinned_accounted_bytes
    {
        return Err(limit("COW preparation version budget"));
    }
    drop(alive);
    check_deadline(deadline)?;

    let mut next = base.state().clone();
    let Changes {
        vertices,
        edges,
        metadata,
        vertex_count,
        edge_count,
        mut stats,
        preparation_bytes: _,
    } = changes;
    for (id, vertex) in vertices {
        check_deadline(deadline)?;
        stats.copied_index_entries += next.vertices.set(id, vertex);
    }
    for (id, edge) in edges {
        check_deadline(deadline)?;
        stats.copied_index_entries += next.edges.set(id, edge);
    }
    next.sequence = stats.sequence;
    next.metadata = metadata;
    next.bytes = bytes;
    next.vertex_count = vertex_count;
    next.edge_count = edge_count;
    let next = Arc::new(next);
    drop(base);

    // Declare owning references before the guard: on every error the guard
    // drops first, so neither a rejected candidate nor a retired payload is
    // destroyed under the publication lock.
    let mut alive = Vec::with_capacity(graph.limits.max_pinned_versions);
    let mut guard = graph.inner.write().map_err(|_| bad("poisoned lock"))?;
    check_deadline(deadline)?;
    check_token(&guard.current, &token)?;
    guard.retired.retain(|w| w.strong_count() > 0);
    alive.extend(guard.retired.iter().filter_map(Weak::upgrade));
    let pinned = Arc::strong_count(&guard.current) > 1;
    let current_bytes = if pinned { guard.current.bytes } else { 0 };
    if alive.len() + usize::from(pinned) > graph.limits.max_pinned_versions
        || version_bytes(bytes, current_bytes, &alive)? > graph.limits.max_pinned_accounted_bytes
    {
        return Err(limit("pinned version backpressure"));
    }
    let old = std::mem::replace(&mut guard.current, next);
    guard.retired.push(Arc::downgrade(&old));
    drop(guard);
    drop(alive);
    drop(old);
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_cow_candidate_cannot_publish_metadata_or_sequence() {
        let graph = IncrementalGraph::new(
            "expired",
            IncrementalMode::Cow,
            IncrementalLimits::default(),
        )
        .unwrap();
        let source = graph.read().unwrap();
        let before = source.token();
        let changes = Changes {
            vertices: HashMap::new(),
            edges: HashMap::new(),
            metadata: b"unpublished".to_vec(),
            vertex_count: 0,
            edge_count: 0,
            preparation_bytes: source.accounted_bytes() + b"unpublished".len(),
            stats: IncrementalCommitStats {
                sequence: before.sequence + 1,
                retained_accounted_bytes: source.accounted_bytes() + b"unpublished".len(),
                ..IncrementalCommitStats::default()
            },
        };
        let deadline = Instant::now() - Duration::from_secs(1);
        assert!(matches!(
            commit(&graph, source, changes, deadline),
            Err(Error::TransactionTooLarge(_))
        ));
        assert_eq!(graph.token().unwrap(), before);
        assert!(graph.read().unwrap().metadata().is_empty());
        // Rejection releases serialization for the next valid writer.
        graph.apply(&before, &[], b"published").unwrap();
        assert_eq!(graph.read().unwrap().metadata(), b"published");
    }
}
