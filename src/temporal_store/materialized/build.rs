use super::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

fn decode_cached(identity: Key, raw: &[u8], meter: &mut Meter) -> Result<Cached> {
    let budget = Budget::new(
        (meter.limits.max_accounted_bytes - meter.charged).min(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
    );
    let cached = match identity {
        Key::Vertex(id) => {
            let decoded = codec::decode_vertex_accounted(raw, &budget)?;
            if decoded.id != id {
                return Err(TemporalStoreError::Corrupt);
            }
            validate_vertex(&decoded)?;
            Cached {
                bytes: decoded.accounted_bytes(),
                record: Record::Vertex(decoded.value),
            }
        }
        Key::Edge(id) => {
            let decoded = codec::decode_edge_accounted(raw, &budget)?;
            if decoded.id != id {
                return Err(TemporalStoreError::Corrupt);
            }
            validate_edge(&decoded)?;
            Cached {
                bytes: decoded.accounted_bytes(),
                record: Record::Edge(decoded.value),
            }
        }
    };
    meter.charge(cached.bytes)?;
    Ok(cached)
}
fn sweep(revs: &[Revision], meter: &mut Meter) -> Result<Vec<Piece>> {
    if revs.len() == 1 {
        return Ok(revs[0]
            .payload
            .map(|p| {
                vec![Piece {
                    from: revs[0].valid.from,
                    to: revs[0].valid.to,
                    payload: p,
                }]
            })
            .unwrap_or_default());
    }
    let mut events = Vec::new();
    for (i, r) in revs.iter().enumerate() {
        events.push((r.valid.from, true, i));
        if let Some(end) = r.valid.to {
            events.push((end, false, i));
        }
    }
    events.sort_unstable();
    let mut active = vec![false; revs.len()];
    let mut heap = BinaryHeap::new();
    let mut result: Vec<Piece> = Vec::new();
    let mut i = 0;
    while i < events.len() {
        meter.check()?;
        let t = events[i].0;
        while i < events.len() && events[i].0 == t {
            let (_, start, id) = events[i];
            active[id] = start;
            if start {
                heap.push((revs[id].version, Reverse(revs[id].ordinal), Reverse(id)));
            }
            i += 1;
        }
        while heap.peek().is_some_and(|(_, _, Reverse(id))| !active[*id]) {
            heap.pop();
        }
        let end = events.get(i).map(|e| e.0);
        if let Some((_, _, Reverse(id))) = heap.peek() {
            if let Some(payload) = revs[*id].payload {
                if let Some(last) = result
                    .last_mut()
                    .filter(|p| p.payload == payload && p.to == Some(t))
                {
                    last.to = end;
                } else {
                    if result.len() >= MAX_ITEMS {
                        return Err(TemporalStoreError::Limit);
                    }
                    result.push(Piece {
                        from: t,
                        to: end,
                        payload,
                    });
                }
            }
        }
    }
    Ok(result)
}
fn finish_group(
    key: Option<Key>,
    revs: &mut Vec<Revision>,
    local: &mut Vec<Option<Cached>>,
    payloads: &mut Vec<Cached>,
    vertices: &mut BTreeMap<u64, Vec<Piece>>,
    edges: &mut BTreeMap<u64, Vec<Piece>>,
    meter: &mut Meter,
) -> Result<()> {
    let Some(key) = key else { return Ok(()) };
    if revs.is_empty() {
        return Ok(());
    }
    let mut pieces = sweep(revs, meter)?;
    let mut remap = vec![None; local.len()];
    for p in &mut pieces {
        let old = p.payload as usize;
        let next = if let Some(n) = remap[old] {
            n
        } else {
            if payloads.len() >= MAX_ITEMS {
                return Err(TemporalStoreError::Limit);
            }
            let n = payloads.len() as u32;
            payloads.push(local[old].take().ok_or(TemporalStoreError::Corrupt)?);
            remap[old] = Some(n);
            n
        };
        p.payload = next;
    }
    meter.charge(SLOT_FEE + pieces.len() * std::mem::size_of::<Piece>())?;
    match key {
        Key::Vertex(id) => {
            vertices.insert(id, pieces);
        }
        Key::Edge(id) => {
            edges.insert(id, pieces);
        }
    }
    revs.clear();
    local.clear();
    Ok(())
}
impl TemporalStore {
    /// Validate and compile an owned read model at one local V, preserving all T.
    ///
    /// The revision and adjacency inventories are checked at the captured head,
    /// even when selecting an older V. Failure publishes no partial model. This
    /// eager validation can fail where an unrelated lazy point query succeeds.
    /// The model owns its data; later commits/closure cannot refresh or invalidate it.
    pub fn materialize(
        &self,
        selected: Option<u64>,
        limits: MaterializationLimits,
    ) -> Result<TemporalReadModel> {
        if limits.max_source_rows == 0
            || limits.max_raw_bytes == 0
            || limits.max_accounted_bytes == 0
            || limits.max_source_rows > MAX_ITEMS
            || limits.max_raw_bytes > MAX_READ_BYTES
            || limits.max_accounted_bytes > MAX_BUILD_BYTES
        {
            return Err(TemporalStoreError::Limit);
        }
        let mut meter = Meter {
            limits,
            charged: 0,
            rows: 0,
            revisions: 0,
            candidates: 0,
            raw: 0,
            deadline: Instant::now() + BUILD_DEADLINE,
        };
        let snapshot = self.db.snapshot();
        if snapshot.get(FORMAT_KEY)?.as_deref() != Some(&[2]) {
            return Err(TemporalStoreError::IncompatibleStore);
        }
        let head = decode_version(snapshot.get(HEAD_KEY)?.as_deref())?;
        let version = selected.unwrap_or(head);
        if version > head {
            return Err(TemporalStoreError::UnavailableVersion);
        }
        let mut expected_candidates = BTreeMap::<(bool, u64, u64), u64>::new();
        let mut vertices = BTreeMap::new();
        let mut edges = BTreeMap::new();
        let mut payloads = Vec::new();
        let mut revs = Vec::new();
        let mut local = Vec::new();
        let mut group = None;
        let mut cursor = snapshot.raw_iterator();
        cursor.seek(RECORD_PREFIX);
        loop {
            meter.check()?;
            cursor.status()?;
            let Some((key, value)) = cursor.item() else {
                break;
            };
            if !key.starts_with(RECORD_PREFIX) {
                break;
            }
            meter.scanned(key.len() + value.len(), true)?;
            if key.len() != REVISION_KEY_LEN {
                return Err(TemporalStoreError::Corrupt);
            }
            let id = u64::from_be_bytes(
                key[RECORD_PREFIX.len() + 1..IDENTITY_PREFIX_LEN]
                    .try_into()
                    .unwrap(),
            );
            let identity = match key[RECORD_PREFIX.len()] {
                1 => Key::Vertex(id),
                2 => Key::Edge(id),
                _ => return Err(TemporalStoreError::Corrupt),
            };
            if group != Some(identity) {
                finish_group(
                    group,
                    &mut revs,
                    &mut local,
                    &mut payloads,
                    &mut vertices,
                    &mut edges,
                    &mut meter,
                )?;
                group = Some(identity);
            }
            let revision = !u64::from_be_bytes(
                key[IDENTITY_PREFIX_LEN..REVISION_VERSION_END]
                    .try_into()
                    .unwrap(),
            );
            let ordinal = u32::from_be_bytes(key[REVISION_VERSION_END..].try_into().unwrap());
            let (valid, raw) = decode_revision(key, value, head)?;
            // Validate the entire captured source, including future revisions
            // relative to selected V. Only selected payloads enter the model.
            let cached = raw
                .map(|raw| decode_cached(identity, raw, &mut meter))
                .transpose()?;
            if let Some(Cached {
                record: Record::Edge(edge),
                ..
            }) = &cached
            {
                for (incoming, endpoint) in [(false, edge.from), (true, edge.to)] {
                    match expected_candidates.entry((incoming, endpoint, edge.id)) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            meter.charge(CANDIDATE_FEE)?;
                            entry.insert(revision);
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            *entry.get_mut() = (*entry.get()).min(revision);
                        }
                    }
                }
            }
            if revision <= version {
                let payload = cached.map(|cached| {
                    let id = local.len() as u32;
                    local.push(Some(cached));
                    id
                });
                revs.push(Revision {
                    version: revision,
                    ordinal,
                    valid,
                    payload,
                });
            }
            cursor.next();
        }
        finish_group(
            group,
            &mut revs,
            &mut local,
            &mut payloads,
            &mut vertices,
            &mut edges,
            &mut meter,
        )?;
        // Reject malformed, missing, extra, or rewritten candidate metadata.
        // Both scans use the same snapshot; this is validation, not index repair.
        let mut expected_candidates = expected_candidates.into_iter();
        cursor.seek(ADJACENCY_PREFIX);
        loop {
            meter.check()?;
            cursor.status()?;
            let Some((key, value)) = cursor.item() else {
                break;
            };
            if !key.starts_with(ADJACENCY_PREFIX) {
                break;
            }
            meter.scanned(key.len() + value.len(), false)?;
            if key.len() != ADJACENCY_KEY_LEN {
                return Err(TemporalStoreError::Corrupt);
            }
            let base = ADJACENCY_PREFIX.len();
            let incoming = match key[base] {
                0 => false,
                1 => true,
                _ => return Err(TemporalStoreError::Corrupt),
            };
            let endpoint = u64::from_be_bytes(key[base + 1..base + 9].try_into().unwrap());
            let edge = u64::from_be_bytes(key[base + 9..].try_into().unwrap());
            let first = decode_version(Some(value))?;
            if expected_candidates.next() != Some(((incoming, endpoint, edge), first)) {
                return Err(TemporalStoreError::Corrupt);
            }
            cursor.next();
        }
        if expected_candidates.next().is_some() {
            return Err(TemporalStoreError::Corrupt);
        }
        for cached in &payloads {
            if let Record::Edge(e) = &cached.record {
                for id in [e.from, e.to] {
                    if let std::collections::btree_map::Entry::Vacant(entry) = vertices.entry(id) {
                        meter.charge(SLOT_FEE)?;
                        entry.insert(Vec::new());
                    }
                }
            }
        }
        if vertices.len() > MAX_ITEMS || edges.len() > MAX_ITEMS {
            return Err(TemporalStoreError::Limit);
        }
        let vertices: Vec<_> = vertices
            .into_iter()
            .map(|(id, pieces)| Slot { id, pieces })
            .collect();
        let edges: Vec<_> = edges
            .into_iter()
            .map(|(id, pieces)| Slot { id, pieces })
            .collect();
        meter.charge((vertices.len() + edges.len()) * SLOT_FEE)?;
        let vertex_index: HashMap<_, _> = vertices
            .iter()
            .enumerate()
            .map(|(i, s)| (s.id, i as u32))
            .collect();
        let edge_index: HashMap<_, _> = edges
            .iter()
            .enumerate()
            .map(|(i, s)| (s.id, i as u32))
            .collect();
        let row_count: usize = edges.iter().map(|e| e.pieces.len()).sum();
        if row_count > MAX_ITEMS {
            return Err(TemporalStoreError::Limit);
        }
        meter.charge(
            row_count * (std::mem::size_of::<Row>() * 2 + 16)
                + vertices.len() * 2 * std::mem::size_of::<Vec<u32>>(),
        )?;
        let mut labels = Vec::new();
        let mut label_index = BTreeMap::new();
        let mut rows = Vec::with_capacity(row_count);
        let mut outgoing = vec![Vec::new(); vertices.len()];
        let mut incoming = vec![Vec::new(); vertices.len()];
        for slot in &edges {
            for piece in &slot.pieces {
                meter.check()?;
                if rows.len() >= MAX_ITEMS {
                    return Err(TemporalStoreError::Limit);
                }
                let Record::Edge(e) = &payloads[piece.payload as usize].record else {
                    return Err(TemporalStoreError::Corrupt);
                };
                let label = if let Some(id) = label_index.get(&e.label) {
                    *id
                } else {
                    meter.charge(e.label.len() * 2 + SLOT_FEE)?;
                    let id = labels.len() as u32;
                    labels.push(e.label.clone());
                    label_index.insert(e.label.clone(), id);
                    id
                };
                let from = vertex_index[&e.from];
                let to = vertex_index[&e.to];
                let row = rows.len() as u32;
                rows.push(Row {
                    edge: e.id,
                    from,
                    to,
                    label,
                    payload: piece.payload,
                    start: piece.from,
                    end: piece.to.unwrap_or(0),
                    has_end: piece.to.is_some(),
                });
                outgoing[from as usize].push(row);
                incoming[to as usize].push(row);
            }
        }
        let outgoing_blocks = blocks::build(&outgoing, &rows, &mut meter)?;
        let incoming_blocks = blocks::build(&incoming, &rows, &mut meter)?;
        let adjacency_block_bytes =
            blocks::retained(&outgoing_blocks) + blocks::retained(&incoming_blocks);
        let segments = vertices
            .iter()
            .chain(edges.iter())
            .map(|s| s.pieces.len())
            .sum();
        let retained = adjacency_block_bytes
            + payloads.capacity() * std::mem::size_of::<Cached>()
            + labels.capacity() * std::mem::size_of::<String>()
            + payloads.iter().map(|p| p.bytes).sum::<usize>()
            + (rows.capacity() * std::mem::size_of::<Row>())
            + (vertex_index.capacity() + edge_index.capacity()) * SLOT_FEE
            + (vertices.capacity() + edges.capacity()) * std::mem::size_of::<Slot>()
            + vertices
                .iter()
                .chain(edges.iter())
                .map(|s| s.pieces.capacity() * std::mem::size_of::<Piece>())
                .sum::<usize>()
            + (outgoing.capacity() + incoming.capacity()) * std::mem::size_of::<Vec<u32>>()
            + outgoing
                .iter()
                .chain(incoming.iter())
                .map(|v| v.capacity() * 4)
                .sum::<usize>()
            + labels.iter().map(String::capacity).sum::<usize>();
        if retained > limits.max_accounted_bytes {
            return Err(TemporalStoreError::Limit);
        }
        meter.check()?;
        let stats = MaterializationStats {
            version,
            source_head: head,
            scanned_revisions: meter.revisions,
            scanned_candidates: meter.candidates,
            scanned_raw_bytes: meter.raw,
            payloads: payloads.len(),
            resolved_segments: segments,
            vertex_slots: vertices.len(),
            edge_slots: edges.len(),
            adjacency_rows: rows.len(),
            adjacency_references: outgoing.iter().chain(incoming.iter()).map(Vec::len).sum(),
            row_storage_bytes: (rows.capacity() * std::mem::size_of::<Row>()),
            adjacency_block_bytes,
            retained_accounted_bytes: retained,
            build_accounted_bytes: meter.charged,
        };
        let graph = TemporalReadModel {
            version,
            vertices,
            edges,
            vertex_index,
            edge_index,
            payloads,
            labels,
            rows,
            outgoing,
            incoming,
            outgoing_blocks,
            incoming_blocks,
            stats,
        };
        graph.validate(&mut meter)?;
        Ok(graph)
    }
}
impl TemporalReadModel {
    fn validate(&self, meter: &mut Meter) -> Result<()> {
        for i in 0..self.rows.len() {
            meter.check()?;
            let r = self.rows[i];
            if (r.has_end && r.end <= r.start) || (!r.has_end && r.end != 0) {
                return Err(TemporalStoreError::Corrupt);
            }
            let from = self
                .vertices
                .get(r.from as usize)
                .ok_or(TemporalStoreError::Corrupt)?;
            let to = self
                .vertices
                .get(r.to as usize)
                .ok_or(TemporalStoreError::Corrupt)?;
            let label = self
                .labels
                .get(r.label as usize)
                .ok_or(TemporalStoreError::Corrupt)?;
            let payload = self
                .payloads
                .get(r.payload as usize)
                .ok_or(TemporalStoreError::Corrupt)?;
            if !matches!(&payload.record,Record::Edge(e) if e.id==r.edge && e.from==from.id && e.to==to.id && &e.label==label)
            {
                return Err(TemporalStoreError::Corrupt);
            }
        }
        for (incoming, lists) in [(false, &self.outgoing), (true, &self.incoming)] {
            for (slot, refs) in lists.iter().enumerate() {
                let mut previous = None;
                for index in refs {
                    meter.check()?;
                    if *index as usize >= self.rows.len() {
                        return Err(TemporalStoreError::Corrupt);
                    }
                    let r = self.rows[*index as usize];
                    if (if incoming { r.to } else { r.from }) as usize != slot
                        || previous.is_some_and(|id| id > r.edge)
                    {
                        return Err(TemporalStoreError::Corrupt);
                    }
                    previous = Some(r.edge);
                }
            }
        }
        blocks::validate(&self.outgoing, &self.outgoing_blocks, &self.rows, meter)?;
        blocks::validate(&self.incoming, &self.incoming_blocks, &self.rows, meter)?;
        Ok(())
    }
}
