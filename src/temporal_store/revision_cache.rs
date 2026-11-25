//! Optional store-local immutable revision prefixes. Cache misses use storage.
use super::{Interval, Key, TemporalMutation, REVISION_KEY_LEN};
use std::cmp::Reverse;
use std::hash::{BuildHasher, RandomState};
use std::mem::size_of;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

const SLOTS: usize = 512;
const CACHE_BYTES: usize = 1024 * 1024;
const BLOCK_BYTES: usize = 256 * 1024;
const MAX_ROWS: usize = 2048;
const BLOCK_OVERHEAD: usize = 256;

struct Charge {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Charge {
    fn grow(&mut self, bytes: usize) -> bool {
        if bytes > BLOCK_BYTES.saturating_sub(self.bytes) {
            return false;
        }
        let mut old = self.used.load(Ordering::Acquire);
        loop {
            let Some(next) = old.checked_add(bytes).filter(|next| *next <= CACHE_BYTES) else {
                return false;
            };
            match self
                .used
                .compare_exchange_weak(old, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => old = current,
            }
        }
        self.bytes += bytes;
        true
    }
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct Row {
    valid: Interval,
    value: Box<[u8]>,
    cumulative_bytes: usize,
    peak_bytes: usize,
}
struct Index {
    from: i64,
    max_to: Option<i64>,
    row: usize,
}
pub(super) struct Block {
    rows: Vec<Row>,
    index: Vec<Index>,
    complete: bool,
    _charge: Charge,
}
pub(super) struct Hit<'a> {
    pub payload: Option<&'a [u8]>,
    pub rows: usize,
    pub bytes: usize,
    pub peak_bytes: usize,
    pub selected_bytes: usize,
}
impl Block {
    fn winning_row(&self, time: i64) -> Option<usize> {
        // Rows are captured in storage priority order. The first visible row
        // wins even if every older interval also covers this instant.
        if self
            .rows
            .first()
            .is_some_and(|row| row.valid.contains(time))
        {
            return Some(0);
        }
        let mut end = self.index.partition_point(|entry| entry.from <= time);
        let mut winner: Option<usize> = None;
        while end > 0 {
            let entry = &self.index[end - 1];
            if entry.max_to.is_some_and(|to| time >= to) {
                break;
            }
            let row = &self.rows[entry.row];
            if row.valid.to.is_none_or(|to| time < to) {
                winner = Some(winner.map_or(entry.row, |old| old.min(entry.row)));
                // Row zero already missed the direct interval check.
                if winner == Some(1) {
                    break;
                }
            }
            end -= 1;
        }
        winner
    }
    pub fn lookup(&self, time: i64) -> Option<Hit<'_>> {
        let (rows, selected) = match self.winning_row(time) {
            Some(i) => (i + 1, Some(&self.rows[i])),
            None if self.complete => (self.rows.len(), None),
            None => return None,
        };
        let last = rows.checked_sub(1).map(|i| &self.rows[i]);
        Some(Hit {
            payload: selected.and_then(|r| (r.value[18] == 1).then_some(&r.value[19..])),
            rows,
            bytes: last.map_or(0, |r| r.cumulative_bytes),
            peak_bytes: last.map_or(0, |r| r.peak_bytes),
            selected_bytes: selected.map_or(0, |r| REVISION_KEY_LEN + r.value.len()),
        })
    }
}
struct Entry {
    key: Key,
    from: u64,
    through: u64,
    sequence_from: u64,
    sequence_through: u64,
    block: Arc<Block>,
}
pub(super) struct RevisionCache {
    slots: Box<[Mutex<Option<Entry>>]>,
    hash: RandomState,
    used: Arc<AtomicUsize>,
    next_victim: AtomicUsize,
}
impl Default for RevisionCache {
    fn default() -> Self {
        Self {
            slots: (0..SLOTS).map(|_| Mutex::new(None)).collect(),
            hash: RandomState::new(),
            used: Arc::new(AtomicUsize::new(0)),
            next_victim: AtomicUsize::new(0),
        }
    }
}
impl RevisionCache {
    fn slot(&self, key: Key) -> &Mutex<Option<Entry>> {
        let identity = match key {
            Key::Vertex(id) => (0_u8, id),
            Key::Edge(id) => (1_u8, id),
        };
        &self.slots[self.hash.hash_one(identity) as usize % SLOTS]
    }
    pub fn get(&self, key: Key, version: u64, sequence: u64) -> Option<Arc<Block>> {
        let guard = self.slot(key).try_lock().ok()?;
        guard
            .as_ref()
            .filter(|e| {
                e.key == key
                    && e.from <= version
                    && version <= e.through
                    && e.sequence_from <= sequence
                    && sequence <= e.sequence_through
            })
            .map(|e| e.block.clone())
    }
    // One builder can inspect at most SLOTS victims across all reservations.
    // Dropping a slot only releases residency: live readers retain the charge.
    fn reserve(&self, charge: &mut Charge, bytes: usize, remaining: &mut usize) -> bool {
        if bytes > BLOCK_BYTES.saturating_sub(charge.bytes) {
            return false;
        }
        if charge.grow(bytes) {
            return true;
        }
        while *remaining > 0 {
            *remaining -= 1;
            let index = self.next_victim.fetch_add(1, Ordering::Relaxed) % SLOTS;
            let old = self.slots[index]
                .try_lock()
                .ok()
                .and_then(|mut slot| slot.take());
            drop(old);
            if charge.grow(bytes) {
                return true;
            }
        }
        false
    }
    pub fn builder(&self) -> Option<Builder<'_>> {
        let mut charge = Charge {
            used: self.used.clone(),
            bytes: 0,
        };
        let mut remaining = SLOTS;
        if !self.reserve(&mut charge, BLOCK_OVERHEAD, &mut remaining) {
            return None;
        }
        Some(Builder {
            cache: self,
            remaining,
            rows: Vec::new(),
            charge,
        })
    }
    pub fn insert(
        &self,
        key: Key,
        version: u64,
        sequence: u64,
        builder: Builder<'_>,
        complete: bool,
    ) {
        self.publish(key, version, sequence, builder.rows.len(), complete, || {
            builder.finish(complete)
        });
    }

    fn publish(
        &self,
        key: Key,
        version: u64,
        sequence: u64,
        row_count: usize,
        complete: bool,
        build: impl FnOnce() -> Block,
    ) {
        let slot = self.slot(key);
        let supersedes = |e: &Entry| {
            e.key == key
                && e.from <= version
                && version <= e.through
                && e.sequence_from <= sequence
                && sequence <= e.sequence_through
                && (e.block.complete
                    || e.block.rows.len() > row_count
                    || (!complete && e.block.rows.len() == row_count))
        };
        let same_key_origin =
            |entry: &Entry| (entry.key == key).then_some((entry.from, entry.sequence_from));
        let observed = {
            let Ok(guard) = slot.try_lock() else { return };
            if guard.as_ref().is_some_and(supersedes) {
                return;
            }
            guard.as_ref().and_then(same_key_origin)
        };
        // Readers can use the resident block while the new interval index is sorted.
        let block = Arc::new(build());
        let Ok(mut guard) = slot.try_lock() else {
            return;
        };
        if guard.as_ref().is_some_and(supersedes) {
            return;
        }
        // An overlapping fill may have installed a newer snapshot of this key.
        // Compare origins only across this build; later historical reads can cache normally.
        if let Some((v, s)) = guard.as_ref().and_then(same_key_origin) {
            if observed != Some((v, s))
                && (v, s) != (version, sequence)
                && v >= version
                && s >= sequence
            {
                return;
            }
        }
        let old = guard.replace(Entry {
            key,
            from: version,
            through: version,
            sequence_from: sequence,
            sequence_through: sequence,
            block,
        });
        drop(guard);
        drop(old);
    }
    pub fn committed(
        &self,
        base: u64,
        version: u64,
        base_sequence: u64,
        sequence: u64,
        mutations: &[TemporalMutation],
    ) {
        if base.checked_add(1) != Some(version) || mutations.len() > super::MAX_BATCH {
            return;
        }
        let mut changed = [Key::Vertex(0); super::MAX_BATCH];
        for (key, mutation) in changed.iter_mut().zip(mutations) {
            *key = mutation.key();
        }
        let changed = &mut changed[..mutations.len()];
        changed.sort_unstable();
        for slot in &self.slots {
            if let Ok(mut guard) = slot.try_lock() {
                if let Some(entry) = guard.as_mut() {
                    if entry.through == base
                        && entry.sequence_through == base_sequence
                        && changed.binary_search(&entry.key).is_err()
                    {
                        entry.through = version;
                        entry.sequence_through = sequence;
                    }
                }
            }
        }
    }
}
pub(super) struct Builder<'a> {
    cache: &'a RevisionCache,
    remaining: usize,
    rows: Vec<Row>,
    charge: Charge,
}
impl Builder<'_> {
    /// Reserve before allocating. Two row widths cover geometric Vec capacity;
    /// one index width covers the immutable index built at publication.
    pub fn push(&mut self, valid: Interval, value: &[u8]) -> bool {
        if self.rows.len() == MAX_ROWS
            || !self.cache.reserve(
                &mut self.charge,
                2 * size_of::<Row>() + size_of::<Index>() + value.len(),
                &mut self.remaining,
            )
        {
            return false;
        }
        if self.rows.len() == self.rows.capacity() {
            self.rows.reserve_exact(self.rows.capacity().max(1));
        }
        let bytes = REVISION_KEY_LEN + value.len();
        let cumulative_bytes = self.rows.last().map_or(0, |r| r.cumulative_bytes) + bytes;
        let peak_bytes = self.rows.last().map_or(0, |r| r.peak_bytes).max(bytes);
        self.rows.push(Row {
            valid,
            value: value.into(),
            cumulative_bytes,
            peak_bytes,
        });
        true
    }
    fn finish(self, complete: bool) -> Block {
        let mut index: Vec<_> = self
            .rows
            .iter()
            .enumerate()
            .map(|(row, r)| Index {
                from: r.valid.from,
                max_to: r.valid.to,
                row,
            })
            .collect();
        // Lookup walks backwards: for a shared start, visit the highest-priority row first.
        index.sort_unstable_by_key(|i| (i.from, Reverse(i.row)));
        let mut max_to = Some(i64::MIN);
        for i in &mut index {
            max_to = match (max_to, i.max_to) {
                (Some(a), Some(b)) => Some(a.max(b)),
                _ => None,
            };
            i.max_to = max_to;
        }
        Block {
            rows: self.rows,
            index,
            complete,
            _charge: self.charge,
        }
    }
}

pub(super) fn capture(builder: &mut Option<Builder<'_>>, valid: Interval, value: &[u8]) {
    if builder.as_mut().is_some_and(|b| !b.push(valid, value)) {
        *builder = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(builder: &mut Builder<'_>, from: i64, to: Option<i64>) {
        let valid = Interval { from, to };
        let value = super::super::encode_assertion(valid, Some(&[42])).unwrap();
        assert!(builder.push(valid, &value));
    }
    #[test]
    fn interval_index_matches_priority_scan_including_overlaps_and_extremes() {
        let cache = RevisionCache::default();
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 10, Some(20));
        row(&mut builder, 15, Some(30));
        row(&mut builder, i64::MIN, Some(10));
        row(&mut builder, 10, None);
        let block = builder.finish(true);
        for time in [i64::MIN, -1, 0, 9, 10, 15, 19, 20, 29, 30, i64::MAX] {
            let expected = block
                .rows
                .iter()
                .position(|r| r.valid.from <= time && r.valid.to.is_none_or(|to| time < to))
                .unwrap();
            assert_eq!(block.lookup(time).unwrap().rows, expected + 1);
        }
    }
    #[test]
    fn equal_start_revisions_keep_priority_tombstones_and_scan_charges() {
        let cache = RevisionCache::default();
        let mut builder = cache.builder().unwrap();
        let valid = Interval {
            from: -2,
            to: Some(2),
        };
        assert!(builder.push(valid, &super::super::encode_assertion(valid, None).unwrap()));
        row(&mut builder, -2, Some(4));
        row(&mut builder, -2, None);
        let block = builder.finish(false);
        for (time, winner) in [(-2, 0), (1, 0), (2, 1), (3, 1), (4, 2)] {
            let hit = block.lookup(time).unwrap();
            let selected = &block.rows[winner];
            assert_eq!(hit.payload.is_none(), winner == 0);
            assert_eq!(hit.rows, winner + 1);
            assert_eq!(hit.bytes, selected.cumulative_bytes);
            assert_eq!(hit.peak_bytes, selected.peak_bytes);
            assert_eq!(hit.selected_bytes, REVISION_KEY_LEN + selected.value.len());
        }
        assert!(block.lookup(-3).is_none());
    }
    #[test]
    fn incomplete_prefix_never_claims_absence() {
        let cache = RevisionCache::default();
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 10, Some(20));
        let block = builder.finish(false);
        assert!(block.lookup(0).is_none());
        assert!(block.lookup(20).is_none());
        assert!(block.lookup(10).is_some());
    }
    #[test]
    fn charge_outlives_slot_and_rejects_before_copying_large_payload() {
        let cache = RevisionCache::default();
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 0, None);
        cache.insert(Key::Vertex(1), 1, 1, builder, true);
        let held = cache.get(Key::Vertex(1), 1, 1).unwrap();
        let charged = cache.used.load(Ordering::Acquire);
        cache.slot(Key::Vertex(1)).lock().unwrap().take();
        assert_eq!(cache.used.load(Ordering::Acquire), charged);
        drop(held);
        assert_eq!(cache.used.load(Ordering::Acquire), 0);
        let mut builder = cache.builder().unwrap();
        let charged = cache.used.load(Ordering::Acquire);
        assert!(!builder.push(Interval { from: 0, to: None }, &vec![0; BLOCK_BYTES]));
        assert!(builder.rows.is_empty());
        assert_eq!(cache.used.load(Ordering::Acquire), charged);
    }
    #[test]
    fn only_contiguous_acknowledged_unchanged_versions_extend() {
        let cache = RevisionCache::default();
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 0, None);
        cache.insert(Key::Vertex(1), 1, 1, builder, true);
        cache.committed(1, 2, 1, 2, &[]);
        assert!(cache.get(Key::Vertex(1), 2, 2).is_some());
        // Version 3 may have committed without acknowledgement: do not bridge it.
        cache.committed(3, 4, 3, 4, &[]);
        assert!(cache.get(Key::Vertex(1), 4, 4).is_none());
        cache.committed(
            2,
            3,
            2,
            3,
            &[TemporalMutation::Absent {
                key: Key::Vertex(1),
                valid: Interval { from: 0, to: None },
            }],
        );
        assert!(cache.get(Key::Vertex(1), 3, 3).is_none());
        assert!(cache.get(Key::Vertex(1), 1, 1).is_some());
    }
    #[test]
    fn sequence_replacement_and_complete_prefix_upgrade() {
        let cache = RevisionCache::default();
        let key = Key::Vertex(1);
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 10, Some(20));
        cache.insert(key, 1, 1, builder, false);
        assert!(cache.get(key, 1, 1).unwrap().lookup(0).is_none());
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 10, Some(20));
        cache.insert(key, 1, 1, builder, true);
        assert!(cache.get(key, 1, 1).unwrap().lookup(0).is_some());
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 30, Some(40));
        cache.insert(key, 1, 2, builder, true);
        assert!(cache.get(key, 1, 1).is_none());
        assert!(cache
            .get(key, 1, 2)
            .unwrap()
            .lookup(35)
            .unwrap()
            .payload
            .is_some());
    }
    #[test]
    fn older_builder_finishing_last_preserves_newer_same_key_publication() {
        let cache = RevisionCache::default();
        let key = Key::Vertex(1);
        let mut old = cache.builder().unwrap();
        row(&mut old, 0, Some(10));
        let mut new = cache.builder().unwrap();
        row(&mut new, 10, None);

        // Install the new snapshot after the old builder's first slot check
        // and before its final replacement check. No thread scheduling required.
        cache.publish(key, 1, 10, old.rows.len(), true, || {
            cache.insert(key, 2, 20, new, true);
            old.finish(true)
        });
        let latest = cache.get(key, 2, 20).expect("newer block should stay hot");
        assert!(latest.lookup(11).unwrap().payload.is_some());
        assert!(cache.get(key, 1, 10).is_none());

        // A later historical workload may still fill this slot deliberately.
        let mut historical = cache.builder().unwrap();
        row(&mut historical, 0, Some(10));
        cache.insert(key, 1, 10, historical, true);
        assert!(cache
            .get(key, 1, 10)
            .unwrap()
            .lookup(1)
            .unwrap()
            .payload
            .is_some());
        assert!(latest.lookup(11).unwrap().payload.is_some());
    }

    #[test]
    fn late_publication_and_contended_notifications_only_cause_misses() {
        let cache = RevisionCache::default();
        let key = Key::Vertex(1);
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 0, None);
        cache.committed(1, 2, 1, 2, &[]);
        // An older pinned reader finishes after the writer's notification.
        cache.insert(key, 1, 1, builder, true);
        assert!(cache.get(key, 2, 2).is_none());
        let held = cache.get(key, 1, 1).unwrap();
        let guard = cache.slot(key).lock().unwrap();
        cache.committed(1, 2, 1, 2, &[]);
        drop(guard);
        assert!(cache.get(key, 2, 2).is_none());
        let mut builder = cache.builder().unwrap();
        row(&mut builder, 10, None);
        cache.insert(key, 2, 2, builder, true);
        assert!(held.lookup(0).unwrap().payload.is_some());
        assert!(cache
            .get(key, 2, 2)
            .unwrap()
            .lookup(0)
            .unwrap()
            .payload
            .is_none());
    }
    // Fill distinct real cache slots exactly, independent of RandomState.
    fn fill_budget(cache: &RevisionCache) -> Vec<Key> {
        let mut keys = Vec::new();
        let mut slots = std::collections::HashSet::new();
        for id in 0.. {
            let key = Key::Vertex(id);
            if slots.insert(cache.slot(key) as *const _ as usize) {
                keys.push(key);
                if keys.len() == CACHE_BYTES / BLOCK_BYTES {
                    break;
                }
            }
        }
        for &key in &keys {
            let block_bytes = BLOCK_BYTES.min(CACHE_BYTES - cache.used.load(Ordering::Acquire));
            let mut builder = cache.builder().unwrap();
            let payload_len =
                block_bytes - BLOCK_OVERHEAD - 2 * size_of::<Row>() - size_of::<Index>() - 19;
            let valid = Interval { from: 0, to: None };
            let value =
                super::super::encode_assertion(valid, Some(&vec![42; payload_len])).unwrap();
            assert!(builder.push(valid, &value));
            cache.insert(key, 1, 1, builder, true);
        }
        assert_eq!(cache.used.load(Ordering::Acquire), CACHE_BYTES);
        keys
    }
    #[test]
    fn saturated_cache_admits_new_hot_identity_and_changed_identity() {
        for changed in [false, true] {
            let cache = RevisionCache::default();
            let keys = fill_budget(&cache);
            let key = if changed { keys[0] } else { Key::Edge(99) };
            if changed {
                cache.committed(
                    1,
                    2,
                    1,
                    2,
                    &[TemporalMutation::Absent {
                        key,
                        valid: Interval { from: 0, to: None },
                    }],
                );
            }
            assert!(cache.get(key, 2, 2).is_none());
            let mut builder = cache
                .builder()
                .expect("resident blocks must yield to a new working set");
            row(&mut builder, 10, None);
            cache.insert(key, 2, 2, builder, true);
            for _ in 0..2 {
                assert!(cache
                    .get(key, 2, 2)
                    .unwrap()
                    .lookup(10)
                    .unwrap()
                    .payload
                    .is_some());
            }
            assert!(cache.used.load(Ordering::Acquire) <= CACHE_BYTES);
        }
    }
    #[test]
    fn an_in_progress_builder_can_reclaim_for_row_growth() {
        let cache = RevisionCache::default();
        let mut builder = cache.builder().unwrap();
        fill_budget(&cache);
        row(&mut builder, 0, None);
        cache.insert(Key::Edge(99), 1, 1, builder, true);
        assert!(cache.get(Key::Edge(99), 1, 1).is_some());
        assert!(cache.used.load(Ordering::Acquire) <= CACHE_BYTES);
    }
    #[test]
    fn pressure_does_not_uncharge_blocks_held_by_readers() {
        let cache = RevisionCache::default();
        let keys = fill_budget(&cache);
        let held: Vec<_> = keys
            .iter()
            .map(|&key| cache.get(key, 1, 1).unwrap())
            .collect();
        assert!(cache.builder().is_none());
        assert_eq!(cache.used.load(Ordering::Acquire), CACHE_BYTES);
        for block in &held {
            assert!(block.lookup(0).unwrap().payload.is_some());
        }
        drop(held);
        let mut builder = cache
            .builder()
            .expect("evicted reader charges must be released");
        row(&mut builder, 0, None);
        cache.insert(Key::Edge(99), 2, 2, builder, true);
        assert!(cache.get(Key::Edge(99), 2, 2).is_some());
    }
    #[test]
    fn pressure_skips_contended_slots_and_oversized_rows_do_not_evict() {
        let cache = RevisionCache::default();
        let keys = fill_budget(&cache);
        let guards: Vec<_> = keys
            .iter()
            .map(|&key| cache.slot(key).lock().unwrap())
            .collect();
        assert!(cache.builder().is_none());
        assert_eq!(cache.used.load(Ordering::Acquire), CACHE_BYTES);
        drop(guards);
        assert!(cache.builder().is_some());

        let cache = RevisionCache::default();
        let mut builder = cache.builder().unwrap();
        let keys = fill_budget(&cache);
        assert!(!builder.push(Interval { from: 0, to: None }, &vec![0; BLOCK_BYTES]));
        for key in keys {
            assert!(cache.get(key, 1, 1).is_some());
        }
    }
    #[cfg(feature = "query-profiling")]
    #[test]
    fn real_query_warms_after_saturated_resident_set_turnover() {
        use crate::{GremlinRow, Value, Vertex};
        let dir = tempfile::tempdir().unwrap();
        let store = super::super::TemporalStore::open(dir.path()).unwrap();
        let mut vertex = Vertex::new(999);
        vertex.properties.insert("value".into(), Value::Integer(42));
        store
            .commit(&[TemporalMutation::Vertex {
                vertex,
                valid: Interval { from: 0, to: None },
            }])
            .unwrap();
        fill_budget(&store.revision_cache);
        for expected_seeks in [1, 0, 0] {
            crate::query_profile::begin();
            let result = store
                .view(None, 0)
                .unwrap()
                .execute_gremlin("g.V(999).values('value')")
                .unwrap();
            let profile = crate::query_profile::finish();
            assert_eq!(result.rows, vec![GremlinRow::Value(Value::Integer(42))]);
            assert_eq!(profile.record_seeks, expected_seeks);
            assert!(store.revision_cache.used.load(Ordering::Acquire) <= CACHE_BYTES);
        }
    }
    #[test]
    fn row_cap_and_global_byte_cap_are_admission_limits() {
        let cache = RevisionCache::default();
        let mut builder = cache.builder().unwrap();
        for i in 0..MAX_ROWS {
            let valid = Interval {
                from: i as i64,
                to: None,
            };
            if !builder.push(valid, &super::super::encode_assertion(valid, None).unwrap()) {
                assert!(builder.charge.bytes <= BLOCK_BYTES);
                break;
            }
        }
        assert!(builder.rows.len() <= MAX_ROWS);
        drop(builder);
        let mut held = Vec::new();
        while let Some(builder) = cache.builder() {
            held.push(builder);
        }
        assert!(cache.used.load(Ordering::Acquire) <= CACHE_BYTES);
        drop(held);
        assert_eq!(cache.used.load(Ordering::Acquire), 0);
    }
}
