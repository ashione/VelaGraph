//! Validated records retained only for one immutable view execution.
use crate::query_memory::{Accounted, Budget, Reclaim, Reservation, MAP_ENTRY};
use crate::{Edge, Vertex};
use std::cell::RefCell;
use std::collections::BTreeMap;

// Separate vertex/edge ceilings: at most 1 MiB and 1,024 entries in total.
const MAX_MEMO_BYTES: usize = 512 * 1024;
const MAX_MEMO_ENTRIES: usize = 512;

#[derive(Debug)]
struct Entry<T> {
    value: Option<Accounted<T>>,
    _slot: Reservation,
}
impl<T> Entry<T> {
    fn bytes(&self) -> usize {
        MAP_ENTRY + self.value.as_ref().map_or(0, Accounted::accounted_bytes)
    }
}
#[derive(Debug)]
pub(super) struct Memo<T> {
    entries: BTreeMap<u64, Entry<T>>,
    bytes: usize,
}
impl<T> Default for Memo<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            bytes: 0,
        }
    }
}
impl<T: Clone> Memo<T> {
    // Move the original charge out before attempting a new cache copy. A cache
    // hit cannot fail merely because both cache and caller need a payload.
    pub(super) fn take(&mut self, id: u64) -> Option<Option<Accounted<T>>> {
        let entry = self.entries.remove(&id)?;
        self.bytes -= entry.bytes();
        Some(entry.value)
    }
    pub(super) fn retain(&mut self, id: u64, value: &Option<Accounted<T>>, budget: &Budget) {
        let Some(bytes) =
            MAP_ENTRY.checked_add(value.as_ref().map_or(0, Accounted::accounted_bytes))
        else {
            return;
        };
        if self.entries.len() >= MAX_MEMO_ENTRIES
            || bytes > MAX_MEMO_BYTES.saturating_sub(self.bytes)
            || bytes > budget.available()
        {
            return;
        }
        // Admission is best effort. The available-byte check means these
        // reservations do not invoke reclamation while this map is borrowed.
        let Ok(slot) = budget.reserve(MAP_ENTRY) else {
            return;
        };
        let Ok(value) = value.as_ref().map(|v| v.try_clone(budget)).transpose() else {
            return;
        };
        self.entries.insert(id, Entry { value, _slot: slot });
        self.bytes += bytes;
    }
    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
}
#[derive(Debug, Default)]
pub(super) struct RecordMemo {
    pub(super) vertices: RefCell<Memo<Vertex>>,
    pub(super) edges: RefCell<Memo<Edge>>,
}
impl Reclaim for RecordMemo {
    fn reclaim(&self) {
        self.vertices.borrow_mut().clear();
        self.edges.borrow_mut().clear();
    }
}
