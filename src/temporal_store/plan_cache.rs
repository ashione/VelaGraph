//! Store-scoped, bounded cache of immutable, successfully parsed query plans.
//! Text limits are cache admission bounds, not replacements for parser limits.
use std::collections::HashMap;
use std::sync::Arc;

use crate::GremlinQuery;

const MAX_ENTRIES: usize = 1024;
const MAX_TEXT_BYTES: usize = 1024 * 1024;
const MAX_CACHEABLE_QUERY_BYTES: usize = 4 * 1024;

struct Entry {
    plan: Arc<GremlinQuery>,
    touched: u64,
}
#[derive(Default)]
pub(super) struct PlanCache {
    entries: HashMap<String, Entry>,
    text_bytes: usize,
    clock: u64,
}
impl PlanCache {
    fn tick(&mut self) -> u64 {
        if let Some(next) = self.clock.checked_add(1) {
            self.clock = next;
        } else {
            self.entries.clear();
            self.text_bytes = 0;
            self.clock = 1;
        }
        self.clock
    }
    pub(super) fn get(&mut self, text: &str) -> Option<Arc<GremlinQuery>> {
        if text.len() > MAX_CACHEABLE_QUERY_BYTES {
            return None;
        }
        let touched = self.tick();
        self.entries.get_mut(text).map(|entry| {
            entry.touched = touched;
            Arc::clone(&entry.plan)
        })
    }
    pub(super) fn insert(&mut self, text: &str, plan: Arc<GremlinQuery>) {
        if text.len() > MAX_CACHEABLE_QUERY_BYTES {
            return;
        }
        let touched = self.tick();
        if let Some(entry) = self.entries.get_mut(text) {
            // A concurrent miss may have already admitted the same immutable plan.
            entry.touched = touched;
            return;
        }
        while self.entries.len() >= MAX_ENTRIES || self.text_bytes + text.len() > MAX_TEXT_BYTES {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.touched)
                .map(|(key, _)| key.clone());
            let Some(key) = oldest else {
                return;
            };
            self.entries.remove(&key);
            self.text_bytes -= key.len();
        }
        self.text_bytes += text.len();
        self.entries
            .insert(text.to_owned(), Entry { plan, touched });
    }
}
