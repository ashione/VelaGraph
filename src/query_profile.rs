//! Opt-in per-thread phase timing for local query diagnostics.
use serde::Serialize;
use std::cell::RefCell;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Totals {
    pub calls: u64,
    pub record_reads: u64,
    pub record_seeks: u64,
    pub record_lookahead_hits: u64,
    pub record_decodes: u64,
    pub record_cache_hits: u64,
    pub record_iterators: u64,
    /// HEAD reads performed while creating temporal views or scanning adjacency.
    pub head_reads: u64,
    pub adjacency_iterators: u64,
    pub adjacency_candidates: u64,
    pub parse_calls: u64,
    pub cache_hits: u64,
    pub parse_ns: u64,
    pub execute_ns: u64,
    pub api_ns: u64,
}
thread_local! {
    static ACTIVE: RefCell<Option<Totals>> = const { RefCell::new(None) };
}
/// Start one logical request's counters on the calling thread.
pub fn begin() {
    ACTIVE.with(|state| *state.borrow_mut() = Some(Totals::default()));
}
/// Stop collection and consume the calling thread's counters.
pub fn finish() -> Totals {
    ACTIVE.with(|state| state.borrow_mut().take().unwrap_or_default())
}

pub(crate) enum Phase {
    Parse,
    Execute,
    Api,
}
pub(crate) struct Timer {
    phase: Phase,
    start: Option<Instant>,
}
impl Timer {
    pub(crate) fn start(phase: Phase) -> Self {
        let active = ACTIVE.with(|state| state.borrow().is_some());
        Self {
            phase,
            start: active.then(Instant::now),
        }
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let Some(start) = self.start else {
            return;
        };
        let ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        ACTIVE.with(|state| {
            if let Some(total) = state.borrow_mut().as_mut() {
                match self.phase {
                    Phase::Parse => {
                        total.parse_ns = total.parse_ns.saturating_add(ns);
                        total.parse_calls += 1;
                    }
                    Phase::Execute => total.execute_ns = total.execute_ns.saturating_add(ns),
                    Phase::Api => {
                        total.api_ns = total.api_ns.saturating_add(ns);
                        total.calls += 1;
                    }
                }
            }
        });
    }
}

/// Count a parsed-plan hit on the currently profiled calling thread.
pub(crate) fn cache_hit() {
    ACTIVE.with(|state| {
        if let Some(total) = state.borrow_mut().as_mut() {
            total.cache_hits += 1;
        }
    });
}

pub(crate) enum ReadEvent {
    Seek,
    LookaheadHit,
    Head,
    AdjacencyIterator,
    AdjacencyCandidate,
    Read,
    Decode,
    CacheHit,
    Iterator,
}
pub(crate) fn read_event(event: ReadEvent) {
    ACTIVE.with(|state| {
        if let Some(t) = state.borrow_mut().as_mut() {
            match event {
                ReadEvent::Seek => t.record_seeks += 1,
                ReadEvent::LookaheadHit => t.record_lookahead_hits += 1,
                ReadEvent::Head => t.head_reads += 1,
                ReadEvent::AdjacencyIterator => t.adjacency_iterators += 1,
                ReadEvent::AdjacencyCandidate => t.adjacency_candidates += 1,
                ReadEvent::Read => t.record_reads += 1,
                ReadEvent::Decode => t.record_decodes += 1,
                ReadEvent::CacheHit => t.record_cache_hits += 1,
                ReadEvent::Iterator => t.record_iterators += 1,
            }
        }
    });
}
