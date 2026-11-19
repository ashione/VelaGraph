//! Observe only this thread's large allocations at the actual allocator boundary.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use velagraph::{
    Edge, IncrementalGraph, IncrementalLimits, IncrementalMode, IncrementalMutation, Value, Vertex,
};
const LARGE: usize = 4 * 1024 * 1024;
thread_local! {static TRACK:Cell<bool>=const{Cell::new(false)};static LARGE_ALLOCS:Cell<usize>=const{Cell::new(0)};}
struct Counting;
fn observe(size: usize) {
    if size >= LARGE && TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = LARGE_ALLOCS.try_with(|n| n.set(n.get() + 1));
    }
}
// SAFETY: every allocation operation is forwarded unchanged to System. The
// thread-local counters have constant initialization and allocate no storage.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        observe(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        observe(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        observe(n);
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;
fn count_large(f: impl FnOnce()) {
    LARGE_ALLOCS.with(|n| n.set(0));
    TRACK.with(|v| v.set(true));
    f();
    TRACK.with(|v| v.set(false));
    assert_eq!(
        LARGE_ALLOCS.with(Cell::get),
        0,
        "over-budget payload was copied"
    );
}
#[test]
fn over_budget_adjacency_rejects_before_copying_payload() {
    let mut edge = Edge::new(1, 1, 2, "LINK");
    edge.properties
        .insert("payload".into(), Value::Text("x".repeat(32 * 1024 * 1024)));
    let g = IncrementalGraph::from_records(
        "large",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vec![Vertex::new(1), Vertex::new(2)],
        vec![edge],
        vec![],
    )
    .unwrap();
    let before = g.token().unwrap();
    let mut tx = g.begin().unwrap();
    count_large(|| assert!(tx.adjacent(1, false).is_err()));
    assert!(tx.commit().is_err());
    assert_eq!(g.token().unwrap(), before);
}
#[test]
fn apply_rejects_large_borrowed_records_before_cloning() {
    let g = IncrementalGraph::from_records(
        "apply",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vec![Vertex::new(1), Vertex::new(2)],
        vec![],
        vec![],
    )
    .unwrap();
    let before = g.token().unwrap();
    let mut vertex = Vertex::new(1);
    vertex
        .properties
        .insert("payload".into(), Value::Text("x".repeat(32 * 1024 * 1024)));
    let mut edge = Edge::new(1, 1, 2, "LINK");
    edge.properties
        .insert("payload".into(), Value::Text("x".repeat(32 * 1024 * 1024)));
    for change in [
        IncrementalMutation::PutVertex(vertex),
        IncrementalMutation::PutEdge(edge),
    ] {
        count_large(|| assert!(g.apply(&before, &[change], &[]).is_err()));
        assert_eq!(g.token().unwrap(), before);
    }
}
