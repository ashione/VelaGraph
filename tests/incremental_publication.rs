//! Pause at real allocator boundaries to test reader access during COW work.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, SyncSender},
    Arc,
};
use std::time::Duration;
use velagraph::{IncrementalGraph, IncrementalLimits, IncrementalMode, Value, Vertex};

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const SHARD_ALLOCATION: usize = 8 * 1024;
const LARGE_PAYLOAD: usize = 2 * 1024 * 1024;
#[derive(Clone, Copy, PartialEq)]
enum Operation {
    Allocate,
    Deallocate,
}
struct Probe {
    operation: Operation,
    minimum: usize,
    ready: SyncSender<()>,
    reader_done: Receiver<()>,
    reader_completed: Arc<AtomicBool>,
    next: Option<Box<Probe>>,
}
thread_local! {
    static PROBE: RefCell<Option<Probe>> = const { RefCell::new(None) };
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
fn observe(operation: Operation, size: usize) {
    if operation == Operation::Allocate
        && size >= SHARD_ALLOCATION
        && COUNT_ALLOCATIONS.try_with(Cell::get).unwrap_or(false)
    {
        let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
    }
    let probe = PROBE
        .try_with(|slot| {
            // Clearing an unused probe may itself deallocate its channels.
            let Ok(mut slot) = slot.try_borrow_mut() else {
                return None;
            };
            if slot
                .as_ref()
                .is_some_and(|p| p.operation == operation && size >= p.minimum)
            {
                slot.take()
            } else {
                None
            }
        })
        .ok()
        .flatten();
    if let Some(p) = probe {
        // The slot is empty before channel operations, so nested allocations
        // cannot reenter the probe. No panic may escape the allocator.
        let sent = p.ready.send(()).is_ok();
        let read = sent && p.reader_done.recv_timeout(PROBE_TIMEOUT).is_ok();
        p.reader_completed.store(read, Ordering::Release);
        if let Some(next) = p.next {
            let next = *next;
            let _ = PROBE.try_with(|slot| *slot.borrow_mut() = Some(next));
        }
    }
}
struct ObservedAllocator;
// SAFETY: allocation operations are forwarded unchanged to System. Observation
// does not dereference the allocation, and the thread-local probe is one-shot.
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        observe(Operation::Allocate, layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        observe(Operation::Allocate, layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        observe(Operation::Allocate, size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        observe(Operation::Deallocate, layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

fn graph(vertices: Vec<Vertex>) -> IncrementalGraph {
    IncrementalGraph::from_records(
        "publication",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        vertices,
        vec![],
        b"before".to_vec(),
    )
    .unwrap()
}
fn reader_during_commit(g: &IncrementalGraph, operation: Operation, minimum: usize) {
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let completed = Arc::new(AtomicBool::new(false));
    let before = g.token().unwrap();
    std::thread::scope(|scope| {
        let completed = completed.clone();
        let writer = scope.spawn(move || {
            let mut tx = g.begin().unwrap();
            tx.put_vertex(Vertex::new(1)).unwrap();
            tx.set_metadata(b"after").unwrap();
            PROBE.with(|slot| {
                *slot.borrow_mut() = Some(Probe {
                    operation,
                    minimum,
                    ready: ready_tx,
                    reader_done: done_rx,
                    reader_completed: completed,
                    next: None,
                })
            });
            let result = tx.commit();
            PROBE.with(|slot| *slot.borrow_mut() = None);
            result.unwrap();
        });
        ready_rx
            .recv_timeout(PROBE_TIMEOUT * 2)
            .expect("allocator probe was not reached");
        let view = g.read().unwrap();
        // Copying precedes publication; retiring the old payload follows it.
        let expected = before.sequence + u64::from(operation == Operation::Deallocate);
        assert_eq!(view.token().sequence, expected);
        assert_eq!(
            view.metadata(),
            if operation == Operation::Allocate {
                b"before".as_slice()
            } else {
                b"after".as_slice()
            }
        );
        let _ = done_tx.send(());
        drop(view);
        writer.join().unwrap();
    });
    assert!(
        completed.load(Ordering::Acquire),
        "new reader blocked on COW copy or reclamation"
    );
}

#[test]
fn index_copy_allows_new_readers_to_acquire_old_version() {
    let g = graph((0..65_536).map(Vertex::new).collect());
    reader_during_commit(&g, Operation::Allocate, SHARD_ALLOCATION);
}

#[test]
fn retired_payload_reclamation_allows_new_readers() {
    let mut v = Vertex::new(1);
    v.properties
        .insert("payload".into(), Value::Text("x".repeat(LARGE_PAYLOAD)));
    let g = graph(vec![v]);
    reader_during_commit(&g, Operation::Deallocate, LARGE_PAYLOAD);
}

#[test]
fn preparation_budget_includes_current_and_candidate_even_without_old_readers() {
    let initial = graph(vec![Vertex::new(1)]);
    let bytes = initial.read().unwrap().accounted_bytes();
    let limits = IncrementalLimits {
        max_accounted_bytes: bytes,
        max_pinned_accounted_bytes: 2 * bytes - 1,
        ..IncrementalLimits::default()
    };
    let g = IncrementalGraph::from_records(
        "publication",
        IncrementalMode::Cow,
        limits,
        vec![Vertex::new(1)],
        vec![],
        b"before".to_vec(),
    )
    .unwrap();
    let before = g.token().unwrap();
    let mut tx = g.begin().unwrap();
    tx.put_vertex(Vertex::new(1)).unwrap();
    assert!(
        tx.commit().is_err(),
        "candidate was not included in admission"
    );
    assert_eq!(g.token().unwrap(), before);
}

#[test]
fn exact_preparation_budget_and_retired_version_release() {
    let initial = graph(vec![Vertex::new(1)]);
    let bytes = initial.read().unwrap().accounted_bytes();
    for budget in [2 * bytes, 3 * bytes - 1] {
        let limits = IncrementalLimits {
            max_accounted_bytes: bytes,
            max_pinned_accounted_bytes: budget,
            ..IncrementalLimits::default()
        };
        let g = IncrementalGraph::from_records(
            "publication",
            IncrementalMode::Cow,
            limits,
            vec![Vertex::new(1)],
            vec![],
            b"before".to_vec(),
        )
        .unwrap();
        let old = g.read().unwrap();
        g.apply(&old.token(), &[], b"before").unwrap();
        let before = g.token().unwrap();
        assert!(g.apply(&before, &[], b"before").is_err());
        assert_eq!(g.token().unwrap(), before);
        drop(old);
        g.apply(&before, &[], b"before").unwrap();
    }
}

#[test]
fn concurrent_writers_do_not_publish_stale_candidates() {
    let g = graph(vec![Vertex::new(1), Vertex::new(2)]);
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let workers: Vec<_> = (1..=2)
            .map(|id| {
                let (g, barrier) = (&g, &barrier);
                scope.spawn(move || {
                    let mut tx = g.begin().unwrap();
                    let mut v = Vertex::new(id);
                    v.properties
                        .insert("writer".into(), Value::Integer(id as i64));
                    tx.put_vertex(v).unwrap();
                    tx.set_metadata(&id.to_le_bytes()).unwrap();
                    barrier.wait();
                    (id, tx.commit())
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|(_, r)| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| matches!(r, Err(velagraph::Error::TransactionConflict)))
            .count(),
        1
    );
    let winner = results.iter().find(|(_, r)| r.is_ok()).unwrap().0;
    let view = g.read().unwrap();
    assert_eq!(view.token().sequence, 1);
    assert_eq!(view.metadata(), winner.to_le_bytes());
    assert_eq!(
        view.get_vertex(winner).unwrap().properties["writer"],
        Value::Integer(winner as i64)
    );
    assert!(view.get_vertex(3 - winner).unwrap().properties.is_empty());
}

#[test]
fn preparation_rejection_precedes_candidate_index_allocation() {
    let initial = graph((0..65_536).map(Vertex::new).collect());
    let bytes = initial.read().unwrap().accounted_bytes();
    drop(initial);
    let limits = IncrementalLimits {
        max_accounted_bytes: bytes,
        max_pinned_accounted_bytes: 2 * bytes - 1,
        ..IncrementalLimits::default()
    };
    let g = IncrementalGraph::from_records(
        "publication",
        IncrementalMode::Cow,
        limits,
        (0..65_536).map(Vertex::new).collect(),
        vec![],
        b"before".to_vec(),
    )
    .unwrap();
    let mut tx = g.begin().unwrap();
    tx.put_vertex(Vertex::new(1)).unwrap();
    ALLOCATIONS.with(|n| n.set(0));
    COUNT_ALLOCATIONS.with(|v| v.set(true));
    let result = tx.commit();
    COUNT_ALLOCATIONS.with(|v| v.set(false));
    assert!(result.is_err());
    assert_eq!(
        ALLOCATIONS.with(Cell::get),
        0,
        "candidate allocated before admission"
    );
    assert_eq!(g.token().unwrap().sequence, 0);
}

#[test]
fn reader_pinned_during_preparation_rejects_publication_and_reclaims_outside_lock() {
    let limits = IncrementalLimits {
        max_pinned_versions: 1,
        ..IncrementalLimits::default()
    };
    let g = IncrementalGraph::from_records(
        "publication",
        IncrementalMode::Cow,
        limits,
        (0..65_536).map(Vertex::new).collect(),
        vec![],
        b"before".to_vec(),
    )
    .unwrap();
    let oldest = g.read().unwrap();
    g.apply(&oldest.token(), &[], b"middle").unwrap();
    let before = g.token().unwrap();
    let (copy_tx, copy_rx) = mpsc::sync_channel(1);
    let (copy_done_tx, copy_done_rx) = mpsc::sync_channel(1);
    let (drop_tx, drop_rx) = mpsc::sync_channel(1);
    let (drop_done_tx, drop_done_rx) = mpsc::sync_channel(1);
    let copy_read = Arc::new(AtomicBool::new(false));
    let drop_read = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        let (copy_read, drop_read) = (copy_read.clone(), drop_read.clone());
        let writer = scope.spawn(|| {
            let mut tx = g.begin().unwrap();
            let mut v = Vertex::new(1);
            v.properties
                .insert("payload".into(), Value::Text("x".repeat(LARGE_PAYLOAD)));
            tx.put_vertex(v).unwrap();
            tx.set_metadata(b"rejected").unwrap();
            PROBE.with(|slot| {
                *slot.borrow_mut() = Some(Probe {
                    operation: Operation::Allocate,
                    minimum: SHARD_ALLOCATION,
                    ready: copy_tx,
                    reader_done: copy_done_rx,
                    reader_completed: copy_read,
                    next: Some(Box::new(Probe {
                        operation: Operation::Deallocate,
                        minimum: LARGE_PAYLOAD,
                        ready: drop_tx,
                        reader_done: drop_done_rx,
                        reader_completed: drop_read,
                        next: None,
                    })),
                })
            });
            let result = tx.commit();
            PROBE.with(|slot| *slot.borrow_mut() = None);
            assert!(result.is_err());
        });
        copy_rx.recv_timeout(PROBE_TIMEOUT * 2).unwrap();
        let pinned = g.read().unwrap();
        assert_eq!(pinned.token(), before);
        copy_done_tx.send(()).unwrap();
        drop_rx.recv_timeout(PROBE_TIMEOUT * 2).unwrap();
        let current = g.read().unwrap();
        assert_eq!(current.token(), before);
        assert_eq!(current.metadata(), b"middle");
        assert!(current.get_vertex(1).unwrap().properties.is_empty());
        let _ = drop_done_tx.send(());
        writer.join().unwrap();
        drop(pinned);
    });
    assert!(copy_read.load(Ordering::Acquire));
    assert!(drop_read.load(Ordering::Acquire));
    assert_eq!(oldest.metadata(), b"before");
    assert_eq!(g.token().unwrap(), before);
}
