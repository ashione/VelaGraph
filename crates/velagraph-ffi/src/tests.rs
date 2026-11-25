use super::*;
use serde_json::{json, Value};
use std::sync::Mutex;
static SERIAL: Mutex<()> = Mutex::new(());
fn call(op: u32, handle: u64, input: Value) -> (u32, Reply) {
    call_bytes(
        op,
        handle,
        &serde_json::to_vec(&input).unwrap(),
        0,
        8 * 1024 * 1024,
    )
}
fn call_bytes(op: u32, handle: u64, data: &[u8], control: u64, limit: u64) -> (u32, Reply) {
    let mut r = Reply::default();
    let s = unsafe {
        vg_call(
            1,
            op,
            handle,
            data.as_ptr(),
            data.len() as u64,
            control,
            5000,
            limit,
            &mut r,
        )
    };
    (s, r)
}
fn output(id: u64) -> Value {
    let mut n = 0;
    assert_eq!(unsafe { vg_buffer_size(id, &mut n) }, 0);
    let mut bytes = vec![0; n as usize];
    assert_eq!(unsafe { vg_buffer_copy(id, bytes.as_mut_ptr(), n) }, 0);
    assert_eq!(vg_release(id), 0);
    serde_json::from_slice(&bytes).unwrap()
}
#[test]
fn creates_memory_store_through_c_abi() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let before = registry::count();
    let (s, r) = call(1, 0, json!({"backend":"memory","identity":"ffi"}));
    assert_eq!(s, 0);
    assert_ne!(r.value, 0);
    assert_eq!(vg_release(r.value), 0);
    assert_eq!(registry::count(), before);
}
#[test]
fn abi_memory_numeric_plan_update_checkpoint_and_close() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let base = registry::count();
    let id = i64::MAX as u64 - 7;
    let (s, r) = call(
        1,
        0,
        json!({"backend":"memory","identity":"ffi","vertices":[{"id":id,"labels":[],"properties":{"n":{"Integer":i64::MAX},"r":{"Real":51.248178375505404},"z":{"Real":-0.0}}}]}),
    );
    assert_eq!(s, 0);
    let store = r.value;
    let (s, r) = call(
        6,
        0,
        json!({"text":"g.V(1).values('n')","bind_start_ids":true}),
    );
    assert_eq!(s, 0);
    let plan = r.value;
    let (s, r) = call(
        3,
        store,
        json!({"queries":[{"prepared":plan,"start_ids":[id]}]}),
    );
    assert_eq!(s, 0);
    assert_eq!(
        output(r.buffer)["results"][0]["rows"][0]["Value"]["Integer"],
        i64::MAX
    );
    let (s, r) = call(
        2,
        store,
        json!({"expected_version":0,"mutations":[],"metadata":[0,255]}),
    );
    assert_eq!(s, 0);
    assert_eq!(r.value, 1);
    assert_eq!(
        call(2, store, json!({"expected_version":0,"mutations":[]})).0,
        status::CONFLICT
    );
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("graph.bin");
    assert_eq!(call(5, store, json!({"path":p})).0, 0);
    let (s, r) = call(
        1,
        0,
        json!({"backend":"memory","identity":"ffi","restore":p}),
    );
    assert_eq!(s, 0);
    let restored = r.value;
    let (s, r) = call_bytes(4, restored, &[], 0, 1024);
    assert_eq!(s, 0);
    let info = output(r.buffer);
    assert_eq!(info["version"], 1);
    assert_eq!(info["metadata"], json!([0, 255]));
    assert_eq!(vg_release(store), 0);
    assert_eq!(vg_release(store), status::HANDLE);
    assert_eq!(vg_release(plan), 0);
    assert_eq!(vg_release(restored), 0);
    assert_eq!(registry::count(), base);
}
#[test]
fn cancellation_and_small_output_do_not_leak_native_resources() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let base = registry::count();
    let wire = bounds::wire_used();
    let (s, r) = call(1, 0, json!({"backend":"memory","identity":"cancel"}));
    assert_eq!(s, 0);
    let store = r.value;
    let mut token = 0;
    assert_eq!(unsafe { vg_control_new(&mut token) }, 0);
    assert_eq!(vg_cancel(token), 0);
    let (s, _) = call_bytes(
        2,
        store,
        br#"{"expected_version":0,"mutations":[],"metadata":[1]}"#,
        token,
        1024,
    );
    assert_eq!(s, status::CANCELLED);
    assert_eq!(call_bytes(4, store, &[], 0, 1).0, status::LIMIT);
    let (s, r) = call_bytes(4, store, &[], 0, 1024);
    assert_eq!(s, 0);
    assert_eq!(output(r.buffer)["version"], 0);
    assert_eq!(vg_release(store), 0);
    assert_eq!(vg_release(token), 0);
    assert_eq!(registry::count(), base);
    assert_eq!(bounds::wire_used(), wire);
}
#[test]
fn temporal_vt_reopen_and_capability_rejections() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("temporal");
    let (s, r) = call(1, 0, json!({"backend":"temporal","path":path}));
    assert_eq!(s, 0);
    let store = r.value;
    assert_eq!(call(2,store,json!({"expected_version":0,"mutations":[{"op":"put_vertex","vertex":{"id":1,"labels":[],"properties":{}},"valid":{"from":10,"to":20}}]})).0,0);
    for (t, n) in [(9, 0), (10, 1), (20, 0)] {
        let (s, r) = call(
            3,
            store,
            json!({"valid_time_micros":t,"version":1,"queries":[{"text":"g.V(1)"}]}),
        );
        assert_eq!(s, 0);
        assert_eq!(
            output(r.buffer)["results"][0]["rows"]
                .as_array()
                .unwrap()
                .len(),
            n
        );
    }
    assert_eq!(
        call(3, store, json!({"queries":[{"text":"g.V(1)"}]})).0,
        status::INVALID
    );
    assert_eq!(
        call(5, store, json!({"path":d.path().join("unsupported")})).0,
        status::UNSUPPORTED
    );
    assert_eq!(vg_release(store), 0);
    let (s, r) = call(1, 0, json!({"backend":"temporal","path":path}));
    assert_eq!(s, 0);
    let reopened = r.value;
    let (s, r) = call_bytes(4, reopened, &[], 0, 2048);
    assert_eq!(s, 0);
    assert_eq!(output(r.buffer)["version"], 1);
    assert_eq!(vg_release(reopened), 0);
}

#[test]
fn invalid_full_load_is_an_argument_error_not_corruption() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for input in [
        json!({"backend":"memory","identity":"invalid","vertices":[{"id":1},{"id":1}]}),
        json!({"backend":"memory","identity":"invalid","vertices":[{"id":1}],"edges":[{"id":7,"from":1,"to":1,"label":"L"},{"id":7,"from":1,"to":1,"label":"L"}]}),
        json!({"backend":"memory","identity":"invalid","vertices":[{"id":1}],"edges":[{"id":7,"from":1,"to":2,"label":"L"}]}),
    ] {
        let (s, r) = call(1, 0, input);
        if r.value != 0 {
            vg_release(r.value);
        }
        assert_eq!(s, status::INVALID);
    }
}

#[test]
fn checkpoint_encoding_capacity_is_a_limit_error() {
    use velagraph::{IncrementalGraph, IncrementalLimits, IncrementalMode, Value as V, Vertex};
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint.bin");
    let limits = IncrementalLimits::default();
    let empty = IncrementalGraph::new("overflow", IncrementalMode::Cow, limits.clone()).unwrap();
    empty.checkpoint(&path).unwrap();
    let before = std::fs::read(&path).unwrap();
    let mut v = Vertex::new(1);
    v.properties
        .insert("large".into(), V::Text("x".repeat(65 * 1024 * 1024)));
    let graph = IncrementalGraph::from_records(
        "overflow",
        IncrementalMode::Cow,
        limits.clone(),
        vec![v],
        vec![],
        vec![],
    )
    .unwrap();
    graph.apply(&graph.token().unwrap(), &[], &[]).unwrap();
    let id = Slot::reserve()
        .unwrap()
        .publish(Resource::Store(engine::Engine::Memory {
            graph,
            limits,
            mode: "cow",
        }))
        .unwrap();
    let (s, _) = call(5, id, json!({"path":path}));
    vg_release(id);
    assert_eq!(s, status::LIMIT);
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn pointer_abi_and_handle_contracts() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let base = registry::count();
    let mut reply = Reply {
        value: 42,
        ..Default::default()
    };
    unsafe {
        assert_eq!(
            vg_call(2, 1, 0, std::ptr::null(), 0, 0, 0, 0, &mut reply),
            status::UNSUPPORTED
        );
        assert_eq!(reply.value, 42); // unknown layout is never touched
        assert_eq!(
            vg_call(1, 1, 0, std::ptr::null(), 1, 0, 0, 0, &mut reply),
            status::INVALID
        );
        assert_eq!(reply.value, 0);
        assert_eq!(
            vg_call(
                1,
                1,
                0,
                std::ptr::null(),
                bounds::MAX_INPUT as u64 + 1,
                0,
                0,
                0,
                &mut reply
            ),
            status::LIMIT
        );
        assert_eq!(vg_control_new(std::ptr::null_mut()), status::INVALID);
        assert_eq!(vg_runtime_stats(std::ptr::null_mut()), status::INVALID);
    }
    let (s, r) = call(1, 0, json!({"backend":"memory","identity":"kinds"}));
    assert_eq!(s, 0);
    let store = r.value;
    assert_eq!(vg_cancel(store), status::HANDLE);
    let mut n = 99;
    assert_eq!(unsafe { vg_buffer_size(store, &mut n) }, status::HANDLE);
    assert_eq!(n, 0);
    let (s, r) = call_bytes(4, store, &[], 0, 1024);
    assert_eq!(s, 0);
    let mut dest = [0x55u8; 2];
    assert_eq!(
        unsafe { vg_buffer_copy(r.buffer, dest.as_mut_ptr(), 2) },
        status::SMALL
    );
    assert_eq!(dest, [0x55; 2]);
    assert_eq!(call_bytes(4, r.buffer, &[], 0, 1024).0, status::HANDLE);
    vg_release(store);
    assert_eq!(output(r.buffer)["identity"], "kinds"); // result outlives store
    assert_eq!(registry::count(), base);
}

#[test]
fn exhaustion_keeps_release_cancel_and_stats_available() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let base = registry::count();
    let usage = bounds::usage();
    let mut token = 0;
    assert_eq!(unsafe { vg_control_new(&mut token) }, 0);
    let calls: Vec<_> = (0..bounds::MAX_CALLS)
        .map(|_| bounds::Permit::call().unwrap())
        .collect();
    assert_eq!(
        call(1, 0, json!({"backend":"memory","identity":"busy"})).0,
        status::BUSY
    );
    assert_eq!(vg_cancel(token), 0);
    let mut stats = Stats::default();
    assert_eq!(unsafe { vg_runtime_stats(&mut stats) }, 0);
    assert_eq!(stats.active_calls, bounds::MAX_CALLS as u64);
    drop(calls);
    let wire = bounds::Permit::wire(bounds::MAX_WIRE).unwrap();
    assert_eq!(
        call(1, 0, json!({"backend":"memory","identity":"full"})).0,
        status::LIMIT
    );
    assert_eq!(vg_cancel(token), 0);
    drop(wire);
    let plans = bounds::Permit::plan(bounds::MAX_PLANS).unwrap();
    assert_eq!(call(6, 0, json!({"text":"g.V(1)"})).0, status::LIMIT);
    drop(plans);
    let slots: Vec<_> = (registry::count()..bounds::MAX_HANDLES)
        .map(|_| Slot::reserve().unwrap())
        .collect();
    let mut rejected = 123;
    assert_eq!(unsafe { vg_control_new(&mut rejected) }, status::LIMIT);
    assert_eq!(rejected, 0);
    assert_eq!(vg_cancel(token), 0);
    assert_eq!(vg_release(token), 0);
    drop(slots);
    assert_eq!(bounds::usage(), usage);
    assert_eq!(registry::count(), base);
}

#[test]
fn retirement_retains_buffer_plan_and_temporal_lock_until_admitted_owner_exits() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let base = registry::count();
    let usage = bounds::usage();
    let (s, r) = call(6, 0, json!({"text":"g.V(1)"}));
    assert_eq!(s, 0);
    let owner = registry::get(r.value).unwrap();
    vg_release(r.value);
    assert!(bounds::usage().1 > usage.1);
    assert!(registry::get(r.value).is_err());
    drop(owner);
    assert_eq!(bounds::usage(), usage);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (s, r) = call(1, 0, json!({"backend":"temporal","path":path}));
    assert_eq!(s, 0);
    let store = r.value;
    let owner = registry::get(store).unwrap();
    let (s, r) = call_bytes(4, store, &[], 0, 2048);
    assert_eq!(s, 0);
    let buffer = registry::get(r.buffer).unwrap();
    vg_release(r.buffer);
    assert!(bounds::usage().0 > usage.0);
    drop(buffer);
    vg_release(store);
    let (s, r) = call(1, 0, json!({"backend":"temporal","path":path}));
    assert_ne!(s, 0);
    assert_eq!(r.value, 0);
    drop(owner);
    let (s, r) = call(1, 0, json!({"backend":"temporal","path":path}));
    assert_eq!(s, 0);
    vg_release(r.value);
    assert_eq!(registry::count(), base);
    assert_eq!(bounds::usage(), usage);
}

#[test]
fn metadata_clear_start_presence_and_unsigned_storage() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (s, r) = call(
        1,
        0,
        json!({"backend":"memory","identity":"strict","metadata":[7],"vertices":[{"id":u64::MAX}]}),
    );
    assert_eq!(s, 0);
    let store = r.value;
    let (s, r) = call(3, store, json!({"queries":[{"text":"g.V()"}]}));
    assert_eq!(s, 0);
    assert_eq!(
        output(r.buffer)["results"][0]["rows"][0]["Vertex"],
        u64::MAX
    );
    let (s, r) = call(6, 0, json!({"text":"g.V(1)","bind_start_ids":true}));
    assert_eq!(s, 0);
    let bound = r.value;
    for q in [
        json!({"prepared":bound}),
        json!({"prepared":bound,"start_ids":[]}),
        json!({"prepared":bound,"start_ids":[u64::MAX]}),
    ] {
        assert_ne!(call(3, store, json!({"queries":[q]})).0, 0);
    }
    let (s, r) = call(6, 0, json!({"text":"g.V()"}));
    assert_eq!(s, 0);
    let literal = r.value;
    assert_ne!(
        call(
            3,
            store,
            json!({"queries":[{"prepared":literal,"start_ids":[]}]})
        )
        .0,
        0
    );
    for (version, metadata) in [(0, None), (1, Some(json!([])))] {
        let mut q = json!({"expected_version":version,"mutations":[]});
        if let Some(m) = metadata {
            q["metadata"] = m;
        }
        assert_eq!(call(2, store, q).0, 0);
        let (s, r) = call_bytes(4, store, &[], 0, 2048);
        assert_eq!(s, 0);
        assert_eq!(output(r.buffer)["metadata"], json!([]));
    }
    for id in [store, bound, literal] {
        assert_eq!(vg_release(id), 0);
    }
}

#[test]
fn uncertain_initialization_and_allocation_failure_are_typed_and_cleaned() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let before = registry::count();
    let usage = bounds::usage();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("uncertain");
    engine::FAIL_AFTER_OPEN.with(|f| f.set(true));
    let (s, r) = call(1, 0, json!({"backend":"temporal","path":path}));
    assert_eq!(s, status::UNKNOWN);
    assert_eq!((r.value, r.buffer), (0, 0));
    assert_eq!(registry::count(), before);
    assert_eq!(bounds::usage(), usage);
    let (s, r) = call(1, 0, json!({"backend":"temporal","path":path}));
    assert_eq!(s, 0);
    let store = r.value;
    output::FAIL_ALLOCATION.with(|f| f.set(true));
    let (s, r) = call_bytes(4, store, &[], 0, 2048);
    assert_eq!(s, status::LIMIT);
    assert_eq!(r.buffer, 0);
    assert_eq!(bounds::usage(), usage);
    let (s, r) = call_bytes(4, store, &[], 0, 2048);
    assert_eq!(s, 0);
    assert_eq!(output(r.buffer)["version"], 0);
    vg_release(store);
    assert_eq!(registry::count(), before);
}

#[test]
fn malformed_checkpoint_payload_is_corruption_not_request_syntax() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("broken.bin");
    let payload = b"not json";
    let hash = payload.iter().fold(14695981039346656037u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(1099511628211)
    });
    let mut bytes = b"VGIM0001".to_vec();
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&hash.to_le_bytes());
    bytes.extend_from_slice(payload);
    std::fs::write(&path, bytes).unwrap();
    let (s, r) = call(
        1,
        0,
        json!({"backend":"memory","identity":"broken","restore":path}),
    );
    assert_eq!(r.value, 0);
    assert_eq!(s, status::CORRUPT);
    // Malformed request JSON itself is still INVALID.
    assert_eq!(call_bytes(1, 0, b"not json", 0, 1024).0, status::INVALID);
}

#[test]
fn packed_query_has_fixed_numeric_rows_and_exact_output_admission() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let base = registry::count();
    let (s, r) = call(
        1,
        0,
        json!({"backend":"memory","identity":"packed","vertices":[{"id":u64::MAX}]}),
    );
    assert_eq!(s, 0);
    let store = r.value;
    let data = br#"{"queries":[{"text":"g.V()"}]}"#;
    let (s, r) = call_bytes(7, store, data, 0, 104);
    assert_eq!(s, 0);
    let mut size = 0;
    assert_eq!(unsafe { vg_buffer_size(r.buffer, &mut size) }, 0);
    assert_eq!(size, 104);
    let mut bytes = vec![0u8; 104];
    assert_eq!(
        unsafe { vg_buffer_copy(r.buffer, bytes.as_mut_ptr(), size) },
        0
    );
    assert_eq!(&bytes[..8], b"VGQR0001");
    assert_eq!(u64::from_le_bytes(bytes[16..24].try_into().unwrap()), 1);
    assert_eq!(u64::from_le_bytes(bytes[80..88].try_into().unwrap()), 1);
    assert_eq!(
        u64::from_le_bytes(bytes[88..96].try_into().unwrap()),
        u64::MAX
    );
    vg_release(r.buffer);
    assert_eq!(call_bytes(7, store, data, 0, 103).0, status::LIMIT);
    vg_release(store);
    assert_eq!(registry::count(), base);
}

#[test]
fn buffer_lease_outlives_original_handle_without_double_charging() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let count = registry::count();
    let usage = bounds::usage();
    let (s, r) = call(1, 0, json!({"backend":"memory","identity":"lease"}));
    assert_eq!(s, 0);
    let store = r.value;
    let (s, r) = call_bytes(4, store, &[], 0, 1024);
    assert_eq!(s, 0);
    let charged = bounds::usage();
    let mut view = BufferView::default();
    assert_eq!(unsafe { vg_buffer_acquire(r.buffer, &mut view) }, 0);
    assert!(!view.data.is_null());
    assert!(view.len > 0);
    assert_eq!(bounds::usage(), charged);
    assert_eq!(vg_release(r.buffer), 0);
    assert_eq!(vg_release(store), 0);
    let bytes = unsafe { std::slice::from_raw_parts(view.data, view.len as usize) };
    assert_eq!(
        serde_json::from_slice::<Value>(bytes).unwrap()["identity"],
        "lease"
    );
    assert_eq!(bounds::usage(), charged);
    assert_eq!(vg_release(view.lease), 0);
    assert_eq!(bounds::usage(), usage);
    assert_eq!(registry::count(), count);
}

#[test]
fn packed_errors_and_lease_exhaustion_release_reservations() {
    use std::time::Duration;
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let count = registry::count();
    let usage = bounds::usage();
    let results = [velagraph::GremlinResult {
        rows: vec![velagraph::GremlinRow::Vertex(1)],
    }];
    for expired in [false, true] {
        let token = CancellationToken::default();
        let c = ExecutionControl::new(
            token.clone(),
            if expired {
                Duration::ZERO
            } else {
                Duration::from_secs(30)
            },
        )
        .unwrap();
        if !expired {
            token.cancel();
        }
        let e = output::Output::reserve(1024)
            .unwrap()
            .encode_packed(0, None, &results, &c)
            .unwrap_err();
        assert_eq!(
            e.code(false),
            if expired {
                status::DEADLINE
            } else {
                status::CANCELLED
            }
        );
        assert_eq!(bounds::usage(), usage);
        assert_eq!(registry::count(), count);
    }
    let c = ExecutionControl::new(CancellationToken::default(), Duration::from_secs(30)).unwrap();
    output::FAIL_ALLOCATION.with(|f| f.set(true));
    let e = output::Output::reserve(1024)
        .unwrap()
        .encode_packed(0, None, &results, &c)
        .unwrap_err();
    assert_eq!(e.code(false), status::LIMIT);
    assert_eq!(bounds::usage(), usage);
    assert_eq!(registry::count(), count);
    let buffer = output::Output::reserve(1024)
        .unwrap()
        .encode_packed(0, None, &results, &c)
        .unwrap();
    let slots: Vec<_> = (registry::count()..bounds::MAX_HANDLES)
        .map(|_| Slot::reserve().unwrap())
        .collect();
    let mut view = BufferView {
        lease: 42,
        data: std::ptr::dangling(),
        len: 42,
    };
    assert_eq!(
        unsafe { vg_buffer_acquire(buffer, &mut view) },
        status::LIMIT
    );
    assert_eq!((view.lease, view.len), (0, 0));
    assert!(view.data.is_null());
    drop(slots);
    assert_eq!(unsafe { vg_buffer_acquire(buffer, &mut view) }, 0);
    assert_eq!(vg_release(buffer), 0);
    assert_eq!(vg_release(view.lease), 0);
    assert_eq!(bounds::usage(), usage);
    assert_eq!(registry::count(), count);
}
