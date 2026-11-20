//! Stable native ABI1. See include/velagraph.h and docs/NATIVE_FFI.md.
mod bounds;
mod engine;
mod output;
mod registry;
mod status;
mod wire;
use registry::{Resource, Slot};
use status::{Failure, Result};
use std::{
    fmt::Write,
    panic::{catch_unwind, AssertUnwindSafe},
    time::Duration,
};
use velagraph::{CancellationToken, ExecutionControl};
/// Fixed C layout; error text is UTF-8 and NUL terminated.
#[repr(C)]
pub struct Reply {
    pub value: u64,
    pub buffer: u64,
    pub error: [u8; 256],
}
impl Default for Reply {
    fn default() -> Self {
        Self {
            value: 0,
            buffer: 0,
            error: [0; 256],
        }
    }
}
struct ErrorText<'a> {
    out: &'a mut [u8],
    used: usize,
}
impl std::fmt::Write for ErrorText<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let mut n = s.len().min(self.out.len() - 1 - self.used);
        while !s.is_char_boundary(n) {
            n -= 1;
        }
        self.out[self.used..self.used + n].copy_from_slice(&s.as_bytes()[..n]);
        self.used += n;
        if n < s.len() {
            Err(std::fmt::Error)
        } else {
            Ok(())
        }
    }
}
fn boundary(
    out: &mut Reply,
    write: &std::cell::Cell<bool>,
    f: impl FnOnce() -> Result<(u64, u64)>,
) -> u32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok((value, buffer))) => {
            out.value = value;
            out.buffer = buffer;
            status::OK
        }
        Ok(Err(e)) => {
            let code = e.code(write.get());
            let _ = write!(
                ErrorText {
                    out: &mut out.error,
                    used: 0
                },
                "{e}"
            );
            code
        }
        Err(_) => {
            let _ = write!(
                ErrorText {
                    out: &mut out.error,
                    used: 0
                },
                "native panic; state outcome may require verification"
            );
            if write.get() {
                status::UNKNOWN
            } else {
                status::INTERNAL
            }
        }
    }
}
fn simple(f: impl FnOnce() -> Result<()>) -> u32 {
    boundary(&mut Reply::default(), &std::cell::Cell::new(false), || {
        f()?;
        Ok((0, 0))
    })
}
/// Current ABI and JSON wire schema version.
#[no_mangle]
pub extern "C" fn vg_abi_version() -> u32 {
    1
}
/// Best-effort runtime resource accounting, not process RSS.
#[repr(C)]
#[derive(Default)]
pub struct Stats {
    pub registered_handles: u64,
    pub wire_reserved_bytes: u64,
    pub plan_reserved_bytes: u64,
    pub active_calls: u64,
}
/// Obtain diagnostics without work admission, so exhaustion remains observable.
/// # Safety
/// out points to an aligned writable Stats, with no concurrent access.
#[no_mangle]
pub unsafe extern "C" fn vg_runtime_stats(out: *mut Stats) -> u32 {
    if out.is_null() || !(out as usize).is_multiple_of(std::mem::align_of::<Stats>()) {
        return status::INVALID;
    }
    unsafe {
        out.write(Stats::default());
    }
    simple(|| {
        let n = registry::registered()?;
        let (w, p, c) = bounds::usage();
        unsafe {
            out.write(Stats {
                registered_handles: n as u64,
                wire_reserved_bytes: w as u64,
                plan_reserved_bytes: p as u64,
                active_calls: c as u64,
            });
        }
        Ok(())
    })
}
/// Execute ABI1. Write acknowledgments are returned directly in `value`.
/// # Safety
/// For ABI1, out is a valid aligned writable Reply. Input is a valid immutable
/// range for this entire synchronous call; the ranges must not overlap.
#[no_mangle]
pub unsafe extern "C" fn vg_call(
    abi: u32,
    op: u32,
    store: u64,
    input: *const u8,
    len: u64,
    control: u64,
    timeout: u64,
    max_output: u64,
    out: *mut Reply,
) -> u32 {
    if abi != 1 {
        return status::UNSUPPORTED;
    }
    if out.is_null() || !(out as usize).is_multiple_of(std::mem::align_of::<Reply>()) {
        return status::INVALID;
    }
    unsafe {
        out.write(Reply::default());
    }
    let out = unsafe { &mut *out };
    let may_write = std::cell::Cell::new(op == 2 || op == 5);
    boundary(out, &may_write, || {
        if !(1..=6).contains(&op) {
            return Err(status::unsupported("unknown native operation"));
        }
        if len > bounds::MAX_INPUT as u64 || len > isize::MAX as u64 {
            return Err(status::limit("native input limit"));
        }
        if input.is_null() && len != 0 {
            return Err(status::invalid("null input with nonzero length"));
        }
        if timeout > bounds::MAX_TIMEOUT_MS {
            return Err(status::invalid("timeout exceeds native maximum"));
        }
        let token = if control == 0 {
            CancellationToken::default()
        } else {
            let r = registry::get(control)?;
            let Resource::Control(t) = r.as_ref() else {
                return Err(Failure::Simple(status::HANDLE, "handle is not a control"));
            };
            t.clone()
        };
        let c = ExecutionControl::new(
            token,
            Duration::from_millis(if timeout == 0 {
                bounds::DEFAULT_TIMEOUT_MS
            } else {
                timeout
            }),
        )?;
        c.check()?;
        let _call = bounds::Permit::call()?;
        let _input = bounds::Permit::wire(len as usize)?;
        let data = if len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(input, len as usize) }
        };
        match op {
            1 => {
                if store != 0 {
                    return Err(status::invalid("open requires zero store handle"));
                }
                let slot = Slot::reserve()?;
                let request: wire::Open = serde_json::from_slice(data)?;
                c.check()?;
                may_write.set(matches!(request, wire::Open::Temporal { .. }));
                let engine = engine::Engine::open(request, &c)?;
                let id = slot.publish(Resource::Store(engine))?;
                Ok((id, 0))
            }
            6 => {
                if store != 0 {
                    return Err(status::invalid("prepare is store-independent"));
                }
                let slot = Slot::reserve()?;
                let request = serde_json::from_slice(data)?;
                let plan = engine::prepare(request, &c)?;
                Ok((slot.publish(Resource::Plan(plan))?, 0))
            }
            _ => {
                let resource = registry::get(store)?;
                let Resource::Store(engine) = resource.as_ref() else {
                    return Err(Failure::Simple(status::HANDLE, "handle is not a store"));
                };
                c.check()?;
                match op {
                    2 => {
                        let q = serde_json::from_slice(data)?;
                        Ok((engine.apply(q, &c)?, 0))
                    }
                    5 => {
                        let q: wire::Checkpoint = serde_json::from_slice(data)?;
                        engine.checkpoint(&q.path, &c)?;
                        Ok((0, 0))
                    }
                    3 | 4 => {
                        let max = if max_output == 0 {
                            bounds::DEFAULT_OUTPUT
                        } else {
                            usize::try_from(max_output)
                                .map_err(|_| status::limit("output limit"))?
                        };
                        if max > bounds::MAX_OUTPUT {
                            return Err(status::limit("output limit"));
                        }
                        let output = output::Output::reserve(max)?;
                        if op == 3 {
                            let q = serde_json::from_slice(data)?;
                            engine.query(q, &c, output)
                        } else {
                            if !data.is_empty() {
                                return Err(status::invalid("info takes no payload"));
                            }
                            Ok((0, engine.info(&c, output)?))
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    })
}
/// Retire any published handle; admitted work keeps ownership until completion.
#[no_mangle]
pub extern "C" fn vg_release(id: u64) -> u32 {
    simple(|| registry::release(id))
}
/// Create a monotonic cancellation token.
/// # Safety
/// out points to an aligned writable u64.
#[no_mangle]
pub unsafe extern "C" fn vg_control_new(out: *mut u64) -> u32 {
    if out.is_null() || !(out as usize).is_multiple_of(std::mem::align_of::<u64>()) {
        return status::INVALID;
    }
    unsafe {
        out.write(0);
    }
    let mut reply = Reply::default();
    let s = boundary(&mut reply, &std::cell::Cell::new(false), || {
        Ok((
            Slot::reserve()?.publish(Resource::Control(CancellationToken::default()))?,
            0,
        ))
    });
    if s == 0 {
        unsafe {
            out.write(reply.value);
        }
    }
    s
}
/// Request cooperative cancellation without entering the work-admission pool.
#[no_mangle]
pub extern "C" fn vg_cancel(id: u64) -> u32 {
    simple(|| {
        let r = registry::get(id)?;
        let Resource::Control(c) = r.as_ref() else {
            return Err(Failure::Simple(status::HANDLE, "handle is not a control"));
        };
        c.cancel();
        Ok(())
    })
}
/// Get a result's byte length. It is independent of its originating store.
/// # Safety
/// out points to an aligned writable u64.
#[no_mangle]
pub unsafe extern "C" fn vg_buffer_size(id: u64, out: *mut u64) -> u32 {
    if out.is_null() || !(out as usize).is_multiple_of(std::mem::align_of::<u64>()) {
        return status::INVALID;
    }
    unsafe {
        out.write(0);
    }
    simple(|| {
        let r = registry::get(id)?;
        let Resource::Buffer(b) = r.as_ref() else {
            return Err(Failure::Simple(status::HANDLE, "handle is not a result"));
        };
        unsafe {
            out.write(b.bytes.len() as u64);
        }
        Ok(())
    })
}
/// Copy a result; insufficient capacity performs no copy. Release may race an
/// admitted copy; the result and its byte reservation survive until that copy ends.
/// # Safety
/// out is a writable range of capacity bytes, disjoint from native data.
#[no_mangle]
pub unsafe extern "C" fn vg_buffer_copy(id: u64, out: *mut u8, capacity: u64) -> u32 {
    simple(|| {
        let r = registry::get(id)?;
        let Resource::Buffer(b) = r.as_ref() else {
            return Err(Failure::Simple(status::HANDLE, "handle is not a result"));
        };
        if capacity < b.bytes.len() as u64 {
            return Err(Failure::Simple(status::SMALL, "result buffer too small"));
        }
        if !b.bytes.is_empty() {
            if out.is_null() {
                return Err(status::invalid("null result destination"));
            }
            unsafe {
                std::ptr::copy_nonoverlapping(b.bytes.as_ptr(), out, b.bytes.len());
            }
        }
        Ok(())
    })
}
#[cfg(test)]
mod tests;
