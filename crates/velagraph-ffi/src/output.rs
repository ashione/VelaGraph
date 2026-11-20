use crate::{
    bounds::Permit,
    registry::{Buffer, Resource, Slot},
    status::{self, Result},
};
use std::io::{self, Write};
use velagraph::ExecutionControl;
#[cfg(test)]
thread_local! { pub(super) static FAIL_ALLOCATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub struct Output {
    permit: Permit,
    slot: Slot,
    limit: usize,
}
struct Writer<'a> {
    bytes: Vec<u8>,
    limit: usize,
    control: &'a ExecutionControl,
    full: bool,
    allocation_failed: bool,
}
impl Write for Writer<'_> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.control
            .check()
            .map_err(|_| io::Error::new(io::ErrorKind::Interrupted, "execution stopped"))?;
        let Some(needed) = self
            .bytes
            .len()
            .checked_add(b.len())
            .filter(|n| *n <= self.limit)
        else {
            self.full = true;
            return Err(io::Error::other("native result limit"));
        };
        if needed > self.bytes.capacity() {
            #[cfg(test)]
            if FAIL_ALLOCATION.with(|flag| flag.replace(false)) {
                self.allocation_failed = true;
                return Err(io::Error::other("injected allocation failure"));
            }
            let capacity = needed
                .max(self.bytes.capacity().saturating_mul(2))
                .max(256)
                .min(self.limit);
            if self
                .bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .is_err()
            {
                self.allocation_failed = true;
                return Err(io::Error::other("native result allocation failed"));
            }
        }
        self.bytes.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Output {
    pub fn reserve(limit: usize) -> Result<Self> {
        Ok(Self {
            permit: Permit::wire(limit)?,
            slot: Slot::reserve()?,
            limit,
        })
    }
    pub fn encode(
        mut self,
        value: &impl serde::Serialize,
        control: &ExecutionControl,
    ) -> Result<u64> {
        let mut w = Writer {
            bytes: Vec::new(),
            limit: self.limit,
            control,
            full: false,
            allocation_failed: false,
        };
        if let Err(e) = serde_json::to_writer(&mut w, value) {
            control.check()?;
            if w.full {
                return Err(status::limit("native encoded result exceeds output limit"));
            }
            if w.allocation_failed {
                return Err(status::limit("native result allocation failed"));
            }
            return Err(e.into());
        }
        control.check()?;
        self.permit.shrink(w.bytes.capacity());
        self.slot.publish(Resource::Buffer(Buffer {
            bytes: w.bytes,
            _permit: self.permit,
        }))
    }
}
