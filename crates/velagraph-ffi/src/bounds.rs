use crate::status::{self, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
pub const MAX_INPUT: usize = 64 * 1024 * 1024;
pub const MAX_OUTPUT: usize = 64 * 1024 * 1024;
pub const DEFAULT_OUTPUT: usize = 8 * 1024 * 1024;
pub const MAX_WIRE: usize = 128 * 1024 * 1024;
pub const MAX_PLANS: usize = 64 * 1024 * 1024;
pub const MAX_HANDLES: usize = 4096;
pub const MAX_CALLS: usize = 16;
pub const DEFAULT_TIMEOUT_MS: u64 = 5000;
pub const MAX_TIMEOUT_MS: u64 = 60000;
static WIRE: AtomicUsize = AtomicUsize::new(0);
static PLANS: AtomicUsize = AtomicUsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);
pub struct Permit {
    counter: &'static AtomicUsize,
    amount: usize,
}
impl Permit {
    fn new(counter: &'static AtomicUsize, amount: usize, max: usize) -> Result<Self> {
        let mut old = counter.load(Ordering::Acquire);
        loop {
            let new = old
                .checked_add(amount)
                .filter(|n| *n <= max)
                .ok_or_else(|| status::limit("native resource admission exceeded"))?;
            match counter.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(Self { counter, amount }),
                Err(n) => old = n,
            }
        }
    }
    pub fn wire(n: usize) -> Result<Self> {
        Self::new(&WIRE, n, MAX_WIRE)
    }
    pub fn plan(n: usize) -> Result<Self> {
        Self::new(&PLANS, n, MAX_PLANS)
    }
    pub fn call() -> Result<Self> {
        Self::new(&CALLS, 1, MAX_CALLS)
            .map_err(|_| status::Failure::Simple(status::BUSY, "native work concurrency limit"))
    }
    pub fn shrink(&mut self, n: usize) {
        assert!(n <= self.amount);
        self.counter.fetch_sub(self.amount - n, Ordering::AcqRel);
        self.amount = n;
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.amount, Ordering::AcqRel);
    }
}
#[cfg(test)]
pub fn wire_used() -> usize {
    WIRE.load(Ordering::Acquire)
}
pub fn usage() -> (usize, usize, usize) {
    (
        WIRE.load(Ordering::Acquire),
        PLANS.load(Ordering::Acquire),
        CALLS.load(Ordering::Acquire),
    )
}
