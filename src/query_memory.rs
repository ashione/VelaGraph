//! Query-local logical accounting, not an allocator or process memory limit.
use crate::{Error, Result};
use std::cell::Cell;
use std::rc::Rc;

pub(crate) const RECORD_BASE: usize = 128;
pub(crate) const MAP_ENTRY: usize = 128;
pub(crate) const PATH_ITEM: usize = 16;
pub(crate) const CONTROL_BYTES: usize = 4096;

fn exhausted() -> Error {
    Error::TraversalLimitExceeded("query accounted byte budget exceeded".into())
}
pub(crate) fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(exhausted)
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(exhausted)
}
#[derive(Debug)]
struct State {
    limit: usize,
    used: Cell<usize>,
    peak: Cell<usize>,
}
#[derive(Debug, Clone)]
pub(crate) struct Budget(Rc<State>);
impl Budget {
    pub(crate) fn new(limit: usize) -> Self {
        Self(Rc::new(State {
            limit,
            used: Cell::new(0),
            peak: Cell::new(0),
        }))
    }
    pub(crate) fn available(&self) -> usize {
        self.0.limit - self.0.used.get()
    }
    pub(crate) fn reserve(&self, bytes: usize) -> Result<Reservation> {
        let used = add(self.0.used.get(), bytes)?;
        if used > self.0.limit {
            return Err(Error::TraversalLimitExceeded(format!(
                "query accounted byte budget exceeded: {} used + {bytes} requested > {} limit",
                self.0.used.get(),
                self.0.limit
            )));
        }
        self.0.used.set(used);
        self.0.peak.set(self.0.peak.get().max(used));
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.0.used.get()
    }
    #[cfg(test)]
    pub(crate) fn peak(&self) -> usize {
        self.0.peak.get()
    }
}
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Budget,
    bytes: usize,
}
impl Reservation {
    pub(crate) fn grow(&mut self, bytes: usize) -> Result<()> {
        let target = add(self.bytes, bytes)?;
        let mut extra = self.budget.reserve(bytes)?;
        extra.bytes = 0;
        self.bytes = target;
        Ok(())
    }
    pub(crate) fn shrink(&mut self, bytes: usize) {
        assert!(bytes <= self.bytes);
        self.budget
            .0
            .used
            .set(self.budget.0.used.get() - (self.bytes - bytes));
        self.bytes = bytes;
    }
    pub(crate) fn split(&mut self, bytes: usize) -> Reservation {
        assert!(bytes <= self.bytes);
        self.bytes -= bytes;
        Reservation {
            budget: self.budget.clone(),
            bytes,
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget
            .0
            .used
            .set(self.budget.0.used.get() - self.bytes);
    }
}
#[derive(Debug)]
pub(crate) struct Accounted<T> {
    pub(crate) value: T,
    pub(crate) charge: Reservation,
}
impl<T> Accounted<T> {
    pub(crate) fn new(value: T, charge: Reservation) -> Self {
        Self { value, charge }
    }
    pub(crate) fn map<U>(self, f: impl FnOnce(T) -> U) -> Accounted<U> {
        Accounted {
            value: f(self.value),
            charge: self.charge,
        }
    }
}
impl<T> std::ops::Deref for Accounted<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

/// Slots are charged independently of payloads. The slot token survives moves
/// into an iterator, even after items have been consumed, and truncate retains it.
/// Capacity means requested slots: allocator rounding/reallocation scratch is
/// deliberately excluded from this logical contract.
pub(crate) struct AccountedVec<T> {
    values: Vec<T>,
    slots: Reservation,
    capacity: usize,
}
impl<T> AccountedVec<T> {
    pub(crate) fn new(budget: &Budget) -> Self {
        Self {
            values: Vec::new(),
            slots: budget.reserve(0).expect("zero reservation"),
            capacity: 0,
        }
    }
    pub(crate) fn push(&mut self, value: T) -> Result<()> {
        if self.values.len() == self.capacity {
            let capacity = if self.capacity == 0 {
                4
            } else {
                mul(self.capacity, 2)?
            };
            self.slots
                .grow(mul(capacity - self.capacity, std::mem::size_of::<T>())?)?;
            // Reservation precedes allocation. Rust allocator failures remain
            // outside this logical budget, as with the rest of the crate.
            self.values.reserve_exact(capacity - self.values.len());
            self.capacity = capacity;
        }
        self.values.push(value);
        Ok(())
    }
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }
    pub(crate) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.values.iter()
    }
    pub(crate) fn truncate(&mut self, len: usize) {
        self.values.truncate(len);
    }
    pub(crate) fn into_parts(self) -> (Vec<T>, Reservation) {
        (self.values, self.slots)
    }
}
pub(crate) struct AccountedIntoIter<T> {
    values: std::vec::IntoIter<T>,
    _slots: Reservation,
}
impl<T> IntoIterator for AccountedVec<T> {
    type Item = T;
    type IntoIter = AccountedIntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        AccountedIntoIter {
            values: self.values.into_iter(),
            _slots: self.slots,
        }
    }
}
impl<T> Iterator for AccountedIntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.values.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_boundary_overflow_and_failure_release() {
        let b = Budget::new(12);
        let mut r = b.reserve(10).unwrap();
        r.grow(2).unwrap();
        assert_eq!(b.used(), 12);
        assert!(r.grow(1).is_err());
        assert!(r.grow(usize::MAX).is_err());
        assert!(mul(usize::MAX, 2).is_err());
        assert_eq!(b.used(), 12);
        r.shrink(3);
        let s = b.reserve(9).unwrap();
        drop(r);
        drop(s);
        assert_eq!(b.used(), 0);
        assert_eq!(b.peak(), 12);
    }
    #[test]
    fn slots_survive_truncate_and_consumed_iterator() {
        let b = Budget::new(1024);
        let mut v = AccountedVec::new(&b);
        v.push(1u64).unwrap();
        v.push(2).unwrap();
        let bytes = b.used();
        v.truncate(1);
        assert_eq!(b.used(), bytes);
        let mut i = v.into_iter();
        assert_eq!(i.next(), Some(1));
        assert_eq!(b.used(), bytes);
        assert_eq!(i.next(), None);
        assert_eq!(b.used(), bytes);
        drop(i);
        assert_eq!(b.used(), 0);
    }
}
