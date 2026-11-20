use crate::{
    bounds::{Permit, MAX_HANDLES},
    engine::Engine,
    status::{self, Result},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};
use velagraph::{CancellationToken, PreparedGremlinQuery};
pub struct Buffer {
    pub bytes: Vec<u8>,
    pub _permit: Permit,
}
pub struct Plan {
    pub query: PreparedGremlinQuery,
    pub _permit: Permit,
}
pub enum Resource {
    Pending,
    Store(Engine),
    Control(CancellationToken),
    Plan(Plan),
    Buffer(Buffer),
}
struct Registry {
    next: u64,
    entries: HashMap<u64, Arc<Resource>>,
}
fn registry() -> &'static Mutex<Registry> {
    static R: OnceLock<Mutex<Registry>> = OnceLock::new();
    R.get_or_init(|| {
        Mutex::new(Registry {
            next: 1,
            entries: HashMap::new(),
        })
    })
}
pub struct Slot {
    id: u64,
    done: bool,
}
impl Slot {
    pub fn reserve() -> Result<Self> {
        let mut r = registry()
            .lock()
            .map_err(|_| status::Failure::Simple(status::INTERNAL, "registry unavailable"))?;
        if r.entries.len() >= MAX_HANDLES {
            return Err(status::limit("native handle limit"));
        }
        let id = r.next;
        r.next = r
            .next
            .checked_add(1)
            .ok_or_else(|| status::limit("handle identifiers exhausted"))?;
        r.entries.insert(id, Arc::new(Resource::Pending));
        Ok(Self { id, done: false })
    }
    pub fn publish(mut self, resource: Resource) -> Result<u64> {
        let resource = Arc::new(resource);
        {
            let mut r = registry()
                .lock()
                .map_err(|_| status::Failure::Simple(status::INTERNAL, "registry unavailable"))?;
            let entry = r
                .entries
                .get_mut(&self.id)
                .ok_or(status::Failure::Simple(status::HANDLE, "retired handle"))?;
            if !matches!(entry.as_ref(), Resource::Pending) {
                return Err(status::Failure::Simple(
                    status::INTERNAL,
                    "invalid reservation",
                ));
            }
            *entry = resource;
        }
        self.done = true;
        Ok(self.id)
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        if !self.done {
            let removed = registry()
                .lock()
                .ok()
                .and_then(|mut r| r.entries.remove(&self.id));
            drop(removed);
        }
    }
}
pub fn get(id: u64) -> Result<Arc<Resource>> {
    registry()
        .lock()
        .map_err(|_| status::Failure::Simple(status::INTERNAL, "registry unavailable"))?
        .entries
        .get(&id)
        .cloned()
        .ok_or(status::Failure::Simple(
            status::HANDLE,
            "invalid or retired handle",
        ))
}
pub fn release(id: u64) -> Result<()> {
    let removed = {
        let mut r = registry()
            .lock()
            .map_err(|_| status::Failure::Simple(status::INTERNAL, "registry unavailable"))?;
        if matches!(r.entries.get(&id).map(Arc::as_ref), Some(Resource::Pending)) {
            return Err(status::Failure::Simple(
                status::HANDLE,
                "unpublished handle",
            ));
        }
        r.entries.remove(&id).ok_or(status::Failure::Simple(
            status::HANDLE,
            "invalid or retired handle",
        ))?
    };
    drop(removed);
    Ok(())
}
#[cfg(test)]
pub fn count() -> usize {
    registry().lock().unwrap().entries.len()
}
pub fn registered() -> Result<usize> {
    Ok(registry()
        .lock()
        .map_err(|_| status::Failure::Simple(status::INTERNAL, "registry unavailable"))?
        .entries
        .len())
}
