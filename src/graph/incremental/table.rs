use super::*;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
const MIX_A: u64 = 0xbf58476d1ce4e5b9;
const MIX_B: u64 = 0x94d049bb133111eb;
fn shard(mut id: u64) -> usize {
    id = (id ^ (id >> 30)).wrapping_mul(MIX_A);
    id = (id ^ (id >> 27)).wrapping_mul(MIX_B);
    ((id ^ (id >> 31)) as usize) & (SHARDS - 1)
}
#[derive(Clone)]
struct Bucket<T: Clone> {
    map: HashMap<u64, T>,
    ids: Vec<u64>,
}
#[derive(Clone)]
pub(super) struct Table<T: Clone> {
    buckets: Vec<Arc<Bucket<T>>>,
}
impl<T: Clone> Table<T> {
    pub fn new() -> Self {
        Self {
            buckets: (0..SHARDS)
                .map(|_| {
                    Arc::new(Bucket {
                        map: HashMap::new(),
                        ids: Vec::new(),
                    })
                })
                .collect(),
        }
    }
    pub fn reserved_bytes(&self) -> usize {
        self.buckets
            .iter()
            .map(|b| b.map.capacity() * 2 * (std::mem::size_of::<T>() + 9) + b.ids.capacity() * 8)
            .sum()
    }
    pub fn get_mut(&mut self, id: u64) -> Option<&mut T> {
        Arc::make_mut(&mut self.buckets[shard(id)]).map.get_mut(&id)
    }
    pub fn get(&self, id: u64) -> Option<&T> {
        self.buckets[shard(id)].map.get(&id)
    }
    // Bulk construction owns every bucket and validates unique record IDs.
    // Edge input is ID-ordered; vertex input is finalized before publication.
    pub fn push_bulk(&mut self, id: u64, value: T) {
        let bucket = Arc::make_mut(&mut self.buckets[shard(id)]);
        let old = bucket.map.insert(id, value);
        debug_assert!(old.is_none(), "bulk input was checked for duplicate IDs");
        bucket.ids.push(id);
    }
    pub fn sort_bulk_ids(&mut self) {
        for bucket in &mut self.buckets {
            Arc::make_mut(bucket).ids.sort_unstable();
        }
    }
    pub fn set(&mut self, id: u64, value: Option<T>) -> usize {
        let bucket = &mut self.buckets[shard(id)];
        let copied = if Arc::strong_count(bucket) > 1 {
            bucket.map.len()
        } else {
            0
        };
        let b = Arc::make_mut(bucket);
        match value {
            Some(v) => {
                if b.map.insert(id, v).is_none() {
                    let i = b.ids.binary_search(&id).unwrap_err();
                    b.ids.insert(i, id);
                }
            }
            None => {
                if b.map.remove(&id).is_some() {
                    let i = b.ids.binary_search(&id).expect("record index invariant");
                    b.ids.remove(i);
                    if b.map.is_empty() {
                        b.map = HashMap::new();
                        b.ids = Vec::new();
                    } else {
                        if b.map.capacity() > b.map.len() * 2 {
                            b.map.shrink_to_fit();
                        }
                        if b.ids.capacity() > b.ids.len() * 2 {
                            b.ids.shrink_to_fit();
                        }
                    }
                }
            }
        };
        copied
    }
    pub fn mutation_entries(&self, changes: impl Iterator<Item = (u64, bool)>) -> usize {
        let mut counts = vec![0usize; SHARDS];
        for (id, present) in changes {
            let i = shard(id);
            if self.buckets[i].map.contains_key(&id) != present {
                counts[i] += 1;
            }
        }
        counts
            .into_iter()
            .enumerate()
            .map(|(i, n)| n.saturating_mul(self.buckets[i].map.len().saturating_add(n)))
            .fold(0, usize::saturating_add)
    }
    pub fn copy_entries(&self, ids: impl Iterator<Item = u64>) -> usize {
        let shards: std::collections::HashSet<_> = ids.map(shard).collect();
        shards.into_iter().map(|i| self.buckets[i].map.len()).sum()
    }
    pub fn sorted_ids(&self, limit: usize) -> Vec<u64> {
        let mut heap = BinaryHeap::new();
        for (i, b) in self.buckets.iter().enumerate() {
            if let Some(id) = b.ids.first() {
                heap.push(Reverse((*id, i, 0usize)));
            }
        }
        let mut out = Vec::with_capacity(limit.min(self.buckets.iter().map(|b| b.ids.len()).sum()));
        while out.len() < limit {
            let Some(Reverse((id, b, i))) = heap.pop() else {
                break;
            };
            out.push(id);
            if let Some(id) = self.buckets[b].ids.get(i + 1) {
                heap.push(Reverse((*id, b, i + 1)));
            }
        }
        out
    }
}
