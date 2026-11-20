use super::*;
pub(super) const CHUNK: usize = 256;
#[derive(Clone, Default)]
pub(super) struct Adj(Option<Arc<AdjStorage>>);

#[derive(Clone)]
enum AdjStorage {
    One(u64),
    Many(AdjChunks),
}

#[derive(Clone)]
struct AdjChunks {
    chunks: Vec<Arc<Vec<u64>>>,
    len: usize,
}

impl Adj {
    pub fn header_bytes(&self) -> usize {
        match self.0.as_deref() {
            Some(AdjStorage::Many(a)) => a.chunks.capacity() * std::mem::size_of::<Arc<Vec<u64>>>(),
            _ => 0,
        }
    }
    pub fn len(&self) -> usize {
        match self.0.as_deref() {
            None => 0,
            Some(AdjStorage::One(_)) => 1,
            Some(AdjStorage::Many(a)) => a.len,
        }
    }
    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        let (single, chunks): (&[u64], &[Arc<Vec<u64>>]) = match self.0.as_deref() {
            None => (&[], &[]),
            Some(AdjStorage::One(id)) => (std::slice::from_ref(id), &[]),
            Some(AdjStorage::Many(a)) => (&[], &a.chunks),
        };
        let mut ids = single.iter();
        let mut chunks = chunks.iter();
        std::iter::from_fn(move || loop {
            if let Some(&id) = ids.next() {
                return Some(id);
            }
            ids = chunks.next()?.iter();
        })
    }
    pub fn bytes(&self) -> usize {
        match self.0.as_deref() {
            Some(AdjStorage::Many(a)) => {
                self.header_bytes()
                    + a.chunks
                        .iter()
                        .map(|c| c.capacity() * 8 + 64)
                        .sum::<usize>()
            }
            _ => 0,
        }
    }
    pub fn append_sorted(&mut self, id: u64) {
        let Some(storage) = &mut self.0 else {
            self.0 = Some(Arc::new(AdjStorage::One(id)));
            return;
        };
        let data = Arc::make_mut(storage);
        match data {
            AdjStorage::One(first) => {
                debug_assert!(*first < id);
                *data = AdjStorage::pair(*first, id);
            }
            AdjStorage::Many(a) => a.append_sorted(id),
        }
    }
    pub fn change(&mut self, id: u64, insert: bool) -> usize {
        let Some(storage) = &mut self.0 else {
            if insert {
                self.0 = Some(Arc::new(AdjStorage::One(id)));
            }
            return 0;
        };
        if let AdjStorage::One(old) = storage.as_ref() {
            if !insert {
                if *old == id {
                    self.0 = None;
                }
                return 0;
            }
            if *old == id {
                return 0;
            }
        }
        let data = Arc::make_mut(storage);
        match data {
            AdjStorage::One(old) => *data = AdjStorage::pair((*old).min(id), (*old).max(id)),
            AdjStorage::Many(a) if !insert && a.len == 2 => {
                // The remaining two IDs can lie in different chunks after removals.
                let first = a.chunks[0][0];
                let last = *a.chunks.last().unwrap().last().unwrap();
                if id == first || id == last {
                    *data = AdjStorage::One(if id == first { last } else { first });
                }
            }
            AdjStorage::Many(a) => return a.change(id, insert),
        }
        0
    }
}
impl AdjStorage {
    fn pair(first: u64, second: u64) -> Self {
        Self::Many(AdjChunks {
            chunks: vec![Arc::new(vec![first, second])],
            len: 2,
        })
    }
}

impl AdjChunks {
    fn append_sorted(&mut self, id: u64) {
        if self.chunks.last().is_none_or(|c| c.len() == CHUNK) {
            self.chunks.reserve_exact(1);
            self.chunks.push(Arc::new(Vec::new()));
        }
        let chunk = Arc::make_mut(self.chunks.last_mut().expect("allocated adjacency chunk"));
        debug_assert!(chunk.last().is_none_or(|old| *old < id));
        chunk.reserve_exact(1);
        chunk.push(id);
        self.len += 1;
    }
    fn change(&mut self, id: u64, insert: bool) -> usize {
        let mut i = self
            .chunks
            .partition_point(|c| c.last().is_some_and(|last| *last < id));
        if i == self.chunks.len() {
            if !insert {
                return 0;
            }
            i -= 1
        }
        let p = self.chunks[i].binary_search(&id);
        if p.is_ok() == insert {
            return 0;
        }
        let copied = if Arc::strong_count(&self.chunks[i]) > 1 {
            self.chunks[i].len()
        } else {
            0
        };
        let c = Arc::make_mut(&mut self.chunks[i]);
        if insert {
            c.insert(p.unwrap_err(), id);
            self.len += 1;
            if c.len() > CHUNK {
                let right = c.split_off(CHUNK / 2);
                self.chunks.insert(i + 1, Arc::new(right));
            }
        } else {
            c.remove(p.unwrap());
            self.len -= 1;
            if c.is_empty() {
                self.chunks.remove(i);
            }
        }
        copied
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_directions_release_storage_without_disturbing_pinned_readers() {
        let mut current = Adj::default();
        assert_eq!(current.change(5, false), 0);
        assert!(current.0.is_none());
        current.append_sorted(5);
        current.append_sorted(7);
        let pinned = current.clone();
        current.change(5, false);
        assert_eq!(current.change(5, false), 0);
        let one_edge = current.clone();
        assert_eq!(current.change(7, false), 0);
        assert!(current.0.is_none());
        assert_eq!(current.bytes(), 0);
        assert_eq!(current.ids().count(), 0);
        assert_eq!(one_edge.ids().collect::<Vec<_>>(), [7]);
        current.change(9, true);
        assert_eq!(current.ids().collect::<Vec<_>>(), [9]);
        assert_eq!(pinned.ids().collect::<Vec<_>>(), [5, 7]);
    }
    #[test]
    fn promotion_and_demotion_preserve_sorted_ids_and_pinned_chunks() {
        let mut current = Adj::default();
        current.change(u64::MAX, true);
        let single = current.clone();
        current.change(u64::MAX, true);
        current.change(0, false);
        assert!(Arc::ptr_eq(
            current.0.as_ref().unwrap(),
            single.0.as_ref().unwrap()
        ));
        current.change(0, true);
        for id in 1..=CHUNK as u64 {
            current.change(id, true);
        }
        let full = current.clone();
        for id in 1..=CHUNK as u64 {
            current.change(id, false);
        }
        assert_eq!(current.ids().collect::<Vec<_>>(), [0, u64::MAX]);
        current.change(42, false);
        current.change(u64::MAX, false);
        assert!(matches!(current.0.as_deref(), Some(AdjStorage::One(0))));
        current.change(0, false);
        assert!(current.0.is_none());
        assert_eq!(single.ids().collect::<Vec<_>>(), [u64::MAX]);
        assert!(full.ids().eq((0..=CHUNK as u64).chain([u64::MAX])));
    }
}
