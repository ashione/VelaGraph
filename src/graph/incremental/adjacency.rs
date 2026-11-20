use super::*;
pub(super) const CHUNK: usize = 256;
#[derive(Clone, Default)]
pub(super) struct Adj(Option<Arc<AdjChunks>>);

#[derive(Clone, Default)]
struct AdjChunks {
    chunks: Vec<Arc<Vec<u64>>>,
    len: usize,
}
impl Adj {
    pub fn header_bytes(&self) -> usize {
        self.0.as_ref().map_or(0, |a| {
            a.chunks.capacity() * std::mem::size_of::<Arc<Vec<u64>>>()
        })
    }
    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |a| a.len)
    }
    fn chunks(&self) -> &[Arc<Vec<u64>>] {
        self.0.as_ref().map_or(&[], |a| a.chunks.as_slice())
    }
    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.chunks().iter().flat_map(|c| c.iter().copied())
    }
    pub fn bytes(&self) -> usize {
        self.header_bytes()
            + self
                .chunks()
                .iter()
                .map(|c| c.capacity() * 8 + 64)
                .sum::<usize>()
    }
    pub fn append_sorted(&mut self, id: u64) {
        Arc::make_mut(self.0.get_or_insert_with(Arc::default)).append_sorted(id);
    }
    pub fn change(&mut self, id: u64, insert: bool) -> usize {
        if insert {
            return Arc::make_mut(self.0.get_or_insert_with(Arc::default)).change(id, true);
        }
        let Some(adj) = &mut self.0 else { return 0 };
        // Dropping the last reference from this version needs no chunk copy.
        if adj.len == 1 && adj.chunks[0][0] == id {
            self.0 = None;
            return 0;
        }
        Arc::make_mut(adj).change(id, false)
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
        if self.chunks.is_empty() {
            if insert {
                self.chunks.push(Arc::new(vec![id]));
                self.len = 1
            }
            return 0;
        }
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
}
