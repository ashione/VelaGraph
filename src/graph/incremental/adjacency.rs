use super::*;
pub(super) const CHUNK: usize = 256;
#[derive(Clone, Default)]
pub(super) struct Adj {
    chunks: Vec<Arc<Vec<u64>>>,
    len: usize,
}
impl Adj {
    pub fn header_bytes(&self) -> usize {
        self.chunks.capacity() * std::mem::size_of::<Arc<Vec<u64>>>()
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.chunks.iter().flat_map(|c| c.iter().copied())
    }
    pub fn bytes(&self) -> usize {
        self.chunks.capacity() * std::mem::size_of::<Arc<Vec<u64>>>()
            + self
                .chunks
                .iter()
                .map(|c| c.capacity() * 8 + 64)
                .sum::<usize>()
    }
    pub fn append_sorted(&mut self, id: u64) {
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
    pub fn change(&mut self, id: u64, insert: bool) -> usize {
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
