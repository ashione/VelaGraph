use super::*;

// Keep small adjacency lists on their original row loop. Long lists retain the
// exact row order; summaries may only reject an entire impossible time range.
pub(super) const BLOCK_ROWS: usize = 32;
const MIN_INDEXED_ROWS: usize = 2 * BLOCK_ROWS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct AdjacencyBlock {
    first_start: i64,
    last_end: Option<i64>,
}
impl AdjacencyBlock {
    fn summarize(refs: &[u32], rows: &[Row]) -> Self {
        let mut first_start = i64::MAX;
        let mut last_end = Some(i64::MIN);
        for index in refs {
            let row = rows[*index as usize];
            first_start = first_start.min(row.start);
            last_end = match (last_end, row.has_end) {
                (Some(end), true) => Some(end.max(row.end)),
                _ => None,
            };
        }
        Self {
            first_start,
            last_end,
        }
    }
    pub(super) fn may_contain(self, time: i64) -> bool {
        self.first_start <= time && self.last_end.is_none_or(|end| time < end)
    }
}
pub(super) fn build(
    lists: &[Vec<u32>],
    rows: &[Row],
    meter: &mut Meter,
) -> Result<Vec<Vec<AdjacencyBlock>>> {
    meter.charge(lists.len() * std::mem::size_of::<Vec<AdjacencyBlock>>())?;
    let mut result = Vec::with_capacity(lists.len());
    for refs in lists {
        meter.check()?;
        let mut blocks = Vec::new();
        if refs.len() >= MIN_INDEXED_ROWS {
            let count = refs.len().div_ceil(BLOCK_ROWS);
            meter.charge(count * std::mem::size_of::<AdjacencyBlock>())?;
            blocks.reserve_exact(count);
            for chunk in refs.chunks(BLOCK_ROWS) {
                meter.check()?;
                blocks.push(AdjacencyBlock::summarize(chunk, rows));
            }
        }
        result.push(blocks);
    }
    Ok(result)
}
pub(super) fn retained(lists: &Vec<Vec<AdjacencyBlock>>) -> usize {
    lists.capacity() * std::mem::size_of::<Vec<AdjacencyBlock>>()
        + lists
            .iter()
            .map(|b| b.capacity() * std::mem::size_of::<AdjacencyBlock>())
            .sum::<usize>()
}
pub(super) fn validate(
    lists: &[Vec<u32>],
    blocks: &[Vec<AdjacencyBlock>],
    rows: &[Row],
    meter: &mut Meter,
) -> Result<()> {
    if lists.len() != blocks.len() {
        return Err(TemporalStoreError::Corrupt);
    }
    for (refs, summaries) in lists.iter().zip(blocks) {
        meter.check()?;
        let count = if refs.len() >= MIN_INDEXED_ROWS {
            refs.len().div_ceil(BLOCK_ROWS)
        } else {
            0
        };
        if summaries.len() != count {
            return Err(TemporalStoreError::Corrupt);
        }
        for (chunk, summary) in refs.chunks(BLOCK_ROWS).zip(summaries) {
            meter.check()?;
            if AdjacencyBlock::summarize(chunk, rows) != *summary {
                return Err(TemporalStoreError::Corrupt);
            }
        }
    }
    Ok(())
}
