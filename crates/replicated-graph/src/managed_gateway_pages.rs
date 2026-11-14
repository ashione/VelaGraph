//! Immutable session-scoped results. The session reservation covers at most
//! 1 MiB of serialized rows, 10,000 cursor pairs (two bounded tree indexes),
//! query identity and a page/encoding scratch copy. This is a conservative
//! logical charge, not allocator/RSS accounting. Unpaged traversal outputs are
//! transient and additionally bounded by the existing active-worker permits.
use super::*;

pub(super) const MAX_ROWS: usize = 10_000;
pub(super) const MAX_RESULT_BYTES: usize = 1024 * 1024;
pub const MAX_PAGE_SIZE: u16 = 1000;

/// Opaque random continuation identity; meaningful only in its issuing session.
/// It is not an authorization credential and contains no client-controlled offset.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageCursor {
    pub token: [u8; 32],
}

/// A contiguous slice of one fully evaluated, immutable traversal result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryPage {
    pub data: QueryData,
    pub next: Option<PageCursor>,
    pub offset: u32,
    pub total: u32,
}

pub(super) struct Materialized {
    pub(super) query: Query,
    pub(super) size: u16,
    pub(super) data: QueryData,
    pub(super) offsets: BTreeMap<PageCursor, usize>,
    pub(super) cursors: BTreeMap<usize, PageCursor>,
}
impl Materialized {
    pub(super) fn page(&self, offset: usize) -> QueryPage {
        let stop = offset
            .saturating_add(usize::from(self.size))
            .min(self.data.rows.len());
        QueryPage {
            data: QueryData {
                rows: self.data.rows[offset..stop].to_vec(),
                completeness: self.data.completeness,
            },
            next: self.cursors.get(&stop).cloned(),
            offset: offset as u32,
            total: self.data.rows.len() as u32,
        }
    }
}
fn live(retained: &Retained, end: Instant) -> Result<()> {
    if retained.closed.load(std::sync::atomic::Ordering::Acquire)
        || Instant::now() >= retained.expires
    {
        return Err(Error::SessionUnavailable);
    }
    retained.snapshot.version()?;
    before_deadline(end)
}
fn bounded(data: &QueryData) -> Result<()> {
    if data.rows.len() > MAX_ROWS {
        return Err(Error::Capacity);
    }
    encode(data, MAX_RESULT_BYTES)?;
    Ok(())
}
/// Request IDs are independent of result identity. Using maximal JSON byte
/// representations here proves every future replay envelope fits the frame.
fn preflight(binding: &Binding, reply: Reply) -> Result<()> {
    if matches!(&reply, Reply::TraversePage { .. }) {
        grpc_codec::preflight_identity(grpc_codec::encode_fixed_page(&reply)?, binding)?;
    }
    encode(
        &Response {
            version: VERSION,
            request_id: [255; 16],
            binding: binding.clone(),
            result: Ok(reply),
        },
        MAX_RESPONSE_BYTES,
    )?;
    Ok(())
}
pub(super) fn execute(
    retained: &Retained,
    session: Session,
    query: Query,
    paging: (Option<PageCursor>, Option<u16>),
    binding: &Binding,
    end: Instant,
) -> Result<Reply> {
    let (cursor, page_size) = paging;
    live(retained, end)?;
    if !binding
        .cohort
        .members
        .iter()
        .any(|m| m.shard == u64::from(query.partition))
    {
        return Err(Error::Identity);
    }
    // Reserve the only result slot before building; a concurrent first-page
    // request cannot build a second result or wait on an unbounded queue.
    let mut slot = retained.result.try_lock().map_err(|_| Error::Capacity)?;
    let Some(size) = page_size else {
        let data = query::execute_traversal(&retained.snapshot, &query, end)?;
        bounded(&data)?;
        live(retained, end)?;
        let reply = Reply::Traverse {
            session,
            query,
            data,
        };
        preflight(binding, reply.clone())?;
        live(retained, end)?;
        return Ok(reply);
    };
    if !(1..=MAX_PAGE_SIZE).contains(&size) {
        return Err(Error::Protocol);
    }
    if let Some(result) = slot.as_ref() {
        if result.query != query || result.size != size {
            return Err(Error::Identity);
        }
    } else {
        if cursor.is_some() {
            return Err(Error::Identity);
        }
        let data = query::execute_traversal(&retained.snapshot, &query, end)?;
        bounded(&data)?;
        let mut result = Materialized {
            query: query.clone(),
            size,
            data,
            offsets: BTreeMap::new(),
            cursors: BTreeMap::new(),
        };
        // At most MAX_ROWS-1 nonzero page starts. Independent random tokens
        // bind result identity as well as the session, query, size and version.
        for offset in (usize::from(size)..result.data.rows.len()).step_by(usize::from(size)) {
            live(retained, end)?;
            let token = PageCursor { token: random()? };
            if result.offsets.insert(token.clone(), offset).is_some() {
                return Err(Error::Unknown);
            }
            result.cursors.insert(offset, token);
        }
        // Validate every full wire response before admitting any result. This
        // prevents successful early pages followed by an oversized later page.
        for offset in (0..result.data.rows.len().max(1)).step_by(usize::from(size)) {
            live(retained, end)?;
            preflight(
                binding,
                Reply::TraversePage {
                    session: session.clone(),
                    query: query.clone(),
                    cursor: result.cursors.get(&offset).cloned(),
                    page_size: size,
                    page: result.page(offset),
                },
            )?;
        }
        live(retained, end)?;
        *slot = Some(result);
    }
    let result = slot.as_ref().ok_or(Error::Unknown)?;
    let offset = match &cursor {
        None => 0,
        Some(token) => *result.offsets.get(token).ok_or(Error::Identity)?,
    };
    let page = result.page(offset);
    live(retained, end)?;
    Ok(Reply::TraversePage {
        session,
        query,
        cursor,
        page_size: size,
        page,
    })
}

pub(super) fn valid_page(
    query: &Query,
    cursor: Option<&PageCursor>,
    size: u16,
    page: &QueryPage,
    binding: &Binding,
) -> bool {
    if !(1..=MAX_PAGE_SIZE).contains(&size)
        || page.total as usize > MAX_ROWS
        || !query::valid_traversal_total(query, page.total)
        || page.offset > page.total
        || !page.offset.is_multiple_of(u32::from(size))
        || cursor.is_none() != (page.offset == 0)
        || cursor.is_some_and(|c| c.token == [0; 32])
        || page
            .next
            .as_ref()
            .is_some_and(|c| c.token == [0; 32] || Some(c) == cursor)
    {
        return false;
    }
    let expected = (page.total - page.offset).min(u32::from(size)) as usize;
    page.data.rows.len() == expected
        && (page.total == 0 || page.offset < page.total)
        && page.next.is_some() == (page.offset + (expected as u32) < page.total)
        && query::valid_traversal_output(query, &page.data, binding)
        && encode(&page.data, MAX_RESULT_BYTES).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immutable_pages_preserve_duplicates_and_replay_tokens() {
        let next = PageCursor { token: [7; 32] };
        let result = Materialized {
            query: Query {
                partition: 0,
                text: "g.V(1).id()".into(),
            },
            size: 2,
            data: QueryData {
                rows: vec![QueryRow::Count(9), QueryRow::Count(9), QueryRow::Count(10)],
                completeness: QueryCompleteness::Unverified,
            },
            offsets: [(next.clone(), 2)].into(),
            cursors: [(2, next.clone())].into(),
        };
        assert_eq!(result.page(0), result.page(0));
        assert_eq!(result.page(0).data.rows.len(), 2);
        assert_eq!(result.page(0).next, Some(next.clone()));
        assert_eq!(result.offsets.get(&next), Some(&2));
        let final_page = result.page(2);
        assert_eq!(final_page.offset, 2);
        assert_eq!(final_page.total, 3);
        assert_eq!(final_page.data.rows, vec![QueryRow::Count(10)]);
        assert_eq!(final_page.next, None);
        assert!(!result.offsets.contains_key(&PageCursor { token: [8; 32] }));
    }

    #[test]
    fn empty_result_is_one_final_page() {
        let result = Materialized {
            query: Query {
                partition: 0,
                text: "g.V(1)".into(),
            },
            size: 1,
            data: QueryData {
                rows: vec![],
                completeness: QueryCompleteness::Unverified,
            },
            offsets: BTreeMap::new(),
            cursors: BTreeMap::new(),
        };
        let page = result.page(0);
        assert_eq!((page.offset, page.total, page.next), (0, 0, None));
        assert!(page.data.rows.is_empty());
    }

    #[test]
    fn cursor_wire_rejects_extra_fields_and_wrong_lengths() {
        let token = PageCursor { token: [1; 32] };
        let mut wire = serde_json::to_value(&token).unwrap();
        wire.as_object_mut()
            .unwrap()
            .insert("offset".into(), 1.into());
        assert!(serde_json::from_value::<PageCursor>(wire).is_err());
        assert!(serde_json::from_str::<PageCursor>(r#"{"token":[1]}"#).is_err());
    }

    #[test]
    fn whole_result_row_and_byte_caps_fail_closed() {
        let mut data = QueryData {
            rows: vec![QueryRow::Count(1); MAX_ROWS],
            completeness: QueryCompleteness::Unverified,
        };
        assert!(bounded(&data).is_ok());
        data.rows.push(QueryRow::Count(1));
        assert_eq!(bounded(&data), Err(Error::Capacity));
        data.rows = vec![QueryRow::Value {
            source: None,
            value: Value::Text("x".repeat(MAX_RESULT_BYTES)),
        }];
        assert_eq!(bounded(&data), Err(Error::Capacity));
    }
}
