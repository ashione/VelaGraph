//! Adapter over an already fixed D1 view, not a third database or coordinator.
//! Global certification belongs to the owner of that view (managed SnapshotSession).
use super::*;
use crate::{KeyRange, ReadView, ScanLimit, WriteOp, WriteRequest};

/// Local temporal head in a certified state-machine snapshot. This is neither
/// its D1 sequence nor its global cohort version. Untouched shards need not advance.
pub fn local_version(view: &ReadView) -> Result<u64> {
    match view.get(FORMAT_KEY) {
        Some(format) if format == [2] => decode_version(view.get(HEAD_KEY).as_deref()),
        Some(_) => Err(TemporalStoreError::IncompatibleStore),
        None => {
            // Refuse damaged/orphaned data, not silently initialize over it.
            let rows = view.scan_page(
                KeyRange::new(b"graph/temporal/", b"graph/temporal0")?,
                ScanLimit::new(1, MAX_READ_BYTES)?,
                None,
            )?;
            if rows.rows.is_empty() {
                Ok(0)
            } else {
                Err(TemporalStoreError::Corrupt)
            }
        }
    }
}

/// Prepare vertex-only assertions in the existing managed database. The caller
/// must submit this request with the returned original sequence CAS, via the
/// existing transaction protocol. No commit or independent RocksDB write occurs.
/// Cross-partition edge identity/endpoint certification is not implemented here.
pub fn prepare_vertices(
    view: &ReadView,
    mutations: &[TemporalMutation],
    request_id: [u8; 16],
) -> Result<(WriteRequest, u64)> {
    if mutations.is_empty()
        || request_id == [0; 16]
        || mutations.iter().any(|m| {
            !matches!(
                m,
                TemporalMutation::Vertex { .. }
                    | TemporalMutation::Absent {
                        key: Key::Vertex(_),
                        ..
                    }
            )
        })
    {
        return Err(TemporalStoreError::IncompatibleStore);
    }
    let encoded = encode_batch(mutations)?;
    let version = local_version(view)?
        .checked_add(1)
        .ok_or(TemporalStoreError::Limit)?;
    let mut operations = Vec::with_capacity(encoded.len() + 2);
    for (ordinal, (mutation, value)) in mutations.iter().zip(encoded).enumerate() {
        operations.push(WriteOp::put(
            revision_key(mutation.key(), version, ordinal as u32),
            value,
        ));
    }
    operations.push(WriteOp::put(FORMAT_KEY, [2]));
    operations.push(WriteOp::put(HEAD_KEY, version.to_be_bytes()));
    Ok((
        WriteRequest {
            shard: view.shard,
            epoch: view.epoch,
            request_id,
            operations,
        },
        view.sequence,
    ))
}

/// Resolve a vertex at T from exactly this immutable snapshot. Bounded identity
/// scan and the shared canonical revision codec are reused; no whole-graph copy.
pub fn vertex(view: &ReadView, valid_time: i64, id: u64) -> Result<Option<Vertex>> {
    let version = local_version(view)?;
    let prefix = identity_prefix(Key::Vertex(id));
    let mut end = prefix.clone();
    // identity prefixes end in a fixed-width ID, including all-ones IDs.
    while end.last() == Some(&255) {
        end.pop();
    }
    if let Some(last) = end.last_mut() {
        *last += 1;
    }
    let range = KeyRange::new(revision_key(Key::Vertex(id), version, 0), end)?;
    let mut cursor = None;
    let mut bytes = 0usize;
    for _ in 0..MAX_READ_REVISIONS {
        let page = view.scan_page(
            range.clone(),
            ScanLimit::new(1, MAX_READ_BYTES)?,
            cursor.as_ref(),
        )?;
        let Some(row) = page.rows.first() else {
            return Ok(None);
        };
        bytes = bytes
            .checked_add(row.key.len() + row.value.len())
            .ok_or(TemporalStoreError::Limit)?;
        if bytes > MAX_READ_BYTES {
            return Err(TemporalStoreError::Limit);
        }
        let (valid, payload) = decode_revision(&row.key, &row.value, version)?;
        if valid.from <= valid_time && valid.to.is_none_or(|end| valid_time < end) {
            let Some(payload) = payload else {
                return Ok(None);
            };
            let vertex = codec::decode_vertex(payload)?;
            if vertex.id != id {
                return Err(TemporalStoreError::Corrupt);
            }
            validate_vertex(&vertex)?;
            return Ok(Some(vertex));
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(None);
        }
    }
    Err(TemporalStoreError::Limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LocalStorageNode, StorageNode};
    use std::collections::BTreeMap;
    fn mutation(id: u64, from: i64, to: Option<i64>) -> TemporalMutation {
        TemporalMutation::Vertex {
            valid: Interval { from, to },
            vertex: Vertex {
                id,
                labels: vec!["entity".into()],
                properties: BTreeMap::new(),
            },
        }
    }
    #[test]
    fn managed_temporal_reopen_cas_original_retry_and_bounds() {
        let dir = tempfile::tempdir().unwrap();
        {
            let node = LocalStorageNode::open(dir.path(), 7).unwrap();
            let before = node.read_view().unwrap();
            let (request, seq) =
                prepare_vertices(&before, &[mutation(u64::MAX, 10, Some(20))], [1; 16]).unwrap();
            node.batch_write_conditional(request.clone(), Some(seq))
                .unwrap();
            node.batch_write_conditional(request, Some(seq)).unwrap();
            let stale = prepare_vertices(&before, &[mutation(1, 0, None)], [2; 16]).unwrap();
            assert!(node
                .batch_write_conditional(stale.0, Some(stale.1))
                .is_err());
            assert!(vertex(&before, 15, u64::MAX).unwrap().is_none());
        }
        let node = LocalStorageNode::open(dir.path(), 7).unwrap();
        let view = node.read_view().unwrap();
        assert_eq!(local_version(&view).unwrap(), 1);
        assert!(vertex(&view, 15, u64::MAX).unwrap().is_some());
        assert!(vertex(&view, 20, u64::MAX).unwrap().is_none());
        assert!(prepare_vertices(&view, &[], [3; 16]).is_err());
        assert!(prepare_vertices(&view, &[mutation(1, 20, Some(10))], [3; 16]).is_err());
        assert!(prepare_vertices(&view, &vec![mutation(1, 0, None); 257], [3; 16]).is_err());
        assert!(prepare_vertices(
            &view,
            &[mutation(1, 0, None), mutation(1, 0, None)],
            [3; 16]
        )
        .is_err());
        assert!(prepare_vertices(
            &view,
            &[TemporalMutation::Absent {
                valid: Interval { from: 0, to: None },
                key: Key::Edge(1)
            }],
            [3; 16]
        )
        .is_err());
    }
    #[test]
    fn managed_temporal_damaged_namespace_never_becomes_empty() {
        for entries in [
            BTreeMap::from([(HEAD_KEY.to_vec(), 0u64.to_be_bytes().to_vec())]),
            BTreeMap::from([(FORMAT_KEY.to_vec(), vec![2])]),
            BTreeMap::from([(FORMAT_KEY.to_vec(), vec![3])]),
            BTreeMap::from([(b"graph/temporal/v1/old".to_vec(), vec![1])]),
        ] {
            let view = ReadView::from_entries(7, 1, 10, entries).unwrap();
            assert!(local_version(&view).is_err());
            assert!(prepare_vertices(&view, &[mutation(1, 0, None)], [1; 16]).is_err());
        }
    }
    #[test]
    fn managed_temporal_revision_limit_is_error_not_absence() {
        let mut entries = BTreeMap::from([
            (FORMAT_KEY.to_vec(), vec![2]),
            (HEAD_KEY.to_vec(), 4097u64.to_be_bytes().to_vec()),
        ]);
        for version in 1..=4097 {
            entries.insert(
                revision_key(Key::Vertex(1), version, 0),
                encode_assertion(Interval { from: 10, to: None }, None).unwrap(),
            );
        }
        let view = ReadView::from_entries(7, 1, 4097, entries).unwrap();
        assert!(matches!(
            vertex(&view, 0, 1),
            Err(TemporalStoreError::Limit)
        ));
        assert!(vertex(&view, 10, 1).unwrap().is_none());
    }
}
