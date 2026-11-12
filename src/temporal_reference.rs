//! Bounded, in-memory semantic reference for #196, not a storage or query API.
//! No persistence, distributed certification, retention eviction, or Gremlin syntax.
use std::collections::{BTreeMap, BTreeSet};

/// UTC microseconds since the Unix epoch; no implicit wall clock conversion.
pub type ValidTime = i64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Interval {
    pub from: ValidTime,
    pub to: Option<ValidTime>,
}
impl Interval {
    fn contains(self, t: ValidTime) -> bool {
        self.from <= t && self.to.is_none_or(|end| t < end)
    }
    fn overlaps(self, other: Self) -> bool {
        self.to.is_none_or(|end| other.from < end) && other.to.is_none_or(|end| self.from < end)
    }
}

/// Identity is local to this reference, never a reusable v1 qualified edge ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Vertex { value: String },
    Edge { from: u64, to: u64, value: String },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Key {
    Vertex(u64),
    Edge(u64),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub key: Key,
    pub valid: Interval,
    /// None asserts absence only over this interval; it does not erase history.
    pub record: Option<Record>,
}
#[derive(Clone, Debug)]
struct Revision {
    version: u64,
    change: Change,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalError {
    InvalidInterval,
    InvalidRecordKind,
    AmbiguousBatch,
    VersionOrder,
    UnavailableVersion,
    Limit,
    InvalidPath,
}

/// Hard reference-model limits, not limits on existing production graph APIs.
const MAX_REVISIONS: usize = 4096;
const MAX_VALUE_BYTES: usize = 4096;
const MAX_PATH_EDGES: usize = 64;

/// Append-only revisions indexed by logical identity. No whole-graph write copy.
#[derive(Default)]
pub struct TemporalReference {
    revisions: BTreeMap<Key, Vec<Revision>>,
    committed: BTreeSet<u64>,
    count: usize,
}
impl TemporalReference {
    /// Caller supplies a strictly increasing *already committed* system version.
    /// This model does not generate or certify distributed commit versions.
    /// Overlapping changes to one identity in a single batch fail atomically.
    pub fn append(&mut self, version: u64, changes: Vec<Change>) -> Result<(), TemporalError> {
        if version == 0 || self.committed.last().is_some_and(|v| version <= *v) {
            return Err(TemporalError::VersionOrder);
        }
        if changes.is_empty() || changes.len() > MAX_REVISIONS - self.count {
            return Err(TemporalError::Limit);
        }
        for (i, change) in changes.iter().enumerate() {
            if change.valid.to.is_some_and(|end| end <= change.valid.from) {
                return Err(TemporalError::InvalidInterval);
            }
            match (&change.key, &change.record) {
                (Key::Vertex(_), Some(Record::Vertex { value }))
                | (Key::Edge(_), Some(Record::Edge { value, .. })) => {
                    if value.len() > MAX_VALUE_BYTES {
                        return Err(TemporalError::Limit);
                    }
                }
                (_, None) => {}
                _ => return Err(TemporalError::InvalidRecordKind),
            }
            if changes[..i]
                .iter()
                .any(|prior| prior.key == change.key && prior.valid.overlaps(change.valid))
            {
                return Err(TemporalError::AmbiguousBatch);
            }
        }
        self.count += changes.len();
        for change in changes {
            self.revisions
                .entry(change.key)
                .or_default()
                .push(Revision { version, change });
        }
        self.committed.insert(version);
        Ok(())
    }

    /// Exact committed version only. Missing history never falls back to latest.
    pub fn view(&self, version: u64, time: ValidTime) -> Result<TemporalView<'_>, TemporalError> {
        if !self.committed.contains(&version) {
            return Err(TemporalError::UnavailableVersion);
        }
        Ok(TemporalView {
            graph: self,
            version,
            time,
        })
    }
}

/// Both coordinates are immutable for all lookups and path checks on this view.
pub struct TemporalView<'a> {
    graph: &'a TemporalReference,
    version: u64,
    time: ValidTime,
}
impl<'a> TemporalView<'a> {
    fn assertion(&self, key: Key) -> Option<&'a Record> {
        self.graph
            .revisions
            .get(&key)?
            .iter()
            .rev()
            .find(|r| r.version <= self.version && r.change.valid.contains(self.time))?
            .change
            .record
            .as_ref()
    }

    /// Edge visibility requires both endpoints at the *same* V/T. Dangling
    /// assertions may be recorded, but can never produce a query edge or path.
    pub fn get(&self, key: Key) -> Option<&'a Record> {
        let record = self.assertion(key)?;
        if let Record::Edge { from, to, .. } = record {
            self.assertion(Key::Vertex(*from))?;
            self.assertion(Key::Vertex(*to))?;
        }
        Some(record)
    }

    /// Validate a directed candidate path; not a traversal language or enumerator.
    /// Returns false for an absent edge/vertex, error for disconnected/oversize input.
    pub fn path_exists(&self, start: u64, edges: &[u64]) -> Result<bool, TemporalError> {
        if edges.len() > MAX_PATH_EDGES {
            return Err(TemporalError::Limit);
        }
        if self.get(Key::Vertex(start)).is_none() {
            return Ok(false);
        }
        let mut current = start;
        for id in edges {
            let Some(Record::Edge { from, to, .. }) = self.get(Key::Edge(*id)) else {
                return Ok(false);
            };
            if *from != current {
                return Err(TemporalError::InvalidPath);
            }
            current = *to;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn vertex(id: u64, from: i64, to: Option<i64>, value: &str) -> Change {
        Change {
            key: Key::Vertex(id),
            valid: Interval { from, to },
            record: Some(Record::Vertex {
                value: value.into(),
            }),
        }
    }
    fn edge(id: u64, from: u64, to: u64, begin: i64, end: Option<i64>) -> Change {
        Change {
            key: Key::Edge(id),
            valid: Interval {
                from: begin,
                to: end,
            },
            record: Some(Record::Edge {
                from,
                to,
                value: "link".into(),
            }),
        }
    }
    fn value(view: &TemporalView<'_>, id: u64) -> Option<String> {
        match view.get(Key::Vertex(id)) {
            Some(Record::Vertex { value }) => Some(value.clone()),
            _ => None,
        }
    }
    #[test]
    fn corrections_future_and_half_open_boundaries() {
        let mut graph = TemporalReference::default();
        graph
            .append(10, vec![vertex(1, 0, None, "original")])
            .unwrap();
        graph
            .append(
                20,
                vec![
                    vertex(1, 5, Some(10), "corrected"),
                    vertex(1, 20, None, "future"),
                ],
            )
            .unwrap();
        for (v, t, expected) in [
            (10, 5, "original"),
            (20, 4, "original"),
            (20, 5, "corrected"),
            (20, 9, "corrected"),
            (20, 10, "original"),
            (20, 19, "original"),
            (20, 20, "future"),
        ] {
            assert_eq!(
                value(&graph.view(v, t).unwrap(), 1).as_deref(),
                Some(expected)
            );
        }
        graph
            .append(30, vec![vertex(1, 6, Some(8), "second correction")])
            .unwrap();
        assert_eq!(
            value(&graph.view(20, 7).unwrap(), 1).as_deref(),
            Some("corrected")
        );
        assert_eq!(
            value(&graph.view(30, 7).unwrap(), 1).as_deref(),
            Some("second correction")
        );
        assert!(matches!(
            graph.view(21, 7),
            Err(TemporalError::UnavailableVersion)
        ));
    }
    #[test]
    fn paths_never_join_different_valid_times_and_endpoints() {
        let mut graph = TemporalReference::default();
        graph
            .append(
                1,
                vec![
                    vertex(1, 0, None, "a"),
                    vertex(2, 0, None, "b"),
                    vertex(3, 0, None, "c"),
                    edge(10, 1, 2, 0, Some(5)),
                    edge(11, 2, 3, 5, None),
                    edge(12, 1, 2, 0, None),
                ],
            )
            .unwrap();
        for t in 0..10 {
            assert!(!graph.view(1, t).unwrap().path_exists(1, &[10, 11]).unwrap());
        }
        assert!(graph.view(1, 5).unwrap().path_exists(1, &[12, 11]).unwrap());
        graph
            .append(
                2,
                vec![Change {
                    key: Key::Vertex(2),
                    valid: Interval {
                        from: 4,
                        to: Some(8),
                    },
                    record: None,
                }],
            )
            .unwrap();
        assert!(!graph.view(2, 5).unwrap().path_exists(1, &[12, 11]).unwrap());
        assert!(graph.view(1, 5).unwrap().path_exists(1, &[12, 11]).unwrap());
        assert!(graph.view(2, 8).unwrap().path_exists(1, &[12, 11]).unwrap());
        graph
            .append(
                3,
                vec![Change {
                    key: Key::Edge(12),
                    valid: Interval { from: 8, to: None },
                    record: None,
                }],
            )
            .unwrap();
        assert!(!graph.view(3, 8).unwrap().path_exists(1, &[12, 11]).unwrap());
        assert_eq!(
            graph.view(1, 6).unwrap().path_exists(1, &[11]),
            Err(TemporalError::InvalidPath)
        );
    }
    #[test]
    fn invalid_batch_is_atomic_and_limits_fail_closed() {
        let mut graph = TemporalReference::default();
        assert_eq!(
            graph.append(1, vec![vertex(1, 0, None, "a"), vertex(1, 2, None, "b")]),
            Err(TemporalError::AmbiguousBatch)
        );
        assert!(graph.committed.is_empty());
        assert_eq!(graph.count, 0);
        assert_eq!(
            graph.append(1, vec![vertex(1, 5, Some(5), "a")]),
            Err(TemporalError::InvalidInterval)
        );
        graph
            .append(1, vec![vertex(1, i64::MIN, None, "a")])
            .unwrap();
        assert_eq!(
            graph.append(1, vec![vertex(2, 0, None, "b")]),
            Err(TemporalError::VersionOrder)
        );
        assert_eq!(
            graph.append(
                2,
                vec![vertex(2, 0, None, &"x".repeat(MAX_VALUE_BYTES + 1))]
            ),
            Err(TemporalError::Limit)
        );
        assert_eq!(
            graph
                .view(1, 0)
                .unwrap()
                .path_exists(1, &[0; MAX_PATH_EDGES + 1]),
            Err(TemporalError::Limit)
        );
        graph
            .append(
                2,
                (2..=MAX_REVISIONS as u64)
                    .map(|id| vertex(id, 0, None, "v"))
                    .collect(),
            )
            .unwrap();
        assert_eq!(
            graph.append(3, vec![vertex(0, 0, None, "v")]),
            Err(TemporalError::Limit)
        );
        assert!(graph
            .view(2, i64::MAX)
            .unwrap()
            .get(Key::Vertex(1))
            .is_some());
    }
    #[test]
    fn adjacent_intervals_failed_batch_retry_and_identity_namespaces() {
        let mut graph = TemporalReference::default();
        graph
            .append(
                1,
                vec![
                    vertex(1, i64::MIN, None, "a"),
                    vertex(2, 0, None, "b"),
                    vertex(3, 0, None, "c"),
                    edge(1, 1, 2, 0, None),
                ],
            )
            .unwrap();
        let before = graph.count;
        let invalid = Change {
            key: Key::Edge(2),
            valid: Interval { from: 0, to: None },
            record: Some(Record::Vertex {
                value: "wrong kind".into(),
            }),
        };
        assert_eq!(
            graph.append(2, vec![vertex(1, 0, None, "must not write"), invalid]),
            Err(TemporalError::InvalidRecordKind)
        );
        assert_eq!(graph.count, before);
        assert!(matches!(
            graph.view(2, 0),
            Err(TemporalError::UnavailableVersion)
        ));
        graph
            .append(
                2,
                vec![
                    vertex(1, 0, Some(5), "left"),
                    vertex(1, 5, Some(i64::MAX), "right"),
                    edge(1, 1, 3, 0, None),
                ],
            )
            .unwrap();
        assert_eq!(
            value(&graph.view(2, 4).unwrap(), 1).as_deref(),
            Some("left")
        );
        assert_eq!(
            value(&graph.view(2, 5).unwrap(), 1).as_deref(),
            Some("right")
        );
        assert_eq!(
            value(&graph.view(2, i64::MAX).unwrap(), 1).as_deref(),
            Some("a")
        );
        assert!(matches!(
            graph.view(1, 0).unwrap().get(Key::Edge(1)),
            Some(Record::Edge { to: 2, .. })
        ));
        assert!(matches!(
            graph.view(2, 0).unwrap().get(Key::Edge(1)),
            Some(Record::Edge { to: 3, .. })
        ));
        assert_eq!(
            value(&graph.view(2, i64::MIN).unwrap(), 1).as_deref(),
            Some("a")
        );
    }

    #[test]
    fn independent_dense_two_dimensional_oracle() {
        // Oracle eagerly overwrites discrete valid-time cells, never resolves
        // revision chains. Each system row is retained independently.
        let mut graph = TemporalReference::default();
        let mut cells = [None; 16];
        let mut expected = Vec::new();
        for v in 1..=48u64 {
            let begin = ((v * 7) % 15) as usize;
            let end = (begin + 1 + ((v * 11) % (16 - begin) as u64) as usize).min(16);
            let deleted = v % 5 == 0;
            let change = Change {
                key: Key::Vertex(1),
                valid: Interval {
                    from: begin as i64,
                    to: Some(end as i64),
                },
                record: if deleted {
                    None
                } else {
                    Some(Record::Vertex {
                        value: v.to_string(),
                    })
                },
            };
            graph.append(v, vec![change]).unwrap();
            cells[begin..end].fill(if deleted { None } else { Some(v) });
            expected.push(cells);
        }
        for (i, row) in expected.iter().enumerate() {
            for (t, want) in row.iter().enumerate() {
                assert_eq!(
                    value(&graph.view(i as u64 + 1, t as i64).unwrap(), 1),
                    want.map(|v| v.to_string()),
                    "V={} T={t}",
                    i + 1
                );
            }
        }
    }
}
