//! Derived durable terminal segments. The complete Journal remains authoritative.
//! There is no independent admission quota, history truncation or ordinal reset.
use crate::d1::D1State;
use crate::participant_writes::Authority;
use crate::terminal_prefix::{Terminal, TerminalPrefix, SEGMENT_TRANSACTIONS};
use crate::transaction_decisions::{Cohort, Journal};
use crate::{D1Error, NodeId};
use openraft::LogId;
use rocksdb::{Direction, IteratorMode, WriteBatch, DB};
use serde::{Deserialize, Serialize};

pub(crate) const PREFIX: &[u8] = b"sm/decision/";
pub(crate) const PREFIX_END: &[u8] = b"sm/decision0";
pub(crate) const MANIFEST_KEY: &[u8] = b"sm/decision-manifest";
const MAX_SEGMENT_BYTES: usize = 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Domain {
    role: Role,
    shard: u64,
    epoch: u64,
    mode: [u8; 16],
    authority: Authority,
    cohort: Cohort,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Role {
    Coordinator,
    Member,
}
struct View<'a> {
    domain: Domain,
    journal: &'a Journal,
    through: u64,
}
impl<'a> View<'a> {
    fn of(state: &'a D1State) -> Option<Self> {
        let (role, journal, through) = if let Some(journal) = &state.coordinator {
            (Role::Coordinator, journal, journal.resolved_prefix())
        } else {
            let visibility = state.visibility.as_ref()?;
            (Role::Member, &visibility.journal, visibility.applied)
        };
        let cohort = journal.cohort()?.clone();
        Some(Self {
            domain: Domain {
                role,
                shard: state.shard,
                epoch: state.epoch,
                mode: state.managed.as_ref()?.mode_id,
                authority: Authority {
                    shard: cohort.coordinator_shard,
                    mode: journal.authority(),
                },
                cohort,
            },
            journal,
            through,
        })
    }
    fn key(&self, first: u64) -> Vec<u8> {
        let mut key = PREFIX.to_vec();
        key.extend_from_slice(&1u32.to_be_bytes());
        key.push(match self.domain.role {
            Role::Coordinator => 0,
            Role::Member => 1,
        });
        key.extend_from_slice(&self.domain.shard.to_be_bytes());
        key.extend_from_slice(&self.domain.epoch.to_be_bytes());
        key.extend_from_slice(&self.domain.mode);
        key.extend_from_slice(&self.domain.authority.shard.to_be_bytes());
        key.extend_from_slice(&self.domain.authority.mode);
        key.extend_from_slice(&self.domain.cohort.coordinator_epoch.to_be_bytes());
        key.extend_from_slice(&first.to_be_bytes());
        key
    }
    fn row(&self, first: u64) -> anyhow::Result<Row> {
        anyhow::ensure!(
            first != 0
                && (first - 1).is_multiple_of(SEGMENT_TRANSACTIONS as u64)
                && first <= self.through,
            "invalid absolute decision segment start"
        );
        let last = first
            .checked_add(SEGMENT_TRANSACTIONS as u64 - 1)
            .ok_or_else(|| anyhow::anyhow!("decision ordinal overflow"))?
            .min(self.through);
        let records = (first..=last)
            .map(|ordinal| {
                let tx = self
                    .journal
                    .transaction_at(ordinal)
                    .ok_or_else(|| anyhow::anyhow!("missing decision ordinal"))?;
                anyhow::ensure!(tx.ordinal == ordinal, "decision ordinal mismatch");
                Ok(Terminal {
                    id: tx.id,
                    ordinal,
                    participants: tx.participants.clone(),
                    prepared: tx.prepared.clone(),
                    decision: tx
                        .decision
                        .ok_or_else(|| anyhow::anyhow!("unresolved decision"))?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Row {
            version: 1,
            domain: self.domain.clone(),
            first,
            records,
        })
    }
}
pub(crate) fn enabled(state: &D1State) -> bool {
    View::of(state).is_some()
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    version: u32,
    domain: Domain,
    first: u64,
    records: Vec<Terminal>,
}
impl Row {
    fn bytes(&self) -> anyhow::Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)?;
        anyhow::ensure!(
            bytes.len() <= MAX_SEGMENT_BYTES,
            "decision segment exceeds bound"
        );
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() <= MAX_SEGMENT_BYTES,
            "decision segment exceeds bound"
        );
        let row: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(
            row.version == 1 && row.bytes()? == bytes,
            "noncanonical decision segment"
        );
        Ok(row)
    }
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    domain: Domain,
    through: u64,
    segments: u64,
    encoded_bytes: u64,
    applied: Option<LogId<NodeId>>,
}
impl Manifest {
    fn of(view: &View<'_>, applied: Option<LogId<NodeId>>) -> anyhow::Result<Self> {
        let mut encoded_bytes = 0u64;
        for first in (1..=view.through).step_by(SEGMENT_TRANSACTIONS) {
            encoded_bytes = encoded_bytes
                .checked_add((view.key(first).len() + view.row(first)?.bytes()?.len()) as u64)
                .ok_or_else(|| anyhow::anyhow!("decision size overflow"))?;
        }
        Ok(Self {
            version: 1,
            domain: view.domain.clone(),
            through: view.through,
            segments: view.through.div_ceil(SEGMENT_TRANSACTIONS as u64),
            encoded_bytes,
            applied,
        })
    }
}

/// Full strict validation on reopen; a legacy format cannot carry orphan rows.
pub(crate) fn validate(
    db: &DB,
    state: Option<&D1State>,
    applied: Option<LogId<NodeId>>,
    indexed: bool,
) -> anyhow::Result<()> {
    let view = state.and_then(View::of);
    let manifest = db.get(MANIFEST_KEY)?;
    let mut count = 0u64;
    let mut encoded_bytes = 0u64;
    for item in db.iterator(IteratorMode::From(PREFIX, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(PREFIX) {
            break;
        }
        anyhow::ensure!(indexed, "orphan or mixed-format decision segment");
        let view = view
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("decision domain missing"))?;
        let first = count
            .checked_mul(SEGMENT_TRANSACTIONS as u64)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| anyhow::anyhow!("decision ordinal overflow"))?;
        let expected = view.row(first)?;
        anyhow::ensure!(
            key.as_ref() == view.key(first) && Row::decode(&value)? == expected,
            "decision segment disagrees with authority"
        );
        count += 1;
        encoded_bytes += (key.len() + value.len()) as u64;
    }
    if indexed {
        let view = view.ok_or_else(|| anyhow::anyhow!("indexed decision state missing"))?;
        let bytes = manifest.ok_or_else(|| anyhow::anyhow!("decision manifest missing"))?;
        anyhow::ensure!(
            bytes.len() <= MAX_MANIFEST_BYTES,
            "decision manifest exceeds bound"
        );
        let expected = Manifest::of(&view, applied)?;
        let actual: Manifest = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            actual == expected && serde_json::to_vec(&actual)? == bytes,
            "decision manifest disagrees with authority"
        );
        anyhow::ensure!(
            count == actual.segments && encoded_bytes == actual.encoded_bytes,
            "missing or extra decision segment"
        );
    } else {
        anyhow::ensure!(
            manifest.is_none(),
            "orphan or mixed-format decision manifest"
        );
    }
    Ok(())
}

/// Append new segments and replace only a growing partial tail in the same batch.
/// Snapshot install and explicit legacy migration rebuild the complete derived set.
pub(crate) fn stage(
    batch: &mut WriteBatch,
    before: Option<&D1State>,
    next: Option<&D1State>,
    applied: Option<LogId<NodeId>>,
    indexed: bool,
    replace: bool,
) -> anyhow::Result<Option<Vec<u8>>> {
    if replace {
        batch.delete_range(PREFIX, PREFIX_END);
        batch.delete(MANIFEST_KEY);
    }
    let Some(view) = next.and_then(View::of) else {
        return Ok(None);
    };
    let start = if indexed && !replace {
        let before = before
            .and_then(View::of)
            .ok_or_else(|| anyhow::anyhow!("decision history missing"))?;
        anyhow::ensure!(
            before.domain == view.domain && before.through <= view.through,
            "decision history changed"
        );
        // A complete previous tail is immutable; only a growing partial tail
        // is replaced. Commands which do not advance the cut write no rows.
        if before.through == view.through {
            view.through
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("decision ordinal overflow"))?
        } else {
            before.through / SEGMENT_TRANSACTIONS as u64 * SEGMENT_TRANSACTIONS as u64 + 1
        }
    } else {
        1
    };
    for first in (start..=view.through).step_by(SEGMENT_TRANSACTIONS) {
        batch.put(view.key(first), view.row(first)?.bytes()?);
    }
    let manifest = serde_json::to_vec(&Manifest::of(&view, applied)?)?;
    batch.put(MANIFEST_KEY, &manifest);
    Ok(Some(manifest))
}

fn checked_manifest(db: &DB, cached: &[u8], view: &View<'_>) -> Result<Manifest, D1Error> {
    if db
        .get(MANIFEST_KEY)
        .map_err(|_| D1Error::DecisionIndexUnavailable)?
        .as_deref()
        != Some(cached)
    {
        return Err(D1Error::DecisionIndexHistory);
    }
    let manifest: Manifest =
        serde_json::from_slice(cached).map_err(|_| D1Error::DecisionIndexHistory)?;
    if manifest.version != 1 || manifest.domain != view.domain || manifest.through != view.through {
        return Err(D1Error::DecisionIndexHistory);
    }
    Ok(manifest)
}
/// Summary reads the atomically published manifest, without fetching any segment.
pub(crate) fn frontier(state: &D1State, index: Option<(&DB, &[u8])>) -> Result<u64, D1Error> {
    let view = View::of(state).ok_or(D1Error::DecisionIndexHistory)?;
    match index {
        Some((db, cached)) => Ok(checked_manifest(db, cached, &view)?.through),
        None => Ok(view.through), // Only explicit legacy or memory-only callers.
    }
}
/// Fetch only segments intersecting 1..=version, checking every loaded row against
/// that exact authority. An old partial tail is trimmed without changing old V.
pub(crate) fn prefix(
    state: &D1State,
    version: u64,
    index: Option<(&DB, &[u8])>,
) -> Result<TerminalPrefix, D1Error> {
    let view = View::of(state).ok_or(D1Error::DecisionIndexHistory)?;
    if version > view.through {
        return Err(D1Error::VisibilityNotApplied);
    }
    let Some((db, cached)) = index else {
        return view
            .journal
            .terminal_prefix(version)
            .map_err(|_| D1Error::DecisionIndexHistory);
    };
    checked_manifest(db, cached, &view)?;
    let mut segments = Vec::new();
    for first in (1..=version).step_by(SEGMENT_TRANSACTIONS) {
        let bytes = db
            .get(view.key(first))
            .map_err(|_| D1Error::DecisionIndexUnavailable)?
            .ok_or(D1Error::DecisionIndexHistory)?;
        let mut row = Row::decode(&bytes).map_err(|_| D1Error::DecisionIndexHistory)?;
        if row != view.row(first).map_err(|_| D1Error::DecisionIndexHistory)? {
            return Err(D1Error::DecisionIndexHistory);
        }
        row.records
            .truncate((version - first + 1).min(SEGMENT_TRANSACTIONS as u64) as usize);
        segments.push(row.records);
    }
    TerminalPrefix::from_segments(
        view.journal.authority(),
        view.domain.cohort,
        version,
        segments,
    )
    .map_err(|_| D1Error::DecisionIndexHistory)
}

/// Ordinary apply must not conceal corruption by rewriting an existing partial
/// tail. Sealed segments are never rewritten except by explicit snapshot install.
pub(crate) fn check_tail(db: &DB, state: &D1State) -> anyhow::Result<()> {
    let view = View::of(state).ok_or_else(|| anyhow::anyhow!("decision domain missing"))?;
    if view.through != 0 && !view.through.is_multiple_of(SEGMENT_TRANSACTIONS as u64) {
        let first = view.through / SEGMENT_TRANSACTIONS as u64 * SEGMENT_TRANSACTIONS as u64 + 1;
        let bytes = db
            .get(view.key(first))?
            .ok_or_else(|| anyhow::anyhow!("decision tail missing"))?;
        anyhow::ensure!(
            Row::decode(&bytes)? == view.row(first)?,
            "decision tail changed"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::{Decision, Member, Participant, MAX_PARTICIPANTS};

    #[test]
    fn maximum_width_schema_fits_segment_bound_without_a_new_quota() {
        // Worst-width integer and byte encodings, maximum cohort/participants,
        // all false bits (longer than true), and a full 32-record segment. This
        // deliberately overestimates real ordinals, which remain capped at 1024.
        let cohort = Cohort {
            coordinator_shard: 0,
            coordinator_epoch: u64::MAX,
            members: (0..MAX_PARTICIPANTS as u64)
                .map(|n| Member {
                    shard: u64::MAX - MAX_PARTICIPANTS as u64 + n,
                    epoch: u64::MAX,
                    mode: [255; 16],
                })
                .collect(),
        };
        let domain = Domain {
            role: Role::Coordinator,
            shard: u64::MAX,
            epoch: u64::MAX,
            mode: [255; 16],
            authority: Authority {
                shard: u64::MAX,
                mode: [255; 16],
            },
            cohort,
        };
        let participants: Vec<_> = domain
            .cohort
            .members
            .iter()
            .map(|m| Participant {
                shard: m.shard,
                epoch: m.epoch,
                mode: m.mode,
                intent: [255; 32],
            })
            .collect();
        let row = Row {
            version: 1,
            domain: domain.clone(),
            first: u64::MAX,
            records: (0..SEGMENT_TRANSACTIONS)
                .map(|_| Terminal {
                    id: [255; 16],
                    ordinal: u64::MAX,
                    participants: participants.clone(),
                    prepared: vec![false; MAX_PARTICIPANTS],
                    decision: Decision::Abort,
                })
                .collect(),
        };
        assert!(row.bytes().unwrap().len() < MAX_SEGMENT_BYTES);
        let manifest = Manifest {
            version: 1,
            domain,
            through: u64::MAX,
            segments: u64::MAX,
            encoded_bytes: u64::MAX,
            applied: Some(LogId::new(
                openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
                u64::MAX,
            )),
        };
        assert!(serde_json::to_vec(&manifest).unwrap().len() < MAX_MANIFEST_BYTES);
    }
}
