//! Immutable, bounded placement catalog in a dedicated, preconfigured D1 group.
//! Fresh reads always cross the metadata leader's quorum barrier. This is not
//! migration, a cross-group fence, or a production routing protocol.
use std::{collections::BTreeMap, error::Error, fmt, time::Duration};

use d2_raft::NodeHandle;
use rocksdb_sql_table::{
    RequestId, StorageNode, StorageNodeError, WriteOp, WriteRequest, WriteResult,
};

use crate::ReplicatedStorageNode;

const KEY: &[u8] = b"catalog/v1/state";
const MAGIC: &[u8; 4] = b"CAT1";
/// Hard prototype limits, not an RSS or production capacity guarantee.
pub const MAX_PARTITIONS: usize = 1024;
pub const MAX_SHARDS: usize = 1024;
pub const MAX_REPLICAS: usize = 31;
pub const MAX_CATALOG_BYTES: usize = 320 * 1024;

/// Preconfigured metadata or data placement. Replica IDs must be strictly sorted.
/// Epoch is the D1 application's ownership epoch, never a Raft term.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Placement {
    pub shard: u64,
    pub group: u64,
    pub epoch: u64,
    pub replicas: Vec<u64>,
}
impl Placement {
    fn validate(&self) -> Result<()> {
        if self.replicas.is_empty()
            || self.replicas.len() > MAX_REPLICAS
            || self.replicas.windows(2).any(|w| w[0] >= w[1])
        {
            return Err(CatalogError::InvalidArgument(
                "replica IDs must be nonempty, sorted, unique, and bounded",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum CatalogError {
    Storage(StorageNodeError),
    InvalidArgument(&'static str),
    Corrupt(&'static str),
    /// An existing partition, shard, or group cannot be rebound in this format.
    BindingConflict,
    /// Preconfigured metadata identity disagrees with the persisted record.
    MetadataMismatch,
    Capacity,
    Unsupported,
}
impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "{e}"),
            Self::InvalidArgument(s) => write!(f, "invalid catalog argument: {s}"),
            Self::Corrupt(s) => write!(f, "corrupt catalog: {s}"),
            Self::BindingConflict => f.write_str("immutable catalog binding conflict"),
            Self::MetadataMismatch => f.write_str("metadata identity mismatch"),
            Self::Capacity => f.write_str("catalog capacity exceeded"),
            Self::Unsupported => f.write_str("catalog migration/ownership change is unsupported"),
        }
    }
}
impl Error for CatalogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(e) => Some(e),
            _ => None,
        }
    }
}
impl From<StorageNodeError> for CatalogError {
    fn from(value: StorageNodeError) -> Self {
        Self::Storage(value)
    }
}
pub type Result<T> = std::result::Result<T, CatalogError>;

/// A historical catalog snapshot from one linearizable metadata read. Retaining
/// this value does not turn subsequent local lookups into fresh quorum reads.
#[derive(Clone, Debug)]
pub struct CatalogSnapshot {
    sequence: u64,
    metadata: Placement,
    shards: BTreeMap<u64, Placement>,
    partitions: BTreeMap<u32, u64>,
}
impl CatalogSnapshot {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn placement(&self, partition: u32) -> Option<&Placement> {
        self.partitions
            .get(&partition)
            .and_then(|s| self.shards.get(s))
    }
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn register(&mut self, partition: u32, placement: Placement) -> Result<()> {
        placement.validate()?;
        if placement.shard == self.metadata.shard || placement.group == self.metadata.group {
            return Err(CatalogError::InvalidArgument(
                "metadata shard and group are reserved",
            ));
        }
        if self
            .partitions
            .get(&partition)
            .is_some_and(|s| *s != placement.shard)
            || self
                .shards
                .get(&placement.shard)
                .is_some_and(|p| *p != placement)
            || self
                .shards
                .values()
                .any(|p| p.group == placement.group && p.shard != placement.shard)
        {
            return Err(CatalogError::BindingConflict);
        }
        if (!self.partitions.contains_key(&partition) && self.partitions.len() >= MAX_PARTITIONS)
            || (!self.shards.contains_key(&placement.shard) && self.shards.len() >= MAX_SHARDS)
        {
            return Err(CatalogError::Capacity);
        }
        self.partitions.insert(partition, placement.shard);
        self.shards.insert(placement.shard, placement);
        Ok(())
    }
}

/// Immutable original D1 identity. Retain and retry this exact value after a
/// timeout or leader failover. It is not a durable client retry journal.
#[derive(Clone, Debug)]
pub struct PreparedRegistration {
    metadata: Placement,
    request: WriteRequest,
    expected_sequence: u64,
}
impl PreparedRegistration {
    pub fn request_id(&self) -> RequestId {
        self.request.request_id
    }
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
}

/// One metadata replica; leader selection is explicit and never automatically
/// falls back to a stale cache. Construct on the hosting runtime; use
/// `run_blocking` for operations, just like `ReplicatedStorageNode`.
#[derive(Clone)]
pub struct Catalog {
    node: ReplicatedStorageNode,
    metadata: Placement,
}
impl Catalog {
    pub fn new(node: &NodeHandle, metadata: Placement, deadline: Duration) -> Result<Self> {
        metadata.validate()?;
        if node.group_id() != metadata.group || !metadata.replicas.contains(&node.id) {
            return Err(CatalogError::MetadataMismatch);
        }
        Ok(Self {
            node: ReplicatedStorageNode::new(node, metadata.shard, deadline)?,
            metadata,
        })
    }

    /// Exactly one D1 leader-barrier read, with its bounded operation deadline.
    /// No metadata quorum means no successful read, including absent partitions.
    pub fn snapshot(&self) -> Result<CatalogSnapshot> {
        let view = self.node.read_view()?;
        if view.shard != self.metadata.shard || view.epoch != self.metadata.epoch {
            return Err(CatalogError::MetadataMismatch);
        }
        match view.get(KEY) {
            Some(bytes) => {
                let mut snapshot = decode(&bytes)?;
                if snapshot.metadata != self.metadata {
                    return Err(CatalogError::MetadataMismatch);
                }
                snapshot.sequence = view.sequence;
                Ok(snapshot)
            }
            None => {
                // The dedicated metadata group must never reinterpret unrelated
                // D1 history or a deleted catalog as an empty catalog.
                if view.sequence != 0 {
                    return Err(CatalogError::Corrupt(
                        "catalog missing from nonempty history",
                    ));
                }
                Ok(CatalogSnapshot {
                    sequence: 0,
                    metadata: self.metadata.clone(),
                    shards: BTreeMap::new(),
                    partitions: BTreeMap::new(),
                })
            }
        }
    }

    pub fn lookup(&self, partition: u32) -> Result<Option<Placement>> {
        Ok(self.snapshot()?.placement(partition).cloned())
    }

    /// Read-only preparation against one metadata snapshot. Even an identical
    /// existing binding creates an exact conditional D1 request. Re-preparing
    /// an already committed request ID is not a substitute for exact retry.
    pub fn prepare_registration(
        &self,
        id: RequestId,
        partition: u32,
        placement: Placement,
    ) -> Result<PreparedRegistration> {
        let mut snapshot = self.snapshot()?;
        snapshot.register(partition, placement)?;
        let value = encode(&snapshot)?;
        Ok(PreparedRegistration {
            metadata: self.metadata.clone(),
            expected_sequence: snapshot.sequence,
            request: WriteRequest {
                shard: self.metadata.shard,
                epoch: self.metadata.epoch,
                request_id: id,
                operations: vec![WriteOp::put(KEY, value)],
            },
        })
    }

    /// No read, identity refresh, condition refresh or retry loop. A definite
    /// Conflict permits a new preparation; an unknown outcome requires this
    /// exact value. D1 serializes the CAS and keeps dedup through snapshots.
    pub fn commit_registration(&self, prepared: &PreparedRegistration) -> Result<WriteResult> {
        if prepared.metadata != self.metadata {
            return Err(CatalogError::MetadataMismatch);
        }
        Ok(self
            .node
            .batch_write_conditional(prepared.request.clone(), Some(prepared.expected_sequence))?)
    }

    /// No cross-group fencing protocol exists. Catalog epochs cannot fence an
    /// independent old owner's Raft group, so movement always fails closed.
    pub fn move_partition(&self, _partition: u32, _placement: Placement) -> Result<()> {
        Err(CatalogError::Unsupported)
    }
}

fn encode(snapshot: &CatalogSnapshot) -> Result<Vec<u8>> {
    let mut out = MAGIC.to_vec();
    put_placement(&mut out, &snapshot.metadata);
    out.extend_from_slice(&(snapshot.shards.len() as u32).to_be_bytes());
    for placement in snapshot.shards.values() {
        put_placement(&mut out, placement);
    }
    out.extend_from_slice(&(snapshot.partitions.len() as u32).to_be_bytes());
    for (partition, shard) in &snapshot.partitions {
        out.extend_from_slice(&partition.to_be_bytes());
        out.extend_from_slice(&shard.to_be_bytes());
    }
    if out.len() > MAX_CATALOG_BYTES {
        return Err(CatalogError::Capacity);
    }
    Ok(out)
}
fn put_placement(out: &mut Vec<u8>, placement: &Placement) {
    for v in [placement.shard, placement.group, placement.epoch] {
        out.extend_from_slice(&v.to_be_bytes());
    }
    out.push(placement.replicas.len() as u8);
    for replica in &placement.replicas {
        out.extend_from_slice(&replica.to_be_bytes());
    }
}
struct Decoder<'a>(&'a [u8]);
impl Decoder<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self
            .0
            .get(..N)
            .ok_or(CatalogError::Corrupt("truncated record"))?;
        let result = bytes
            .try_into()
            .map_err(|_| CatalogError::Corrupt("invalid field width"))?;
        self.0 = &self.0[N..];
        Ok(result)
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take()?))
    }
    fn placement(&mut self) -> Result<Placement> {
        let shard = self.u64()?;
        let group = self.u64()?;
        let epoch = self.u64()?;
        let count = self.take::<1>()?[0] as usize;
        if count == 0 || count > MAX_REPLICAS {
            return Err(CatalogError::Corrupt("invalid replica count"));
        }
        let mut replicas = Vec::with_capacity(count);
        for _ in 0..count {
            replicas.push(self.u64()?);
        }
        let placement = Placement {
            shard,
            group,
            epoch,
            replicas,
        };
        placement
            .validate()
            .map_err(|_| CatalogError::Corrupt("noncanonical replicas"))?;
        Ok(placement)
    }
}
fn decode(bytes: &[u8]) -> Result<CatalogSnapshot> {
    if bytes.len() > MAX_CATALOG_BYTES {
        return Err(CatalogError::Corrupt("record too large"));
    }
    let mut d = Decoder(bytes);
    if &d.take::<4>()? != MAGIC {
        return Err(CatalogError::Corrupt("unsupported catalog version"));
    }
    let metadata = d.placement()?;
    let mut snapshot = CatalogSnapshot {
        sequence: 0,
        metadata,
        shards: BTreeMap::new(),
        partitions: BTreeMap::new(),
    };
    let count = d.u32()? as usize;
    if count > MAX_SHARDS {
        return Err(CatalogError::Corrupt("too many shards"));
    }
    let mut last = None;
    let mut groups = std::collections::BTreeSet::new();
    for _ in 0..count {
        let p = d.placement()?;
        if last.is_some_and(|s| s >= p.shard)
            || !groups.insert(p.group)
            || p.shard == snapshot.metadata.shard
            || p.group == snapshot.metadata.group
        {
            return Err(CatalogError::Corrupt("invalid shard/group binding"));
        }
        last = Some(p.shard);
        snapshot.shards.insert(p.shard, p);
    }
    let count = d.u32()? as usize;
    if count > MAX_PARTITIONS {
        return Err(CatalogError::Corrupt("too many partitions"));
    }
    let mut last = None;
    let mut used = std::collections::BTreeSet::new();
    for _ in 0..count {
        let p = d.u32()?;
        let s = d.u64()?;
        if last.is_some_and(|v| v >= p) || !snapshot.shards.contains_key(&s) {
            return Err(CatalogError::Corrupt("invalid partition binding"));
        }
        last = Some(p);
        used.insert(s);
        snapshot.partitions.insert(p, s);
    }
    if !d.0.is_empty() || used.len() != snapshot.shards.len() {
        return Err(CatalogError::Corrupt(
            "trailing bytes or unreferenced shard",
        ));
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn p(shard: u64, group: u64) -> Placement {
        Placement {
            shard,
            group,
            epoch: 3,
            replicas: vec![1, 2, 3],
        }
    }
    fn empty() -> CatalogSnapshot {
        CatalogSnapshot {
            sequence: 0,
            metadata: p(900, 100),
            shards: BTreeMap::new(),
            partitions: BTreeMap::new(),
        }
    }
    #[test]
    fn golden_and_canonical_format() {
        let mut s = CatalogSnapshot {
            metadata: Placement {
                shard: 9,
                group: 8,
                epoch: 1,
                replicas: vec![1],
            },
            ..empty()
        };
        s.register(
            4,
            Placement {
                shard: 7,
                group: 6,
                epoch: 2,
                replicas: vec![2],
            },
        )
        .unwrap();
        let bytes = encode(&s).unwrap();
        let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(
            hex,
            concat!(
                "43415431",
                "0000000000000009",
                "0000000000000008",
                "0000000000000001",
                "01",
                "0000000000000001",
                "00000001",
                "0000000000000007",
                "0000000000000006",
                "0000000000000002",
                "01",
                "0000000000000002",
                "00000001",
                "00000004",
                "0000000000000007"
            )
        );
        assert_eq!(encode(&decode(&bytes).unwrap()).unwrap(), bytes);
        for n in 0..bytes.len() {
            assert!(decode(&bytes[..n]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        let mut version = bytes;
        version[3] = b'2';
        assert!(decode(&version).is_err());
        let mut a = empty();
        let mut b = empty();
        a.register(3, p(3, 3)).unwrap();
        a.register(2, p(2, 2)).unwrap();
        b.register(2, p(2, 2)).unwrap();
        b.register(3, p(3, 3)).unwrap();
        assert_eq!(encode(&a).unwrap(), encode(&b).unwrap());
    }
    #[test]
    fn bindings_are_immutable_and_metadata_is_reserved() {
        let mut s = empty();
        s.register(0, p(1, 2)).unwrap();
        s.register(0, p(1, 2)).unwrap();
        s.register(1, p(1, 2)).unwrap();
        let before = encode(&s).unwrap();
        for (part, place) in [(0, p(2, 3)), (2, p(1, 3)), (2, p(2, 2))] {
            assert!(matches!(
                s.register(part, place),
                Err(CatalogError::BindingConflict)
            ));
        }
        let mut moved = p(1, 2);
        moved.epoch = 4;
        assert!(matches!(
            s.register(1, moved),
            Err(CatalogError::BindingConflict)
        ));
        let mut membership = p(1, 2);
        membership.replicas = vec![1, 2];
        assert!(matches!(
            s.register(1, membership),
            Err(CatalogError::BindingConflict)
        ));
        assert!(s.register(2, p(900, 3)).is_err());
        assert!(s.register(2, p(2, 100)).is_err());
        assert_eq!(encode(&s).unwrap(), before);
    }
    #[test]
    fn counts_bytes_and_replicas_are_bounded() {
        let mut s = empty();
        for n in 0..MAX_PARTITIONS {
            s.register(n as u32, p(1, 2)).unwrap();
        }
        let before = encode(&s).unwrap();
        assert!(matches!(
            s.register(MAX_PARTITIONS as u32, p(1, 2)),
            Err(CatalogError::Capacity)
        ));
        assert_eq!(encode(&s).unwrap(), before);
        for replicas in [
            vec![],
            vec![2, 1],
            vec![1, 1],
            (0..=MAX_REPLICAS as u64).collect(),
        ] {
            let mut placement = p(1, 2);
            placement.replicas = replicas;
            assert!(placement.validate().is_err());
        }
        let mut largest = empty();
        for n in 0..MAX_SHARDS {
            let mut placement = p(n as u64 + 1000, n as u64 + 1000);
            placement.replicas = (0..MAX_REPLICAS as u64).collect();
            largest.register(n as u32, placement).unwrap();
        }
        let bytes = encode(&largest).unwrap();
        assert!(bytes.len() <= MAX_CATALOG_BYTES);
        assert_eq!(decode(&bytes).unwrap().shard_count(), MAX_SHARDS);
        assert!(decode(&vec![0; MAX_CATALOG_BYTES + 1]).is_err());
        // Malicious count is checked before allocating or looping over it.
        let mut bytes = encode(&empty()).unwrap();
        let count_at = 4 + 25 + 3 * 8;
        bytes[count_at..count_at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode(&bytes).is_err());
    }
    #[test]
    fn corrupt_aliases_references_and_unsorted_records_fail_closed() {
        let mut s = empty();
        s.register(0, p(1, 2)).unwrap();
        s.register(1, p(2, 3)).unwrap();
        let good = encode(&s).unwrap();
        let header = 4 + 49 + 4;
        let mut bad = good.clone();
        bad[header + 49 + 8..header + 49 + 16].copy_from_slice(&2u64.to_be_bytes());
        assert!(decode(&bad).is_err()); // two shards in same group
        let mut bad = good.clone();
        bad[header + 49..header + 49 + 8].copy_from_slice(&0u64.to_be_bytes());
        assert!(decode(&bad).is_err()); // noncanonical shard ordering
        let mut bad = good.clone();
        let len = bad.len();
        bad[len - 8..].copy_from_slice(&99u64.to_be_bytes());
        assert!(decode(&bad).is_err()); // dangling partition
        let mut bad = good;
        bad[4 + 25 + 8..4 + 25 + 16].copy_from_slice(&1u64.to_be_bytes());
        assert!(decode(&bad).is_err()); // duplicate metadata replica
    }
}
