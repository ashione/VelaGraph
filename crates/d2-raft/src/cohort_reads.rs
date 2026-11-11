//! Complete fixed-cohort, empty-baseline materialized snapshots. This trusted
//! local/remote primitive does not enable the remote graph Global query API.
use crate::managed_rpc::{self, NodeRef};
use crate::participant_writes::Authority;
use crate::terminal_prefix::TerminalPrefix;
use crate::transaction_decisions::Cohort;
use crate::versioned_reads::{ReadLimit, ReadSnapshot, Target};
use crate::{D1Error, NodeHandle};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub const MAX_SESSION_LIFETIME: Duration = Duration::from_secs(60);
pub const MAX_ACQUIRE_TIME: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidLimits,
    UnboundCohort,
    Identity,
    Unresolved,
    Unavailable,
    Expired,
    Capacity,
    Application(D1Error),
}

/// Lightweight leader-observed readiness, not itself a read certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityInfo {
    pub authority: Authority,
    pub baseline_sequence: u64,
    pub cohort: Option<Cohort>,
    pub applied_prefix: u64,
}

/// An immutable materialized view, pinned only in this process. All source
/// history is retained; this is not a distributed GC lease or restart token.
/// Dropping the session releases its captured memory. No partial session exists.
#[derive(Debug)]
pub struct SnapshotSession {
    authority: Authority,
    cohort: Cohort,
    version: u64,
    expires: Instant,
    snapshots: BTreeMap<u64, ReadSnapshot>,
}
impl SnapshotSession {
    fn live(&self) -> Result<(), Error> {
        if Instant::now() >= self.expires {
            Err(Error::Expired)
        } else {
            Ok(())
        }
    }
    pub fn expires_at(&self) -> Result<Instant, Error> {
        self.live()?;
        Ok(self.expires)
    }
    pub fn version(&self) -> Result<u64, Error> {
        self.live()?;
        Ok(self.version)
    }
    pub fn authority(&self) -> Result<&Authority, Error> {
        self.live()?;
        Ok(&self.authority)
    }
    pub fn cohort(&self) -> Result<&Cohort, Error> {
        self.live()?;
        Ok(&self.cohort)
    }
    pub fn shard(&self, shard: u64) -> Result<&ReadSnapshot, Error> {
        self.live()?;
        self.snapshots.get(&shard).ok_or(Error::Identity)
    }
}

/// Capture every persisted cohort member at exactly one coordinator version.
/// `None` selects min(coordinator resolved, every member applied), then uses
/// that one fixed version everywhere. An explicit version is never downgraded.
/// `limit.rows/bytes/operations` bound aggregate capture and replay. TTL and the total acquisition timeout include all barriers.
#[allow(clippy::too_many_arguments)]
pub async fn acquire(
    coordinator: &NodeHandle,
    authority: Authority,
    coordinator_epoch: u64,
    members: Vec<(&NodeHandle, Target)>,
    version: Option<u64>,
    limit: ReadLimit,
    lifetime: Duration,
) -> Result<SnapshotSession, Error> {
    acquire_endpoints(
        NodeRef::Local(coordinator),
        authority,
        coordinator_epoch,
        members
            .into_iter()
            .map(|(n, t)| (NodeRef::Local(n), t))
            .collect(),
        version,
        limit,
        lifetime,
    )
    .await
}

impl From<managed_rpc::Error> for Error {
    fn from(error: managed_rpc::Error) -> Self {
        match error {
            managed_rpc::Error::Application(e) => Self::Application(e),
            managed_rpc::Error::Identity | managed_rpc::Error::Protocol => Self::Identity,
            managed_rpc::Error::Capacity => Self::Capacity,
            managed_rpc::Error::Unavailable => Self::Unavailable,
        }
    }
}

/// The same complete-cohort protocol over explicitly pinned trusted endpoints.
/// No unchecked constructor can turn remote rows into a certified session.
#[allow(clippy::too_many_arguments)]
pub async fn acquire_endpoints(
    coordinator: NodeRef<'_>,
    authority: Authority,
    coordinator_epoch: u64,
    members: Vec<(NodeRef<'_>, Target)>,
    version: Option<u64>,
    limit: ReadLimit,
    lifetime: Duration,
) -> Result<SnapshotSession, Error> {
    if lifetime.is_zero()
        || lifetime > MAX_SESSION_LIFETIME
        || limit.rows == 0
        || limit.rows > crate::versioned_reads::MAX_READ_ROWS
        || limit.bytes == 0
        || limit.bytes > crate::versioned_reads::MAX_READ_BYTES
        || limit.operations == 0
        || limit.operations > crate::versioned_reads::MAX_REPLAY_OPERATIONS
    {
        return Err(Error::InvalidLimits);
    }
    let window = AcquisitionWindow::new(lifetime)?;
    let result = tokio::time::timeout(lifetime.min(MAX_ACQUIRE_TIME), async {
        let cut = admit_cut(
            coordinator,
            authority,
            coordinator_epoch,
            members,
            version,
            window,
        )
        .await?;
        capture_cut(cut, limit).await
    })
    .await;
    result.map_err(|_| window.timeout_error())?
}

/// Admission is deliberately not a read certificate or a restart token. It
/// selects one coordinator prefix after observing every member's readiness, but
/// each member must still return matching same-lock evidence with its read.
/// Private fields preserve the original endpoints and acquisition window.
/// Consumers must not refresh V, replace a replica, or restart its deadline.
pub struct AdmittedCut<'a> {
    authority: Authority,
    cohort: Cohort,
    version: u64,
    proof: TerminalPrefix,
    members: Vec<(NodeRef<'a>, Target)>,
    window: AcquisitionWindow,
}

impl<'a> AdmittedCut<'a> {
    /// Admission is not certification. Each selected member must still return
    /// the exact proof with its same-lock captured read before any result is used.
    pub fn check(&self) -> Result<(), Error> {
        self.window.check()
    }
    pub fn authority(&self) -> &Authority {
        &self.authority
    }
    pub fn cohort(&self) -> &Cohort {
        &self.cohort
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn proof(&self) -> &TerminalPrefix {
        &self.proof
    }
    pub fn members(&self) -> &[(NodeRef<'a>, Target)] {
        &self.members
    }
    pub fn expires_at(&self) -> Instant {
        self.window.expires
    }
    pub fn deadline(&self) -> Instant {
        self.window.deadline
    }
}

/// Admit one immutable complete cohort cut without downloading graph KV.
/// The returned private-field value preserves exact endpoints, proof, V and
/// original deadlines. It is neither a read certificate nor a restart token.
/// Callers must compare every member's same-lock proof before publishing data.
pub async fn admit_endpoints<'a>(
    coordinator: NodeRef<'a>,
    authority: Authority,
    coordinator_epoch: u64,
    members: Vec<(NodeRef<'a>, Target)>,
    version: Option<u64>,
    lifetime: Duration,
) -> Result<AdmittedCut<'a>, Error> {
    let window = AcquisitionWindow::new(lifetime)?;
    let cut = tokio::time::timeout_at(
        tokio::time::Instant::from_std(window.deadline),
        admit_cut(
            coordinator,
            authority,
            coordinator_epoch,
            members,
            version,
            window,
        ),
    )
    .await
    .map_err(|_| window.timeout_error())??;
    cut.check()?;
    Ok(cut)
}

#[derive(Clone, Copy)]
struct AcquisitionWindow {
    expires: Instant,
    deadline: Instant,
}
impl AcquisitionWindow {
    fn new(lifetime: Duration) -> Result<Self, Error> {
        if lifetime.is_zero() || lifetime > MAX_SESSION_LIFETIME {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            expires: Instant::now()
                .checked_add(lifetime)
                .ok_or(Error::InvalidLimits)?,
            deadline: Instant::now() + lifetime.min(MAX_ACQUIRE_TIME),
        })
    }
    fn check(self) -> Result<(), Error> {
        if Instant::now() >= self.expires {
            Err(Error::Expired)
        } else if Instant::now() >= self.deadline {
            Err(Error::Unavailable)
        } else {
            Ok(())
        }
    }
    fn timeout_error(self) -> Error {
        if Instant::now() >= self.expires {
            Error::Expired
        } else {
            Error::Unavailable
        }
    }
}

async fn admit_cut<'a>(
    coordinator: NodeRef<'a>,
    authority: Authority,
    coordinator_epoch: u64,
    members: Vec<(NodeRef<'a>, Target)>,
    version: Option<u64>,
    window: AcquisitionWindow,
) -> Result<AdmittedCut<'a>, Error> {
    let summary = coordinator
        .summary(&authority, coordinator_epoch)
        .await
        .map_err(|e| {
            if e == managed_rpc::Error::Application(D1Error::VisibilityHistory) {
                Error::UnboundCohort
            } else {
                Error::from(e)
            }
        })?;
    let cohort = summary.cohort;
    if cohort.coordinator_shard != authority.shard
        || cohort.coordinator_epoch != coordinator_epoch
        || members.len() != cohort.members.len()
    {
        return Err(Error::Identity);
    }
    for ((_, target), expected) in members.iter().zip(&cohort.members) {
        if target.shard != expected.shard
            || target.epoch != expected.epoch
            || target.mode != expected.mode
        {
            return Err(Error::Identity);
        }
    }
    let mut common = summary.resolved_prefix;
    for (node, target) in &members {
        window.check()?;
        let info = node.visibility(target).await?;
        if info.baseline_sequence != 0
            || info.authority != authority
            || info.cohort.as_ref() != Some(&cohort)
        {
            return Err(Error::Identity);
        }
        common = common.min(info.applied_prefix);
    }
    let version = version.unwrap_or(common);
    if version > summary.resolved_prefix {
        return Err(Error::Unresolved);
    }
    window.check()?;
    let proof = coordinator
        .terminal(&authority, coordinator_epoch, version)
        .await?;
    if proof.cohort() != &cohort {
        return Err(Error::Identity);
    }
    Ok(AdmittedCut {
        authority,
        cohort,
        version,
        proof,
        members,
        window,
    })
}

async fn capture_cut(cut: AdmittedCut<'_>, limit: ReadLimit) -> Result<SnapshotSession, Error> {
    let AdmittedCut {
        authority,
        cohort,
        version,
        proof,
        members,
        window,
    } = cut;
    let mut snapshots = BTreeMap::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    let mut operations = 0usize;
    for (node, target) in members {
        window.check()?;
        let remaining = ReadLimit {
            rows: limit.rows.saturating_sub(rows),
            bytes: limit.bytes.saturating_sub(bytes),
            operations: limit.operations.saturating_sub(operations),
        };
        // Exhaustion is fail-closed even if a later shard might be empty.
        if remaining.rows == 0 || remaining.bytes == 0 || remaining.operations == 0 {
            return Err(Error::Capacity);
        }
        let snapshot = node.read_terminal(&target, remaining, &proof).await?;
        window.check()?;
        if snapshot.shard != target.shard
            || snapshot.epoch != target.epoch
            || snapshot.version != version
            || snapshot.applied_prefix < version
            || snapshot.baseline_sequence != 0
            || snapshot.authority != authority
            || snapshot.cohort.as_ref() != Some(&cohort)
        {
            return Err(Error::Identity);
        }
        operations = operations.saturating_add(snapshot.replay_operations);
        rows = rows.saturating_add(snapshot.entries.len());
        bytes = bytes.saturating_add(
            snapshot
                .entries
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>(),
        );
        if rows > limit.rows || bytes > limit.bytes || operations > limit.operations {
            return Err(Error::Capacity);
        }
        snapshots.insert(target.shard, snapshot);
    }
    window.check()?;
    Ok(SnapshotSession {
        authority,
        cohort,
        version,
        expires: window.expires,
        snapshots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::{Command, Decision, Journal, Member, Participant};
    use crate::{Cluster, D1Command, D1Result, D1WriteRequest, LogCommand, WriteOp};
    fn authority() -> Authority {
        Authority {
            shard: 9,
            mode: [9; 16],
        }
    }
    fn cohort() -> Cohort {
        Cohort {
            profile: Default::default(),
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: vec![member(7), member(8)],
        }
    }
    fn member(shard: u64) -> Member {
        Member {
            shard,
            epoch: 1,
            mode: [shard as u8; 16],
        }
    }
    fn target(shard: u64) -> Target {
        Target {
            shard,
            epoch: 1,
            mode: [shard as u8; 16],
        }
    }
    fn descriptor(shard: u64) -> Participant {
        Participant {
            shard,
            epoch: 1,
            mode: [shard as u8; 16],
            intent: [shard as u8; 32],
        }
    }
    fn limit() -> ReadLimit {
        ReadLimit {
            rows: 100,
            bytes: 4096,
            operations: 100,
        }
    }
    async fn submit(node: &NodeHandle, command: D1Command) -> Result<D1Result, D1Error> {
        node.raft
            .client_write(LogCommand::D1V1(command))
            .await
            .unwrap()
            .data
            .d1
            .unwrap()
    }
    async fn journal_command(node: &NodeHandle, command: Command) -> Result<D1Result, D1Error> {
        submit(
            node,
            D1Command::Coordinator {
                shard: 9,
                epoch: 1,
                mode_id: [9; 16],
                command,
            },
        )
        .await
    }
    async fn start(cluster: &Cluster, shard: u64, journal: &Journal) -> NodeHandle {
        let node = cluster.in_group(shard).start(1).await.unwrap();
        node.raft
            .initialize(BTreeMap::from([(
                1,
                openraft::BasicNode {
                    addr: format!("inproc://{shard}/1"),
                },
            )]))
            .await
            .unwrap();
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |m| m.current_leader == Some(1) && m.last_applied.is_some(),
                "leader",
            )
            .await
            .unwrap();
        submit(&node, D1Command::Activate { shard, epoch: 1 })
            .await
            .unwrap();
        submit(
            &node,
            D1Command::EnableManagedWrites {
                shard,
                epoch: 1,
                expected_sequence: 0,
                mode_id: [shard as u8; 16],
            },
        )
        .await
        .unwrap();
        if shard == 9 {
            submit(
                &node,
                D1Command::EnableCoordinator {
                    shard,
                    epoch: 1,
                    mode_id: [9; 16],
                },
            )
            .await
            .unwrap();
            journal_command(&node, Command::BindCohort { cohort: cohort() })
                .await
                .unwrap();
        } else {
            submit(
                &node,
                D1Command::EnableParticipant {
                    shard,
                    epoch: 1,
                    mode_id: [shard as u8; 16],
                    authority: authority(),
                },
            )
            .await
            .unwrap();
            submit(
                &node,
                D1Command::EnableVersionedReads {
                    shard,
                    epoch: 1,
                    mode_id: [shard as u8; 16],
                    journal: journal.clone(),
                },
            )
            .await
            .unwrap();
        }
        node
    }
    async fn commit(coordinator: &NodeHandle, nodes: &[(&NodeHandle, u64)], id: u8, sequence: u64) {
        journal_command(
            coordinator,
            Command::Begin {
                id: [id; 16],
                participants: nodes.iter().map(|(_, s)| descriptor(*s)).collect(),
            },
        )
        .await
        .unwrap();
        for (node, shard) in nodes {
            crate::participant_driver::prepare(
                coordinator,
                node,
                authority(),
                1,
                descriptor(*shard),
                D1WriteRequest {
                    shard: *shard,
                    epoch: 1,
                    request_id: [id; 16],
                    expected_sequence: Some(sequence),
                    operations: vec![WriteOp::put("x", format!("v{id}"))],
                },
            )
            .await
            .unwrap();
        }
        journal_command(
            coordinator,
            Command::Decide {
                id: [id; 16],
                decision: Decision::Commit,
            },
        )
        .await
        .unwrap();
    }
    async fn advance(coordinator: &NodeHandle, node: &NodeHandle, shard: u64, version: u64) {
        crate::versioned_reads::advance(coordinator, node, authority(), 1, target(shard), version)
            .await
            .unwrap();
    }
    async fn capture(
        coordinator: &NodeHandle,
        a: &NodeHandle,
        b: &NodeHandle,
        version: Option<u64>,
    ) -> Result<SnapshotSession, Error> {
        acquire(
            coordinator,
            authority(),
            1,
            vec![(a, target(7)), (b, target(8))],
            version,
            limit(),
            Duration::from_secs(20),
        )
        .await
    }
    async fn admitted<'a>(
        coordinator: &'a NodeHandle,
        a: &'a NodeHandle,
        b: &'a NodeHandle,
        version: Option<u64>,
    ) -> Result<AdmittedCut<'a>, Error> {
        admit_cut(
            NodeRef::Local(coordinator),
            authority(),
            1,
            vec![
                (NodeRef::Local(a), target(7)),
                (NodeRef::Local(b), target(8)),
            ],
            version,
            AcquisitionWindow::new(Duration::from_secs(20)).unwrap(),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admitted_cut_retains_original_endpoints_version_and_deadlines() {
        let _resources = crate::managed_rpc::test_resource_guard().await;
        let cluster = Cluster::new();
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(Command::BindCohort { cohort: cohort() }).unwrap();
        let c = start(&cluster, 9, &j).await;
        let a = start(&cluster, 7, &j).await;
        let b = start(&cluster, 8, &j).await;
        let cut = admitted(&c, &a, &b, None).await.unwrap();
        assert_eq!(cut.authority, authority());
        assert_eq!(cut.cohort, cohort());
        assert_eq!(cut.version, 0);
        assert_eq!(cut.proof, j.terminal_prefix(0).unwrap());
        assert!(matches!(cut.members[0].0, NodeRef::Local(n) if std::ptr::eq(n, &a)));
        assert!(matches!(cut.members[1].0, NodeRef::Local(n) if std::ptr::eq(n, &b)));
        assert_eq!(cut.members[0].1, target(7));
        assert_eq!(cut.members[1].1, target(8));
        let expires = cut.window.expires;
        commit(&c, &[(&a, 7), (&b, 8)], 1, 0).await;
        advance(&c, &a, 7, 1).await;
        advance(&c, &b, 8, 1).await;
        let old = capture_cut(cut, limit()).await.unwrap();
        assert_eq!(old.version().unwrap(), 0);
        assert_eq!(old.expires_at().unwrap(), expires);
        assert!(old.shard(7).unwrap().entries.is_empty());
        assert!(old.shard(8).unwrap().entries.is_empty());
        assert_eq!(
            capture(&c, &a, &b, None).await.unwrap().version().unwrap(),
            1
        );

        // Selecting a prefix is not certifying its data: even a nonparticipant
        // must consume that prefix before capture can publish a session.
        commit(&c, &[(&a, 7)], 2, 1).await;
        advance(&c, &a, 7, 2).await;
        let automatic = admitted(&c, &a, &b, None).await.unwrap();
        assert_eq!(automatic.version, 1);
        let explicit = admitted(&c, &a, &b, Some(2)).await.unwrap();
        assert_eq!(explicit.version, 2);
        assert_eq!(
            capture_cut(explicit, limit()).await.unwrap_err(),
            Error::Application(D1Error::VisibilityNotApplied)
        );
        advance(&c, &b, 8, 2).await;
        assert_eq!(
            capture_cut(automatic, limit())
                .await
                .unwrap()
                .version()
                .unwrap(),
            1
        );

        // Admission must not create a renewable acquisition window.
        let mut expired = admitted(&c, &a, &b, None).await.unwrap();
        expired.window.expires = Instant::now();
        assert_eq!(
            capture_cut(expired, limit()).await.unwrap_err(),
            Error::Expired
        );
        let mut timed_out = admitted(&c, &a, &b, None).await.unwrap();
        timed_out.window.deadline = Instant::now();
        assert_eq!(
            capture_cut(timed_out, limit()).await.unwrap_err(),
            Error::Unavailable
        );
        for node in [&a, &b, &c] {
            node.raft.shutdown().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admission_rejects_incomplete_or_changed_cohort_and_unresolved_holes() {
        let _resources = crate::managed_rpc::test_resource_guard().await;
        let cluster = Cluster::new();
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(Command::BindCohort { cohort: cohort() }).unwrap();
        let c = start(&cluster, 9, &j).await;
        let a = start(&cluster, 7, &j).await;
        let b = start(&cluster, 8, &j).await;
        let valid = vec![
            (NodeRef::Local(&a), target(7)),
            (NodeRef::Local(&b), target(8)),
        ];
        let mut wrong_epoch = valid.clone();
        wrong_epoch[1].1.epoch += 1;
        let mut wrong_mode = valid.clone();
        wrong_mode[1].1.mode = [42; 16];
        for members in [
            vec![valid[0].clone()],
            vec![valid[0].clone(), valid[0].clone()],
            vec![valid[1].clone(), valid[0].clone()],
            wrong_epoch,
            wrong_mode,
        ] {
            assert!(matches!(
                admit_cut(
                    NodeRef::Local(&c),
                    authority(),
                    1,
                    members,
                    Some(0),
                    AcquisitionWindow::new(Duration::from_secs(20)).unwrap()
                )
                .await,
                Err(Error::Identity)
            ));
        }
        // A correctly shaped target list must not redirect a member read to
        // another member's handle, even at the identical empty version zero.
        assert!(admit_cut(
            NodeRef::Local(&c),
            authority(),
            1,
            vec![
                (NodeRef::Local(&b), target(7)),
                (NodeRef::Local(&a), target(8))
            ],
            Some(0),
            AcquisitionWindow::new(Duration::from_secs(20)).unwrap()
        )
        .await
        .is_err());
        let mut wrong_authority = authority();
        wrong_authority.mode = [42; 16];
        assert!(admit_cut(
            NodeRef::Local(&c),
            wrong_authority,
            1,
            valid,
            Some(0),
            AcquisitionWindow::new(Duration::from_secs(20)).unwrap()
        )
        .await
        .is_err());

        journal_command(
            &c,
            Command::Begin {
                id: [1; 16],
                participants: vec![descriptor(7)],
            },
        )
        .await
        .unwrap();
        journal_command(
            &c,
            Command::Begin {
                id: [2; 16],
                participants: vec![descriptor(8)],
            },
        )
        .await
        .unwrap();
        journal_command(
            &c,
            Command::Decide {
                id: [2; 16],
                decision: Decision::Abort,
            },
        )
        .await
        .unwrap();
        assert_eq!(admitted(&c, &a, &b, None).await.unwrap().version, 0);
        for version in [1, 2] {
            assert!(matches!(
                admitted(&c, &a, &b, Some(version)).await,
                Err(Error::Unresolved)
            ));
        }
        for node in [&a, &b, &c] {
            node.raft.shutdown().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn complete_cohort_same_version_survives_continuing_writes_and_refuses_partial_apply() {
        let _resources = crate::managed_rpc::test_resource_guard().await;
        let cluster = Cluster::new();
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(Command::BindCohort { cohort: cohort() }).unwrap();
        let c = start(&cluster, 9, &j).await;
        let a = start(&cluster, 7, &j).await;
        let b = start(&cluster, 8, &j).await;
        let empty = capture(&c, &a, &b, None).await.unwrap();
        assert_eq!(empty.version().unwrap(), 0);
        assert!(empty.shard(7).unwrap().entries.is_empty());
        assert_eq!(
            acquire(
                &c,
                authority(),
                1,
                vec![(&a, target(7))],
                None,
                limit(),
                Duration::from_secs(10)
            )
            .await
            .unwrap_err(),
            Error::Identity
        );
        commit(&c, &[(&a, 7), (&b, 8)], 1, 0).await;
        advance(&c, &a, 7, 1).await;
        assert_eq!(
            capture(&c, &a, &b, Some(1)).await.unwrap_err(),
            Error::Application(D1Error::VisibilityNotApplied)
        );
        let still_empty = capture(&c, &a, &b, None).await.unwrap();
        assert_eq!(still_empty.version().unwrap(), 0);
        assert!(still_empty.shard(7).unwrap().entries.is_empty());
        advance(&c, &b, 8, 1).await;
        let pinned = capture(&c, &a, &b, None).await.unwrap();
        assert_eq!(
            pinned.shard(7).unwrap().entries,
            pinned.shard(8).unwrap().entries
        );
        // Single-member transaction still requires the nonparticipant to consume
        // the decision prefix before a complete view of the new version exists.
        commit(&c, &[(&a, 7)], 2, 1).await;
        advance(&c, &a, 7, 2).await;
        assert_eq!(
            capture(&c, &a, &b, Some(2)).await.unwrap_err(),
            Error::Application(D1Error::VisibilityNotApplied)
        );
        assert_eq!(
            capture(&c, &a, &b, None).await.unwrap().version().unwrap(),
            1
        );
        advance(&c, &b, 8, 2).await;
        let next = capture(&c, &a, &b, None).await.unwrap();
        assert_eq!(next.version().unwrap(), 2);
        assert_eq!(
            pinned
                .shard(7)
                .unwrap()
                .entries
                .get(b"x".as_slice())
                .unwrap(),
            b"v1"
        );
        assert_eq!(
            next.shard(7).unwrap().entries.get(b"x".as_slice()).unwrap(),
            b"v2"
        );
        assert_eq!(
            next.shard(8).unwrap().entries.get(b"x".as_slice()).unwrap(),
            b"v1"
        );
        assert_eq!(
            capture(&c, &a, &b, Some(3)).await.unwrap_err(),
            Error::Unresolved
        );
        let low = ReadLimit { rows: 1, ..limit() };
        assert_eq!(
            acquire(
                &c,
                authority(),
                1,
                vec![(&a, target(7)), (&b, target(8))],
                Some(1),
                low,
                Duration::from_secs(10)
            )
            .await
            .unwrap_err(),
            Error::Capacity
        );
        let low = ReadLimit {
            operations: 1,
            ..limit()
        };
        assert_eq!(
            acquire(
                &c,
                authority(),
                1,
                vec![(&a, target(7)), (&b, target(8))],
                Some(1),
                low,
                Duration::from_secs(10)
            )
            .await
            .unwrap_err(),
            Error::Capacity
        );
        assert_eq!(
            submit(
                &c,
                D1Command::SetEpoch {
                    shard: 9,
                    expected_epoch: 1,
                    new_epoch: 2
                }
            )
            .await,
            Err(D1Error::VisibilityRequired)
        );
        let mut expired = pinned;
        expired.expires = Instant::now();
        assert_eq!(expired.version(), Err(Error::Expired));
        assert!(matches!(
            acquire(
                &c,
                authority(),
                1,
                vec![(&a, target(7)), (&b, target(8))],
                None,
                limit(),
                Duration::from_nanos(1)
            )
            .await,
            Err(Error::Expired)
        ));
        let low = ReadLimit {
            bytes: 4,
            ..limit()
        };
        assert!(acquire(
            &c,
            authority(),
            1,
            vec![(&a, target(7)), (&b, target(8))],
            Some(1),
            low,
            Duration::from_secs(10)
        )
        .await
        .is_err());
        b.raft.shutdown().await.unwrap();
        assert!(acquire(
            &c,
            authority(),
            1,
            vec![(&a, target(7)), (&b, target(8))],
            Some(1),
            limit(),
            Duration::from_millis(50)
        )
        .await
        .is_err());
        assert_eq!(next.version().unwrap(), 2);
        for node in [&a, &c] {
            node.raft.shutdown().await.unwrap();
        }
    }
}
