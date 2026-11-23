//! Retained local physical inputs for verifying a fixed managed decision prefix.
//! This is an offline prefix check, not a global restore manifest or READY gate.
use super::{blocking_section, open_verified_state, sync_parent};
use crate::participant_writes::Authority;
use crate::transaction_decisions::Cohort;
use crate::{NodeHandle, NodeId};
use anyhow::{ensure, Context};
use std::fs;
use std::path::Path;

fn check_request(
    authority: &Authority,
    cohort: &Cohort,
    version: u64,
    members: usize,
) -> anyhow::Result<()> {
    cohort
        .validate()
        .map_err(|_| anyhow::anyhow!("invalid recovery cohort"))?;
    ensure!(
        cohort.coordinator_shard == authority.shard && authority.mode != [0; 16],
        "invalid recovery authority"
    );
    ensure!(
        members == cohort.members.len(),
        "incomplete recovery cohort"
    );
    ensure!(
        version <= cohort.profile.max_transactions() as u64,
        "version exceeds cohort capacity"
    );
    Ok(())
}

/// Capture one durable local replica per group, in cohort order, into a fresh
/// caller-owned directory on a trusted filesystem. This layout uses the shard
/// ID as the Raft group ID. Keep this directory immutable, including after an
/// error or cancellation. A new attempt must use a fresh path.
pub async fn capture(
    root: &Path,
    authority: &Authority,
    cohort: &Cohort,
    version: u64,
    coordinator: &NodeHandle,
    members: &[&NodeHandle],
) -> anyhow::Result<()> {
    check_request(authority, cohort, version, members.len())?;
    ensure!(
        coordinator.group_id() == authority.shard,
        "coordinator group mismatch"
    );
    for (node, member) in members.iter().zip(&cohort.members) {
        ensure!(node.group_id() == member.shard, "member group mismatch");
    }
    fs::create_dir(root).context("create fresh recovery input directory")?;
    sync_parent(root)?;
    for node in std::iter::once(coordinator).chain(members.iter().copied()) {
        // Uncontended storage locks may never yield. Let the caller abandon
        // the operation before starting another synchronous replica capture.
        tokio::task::yield_now().await;
        node.physical_checkpoint(root.join(node.group_id().to_string()))
            .await
            .with_context(|| {
                format!("capture physical checkpoint for group {}", node.group_id())
            })?;
    }
    let node_ids: Vec<_> = members.iter().map(|node| node.id).collect();
    verify(root, authority, cohort, version, coordinator.id, &node_ids).await
}

/// Reopen and validate the local inputs from disk. Supply the expected node
/// IDs in cohort order. Each physical cut may include work after `version`.
/// Success proves agreement through that ordinal only, and authorizes no node
/// startup, log purge, or release of retained inputs.
pub async fn verify(
    root: &Path,
    authority: &Authority,
    cohort: &Cohort,
    version: u64,
    coordinator_node: NodeId,
    member_nodes: &[NodeId],
) -> anyhow::Result<()> {
    check_request(authority, cohort, version, member_nodes.len())?;
    let root: std::path::PathBuf = root.components().collect();
    ensure!(
        fs::symlink_metadata(&root)?.file_type().is_dir(),
        "invalid recovery input directory"
    );
    let open = |shard: u64, node| {
        blocking_section(|| {
            open_verified_state(&root.join(shard.to_string()), shard, node)
                .with_context(|| format!("inspect physical checkpoint for group {shard}"))
        })
    };
    tokio::task::yield_now().await;
    let proof = {
        let coordinator = open(authority.shard, coordinator_node)?;
        coordinator
            .coordinator_terminal_prefix(
                authority.shard,
                cohort.coordinator_epoch,
                authority.mode,
                version,
            )
            .await
            .map_err(|error| anyhow::anyhow!("coordinator terminal prefix: {error:?}"))?
    };
    ensure!(
        proof.authority() == authority.mode
            && proof.cohort() == cohort
            && proof.version() == version,
        "coordinator recovery identity mismatch"
    );
    for (member, node) in cohort.members.iter().zip(member_nodes) {
        tokio::task::yield_now().await;
        let state = open(member.shard, *node)?;
        let info = state
            .participant_visibility_info(member.shard, member.epoch, member.mode)
            .await
            .map_err(|error| anyhow::anyhow!("group {} visibility: {error:?}", member.shard))?;
        ensure!(
            info.authority == *authority
                && info.cohort.as_ref() == Some(cohort)
                && info.baseline_sequence == 0
                && info.applied_prefix >= version,
            "group {} has incompatible or lagging visibility",
            member.shard
        );
        state
            .participant_terminal_prefix(member.shard, member.epoch, member.mode, &proof)
            .await
            .map_err(|error| {
                anyhow::anyhow!("group {} terminal prefix: {error:?}", member.shard)
            })?;
    }
    Ok(())
}
