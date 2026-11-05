use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::types::{ApplyResult, LogCommand, NodeId, TypeConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    pub data: Vec<u8>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub data: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
pub struct StateMachine {
    pub state: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl StateMachine {
    pub async fn get(&self, key: &str) -> Option<String> {
        self.state.read().await.data.get(key).cloned()
    }

    pub async fn scan(&self) -> BTreeMap<String, String> {
        self.state.read().await.data.clone()
    }

    pub async fn applied(&self) -> Option<LogId<NodeId>> {
        self.state.read().await.last_applied_log
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.state.read().await;
        let data =
            serde_json::to_vec(&sm.data).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();
        let mut current = self.current_snapshot.write().await;
        drop(sm);

        let idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match last_applied_log {
            Some(log) => format!("{}-{}-{}", log.leader_id, log.index, idx),
            None => format!("--{idx}"),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };
        *current = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let sm = self.state.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut out = Vec::new();
        let mut sm = self.state.write().await;
        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => out.push(ApplyResult { value: None }),
                EntryPayload::Membership(mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    out.push(ApplyResult { value: None });
                }
                EntryPayload::Normal(cmd) => match cmd {
                    LogCommand::Noop => out.push(ApplyResult { value: None }),
                    LogCommand::Put(key, value) => {
                        sm.data.insert(key, value.clone());
                        out.push(ApplyResult { value: Some(value) });
                    }
                    LogCommand::Delete(key) => {
                        sm.data.remove(&key);
                        out.push(ApplyResult { value: None });
                    }
                },
            }
        }
        Ok(out)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as openraft::RaftTypeConfig>::SnapshotData>, StorageError<NodeId>>
    {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as openraft::RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let data = serde_json::from_slice(&bytes)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut sm = self.state.write().await;
        *sm = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data,
        };
        let mut current = self.current_snapshot.write().await;
        drop(sm);
        *current = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
