//! Fresh mixed-API managed capacity acceptance in independent OS processes.
//! This ordinary target is deliberately separate from managed_process: adding
//! this fixture must not serialize or alter that target's original scenarios.
// Some exact shared helpers are used only by the other integration targets.
#![allow(dead_code)]

use anyhow::{anyhow, Context};
use d2_raft::{
    managed_rpc::{activate_byte_bound_member, Client, Identity, Role, Server},
    participant_writes::Authority,
    transaction_decisions::{CapacityProfile, Cohort, Command as DecisionCommand, Journal, Member},
    versioned_reads::Target,
    Cluster, D1Command, LogCommand, NodeHandle, StateMachine,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use replicated_graph::{
    managed_gateway::{
        self as gateway, Binding, Gateway, Mapping, Operation, Query, Reply, Session, WriteStatus,
    },
    remote::query::{QueryCompleteness, QueryRow},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{Cursor, Write as _},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

include!("support/managed_process_fixture.rs");

impl Process {
    async fn reopen(&mut self) -> anyhow::Result<()> {
        fs::remove_file(self.directory.path().join("ready.json"))?;
        self.child = Self::launch(self.directory.path(), self.shard, self.address, false)?;
        self.ready().await
    }
}

#[path = "support/managed_local_capacity.rs"]
mod managed_local_capacity;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expanded_local_read_crosses_original_capture_boundary() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(180), managed_local_capacity::scenario()).await?
}
