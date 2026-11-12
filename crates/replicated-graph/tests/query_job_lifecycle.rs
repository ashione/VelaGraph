//! Independent-process query-job lifecycle acceptance.
//! This automatically discovered target gives the long-running job its own
//! process-wide client admission budget and retained-fixture phase mutex.
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
        self as gateway, Binding, EdgeData, Entity, Gateway, Mapping, Mutation, Operation,
        PageCursor, Patch, Query, QueryPage, RemoteConfig, Reply, Session, VertexData, Write,
        WriteStatus,
    },
    remote::query::{QualifiedElement, QueryCompleteness, QueryRow},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Cursor, Write as _},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use velagraph::Value;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Share only fixture helpers, never another target's acceptance tests.
include!("support/managed_process_fixture.rs");

#[path = "support/query_job_lifecycle.rs"]
mod query_job_lifecycle;
