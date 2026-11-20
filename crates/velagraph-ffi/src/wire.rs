use serde::Deserialize;
use std::collections::BTreeMap;
use velagraph::{IncrementalLimits, QueryBatchLimits, Value};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Vertex {
    pub id: u64,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, Value>,
}
impl From<Vertex> for velagraph::Vertex {
    fn from(v: Vertex) -> Self {
        Self {
            id: v.id,
            labels: v.labels,
            properties: v.properties,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub id: u64,
    pub from: u64,
    pub to: u64,
    pub label: String,
    #[serde(default)]
    pub properties: BTreeMap<String, Value>,
}
impl From<Edge> for velagraph::Edge {
    fn from(e: Edge) -> Self {
        Self {
            id: e.id,
            from: e.from,
            to: e.to,
            label: e.label,
            properties: e.properties,
        }
    }
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Cow,
    Locked,
}
#[derive(Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum Open {
    Memory {
        identity: String,
        #[serde(default)]
        mode: Mode,
        #[serde(default)]
        vertices: Vec<Vertex>,
        #[serde(default)]
        edges: Vec<Edge>,
        #[serde(default)]
        metadata: Vec<u8>,
        restore: Option<String>,
        limits: Option<MemoryLimits>,
    },
    Temporal {
        path: String,
    },
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryLimits {
    pub max_records: Option<usize>,
    pub max_accounted_bytes: Option<usize>,
    pub max_batch_operations: Option<usize>,
    pub max_batch_bytes: Option<usize>,
    pub max_pinned_versions: Option<usize>,
    pub max_pinned_accounted_bytes: Option<usize>,
}
impl MemoryLimits {
    pub fn kernel(self) -> IncrementalLimits {
        let mut l = IncrementalLimits::default();
        if let Some(v) = self.max_records {
            l.max_records = v;
        }
        if let Some(v) = self.max_accounted_bytes {
            l.max_accounted_bytes = v;
        }
        if let Some(v) = self.max_batch_operations {
            l.max_batch_operations = v;
        }
        if let Some(v) = self.max_batch_bytes {
            l.max_batch_bytes = v;
        }
        if let Some(v) = self.max_pinned_versions {
            l.max_pinned_versions = v;
        }
        if let Some(v) = self.max_pinned_accounted_bytes {
            l.max_pinned_accounted_bytes = v;
        }
        l
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interval {
    pub from: i64,
    pub to: Option<i64>,
}
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Mutation {
    PutVertex {
        vertex: Vertex,
        valid: Option<Interval>,
    },
    PutEdge {
        edge: Edge,
        valid: Option<Interval>,
    },
    DeleteVertex {
        id: u64,
        valid: Option<Interval>,
    },
    DeleteEdge {
        id: u64,
        valid: Option<Interval>,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Apply {
    pub expected_version: u64,
    pub mutations: Vec<Mutation>,
    pub metadata: Option<Vec<u8>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepare {
    pub text: String,
    #[serde(default)]
    pub bind_start_ids: bool,
    #[serde(default)]
    pub value_steps: Vec<usize>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub text: Option<String>,
    pub prepared: Option<u64>,
    pub start_ids: Option<Vec<u64>>,
    #[serde(default)]
    pub values: Vec<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Queries {
    pub queries: Vec<Query>,
    pub version: Option<u64>,
    pub valid_time_micros: Option<i64>,
    pub limits: Option<QueryLimits>,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryLimits {
    max_queries: Option<usize>,
    max_rows: Option<usize>,
    max_work_operations: Option<usize>,
    max_work_bytes: Option<usize>,
    max_live_bytes: Option<usize>,
}
impl QueryLimits {
    pub fn kernel(self) -> QueryBatchLimits {
        let mut l = QueryBatchLimits::default();
        if let Some(v) = self.max_queries {
            l.max_queries = v;
        }
        if let Some(v) = self.max_rows {
            l.max_rows = v;
        }
        if let Some(v) = self.max_work_operations {
            l.max_work_operations = v;
        }
        if let Some(v) = self.max_work_bytes {
            l.max_work_bytes = v;
        }
        if let Some(v) = self.max_live_bytes {
            l.max_live_bytes = v;
        }
        l
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub path: String,
}
