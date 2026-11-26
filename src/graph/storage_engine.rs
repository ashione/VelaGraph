//! GraphEngine entry point for a graph/v1 partition on a storage node.
use super::{Edge, GraphEngine, PartitionGraph, StorageGraphEngine, Vertex};
use crate::{
    Error, GraphRecordReadView, GraphRecordStore, GremlinResult, Result, ShardId, StorageNode,
    MAX_ADJACENCY_EDGES,
};

impl GraphEngine {
    /// Bind a graph partition to a storage node. Each operation validates the
    /// configured shard against the node's view. Raft-backed calls use the
    /// adapter's blocking execution context.
    pub fn from_storage_node<S: StorageNode>(
        node: S,
        shard: ShardId,
        partition: u32,
    ) -> StorageGraphEngine<S> {
        PartitionGraph {
            backend: GraphRecordStore::new(node, shard, partition),
        }
    }
}

impl<S: StorageNode> StorageGraphEngine<S> {
    pub fn partition(&self) -> u32 {
        self.backend.partition()
    }

    /// Use `prepare_*` and `commit_prepared` on this store to retain an exact
    /// mutation for retry across an uncertain response or a replica restart.
    pub fn records(&self) -> &GraphRecordStore<S> {
        &self.backend
    }

    pub fn read_view(&self) -> Result<GraphRecordReadView<'_, S>> {
        self.backend.read_view()
    }

    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        self.backend.get_vertex(id)
    }

    pub fn create_vertex(&self, vertex: &Vertex) -> Result<()> {
        self.backend.create_vertex(vertex)
    }

    pub fn update_vertex(&self, vertex: &Vertex) -> Result<()> {
        self.backend.update_vertex(vertex)
    }

    pub fn delete_vertex(&self, id: u64) -> Result<bool> {
        match self.backend.delete_vertex(id) {
            Ok(()) => Ok(true),
            Err(Error::VertexNotFound(missing)) if missing == id => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        self.backend.get_edge(id)
    }

    pub fn create_edge(&self, edge: &Edge) -> Result<()> {
        self.backend.create_edge(edge)
    }

    pub fn update_edge(&self, edge: &Edge) -> Result<()> {
        self.backend.update_edge(edge)
    }

    pub fn delete_edge(&self, id: u64) -> Result<bool> {
        match self.backend.delete_edge(id) {
            Ok(()) => Ok(true),
            Err(Error::EdgeNotFound(missing)) if missing == id => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn out_edges(&self, id: u64, label: Option<&str>) -> Result<Vec<Edge>> {
        self.backend.out_edges(id, label, MAX_ADJACENCY_EDGES)
    }

    pub fn in_edges(&self, id: u64, label: Option<&str>) -> Result<Vec<Edge>> {
        self.backend.in_edges(id, label, MAX_ADJACENCY_EDGES)
    }

    pub fn gremlin(&self, query: &str) -> Result<GremlinResult> {
        self.backend.execute_gremlin(query)
    }
}
