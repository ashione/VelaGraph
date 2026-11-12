//! Closed set of concrete quorum-reading adapters used by catalog and routing.
use crate::{RemoteStorageNode, ReplicatedStorageNode, Result};
use std::{sync::Arc, time::Instant};
use tokio::sync::Semaphore;
use velagraph::{
    KeyRange, KeyValue, ReadView, ScanCursor, ScanLimit, ScanPage, Sequence, ShardId, StorageAdmin,
    StorageNode, WriteRequest, WriteResult,
};

#[derive(Clone)]
pub(crate) enum Adapter {
    Local(ReplicatedStorageNode),
    Remote(RemoteStorageNode),
    Frozen(crate::remote::query::FrozenNode),
}
macro_rules! dispatch {
    ($self:ident, $method:ident ($($arg:expr),*)) => {
        match $self { Self::Local(node) => node.$method($($arg),*), Self::Remote(node) => node.$method($($arg),*), Self::Frozen(node) => node.$method($($arg),*) }
    }
}
impl Adapter {
    pub(crate) fn scoped(&self, shard: ShardId, end: Instant, admission: Arc<Semaphore>) -> Self {
        match self {
            Self::Local(node) => Self::Local(node.scoped(shard, end, admission)),
            Self::Remote(node) => Self::Remote(node.scoped(shard, end, admission)),
            Self::Frozen(node) => Self::Frozen(node.clone()),
        }
    }
    pub(crate) fn probe_leader(&self) -> Result<()> {
        dispatch!(self, probe_leader())
    }
    pub(crate) fn check_view(&self, view: &ReadView) -> Result<()> {
        dispatch!(self, check_view(view))
    }
}
impl StorageNode for Adapter {
    fn check_execution_context(&self) -> Result<()> {
        dispatch!(self, check_execution_context())
    }
    fn read_view(&self) -> Result<ReadView> {
        dispatch!(self, read_view())
    }
    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>> {
        dispatch!(self, get(key, view))
    }
    fn scan(&self, range: KeyRange, limit: ScanLimit, view: &ReadView) -> Result<Vec<KeyValue>> {
        dispatch!(self, scan(range, limit, view))
    }
    fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
        cursor: Option<&ScanCursor>,
    ) -> Result<ScanPage> {
        dispatch!(self, scan_page(range, limit, view, cursor))
    }
    fn batch_write(&self, request: WriteRequest) -> Result<WriteResult> {
        dispatch!(self, batch_write(request))
    }
    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        expected: Option<Sequence>,
    ) -> Result<WriteResult> {
        dispatch!(self, batch_write_conditional(request, expected))
    }
    fn admin(&self) -> &dyn StorageAdmin {
        dispatch!(self, admin())
    }
}
