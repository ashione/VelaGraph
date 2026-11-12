//! Query-bound fixed-V orchestration. No raw KV is downloaded on this path.
use super::*;
use crate::remote::{
    distributed::{fixed, fixed_execution, DistributedRequest, PartitionRoute},
    RemoteError, RemoteIdentity,
};
use d2_raft::managed_rpc::{Identity, Role};

/// Deployment-owned graph port and exact matching managed identity. The port is
/// never inferred from a managed address and never accepted from a query.
#[derive(Clone, Debug)]
pub struct LocalGraphRoute {
    pub endpoint: SocketAddr,
    pub identity: RemoteIdentity,
    pub managed_identity: Identity,
}
/// A query-result identity, distinct from both observed and materialized sessions.
/// It is not a credential or a restartable token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSession {
    pub incarnation: [u8; 16],
    pub token: [u8; 32],
    pub version: u64,
}
pub(super) struct Retained {
    descriptor: LocalSession,
    requested_profile: Option<LocalReadProfile>,
    pub(super) expires: Instant,
    _memory: OwnedSemaphorePermit,
    result: pages::Materialized,
    pub(super) closed: std::sync::atomic::AtomicBool,
    // Immutable session/result/page sizing: one successful HTTP frame proof is
    // valid for every subsequent fetch. Failed/expired proofs are never cached.
    http_preflighted: std::sync::atomic::AtomicBool,
    grpc_preflighted: std::sync::atomic::AtomicBool,
}
pub(super) fn snapshot_error(error: cohort_reads::Error) -> Error {
    managed_graph::Error::from(error).into()
}
pub(super) fn remote_error(error: RemoteError) -> Error {
    match error {
        RemoteError::ResourceExhausted
        | RemoteError::Application(d2_raft::D1Error::VisibilityReadLimit) => Error::Capacity,
        RemoteError::Application(_) => Error::Rejected,
        RemoteError::InvalidArgument | RemoteError::Protocol => Error::Identity,
        RemoteError::SessionUnavailable => Error::SessionUnavailable,
        _ => Error::Unknown,
    }
}
pub(super) fn request(query: &Query) -> DistributedRequest {
    DistributedRequest {
        start_partition: query.partition,
        text: query.text.clone(),
        timeout_millis: 5000,
    }
}
pub(super) fn validate_routes(config: &RemoteConfig, routes: &[LocalGraphRoute]) -> Result<()> {
    let cohort = &config.binding.cohort;
    if routes.is_empty()
        || routes.len() > fixed::MAX_MEMBERS
        || routes.len() != cohort.members.len()
        || routes.len() != config.members.len()
    {
        return Err(Error::Identity);
    }
    let coordinator = config.coordinator.identity();
    if coordinator.role != Role::Coordinator
        || coordinator.authority != config.binding.authority
        || coordinator.cohort != *cohort
    {
        return Err(Error::Identity);
    }
    let mut endpoints = BTreeSet::new();
    let mut identities = BTreeSet::new();
    let mut groups = BTreeSet::new();
    for ((route, client), member) in routes.iter().zip(&config.members).zip(&cohort.members) {
        let id = &route.managed_identity;
        if id != client.identity()
            || id.role != Role::Participant
            || id.authority != config.binding.authority
            || id.cohort != *cohort
            || id.target.shard != member.shard
            || id.target.epoch != member.epoch
            || id.target.mode != member.mode
            || member.shard > u64::from(u32::MAX)
            || route.identity.group != id.group
            || route.identity.node != id.node
            || id.node == 0
            || route.endpoint.port() == 0
            || !endpoints.insert(route.endpoint)
            || !identities.insert((id.group, id.node))
            || !groups.insert(id.group)
        {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
pub(super) fn live(retained: &Retained, end: Instant) -> Result<()> {
    if retained.closed.load(std::sync::atomic::Ordering::Acquire)
        || Instant::now() >= retained.expires
    {
        return Err(Error::SessionUnavailable);
    }
    before_deadline(end)
}
pub(super) fn preflight(binding: &Binding, reply: Reply) -> Result<()> {
    encode(
        &Response {
            version: VERSION,
            request_id: [255; 16],
            binding: binding.clone(),
            result: Ok(reply),
        },
        MAX_RESPONSE_BYTES,
    )?;
    Ok(())
}
fn preflight_pages(
    binding: &Binding,
    profile: Option<LocalReadProfile>,
    session: &LocalSession,
    result: &pages::Materialized,
    end: Instant,
) -> Result<()> {
    for offset in (0..result.data.rows.len().max(1)).step_by(usize::from(result.size)) {
        before_deadline(end)?;
        preflight(
            binding,
            Reply::FetchLocalQuery {
                session: session.clone(),
                query: result.query.clone(),
                cursor: result.cursors.get(&offset).cloned(),
                page_size: result.size,
                page: result.page(offset),
            },
        )?;
    }
    before_deadline(end)?;
    preflight(binding, open_reply(profile, session, result))
}
// HTTP has a different preview envelope. Validate every future page before
// delivering any HTTP page, including a session originally opened over TCP.
fn preflight_http_pages(
    session: &LocalSession,
    result: &pages::Materialized,
    end: Instant,
) -> Result<()> {
    for offset in (0..result.data.rows.len().max(1)).step_by(usize::from(result.size)) {
        before_deadline(end)?;
        http_codec::encode(&Ok(Reply::FetchLocalQuery {
            session: session.clone(),
            query: result.query.clone(),
            cursor: result.cursors.get(&offset).cloned(),
            page_size: result.size,
            page: result.page(offset),
        }))?;
    }
    before_deadline(end)
}
impl Retained {
    pub(super) fn preflight_grpc(&self, end: Instant) -> Result<()> {
        live(self, end)?;
        if !self
            .grpc_preflighted
            .load(std::sync::atomic::Ordering::Acquire)
        {
            for offset in
                (0..self.result.data.rows.len().max(1)).step_by(usize::from(self.result.size))
            {
                before_deadline(end)?;
                grpc_codec::encode_fetch(&Reply::FetchLocalQuery {
                    session: self.descriptor.clone(),
                    query: self.result.query.clone(),
                    cursor: self.result.cursors.get(&offset).cloned(),
                    page_size: self.result.size,
                    page: self.result.page(offset),
                })?;
            }
            live(self, end)?;
            self.grpc_preflighted
                .store(true, std::sync::atomic::Ordering::Release);
        }
        live(self, end)
    }
    pub(super) fn preflight_http(&self, end: Instant) -> Result<()> {
        live(self, end)?;
        if !self
            .http_preflighted
            .load(std::sync::atomic::Ordering::Acquire)
        {
            preflight_http_pages(&self.descriptor, &self.result, end)?;
            live(self, end)?;
            self.http_preflighted
                .store(true, std::sync::atomic::Ordering::Release);
        }
        live(self, end)
    }
}
fn open_reply(
    profile: Option<LocalReadProfile>,
    session: &LocalSession,
    result: &pages::Materialized,
) -> Reply {
    match profile {
        None => Reply::OpenLocalQuery {
            session: session.clone(),
            query: result.query.clone(),
            page_size: result.size,
            page: result.page(0),
        },
        Some(profile) => Reply::OpenLocalQueryWithProfile {
            profile,
            session: session.clone(),
            query: result.query.clone(),
            page_size: result.size,
            page: result.page(0),
        },
    }
}

pub(super) fn valid_page(
    query: &Query,
    cursor: Option<&PageCursor>,
    size: u16,
    page: &QueryPage,
    binding: &Binding,
) -> bool {
    fixed_execution::validate(&request(query)).is_ok()
        && pages::valid_page(query, cursor, size, page, binding)
}
impl State {
    pub(super) fn get_local(&self, descriptor: &LocalSession) -> Result<Arc<Retained>> {
        self.expire();
        if descriptor.incarnation != self.incarnation {
            return Err(Error::SessionUnavailable);
        }
        let sessions = self.local_sessions.lock().unwrap();
        let found = sessions
            .get(&descriptor.token)
            .ok_or(Error::SessionUnavailable)?;
        if found.descriptor != *descriptor {
            return Err(Error::Identity);
        }
        if found.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::SessionUnavailable);
        }
        Ok(found.clone())
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn open_local(
        &self,
        requested_profile: Option<LocalReadProfile>,
        version: Option<u64>,
        minimum_version: Option<u64>,
        ttl_millis: u64,
        query: Query,
        page_size: u16,
        work: Arc<OwnedSemaphorePermit>,
        end: Instant,
    ) -> Result<Reply> {
        let routes = self
            .config
            .local_routes
            .as_ref()
            .ok_or(Error::Query(QueryFailure::Unsupported))?;
        let ttl = Duration::from_millis(ttl_millis);
        if ttl.is_zero() || ttl > MAX_TTL || !(1..=MAX_PAGE_SIZE).contains(&page_size) {
            return Err(Error::Protocol);
        }
        let req = request(&query);
        fixed_execution::validate(&req).map_err(Error::Query)?;
        if !self
            .config
            .binding
            .cohort
            .members
            .iter()
            .any(|m| m.shard == u64::from(query.partition))
        {
            return Err(Error::Identity);
        }
        // Reuse the existing process-wide observed coordinator execution gate.
        // Certification and proof preflight are part of the same admitted work.
        let execution = fixed_execution::admit_execution().map_err(Error::Query)?;
        self.expire();
        let memory = self
            .memory
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let cut = if let Some(minimum) = minimum_version {
            cohort_reads::admit_endpoints_at_least(
                self.config.coordinator.reference(),
                self.config.binding.authority.clone(),
                &self.config.binding.cohort,
                self.config.endpoints(),
                minimum,
                ttl,
                end,
            )
            .await
        } else {
            cohort_reads::admit_endpoints(
                self.config.coordinator.reference(),
                self.config.binding.authority.clone(),
                self.config.binding.cohort.coordinator_epoch,
                self.config.endpoints(),
                version,
                ttl,
            )
            .await
        }
        .map_err(snapshot_error)?;
        before_deadline(end)?;
        if cut.cohort() != &self.config.binding.cohort
            || cut.authority() != &self.config.binding.authority
        {
            return Err(Error::Identity);
        }
        let expires = cut.expires_at();
        let acquire_end = end.min(cut.deadline());
        let profile = requested_profile.unwrap_or_default();
        let limit = profile.capture_limit();
        let remaining_limit = |rows: usize, bytes: usize, ops: usize| ReadLimit {
            rows: limit.rows.saturating_sub(rows),
            bytes: limit.bytes.saturating_sub(bytes),
            operations: limit.operations.saturating_sub(ops),
        };
        let make_open = |target: Target, limit: ReadLimit| fixed::FixedOpenRequest {
            profile,
            target,
            proof: cut.proof().clone(),
            capture_limit: limit,
            ttl_millis: expires
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(60_000) as u64,
            acquire_timeout_millis: acquire_end
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(5000) as u64,
        };
        // All members' full request and worst reply must fit before opening one.
        for (route, (_, target)) in routes.iter().zip(cut.members()) {
            cut.check().map_err(snapshot_error)?;
            fixed::preflight(
                route.identity,
                &make_open(target.clone(), remaining_limit(0, 0, 0)),
                tokio::time::Instant::from_std(acquire_end),
            )
            .map_err(remote_error)?;
        }
        let mut sessions = BTreeMap::new();
        let result = async {
            let (mut rows, mut bytes, mut operations) = (0usize, 0usize, 0usize);
            for (route, (_, target)) in routes.iter().zip(cut.members()) {
                cut.check().map_err(snapshot_error)?;
                before_deadline(acquire_end)?;
                let remaining = remaining_limit(rows, bytes, operations);
                // Match materialized acquisition: even a later empty member
                // cannot turn an exhausted aggregate budget into success.
                if remaining.rows == 0 || remaining.bytes == 0 || remaining.operations == 0 {
                    return Err(Error::Capacity);
                }
                let opened = fixed::open(
                    route.endpoint,
                    route.identity,
                    make_open(target.clone(), remaining),
                    acquire_end.saturating_duration_since(Instant::now()),
                )
                .await
                .map_err(remote_error)?;
                let descriptor = &opened.descriptor;
                if descriptor.authority != *cut.authority()
                    || descriptor.cohort != *cut.cohort()
                    || descriptor.version != cut.version()
                    || descriptor.mode != target.mode
                    || descriptor.stamp.identity != route.identity
                    || descriptor.stamp.shard != target.shard
                    || descriptor.stamp.epoch != target.epoch
                    || descriptor.incarnation == [0; 16]
                    || descriptor.token == [0; 16]
                {
                    return Err(Error::Identity);
                }
                // Once endpoint/descriptor identity is safe, own cleanup even
                // if subsequent proof or aggregate-cost validation fails.
                let partition = u32::try_from(target.shard).map_err(|_| Error::Identity)?;
                sessions.insert(
                    target.shard,
                    fixed_execution::NodeSession::new(
                        PartitionRoute {
                            partition,
                            shard: target.shard,
                            epoch: target.epoch,
                            identity: route.identity,
                            endpoint: route.endpoint,
                        },
                        opened.descriptor.clone(),
                        opened.capture_cost.bytes,
                        opened.capture_cost.rows,
                    ),
                );
                // Compare the complete retained certificate, not a boolean or V.
                if opened.descriptor.profile != profile || opened.retained_proof != *cut.proof() {
                    return Err(Error::Identity);
                }
                rows = rows
                    .checked_add(opened.capture_cost.rows)
                    .ok_or(Error::Capacity)?;
                bytes = bytes
                    .checked_add(opened.capture_cost.bytes)
                    .ok_or(Error::Capacity)?;
                operations = operations
                    .checked_add(opened.capture_cost.replay_operations)
                    .ok_or(Error::Capacity)?;
                if rows > limit.rows || bytes > limit.bytes || operations > limit.operations {
                    return Err(Error::Capacity);
                }
            }
            cut.check().map_err(snapshot_error)?;
            fixed_execution::execute(
                &mut sessions,
                &req,
                self.config.local_query_parallelism,
                tokio::time::Instant::from_std(end.min(expires)),
            )
            .await
            .map_err(Error::Query)
        }
        .await;
        // Successful cleanup is not an extra required graph read and cannot
        // convert an already complete result into a stronger liveness promise.
        fixed_execution::close(&sessions, tokio::time::Instant::from_std(end.min(expires))).await;
        let rows = result?;
        let descriptor = LocalSession {
            incarnation: self.incarnation,
            token: random()?,
            version: cut.version(),
        };
        let binding = self.config.binding.clone();
        let retained = tokio::task::spawn_blocking(move || {
            // Blocking encoding cannot be canceled; hold both shared permits.
            let _work = work;
            let _execution = execution;
            let data = QueryData {
                rows,
                completeness: QueryCompleteness::Unverified,
            };
            if data.rows.len() > pages::MAX_ROWS
                || !query::valid_traversal_output(&query, &data, &binding)
            {
                return Err(Error::Capacity);
            }
            encode(&data, pages::MAX_RESULT_BYTES)?;
            let mut materialized = pages::Materialized {
                query: query.clone(),
                size: page_size,
                data,
                offsets: BTreeMap::new(),
                cursors: BTreeMap::new(),
            };
            for offset in (usize::from(page_size)..materialized.data.rows.len())
                .step_by(usize::from(page_size))
            {
                before_deadline(end)?;
                let cursor = PageCursor { token: random()? };
                if materialized
                    .offsets
                    .insert(cursor.clone(), offset)
                    .is_some()
                {
                    return Err(Error::Unknown);
                }
                materialized.cursors.insert(offset, cursor);
            }
            let retained = Arc::new(Retained {
                descriptor,
                requested_profile,
                expires,
                _memory: memory,
                result: materialized,
                closed: std::sync::atomic::AtomicBool::new(false),
                http_preflighted: std::sync::atomic::AtomicBool::new(false),
                grpc_preflighted: std::sync::atomic::AtomicBool::new(false),
            });
            preflight_pages(
                &binding,
                retained.requested_profile,
                &retained.descriptor,
                &retained.result,
                end.min(expires),
            )?;
            live(&retained, end)?;
            Ok(retained)
        })
        .await
        .map_err(|_| Error::Unknown)??;
        live(&retained, end)?;
        let reply = open_reply(
            retained.requested_profile,
            &retained.descriptor,
            &retained.result,
        );
        let mut sessions = self.local_sessions.lock().unwrap();
        if sessions.contains_key(&retained.descriptor.token) {
            return Err(Error::Unknown);
        }
        sessions.insert(retained.descriptor.token, retained);
        Ok(reply)
    }
    pub(super) fn fetch_local(
        &self,
        session: LocalSession,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
        end: Instant,
    ) -> Result<Reply> {
        let retained = self.get_local(&session)?;
        live(&retained, end)?;
        if retained.result.query != query || retained.result.size != page_size {
            return Err(Error::Identity);
        }
        let offset = match &cursor {
            None => 0,
            Some(cursor) => *retained.result.offsets.get(cursor).ok_or(Error::Identity)?,
        };
        let page = retained.result.page(offset);
        live(&retained, end)?;
        Ok(Reply::FetchLocalQuery {
            session,
            query,
            cursor,
            page_size,
            page,
        })
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    use d2_raft::transaction_decisions::Member;
    #[test]
    fn local_send_check_rejects_expiry_and_close_after_encoding() {
        let now = Instant::now();
        let mut retained = Retained {
            requested_profile: None,
            descriptor: LocalSession {
                incarnation: [1; 16],
                token: [1; 32],
                version: 0,
            },
            expires: now + Duration::from_secs(10),
            _memory: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            result: pages::Materialized {
                query: Query {
                    partition: 7,
                    text: "g.V(1).count()".into(),
                },
                size: 1,
                data: QueryData {
                    completeness: QueryCompleteness::Unverified,
                    rows: vec![QueryRow::Count(0)],
                },
                offsets: BTreeMap::new(),
                cursors: BTreeMap::new(),
            },
            closed: std::sync::atomic::AtomicBool::new(false),
            http_preflighted: std::sync::atomic::AtomicBool::new(false),
            grpc_preflighted: std::sync::atomic::AtomicBool::new(false),
        };
        let end = now + Duration::from_secs(20);
        live(&retained, end).unwrap();
        assert!(!retained
            .http_preflighted
            .load(std::sync::atomic::Ordering::Acquire));
        retained.preflight_http(end).unwrap();
        assert!(retained
            .http_preflighted
            .load(std::sync::atomic::Ordering::Acquire));
        retained.preflight_http(end).unwrap();
        // The encoder can complete before TTL while the awaiting task resumes
        // afterwards. The same mandatory pre-send check must reject this state.
        retained.expires = now;
        assert_eq!(live(&retained, end), Err(Error::SessionUnavailable));
        assert_eq!(retained.preflight_http(end), Err(Error::SessionUnavailable));
        retained.expires = now + Duration::from_secs(10);
        retained
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        assert_eq!(live(&retained, end), Err(Error::SessionUnavailable));
        assert_eq!(retained.preflight_http(end), Err(Error::SessionUnavailable));
    }
    #[test]
    fn seal_checks_late_full_envelope_before_ready() {
        // Synthetic serializer boundary, not a claim that M1's default-100
        // three-hop result grammar can produce this oversized property row.
        let binding = Binding {
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: Cohort {
                profile: Default::default(),
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
            },
            mapping: Mapping::PartitionEqualsShardV1,
        };
        let session = LocalSession {
            incarnation: [255; 16],
            token: [255; 32],
            version: u64::MAX,
        };
        let cursor = PageCursor { token: [255; 32] };
        let result = pages::Materialized {
            query: Query {
                partition: 7,
                text: "g.V(1)".into(),
            },
            size: 1,
            data: QueryData {
                completeness: QueryCompleteness::Unverified,
                rows: vec![
                    QueryRow::Count(1),
                    QueryRow::Value {
                        source: None,
                        value: Value::Text("x".repeat(MAX_RESPONSE_BYTES)),
                    },
                ],
            },
            offsets: [(cursor.clone(), 1)].into(),
            cursors: [(1, cursor)].into(),
        };
        preflight(
            &binding,
            Reply::OpenLocalQuery {
                session: session.clone(),
                query: result.query.clone(),
                page_size: 1,
                page: result.page(0),
            },
        )
        .unwrap();
        assert_eq!(
            preflight_pages(
                &binding,
                None,
                &session,
                &result,
                Instant::now() + Duration::from_secs(5)
            ),
            Err(Error::Capacity)
        );
    }
    #[test]
    fn explicit_profile_first_reply_uses_its_complete_larger_envelope() {
        // Synthetic serializer boundary, not supported M1 grammar output.
        let binding = Binding {
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: Cohort {
                profile: Default::default(),
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
            },
            mapping: Mapping::PartitionEqualsShardV1,
        };
        let session = LocalSession {
            incarnation: [255; 16],
            token: [255; 32],
            version: u64::MAX,
        };
        let mut result = pages::Materialized {
            query: Query {
                partition: 7,
                text: "g.V(1)".into(),
            },
            size: 1,
            data: QueryData {
                completeness: QueryCompleteness::Unverified,
                rows: vec![QueryRow::Value {
                    source: None,
                    value: Value::Text(String::new()),
                }],
            },
            offsets: BTreeMap::new(),
            cursors: BTreeMap::new(),
        };
        let frame_len = |reply| {
            serde_json::to_vec(&Response {
                version: VERSION,
                request_id: [255; 16],
                binding: binding.clone(),
                result: Ok(reply),
            })
            .unwrap()
            .len()
        };
        let old_open = frame_len(open_reply(None, &session, &result));
        let old_fetch = frame_len(Reply::FetchLocalQuery {
            session: session.clone(),
            query: result.query.clone(),
            cursor: None,
            page_size: 1,
            page: result.page(0),
        });
        let padding = MAX_RESPONSE_BYTES - old_open.max(old_fetch);
        result.data.rows = vec![QueryRow::Value {
            source: None,
            value: Value::Text("x".repeat(padding)),
        }];
        let end = Instant::now() + Duration::from_secs(5);
        preflight_pages(&binding, None, &session, &result, end).unwrap();
        for profile in [
            LocalReadProfile::Parity2MiBV1,
            LocalReadProfile::LocalRead8MiBV1,
        ] {
            assert_eq!(
                preflight_pages(&binding, Some(profile), &session, &result, end),
                Err(Error::Capacity)
            );
        }
    }
    #[test]
    fn capture_capacity_errors_do_not_become_unknown_or_identity() {
        assert_eq!(
            remote_error(RemoteError::Application(
                d2_raft::D1Error::VisibilityReadLimit
            )),
            Error::Capacity
        );
    }
}

#[cfg(test)]
mod http_boundary_tests {
    use super::*;
    #[test]
    fn http_preflight_rejects_later_page_before_first_delivery() {
        let session = LocalSession {
            incarnation: [1; 16],
            token: [2; 32],
            version: 1,
        };
        let result = pages::Materialized {
            query: Query {
                partition: 7,
                text: "g.V(1,2).valueMap('x')".into(),
            },
            size: 1,
            data: QueryData {
                completeness: QueryCompleteness::Unverified,
                rows: vec![
                    QueryRow::Count(1),
                    QueryRow::PropertyMap {
                        source: crate::remote::query::QualifiedElement::Vertex {
                            partition: 7,
                            id: 1,
                        },
                        properties: BTreeMap::from([("x".into(), vec![Value::Integer(0); 60_000])]),
                    },
                ],
            },
            offsets: BTreeMap::new(),
            cursors: BTreeMap::new(),
        };
        // A small first page alone is insufficient to admit this result.
        assert!(http_codec::encode(&Ok(open_reply(None, &session, &result))).is_ok());
        assert_eq!(
            preflight_http_pages(&session, &result, Instant::now() + Duration::from_secs(5)),
            Err(Error::Capacity)
        );
    }
}
