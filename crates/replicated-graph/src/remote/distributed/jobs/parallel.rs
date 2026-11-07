//! Disjoint, conservative budget reservations for independent shard sessions.
use super::*;
use std::{future::Future, task::Poll};

const WIDTH: usize = 3;
pub(super) struct Batch {
    pub shard: u64,
    pub indices: Vec<usize>,
    pub fragment: Fragment,
}

// Bounds successful fragment work, not allocator RSS. A snapshot row includes
// both key and value, including malformed records. No integrity check is skipped.
fn reservation(batch: &Batch, row_bytes: usize, descriptor: &Descriptor) -> Option<Cost> {
    let Fragment::HierarchyBatch { fragments, .. } = &batch.fragment else {
        return None;
    };
    let mut row = row_bytes.max(256);
    for f in fragments {
        let label = match f {
            Fragment::VerifyReverse(e) => e.label.len(),
            Fragment::ResolveIncoming { edge, .. } => edge.label.len(),
            _ => 0,
        };
        // Even an absent reverse key can be longer than any local stored key.
        row = row.max(label.checked_add(256)?);
    }
    let get = 128usize.checked_add(row.checked_mul(5)?)?;
    let mut cost = Cost {
        bytes: 512usize.checked_add(fragments.len().checked_mul(128)?)?,
        work: 1usize.checked_add(fragments.len())?,
    };
    for f in fragments {
        let (bytes, work) = match f {
            Fragment::Exists(_) => (get, 1),
            Fragment::VerifyReverse(_) => (get.checked_mul(2)?, 2),
            Fragment::ResolveIncoming { .. } => (get.checked_mul(3)?, 3),
            Fragment::In(_) | Fragment::Out(_) => {
                // observed_index: one vertex get, one scan, <=1000 rows,
                // and one entry charge. Out additionally validates canonical
                // record, outgoing index and source vertex for every edge.
                let each = 384usize.checked_add(row.checked_mul(5)?)?;
                let out = matches!(f, Fragment::Out(_));
                let each = each.checked_add(if out { get.checked_mul(3)? } else { 0 })?;
                (
                    get.checked_add(128)?.checked_add(each.checked_mul(1000)?)?,
                    if out { 5002 } else { 2002 },
                )
            }
            _ => return None,
        };
        cost.bytes = cost.bytes.checked_add(bytes)?;
        cost.work = cost.work.checked_add(work)?;
    }
    // bounded_encode admits at most MAX_WIRE bytes, charged twice. Request
    // counter widths are maximal here; actual reservations never encode wider.
    cost.bytes = cost.bytes.checked_add(MAX_WIRE.checked_mul(2)?)?;
    let request = FragmentRequest {
        descriptor: descriptor.clone(),
        fragment: batch.fragment.clone(),
        remaining: JOB_CEILING,
    };
    let encoded = serde_json::to_vec(&request).ok()?;
    cost.bytes = cost
        .bytes
        .checked_add(channel::TRANSPORT_BYTES)?
        .checked_add(encoded.len().checked_mul(2)?)?;
    cost.work = cost.work.checked_add(1)?;
    Some(cost)
}
fn fits(used: Cost, extra: Cost, remaining: Cost) -> bool {
    used.bytes
        .checked_add(extra.bytes)
        .is_some_and(|n| n <= remaining.bytes)
        && used
            .work
            .checked_add(extra.work)
            .is_some_and(|n| n <= remaining.work)
}

// Validate the returned leaf semantics before any socket can be reused by a
// later wave. Caller-level weighted merging remains separate and unchanged.
fn validate_leaves(expected: &[Fragment], values: &[FragmentValue]) -> Result<(), QueryFailure> {
    if expected.len() != values.len() {
        return Err(QueryFailure::IncompleteOrCorrupt);
    }
    let mut entries = 0usize;
    for (fragment, value) in expected.iter().zip(values) {
        let valid = match (fragment, value) {
            (Fragment::Exists(_), FragmentValue::Bool(_)) => true,
            (Fragment::VerifyReverse(_), FragmentValue::Bool(true)) => true,
            (Fragment::In(_), FragmentValue::Incoming(edges)) => {
                entries += edges.len();
                edges.len() <= 1000
                    && edges.iter().all(|e| e.label.len() <= 64)
                    && !edges
                        .windows(2)
                        .any(|w| (w[0].id, w[0].partition) >= (w[1].id, w[1].partition))
            }
            (Fragment::Out(vertex), FragmentValue::Edges(edges)) => {
                entries += edges.len();
                edges.len() <= 1000
                    && edges
                        .iter()
                        .all(|e| e.from == *vertex && e.label.len() <= 64)
                    && !edges.windows(2).any(|w| w[0].id >= w[1].id)
            }
            (
                Fragment::ResolveIncoming {
                    target,
                    edge: observed,
                },
                FragmentValue::Edge(edge),
            ) => {
                edge.to == *target
                    && edge.id == observed.id
                    && edge.from.partition == observed.partition
                    && edge.label == observed.label
            }
            _ => false,
        };
        if !valid {
            return Err(QueryFailure::IncompleteOrCorrupt);
        }
        if entries > MAX_PAGE_EDGES {
            return Err(QueryFailure::ResourceExhausted);
        }
    }
    Ok(())
}

pub(super) async fn run(
    job: &mut Job,
    mut pending: Vec<Batch>,
    outputs: &mut [Option<FragmentValue>],
    end: Instant,
) -> Result<(), QueryFailure> {
    while !pending.is_empty() {
        check(end)?;
        let remaining = Cost {
            bytes: JOB_CEILING.bytes.saturating_sub(job.cost.bytes),
            work: JOB_CEILING.work.saturating_sub(job.cost.work),
        };
        let mut selected = Vec::new();
        let mut shards = BTreeSet::new();
        let mut reserved = Cost::default();
        for (i, batch) in pending.iter().enumerate() {
            if shards.contains(&batch.shard) {
                continue;
            }
            let bound = job.row_bounds.get(&batch.shard).and_then(|row| {
                let (_, descriptor) = job.sessions.get(&batch.shard)?;
                reservation(batch, *row, descriptor)
            });
            let Some(bound) = bound.filter(|b| fits(reserved, *b, remaining)) else {
                if i == 0 {
                    break;
                }
                continue;
            };
            reserved.bytes += bound.bytes;
            reserved.work += bound.work;
            selected.push((i, bound));
            shards.insert(batch.shard);
            if selected.len() == WIDTH {
                break;
            }
        }
        // A large/skewed row, unknown bound or low remaining allowance never
        // gains a smaller quota: use precisely the existing serial path.
        if selected.len() < 2 {
            selected = vec![(0, remaining)];
        }
        let mut wave = Vec::new();
        for (i, bound) in selected.into_iter().rev() {
            wave.push((pending.remove(i), bound));
        }
        wave.reverse();
        let mut futures = Vec::new();
        for (batch, bound) in wave {
            let enabled = job.connections.is_some();
            let mut connection = job
                .connections
                .as_mut()
                .and_then(|connections| connections.remove(&batch.shard));
            let catalog = &job.catalog;
            let sessions = job.sessions.clone();
            futures.push(Box::pin(async move {
                let mut execution = Execution {
                    catalog,
                    sessions,
                    end,
                    cost: Cost::default(),
                    ceiling: bound,
                };
                let expected = match &batch.fragment {
                    Fragment::HierarchyBatch { fragments, .. } => fragments.clone(),
                    _ => unreachable!("job batches are typed"),
                };
                let value = execution
                    .fragment_transport(batch.fragment, enabled.then_some(&mut connection))
                    .await;
                (
                    batch.indices,
                    execution.cost,
                    value,
                    batch.shard,
                    connection,
                    expected,
                )
            }));
        }
        let mut replies: Vec<_> = (0..futures.len()).map(|_| None).collect();
        // Structured concurrency: no detached tasks, at most WIDTH live RPCs,
        // one per session. Dropping Advance drops every local RPC future. Data
        // workers that already started retain their own deadline and accounting.
        std::future::poll_fn(|cx| {
            let mut ready = true;
            for (future, reply) in futures.iter_mut().zip(&mut replies) {
                if reply.is_none() {
                    match future.as_mut().poll(cx) {
                        Poll::Ready(value) => *reply = Some(value),
                        Poll::Pending => ready = false,
                    }
                }
            }
            if ready {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        drop(futures);
        let mut accepted = Vec::new();
        for reply in replies {
            let (indices, cost, result, shard, connection, expected) = reply.unwrap();
            job.charge(cost.bytes, cost.work)?;
            let FragmentValue::HierarchyBatch(values) = result? else {
                return Err(QueryFailure::IncompleteOrCorrupt);
            };
            if values.len() != indices.len() {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            validate_leaves(&expected, &values)?;
            accepted.push((indices, values, shard, connection));
        }
        for (indices, values, shard, connection) in accepted {
            if let (Some(cache), Some(connection)) = (&mut job.connections, connection) {
                cache.insert(shard, connection);
            }
            for (index, value) in indices.into_iter().zip(values) {
                outputs[index] = Some(value);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn reservation_for_test(
    fragment: Fragment,
    row: usize,
    descriptor: &Descriptor,
) -> Option<Cost> {
    reservation(
        &Batch {
            shard: descriptor.stamp.shard,
            indices: vec![0],
            fragment,
        },
        row,
        descriptor,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn fixture() -> (Job, Vec<TcpListener>) {
        let mut routes = Vec::new();
        let mut sessions = BTreeMap::new();
        let mut listeners = Vec::new();
        for shard in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let route = PartitionRoute {
                partition: shard as u32 - 1,
                shard,
                epoch: 1,
                identity: RemoteIdentity {
                    group: shard,
                    node: 1,
                },
                endpoint: listener.local_addr().unwrap(),
            };
            let descriptor = Descriptor {
                stamp: QueryStamp {
                    identity: route.identity,
                    shard,
                    epoch: 1,
                    sequence: 7,
                },
                incarnation: [1; 16],
                token: [shard as u8; 16],
            };
            sessions.insert(shard, (route.clone(), descriptor));
            routes.push(route);
            listeners.push(listener);
        }
        let job = Job {
            catalog: Arc::new(StaticCatalog { partitions: routes }),
            sessions,
            row_bounds: BTreeMap::from([(1, 256), (2, 256), (3, 256)]),
            connections: None,
            rows: None,
            expires: Instant::now() + Duration::from_secs(5),
            cost: Cost::default(),
            plan: parse("g.V(1).repeat(__.out()).times(1).count()").unwrap(),
            compact: None,
            frontier: BTreeMap::new(),
            next: BTreeMap::new(),
            seen: BTreeSet::new(),
            depth: 0,
            total: 0,
            next_mass: 0,
            initialized: true,
            sequence: 0,
            last: None,
            completed: false,
            _permit: Some(JOB_SLOTS.acquire().await.unwrap()),
            _retained: RETAINED_SLOTS.acquire().await.unwrap(),
        };
        (job, listeners)
    }
    fn batch(shard: u64, index: usize) -> Batch {
        Batch {
            shard,
            indices: vec![index],
            fragment: Fragment::HierarchyBatch {
                partition: shard as u32 - 1,
                fragments: vec![Fragment::Exists(VertexId {
                    partition: shard as u32 - 1,
                    id: index as u64,
                })],
            },
        }
    }
    async fn request(socket: &mut TcpStream) -> FragmentRequest {
        let request: super::super::super::super::Request =
            read_frame(socket, Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        let Operation::QueryFragment(request) = request.operation else {
            panic!("fragment expected")
        };
        request
    }
    async fn reply(socket: &mut TcpStream, request: FragmentRequest, corrupt: bool) {
        write_frame(
            socket,
            &super::super::super::super::Response {
                version: VERSION,
                result: Ok(Reply::QueryFragment(Ok(FragmentOutput {
                    descriptor: request.descriptor.clone(),
                    cost: Cost {
                        bytes: 1000,
                        work: 2,
                    },
                    value: if corrupt {
                        FragmentValue::Bool(false)
                    } else {
                        FragmentValue::HierarchyBatch(vec![FragmentValue::Bool(
                            request.descriptor.stamp.shard != 2,
                        )])
                    },
                }))),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn independent_sessions_overlap_with_disjoint_allowances_and_atomic_outputs() {
        for corrupt in [false, true] {
            let (mut job, listeners) = fixture().await;
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let granted = Arc::new(Mutex::new(Cost::default()));
            let mut servers = Vec::new();
            for (i, listener) in listeners.into_iter().enumerate() {
                let barrier = barrier.clone();
                let granted = granted.clone();
                servers.push(tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let request = request(&mut socket).await;
                    {
                        let mut total = granted.lock().unwrap();
                        total.bytes += request.remaining.bytes;
                        total.work += request.remaining.work;
                    }
                    // Serial execution cannot pass this barrier.
                    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                        .await
                        .unwrap();
                    reply(&mut socket, request, corrupt && i == 1).await;
                }));
            }
            let mut outputs = vec![None, None, None];
            let result = run(
                &mut job,
                vec![batch(1, 2), batch(2, 0), batch(3, 1)],
                &mut outputs,
                Instant::now() + Duration::from_secs(5),
            )
            .await;
            for server in servers {
                tokio::time::timeout(Duration::from_secs(5), server)
                    .await
                    .unwrap()
                    .unwrap();
            }
            if corrupt {
                assert!(matches!(result, Err(QueryFailure::IncompleteOrCorrupt)));
                assert!(outputs.iter().all(Option::is_none));
            } else {
                result.unwrap();
                assert!(matches!(outputs[0], Some(FragmentValue::Bool(false))));
                assert!(matches!(outputs[1], Some(FragmentValue::Bool(true))));
                assert!(matches!(outputs[2], Some(FragmentValue::Bool(true))));
                assert!(job.cost.bytes > 3000);
                assert_eq!(job.cost.work, 9);
            }
            let total = *granted.lock().unwrap();
            assert!(total.bytes <= JOB_CEILING.bytes && total.work <= JOB_CEILING.work);
        }
    }
    #[tokio::test]
    async fn low_budget_falls_back_to_whole_remaining_serial_allowance() {
        let (mut job, listeners) = fixture().await;
        job.cost = Cost {
            bytes: JOB_CEILING.bytes - 65536,
            work: JOB_CEILING.work - 100,
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut servers = Vec::new();
        for listener in listeners {
            let requests = requests.clone();
            servers.push(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = request(&mut socket).await;
                requests.lock().unwrap().push(request.remaining);
                reply(&mut socket, request, false).await;
            }));
        }
        let mut outputs = vec![None, None, None];
        run(
            &mut job,
            vec![batch(1, 0), batch(2, 1), batch(3, 2)],
            &mut outputs,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        for server in servers {
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
        }
        let grants = requests.lock().unwrap();
        assert_eq!(grants.len(), 3);
        assert!(grants[0].bytes > 60000 && grants[0].work == 99);
        assert!(grants
            .windows(2)
            .all(|pair| pair[1].bytes < pair[0].bytes && pair[1].work + 3 == pair[0].work));
    }
    #[tokio::test]
    async fn dropping_wave_closes_all_local_rpc_futures() {
        let (mut job, listeners) = fixture().await;
        let started = Arc::new(AtomicUsize::new(0));
        let mut servers = Vec::new();
        for listener in listeners {
            let started = started.clone();
            servers.push(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let _request = request(&mut socket).await;
                started.fetch_add(1, Ordering::SeqCst);
                let mut byte = [0];
                assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
            }));
        }
        let task = tokio::spawn(async move {
            let mut outputs = vec![None, None, None];
            run(
                &mut job,
                vec![batch(1, 0), batch(2, 1), batch(3, 2)],
                &mut outputs,
                Instant::now() + Duration::from_secs(5),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        for server in servers {
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
    #[tokio::test]
    async fn same_session_batches_are_serial_while_other_shard_overlaps() {
        let (mut job, mut listeners) = fixture().await;
        drop(listeners.pop());
        let second_listener = listeners.pop().unwrap();
        let first_listener = listeners.pop().unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let peer_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            let (mut socket, _) = first_listener.accept().await.unwrap();
            let first_request = request(&mut socket).await;
            tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                .await
                .unwrap();
            // No second request to this session may be emitted before the
            // first reply, even though another independent shard is active.
            assert!(
                tokio::time::timeout(Duration::from_millis(30), first_listener.accept())
                    .await
                    .is_err()
            );
            reply(&mut socket, first_request, false).await;
            let (mut socket, _) = first_listener.accept().await.unwrap();
            let second_request = request(&mut socket).await;
            reply(&mut socket, second_request, false).await;
        });
        let second = tokio::spawn(async move {
            let (mut socket, _) = second_listener.accept().await.unwrap();
            let request = request(&mut socket).await;
            tokio::time::timeout(Duration::from_secs(5), peer_barrier.wait())
                .await
                .unwrap();
            reply(&mut socket, request, false).await;
        });
        let mut outputs = vec![None, None, None];
        run(
            &mut job,
            vec![batch(1, 0), batch(1, 1), batch(2, 2)],
            &mut outputs,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        for server in [first, second] {
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
        }
        assert!(matches!(outputs[0], Some(FragmentValue::Bool(true))));
        assert!(matches!(outputs[1], Some(FragmentValue::Bool(true))));
        assert!(matches!(outputs[2], Some(FragmentValue::Bool(false))));
    }
    #[test]
    fn reservation_overflow_is_a_serial_fallback_not_a_small_grant() {
        let descriptor = Descriptor {
            stamp: QueryStamp {
                identity: RemoteIdentity { group: 1, node: 1 },
                shard: 1,
                epoch: 1,
                sequence: 1,
            },
            incarnation: [1; 16],
            token: [2; 16],
        };
        assert!(reservation(&batch(1, 0), usize::MAX, &descriptor).is_none());
        assert!(!fits(
            Cost {
                bytes: usize::MAX,
                work: 0
            },
            Cost { bytes: 1, work: 0 },
            JOB_CEILING
        ));
    }
}
