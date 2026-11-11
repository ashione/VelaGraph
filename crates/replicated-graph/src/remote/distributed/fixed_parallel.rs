//! Ordered leaf scheduling over certified sessions, independent of query shape.
//! Every in-flight allowance is disjoint; one session has at most one RPC.
use super::*;
use fixed_execution::NodeSession;
use std::{future::Future, task::Poll};

// Matches the existing bounded job executor. This is a scheduling width, not a
// new query/node budget. Skew or insufficient headroom uses the serial allowance.
pub(super) const WIDTH: usize = 3;

fn fits(used: Cost, extra: Cost, remaining: Cost) -> bool {
    used.bytes
        .checked_add(extra.bytes)
        .is_some_and(|n| n <= remaining.bytes)
        && used
            .work
            .checked_add(extra.work)
            .is_some_and(|n| n <= remaining.work)
}

// Bound the successful leaf's MeteredNode charges, both serialization copies,
// and client transport charges. Total retained capture bytes bound any stored
// key+value, including malformed records; absent keys are covered separately.
// This deliberately trades parallel opportunities for no new wire contract.
fn reservation(fragment: &Fragment, node: &NodeSession) -> Option<Cost> {
    let label = match fragment {
        Fragment::VerifyReverse(edge) => edge.label.len(),
        Fragment::ResolveIncoming { edge, .. } => edge.label.len(),
        _ => 0,
    };
    let row = node.capture_bytes.max(label.checked_add(256)?);
    let get = 128usize.checked_add(row.checked_mul(5)?)?;
    let (bytes, work) = match fragment {
        Fragment::Exists(_) => (get, 1usize),
        Fragment::VerifyReverse(_) => (get.checked_mul(2)?, 2),
        Fragment::ResolveIncoming { .. } => (get.checked_mul(3)?, 3),
        Fragment::Out(_) | Fragment::In(_) => {
            let each = 384usize.checked_add(row.checked_mul(5)?)?;
            let out = matches!(fragment, Fragment::Out(_));
            let each = each.checked_add(if out { get.checked_mul(3)? } else { 0 })?;
            // After validating the source, all candidate index rows are
            // distinct from that vertex. Do not assume a canonical row exists
            // for each index: dangling indexes must reach their original
            // corruption error, not exhaust a smaller speculative allowance.
            // This uses immutable capture dimensions, never labels, IDs,
            // estimated degrees, or a query-specific plan.
            let entries = node.capture_rows.saturating_sub(1).min(1000);
            (
                get.checked_add(128)?
                    .checked_add(each.checked_mul(entries)?)?,
                // MeteredNode::scan requires one unit of headroom even for
                // an empty scan; it does not charge that unit when no row exists.
                2usize.checked_add(entries.checked_mul(if out { 5 } else { 2 })?.max(1))?,
            )
        }
        _ => return None,
    };
    let request = fixed::FixedFragmentRequest {
        descriptor: node.descriptor.clone(),
        request_sequence: node.sequence,
        fragment: fragment.clone(),
        remaining: CEILING,
    };
    let encoded = serde_json::to_vec(&request).ok()?;
    Some(Cost {
        bytes: bytes
            .checked_add(1024)?
            .checked_add(MAX_WIRE.checked_mul(2)?)?
            .checked_add(encoded.len().checked_mul(2)?)?,
        work: work.checked_add(2)?,
    })
}

fn validate(fragment: &Fragment, value: &FragmentValue) -> Result<(), QueryFailure> {
    let valid = match (fragment, value) {
        (Fragment::Exists(_), FragmentValue::Bool(_)) => true,
        (Fragment::VerifyReverse(_), FragmentValue::Bool(true)) => true,
        (Fragment::Out(source), FragmentValue::Edges(edges)) => {
            edges.len() <= 1000
                && edges
                    .iter()
                    .all(|e| e.from == *source && e.label.len() <= 64)
                && edges
                    .windows(2)
                    .all(|w| (w[0].id, w[0].from.partition) < (w[1].id, w[1].from.partition))
        }
        (Fragment::In(_), FragmentValue::Incoming(edges)) => {
            edges.len() <= 1000
                && edges.iter().all(|e| e.label.len() <= 64)
                && edges
                    .windows(2)
                    .all(|w| (w[0].id, w[0].partition) < (w[1].id, w[1].partition))
        }
        (
            Fragment::ResolveIncoming {
                target,
                edge: observed,
            },
            FragmentValue::Edge(edge),
        ) => {
            edge.label.len() <= 64
                && edge.to == *target
                && edge.id == observed.id
                && edge.from.partition == observed.partition
                && edge.label == observed.label
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(QueryFailure::IncompleteOrCorrupt)
    }
}

pub(super) async fn run(
    sessions: &mut BTreeMap<u64, NodeSession>,
    fragments: Vec<Fragment>,
    cost: &mut Cost,
    end: Instant,
) -> Result<Vec<FragmentValue>, QueryFailure> {
    check(end)?;
    if fragments.len() > MAX_ROWS {
        return Err(QueryFailure::ResourceExhausted);
    }
    let mut pending: Vec<_> = fragments.into_iter().enumerate().collect();
    let mut outputs: Vec<_> = (0..pending.len()).map(|_| None).collect();
    // Each wave removes at least one bounded input; no retries or new sessions.
    while !pending.is_empty() {
        check(end)?;
        let remaining = Cost {
            bytes: CEILING.bytes.saturating_sub(cost.bytes),
            work: CEILING.work.saturating_sub(cost.work),
        };
        let mut selected = Vec::new();
        let mut shards = BTreeSet::new();
        let mut reserved = Cost::default();
        for (i, (_, fragment)) in pending.iter().enumerate() {
            let shard = u64::from(fragment.partition());
            if shards.contains(&shard) {
                continue;
            }
            let node = sessions.get(&shard).ok_or(QueryFailure::UnsupportedScope)?;
            let Some(bound) = reservation(fragment, node).filter(|b| fits(reserved, *b, remaining))
            else {
                if i == 0 {
                    break;
                }
                continue;
            };
            reserved.bytes += bound.bytes;
            reserved.work += bound.work;
            selected.push((i, bound));
            shards.insert(shard);
            if selected.len() == WIDTH {
                break;
            }
        }
        // Never divide the remaining budget: a valid large or skewed request
        // keeps exactly the original serial allowance when a wave cannot fit.
        if selected.len() < 2 {
            selected = vec![(0, remaining)];
        }
        let mut wave = Vec::new();
        for (i, bound) in selected.into_iter().rev() {
            wave.push((pending.remove(i), bound));
        }
        wave.reverse();
        let mut futures = Vec::new();
        for ((ordinal, fragment), bound) in wave {
            let node = sessions
                .get_mut(&u64::from(fragment.partition()))
                .ok_or(QueryFailure::UnsupportedScope)?;
            // Prepare mutates the original lane before dispatch, so cancellation
            // cannot restore a consumed sequence or lose cleanup ownership.
            let prepared = fixed_execution::prepare(node, fragment.clone(), bound)?;
            cost.charge(prepared.cost.bytes, prepared.cost.work, CEILING)?;
            futures.push(Box::pin(async move {
                let output = fixed_execution::invoke(prepared, end)
                    .await
                    .and_then(|output| {
                        validate(&fragment, &output.value)?;
                        Ok(output)
                    });
                (ordinal, output)
            }));
        }
        let mut replies: Vec<_> = (0..futures.len()).map(|_| None).collect();
        // No spawned tasks. Cancellation drops all local sockets/futures. A
        // server worker already running retains its own permit/view/deadline.
        let completed = std::future::poll_fn(|cx| {
            let mut ready = true;
            for (future, reply) in futures.iter_mut().zip(&mut replies) {
                if reply.is_none() {
                    match future.as_mut().poll(cx) {
                        Poll::Ready((ordinal, result)) => match result {
                            Ok(output) => *reply = Some((ordinal, output)),
                            Err(error) => return Poll::Ready(Err(error)),
                        },
                        Poll::Pending => ready = false,
                    }
                }
            }
            if ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
        .await;
        drop(futures);
        completed?;
        // No wave becomes visible to operators until every leaf, descriptor,
        // sequence and budget has passed. Completion order never sets row order.
        let mut accepted = Vec::new();
        for reply in replies {
            let (ordinal, output) = reply.unwrap();
            cost.charge(output.cost.bytes, output.cost.work, CEILING)?;
            accepted.push((ordinal, output.value));
        }
        for (ordinal, value) in accepted {
            outputs[ordinal] = Some(value);
        }
    }
    check(end)?;
    Ok(outputs.into_iter().map(Option::unwrap).collect())
}

#[cfg(test)]
pub(super) fn reservation_for_test(fragment: &Fragment, node: &NodeSession) -> Option<Cost> {
    reservation(fragment, node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2_raft::{
        participant_writes::Authority,
        transaction_decisions::{Cohort, Member},
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    async fn fixture() -> (BTreeMap<u64, NodeSession>, Vec<TcpListener>) {
        let cohort = Cohort {
            coordinator_shard: 9,
            coordinator_epoch: 1,
            profile: Default::default(),
            members: (1..=4)
                .map(|shard| Member {
                    shard,
                    epoch: 1,
                    mode: [shard as u8; 16],
                })
                .collect(),
        };
        let mut nodes = BTreeMap::new();
        let mut listeners = Vec::new();
        for shard in 1..=4 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let route = PartitionRoute {
                partition: shard as u32,
                shard,
                epoch: 1,
                identity: RemoteIdentity {
                    group: shard,
                    node: 1,
                },
                endpoint: listener.local_addr().unwrap(),
            };
            let descriptor = fixed::FixedDescriptor {
                stamp: QueryStamp {
                    identity: route.identity,
                    shard,
                    epoch: 1,
                    sequence: 7,
                },
                incarnation: [1; 16],
                token: [shard as u8; 16],
                authority: Authority {
                    shard: 9,
                    mode: [9; 16],
                },
                cohort: cohort.clone(),
                version: 8,
                mode: [shard as u8; 16],
            };
            nodes.insert(shard, NodeSession::new(route, descriptor, 256, 10));
            listeners.push(listener);
        }
        (nodes, listeners)
    }
    fn fragment(shard: u32, id: u64) -> Fragment {
        Fragment::Exists(VertexId {
            partition: shard,
            id,
        })
    }
    async fn request(socket: &mut TcpStream) -> fixed::FixedFragmentRequest {
        let request: super::super::super::Request =
            read_frame(socket, Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        let Operation::FixedQueryFragment(request) = request.operation else {
            panic!("fixed leaf expected");
        };
        request
    }
    async fn reply_result(
        socket: &mut TcpStream,
        request: fixed::FixedFragmentRequest,
        fault: usize,
    ) -> Result<(), RemoteError> {
        let descriptor = request.descriptor.clone();
        let value = match &request.fragment {
            Fragment::Exists(v) => FragmentValue::Bool(v.id % 2 == 0),
            Fragment::VerifyReverse(_) => FragmentValue::Bool(true),
            Fragment::Out(_) => FragmentValue::Edges(Vec::new()),
            _ => panic!("fixture leaf"),
        };
        let mut output = fixed::FixedFragmentOutput {
            descriptor,
            request_sequence: request.request_sequence,
            cost: Cost {
                bytes: 1000,
                work: 2,
            },
            value,
        };
        match fault {
            1 => output.descriptor.version += 1,
            2 => output.request_sequence += 1,
            3 => {
                output.cost = Cost {
                    bytes: request.remaining.bytes + 1,
                    work: 2,
                }
            }
            4 => output.value = FragmentValue::Value(None),
            _ => {}
        }
        write_frame(
            socket,
            &super::super::super::Response {
                version: VERSION,
                result: Ok(Reply::FixedQueryFragment(Ok(output))),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
    }

    async fn reply(socket: &mut TcpStream, request: fixed::FixedFragmentRequest, fault: usize) {
        reply_result(socket, request, fault).await.unwrap();
    }

    #[tokio::test]
    async fn independent_lanes_overlap_with_disjoint_budgets_and_keep_input_order() {
        let (mut nodes, mut listeners) = fixture().await;
        drop(listeners.pop());
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let grants = Arc::new(Mutex::new(Cost::default()));
        let finished = Arc::new(Mutex::new(Vec::new()));
        let mut servers = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let barrier = barrier.clone();
            let grants = grants.clone();
            let finished = finished.clone();
            servers.push(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = request(&mut socket).await;
                assert_eq!(request.request_sequence, 0);
                assert_eq!(request.descriptor.version, 8);
                {
                    let mut grants = grants.lock().unwrap();
                    grants.bytes += request.remaining.bytes;
                    grants.work += request.remaining.work;
                }
                // A serial executor cannot pass; this is not a timing assertion.
                tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                    .await
                    .unwrap();
                // Force reverse completion independently of worker scheduling.
                while finished.lock().unwrap().len() != 2 - index {
                    tokio::task::yield_now().await;
                }
                reply(&mut socket, request, 0).await;
                finished.lock().unwrap().push(index);
            }));
        }
        let mut cost = Cost::default();
        let values = run(
            &mut nodes,
            vec![fragment(1, 2), fragment(2, 3), fragment(3, 4)],
            &mut cost,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        for server in servers {
            server.await.unwrap();
        }
        assert!(matches!(
            &values[..],
            [
                FragmentValue::Bool(true),
                FragmentValue::Bool(false),
                FragmentValue::Bool(true)
            ]
        ));
        assert_eq!(*finished.lock().unwrap(), vec![2, 1, 0]);
        let grants = *grants.lock().unwrap();
        assert!(grants.bytes + (cost.bytes - 3000) <= CEILING.bytes);
        assert!(grants.work + 3 <= CEILING.work);
        assert_eq!(cost.work, 9);
        assert!(nodes.values().take(3).all(|node| node.sequence == 1));
    }

    #[tokio::test]
    async fn outgoing_lanes_overlap_without_dividing_a_valid_leaf_allowance() {
        let (mut nodes, mut listeners) = fixture().await;
        listeners.truncate(3);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut servers = Vec::new();
        let mut grants = Cost::default();
        for shard in 1..=3 {
            let leaf = Fragment::Out(VertexId {
                partition: shard,
                id: 99,
            });
            let bound = reservation(&leaf, &nodes[&u64::from(shard)]).unwrap();
            assert!(fits(grants, bound, CEILING));
            grants.bytes += bound.bytes;
            grants.work += bound.work;
        }
        for listener in listeners {
            let barrier = barrier.clone();
            servers.push(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = request(&mut socket).await;
                assert!(matches!(request.fragment, Fragment::Out(_)));
                // Regression: the old unconditional 5004 work reservation
                // cannot admit even two Out leaves and cannot pass this gate.
                tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                    .await
                    .unwrap();
                reply(&mut socket, request, 0).await;
            }));
        }
        let result = run(
            &mut nodes,
            (1..=3)
                .map(|partition| Fragment::Out(VertexId { partition, id: 99 }))
                .collect(),
            &mut Cost::default(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(result
            .iter()
            .all(|v| matches!(v, FragmentValue::Edges(e) if e.is_empty())));
        for server in servers {
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn capture_dimensions_bound_work_and_keep_large_captures_serial() {
        let (mut nodes, _) = fixture().await;
        let node = nodes.get_mut(&1).unwrap();
        let leaf = Fragment::Out(VertexId {
            partition: 1,
            id: 99,
        });
        // Includes the source vertex and every possible index candidate,
        // including dangling indexes with missing canonical records.
        for (rows, expected_work) in [
            (0, 5),
            (1, 5),
            (2, 9),
            (3, 14),
            (4, 19),
            (1000, 4999),
            (1001, 5004),
            (usize::MAX, 5004),
        ] {
            node.capture_rows = rows;
            assert_eq!(reservation(&leaf, node).unwrap().work, expected_work);
        }
        let bound = reservation(&leaf, node).unwrap();
        assert!(!fits(bound, bound, CEILING));
        // A huge vertex/property payload must not be treated as an average
        // row; insufficient byte headroom retains the serial fallback too.
        node.capture_rows = 10;
        node.capture_bytes = fixed::CAPTURE_BYTES;
        let bound = reservation(&leaf, node).unwrap();
        assert!(bound.bytes > CEILING.bytes);
    }

    #[tokio::test]
    async fn same_session_is_serial_and_width_is_bounded() {
        let (mut nodes, mut listeners) = fixture().await;
        let fourth = listeners.pop().unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let first_wave_done = Arc::new(tokio::sync::Notify::new());
        let first_wave_signal = first_wave_done.clone();
        let fourth_server = tokio::spawn(async move {
            let waiting = first_wave_signal.notified();
            tokio::pin!(waiting);
            waiting.as_mut().enable();
            // No fourth lane until a full bounded wave completes.
            tokio::select! {
                _ = &mut waiting => {},
                _ = fourth.accept() => panic!("more than three in-flight lanes"),
            }
            let (mut socket, _) = fourth.accept().await.unwrap();
            let request = request(&mut socket).await;
            reply(&mut socket, request, 0).await;
        });
        let mut servers = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let barrier = barrier.clone();
            let first_wave_done = first_wave_done.clone();
            servers.push(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let first = request(&mut socket).await;
                assert_eq!(first.request_sequence, 0);
                tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                    .await
                    .unwrap();
                if index == 0 {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(20), listener.accept())
                            .await
                            .is_err()
                    );
                }
                first_wave_done.notify_one();
                reply(&mut socket, first, 0).await;
                if index == 0 {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let second = request(&mut socket).await;
                    assert_eq!(second.request_sequence, 1);
                    reply(&mut socket, second, 0).await;
                }
            }));
        }
        let values = run(
            &mut nodes,
            vec![
                fragment(1, 2),
                fragment(1, 2),
                fragment(2, 3),
                fragment(3, 4),
                fragment(4, 5),
            ],
            &mut Cost::default(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(values.len(), 5);
        assert_eq!(nodes[&1].sequence, 2);
        for server in servers {
            server.await.unwrap();
        }
        fourth_server.await.unwrap();
    }

    #[tokio::test]
    async fn malformed_or_over_budget_wave_fails_without_retry() {
        for fault in 1..=4 {
            let (mut nodes, mut listeners) = fixture().await;
            listeners.truncate(2);
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let mut servers = Vec::new();
            for (index, listener) in listeners.into_iter().enumerate() {
                let barrier = barrier.clone();
                servers.push(tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let request = request(&mut socket).await;
                    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                        .await
                        .unwrap();
                    if index == 0 {
                        reply(&mut socket, request, fault).await;
                    } else {
                        let _ = reply_result(&mut socket, request, 0).await;
                    }
                    assert!(
                        tokio::time::timeout(Duration::from_millis(20), listener.accept())
                            .await
                            .is_err()
                    );
                }));
            }
            let result = run(
                &mut nodes,
                vec![fragment(1, 2), fragment(2, 3)],
                &mut Cost::default(),
                Instant::now() + Duration::from_secs(5),
            )
            .await;
            assert!(matches!(
                result,
                Err(QueryFailure::IncompleteOrCorrupt | QueryFailure::ResourceExhausted)
            ));
            for server in servers {
                server.await.unwrap();
            }
            assert_eq!(nodes[&1].sequence, 1);
            assert_eq!(nodes[&2].sequence, 1);
        }
    }

    #[tokio::test]
    async fn cancellation_drops_every_socket_and_preserves_consumed_sequences() {
        let (mut nodes, mut listeners) = fixture().await;
        listeners.truncate(3);
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
        let mut cost = Cost::default();
        {
            let running = run(
                &mut nodes,
                (1..=3)
                    .map(|partition| Fragment::Out(VertexId { partition, id: 2 }))
                    .collect(),
                &mut cost,
                Instant::now() + Duration::from_secs(5),
            );
            tokio::pin!(running);
            tokio::select! {
                _ = &mut running => panic!("servers do not return"),
                _ = async { while started.load(Ordering::SeqCst) != 3 { tokio::task::yield_now().await; } } => {},
            }
        }
        for server in servers {
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
        }
        assert!(nodes.values().take(3).all(|node| node.sequence == 1));
        assert_eq!(cost.work, 3);
    }

    #[tokio::test]
    async fn low_remaining_budget_keeps_the_original_serial_allowance() {
        let (mut nodes, mut listeners) = fixture().await;
        listeners.truncate(2);
        let grants = Arc::new(Mutex::new(Vec::new()));
        let mut servers = Vec::new();
        for listener in listeners {
            let grants = grants.clone();
            servers.push(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = request(&mut socket).await;
                grants.lock().unwrap().push(request.remaining);
                reply(&mut socket, request, 0).await;
            }));
        }
        let mut cost = Cost {
            bytes: CEILING.bytes - 65536,
            work: CEILING.work - 100,
        };
        run(
            &mut nodes,
            vec![fragment(1, 2), fragment(2, 3)],
            &mut cost,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        for server in servers {
            server.await.unwrap();
        }
        let grants = grants.lock().unwrap();
        assert!(grants[0].bytes > 60000);
        assert_eq!(grants[0].work, 99);
        assert_eq!(grants[1].work, 96);
        assert!(grants[1].bytes < grants[0].bytes);
    }

    #[tokio::test]
    async fn overflow_is_not_a_smaller_parallel_grant() {
        let (mut nodes, _listeners) = fixture().await;
        nodes.get_mut(&1).unwrap().capture_bytes = usize::MAX;
        assert!(reservation(&fragment(1, 1), &nodes[&1]).is_none());
        assert!(!fits(
            Cost {
                bytes: usize::MAX,
                work: 0
            },
            Cost { bytes: 1, work: 0 },
            CEILING
        ));
    }
    #[tokio::test]
    async fn failed_lane_cancels_stalled_sibling_without_waiting_for_deadline() {
        let (mut nodes, mut listeners) = fixture().await;
        listeners.truncate(2);
        let healthy = listeners.pop().unwrap();
        let failing = listeners.pop().unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let other = barrier.clone();
        let stalled = tokio::spawn(async move {
            let (mut socket, _) = healthy.accept().await.unwrap();
            let _ = request(&mut socket).await;
            other.wait().await;
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        });
        let failed = tokio::spawn(async move {
            let (mut socket, _) = failing.accept().await.unwrap();
            let request = request(&mut socket).await;
            barrier.wait().await;
            reply(&mut socket, request, 2).await;
        });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            run(
                &mut nodes,
                vec![fragment(1, 2), fragment(2, 3)],
                &mut Cost::default(),
                Instant::now() + Duration::from_secs(5),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(QueryFailure::IncompleteOrCorrupt)));
        failed.await.unwrap();
        stalled.await.unwrap();
    }

    #[tokio::test]
    async fn expired_empty_stage_cannot_return_success() {
        let result = run(
            &mut BTreeMap::new(),
            Vec::new(),
            &mut Cost::default(),
            Instant::now(),
        )
        .await;
        assert!(matches!(result, Err(QueryFailure::Timeout)));
    }
}
