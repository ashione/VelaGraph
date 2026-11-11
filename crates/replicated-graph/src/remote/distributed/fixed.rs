//! Node-local fixed-version protocol. These capabilities are not authentication:
//! callers and endpoint configuration must belong to one trusted deployment.
use super::*;
use d2_raft::{
    participant_writes::Authority,
    terminal_prefix::TerminalPrefix,
    transaction_decisions::Cohort,
    versioned_reads::{ReadLimit, Target},
};

pub(crate) const CAPTURE_ROWS: usize = 10_000;
pub(crate) const CAPTURE_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const CAPTURE_OPERATIONS: usize = 100_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixedOpenRequest {
    pub(crate) target: Target,
    pub(crate) proof: TerminalPrefix,
    pub(crate) capture_limit: ReadLimit,
    pub(crate) ttl_millis: u64,
    pub(crate) acquire_timeout_millis: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixedDescriptor {
    pub(crate) stamp: QueryStamp,
    pub(crate) incarnation: [u8; 16],
    pub(crate) token: [u8; 16],
    pub(crate) authority: Authority,
    pub(crate) cohort: Cohort,
    pub(crate) version: u64,
    pub(crate) mode: [u8; 16],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaptureCost {
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) replay_operations: usize,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixedOpened {
    pub(crate) descriptor: FixedDescriptor,
    pub(crate) retained_proof: TerminalPrefix,
    pub(crate) capture_cost: CaptureCost,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixedFragmentRequest {
    pub(super) descriptor: FixedDescriptor,
    pub(super) request_sequence: u64,
    pub(super) fragment: Fragment,
    pub(super) remaining: Cost,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixedFragmentOutput {
    pub(super) descriptor: FixedDescriptor,
    pub(super) request_sequence: u64,
    pub(super) cost: Cost,
    pub(super) value: FragmentValue,
}

impl FixedOpenRequest {
    pub(crate) fn validate(&self, shard: u64) -> Result<(), RemoteError> {
        let target = &self.target;
        if target.shard != shard
            || u32::try_from(shard).is_err()
            || target.epoch == 0
            || target.mode == [0; 16]
            || self.proof.cohort().members.len() > 8
            || !self.proof.cohort().members.iter().any(|member| {
                member.shard == shard && member.epoch == target.epoch && member.mode == target.mode
            })
            || !(1..=60_000).contains(&self.ttl_millis)
            || !(1..=5000).contains(&self.acquire_timeout_millis)
            || !(1..=CAPTURE_ROWS).contains(&self.capture_limit.rows)
            || !(1..=CAPTURE_BYTES).contains(&self.capture_limit.bytes)
            || !(1..=CAPTURE_OPERATIONS).contains(&self.capture_limit.operations)
        {
            return Err(RemoteError::InvalidArgument);
        }
        Ok(())
    }
    pub(super) fn worst_descriptor(&self, identity: RemoteIdentity) -> FixedDescriptor {
        FixedDescriptor {
            stamp: QueryStamp {
                identity,
                shard: self.target.shard,
                epoch: self.target.epoch,
                sequence: u64::MAX,
            },
            incarnation: [255; 16],
            token: [255; 16],
            authority: Authority {
                shard: self.proof.cohort().coordinator_shard,
                mode: self.proof.authority(),
            },
            cohort: self.proof.cohort().clone(),
            version: self.proof.version(),
            mode: self.target.mode,
        }
    }
}

// Count the exact whole JSON envelope without retaining a second encoded frame.
// TerminalPrefix's existing serde implementation still makes a bounded proof
// clone; the graph frame cap is not an RSS or certification-memory bound.
struct FrameCounter {
    bytes: usize,
    end: Instant,
}
impl io::Write for FrameCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if Instant::now() >= self.end {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline"));
        }
        if bytes.len() > MAX_FRAME_BYTES.saturating_sub(self.bytes) {
            return Err(io::Error::other("fixed query frame limit"));
        }
        self.bytes += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn frame_size(value: &impl Serialize, end: Instant) -> Result<usize, RemoteError> {
    let mut counter = FrameCounter { bytes: 0, end };
    blocking_json(|| serde_json::to_writer(&mut counter, value)).map_err(|_| {
        if Instant::now() >= end {
            RemoteError::Timeout { request_id: None }
        } else {
            RemoteError::ResourceExhausted
        }
    })?;
    Ok(counter.bytes)
}

/// Both envelopes must fit before any member is opened. Run for every pinned
/// member first. The server repeats this check before reserving its session.
pub(crate) fn preflight(
    identity: RemoteIdentity,
    request: &FixedOpenRequest,
    end: Instant,
) -> Result<(), RemoteError> {
    request.validate(request.target.shard)?;
    frame_size(
        &Request {
            version: VERSION,
            identity: Some(identity),
            operation: Operation::OpenFixedQuery(request.clone()),
        },
        end,
    )?;
    frame_size(
        &Response {
            version: VERSION,
            result: Ok(Reply::OpenFixedQuery(FixedOpened {
                descriptor: request.worst_descriptor(identity),
                retained_proof: request.proof.clone(),
                capture_cost: CaptureCost {
                    rows: usize::MAX,
                    bytes: usize::MAX,
                    replay_operations: usize::MAX,
                },
            })),
        },
        end,
    )?;
    Ok(())
}

// Local preflight can be expensive for a full proof. It must not become an
// unaccounted extension of the relative node lease sent afterwards. Network
// delay remains a cross-clock uncertainty; the gateway's absolute TTL is final.
fn narrow_open_deadlines(
    request: &mut FixedOpenRequest,
    elapsed: Duration,
    remaining: Duration,
) -> Result<(), RemoteError> {
    let ttl = Duration::from_millis(request.ttl_millis).saturating_sub(elapsed);
    let acquire = Duration::from_millis(request.acquire_timeout_millis)
        .saturating_sub(elapsed)
        .min(remaining)
        .min(ttl);
    request.ttl_millis = ttl.as_millis() as u64;
    request.acquire_timeout_millis = acquire.as_millis() as u64;
    if request.ttl_millis == 0 || request.acquire_timeout_millis == 0 {
        return Err(RemoteError::Timeout { request_id: None });
    }
    Ok(())
}

pub(crate) async fn open(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    mut request: FixedOpenRequest,
    duration: Duration,
) -> Result<FixedOpened, RemoteError> {
    let started = Instant::now();
    let end = checked_deadline(duration, None)?;
    preflight(identity, &request, end)?;
    let now = Instant::now();
    narrow_open_deadlines(
        &mut request,
        now - started,
        end.saturating_duration_since(now),
    )?;
    let reply = rpc(
        endpoint,
        Some(identity),
        Operation::OpenFixedQuery(request),
        end.saturating_duration_since(Instant::now()),
        None,
    )
    .await?;
    match reply {
        Reply::OpenFixedQuery(opened) => Ok(opened),
        _ => Err(RemoteError::Protocol),
    }
}
pub(super) async fn fragment(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    request: FixedFragmentRequest,
    duration: Duration,
) -> Result<Result<FixedFragmentOutput, QueryFailure>, RemoteError> {
    match rpc(
        endpoint,
        Some(identity),
        Operation::FixedQueryFragment(request),
        duration,
        None,
    )
    .await?
    {
        Reply::FixedQueryFragment(output) => Ok(output),
        _ => Err(RemoteError::Protocol),
    }
}
pub(crate) async fn close(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    descriptor: FixedDescriptor,
    duration: Duration,
) -> Result<(), RemoteError> {
    match rpc(
        endpoint,
        Some(identity),
        Operation::CloseFixedQuery { descriptor },
        duration,
        None,
    )
    .await?
    {
        Reply::CloseFixedQuery => Ok(()),
        _ => Err(RemoteError::Protocol),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> FixedOpenRequest {
        use d2_raft::transaction_decisions::{Command, Journal, Member};
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: Cohort {
                    coordinator_shard: 9,
                    coordinator_epoch: 1,
                    members: vec![Member {
                        shard: 7,
                        epoch: 1,
                        mode: [7; 16],
                    }],
                    profile: Default::default(),
                },
            })
            .unwrap();
        FixedOpenRequest {
            target: Target {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            },
            proof: journal.terminal_prefix(0).unwrap(),
            capture_limit: ReadLimit {
                rows: 100,
                bytes: 4096,
                operations: 100,
            },
            ttl_millis: 5000,
            acquire_timeout_millis: 5000,
        }
    }
    #[test]
    fn local_preflight_spends_original_open_lease_without_rounding_up() {
        let mut request = request();
        request.ttl_millis = 1000;
        request.acquire_timeout_millis = 800;
        narrow_open_deadlines(
            &mut request,
            Duration::from_micros(100_500),
            Duration::from_millis(650),
        )
        .unwrap();
        assert_eq!(request.ttl_millis, 899);
        assert_eq!(request.acquire_timeout_millis, 650);
        assert!(matches!(
            narrow_open_deadlines(
                &mut request,
                Duration::from_millis(900),
                Duration::from_secs(5)
            ),
            Err(RemoteError::Timeout { request_id: None })
        ));
    }

    #[test]
    fn fixed_envelopes_count_all_fields_and_reject_malformed_proof() {
        let identity = RemoteIdentity {
            group: u64::MAX,
            node: u64::MAX,
        };
        let request = request();
        let end = Instant::now() + Duration::from_secs(5);
        preflight(identity, &request, end).unwrap();
        let full = Request {
            version: VERSION,
            identity: Some(identity),
            operation: Operation::OpenFixedQuery(request.clone()),
        };
        assert_eq!(
            frame_size(&full, end).unwrap(),
            serde_json::to_vec(&full).unwrap().len()
        );
        assert!(
            frame_size(&full, end).unwrap() > serde_json::to_vec(&request.proof).unwrap().len()
        );
        let reply = Response {
            version: VERSION,
            result: Ok(Reply::OpenFixedQuery(FixedOpened {
                descriptor: request.worst_descriptor(identity),
                retained_proof: request.proof.clone(),
                capture_cost: CaptureCost {
                    rows: usize::MAX,
                    bytes: usize::MAX,
                    replay_operations: usize::MAX,
                },
            })),
        };
        assert_eq!(
            frame_size(&reply, end).unwrap(),
            serde_json::to_vec(&reply).unwrap().len()
        );
        let mut malformed = serde_json::to_value(request.proof).unwrap();
        malformed["through"] = 1.into();
        assert!(serde_json::from_value::<TerminalPrefix>(malformed).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_fixed_rpc_uses_managed_node_and_exact_endpoint_identity() {
        let _fixture = sessions::TEST_SESSION_FIXTURES.lock().await;
        use d2_raft::{
            transaction_decisions::{Command, Journal},
            Cluster,
        };
        let cluster = Cluster::new();
        let node = cluster.in_group(7).start(1).await.unwrap();
        node.raft
            .initialize(BTreeMap::from([(
                1,
                openraft::BasicNode {
                    addr: "inproc://7/1".into(),
                },
            )]))
            .await
            .unwrap();
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |m| m.current_leader == Some(1) && m.last_applied.is_some(),
                "leader",
            )
            .await
            .unwrap();
        let request = request();
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: request.proof.cohort().clone(),
            })
            .unwrap();
        for command in [
            D1Command::Activate { shard: 7, epoch: 1 },
            D1Command::EnableManagedWrites {
                shard: 7,
                epoch: 1,
                expected_sequence: 0,
                mode_id: [7; 16],
            },
            D1Command::EnableParticipant {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                authority: Authority {
                    shard: 9,
                    mode: [9; 16],
                },
            },
            D1Command::EnableVersionedReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal,
            },
        ] {
            node.raft
                .client_write(LogCommand::D1V1(command))
                .await
                .unwrap()
                .data
                .d1
                .unwrap()
                .unwrap();
        }
        let service = GraphService::start_managed(
            &node,
            "127.0.0.1:0".parse().unwrap(),
            7,
            7,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let identity = RemoteIdentity { group: 7, node: 1 };
        let endpoint = service.local_addr();
        assert!(matches!(
            open(
                endpoint,
                RemoteIdentity { group: 8, node: 1 },
                request.clone(),
                Duration::from_secs(5)
            )
            .await,
            Err(RemoteError::InvalidArgument)
        ));
        let opened = open(endpoint, identity, request.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(opened.retained_proof, request.proof);
        let output = fragment(
            endpoint,
            identity,
            FixedFragmentRequest {
                descriptor: opened.descriptor.clone(),
                request_sequence: 0,
                fragment: Fragment::Exists(VertexId {
                    partition: 7,
                    id: 1,
                }),
                remaining: CEILING,
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(output.descriptor, opened.descriptor);
        assert!(matches!(output.value, FragmentValue::Bool(false)));
        close(
            endpoint,
            identity,
            opened.descriptor,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        service.shutdown().await;
        node.raft.shutdown().await.unwrap();
    }

    fn synthetic_proof(count: u64) -> TerminalPrefix {
        use serde_json::json;
        use std::io::Write as _;
        assert_eq!(count % 32, 0);
        let mut encoded = Vec::new();
        write!(
            &mut encoded,
            "{{\"version\":2,\"authority\":{},\"cohort\":{},\"through\":{count},\"segments\":[",
            serde_json::to_string(&[9; 16]).unwrap(),
            json!({"coordinator_shard":9, "coordinator_epoch":1,
                "members":[{"shard":7, "epoch":1, "mode":([7;16])}], "profile":"ByteBound16MiBV1"})
        )
        .unwrap();
        for segment in 0..count / 32 {
            if segment > 0 {
                encoded.push(b',');
            }
            encoded.push(b'[');
            for offset in 1..=32 {
                if offset > 1 {
                    encoded.push(b',');
                }
                let ordinal = segment * 32 + offset;
                let mut id = [255; 16];
                id[..8].copy_from_slice(&ordinal.to_be_bytes());
                serde_json::to_writer(
                    &mut encoded,
                    &json!({"id": id, "ordinal": ordinal,
                    "participants": [{"shard":7, "epoch":1, "mode":([7;16]), "intent":([255;32])}],
                    "prepared": [false], "decision": "Abort"}),
                )
                .unwrap();
            }
            encoded.push(b']');
        }
        encoded.extend_from_slice(b"]}");
        serde_json::from_slice(&encoded).unwrap()
    }

    #[tokio::test]
    async fn oversized_valid_proof_fails_before_connecting_to_any_node() {
        let mut request = request();
        request.proof = synthetic_proof(53_248);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let identity = RemoteIdentity { group: 7, node: 1 };
        assert!(matches!(
            open(
                listener.local_addr().unwrap(),
                identity,
                request,
                Duration::from_secs(5)
            )
            .await,
            Err(RemoteError::ResourceExhausted)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fixed_open_receives_proof_above_ordinary_query_reply_limit() {
        let count = 8192;
        let mut request = request();
        request.proof = synthetic_proof(count);
        let identity = RemoteIdentity { group: 7, node: 1 };
        let response = Response {
            version: VERSION,
            result: Ok(Reply::OpenFixedQuery(FixedOpened {
                descriptor: request.worst_descriptor(identity),
                retained_proof: request.proof.clone(),
                capture_cost: CaptureCost {
                    rows: 0,
                    bytes: 0,
                    replay_operations: 0,
                },
            })),
        };
        let end = Instant::now() + Duration::from_secs(5);
        let bytes = frame_size(&response, end).unwrap();
        assert!(bytes > query::MAX_QUERY_RESPONSE_BYTES + 1024 && bytes < MAX_FRAME_BYTES);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let received: Request = read_frame(&mut stream, end).await.unwrap();
            assert!(matches!(received.operation, Operation::OpenFixedQuery(_)));
            write_frame(&mut stream, &response, end).await.unwrap();
        });
        // No certification assertion: this only proves that the new operation
        // has its own 16MiB receive branch; gateway checks the actual proof.
        let opened = open(endpoint, identity, request, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(opened.retained_proof.version(), count);
        peer.await.unwrap();
    }

    #[test]
    fn entire_frame_exact_limit_and_limit_plus_one() {
        let end = Instant::now() + Duration::from_secs(5);
        // A JSON string adds two quote bytes; the entire envelope counts.
        assert_eq!(
            frame_size(&"x".repeat(MAX_FRAME_BYTES - 2), end).unwrap(),
            MAX_FRAME_BYTES
        );
        assert!(matches!(
            frame_size(&"x".repeat(MAX_FRAME_BYTES - 1), end),
            Err(RemoteError::ResourceExhausted)
        ));
    }
}
