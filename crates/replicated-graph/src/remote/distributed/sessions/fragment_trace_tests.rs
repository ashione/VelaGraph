//! Real loopback typed Fragment RPC, actual expiry lookup and production client mapper.
//! The fixture service delegates to Sessions::fragment; it does not replace an
//! actual-removal event with a fabricated Status. It is not a whole-cohort test.
use super::super::{
    fragment_trace as trace,
    grpc::{wire, QueryClient},
};
use super::*;
use crate::remote::paged::grpc::status;
use tonic::{Request, Response, Status};
use wire::query_service_server::{QueryService, QueryServiceServer};

struct Peer {
    registries: BTreeMap<u64, Arc<Sessions>>,
    capture: trace::Capture,
    barrier: Arc<tokio::sync::Barrier>,
    enabled: bool,
}
#[tonic::async_trait]
impl QueryService for Peer {
    async fn open(
        &self,
        _: Request<wire::OpenRequest>,
    ) -> Result<Response<wire::Descriptor>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn open_hierarchy(
        &self,
        _: Request<wire::OpenRequest>,
    ) -> Result<Response<wire::HierarchyDescriptor>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn close(&self, _: Request<wire::CloseRequest>) -> Result<Response<wire::Empty>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn fragment(
        &self,
        request: Request<wire::FragmentRequest>,
    ) -> Result<Response<wire::FragmentResponse>, Status> {
        let call = trace::incoming(self.enabled, request.metadata());
        let request: FragmentRequest = request.into_inner().try_into().map_err(status)?;
        let shard = request.descriptor.stamp.shard;
        let identity = request.descriptor.stamp.identity;
        let sessions = self.registries.get(&shard).unwrap().clone();
        let check_sessions = sessions.clone();
        let check: Box<dyn Fn() + Send + Sync> = Box::new(move || {
            assert!(
                check_sessions.values.try_lock().is_ok(),
                "emit held observed-map lock"
            );
            assert!(
                check_sessions.fixed_values.try_lock().is_ok(),
                "emit held fixed-map lock"
            );
        });
        trace::CAPTURE
            .scope(
                self.capture.clone(),
                trace::CHECK_UNLOCK.scope(
                    check,
                    trace::scope(call, async {
                        // Both RPC scopes are entered before either real lookup runs.
                        self.barrier.wait().await;
                        let result = sessions
                            .fragment(
                                &StateMachine::default(),
                                identity,
                                shard,
                                request,
                                Instant::now() + Duration::from_secs(2),
                            )
                            .await;
                        match result {
                            Err(error) => Err(status(error)),
                            Ok(_) => panic!("expired fixture must reject"),
                        }
                    }),
                ),
            )
            .await
    }
}
struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub(super) fn fixture(shard: u64) -> (Arc<Sessions>, Descriptor) {
    let sessions = Arc::new(Sessions::new().unwrap());
    let descriptor = Descriptor {
        stamp: QueryStamp {
            identity: RemoteIdentity {
                group: shard,
                node: 1,
            },
            shard,
            epoch: 1,
            sequence: 1,
        },
        incarnation: sessions.incarnation,
        token: [shard as u8; 16],
    };
    let session = Session {
        descriptor: descriptor.clone(),
        hierarchy: false,
        node: query::FrozenNode {
            view: ReadView::from_entries(shard, 1, 1, BTreeMap::new()).unwrap(),
            end: std::time::Instant::now() - Duration::from_millis(1),
        },
        used: Mutex::new(Cost::default()),
        scan_used: Mutex::new(Cost::default()),
        _slot: SESSION_SLOTS.try_acquire().unwrap(),
        _bytes: SESSION_BYTES.try_acquire_many(0).unwrap(),
    };
    sessions
        .values
        .lock()
        .unwrap()
        .insert(descriptor.token, Arc::new(session));
    (sessions, descriptor)
}
pub(super) fn request(descriptor: Descriptor) -> FragmentRequest {
    FragmentRequest {
        descriptor,
        fragment: Fragment::Exists(VertexId {
            partition: 0,
            id: 1,
        }),
        remaining: Cost {
            bytes: 4096,
            work: 100,
        },
    }
}
#[tokio::test]
async fn loopback_actual_expiry_and_status_share_per_rpc_id_without_cross_talk() {
    let _fixtures = TEST_SESSION_FIXTURES.lock().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        let (sa, a) = fixture(71);
        let (sb, b) = fixture(72);
        let server_events = trace::Capture::default();
        let client_events = trace::Capture::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = Peer {
            registries: BTreeMap::from([(71, sa), (72, sb)]),
            capture: server_events.clone(),
            barrier: Arc::new(tokio::sync::Barrier::new(2)),
            enabled: true,
        };
        let task = AbortOnDrop(tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(QueryServiceServer::new(peer))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        }));
        let ca =
            QueryClient::connect_lazy(address, a.stamp.identity, Duration::from_secs(2)).unwrap();
        let cb =
            QueryClient::connect_lazy(address, b.stamp.identity, Duration::from_secs(2)).unwrap();
        let ids = [trace::CallId([3; 16]), trace::CallId([4; 16])];
        let (ra, rb) = trace::CAPTURE
            .scope(client_events.clone(), async {
                tokio::join!(
                    ca.fragment_with_call(request(a.clone()), Duration::from_secs(2), ids[0]),
                    cb.fragment_with_call(request(b.clone()), Duration::from_secs(2), ids[1])
                )
            })
            .await;
        assert!(matches!(ra, Err(RemoteError::SessionUnavailable)));
        assert!(matches!(rb, Err(RemoteError::SessionUnavailable)));
        let se = server_events.lock().unwrap().clone();
        let ce = client_events.lock().unwrap().clone();
        assert_eq!(se.len(), 2);
        assert_eq!(ce.len(), 2);
        for id in ids {
            assert_eq!(
                se.iter()
                    .filter(|r| r.call == id && matches!(r.event, trace::Event::Removal { .. }))
                    .count(),
                1
            );
            assert_eq!(
                ce.iter()
                    .filter(|r| r.call == id
                        && r.event
                            == trace::Event::Status {
                                code: tonic::Code::NotFound,
                                reason: "session_unavailable"
                            })
                    .count(),
                1
            );
        }
        // Same descriptors already swept: real errors remain but no false removal.
        let later_ids = [trace::CallId([5; 16]), trace::CallId([6; 16])];
        let (ra, rb) = trace::CAPTURE
            .scope(client_events.clone(), async {
                tokio::join!(
                    ca.fragment_with_call(request(a), Duration::from_secs(2), later_ids[0]),
                    cb.fragment_with_call(request(b), Duration::from_secs(2), later_ids[1])
                )
            })
            .await;
        assert!(matches!(ra, Err(RemoteError::SessionUnavailable)));
        assert!(matches!(rb, Err(RemoteError::SessionUnavailable)));
        assert_eq!(server_events.lock().unwrap().len(), 2);
        let ce = client_events.lock().unwrap();
        assert_eq!(ce.len(), 4);
        for id in later_ids {
            assert_eq!(
                ce.iter()
                    .filter(|r| r.call == id
                        && r.event
                            == trace::Event::Status {
                                code: tonic::Code::NotFound,
                                reason: "session_unavailable"
                            })
                    .count(),
                1
            );
        }
        drop(ce);
        task.0.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn loopback_disabled_or_invalid_metadata_preserves_actual_error() {
    let _fixtures = TEST_SESSION_FIXTURES.lock().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        // Each case owns a fresh expired registry; no prior sweep can hide evidence.
        for (server_enabled, mode, expected_events) in [
            (true, 0, 0),
            (false, 0, 0),
            (false, 1, 0),
            (true, 1, 1),
            (true, 2, 0),
            (true, 3, 0),
            (true, 4, 0),
        ] {
            let (sessions, descriptor) = fixture(73);
            let sink = trace::Capture::default();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let peer = Peer {
                registries: BTreeMap::from([(73, sessions)]),
                capture: sink.clone(),
                barrier: Arc::new(tokio::sync::Barrier::new(1)),
                enabled: server_enabled,
            };
            let task = AbortOnDrop(tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(QueryServiceServer::new(peer))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            }));
            let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                .unwrap()
                .connect_lazy();
            let mut client = wire::query_service_client::QueryServiceClient::new(channel);
            let mut payload: wire::FragmentRequest = request(descriptor.clone()).into();
            payload.context = Some(wire::Context {
                version: 1,
                identity: Some(wire::Identity { group: 73, node: 1 }),
                timeout_millis: 2000,
            });
            let mut request = Request::new(payload);
            request.set_timeout(Duration::from_secs(2));
            let id = trace::CallId([8; 16]);
            if mode == 1 || mode == 3 {
                id.insert(request.metadata_mut());
            }
            if mode == 2 {
                request
                    .metadata_mut()
                    .insert("x-velagraph-diag-call", "INVALID".parse().unwrap());
            }
            if mode == 3 {
                request.metadata_mut().append(
                    "x-velagraph-diag-call",
                    "08080808080808080808080808080808".parse().unwrap(),
                );
            }
            if mode == 4 {
                request.metadata_mut().insert(
                    "x-velagraph-diag-call",
                    "080808080808080808080808080808080".parse().unwrap(),
                );
            }
            let status = client.fragment(request).await.unwrap_err();
            assert_eq!(status.code(), tonic::Code::NotFound);
            assert!(matches!(
                crate::remote::paged::grpc::remote_status(status),
                RemoteError::SessionUnavailable
            ));
            let events = sink.lock().unwrap();
            assert_eq!(events.len(), expected_events);
            if expected_events == 1 {
                assert_eq!(events[0].call, id);
                assert!(matches!(events[0].event, trace::Event::Removal { .. }));
            }
            drop(events);
            task.0.abort();
        }
    })
    .await
    .unwrap();
}
