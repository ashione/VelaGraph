//! Opt-in child process: real Handler and OS stderr, not CAPTURE or mock Peer.
//! Test-created expired entries bypass normal OpenQuery; standalone Handler omits
//! host admission/background cleanup. This is not a natural failure reproduction.
use super::super::grpc::{self, wire, QueryClient};
use super::fragment_trace_tests::{fixture, request};
use super::*;
use crate::remote::{paged::grpc::Handler, GraphState};

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn server(node: &d2_raft::TcpNode, shard: u64) -> (Server, std::net::SocketAddr, Descriptor) {
    let (sessions, descriptor) = fixture(shard);
    let mut state = GraphState::from_tcp(node, shard, shard, Duration::from_secs(3)).unwrap();
    state.query_sessions = sessions;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = grpc::service(Handler::diagnostic_fixture(&state));
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (Server(task), address, descriptor)
}
fn check(result: Result<Result<FragmentOutput, QueryFailure>, RemoteError>) {
    assert!(matches!(result, Err(RemoteError::SessionUnavailable)));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess-only real Handler diagnostic control"]
async fn handler_sink_child() {
    let mode = std::env::var("VELAGRAPH_HANDLER_CONTROL").expect("explicit child mode");
    assert!(matches!(mode.as_str(), "on" | "off" | "metadata"));
    let directory = tempfile::tempdir().unwrap();
    let node = d2_raft::TcpNode::start(d2_raft::TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: directory.path().into(),
    })
    .await
    .unwrap();
    node.bootstrap(BTreeMap::from([(1, node.raft_addr.clone())]))
        .await
        .unwrap();
    // Fragment lookup does not require a new read barrier or graph activation.
    if mode != "metadata" {
        let (_a, aa, da) = server(&node, 71).await;
        let (_b, ab, db) = server(&node, 72).await;
        let ca = QueryClient::connect_lazy(aa, da.stamp.identity, Duration::from_secs(3)).unwrap();
        let cb = QueryClient::connect_lazy(ab, db.stamp.identity, Duration::from_secs(3)).unwrap();
        let (a, b) = tokio::join!(
            ca.fragment_observed(request(da.clone()), Duration::from_secs(3)),
            cb.fragment_observed(request(db), Duration::from_secs(3))
        );
        check(a);
        check(b);
        check(
            ca.fragment_observed(request(da), Duration::from_secs(3))
                .await,
        );
        println!("HANDLER_CONTROL mode={mode} business=3_session_unavailable");
    } else {
        for (index, kind) in [
            "valid_before",
            "missing",
            "uppercase",
            "duplicate",
            "valid_after",
        ]
        .iter()
        .enumerate()
        {
            let (_server, address, descriptor) = server(&node, 80 + index as u64).await;
            let mut client = wire::query_service_client::QueryServiceClient::connect(format!(
                "http://{address}"
            ))
            .await
            .unwrap();
            let identity = descriptor.stamp.identity;
            let mut body: wire::FragmentRequest = request(descriptor).into();
            body.context = Some(wire::Context {
                version: 1,
                identity: Some(wire::Identity {
                    group: identity.group,
                    node: identity.node,
                }),
                timeout_millis: 3000,
            });
            let mut rpc = tonic::Request::new(body);
            let key = "x-velagraph-diag-call";
            match *kind {
                "valid_before" => {
                    rpc.metadata_mut()
                        .insert(key, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse().unwrap());
                }
                "valid_after" => {
                    rpc.metadata_mut()
                        .insert(key, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".parse().unwrap());
                }
                "uppercase" => {
                    rpc.metadata_mut()
                        .insert(key, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC".parse().unwrap());
                }
                "duplicate" => {
                    rpc.metadata_mut()
                        .append(key, "dddddddddddddddddddddddddddddddd".parse().unwrap());
                    rpc.metadata_mut()
                        .append(key, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".parse().unwrap());
                }
                _ => {}
            }
            let error = client.fragment(rpc).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::NotFound);
            assert!(matches!(
                crate::remote::paged::grpc::remote_status(error),
                RemoteError::SessionUnavailable
            ));
            println!("HANDLER_CONTROL metadata={kind} business=session_unavailable");
        }
    }
    node.shutdown().await;
}
