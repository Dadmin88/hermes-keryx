use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use keryx_core::PeerId;
use keryx_daemon::{GrpcRelayTaskPublisher, RelayTaskPublisher};
use keryx_proto::v1::{ClaimTaskRequest, ListPeersRequest, SendTaskRequest, TaskEnvelope, TaskId};
use keryx_relay::health_server::serve_grpc_health;
use keryx_relay::RelayRuntime;
use tokio::net::TcpListener;
use tonic::Code;

mod common;

use common::{MockRelayPublisher, RpcTestHarness};

fn envelope(task_id: &str) -> TaskEnvelope {
    TaskEnvelope {
        task_id: Some(TaskId {
            value: task_id.to_string(),
        }),
        correlation_id: None,
        idempotency_key: None,
        status: 0,
        messages: vec![],
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn send_task_routes_to_local_store() {
    let mut harness = RpcTestHarness::start().await;
    let local_peer = harness.runtime.config().local_peer_id().to_string();

    let response = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: local_peer.clone(),
            envelope: Some(envelope("route-local-1")),
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.delivery_route, "local");
    assert_eq!(response.routed_to, local_peer);
    assert_eq!(response.status, "pending");
    assert_eq!(
        response.task_id.as_ref().map(|id| id.value.as_str()),
        Some("route-local-1")
    );

    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "route-local-1".to_string(),
            }),
            worker_id: Some(keryx_proto::v1::AgentId {
                value: "worker-route".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(claim.status, "running");
}

#[tokio::test]
async fn list_peers_includes_local_peer() {
    let mut harness = RpcTestHarness::start().await;
    let response = harness
        .client
        .list_peers(ListPeersRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.peers.len(), 1);
    assert!(response.peers[0].local);
    assert!(response.peers[0].connected);
    assert_eq!(
        response.peers[0].peer_id,
        harness.runtime.config().local_peer_id().to_string()
    );
}

#[tokio::test]
async fn send_task_routes_to_relay_peer() {
    let mut harness = RpcTestHarness::start().await;
    let mock = Arc::new(MockRelayPublisher::new());
    harness.runtime.router().set_publisher(mock.clone()).await;
    let remote = PeerId::new("node-remote-a").unwrap();
    harness
        .runtime
        .router()
        .set_peer_connected(&remote, true)
        .await;

    let response = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: remote.to_string(),
            envelope: Some(envelope("route-relay-1")),
            timeout_ms: 5_000,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.delivery_route, "relay");
    assert_eq!(response.status, "delivered");
    assert_eq!(response.routed_to, remote.to_string());
    let deliveries = mock.deliveries().await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, remote.to_string());
    assert_eq!(deliveries[0].1, "route-relay-1");
}

#[tokio::test]
async fn send_task_routes_to_relay_routable_peer_without_connected_peerstore_entry() {
    let mut harness = RpcTestHarness::start().await;
    let mock = Arc::new(MockRelayPublisher::new());
    harness.runtime.router().set_publisher(mock.clone()).await;
    let remote = PeerId::new("node-registry-discovered").unwrap();
    harness
        .runtime
        .router()
        .set_peer_routable(&remote, true)
        .await;

    let response = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: remote.to_string(),
            envelope: Some(envelope("route-registry-1")),
            timeout_ms: 5_000,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.delivery_route, "relay");
    assert_eq!(response.status, "delivered");
    assert_eq!(response.routed_to, remote.to_string());
    let deliveries = mock.deliveries().await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, remote.to_string());
    assert_eq!(deliveries[0].1, "route-registry-1");
}

#[tokio::test]
async fn send_task_known_routable_peer_without_publisher_is_unavailable() {
    let mut harness = RpcTestHarness::start().await;
    let remote = PeerId::new("node-routable-no-publisher").unwrap();
    harness
        .runtime
        .router()
        .set_peer_routable(&remote, true)
        .await;

    let error = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: remote.to_string(),
            envelope: Some(envelope("route-no-publisher")),
            timeout_ms: 0,
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("relay"));
}

#[tokio::test]
async fn send_task_unknown_peer_is_not_found() {
    let mut harness = RpcTestHarness::start().await;
    let error = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: "node-missing".to_string(),
            envelope: Some(envelope("route-missing")),
            timeout_ms: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
}

#[tokio::test]
async fn send_task_relay_timeout_maps_to_deadline_exceeded() {
    let mut harness = RpcTestHarness::start().await;
    let mock = Arc::new(MockRelayPublisher::new().with_delay(Duration::from_millis(200)));
    harness.runtime.router().set_publisher(mock).await;
    let remote = PeerId::new("node-slow").unwrap();
    harness
        .runtime
        .router()
        .set_peer_connected(&remote, true)
        .await;

    let error = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: remote.to_string(),
            envelope: Some(envelope("route-timeout")),
            timeout_ms: 50,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
}

#[tokio::test]
async fn send_task_with_configured_relay_publisher_routes_unconnected_peer() {
    let mut harness = RpcTestHarness::start().await;
    let mock = Arc::new(MockRelayPublisher::new());
    harness.runtime.router().set_publisher(mock.clone()).await;
    let remote = PeerId::new("node-relay-mailbox").unwrap();

    let response = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: remote.to_string(),
            envelope: Some(envelope("route-relay-mailbox")),
            timeout_ms: 5_000,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.delivery_route, "relay");
    assert_eq!(response.status, "delivered");
    assert_eq!(mock.call_count(), 1);
    let peers = harness
        .client
        .list_peers(ListPeersRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(peers.peers.len(), 1);
    assert!(peers.peers[0].local);
}

#[tokio::test]
async fn send_task_relay_failure_maps_to_unavailable() {
    let mut harness = RpcTestHarness::start().await;
    let mock = Arc::new(MockRelayPublisher::new().failing());
    harness.runtime.router().set_publisher(mock.clone()).await;
    let remote = PeerId::new("node-relay-failure").unwrap();
    harness
        .runtime
        .router()
        .set_peer_routable(&remote, true)
        .await;

    let error = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: remote.to_string(),
            envelope: Some(envelope("route-relay-failure")),
            timeout_ms: 5_000,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    assert_eq!(mock.call_count(), 1);
}

#[tokio::test]
async fn grpc_relay_task_publisher_publishes_to_relay_mailbox() {
    let runtime = RelayRuntime::new("relay-publisher-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let publisher = GrpcRelayTaskPublisher::new(format!("http://{addr}"));
    let remote = PeerId::new("node-grpc-mailbox").unwrap();

    publisher
        .deliver_task(
            &remote,
            envelope("route-grpc-mailbox"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

    assert_eq!(runtime.mailbox_depth(remote.as_str()), 1);
}

#[tokio::test]
async fn grpc_relay_task_publisher_maps_publish_failure() {
    let runtime = RelayRuntime::new("relay-publisher-failure-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let publisher = GrpcRelayTaskPublisher::new(format!("http://{addr}"));
    let remote = PeerId::new("node-grpc-failure").unwrap();
    let mut bad_envelope = envelope("route-grpc-failure");
    bad_envelope.task_id = None;

    let error = publisher
        .deliver_task(&remote, bad_envelope, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        keryx_daemon::RoutingError::RelayFailed { .. }
    ));
    assert_eq!(runtime.mailbox_depth(remote.as_str()), 0);
}

async fn spawn_relay(runtime: Arc<RelayRuntime>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(async move {
        let _ = serve_grpc_health(runtime, None, addr).await;
    });
    for _ in 0..40 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("failed to connect gRPC relay at http://{addr}");
}
