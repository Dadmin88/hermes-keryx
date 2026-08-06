use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use keryx_core::{NodeId, PeerId};
use keryx_daemon::{GrpcRelayTaskPublisher, KeryxDaemonConfig, RelayTaskPublisher};
use keryx_proto::v1::keryx_relay_client::KeryxRelayClient;
use keryx_proto::v1::{
    AgentId, ClaimNextTaskRequest, ClaimTaskRequest, ListPeersRequest, NodeFrame, SendTaskRequest,
    SubmitRemoteTaskRequest, TaskEnvelope, TaskId,
};
use keryx_relay::health_server::{serve_grpc_health_with_auth, NODE_ID_METADATA_KEY};
use keryx_relay::registry::SkillRegistry;
use keryx_relay::security::NodeTokenAuth;
use keryx_relay::RelayRuntime;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::Request;

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
        deadline_ms: 0,
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
    assert_eq!(response.status, "relay_accepted");
    assert_eq!(response.routed_to, remote.to_string());
    assert_eq!(response.relay_frame_id, "relay-test-route-relay-1");
    assert_eq!(response.authenticated_source_peer_id, "peer-local");
    assert_eq!(response.accepted_destination_peer_id, remote.to_string());
    assert_eq!(response.accepted_route, "relay");
    assert_eq!(response.accepted_at_ms, 1);
    let claim_error = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "route-relay-1".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "worker-local-thief".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap_err();
    assert_eq!(claim_error.code(), Code::FailedPrecondition);
    let deliveries = mock.deliveries().await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, remote.to_string());
    assert_eq!(deliveries[0].1, "route-relay-1");
}

#[tokio::test]
async fn accepted_remote_task_retry_returns_durable_receipt_without_republishing() {
    let mut harness = RpcTestHarness::start().await;
    let mock = Arc::new(MockRelayPublisher::new());
    harness.runtime.router().set_publisher(mock.clone()).await;
    let remote = PeerId::new("node-remote-retry").unwrap();
    harness
        .runtime
        .router()
        .set_peer_connected(&remote, true)
        .await;
    let request = SendTaskRequest {
        target_peer_id: remote.to_string(),
        envelope: Some(envelope("route-relay-retry")),
        timeout_ms: 5_000,
    };

    let first = harness
        .client
        .send_task(request.clone())
        .await
        .unwrap()
        .into_inner();
    let second = harness
        .client
        .send_task(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.relay_frame_id, first.relay_frame_id);
    assert_eq!(second.accepted_at_ms, first.accepted_at_ms);
    assert_eq!(mock.call_count(), 1);
}

#[tokio::test]
async fn remote_target_is_not_claimable_while_publish_is_in_flight() {
    let mut harness = RpcTestHarness::start().await;
    let mock =
        Arc::new(MockRelayPublisher::new().with_delay(std::time::Duration::from_millis(100)));
    harness.runtime.router().set_publisher(mock.clone()).await;
    let remote = PeerId::new("node-remote-in-flight").unwrap();
    harness
        .runtime
        .router()
        .set_peer_connected(&remote, true)
        .await;

    let mut send_client = harness.client.clone();
    let remote_value = remote.to_string();
    let send = tokio::spawn(async move {
        send_client
            .send_task(SendTaskRequest {
                target_peer_id: remote_value,
                envelope: Some(envelope("route-relay-in-flight")),
                timeout_ms: 5_000,
            })
            .await
    });
    while mock.call_count() == 0 {
        tokio::task::yield_now().await;
    }

    let claim_error = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "route-relay-in-flight".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "local-thief".to_string(),
            }),
            lease_duration_ms: 30_000,
        })
        .await
        .unwrap_err();
    assert_eq!(claim_error.code(), Code::FailedPrecondition);
    assert!(send.await.unwrap().is_ok());
}

#[tokio::test]
async fn remote_target_remains_not_claimable_after_publish_failure() {
    let mut harness = RpcTestHarness::start().await;
    let mock = Arc::new(MockRelayPublisher::new().failing());
    harness.runtime.router().set_publisher(mock).await;
    let remote = PeerId::new("node-remote-failed-publish").unwrap();
    harness
        .runtime
        .router()
        .set_peer_connected(&remote, true)
        .await;

    assert!(harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: remote.to_string(),
            envelope: Some(envelope("route-relay-failed-publish")),
            timeout_ms: 5_000,
        })
        .await
        .is_err());

    let claim_error = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "route-relay-failed-publish".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "local-thief".to_string(),
            }),
            lease_duration_ms: 30_000,
        })
        .await
        .unwrap_err();
    assert_eq!(claim_error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn remote_task_deadline_survives_relay_delivery_into_destination_claim_response() {
    let relay = RelayRuntime::new("relay-deadline-propagation-test");
    relay.mark_transport_listening();
    let relay_addr = spawn_relay(Arc::clone(&relay)).await;
    let relay_endpoint = format!("http://{relay_addr}");
    let source_peer = PeerId::new("node-deadline-source").unwrap();
    let destination_peer = PeerId::new("node-deadline-destination").unwrap();
    let source = RpcTestHarness::start_with_config(
        KeryxDaemonConfig::new(tempfile::tempdir().unwrap().keep(), 0)
            .with_local_peer_id(source_peer.clone())
            .with_relay_endpoint(Some(relay_endpoint.clone())),
    )
    .await;
    let mut destination = RpcTestHarness::start_with_config(
        KeryxDaemonConfig::new(tempfile::tempdir().unwrap().keep(), 0)
            .with_local_peer_id(destination_peer.clone()),
    )
    .await;
    source
        .runtime
        .router()
        .set_publisher(Arc::new(
            GrpcRelayTaskPublisher::new(relay_endpoint.clone(), source_peer.clone())
                .with_node_token("node-deadline-source-test-token"),
        ))
        .await;

    let (_node_tx, node_rx) = mpsc::channel::<NodeFrame>(1);
    let mut relay_client = KeryxRelayClient::connect(relay_endpoint).await.unwrap();
    let mut connect_request = Request::new(ReceiverStream::new(node_rx));
    connect_request.metadata_mut().insert(
        NODE_ID_METADATA_KEY,
        destination_peer.as_str().parse().unwrap(),
    );
    connect_request.metadata_mut().insert(
        "x-keryx-node-token",
        "node-deadline-destination-test-token".parse().unwrap(),
    );
    let mut relay_stream = relay_client
        .connect_node(connect_request)
        .await
        .unwrap()
        .into_inner();

    let deadline_ms = unix_ms_now() + 60_000;
    let mut outbound = envelope("route-deadline-relay");
    outbound.deadline_ms = deadline_ms;
    source
        .runtime
        .router()
        .set_peer_connected(&destination_peer, true)
        .await;
    let mut source_client = source.client.clone();
    source_client
        .send_task(SendTaskRequest {
            target_peer_id: destination_peer.to_string(),
            envelope: Some(outbound),
            timeout_ms: 5_000,
        })
        .await
        .unwrap();

    let frame = tokio::time::timeout(Duration::from_secs(3), relay_stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(frame.task.as_ref().unwrap().deadline_ms, deadline_ms);
    destination
        .client
        .submit_remote_task(SubmitRemoteTaskRequest {
            envelope: frame.task,
            authenticated_sender_peer_id: frame.authenticated_source_node_id,
            destination_peer_id: frame.destination_node_id,
            relay_frame_id: frame.frame_id,
        })
        .await
        .unwrap();

    let stored = destination
        .runtime
        .store()
        .get_task(&keryx_core::TaskId::new("route-deadline-relay").unwrap())
        .await
        .unwrap();
    assert_eq!(stored.deadline_ms, Some(deadline_ms));
    let claim = destination
        .client
        .claim_next_task(ClaimNextTaskRequest {
            worker_id: Some(AgentId {
                value: "deadline-worker".to_string(),
            }),
            accepted_skill_ids: vec![],
            accepted_capability_ids: vec![],
            lease_duration_ms: 60_000,
            wait_timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(claim.has_task);
    assert_eq!(claim.envelope.unwrap().deadline_ms, deadline_ms);
}

#[tokio::test]
async fn already_expired_remote_task_is_timed_out_before_claim() {
    let destination_peer = PeerId::new("node-expired-destination").unwrap();
    let mut destination = RpcTestHarness::start_with_config(
        KeryxDaemonConfig::new(tempfile::tempdir().unwrap().keep(), 0)
            .with_local_peer_id(destination_peer.clone()),
    )
    .await;
    let mut expired = envelope("route-expired-remote");
    expired.deadline_ms = unix_ms_now() - 1;

    destination
        .client
        .submit_remote_task(SubmitRemoteTaskRequest {
            envelope: Some(expired),
            authenticated_sender_peer_id: "node-expired-source".to_string(),
            destination_peer_id: destination_peer.to_string(),
            relay_frame_id: "relay-expired".to_string(),
        })
        .await
        .unwrap();

    let claim = destination
        .client
        .claim_next_task(ClaimNextTaskRequest {
            worker_id: Some(AgentId {
                value: "expired-worker".to_string(),
            }),
            accepted_skill_ids: vec![],
            accepted_capability_ids: vec![],
            lease_duration_ms: 60_000,
            wait_timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!claim.has_task);
    let stored = destination
        .runtime
        .store()
        .get_task(&keryx_core::TaskId::new("route-expired-remote").unwrap())
        .await
        .unwrap();
    assert_eq!(stored.status, keryx_core::TaskStatus::Failed);

    let mut direct_expired = envelope("route-expired-direct-claim");
    direct_expired.deadline_ms = unix_ms_now() - 1;
    destination
        .client
        .submit_remote_task(SubmitRemoteTaskRequest {
            envelope: Some(direct_expired),
            authenticated_sender_peer_id: "node-expired-source".to_string(),
            destination_peer_id: destination_peer.to_string(),
            relay_frame_id: "relay-expired-direct".to_string(),
        })
        .await
        .unwrap();
    let direct_error = destination
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "route-expired-direct-claim".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "expired-direct-worker".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap_err();
    assert_eq!(direct_error.code(), Code::FailedPrecondition);
    let directly_claimed = destination
        .runtime
        .store()
        .get_task(&keryx_core::TaskId::new("route-expired-direct-claim").unwrap())
        .await
        .unwrap();
    assert_eq!(directly_claimed.status, keryx_core::TaskStatus::Failed);
}

#[tokio::test]
async fn negative_remote_deadline_is_rejected_at_ingress() {
    let destination_peer = PeerId::new("node-invalid-deadline-destination").unwrap();
    let mut destination = RpcTestHarness::start_with_config(
        KeryxDaemonConfig::new(tempfile::tempdir().unwrap().keep(), 0)
            .with_local_peer_id(destination_peer.clone()),
    )
    .await;
    let mut invalid = envelope("route-invalid-deadline");
    invalid.deadline_ms = -1;

    let error = destination
        .client
        .submit_remote_task(SubmitRemoteTaskRequest {
            envelope: Some(invalid),
            authenticated_sender_peer_id: "node-invalid-deadline-source".to_string(),
            destination_peer_id: destination_peer.to_string(),
            relay_frame_id: "relay-invalid-deadline".to_string(),
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(destination
        .runtime
        .store()
        .get_task(&keryx_core::TaskId::new("route-invalid-deadline").unwrap())
        .await
        .is_err());
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
    assert_eq!(response.status, "relay_accepted");
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
    assert_eq!(response.status, "relay_accepted");
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
    let context = harness
        .runtime
        .store()
        .get_transport_context(&keryx_core::TaskId::new("route-relay-failure").unwrap())
        .await
        .unwrap();
    assert_eq!(context.expected_executor_peer_id.as_ref(), Some(&remote));
    assert_eq!(context.destination_peer_id, remote);
    assert!(context.relay_frame_id.is_none());
}

#[tokio::test]
async fn grpc_relay_task_publisher_publishes_to_relay_mailbox() {
    let runtime = RelayRuntime::new("relay-publisher-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let publisher = GrpcRelayTaskPublisher::new(
        format!("http://{addr}"),
        PeerId::new("node-grpc-source").unwrap(),
    )
    .with_node_token("node-grpc-source-test-token");
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
    let publisher = GrpcRelayTaskPublisher::new(
        format!("http://{addr}"),
        PeerId::new("node-grpc-source").unwrap(),
    )
    .with_node_token("node-grpc-source-test-token");
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
    let registry = Arc::new(SkillRegistry::new());
    registry
        .register_with_features(
            PeerId::new("node-deadline-destination").unwrap(),
            Vec::new(),
            "node-deadline-destination".to_string(),
            String::new(),
            vec!["absolute_deadlines_v1".to_string()],
            None,
        )
        .await;
    let node_auth = Arc::new(NodeTokenAuth::new(
        HashMap::from([
            (
                NodeId::new("node-deadline-source").unwrap(),
                "node-deadline-source-test-token".to_string(),
            ),
            (
                NodeId::new("node-deadline-destination").unwrap(),
                "node-deadline-destination-test-token".to_string(),
            ),
            (
                NodeId::new("node-grpc-source").unwrap(),
                "node-grpc-source-test-token".to_string(),
            ),
        ]),
        HashSet::new(),
    ));
    tokio::spawn(async move {
        let _ = serve_grpc_health_with_auth(runtime, registry, node_auth, addr).await;
    });
    for _ in 0..40 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("failed to connect gRPC relay at http://{addr}");
}

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
