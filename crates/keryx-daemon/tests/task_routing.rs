use std::sync::Arc;
use std::time::Duration;

use keryx_core::PeerId;
use keryx_proto::v1::{ClaimTaskRequest, ListPeersRequest, SendTaskRequest, TaskEnvelope, TaskId};
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
