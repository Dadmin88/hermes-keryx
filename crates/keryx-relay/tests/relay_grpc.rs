use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use keryx_core::{
    Digest, PeerId, MAX_CROSS_NODE_RESULT_ARTIFACT_BYTES, RESULT_ARTIFACT_FRAME_MAX_BYTES,
};
use keryx_proto::v1::keryx_relay_client::KeryxRelayClient;
use keryx_proto::v1::{
    AckFrameRequest, AckTaskRequest, ArtifactId, NodeFrame, NodeId, PublishResultRequest,
    PublishTaskRequest, RegisterNodeRequest, ResultArtifact, TaskEnvelope, TaskId,
    TaskResultEnvelope, TaskStatus, TerminalOutcome,
};
use keryx_relay::health_server::{
    serve_grpc_health, serve_grpc_health_with_auth, serve_grpc_health_with_auth_and_tls,
    NODE_ID_METADATA_KEY, NODE_TOKEN_METADATA_KEY,
};
use keryx_relay::registry::SkillRegistry;
use keryx_relay::runtime::RelayRuntime;
use keryx_relay::security::NodeTokenAuth;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request};

#[tokio::test]
async fn authenticated_plaintext_helper_rejects_non_loopback_bind() {
    let runtime = RelayRuntime::new("relay-control-plaintext-test");
    let registry = Arc::new(SkillRegistry::new());
    let auth = Arc::new(NodeTokenAuth::new(HashMap::new(), Default::default()));

    let error = serve_grpc_health_with_auth(runtime, registry, auth, "0.0.0.0:0".parse().unwrap())
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("non-loopback authenticated relay control listeners require TLS"));
}

#[tokio::test]
async fn authenticated_relay_control_accepts_tls_connection() {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let runtime = RelayRuntime::new("relay-control-tls-test");
    let auth = Arc::new(NodeTokenAuth::new(HashMap::new(), Default::default()));
    let server = tokio::spawn(serve_grpc_health_with_auth_and_tls(
        runtime,
        Arc::new(SkillRegistry::new()),
        auth,
        addr,
        Some(Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes())),
    ));
    tokio::time::sleep(Duration::from_millis(25)).await;
    let channel = Endpoint::from_shared(format!("https://localhost:{}", addr.port()))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert_pem.as_bytes())),
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = KeryxRelayClient::new(channel);

    client
        .health(keryx_proto::v1::HealthRequest {})
        .await
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn relay_without_configured_auth_rejects_task_publication() {
    let runtime = RelayRuntime::new("relay-no-auth-task-test");
    runtime.mark_transport_listening();
    let addr = spawn_unauthenticated_relay(Arc::clone(&runtime)).await;
    let mut client = KeryxRelayClient::new(connect_grpc(addr).await);

    let error = client
        .publish_task(PublishTaskRequest {
            task: Some(task("task-forged-source", "node-target")),
            target_node_id: "node-target".to_string(),
            source_node_id: "forged-source".to_string(),
        })
        .await
        .expect_err("relay mutations must fail closed without configured auth");

    assert_eq!(error.code(), Code::Unauthenticated);
    assert_eq!(runtime.mailbox_depth("node-target"), 0);
}

#[tokio::test]
async fn relay_without_configured_auth_rejects_node_registration() {
    let runtime = RelayRuntime::new("relay-no-auth-register-test");
    runtime.mark_transport_listening();
    let addr = spawn_unauthenticated_relay(Arc::clone(&runtime)).await;
    let mut client = KeryxRelayClient::new(connect_grpc(addr).await);

    let error = client
        .register_node(RegisterNodeRequest {
            node_id: Some(NodeId {
                value: "unverified-node".to_string(),
            }),
            token: String::new(),
        })
        .await
        .expect_err("registration is a mutating authenticated operation");

    assert_eq!(error.code(), Code::Unauthenticated);
    assert!(runtime.peer_identity("unverified-node").is_none());
}

#[tokio::test]
async fn relay_without_configured_auth_rejects_connect_and_ack_mutations() {
    let runtime = RelayRuntime::new("relay-no-auth-stream-ack-test");
    runtime.mark_transport_listening();
    let addr = spawn_unauthenticated_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let (_tx, rx) = mpsc::channel(1);
    let connect_error = KeryxRelayClient::new(channel.clone())
        .connect_node(connect_request("unverified-node", rx))
        .await
        .expect_err("ConnectNode must fail closed without configured auth");
    assert_eq!(connect_error.code(), Code::Unauthenticated);

    let ack_frame_error = KeryxRelayClient::new(channel.clone())
        .ack_frame(authenticated_request(
            "unverified-node",
            AckFrameRequest {
                frame_id: "unverified-frame".to_string(),
            },
        ))
        .await
        .expect_err("AckFrame must fail closed without configured auth");
    assert_eq!(ack_frame_error.code(), Code::Unauthenticated);

    let ack_task_error = KeryxRelayClient::new(channel)
        .ack_task(authenticated_request(
            "unverified-node",
            AckTaskRequest {
                task_id: Some(TaskId {
                    value: "unverified-task".to_string(),
                }),
            },
        ))
        .await
        .expect_err("AckTask must fail closed without configured auth");
    assert_eq!(ack_task_error.code(), Code::Unauthenticated);
    assert!(runtime.peer_identity("unverified-node").is_none());
}

#[tokio::test]
async fn frame_acknowledgement_is_bound_to_authenticated_destination() {
    let runtime = RelayRuntime::new("relay-recipient-bound-ack-test");
    runtime.mark_transport_listening();
    let addr = spawn_authenticated_relay_for(
        Arc::clone(&runtime),
        &["source-node", "destination-a", "destination-b"],
    )
    .await;
    let channel = connect_grpc(addr).await;
    let mut publisher = KeryxRelayClient::new(channel.clone());
    let forged_publish = authenticated_request(
        "source-node",
        PublishTaskRequest {
            task: Some(task("task-forged-claim", "destination-b")),
            target_node_id: "destination-b".to_string(),
            source_node_id: "forged-source".to_string(),
        },
    );
    let forged_error = publisher
        .publish_task(forged_publish)
        .await
        .expect_err("claimed identity must not override authenticated metadata");
    assert_eq!(forged_error.code(), Code::PermissionDenied);
    assert_eq!(runtime.mailbox_depth("destination-b"), 0);

    let mut outbound = task("task-recipient-bound", "destination-b");
    outbound
        .metadata
        .insert("frame_id".to_string(), "attacker-chosen-frame".to_string());
    let mut publish = Request::new(PublishTaskRequest {
        task: Some(outbound.clone()),
        target_node_id: "destination-b".to_string(),
        source_node_id: "source-node".to_string(),
    });
    add_auth_metadata(&mut publish, "source-node");
    let receipt = publisher
        .publish_task(publish)
        .await
        .expect("authenticated publish")
        .into_inner();
    assert_ne!(receipt.frame_id, "attacker-chosen-frame");
    assert_ne!(receipt.frame_id, "relay-task-recipient-bound");
    assert_eq!(receipt.authenticated_source_peer_id, "source-node");
    assert_eq!(receipt.accepted_destination_peer_id, "destination-b");
    assert_eq!(receipt.accepted_route, "relay");
    assert!(receipt.accepted_at_ms > 0);
    assert_eq!(runtime.mailbox_depth("destination-b"), 1);

    let mut retry = Request::new(PublishTaskRequest {
        task: Some(outbound.clone()),
        target_node_id: "destination-b".to_string(),
        source_node_id: "source-node".to_string(),
    });
    add_auth_metadata(&mut retry, "source-node");
    let retried_receipt = publisher
        .publish_task(retry)
        .await
        .expect("identical publish retry")
        .into_inner();
    assert_eq!(retried_receipt.frame_id, receipt.frame_id);
    assert_eq!(retried_receipt.accepted_at_ms, receipt.accepted_at_ms);
    assert_eq!(runtime.mailbox_depth("destination-b"), 1);

    outbound
        .metadata
        .insert("changed".to_string(), "true".to_string());
    let mut conflicting = Request::new(PublishTaskRequest {
        task: Some(outbound),
        target_node_id: "destination-b".to_string(),
        source_node_id: "source-node".to_string(),
    });
    add_auth_metadata(&mut conflicting, "source-node");
    let conflict = publisher
        .publish_task(conflicting)
        .await
        .expect_err("same task identity with a changed envelope must fail");
    assert_eq!(conflict.code(), Code::AlreadyExists);
    assert_eq!(runtime.mailbox_depth("destination-b"), 1);

    let mut wrong_peer = KeryxRelayClient::new(channel.clone());
    let mut wrong_ack = Request::new(AckFrameRequest {
        frame_id: receipt.frame_id.clone(),
    });
    add_auth_metadata(&mut wrong_ack, "destination-a");
    let error = wrong_peer
        .ack_frame(wrong_ack)
        .await
        .expect_err("one peer must not acknowledge another peer's frame");
    assert_eq!(error.code(), Code::PermissionDenied);
    assert_eq!(runtime.mailbox_depth("destination-b"), 1);

    let mut destination = KeryxRelayClient::new(channel);
    let mut correct_ack = Request::new(AckFrameRequest {
        frame_id: receipt.frame_id.clone(),
    });
    add_auth_metadata(&mut correct_ack, "destination-b");
    assert!(
        destination
            .ack_frame(correct_ack)
            .await
            .expect("destination acknowledges exact frame")
            .into_inner()
            .accepted
    );
    assert_eq!(runtime.mailbox_depth("destination-b"), 0);

    let mut duplicate_ack = Request::new(AckFrameRequest {
        frame_id: receipt.frame_id,
    });
    add_auth_metadata(&mut duplicate_ack, "destination-b");
    assert!(
        destination
            .ack_frame(duplicate_ack)
            .await
            .expect("duplicate acknowledgement is idempotent")
            .into_inner()
            .accepted
    );
}

#[tokio::test]
async fn legacy_task_acknowledgement_is_rejected() {
    let runtime = RelayRuntime::new("relay-legacy-ack-rejected-test");
    runtime.mark_transport_listening();
    let addr = spawn_authenticated_relay_for(Arc::clone(&runtime), &["destination-node"]).await;
    let mut client = KeryxRelayClient::new(connect_grpc(addr).await);
    let mut request = Request::new(AckTaskRequest {
        task_id: Some(TaskId {
            value: "legacy-task".to_string(),
        }),
    });
    add_auth_metadata(&mut request, "destination-node");

    let error = client
        .ack_task(request)
        .await
        .expect_err("legacy task acknowledgement cannot prove frame ownership");

    assert_eq!(error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn connect_node_relays_node_frames_to_connected_target() {
    let runtime = RelayRuntime::new("relay-grpc-frame-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let mut admin = KeryxRelayClient::new(channel.clone());
    register(&mut admin, "node-a").await;
    register(&mut admin, "node-b").await;

    let (_b_tx, b_rx) = mpsc::channel(4);
    let mut node_b = KeryxRelayClient::new(channel.clone());
    let mut b_stream = node_b
        .connect_node(connect_request("node-b", b_rx))
        .await
        .expect("connect node-b")
        .into_inner();

    let (a_tx, a_rx) = mpsc::channel(4);
    let mut node_a = KeryxRelayClient::new(channel);
    let mut _a_stream = node_a
        .connect_node(connect_request("node-a", a_rx))
        .await
        .expect("connect node-a")
        .into_inner();

    a_tx.send(NodeFrame {
        frame_id: "frame-a-to-b".to_string(),
        target_node_id: "node-b".to_string(),
        task: Some(task("task-a-to-b", "node-b")),
        result: None,
    })
    .await
    .expect("send node frame");

    let delivered = tokio::time::timeout(Duration::from_secs(3), b_stream.next())
        .await
        .expect("relay frame timeout")
        .expect("relay stream ended")
        .expect("relay frame status");

    assert_ne!(delivered.frame_id, "frame-a-to-b");
    assert!(!delivered.frame_id.trim().is_empty());
    assert_eq!(task_id(&delivered.task.unwrap()), "task-a-to-b");
    assert_eq!(runtime.metrics().snapshot().tasks_routed, 1);
}

#[tokio::test]
async fn publish_task_stores_offline_mailbox_and_delivers_on_reconnect() {
    let runtime = RelayRuntime::new("relay-grpc-offline-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let mut client = KeryxRelayClient::new(channel.clone());
    register(&mut client, "node-offline").await;

    let response = client
        .publish_task(authenticated_request(
            "node-publisher",
            PublishTaskRequest {
                task: Some(task("task-offline", "node-offline")),
                target_node_id: "node-offline".to_string(),
                source_node_id: "node-publisher".to_string(),
            },
        ))
        .await
        .expect("publish offline task")
        .into_inner();
    assert_eq!(response.task_id.as_ref().unwrap().value, "task-offline");
    let frame_id = response.frame_id;
    assert_eq!(runtime.mailbox_depth("node-offline"), 1);

    let (_node_tx, node_rx) = mpsc::channel(4);
    let mut node_client = KeryxRelayClient::new(channel.clone());
    let mut stream = node_client
        .connect_node(connect_request("node-offline", node_rx))
        .await
        .expect("connect offline node")
        .into_inner();

    let delivered = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("offline delivery timeout")
        .expect("relay stream ended")
        .expect("relay frame status");
    assert_eq!(task_id(&delivered.task.unwrap()), "task-offline");
    assert_eq!(runtime.mailbox_depth("node-offline"), 0);

    let mut ack_request = Request::new(AckFrameRequest { frame_id });
    add_auth_metadata(&mut ack_request, "node-offline");
    let acked = client
        .ack_frame(ack_request)
        .await
        .expect("ack exact frame")
        .into_inner();
    assert!(acked.accepted);
}

#[tokio::test]
async fn descriptor_only_publish_result_requires_configured_authentication() {
    let runtime = RelayRuntime::new("relay-grpc-result-auth-required");
    runtime.mark_transport_listening();
    let addr = spawn_unauthenticated_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;
    let mut publisher = result_frame_client(channel);

    let error = publisher
        .publish_result(PublishResultRequest {
            result: Some(TaskResultEnvelope {
                protocol_version: 1,
                task_id: Some(TaskId {
                    value: "result-auth-required".to_string(),
                }),
                correlation_id: None,
                outcome: TerminalOutcome::Completed as i32,
                executor_peer_id: "executor-node".to_string(),
                duration_ms: 0,
                completed_at_ms: 0,
                error_reason: String::new(),
                result_metadata: HashMap::new(),
                output_artifacts: vec![ResultArtifact {
                    path: "result.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    metadata: HashMap::new(),
                    artifact_id: None,
                    sha256: String::new(),
                    byte_len: 0,
                    content: Vec::new(),
                    content_present: false,
                }],
            }),
            target_node_id: "origin-node".to_string(),
            source_node_id: "executor-node".to_string(),
            frame_id: "result-auth-required".to_string(),
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::Unauthenticated);
    assert_eq!(runtime.mailbox_depth("origin-node"), 0);
}

#[tokio::test]
async fn authenticated_publish_result_delivers_four_mib_artifact_payload_unchanged() {
    let runtime = RelayRuntime::new("relay-grpc-result-frame-limit-test");
    runtime.mark_transport_listening();
    let addr = spawn_authenticated_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let (_origin_tx, origin_rx) = mpsc::channel(4);
    let mut origin = result_frame_client(channel.clone());
    let mut origin_stream = origin
        .connect_node(authenticated_connect_request("origin-node", origin_rx))
        .await
        .expect("connect authenticated origin node")
        .into_inner();

    let content = vec![0xA5; MAX_CROSS_NODE_RESULT_ARTIFACT_BYTES];
    let digest = Digest::compute(&content).to_string();
    let result = TaskResultEnvelope {
        protocol_version: 2,
        task_id: Some(TaskId {
            value: "result-frame-limit-task".to_string(),
        }),
        correlation_id: None,
        outcome: TerminalOutcome::Completed as i32,
        executor_peer_id: "executor-node".to_string(),
        duration_ms: 42,
        completed_at_ms: 1_800_000_000_000,
        error_reason: String::new(),
        result_metadata: HashMap::from([(String::from("summary"), String::from("4 MiB payload"))]),
        output_artifacts: vec![ResultArtifact {
            path: "outputs/final.bin".to_string(),
            media_type: "application/octet-stream".to_string(),
            metadata: HashMap::from([(String::from("role"), String::from("final"))]),
            artifact_id: Some(ArtifactId {
                value: "remote-artifact-id".to_string(),
            }),
            sha256: digest.clone(),
            byte_len: content.len() as u64,
            content: content.clone(),
            content_present: true,
        }],
    };

    let mut publisher = result_frame_client(channel);
    let mut request = Request::new(PublishResultRequest {
        result: Some(result),
        target_node_id: "origin-node".to_string(),
        source_node_id: "executor-node".to_string(),
        frame_id: "result-frame-limit".to_string(),
    });
    add_auth_metadata(&mut request, "executor-node");
    publisher
        .publish_result(request)
        .await
        .expect("publish authenticated four MiB result");

    let delivered = tokio::time::timeout(Duration::from_secs(3), origin_stream.next())
        .await
        .expect("result relay frame timeout")
        .expect("origin stream ended")
        .expect("result relay frame status");
    let delivered_result = delivered.result.expect("result frame");
    let delivered_artifact = delivered_result
        .output_artifacts
        .first()
        .expect("result artifact");
    assert_eq!(delivered_result.protocol_version, 2);
    assert!(delivered_artifact.content_present);
    assert_eq!(delivered_artifact.content, content);
    assert_eq!(delivered_artifact.sha256, digest);
    assert_eq!(
        delivered_artifact.byte_len,
        MAX_CROSS_NODE_RESULT_ARTIFACT_BYTES as u64
    );
}

#[tokio::test]
async fn byte_result_requires_destination_capability() {
    let runtime = RelayRuntime::new("relay-result-feature-gate");
    runtime.mark_transport_listening();
    let addr = spawn_authenticated_relay_for_features(
        Arc::clone(&runtime),
        &["origin-node", "executor-node"],
        &[],
    )
    .await;
    let channel = connect_grpc(addr).await;
    let mut publisher = result_frame_client(channel);
    let mut request = Request::new(PublishResultRequest {
        result: Some(TaskResultEnvelope {
            protocol_version: 2,
            task_id: Some(TaskId {
                value: "unsupported-byte-result".to_string(),
            }),
            correlation_id: None,
            outcome: TerminalOutcome::Completed as i32,
            executor_peer_id: "executor-node".to_string(),
            duration_ms: 0,
            completed_at_ms: 1,
            error_reason: String::new(),
            result_metadata: HashMap::new(),
            output_artifacts: vec![ResultArtifact {
                path: "result.bin".to_string(),
                media_type: "application/octet-stream".to_string(),
                metadata: HashMap::new(),
                artifact_id: None,
                sha256: Digest::compute(b"x").to_string(),
                byte_len: 1,
                content: vec![b'x'],
                content_present: false,
            }],
        }),
        target_node_id: "origin-node".to_string(),
        source_node_id: "executor-node".to_string(),
        frame_id: String::new(),
    });
    add_auth_metadata(&mut request, "executor-node");
    let error = publisher
        .publish_result(request)
        .await
        .expect_err("byte result without destination capability must fail");
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert_eq!(runtime.mailbox_depth("origin-node"), 0);
}

#[tokio::test]
async fn relay_rejects_result_frame_larger_than_transport_cap_without_delivery() {
    let runtime = RelayRuntime::new("relay-grpc-result-frame-overflow-test");
    runtime.mark_transport_listening();
    let addr = spawn_authenticated_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let (_origin_tx, origin_rx) = mpsc::channel(4);
    let mut origin = result_frame_client(channel.clone());
    let mut origin_stream = origin
        .connect_node(authenticated_connect_request("origin-node", origin_rx))
        .await
        .expect("connect authenticated origin node");
    let content = vec![0x5A; RESULT_ARTIFACT_FRAME_MAX_BYTES];
    let mut publisher =
        result_frame_client_with_limit(channel, RESULT_ARTIFACT_FRAME_MAX_BYTES + 1024);
    let mut request = Request::new(PublishResultRequest {
        result: Some(TaskResultEnvelope {
            protocol_version: 2,
            task_id: Some(TaskId {
                value: "result-frame-overflow-task".to_string(),
            }),
            correlation_id: None,
            outcome: TerminalOutcome::Completed as i32,
            executor_peer_id: "executor-node".to_string(),
            duration_ms: 0,
            completed_at_ms: 0,
            error_reason: String::new(),
            result_metadata: HashMap::new(),
            output_artifacts: vec![ResultArtifact {
                path: "outputs/overflow.bin".to_string(),
                media_type: "application/octet-stream".to_string(),
                metadata: HashMap::new(),
                artifact_id: None,
                sha256: String::new(),
                byte_len: content.len() as u64,
                content,
                content_present: true,
            }],
        }),
        target_node_id: "origin-node".to_string(),
        source_node_id: "executor-node".to_string(),
        frame_id: "result-frame-overflow".to_string(),
    });
    add_auth_metadata(&mut request, "executor-node");
    let error = publisher
        .publish_result(request)
        .await
        .expect_err("frame larger than relay transport cap must be rejected");
    // tonic maps configured gRPC message-size violations to OutOfRange.
    assert_eq!(error.code(), tonic::Code::OutOfRange);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(250),
            origin_stream.get_mut().message()
        )
        .await
        .is_err(),
        "oversized result frame must not be delivered"
    );
}

#[tokio::test]
async fn publish_task_preserves_execution_deadline_through_offline_mailbox() {
    let runtime = RelayRuntime::new("relay-grpc-deadline-test");
    runtime.mark_transport_listening();
    let addr = spawn_authenticated_relay_for(
        Arc::clone(&runtime),
        &["node-deadline-offline", "node-deadline-publisher"],
    )
    .await;
    let channel = connect_grpc(addr).await;

    let mut client = KeryxRelayClient::new(channel.clone());
    register(&mut client, "node-deadline-offline").await;
    let deadline_ms = 1_800_000_000_000;
    let mut outbound = task("task-deadline-offline", "node-deadline-offline");
    outbound.deadline_ms = deadline_ms;
    client
        .publish_task(authenticated_request(
            "node-deadline-publisher",
            PublishTaskRequest {
                task: Some(outbound),
                target_node_id: "node-deadline-offline".to_string(),
                source_node_id: "node-deadline-publisher".to_string(),
            },
        ))
        .await
        .expect("publish offline task");

    let (_node_tx, node_rx) = mpsc::channel(4);
    let mut node_client = KeryxRelayClient::new(channel);
    let mut stream = node_client
        .connect_node(authenticated_connect_request(
            "node-deadline-offline",
            node_rx,
        ))
        .await
        .expect("connect offline node")
        .into_inner();
    let delivered = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("offline delivery timeout")
        .expect("relay stream ended")
        .expect("relay frame status");

    assert_eq!(delivered.task.unwrap().deadline_ms, deadline_ms);
}

#[tokio::test]
async fn deadline_requires_destination_capability() {
    let runtime = RelayRuntime::new("relay-deadline-feature-gate");
    runtime.mark_transport_listening();
    let addr = spawn_authenticated_relay_for_features(
        Arc::clone(&runtime),
        &["deadline-origin", "deadline-target"],
        &[],
    )
    .await;
    let channel = connect_grpc(addr).await;
    let mut publisher = KeryxRelayClient::new(channel);
    let mut outbound = task("unsupported-deadline", "deadline-target");
    outbound.deadline_ms = 1_800_000_000_000;
    let error = publisher
        .publish_task(authenticated_request(
            "deadline-origin",
            PublishTaskRequest {
                task: Some(outbound),
                target_node_id: "deadline-target".to_string(),
                source_node_id: "deadline-origin".to_string(),
            },
        ))
        .await
        .expect_err("deadline without destination capability must fail");
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert_eq!(runtime.mailbox_depth("deadline-target"), 0);
}

#[tokio::test]
async fn ack_frame_removes_pending_offline_mailbox_entry() {
    let runtime = RelayRuntime::new("relay-grpc-ack-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let mut client = KeryxRelayClient::new(channel);
    register(&mut client, "node-pending").await;
    let receipt = client
        .publish_task(authenticated_request(
            "node-publisher",
            PublishTaskRequest {
                task: Some(task("task-pending", "node-pending")),
                target_node_id: "node-pending".to_string(),
                source_node_id: "node-publisher".to_string(),
            },
        ))
        .await
        .expect("publish pending task")
        .into_inner();
    assert_eq!(runtime.mailbox_depth("node-pending"), 1);

    let mut ack_request = Request::new(AckFrameRequest {
        frame_id: receipt.frame_id,
    });
    add_auth_metadata(&mut ack_request, "node-pending");
    let acked = client
        .ack_frame(ack_request)
        .await
        .expect("ack pending frame")
        .into_inner();
    assert!(acked.accepted);
    assert_eq!(runtime.mailbox_depth("node-pending"), 0);
}

#[tokio::test]
async fn register_node_appears_in_skill_registry() {
    let runtime = RelayRuntime::new("relay-grpc-registry-node-test");
    runtime.mark_transport_listening();
    let registry = Arc::new(SkillRegistry::new());
    let addr = spawn_relay_with_registry(Arc::clone(&runtime), Arc::clone(&registry)).await;
    let channel = connect_grpc(addr).await;

    let mut client = KeryxRelayClient::new(channel);
    register(&mut client, "node-registry").await;

    let registration = registry
        .get(&PeerId::new("node-registry").unwrap())
        .await
        .expect("registered node should appear in registry");
    assert_eq!(registration.peer_id.as_str(), "node-registry");
    assert!(registration.skills.is_empty());
}

#[tokio::test]
async fn publish_task_skill_metadata_is_discoverable_for_target_node() {
    let runtime = RelayRuntime::new("relay-grpc-registry-skill-test");
    runtime.mark_transport_listening();
    let registry = Arc::new(SkillRegistry::new());
    let addr = spawn_relay_with_registry(Arc::clone(&runtime), Arc::clone(&registry)).await;
    let channel = connect_grpc(addr).await;

    let mut client = KeryxRelayClient::new(channel);
    register(&mut client, "node-python").await;
    client
        .publish_task(authenticated_request(
            "node-publisher",
            PublishTaskRequest {
                task: Some(task_with_skill("task-python", "node-python", "python")),
                target_node_id: "node-python".to_string(),
                source_node_id: "node-publisher".to_string(),
            },
        ))
        .await
        .expect("publish task with skill metadata");

    let found = registry.discover(Some("python"), &[], 10).await;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].peer_id.as_str(), "node-python");
    assert_eq!(found[0].skills[0].skill_id, "python");
}

#[tokio::test]
async fn offline_registered_nodes_are_pruned_after_registry_timeout() {
    let runtime = RelayRuntime::new("relay-grpc-registry-timeout-test");
    runtime.mark_transport_listening();
    let registry = Arc::new(SkillRegistry::with_default_ttl(Duration::from_millis(50)));
    let addr = spawn_relay_with_registry(Arc::clone(&runtime), Arc::clone(&registry)).await;
    let channel = connect_grpc(addr).await;

    let mut client = KeryxRelayClient::new(channel);
    register(&mut client, "node-timeout").await;
    assert_eq!(registry.registration_count().await, 1);

    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(registry.registration_count().await, 0);
}

async fn spawn_unauthenticated_relay(runtime: Arc<RelayRuntime>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(async move {
        let _ = serve_grpc_health(runtime, None, addr).await;
    });
    addr
}

async fn spawn_relay(runtime: Arc<RelayRuntime>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let auth = test_node_auth();
    tokio::spawn(async move {
        let _ =
            serve_grpc_health_with_auth(runtime, Arc::new(SkillRegistry::new()), auth, addr).await;
    });
    addr
}

async fn spawn_relay_with_registry(
    runtime: Arc<RelayRuntime>,
    registry: Arc<SkillRegistry>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let auth = test_node_auth();
    tokio::spawn(async move {
        let _ = serve_grpc_health_with_auth(runtime, registry, auth, addr).await;
    });
    addr
}

fn test_node_auth() -> Arc<NodeTokenAuth> {
    let node_ids = [
        "node-a",
        "node-b",
        "node-offline",
        "node-publisher",
        "node-deadline-offline",
        "node-deadline-publisher",
        "node-pending",
        "node-registry",
        "node-python",
        "node-timeout",
    ];
    let tokens = node_ids
        .into_iter()
        .map(|node_id| (node_id.parse().unwrap(), format!("{node_id}-test-token")))
        .collect();
    Arc::new(NodeTokenAuth::new(tokens, Default::default()))
}

async fn spawn_authenticated_relay(runtime: Arc<RelayRuntime>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let auth = Arc::new(NodeTokenAuth::new(
        HashMap::from([
            (
                "origin-node".parse().unwrap(),
                "origin-node-test-token".to_string(),
            ),
            (
                "executor-node".parse().unwrap(),
                "executor-node-test-token".to_string(),
            ),
        ]),
        Default::default(),
    ));
    let registry = Arc::new(SkillRegistry::new());
    for node_id in ["origin-node", "executor-node"] {
        registry
            .register_with_features(
                node_id.parse().unwrap(),
                Vec::new(),
                node_id.to_string(),
                String::new(),
                vec![
                    "absolute_deadlines_v1".to_string(),
                    "result_artifact_bytes_v1".to_string(),
                ],
                None,
            )
            .await;
    }
    tokio::spawn(async move {
        let _ = serve_grpc_health_with_auth(runtime, registry, auth, addr).await;
    });
    addr
}

async fn spawn_authenticated_relay_for(
    runtime: Arc<RelayRuntime>,
    node_ids: &[&str],
) -> SocketAddr {
    spawn_authenticated_relay_for_features(
        runtime,
        node_ids,
        &["absolute_deadlines_v1", "result_artifact_bytes_v1"],
    )
    .await
}

async fn spawn_authenticated_relay_for_features(
    runtime: Arc<RelayRuntime>,
    node_ids: &[&str],
    protocol_features: &[&str],
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let tokens = node_ids
        .iter()
        .map(|node_id| ((*node_id).parse().unwrap(), format!("{node_id}-test-token")))
        .collect();
    let auth = Arc::new(NodeTokenAuth::new(tokens, Default::default()));
    let registry = Arc::new(SkillRegistry::new());
    for node_id in node_ids {
        registry
            .register_with_features(
                (*node_id).parse().unwrap(),
                Vec::new(),
                (*node_id).to_string(),
                String::new(),
                protocol_features
                    .iter()
                    .map(|feature| (*feature).to_string())
                    .collect(),
                None,
            )
            .await;
    }
    tokio::spawn(async move {
        let _ = serve_grpc_health_with_auth(runtime, registry, auth, addr).await;
    });
    addr
}

fn result_frame_client(channel: Channel) -> KeryxRelayClient<Channel> {
    result_frame_client_with_limit(channel, RESULT_ARTIFACT_FRAME_MAX_BYTES)
}

fn result_frame_client_with_limit(
    channel: Channel,
    max_message_size: usize,
) -> KeryxRelayClient<Channel> {
    KeryxRelayClient::new(channel)
        .max_encoding_message_size(max_message_size)
        .max_decoding_message_size(max_message_size)
}

fn authenticated_connect_request(
    node_id: &str,
    rx: mpsc::Receiver<NodeFrame>,
) -> Request<ReceiverStream<NodeFrame>> {
    let mut request = connect_request(node_id, rx);
    add_auth_metadata(&mut request, node_id);
    request
}

fn authenticated_request<T>(node_id: &str, message: T) -> Request<T> {
    let mut request = Request::new(message);
    add_auth_metadata(&mut request, node_id);
    request
}

fn add_auth_metadata<T>(request: &mut Request<T>, node_id: &str) {
    request
        .metadata_mut()
        .insert(NODE_ID_METADATA_KEY, node_id.parse().unwrap());
    request.metadata_mut().insert(
        NODE_TOKEN_METADATA_KEY,
        format!("{node_id}-test-token").parse().unwrap(),
    );
}

async fn connect_grpc(addr: SocketAddr) -> Channel {
    let uri = format!("http://{addr}");
    for _ in 0..40 {
        if let Ok(channel) = Endpoint::from_shared(uri.clone()).unwrap().connect().await {
            return channel;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("failed to connect gRPC relay at {uri}");
}

async fn register(client: &mut KeryxRelayClient<Channel>, node_id: &str) {
    let response = client
        .register_node(RegisterNodeRequest {
            node_id: Some(NodeId {
                value: node_id.to_string(),
            }),
            token: format!("{node_id}-test-token"),
        })
        .await
        .expect("register node")
        .into_inner();
    assert!(response.accepted);
}

fn connect_request(
    node_id: &str,
    rx: mpsc::Receiver<NodeFrame>,
) -> Request<ReceiverStream<NodeFrame>> {
    let mut request = Request::new(ReceiverStream::new(rx));
    add_auth_metadata(&mut request, node_id);
    request
}

fn task(task_id: &str, target_node_id: &str) -> TaskEnvelope {
    let mut metadata = HashMap::new();
    metadata.insert("target_node_id".to_string(), target_node_id.to_string());
    TaskEnvelope {
        task_id: Some(TaskId {
            value: task_id.to_string(),
        }),
        correlation_id: None,
        idempotency_key: None,
        status: TaskStatus::Created as i32,
        messages: vec![],
        metadata,
        deadline_ms: 0,
    }
}

fn task_with_skill(task_id: &str, target_node_id: &str, skill_id: &str) -> TaskEnvelope {
    let mut t = task(task_id, target_node_id);
    t.metadata
        .insert("skill_id".to_string(), skill_id.to_string());
    t
}

fn task_id(task: &TaskEnvelope) -> &str {
    task.task_id.as_ref().unwrap().value.as_str()
}
