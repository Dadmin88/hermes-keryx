mod common;

use std::sync::Arc;

use common::RpcTestHarness;
use keryx_core::{origin_result_artifact_id, Digest, PeerId, TaskId as CoreTaskId, TaskStatus};
use keryx_daemon::{DiscoverySettings, KeryxDaemonConfig};
use keryx_proto::v1::{
    AgentId, ArtifactId, ClaimNextResultDeliveryRequest, ClaimTaskRequest, CompleteTaskRequest,
    GetArtifactRequest, GetTaskResultRequest, IngestRemoteResultRequest, ResultArtifact,
    TaskArtifact, TaskEnvelope, TaskId, TaskResultEnvelope, TerminalOutcome,
};
use keryx_relay::{serve_registry_rpc, RegistryRpcService, SkillRegistry};
use keryx_store::{TaskEnvelopeRecord, TaskRecord, TaskTransportContextRecord};
use prost::Message;
use tokio::net::TcpListener;
use tonic::Code;

const ORIGIN: &str = "artifact-origin";
const EXECUTOR: &str = "artifact-executor";

async fn start_registry(
    registry: Arc<SkillRegistry>,
) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(serve_registry_rpc(
        RegistryRpcService::new(registry),
        listener,
    ));
    (endpoint, server)
}

fn peer(value: &str) -> PeerId {
    PeerId::new(value).unwrap()
}

fn task_id(value: &str) -> TaskId {
    TaskId {
        value: value.to_string(),
    }
}

fn envelope(id: &TaskId) -> TaskEnvelope {
    TaskEnvelope {
        task_id: Some(id.clone()),
        correlation_id: None,
        idempotency_key: None,
        status: 0,
        messages: Vec::new(),
        metadata: Default::default(),
        deadline_ms: 0,
    }
}

async fn origin_harness() -> RpcTestHarness {
    RpcTestHarness::start_with_config(
        KeryxDaemonConfig::new(tempfile::tempdir().unwrap().keep(), 0)
            .with_local_peer_id(peer(ORIGIN)),
    )
    .await
}

async fn seed_origin_task(harness: &RpcTestHarness, id: &TaskId) {
    let core_id = CoreTaskId::new(id.value.clone()).unwrap();
    let task_envelope = envelope(id);
    harness
        .runtime
        .accept_pending_remote_task_with_backpressure(
            TaskRecord::new(core_id.clone(), TaskStatus::Pending, None),
            TaskEnvelopeRecord::new(core_id.clone(), task_envelope.encode_to_vec(), 1),
            TaskTransportContextRecord {
                task_id: core_id,
                authenticated_sender_peer_id: Some(peer("artifact-sender")),
                expected_executor_peer_id: Some(peer(EXECUTOR)),
                destination_peer_id: peer(ORIGIN),
                relay_frame_id: Some("artifact-frame".to_string()),
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();
}

fn result(
    id: &TaskId,
    protocol_version: u32,
    artifacts: Vec<ResultArtifact>,
) -> TaskResultEnvelope {
    TaskResultEnvelope {
        protocol_version,
        task_id: Some(id.clone()),
        correlation_id: None,
        outcome: TerminalOutcome::Completed as i32,
        executor_peer_id: EXECUTOR.to_string(),
        duration_ms: 7,
        completed_at_ms: 9,
        error_reason: String::new(),
        result_metadata: Default::default(),
        output_artifacts: artifacts,
    }
}

fn present_artifact(content: Vec<u8>) -> ResultArtifact {
    ResultArtifact {
        path: "../../escape\\still-display-only".to_string(),
        media_type: "application/octet-stream".to_string(),
        metadata: [("source".to_string(), "/absolute/name".to_string())]
            .into_iter()
            .collect(),
        artifact_id: None,
        sha256: Digest::compute(&content).as_str().to_string(),
        byte_len: content.len() as u64,
        content,
        content_present: true,
    }
}

fn descriptor_artifact(path: &str) -> ResultArtifact {
    ResultArtifact {
        path: path.to_string(),
        media_type: "text/plain".to_string(),
        metadata: Default::default(),
        artifact_id: None,
        sha256: String::new(),
        byte_len: 0,
        content: Vec::new(),
        content_present: false,
    }
}

async fn ingest(
    harness: &mut RpcTestHarness,
    result: TaskResultEnvelope,
    destination: &str,
    authenticated_executor: &str,
) -> Result<(), tonic::Status> {
    harness
        .client
        .ingest_remote_result(IngestRemoteResultRequest {
            result: Some(result),
            authenticated_executor_peer_id: authenticated_executor.to_string(),
            destination_peer_id: destination.to_string(),
            relay_frame_id: "artifact-frame".to_string(),
        })
        .await
        .map(|_| ())
}

#[tokio::test]
async fn worker_completion_retains_present_binary_artifacts_and_uses_v2() {
    let registry = Arc::new(SkillRegistry::new());
    registry
        .register_with_features(
            peer(ORIGIN),
            Vec::new(),
            "origin".into(),
            String::new(),
            Vec::new(),
            None,
        )
        .await;
    let (registry_endpoint, _registry_server) = start_registry(Arc::clone(&registry)).await;
    let mut harness = RpcTestHarness::start_with_config(
        KeryxDaemonConfig::new(tempfile::tempdir().unwrap().keep(), 0)
            .with_local_peer_id(peer(EXECUTOR))
            .with_discovery(Some(DiscoverySettings {
                registry_endpoint,
                registry_ca_cert_path: None,
                registration: None,
                node_token: None,
            })),
    )
    .await;
    let id = task_id("worker-byte-result");
    let core_id = CoreTaskId::new(id.value.clone()).unwrap();
    let task_envelope = envelope(&id);
    harness
        .runtime
        .accept_pending_remote_task_with_backpressure(
            TaskRecord::new(core_id.clone(), TaskStatus::Pending, None),
            TaskEnvelopeRecord::new(core_id.clone(), task_envelope.encode_to_vec(), 1),
            TaskTransportContextRecord {
                task_id: core_id.clone(),
                authenticated_sender_peer_id: Some(peer(ORIGIN)),
                expected_executor_peer_id: Some(peer(EXECUTOR)),
                destination_peer_id: peer(EXECUTOR),
                relay_frame_id: Some("worker-frame".to_string()),
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();
    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(id.clone()),
            worker_id: Some(AgentId {
                value: "byte-worker".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    let bytes = vec![0, 255, 1, 0, 128];
    let completion = CompleteTaskRequest {
        task_id: Some(id.clone()),
        lease_id: claim.lease_id,
        worker_id: Some(AgentId {
            value: "byte-worker".to_string(),
        }),
        duration_ms: 1,
        result_metadata: Default::default(),
        output_artifacts: vec![TaskArtifact {
            path: "../../not-a-path".to_string(),
            media_type: "application/octet-stream".to_string(),
            metadata: Default::default(),
            content: bytes.clone(),
            byte_len: bytes.len() as u64,
            sha256: Digest::compute(&bytes).as_str().to_string(),
            content_present: true,
        }],
    };
    let unsupported = harness
        .client
        .complete_task(completion.clone())
        .await
        .unwrap_err();
    assert_eq!(unsupported.code(), Code::FailedPrecondition);
    assert_eq!(
        harness
            .runtime
            .store()
            .get_task(&core_id)
            .await
            .unwrap()
            .status,
        TaskStatus::Running
    );
    registry
        .register_with_features(
            peer(ORIGIN),
            Vec::new(),
            "origin".into(),
            String::new(),
            vec!["result_artifact_bytes_v1".into()],
            None,
        )
        .await;
    harness.client.complete_task(completion).await.unwrap();
    let delivery = harness
        .client
        .claim_next_result_delivery(ClaimNextResultDeliveryRequest {
            worker_id: "delivery-worker".to_string(),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    let stored = delivery.result.unwrap();
    assert_eq!(stored.protocol_version, 2);
    assert_eq!(stored.output_artifacts.len(), 1);
    let artifact = &stored.output_artifacts[0];
    assert_eq!(artifact.content, bytes);
    assert!(artifact.content_present);
    assert_eq!(artifact.byte_len, 5);
    assert_eq!(artifact.sha256, Digest::compute(&artifact.content).as_str());
    assert!(artifact.artifact_id.is_none());
}

#[tokio::test]
async fn worker_rejects_artifact_length_digest_and_absence_mismatches_before_completion() {
    for (suffix, artifact) in [
        (
            "length",
            TaskArtifact {
                path: "bad".to_string(),
                media_type: "application/octet-stream".to_string(),
                metadata: Default::default(),
                content: b"bytes".to_vec(),
                byte_len: 6,
                sha256: Digest::compute(b"bytes").as_str().to_string(),
                content_present: true,
            },
        ),
        (
            "digest",
            TaskArtifact {
                path: "bad".to_string(),
                media_type: "application/octet-stream".to_string(),
                metadata: Default::default(),
                content: b"bytes".to_vec(),
                byte_len: 5,
                sha256: Digest::compute(b"other").as_str().to_string(),
                content_present: true,
            },
        ),
        (
            "absence",
            TaskArtifact {
                path: "bad".to_string(),
                media_type: "application/octet-stream".to_string(),
                metadata: Default::default(),
                content: b"bytes".to_vec(),
                byte_len: 0,
                sha256: String::new(),
                content_present: false,
            },
        ),
    ] {
        let mut harness = RpcTestHarness::start().await;
        let id = task_id(&format!("worker-reject-{suffix}"));
        harness
            .client
            .submit_task(keryx_proto::v1::SubmitTaskRequest {
                envelope: Some(envelope(&id)),
            })
            .await
            .unwrap();
        let claim = harness
            .client
            .claim_task(ClaimTaskRequest {
                task_id: Some(id.clone()),
                worker_id: Some(AgentId {
                    value: "reject-worker".to_string(),
                }),
                lease_duration_ms: 60_000,
            })
            .await
            .unwrap()
            .into_inner();
        let error = harness
            .client
            .complete_task(CompleteTaskRequest {
                task_id: Some(id.clone()),
                lease_id: claim.lease_id,
                worker_id: Some(AgentId {
                    value: "reject-worker".to_string(),
                }),
                duration_ms: 1,
                result_metadata: Default::default(),
                output_artifacts: vec![artifact],
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument, "{suffix}");
        assert_eq!(
            harness
                .runtime
                .store()
                .get_task(&CoreTaskId::new(id.value).unwrap())
                .await
                .unwrap()
                .status,
            TaskStatus::Running,
            "{suffix} must not terminalize the task"
        );
    }
}

#[tokio::test]
async fn origin_ingest_canonicalizes_binary_and_zero_byte_artifacts_for_result_and_retrieval() {
    let mut harness = origin_harness().await;
    let id = task_id("origin-canonical-binary");
    seed_origin_task(&harness, &id).await;
    let binary = present_artifact(vec![0, 255, 1, 0, 128]);
    let zero = present_artifact(Vec::new());
    ingest(
        &mut harness,
        result(&id, 2, vec![binary.clone(), zero.clone()]),
        ORIGIN,
        EXECUTOR,
    )
    .await
    .unwrap();
    let persisted = harness
        .client
        .get_task_result(GetTaskResultRequest {
            task_id: Some(id.clone()),
        })
        .await
        .unwrap()
        .into_inner()
        .result
        .unwrap();
    assert_eq!(persisted.protocol_version, 2);
    assert_eq!(persisted.output_artifacts.len(), 2);
    for (ordinal, artifact) in persisted.output_artifacts.iter().enumerate() {
        assert_eq!(
            artifact.artifact_id.as_ref().unwrap().value,
            origin_result_artifact_id(&CoreTaskId::new(id.value.clone()).unwrap(), ordinal as u32)
                .as_str()
        );
        assert!(!artifact.content_present);
        assert!(artifact.content.is_empty());
    }
    assert_eq!(persisted.output_artifacts[0].sha256, binary.sha256);
    assert_eq!(persisted.output_artifacts[0].byte_len, binary.byte_len);
    assert_eq!(persisted.output_artifacts[1].byte_len, 0);
    let content = harness
        .client
        .get_artifact(GetArtifactRequest {
            artifact_id: persisted.output_artifacts[0].artifact_id.clone(),
            metadata_only: false,
        })
        .await
        .unwrap()
        .into_inner()
        .content;
    assert_eq!(content, binary.content);
    assert!(!harness.runtime.config().blob_dir().join("escape").exists());
}

#[tokio::test]
async fn origin_ingest_assigns_descriptor_ids_and_preserves_mixed_result_ordinals() {
    let mut harness = origin_harness().await;

    let descriptor_id = task_id("origin-canonical-descriptor");
    seed_origin_task(&harness, &descriptor_id).await;
    ingest(
        &mut harness,
        result(
            &descriptor_id,
            1,
            vec![descriptor_artifact("display-only.txt")],
        ),
        ORIGIN,
        EXECUTOR,
    )
    .await
    .unwrap();
    let descriptor_result = harness
        .client
        .get_task_result(GetTaskResultRequest {
            task_id: Some(descriptor_id.clone()),
        })
        .await
        .unwrap()
        .into_inner()
        .result
        .unwrap();
    assert_eq!(
        descriptor_result.output_artifacts[0]
            .artifact_id
            .as_ref()
            .unwrap()
            .value,
        origin_result_artifact_id(&CoreTaskId::new(descriptor_id.value.clone()).unwrap(), 0,)
            .as_str()
    );

    let mixed_id = task_id("origin-canonical-mixed");
    seed_origin_task(&harness, &mixed_id).await;
    let bytes = vec![0, 255, 7, 0];
    ingest(
        &mut harness,
        result(
            &mixed_id,
            2,
            vec![
                descriptor_artifact("../../display-only.txt"),
                present_artifact(bytes.clone()),
            ],
        ),
        ORIGIN,
        EXECUTOR,
    )
    .await
    .unwrap();
    let mixed_result = harness
        .client
        .get_task_result(GetTaskResultRequest {
            task_id: Some(mixed_id.clone()),
        })
        .await
        .unwrap()
        .into_inner()
        .result
        .unwrap();
    let core_mixed_id = CoreTaskId::new(mixed_id.value).unwrap();
    assert_eq!(mixed_result.output_artifacts.len(), 2);
    for (ordinal, artifact) in mixed_result.output_artifacts.iter().enumerate() {
        assert_eq!(
            artifact.artifact_id.as_ref().unwrap().value,
            origin_result_artifact_id(&core_mixed_id, ordinal as u32).as_str()
        );
    }
    assert_eq!(
        harness
            .client
            .get_artifact(GetArtifactRequest {
                artifact_id: mixed_result.output_artifacts[1].artifact_id.clone(),
                metadata_only: false,
            })
            .await
            .unwrap()
            .into_inner()
            .content,
        bytes
    );
}

#[tokio::test]
async fn origin_ingest_rejects_untrusted_id_version_hash_length_and_auth_without_mutation() {
    for (suffix, mut incoming, destination, authenticated, expected) in [
        (
            "id",
            result(
                &task_id("placeholder"),
                2,
                vec![ResultArtifact {
                    artifact_id: Some(ArtifactId {
                        value: "executor-chosen".to_string(),
                    }),
                    ..present_artifact(b"bytes".to_vec())
                }],
            ),
            ORIGIN,
            EXECUTOR,
            Code::InvalidArgument,
        ),
        (
            "version",
            result(
                &task_id("placeholder"),
                1,
                vec![present_artifact(b"bytes".to_vec())],
            ),
            ORIGIN,
            EXECUTOR,
            Code::FailedPrecondition,
        ),
        (
            "hash",
            result(
                &task_id("placeholder"),
                2,
                vec![present_artifact(b"bytes".to_vec())],
            ),
            ORIGIN,
            EXECUTOR,
            Code::InvalidArgument,
        ),
        (
            "length",
            result(
                &task_id("placeholder"),
                2,
                vec![present_artifact(b"bytes".to_vec())],
            ),
            ORIGIN,
            EXECUTOR,
            Code::InvalidArgument,
        ),
        (
            "absence",
            result(
                &task_id("placeholder"),
                1,
                vec![ResultArtifact {
                    content: b"ambiguous".to_vec(),
                    content_present: false,
                    ..present_artifact(Vec::new())
                }],
            ),
            ORIGIN,
            EXECUTOR,
            Code::InvalidArgument,
        ),
        (
            "destination",
            result(
                &task_id("placeholder"),
                2,
                vec![present_artifact(b"bytes".to_vec())],
            ),
            "other-origin",
            EXECUTOR,
            Code::PermissionDenied,
        ),
        (
            "executor",
            result(
                &task_id("placeholder"),
                2,
                vec![present_artifact(b"bytes".to_vec())],
            ),
            ORIGIN,
            "wrong-executor",
            Code::PermissionDenied,
        ),
    ] {
        let mut harness = origin_harness().await;
        let id = task_id(&format!("origin-reject-{suffix}"));
        seed_origin_task(&harness, &id).await;
        incoming.task_id = Some(id.clone());
        if suffix == "hash" {
            incoming.output_artifacts[0].sha256 = Digest::compute(b"other").as_str().to_string();
        }
        if suffix == "length" {
            incoming.output_artifacts[0].byte_len += 1;
        }
        let error = ingest(&mut harness, incoming, destination, authenticated)
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected, "{suffix}");
        let core_id = CoreTaskId::new(id.value).unwrap();
        assert_eq!(
            harness
                .runtime
                .store()
                .get_task(&core_id)
                .await
                .unwrap()
                .status,
            TaskStatus::Pending,
            "{suffix} must not mutate task"
        );
        assert!(harness
            .runtime
            .store()
            .list_artifacts_for_task(&core_id)
            .await
            .unwrap()
            .is_empty());
        assert!(harness
            .runtime
            .store()
            .get_terminal_result(&core_id)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn origin_ingest_accepts_exactly_four_mib_and_exact_replay_but_rejects_conflicts_and_plus_one(
) {
    let mut harness = origin_harness().await;
    let id = task_id("origin-four-mib");
    seed_origin_task(&harness, &id).await;
    let exact = present_artifact(vec![9; 4 * 1024 * 1024]);
    let request = result(&id, 2, vec![exact.clone()]);
    ingest(&mut harness, request.clone(), ORIGIN, EXECUTOR)
        .await
        .unwrap();
    ingest(&mut harness, request, ORIGIN, EXECUTOR)
        .await
        .unwrap();
    let mut conflict = exact;
    conflict.content = vec![8; 4 * 1024 * 1024];
    conflict.sha256 = Digest::compute(&conflict.content).as_str().to_string();
    assert_eq!(
        ingest(
            &mut harness,
            result(&id, 2, vec![conflict]),
            ORIGIN,
            EXECUTOR
        )
        .await
        .unwrap_err()
        .code(),
        Code::AlreadyExists
    );

    let oversize_id = task_id("origin-over-four-mib");
    seed_origin_task(&harness, &oversize_id).await;
    let error = ingest(
        &mut harness,
        result(
            &oversize_id,
            2,
            vec![present_artifact(vec![1; 4 * 1024 * 1024 + 1])],
        ),
        ORIGIN,
        EXECUTOR,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        harness
            .runtime
            .store()
            .get_task(&CoreTaskId::new(oversize_id.value).unwrap())
            .await
            .unwrap()
            .status,
        TaskStatus::Pending
    );
}
