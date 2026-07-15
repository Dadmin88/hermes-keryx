mod common;

use common::RpcTestHarness;
use keryx_core::{LimitsConfig, TaskStatus};
use keryx_daemon::{
    handle_incoming_task, IncomingDispatchConfig, IncomingHandleResult, IncomingRelayTask,
    KeryxDaemonConfig, StaticSenderAllowlist,
};
use keryx_proto::v1::{
    AgentId, ClaimTaskRequest, CompleteTaskRequest, DoctorRequest, SendTaskRequest, StatusRequest,
    SubmitTaskRequest, TaskEnvelope, TaskId, TaskMessage, TaskMessagePart,
};
use tonic::Code;

fn envelope(task_id: &str) -> TaskEnvelope {
    TaskEnvelope {
        task_id: Some(TaskId {
            value: task_id.to_string(),
        }),
        correlation_id: None,
        idempotency_key: None,
        status: 0,
        messages: Vec::new(),
        metadata: Default::default(),
    }
}

fn sized_envelope(task_id: &str, raw_len: usize) -> TaskEnvelope {
    TaskEnvelope {
        task_id: Some(TaskId {
            value: task_id.to_string(),
        }),
        correlation_id: None,
        idempotency_key: None,
        status: 0,
        messages: vec![TaskMessage {
            parts: vec![TaskMessagePart {
                media_type: "application/octet-stream".to_string(),
                text: String::new(),
                raw: vec![b'x'; raw_len],
                metadata: Default::default(),
            }],
            metadata: Default::default(),
        }],
        metadata: Default::default(),
    }
}

async fn harness_with_limits(limits: LimitsConfig) -> RpcTestHarness {
    let data_dir = tempfile::tempdir()
        .unwrap()
        .keep()
        .join("backpressure-keryx-home");
    let config = KeryxDaemonConfig::new(data_dir, 42).with_limits(limits);
    RpcTestHarness::start_with_config(config).await
}

#[tokio::test]
async fn submit_rejects_when_pending_queue_is_full() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 1,
        max_envelope_bytes: 0,
        ..LimitsConfig::unlimited()
    })
    .await;

    harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope("queue-full-1")),
        })
        .await
        .unwrap();
    let error = harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope("queue-full-2")),
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("pending_tasks"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submits_do_not_overshoot_pending_queue_limit() {
    let harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 1,
        max_envelope_bytes: 0,
        ..LimitsConfig::unlimited()
    })
    .await;

    let mut joins = Vec::new();
    for index in 0..16 {
        let mut client = harness.client.clone();
        joins.push(tokio::spawn(async move {
            client
                .submit_task(SubmitTaskRequest {
                    envelope: Some(envelope(&format!("race-{index}"))),
                })
                .await
                .map(|_| ())
                .map_err(|error| error.code())
        }));
    }

    let mut accepted = 0;
    let mut rejected = 0;
    for join in joins {
        match join.await.unwrap() {
            Ok(()) => accepted += 1,
            Err(Code::ResourceExhausted) => rejected += 1,
            Err(code) => panic!("unexpected submit error code: {code:?}"),
        }
    }

    assert_eq!(accepted, 1);
    assert_eq!(rejected, 15);
    assert_eq!(
        harness
            .runtime
            .store()
            .count_tasks_by_status(TaskStatus::Pending)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn submit_rejects_when_envelope_is_too_large() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 0,
        max_envelope_bytes: 1,
        ..LimitsConfig::unlimited()
    })
    .await;

    let error = harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope("too-large-envelope")),
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("envelope_bytes"));
}

#[tokio::test]
async fn submit_rejects_when_retained_envelope_bytes_are_full_after_completion() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 1,
        max_envelope_bytes: 1024,
        max_retained_envelope_bytes: 700,
    })
    .await;

    for task_id in ["retained-full-1", "retained-full-2"] {
        harness
            .client
            .submit_task(SubmitTaskRequest {
                envelope: Some(sized_envelope(task_id, 256)),
            })
            .await
            .unwrap();
        let claim = harness
            .client
            .claim_task(ClaimTaskRequest {
                task_id: Some(TaskId {
                    value: task_id.to_string(),
                }),
                worker_id: Some(AgentId {
                    value: "worker-retained".to_string(),
                }),
                lease_duration_ms: 60_000,
            })
            .await
            .unwrap()
            .into_inner();
        harness
            .client
            .complete_task(CompleteTaskRequest {
                task_id: Some(TaskId {
                    value: task_id.to_string(),
                }),
                lease_id: claim.lease_id,
                worker_id: Some(AgentId {
                    value: "worker-retained".to_string(),
                }),
                duration_ms: 0,
                result_metadata: Default::default(),
                output_artifacts: vec![],
            })
            .await
            .unwrap();
    }

    assert_eq!(
        harness
            .runtime
            .store()
            .count_tasks_by_status(TaskStatus::Pending)
            .await
            .unwrap(),
        0
    );
    let error = harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(sized_envelope("retained-full-3", 256)),
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("retained_envelope_bytes"));
}

#[tokio::test]
async fn local_send_task_respects_pending_queue_limit() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 1,
        max_envelope_bytes: 0,
        ..LimitsConfig::unlimited()
    })
    .await;
    let local_peer = harness.runtime.config().local_peer_id().to_string();

    harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope("local-send-full-1")),
        })
        .await
        .unwrap();
    let error = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: local_peer,
            envelope: Some(envelope("local-send-full-2")),
            timeout_ms: 0,
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("pending_tasks"));
    assert_eq!(
        harness
            .runtime
            .store()
            .count_tasks_by_status(TaskStatus::Pending)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn local_send_task_respects_envelope_size_limit() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 0,
        max_envelope_bytes: 1,
        ..LimitsConfig::unlimited()
    })
    .await;
    let local_peer = harness.runtime.config().local_peer_id().to_string();

    let error = harness
        .client
        .send_task(SendTaskRequest {
            target_peer_id: local_peer,
            envelope: Some(envelope("local-send-too-large")),
            timeout_ms: 0,
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("envelope_bytes"));
}

#[tokio::test]
async fn incoming_relay_task_respects_pending_queue_limit() {
    let harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 1,
        max_envelope_bytes: 0,
        ..LimitsConfig::unlimited()
    })
    .await;
    let allowlist = StaticSenderAllowlist::new().with_nodes(["node-trusted"]);

    let first = handle_incoming_task(
        harness.runtime.as_ref(),
        &allowlist,
        &IncomingDispatchConfig::default(),
        IncomingRelayTask::new("frame-1", "node-trusted", envelope("incoming-full-1")),
    )
    .await;
    assert!(matches!(first, IncomingHandleResult::Accepted { .. }));

    let second = handle_incoming_task(
        harness.runtime.as_ref(),
        &allowlist,
        &IncomingDispatchConfig::default(),
        IncomingRelayTask::new("frame-2", "node-trusted", envelope("incoming-full-2")),
    )
    .await;
    match second {
        IncomingHandleResult::Store(error) => {
            assert!(error.to_string().contains("pending_tasks"));
        }
        other => panic!("expected pending limit store error, got {other:?}"),
    }
    assert_eq!(
        harness
            .runtime
            .store()
            .count_tasks_by_status(TaskStatus::Pending)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn incoming_relay_task_respects_envelope_size_limit() {
    let harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 0,
        max_envelope_bytes: 1,
        ..LimitsConfig::unlimited()
    })
    .await;
    let allowlist = StaticSenderAllowlist::new().with_nodes(["node-trusted"]);

    let result = handle_incoming_task(
        harness.runtime.as_ref(),
        &allowlist,
        &IncomingDispatchConfig::default(),
        IncomingRelayTask::new("frame-1", "node-trusted", envelope("incoming-too-large")),
    )
    .await;
    match result {
        IncomingHandleResult::Store(error) => {
            assert!(error.to_string().contains("envelope_bytes"));
        }
        other => panic!("expected envelope limit store error, got {other:?}"),
    }
}

#[tokio::test]
async fn status_reports_configured_limits_and_current_pending_count() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 2,
        max_envelope_bytes: 512,
        ..LimitsConfig::unlimited()
    })
    .await;
    harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope("status-limit-1")),
        })
        .await
        .unwrap();

    let status = harness
        .client
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(status.max_pending_tasks, 2);
    assert_eq!(status.max_envelope_bytes, 512);
    assert_eq!(status.current_pending_tasks, Some(1));
    assert!(status.warnings.is_empty());
}

#[tokio::test]
async fn doctor_reports_limit_usage_and_fails_when_at_capacity() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 1,
        max_envelope_bytes: 1024,
        ..LimitsConfig::unlimited()
    })
    .await;
    harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope("doctor-limit-1")),
        })
        .await
        .unwrap();

    let doctor = harness
        .client
        .doctor(DoctorRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(doctor.status, "fail");
    assert!(doctor.messages.iter().any(|message| {
        message.contains("[fail] limits")
            && message.contains("pending_tasks=1/1")
            && message.contains("envelope_bytes_limit=1024")
    }));
}

#[tokio::test]
async fn status_and_doctor_report_unknown_pending_count_after_count_failure() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 2,
        max_envelope_bytes: 512,
        ..LimitsConfig::unlimited()
    })
    .await;
    harness.runtime.store().close().await;

    let status = harness
        .client
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.current_pending_tasks, None);
    assert!(status
        .warnings
        .iter()
        .any(|warning| warning.contains("pending task count unavailable")));

    let doctor = harness
        .client
        .doctor(DoctorRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(doctor.status, "fail");
    assert!(doctor.messages.iter().any(|message| {
        message.contains("[fail] limits")
            && message.contains("pending_tasks=unknown/2")
            && message.contains("pending task count unavailable")
    }));
}
