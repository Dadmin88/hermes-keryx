mod common;

use common::RpcTestHarness;
use keryx_core::{LimitsConfig, TaskStatus};
use keryx_daemon::KeryxDaemonConfig;
use keryx_proto::v1::{DoctorRequest, StatusRequest, SubmitTaskRequest, TaskEnvelope, TaskId};
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
async fn status_reports_configured_limits_and_current_pending_count() {
    let mut harness = harness_with_limits(LimitsConfig {
        max_pending_tasks: 2,
        max_envelope_bytes: 512,
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
