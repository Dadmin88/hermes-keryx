use std::sync::Arc;

use keryx_core::{AgentId, TaskId as CoreTaskId, TaskStatus};
use keryx_daemon::{
    handle_incoming_task, IncomingDispatchConfig, IncomingHandleResult, IncomingRelayTask,
    KeryxDaemonConfig, KeryxDaemonRuntime, StaticSenderAllowlist,
};
use keryx_proto::v1::{ClaimTaskRequest, TaskEnvelope, TaskId};
use tokio::sync::mpsc;

mod common;

use common::RpcTestHarness;

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

fn envelope_with_deadline(task_id: &str, deadline_ms: i64) -> TaskEnvelope {
    TaskEnvelope {
        deadline_ms,
        ..envelope(task_id)
    }
}

#[tokio::test]
async fn incoming_task_accepted_into_store() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path().join("incoming-home"), 0))
            .await
            .unwrap(),
    );
    let allowlist = StaticSenderAllowlist::new().with_nodes(["node-trusted"]);
    let incoming =
        IncomingRelayTask::new("frame-accept", "node-trusted", envelope("incoming-task-1"));

    let result = handle_incoming_task(
        runtime.as_ref(),
        &allowlist,
        &IncomingDispatchConfig::default(),
        incoming,
    )
    .await;

    match result {
        IncomingHandleResult::Accepted {
            task_id,
            dispatched,
            lease_id,
        } => {
            assert_eq!(task_id.as_str(), "incoming-task-1");
            assert!(!dispatched);
            assert!(lease_id.is_none());
        }
        other => panic!("expected accepted, got {other:?}"),
    }

    let data_dir = dir.path().join("incoming-home");
    drop(runtime);
    let mut harness = RpcTestHarness::start_with_data_dir(data_dir).await;
    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "incoming-task-1".to_string(),
            }),
            worker_id: Some(keryx_proto::v1::AgentId {
                value: "worker-a".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(claim.status, "running");
}

#[tokio::test]
async fn incoming_task_persists_positive_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path().join("deadline-home"), 0))
            .await
            .unwrap(),
    );
    let allowlist = StaticSenderAllowlist::new().with_nodes(["node-trusted"]);
    let deadline_ms = 1_800_000_000_000;

    let result = handle_incoming_task(
        runtime.as_ref(),
        &allowlist,
        &IncomingDispatchConfig::default(),
        IncomingRelayTask::new(
            "frame-deadline",
            "node-trusted",
            envelope_with_deadline("incoming-task-deadline", deadline_ms),
        ),
    )
    .await;

    assert!(matches!(result, IncomingHandleResult::Accepted { .. }));
    let task_id = CoreTaskId::new("incoming-task-deadline").unwrap();
    assert_eq!(
        runtime
            .store()
            .get_task(&task_id)
            .await
            .unwrap()
            .deadline_ms,
        Some(deadline_ms)
    );
}

#[tokio::test]
async fn incoming_negative_deadline_is_rejected_without_store_write() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(
            dir.path().join("negative-deadline-home"),
            0,
        ))
        .await
        .unwrap(),
    );
    let allowlist = StaticSenderAllowlist::new().with_nodes(["node-trusted"]);

    let result = handle_incoming_task(
        runtime.as_ref(),
        &allowlist,
        &IncomingDispatchConfig::default(),
        IncomingRelayTask::new(
            "frame-negative-deadline",
            "node-trusted",
            envelope_with_deadline("incoming-task-negative-deadline", -1),
        ),
    )
    .await;

    assert!(matches!(result, IncomingHandleResult::InvalidEnvelope(_)));
    let task_id = CoreTaskId::new("incoming-task-negative-deadline").unwrap();
    assert!(runtime.store().get_task(&task_id).await.is_err());
}

#[tokio::test]
async fn incoming_expired_deadline_is_terminalized_before_auto_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(
            dir.path().join("expired-deadline-home"),
            0,
        ))
        .await
        .unwrap(),
    );
    let allowlist = StaticSenderAllowlist::new().with_nodes(["node-trusted"]);
    let dispatch =
        IncomingDispatchConfig::auto_dispatch(AgentId::new("deadline-worker").unwrap(), 60_000);

    let result = handle_incoming_task(
        runtime.as_ref(),
        &allowlist,
        &dispatch,
        IncomingRelayTask::new(
            "frame-expired-deadline",
            "node-trusted",
            envelope_with_deadline("incoming-task-expired-deadline", 1),
        ),
    )
    .await;

    assert!(matches!(
        result,
        IncomingHandleResult::Accepted {
            dispatched: false,
            lease_id: None,
            ..
        }
    ));
    let task_id = CoreTaskId::new("incoming-task-expired-deadline").unwrap();
    assert_eq!(
        runtime.store().get_task(&task_id).await.unwrap().status,
        TaskStatus::Failed
    );
}

#[tokio::test]
async fn incoming_task_rejected_from_non_allowed_sender() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path().join("reject-home"), 0))
            .await
            .unwrap(),
    );
    let allowlist = StaticSenderAllowlist::new().with_nodes(["node-trusted"]);
    let incoming = IncomingRelayTask::new(
        "frame-reject",
        "node-untrusted",
        envelope("incoming-task-reject"),
    );

    let result = handle_incoming_task(
        runtime.as_ref(),
        &allowlist,
        &IncomingDispatchConfig::default(),
        incoming,
    )
    .await;

    assert_eq!(
        result,
        IncomingHandleResult::RejectedSender {
            sender_node_id: "node-untrusted".to_string(),
        }
    );

    let data_dir = dir.path().join("reject-home");
    drop(runtime);
    let mut harness = RpcTestHarness::start_with_data_dir(data_dir).await;
    let claim_err = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "incoming-task-reject".to_string(),
            }),
            worker_id: Some(keryx_proto::v1::AgentId {
                value: "worker-a".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap_err();
    assert_eq!(claim_err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn incoming_task_dispatched_to_local_worker() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("dispatch-home");
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 0))
            .await
            .unwrap(),
    );
    let allowlist = Arc::new(StaticSenderAllowlist::new().with_nodes(["node-trusted"]));
    let worker_id = AgentId::new("relay-local-worker").unwrap();
    let dispatch = IncomingDispatchConfig::auto_dispatch(worker_id.clone(), 120_000);

    let result = handle_incoming_task(
        runtime.as_ref(),
        allowlist.as_ref(),
        &dispatch,
        IncomingRelayTask::new(
            "frame-dispatch",
            "node-trusted",
            envelope("incoming-task-dispatch"),
        ),
    )
    .await;

    match result {
        IncomingHandleResult::Accepted {
            dispatched,
            lease_id,
            ..
        } => {
            assert!(dispatched);
            assert!(lease_id.is_some());
        }
        other => panic!("expected accepted with dispatch, got {other:?}"),
    }

    drop(runtime);
    let mut harness = RpcTestHarness::start_with_data_dir(data_dir).await;
    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "incoming-task-dispatch".to_string(),
            }),
            worker_id: Some(keryx_proto::v1::AgentId {
                value: "other-worker".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap_err();
    assert_eq!(claim.code(), tonic::Code::Aborted);
}

#[tokio::test]
async fn incoming_task_loop_processes_relay_channel() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path().join("loop-home"), 0))
            .await
            .unwrap(),
    );
    let allowlist = Arc::new(StaticSenderAllowlist::new().with_nodes(["node-trusted"]));
    let (tx, rx) = mpsc::channel(4);
    let loop_handle =
        runtime.spawn_incoming_task_loop(allowlist, IncomingDispatchConfig::default(), rx);

    tx.send(IncomingRelayTask::new(
        "frame-loop",
        "node-trusted",
        envelope("incoming-task-loop"),
    ))
    .await
    .unwrap();
    drop(tx);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    loop_handle.shutdown().await;

    let data_dir = dir.path().join("loop-home");
    drop(runtime);
    let mut harness = RpcTestHarness::start_with_data_dir(data_dir).await;
    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "incoming-task-loop".to_string(),
            }),
            worker_id: Some(keryx_proto::v1::AgentId {
                value: "worker-loop".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(claim.status, "running");
}

#[tokio::test]
async fn incoming_invalid_envelope_returns_error_without_store_write() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path().join("invalid-home"), 0))
            .await
            .unwrap(),
    );
    let allowlist = StaticSenderAllowlist::new().with_nodes(["node-trusted"]);
    let incoming = IncomingRelayTask::new(
        "frame-invalid",
        "node-trusted",
        TaskEnvelope {
            task_id: None,
            correlation_id: None,
            idempotency_key: None,
            status: 0,
            messages: vec![],
            metadata: Default::default(),
            deadline_ms: 0,
        },
    );

    let result = handle_incoming_task(
        runtime.as_ref(),
        &allowlist,
        &IncomingDispatchConfig::default(),
        incoming,
    )
    .await;

    assert!(matches!(result, IncomingHandleResult::InvalidEnvelope(_)));
}
