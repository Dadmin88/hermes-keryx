mod common;

use common::RpcTestHarness;
use keryx_core::{
    AgentId as CoreAgentId, IdempotencyKey, LeaseId, PeerId, RetryPolicy, TaskId as CoreTaskId,
    TaskStatus,
};
use keryx_daemon::KeryxDaemonConfig;
use keryx_proto::v1::{
    AckResultDeliveryRequest, AgentId, CancelTaskRequest, ClaimNextResultDeliveryRequest,
    ClaimTaskRequest, CompleteTaskRequest, GetTaskResultRequest, SubmitTaskRequest, TaskEnvelope,
    TaskId, TerminalOutcome,
};
use keryx_store::{
    LeaseRecord, SqliteStore, StoreError, TaskEnvelopeRecord, TaskRecord,
    TaskTransportContextRecord, TerminalResultRecord,
};
use prost::Message;
use tonic::Code;

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

async fn submit(harness: &mut RpcTestHarness, task_id: &str) {
    harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope(task_id)),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn genuine_v6_terminal_row_migrates_without_fabricated_result() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("keryx.db");
    let task_id = CoreTaskId::new("legacy-terminal-without-result").unwrap();

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    store
        .accept_task(TaskRecord::new(task_id.clone(), TaskStatus::Pending, None))
        .await
        .unwrap();
    store
        .transition_task(&task_id, TaskStatus::Running)
        .await
        .unwrap();
    store
        .transition_task(&task_id, TaskStatus::Completed)
        .await
        .unwrap();
    store.close().await;

    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    for statement in [
        "DROP TABLE result_outbox",
        "DROP TABLE task_terminal_results",
        "DROP TABLE task_transport_context",
        "DELETE FROM schema_migrations WHERE version = 7",
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    let version: i64 = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(version, 6);
    pool.close().await;

    let mut harness = RpcTestHarness::start_with_config(
        KeryxDaemonConfig::new(dir.path(), 1)
            .with_local_peer_id(PeerId::new("peer-local").unwrap()),
    )
    .await;
    assert_eq!(harness.runtime.store().schema_version().await.unwrap(), 7);
    assert!(matches!(
        harness.runtime.store().get_terminal_result(&task_id).await,
        Err(StoreError::TerminalResultNotFound(_))
    ));

    let response = harness
        .client
        .get_task_result(GetTaskResultRequest {
            task_id: Some(TaskId {
                value: task_id.as_str().to_string(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!response.found);
    assert_eq!(response.status, "completed");
    assert!(response.terminal_result_unavailable);
    assert_eq!(
        response.data_unavailable_reason,
        "terminal_result_unavailable"
    );
}

#[tokio::test]
async fn remote_target_cancel_fails_closed_without_mutating_origin_state() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = CoreTaskId::new("cancel-remote-target").unwrap();
    let target = PeerId::new("peer-remote").unwrap();
    harness
        .runtime
        .store()
        .accept_task_with_envelope_and_context(
            TaskRecord::new(task_id.clone(), TaskStatus::Pending, None),
            TaskEnvelopeRecord::new(
                task_id.clone(),
                envelope(task_id.as_str()).encode_to_vec(),
                1,
            ),
            TaskTransportContextRecord {
                task_id: task_id.clone(),
                authenticated_sender_peer_id: Some(
                    harness.runtime.config().local_peer_id().clone(),
                ),
                expected_executor_peer_id: Some(target.clone()),
                destination_peer_id: target,
                relay_frame_id: Some("relay-remote-cancel".to_string()),
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();

    let error = harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(TaskId {
                value: task_id.as_str().to_string(),
            }),
            reason: "must not claim remote cancellation".to_string(),
            metadata: Default::default(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert_eq!(
        harness
            .runtime
            .store()
            .get_task(&task_id)
            .await
            .unwrap()
            .status,
        TaskStatus::Pending
    );
    assert!(matches!(
        harness.runtime.store().get_terminal_result(&task_id).await,
        Err(StoreError::TerminalResultNotFound(_))
    ));
}

#[tokio::test]
async fn destination_cancel_emits_one_idempotent_canceled_result_delivery() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = CoreTaskId::new("cancel-remote-origin").unwrap();
    let origin = PeerId::new("peer-origin").unwrap();
    let local = harness.runtime.config().local_peer_id().clone();
    harness
        .runtime
        .store()
        .accept_task_with_envelope_and_context(
            TaskRecord::new(task_id.clone(), TaskStatus::Pending, None),
            TaskEnvelopeRecord::new(
                task_id.clone(),
                envelope(task_id.as_str()).encode_to_vec(),
                1,
            ),
            TaskTransportContextRecord {
                task_id: task_id.clone(),
                authenticated_sender_peer_id: Some(origin.clone()),
                expected_executor_peer_id: Some(local.clone()),
                destination_peer_id: local,
                relay_frame_id: Some("relay-cancel-origin".to_string()),
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();

    let first = harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(TaskId {
                value: task_id.to_string(),
            }),
            reason: "first durable reason".to_string(),
            metadata: [("request".to_string(), "first".to_string())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.reason, "first durable reason");

    let delivery = harness
        .client
        .claim_next_result_delivery(ClaimNextResultDeliveryRequest {
            worker_id: "cancel-delivery-worker".to_string(),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(delivery.has_delivery);
    assert_eq!(delivery.target_peer_id, origin.as_str());
    let result = delivery.result.unwrap();
    assert_eq!(result.outcome, TerminalOutcome::Canceled as i32);
    assert_eq!(result.error_reason, "first durable reason");
    assert!(
        harness
            .client
            .ack_result_delivery(AckResultDeliveryRequest {
                delivery_id: delivery.delivery_id,
                worker_id: "cancel-delivery-worker".to_string(),
            })
            .await
            .unwrap()
            .into_inner()
            .accepted
    );

    let duplicate = harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(TaskId {
                value: task_id.to_string(),
            }),
            reason: "second reason must not replace the first".to_string(),
            metadata: [("request".to_string(), "second".to_string())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(duplicate.canceled);
    assert_eq!(duplicate.reason, "first durable reason");

    let no_duplicate = harness
        .client
        .claim_next_result_delivery(ClaimNextResultDeliveryRequest {
            worker_id: "cancel-delivery-worker".to_string(),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!no_duplicate.has_delivery);
}

#[tokio::test]
async fn pending_cancel_persists_canceled_terminal_result() {
    let mut harness = RpcTestHarness::start().await;
    submit(&mut harness, "cancel-pending").await;

    let canceled = harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(TaskId {
                value: "cancel-pending".to_string(),
            }),
            reason: "operator request".to_string(),
            metadata: Default::default(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(canceled.canceled);
    assert_eq!(canceled.status, "canceled");

    let reopened = harness
        .client
        .get_task_result(GetTaskResultRequest {
            task_id: Some(TaskId {
                value: "cancel-pending".to_string(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(reopened.found);
    assert_eq!(reopened.status, "canceled");
    assert_eq!(
        reopened.result.unwrap().outcome,
        TerminalOutcome::Canceled as i32
    );
}

#[tokio::test]
async fn running_cancel_persists_canceled_terminal_result() {
    let mut harness = RpcTestHarness::start().await;
    submit(&mut harness, "cancel-running").await;
    harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "cancel-running".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "cancel-worker".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap();

    harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(TaskId {
                value: "cancel-running".to_string(),
            }),
            reason: "operator request".to_string(),
            metadata: Default::default(),
        })
        .await
        .unwrap();
    let reopened = harness
        .client
        .get_task_result(GetTaskResultRequest {
            task_id: Some(TaskId {
                value: "cancel-running".to_string(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reopened.status, "canceled");
    assert_eq!(
        reopened.result.unwrap().outcome,
        TerminalOutcome::Canceled as i32
    );
}

#[tokio::test]
async fn terminal_cancel_fails_with_stable_precondition_error() {
    let mut harness = RpcTestHarness::start().await;
    submit(&mut harness, "cancel-terminal").await;
    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "cancel-terminal".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "terminal-worker".to_string(),
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
                value: "cancel-terminal".to_string(),
            }),
            lease_id: claim.lease_id,
            worker_id: Some(AgentId {
                value: "terminal-worker".to_string(),
            }),
            duration_ms: 1,
            result_metadata: Default::default(),
            output_artifacts: vec![],
        })
        .await
        .unwrap();

    let error = harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(TaskId {
                value: "cancel-terminal".to_string(),
            }),
            reason: "too late".to_string(),
            metadata: Default::default(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn rejected_terminal_result_reopens_as_rejected_not_failed() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = CoreTaskId::new("rejected-reopen").unwrap();
    let worker_id = CoreAgentId::new("rejected-worker").unwrap();
    let lease_id = LeaseId::new("rejected-lease").unwrap();
    harness
        .runtime
        .store()
        .accept_task(TaskRecord::new(
            task_id.clone(),
            TaskStatus::Pending,
            Some(IdempotencyKey::new("rejected-idem").unwrap()),
        ))
        .await
        .unwrap();
    harness
        .runtime
        .store()
        .lease_task(
            &task_id,
            LeaseRecord::new(
                lease_id.clone(),
                task_id.clone(),
                worker_id.clone(),
                1,
                9_999_999_999_999,
            ),
        )
        .await
        .unwrap();
    let encoded = keryx_proto::v1::TaskResultEnvelope {
        protocol_version: 2,
        task_id: Some(TaskId {
            value: task_id.to_string(),
        }),
        correlation_id: None,
        outcome: TerminalOutcome::Rejected as i32,
        executor_peer_id: "peer-local".to_string(),
        duration_ms: 0,
        completed_at_ms: 1,
        error_reason: "rejected".to_string(),
        result_metadata: Default::default(),
        output_artifacts: vec![],
    }
    .encode_to_vec();
    harness
        .runtime
        .store()
        .fail_task_with_result(
            &task_id,
            &lease_id,
            &worker_id,
            "rejected",
            &RetryPolicy::no_retries(),
            TerminalResultRecord {
                task_id: task_id.clone(),
                encoded_result: encoded,
                terminal_status: TaskStatus::Failed,
                return_peer_id: None,
                executor_peer_id: PeerId::new("peer-local").unwrap(),
                created_at_ms: 1,
            },
        )
        .await
        .unwrap();

    let reopened = harness
        .client
        .get_task_result(GetTaskResultRequest {
            task_id: Some(TaskId {
                value: task_id.to_string(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reopened.status, "rejected");
    assert_eq!(
        reopened.result.unwrap().outcome,
        TerminalOutcome::Rejected as i32
    );
}
