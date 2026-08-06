mod common;

use common::RpcTestHarness;
use keryx_core::{
    AgentId as CoreAgentId, IdempotencyKey, LeaseId, PeerId, RetryPolicy, TaskId as CoreTaskId,
    TaskStatus,
};
use keryx_proto::v1::{
    AgentId, CancelTaskRequest, ClaimTaskRequest, CompleteTaskRequest, GetTaskResultRequest,
    SubmitTaskRequest, TaskEnvelope, TaskId, TerminalOutcome,
};
use keryx_store::{LeaseRecord, TaskRecord, TerminalResultRecord};
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
async fn pre_v7_terminal_row_without_durable_result_stays_terminal_and_signals_unavailable() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = CoreTaskId::new("legacy-terminal-without-result").unwrap();
    harness
        .runtime
        .store()
        .accept_task(TaskRecord::new(task_id.clone(), TaskStatus::Pending, None))
        .await
        .unwrap();
    harness
        .runtime
        .store()
        .transition_task(&task_id, TaskStatus::Running)
        .await
        .unwrap();
    harness
        .runtime
        .store()
        .transition_task(&task_id, TaskStatus::Completed)
        .await
        .unwrap();

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
