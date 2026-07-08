mod common;

use common::RpcTestHarness;
use keryx_proto::v1::{
    AgentId, ClaimTaskRequest, FailTaskRequest, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use tonic::Code;

#[tokio::test]
async fn fail_task_via_rpc_clears_lease_and_returns_failure_metadata() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "task-fail-1".to_string(),
    };
    harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(TaskEnvelope {
                task_id: Some(task_id.clone()),
                correlation_id: None,
                idempotency_key: None,
                status: 0,
                messages: vec![],
                metadata: Default::default(),
            }),
        })
        .await
        .unwrap();

    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(task_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-fail".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();

    let lease_id = claim.lease_id.clone().unwrap();
    let mut failure_metadata = std::collections::HashMap::new();
    failure_metadata.insert("exit_code".to_string(), "1".to_string());

    let failed = harness
        .client
        .fail_task(FailTaskRequest {
            task_id: Some(task_id.clone()),
            lease_id: Some(lease_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-fail".to_string(),
            }),
            duration_ms: 9_876,
            error_reason: "worker panic".to_string(),
            failure_metadata: failure_metadata.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(failed.task_id.unwrap().value, task_id.value);
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.duration_ms, 9_876);
    assert_eq!(failed.error_reason, "worker panic");
    assert_eq!(failed.retry_count, 0);
    assert!(!failed.dead_lettered);
    assert_eq!(
        failed.failure_metadata.get("exit_code").map(String::as_str),
        Some("1")
    );

    let second_fail = harness
        .client
        .fail_task(FailTaskRequest {
            task_id: Some(TaskId {
                value: "task-fail-1".to_string(),
            }),
            lease_id: Some(lease_id),
            worker_id: Some(AgentId {
                value: "worker-fail".to_string(),
            }),
            duration_ms: 0,
            error_reason: "again".to_string(),
            failure_metadata: Default::default(),
        })
        .await
        .unwrap_err();

    assert_eq!(second_fail.code(), Code::NotFound);
}
