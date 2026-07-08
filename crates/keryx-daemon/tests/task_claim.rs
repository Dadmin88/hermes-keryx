mod common;

use common::RpcTestHarness;
use keryx_proto::v1::{
    AgentId, ClaimTaskRequest, CompleteTaskRequest, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use tonic::Code;

#[tokio::test]
async fn claim_task_via_rpc_leases_pending_task_as_running() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "task-claim-1".to_string(),
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
                value: "worker-a".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(claim.task_id.unwrap().value, task_id.value);
    assert_eq!(claim.worker_id.unwrap().value, "worker-a");
    assert_eq!(claim.status, "running");
    assert!(!claim.lease_id.unwrap().value.is_empty());
    assert!(claim.expires_at_ms > claim.leased_at_ms);
}

#[tokio::test]
async fn claim_task_via_rpc_rejects_completed_tasks_with_failed_precondition() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "task-claim-completed".to_string(),
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

    let initial_claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(task_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-a".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();

    harness
        .client
        .complete_task(CompleteTaskRequest {
            task_id: Some(task_id.clone()),
            lease_id: initial_claim.lease_id,
            worker_id: Some(AgentId {
                value: "worker-a".to_string(),
            }),
            duration_ms: 0,
            result_metadata: Default::default(),
            output_artifacts: vec![],
        })
        .await
        .unwrap();

    let completed_claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(task_id),
            worker_id: Some(AgentId {
                value: "worker-b".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap_err();

    assert_eq!(completed_claim.code(), Code::FailedPrecondition);
}
