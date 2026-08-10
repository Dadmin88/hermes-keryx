mod common;

use common::RpcTestHarness;
use keryx_proto::v1::{
    AgentId, CancelTaskRequest, ClaimTaskRequest, CompleteTaskRequest, SubmitTaskRequest,
    TaskEnvelope, TaskId,
};
use tonic::Code;

#[tokio::test]
async fn cancel_running_task_requires_active_lease_owner() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "task-cancel-owner-1".to_string(),
    };
    let worker_id = AgentId {
        value: "worker-cancel-owner".to_string(),
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
            worker_id: Some(worker_id.clone()),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    let lease_id = claim.lease_id.clone().unwrap();

    let unauthenticated_cancel = harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(task_id.clone()),
            reason: "attacker does not own the lease".to_string(),
            metadata: Default::default(),
            lease_id: None,
            worker_id: None,
        })
        .await
        .unwrap_err();

    assert_eq!(unauthenticated_cancel.code(), Code::NotFound);

    let wrong_worker_cancel = harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(task_id.clone()),
            reason: "wrong worker".to_string(),
            metadata: Default::default(),
            lease_id: Some(lease_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-attacker".to_string(),
            }),
        })
        .await
        .unwrap_err();

    assert_eq!(wrong_worker_cancel.code(), Code::PermissionDenied);

    let canceled = harness
        .client
        .cancel_task(CancelTaskRequest {
            task_id: Some(task_id.clone()),
            reason: "owner requested".to_string(),
            metadata: Default::default(),
            lease_id: Some(lease_id.clone()),
            worker_id: Some(worker_id.clone()),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(canceled.status, "failed");
    assert!(canceled.canceled);

    let completion_after_cancel = harness
        .client
        .complete_task(CompleteTaskRequest {
            task_id: Some(task_id),
            lease_id: Some(lease_id),
            worker_id: Some(worker_id),
            duration_ms: 1,
            result_metadata: Default::default(),
            output_artifacts: vec![],
        })
        .await
        .unwrap_err();

    assert_eq!(completion_after_cancel.code(), Code::NotFound);
}
