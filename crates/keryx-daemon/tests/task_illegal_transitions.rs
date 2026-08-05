mod common;

use common::RpcTestHarness;
use keryx_proto::v1::{
    AgentId, CompleteTaskRequest, FailTaskRequest, LeaseId, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use tonic::Code;

#[tokio::test]
async fn complete_or_fail_on_pending_task_without_lease_is_rejected() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "task-illegal-1".to_string(),
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
                deadline_ms: 0,
            }),
        })
        .await
        .unwrap();

    let bogus_lease = LeaseId {
        value: "lease-not-held".to_string(),
    };
    let worker = AgentId {
        value: "worker-illegal".to_string(),
    };

    let complete_err = harness
        .client
        .complete_task(CompleteTaskRequest {
            task_id: Some(task_id.clone()),
            lease_id: Some(bogus_lease.clone()),
            worker_id: Some(worker.clone()),
            duration_ms: 0,
            result_metadata: Default::default(),
            output_artifacts: vec![],
        })
        .await
        .unwrap_err();

    assert_eq!(complete_err.code(), Code::NotFound);

    let fail_err = harness
        .client
        .fail_task(FailTaskRequest {
            task_id: Some(task_id),
            lease_id: Some(bogus_lease),
            worker_id: Some(worker),
            duration_ms: 0,
            error_reason: "should not apply".to_string(),
            failure_metadata: Default::default(),
        })
        .await
        .unwrap_err();

    assert_eq!(fail_err.code(), Code::NotFound);
}
