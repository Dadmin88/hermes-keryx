mod common;

use common::RpcTestHarness;
use keryx_proto::v1::{SubmitTaskRequest, TaskEnvelope, TaskId};

#[tokio::test]
async fn submit_task_via_rpc_accepts_pending_task() {
    let mut harness = RpcTestHarness::start().await;
    let response = harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(TaskEnvelope {
                task_id: Some(TaskId {
                    value: "task-submit-1".to_string(),
                }),
                correlation_id: None,
                idempotency_key: None,
                status: 0,
                messages: vec![],
                metadata: Default::default(),
                deadline_ms: 0,
            }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.task_id.unwrap().value, "task-submit-1".to_string());
    assert_eq!(response.status, "pending");
}
