mod common;

use common::RpcTestHarness;
use keryx_proto::v1::{
    AgentId, ClaimTaskRequest, CompleteTaskRequest, SubmitTaskRequest, TaskArtifact, TaskEnvelope,
    TaskId,
};
use tonic::Code;

#[tokio::test]
async fn complete_task_via_rpc_clears_lease_and_marks_completed() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "task-complete-1".to_string(),
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
                value: "worker-complete".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();

    let lease_id = claim.lease_id.clone().unwrap();
    let mut result_metadata = std::collections::HashMap::new();
    result_metadata.insert("summary".to_string(), "done".to_string());

    let completed = harness
        .client
        .complete_task(CompleteTaskRequest {
            task_id: Some(task_id.clone()),
            lease_id: Some(lease_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-complete".to_string(),
            }),
            duration_ms: 1_234,
            result_metadata: result_metadata.clone(),
            output_artifacts: vec![TaskArtifact {
                path: "/tmp/out.txt".to_string(),
                media_type: "text/plain".to_string(),
                metadata: Default::default(),
            }],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(completed.task_id.unwrap().value, task_id.value);
    assert_eq!(completed.status, "completed");
    assert_eq!(completed.duration_ms, 1_234);
    assert_eq!(
        completed.result_metadata.get("summary").map(String::as_str),
        Some("done")
    );
    assert_eq!(completed.output_artifacts.len(), 1);

    let second_complete = harness
        .client
        .complete_task(CompleteTaskRequest {
            task_id: Some(TaskId {
                value: "task-complete-1".to_string(),
            }),
            lease_id: Some(lease_id),
            worker_id: Some(AgentId {
                value: "worker-complete".to_string(),
            }),
            duration_ms: 0,
            result_metadata: Default::default(),
            output_artifacts: vec![],
        })
        .await
        .unwrap_err();

    assert_eq!(second_complete.code(), Code::NotFound);
}
