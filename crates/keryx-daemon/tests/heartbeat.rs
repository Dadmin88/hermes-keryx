mod common;

use common::RpcTestHarness;
use keryx_proto::v1::{
    AgentId, ClaimTaskRequest, HeartbeatRequest, LeaseId, SubmitTaskRequest, TaskEnvelope, TaskId,
};

#[tokio::test]
async fn heartbeat_via_rpc_renews_active_lease() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "task-heartbeat-1".to_string(),
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

    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(task_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-b".to_string(),
            }),
            lease_duration_ms: 5_000,
        })
        .await
        .unwrap()
        .into_inner();

    let initial_expires = claim.expires_at_ms;
    let lease_id = claim.lease_id.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let heartbeat = harness
        .client
        .heartbeat(HeartbeatRequest {
            task_id: Some(task_id),
            lease_id: Some(LeaseId {
                value: lease_id.value,
            }),
            worker_id: Some(AgentId {
                value: "worker-b".to_string(),
            }),
            lease_duration_ms: 120_000,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(heartbeat.expires_at_ms > initial_expires);
    assert!(!heartbeat.lease_id.unwrap().value.is_empty());
}
