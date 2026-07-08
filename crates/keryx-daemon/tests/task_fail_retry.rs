use keryx_core::RetryPolicy;
use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_proto::v1::{
    AgentId, ClaimTaskRequest, FailTaskRequest, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test]
async fn fail_task_via_rpc_requeues_with_retry_count_until_dead_lettered() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("retry-rpc-home");
    let policy = RetryPolicy {
        max_retries: 1,
        backoff_ms: 0,
        dead_letter_after: 2,
    };
    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(data_dir, 42).with_fail_retry_policy(policy),
    )
    .await
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));

    let mut client =
        keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient::connect(format!("http://{addr}"))
            .await
            .unwrap();

    let task_id = TaskId {
        value: "task-fail-retry".to_string(),
    };
    client
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

    let claim = client
        .claim_task(ClaimTaskRequest {
            task_id: Some(task_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-retry".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(claim.retry_count, 0);

    let first_fail = client
        .fail_task(FailTaskRequest {
            task_id: Some(task_id.clone()),
            lease_id: claim.lease_id,
            worker_id: Some(AgentId {
                value: "worker-retry".to_string(),
            }),
            duration_ms: 1,
            error_reason: "transient".to_string(),
            failure_metadata: Default::default(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_fail.status, "pending");
    assert_eq!(first_fail.retry_count, 1);
    assert!(!first_fail.dead_lettered);

    let reclaim = client
        .claim_task(ClaimTaskRequest {
            task_id: Some(task_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-retry".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reclaim.retry_count, 1);

    let final_fail = client
        .fail_task(FailTaskRequest {
            task_id: Some(task_id),
            lease_id: reclaim.lease_id,
            worker_id: Some(AgentId {
                value: "worker-retry".to_string(),
            }),
            duration_ms: 2,
            error_reason: "still broken".to_string(),
            failure_metadata: Default::default(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(final_fail.status, "failed");
    assert_eq!(final_fail.retry_count, 2);
    assert!(final_fail.dead_lettered);

    server.abort();
}
