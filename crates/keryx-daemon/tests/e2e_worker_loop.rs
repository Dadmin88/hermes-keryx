use std::sync::Arc;
use std::time::Duration;

use keryx_core::TaskId as CoreTaskId;
use keryx_core::{KeryxEventType, TaskStatus};
use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime, LeaseRecoveryLoop};
use keryx_proto::v1::{
    keryx_daemon_client::KeryxDaemonClient, AgentId, ClaimTaskRequest, CompleteTaskRequest,
    FailTaskRequest, HeartbeatRequest, StatusRequest, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Interceptor;
use tonic::Request;

const TEST_DAEMON_TOKEN: &str = "keryx-worker-loop-test-daemon-token";

#[derive(Clone)]
struct TestDaemonTokenInterceptor;

impl Interceptor for TestDaemonTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {TEST_DAEMON_TOKEN}")
                .parse()
                .expect("static test daemon token is valid metadata"),
        );
        Ok(request)
    }
}

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

#[tokio::test]
async fn full_worker_lifecycle_with_recovery_requeues_abandoned_lease() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("e2e-keryx-home");
    let config = KeryxDaemonConfig::new(data_dir, 0)
        .with_lease_recovery_interval_ms(25)
        .with_fail_retry_policy(keryx_core::RetryPolicy::no_retries())
        .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string()));
    let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());
    let recovery = LeaseRecoveryLoop::spawn(Arc::clone(&runtime));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rpc_runtime = (*runtime).clone();
    let server = tokio::spawn(serve_daemon_rpc(
        rpc_runtime,
        TcpListenerStream::new(listener),
    ));

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = KeryxDaemonClient::with_interceptor(channel, TestDaemonTokenInterceptor);

    for id in ["e2e-task-1", "e2e-task-2", "e2e-task-3"] {
        client
            .submit_task(SubmitTaskRequest {
                envelope: Some(envelope(id)),
            })
            .await
            .unwrap();
    }

    let claim1 = client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "e2e-task-1".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "worker-one".to_string(),
            }),
            lease_duration_ms: 120_000,
        })
        .await
        .unwrap()
        .into_inner();

    let claim2 = client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "e2e-task-2".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "worker-two".to_string(),
            }),
            lease_duration_ms: 120_000,
        })
        .await
        .unwrap()
        .into_inner();

    let claim3 = client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "e2e-task-3".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "worker-three".to_string(),
            }),
            lease_duration_ms: 1,
        })
        .await
        .unwrap()
        .into_inner();

    let lease1 = claim1.lease_id.clone().unwrap();
    client
        .heartbeat(HeartbeatRequest {
            task_id: Some(TaskId {
                value: "e2e-task-1".to_string(),
            }),
            lease_id: Some(lease1.clone()),
            worker_id: Some(AgentId {
                value: "worker-one".to_string(),
            }),
            lease_duration_ms: 120_000,
        })
        .await
        .unwrap();

    let completed = client
        .complete_task(CompleteTaskRequest {
            task_id: Some(TaskId {
                value: "e2e-task-1".to_string(),
            }),
            lease_id: Some(lease1),
            worker_id: Some(AgentId {
                value: "worker-one".to_string(),
            }),
            duration_ms: 42,
            result_metadata: Default::default(),
            output_artifacts: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(completed.status, "completed");

    let failed = client
        .fail_task(FailTaskRequest {
            task_id: Some(TaskId {
                value: "e2e-task-2".to_string(),
            }),
            lease_id: claim2.lease_id.clone(),
            worker_id: Some(AgentId {
                value: "worker-two".to_string(),
            }),
            duration_ms: 7,
            error_reason: "simulated worker error".to_string(),
            failure_metadata: Default::default(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(failed.status, "failed");

    tokio::time::sleep(Duration::from_millis(150)).await;

    let status = client.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.status, "ready");
    assert!(status.store_ready);

    let id1 = CoreTaskId::new("e2e-task-1").unwrap();
    let id2 = CoreTaskId::new("e2e-task-2").unwrap();
    let id3 = CoreTaskId::new("e2e-task-3").unwrap();

    assert_eq!(
        runtime.store().get_task(&id1).await.unwrap().status,
        TaskStatus::Completed
    );
    assert_eq!(
        runtime.store().get_task(&id2).await.unwrap().status,
        TaskStatus::Failed
    );
    assert_eq!(
        runtime.store().get_task(&id3).await.unwrap().status,
        TaskStatus::Pending
    );
    assert!(runtime.store().active_lease(&id3).await.unwrap().is_none());

    let events = runtime.store().events_for_task(&id3).await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.event_type == KeryxEventType::RecoveryAction),
        "task 3 should have RecoveryAction after lease expiry"
    );

    let reclaim = client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "e2e-task-3".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "worker-four".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reclaim.status, "running");
    assert_ne!(
        reclaim.lease_id.unwrap().value,
        claim3.lease_id.unwrap().value
    );

    recovery.shutdown().await;
    server.abort();
}
