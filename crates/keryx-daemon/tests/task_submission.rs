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
            }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.task_id.unwrap().value, "task-submit-1".to_string());
    assert_eq!(response.status, "pending");
}

#[tokio::test]
async fn submit_task_requires_daemon_rpc_authorization_when_token_configured() {
    use keryx_daemon::KeryxDaemonConfig;
    use tonic::Code;

    let dir = tempfile::tempdir().unwrap();
    let config = KeryxDaemonConfig::new(dir.path().join("auth-keryx-home"), 42)
        .with_daemon_rpc_token(Some("test-token".to_string()));
    let mut harness = common::RpcTestHarness::start_with_config(config).await;

    let unauthenticated = harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(TaskEnvelope {
                task_id: Some(TaskId {
                    value: "task-auth-denied".into(),
                }),
                correlation_id: None,
                idempotency_key: None,
                status: 0,
                messages: Vec::new(),
                metadata: Default::default(),
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    let mut request = tonic::Request::new(SubmitTaskRequest {
        envelope: Some(TaskEnvelope {
            task_id: Some(TaskId {
                value: "task-auth-accepted".into(),
            }),
            correlation_id: None,
            idempotency_key: None,
            status: 0,
            messages: Vec::new(),
            metadata: Default::default(),
        }),
    });
    request
        .metadata_mut()
        .insert("authorization", "Bearer test-token".parse().unwrap());

    let accepted = harness
        .client
        .submit_task(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(accepted.status, "pending");
}
