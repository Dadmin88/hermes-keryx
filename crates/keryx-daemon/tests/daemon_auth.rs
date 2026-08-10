mod common;

use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient;
use keryx_proto::v1::{
    GetTaskResultRequest, StatusRequest, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request};

const TOKEN: &str = "unified-daemon-auth-test-token";

fn envelope(task_id: &str) -> TaskEnvelope {
    TaskEnvelope {
        task_id: Some(TaskId {
            value: task_id.to_string(),
        }),
        ..TaskEnvelope::default()
    }
}

#[tokio::test]
async fn network_daemon_allows_public_reads_but_denies_sensitive_calls_without_bearer() {
    let dir = tempfile::tempdir().unwrap();
    let config =
        KeryxDaemonConfig::new(dir.path(), 0).with_daemon_rpc_token(Some(TOKEN.to_string()));
    let runtime = KeryxDaemonRuntime::startup(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));

    let mut raw = KeryxDaemonClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    raw.status(StatusRequest {}).await.unwrap();

    let missing = raw
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope("auth-missing")),
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::Unauthenticated);

    let result_read = raw
        .get_task_result(GetTaskResultRequest {
            task_id: Some(TaskId {
                value: "auth-missing".to_string(),
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(result_read.code(), Code::Unauthenticated);

    let mut wrong = Request::new(SubmitTaskRequest {
        envelope: Some(envelope("auth-wrong")),
    });
    wrong
        .metadata_mut()
        .insert("authorization", "Bearer wrong-token".parse().unwrap());
    let wrong = raw.submit_task(wrong).await.unwrap_err();
    assert_eq!(wrong.code(), Code::PermissionDenied);

    let mut authorized = Request::new(SubmitTaskRequest {
        envelope: Some(envelope("auth-ok")),
    });
    authorized
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    raw.submit_task(authorized).await.unwrap();

    server.abort();
}
