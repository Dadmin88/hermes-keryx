use std::str::FromStr;
use std::sync::Arc;

use keryx_core::{IdempotencyKey, TaskStatus};
use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_proto::v1::{keryx_daemon_client::KeryxDaemonClient, LivenessRequest, ReadinessRequest};
use keryx_store::{SqliteStore, TaskRecord};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

fn task(id: &str, idem: &str) -> TaskRecord {
    TaskRecord::new(
        keryx_core::TaskId::new(id).unwrap(),
        TaskStatus::Pending,
        Some(IdempotencyKey::new(idem).unwrap()),
    )
}

#[tokio::test]
async fn liveness_rpc_is_always_true_while_daemon_is_running() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("liveness-home");
    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(data_dir, 42)
            .with_daemon_rpc_token(Some("keryx-health-test-daemon-token".to_string())),
    )
    .await
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));

    let mut client = KeryxDaemonClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let liveness = client
        .liveness(LivenessRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(liveness.alive);

    server.abort();
}

#[tokio::test]
async fn readiness_rpc_reports_not_ready_after_store_corruption_probe() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("readiness-corrupt-home");
    let db_path = data_dir.join("keryx.db");

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let record = task("health-corrupt-task-1", "health-corrupt-idem-1");
    store.accept_task(record.clone()).await.unwrap();

    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(data_dir.clone(), 1)
                .with_daemon_rpc_token(Some("keryx-health-test-daemon-token".to_string())),
        )
        .await
        .unwrap(),
    );

    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_events WHERE task_id = ?")
        .bind(record.task_id().as_str())
        .execute(&pool)
        .await
        .unwrap();

    runtime.refresh_readiness().await;

    let snapshot = runtime.readiness_snapshot().await;
    assert!(!snapshot.ready);
    assert!(
        snapshot
            .not_ready_reasons
            .iter()
            .any(|reason| reason.contains("corruption") || reason.contains("unrepaired")),
        "expected corruption reason, got {:?}",
        snapshot.not_ready_reasons
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rpc_runtime = (*runtime).clone();
    let server = tokio::spawn(serve_daemon_rpc(
        rpc_runtime,
        TcpListenerStream::new(listener),
    ));

    let mut client = KeryxDaemonClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let readiness = client
        .readiness(ReadinessRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(!readiness.ready);
    assert!(!readiness.not_ready_reasons.is_empty());

    server.abort();
}
