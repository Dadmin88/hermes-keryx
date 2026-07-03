use std::sync::Arc;
use std::time::Duration;

use keryx_core::{KeryxEventType, TaskStatus};
use keryx_daemon::{KeryxDaemonConfig, KeryxDaemonRuntime, LeaseRecoveryLoop};
use keryx_proto::v1::{AgentId, ClaimTaskRequest, SubmitTaskRequest, TaskEnvelope, TaskId};
use keryx_store::SqliteStore;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use keryx_core::{AgentId as CoreAgentId, IdempotencyKey, LeaseId};
use keryx_daemon::serve_daemon_rpc;
use keryx_store::{LeaseRecord, TaskRecord};

fn task(id: &str, idem: &str) -> TaskRecord {
    TaskRecord::new(
        keryx_core::TaskId::new(id).unwrap(),
        TaskStatus::Pending,
        Some(IdempotencyKey::new(idem).unwrap()),
    )
}

#[tokio::test]
async fn recovery_loop_returns_expired_lease_task_to_pending_and_logs_event() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("loop-keryx-home");
    let db_path = data_dir.join("keryx.db");

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let record = task("loop-recovery-task-1", "loop-recovery-idem-1");
    store.accept_task(record.clone()).await.unwrap();
    store
        .lease_task(
            record.task_id(),
            LeaseRecord::new(
                LeaseId::new("loop-lease-1").unwrap(),
                record.task_id().clone(),
                CoreAgentId::new("loop-worker-1").unwrap(),
                500,
                2_000,
            ),
        )
        .await
        .unwrap();

    let config = KeryxDaemonConfig::new(data_dir, 1_000).with_lease_recovery_interval_ms(25);
    let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());
    assert_eq!(
        runtime
            .store()
            .get_task(record.task_id())
            .await
            .unwrap()
            .status,
        TaskStatus::Running
    );

    let handle = LeaseRecoveryLoop::spawn(Arc::clone(&runtime));
    tokio::time::sleep(Duration::from_millis(150)).await;
    handle.shutdown().await;

    assert_eq!(
        runtime
            .store()
            .get_task(record.task_id())
            .await
            .unwrap()
            .status,
        TaskStatus::Pending
    );
    assert!(runtime
        .store()
        .active_lease(record.task_id())
        .await
        .unwrap()
        .is_none());

    let events = runtime
        .store()
        .events_for_task(record.task_id())
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.event_type == KeryxEventType::RecoveryAction),
        "expected RecoveryAction in event log, got: {events:?}"
    );
}

#[tokio::test]
async fn recovery_loop_recovers_rpc_claimed_task_after_lease_expires() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("loop-rpc-home");
    let config = KeryxDaemonConfig::new(data_dir, 0).with_lease_recovery_interval_ms(20);
    let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());
    let recovery = LeaseRecoveryLoop::spawn(Arc::clone(&runtime));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rpc_runtime = (*runtime).clone();
    let server = tokio::spawn(serve_daemon_rpc(
        rpc_runtime,
        TcpListenerStream::new(listener),
    ));

    let mut client =
        keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient::connect(format!("http://{addr}"))
            .await
            .unwrap();

    let task_id = TaskId {
        value: "loop-rpc-task-1".to_string(),
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

    client
        .claim_task(ClaimTaskRequest {
            task_id: Some(task_id.clone()),
            worker_id: Some(AgentId {
                value: "rpc-worker".to_string(),
            }),
            lease_duration_ms: 1,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(120)).await;
    recovery.shutdown().await;
    server.abort();

    let core_task_id = keryx_core::TaskId::new(task_id.value).unwrap();
    assert_eq!(
        runtime
            .store()
            .get_task(&core_task_id)
            .await
            .unwrap()
            .status,
        TaskStatus::Pending
    );
    let events = runtime
        .store()
        .events_for_task(&core_task_id)
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.event_type == KeryxEventType::RecoveryAction));
}

#[tokio::test]
async fn recovery_loop_shuts_down_gracefully() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("loop-shutdown-home");
    let config = KeryxDaemonConfig::new(data_dir, 1).with_lease_recovery_interval_ms(10);
    let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());
    let handle = LeaseRecoveryLoop::spawn(runtime);
    handle.shutdown().await;
}
