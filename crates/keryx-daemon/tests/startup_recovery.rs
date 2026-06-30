use keryx_core::{IdempotencyKey, KeryxEventType, LeaseId, TaskId, TaskStatus};
use keryx_daemon::{KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_store::{LeaseRecord, SqliteStore, TaskRecord};
use tempfile::tempdir;

fn task(id: &str, idem: &str) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        TaskStatus::Accepted,
        Some(IdempotencyKey::new(idem).unwrap()),
    )
}

#[tokio::test]
async fn startup_migrates_sqlite_and_recovers_stale_leases_before_reporting_ready() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("keryx-home");
    let db_path = data_dir.join("keryx.db");

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let record = task("daemon-recovery-task-1", "daemon-recovery-idem-1");
    store.accept_task(record.clone()).await.unwrap();
    store
        .transition_task(record.task_id(), TaskStatus::Queued)
        .await
        .unwrap();
    store
        .lease_task(
            record.task_id(),
            LeaseRecord::new(
                LeaseId::new("daemon-lease-1").unwrap(),
                record.task_id().clone(),
                100,
                500,
            ),
        )
        .await
        .unwrap();

    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 501))
        .await
        .unwrap();

    assert_eq!(runtime.report().schema_version, 1);
    assert_eq!(runtime.report().db_path, db_path);
    assert_eq!(runtime.report().recovered_tasks, 1);
    assert_eq!(
        runtime
            .store()
            .get_task(record.task_id())
            .await
            .unwrap()
            .status,
        TaskStatus::Queued
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
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::RecoveryAction
    );
}

#[tokio::test]
async fn startup_creates_default_database_under_data_dir() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("fresh-keryx-home");

    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 1))
        .await
        .unwrap();

    assert_eq!(runtime.report().schema_version, 1);
    assert_eq!(runtime.report().recovered_tasks, 0);
    assert!(data_dir.join("keryx.db").exists());
}

#[tokio::test]
async fn runtime_status_report_reflects_ready_sqlite_store() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("status-keryx-home");

    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 1234))
        .await
        .unwrap();
    let status = runtime.status_report();

    assert!(status.daemon_ready);
    assert_eq!(status.data_dir, data_dir);
    assert_eq!(status.db_path, data_dir.join("keryx.db"));
    assert_eq!(status.schema_version, 1);
    assert_eq!(status.recovered_tasks, 0);
    assert!(status.store.ready);
    assert_eq!(status.store.kind, "sqlite");
    assert_eq!(status.store.path, data_dir.join("keryx.db"));
}

#[tokio::test]
async fn runtime_doctor_report_marks_runtime_healthy_when_store_ready() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("doctor-keryx-home");

    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 1234))
        .await
        .unwrap();
    let doctor = runtime.doctor_report();

    assert!(doctor.healthy);
    assert!(doctor.status.daemon_ready);
    assert!(doctor.checks.iter().any(|check| {
        check.name == "data_dir" && check.ready && check.detail.contains("doctor-keryx-home")
    }));
    assert!(doctor.checks.iter().any(|check| {
        check.name == "sqlite_store" && check.ready && check.detail.contains("schema_version=1")
    }));
    assert!(doctor.checks.iter().any(|check| {
        check.name == "startup_recovery"
            && check.ready
            && check.detail.contains("recovered_tasks=0")
    }));
}
