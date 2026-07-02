use keryx_core::{AgentId, IdempotencyKey, KeryxEventType, LeaseId, TaskId, TaskStatus};
use keryx_daemon::{KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_store::{LeaseRecord, SqliteStore, StoreError, TaskRecord, CURRENT_SCHEMA_VERSION};
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};
use std::str::FromStr;
use tempfile::tempdir;

fn task(id: &str, idem: &str) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        TaskStatus::Pending,
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
        .lease_task(
            record.task_id(),
            LeaseRecord::new(
                LeaseId::new("daemon-lease-1").unwrap(),
                record.task_id().clone(),
                AgentId::new("daemon-worker-1").unwrap(),
                100,
                500,
            ),
        )
        .await
        .unwrap();

    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 501))
        .await
        .unwrap();

    assert_eq!(runtime.report().schema_version, CURRENT_SCHEMA_VERSION);
    assert_eq!(
        runtime.report().supported_schema_version,
        CURRENT_SCHEMA_VERSION
    );
    assert_eq!(runtime.report().db_path, db_path);
    assert_eq!(runtime.report().recovery.recovered_task_count(), 1);
    assert_eq!(runtime.report().recovery.cleaned_terminal_leases, 0);
    assert_eq!(runtime.report().recovery.corruption_count(), 0);
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

    assert_eq!(runtime.report().schema_version, CURRENT_SCHEMA_VERSION);
    assert_eq!(
        runtime.report().supported_schema_version,
        CURRENT_SCHEMA_VERSION
    );
    assert_eq!(runtime.report().recovery.recovered_task_count(), 0);
    assert_eq!(runtime.report().recovery.cleaned_terminal_leases, 0);
    assert_eq!(runtime.report().recovery.corruption_count(), 0);
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
    assert_eq!(status.schema_version, CURRENT_SCHEMA_VERSION);
    assert_eq!(status.supported_schema_version, CURRENT_SCHEMA_VERSION);
    assert_eq!(status.recovered_tasks, 0);
    assert_eq!(status.cleaned_terminal_leases, 0);
    assert_eq!(status.corruption_count, 0);
    assert_eq!(
        status.startup_recovery_duration_ms,
        runtime.report().startup_recovery_duration_ms
    );
    assert!(status.store.ready);
    assert_eq!(status.store.kind, "sqlite");
    assert_eq!(status.store.path, data_dir.join("keryx.db"));
}

#[tokio::test]
async fn runtime_status_report_counts_terminal_stale_lease_cleanup() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("terminal-cleanup-status-home");
    let db_path = data_dir.join("keryx.db");

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let record = task(
        "daemon-terminal-cleanup-task",
        "daemon-terminal-cleanup-idem",
    );
    store.accept_task(record.clone()).await.unwrap();
    let lease = LeaseRecord::new(
        LeaseId::new("daemon-terminal-cleanup-lease").unwrap(),
        record.task_id().clone(),
        AgentId::new("daemon-terminal-cleanup-worker").unwrap(),
        100,
        500,
    );
    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    store
        .complete_task(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
        )
        .await
        .unwrap();

    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("UPDATE leases SET active = 1, expires_at_ms = 500 WHERE task_id = ?")
        .bind(record.task_id().as_str())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    drop(store);

    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir, 501))
        .await
        .unwrap();
    let status = runtime.status_report();
    let doctor = runtime.doctor_report();

    assert_eq!(runtime.report().recovery.recovered_task_count(), 0);
    assert_eq!(runtime.report().recovery.cleaned_terminal_leases, 1);
    assert_eq!(runtime.report().recovery.corruption_count(), 0);
    assert_eq!(status.recovered_tasks, 0);
    assert_eq!(status.cleaned_terminal_leases, 1);
    assert_eq!(status.corruption_count, 0);
    assert_eq!(status.supported_schema_version, CURRENT_SCHEMA_VERSION);
    assert!(doctor.healthy);
    assert!(doctor.checks.iter().any(|check| {
        check.name == "startup_recovery"
            && check.ready
            && check.detail.contains("cleaned_terminal_leases=1")
    }));
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
        check.name == "sqlite_store" && check.ready && check.detail.contains("kind=sqlite")
    }));
    assert!(doctor.checks.iter().any(|check| {
        check.name == "schema_version"
            && check.ready
            && check
                .detail
                .contains(&format!("schema_version={CURRENT_SCHEMA_VERSION}"))
    }));
    assert!(doctor.checks.iter().any(|check| {
        check.name == "startup_recovery"
            && check.ready
            && check.detail.contains("recovered_tasks=0")
            && check.detail.contains("cleaned_terminal_leases=0")
            && check.detail.contains("corruption_count=0")
            && check.detail.contains("duration_ms=")
    }));
}

#[tokio::test]
async fn startup_returns_typed_error_when_startup_recovery_detects_corruption() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("corrupt-startup-home");
    let db_path = data_dir.join("keryx.db");

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let record = task("daemon-corrupt-task-1", "daemon-corrupt-idem-1");
    store.accept_task(record.clone()).await.unwrap();

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

    let err = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 1))
        .await
        .unwrap_err();

    assert_eq!(
        err,
        StoreError::UnrepairedCorruption {
            corrupted_tasks: vec![record.task_id().clone()]
        }
    );
}

#[tokio::test]
async fn startup_returns_typed_error_for_unsupported_schema() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("unsupported-schema-home");
    let db_path = data_dir.join("keryx.db");

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("INSERT INTO schema_migrations (version, name) VALUES (?, 'future')")
        .bind(CURRENT_SCHEMA_VERSION + 1)
        .execute(&pool)
        .await
        .unwrap();

    let err = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir, 1))
        .await
        .unwrap_err();

    assert_eq!(
        err,
        StoreError::UnsupportedSchema {
            found_version: CURRENT_SCHEMA_VERSION + 1,
            supported_version: CURRENT_SCHEMA_VERSION,
        }
    );
}

#[tokio::test]
async fn startup_returns_typed_error_for_migration_failure() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("migration-failure-home");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("keryx.db");

    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query(
        "CREATE VIEW schema_migrations AS SELECT 1 AS version, 'bad' AS name, CURRENT_TIMESTAMP AS applied_at",
    )
    .execute(&pool)
    .await
    .unwrap();

    let err = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir, 1))
        .await
        .unwrap_err();

    match err {
        StoreError::MigrationFailed(message) => {
            assert!(message.contains("schema_migrations"));
        }
        other => panic!("expected migration failure, got {other:?}"),
    }
}
