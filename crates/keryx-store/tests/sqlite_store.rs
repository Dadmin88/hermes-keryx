use std::str::FromStr;

use keryx_core::{
    AgentId, IdempotencyKey, KeryxEventType, LeaseId, RetryPolicy, TaskId, TaskStatus,
    ValidationError,
};
use keryx_store::{LeaseRecord, SqliteStore, StoreError, TaskRecord};
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};
use tempfile::tempdir;

fn task(id: &str, status: TaskStatus, idem: Option<&str>) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        status,
        idem.map(|key| IdempotencyKey::new(key).unwrap()),
    )
}

fn worker(id: &str) -> AgentId {
    AgentId::new(id).unwrap()
}

fn lease(task_id: &TaskId, lease_id: &str, worker_id: &str, expires_at_ms: i64) -> LeaseRecord {
    LeaseRecord::new(
        LeaseId::new(lease_id).unwrap(),
        task_id.clone(),
        worker(worker_id),
        100,
        expires_at_ms,
    )
}

async fn temp_store() -> SqliteStore {
    let dir = tempdir().unwrap().keep();
    let db_path = dir.join("keryx.db");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    store
}

async fn seed_schema_v1_database_with_active_lease(
    db_path: &std::path::Path,
    task_id: &TaskId,
    lease_id: &LeaseId,
    expires_at_ms: i64,
) {
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO schema_migrations (version, name) VALUES (1, 'initial')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE tasks (task_id TEXT PRIMARY KEY, status TEXT NOT NULL, idempotency_key TEXT UNIQUE, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE task_events (task_id TEXT NOT NULL, sequence INTEGER NOT NULL, event_type TEXT NOT NULL, from_status TEXT, to_status TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (task_id, sequence), FOREIGN KEY(task_id) REFERENCES tasks(task_id))",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE leases (lease_id TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE, leased_at_ms INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1, FOREIGN KEY(task_id) REFERENCES tasks(task_id))",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE idempotency_keys (key TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE, FOREIGN KEY(task_id) REFERENCES tasks(task_id))",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO tasks (task_id, status, idempotency_key) VALUES (?, 'running', 'legacy-idem')",
    )
    .bind(task_id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO idempotency_keys (key, task_id) VALUES ('legacy-idem', ?)")
        .bind(task_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_events (task_id, sequence, event_type, from_status, to_status) VALUES (?, 1, 'task_accepted', NULL, 'pending')",
    )
    .bind(task_id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_events (task_id, sequence, event_type, from_status, to_status) VALUES (?, 2, 'task_started', 'pending', 'running')",
    )
    .bind(task_id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO leases (lease_id, task_id, leased_at_ms, expires_at_ms, active) VALUES (?, ?, 100, ?, 1)",
    )
    .bind(lease_id.as_str())
    .bind(task_id.as_str())
    .bind(expires_at_ms)
    .execute(&pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn sqlite_migration_from_empty_database_creates_schema_version() {
    let store = temp_store().await;

    assert_eq!(store.schema_version().await.unwrap(), 4);
}

#[tokio::test]
async fn sqlite_pending_running_completed_succeeds_via_lease_and_complete() {
    let store = temp_store().await;
    let record = task("sqlite-task-1", TaskStatus::Pending, Some("sqlite-idem-1"));
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(record.task_id(), "sqlite-lease-1", "sqlite-worker-1", 1_000);

    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    let completed = store
        .complete_task(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(completed.status, TaskStatus::Completed);
    assert!(store
        .active_lease(record.task_id())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store.replay_task(record.task_id()).await.unwrap().status,
        TaskStatus::Completed
    );
}

#[tokio::test]
async fn sqlite_count_tasks_by_status_returns_current_counts() {
    let store = temp_store().await;
    let pending = task(
        "sqlite-count-pending",
        TaskStatus::Pending,
        Some("sqlite-count-idem-pending"),
    );
    let running = task(
        "sqlite-count-running",
        TaskStatus::Pending,
        Some("sqlite-count-idem-running"),
    );
    let failed = task(
        "sqlite-count-failed",
        TaskStatus::Pending,
        Some("sqlite-count-idem-failed"),
    );
    store.accept_task(pending).await.unwrap();
    store.accept_task(running.clone()).await.unwrap();
    store.accept_task(failed.clone()).await.unwrap();
    let running_lease = lease(
        running.task_id(),
        "sqlite-count-lease-running",
        "sqlite-count-worker",
        1_000,
    );
    store
        .lease_task(running.task_id(), running_lease)
        .await
        .unwrap();
    let failed_lease = lease(
        failed.task_id(),
        "sqlite-count-lease-failed",
        "sqlite-count-worker",
        1_000,
    );
    store
        .lease_task(failed.task_id(), failed_lease.clone())
        .await
        .unwrap();
    store
        .fail_task(
            failed.task_id(),
            &failed_lease.lease_id,
            failed_lease.worker_id.as_ref().unwrap(),
            "boom",
            &RetryPolicy::no_retries(),
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .count_tasks_by_status(TaskStatus::Pending)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .count_tasks_by_status(TaskStatus::Running)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .count_tasks_by_status(TaskStatus::Failed)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .count_tasks_by_status(TaskStatus::Completed)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_pending_running_failed_succeeds_via_lease_and_fail() {
    let store = temp_store().await;
    let record = task("sqlite-task-2", TaskStatus::Pending, Some("sqlite-idem-2"));
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(record.task_id(), "sqlite-lease-2", "sqlite-worker-2", 1_000);

    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    let failed = store
        .fail_task(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
            "",
            &RetryPolicy::no_retries(),
        )
        .await
        .unwrap();

    assert_eq!(failed.status, TaskStatus::Failed);
    assert!(store
        .active_lease(record.task_id())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sqlite_lease_owner_mismatch_and_missing_owner_are_rejected() {
    let store = temp_store().await;
    let record = task(
        "sqlite-task-owner-1",
        TaskStatus::Pending,
        Some("sqlite-idem-owner-1"),
    );
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(
        record.task_id(),
        "sqlite-lease-owner-1",
        "sqlite-worker-owner-1",
        1_000,
    );

    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();

    assert_eq!(
        store
            .complete_task(
                record.task_id(),
                &lease.lease_id,
                &worker("sqlite-worker-owner-other"),
            )
            .await
            .unwrap_err(),
        StoreError::LeaseOwnerMismatch {
            task_id: record.task_id().clone(),
            worker_id: worker("sqlite-worker-owner-other"),
        }
    );

    let missing_owner_task = task(
        "sqlite-task-owner-2",
        TaskStatus::Pending,
        Some("sqlite-idem-owner-2"),
    );
    store.accept_task(missing_owner_task.clone()).await.unwrap();
    let missing_owner_lease = LeaseRecord {
        lease_id: LeaseId::new("sqlite-lease-owner-2").unwrap(),
        task_id: missing_owner_task.task_id().clone(),
        worker_id: None,
        leased_at_ms: 100,
        expires_at_ms: 1_000,
    };

    assert_eq!(
        store
            .lease_task(missing_owner_task.task_id(), missing_owner_lease.clone())
            .await
            .unwrap_err(),
        StoreError::LeaseOwnerMissing {
            task_id: missing_owner_task.task_id().clone(),
            lease_id: missing_owner_lease.lease_id,
        }
    );
}

#[tokio::test]
async fn sqlite_pending_completed_fails_with_validation_error() {
    let store = temp_store().await;
    let record = task("sqlite-task-3", TaskStatus::Pending, Some("sqlite-idem-3"));
    store.accept_task(record.clone()).await.unwrap();

    let err = store
        .transition_task(record.task_id(), TaskStatus::Completed)
        .await
        .unwrap_err();

    assert_eq!(
        err,
        StoreError::Validation(ValidationError::InvalidTaskTransition {
            from: TaskStatus::Pending,
            to: TaskStatus::Completed,
        })
    );
}

#[tokio::test]
async fn sqlite_completed_and_failed_tasks_are_terminally_immutable() {
    let store = temp_store().await;

    let completed = task("sqlite-task-4", TaskStatus::Pending, Some("sqlite-idem-4"));
    store.accept_task(completed.clone()).await.unwrap();
    let completed_lease = lease(
        completed.task_id(),
        "sqlite-lease-4",
        "sqlite-worker-4",
        1_000,
    );
    store
        .lease_task(completed.task_id(), completed_lease.clone())
        .await
        .unwrap();
    store
        .complete_task(
            completed.task_id(),
            &completed_lease.lease_id,
            completed_lease.worker_id.as_ref().unwrap(),
        )
        .await
        .unwrap();

    for to in [TaskStatus::Pending, TaskStatus::Running, TaskStatus::Failed] {
        assert_eq!(
            store
                .transition_task(completed.task_id(), to)
                .await
                .unwrap_err(),
            StoreError::Validation(ValidationError::TerminalTaskTransition {
                from: TaskStatus::Completed,
                to,
            })
        );
    }

    let failed = task("sqlite-task-5", TaskStatus::Pending, Some("sqlite-idem-5"));
    store.accept_task(failed.clone()).await.unwrap();
    let failed_lease = lease(failed.task_id(), "sqlite-lease-5", "sqlite-worker-5", 1_000);
    store
        .lease_task(failed.task_id(), failed_lease.clone())
        .await
        .unwrap();
    store
        .fail_task(
            failed.task_id(),
            &failed_lease.lease_id,
            failed_lease.worker_id.as_ref().unwrap(),
            "",
            &RetryPolicy::no_retries(),
        )
        .await
        .unwrap();

    for to in [
        TaskStatus::Pending,
        TaskStatus::Running,
        TaskStatus::Completed,
    ] {
        assert_eq!(
            store
                .transition_task(failed.task_id(), to)
                .await
                .unwrap_err(),
            StoreError::Validation(ValidationError::TerminalTaskTransition {
                from: TaskStatus::Failed,
                to,
            })
        );
    }
}

#[tokio::test]
async fn sqlite_idempotency_duplicate_and_conflict_behave_like_memory_store() {
    let store = temp_store().await;
    let original = task("sqlite-task-6", TaskStatus::Pending, Some("sqlite-idem-6"));
    store.accept_task(original.clone()).await.unwrap();

    let duplicate = store.accept_task(original.clone()).await.unwrap();
    assert_eq!(duplicate, original);
    assert_eq!(
        store
            .events_for_task(original.task_id())
            .await
            .unwrap()
            .len(),
        1
    );

    let conflict = store
        .accept_task(task(
            "sqlite-task-conflict",
            TaskStatus::Pending,
            Some("sqlite-idem-6"),
        ))
        .await
        .unwrap_err();
    assert!(matches!(conflict, StoreError::IdempotencyConflict { .. }));
}

#[tokio::test]
async fn sqlite_migration_requeues_legacy_active_leases_without_fabricating_owner() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("legacy-keryx.db");
    let task_id = TaskId::new("legacy-running-task").unwrap();
    let lease_id = LeaseId::new("legacy-running-lease").unwrap();
    seed_schema_v1_database_with_active_lease(&db_path, &task_id, &lease_id, 9_999).await;

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();

    assert_eq!(store.schema_version().await.unwrap(), 4);
    assert_eq!(
        store.get_task(&task_id).await.unwrap().status,
        TaskStatus::Pending
    );
    assert!(store.active_lease(&task_id).await.unwrap().is_none());
    let events = store.events_for_task(&task_id).await.unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::RecoveryAction
    );
    assert_eq!(
        events.last().unwrap().from_status,
        Some(TaskStatus::Running)
    );
    assert_eq!(events.last().unwrap().to_status, TaskStatus::Pending);

    assert_eq!(
        store
            .renew_lease(&task_id, &lease_id, &worker("legacy-worker"), 500, 1_500)
            .await
            .unwrap_err(),
        StoreError::Validation(ValidationError::InvalidTaskTransition {
            from: TaskStatus::Pending,
            to: TaskStatus::Running,
        })
    );

    let new_lease = lease(&task_id, "legacy-new-lease", "legacy-worker", 1_500);
    let leased = store.lease_task(&task_id, new_lease.clone()).await.unwrap();
    assert_eq!(leased.status, TaskStatus::Running);
}

#[tokio::test]
async fn sqlite_replay_and_recovery_report_missing_events_as_corruption() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("corrupt-keryx.db");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();

    let record = task(
        "sqlite-corrupt-task",
        TaskStatus::Pending,
        Some("sqlite-corrupt-idem"),
    );
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

    assert_eq!(
        store.events_for_task(record.task_id()).await.unwrap_err(),
        StoreError::CorruptEventStream(record.task_id().clone())
    );
    assert_eq!(
        store.replay_task(record.task_id()).await.unwrap_err(),
        StoreError::CorruptEventStream(record.task_id().clone())
    );

    let report = store.recover_stale_leases(0, None).await.unwrap();
    assert_eq!(report.corrupted_tasks, vec![record.task_id().clone()]);
}

#[tokio::test]
async fn sqlite_replay_and_recovery_report_status_mismatch_as_corruption() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("corrupt-status-keryx.db");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();

    let record = task(
        "sqlite-corrupt-status-task",
        TaskStatus::Pending,
        Some("sqlite-corrupt-status-idem"),
    );
    store.accept_task(record.clone()).await.unwrap();

    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET status = 'running' WHERE task_id = ?")
        .bind(record.task_id().as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        store.replay_task(record.task_id()).await.unwrap_err(),
        StoreError::CorruptEventStream(record.task_id().clone())
    );

    let report = store.recover_stale_leases(0, None).await.unwrap();
    assert_eq!(report.corruption_count(), 1);
    assert_eq!(report.corrupted_tasks, vec![record.task_id().clone()]);
}

#[tokio::test]
async fn sqlite_replay_rejects_dead_lettered_snapshot_without_retry_count() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("corrupt-dead-letter-retry-keryx.db");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();

    let record = task(
        "sqlite-dead-letter-without-retry-task",
        TaskStatus::Pending,
        Some("sqlite-dead-letter-without-retry-idem"),
    );
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(
        record.task_id(),
        "sqlite-dead-letter-without-retry-lease",
        "sqlite-dead-letter-without-retry-worker",
        1_000,
    );
    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    let dead_lettered = store
        .dead_letter_task(record.task_id(), "still broken")
        .await
        .unwrap();
    assert!(dead_lettered.dead_lettered);
    assert_eq!(dead_lettered.retry_count, 1);

    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET retry_count = 0 WHERE task_id = ?")
        .bind(record.task_id().as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        store.replay_task(record.task_id()).await.unwrap_err(),
        StoreError::CorruptEventStream(record.task_id().clone())
    );
}
