use keryx_core::{IdempotencyKey, LeaseId, TaskId, TaskStatus, ValidationError};
use keryx_store::{LeaseRecord, SqliteStore, StoreError, TaskRecord};
use tempfile::tempdir;

fn task(id: &str, status: TaskStatus, idem: Option<&str>) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        status,
        idem.map(|key| IdempotencyKey::new(key).unwrap()),
    )
}

fn lease(task_id: &TaskId, lease_id: &str, expires_at_ms: i64) -> LeaseRecord {
    LeaseRecord::new(
        LeaseId::new(lease_id).unwrap(),
        task_id.clone(),
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

#[tokio::test]
async fn sqlite_migration_from_empty_database_creates_schema_version() {
    let store = temp_store().await;

    assert_eq!(store.schema_version().await.unwrap(), 1);
}

#[tokio::test]
async fn sqlite_pending_running_completed_succeeds_via_lease_and_complete() {
    let store = temp_store().await;
    let record = task("sqlite-task-1", TaskStatus::Pending, Some("sqlite-idem-1"));
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(record.task_id(), "sqlite-lease-1", 1_000);

    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    let completed = store
        .complete_task(record.task_id(), &lease.lease_id)
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
async fn sqlite_pending_running_failed_succeeds_via_lease_and_fail() {
    let store = temp_store().await;
    let record = task("sqlite-task-2", TaskStatus::Pending, Some("sqlite-idem-2"));
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(record.task_id(), "sqlite-lease-2", 1_000);

    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    let failed = store
        .fail_task(record.task_id(), &lease.lease_id)
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
    let completed_lease = lease(completed.task_id(), "sqlite-lease-4", 1_000);
    store
        .lease_task(completed.task_id(), completed_lease.clone())
        .await
        .unwrap();
    store
        .complete_task(completed.task_id(), &completed_lease.lease_id)
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
    let failed_lease = lease(failed.task_id(), "sqlite-lease-5", 1_000);
    store
        .lease_task(failed.task_id(), failed_lease.clone())
        .await
        .unwrap();
    store
        .fail_task(failed.task_id(), &failed_lease.lease_id)
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
