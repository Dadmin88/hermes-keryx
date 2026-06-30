use keryx_core::{IdempotencyKey, KeryxEventType, TaskId, TaskStatus};
use keryx_store::{SqliteStore, StoreError, TaskRecord};
use tempfile::tempdir;

fn task(id: &str, status: TaskStatus, idem: Option<&str>) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        status,
        idem.map(|key| IdempotencyKey::new(key).unwrap()),
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
async fn sqlite_accept_task_persists_task_event_and_idempotency() {
    let store = temp_store().await;
    let record = task("sqlite-task-1", TaskStatus::Accepted, Some("sqlite-idem-1"));

    store.accept_task(record.clone()).await.unwrap();

    assert_eq!(store.get_task(record.task_id()).await.unwrap(), record);
    let events = store.events_for_task(record.task_id()).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, KeryxEventType::TaskAccepted);
    assert_eq!(events[0].to_status, TaskStatus::Accepted);
}

#[tokio::test]
async fn sqlite_transitions_append_events_and_replay_terminal_state() {
    let store = temp_store().await;
    let record = task("sqlite-task-2", TaskStatus::Accepted, Some("sqlite-idem-2"));
    store.accept_task(record.clone()).await.unwrap();

    store
        .transition_task(record.task_id(), TaskStatus::Queued)
        .await
        .unwrap();
    store
        .transition_task(record.task_id(), TaskStatus::Leased)
        .await
        .unwrap();
    store
        .transition_task(record.task_id(), TaskStatus::Running)
        .await
        .unwrap();
    store
        .transition_task(record.task_id(), TaskStatus::Completed)
        .await
        .unwrap();

    let replayed = store.replay_task(record.task_id()).await.unwrap();
    assert_eq!(replayed.status, TaskStatus::Completed);
    let events = store.events_for_task(record.task_id()).await.unwrap();
    assert_eq!(events.len(), 5);
}

#[tokio::test]
async fn sqlite_idempotency_duplicate_and_conflict_behave_like_memory_store() {
    let store = temp_store().await;
    let original = task("sqlite-task-3", TaskStatus::Accepted, Some("sqlite-idem-3"));
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
            TaskStatus::Accepted,
            Some("sqlite-idem-3"),
        ))
        .await
        .unwrap_err();
    assert!(matches!(conflict, StoreError::IdempotencyConflict { .. }));
}
