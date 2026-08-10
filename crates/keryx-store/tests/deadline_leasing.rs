use keryx_core::{AgentId, KeryxEventType, LeaseId, TaskId, TaskStatus};
use keryx_store::{InMemoryStore, LeaseRecord, SqliteStore, TaskRecord, TaskStore};
use tempfile::tempdir;

fn expired_task(id: &str) -> TaskRecord {
    let mut task = TaskRecord::new(TaskId::new(id).unwrap(), TaskStatus::Pending, None);
    task.deadline_ms = Some(100);
    task
}

fn lease(task_id: &TaskId, id: &str) -> LeaseRecord {
    LeaseRecord::new(
        LeaseId::new(id).unwrap(),
        task_id.clone(),
        AgentId::new("deadline-worker").unwrap(),
        100,
        1_000,
    )
}

#[test]
fn in_memory_lease_atomically_rejects_and_fails_expired_task() {
    let store = InMemoryStore::default();
    let task = expired_task("memory-expired-at-lease");
    store.accept_task(task.clone()).unwrap();

    let error = store
        .lease_task(
            task.task_id(),
            lease(task.task_id(), "memory-expired-lease"),
        )
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "task deadline expired: task=memory-expired-at-lease deadline_ms=100 attempted_lease_at_ms=100"
    );
    assert_eq!(
        store.get_task(task.task_id()).unwrap().status,
        TaskStatus::Failed
    );
    let events = store.events_for_task(task.task_id()).unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::TaskTimedOut
    );
    assert_eq!(
        events.last().unwrap().from_status,
        Some(TaskStatus::Pending)
    );
    assert_eq!(events.last().unwrap().to_status, TaskStatus::Failed);
}

#[tokio::test]
async fn sqlite_lease_atomically_rejects_and_fails_expired_task() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(&dir.path().join("keryx.sqlite3"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let task = expired_task("sqlite-expired-at-lease");
    store.accept_task(task.clone()).await.unwrap();

    let error = store
        .lease_task(
            task.task_id(),
            lease(task.task_id(), "sqlite-expired-lease"),
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "task deadline expired: task=sqlite-expired-at-lease deadline_ms=100 attempted_lease_at_ms=100"
    );
    assert_eq!(
        store.get_task(task.task_id()).await.unwrap().status,
        TaskStatus::Failed
    );
    let events = store.events_for_task(task.task_id()).await.unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::TaskTimedOut
    );
    assert_eq!(
        events.last().unwrap().from_status,
        Some(TaskStatus::Pending)
    );
    assert_eq!(events.last().unwrap().to_status, TaskStatus::Failed);
}
