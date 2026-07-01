use keryx_core::{IdempotencyKey, KeryxEventType, LeaseId, TaskId, TaskStatus};
use keryx_store::{LeaseRecord, SqliteStore, TaskRecord};
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
async fn sqlite_lease_task_persists_lease_and_task_started_event() {
    let store = temp_store().await;
    let record = task(
        "sqlite-lease-task-1",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-1"),
    );
    store.accept_task(record.clone()).await.unwrap();

    let leased = store
        .lease_task(
            record.task_id(),
            lease(record.task_id(), "sqlite-lease-1", 1_000),
        )
        .await
        .unwrap();

    assert_eq!(leased.status, TaskStatus::Running);
    assert_eq!(
        store
            .active_lease(record.task_id())
            .await
            .unwrap()
            .unwrap()
            .lease_id
            .as_str(),
        "sqlite-lease-1"
    );
    let events = store.events_for_task(record.task_id()).await.unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::TaskStarted
    );
}

#[tokio::test]
async fn sqlite_lease_renewal_updates_metadata_only_and_keeps_running_status() {
    let store = temp_store().await;
    let record = task(
        "sqlite-lease-task-2",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-2"),
    );
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(record.task_id(), "sqlite-lease-2", 500);
    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();

    let renewed = store
        .renew_lease(record.task_id(), &lease.lease_id, 400, 900)
        .await
        .unwrap();

    assert_eq!(renewed.expires_at_ms, 900);
    assert_eq!(
        store.get_task(record.task_id()).await.unwrap().status,
        TaskStatus::Running
    );
    let events = store.events_for_task(record.task_id()).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::TaskStarted
    );
}

#[tokio::test]
async fn sqlite_recovery_requeues_expired_running_leases_deterministically() {
    let store = temp_store().await;

    let first = task(
        "sqlite-lease-task-a",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-a"),
    );
    store.accept_task(first.clone()).await.unwrap();
    store
        .lease_task(
            first.task_id(),
            lease(first.task_id(), "sqlite-lease-a", 500),
        )
        .await
        .unwrap();

    let second = task(
        "sqlite-lease-task-b",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-b"),
    );
    store.accept_task(second.clone()).await.unwrap();
    store
        .lease_task(
            second.task_id(),
            lease(second.task_id(), "sqlite-lease-b", 500),
        )
        .await
        .unwrap();

    let recovered = store.recover_stale_leases(501).await.unwrap();

    assert_eq!(
        recovered
            .iter()
            .map(|task| task.task_id().as_str())
            .collect::<Vec<_>>(),
        vec!["sqlite-lease-task-a", "sqlite-lease-task-b"]
    );
    assert_eq!(
        store.get_task(first.task_id()).await.unwrap().status,
        TaskStatus::Pending
    );
    assert_eq!(
        store.get_task(second.task_id()).await.unwrap().status,
        TaskStatus::Pending
    );
}

#[tokio::test]
async fn sqlite_recovery_preserves_terminal_tasks_and_cleans_stale_metadata() {
    let store = temp_store().await;
    let record = task(
        "sqlite-lease-task-3",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-3"),
    );
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(record.task_id(), "sqlite-lease-3", 500);
    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    store
        .complete_task(record.task_id(), &lease.lease_id)
        .await
        .unwrap();

    let recovered = store.recover_stale_leases(501).await.unwrap();

    assert!(recovered.is_empty());
    assert_eq!(
        store.get_task(record.task_id()).await.unwrap().status,
        TaskStatus::Completed
    );
    assert!(store
        .active_lease(record.task_id())
        .await
        .unwrap()
        .is_none());
    let events = store.events_for_task(record.task_id()).await.unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::TaskCompleted
    );
}
