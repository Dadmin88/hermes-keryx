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
async fn sqlite_lease_task_persists_lease_and_task_leased_event() {
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
async fn sqlite_recovery_requeues_stale_leases_and_emits_recovery_event() {
    let store = temp_store().await;
    let record = task(
        "sqlite-lease-task-2",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-2"),
    );
    store.accept_task(record.clone()).await.unwrap();
    store
        .lease_task(
            record.task_id(),
            lease(record.task_id(), "sqlite-lease-2", 500),
        )
        .await
        .unwrap();

    let recovered = store.recover_stale_leases(501).await.unwrap();

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status, TaskStatus::Pending);
    assert!(store
        .active_lease(record.task_id())
        .await
        .unwrap()
        .is_none());
    let events = store.events_for_task(record.task_id()).await.unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::RecoveryAction
    );
    assert_eq!(events.last().unwrap().to_status, TaskStatus::Pending);
}
