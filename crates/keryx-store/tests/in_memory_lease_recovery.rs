use keryx_core::{IdempotencyKey, KeryxEventType, LeaseId, TaskId, TaskStatus};
use keryx_store::{InMemoryStore, LeaseRecord, TaskRecord, TaskStore};

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

#[test]
fn in_memory_lease_task_persists_lease_and_task_leased_event() {
    let store = InMemoryStore::default();
    let record = task("lease-task-1", TaskStatus::Pending, Some("lease-idem-1"));
    store.accept_task(record.clone()).unwrap();

    let leased = store
        .lease_task(record.task_id(), lease(record.task_id(), "lease-1", 1_000))
        .unwrap();

    assert_eq!(leased.status, TaskStatus::Running);
    assert_eq!(
        store
            .active_lease(record.task_id())
            .unwrap()
            .unwrap()
            .lease_id
            .as_str(),
        "lease-1"
    );
    let events = store.events_for_task(record.task_id()).unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::TaskStarted
    );
    assert_eq!(events.last().unwrap().to_status, TaskStatus::Running);
}

#[test]
fn in_memory_recovery_requeues_stale_leases_and_emits_recovery_event() {
    let store = InMemoryStore::default();
    let record = task("lease-task-2", TaskStatus::Pending, Some("lease-idem-2"));
    store.accept_task(record.clone()).unwrap();
    store
        .lease_task(record.task_id(), lease(record.task_id(), "lease-2", 500))
        .unwrap();

    let recovered = store.recover_stale_leases(501).unwrap();

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status, TaskStatus::Pending);
    assert!(store.active_lease(record.task_id()).unwrap().is_none());
    let events = store.events_for_task(record.task_id()).unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::RecoveryAction
    );
    assert_eq!(
        events.last().unwrap().from_status,
        Some(TaskStatus::Running)
    );
    assert_eq!(events.last().unwrap().to_status, TaskStatus::Pending);
}

#[test]
fn in_memory_recovery_preserves_terminal_tasks() {
    let store = InMemoryStore::default();
    let record = task("lease-task-3", TaskStatus::Pending, Some("lease-idem-3"));
    store.accept_task(record.clone()).unwrap();
    store
        .lease_task(record.task_id(), lease(record.task_id(), "lease-3", 500))
        .unwrap();
    store
        .transition_task(record.task_id(), TaskStatus::Completed)
        .unwrap();

    let recovered = store.recover_stale_leases(501).unwrap();

    assert!(recovered.is_empty());
    assert_eq!(
        store.get_task(record.task_id()).unwrap().status,
        TaskStatus::Completed
    );
}
