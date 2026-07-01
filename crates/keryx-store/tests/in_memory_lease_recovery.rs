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
fn in_memory_lease_task_persists_lease_and_task_started_event() {
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
fn in_memory_lease_renewal_updates_metadata_only_and_keeps_running_status() {
    let store = InMemoryStore::default();
    let record = task("lease-task-2", TaskStatus::Pending, Some("lease-idem-2"));
    store.accept_task(record.clone()).unwrap();
    let lease = lease(record.task_id(), "lease-2", 500);
    store.lease_task(record.task_id(), lease.clone()).unwrap();

    let renewed = store
        .renew_lease(record.task_id(), &lease.lease_id, 400, 900)
        .unwrap();

    assert_eq!(renewed.expires_at_ms, 900);
    assert_eq!(
        store.get_task(record.task_id()).unwrap().status,
        TaskStatus::Running
    );
    let events = store.events_for_task(record.task_id()).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::TaskStarted
    );
}

#[test]
fn in_memory_recovery_requeues_expired_running_leases_deterministically() {
    let store = InMemoryStore::default();

    let first = task("lease-task-a", TaskStatus::Pending, Some("lease-idem-a"));
    store.accept_task(first.clone()).unwrap();
    store
        .lease_task(first.task_id(), lease(first.task_id(), "lease-a", 500))
        .unwrap();

    let second = task("lease-task-b", TaskStatus::Pending, Some("lease-idem-b"));
    store.accept_task(second.clone()).unwrap();
    store
        .lease_task(second.task_id(), lease(second.task_id(), "lease-b", 500))
        .unwrap();

    let recovered = store.recover_stale_leases(501).unwrap();

    assert_eq!(
        recovered
            .iter()
            .map(|task| task.task_id().as_str())
            .collect::<Vec<_>>(),
        vec!["lease-task-a", "lease-task-b"]
    );
    assert_eq!(
        store.get_task(first.task_id()).unwrap().status,
        TaskStatus::Pending
    );
    assert_eq!(
        store.get_task(second.task_id()).unwrap().status,
        TaskStatus::Pending
    );
}

#[test]
fn in_memory_recovery_preserves_terminal_tasks_and_cleans_stale_metadata() {
    let store = InMemoryStore::default();
    let record = task("lease-task-3", TaskStatus::Pending, Some("lease-idem-3"));
    store.accept_task(record.clone()).unwrap();
    let lease = lease(record.task_id(), "lease-3", 500);
    store.lease_task(record.task_id(), lease.clone()).unwrap();
    store
        .complete_task(record.task_id(), &lease.lease_id)
        .unwrap();

    let recovered = store.recover_stale_leases(501).unwrap();

    assert!(recovered.is_empty());
    assert_eq!(
        store.get_task(record.task_id()).unwrap().status,
        TaskStatus::Completed
    );
    assert!(store.active_lease(record.task_id()).unwrap().is_none());
    let events = store.events_for_task(record.task_id()).unwrap();
    assert_eq!(
        events.last().unwrap().event_type,
        KeryxEventType::TaskCompleted
    );
}
