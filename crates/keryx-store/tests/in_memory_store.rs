use keryx_core::{IdempotencyKey, KeryxEventType, LeaseId, TaskId, TaskStatus, ValidationError};
use keryx_store::{InMemoryStore, LeaseRecord, StoreError, TaskRecord, TaskStore};

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
fn accepted_task_is_persisted_with_task_created_event_before_ack() {
    let store = InMemoryStore::default();
    let record = task("task-1", TaskStatus::Pending, Some("idem-1"));

    store.accept_task(record.clone()).expect("accept task");

    assert_eq!(store.get_task(record.task_id()).unwrap(), record);
    let events = store.events_for_task(record.task_id()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, KeryxEventType::TaskAccepted);
    assert_eq!(events[0].to_status, TaskStatus::Pending);
}

#[test]
fn pending_running_completed_succeeds_via_lease_and_complete() {
    let store = InMemoryStore::default();
    let record = task("task-2", TaskStatus::Pending, Some("idem-2"));
    store.accept_task(record.clone()).unwrap();
    let lease = lease(record.task_id(), "lease-2", 1_000);

    store.lease_task(record.task_id(), lease.clone()).unwrap();
    let completed = store
        .complete_task(record.task_id(), &lease.lease_id)
        .unwrap();

    assert_eq!(completed.status, TaskStatus::Completed);
    assert!(store.active_lease(record.task_id()).unwrap().is_none());
    assert_eq!(
        store.replay_task(record.task_id()).unwrap().status,
        TaskStatus::Completed
    );
}

#[test]
fn pending_running_failed_succeeds_via_lease_and_fail() {
    let store = InMemoryStore::default();
    let record = task("task-3", TaskStatus::Pending, Some("idem-3"));
    store.accept_task(record.clone()).unwrap();
    let lease = lease(record.task_id(), "lease-3", 1_000);

    store.lease_task(record.task_id(), lease.clone()).unwrap();
    let failed = store.fail_task(record.task_id(), &lease.lease_id).unwrap();

    assert_eq!(failed.status, TaskStatus::Failed);
    assert!(store.active_lease(record.task_id()).unwrap().is_none());
}

#[test]
fn pending_completed_fails_with_validation_error() {
    let store = InMemoryStore::default();
    let record = task("task-4", TaskStatus::Pending, Some("idem-4"));
    store.accept_task(record.clone()).unwrap();

    let err = store
        .transition_task(record.task_id(), TaskStatus::Completed)
        .unwrap_err();

    assert_eq!(
        err,
        StoreError::Validation(ValidationError::InvalidTaskTransition {
            from: TaskStatus::Pending,
            to: TaskStatus::Completed,
        })
    );
}

#[test]
fn completed_and_failed_tasks_are_terminally_immutable() {
    let store = InMemoryStore::default();
    let completed = task("task-5", TaskStatus::Pending, Some("idem-5"));
    store.accept_task(completed.clone()).unwrap();
    let completed_lease = lease(completed.task_id(), "lease-5", 1_000);
    store
        .lease_task(completed.task_id(), completed_lease.clone())
        .unwrap();
    store
        .complete_task(completed.task_id(), &completed_lease.lease_id)
        .unwrap();

    for to in [TaskStatus::Pending, TaskStatus::Running, TaskStatus::Failed] {
        assert_eq!(
            store.transition_task(completed.task_id(), to).unwrap_err(),
            StoreError::Validation(ValidationError::TerminalTaskTransition {
                from: TaskStatus::Completed,
                to,
            })
        );
    }

    let failed = task("task-6", TaskStatus::Pending, Some("idem-6"));
    store.accept_task(failed.clone()).unwrap();
    let failed_lease = lease(failed.task_id(), "lease-6", 1_000);
    store
        .lease_task(failed.task_id(), failed_lease.clone())
        .unwrap();
    store
        .fail_task(failed.task_id(), &failed_lease.lease_id)
        .unwrap();

    for to in [
        TaskStatus::Pending,
        TaskStatus::Running,
        TaskStatus::Completed,
    ] {
        assert_eq!(
            store.transition_task(failed.task_id(), to).unwrap_err(),
            StoreError::Validation(ValidationError::TerminalTaskTransition {
                from: TaskStatus::Failed,
                to,
            })
        );
    }
}

#[test]
fn duplicate_idempotency_key_returns_existing_compatible_task() {
    let store = InMemoryStore::default();
    let original = task("task-7", TaskStatus::Pending, Some("idem-7"));
    store.accept_task(original.clone()).unwrap();

    let duplicate = task("task-7", TaskStatus::Pending, Some("idem-7"));
    let returned = store.accept_task(duplicate).unwrap();

    assert_eq!(returned, original);
    assert_eq!(store.events_for_task(original.task_id()).unwrap().len(), 1);
}

#[test]
fn conflicting_idempotency_key_reuse_is_rejected() {
    let store = InMemoryStore::default();
    store
        .accept_task(task("task-8", TaskStatus::Pending, Some("idem-8")))
        .unwrap();

    let err = store
        .accept_task(task("task-other", TaskStatus::Pending, Some("idem-8")))
        .unwrap_err();

    assert!(matches!(err, StoreError::IdempotencyConflict { .. }));
}
