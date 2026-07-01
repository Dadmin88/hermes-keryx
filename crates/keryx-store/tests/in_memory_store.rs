use keryx_core::{IdempotencyKey, KeryxEventType, TaskId, TaskStatus};
use keryx_store::{InMemoryStore, StoreError, TaskRecord, TaskStore};

fn task(id: &str, status: TaskStatus, idem: Option<&str>) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        status,
        idem.map(|key| IdempotencyKey::new(key).unwrap()),
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
fn task_state_transition_persists_snapshot_and_event() {
    let store = InMemoryStore::default();
    let record = task("task-2", TaskStatus::Pending, Some("idem-2"));
    store.accept_task(record.clone()).unwrap();

    store
        .transition_task(record.task_id(), TaskStatus::Running)
        .unwrap();

    let updated = store.get_task(record.task_id()).unwrap();
    assert_eq!(updated.status, TaskStatus::Running);
    let events = store.events_for_task(record.task_id()).unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![KeryxEventType::TaskAccepted, KeryxEventType::TaskStarted]
    );
}

#[test]
fn duplicate_idempotency_key_returns_existing_compatible_task() {
    let store = InMemoryStore::default();
    let original = task("task-3", TaskStatus::Pending, Some("idem-3"));
    store.accept_task(original.clone()).unwrap();

    let duplicate = task("task-3", TaskStatus::Pending, Some("idem-3"));
    let returned = store.accept_task(duplicate).unwrap();

    assert_eq!(returned, original);
    assert_eq!(store.events_for_task(original.task_id()).unwrap().len(), 1);
}

#[test]
fn conflicting_idempotency_key_reuse_is_rejected() {
    let store = InMemoryStore::default();
    store
        .accept_task(task("task-4", TaskStatus::Pending, Some("idem-4")))
        .unwrap();

    let err = store
        .accept_task(task("task-other", TaskStatus::Pending, Some("idem-4")))
        .unwrap_err();

    assert!(matches!(err, StoreError::IdempotencyConflict { .. }));
}

#[test]
fn event_replay_reconstructs_current_task_state() {
    let store = InMemoryStore::default();
    let record = task("task-5", TaskStatus::Pending, Some("idem-5"));
    store.accept_task(record.clone()).unwrap();
    store
        .transition_task(record.task_id(), TaskStatus::Running)
        .unwrap();
    store
        .transition_task(record.task_id(), TaskStatus::Completed)
        .unwrap();

    let replayed = store.replay_task(record.task_id()).unwrap();

    assert_eq!(replayed.status, TaskStatus::Completed);
    assert_eq!(replayed.task_id(), record.task_id());
}
