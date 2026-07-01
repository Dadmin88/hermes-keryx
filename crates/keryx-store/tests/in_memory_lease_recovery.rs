use keryx_core::{AgentId, IdempotencyKey, KeryxEventType, LeaseId, TaskId, TaskStatus};
use keryx_store::{InMemoryStore, LeaseRecord, TaskRecord, TaskStore};

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

#[test]
fn in_memory_lease_task_persists_lease_and_task_started_event() {
    let store = InMemoryStore::default();
    let record = task("lease-task-1", TaskStatus::Pending, Some("lease-idem-1"));
    store.accept_task(record.clone()).unwrap();

    let leased = store
        .lease_task(
            record.task_id(),
            lease(record.task_id(), "lease-1", "worker-1", 1_000),
        )
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
    let lease = lease(record.task_id(), "lease-2", "worker-2", 500);
    store.lease_task(record.task_id(), lease.clone()).unwrap();

    let renewed = store
        .renew_lease(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
            400,
            900,
        )
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
        .lease_task(
            first.task_id(),
            lease(first.task_id(), "lease-a", "worker-a", 500),
        )
        .unwrap();

    let second = task("lease-task-b", TaskStatus::Pending, Some("lease-idem-b"));
    store.accept_task(second.clone()).unwrap();
    store
        .lease_task(
            second.task_id(),
            lease(second.task_id(), "lease-b", "worker-b", 500),
        )
        .unwrap();

    let recovered = store.recover_stale_leases(501, None).unwrap();

    assert_eq!(
        recovered
            .recovered_tasks
            .iter()
            .map(|task| task.task_id().as_str())
            .collect::<Vec<_>>(),
        vec!["lease-task-a", "lease-task-b"]
    );
    assert_eq!(recovered.cleaned_terminal_leases, 0);
    assert_eq!(recovered.corruption_count(), 0);
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
fn in_memory_recovery_limit_is_applied_after_deterministic_ordering() {
    let store = InMemoryStore::default();

    let first = task(
        "lease-task-limit-a",
        TaskStatus::Pending,
        Some("lease-idem-limit-a"),
    );
    store.accept_task(first.clone()).unwrap();
    store
        .lease_task(
            first.task_id(),
            lease(first.task_id(), "lease-limit-a", "worker-limit-a", 500),
        )
        .unwrap();

    let second = task(
        "lease-task-limit-b",
        TaskStatus::Pending,
        Some("lease-idem-limit-b"),
    );
    store.accept_task(second.clone()).unwrap();
    store
        .lease_task(
            second.task_id(),
            lease(second.task_id(), "lease-limit-b", "worker-limit-b", 400),
        )
        .unwrap();

    let recovered = store.recover_stale_leases(501, Some(1)).unwrap();

    assert_eq!(recovered.recovered_tasks, vec![second.clone()]);
    assert_eq!(
        store.get_task(first.task_id()).unwrap().status,
        TaskStatus::Running
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
    let lease = lease(record.task_id(), "lease-3", "worker-3", 500);
    store.lease_task(record.task_id(), lease.clone()).unwrap();
    store
        .complete_task(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
        )
        .unwrap();

    let recovered = store.recover_stale_leases(501, None).unwrap();

    assert!(recovered.recovered_tasks.is_empty());
    assert_eq!(recovered.cleaned_terminal_leases, 0);
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

#[test]
fn in_memory_stale_tokens_are_rejected_after_recovery_and_reissue() {
    let store = InMemoryStore::default();
    let record = task("lease-task-4", TaskStatus::Pending, Some("lease-idem-4"));
    store.accept_task(record.clone()).unwrap();
    let first_lease = lease(record.task_id(), "lease-4a", "worker-4a", 500);
    store
        .lease_task(record.task_id(), first_lease.clone())
        .unwrap();

    let recovered = store.recover_stale_leases(501, None).unwrap();
    assert_eq!(recovered.recovered_tasks, vec![record.clone()]);

    assert_eq!(
        store
            .renew_lease(
                record.task_id(),
                &first_lease.lease_id,
                first_lease.worker_id.as_ref().unwrap(),
                501,
                900,
            )
            .unwrap_err(),
        keryx_store::StoreError::Validation(keryx_core::ValidationError::InvalidTaskTransition {
            from: TaskStatus::Pending,
            to: TaskStatus::Running,
        },)
    );
    assert_eq!(
        store
            .complete_task(
                record.task_id(),
                &first_lease.lease_id,
                first_lease.worker_id.as_ref().unwrap(),
            )
            .unwrap_err(),
        keryx_store::StoreError::LeaseNotFound(record.task_id().clone())
    );
    assert_eq!(
        store
            .fail_task(
                record.task_id(),
                &first_lease.lease_id,
                first_lease.worker_id.as_ref().unwrap(),
            )
            .unwrap_err(),
        keryx_store::StoreError::LeaseNotFound(record.task_id().clone())
    );

    let second_lease = lease(record.task_id(), "lease-4b", "worker-4b", 1_000);
    store
        .lease_task(record.task_id(), second_lease.clone())
        .unwrap();

    assert_eq!(
        store
            .renew_lease(
                record.task_id(),
                &first_lease.lease_id,
                first_lease.worker_id.as_ref().unwrap(),
                600,
                1_100,
            )
            .unwrap_err(),
        keryx_store::StoreError::LeaseMismatch {
            task_id: record.task_id().clone(),
            lease_id: first_lease.lease_id.clone(),
        }
    );
    assert_eq!(
        store
            .complete_task(
                record.task_id(),
                &first_lease.lease_id,
                first_lease.worker_id.as_ref().unwrap(),
            )
            .unwrap_err(),
        keryx_store::StoreError::LeaseMismatch {
            task_id: record.task_id().clone(),
            lease_id: first_lease.lease_id.clone(),
        }
    );
    assert_eq!(
        store
            .fail_task(
                record.task_id(),
                &first_lease.lease_id,
                first_lease.worker_id.as_ref().unwrap(),
            )
            .unwrap_err(),
        keryx_store::StoreError::LeaseMismatch {
            task_id: record.task_id().clone(),
            lease_id: first_lease.lease_id.clone(),
        }
    );

    let completed = store
        .complete_task(
            record.task_id(),
            &second_lease.lease_id,
            second_lease.worker_id.as_ref().unwrap(),
        )
        .unwrap();
    assert_eq!(completed.status, TaskStatus::Completed);
}
