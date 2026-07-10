use keryx_core::{
    AgentId, IdempotencyKey, KeryxEventType, LeaseId, RetryPolicy, TaskId, TaskStatus,
};
use keryx_store::{InMemoryStore, LeaseRecord, SqliteStore, StoreError, TaskRecord, TaskStore};
use tempfile::tempdir;

fn task(id: &str) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        TaskStatus::Pending,
        Some(IdempotencyKey::new("idem-retry").unwrap()),
    )
}

fn lease(task_id: &TaskId, lease_id: &str, worker: &str) -> LeaseRecord {
    LeaseRecord::new(
        LeaseId::new(lease_id).unwrap(),
        task_id.clone(),
        AgentId::new(worker).unwrap(),
        1,
        9_999,
    )
}

async fn temp_store() -> SqliteStore {
    let dir = tempdir().unwrap().keep();
    let db_path = dir.join("keryx.db");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    store
}

#[test]
fn fail_task_retries_to_pending_until_policy_exhausted() {
    let store = InMemoryStore::default();
    let record = task("retry-task");
    store.accept_task(record.clone()).unwrap();
    let policy = RetryPolicy {
        max_retries: 2,
        backoff_ms: 0,
        dead_letter_after: 3,
    };

    for attempt in 0..2 {
        let lease = lease(record.task_id(), &format!("lease-{attempt}"), "worker");
        store.lease_task(record.task_id(), lease.clone()).unwrap();
        let failed = store
            .fail_task(
                record.task_id(),
                &lease.lease_id,
                lease.worker_id.as_ref().unwrap(),
                "transient",
                &policy,
            )
            .unwrap();
        assert_eq!(failed.status, TaskStatus::Pending);
        assert_eq!(failed.retry_count, attempt + 1);
        assert!(!failed.dead_lettered);
    }

    let final_lease = lease(record.task_id(), "lease-final", "worker");
    store
        .lease_task(record.task_id(), final_lease.clone())
        .unwrap();
    let dead = store
        .fail_task(
            record.task_id(),
            &final_lease.lease_id,
            final_lease.worker_id.as_ref().unwrap(),
            "still broken",
            &policy,
        )
        .unwrap();
    assert_eq!(dead.status, TaskStatus::Failed);
    assert_eq!(dead.retry_count, 3);
    assert!(dead.dead_lettered);
    assert_eq!(dead.dead_letter_reason.as_deref(), Some("still broken"));

    let events = store.events_for_task(record.task_id()).unwrap();
    assert!(events
        .iter()
        .any(|event| event.event_type == KeryxEventType::TaskDeadLettered));
}

#[test]
fn fail_task_with_no_retries_goes_directly_to_failed_without_dead_letter() {
    let store = InMemoryStore::default();
    let record = task("no-retry-task");
    store.accept_task(record.clone()).unwrap();
    let lease = lease(record.task_id(), "lease-1", "worker");
    store.lease_task(record.task_id(), lease.clone()).unwrap();

    let failed = store
        .fail_task(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
            "fatal",
            &RetryPolicy::no_retries(),
        )
        .unwrap();
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.retry_count, 0);
    assert!(!failed.dead_lettered);
}

#[test]
fn replay_preserves_snapshot_retry_metadata_across_retry_and_dead_letter_in_memory() {
    let store = InMemoryStore::default();
    let record = task("retry-replay-task");
    store.accept_task(record.clone()).unwrap();
    let policy = RetryPolicy {
        max_retries: 2,
        backoff_ms: 0,
        dead_letter_after: 3,
    };

    let first_lease = lease(record.task_id(), "lease-replay-1", "worker");
    store
        .lease_task(record.task_id(), first_lease.clone())
        .unwrap();
    let first_retry = store
        .fail_task(
            record.task_id(),
            &first_lease.lease_id,
            first_lease.worker_id.as_ref().unwrap(),
            "transient",
            &policy,
        )
        .unwrap();
    assert_eq!(store.replay_task(record.task_id()).unwrap(), first_retry);

    let second_lease = lease(record.task_id(), "lease-replay-2", "worker");
    store
        .lease_task(record.task_id(), second_lease.clone())
        .unwrap();
    let second_retry = store
        .fail_task(
            record.task_id(),
            &second_lease.lease_id,
            second_lease.worker_id.as_ref().unwrap(),
            "transient-again",
            &policy,
        )
        .unwrap();
    assert_eq!(store.replay_task(record.task_id()).unwrap(), second_retry);

    let final_lease = lease(record.task_id(), "lease-replay-final", "worker");
    store
        .lease_task(record.task_id(), final_lease.clone())
        .unwrap();
    let dead_lettered = store
        .fail_task(
            record.task_id(),
            &final_lease.lease_id,
            final_lease.worker_id.as_ref().unwrap(),
            "still broken",
            &policy,
        )
        .unwrap();

    assert_eq!(dead_lettered.retry_count, 3);
    assert!(dead_lettered.dead_lettered);
    assert_eq!(store.replay_task(record.task_id()).unwrap(), dead_lettered);
}

#[tokio::test]
async fn replay_preserves_snapshot_retry_metadata_across_retry_and_dead_letter_in_sqlite() {
    let store = temp_store().await;
    let record = task("sqlite-retry-replay-task");
    store.accept_task(record.clone()).await.unwrap();
    let policy = RetryPolicy {
        max_retries: 2,
        backoff_ms: 0,
        dead_letter_after: 3,
    };

    let first_lease = lease(record.task_id(), "sqlite-lease-replay-1", "worker");
    store
        .lease_task(record.task_id(), first_lease.clone())
        .await
        .unwrap();
    let first_retry = store
        .fail_task(
            record.task_id(),
            &first_lease.lease_id,
            first_lease.worker_id.as_ref().unwrap(),
            "transient",
            &policy,
        )
        .await
        .unwrap();
    assert_eq!(
        store.replay_task(record.task_id()).await.unwrap(),
        first_retry
    );

    let second_lease = lease(record.task_id(), "sqlite-lease-replay-2", "worker");
    store
        .lease_task(record.task_id(), second_lease.clone())
        .await
        .unwrap();
    let second_retry = store
        .fail_task(
            record.task_id(),
            &second_lease.lease_id,
            second_lease.worker_id.as_ref().unwrap(),
            "transient-again",
            &policy,
        )
        .await
        .unwrap();
    assert_eq!(
        store.replay_task(record.task_id()).await.unwrap(),
        second_retry
    );

    let final_lease = lease(record.task_id(), "sqlite-lease-replay-final", "worker");
    store
        .lease_task(record.task_id(), final_lease.clone())
        .await
        .unwrap();
    let dead_lettered = store
        .fail_task(
            record.task_id(),
            &final_lease.lease_id,
            final_lease.worker_id.as_ref().unwrap(),
            "still broken",
            &policy,
        )
        .await
        .unwrap();

    assert_eq!(dead_lettered.retry_count, 3);
    assert!(dead_lettered.dead_lettered);
    assert_eq!(
        store.replay_task(record.task_id()).await.unwrap(),
        dead_lettered
    );
}

#[test]
fn manual_retry_and_dead_letter_require_matching_worker_in_memory() {
    let store = InMemoryStore::default();
    let retry_record = task("manual-retry-owner-task");
    store.accept_task(retry_record.clone()).unwrap();
    let retry_lease = lease(retry_record.task_id(), "manual-retry-owner-lease", "owner");
    store
        .lease_task(retry_record.task_id(), retry_lease.clone())
        .unwrap();
    let attacker = AgentId::new("attacker").unwrap();

    assert_eq!(
        store
            .retry_task(retry_record.task_id(), &retry_lease.lease_id, &attacker)
            .unwrap_err(),
        StoreError::LeaseOwnerMismatch {
            task_id: retry_record.task_id().clone(),
            worker_id: attacker.clone(),
        }
    );

    let retried = store
        .retry_task(
            retry_record.task_id(),
            &retry_lease.lease_id,
            retry_lease.worker_id.as_ref().unwrap(),
        )
        .unwrap();
    assert_eq!(retried.status, TaskStatus::Pending);

    let dead_record = TaskRecord::new(
        TaskId::new("manual-dead-owner-task").unwrap(),
        TaskStatus::Pending,
        Some(IdempotencyKey::new("idem-manual-dead-owner").unwrap()),
    );
    store.accept_task(dead_record.clone()).unwrap();
    let dead_lease = lease(dead_record.task_id(), "manual-dead-owner-lease", "owner");
    store
        .lease_task(dead_record.task_id(), dead_lease.clone())
        .unwrap();

    assert_eq!(
        store
            .dead_letter_task(
                dead_record.task_id(),
                &dead_lease.lease_id,
                &attacker,
                "not mine",
            )
            .unwrap_err(),
        StoreError::LeaseOwnerMismatch {
            task_id: dead_record.task_id().clone(),
            worker_id: attacker,
        }
    );

    let dead = store
        .dead_letter_task(
            dead_record.task_id(),
            &dead_lease.lease_id,
            dead_lease.worker_id.as_ref().unwrap(),
            "still broken",
        )
        .unwrap();
    assert_eq!(dead.status, TaskStatus::Failed);
    assert!(dead.dead_lettered);
}

#[tokio::test]
async fn manual_retry_and_dead_letter_require_matching_worker_in_sqlite() {
    let store = temp_store().await;
    let retry_record = task("sqlite-manual-retry-owner-task");
    store.accept_task(retry_record.clone()).await.unwrap();
    let retry_lease = lease(
        retry_record.task_id(),
        "sqlite-manual-retry-owner-lease",
        "owner",
    );
    store
        .lease_task(retry_record.task_id(), retry_lease.clone())
        .await
        .unwrap();
    let attacker = AgentId::new("attacker").unwrap();

    assert_eq!(
        store
            .retry_task(retry_record.task_id(), &retry_lease.lease_id, &attacker)
            .await
            .unwrap_err(),
        StoreError::LeaseOwnerMismatch {
            task_id: retry_record.task_id().clone(),
            worker_id: attacker.clone(),
        }
    );

    let retried = store
        .retry_task(
            retry_record.task_id(),
            &retry_lease.lease_id,
            retry_lease.worker_id.as_ref().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(retried.status, TaskStatus::Pending);

    let dead_record = TaskRecord::new(
        TaskId::new("sqlite-manual-dead-owner-task").unwrap(),
        TaskStatus::Pending,
        Some(IdempotencyKey::new("idem-sqlite-manual-dead-owner").unwrap()),
    );
    store.accept_task(dead_record.clone()).await.unwrap();
    let dead_lease = lease(
        dead_record.task_id(),
        "sqlite-manual-dead-owner-lease",
        "owner",
    );
    store
        .lease_task(dead_record.task_id(), dead_lease.clone())
        .await
        .unwrap();

    assert_eq!(
        store
            .dead_letter_task(
                dead_record.task_id(),
                &dead_lease.lease_id,
                &attacker,
                "not mine",
            )
            .await
            .unwrap_err(),
        StoreError::LeaseOwnerMismatch {
            task_id: dead_record.task_id().clone(),
            worker_id: attacker,
        }
    );

    let dead = store
        .dead_letter_task(
            dead_record.task_id(),
            &dead_lease.lease_id,
            dead_lease.worker_id.as_ref().unwrap(),
            "still broken",
        )
        .await
        .unwrap();
    assert_eq!(dead.status, TaskStatus::Failed);
    assert!(dead.dead_lettered);
}
