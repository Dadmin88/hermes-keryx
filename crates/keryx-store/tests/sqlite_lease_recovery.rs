use std::{path::PathBuf, str::FromStr};

use keryx_core::{
    AgentId, IdempotencyKey, KeryxEventType, LeaseId, RetryPolicy, TaskId, TaskStatus,
};
use keryx_store::{LeaseRecord, SqliteStore, TaskRecord};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::tempdir;

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

async fn temp_store() -> SqliteStore {
    temp_store_with_path().await.0
}

async fn temp_store_with_path() -> (SqliteStore, PathBuf) {
    let dir = tempdir().unwrap().keep();
    let db_path = dir.join("keryx.db");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    (store, db_path)
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
            lease(record.task_id(), "sqlite-lease-1", "sqlite-worker-1", 1_000),
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
    let lease = lease(record.task_id(), "sqlite-lease-2", "sqlite-worker-2", 500);
    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();

    let renewed = store
        .renew_lease(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
            400,
            900,
        )
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
            lease(first.task_id(), "sqlite-lease-a", "sqlite-worker-a", 500),
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
            lease(second.task_id(), "sqlite-lease-b", "sqlite-worker-b", 500),
        )
        .await
        .unwrap();

    let recovered = store.recover_stale_leases(501, None).await.unwrap();

    assert_eq!(
        recovered
            .recovered_tasks
            .iter()
            .map(|task| task.task_id().as_str())
            .collect::<Vec<_>>(),
        vec!["sqlite-lease-task-a", "sqlite-lease-task-b"]
    );
    assert_eq!(recovered.cleaned_terminal_leases, 0);
    assert_eq!(recovered.corruption_count(), 0);
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
    let lease = lease(record.task_id(), "sqlite-lease-3", "sqlite-worker-3", 500);
    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    store
        .complete_task(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
        )
        .await
        .unwrap();

    let recovered = store.recover_stale_leases(501, None).await.unwrap();

    assert!(recovered.recovered_tasks.is_empty());
    assert_eq!(recovered.cleaned_terminal_leases, 0);
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

#[tokio::test]
async fn sqlite_recovery_cleans_terminal_stale_lease_without_changing_terminal_status() {
    let (store, db_path) = temp_store_with_path().await;
    let record = task(
        "sqlite-terminal-stale-lease-task",
        TaskStatus::Pending,
        Some("sqlite-terminal-stale-lease-idem"),
    );
    store.accept_task(record.clone()).await.unwrap();
    let lease = lease(
        record.task_id(),
        "sqlite-terminal-stale-lease",
        "sqlite-terminal-stale-worker",
        500,
    );
    store
        .lease_task(record.task_id(), lease.clone())
        .await
        .unwrap();
    store
        .complete_task(
            record.task_id(),
            &lease.lease_id,
            lease.worker_id.as_ref().unwrap(),
        )
        .await
        .unwrap();
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("UPDATE leases SET active = 1, expires_at_ms = 500 WHERE task_id = ?")
        .bind(record.task_id().as_str())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let recovered = store.recover_stale_leases(501, None).await.unwrap();

    assert!(recovered.recovered_tasks.is_empty());
    assert_eq!(recovered.cleaned_terminal_leases, 1);
    assert_eq!(recovered.corruption_count(), 0);
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
    let last = events.last().unwrap();
    assert_eq!(last.event_type, KeryxEventType::RecoveryAction);
    assert_eq!(last.from_status, Some(TaskStatus::Completed));
    assert_eq!(last.to_status, TaskStatus::Completed);
}

#[tokio::test]
async fn sqlite_replay_reports_missing_events_as_corruption() {
    let (store, db_path) = temp_store_with_path().await;
    let record = task(
        "sqlite-missing-events-task",
        TaskStatus::Pending,
        Some("sqlite-missing-events-idem"),
    );
    store.accept_task(record.clone()).await.unwrap();
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_events WHERE task_id = ?")
        .bind(record.task_id().as_str())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    assert_eq!(
        store.events_for_task(record.task_id()).await.unwrap_err(),
        keryx_store::StoreError::CorruptEventStream(record.task_id().clone())
    );
    assert_eq!(
        store.replay_task(record.task_id()).await.unwrap_err(),
        keryx_store::StoreError::CorruptEventStream(record.task_id().clone())
    );
    let report = store.recover_stale_leases(0, None).await.unwrap();
    assert_eq!(report.corrupted_tasks, vec![record.task_id().clone()]);
}

#[tokio::test]
async fn sqlite_recovery_limit_is_applied_after_deterministic_ordering() {
    let store = temp_store().await;

    let first = task(
        "sqlite-lease-task-limit-a",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-limit-a"),
    );
    store.accept_task(first.clone()).await.unwrap();
    store
        .lease_task(
            first.task_id(),
            lease(
                first.task_id(),
                "sqlite-lease-limit-a",
                "sqlite-worker-limit-a",
                500,
            ),
        )
        .await
        .unwrap();

    let second = task(
        "sqlite-lease-task-limit-b",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-limit-b"),
    );
    store.accept_task(second.clone()).await.unwrap();
    store
        .lease_task(
            second.task_id(),
            lease(
                second.task_id(),
                "sqlite-lease-limit-b",
                "sqlite-worker-limit-b",
                400,
            ),
        )
        .await
        .unwrap();

    let recovered = store.recover_stale_leases(501, Some(1)).await.unwrap();

    assert_eq!(recovered.recovered_tasks, vec![second.clone()]);
    assert_eq!(
        store.get_task(first.task_id()).await.unwrap().status,
        TaskStatus::Running
    );
    assert_eq!(
        store.get_task(second.task_id()).await.unwrap().status,
        TaskStatus::Pending
    );
    assert_eq!(
        store
            .active_lease(first.task_id())
            .await
            .unwrap()
            .unwrap()
            .lease_id
            .as_str(),
        "sqlite-lease-limit-a"
    );
    assert!(store
        .active_lease(second.task_id())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sqlite_stale_tokens_are_rejected_after_recovery_and_reissue() {
    let store = temp_store().await;
    let record = task(
        "sqlite-lease-task-4",
        TaskStatus::Pending,
        Some("sqlite-lease-idem-4"),
    );
    store.accept_task(record.clone()).await.unwrap();
    let first_lease = lease(record.task_id(), "sqlite-lease-4a", "sqlite-worker-4a", 500);
    store
        .lease_task(record.task_id(), first_lease.clone())
        .await
        .unwrap();

    let recovered = store.recover_stale_leases(501, None).await.unwrap();
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
            .await
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
            .await
            .unwrap_err(),
        keryx_store::StoreError::LeaseNotFound(record.task_id().clone())
    );
    assert_eq!(
        store
            .fail_task(
                record.task_id(),
                &first_lease.lease_id,
                first_lease.worker_id.as_ref().unwrap(),
                "",
                &RetryPolicy::no_retries(),
            )
            .await
            .unwrap_err(),
        keryx_store::StoreError::LeaseNotFound(record.task_id().clone())
    );

    let second_lease = lease(
        record.task_id(),
        "sqlite-lease-4b",
        "sqlite-worker-4b",
        1_000,
    );
    store
        .lease_task(record.task_id(), second_lease.clone())
        .await
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
            .await
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
            .await
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
                "",
                &RetryPolicy::no_retries(),
            )
            .await
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
        .await
        .unwrap();
    assert_eq!(completed.status, TaskStatus::Completed);
}
