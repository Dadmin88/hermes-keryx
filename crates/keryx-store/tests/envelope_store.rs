use keryx_core::{IdempotencyKey, TaskId, TaskStatus};
use keryx_store::{
    InMemoryStore, SqliteStore, StoreError, TaskEnvelopeRecord, TaskRecord, TaskStore,
    CURRENT_SCHEMA_VERSION,
};
use tempfile::tempdir;

fn task(id: &str, idempotency_key: &str) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        TaskStatus::Pending,
        Some(IdempotencyKey::new(idempotency_key).unwrap()),
    )
}

fn envelope(id: &str, bytes: &[u8]) -> TaskEnvelopeRecord {
    TaskEnvelopeRecord::new(TaskId::new(id).unwrap(), bytes.to_vec(), 1_726_000_000_000)
}

#[test]
fn in_memory_accepts_task_and_envelope_atomically() {
    let store = InMemoryStore::default();
    let task = task("memory-envelope", "memory-envelope-idem");
    let stored_envelope = envelope("memory-envelope", b"complete-envelope");

    let accepted = store
        .accept_task_with_envelope(task.clone(), stored_envelope.clone())
        .unwrap();

    assert_eq!(accepted, task);
    assert_eq!(
        store.get_task_envelope(task.task_id()).unwrap(),
        stored_envelope
    );
    assert_eq!(
        store
            .accept_task_with_envelope(task.clone(), stored_envelope.clone())
            .unwrap(),
        task
    );

    let conflict = store
        .accept_task_with_envelope(task.clone(), envelope("memory-envelope", b"different"))
        .unwrap_err();
    assert_eq!(
        conflict,
        StoreError::TaskEnvelopeConflict(task.task_id().clone())
    );
}

#[test]
fn in_memory_rejects_mismatched_ids_without_accepting_task() {
    let store = InMemoryStore::default();
    let task = task("memory-task", "memory-task-idem");

    let error = store
        .accept_task_with_envelope(task.clone(), envelope("different-task", b"payload"))
        .unwrap_err();

    assert_eq!(
        error,
        StoreError::TaskEnvelopeMismatch {
            task_id: task.task_id().clone(),
            envelope_task_id: TaskId::new("different-task").unwrap(),
        }
    );
    assert_eq!(
        store.get_task(task.task_id()).unwrap_err(),
        StoreError::TaskNotFound(task.task_id().clone())
    );
}

#[tokio::test]
async fn sqlite_envelope_survives_reopen_and_schema_is_v7() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("keryx.sqlite3");
    let task = task("sqlite-envelope", "sqlite-envelope-idem");
    let stored_envelope = envelope("sqlite-envelope", b"nested protobuf envelope bytes");

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    assert_eq!(
        store.schema_version().await.unwrap(),
        CURRENT_SCHEMA_VERSION
    );
    assert_eq!(CURRENT_SCHEMA_VERSION, 7);
    store
        .accept_task_with_envelope(task.clone(), stored_envelope.clone())
        .await
        .unwrap();
    store.close().await;

    let reopened = SqliteStore::connect(&db_path).await.unwrap();
    reopened.migrate().await.unwrap();
    assert_eq!(
        reopened.get_task_envelope(task.task_id()).await.unwrap(),
        stored_envelope
    );
    assert_eq!(reopened.get_task(task.task_id()).await.unwrap(), task);
}

#[tokio::test]
async fn sqlite_idempotent_retry_requires_identical_envelope() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("keryx.sqlite3");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let task = task("sqlite-idempotent", "sqlite-idempotent-key");
    let stored_envelope = envelope("sqlite-idempotent", b"original");

    store
        .accept_task_with_envelope(task.clone(), stored_envelope.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .accept_task_with_envelope(task.clone(), stored_envelope.clone())
            .await
            .unwrap(),
        task
    );

    let error = store
        .accept_task_with_envelope(task.clone(), envelope("sqlite-idempotent", b"changed"))
        .await
        .unwrap_err();
    assert_eq!(
        error,
        StoreError::TaskEnvelopeConflict(task.task_id().clone())
    );
}

#[tokio::test]
async fn sqlite_mismatch_rolls_back_lifecycle_acceptance() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("keryx.sqlite3");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let task = task("sqlite-rollback", "sqlite-rollback-key");

    let error = store
        .accept_task_with_envelope(task.clone(), envelope("wrong-task", b"payload"))
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::TaskEnvelopeMismatch { .. }));
    assert_eq!(
        store.get_task(task.task_id()).await.unwrap_err(),
        StoreError::TaskNotFound(task.task_id().clone())
    );
    assert_eq!(
        store.get_task_envelope(task.task_id()).await.unwrap_err(),
        StoreError::TaskEnvelopeNotFound(task.task_id().clone())
    );
}

#[tokio::test]
async fn legacy_lifecycle_only_acceptance_still_works() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("keryx.sqlite3");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let task = task("legacy-lifecycle", "legacy-lifecycle-key");

    assert_eq!(store.accept_task(task.clone()).await.unwrap(), task);
    assert_eq!(
        store.get_task_envelope(task.task_id()).await.unwrap_err(),
        StoreError::TaskEnvelopeNotFound(task.task_id().clone())
    );
}
