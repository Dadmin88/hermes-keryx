use keryx_core::{IdempotencyKey, TaskId, TaskStatus};
use keryx_store::{SqliteStore, TaskEnvelopeRecord, TaskRecord};
use tempfile::tempdir;

fn task(id: &str) -> TaskRecord {
    TaskRecord::new(
        TaskId::new(id).unwrap(),
        TaskStatus::Pending,
        Some(IdempotencyKey::new(format!("idem-{id}")).unwrap()),
    )
}

fn envelope(id: &str, received_at_ms: i64) -> TaskEnvelopeRecord {
    TaskEnvelopeRecord::new(
        TaskId::new(id).unwrap(),
        format!("envelope-{id}").into_bytes(),
        received_at_ms,
    )
}

#[tokio::test]
async fn pending_envelopes_are_deterministic_and_exclude_lifecycle_only_tasks() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();

    store
        .accept_task_with_envelope(task("task-later"), envelope("task-later", 20))
        .await
        .unwrap();
    store
        .accept_task_with_envelope(task("task-first-b"), envelope("task-first-b", 10))
        .await
        .unwrap();
    store
        .accept_task_with_envelope(task("task-first-a"), envelope("task-first-a", 10))
        .await
        .unwrap();
    store.accept_task(task("lifecycle-only")).await.unwrap();

    let pending = store.pending_task_envelopes(2).await.unwrap();
    let ids = pending
        .iter()
        .map(|item| item.task.task_id().as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["task-first-a", "task-first-b"]);
}
