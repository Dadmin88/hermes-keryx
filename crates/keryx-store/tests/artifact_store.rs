use std::{path::PathBuf, str::FromStr};

use keryx_core::{
    should_inline, ArtifactId, ArtifactMeta, Digest, MediaType, TaskId, TaskStatus, MAX_BLOB_BYTES,
    MAX_INLINE_ARTIFACT_BYTES,
};
use keryx_store::{InMemoryStore, SqliteStore, StoreError, TaskRecord, TaskStore};
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, Row};
use tempfile::tempdir;

fn task(id: &str) -> TaskRecord {
    TaskRecord::new(TaskId::new(id).unwrap(), TaskStatus::Pending, None)
}

fn artifact_meta(
    task_id: &TaskId,
    artifact_id: &str,
    bytes: &[u8],
    media_type: &str,
) -> ArtifactMeta {
    ArtifactMeta {
        artifact_id: ArtifactId::new(artifact_id).unwrap(),
        task_id: task_id.clone(),
        digest: Digest::compute(bytes),
        media_type: MediaType::new(media_type),
        byte_len: bytes.len() as u64,
        inline: should_inline(bytes.len() as u64),
        created_at: "2026-07-03T00:00:00Z".to_owned(),
    }
}

async fn temp_sqlite_store() -> (SqliteStore, PathBuf, PathBuf) {
    let dir = tempdir().unwrap().keep();
    let db_path = dir.join("keryx.db");
    let blob_dir = dir.join("blobs");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    (store, db_path, blob_dir)
}

async fn connect_pool(db_path: &std::path::Path, create_if_missing: bool) -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(create_if_missing);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap()
}

async fn blob_ref_count(db_path: &std::path::Path, digest: &Digest) -> Option<i64> {
    let pool = connect_pool(db_path, false).await;
    let row = sqlx::query("SELECT ref_count FROM blobs WHERE digest = ?")
        .bind(digest.as_str())
        .fetch_optional(&pool)
        .await
        .unwrap();
    row.map(|row| row.get::<i64, _>("ref_count"))
}

async fn table_exists(db_path: &std::path::Path, table_name: &str) -> bool {
    let pool = connect_pool(db_path, false).await;
    sqlx::query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1")
        .bind(table_name)
        .fetch_optional(&pool)
        .await
        .unwrap()
        .is_some()
}

async fn seed_schema_v3_database(db_path: &std::path::Path) {
    let pool = connect_pool(db_path, true).await;
    sqlx::query(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO schema_migrations (version, name) VALUES (1, 'initial')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO schema_migrations (version, name) VALUES (2, 'lease_worker_identity')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO schema_migrations (version, name) VALUES (3, 'task_retry_dead_letter')",
    )
    .execute(&pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn sqlite_round_trips_inline_artifacts_and_lists_them() {
    let (store, _db_path, blob_dir) = temp_sqlite_store().await;
    let task = task("artifact-sqlite-inline");
    store.accept_task(task.clone()).await.unwrap();
    let bytes = b"inline artifact bytes";
    let meta = artifact_meta(task.task_id(), "artifact-inline-1", bytes, "text/plain");

    let stored = store.put_artifact(&meta, bytes, &blob_dir).await.unwrap();
    let (fetched, fetched_bytes) = store
        .get_artifact(&meta.artifact_id, &blob_dir)
        .await
        .unwrap();
    let listed = store.list_artifacts_for_task(task.task_id()).await.unwrap();

    assert!(stored.inline);
    assert_eq!(stored, fetched);
    assert_eq!(fetched_bytes, bytes);
    assert_eq!(listed, vec![stored.clone()]);
    assert!(!blob_dir.join(stored.digest.as_str()).exists());
}

#[tokio::test]
async fn sqlite_deduplicates_blob_files_and_cleans_them_up() {
    let (store, db_path, blob_dir) = temp_sqlite_store().await;
    let task = task("artifact-sqlite-blob");
    store.accept_task(task.clone()).await.unwrap();
    let bytes = vec![7_u8; MAX_INLINE_ARTIFACT_BYTES + 1];
    let meta_one = artifact_meta(
        task.task_id(),
        "artifact-blob-1",
        &bytes,
        "application/octet-stream",
    );
    let meta_two = artifact_meta(
        task.task_id(),
        "artifact-blob-2",
        &bytes,
        "application/octet-stream",
    );

    let first = store
        .put_artifact(&meta_one, &bytes, &blob_dir)
        .await
        .unwrap();
    let second = store
        .put_artifact(&meta_two, &bytes, &blob_dir)
        .await
        .unwrap();
    let blob_path = blob_dir.join(first.digest.as_str());

    assert!(!first.inline);
    assert_eq!(first.digest, second.digest);
    assert!(blob_path.exists());
    assert_eq!(blob_ref_count(&db_path, &first.digest).await, Some(2));

    store
        .delete_artifact(&meta_one.artifact_id, &blob_dir)
        .await
        .unwrap();
    assert!(blob_path.exists());
    assert_eq!(blob_ref_count(&db_path, &first.digest).await, Some(1));

    store
        .delete_artifact(&meta_two.artifact_id, &blob_dir)
        .await
        .unwrap();
    assert!(!blob_path.exists());
    assert_eq!(blob_ref_count(&db_path, &first.digest).await, None);
}

#[tokio::test]
#[cfg(unix)]
async fn sqlite_blob_put_rejects_preexisting_symlink() {
    use std::os::unix::fs::symlink;

    let (store, _db_path, blob_dir) = temp_sqlite_store().await;
    let task = task("artifact-symlink-task");
    store.accept_task(task.clone()).await.unwrap();
    let bytes = vec![8_u8; MAX_INLINE_ARTIFACT_BYTES + 1];
    let meta = artifact_meta(
        task.task_id(),
        "artifact-symlink-write",
        &bytes,
        "application/octet-stream",
    );
    std::fs::create_dir_all(&blob_dir).unwrap();
    let victim = blob_dir.parent().unwrap().join("victim.txt");
    std::fs::write(&victim, b"do not overwrite").unwrap();
    symlink(&victim, blob_dir.join(meta.digest.as_str())).unwrap();

    let error = store
        .put_artifact(&meta, &bytes, &blob_dir)
        .await
        .unwrap_err();

    assert!(matches!(error, StoreError::Database(_)));
    assert_eq!(std::fs::read(&victim).unwrap(), b"do not overwrite");
}

#[tokio::test]
async fn sqlite_blob_get_rejects_tampered_content() {
    let (store, _db_path, blob_dir) = temp_sqlite_store().await;
    let task = task("artifact-tampered-task");
    store.accept_task(task.clone()).await.unwrap();
    let bytes = vec![9_u8; MAX_INLINE_ARTIFACT_BYTES + 1];
    let meta = artifact_meta(
        task.task_id(),
        "artifact-tampered-read",
        &bytes,
        "application/octet-stream",
    );
    let stored = store.put_artifact(&meta, &bytes, &blob_dir).await.unwrap();
    let tampered = vec![10_u8; bytes.len()];
    std::fs::write(blob_dir.join(stored.digest.as_str()), tampered).unwrap();

    let error = store
        .get_artifact(&meta.artifact_id, &blob_dir)
        .await
        .unwrap_err();

    assert!(matches!(error, StoreError::DigestMismatch { .. }));
}

#[tokio::test]
async fn sqlite_put_artifact_rejects_unknown_task_digest_mismatch_and_size_limit() {
    let (store, _db_path, blob_dir) = temp_sqlite_store().await;
    let task_id = TaskId::new("missing-artifact-task").unwrap();
    let bytes = b"content";
    let meta = artifact_meta(&task_id, "artifact-missing-task", bytes, "text/plain");

    let missing_task = store
        .put_artifact(&meta, bytes, &blob_dir)
        .await
        .unwrap_err();
    assert!(matches!(missing_task, StoreError::TaskNotFound(_)));

    let task = task("present-artifact-task");
    store.accept_task(task.clone()).await.unwrap();
    let mut bad_meta = artifact_meta(task.task_id(), "artifact-bad-digest", bytes, "text/plain");
    bad_meta.digest = Digest::compute(b"different");

    let mismatch = store
        .put_artifact(&bad_meta, bytes, &blob_dir)
        .await
        .unwrap_err();
    assert!(matches!(mismatch, StoreError::DigestMismatch { .. }));

    let oversize_bytes = vec![0_u8; MAX_BLOB_BYTES + 1];
    let oversize_meta = artifact_meta(
        task.task_id(),
        "artifact-too-large",
        &oversize_bytes,
        "application/octet-stream",
    );
    let oversize = store
        .put_artifact(&oversize_meta, &oversize_bytes, &blob_dir)
        .await
        .unwrap_err();
    assert!(matches!(
        oversize,
        StoreError::ArtifactTooLarge {
            byte_len: _,
            limit_bytes: _
        }
    ));
}

#[tokio::test]
async fn sqlite_missing_artifact_reads_and_deletes_return_not_found() {
    let (store, _db_path, blob_dir) = temp_sqlite_store().await;
    let artifact_id = ArtifactId::new("artifact-missing").unwrap();

    let get_error = store
        .get_artifact(&artifact_id, &blob_dir)
        .await
        .unwrap_err();
    assert!(matches!(get_error, StoreError::ArtifactNotFound(_)));

    let delete_error = store
        .delete_artifact(&artifact_id, &blob_dir)
        .await
        .unwrap_err();
    assert!(matches!(delete_error, StoreError::ArtifactNotFound(_)));
}

#[tokio::test]
async fn sqlite_delete_inline_artifact_removes_metadata() {
    let (store, _db_path, blob_dir) = temp_sqlite_store().await;
    let task = task("artifact-delete-inline");
    store.accept_task(task.clone()).await.unwrap();
    let bytes = b"inline delete me";
    let meta = artifact_meta(
        task.task_id(),
        "artifact-inline-delete",
        bytes,
        "text/plain",
    );

    let stored = store.put_artifact(&meta, bytes, &blob_dir).await.unwrap();
    assert!(stored.inline);

    store
        .delete_artifact(&meta.artifact_id, &blob_dir)
        .await
        .unwrap();
    assert!(store
        .list_artifacts_for_task(task.task_id())
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        store
            .get_artifact(&meta.artifact_id, &blob_dir)
            .await
            .unwrap_err(),
        StoreError::ArtifactNotFound(_)
    ));
    assert!(!blob_dir.join(stored.digest.as_str()).exists());
}

#[tokio::test]
async fn sqlite_list_artifacts_for_empty_task_is_empty() {
    let (store, _db_path, _blob_dir) = temp_sqlite_store().await;
    let task = task("artifact-empty-list");
    store.accept_task(task.clone()).await.unwrap();

    assert!(store
        .list_artifacts_for_task(task.task_id())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn sqlite_migration_v4_creates_artifact_tables() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("legacy-v3.db");
    seed_schema_v3_database(&db_path).await;

    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();

    assert_eq!(store.schema_version().await.unwrap(), 4);
    assert!(table_exists(&db_path, "artifacts").await);
    assert!(table_exists(&db_path, "blobs").await);
}

#[tokio::test]
async fn sqlite_duplicate_put_is_idempotent_and_overwrite_replaces_old_blob() {
    let (store, db_path, blob_dir) = temp_sqlite_store().await;
    let task = task("artifact-overwrite-task");
    store.accept_task(task.clone()).await.unwrap();

    let first_bytes = vec![1_u8; MAX_INLINE_ARTIFACT_BYTES + 3];
    let first_meta = artifact_meta(
        task.task_id(),
        "artifact-overwrite",
        &first_bytes,
        "application/octet-stream",
    );
    let first = store
        .put_artifact(&first_meta, &first_bytes, &blob_dir)
        .await
        .unwrap();
    let first_blob = blob_dir.join(first.digest.as_str());

    let duplicate = store
        .put_artifact(&first_meta, &first_bytes, &blob_dir)
        .await
        .unwrap();
    assert_eq!(duplicate.digest, first.digest);
    assert_eq!(blob_ref_count(&db_path, &first.digest).await, Some(1));
    assert_eq!(
        store.list_artifacts_for_task(task.task_id()).await.unwrap(),
        vec![duplicate.clone()]
    );

    let second_bytes = vec![2_u8; MAX_INLINE_ARTIFACT_BYTES + 5];
    let second_meta = artifact_meta(
        task.task_id(),
        "artifact-overwrite",
        &second_bytes,
        "application/octet-stream",
    );
    let second = store
        .put_artifact(&second_meta, &second_bytes, &blob_dir)
        .await
        .unwrap();
    let second_blob = blob_dir.join(second.digest.as_str());
    let (fetched, fetched_bytes) = store
        .get_artifact(&second_meta.artifact_id, &blob_dir)
        .await
        .unwrap();

    assert_eq!(fetched, second);
    assert_eq!(fetched_bytes, second_bytes);
    assert!(!first_blob.exists());
    assert!(second_blob.exists());
    assert_eq!(blob_ref_count(&db_path, &first.digest).await, None);
    assert_eq!(blob_ref_count(&db_path, &second.digest).await, Some(1));
}

#[tokio::test]
async fn sqlite_artifact_task_ownership_moves_across_tasks() {
    let (store, db_path, blob_dir) = temp_sqlite_store().await;
    let first_task = task("artifact-owner-task-1");
    let second_task = task("artifact-owner-task-2");
    store.accept_task(first_task.clone()).await.unwrap();
    store.accept_task(second_task.clone()).await.unwrap();

    let bytes = vec![3_u8; MAX_INLINE_ARTIFACT_BYTES + 9];
    let first_meta = artifact_meta(
        first_task.task_id(),
        "artifact-shared-owner",
        &bytes,
        "application/octet-stream",
    );
    let second_meta = artifact_meta(
        second_task.task_id(),
        "artifact-shared-owner",
        &bytes,
        "application/octet-stream",
    );

    store
        .put_artifact(&first_meta, &bytes, &blob_dir)
        .await
        .unwrap();
    let reassigned = store
        .put_artifact(&second_meta, &bytes, &blob_dir)
        .await
        .unwrap();

    assert!(store
        .list_artifacts_for_task(first_task.task_id())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .list_artifacts_for_task(second_task.task_id())
            .await
            .unwrap(),
        vec![reassigned.clone()]
    );
    assert_eq!(reassigned.task_id, second_task.task_id().clone());
    assert_eq!(blob_ref_count(&db_path, &reassigned.digest).await, Some(1));
}

#[tokio::test]
async fn sqlite_inline_and_blob_threshold_behavior_is_explicit() {
    let (store, _db_path, blob_dir) = temp_sqlite_store().await;
    let task = task("artifact-threshold-task");
    store.accept_task(task.clone()).await.unwrap();

    let inline_bytes = vec![4_u8; MAX_INLINE_ARTIFACT_BYTES];
    let blob_bytes = vec![5_u8; MAX_INLINE_ARTIFACT_BYTES + 1];
    let inline_meta = artifact_meta(
        task.task_id(),
        "artifact-threshold-inline",
        &inline_bytes,
        "text/plain",
    );
    let blob_meta = artifact_meta(
        task.task_id(),
        "artifact-threshold-blob",
        &blob_bytes,
        "application/octet-stream",
    );

    let inline_record = store
        .put_artifact(&inline_meta, &inline_bytes, &blob_dir)
        .await
        .unwrap();
    let blob_record = store
        .put_artifact(&blob_meta, &blob_bytes, &blob_dir)
        .await
        .unwrap();

    assert!(inline_record.inline);
    assert!(!blob_record.inline);
    assert!(!blob_dir.join(inline_record.digest.as_str()).exists());
    assert!(blob_dir.join(blob_record.digest.as_str()).exists());
}

#[tokio::test]
async fn in_memory_store_round_trips_blob_artifacts_and_removes_blob_file_on_delete() {
    let dir = tempdir().unwrap().keep();
    let blob_dir = dir.join("blobs");
    let store = InMemoryStore::default();
    let task = task("artifact-memory-blob");
    store.accept_task(task.clone()).unwrap();
    let bytes = vec![3_u8; MAX_INLINE_ARTIFACT_BYTES + 1];
    let meta = artifact_meta(
        task.task_id(),
        "artifact-memory-1",
        &bytes,
        "application/octet-stream",
    );

    let stored = store.put_artifact(&meta, &bytes, &blob_dir).await.unwrap();
    let (fetched, fetched_bytes) = store
        .get_artifact(&meta.artifact_id, &blob_dir)
        .await
        .unwrap();

    assert!(!stored.inline);
    assert_eq!(stored, fetched);
    assert_eq!(fetched_bytes, bytes);
    assert!(blob_dir.join(stored.digest.as_str()).exists());

    store
        .delete_artifact(&meta.artifact_id, &blob_dir)
        .await
        .unwrap();
    assert!(!blob_dir.join(stored.digest.as_str()).exists());
}
