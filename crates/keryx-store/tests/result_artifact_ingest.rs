use keryx_core::{
    origin_result_artifact_id, should_inline, ArtifactMeta, Digest, MediaType, PeerId, TaskId,
    TaskStatus,
};
use keryx_store::{
    OriginResultArtifact, RemoteResultIngestOutcome, RemoteResultTerminalReason, SqliteStore,
    StoreError, TaskEnvelopeRecord, TaskRecord, TaskTransportContextRecord, TerminalResultRecord,
};
use sqlx::SqlitePool;
use tempfile::tempdir;

fn peer(value: &str) -> PeerId {
    PeerId::new(value).unwrap()
}

async fn remote_task(store: &SqliteStore, task_id: &TaskId) {
    let executor = peer("remote-executor");
    store
        .accept_task_with_envelope_and_context(
            TaskRecord::new(task_id.clone(), TaskStatus::Pending, None),
            TaskEnvelopeRecord::new(task_id.clone(), b"envelope".to_vec(), 10),
            TaskTransportContextRecord {
                task_id: task_id.clone(),
                authenticated_sender_peer_id: Some(peer("origin-sender")),
                expected_executor_peer_id: Some(executor),
                destination_peer_id: peer("remote-executor"),
                relay_frame_id: Some("frame-1".to_owned()),
                received_at_ms: 10,
            },
        )
        .await
        .unwrap();
}

fn result(task_id: &TaskId) -> TerminalResultRecord {
    TerminalResultRecord {
        task_id: task_id.clone(),
        encoded_result: b"canonical-descriptor-result".to_vec(),
        terminal_status: TaskStatus::Completed,
        return_peer_id: None,
        executor_peer_id: peer("remote-executor"),
        created_at_ms: 20,
    }
}

fn artifact(task_id: &TaskId, ordinal: u32, content: Vec<u8>) -> OriginResultArtifact {
    OriginResultArtifact {
        ordinal,
        meta: ArtifactMeta {
            artifact_id: origin_result_artifact_id(task_id, ordinal),
            task_id: task_id.clone(),
            digest: Digest::compute(&content),
            media_type: MediaType::new("application/octet-stream"),
            byte_len: content.len() as u64,
            inline: should_inline(content.len() as u64),
            created_at: "2026-08-05T00:00:00Z".to_owned(),
        },
        content,
    }
}

#[tokio::test]
async fn origin_ingest_persists_zero_byte_content_and_descriptor_result_atomically() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let task_id = TaskId::new("origin-zero-byte").unwrap();
    remote_task(&store, &task_id).await;
    let record = result(&task_id);
    let ingest = artifact(&task_id, 0, Vec::new());

    let updated = store
        .apply_remote_result_with_artifacts(
            record.clone(),
            std::slice::from_ref(&ingest),
            &peer("remote-executor"),
            dir.path().join("blobs"),
        )
        .await
        .unwrap();

    assert_eq!(updated.status, TaskStatus::Completed);
    assert_eq!(store.get_terminal_result(&task_id).await.unwrap(), record);
    let artifacts = store.list_artifacts_for_task(&task_id).await.unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].artifact_id, ingest.meta.artifact_id);
    assert_eq!(artifacts[0].byte_len, 0);
    assert_eq!(
        store
            .get_artifact(&ingest.meta.artifact_id, dir.path().join("blobs").as_path())
            .await
            .unwrap()
            .1,
        Vec::<u8>::new()
    );
}

#[tokio::test]
async fn origin_ingest_preserves_binary_multi_artifact_ids_and_order() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let task_id = TaskId::new("origin-binary-many").unwrap();
    remote_task(&store, &task_id).await;
    let first = artifact(&task_id, 0, vec![0, 255, 1, 0, 128]);
    let second = artifact(&task_id, 1, vec![7; 65_537]);
    let blob_dir = dir.path().join("blobs");

    store
        .apply_remote_result_with_artifacts(
            result(&task_id),
            &[first.clone(), second.clone()],
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .get_artifact(&first.meta.artifact_id, &blob_dir)
            .await
            .unwrap()
            .1,
        first.content
    );
    assert_eq!(
        store
            .get_artifact(&second.meta.artifact_id, &blob_dir)
            .await
            .unwrap()
            .1,
        second.content
    );
    let listed = store.list_artifacts_for_task(&task_id).await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed
        .iter()
        .any(|row| row.artifact_id == first.meta.artifact_id));
    assert!(listed
        .iter()
        .any(|row| row.artifact_id == second.meta.artifact_id));
}

#[tokio::test]
async fn origin_ingest_accepts_sparse_content_ordinals_but_rejects_duplicates() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let sparse_task = TaskId::new("origin-sparse-content-ordinals").unwrap();
    remote_task(&store, &sparse_task).await;
    let sparse = artifact(&sparse_task, 1, b"ordinal-one".to_vec());
    store
        .apply_remote_result_with_artifacts(
            result(&sparse_task),
            std::slice::from_ref(&sparse),
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_artifact(&sparse.meta.artifact_id, &blob_dir)
            .await
            .unwrap()
            .1,
        sparse.content
    );

    let duplicate_task = TaskId::new("origin-duplicate-content-ordinals").unwrap();
    remote_task(&store, &duplicate_task).await;
    let first = artifact(&duplicate_task, 1, b"first".to_vec());
    let second = artifact(&duplicate_task, 1, b"second".to_vec());
    assert!(matches!(
        store
            .apply_remote_result_with_artifacts(
                result(&duplicate_task),
                &[first, second],
                &peer("remote-executor"),
                &blob_dir,
            )
            .await,
        Err(StoreError::OriginResultArtifactOrdinalMismatch { .. })
    ));
    assert_rejected_without_mutation(&store, &duplicate_task).await;
}

async fn assert_rejected_without_mutation(store: &SqliteStore, task_id: &TaskId) {
    assert_eq!(
        store.get_task(task_id).await.unwrap().status,
        TaskStatus::Pending
    );
    assert!(store
        .list_artifacts_for_task(task_id)
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        store.get_terminal_result(task_id).await,
        Err(StoreError::TerminalResultNotFound(_))
    ));
}

#[tokio::test]
async fn origin_ingest_accepts_exactly_four_mib_and_rejects_oversize_without_mutation() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let exact_task = TaskId::new("origin-four-mib").unwrap();
    remote_task(&store, &exact_task).await;
    let exact = artifact(&exact_task, 0, vec![9; 4 * 1024 * 1024]);
    store
        .apply_remote_result_with_artifacts(
            result(&exact_task),
            std::slice::from_ref(&exact),
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_task(&exact_task).await.unwrap().status,
        TaskStatus::Completed
    );
    assert_eq!(
        store
            .get_artifact(&exact.meta.artifact_id, &blob_dir)
            .await
            .unwrap()
            .1,
        exact.content
    );

    let oversize_task = TaskId::new("origin-over-four-mib").unwrap();
    remote_task(&store, &oversize_task).await;
    let oversized = artifact(&oversize_task, 0, vec![3; 4 * 1024 * 1024 + 1]);
    let error = store
        .apply_remote_result_with_artifacts(
            result(&oversize_task),
            &[oversized],
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::ArtifactTooLarge { .. }));
    assert_rejected_without_mutation(&store, &oversize_task).await;
}

#[tokio::test]
async fn origin_ingest_rejects_declared_length_digest_and_expected_executor_without_mutation() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");

    let length_task = TaskId::new("origin-length-mismatch").unwrap();
    remote_task(&store, &length_task).await;
    let mut wrong_len = artifact(&length_task, 0, b"bytes".to_vec());
    wrong_len.meta.byte_len += 1;
    assert!(matches!(
        store
            .apply_remote_result_with_artifacts(
                result(&length_task),
                &[wrong_len],
                &peer("remote-executor"),
                &blob_dir,
            )
            .await,
        Err(StoreError::ArtifactLengthMismatch { .. })
    ));
    assert_rejected_without_mutation(&store, &length_task).await;

    let digest_task = TaskId::new("origin-digest-mismatch").unwrap();
    remote_task(&store, &digest_task).await;
    let mut wrong_digest = artifact(&digest_task, 0, b"bytes".to_vec());
    wrong_digest.meta.digest = Digest::compute(b"other");
    assert!(matches!(
        store
            .apply_remote_result_with_artifacts(
                result(&digest_task),
                &[wrong_digest],
                &peer("remote-executor"),
                &blob_dir,
            )
            .await,
        Err(StoreError::DigestMismatch { .. })
    ));
    assert_rejected_without_mutation(&store, &digest_task).await;

    let executor_task = TaskId::new("origin-executor-mismatch").unwrap();
    remote_task(&store, &executor_task).await;
    let mut unexpected = result(&executor_task);
    unexpected.executor_peer_id = peer("unexpected-executor");
    assert!(matches!(
        store
            .apply_remote_result_with_artifacts(
                unexpected,
                &[artifact(&executor_task, 0, b"bytes".to_vec())],
                &peer("unexpected-executor"),
                &blob_dir,
            )
            .await,
        Err(StoreError::RemoteResultExecutorMismatch { .. })
    ));
    assert_rejected_without_mutation(&store, &executor_task).await;
}

#[tokio::test]
async fn origin_ingest_exact_replay_is_idempotent_and_conflicts_do_not_overwrite() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let task_id = TaskId::new("origin-replay-conflict").unwrap();
    remote_task(&store, &task_id).await;
    let record = result(&task_id);
    let original = artifact(&task_id, 0, vec![2; 65_537]);
    store
        .apply_remote_result_with_artifacts(
            record.clone(),
            std::slice::from_ref(&original),
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();
    let event_count = store.events_for_task(&task_id).await.unwrap().len();
    store
        .apply_remote_result_with_artifacts(
            record.clone(),
            std::slice::from_ref(&original),
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();
    assert_eq!(
        store.events_for_task(&task_id).await.unwrap().len(),
        event_count
    );

    let changed_bytes = artifact(&task_id, 0, vec![4; 65_537]);
    assert!(matches!(
        store
            .apply_remote_result_with_artifacts(
                record.clone(),
                &[changed_bytes],
                &peer("remote-executor"),
                &blob_dir,
            )
            .await,
        Err(StoreError::TerminalResultConflict(_))
    ));
    let mut changed_metadata = original.clone();
    changed_metadata.meta.media_type = MediaType::new("application/json");
    assert!(matches!(
        store
            .apply_remote_result_with_artifacts(
                record,
                &[changed_metadata],
                &peer("remote-executor"),
                &blob_dir,
            )
            .await,
        Err(StoreError::TerminalResultConflict(_))
    ));
    let mut changed_ordinal = original.clone();
    changed_ordinal.ordinal = 1;
    changed_ordinal.meta.artifact_id = origin_result_artifact_id(&task_id, 1);
    assert!(matches!(
        store
            .apply_remote_result_with_artifacts(
                result(&task_id),
                &[changed_ordinal],
                &peer("remote-executor"),
                &blob_dir,
            )
            .await,
        Err(StoreError::TerminalResultConflict(_))
    ));
    assert_eq!(
        store.get_terminal_result(&task_id).await.unwrap(),
        result(&task_id)
    );
    assert_eq!(
        store
            .get_artifact(&original.meta.artifact_id, &blob_dir)
            .await
            .unwrap()
            .1,
        original.content
    );
}

#[tokio::test]
async fn origin_ingest_deduplicates_same_digest_across_ids_and_never_uses_metadata_as_a_path() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let task_id = TaskId::new("origin-deduplicated-digest").unwrap();
    remote_task(&store, &task_id).await;
    let bytes = vec![5; 65_537];
    let mut first = artifact(&task_id, 0, bytes.clone());
    let mut second = artifact(&task_id, 1, bytes.clone());
    first.meta.media_type = MediaType::new("../../traversal\\name");
    second.meta.created_at = "/absolute/path/with/separators".to_owned();
    store
        .apply_remote_result_with_artifacts(
            result(&task_id),
            &[first.clone(), second.clone()],
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_artifact(&first.meta.artifact_id, &blob_dir)
            .await
            .unwrap()
            .1,
        bytes
    );
    assert_eq!(
        store
            .get_artifact(&second.meta.artifact_id, &blob_dir)
            .await
            .unwrap()
            .1,
        bytes
    );
    assert!(blob_dir.join(first.meta.digest.as_str()).is_file());
    assert!(!dir.path().join("traversal").exists());
    assert!(!dir.path().join("absolute").exists());
}

#[tokio::test]
async fn origin_ingest_rejects_preexisting_artifact_id_without_overwriting_another_task() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let target_task = TaskId::new("origin-id-target").unwrap();
    let owner_task = TaskId::new("origin-id-owner").unwrap();
    remote_task(&store, &target_task).await;
    store
        .accept_task(TaskRecord::new(
            owner_task.clone(),
            TaskStatus::Pending,
            None,
        ))
        .await
        .unwrap();
    let bytes = b"existing-owner-bytes".to_vec();
    let owner_meta = ArtifactMeta {
        artifact_id: origin_result_artifact_id(&target_task, 0),
        task_id: owner_task.clone(),
        digest: Digest::compute(&bytes),
        media_type: MediaType::new("application/octet-stream"),
        byte_len: bytes.len() as u64,
        inline: should_inline(bytes.len() as u64),
        created_at: "2026-08-05T00:00:00Z".to_owned(),
    };
    store
        .put_artifact(&owner_meta, &bytes, &blob_dir)
        .await
        .unwrap();

    let error = store
        .apply_remote_result_with_artifacts(
            result(&target_task),
            &[artifact(&target_task, 0, b"new-target-bytes".to_vec())],
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::OriginResultArtifactConflict(_)));
    assert_eq!(
        store
            .get_artifact(&owner_meta.artifact_id, &blob_dir)
            .await
            .unwrap()
            .0
            .task_id,
        owner_task
    );
    assert_rejected_without_mutation(&store, &target_task).await;
}

#[tokio::test]
async fn origin_ingest_cleans_prepared_blob_when_terminal_transition_is_rejected() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let task_id = TaskId::new("origin-prepared-blob-cleanup").unwrap();
    remote_task(&store, &task_id).await;
    store
        .transition_task(&task_id, TaskStatus::Running)
        .await
        .unwrap();
    store
        .transition_task(&task_id, TaskStatus::Completed)
        .await
        .unwrap();
    let artifact = artifact(&task_id, 0, vec![8; 65_537]);
    let blob_path = blob_dir.join(artifact.meta.digest.as_str());

    assert!(store
        .apply_remote_result_with_artifacts(
            result(&task_id),
            std::slice::from_ref(&artifact),
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .is_err());
    assert!(!blob_path.exists());
    assert!(store
        .list_artifacts_for_task(&task_id)
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        store.get_terminal_result(&task_id).await,
        Err(StoreError::TerminalResultNotFound(_))
    ));
    assert_eq!(
        store.get_task(&task_id).await.unwrap().status,
        TaskStatus::Completed
    );
}

#[tokio::test]
async fn authenticated_late_result_after_deadline_rejects_artifacts_then_settles_without_mutation()
{
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let task_id = TaskId::new("origin-late-after-deadline").unwrap();
    let mut task = TaskRecord::new(task_id.clone(), TaskStatus::Pending, None);
    task.deadline_ms = Some(15);
    store
        .accept_task_with_envelope_and_context(
            task,
            TaskEnvelopeRecord::new(task_id.clone(), b"envelope".to_vec(), 10),
            TaskTransportContextRecord {
                task_id: task_id.clone(),
                authenticated_sender_peer_id: Some(peer("origin-sender")),
                expected_executor_peer_id: Some(peer("remote-executor")),
                destination_peer_id: peer("remote-executor"),
                relay_frame_id: Some("frame-deadline".to_owned()),
                received_at_ms: 10,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.fail_expired_deadlines(20, None).await.unwrap().len(),
        1
    );
    let before_events = store.events_for_task(&task_id).await.unwrap();
    let late_artifact = artifact(&task_id, 0, b"late bytes".to_vec());

    let rejection = store
        .ingest_remote_result_with_artifacts(
            result(&task_id),
            std::slice::from_ref(&late_artifact),
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        rejection,
        StoreError::RemoteResultTerminalArtifactsRejected {
            reason: RemoteResultTerminalReason::DeadlineExpired,
            ..
        }
    ));

    let outcome = store
        .ingest_remote_result_with_artifacts(
            result(&task_id),
            &[],
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        RemoteResultIngestOutcome::SettledTerminal {
            reason: RemoteResultTerminalReason::DeadlineExpired,
            ..
        }
    ));
    assert_eq!(
        store.get_task(&task_id).await.unwrap().status,
        TaskStatus::Failed
    );
    assert_eq!(
        store.events_for_task(&task_id).await.unwrap(),
        before_events
    );
    assert!(matches!(
        store.get_terminal_result(&task_id).await,
        Err(StoreError::TerminalResultNotFound(_))
    ));
    assert!(store
        .list_artifacts_for_task(&task_id)
        .await
        .unwrap()
        .is_empty());
    assert!(!blob_dir.join(late_artifact.meta.digest.as_str()).exists());
}

#[tokio::test]
async fn authenticated_late_result_after_cancellation_rejects_artifacts_then_preserves_canonical_outcome(
) {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let task_id = TaskId::new("origin-late-after-cancel").unwrap();
    remote_task(&store, &task_id).await;
    let canceled = TerminalResultRecord {
        task_id: task_id.clone(),
        encoded_result: b"canonical-canceled-result".to_vec(),
        terminal_status: TaskStatus::Failed,
        return_peer_id: None,
        executor_peer_id: peer("origin-sender"),
        created_at_ms: 15,
    };
    store
        .cancel_task_with_result(&task_id, None, None, "owner canceled", 15, canceled.clone())
        .await
        .unwrap();
    let before_events = store.events_for_task(&task_id).await.unwrap();
    let late_artifact = artifact(&task_id, 0, b"late bytes".to_vec());

    let rejection = store
        .ingest_remote_result_with_artifacts(
            result(&task_id),
            std::slice::from_ref(&late_artifact),
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        rejection,
        StoreError::RemoteResultTerminalArtifactsRejected {
            reason: RemoteResultTerminalReason::Canceled,
            ..
        }
    ));

    let outcome = store
        .ingest_remote_result_with_artifacts(
            result(&task_id),
            &[],
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        RemoteResultIngestOutcome::SettledTerminal {
            reason: RemoteResultTerminalReason::Canceled,
            ..
        }
    ));
    assert_eq!(
        store.get_task(&task_id).await.unwrap().status,
        TaskStatus::Failed
    );
    assert_eq!(
        store.events_for_task(&task_id).await.unwrap(),
        before_events
    );
    assert_eq!(store.get_terminal_result(&task_id).await.unwrap(), canceled);
    assert!(store
        .list_artifacts_for_task(&task_id)
        .await
        .unwrap()
        .is_empty());
    assert!(!blob_dir.join(late_artifact.meta.digest.as_str()).exists());
}

#[tokio::test]
async fn duplicate_remote_result_is_reported_as_idempotent() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let blob_dir = dir.path().join("blobs");
    let task_id = TaskId::new("origin-typed-duplicate").unwrap();
    remote_task(&store, &task_id).await;
    let record = result(&task_id);

    let first = store
        .ingest_remote_result_with_artifacts(
            record.clone(),
            &[],
            &peer("remote-executor"),
            &blob_dir,
        )
        .await
        .unwrap();
    let before_events = store.events_for_task(&task_id).await.unwrap();
    let duplicate = store
        .ingest_remote_result_with_artifacts(record, &[], &peer("remote-executor"), &blob_dir)
        .await
        .unwrap();

    assert!(matches!(first, RemoteResultIngestOutcome::Applied(_)));
    assert!(matches!(duplicate, RemoteResultIngestOutcome::Duplicate(_)));
    assert_eq!(
        store.events_for_task(&task_id).await.unwrap(),
        before_events
    );
}

#[tokio::test]
async fn unauthorized_late_result_is_not_terminally_settled() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let task_id = TaskId::new("origin-unauthorized-late").unwrap();
    let mut task = TaskRecord::new(task_id.clone(), TaskStatus::Pending, None);
    task.deadline_ms = Some(15);
    store
        .accept_task_with_envelope_and_context(
            task,
            TaskEnvelopeRecord::new(task_id.clone(), b"envelope".to_vec(), 10),
            TaskTransportContextRecord {
                task_id: task_id.clone(),
                authenticated_sender_peer_id: Some(peer("origin-sender")),
                expected_executor_peer_id: Some(peer("remote-executor")),
                destination_peer_id: peer("remote-executor"),
                relay_frame_id: Some("frame-unauthorized".to_owned()),
                received_at_ms: 10,
            },
        )
        .await
        .unwrap();
    store.fail_expired_deadlines(20, None).await.unwrap();

    let error = store
        .ingest_remote_result_with_artifacts(
            result(&task_id),
            &[],
            &peer("forged-executor"),
            dir.path().join("blobs"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        StoreError::RemoteResultExecutorMismatch { .. }
    ));
}

#[tokio::test]
async fn deadline_task_with_unexpected_exact_canonical_result_fails_closed() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("keryx.db");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    let task_id = TaskId::new("deadline-corrupt-duplicate").unwrap();
    let mut task = TaskRecord::new(task_id.clone(), TaskStatus::Pending, None);
    task.deadline_ms = Some(15);
    store
        .accept_task_with_envelope_and_context(
            task,
            TaskEnvelopeRecord::new(task_id.clone(), b"envelope".to_vec(), 10),
            TaskTransportContextRecord {
                task_id: task_id.clone(),
                authenticated_sender_peer_id: Some(peer("origin-sender")),
                expected_executor_peer_id: Some(peer("remote-executor")),
                destination_peer_id: peer("origin-sender"),
                relay_frame_id: Some("frame-corrupt-duplicate".to_owned()),
                received_at_ms: 10,
            },
        )
        .await
        .unwrap();
    store.fail_expired_deadlines(20, None).await.unwrap();
    let record = result(&task_id);
    let pool = SqlitePool::connect(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_terminal_results (task_id, encoded_result, terminal_status, return_peer_id, executor_peer_id, created_at_ms) VALUES (?, ?, ?, NULL, ?, ?)")
        .bind(task_id.as_str())
        .bind(&record.encoded_result)
        .bind("completed")
        .bind(record.executor_peer_id.as_str())
        .bind(record.created_at_ms)
        .execute(&pool)
        .await
        .unwrap();

    let error = store
        .ingest_remote_result_with_artifacts(
            record,
            &[],
            &peer("remote-executor"),
            dir.path().join("blobs"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, StoreError::TerminalResultConflict(_)));
    assert_eq!(
        store.get_task(&task_id).await.unwrap().status,
        TaskStatus::Failed
    );
}
