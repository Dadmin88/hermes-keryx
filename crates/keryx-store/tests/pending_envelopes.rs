use keryx_core::{AgentId, IdempotencyKey, LeaseId, PeerId, TaskId, TaskStatus};
use keryx_store::{
    LeaseRecord, SqliteStore, StoreError, TaskEnvelopeRecord, TaskRecord,
    TaskTransportContextRecord,
};
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

#[tokio::test]
async fn claimable_pending_envelopes_exclude_other_peer_targets() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let local = PeerId::new("peer-local").unwrap();
    let remote = PeerId::new("peer-remote").unwrap();

    let remote_task = task("remote-only");
    store
        .accept_task_with_envelope_and_context(
            remote_task.clone(),
            envelope("remote-only", 1),
            TaskTransportContextRecord {
                task_id: remote_task.task_id().clone(),
                authenticated_sender_peer_id: Some(local.clone()),
                expected_executor_peer_id: Some(remote.clone()),
                destination_peer_id: remote,
                relay_frame_id: Some("relay-frame-remote".to_string()),
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();
    store
        .accept_task_with_envelope(task("local-claimable"), envelope("local-claimable", 2))
        .await
        .unwrap();

    let claimable = store
        .claimable_pending_task_envelopes(&local, 10)
        .await
        .unwrap();
    assert_eq!(claimable.len(), 1);
    assert_eq!(claimable[0].task.task_id().as_str(), "local-claimable");
    assert_eq!(store.pending_task_envelopes(10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn peer_guard_and_lease_transition_are_atomic() {
    let dir = tempdir().unwrap();
    let store = SqliteStore::connect(dir.path().join("keryx.db"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let local = PeerId::new("peer-local").unwrap();
    let remote = PeerId::new("peer-remote").unwrap();
    let record = task("remote-lease-guard");
    store
        .accept_task_with_envelope_and_context(
            record.clone(),
            envelope("remote-lease-guard", 1),
            TaskTransportContextRecord {
                task_id: record.task_id().clone(),
                authenticated_sender_peer_id: Some(local.clone()),
                expected_executor_peer_id: Some(remote.clone()),
                destination_peer_id: remote.clone(),
                relay_frame_id: None,
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();

    let wrong_lease = LeaseRecord::new(
        LeaseId::new("wrong-peer-lease").unwrap(),
        record.task_id().clone(),
        AgentId::new("worker-local").unwrap(),
        10,
        100,
    );
    let error = store
        .lease_task_for_peer(record.task_id(), wrong_lease, &local)
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::TaskExecutorMismatch { .. }));
    assert_eq!(
        store.get_task(record.task_id()).await.unwrap().status,
        TaskStatus::Pending
    );
    assert!(store
        .active_lease(record.task_id())
        .await
        .unwrap()
        .is_none());

    let correct_lease = LeaseRecord::new(
        LeaseId::new("right-peer-lease").unwrap(),
        record.task_id().clone(),
        AgentId::new("worker-remote").unwrap(),
        10,
        100,
    );
    assert_eq!(
        store
            .lease_task_for_peer(record.task_id(), correct_lease, &remote)
            .await
            .unwrap()
            .status,
        TaskStatus::Running
    );
}

#[tokio::test]
async fn relay_receipt_survives_restart_and_new_delivery_replaces_stale_generation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("keryx.db");
    let local = PeerId::new("peer-local").unwrap();
    let remote = PeerId::new("peer-remote").unwrap();
    let record = task("durable-relay-receipt");
    let store = SqliteStore::connect(&db_path).await.unwrap();
    store.migrate().await.unwrap();
    store
        .accept_task_with_envelope_and_context(
            record.clone(),
            envelope("durable-relay-receipt", 1),
            TaskTransportContextRecord {
                task_id: record.task_id().clone(),
                authenticated_sender_peer_id: Some(local.clone()),
                expected_executor_peer_id: Some(remote.clone()),
                destination_peer_id: remote.clone(),
                relay_frame_id: None,
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();
    store
        .record_relay_receipt(record.task_id(), &local, &remote, "relay-receipt-1", 42)
        .await
        .unwrap();
    store.close().await;

    let reopened = SqliteStore::connect(&db_path).await.unwrap();
    reopened.migrate().await.unwrap();
    let context = reopened
        .get_transport_context(record.task_id())
        .await
        .unwrap();
    assert_eq!(context.relay_frame_id.as_deref(), Some("relay-receipt-1"));
    assert_eq!(context.received_at_ms, 42);
    reopened
        .record_relay_receipt(record.task_id(), &local, &remote, "relay-receipt-1", 42)
        .await
        .unwrap();
    reopened
        .record_relay_receipt(record.task_id(), &local, &remote, "relay-receipt-2", 43)
        .await
        .unwrap();
    let refreshed = reopened
        .get_transport_context(record.task_id())
        .await
        .unwrap();
    assert_eq!(refreshed.relay_frame_id.as_deref(), Some("relay-receipt-2"));
    assert_eq!(refreshed.received_at_ms, 43);
}
