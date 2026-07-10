use std::collections::HashMap;

use keryx_core::TaskId;
use keryx_daemon::{KeryxDaemonConfig, KeryxDaemonRpcService, KeryxDaemonRuntime};
use keryx_proto::v1::{
    keryx_daemon_server::KeryxDaemon, CorrelationId, IdempotencyKey, SubmitTaskRequest,
    TaskEnvelope, TaskId as ProtoTaskId, TaskMessage, TaskMessagePart,
};
use keryx_store::SqliteStore;
use prost::Message;
use tempfile::tempdir;
use tonic::Request;

#[tokio::test]
async fn submit_task_retains_complete_envelope_across_restart() {
    let dir = tempdir().unwrap();
    let config = KeryxDaemonConfig::new(dir.path(), 0);
    let db_path = config.db_path();
    let runtime = KeryxDaemonRuntime::startup(config).await.unwrap();
    let store = runtime.store().clone();
    let service = KeryxDaemonRpcService::new(runtime);

    let envelope = TaskEnvelope {
        task_id: Some(ProtoTaskId {
            value: "phase17-envelope".into(),
        }),
        correlation_id: Some(CorrelationId {
            value: "correlation-17".into(),
        }),
        idempotency_key: Some(IdempotencyKey {
            value: "phase17-envelope-idem".into(),
        }),
        status: 1,
        messages: vec![TaskMessage {
            parts: vec![
                TaskMessagePart {
                    media_type: "text/plain".into(),
                    text: "retain this prompt".into(),
                    raw: Vec::new(),
                    metadata: HashMap::from([("part".into(), "text".into())]),
                },
                TaskMessagePart {
                    media_type: "application/octet-stream".into(),
                    text: String::new(),
                    raw: vec![0, 1, 2, 3, 255],
                    metadata: HashMap::from([("part".into(), "raw".into())]),
                },
            ],
            metadata: HashMap::from([("role".into(), "user".into())]),
        }],
        metadata: HashMap::from([
            ("skill".into(), "backend-api".into()),
            ("origin_peer_id".into(), "untrusted-hint".into()),
        ]),
    };

    service
        .submit_task(Request::new(SubmitTaskRequest {
            envelope: Some(envelope.clone()),
        }))
        .await
        .unwrap();

    let task_id = TaskId::new("phase17-envelope").unwrap();
    let stored = store.get_task_envelope(&task_id).await.unwrap();
    assert_eq!(
        TaskEnvelope::decode(stored.encoded_envelope.as_slice()).unwrap(),
        envelope
    );
    assert!(stored.received_at_ms > 0);

    drop(service);
    store.close().await;

    let reopened = SqliteStore::connect(db_path).await.unwrap();
    reopened.migrate().await.unwrap();
    let after_restart = reopened.get_task_envelope(&task_id).await.unwrap();
    assert_eq!(
        TaskEnvelope::decode(after_restart.encoded_envelope.as_slice()).unwrap(),
        envelope
    );
}
