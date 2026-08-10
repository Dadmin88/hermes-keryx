use std::collections::HashMap;

use keryx_core::{IdempotencyKey, TaskId, TaskStatus};
use keryx_daemon::{KeryxDaemonConfig, KeryxDaemonRpcService, KeryxDaemonRuntime};
use keryx_proto::v1::{
    keryx_daemon_server::KeryxDaemon, AgentId, ClaimNextTaskRequest, SubmitTaskRequest,
    TaskEnvelope, TaskId as ProtoTaskId, TaskMessage, TaskMessagePart,
};
use keryx_store::{TaskEnvelopeRecord, TaskRecord};
use prost::Message;
use tempfile::tempdir;
use tonic::{Code, Request};

const CLAIM_TOKEN: &str = "test-claim-token-123456";

fn envelope(task_id: &str, skill: &str) -> TaskEnvelope {
    TaskEnvelope {
        task_id: Some(ProtoTaskId {
            value: task_id.to_string(),
        }),
        correlation_id: None,
        idempotency_key: Some(keryx_proto::v1::IdempotencyKey {
            value: format!("idem-{task_id}"),
        }),
        status: 1,
        messages: vec![TaskMessage {
            parts: vec![TaskMessagePart {
                media_type: "text/plain".into(),
                text: format!("work for {task_id}"),
                raw: Vec::new(),
                metadata: HashMap::new(),
            }],
            metadata: HashMap::new(),
        }],
        metadata: HashMap::from([("skill".to_string(), skill.to_string())]),
    }
}

fn config(data_dir: impl Into<std::path::PathBuf>) -> KeryxDaemonConfig {
    KeryxDaemonConfig::new(data_dir, 0).with_claim_next_token(Some(CLAIM_TOKEN.to_string()))
}

fn claim_request(worker: &str, skills: &[&str], wait_timeout_ms: i64) -> ClaimNextTaskRequest {
    ClaimNextTaskRequest {
        worker_id: Some(AgentId {
            value: worker.to_string(),
        }),
        accepted_skill_ids: skills.iter().map(|value| (*value).to_string()).collect(),
        accepted_capability_ids: Vec::new(),
        lease_duration_ms: 5_000,
        wait_timeout_ms,
        claim_token: CLAIM_TOKEN.to_string(),
    }
}

async fn direct_accept(
    runtime: &KeryxDaemonRuntime,
    task_id: &str,
    skill: &str,
    received_at_ms: i64,
) {
    let proto = envelope(task_id, skill);
    let record = TaskRecord::new(
        TaskId::new(task_id).unwrap(),
        TaskStatus::Pending,
        Some(IdempotencyKey::new(format!("idem-{task_id}")).unwrap()),
    );
    runtime
        .store()
        .accept_task_with_envelope(
            record,
            TaskEnvelopeRecord::new(
                TaskId::new(task_id).unwrap(),
                proto.encode_to_vec(),
                received_at_ms,
            ),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn claim_next_rejects_missing_claim_token() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(config(dir.path()))
        .await
        .unwrap();
    direct_accept(&runtime, "task-secret", "ops", 1).await;
    let service = KeryxDaemonRpcService::new(runtime);

    let error = service
        .claim_next_task(Request::new(ClaimNextTaskRequest {
            worker_id: Some(AgentId {
                value: "attacker-worker".into(),
            }),
            accepted_skill_ids: Vec::new(),
            accepted_capability_ids: Vec::new(),
            lease_duration_ms: 5_000,
            wait_timeout_ms: 0,
            claim_token: String::new(),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn claim_next_rejects_when_daemon_token_is_not_configured() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path(), 0))
        .await
        .unwrap();
    direct_accept(&runtime, "task-secret", "ops", 1).await;
    let service = KeryxDaemonRpcService::new(runtime);

    let error = service
        .claim_next_task(Request::new(claim_request("worker-a", &["ops"], 0)))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn claim_next_returns_no_work_without_waiting() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(config(dir.path()))
        .await
        .unwrap();
    let service = KeryxDaemonRpcService::new(runtime);
    let response = service
        .claim_next_task(Request::new(claim_request("worker-a", &[], 0)))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.has_task);
    assert!(response.envelope.is_none());
}

#[tokio::test]
async fn claim_next_selects_oldest_matching_envelope() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(config(dir.path()))
        .await
        .unwrap();
    direct_accept(&runtime, "task-later", "backend", 20).await;
    direct_accept(&runtime, "task-design", "design", 5).await;
    direct_accept(&runtime, "task-backend", "backend", 10).await;
    let service = KeryxDaemonRpcService::new(runtime);

    let response = service
        .claim_next_task(Request::new(claim_request(
            "worker-backend",
            &["backend"],
            0,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(response.has_task);
    assert_eq!(response.task_id.unwrap().value, "task-backend");
    assert_eq!(response.envelope.unwrap().metadata["skill"], "backend");
}

#[tokio::test]
async fn concurrent_workers_never_receive_the_same_task() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(config(dir.path()))
        .await
        .unwrap();
    direct_accept(&runtime, "task-race", "backend", 1).await;
    let service = KeryxDaemonRpcService::new(runtime);
    let left = service.clone();
    let right = service.clone();

    let (left, right) = tokio::join!(
        left.claim_next_task(Request::new(claim_request("worker-left", &[], 0))),
        right.claim_next_task(Request::new(claim_request("worker-right", &[], 0))),
    );
    let responses = [left.unwrap().into_inner(), right.unwrap().into_inner()];
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.has_task)
            .count(),
        1
    );
}

#[tokio::test]
async fn long_poll_wakes_after_submit() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(config(dir.path()))
        .await
        .unwrap();
    let service = KeryxDaemonRpcService::new(runtime);
    let waiter = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .claim_next_task(Request::new(claim_request(
                    "worker-waiting",
                    &["research"],
                    2_000,
                )))
                .await
                .unwrap()
                .into_inner()
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    service
        .submit_task(Request::new(SubmitTaskRequest {
            envelope: Some(envelope("task-wakeup", "research")),
        }))
        .await
        .unwrap();

    let response = tokio::time::timeout(std::time::Duration::from_secs(3), waiter)
        .await
        .unwrap()
        .unwrap();
    assert!(response.has_task);
    assert_eq!(response.task_id.unwrap().value, "task-wakeup");
}

#[tokio::test]
async fn stale_claim_is_recoverable_and_can_be_claimed_again() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(config(dir.path()))
        .await
        .unwrap();
    direct_accept(&runtime, "task-recover", "ops", 1).await;
    let store = runtime.store().clone();
    let service = KeryxDaemonRpcService::new(runtime);

    let first = service
        .claim_next_task(Request::new(ClaimNextTaskRequest {
            worker_id: Some(AgentId {
                value: "worker-first".into(),
            }),
            accepted_skill_ids: vec!["ops".into()],
            accepted_capability_ids: Vec::new(),
            lease_duration_ms: 1,
            wait_timeout_ms: 0,
            claim_token: CLAIM_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(first.has_task);
    store.recover_stale_leases(i64::MAX, None).await.unwrap();

    let second = service
        .claim_next_task(Request::new(claim_request("worker-second", &["ops"], 0)))
        .await
        .unwrap()
        .into_inner();
    assert!(second.has_task);
    assert_eq!(second.task_id.unwrap().value, "task-recover");
}
