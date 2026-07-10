#!/usr/bin/env python3
from pathlib import Path

path = Path("scripts/phase17_apply.py")
text = path.read_text(encoding="utf-8")

helper_anchor = '''        Ok(())
    }

    fn insert_terminal_result_in_state(
'''
helper_replacement = '''        Ok(())
    }

    fn terminal_results_equivalent(
        left: &TaskResultRecord,
        right: &TaskResultRecord,
    ) -> bool {
        left.task_id == right.task_id
            && left.status == right.status
            && left.encoded_result == right.encoded_result
            && left.producer_node_id == right.producer_node_id
            && left.origin_node_id == right.origin_node_id
    }

    fn insert_terminal_result_in_state(
'''
if text.count(helper_anchor) != 1:
    raise SystemExit("expected terminal-result helper anchor exactly once")
text = text.replace(helper_anchor, helper_replacement, 1)

replacements = {
    "return if existing == &result {": "return if terminal_results_equivalent(existing, &result) {",
    "if existing == &result {": "if terminal_results_equivalent(existing, &result) {",
    "Some(result) if existing == result =>": "Some(result) if terminal_results_equivalent(existing, result) =>",
    "if existing == result {": "if terminal_results_equivalent(&existing, &result) {",
    "Some(result) if &existing == result =>": "Some(result) if terminal_results_equivalent(&existing, result) =>",
    "return if existing == *result {": "return if terminal_results_equivalent(&existing, result) {",
}
for old, new in replacements.items():
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected exactly one result-equivalence site for {old!r}, found {count}")
    text = text.replace(old, new, 1)

complete_timestamp = '''            producer_node_id: self.runtime.config().local_peer_id().as_str().to_string(),
            completed_at_ms,
        };
'''
complete_timestamp_fixed = '''            producer_node_id: self.runtime.config().local_peer_id().as_str().to_string(),
            completed_at_ms: 0,
        };
'''
if text.count(complete_timestamp) != 1:
    raise SystemExit("expected one completion payload timestamp")
text = text.replace(complete_timestamp, complete_timestamp_fixed, 1)

failure_timestamp = '''            producer_node_id: self.runtime.config().local_peer_id().as_str().to_string(),
            completed_at_ms,
        });
'''
failure_timestamp_fixed = '''            producer_node_id: self.runtime.config().local_peer_id().as_str().to_string(),
            completed_at_ms: 0,
        });
'''
if text.count(failure_timestamp) != 1:
    raise SystemExit("expected one failure payload timestamp")
text = text.replace(failure_timestamp, failure_timestamp_fixed, 1)

old_daemon_import_generator = '''replace_once(
    DAEMON,
    "    SendTaskRequest, SendTaskResponse, StatusRequest, StatusResponse, SubmitTaskRequest,\n    SubmitTaskResponse, TaskEnvelope, TaskId as ProtoTaskId,\n",
    "    SendTaskRequest, SendTaskResponse, StatusRequest, StatusResponse, SubmitTaskRequest,\n"
    "    SubmitTaskResponse, TaskEnvelope, TaskId as ProtoTaskId, TerminalTaskResult,\n",
)
'''
new_daemon_import_generator = '''replace_once(
    DAEMON,
    "    PutArtifactResponse, ReadinessRequest, ReadinessResponse, SendTaskRequest, SendTaskResponse,\\n"
    "    StatusRequest, StatusResponse, SubmitTaskRequest, SubmitTaskResponse, TaskEnvelope,\\n"
    "    TaskId as ProtoTaskId,\\n",
    "    PutArtifactResponse, ReadinessRequest, ReadinessResponse, SendTaskRequest, SendTaskResponse,\\n"
    "    StatusRequest, StatusResponse, SubmitTaskRequest, SubmitTaskResponse, TaskEnvelope,\\n"
    "    TaskId as ProtoTaskId, TerminalTaskResult,\\n",
)
'''
if text.count(old_daemon_import_generator) != 1:
    raise SystemExit("expected old daemon import generator exactly once")
text = text.replace(old_daemon_import_generator, new_daemon_import_generator, 1)

idempotency_marker = '''    #[tokio::test]
    async fn local_completion_persists_result_without_outbox() {
'''
idempotency_test = '''    #[tokio::test]
    async fn identical_completion_retry_is_idempotent_across_timestamp_changes() {
        let dir = tempdir().unwrap();
        let store = SqliteStore::connect(dir.path().join("keryx.db"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let (task_id, lease_id, worker_id) = running(&store, "result-idempotent").await;
        let original = result("result-idempotent", TaskStatus::Completed, b"same-result");
        store
            .complete_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                original.clone(),
                Some("node-origin"),
            )
            .await
            .unwrap();
        let retry = TaskResultRecord::new(
            task_id.clone(),
            TaskStatus::Completed,
            b"same-result".to_vec(),
            "node-producer",
            "node-origin",
            999,
        );
        let completed = store
            .complete_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                retry,
                Some("node-origin"),
            )
            .await
            .unwrap();
        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(store.get_task_result(&task_id).await.unwrap(), original);
        assert_eq!(store.pending_result_deliveries().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn local_completion_persists_result_without_outbox() {
'''
if text.count(idempotency_marker) != 1:
    raise SystemExit("expected local completion test marker exactly once")
text = text.replace(idempotency_marker, idempotency_test, 1)

fixture_start = "# Existing test fixtures that construct InMemoryState explicitly need the new maps.\n"
fixture_end = "# ---------------------------------------------------------------------------\n# Daemon integration. Only the reserved authenticated-sender key creates outbox.\n"
if text.count(fixture_start) != 1 or text.count(fixture_end) != 1:
    raise SystemExit("expected exactly one bounded fixture generator section")
start = text.index(fixture_start)
end = text.index(fixture_end, start)
fixture_patch = '''# Existing test fixtures that construct InMemoryState explicitly need the new maps.
fixture_path = Path(STORE)
fixture_text = fixture_path.read_text(encoding="utf-8")
fixture_old = "            envelopes: HashMap::new(),\\n        };"
fixture_new = (
    "            envelopes: HashMap::new(),\\n"
    "            results: HashMap::new(),\\n"
    "            result_outbox: HashMap::new(),\\n"
    "        };"
)
fixture_count = fixture_text.count(fixture_old)
if fixture_count != 3:
    raise SystemExit(f"{STORE}: expected 3 explicit InMemoryState fixtures, found {fixture_count}")
fixture_path.write_text(fixture_text.replace(fixture_old, fixture_new), encoding="utf-8")

'''
text = text[:start] + fixture_patch + text[end:]

path.write_text(text, encoding="utf-8")
