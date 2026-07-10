#!/usr/bin/env python3
"""Apply the Phase 17.2 ClaimNextTask implementation with strict source anchors."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:160]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


def insert_before(path: str, marker: str, addition: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    if addition.strip() in text:
        return
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"{path}: expected one marker, found {count}: {marker[:160]!r}")
    file.write_text(text.replace(marker, addition.rstrip() + "\n\n" + marker, 1), encoding="utf-8")


def write_new(path: str, content: str) -> None:
    file = Path(path)
    if file.exists():
        raise SystemExit(f"{path}: file already exists")
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(dedent(content).lstrip(), encoding="utf-8")


PROTO = "proto/hermes/keryx/v1/daemon.proto"
STORE = "crates/keryx-store/src/lib.rs"
DAEMON = "crates/keryx-daemon/src/lib.rs"
INCOMING = "crates/keryx-daemon/src/incoming.rs"
MODELS = "sdk/python/keryx/models.py"
NODE = "sdk/python/keryx/node.py"
SDK_INIT = "sdk/python/keryx/__init__.py"
PRODUCT = "docs/current-product.md"

# --- Protocol surface ---
replace_once(
    PROTO,
    "  rpc ClaimTask(ClaimTaskRequest) returns (ClaimTaskResponse);\n",
    "  rpc ClaimTask(ClaimTaskRequest) returns (ClaimTaskResponse);\n"
    "  rpc ClaimNextTask(ClaimNextTaskRequest) returns (ClaimNextTaskResponse);\n",
)

insert_before(
    PROTO,
    "message HeartbeatRequest {",
    dedent(
        """
        message ClaimNextTaskRequest {
          AgentId worker_id = 1;
          repeated string accepted_skill_ids = 2;
          repeated string accepted_capability_ids = 3;
          // When zero, the daemon applies a default lease TTL.
          int64 lease_duration_ms = 4;
          // Zero returns immediately. Positive values are bounded by the daemon.
          int64 wait_timeout_ms = 5;
        }

        message ClaimNextTaskResponse {
          bool has_task = 1;
          TaskEnvelope envelope = 2;
          TaskId task_id = 3;
          LeaseId lease_id = 4;
          AgentId worker_id = 5;
          int64 leased_at_ms = 6;
          int64 expires_at_ms = 7;
          string status = 8;
          uint32 retry_count = 9;
          bool dead_lettered = 10;
          // Empty until the relay/edge authenticated identity contract is persisted.
          string sender_peer_id = 11;
        }
        """
    ),
)

# --- Store pending-envelope view ---
insert_before(
    STORE,
    "#[derive(Debug, Clone, PartialEq, Eq)]\npub struct TaskEventRecord {",
    dedent(
        """
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct PendingTaskEnvelope {
            pub task: TaskRecord,
            pub envelope: TaskEnvelopeRecord,
        }
        """
    ),
)

insert_before(
    STORE,
    "    pub fn lease_task(&self, task_id: &TaskId, lease: LeaseRecord) -> StoreResult<TaskRecord> {",
    dedent(
        """
            pub fn pending_task_envelopes(
                &self,
                limit: usize,
            ) -> StoreResult<Vec<PendingTaskEnvelope>> {
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let state = self.lock()?;
                let mut pending = state
                    .tasks
                    .values()
                    .filter(|task| task.status == TaskStatus::Pending)
                    .filter_map(|task| {
                        state.envelopes.get(task.task_id()).map(|envelope| PendingTaskEnvelope {
                            task: task.clone(),
                            envelope: envelope.clone(),
                        })
                    })
                    .collect::<Vec<_>>();
                pending.sort_by(|left, right| {
                    left.envelope
                        .received_at_ms
                        .cmp(&right.envelope.received_at_ms)
                        .then_with(|| left.task.task_id().as_str().cmp(right.task.task_id().as_str()))
                });
                pending.truncate(limit);
                Ok(pending)
            }
        """
    ),
)

insert_before(
    STORE,
    "    pub async fn accept_task(&self, task: TaskRecord) -> StoreResult<TaskRecord> {",
    dedent(
        """
            pub async fn pending_task_envelopes(
                &self,
                limit: usize,
            ) -> StoreResult<Vec<PendingTaskEnvelope>> {
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let rows = sqlx::query(
                    "SELECT t.task_id, t.status, t.idempotency_key, t.retry_count, t.dead_lettered, t.dead_letter_reason, t.deadline_ms, e.encoded_envelope, e.received_at_ms \
                     FROM tasks t INNER JOIN task_envelopes e ON e.task_id = t.task_id \
                     WHERE t.status = 'pending' \
                     ORDER BY e.received_at_ms ASC, t.task_id ASC LIMIT ?",
                )
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?;
                rows.into_iter().map(row_to_pending_task_envelope).collect()
            }
        """
    ),
)

insert_before(
    STORE,
    "fn row_to_lease(row: sqlx::sqlite::SqliteRow) -> StoreResult<LeaseRecord> {",
    dedent(
        """
        fn row_to_pending_task_envelope(
            row: sqlx::sqlite::SqliteRow,
        ) -> StoreResult<PendingTaskEnvelope> {
            let task_id = TaskId::new(row.get::<String, _>("task_id"))?;
            let status = str_to_status(&row.get::<String, _>("status"))?;
            let idempotency_key = row
                .try_get::<Option<String>, _>("idempotency_key")?
                .map(IdempotencyKey::new)
                .transpose()?;
            let task = TaskRecord {
                id: task_id.clone(),
                status,
                idempotency_key,
                deadline_ms: row.try_get::<Option<i64>, _>("deadline_ms")?,
                retry_count: row
                    .try_get::<Option<i64>, _>("retry_count")?
                    .unwrap_or(0)
                    .max(0) as u32,
                dead_lettered: row
                    .try_get::<Option<i64>, _>("dead_lettered")?
                    .unwrap_or(0)
                    != 0,
                dead_letter_reason: row.try_get::<Option<String>, _>("dead_letter_reason")?,
            };
            Ok(PendingTaskEnvelope {
                task,
                envelope: TaskEnvelopeRecord {
                    task_id,
                    encoded_envelope: row.get::<Vec<u8>, _>("encoded_envelope"),
                    received_at_ms: row.get::<i64, _>("received_at_ms"),
                },
            })
        }
        """
    ),
)

# --- Daemon runtime, notification, and RPC ---
replace_once(
    DAEMON,
    "use std::path::{Path, PathBuf};\n",
    "use std::collections::HashSet;\nuse std::path::{Path, PathBuf};\n",
)
replace_once(
    DAEMON,
    "    CancelTaskResponse, ClaimTaskRequest, ClaimTaskResponse, CompleteTaskRequest,\n",
    "    CancelTaskResponse, ClaimNextTaskRequest, ClaimNextTaskResponse, ClaimTaskRequest,\n"
    "    ClaimTaskResponse, CompleteTaskRequest,\n",
)
replace_once(
    DAEMON,
    "    SubmitTaskResponse, TaskId as ProtoTaskId,\n",
    "    SubmitTaskResponse, TaskEnvelope, TaskId as ProtoTaskId,\n",
)
replace_once(
    DAEMON,
    "use tokio::sync::{Mutex, RwLock};\n",
    "use tokio::sync::{Mutex, Notify, RwLock};\n",
)
replace_once(
    DAEMON,
    "    LeaseRecord, RecoveryReport, SqliteStore, StoreError, StoreResult, TaskEnvelopeRecord,\n    TaskRecord, CURRENT_SCHEMA_VERSION,\n",
    "    LeaseRecord, PendingTaskEnvelope, RecoveryReport, SqliteStore, StoreError, StoreResult,\n"
    "    TaskEnvelopeRecord, TaskRecord, CURRENT_SCHEMA_VERSION,\n",
)

insert_before(
    DAEMON,
    "/// Default background health probe interval.",
    dedent(
        """
        const CLAIM_NEXT_SCAN_LIMIT: usize = 256;
        const MAX_CLAIM_NEXT_WAIT_MS: u64 = 30_000;
        """
    ),
)

replace_once(
    DAEMON,
    "    cancellation: Arc<CancellationState>,\n}",
    "    cancellation: Arc<CancellationState>,\n    task_available: Arc<Notify>,\n}",
)
replace_once(
    DAEMON,
    "            cancellation: Arc::new(CancellationState::new()),\n",
    "            cancellation: Arc::new(CancellationState::new()),\n"
    "            task_available: Arc::new(Notify::new()),\n",
)

replace_once(
    DAEMON,
    dedent(
        """
            pub async fn accept_pending_task_with_backpressure(
                &self,
                record: TaskRecord,
                envelope_bytes: u64,
            ) -> StoreResult<TaskRecord> {
                self.config
                    .limits()
                    .check_envelope_bytes(envelope_bytes)
                    .map_err(|error| StoreError::Validation(error.into()))?;
                let _submit_backpressure_guard = self.submit_backpressure_lock.lock().await;
                let pending_count = self
                    .store
                    .count_tasks_by_status(TaskStatus::Pending)
                    .await?;
                self.config
                    .limits()
                    .check_pending_tasks(pending_count)
                    .map_err(|error| StoreError::Validation(error.into()))?;
                self.store.accept_task(record).await
            }
        """
    ),
    dedent(
        """
            pub async fn accept_pending_task_with_backpressure(
                &self,
                record: TaskRecord,
                envelope_bytes: u64,
            ) -> StoreResult<TaskRecord> {
                self.config
                    .limits()
                    .check_envelope_bytes(envelope_bytes)
                    .map_err(|error| StoreError::Validation(error.into()))?;
                let _submit_backpressure_guard = self.submit_backpressure_lock.lock().await;
                let pending_count = self
                    .store
                    .count_tasks_by_status(TaskStatus::Pending)
                    .await?;
                self.config
                    .limits()
                    .check_pending_tasks(pending_count)
                    .map_err(|error| StoreError::Validation(error.into()))?;
                let accepted = self.store.accept_task(record).await?;
                self.task_available.notify_waiters();
                Ok(accepted)
            }

            pub async fn accept_pending_task_with_envelope_backpressure(
                &self,
                record: TaskRecord,
                envelope: TaskEnvelopeRecord,
            ) -> StoreResult<TaskRecord> {
                self.config
                    .limits()
                    .check_envelope_bytes(envelope.encoded_envelope.len() as u64)
                    .map_err(|error| StoreError::Validation(error.into()))?;
                let _submit_backpressure_guard = self.submit_backpressure_lock.lock().await;
                let pending_count = self
                    .store
                    .count_tasks_by_status(TaskStatus::Pending)
                    .await?;
                self.config
                    .limits()
                    .check_pending_tasks(pending_count)
                    .map_err(|error| StoreError::Validation(error.into()))?;
                let accepted = self
                    .store
                    .accept_task_with_envelope(record, envelope)
                    .await?;
                self.task_available.notify_waiters();
                Ok(accepted)
            }
        """
    ),
)

insert_before(
    DAEMON,
    "}\n\n/// Serve the minimal local daemon RPC surface used by the CLI readiness client.",
    dedent(
        """
            async fn try_claim_next_task(
                &self,
                worker_id: &AgentId,
                accepted_skill_ids: &HashSet<String>,
                accepted_capability_ids: &HashSet<String>,
                lease_duration_ms: i64,
            ) -> Result<Option<ClaimNextTaskResponse>, Status> {
                let candidates = self
                    .runtime
                    .store()
                    .pending_task_envelopes(CLAIM_NEXT_SCAN_LIMIT)
                    .await
                    .map_err(store_error_to_status)?;
                for candidate in candidates {
                    let envelope = TaskEnvelope::decode(
                        candidate.envelope.encoded_envelope.as_slice(),
                    )
                    .map_err(|error| {
                        Status::data_loss(format!(
                            "stored envelope for task {} is invalid: {error}",
                            candidate.task.task_id().as_str()
                        ))
                    })?;
                    if !envelope_matches_claim_filters(
                        &envelope,
                        accepted_skill_ids,
                        accepted_capability_ids,
                    ) {
                        continue;
                    }

                    let leased_at_ms = unix_ms_now();
                    let expires_at_ms = leased_at_ms.saturating_add(lease_duration_ms);
                    let lease_id = new_lease_id(candidate.task.task_id(), leased_at_ms);
                    let lease = LeaseRecord::new(
                        lease_id.clone(),
                        candidate.task.task_id().clone(),
                        worker_id.clone(),
                        leased_at_ms,
                        expires_at_ms,
                    );
                    match self
                        .runtime
                        .store()
                        .lease_task(candidate.task.task_id(), lease)
                        .await
                    {
                        Ok(task) => {
                            self.runtime.metrics().increment_tasks_claimed();
                            return Ok(Some(ClaimNextTaskResponse {
                                has_task: true,
                                envelope: Some(envelope),
                                task_id: Some(proto_task_id(task.task_id())),
                                lease_id: Some(proto_lease_id(&lease_id)),
                                worker_id: Some(proto_agent_id(worker_id)),
                                leased_at_ms,
                                expires_at_ms,
                                status: task_status_label(task.status).to_string(),
                                retry_count: task.retry_count,
                                dead_lettered: task.dead_lettered,
                                sender_peer_id: String::new(),
                            }));
                        }
                        Err(StoreError::LeaseConflict { .. } | StoreError::TaskNotFound(_)) => {
                            continue;
                        }
                        Err(StoreError::Validation(
                            ValidationError::InvalidTaskTransition { .. },
                        )) => {
                            continue;
                        }
                        Err(error) => return Err(store_error_to_status(error)),
                    }
                }
                Ok(None)
            }
        """
    ),
)

# SubmitTask now goes through the notification-aware runtime method.
replace_once(
    DAEMON,
    dedent(
        """
                let _submit_backpressure_guard = self.runtime.submit_backpressure_lock.lock().await;
                let pending_count = self
                    .runtime
                    .store()
                    .count_tasks_by_status(TaskStatus::Pending)
                    .await
                    .map_err(store_error_to_status)?;
                self.runtime
                    .config()
                    .limits()
                    .check_pending_tasks(pending_count)
                    .map_err(limit_exceeded_to_status)?;
                let accepted = self
                    .runtime
                    .store()
                    .accept_task_with_envelope(record, envelope_record)
                    .await
                    .map_err(store_error_to_status)?;
        """
    ),
    dedent(
        """
                let accepted = self
                    .runtime
                    .accept_pending_task_with_envelope_backpressure(record, envelope_record)
                    .await
                    .map_err(store_error_to_status)?;
        """
    ),
)

insert_before(
    DAEMON,
    "    #[instrument(\n        name = \"keryx::rpc::heartbeat\"",
    dedent(
        """
            #[instrument(
                name = "keryx::rpc::claim_next_task",
                skip(self, request),
                fields(worker_id = tracing::field::Empty)
            )]
            async fn claim_next_task(
                &self,
                request: Request<ClaimNextTaskRequest>,
            ) -> Result<Response<ClaimNextTaskResponse>, Status> {
                let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
                let inner = request.into_inner();
                let worker_id = parse_required_agent_id(inner.worker_id.as_ref())?;
                tracing::Span::current()
                    .record("worker_id", tracing::field::display(worker_id.as_str()));
                let accepted_skill_ids = normalized_filter_set(inner.accepted_skill_ids);
                let accepted_capability_ids =
                    normalized_filter_set(inner.accepted_capability_ids);
                let lease_duration_ms =
                    normalize_lease_duration_ms(inner.lease_duration_ms, self.runtime.config());
                let wait_timeout_ms = normalize_claim_wait_ms(inner.wait_timeout_ms);
                let deadline =
                    tokio::time::Instant::now() + Duration::from_millis(wait_timeout_ms);

                loop {
                    let notified = self.runtime.task_available.notified();
                    if let Some(response) = self
                        .try_claim_next_task(
                            &worker_id,
                            &accepted_skill_ids,
                            &accepted_capability_ids,
                            lease_duration_ms,
                        )
                        .await?
                    {
                        return Ok(Response::new(response));
                    }
                    if wait_timeout_ms == 0 || tokio::time::Instant::now() >= deadline {
                        return Ok(Response::new(empty_claim_next_response()));
                    }
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(deadline) => {
                            return Ok(Response::new(empty_claim_next_response()));
                        }
                        _ = self.runtime.shutdown.grpc_shutdown_wait() => {
                            return Err(Status::unavailable("daemon is shutting down"));
                        }
                    }
                }
            }
        """
    ),
)

insert_before(
    DAEMON,
    "fn unix_ms_now() -> i64 {",
    dedent(
        """
        fn normalized_filter_set(values: Vec<String>) -> HashSet<String> {
            values
                .into_iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect()
        }

        fn metadata_matches_any(
            envelope: &TaskEnvelope,
            keys: &[&str],
            accepted: &HashSet<String>,
        ) -> bool {
            if accepted.is_empty() {
                return true;
            }
            keys.iter()
                .filter_map(|key| envelope.metadata.get(*key))
                .flat_map(|value| value.split(','))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .any(|value| accepted.contains(value))
        }

        fn envelope_matches_claim_filters(
            envelope: &TaskEnvelope,
            accepted_skill_ids: &HashSet<String>,
            accepted_capability_ids: &HashSet<String>,
        ) -> bool {
            metadata_matches_any(
                envelope,
                &["skill", "skill_id", "target_skill_id", "skills"],
                accepted_skill_ids,
            ) && metadata_matches_any(
                envelope,
                &[
                    "capability",
                    "capability_id",
                    "target_capability_id",
                    "capabilities",
                ],
                accepted_capability_ids,
            )
        }

        fn normalize_claim_wait_ms(wait_timeout_ms: i64) -> u64 {
            if wait_timeout_ms <= 0 {
                0
            } else {
                (wait_timeout_ms as u64).min(MAX_CLAIM_NEXT_WAIT_MS)
            }
        }

        fn empty_claim_next_response() -> ClaimNextTaskResponse {
            ClaimNextTaskResponse {
                has_task: false,
                envelope: None,
                task_id: None,
                lease_id: None,
                worker_id: None,
                leased_at_ms: 0,
                expires_at_ms: 0,
                status: String::new(),
                retry_count: 0,
                dead_lettered: false,
                sender_peer_id: String::new(),
            }
        }
        """
    ),
)

# Relay-delivered tasks also retain the envelope and wake workers.
replace_once(
    INCOMING,
    "use keryx_store::{LeaseRecord, StoreError, StoreResult, TaskRecord};\n",
    "use keryx_store::{LeaseRecord, StoreError, StoreResult, TaskEnvelopeRecord, TaskRecord};\n",
)
replace_once(
    INCOMING,
    "    let envelope_bytes = incoming.envelope.encoded_len() as u64;\n\n    let record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);\n    let accepted = match runtime\n        .accept_pending_task_with_backpressure(record, envelope_bytes)\n",
    "    let encoded_envelope = incoming.envelope.encode_to_vec();\n\n"
    "    let record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);\n"
    "    let envelope_record =\n"
    "        TaskEnvelopeRecord::new(task_id.clone(), encoded_envelope, unix_ms_now());\n"
    "    let accepted = match runtime\n"
    "        .accept_pending_task_with_envelope_backpressure(record, envelope_record)\n",
)

# --- Python native claim-next surface ---
replace_once(
    MODELS,
    "class TaskResult:\n",
    dedent(
        """
        class ClaimedTask:
            """A task atomically dequeued from the daemon for worker execution."""

            has_task: bool
            task_id: str = ""
            lease_id: str = ""
            worker_id: str = ""
            leased_at_ms: int = 0
            expires_at_ms: int = 0
            status: str = ""
            retry_count: int = 0
            dead_lettered: bool = False
            sender_peer_id: str = ""
            envelope: Any | None = None

            @classmethod
            def from_proto(cls, response: daemon_pb2.ClaimNextTaskResponse) -> "ClaimedTask":
                envelope = response.envelope if response.has_task and response.HasField("envelope") else None
                return cls(
                    has_task=response.has_task,
                    task_id=_id_value(response.task_id),
                    lease_id=_id_value(response.lease_id),
                    worker_id=_id_value(response.worker_id),
                    leased_at_ms=response.leased_at_ms,
                    expires_at_ms=response.expires_at_ms,
                    status=response.status,
                    retry_count=response.retry_count,
                    dead_lettered=response.dead_lettered,
                    sender_peer_id=response.sender_peer_id,
                    envelope=envelope,
                )


        @dataclass(slots=True)
        class TaskResult:
        """
    ),
)
# The replacement above consumes the existing decorator; repair the exact neighboring block.
replace_once(
    MODELS,
    "@dataclass(slots=True)\n@dataclass(slots=True)\nclass ClaimedTask:",
    "@dataclass(slots=True)\nclass ClaimedTask:",
)

replace_once(
    NODE,
    "from keryx.models import TaskArtifact, TaskResult, TaskState\n",
    "from keryx.models import ClaimedTask, TaskArtifact, TaskResult, TaskState\n",
)
insert_before(
    NODE,
    "    async def heartbeat(\n",
    dedent(
        """
            async def claim_next(
                self,
                *,
                worker_id: str | None = None,
                accepted_skill_ids: Sequence[str] | None = None,
                accepted_capability_ids: Sequence[str] | None = None,
                lease_duration_ms: int | None = None,
                wait_timeout_ms: int = 0,
            ) -> ClaimedTask:
                daemon = await self._daemon()
                worker = self._resolve_worker_id(worker_id)
                response = await daemon.ClaimNextTask(
                    daemon_pb2.ClaimNextTaskRequest(
                        worker_id=common_pb2.AgentId(value=worker),
                        accepted_skill_ids=list(accepted_skill_ids or []),
                        accepted_capability_ids=list(accepted_capability_ids or []),
                        lease_duration_ms=(
                            self._config.default_lease_duration_ms
                            if lease_duration_ms is None
                            else lease_duration_ms
                        ),
                        wait_timeout_ms=wait_timeout_ms,
                    )
                )
                return ClaimedTask.from_proto(response)

            async def claim_next_task(self, **kwargs: Any) -> ClaimedTask:
                return await self.claim_next(**kwargs)
        """
    ),
)
replace_once(
    SDK_INIT,
    "from keryx.models import TaskArtifact, TaskResult, TaskState\n",
    "from keryx.models import ClaimedTask, TaskArtifact, TaskResult, TaskState\n",
)
replace_once(
    SDK_INIT,
    '    "TaskState",\n',
    '    "TaskState",\n    "ClaimedTask",\n',
)

# --- Product truth ---
replace_once(
    PRODUCT,
    "- worker lifecycle: `SubmitTask`, `ClaimTask`, `Heartbeat`, `CompleteTask`, `FailTask`, `CancelTask`\n",
    "- worker lifecycle: `SubmitTask`, `ClaimTask`, `ClaimNextTask`, `Heartbeat`, `CompleteTask`, `FailTask`, `CancelTask`\n",
)
replace_once(
    PRODUCT,
    "Phase 17.1 is complete at the storage/daemon layer: relay-delivered envelopes can survive destination-daemon restart without losing their task messages or context.\n",
    "Phase 17.1 retains complete envelopes durably. Phase 17.2 adds atomic worker dequeue through `ClaimNextTask`, with deterministic selection, exact skill/capability filters, bounded long polling, and lease-safe concurrent claims.\n",
)
replace_once(
    PRODUCT,
    "- an atomic claim-next or pending-task delivery API for workers\n",
    "- Python `serve_forever()` consumption of the available `ClaimNextTask` worker API\n",
)
replace_once(
    PRODUCT,
    "Native daemon lifecycle methods include `connect`, `status`, `doctor`, `peers`, `skills`, `submit`, `claim`, `heartbeat`, `complete`, `fail`, and `cancel`. Compatibility helpers include `start`, `stop`, `discover`, `send_task`, `register_skills`, `deregister_skills`, and `serve_forever`.\n",
    "Native daemon lifecycle methods include `connect`, `status`, `doctor`, `peers`, `skills`, `submit`, `claim`, `claim_next`, `heartbeat`, `complete`, `fail`, and `cancel`. Compatibility helpers include `start`, `stop`, `discover`, `send_task`, `register_skills`, `deregister_skills`, and `serve_forever`.\n",
)

# --- Rust tests ---
write_new(
    "crates/keryx-store/tests/pending_envelopes.rs",
    r'''
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
    ''',
)

write_new(
    "crates/keryx-daemon/tests/claim_next_task.rs",
    r'''
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
    use tonic::Request;

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

    fn claim_request(worker: &str, skills: &[&str], wait_timeout_ms: i64) -> ClaimNextTaskRequest {
        ClaimNextTaskRequest {
            worker_id: Some(AgentId {
                value: worker.to_string(),
            }),
            accepted_skill_ids: skills.iter().map(|value| (*value).to_string()).collect(),
            accepted_capability_ids: Vec::new(),
            lease_duration_ms: 5_000,
            wait_timeout_ms,
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
    async fn claim_next_returns_no_work_without_waiting() {
        let dir = tempdir().unwrap();
        let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path(), 0))
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
        let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path(), 0))
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
        let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path(), 0))
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
        assert_eq!(responses.iter().filter(|response| response.has_task).count(), 1);
    }

    #[tokio::test]
    async fn long_poll_wakes_after_submit() {
        let dir = tempdir().unwrap();
        let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path(), 0))
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
        let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path(), 0))
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
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(first.has_task);
        store.recover_stale_leases(i64::MAX, None).await.unwrap();

        let second = service
            .claim_next_task(Request::new(claim_request(
                "worker-second",
                &["ops"],
                0,
            )))
            .await
            .unwrap()
            .into_inner();
        assert!(second.has_task);
        assert_eq!(second.task_id.unwrap().value, "task-recover");
    }
    ''',
)

# --- Python tests ---
write_new(
    "sdk/python/tests/test_claim_next.py",
    r'''
    from __future__ import annotations

    from unittest.mock import AsyncMock

    import pytest

    from keryx import ClaimedTask, KeryxNode
    from hermes.keryx.v1 import common_pb2, daemon_pb2, task_pb2


    @pytest.mark.asyncio
    async def test_claim_next_builds_request_and_returns_envelope() -> None:
        envelope = task_pb2.TaskEnvelope(
            task_id=common_pb2.TaskId(value="task-next"),
            metadata={"skill": "backend"},
        )
        stub = AsyncMock()
        stub.ClaimNextTask = AsyncMock(
            return_value=daemon_pb2.ClaimNextTaskResponse(
                has_task=True,
                envelope=envelope,
                task_id=common_pb2.TaskId(value="task-next"),
                lease_id=common_pb2.LeaseId(value="lease-next"),
                worker_id=common_pb2.AgentId(value="worker-next"),
                leased_at_ms=10,
                expires_at_ms=20,
                status="running",
                retry_count=1,
                sender_peer_id="",
            )
        )
        node = KeryxNode(daemon_stub=stub, worker_id="worker-next")

        claimed = await node.claim_next(
            accepted_skill_ids=["backend"],
            wait_timeout_ms=500,
        )

        assert isinstance(claimed, ClaimedTask)
        assert claimed.has_task
        assert claimed.task_id == "task-next"
        assert claimed.envelope.metadata["skill"] == "backend"
        request = stub.ClaimNextTask.await_args.args[0]
        assert request.worker_id.value == "worker-next"
        assert list(request.accepted_skill_ids) == ["backend"]
        assert request.wait_timeout_ms == 500


    @pytest.mark.asyncio
    async def test_claim_next_can_return_no_work() -> None:
        stub = AsyncMock()
        stub.ClaimNextTask = AsyncMock(
            return_value=daemon_pb2.ClaimNextTaskResponse(has_task=False)
        )
        node = KeryxNode(daemon_stub=stub, worker_id="worker-empty")

        claimed = await node.claim_next()

        assert not claimed.has_task
        assert claimed.envelope is None
        assert claimed.task_id == ""
    ''',
)
