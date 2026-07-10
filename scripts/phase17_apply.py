#!/usr/bin/env python3
"""Apply Phase 17.4a durable terminal results and delivery outbox."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent


def _indent_block(value: str, prefix: str) -> str:
    return "".join(prefix + line if line.strip() else line for line in value.splitlines(keepends=True))


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count == 0:
        for depth in range(1, 7):
            prefix = "    " * depth
            nested_old = _indent_block(old, prefix)
            if text.count(nested_old) == 1:
                old = nested_old
                new = _indent_block(new, prefix)
                count = 1
                break
    if count == 0 and old.endswith("\n"):
        trimmed = old.rstrip("\n")
        if text.count(trimmed) == 1:
            old = trimmed
            new = new.rstrip("\n")
            count = 1
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:180]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


def insert_before(path: str, marker: str, addition: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"{path}: expected one marker, found {count}: {marker[:180]!r}")
    addition = dedent(addition).rstrip()
    prefix = marker[: len(marker) - len(marker.lstrip(" "))]
    if prefix:
        addition = _indent_block(addition, prefix)
    file.write_text(text.replace(marker, addition + "\n\n" + marker, 1), encoding="utf-8")


def write_new(path: str, content: str) -> None:
    file = Path(path)
    if file.exists():
        raise SystemExit(f"{path}: already exists")
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(dedent(content).lstrip(), encoding="utf-8")


PROTO = "proto/hermes/keryx/v1/daemon.proto"
STORE = "crates/keryx-store/src/lib.rs"
DAEMON = "crates/keryx-daemon/src/lib.rs"
PRODUCT = "docs/current-product.md"

# ---------------------------------------------------------------------------
# Protocol: durable terminal result payload. Transport is a later 17.4 slice.
# ---------------------------------------------------------------------------
insert_before(
    PROTO,
    "message CompleteTaskRequest {",
    r'''
    message TerminalTaskResult {
      TaskId task_id = 1;
      string status = 2;
      int64 duration_ms = 3;
      map<string, string> result_metadata = 4;
      repeated TaskArtifact output_artifacts = 5;
      string error_reason = 6;
      map<string, string> failure_metadata = 7;
      uint32 retry_count = 8;
      bool dead_lettered = 9;
      // Empty for local-only results. Reserved for the authenticated return target.
      string origin_node_id = 10;
      string producer_node_id = 11;
      int64 completed_at_ms = 12;
    }
    ''',
)

# ---------------------------------------------------------------------------
# Store model, schema v7, and atomic lifecycle/result methods.
# ---------------------------------------------------------------------------
replace_once(
    STORE,
    "    #[error(\"task envelope conflicts with the stored envelope for task {0}\")]\n    TaskEnvelopeConflict(TaskId),\n",
    "    #[error(\"task envelope conflicts with the stored envelope for task {0}\")]\n"
    "    TaskEnvelopeConflict(TaskId),\n"
    "    #[error(\"terminal result not found for task: {0}\")]\n"
    "    TaskResultNotFound(TaskId),\n"
    "    #[error(\"terminal result task id {result_task_id} does not match task {task_id}\")]\n"
    "    TaskResultMismatch { task_id: TaskId, result_task_id: TaskId },\n"
    "    #[error(\"terminal result conflicts with the stored result for task {0}\")]\n"
    "    TaskResultConflict(TaskId),\n",
)
replace_once(STORE, "pub const CURRENT_SCHEMA_VERSION: i64 = 6;\n", "pub const CURRENT_SCHEMA_VERSION: i64 = 7;\n")

insert_before(
    STORE,
    "#[derive(Debug, Clone, PartialEq, Eq)]\npub struct TaskEventRecord {",
    r'''
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TaskResultRecord {
        pub task_id: TaskId,
        pub status: TaskStatus,
        pub encoded_result: Vec<u8>,
        pub producer_node_id: String,
        pub origin_node_id: String,
        pub completed_at_ms: i64,
    }

    impl TaskResultRecord {
        #[must_use]
        pub fn new(
            task_id: TaskId,
            status: TaskStatus,
            encoded_result: Vec<u8>,
            producer_node_id: impl Into<String>,
            origin_node_id: impl Into<String>,
            completed_at_ms: i64,
        ) -> Self {
            Self {
                task_id,
                status,
                encoded_result,
                producer_node_id: producer_node_id.into(),
                origin_node_id: origin_node_id.into(),
                completed_at_ms,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ResultOutboxRecord {
        pub task_id: TaskId,
        pub target_node_id: String,
        pub state: String,
        pub attempt_count: u32,
        pub next_attempt_ms: i64,
        pub last_error: String,
    }
    ''',
)

replace_once(
    STORE,
    "    envelopes: HashMap<TaskId, TaskEnvelopeRecord>,\n}",
    "    envelopes: HashMap<TaskId, TaskEnvelopeRecord>,\n"
    "    results: HashMap<TaskId, TaskResultRecord>,\n"
    "    result_outbox: HashMap<TaskId, ResultOutboxRecord>,\n}",
)

insert_before(
    STORE,
    "impl InMemoryStore {",
    r'''
    fn validate_terminal_result(
        task_id: &TaskId,
        expected_status: TaskStatus,
        result: &TaskResultRecord,
    ) -> StoreResult<()> {
        if &result.task_id != task_id {
            return Err(StoreError::TaskResultMismatch {
                task_id: task_id.clone(),
                result_task_id: result.task_id.clone(),
            });
        }
        if !result.status.is_terminal() || result.status != expected_status {
            return Err(StoreError::Validation(
                ValidationError::InvalidTaskTransition {
                    from: expected_status,
                    to: result.status,
                },
            ));
        }
        Ok(())
    }

    fn insert_terminal_result_in_state(
        state: &mut InMemoryState,
        result: TaskResultRecord,
        target_node_id: Option<&str>,
    ) -> StoreResult<()> {
        if let Some(existing) = state.results.get(&result.task_id) {
            return if existing == &result {
                Ok(())
            } else {
                Err(StoreError::TaskResultConflict(result.task_id.clone()))
            };
        }
        let task_id = result.task_id.clone();
        state.results.insert(task_id.clone(), result);
        if let Some(target) = target_node_id.map(str::trim).filter(|value| !value.is_empty()) {
            state.result_outbox.insert(
                task_id.clone(),
                ResultOutboxRecord {
                    task_id,
                    target_node_id: target.to_string(),
                    state: "pending".to_string(),
                    attempt_count: 0,
                    next_attempt_ms: 0,
                    last_error: String::new(),
                },
            );
        }
        Ok(())
    }
    ''',
)

insert_before(
    STORE,
    "    pub fn complete_task(\n",
    r'''
    pub fn complete_task_with_result(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        result: TaskResultRecord,
        target_node_id: Option<&str>,
    ) -> StoreResult<TaskRecord> {
        validate_terminal_result(task_id, TaskStatus::Completed, &result)?;
        let mut state = self.lock()?;
        if let Some(existing) = state.results.get(task_id) {
            if existing == &result {
                return state
                    .tasks
                    .get(task_id)
                    .cloned()
                    .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()));
            }
            return Err(StoreError::TaskResultConflict(task_id.clone()));
        }
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        let updated = self.finish_task_in_state(
            &mut state,
            task_id,
            lease_id,
            worker_id,
            TaskStatus::Completed,
            task.retry_count,
            task.dead_lettered,
            task.dead_letter_reason.clone(),
        )?;
        insert_terminal_result_in_state(&mut state, result, target_node_id)?;
        Ok(updated)
    }

    pub fn fail_task_with_result(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        error_reason: &str,
        policy: &RetryPolicy,
        result: Option<TaskResultRecord>,
        target_node_id: Option<&str>,
    ) -> StoreResult<TaskRecord> {
        if let Some(result) = result.as_ref() {
            validate_terminal_result(task_id, TaskStatus::Failed, result)?;
        }
        let mut state = self.lock()?;
        if let Some(existing) = state.results.get(task_id) {
            return match result.as_ref() {
                Some(result) if existing == result => state
                    .tasks
                    .get(task_id)
                    .cloned()
                    .ok_or_else(|| StoreError::TaskNotFound(task_id.clone())),
                _ => Err(StoreError::TaskResultConflict(task_id.clone())),
            };
        }
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        let active = state
            .leases
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, &active, lease_id)?;
        ensure_matching_worker_id(task_id, &active, worker_id)?;
        require_status(task.status, TaskStatus::Running)?;

        let updated = if policy.max_retries == 0 {
            self.finish_task_in_state(
                &mut state,
                task_id,
                lease_id,
                worker_id,
                TaskStatus::Failed,
                task.retry_count,
                false,
                None,
            )?
        } else if policy.should_retry_after_failure(task.retry_count) {
            if result.is_some() {
                return Err(StoreError::TaskResultConflict(task_id.clone()));
            }
            return self.retry_task_in_state(&mut state, task_id, &active, task);
        } else {
            self.dead_letter_task_in_state(&mut state, task_id, &active, task, error_reason)?
        };
        if let Some(result) = result {
            insert_terminal_result_in_state(&mut state, result, target_node_id)?;
        }
        Ok(updated)
    }

    pub fn get_task_result(&self, task_id: &TaskId) -> StoreResult<TaskResultRecord> {
        self.lock()?
            .results
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskResultNotFound(task_id.clone()))
    }

    pub fn pending_result_deliveries(&self) -> StoreResult<Vec<ResultOutboxRecord>> {
        let state = self.lock()?;
        let mut records = state
            .result_outbox
            .values()
            .filter(|record| record.state == "pending")
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.task_id.as_str().cmp(right.task_id.as_str()));
        Ok(records)
    }
    ''',
)

# Migration v7.
insert_before(
    STORE,
    "        let legacy_unowned_rows = sqlx::query(\n",
    r'''
        let task_results_exists = sqlx::query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'task_results'",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?
        .is_some();
        if !task_results_exists {
            for statement in MIGRATION_007 {
                sqlx::query(statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
            }
        }
        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (7, 'terminal_results')",
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
    ''',
)

insert_before(
    STORE,
    "    pub async fn complete_task(\n",
    r'''
    pub async fn complete_task_with_result(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        result: TaskResultRecord,
        target_node_id: Option<&str>,
    ) -> StoreResult<TaskRecord> {
        validate_terminal_result(task_id, TaskStatus::Completed, &result)?;
        let mut tx = self.pool.begin().await?;
        if let Some(existing) = fetch_task_result_optional_with_executor(&mut tx, task_id).await? {
            if existing == result {
                let task = fetch_task_with_executor(&mut tx, task_id).await?;
                tx.commit().await?;
                return Ok(task);
            }
            return Err(StoreError::TaskResultConflict(task_id.clone()));
        }
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let updated = self
            .finish_task_in_tx(
                &mut tx,
                task_id,
                lease_id,
                worker_id,
                TaskStatus::Completed,
                task.retry_count,
                task.dead_lettered,
                task.dead_letter_reason.clone(),
            )
            .await?;
        insert_terminal_result_with_executor(&mut tx, &result, target_node_id).await?;
        tx.commit().await?;
        Ok(updated)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fail_task_with_result(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        error_reason: &str,
        policy: &RetryPolicy,
        result: Option<TaskResultRecord>,
        target_node_id: Option<&str>,
    ) -> StoreResult<TaskRecord> {
        if let Some(result) = result.as_ref() {
            validate_terminal_result(task_id, TaskStatus::Failed, result)?;
        }
        let mut tx = self.pool.begin().await?;
        if let Some(existing) = fetch_task_result_optional_with_executor(&mut tx, task_id).await? {
            return match result.as_ref() {
                Some(result) if &existing == result => {
                    let task = fetch_task_with_executor(&mut tx, task_id).await?;
                    tx.commit().await?;
                    Ok(task)
                }
                _ => Err(StoreError::TaskResultConflict(task_id.clone())),
            };
        }
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let active = fetch_active_lease_with_executor(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, &active, lease_id)?;
        ensure_matching_worker_id(task_id, &active, worker_id)?;
        require_status(task.status, TaskStatus::Running)?;

        let updated = if policy.max_retries == 0 {
            self.finish_task_in_tx(
                &mut tx,
                task_id,
                lease_id,
                worker_id,
                TaskStatus::Failed,
                task.retry_count,
                false,
                None,
            )
            .await?
        } else if policy.should_retry_after_failure(task.retry_count) {
            if result.is_some() {
                return Err(StoreError::TaskResultConflict(task_id.clone()));
            }
            let updated = sqlite_retry_task_in_tx(&mut tx, task_id, &task).await?;
            tx.commit().await?;
            return Ok(updated);
        } else {
            sqlite_dead_letter_task_in_tx(&mut tx, task_id, &task, error_reason).await?
        };
        if let Some(result) = result.as_ref() {
            insert_terminal_result_with_executor(&mut tx, result, target_node_id).await?;
        }
        tx.commit().await?;
        Ok(updated)
    }

    pub async fn get_task_result(&self, task_id: &TaskId) -> StoreResult<TaskResultRecord> {
        fetch_task_result_optional_from_pool(&self.pool, task_id)
            .await?
            .ok_or_else(|| StoreError::TaskResultNotFound(task_id.clone()))
    }

    pub async fn pending_result_deliveries(&self) -> StoreResult<Vec<ResultOutboxRecord>> {
        let rows = sqlx::query(
            "SELECT task_id, target_node_id, state, attempt_count, next_attempt_ms, last_error FROM result_outbox WHERE state = 'pending' ORDER BY task_id ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_result_outbox).collect()
    }
    ''',
)

insert_before(
    STORE,
    "fn row_to_lease(row: sqlx::sqlite::SqliteRow) -> StoreResult<LeaseRecord> {",
    r'''
    fn row_to_task_result(row: sqlx::sqlite::SqliteRow) -> StoreResult<TaskResultRecord> {
        Ok(TaskResultRecord {
            task_id: TaskId::new(row.get::<String, _>("task_id"))?,
            status: str_to_status(&row.get::<String, _>("status"))?,
            encoded_result: row.get::<Vec<u8>, _>("encoded_result"),
            producer_node_id: row.get::<String, _>("producer_node_id"),
            origin_node_id: row.get::<String, _>("origin_node_id"),
            completed_at_ms: row.get::<i64, _>("completed_at_ms"),
        })
    }

    fn row_to_result_outbox(row: sqlx::sqlite::SqliteRow) -> StoreResult<ResultOutboxRecord> {
        Ok(ResultOutboxRecord {
            task_id: TaskId::new(row.get::<String, _>("task_id"))?,
            target_node_id: row.get::<String, _>("target_node_id"),
            state: row.get::<String, _>("state"),
            attempt_count: row.get::<i64, _>("attempt_count").max(0) as u32,
            next_attempt_ms: row.get::<i64, _>("next_attempt_ms"),
            last_error: row.get::<String, _>("last_error"),
        })
    }

    async fn fetch_task_result_optional_from_pool(
        pool: &SqlitePool,
        task_id: &TaskId,
    ) -> StoreResult<Option<TaskResultRecord>> {
        let row = sqlx::query(
            "SELECT task_id, status, encoded_result, producer_node_id, origin_node_id, completed_at_ms FROM task_results WHERE task_id = ?",
        )
        .bind(task_id.as_str())
        .fetch_optional(pool)
        .await?;
        row.map(row_to_task_result).transpose()
    }

    async fn fetch_task_result_optional_with_executor(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &TaskId,
    ) -> StoreResult<Option<TaskResultRecord>> {
        let row = sqlx::query(
            "SELECT task_id, status, encoded_result, producer_node_id, origin_node_id, completed_at_ms FROM task_results WHERE task_id = ?",
        )
        .bind(task_id.as_str())
        .fetch_optional(&mut **tx)
        .await?;
        row.map(row_to_task_result).transpose()
    }

    async fn insert_terminal_result_with_executor(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        result: &TaskResultRecord,
        target_node_id: Option<&str>,
    ) -> StoreResult<()> {
        if let Some(existing) = fetch_task_result_optional_with_executor(tx, &result.task_id).await? {
            return if existing == *result {
                Ok(())
            } else {
                Err(StoreError::TaskResultConflict(result.task_id.clone()))
            };
        }
        sqlx::query(
            "INSERT INTO task_results (task_id, status, encoded_result, producer_node_id, origin_node_id, completed_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(result.task_id.as_str())
        .bind(status_to_str(result.status))
        .bind(&result.encoded_result)
        .bind(&result.producer_node_id)
        .bind(&result.origin_node_id)
        .bind(result.completed_at_ms)
        .execute(&mut **tx)
        .await?;
        if let Some(target) = target_node_id.map(str::trim).filter(|value| !value.is_empty()) {
            sqlx::query(
                "INSERT INTO result_outbox (task_id, target_node_id, state, attempt_count, next_attempt_ms, last_error) VALUES (?, ?, 'pending', 0, 0, '')",
            )
            .bind(result.task_id.as_str())
            .bind(target)
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
    ''',
)

insert_before(
    STORE,
    "const fn status_to_str(status: TaskStatus) -> &'static str {",
    r'''
    const MIGRATION_007: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS task_results (task_id TEXT PRIMARY KEY, status TEXT NOT NULL, encoded_result BLOB NOT NULL, producer_node_id TEXT NOT NULL, origin_node_id TEXT NOT NULL DEFAULT '', completed_at_ms INTEGER NOT NULL, FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE)",
        "CREATE TABLE IF NOT EXISTS result_outbox (task_id TEXT PRIMARY KEY, target_node_id TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', attempt_count INTEGER NOT NULL DEFAULT 0, next_attempt_ms INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '', FOREIGN KEY(task_id) REFERENCES task_results(task_id) ON DELETE CASCADE)",
        "CREATE INDEX IF NOT EXISTS idx_result_outbox_pending ON result_outbox(state, next_attempt_ms, task_id)",
    ];
    ''',
)

# Existing test fixtures that construct InMemoryState explicitly need the new maps.
replace_once(
    STORE,
    "            envelopes: HashMap::new(),\n        };",
    "            envelopes: HashMap::new(),\n"
    "            results: HashMap::new(),\n"
    "            result_outbox: HashMap::new(),\n"
    "        };",
)

# ---------------------------------------------------------------------------
# Daemon integration. Only the reserved authenticated-sender key creates outbox.
# ---------------------------------------------------------------------------
replace_once(
    DAEMON,
    "    SendTaskRequest, SendTaskResponse, StatusRequest, StatusResponse, SubmitTaskRequest,\n    SubmitTaskResponse, TaskEnvelope, TaskId as ProtoTaskId,\n",
    "    SendTaskRequest, SendTaskResponse, StatusRequest, StatusResponse, SubmitTaskRequest,\n"
    "    SubmitTaskResponse, TaskEnvelope, TaskId as ProtoTaskId, TerminalTaskResult,\n",
)
replace_once(
    DAEMON,
    "    LeaseRecord, RecoveryReport, SqliteStore, StoreError, StoreResult, TaskEnvelopeRecord,\n    TaskRecord, CURRENT_SCHEMA_VERSION,\n",
    "    LeaseRecord, RecoveryReport, SqliteStore, StoreError, StoreResult, TaskEnvelopeRecord,\n"
    "    TaskRecord, TaskResultRecord, CURRENT_SCHEMA_VERSION,\n",
)
insert_before(
    DAEMON,
    "/// Default background health probe interval.",
    r'''
    const AUTHENTICATED_SENDER_NODE_ID_METADATA: &str =
        "keryx.authenticated_sender_node_id";
    ''',
)

replace_once(
    DAEMON,
    r'''
        let task = self
            .runtime
            .store()
            .complete_task(&task_id, &lease_id, &worker_id)
            .await
            .map_err(store_error_to_status)?;
    ''',
    r'''
        let origin_node_id = authenticated_origin_node_id(&self.runtime, &task_id).await?;
        let completed_at_ms = unix_ms_now();
        let terminal_result = TerminalTaskResult {
            task_id: Some(proto_task_id(&task_id)),
            status: "completed".to_string(),
            duration_ms: inner.duration_ms,
            result_metadata: inner.result_metadata.clone(),
            output_artifacts: inner.output_artifacts.clone(),
            error_reason: String::new(),
            failure_metadata: Default::default(),
            retry_count: 0,
            dead_lettered: false,
            origin_node_id: origin_node_id.clone(),
            producer_node_id: self.runtime.config().local_peer_id().as_str().to_string(),
            completed_at_ms,
        };
        let result_record = TaskResultRecord::new(
            task_id.clone(),
            TaskStatus::Completed,
            terminal_result.encode_to_vec(),
            terminal_result.producer_node_id.clone(),
            terminal_result.origin_node_id.clone(),
            completed_at_ms,
        );
        let task = self
            .runtime
            .store()
            .complete_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                result_record,
                non_empty(&origin_node_id),
            )
            .await
            .map_err(store_error_to_status)?;
    ''',
)

replace_once(
    DAEMON,
    r'''
        let policy = self.runtime.config().fail_retry_policy();
        let task = self
            .runtime
            .store()
            .fail_task(&task_id, &lease_id, &worker_id, &error_reason, &policy)
            .await
            .map_err(store_error_to_status)?;
    ''',
    r'''
        let policy = self.runtime.config().fail_retry_policy();
        let current = self
            .runtime
            .store()
            .get_task(&task_id)
            .await
            .map_err(store_error_to_status)?;
        let terminal_failure =
            policy.max_retries == 0 || !policy.should_retry_after_failure(current.retry_count);
        let origin_node_id = if terminal_failure {
            authenticated_origin_node_id(&self.runtime, &task_id).await?
        } else {
            String::new()
        };
        let completed_at_ms = unix_ms_now();
        let final_retry_count = if policy.max_retries == 0 {
            current.retry_count
        } else {
            current.retry_count.saturating_add(1)
        };
        let terminal_result = terminal_failure.then(|| TerminalTaskResult {
            task_id: Some(proto_task_id(&task_id)),
            status: "failed".to_string(),
            duration_ms: inner.duration_ms,
            result_metadata: Default::default(),
            output_artifacts: Vec::new(),
            error_reason: error_reason.clone(),
            failure_metadata: inner.failure_metadata.clone(),
            retry_count: final_retry_count,
            dead_lettered: policy.max_retries > 0,
            origin_node_id: origin_node_id.clone(),
            producer_node_id: self.runtime.config().local_peer_id().as_str().to_string(),
            completed_at_ms,
        });
        let result_record = terminal_result.as_ref().map(|terminal_result| {
            TaskResultRecord::new(
                task_id.clone(),
                TaskStatus::Failed,
                terminal_result.encode_to_vec(),
                terminal_result.producer_node_id.clone(),
                terminal_result.origin_node_id.clone(),
                completed_at_ms,
            )
        });
        let task = self
            .runtime
            .store()
            .fail_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                &error_reason,
                &policy,
                result_record,
                non_empty(&origin_node_id),
            )
            .await
            .map_err(store_error_to_status)?;
    ''',
)

insert_before(
    DAEMON,
    "fn unix_ms_now() -> i64 {",
    r'''
    async fn authenticated_origin_node_id(
        runtime: &KeryxDaemonRuntime,
        task_id: &TaskId,
    ) -> Result<String, Status> {
        let stored = match runtime.store().get_task_envelope(task_id).await {
            Ok(stored) => stored,
            Err(StoreError::TaskEnvelopeNotFound(_)) => return Ok(String::new()),
            Err(error) => return Err(store_error_to_status(error)),
        };
        let envelope = TaskEnvelope::decode(stored.encoded_envelope.as_slice()).map_err(|error| {
            Status::data_loss(format!(
                "stored envelope for task {} is invalid: {error}",
                task_id.as_str()
            ))
        })?;
        Ok(envelope
            .metadata
            .get(AUTHENTICATED_SENDER_NODE_ID_METADATA)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_default())
    }

    fn non_empty(value: &str) -> Option<&str> {
        (!value.trim().is_empty()).then_some(value)
    }
    ''',
)

replace_once(
    DAEMON,
    "        StoreError::TaskEnvelopeConflict(task_id) => Status::already_exists(format!(\n            \"task envelope conflicts with the stored envelope for task {}\",\n            task_id.as_str()\n        )),\n",
    "        StoreError::TaskEnvelopeConflict(task_id) => Status::already_exists(format!(\n"
    "            \"task envelope conflicts with the stored envelope for task {}\",\n"
    "            task_id.as_str()\n"
    "        )),\n"
    "        StoreError::TaskResultNotFound(task_id) => {\n"
    "            Status::not_found(format!(\"terminal result not found for task {task_id}\"))\n"
    "        }\n"
    "        StoreError::TaskResultMismatch { task_id, result_task_id } => {\n"
    "            Status::failed_precondition(format!(\n"
    "                \"terminal result task id {} does not match task {}\",\n"
    "                result_task_id.as_str(), task_id.as_str()\n"
    "            ))\n"
    "        }\n"
    "        StoreError::TaskResultConflict(task_id) => Status::already_exists(format!(\n"
    "            \"terminal result conflicts with the stored result for task {}\",\n"
    "            task_id.as_str()\n"
    "        )),\n",
)

# ---------------------------------------------------------------------------
# Documentation truth.
# ---------------------------------------------------------------------------
replace_once(PRODUCT, "| schema version | `6` |\n", "| schema version | `7` |\n")
replace_once(
    PRODUCT,
    "- complete encoded `TaskEnvelope` records keyed by task ID\n",
    "- complete encoded `TaskEnvelope` records keyed by task ID\n"
    "- opaque terminal-result records and a durable result delivery outbox\n",
)
replace_once(
    PRODUCT,
    "Schema v6 adds `task_envelopes`. `SubmitTask` now persists the complete encoded protobuf envelope atomically with the pending lifecycle row, idempotency key, and accepted event. Nested messages, raw bytes, metadata maps, correlation IDs, and requested capability hints therefore survive daemon restart.\n",
    "Schema v6 adds `task_envelopes`. Schema v7 adds `task_results` and `result_outbox`. `SubmitTask` persists complete envelopes atomically, while terminal completion/final failure now persists the encoded result in the same transaction as the lifecycle transition. A result creates an outbox row only when the stored envelope contains the reserved authenticated-sender field.\n",
)
replace_once(
    PRODUCT,
    "- authenticated terminal result/artifact routing back to the origin\n",
    "- authenticated relay/edge transport for the durable terminal-result outbox\n",
)

# ---------------------------------------------------------------------------
# Tests.
# ---------------------------------------------------------------------------
write_new(
    "crates/keryx-store/tests/terminal_result_store.rs",
    r'''
    use keryx_core::{AgentId, IdempotencyKey, LeaseId, RetryPolicy, TaskId, TaskStatus};
    use keryx_store::{
        LeaseRecord, SqliteStore, StoreError, TaskEnvelopeRecord, TaskRecord, TaskResultRecord,
    };
    use tempfile::tempdir;

    fn task(id: &str) -> TaskRecord {
        TaskRecord::new(
            TaskId::new(id).unwrap(),
            TaskStatus::Pending,
            Some(IdempotencyKey::new(format!("idem-{id}")).unwrap()),
        )
    }

    async fn running(store: &SqliteStore, id: &str) -> (TaskId, LeaseId, AgentId) {
        let task_id = TaskId::new(id).unwrap();
        store
            .accept_task_with_envelope(
                task(id),
                TaskEnvelopeRecord::new(task_id.clone(), vec![1, 2, 3], 10),
            )
            .await
            .unwrap();
        let lease_id = LeaseId::new(format!("lease-{id}")).unwrap();
        let worker_id = AgentId::new(format!("worker-{id}")).unwrap();
        store
            .lease_task(
                &task_id,
                LeaseRecord::new(
                    lease_id.clone(),
                    task_id.clone(),
                    worker_id.clone(),
                    10,
                    1_000,
                ),
            )
            .await
            .unwrap();
        (task_id, lease_id, worker_id)
    }

    fn result(id: &str, status: TaskStatus, bytes: &[u8]) -> TaskResultRecord {
        TaskResultRecord::new(
            TaskId::new(id).unwrap(),
            status,
            bytes.to_vec(),
            "node-producer",
            "node-origin",
            50,
        )
    }

    #[tokio::test]
    async fn completion_result_and_outbox_survive_restart() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("keryx.db");
        let store = SqliteStore::connect(&db).await.unwrap();
        store.migrate().await.unwrap();
        let (task_id, lease_id, worker_id) = running(&store, "result-complete").await;
        let record = result("result-complete", TaskStatus::Completed, b"completed-result");

        let completed = store
            .complete_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                record.clone(),
                Some("node-origin"),
            )
            .await
            .unwrap();
        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(store.get_task_result(&task_id).await.unwrap(), record);
        let pending = store.pending_result_deliveries().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].target_node_id, "node-origin");
        store.close().await;

        let reopened = SqliteStore::connect(&db).await.unwrap();
        reopened.migrate().await.unwrap();
        assert_eq!(
            reopened.get_task_result(&task_id).await.unwrap().encoded_result,
            b"completed-result"
        );
        assert_eq!(reopened.pending_result_deliveries().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn local_completion_persists_result_without_outbox() {
        let dir = tempdir().unwrap();
        let store = SqliteStore::connect(dir.path().join("keryx.db"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let (task_id, lease_id, worker_id) = running(&store, "result-local").await;
        store
            .complete_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                result("result-local", TaskStatus::Completed, b"local"),
                None,
            )
            .await
            .unwrap();
        assert!(store.get_task_result(&task_id).await.is_ok());
        assert!(store.pending_result_deliveries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn result_id_mismatch_rolls_back_lifecycle() {
        let dir = tempdir().unwrap();
        let store = SqliteStore::connect(dir.path().join("keryx.db"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let (task_id, lease_id, worker_id) = running(&store, "result-mismatch").await;
        let error = store
            .complete_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                result("different-task", TaskStatus::Completed, b"bad"),
                Some("node-origin"),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::TaskResultMismatch { .. }));
        assert_eq!(store.get_task(&task_id).await.unwrap().status, TaskStatus::Running);
        assert!(store.active_lease(&task_id).await.unwrap().is_some());
        assert!(matches!(
            store.get_task_result(&task_id).await.unwrap_err(),
            StoreError::TaskResultNotFound(_)
        ));
    }

    #[tokio::test]
    async fn retrying_failure_creates_no_terminal_result() {
        let dir = tempdir().unwrap();
        let store = SqliteStore::connect(dir.path().join("keryx.db"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let (task_id, lease_id, worker_id) = running(&store, "result-retry").await;
        let updated = store
            .fail_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                "retry me",
                &RetryPolicy::default(),
                None,
                Some("node-origin"),
            )
            .await
            .unwrap();
        assert_eq!(updated.status, TaskStatus::Pending);
        assert!(matches!(
            store.get_task_result(&task_id).await.unwrap_err(),
            StoreError::TaskResultNotFound(_)
        ));
        assert!(store.pending_result_deliveries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn terminal_failure_persists_result_and_conflicting_retry_fails_closed() {
        let dir = tempdir().unwrap();
        let store = SqliteStore::connect(dir.path().join("keryx.db"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let (task_id, lease_id, worker_id) = running(&store, "result-failed").await;
        let record = result("result-failed", TaskStatus::Failed, b"failed-result");
        let failed = store
            .fail_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                "terminal",
                &RetryPolicy::no_retries(),
                Some(record.clone()),
                Some("node-origin"),
            )
            .await
            .unwrap();
        assert_eq!(failed.status, TaskStatus::Failed);
        assert_eq!(store.get_task_result(&task_id).await.unwrap(), record);

        let conflict = store
            .fail_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                "terminal",
                &RetryPolicy::no_retries(),
                Some(result("result-failed", TaskStatus::Failed, b"different")),
                Some("node-origin"),
            )
            .await
            .unwrap_err();
        assert_eq!(conflict, StoreError::TaskResultConflict(task_id));
    }
    ''',
)

write_new(
    "crates/keryx-daemon/tests/terminal_result_persistence.rs",
    r'''
    use std::collections::HashMap;

    use keryx_core::{RetryPolicy, TaskId};
    use keryx_daemon::{KeryxDaemonConfig, KeryxDaemonRpcService, KeryxDaemonRuntime};
    use keryx_proto::v1::{
        keryx_daemon_server::KeryxDaemon, AgentId, ClaimTaskRequest, CompleteTaskRequest,
        FailTaskRequest, IdempotencyKey, SubmitTaskRequest, TaskEnvelope, TaskId as ProtoTaskId,
        TaskMessage, TaskMessagePart, TerminalTaskResult,
    };
    use prost::Message;
    use tempfile::tempdir;
    use tonic::Request;

    fn envelope(task_id: &str, authenticated_sender: Option<&str>) -> TaskEnvelope {
        let mut metadata = HashMap::new();
        if let Some(sender) = authenticated_sender {
            metadata.insert(
                "keryx.authenticated_sender_node_id".to_string(),
                sender.to_string(),
            );
        }
        TaskEnvelope {
            task_id: Some(ProtoTaskId {
                value: task_id.to_string(),
            }),
            correlation_id: None,
            idempotency_key: Some(IdempotencyKey {
                value: format!("idem-{task_id}"),
            }),
            status: 1,
            messages: vec![TaskMessage {
                parts: vec![TaskMessagePart {
                    media_type: "text/plain".into(),
                    text: "do work".into(),
                    raw: Vec::new(),
                    metadata: HashMap::new(),
                }],
                metadata: HashMap::new(),
            }],
            metadata,
        }
    }

    async fn submit_and_claim(
        service: &KeryxDaemonRpcService,
        task_id: &str,
        authenticated_sender: Option<&str>,
    ) -> (String, String) {
        service
            .submit_task(Request::new(SubmitTaskRequest {
                envelope: Some(envelope(task_id, authenticated_sender)),
            }))
            .await
            .unwrap();
        let claimed = service
            .claim_task(Request::new(ClaimTaskRequest {
                task_id: Some(ProtoTaskId {
                    value: task_id.to_string(),
                }),
                worker_id: Some(AgentId {
                    value: "worker-result".into(),
                }),
                lease_duration_ms: 5_000,
            }))
            .await
            .unwrap()
            .into_inner();
        (
            claimed.lease_id.unwrap().value,
            claimed.worker_id.unwrap().value,
        )
    }

    #[tokio::test]
    async fn complete_task_persists_terminal_proto_and_outbox() {
        let dir = tempdir().unwrap();
        let runtime = KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(dir.path(), 0)
                .with_local_peer_id(keryx_core::PeerId::new("node-destination").unwrap()),
        )
        .await
        .unwrap();
        let store = runtime.store().clone();
        let service = KeryxDaemonRpcService::new(runtime);
        let (lease_id, worker_id) =
            submit_and_claim(&service, "daemon-result-complete", Some("node-origin")).await;

        service
            .complete_task(Request::new(CompleteTaskRequest {
                task_id: Some(ProtoTaskId {
                    value: "daemon-result-complete".into(),
                }),
                lease_id: Some(keryx_proto::v1::LeaseId { value: lease_id }),
                worker_id: Some(AgentId { value: worker_id }),
                duration_ms: 123,
                result_metadata: HashMap::from([("result_text".into(), "done".into())]),
                output_artifacts: Vec::new(),
            }))
            .await
            .unwrap();

        let task_id = TaskId::new("daemon-result-complete").unwrap();
        let record = store.get_task_result(&task_id).await.unwrap();
        let decoded = TerminalTaskResult::decode(record.encoded_result.as_slice()).unwrap();
        assert_eq!(decoded.status, "completed");
        assert_eq!(decoded.origin_node_id, "node-origin");
        assert_eq!(decoded.producer_node_id, "node-destination");
        assert_eq!(decoded.result_metadata["result_text"], "done");
        assert_eq!(store.pending_result_deliveries().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn retrying_failure_has_no_terminal_result_until_final_attempt() {
        let dir = tempdir().unwrap();
        let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(dir.path(), 0))
            .await
            .unwrap();
        let store = runtime.store().clone();
        let service = KeryxDaemonRpcService::new(runtime);
        let (lease_id, worker_id) =
            submit_and_claim(&service, "daemon-result-retry", Some("node-origin")).await;
        service
            .fail_task(Request::new(FailTaskRequest {
                task_id: Some(ProtoTaskId {
                    value: "daemon-result-retry".into(),
                }),
                lease_id: Some(keryx_proto::v1::LeaseId { value: lease_id }),
                worker_id: Some(AgentId { value: worker_id }),
                duration_ms: 10,
                error_reason: "retry".into(),
                failure_metadata: HashMap::new(),
            }))
            .await
            .unwrap();
        let task_id = TaskId::new("daemon-result-retry").unwrap();
        assert!(store.get_task_result(&task_id).await.is_err());
        assert!(store.pending_result_deliveries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_retry_failure_persists_terminal_result_without_untrusted_route() {
        let dir = tempdir().unwrap();
        let runtime = KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(dir.path(), 0)
                .with_fail_retry_policy(RetryPolicy::no_retries()),
        )
        .await
        .unwrap();
        let store = runtime.store().clone();
        let service = KeryxDaemonRpcService::new(runtime);
        let (lease_id, worker_id) =
            submit_and_claim(&service, "daemon-result-failed", None).await;
        service
            .fail_task(Request::new(FailTaskRequest {
                task_id: Some(ProtoTaskId {
                    value: "daemon-result-failed".into(),
                }),
                lease_id: Some(keryx_proto::v1::LeaseId { value: lease_id }),
                worker_id: Some(AgentId { value: worker_id }),
                duration_ms: 10,
                error_reason: "failed".into(),
                failure_metadata: HashMap::from([("kind".into(), "worker".into())]),
            }))
            .await
            .unwrap();
        let task_id = TaskId::new("daemon-result-failed").unwrap();
        let decoded = TerminalTaskResult::decode(
            store
                .get_task_result(&task_id)
                .await
                .unwrap()
                .encoded_result
                .as_slice(),
        )
        .unwrap();
        assert_eq!(decoded.status, "failed");
        assert_eq!(decoded.error_reason, "failed");
        assert!(decoded.origin_node_id.is_empty());
        assert!(store.pending_result_deliveries().await.unwrap().is_empty());
    }
    ''',
)
