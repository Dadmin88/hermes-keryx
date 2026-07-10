#!/usr/bin/env python3
"""Apply Phase 17.1 durable task-envelope persistence.

This is a temporary, assertion-heavy migration helper used to build the focused
feature branch through GitHub Actions. Remove it before the implementation PR is
proposed to main.
"""

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:120]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


def insert_before(path: str, marker: str, addition: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    if addition.strip() in text:
        return
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"{path}: expected one marker, found {count}: {marker[:120]!r}")
    file.write_text(text.replace(marker, addition.rstrip() + "\n\n" + marker, 1), encoding="utf-8")


STORE = "crates/keryx-store/src/lib.rs"
DAEMON = "crates/keryx-daemon/src/lib.rs"

replace_once(STORE, "pub const CURRENT_SCHEMA_VERSION: i64 = 5;", "pub const CURRENT_SCHEMA_VERSION: i64 = 6;")

insert_before(
    STORE,
    "    #[error(\"validation failed: {0}\")]\n    Validation(#[from] ValidationError),",
    '''    #[error("task envelope not found: {0}")]
    TaskEnvelopeNotFound(TaskId),
    #[error("task envelope id {envelope_task_id} does not match task {task_id}")]
    TaskEnvelopeMismatch {
        task_id: TaskId,
        envelope_task_id: TaskId,
    },
    #[error("task envelope conflicts with the stored envelope for task {0}")]
    TaskEnvelopeConflict(TaskId),''',
)

insert_before(
    STORE,
    "#[derive(Debug, Clone, PartialEq, Eq)]\npub struct TaskEventRecord {",
    '''#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEnvelopeRecord {
    pub task_id: TaskId,
    pub encoded_envelope: Vec<u8>,
    pub received_at_ms: i64,
}

impl TaskEnvelopeRecord {
    #[must_use]
    pub const fn new(task_id: TaskId, encoded_envelope: Vec<u8>, received_at_ms: i64) -> Self {
        Self {
            task_id,
            encoded_envelope,
            received_at_ms,
        }
    }
}''',
)

replace_once(
    STORE,
    "    blobs: HashMap<Digest, (Vec<u8>, u32)>,\n}",
    "    blobs: HashMap<Digest, (Vec<u8>, u32)>,\n    envelopes: HashMap<TaskId, TaskEnvelopeRecord>,\n}",
)

# Existing explicit InMemoryState test fixtures need the new field.
store_text = Path(STORE).read_text(encoding="utf-8")
store_text = store_text.replace(
    "            blobs: HashMap::new(),\n        };",
    "            blobs: HashMap::new(),\n            envelopes: HashMap::new(),\n        };",
)
store_text = store_text.replace(
    "                blobs: HashMap::new(),\n            }),",
    "                blobs: HashMap::new(),\n                envelopes: HashMap::new(),\n            }),",
)
Path(STORE).write_text(store_text, encoding="utf-8")

insert_before(
    STORE,
    "    pub fn lease_task(&self, task_id: &TaskId, lease: LeaseRecord) -> StoreResult<TaskRecord> {",
    '''    pub fn accept_task_with_envelope(
        &self,
        task: TaskRecord,
        envelope: TaskEnvelopeRecord,
    ) -> StoreResult<TaskRecord> {
        validate_accepted_task_status(&task)?;
        ensure_pending_accept(&task)?;
        ensure_matching_envelope_task_id(&task, &envelope)?;

        let mut state = self.lock()?;
        if let Some(key) = &task.idempotency_key {
            if let Some(existing_task_id) = state.idempotency.get(key) {
                let existing = state
                    .tasks
                    .get(existing_task_id)
                    .cloned()
                    .ok_or_else(|| StoreError::CorruptEventStream(existing_task_id.clone()))?;
                if existing == task {
                    return match state.envelopes.get(existing_task_id) {
                        Some(existing_envelope) if existing_envelope == &envelope => Ok(existing),
                        _ => Err(StoreError::TaskEnvelopeConflict(existing_task_id.clone())),
                    };
                }
                return Err(StoreError::IdempotencyConflict {
                    key: key.clone(),
                    existing_task_id: existing_task_id.clone(),
                });
            }
        }

        let task_id = task.task_id().clone();
        if state.tasks.contains_key(&task_id) {
            return Err(StoreError::TaskAlreadyExists(task_id));
        }
        if let Some(key) = &task.idempotency_key {
            state.idempotency.insert(key.clone(), task_id.clone());
        }
        append_in_memory_event(
            &mut state,
            &task_id,
            KeryxEventType::TaskAccepted,
            None,
            task.status,
        );
        state.tasks.insert(task_id.clone(), task.clone());
        state.envelopes.insert(task_id, envelope);
        Ok(task)
    }

    pub fn get_task_envelope(&self, task_id: &TaskId) -> StoreResult<TaskEnvelopeRecord> {
        self.lock()?
            .envelopes
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskEnvelopeNotFound(task_id.clone()))
    }''',
)

insert_before(
    STORE,
    "    pub async fn put_artifact(\n        &self,",
    '''    pub async fn accept_task_with_envelope(
        &self,
        task: TaskRecord,
        envelope: TaskEnvelopeRecord,
    ) -> StoreResult<TaskRecord> {
        validate_accepted_task_status(&task)?;
        ensure_pending_accept(&task)?;
        ensure_matching_envelope_task_id(&task, &envelope)?;

        let mut tx = self.pool.begin().await?;
        if let Some(key) = &task.idempotency_key {
            let existing = sqlx::query("SELECT task_id FROM idempotency_keys WHERE key = ?")
                .bind(key.as_str())
                .fetch_optional(&mut *tx)
                .await?;
            if let Some(row) = existing {
                let existing_task_id = TaskId::new(row.get::<String, _>("task_id"))?;
                let existing_task = fetch_task_with_executor(&mut tx, &existing_task_id).await?;
                if existing_task == task {
                    let existing_envelope =
                        fetch_task_envelope_optional_with_executor(&mut tx, &existing_task_id).await?;
                    if existing_envelope.as_ref() == Some(&envelope) {
                        tx.commit().await?;
                        return Ok(existing_task);
                    }
                    return Err(StoreError::TaskEnvelopeConflict(existing_task_id));
                }
                return Err(StoreError::IdempotencyConflict {
                    key: key.clone(),
                    existing_task_id,
                });
            }
        }

        let task_id = task.task_id().clone();
        let existing_task = sqlx::query("SELECT task_id FROM tasks WHERE task_id = ?")
            .bind(task_id.as_str())
            .fetch_optional(&mut *tx)
            .await?;
        if existing_task.is_some() {
            return Err(StoreError::TaskAlreadyExists(task_id));
        }

        sqlx::query(
            "INSERT INTO tasks (task_id, status, idempotency_key, retry_count, dead_lettered, dead_letter_reason, deadline_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(task.task_id().as_str())
        .bind(status_to_str(task.status))
        .bind(task.idempotency_key.as_ref().map(IdempotencyKey::as_str))
        .bind(i64::from(task.retry_count))
        .bind(i64::from(task.dead_lettered))
        .bind(task.dead_letter_reason.as_deref())
        .bind(task.deadline_ms)
        .execute(&mut *tx)
        .await?;
        if let Some(key) = &task.idempotency_key {
            sqlx::query("INSERT INTO idempotency_keys (key, task_id) VALUES (?, ?)")
                .bind(key.as_str())
                .bind(task.task_id().as_str())
                .execute(&mut *tx)
                .await?;
        }
        insert_event(
            &mut tx,
            task.task_id(),
            1,
            KeryxEventType::TaskAccepted,
            None,
            task.status,
        )
        .await?;
        sqlx::query(
            "INSERT INTO task_envelopes (task_id, encoded_envelope, received_at_ms) VALUES (?, ?, ?)",
        )
        .bind(envelope.task_id.as_str())
        .bind(&envelope.encoded_envelope)
        .bind(envelope.received_at_ms)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(task)
    }

    pub async fn get_task_envelope(
        &self,
        task_id: &TaskId,
    ) -> StoreResult<TaskEnvelopeRecord> {
        let row = sqlx::query(
            "SELECT task_id, encoded_envelope, received_at_ms FROM task_envelopes WHERE task_id = ?",
        )
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_task_envelope)
            .transpose()?
            .ok_or_else(|| StoreError::TaskEnvelopeNotFound(task_id.clone()))
    }''',
)

replace_once(
    STORE,
    '''        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (5, 'task_deadlines')",
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        let legacy_unowned_rows = sqlx::query(''',
    '''        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (5, 'task_deadlines')",
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        let task_envelopes_exists = sqlx::query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'task_envelopes'",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?
        .is_some();
        if !task_envelopes_exists {
            for statement in MIGRATION_006 {
                sqlx::query(statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
            }
        }
        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (6, 'task_envelopes')",
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        let legacy_unowned_rows = sqlx::query(''',
)

insert_before(
    STORE,
    "fn ensure_pending_accept(task: &TaskRecord) -> StoreResult<()> {",
    '''fn ensure_matching_envelope_task_id(
    task: &TaskRecord,
    envelope: &TaskEnvelopeRecord,
) -> StoreResult<()> {
    if task.task_id() == &envelope.task_id {
        Ok(())
    } else {
        Err(StoreError::TaskEnvelopeMismatch {
            task_id: task.task_id().clone(),
            envelope_task_id: envelope.task_id.clone(),
        })
    }
}''',
)

insert_before(
    STORE,
    "fn row_to_lease(row: sqlx::sqlite::SqliteRow) -> StoreResult<LeaseRecord> {",
    '''fn row_to_task_envelope(row: sqlx::sqlite::SqliteRow) -> StoreResult<TaskEnvelopeRecord> {
    Ok(TaskEnvelopeRecord {
        task_id: TaskId::new(row.get::<String, _>("task_id"))?,
        encoded_envelope: row.get::<Vec<u8>, _>("encoded_envelope"),
        received_at_ms: row.get::<i64, _>("received_at_ms"),
    })
}

async fn fetch_task_envelope_optional_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<Option<TaskEnvelopeRecord>> {
    let row = sqlx::query(
        "SELECT task_id, encoded_envelope, received_at_ms FROM task_envelopes WHERE task_id = ?",
    )
    .bind(task_id.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    row.map(row_to_task_envelope).transpose()
}''',
)

insert_before(
    STORE,
    "const fn status_to_str(status: TaskStatus) -> &'static str {",
    '''const MIGRATION_006: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS task_envelopes (task_id TEXT PRIMARY KEY, encoded_envelope BLOB NOT NULL, received_at_ms INTEGER NOT NULL, FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE)",
];''',
)

# Daemon imports and SubmitTask wiring.
replace_once(
    DAEMON,
    "    LeaseRecord, RecoveryReport, SqliteStore, StoreError, StoreResult, TaskRecord,\n    CURRENT_SCHEMA_VERSION,\n",
    "    LeaseRecord, RecoveryReport, SqliteStore, StoreError, StoreResult, TaskEnvelopeRecord,\n    TaskRecord, CURRENT_SCHEMA_VERSION,\n",
)
replace_once(
    DAEMON,
    "        let envelope_bytes = envelope.encoded_len() as u64;",
    "        let encoded_envelope = envelope.encode_to_vec();\n        let envelope_bytes = encoded_envelope.len() as u64;",
)
replace_once(
    DAEMON,
    "        let record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);",
    "        let record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);\n        let envelope_record =\n            TaskEnvelopeRecord::new(task_id.clone(), encoded_envelope, unix_ms_now());",
)
replace_once(
    DAEMON,
    '''            .store()
            .accept_task(record)
            .await''',
    '''            .store()
            .accept_task_with_envelope(record, envelope_record)
            .await''',
)
