//! Storage traits plus in-memory and SQLite implementations for Hermes Keryx.

use std::{collections::HashMap, path::Path, str::FromStr, sync::Mutex};

use keryx_core::{
    event_for_transition, IdempotencyKey, KeryxCoreError, KeryxEventType, LeaseId, TaskId,
    TaskStatus, ValidationError,
};
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, Row, SqlitePool};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("task already exists: {0}")]
    TaskAlreadyExists(TaskId),
    #[error("idempotency key {key} already belongs to task {existing_task_id}")]
    IdempotencyConflict {
        key: IdempotencyKey,
        existing_task_id: TaskId,
    },
    #[error("validation failed: {0}")]
    Validation(#[from] ValidationError),
    #[error("event stream for task {0} is corrupt or incomplete")]
    CorruptEventStream(TaskId),
    #[error("store lock poisoned")]
    LockPoisoned,
    #[error("lease not found for task: {0}")]
    LeaseNotFound(TaskId),
    #[error("database error: {0}")]
    Database(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value.to_string())
    }
}

impl From<std::io::Error> for StoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Database(value.to_string())
    }
}

impl From<KeryxCoreError> for StoreError {
    fn from(value: KeryxCoreError) -> Self {
        match value {
            KeryxCoreError::Validation(error) => Self::Validation(error),
            KeryxCoreError::TaskNotFound(task_id) => TaskId::new(&task_id)
                .map(Self::TaskNotFound)
                .unwrap_or_else(Self::Validation),
            KeryxCoreError::PolicyDenied(message) => Self::Database(message),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    id: TaskId,
    pub status: TaskStatus,
    pub idempotency_key: Option<IdempotencyKey>,
}

impl TaskRecord {
    #[must_use]
    pub const fn new(
        id: TaskId,
        status: TaskStatus,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Self {
        Self {
            id,
            status,
            idempotency_key,
        }
    }

    #[must_use]
    pub const fn task_id(&self) -> &TaskId {
        &self.id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEventRecord {
    pub task_id: TaskId,
    pub sequence: u64,
    pub event_type: KeryxEventType,
    pub from_status: Option<TaskStatus>,
    pub to_status: TaskStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    pub lease_id: LeaseId,
    pub task_id: TaskId,
    pub leased_at_ms: i64,
    pub expires_at_ms: i64,
}

impl LeaseRecord {
    #[must_use]
    pub const fn new(
        lease_id: LeaseId,
        task_id: TaskId,
        leased_at_ms: i64,
        expires_at_ms: i64,
    ) -> Self {
        Self {
            lease_id,
            task_id,
            leased_at_ms,
            expires_at_ms,
        }
    }
}

pub trait AgentStore {}
pub trait CapabilityStore {}
pub trait EventStore {}
pub trait LeaseStore {}
pub trait RouteStore {}
pub trait PolicyStore {}
pub trait OutboxStore {}
pub trait InboxStore {}
pub trait ArtifactStore {}

pub trait TaskStore {
    fn accept_task(&self, task: TaskRecord) -> StoreResult<TaskRecord>;
    fn get_task(&self, task_id: &TaskId) -> StoreResult<TaskRecord>;
    fn transition_task(&self, task_id: &TaskId, to: TaskStatus) -> StoreResult<TaskRecord>;
    fn events_for_task(&self, task_id: &TaskId) -> StoreResult<Vec<TaskEventRecord>>;
    fn replay_task(&self, task_id: &TaskId) -> StoreResult<TaskRecord>;
}

#[derive(Debug, Default)]
pub struct InMemoryStore {
    inner: Mutex<InMemoryState>,
}

#[derive(Debug, Default)]
struct InMemoryState {
    tasks: HashMap<TaskId, TaskRecord>,
    events: HashMap<TaskId, Vec<TaskEventRecord>>,
    idempotency: HashMap<IdempotencyKey, TaskId>,
    leases: HashMap<TaskId, LeaseRecord>,
}

impl InMemoryStore {
    fn lock(&self) -> StoreResult<std::sync::MutexGuard<'_, InMemoryState>> {
        self.inner.lock().map_err(|_| StoreError::LockPoisoned)
    }

    pub fn lease_task(&self, task_id: &TaskId, lease: LeaseRecord) -> StoreResult<TaskRecord> {
        let mut state = self.lock()?;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        let event_type = event_for_transition(task.status, TaskStatus::Running)?;
        let sequence = state
            .events
            .get(task_id)
            .map_or(1, |events| events.len() as u64 + 1);
        let mut updated = task;
        let from_status = updated.status;
        updated.status = TaskStatus::Running;
        state.leases.insert(task_id.clone(), lease);
        state
            .events
            .entry(task_id.clone())
            .or_default()
            .push(TaskEventRecord {
                task_id: task_id.clone(),
                sequence,
                event_type,
                from_status: Some(from_status),
                to_status: TaskStatus::Running,
            });
        state.tasks.insert(task_id.clone(), updated.clone());
        Ok(updated)
    }

    pub fn active_lease(&self, task_id: &TaskId) -> StoreResult<Option<LeaseRecord>> {
        Ok(self.lock()?.leases.get(task_id).cloned())
    }

    pub fn recover_stale_leases(&self, now_ms: i64) -> StoreResult<Vec<TaskRecord>> {
        let mut state = self.lock()?;
        let stale_task_ids = state
            .leases
            .values()
            .filter(|lease| lease.expires_at_ms <= now_ms)
            .map(|lease| lease.task_id.clone())
            .collect::<Vec<_>>();
        let mut recovered = Vec::new();
        for task_id in stale_task_ids {
            let Some(task) = state.tasks.get(&task_id).cloned() else {
                state.leases.remove(&task_id);
                continue;
            };
            if task.status.is_terminal() {
                continue;
            }
            let sequence = state
                .events
                .get(&task_id)
                .map_or(1, |events| events.len() as u64 + 1);
            let mut updated = task;
            let from_status = updated.status;
            updated.status = TaskStatus::Pending;
            state.leases.remove(&task_id);
            state
                .events
                .entry(task_id.clone())
                .or_default()
                .push(TaskEventRecord {
                    task_id: task_id.clone(),
                    sequence,
                    event_type: KeryxEventType::RecoveryAction,
                    from_status: Some(from_status),
                    to_status: TaskStatus::Pending,
                });
            state.tasks.insert(task_id, updated.clone());
            recovered.push(updated);
        }
        Ok(recovered)
    }
}

impl TaskStore for InMemoryStore {
    fn accept_task(&self, task: TaskRecord) -> StoreResult<TaskRecord> {
        let mut state = self.lock()?;

        if let Some(key) = &task.idempotency_key {
            if let Some(existing_task_id) = state.idempotency.get(key) {
                let existing = state
                    .tasks
                    .get(existing_task_id)
                    .ok_or_else(|| StoreError::CorruptEventStream(existing_task_id.clone()))?;
                if existing == &task {
                    return Ok(existing.clone());
                }
                return Err(StoreError::IdempotencyConflict {
                    key: key.clone(),
                    existing_task_id: existing_task_id.clone(),
                });
            }
        }

        if state.tasks.contains_key(task.task_id()) {
            return Err(StoreError::TaskAlreadyExists(task.task_id().clone()));
        }

        let event = TaskEventRecord {
            task_id: task.task_id().clone(),
            sequence: 1,
            event_type: KeryxEventType::TaskAccepted,
            from_status: None,
            to_status: task.status,
        };

        if let Some(key) = &task.idempotency_key {
            state
                .idempotency
                .insert(key.clone(), task.task_id().clone());
        }
        state.events.insert(task.task_id().clone(), vec![event]);
        state.tasks.insert(task.task_id().clone(), task.clone());
        Ok(task)
    }

    fn get_task(&self, task_id: &TaskId) -> StoreResult<TaskRecord> {
        self.lock()?
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))
    }

    fn transition_task(&self, task_id: &TaskId, to: TaskStatus) -> StoreResult<TaskRecord> {
        let mut state = self.lock()?;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        let event_type = event_for_transition(task.status, to)?;

        let sequence = state
            .events
            .get(task_id)
            .map_or(1, |events| events.len() as u64 + 1);
        let event = TaskEventRecord {
            task_id: task_id.clone(),
            sequence,
            event_type,
            from_status: Some(task.status),
            to_status: to,
        };

        let mut updated = task;
        updated.status = to;
        state.events.entry(task_id.clone()).or_default().push(event);
        state.tasks.insert(task_id.clone(), updated.clone());
        Ok(updated)
    }

    fn events_for_task(&self, task_id: &TaskId) -> StoreResult<Vec<TaskEventRecord>> {
        self.lock()?
            .events
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))
    }

    fn replay_task(&self, task_id: &TaskId) -> StoreResult<TaskRecord> {
        let state = self.lock()?;
        let snapshot = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        let events = state
            .events
            .get(task_id)
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        let last_event = events
            .last()
            .ok_or_else(|| StoreError::CorruptEventStream(task_id.clone()))?;

        let mut replayed = snapshot;
        replayed.status = last_event.to_status;
        Ok(replayed)
    }
}

#[derive(Debug, Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    pub async fn connect(path: impl AsRef<Path>) -> StoreResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;
        for statement in MIGRATION_001 {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (1, 'initial')",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn schema_version(&self) -> StoreResult<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>("version"))
    }

    pub async fn accept_task(&self, task: TaskRecord) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;

        if let Some(key) = &task.idempotency_key {
            let existing = sqlx::query("SELECT task_id FROM idempotency_keys WHERE key = ?")
                .bind(key.as_str())
                .fetch_optional(&mut *tx)
                .await?;
            if let Some(row) = existing {
                let existing_task_id = TaskId::new(row.get::<String, _>("task_id"))?;
                let existing = fetch_task_with_executor(&mut tx, &existing_task_id).await?;
                if existing == task {
                    tx.commit().await?;
                    return Ok(existing);
                }
                return Err(StoreError::IdempotencyConflict {
                    key: key.clone(),
                    existing_task_id,
                });
            }
        }

        let existing_task = sqlx::query("SELECT task_id FROM tasks WHERE task_id = ?")
            .bind(task.task_id().as_str())
            .fetch_optional(&mut *tx)
            .await?;
        if existing_task.is_some() {
            return Err(StoreError::TaskAlreadyExists(task.task_id().clone()));
        }

        sqlx::query("INSERT INTO tasks (task_id, status, idempotency_key) VALUES (?, ?, ?)")
            .bind(task.task_id().as_str())
            .bind(status_to_str(task.status))
            .bind(task.idempotency_key.as_ref().map(IdempotencyKey::as_str))
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
        tx.commit().await?;
        Ok(task)
    }

    pub async fn get_task(&self, task_id: &TaskId) -> StoreResult<TaskRecord> {
        let row =
            sqlx::query("SELECT task_id, status, idempotency_key FROM tasks WHERE task_id = ?")
                .bind(task_id.as_str())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        row_to_task(row)
    }

    pub async fn transition_task(
        &self,
        task_id: &TaskId,
        to: TaskStatus,
    ) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let event_type = event_for_transition(task.status, to)?;
        let row = sqlx::query("SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM task_events WHERE task_id = ?")
            .bind(task_id.as_str())
            .fetch_one(&mut *tx)
            .await?;
        let sequence = row.get::<i64, _>("next_sequence") as u64;
        sqlx::query("UPDATE tasks SET status = ? WHERE task_id = ?")
            .bind(status_to_str(to))
            .bind(task_id.as_str())
            .execute(&mut *tx)
            .await?;
        insert_event(
            &mut tx,
            task_id,
            sequence,
            event_type,
            Some(task.status),
            to,
        )
        .await?;
        tx.commit().await?;
        self.get_task(task_id).await
    }

    pub async fn events_for_task(&self, task_id: &TaskId) -> StoreResult<Vec<TaskEventRecord>> {
        let rows = sqlx::query(
            "SELECT task_id, sequence, event_type, from_status, to_status FROM task_events WHERE task_id = ? ORDER BY sequence ASC",
        )
        .bind(task_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        if rows.is_empty() {
            return Err(StoreError::TaskNotFound(task_id.clone()));
        }
        rows.into_iter().map(row_to_event).collect()
    }

    pub async fn replay_task(&self, task_id: &TaskId) -> StoreResult<TaskRecord> {
        let mut task = self.get_task(task_id).await?;
        let events = self.events_for_task(task_id).await?;
        let last = events
            .last()
            .ok_or_else(|| StoreError::CorruptEventStream(task_id.clone()))?;
        task.status = last.to_status;
        Ok(task)
    }

    pub async fn lease_task(
        &self,
        task_id: &TaskId,
        lease: LeaseRecord,
    ) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let event_type = event_for_transition(task.status, TaskStatus::Running)?;
        let row = sqlx::query("SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM task_events WHERE task_id = ?")
            .bind(task_id.as_str())
            .fetch_one(&mut *tx)
            .await?;
        let sequence = row.get::<i64, _>("next_sequence") as u64;
        sqlx::query("INSERT OR REPLACE INTO leases (lease_id, task_id, leased_at_ms, expires_at_ms, active) VALUES (?, ?, ?, ?, 1)")
            .bind(lease.lease_id.as_str())
            .bind(task_id.as_str())
            .bind(lease.leased_at_ms)
            .bind(lease.expires_at_ms)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE tasks SET status = ? WHERE task_id = ?")
            .bind(status_to_str(TaskStatus::Running))
            .bind(task_id.as_str())
            .execute(&mut *tx)
            .await?;
        insert_event(
            &mut tx,
            task_id,
            sequence,
            event_type,
            Some(task.status),
            TaskStatus::Running,
        )
        .await?;
        tx.commit().await?;
        self.get_task(task_id).await
    }

    pub async fn active_lease(&self, task_id: &TaskId) -> StoreResult<Option<LeaseRecord>> {
        let row = sqlx::query("SELECT lease_id, task_id, leased_at_ms, expires_at_ms FROM leases WHERE task_id = ? AND active = 1")
            .bind(task_id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_to_lease).transpose()
    }

    pub async fn recover_stale_leases(&self, now_ms: i64) -> StoreResult<Vec<TaskRecord>> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query("SELECT lease_id, task_id, leased_at_ms, expires_at_ms FROM leases WHERE active = 1 AND expires_at_ms <= ? ORDER BY expires_at_ms ASC")
            .bind(now_ms)
            .fetch_all(&mut *tx)
            .await?;
        let mut recovered = Vec::new();
        for row in rows {
            let lease = row_to_lease(row)?;
            let task = fetch_task_with_executor(&mut tx, &lease.task_id).await?;
            if task.status.is_terminal() {
                continue;
            }
            let seq_row = sqlx::query("SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM task_events WHERE task_id = ?")
                .bind(lease.task_id.as_str())
                .fetch_one(&mut *tx)
                .await?;
            let sequence = seq_row.get::<i64, _>("next_sequence") as u64;
            sqlx::query("UPDATE tasks SET status = ? WHERE task_id = ?")
                .bind(status_to_str(TaskStatus::Pending))
                .bind(lease.task_id.as_str())
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE leases SET active = 0 WHERE task_id = ?")
                .bind(lease.task_id.as_str())
                .execute(&mut *tx)
                .await?;
            insert_event(
                &mut tx,
                &lease.task_id,
                sequence,
                KeryxEventType::RecoveryAction,
                Some(task.status),
                TaskStatus::Pending,
            )
            .await?;
            recovered.push(TaskRecord::new(
                lease.task_id,
                TaskStatus::Pending,
                task.idempotency_key,
            ));
        }
        tx.commit().await?;
        Ok(recovered)
    }
}

async fn fetch_task_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<TaskRecord> {
    let row = sqlx::query("SELECT task_id, status, idempotency_key FROM tasks WHERE task_id = ?")
        .bind(task_id.as_str())
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
    row_to_task(row)
}

async fn insert_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
    sequence: u64,
    event_type: KeryxEventType,
    from_status: Option<TaskStatus>,
    to_status: TaskStatus,
) -> StoreResult<()> {
    sqlx::query(
        "INSERT INTO task_events (task_id, sequence, event_type, from_status, to_status) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(task_id.as_str())
    .bind(sequence as i64)
    .bind(event_type_to_str(event_type))
    .bind(from_status.map(status_to_str))
    .bind(status_to_str(to_status))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn row_to_task(row: sqlx::sqlite::SqliteRow) -> StoreResult<TaskRecord> {
    let task_id = TaskId::new(row.get::<String, _>("task_id"))?;
    let status = str_to_status(&row.get::<String, _>("status"))?;
    let idempotency_key = row
        .try_get::<Option<String>, _>("idempotency_key")?
        .map(IdempotencyKey::new)
        .transpose()?;
    Ok(TaskRecord::new(task_id, status, idempotency_key))
}

fn row_to_lease(row: sqlx::sqlite::SqliteRow) -> StoreResult<LeaseRecord> {
    Ok(LeaseRecord::new(
        LeaseId::new(row.get::<String, _>("lease_id"))?,
        TaskId::new(row.get::<String, _>("task_id"))?,
        row.get::<i64, _>("leased_at_ms"),
        row.get::<i64, _>("expires_at_ms"),
    ))
}

fn row_to_event(row: sqlx::sqlite::SqliteRow) -> StoreResult<TaskEventRecord> {
    let task_id = TaskId::new(row.get::<String, _>("task_id"))?;
    let sequence = row.get::<i64, _>("sequence") as u64;
    let event_type = str_to_event_type(&row.get::<String, _>("event_type"))?;
    let from_status = row
        .try_get::<Option<String>, _>("from_status")?
        .as_deref()
        .map(str_to_status)
        .transpose()?;
    let to_status = str_to_status(&row.get::<String, _>("to_status"))?;
    Ok(TaskEventRecord {
        task_id,
        sequence,
        event_type,
        from_status,
        to_status,
    })
}

const MIGRATION_001: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS agents (agent_id TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS capabilities (capability_id TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS tasks (task_id TEXT PRIMARY KEY, status TEXT NOT NULL, idempotency_key TEXT UNIQUE, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS task_events (task_id TEXT NOT NULL, sequence INTEGER NOT NULL, event_type TEXT NOT NULL, from_status TEXT, to_status TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (task_id, sequence), FOREIGN KEY(task_id) REFERENCES tasks(task_id))",
    "CREATE TABLE IF NOT EXISTS leases (lease_id TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE, leased_at_ms INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1, FOREIGN KEY(task_id) REFERENCES tasks(task_id))",
    "CREATE TABLE IF NOT EXISTS routes (route_id TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS approvals (task_id TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS outbox (frame_id TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS inbox (frame_id TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS artifacts (artifact_id TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS blobs (digest TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS idempotency_keys (key TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE, FOREIGN KEY(task_id) REFERENCES tasks(task_id))",
];

const fn status_to_str(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
    }
}

fn str_to_status(value: &str) -> StoreResult<TaskStatus> {
    match value {
        "created" | "accepted" | "queued" | "awaiting_approval" | "pending" => {
            Ok(TaskStatus::Pending)
        }
        "leased" | "awaiting_input" | "running" => Ok(TaskStatus::Running),
        "completed" => Ok(TaskStatus::Completed),
        "failed" | "canceled" | "timed_out" | "rejected" | "dead_lettered" => {
            Ok(TaskStatus::Failed)
        }
        other => Err(StoreError::Database(format!(
            "unknown task status: {other}"
        ))),
    }
}

const fn event_type_to_str(event_type: KeryxEventType) -> &'static str {
    match event_type {
        KeryxEventType::TaskAccepted => "task_accepted",
        KeryxEventType::TaskQueued => "task_queued",
        KeryxEventType::TaskApprovalRequested => "task_approval_requested",
        KeryxEventType::TaskApprovalGranted => "task_approval_granted",
        KeryxEventType::TaskApprovalDenied => "task_approval_denied",
        KeryxEventType::TaskLeased => "task_leased",
        KeryxEventType::TaskStarted => "task_started",
        KeryxEventType::TaskAwaitingInput => "task_awaiting_input",
        KeryxEventType::TaskCompleted => "task_completed",
        KeryxEventType::TaskFailed => "task_failed",
        KeryxEventType::TaskCanceled => "task_canceled",
        KeryxEventType::TaskTimedOut => "task_timed_out",
        KeryxEventType::TaskDeadLettered => "task_dead_lettered",
        KeryxEventType::RecoveryAction => "recovery_action",
    }
}

fn str_to_event_type(value: &str) -> StoreResult<KeryxEventType> {
    match value {
        "task_accepted" => Ok(KeryxEventType::TaskAccepted),
        "task_queued" => Ok(KeryxEventType::TaskQueued),
        "task_approval_requested" => Ok(KeryxEventType::TaskApprovalRequested),
        "task_approval_granted" => Ok(KeryxEventType::TaskApprovalGranted),
        "task_approval_denied" => Ok(KeryxEventType::TaskApprovalDenied),
        "task_leased" => Ok(KeryxEventType::TaskLeased),
        "task_started" => Ok(KeryxEventType::TaskStarted),
        "task_awaiting_input" => Ok(KeryxEventType::TaskAwaitingInput),
        "task_completed" => Ok(KeryxEventType::TaskCompleted),
        "task_failed" => Ok(KeryxEventType::TaskFailed),
        "task_canceled" => Ok(KeryxEventType::TaskCanceled),
        "task_timed_out" => Ok(KeryxEventType::TaskTimedOut),
        "task_dead_lettered" => Ok(KeryxEventType::TaskDeadLettered),
        "recovery_action" => Ok(KeryxEventType::RecoveryAction),
        other => Err(StoreError::Database(format!("unknown event type: {other}"))),
    }
}
