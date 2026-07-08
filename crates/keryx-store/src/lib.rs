//! Storage traits plus in-memory and SQLite implementations for Hermes Keryx.

use std::{collections::HashMap, path::Path, str::FromStr, sync::Mutex};

use keryx_core::{
    validate_transition, AgentId, IdempotencyKey, KeryxCoreError, KeryxEventType, LeaseId, TaskId,
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
    #[error("task already has an active lease: {task_id}")]
    LeaseConflict { task_id: TaskId },
    #[error("lease {lease_id} does not own task {task_id}")]
    LeaseMismatch { task_id: TaskId, lease_id: LeaseId },
    #[error("worker {worker_id} does not own active lease for task {task_id}")]
    LeaseOwnerMismatch { task_id: TaskId, worker_id: AgentId },
    #[error("lease {lease_id} for task {task_id} is missing a worker owner")]
    LeaseOwnerMissing { task_id: TaskId, lease_id: LeaseId },
    #[error(
        "invalid lease expiry for {lease_id}: requested={requested_expires_at_ms}, current={current_expires_at_ms}, now={now_ms}"
    )]
    InvalidLeaseExpiry {
        lease_id: LeaseId,
        current_expires_at_ms: i64,
        requested_expires_at_ms: i64,
        now_ms: i64,
    },
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

/// Typed summary returned by explicit stale-lease recovery.
///
/// Recovery metadata is intentionally split for Phase 4: durable per-task audit
/// details live in appended `RecoveryAction` event rows, while this report gives
/// daemon/status callers cheap typed counters and recovered task snapshots without
/// reading event payloads. `recovered_tasks` preserves the old caller shape as the
/// ordered list of running tasks returned to `Pending`; terminal and already-pending
/// lease cleanups are summarized as counts instead of synthetic task transitions.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryReport {
    /// Running tasks recovered to `Pending`, ordered by stale lease
    /// `(expires_at_ms ASC, task_id ASC)` after any caller-supplied limit.
    pub recovered_tasks: Vec<TaskRecord>,
    /// Count of stale active leases cleaned for terminal tasks without changing
    /// their terminal status.
    pub cleaned_terminal_leases: usize,
    /// Task snapshots whose event stream is missing or does not replay to the
    /// stored snapshot, ordered by `task_id` for deterministic reports.
    pub corrupted_tasks: Vec<TaskId>,
}

impl RecoveryReport {
    #[must_use]
    pub fn recovered_task_count(&self) -> usize {
        self.recovered_tasks.len()
    }

    #[must_use]
    pub fn corruption_count(&self) -> usize {
        self.corrupted_tasks.len()
    }

    /// Compatibility helper for older callers that only consumed recovered tasks.
    #[must_use]
    pub fn into_recovered_tasks(self) -> Vec<TaskRecord> {
        self.recovered_tasks
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    pub lease_id: LeaseId,
    pub task_id: TaskId,
    pub worker_id: Option<AgentId>,
    pub leased_at_ms: i64,
    pub expires_at_ms: i64,
}

impl LeaseRecord {
    #[must_use]
    pub fn new(
        lease_id: LeaseId,
        task_id: TaskId,
        worker_id: AgentId,
        leased_at_ms: i64,
        expires_at_ms: i64,
    ) -> Self {
        Self {
            lease_id,
            task_id,
            worker_id: Some(worker_id),
            leased_at_ms,
            expires_at_ms,
        }
    }

    #[must_use]
    fn from_parts(
        lease_id: LeaseId,
        task_id: TaskId,
        worker_id: Option<AgentId>,
        leased_at_ms: i64,
        expires_at_ms: i64,
    ) -> Self {
        Self {
            lease_id,
            task_id,
            worker_id,
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

fn validate_accepted_task_status(task: &TaskRecord) -> StoreResult<()> {
    if task.status == TaskStatus::Pending {
        Ok(())
    } else {
        Err(StoreError::Validation(
            ValidationError::InvalidTaskTransition {
                from: TaskStatus::Pending,
                to: task.status,
            },
        ))
    }
}

impl InMemoryStore {
    fn lock(&self) -> StoreResult<std::sync::MutexGuard<'_, InMemoryState>> {
        self.inner.lock().map_err(|_| StoreError::LockPoisoned)
    }

    pub fn lease_task(&self, task_id: &TaskId, lease: LeaseRecord) -> StoreResult<TaskRecord> {
        let mut state = self.lock()?;
        ensure_matching_task_id(task_id, &lease)?;
        ensure_lease_has_owner(&lease)?;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        if state.leases.contains_key(task_id) {
            return Err(StoreError::LeaseConflict {
                task_id: task_id.clone(),
            });
        }
        let transition = validate_transition(task.status, TaskStatus::Running)?;
        let mut updated = task;
        updated.status = TaskStatus::Running;
        state.leases.insert(task_id.clone(), lease);
        append_in_memory_event(
            &mut state,
            task_id,
            transition.event_type,
            Some(transition.from),
            transition.to,
        );
        state.tasks.insert(task_id.clone(), updated.clone());
        Ok(updated)
    }

    pub fn renew_lease(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        now_ms: i64,
        new_expires_at_ms: i64,
    ) -> StoreResult<LeaseRecord> {
        let mut state = self.lock()?;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        require_status(task.status, TaskStatus::Running)?;
        let active = state
            .leases
            .get_mut(task_id)
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, active, lease_id)?;
        ensure_matching_worker_id(task_id, active, worker_id)?;
        ensure_valid_lease_expiry(active, now_ms, new_expires_at_ms)?;
        active.expires_at_ms = new_expires_at_ms;
        Ok(active.clone())
    }

    pub fn complete_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
    ) -> StoreResult<TaskRecord> {
        self.finish_task(task_id, lease_id, worker_id, TaskStatus::Completed)
    }

    pub fn fail_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
    ) -> StoreResult<TaskRecord> {
        self.finish_task(task_id, lease_id, worker_id, TaskStatus::Failed)
    }

    fn finish_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        to: TaskStatus,
    ) -> StoreResult<TaskRecord> {
        let mut state = self.lock()?;
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
        let transition = validate_transition(task.status, to)?;
        let mut updated = task;
        updated.status = to;
        state.leases.remove(task_id);
        append_in_memory_event(
            &mut state,
            task_id,
            transition.event_type,
            Some(transition.from),
            transition.to,
        );
        state.tasks.insert(task_id.clone(), updated.clone());
        Ok(updated)
    }

    pub fn active_lease(&self, task_id: &TaskId) -> StoreResult<Option<LeaseRecord>> {
        Ok(self.lock()?.leases.get(task_id).cloned())
    }

    pub fn recover_stale_leases(
        &self,
        now_ms: i64,
        limit: Option<usize>,
    ) -> StoreResult<RecoveryReport> {
        let mut state = self.lock()?;
        let mut stale_leases = state
            .leases
            .values()
            .filter(|lease| lease.expires_at_ms <= now_ms)
            .cloned()
            .collect::<Vec<_>>();
        stale_leases.sort_by(|left, right| {
            left.expires_at_ms
                .cmp(&right.expires_at_ms)
                .then_with(|| left.task_id.as_str().cmp(right.task_id.as_str()))
        });
        if let Some(limit) = limit {
            stale_leases.truncate(limit);
        }

        let mut recovered = Vec::new();
        let mut cleaned_terminal_leases = 0;
        for lease in stale_leases {
            state.leases.remove(&lease.task_id);
            let Some(task) = state.tasks.get(&lease.task_id).cloned() else {
                continue;
            };

            let (to_status, should_requeue) = match task.status {
                TaskStatus::Running => (TaskStatus::Pending, true),
                TaskStatus::Pending => (TaskStatus::Pending, false),
                TaskStatus::Completed => (TaskStatus::Completed, false),
                TaskStatus::Failed => (TaskStatus::Failed, false),
            };

            let from_status = task.status;
            let mut updated = task;
            updated.status = to_status;
            append_in_memory_event(
                &mut state,
                &lease.task_id,
                KeryxEventType::RecoveryAction,
                Some(from_status),
                to_status,
            );
            state.tasks.insert(lease.task_id.clone(), updated.clone());
            if should_requeue {
                recovered.push(updated);
            } else if from_status.is_terminal() {
                cleaned_terminal_leases += 1;
            }
        }

        let corrupted_tasks = collect_corrupt_in_memory_tasks(&state);
        Ok(RecoveryReport {
            recovered_tasks: recovered,
            cleaned_terminal_leases,
            corrupted_tasks,
        })
    }
}

impl TaskStore for InMemoryStore {
    fn accept_task(&self, task: TaskRecord) -> StoreResult<TaskRecord> {
        let mut state = self.lock()?;
        ensure_pending_accept(&task)?;

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

        if let Some(key) = &task.idempotency_key {
            state
                .idempotency
                .insert(key.clone(), task.task_id().clone());
        }
        append_in_memory_event(
            &mut state,
            task.task_id(),
            KeryxEventType::TaskAccepted,
            None,
            task.status,
        );
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
        let transition = validate_transition(task.status, to)?;

        let mut updated = task;
        updated.status = to;
        if to.is_terminal() {
            state.leases.remove(task_id);
        }
        append_in_memory_event(
            &mut state,
            task_id,
            transition.event_type,
            Some(transition.from),
            transition.to,
        );
        state.tasks.insert(task_id.clone(), updated.clone());
        Ok(updated)
    }

    fn events_for_task(&self, task_id: &TaskId) -> StoreResult<Vec<TaskEventRecord>> {
        let state = self.lock()?;
        match state.events.get(task_id) {
            Some(events) => Ok(events.clone()),
            None if state.tasks.contains_key(task_id) => {
                Err(StoreError::CorruptEventStream(task_id.clone()))
            }
            None => Err(StoreError::TaskNotFound(task_id.clone())),
        }
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
            .ok_or_else(|| StoreError::CorruptEventStream(task_id.clone()))?;
        replay_task_from_snapshot_and_events(&snapshot, events)
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
        let worker_id_column_exists =
            sqlx::query("SELECT 1 FROM pragma_table_info('leases') WHERE name = 'worker_id'")
                .fetch_optional(&mut *tx)
                .await?
                .is_some();
        if !worker_id_column_exists {
            for statement in MIGRATION_002 {
                sqlx::query(statement).execute(&mut *tx).await?;
            }
        }
        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (2, 'lease_worker_identity')",
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
        validate_accepted_task_status(&task)?;
        let mut tx = self.pool.begin().await?;
        ensure_pending_accept(&task)?;

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
        let transition = validate_transition(task.status, to)?;
        let sequence = next_sequence_with_executor(&mut tx, task_id).await?;
        if to.is_terminal() {
            deactivate_lease_for_task_with_executor(&mut tx, task_id).await?;
        }
        update_task_status_with_executor(&mut tx, task_id, to).await?;
        insert_event(
            &mut tx,
            task_id,
            sequence,
            transition.event_type,
            Some(transition.from),
            transition.to,
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
            return match self.get_task(task_id).await {
                Ok(_) => Err(StoreError::CorruptEventStream(task_id.clone())),
                Err(StoreError::TaskNotFound(_)) => Err(StoreError::TaskNotFound(task_id.clone())),
                Err(error) => Err(error),
            };
        }
        rows.into_iter().map(row_to_event).collect()
    }

    pub async fn replay_task(&self, task_id: &TaskId) -> StoreResult<TaskRecord> {
        let task = self.get_task(task_id).await?;
        let events = self.events_for_task(task_id).await?;
        replay_task_from_snapshot_and_events(&task, &events)
    }

    pub async fn lease_task(
        &self,
        task_id: &TaskId,
        lease: LeaseRecord,
    ) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;
        ensure_matching_task_id(task_id, &lease)?;
        ensure_lease_has_owner(&lease)?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        if fetch_active_lease_with_executor(&mut tx, task_id)
            .await?
            .is_some()
        {
            return Err(StoreError::LeaseConflict {
                task_id: task_id.clone(),
            });
        }
        let transition = validate_transition(task.status, TaskStatus::Running)?;
        let sequence = next_sequence_with_executor(&mut tx, task_id).await?;
        sqlx::query(
            "INSERT OR REPLACE INTO leases (lease_id, task_id, worker_id, leased_at_ms, expires_at_ms, active) VALUES (?, ?, ?, ?, ?, 1)",
        )
        .bind(lease.lease_id.as_str())
        .bind(task_id.as_str())
        .bind(
            lease
                .worker_id
                .as_ref()
                .expect("validated lease worker owner")
                .as_str(),
        )
        .bind(lease.leased_at_ms)
        .bind(lease.expires_at_ms)
        .execute(&mut *tx)
        .await?;
        update_task_status_with_executor(&mut tx, task_id, TaskStatus::Running).await?;
        insert_event(
            &mut tx,
            task_id,
            sequence,
            transition.event_type,
            Some(transition.from),
            transition.to,
        )
        .await?;
        tx.commit().await?;
        self.get_task(task_id).await
    }

    pub async fn renew_lease(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        now_ms: i64,
        new_expires_at_ms: i64,
    ) -> StoreResult<LeaseRecord> {
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        require_status(task.status, TaskStatus::Running)?;
        let active = fetch_active_lease_with_executor(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, &active, lease_id)?;
        ensure_matching_worker_id(task_id, &active, worker_id)?;
        ensure_valid_lease_expiry(&active, now_ms, new_expires_at_ms)?;

        sqlx::query(
            "UPDATE leases SET expires_at_ms = ?, active = 1 WHERE task_id = ? AND lease_id = ? AND worker_id = ?",
        )
        .bind(new_expires_at_ms)
        .bind(task_id.as_str())
        .bind(lease_id.as_str())
        .bind(worker_id.as_str())
        .execute(&mut *tx)
        .await?;
        touch_task_updated_at_with_executor(&mut tx, task_id).await?;
        tx.commit().await?;

        Ok(LeaseRecord::from_parts(
            active.lease_id,
            active.task_id,
            active.worker_id,
            active.leased_at_ms,
            new_expires_at_ms,
        ))
    }

    pub async fn complete_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
    ) -> StoreResult<TaskRecord> {
        self.finish_task(task_id, lease_id, worker_id, TaskStatus::Completed)
            .await
    }

    pub async fn fail_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
    ) -> StoreResult<TaskRecord> {
        self.finish_task(task_id, lease_id, worker_id, TaskStatus::Failed)
            .await
    }

    async fn finish_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        to: TaskStatus,
    ) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let active = fetch_active_lease_with_executor(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, &active, lease_id)?;
        ensure_matching_worker_id(task_id, &active, worker_id)?;
        let transition = validate_transition(task.status, to)?;
        let sequence = next_sequence_with_executor(&mut tx, task_id).await?;
        deactivate_lease_for_task_with_executor(&mut tx, task_id).await?;
        update_task_status_with_executor(&mut tx, task_id, to).await?;
        insert_event(
            &mut tx,
            task_id,
            sequence,
            transition.event_type,
            Some(transition.from),
            transition.to,
        )
        .await?;
        tx.commit().await?;
        self.get_task(task_id).await
    }

    pub async fn active_lease(&self, task_id: &TaskId) -> StoreResult<Option<LeaseRecord>> {
        let row = sqlx::query(
            "SELECT lease_id, task_id, worker_id, leased_at_ms, expires_at_ms FROM leases WHERE task_id = ? AND active = 1",
        )
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_lease).transpose()
    }

    pub async fn recover_stale_leases(
        &self,
        now_ms: i64,
        limit: Option<usize>,
    ) -> StoreResult<RecoveryReport> {
        let mut tx = self.pool.begin().await?;
        let rows = match limit {
            Some(limit) => {
                sqlx::query(
                    "SELECT lease_id, task_id, worker_id, leased_at_ms, expires_at_ms FROM leases WHERE active = 1 AND expires_at_ms <= ? ORDER BY expires_at_ms ASC, task_id ASC LIMIT ?",
                )
                .bind(now_ms)
                .bind(limit as i64)
                .fetch_all(&mut *tx)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT lease_id, task_id, worker_id, leased_at_ms, expires_at_ms FROM leases WHERE active = 1 AND expires_at_ms <= ? ORDER BY expires_at_ms ASC, task_id ASC",
                )
                .bind(now_ms)
                .fetch_all(&mut *tx)
                .await?
            }
        };
        let mut report = recover_sqlite_leases_with_executor(&mut tx, rows).await?;
        report.corrupted_tasks = collect_corrupt_sqlite_tasks_with_executor(&mut tx).await?;
        tx.commit().await?;
        Ok(report)
    }
}

fn ensure_pending_accept(task: &TaskRecord) -> StoreResult<()> {
    if task.status == TaskStatus::Pending {
        Ok(())
    } else {
        Err(StoreError::Validation(
            ValidationError::InvalidTaskTransition {
                from: task.status,
                to: TaskStatus::Pending,
            },
        ))
    }
}

fn ensure_matching_task_id(task_id: &TaskId, lease: &LeaseRecord) -> StoreResult<()> {
    if &lease.task_id == task_id {
        Ok(())
    } else {
        Err(StoreError::Database(format!(
            "lease task id mismatch: expected {}, got {}",
            task_id, lease.task_id
        )))
    }
}

fn ensure_lease_has_owner(lease: &LeaseRecord) -> StoreResult<()> {
    if lease.worker_id.is_some() {
        Ok(())
    } else {
        Err(StoreError::LeaseOwnerMissing {
            task_id: lease.task_id.clone(),
            lease_id: lease.lease_id.clone(),
        })
    }
}

fn require_status(current: TaskStatus, required: TaskStatus) -> StoreResult<()> {
    if current == required {
        Ok(())
    } else if current.is_terminal() {
        Err(StoreError::Validation(
            ValidationError::TerminalTaskTransition {
                from: current,
                to: required,
            },
        ))
    } else {
        Err(StoreError::Validation(
            ValidationError::InvalidTaskTransition {
                from: current,
                to: required,
            },
        ))
    }
}

fn ensure_matching_lease_id(
    task_id: &TaskId,
    active: &LeaseRecord,
    lease_id: &LeaseId,
) -> StoreResult<()> {
    if &active.lease_id == lease_id {
        Ok(())
    } else {
        Err(StoreError::LeaseMismatch {
            task_id: task_id.clone(),
            lease_id: lease_id.clone(),
        })
    }
}

fn ensure_matching_worker_id(
    task_id: &TaskId,
    active: &LeaseRecord,
    worker_id: &AgentId,
) -> StoreResult<()> {
    match &active.worker_id {
        Some(active_worker_id) if active_worker_id == worker_id => Ok(()),
        Some(_) => Err(StoreError::LeaseOwnerMismatch {
            task_id: task_id.clone(),
            worker_id: worker_id.clone(),
        }),
        None => Err(StoreError::LeaseOwnerMissing {
            task_id: task_id.clone(),
            lease_id: active.lease_id.clone(),
        }),
    }
}

fn ensure_valid_lease_expiry(
    active: &LeaseRecord,
    now_ms: i64,
    new_expires_at_ms: i64,
) -> StoreResult<()> {
    if new_expires_at_ms > now_ms && new_expires_at_ms > active.expires_at_ms {
        Ok(())
    } else {
        Err(StoreError::InvalidLeaseExpiry {
            lease_id: active.lease_id.clone(),
            current_expires_at_ms: active.expires_at_ms,
            requested_expires_at_ms: new_expires_at_ms,
            now_ms,
        })
    }
}

fn append_in_memory_event(
    state: &mut InMemoryState,
    task_id: &TaskId,
    event_type: KeryxEventType,
    from_status: Option<TaskStatus>,
    to_status: TaskStatus,
) {
    let sequence = state
        .events
        .get(task_id)
        .map_or(1, |events| events.len() as u64 + 1);
    state
        .events
        .entry(task_id.clone())
        .or_default()
        .push(TaskEventRecord {
            task_id: task_id.clone(),
            sequence,
            event_type,
            from_status,
            to_status,
        });
}

async fn fetch_task_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<TaskRecord> {
    fetch_task_optional_with_executor(tx, task_id)
        .await?
        .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))
}

async fn fetch_task_optional_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<Option<TaskRecord>> {
    let row = sqlx::query("SELECT task_id, status, idempotency_key FROM tasks WHERE task_id = ?")
        .bind(task_id.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    row.map(row_to_task).transpose()
}

async fn fetch_active_lease_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<Option<LeaseRecord>> {
    let row = sqlx::query(
        "SELECT lease_id, task_id, worker_id, leased_at_ms, expires_at_ms FROM leases WHERE task_id = ? AND active = 1",
    )
    .bind(task_id.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    row.map(row_to_lease).transpose()
}

async fn recover_sqlite_leases_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> StoreResult<RecoveryReport> {
    let mut recovered = Vec::new();
    let mut cleaned_terminal_leases = 0;
    for row in rows {
        let lease = row_to_lease(row)?;
        let task = fetch_task_optional_with_executor(tx, &lease.task_id).await?;
        deactivate_lease_for_task_with_executor(tx, &lease.task_id).await?;

        let Some(task) = task else {
            continue;
        };

        let (to_status, should_requeue) = match task.status {
            TaskStatus::Running => (TaskStatus::Pending, true),
            TaskStatus::Pending => (TaskStatus::Pending, false),
            TaskStatus::Completed => (TaskStatus::Completed, false),
            TaskStatus::Failed => (TaskStatus::Failed, false),
        };

        if to_status != task.status {
            update_task_status_with_executor(tx, &lease.task_id, to_status).await?;
        } else {
            touch_task_updated_at_with_executor(tx, &lease.task_id).await?;
        }
        let sequence = next_sequence_with_executor(tx, &lease.task_id).await?;
        insert_event(
            tx,
            &lease.task_id,
            sequence,
            KeryxEventType::RecoveryAction,
            Some(task.status),
            to_status,
        )
        .await?;

        if should_requeue {
            recovered.push(TaskRecord::new(
                lease.task_id,
                to_status,
                task.idempotency_key,
            ));
        } else if task.status.is_terminal() {
            cleaned_terminal_leases += 1;
        }
    }
    Ok(RecoveryReport {
        recovered_tasks: recovered,
        cleaned_terminal_leases,
        corrupted_tasks: Vec::new(),
    })
}

async fn collect_corrupt_sqlite_tasks_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> StoreResult<Vec<TaskId>> {
    let rows = sqlx::query("SELECT task_id FROM tasks ORDER BY task_id ASC")
        .fetch_all(&mut **tx)
        .await?;

    let mut corrupted = Vec::new();
    for row in rows {
        let task_id = TaskId::new(row.get::<String, _>("task_id"))?;
        let task = fetch_task_with_executor(tx, &task_id).await?;
        match events_for_task_with_executor(tx, &task_id).await {
            Ok(events)
                if matches!(
                    replay_task_from_snapshot_and_events(&task, &events),
                    Err(StoreError::CorruptEventStream(_))
                ) =>
            {
                corrupted.push(task_id);
            }
            Err(StoreError::CorruptEventStream(_)) => corrupted.push(task_id),
            Ok(_) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(corrupted)
}

async fn events_for_task_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<Vec<TaskEventRecord>> {
    let rows = sqlx::query(
        "SELECT task_id, sequence, event_type, from_status, to_status FROM task_events WHERE task_id = ? ORDER BY sequence ASC",
    )
    .bind(task_id.as_str())
    .fetch_all(&mut **tx)
    .await?;
    if rows.is_empty() {
        return Err(StoreError::CorruptEventStream(task_id.clone()));
    }
    rows.into_iter().map(row_to_event).collect()
}

fn collect_corrupt_in_memory_tasks(state: &InMemoryState) -> Vec<TaskId> {
    let mut task_ids = state.tasks.keys().cloned().collect::<Vec<_>>();
    task_ids.sort();
    task_ids
        .into_iter()
        .filter(|task_id| {
            let Some(task) = state.tasks.get(task_id) else {
                return false;
            };
            let Some(events) = state.events.get(task_id) else {
                return true;
            };
            matches!(
                replay_task_from_snapshot_and_events(task, events),
                Err(StoreError::CorruptEventStream(_))
            )
        })
        .collect()
}

fn replay_task_from_snapshot_and_events(
    snapshot: &TaskRecord,
    events: &[TaskEventRecord],
) -> StoreResult<TaskRecord> {
    let task_id = snapshot.task_id().clone();
    let Some(first) = events.first() else {
        return Err(StoreError::CorruptEventStream(task_id));
    };

    if first.task_id != task_id
        || first.sequence != 1
        || first.event_type != KeryxEventType::TaskAccepted
        || first.from_status.is_some()
        || first.to_status != TaskStatus::Pending
    {
        return Err(StoreError::CorruptEventStream(task_id));
    }

    let mut current_status = first.to_status;
    for (index, event) in events.iter().enumerate().skip(1) {
        if event.task_id != snapshot.task_id().clone()
            || event.sequence != (index as u64) + 1
            || event.from_status != Some(current_status)
        {
            return Err(StoreError::CorruptEventStream(task_id));
        }

        match event.event_type {
            KeryxEventType::TaskStarted
            | KeryxEventType::TaskCompleted
            | KeryxEventType::TaskFailed => {
                let transition = validate_transition(current_status, event.to_status)
                    .map_err(|_| StoreError::CorruptEventStream(task_id.clone()))?;
                if transition.event_type != event.event_type {
                    return Err(StoreError::CorruptEventStream(task_id));
                }
                current_status = transition.to;
            }
            KeryxEventType::RecoveryAction => {
                let expected_to_status = match current_status {
                    TaskStatus::Running => TaskStatus::Pending,
                    TaskStatus::Pending => TaskStatus::Pending,
                    TaskStatus::Completed => TaskStatus::Completed,
                    TaskStatus::Failed => TaskStatus::Failed,
                };
                if event.to_status != expected_to_status {
                    return Err(StoreError::CorruptEventStream(task_id));
                }
                current_status = event.to_status;
            }
            _ => return Err(StoreError::CorruptEventStream(task_id)),
        }
    }

    if snapshot.status != current_status {
        return Err(StoreError::CorruptEventStream(task_id));
    }

    Ok(snapshot.clone())
}

async fn next_sequence_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<u64> {
    let row = sqlx::query(
        "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM task_events WHERE task_id = ?",
    )
    .bind(task_id.as_str())
    .fetch_one(&mut **tx)
    .await?;
    Ok(row.get::<i64, _>("next_sequence") as u64)
}

async fn update_task_status_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
    status: TaskStatus,
) -> StoreResult<()> {
    sqlx::query("UPDATE tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE task_id = ?")
        .bind(status_to_str(status))
        .bind(task_id.as_str())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn touch_task_updated_at_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<()> {
    sqlx::query("UPDATE tasks SET updated_at = CURRENT_TIMESTAMP WHERE task_id = ?")
        .bind(task_id.as_str())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn deactivate_lease_for_task_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<()> {
    sqlx::query("UPDATE leases SET active = 0 WHERE task_id = ? AND active = 1")
        .bind(task_id.as_str())
        .execute(&mut **tx)
        .await?;
    Ok(())
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
    Ok(LeaseRecord::from_parts(
        LeaseId::new(row.get::<String, _>("lease_id"))?,
        TaskId::new(row.get::<String, _>("task_id"))?,
        row.try_get::<Option<String>, _>("worker_id")?
            .map(AgentId::new)
            .transpose()?,
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

const MIGRATION_002: &[&str] = &["ALTER TABLE leases ADD COLUMN worker_id TEXT"];

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use keryx_core::{AgentId, IdempotencyKey, KeryxEventType, LeaseId, TaskId, TaskStatus};

    use super::{
        append_in_memory_event, InMemoryState, InMemoryStore, LeaseRecord, StoreError, TaskRecord,
        TaskStore,
    };

    fn task(id: &str, status: TaskStatus, idem: Option<&str>) -> TaskRecord {
        TaskRecord::new(
            TaskId::new(id).unwrap(),
            status,
            idem.map(|value| IdempotencyKey::new(value).unwrap()),
        )
    }

    fn lease(task_id: &TaskId, lease_id: &str, worker_id: &str, expires_at_ms: i64) -> LeaseRecord {
        LeaseRecord::new(
            LeaseId::new(lease_id).unwrap(),
            task_id.clone(),
            AgentId::new(worker_id).unwrap(),
            100,
            expires_at_ms,
        )
    }

    #[test]
    fn in_memory_recovery_cleans_terminal_stale_lease_without_changing_terminal_status() {
        let task = task(
            "corrupt-terminal-task",
            TaskStatus::Completed,
            Some("terminal-idem"),
        );
        let mut state = InMemoryState {
            tasks: HashMap::from([(task.task_id().clone(), task.clone())]),
            events: HashMap::new(),
            idempotency: HashMap::new(),
            leases: HashMap::from([(
                task.task_id().clone(),
                lease(task.task_id(), "terminal-lease", "terminal-worker", 500),
            )]),
        };
        append_in_memory_event(
            &mut state,
            task.task_id(),
            KeryxEventType::TaskAccepted,
            None,
            TaskStatus::Pending,
        );
        append_in_memory_event(
            &mut state,
            task.task_id(),
            KeryxEventType::TaskStarted,
            Some(TaskStatus::Pending),
            TaskStatus::Running,
        );
        append_in_memory_event(
            &mut state,
            task.task_id(),
            KeryxEventType::TaskCompleted,
            Some(TaskStatus::Running),
            TaskStatus::Completed,
        );
        let store = InMemoryStore {
            inner: Mutex::new(state),
        };

        let report = store.recover_stale_leases(501, None).unwrap();

        assert!(report.recovered_tasks.is_empty());
        assert_eq!(report.cleaned_terminal_leases, 1);
        assert_eq!(report.corruption_count(), 0);
        assert_eq!(
            store.get_task(task.task_id()).unwrap().status,
            TaskStatus::Completed
        );
        assert!(store.active_lease(task.task_id()).unwrap().is_none());
        let events = store.events_for_task(task.task_id()).unwrap();
        let last = events.last().unwrap();
        assert_eq!(last.event_type, KeryxEventType::RecoveryAction);
        assert_eq!(last.from_status, Some(TaskStatus::Completed));
        assert_eq!(last.to_status, TaskStatus::Completed);
    }

    #[test]
    fn in_memory_replay_reports_missing_events_as_corruption() {
        let task = task(
            "missing-events-task",
            TaskStatus::Pending,
            Some("missing-events-idem"),
        );
        let store = InMemoryStore {
            inner: Mutex::new(InMemoryState {
                tasks: HashMap::from([(task.task_id().clone(), task.clone())]),
                events: HashMap::new(),
                idempotency: HashMap::new(),
                leases: HashMap::new(),
            }),
        };

        assert_eq!(
            store.events_for_task(task.task_id()).unwrap_err(),
            StoreError::CorruptEventStream(task.task_id().clone())
        );
        assert_eq!(
            store.replay_task(task.task_id()).unwrap_err(),
            StoreError::CorruptEventStream(task.task_id().clone())
        );
        let report = store.recover_stale_leases(0, None).unwrap();
        assert_eq!(report.corrupted_tasks, vec![task.task_id().clone()]);
    }

    #[test]
    fn in_memory_replay_reports_snapshot_event_status_mismatch_as_corruption() {
        let task = task(
            "mismatched-snapshot-task",
            TaskStatus::Running,
            Some("mismatched-snapshot-idem"),
        );
        let mut state = InMemoryState {
            tasks: HashMap::from([(task.task_id().clone(), task.clone())]),
            events: HashMap::new(),
            idempotency: HashMap::new(),
            leases: HashMap::new(),
        };
        append_in_memory_event(
            &mut state,
            task.task_id(),
            KeryxEventType::TaskAccepted,
            None,
            TaskStatus::Pending,
        );
        let store = InMemoryStore {
            inner: Mutex::new(state),
        };

        assert_eq!(
            store.replay_task(task.task_id()).unwrap_err(),
            StoreError::CorruptEventStream(task.task_id().clone())
        );
        let report = store.recover_stale_leases(0, None).unwrap();
        assert_eq!(report.corruption_count(), 1);
        assert_eq!(report.corrupted_tasks, vec![task.task_id().clone()]);
    }
}
