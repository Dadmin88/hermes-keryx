//! Storage traits plus in-memory and SQLite implementations for Hermes Keryx.

mod results;
pub use results::*;

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Mutex,
};

use keryx_core::{
    is_valid_operational_legacy, normalize_legacy_transition, should_inline,
    validate_artifact_size, validate_cancel_transition, validate_transition, AgentId, ArtifactId,
    CanonicalTransition, Digest, IdempotencyKey, KeryxCoreError, KeryxEventType, LeaseId,
    LegacyEventType, MediaType, PeerId, RetryPolicy, TaskId, TaskStatus, ValidationError,
};
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, Row, SqlitePool};
use thiserror::Error;
use tracing::{info, instrument};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("task already exists: {0}")]
    TaskAlreadyExists(TaskId),
    #[error("artifact not found: {0}")]
    ArtifactNotFound(ArtifactId),
    #[error("artifact too large: {byte_len} bytes exceeds {limit_bytes}")]
    ArtifactTooLarge { byte_len: u64, limit_bytes: u64 },
    #[error("digest mismatch: expected {expected}, computed {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("artifact length mismatch: declared {declared}, actual {actual}")]
    ArtifactLengthMismatch { declared: u64, actual: u64 },
    #[error("origin result artifact id mismatch for task {task_id} ordinal {ordinal}")]
    OriginResultArtifactIdMismatch { task_id: TaskId, ordinal: u32 },
    #[error("origin result artifact ordinal mismatch: expected {expected}, got {actual}")]
    OriginResultArtifactOrdinalMismatch { expected: u32, actual: u32 },
    #[error("origin result artifact task mismatch: expected {task_id}, got {artifact_task_id}")]
    OriginResultArtifactTaskMismatch {
        task_id: TaskId,
        artifact_task_id: TaskId,
    },
    #[error("origin result artifact conflict: {0}")]
    OriginResultArtifactConflict(ArtifactId),
    #[error("idempotency key {key} already belongs to task {existing_task_id}")]
    IdempotencyConflict {
        key: IdempotencyKey,
        existing_task_id: TaskId,
    },
    #[error("task envelope not found: {0}")]
    TaskEnvelopeNotFound(TaskId),
    #[error("task envelope id {envelope_task_id} does not match task {task_id}")]
    TaskEnvelopeMismatch {
        task_id: TaskId,
        envelope_task_id: TaskId,
    },
    #[error("task envelope conflicts with the stored envelope for task {0}")]
    TaskEnvelopeConflict(TaskId),
    #[error("transport context not found for task {0}")]
    TransportContextNotFound(TaskId),
    #[error("transport context conflicts for task {0}")]
    TransportContextConflict(TaskId),
    #[error("task {task_id} targets executor {expected}, not local peer {actual}")]
    TaskExecutorMismatch {
        task_id: TaskId,
        expected: PeerId,
        actual: PeerId,
    },
    #[error("transport context task {context_task_id} does not match task {task_id}")]
    TransportContextTaskMismatch {
        task_id: TaskId,
        context_task_id: TaskId,
    },
    #[error("terminal result not found for task {0}")]
    TerminalResultNotFound(TaskId),
    #[error("terminal result conflicts for task {0}")]
    TerminalResultConflict(TaskId),
    #[error("terminal result task {result_task_id} does not match task {task_id}")]
    TerminalResultTaskMismatch {
        task_id: TaskId,
        result_task_id: TaskId,
    },
    #[error("terminal result for task {0} is not terminal")]
    TerminalResultNotTerminal(TaskId),
    #[error("result delivery lease mismatch: {0}")]
    ResultDeliveryLeaseMismatch(String),
    #[error(
        "remote result executor mismatch for task {task_id}: expected {expected}, got {actual}"
    )]
    RemoteResultExecutorMismatch {
        task_id: TaskId,
        expected: keryx_core::PeerId,
        actual: keryx_core::PeerId,
    },
    #[error("remote result for task {task_id} was settled against terminal reason {reason}")]
    RemoteResultTerminallySettled {
        task_id: TaskId,
        reason: RemoteResultTerminalReason,
    },

    #[error(
        "remote result for task {task_id} carries artifacts that cannot settle terminal reason {reason}"
    )]
    RemoteResultTerminalArtifactsRejected {
        task_id: TaskId,
        reason: RemoteResultTerminalReason,
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
    #[error(
        "task deadline expired: task={task_id} deadline_ms={deadline_ms} attempted_lease_at_ms={attempted_lease_at_ms}"
    )]
    TaskDeadlineExpired {
        task_id: TaskId,
        deadline_ms: i64,
        attempted_lease_at_ms: i64,
    },
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
    #[error("unsupported schema version: found={found_version}, supported={supported_version}")]
    UnsupportedSchema {
        found_version: i64,
        supported_version: i64,
    },
    #[error("migration failed: {0}")]
    MigrationFailed(String),
    #[error("startup recovery found unrepaired corruption in tasks: {corrupted_tasks:?}")]
    UnrepairedCorruption { corrupted_tasks: Vec<TaskId> },
    #[error("blob directory error: {0}")]
    BlobDir(String),
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

pub const CURRENT_SCHEMA_VERSION: i64 = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    id: TaskId,
    pub status: TaskStatus,
    pub idempotency_key: Option<IdempotencyKey>,
    pub deadline_ms: Option<i64>,
    pub retry_count: u32,
    pub dead_lettered: bool,
    pub dead_letter_reason: Option<String>,
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
            deadline_ms: None,
            retry_count: 0,
            dead_lettered: false,
            dead_letter_reason: None,
        }
    }

    #[must_use]
    pub const fn task_id(&self) -> &TaskId {
        &self.id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTaskEnvelope {
    pub task: TaskRecord,
    pub envelope: TaskEnvelopeRecord,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub artifact_id: ArtifactId,
    pub task_id: TaskId,
    pub digest: Digest,
    pub media_type: MediaType,
    pub byte_len: u64,
    pub inline: bool,
    pub created_at: String,
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
    fn count_tasks_by_status(&self, status: TaskStatus) -> StoreResult<u64>;
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
    artifacts: HashMap<ArtifactId, ArtifactRecord>,
    inline_artifacts: HashMap<ArtifactId, Vec<u8>>,
    blobs: HashMap<Digest, (Vec<u8>, u32)>,
    envelopes: HashMap<TaskId, TaskEnvelopeRecord>,
    transport_contexts: HashMap<TaskId, TaskTransportContextRecord>,
    terminal_results: HashMap<TaskId, TerminalResultRecord>,
    result_outbox: HashMap<String, ResultOutboxRecord>,
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

enum LegacyAppendPlan {
    Lifecycle(CanonicalTransition),
    Operational {
        event_type: KeryxEventType,
        status: TaskStatus,
    },
}

fn plan_legacy_event_append(
    from_status: TaskStatus,
    legacy_event: LegacyEventType,
) -> StoreResult<LegacyAppendPlan> {
    if let Some(transition) = normalize_legacy_transition(from_status, legacy_event) {
        return Ok(LegacyAppendPlan::Lifecycle(transition));
    }
    if is_valid_operational_legacy(from_status, legacy_event) {
        return Ok(LegacyAppendPlan::Operational {
            event_type: legacy_event.as_keryx_event_type(),
            status: from_status,
        });
    }
    Err(StoreError::Validation(
        ValidationError::InvalidTaskTransition {
            from: from_status,
            to: from_status,
        },
    ))
}

fn is_replayable_operational_legacy_event(event_type: KeryxEventType) -> bool {
    matches!(
        event_type,
        KeryxEventType::TaskQueued
            | KeryxEventType::TaskApprovalRequested
            | KeryxEventType::TaskApprovalGranted
            | KeryxEventType::TaskAwaitingInput
    )
}

fn validate_deadline_transition(from: TaskStatus) -> StoreResult<()> {
    match from {
        TaskStatus::Pending | TaskStatus::Running => Ok(()),
        TaskStatus::Completed | TaskStatus::Failed => Err(StoreError::Validation(
            ValidationError::TerminalTaskTransition {
                from,
                to: TaskStatus::Failed,
            },
        )),
    }
}

fn ensure_active_lease_unexpired(active: &LeaseRecord, now_ms: i64) -> StoreResult<()> {
    ensure_valid_lease_expiry(active, now_ms, active.expires_at_ms.saturating_add(1))
}

impl InMemoryStore {
    fn lock(&self) -> StoreResult<std::sync::MutexGuard<'_, InMemoryState>> {
        self.inner.lock().map_err(|_| StoreError::LockPoisoned)
    }

    pub fn accept_task_with_envelope(
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
    }

    pub fn pending_task_envelopes(&self, limit: usize) -> StoreResult<Vec<PendingTaskEnvelope>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let state = self.lock()?;
        let mut pending = state
            .tasks
            .values()
            .filter(|task| task.status == TaskStatus::Pending)
            .filter_map(|task| {
                state
                    .envelopes
                    .get(task.task_id())
                    .map(|envelope| PendingTaskEnvelope {
                        task: task.clone(),
                        envelope: envelope.clone(),
                    })
            })
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| {
            left.envelope
                .received_at_ms
                .cmp(&right.envelope.received_at_ms)
                .then_with(|| {
                    left.task
                        .task_id()
                        .as_str()
                        .cmp(right.task.task_id().as_str())
                })
        });
        pending.truncate(limit);
        Ok(pending)
    }

    pub fn claimable_pending_task_envelopes(
        &self,
        local_peer_id: &PeerId,
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
            .filter(|task| {
                state
                    .transport_contexts
                    .get(task.task_id())
                    .and_then(|context| context.expected_executor_peer_id.as_ref())
                    .map(|expected| expected == local_peer_id)
                    .unwrap_or(true)
            })
            .filter_map(|task| {
                state
                    .envelopes
                    .get(task.task_id())
                    .map(|envelope| PendingTaskEnvelope {
                        task: task.clone(),
                        envelope: envelope.clone(),
                    })
            })
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| {
            left.envelope
                .received_at_ms
                .cmp(&right.envelope.received_at_ms)
                .then_with(|| {
                    left.task
                        .task_id()
                        .as_str()
                        .cmp(right.task.task_id().as_str())
                })
        });
        pending.truncate(limit);
        Ok(pending)
    }

    pub fn lease_task(&self, task_id: &TaskId, lease: LeaseRecord) -> StoreResult<TaskRecord> {
        self.lease_task_with_peer_guard(task_id, lease, None)
    }

    pub fn lease_task_for_peer(
        &self,
        task_id: &TaskId,
        lease: LeaseRecord,
        local_peer_id: &PeerId,
    ) -> StoreResult<TaskRecord> {
        self.lease_task_with_peer_guard(task_id, lease, Some(local_peer_id))
    }

    fn lease_task_with_peer_guard(
        &self,
        task_id: &TaskId,
        lease: LeaseRecord,
        local_peer_id: Option<&PeerId>,
    ) -> StoreResult<TaskRecord> {
        let mut state = self.lock()?;
        if let Some(local_peer_id) = local_peer_id {
            if let Some(expected) = state
                .transport_contexts
                .get(task_id)
                .and_then(|context| context.expected_executor_peer_id.as_ref())
                .filter(|expected| *expected != local_peer_id)
            {
                return Err(StoreError::TaskExecutorMismatch {
                    task_id: task_id.clone(),
                    expected: expected.clone(),
                    actual: local_peer_id.clone(),
                });
            }
        }
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
        if let Some(deadline_ms) = task
            .deadline_ms
            .filter(|deadline_ms| *deadline_ms <= lease.leased_at_ms)
        {
            validate_deadline_transition(task.status)?;
            let mut failed = task;
            let from_status = failed.status;
            failed.status = TaskStatus::Failed;
            append_in_memory_event(
                &mut state,
                task_id,
                KeryxEventType::TaskTimedOut,
                Some(from_status),
                TaskStatus::Failed,
            );
            state.tasks.insert(task_id.clone(), failed);
            return Err(StoreError::TaskDeadlineExpired {
                task_id: task_id.clone(),
                deadline_ms,
                attempted_lease_at_ms: lease.leased_at_ms,
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
        error_reason: &str,
        policy: &RetryPolicy,
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
        require_status(task.status, TaskStatus::Running)?;

        if policy.max_retries == 0 {
            return self.finish_task_in_state(
                &mut state,
                task_id,
                lease_id,
                worker_id,
                TaskStatus::Failed,
                task.retry_count,
                false,
                None,
            );
        }

        if policy.should_retry_after_failure(task.retry_count) {
            return self.retry_task_in_state(&mut state, task_id, &active, task);
        }

        self.dead_letter_task_in_state(&mut state, task_id, &active, task, error_reason)
    }

    pub fn retry_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
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
        self.retry_task_in_state(&mut state, task_id, &active, task)
    }

    pub fn dead_letter_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        reason: &str,
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
        self.dead_letter_task_in_state(&mut state, task_id, &active, task, reason)
    }

    pub fn cancel_task(
        &self,
        task_id: &TaskId,
        _reason: &str,
        now_ms: i64,
    ) -> StoreResult<TaskRecord> {
        let mut state = self.lock()?;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        if task.status.is_terminal() {
            return Ok(task);
        }
        if task.status == TaskStatus::Running {
            let active = state
                .leases
                .get(task_id)
                .cloned()
                .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
            ensure_active_lease_unexpired(&active, now_ms)?;
        }
        let transition = validate_cancel_transition(task.status)?;
        let mut updated = task;
        updated.status = transition.to;
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

    pub fn fail_expired_deadlines(
        &self,
        now_ms: i64,
        limit: Option<usize>,
    ) -> StoreResult<Vec<TaskRecord>> {
        let mut state = self.lock()?;
        let mut expired: Vec<(i64, TaskId)> = state
            .tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::Running))
            .filter_map(|task| {
                task.deadline_ms
                    .filter(|deadline_ms| *deadline_ms <= now_ms)
                    .map(|deadline_ms| (deadline_ms, task.task_id().clone()))
            })
            .collect();
        expired.sort_by(|(left_deadline, left_id), (right_deadline, right_id)| {
            left_deadline
                .cmp(right_deadline)
                .then_with(|| left_id.as_str().cmp(right_id.as_str()))
        });
        if let Some(limit) = limit {
            expired.truncate(limit);
        }

        let mut failed = Vec::with_capacity(expired.len());
        for (_, expired_task_id) in expired {
            let task = state
                .tasks
                .get(&expired_task_id)
                .cloned()
                .ok_or_else(|| StoreError::TaskNotFound(expired_task_id.clone()))?;
            validate_deadline_transition(task.status)?;
            let mut updated = task;
            let from_status = updated.status;
            updated.status = TaskStatus::Failed;
            state.leases.remove(&expired_task_id);
            append_in_memory_event(
                &mut state,
                &expired_task_id,
                KeryxEventType::TaskTimedOut,
                Some(from_status),
                TaskStatus::Failed,
            );
            state.tasks.insert(expired_task_id, updated.clone());
            failed.push(updated);
        }
        Ok(failed)
    }

    fn retry_task_in_state(
        &self,
        state: &mut InMemoryState,
        task_id: &TaskId,
        active: &LeaseRecord,
        mut task: TaskRecord,
    ) -> StoreResult<TaskRecord> {
        ensure_matching_lease_id(task_id, active, &active.lease_id)?;
        require_status(task.status, TaskStatus::Running)?;
        task.retry_count = task.retry_count.saturating_add(1);
        task.status = TaskStatus::Pending;
        state.leases.remove(task_id);
        append_in_memory_event(
            state,
            task_id,
            KeryxEventType::RecoveryAction,
            Some(TaskStatus::Running),
            TaskStatus::Pending,
        );
        state.tasks.insert(task_id.clone(), task.clone());
        Ok(task)
    }

    fn dead_letter_task_in_state(
        &self,
        state: &mut InMemoryState,
        task_id: &TaskId,
        active: &LeaseRecord,
        mut task: TaskRecord,
        reason: &str,
    ) -> StoreResult<TaskRecord> {
        ensure_matching_lease_id(task_id, active, &active.lease_id)?;
        require_status(task.status, TaskStatus::Running)?;
        task.retry_count = task.retry_count.saturating_add(1);
        task.dead_lettered = true;
        task.dead_letter_reason = Some(reason.to_owned());
        let transition = validate_transition(task.status, TaskStatus::Failed)?;
        task.status = TaskStatus::Failed;
        state.leases.remove(task_id);
        append_in_memory_event(
            state,
            task_id,
            KeryxEventType::TaskDeadLettered,
            Some(transition.from),
            transition.to,
        );
        state.tasks.insert(task_id.clone(), task.clone());
        Ok(task)
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
        self.finish_task_in_state(
            &mut state,
            task_id,
            lease_id,
            worker_id,
            to,
            task.retry_count,
            task.dead_lettered,
            task.dead_letter_reason.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_task_in_state(
        &self,
        state: &mut InMemoryState,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        to: TaskStatus,
        retry_count: u32,
        dead_lettered: bool,
        dead_letter_reason: Option<String>,
    ) -> StoreResult<TaskRecord> {
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
        updated.retry_count = retry_count;
        updated.dead_lettered = dead_lettered;
        updated.dead_letter_reason = dead_letter_reason;
        state.leases.remove(task_id);
        append_in_memory_event(
            state,
            task_id,
            transition.event_type,
            Some(transition.from),
            transition.to,
        );
        state.tasks.insert(task_id.clone(), updated.clone());
        Ok(updated)
    }

    pub fn accept_legacy_event(
        &self,
        task_id: &TaskId,
        event_type: KeryxEventType,
    ) -> StoreResult<TaskRecord> {
        let legacy_event = LegacyEventType::from_keryx_event_type(event_type).ok_or(
            StoreError::Validation(ValidationError::InvalidTaskTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Pending,
            }),
        )?;
        let mut state = self.lock()?;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        let plan = plan_legacy_event_append(task.status, legacy_event)?;
        match plan {
            LegacyAppendPlan::Lifecycle(transition) => {
                let mut updated = task;
                updated.status = transition.to;
                if transition.to.is_terminal() {
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
            LegacyAppendPlan::Operational {
                event_type: preserved,
                status,
            } => {
                append_in_memory_event(&mut state, task_id, preserved, Some(status), status);
                Ok(task)
            }
        }
    }

    pub fn active_lease(&self, task_id: &TaskId) -> StoreResult<Option<LeaseRecord>> {
        Ok(self.lock()?.leases.get(task_id).cloned())
    }

    pub async fn put_artifact(
        &self,
        meta: &keryx_core::ArtifactMeta,
        bytes: &[u8],
        blob_dir: &Path,
    ) -> StoreResult<ArtifactRecord> {
        validate_artifact_size(bytes.len() as u64).map_err(map_artifact_validation_error)?;
        let computed = Digest::compute(bytes);
        if computed != meta.digest {
            return Err(StoreError::DigestMismatch {
                expected: meta.digest.as_str().to_owned(),
                actual: computed.as_str().to_owned(),
            });
        }

        let mut state = self.lock()?;
        if !state.tasks.contains_key(&meta.task_id) {
            return Err(StoreError::TaskNotFound(meta.task_id.clone()));
        }

        let record = artifact_record_from_meta(meta, computed, bytes.len() as u64);
        if let Some(existing) = state.artifacts.get(&record.artifact_id).cloned() {
            drop_in_memory_artifact_association(&mut state, &existing, blob_dir)?;
        }

        if record.inline {
            state
                .inline_artifacts
                .insert(record.artifact_id.clone(), bytes.to_vec());
        } else {
            ensure_blob_dir(blob_dir)?;
            std::fs::write(blob_path(blob_dir, &record.digest), bytes)?;
            let entry = state
                .blobs
                .entry(record.digest.clone())
                .or_insert_with(|| (bytes.to_vec(), 0));
            entry.0 = bytes.to_vec();
            entry.1 = entry.1.saturating_add(1);
        }

        state
            .artifacts
            .insert(record.artifact_id.clone(), record.clone());
        Ok(record)
    }

    pub async fn get_artifact(
        &self,
        artifact_id: &ArtifactId,
        _blob_dir: &Path,
    ) -> StoreResult<(ArtifactRecord, Vec<u8>)> {
        let state = self.lock()?;
        let record = state
            .artifacts
            .get(artifact_id)
            .cloned()
            .ok_or_else(|| StoreError::ArtifactNotFound(artifact_id.clone()))?;
        let bytes = if record.inline {
            state
                .inline_artifacts
                .get(artifact_id)
                .cloned()
                .ok_or_else(|| StoreError::ArtifactNotFound(artifact_id.clone()))?
        } else {
            state
                .blobs
                .get(&record.digest)
                .map(|(bytes, _)| bytes.clone())
                .ok_or_else(|| StoreError::ArtifactNotFound(artifact_id.clone()))?
        };
        Ok((record, bytes))
    }

    pub async fn list_artifacts_for_task(
        &self,
        task_id: &TaskId,
    ) -> StoreResult<Vec<ArtifactRecord>> {
        let state = self.lock()?;
        let mut records = state
            .artifacts
            .values()
            .filter(|record| &record.task_id == task_id)
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.artifact_id.as_str().cmp(right.artifact_id.as_str()));
        Ok(records)
    }

    pub async fn delete_artifact(
        &self,
        artifact_id: &ArtifactId,
        blob_dir: &Path,
    ) -> StoreResult<()> {
        let mut state = self.lock()?;
        let record = state
            .artifacts
            .remove(artifact_id)
            .ok_or_else(|| StoreError::ArtifactNotFound(artifact_id.clone()))?;
        if record.inline {
            state.inline_artifacts.remove(artifact_id);
        } else {
            decrement_in_memory_blob_ref(&mut state, &record.digest, blob_dir)?;
        }
        Ok(())
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

    fn count_tasks_by_status(&self, status: TaskStatus) -> StoreResult<u64> {
        let state = self.lock()?;
        Ok(state
            .tasks
            .values()
            .filter(|task| task.status == status)
            .count() as u64)
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
        let existing_version = detect_sqlite_schema_version(&self.pool).await?;
        if existing_version > CURRENT_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found_version: existing_version,
                supported_version: CURRENT_SCHEMA_VERSION,
            });
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        for statement in MIGRATION_001 {
            sqlx::query(statement)
                .execute(&mut *tx)
                .await
                .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        }
        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (1, 'initial')",
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        let worker_id_column_exists =
            sqlx::query("SELECT 1 FROM pragma_table_info('leases') WHERE name = 'worker_id'")
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| StoreError::MigrationFailed(error.to_string()))?
                .is_some();
        if !worker_id_column_exists {
            for statement in MIGRATION_002 {
                sqlx::query(statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
            }
        }
        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (2, 'lease_worker_identity')",
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        let retry_column_exists =
            sqlx::query("SELECT 1 FROM pragma_table_info('tasks') WHERE name = 'retry_count'")
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| StoreError::MigrationFailed(error.to_string()))?
                .is_some();
        if !retry_column_exists {
            for statement in MIGRATION_003 {
                sqlx::query(statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
            }
        }
        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (3, 'task_retry_dead_letter')",
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        let artifact_storage_exists =
            sqlx::query("SELECT 1 FROM schema_migrations WHERE version = 4 LIMIT 1")
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| StoreError::MigrationFailed(error.to_string()))?
                .is_some();
        if !artifact_storage_exists {
            for statement in MIGRATION_004 {
                sqlx::query(statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
            }
        }
        let deadline_column_exists =
            sqlx::query("SELECT 1 FROM pragma_table_info('tasks') WHERE name = 'deadline_ms'")
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| StoreError::MigrationFailed(error.to_string()))?
                .is_some();
        if !deadline_column_exists {
            for statement in MIGRATION_005 {
                sqlx::query(statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
            }
        }
        sqlx::query(
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
        let phase17_results_exist = sqlx::query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'task_terminal_results'",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?
        .is_some();
        if !phase17_results_exist {
            for statement in results::MIGRATION_007 {
                sqlx::query(statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
            }
        }
        sqlx::query(
            "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (7, 'phase17_terminal_results')",
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        let legacy_unowned_rows = sqlx::query(
            "SELECT lease_id, task_id, worker_id, leased_at_ms, expires_at_ms FROM leases WHERE active = 1 AND worker_id IS NULL ORDER BY expires_at_ms ASC, task_id ASC",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        recover_sqlite_leases_with_executor(&mut tx, legacy_unowned_rows)
            .await
            .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;
        tx.commit()
            .await
            .map_err(|error| StoreError::MigrationFailed(error.to_string()))?;

        let final_version = detect_sqlite_schema_version(&self.pool).await?;
        if final_version != CURRENT_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found_version: final_version,
                supported_version: CURRENT_SCHEMA_VERSION,
            });
        }
        Ok(())
    }

    pub async fn schema_version(&self) -> StoreResult<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>("version"))
    }

    /// Close the underlying connection pool. Safe to call on any clone of the store.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    pub async fn put_artifact(
        &self,
        meta: &keryx_core::ArtifactMeta,
        bytes: &[u8],
        blob_dir: &Path,
    ) -> StoreResult<ArtifactRecord> {
        validate_artifact_size(bytes.len() as u64).map_err(map_artifact_validation_error)?;
        let computed = Digest::compute(bytes);
        if computed != meta.digest {
            return Err(StoreError::DigestMismatch {
                expected: meta.digest.as_str().to_owned(),
                actual: computed.as_str().to_owned(),
            });
        }

        let record = artifact_record_from_meta(meta, computed, bytes.len() as u64);
        let mut tx = self.pool.begin().await?;
        fetch_task_with_executor(&mut tx, &record.task_id).await?;

        let existing = fetch_artifact_optional_with_executor(&mut tx, &record.artifact_id).await?;
        let prepared_blob = if should_prepare_blob_write(existing.as_ref(), &record) {
            Some(prepare_blob_write_with_executor(&mut tx, &record.digest, bytes, blob_dir).await?)
        } else {
            None
        };

        let tx_result = async {
            let mut cleanup_digests = Vec::new();
            if let Some(existing) = existing.as_ref() {
                if should_drop_blob_association(existing, &record)
                    && decrement_blob_ref_with_executor(&mut tx, &existing.digest).await?
                {
                    cleanup_digests.push(existing.digest.clone());
                }
            }
            if should_write_blob_association(existing.as_ref(), &record) {
                increment_blob_ref_with_executor(&mut tx, &record.digest, bytes.len() as u64).await?;
            }

            sqlx::query(
                "INSERT INTO artifacts (artifact_id, task_id, digest, media_type, byte_len, inline, inline_blob, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(artifact_id) DO UPDATE SET task_id = excluded.task_id, digest = excluded.digest, media_type = excluded.media_type, byte_len = excluded.byte_len, inline = excluded.inline, inline_blob = excluded.inline_blob, created_at = excluded.created_at",
            )
            .bind(record.artifact_id.as_str())
            .bind(record.task_id.as_str())
            .bind(record.digest.as_str())
            .bind(record.media_type.as_str())
            .bind(record.byte_len as i64)
            .bind(i64::from(record.inline))
            .bind(record.inline.then_some(bytes))
            .bind(&record.created_at)
            .execute(&mut *tx)
            .await?;

            StoreResult::Ok(cleanup_digests)
        }
        .await;

        let cleanup_digests = match tx_result {
            Ok(cleanup_digests) => cleanup_digests,
            Err(error) => {
                tx.rollback().await.ok();
                rollback_prepared_blob_write(prepared_blob.as_ref())?;
                return Err(error);
            }
        };

        if let Err(error) = tx.commit().await {
            rollback_prepared_blob_write(prepared_blob.as_ref())?;
            return Err(error.into());
        }
        finalize_blob_cleanup(&self.pool, &cleanup_digests, blob_dir).await?;
        Ok(record)
    }

    pub async fn get_artifact(
        &self,
        artifact_id: &ArtifactId,
        blob_dir: &Path,
    ) -> StoreResult<(ArtifactRecord, Vec<u8>)> {
        let record = fetch_artifact_optional_from_pool(&self.pool, artifact_id)
            .await?
            .ok_or_else(|| StoreError::ArtifactNotFound(artifact_id.clone()))?;

        let bytes = if record.inline {
            let row = sqlx::query("SELECT inline_blob FROM artifacts WHERE artifact_id = ?")
                .bind(artifact_id.as_str())
                .fetch_one(&self.pool)
                .await?;
            row.try_get::<Option<Vec<u8>>, _>("inline_blob")?
                .ok_or_else(|| StoreError::ArtifactNotFound(artifact_id.clone()))?
        } else {
            read_verified_blob(blob_dir, &record.digest, record.byte_len)?
        };

        Ok((record, bytes))
    }

    pub async fn list_artifacts_for_task(
        &self,
        task_id: &TaskId,
    ) -> StoreResult<Vec<ArtifactRecord>> {
        let rows = sqlx::query(
            "SELECT artifact_id, task_id, digest, media_type, byte_len, inline, created_at FROM artifacts WHERE task_id = ? ORDER BY artifact_id ASC",
        )
        .bind(task_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_artifact).collect()
    }

    pub async fn delete_artifact(
        &self,
        artifact_id: &ArtifactId,
        blob_dir: &Path,
    ) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;
        let record = fetch_artifact_optional_with_executor(&mut tx, artifact_id)
            .await?
            .ok_or_else(|| StoreError::ArtifactNotFound(artifact_id.clone()))?;
        sqlx::query("DELETE FROM artifacts WHERE artifact_id = ?")
            .bind(artifact_id.as_str())
            .execute(&mut *tx)
            .await?;
        let mut cleanup_digests = Vec::new();
        if !record.inline && decrement_blob_ref_with_executor(&mut tx, &record.digest).await? {
            cleanup_digests.push(record.digest.clone());
        }
        tx.commit().await?;
        finalize_blob_cleanup(&self.pool, &cleanup_digests, blob_dir).await?;
        Ok(())
    }

    #[instrument(skip(self), fields(task_id = %task.task_id().as_str()))]
    pub async fn accept_task_with_envelope(
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
                        fetch_task_envelope_optional_with_executor(&mut tx, &existing_task_id)
                            .await?;
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

    pub async fn get_task_envelope(&self, task_id: &TaskId) -> StoreResult<TaskEnvelopeRecord> {
        let row = sqlx::query(
            "SELECT task_id, encoded_envelope, received_at_ms FROM task_envelopes WHERE task_id = ?",
        )
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_task_envelope)
            .transpose()?
            .ok_or_else(|| StoreError::TaskEnvelopeNotFound(task_id.clone()))
    }

    pub async fn pending_task_envelopes(
        &self,
        limit: usize,
    ) -> StoreResult<Vec<PendingTaskEnvelope>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT t.task_id, t.status, t.idempotency_key, t.retry_count, t.dead_lettered, t.dead_letter_reason, t.deadline_ms, e.encoded_envelope, e.received_at_ms                      FROM tasks t INNER JOIN task_envelopes e ON e.task_id = t.task_id                      WHERE t.status = 'pending'                      ORDER BY e.received_at_ms ASC, t.task_id ASC LIMIT ?",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_pending_task_envelope).collect()
    }

    pub async fn claimable_pending_task_envelopes(
        &self,
        local_peer_id: &PeerId,
        limit: usize,
    ) -> StoreResult<Vec<PendingTaskEnvelope>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT t.task_id, t.status, t.idempotency_key, t.retry_count, t.dead_lettered, t.dead_letter_reason, t.deadline_ms, e.encoded_envelope, e.received_at_ms \
             FROM tasks t \
             INNER JOIN task_envelopes e ON e.task_id = t.task_id \
             LEFT JOIN task_transport_context c ON c.task_id = t.task_id \
             WHERE t.status = 'pending' \
               AND (c.expected_executor_peer_id IS NULL OR c.expected_executor_peer_id = ?) \
             ORDER BY e.received_at_ms ASC, t.task_id ASC LIMIT ?",
        )
        .bind(local_peer_id.as_str())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_pending_task_envelope).collect()
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
        tx.commit().await?;
        Ok(task)
    }

    pub async fn get_task(&self, task_id: &TaskId) -> StoreResult<TaskRecord> {
        let row =
            sqlx::query(
                "SELECT task_id, status, idempotency_key, retry_count, dead_lettered, dead_letter_reason, deadline_ms FROM tasks WHERE task_id = ?",
            )
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

    pub async fn count_tasks_by_status(&self, status: TaskStatus) -> StoreResult<u64> {
        let row = sqlx::query("SELECT COUNT(*) AS count FROM tasks WHERE status = ?")
            .bind(status_to_str(status))
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>("count") as u64)
    }

    pub async fn retained_task_envelope_bytes(&self) -> StoreResult<u64> {
        let row = sqlx::query(
            "SELECT COALESCE(SUM(LENGTH(encoded_envelope)), 0) AS retained_bytes FROM task_envelopes",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>("retained_bytes") as u64)
    }

    pub async fn accept_legacy_event(
        &self,
        task_id: &TaskId,
        event_type: KeryxEventType,
    ) -> StoreResult<TaskRecord> {
        let legacy_event = LegacyEventType::from_keryx_event_type(event_type).ok_or(
            StoreError::Validation(ValidationError::InvalidTaskTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Pending,
            }),
        )?;
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let plan = plan_legacy_event_append(task.status, legacy_event)?;
        let sequence = next_sequence_with_executor(&mut tx, task_id).await?;
        match plan {
            LegacyAppendPlan::Lifecycle(transition) => {
                if transition.to.is_terminal() {
                    deactivate_lease_for_task_with_executor(&mut tx, task_id).await?;
                }
                update_task_status_with_executor(&mut tx, task_id, transition.to).await?;
                insert_event(
                    &mut tx,
                    task_id,
                    sequence,
                    transition.event_type,
                    Some(transition.from),
                    transition.to,
                )
                .await?;
            }
            LegacyAppendPlan::Operational {
                event_type: preserved,
                status,
            } => {
                insert_event(&mut tx, task_id, sequence, preserved, Some(status), status).await?;
            }
        }
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

    #[instrument(skip(self, lease), fields(task_id = %task_id.as_str(), worker_id = tracing::field::Empty))]
    pub async fn lease_task(
        &self,
        task_id: &TaskId,
        lease: LeaseRecord,
    ) -> StoreResult<TaskRecord> {
        self.lease_task_with_peer_guard(task_id, lease, None).await
    }

    #[instrument(skip(self, lease), fields(task_id = %task_id.as_str(), worker_id = tracing::field::Empty))]
    pub async fn lease_task_for_peer(
        &self,
        task_id: &TaskId,
        lease: LeaseRecord,
        local_peer_id: &PeerId,
    ) -> StoreResult<TaskRecord> {
        self.lease_task_with_peer_guard(task_id, lease, Some(local_peer_id))
            .await
    }

    async fn lease_task_with_peer_guard(
        &self,
        task_id: &TaskId,
        lease: LeaseRecord,
        local_peer_id: Option<&PeerId>,
    ) -> StoreResult<TaskRecord> {
        if let Some(worker_id) = lease.worker_id.as_ref() {
            tracing::Span::current()
                .record("worker_id", tracing::field::display(worker_id.as_str()));
        }
        let mut tx = self.pool.begin().await?;
        ensure_matching_task_id(task_id, &lease)?;
        ensure_lease_has_owner(&lease)?;
        if let Some(local_peer_id) = local_peer_id {
            if let Some(row) = sqlx::query(
                "SELECT expected_executor_peer_id FROM task_transport_context WHERE task_id = ?",
            )
            .bind(task_id.as_str())
            .fetch_optional(&mut *tx)
            .await?
            {
                if let Some(expected) = row.get::<Option<String>, _>("expected_executor_peer_id") {
                    if expected != local_peer_id.as_str() {
                        return Err(StoreError::TaskExecutorMismatch {
                            task_id: task_id.clone(),
                            expected: PeerId::new(expected)?,
                            actual: local_peer_id.clone(),
                        });
                    }
                }
            }
        }
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        if fetch_active_lease_with_executor(&mut tx, task_id)
            .await?
            .is_some()
        {
            return Err(StoreError::LeaseConflict {
                task_id: task_id.clone(),
            });
        }
        if let Some(deadline_ms) = task
            .deadline_ms
            .filter(|deadline_ms| *deadline_ms <= lease.leased_at_ms)
        {
            validate_deadline_transition(task.status)?;
            let sequence = next_sequence_with_executor(&mut tx, task_id).await?;
            update_task_status_with_executor(&mut tx, task_id, TaskStatus::Failed).await?;
            insert_event(
                &mut tx,
                task_id,
                sequence,
                KeryxEventType::TaskTimedOut,
                Some(task.status),
                TaskStatus::Failed,
            )
            .await?;
            tx.commit().await?;
            return Err(StoreError::TaskDeadlineExpired {
                task_id: task_id.clone(),
                deadline_ms,
                attempted_lease_at_ms: lease.leased_at_ms,
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

    #[instrument(skip(self), fields(task_id = %task_id.as_str(), lease_id = %lease_id.as_str()))]
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

    #[instrument(skip(self), fields(task_id = %task_id.as_str(), lease_id = %lease_id.as_str()))]
    pub async fn complete_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
    ) -> StoreResult<TaskRecord> {
        self.finish_task(task_id, lease_id, worker_id, TaskStatus::Completed)
            .await
    }

    #[instrument(skip(self, error_reason, policy), fields(task_id = %task_id.as_str(), lease_id = %lease_id.as_str()))]
    pub async fn fail_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        error_reason: &str,
        policy: &RetryPolicy,
    ) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;
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
            sqlite_retry_task_in_tx(&mut tx, task_id, &task).await?
        } else {
            sqlite_dead_letter_task_in_tx(&mut tx, task_id, &task, error_reason).await?
        };
        tx.commit().await?;
        Ok(updated)
    }

    pub async fn retry_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
    ) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let active = fetch_active_lease_with_executor(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, &active, lease_id)?;
        ensure_matching_worker_id(task_id, &active, worker_id)?;
        let updated = sqlite_retry_task_in_tx(&mut tx, task_id, &task).await?;
        tx.commit().await?;
        Ok(updated)
    }

    pub async fn dead_letter_task(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        reason: &str,
    ) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let active = fetch_active_lease_with_executor(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, &active, lease_id)?;
        ensure_matching_worker_id(task_id, &active, worker_id)?;
        let updated = sqlite_dead_letter_task_in_tx(&mut tx, task_id, &task, reason).await?;
        tx.commit().await?;
        Ok(updated)
    }

    pub async fn cancel_task(
        &self,
        task_id: &TaskId,
        _reason: &str,
        now_ms: i64,
    ) -> StoreResult<TaskRecord> {
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        if task.status.is_terminal() {
            tx.commit().await?;
            return Ok(task);
        }
        if task.status == TaskStatus::Running {
            let active = fetch_active_lease_with_executor(&mut tx, task_id)
                .await?
                .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
            ensure_active_lease_unexpired(&active, now_ms)?;
        }
        let transition = validate_cancel_transition(task.status)?;
        let sequence = next_sequence_with_executor(&mut tx, task_id).await?;
        deactivate_lease_for_task_with_executor(&mut tx, task_id).await?;
        update_task_status_with_executor(&mut tx, task_id, transition.to).await?;
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

    pub async fn fail_expired_deadlines(
        &self,
        now_ms: i64,
        limit: Option<usize>,
    ) -> StoreResult<Vec<TaskRecord>> {
        let mut tx = self.pool.begin().await?;
        let rows = match limit {
            Some(limit) => {
                sqlx::query(
                    "SELECT task_id, status, idempotency_key, retry_count, dead_lettered, dead_letter_reason, deadline_ms FROM tasks WHERE deadline_ms IS NOT NULL AND deadline_ms <= ? AND status IN ('pending', 'running') ORDER BY deadline_ms ASC, task_id ASC LIMIT ?",
                )
                .bind(now_ms)
                .bind(limit as i64)
                .fetch_all(&mut *tx)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT task_id, status, idempotency_key, retry_count, dead_lettered, dead_letter_reason, deadline_ms FROM tasks WHERE deadline_ms IS NOT NULL AND deadline_ms <= ? AND status IN ('pending', 'running') ORDER BY deadline_ms ASC, task_id ASC",
                )
                .bind(now_ms)
                .fetch_all(&mut *tx)
                .await?
            }
        };

        let mut failed = Vec::with_capacity(rows.len());
        for row in rows {
            let task = row_to_task(row)?;
            validate_deadline_transition(task.status)?;
            let sequence = next_sequence_with_executor(&mut tx, task.task_id()).await?;
            deactivate_lease_for_task_with_executor(&mut tx, task.task_id()).await?;
            update_task_status_with_executor(&mut tx, task.task_id(), TaskStatus::Failed).await?;
            insert_event(
                &mut tx,
                task.task_id(),
                sequence,
                KeryxEventType::TaskTimedOut,
                Some(task.status),
                TaskStatus::Failed,
            )
            .await?;
            let mut updated = task;
            updated.status = TaskStatus::Failed;
            failed.push(updated);
        }
        tx.commit().await?;
        Ok(failed)
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
        let updated = self
            .finish_task_in_tx(
                &mut tx,
                task_id,
                lease_id,
                worker_id,
                to,
                task.retry_count,
                task.dead_lettered,
                task.dead_letter_reason.clone(),
            )
            .await?;
        tx.commit().await?;
        Ok(updated)
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_task_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        to: TaskStatus,
        retry_count: u32,
        dead_lettered: bool,
        dead_letter_reason: Option<String>,
    ) -> StoreResult<TaskRecord> {
        let task = fetch_task_with_executor(tx, task_id).await?;
        let active = fetch_active_lease_with_executor(tx, task_id)
            .await?
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, &active, lease_id)?;
        ensure_matching_worker_id(task_id, &active, worker_id)?;
        let transition = validate_transition(task.status, to)?;
        let sequence = next_sequence_with_executor(tx, task_id).await?;
        deactivate_lease_for_task_with_executor(tx, task_id).await?;
        update_task_metadata_with_executor(
            tx,
            task_id,
            to,
            retry_count,
            dead_lettered,
            dead_letter_reason.as_deref(),
        )
        .await?;
        insert_event(
            tx,
            task_id,
            sequence,
            transition.event_type,
            Some(transition.from),
            transition.to,
        )
        .await?;
        fetch_task_with_executor(tx, task_id).await
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

    #[instrument(skip(self))]
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
        info!(
            tasks_recovered = report.recovered_task_count(),
            leases_cleaned = report.cleaned_terminal_leases,
            corruption_count = report.corruption_count(),
            "recover_stale_leases completed"
        );
        Ok(report)
    }
}

async fn detect_sqlite_schema_version(pool: &SqlitePool) -> StoreResult<i64> {
    let schema_table_exists = sqlx::query(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
    )
    .fetch_optional(pool)
    .await?;
    if schema_table_exists.is_none() {
        return Ok(0);
    }

    let row = sqlx::query("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations")
        .fetch_one(pool)
        .await?;
    Ok(row.get::<i64, _>("version"))
}

fn ensure_matching_envelope_task_id(
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
    if active.expires_at_ms > now_ms
        && new_expires_at_ms > now_ms
        && new_expires_at_ms > active.expires_at_ms
    {
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
    let row = sqlx::query(
        "SELECT task_id, status, idempotency_key, retry_count, dead_lettered, dead_letter_reason, deadline_ms FROM tasks WHERE task_id = ?",
    )
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
            let mut recovered_task = task;
            recovered_task.status = to_status;
            recovered.push(recovered_task);
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
    // `retry_count`, `dead_lettered`, and `dead_letter_reason` remain authoritative
    // snapshot metadata for now. Replay only validates lifecycle sequencing plus the
    // parts of retry/dead-letter state the event log can express unambiguously.
    //
    // Today retry-driven requeues and stale-lease recovery both persist as
    // `RecoveryAction`, so the event stream cannot faithfully reconstruct the full
    // retry counter without double-counting or conflating retry with recovery.
    let mut replay_dead_lettered = false;
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
            KeryxEventType::TaskCanceled => {
                let transition = validate_cancel_transition(current_status)
                    .map_err(|_| StoreError::CorruptEventStream(task_id.clone()))?;
                if transition.to != event.to_status {
                    return Err(StoreError::CorruptEventStream(task_id));
                }
                current_status = transition.to;
            }
            KeryxEventType::TaskTimedOut => {
                validate_deadline_transition(current_status)
                    .map_err(|_| StoreError::CorruptEventStream(task_id.clone()))?;
                if event.to_status != TaskStatus::Failed {
                    return Err(StoreError::CorruptEventStream(task_id));
                }
                current_status = event.to_status;
            }
            KeryxEventType::TaskLeased
            | KeryxEventType::TaskDeadLettered
            | KeryxEventType::TaskApprovalDenied => {
                let legacy = LegacyEventType::from_keryx_event_type(event.event_type)
                    .ok_or(StoreError::CorruptEventStream(task_id.clone()))?;
                let transition = normalize_legacy_transition(current_status, legacy)
                    .ok_or(StoreError::CorruptEventStream(task_id.clone()))?;
                if transition.to != event.to_status {
                    return Err(StoreError::CorruptEventStream(task_id));
                }
                if event.event_type == KeryxEventType::TaskDeadLettered {
                    replay_dead_lettered = true;
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
            event_type if is_replayable_operational_legacy_event(event_type) => {
                if event.to_status != current_status {
                    return Err(StoreError::CorruptEventStream(task_id));
                }
            }
            _ => return Err(StoreError::CorruptEventStream(task_id)),
        }
    }

    if snapshot.status != current_status {
        return Err(StoreError::CorruptEventStream(task_id));
    }
    if snapshot.dead_lettered != replay_dead_lettered {
        return Err(StoreError::CorruptEventStream(task_id));
    }
    if snapshot.dead_lettered && snapshot.retry_count == 0 {
        return Err(StoreError::CorruptEventStream(task_id));
    }

    let mut rebuilt = snapshot.clone();
    rebuilt.status = current_status;
    Ok(rebuilt)
}

async fn sqlite_retry_task_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
    task: &TaskRecord,
) -> StoreResult<TaskRecord> {
    require_status(task.status, TaskStatus::Running)?;
    let retry_count = task.retry_count.saturating_add(1);
    let sequence = next_sequence_with_executor(tx, task_id).await?;
    deactivate_lease_for_task_with_executor(tx, task_id).await?;
    update_task_metadata_with_executor(tx, task_id, TaskStatus::Pending, retry_count, false, None)
        .await?;
    insert_event(
        tx,
        task_id,
        sequence,
        KeryxEventType::RecoveryAction,
        Some(TaskStatus::Running),
        TaskStatus::Pending,
    )
    .await?;
    fetch_task_with_executor(tx, task_id).await
}

async fn sqlite_dead_letter_task_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
    task: &TaskRecord,
    reason: &str,
) -> StoreResult<TaskRecord> {
    require_status(task.status, TaskStatus::Running)?;
    let retry_count = task.retry_count.saturating_add(1);
    let transition = validate_transition(task.status, TaskStatus::Failed)?;
    let sequence = next_sequence_with_executor(tx, task_id).await?;
    deactivate_lease_for_task_with_executor(tx, task_id).await?;
    update_task_metadata_with_executor(
        tx,
        task_id,
        TaskStatus::Failed,
        retry_count,
        true,
        Some(reason),
    )
    .await?;
    insert_event(
        tx,
        task_id,
        sequence,
        KeryxEventType::TaskDeadLettered,
        Some(transition.from),
        transition.to,
    )
    .await?;
    fetch_task_with_executor(tx, task_id).await
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

async fn update_task_metadata_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
    status: TaskStatus,
    retry_count: u32,
    dead_lettered: bool,
    dead_letter_reason: Option<&str>,
) -> StoreResult<()> {
    sqlx::query(
        "UPDATE tasks SET status = ?, retry_count = ?, dead_lettered = ?, dead_letter_reason = ?, updated_at = CURRENT_TIMESTAMP WHERE task_id = ?",
    )
    .bind(status_to_str(status))
    .bind(i64::from(retry_count))
    .bind(i64::from(dead_lettered))
    .bind(dead_letter_reason)
    .bind(task_id.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(())
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
    let retry_count = row
        .try_get::<Option<i64>, _>("retry_count")?
        .unwrap_or(0)
        .max(0) as u32;
    let dead_lettered = row.try_get::<Option<i64>, _>("dead_lettered")?.unwrap_or(0) != 0;
    let dead_letter_reason = row.try_get::<Option<String>, _>("dead_letter_reason")?;
    let deadline_ms = row.try_get::<Option<i64>, _>("deadline_ms")?;
    Ok(TaskRecord {
        id: task_id,
        status,
        idempotency_key,
        deadline_ms,
        retry_count,
        dead_lettered,
        dead_letter_reason,
    })
}

fn row_to_task_envelope(row: sqlx::sqlite::SqliteRow) -> StoreResult<TaskEnvelopeRecord> {
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
}

fn row_to_pending_task_envelope(row: sqlx::sqlite::SqliteRow) -> StoreResult<PendingTaskEnvelope> {
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
        dead_lettered: row.try_get::<Option<i64>, _>("dead_lettered")?.unwrap_or(0) != 0,
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

fn row_to_artifact(row: sqlx::sqlite::SqliteRow) -> StoreResult<ArtifactRecord> {
    Ok(ArtifactRecord {
        artifact_id: ArtifactId::new(row.get::<String, _>("artifact_id"))?,
        task_id: TaskId::new(row.get::<String, _>("task_id"))?,
        digest: Digest::new(row.get::<String, _>("digest"))?,
        media_type: MediaType::new(row.get::<String, _>("media_type")),
        byte_len: row.get::<i64, _>("byte_len").max(0) as u64,
        inline: row.get::<i64, _>("inline") != 0,
        created_at: row.get::<String, _>("created_at"),
    })
}

fn artifact_record_from_meta(
    meta: &keryx_core::ArtifactMeta,
    digest: Digest,
    byte_len: u64,
) -> ArtifactRecord {
    ArtifactRecord {
        artifact_id: meta.artifact_id.clone(),
        task_id: meta.task_id.clone(),
        digest,
        media_type: meta.media_type.clone(),
        byte_len,
        inline: should_inline(byte_len),
        created_at: meta.created_at.clone(),
    }
}

fn map_artifact_validation_error(error: ValidationError) -> StoreError {
    match error {
        ValidationError::ArtifactTooLarge {
            byte_len,
            limit_bytes,
        } => StoreError::ArtifactTooLarge {
            byte_len,
            limit_bytes,
        },
        other => StoreError::Validation(other),
    }
}

fn blob_path(blob_dir: &Path, digest: &Digest) -> PathBuf {
    blob_dir.join(digest.as_str())
}

fn ensure_blob_dir(blob_dir: &Path) -> StoreResult<()> {
    std::fs::create_dir_all(blob_dir)
        .map_err(|error| StoreError::BlobDir(format!("{}: {error}", blob_dir.display())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(blob_dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| StoreError::BlobDir(format!("{}: {error}", blob_dir.display())))?;
    }
    Ok(())
}

fn read_verified_blob(blob_dir: &Path, digest: &Digest, byte_len: u64) -> StoreResult<Vec<u8>> {
    let path = blob_path(blob_dir, digest);
    let bytes = read_blob_file(&path)?;
    if bytes.len() as u64 != byte_len {
        return Err(StoreError::Database(format!(
            "blob {} has byte length {}, expected {byte_len}",
            digest.as_str(),
            bytes.len()
        )));
    }
    let actual = Digest::compute(&bytes);
    if &actual != digest {
        return Err(StoreError::DigestMismatch {
            expected: digest.as_str().to_owned(),
            actual: actual.as_str().to_owned(),
        });
    }
    Ok(bytes)
}

fn read_blob_file(path: &Path) -> StoreResult<Vec<u8>> {
    let mut file = open_blob_file_for_read(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(StoreError::Database(format!(
            "blob path is not a regular file: {}",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn open_blob_file_for_read(path: &Path) -> StoreResult<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(StoreError::from)
}

fn write_new_blob_file(path: &Path, bytes: &[u8]) -> StoreResult<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(StoreError::from)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn is_not_found_message(message: &str) -> bool {
    message.contains("No such file or directory") || message.contains("os error 2")
}

fn remove_blob_file_if_present(path: &Path) -> StoreResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StoreError::Database(error.to_string())),
    }
}

fn drop_in_memory_artifact_association(
    state: &mut InMemoryState,
    existing: &ArtifactRecord,
    blob_dir: &Path,
) -> StoreResult<()> {
    if existing.inline {
        state.inline_artifacts.remove(&existing.artifact_id);
    } else {
        decrement_in_memory_blob_ref(state, &existing.digest, blob_dir)?;
    }
    Ok(())
}

fn decrement_in_memory_blob_ref(
    state: &mut InMemoryState,
    digest: &Digest,
    blob_dir: &Path,
) -> StoreResult<()> {
    let mut remove_file = false;
    match state.blobs.get_mut(digest) {
        Some((_, ref_count)) if *ref_count > 1 => *ref_count -= 1,
        Some(_) => {
            state.blobs.remove(digest);
            remove_file = true;
        }
        None => {}
    }
    if remove_file {
        remove_blob_file_if_present(&blob_path(blob_dir, digest))?;
    }
    Ok(())
}

async fn fetch_artifact_optional_from_pool(
    pool: &SqlitePool,
    artifact_id: &ArtifactId,
) -> StoreResult<Option<ArtifactRecord>> {
    let row = sqlx::query(
        "SELECT artifact_id, task_id, digest, media_type, byte_len, inline, created_at FROM artifacts WHERE artifact_id = ?",
    )
    .bind(artifact_id.as_str())
    .fetch_optional(pool)
    .await?;
    row.map(row_to_artifact).transpose()
}

async fn fetch_artifact_optional_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    artifact_id: &ArtifactId,
) -> StoreResult<Option<ArtifactRecord>> {
    let row = sqlx::query(
        "SELECT artifact_id, task_id, digest, media_type, byte_len, inline, created_at FROM artifacts WHERE artifact_id = ?",
    )
    .bind(artifact_id.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    row.map(row_to_artifact).transpose()
}

#[derive(Debug)]
struct PreparedBlobWrite {
    path: PathBuf,
    remove_on_rollback: bool,
}

fn should_drop_blob_association(existing: &ArtifactRecord, replacement: &ArtifactRecord) -> bool {
    !existing.inline && (replacement.inline || existing.digest != replacement.digest)
}

fn should_write_blob_association(
    existing: Option<&ArtifactRecord>,
    replacement: &ArtifactRecord,
) -> bool {
    !replacement.inline
        && existing
            .map(|existing| existing.inline || existing.digest != replacement.digest)
            .unwrap_or(true)
}

fn should_prepare_blob_write(
    existing: Option<&ArtifactRecord>,
    replacement: &ArtifactRecord,
) -> bool {
    should_write_blob_association(existing, replacement)
}

async fn prepare_blob_write_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    digest: &Digest,
    bytes: &[u8],
    blob_dir: &Path,
) -> StoreResult<PreparedBlobWrite> {
    let blob_previously_tracked = sqlx::query("SELECT 1 FROM blobs WHERE digest = ? LIMIT 1")
        .bind(digest.as_str())
        .fetch_optional(&mut **tx)
        .await?
        .is_some();
    let path = blob_path(blob_dir, digest);
    ensure_blob_dir(blob_dir)?;
    if blob_previously_tracked {
        match read_verified_blob(blob_dir, digest, bytes.len() as u64) {
            Ok(existing) if existing == bytes => {}
            Ok(_) => {
                return Err(StoreError::DigestMismatch {
                    expected: digest.as_str().to_owned(),
                    actual: Digest::compute(&read_blob_file(&path)?).as_str().to_owned(),
                });
            }
            Err(StoreError::Database(message)) if is_not_found_message(&message) => {
                write_new_blob_file(&path, bytes)?;
            }
            Err(error) => return Err(error),
        }
    } else {
        write_new_blob_file(&path, bytes)?;
    }
    Ok(PreparedBlobWrite {
        path,
        remove_on_rollback: !blob_previously_tracked,
    })
}

fn rollback_prepared_blob_write(prepared_blob: Option<&PreparedBlobWrite>) -> StoreResult<()> {
    if let Some(prepared_blob) =
        prepared_blob.filter(|prepared_blob| prepared_blob.remove_on_rollback)
    {
        remove_blob_file_if_present(&prepared_blob.path)?;
    }
    Ok(())
}

async fn increment_blob_ref_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    digest: &Digest,
    byte_len: u64,
) -> StoreResult<()> {
    sqlx::query(
        "INSERT INTO blobs (digest, ref_count, byte_len) VALUES (?, 1, ?) ON CONFLICT(digest) DO UPDATE SET ref_count = ref_count + 1, byte_len = excluded.byte_len",
    )
    .bind(digest.as_str())
    .bind(byte_len as i64)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn decrement_blob_ref_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    digest: &Digest,
) -> StoreResult<bool> {
    let row = sqlx::query("SELECT ref_count FROM blobs WHERE digest = ?")
        .bind(digest.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let ref_count = row.get::<i64, _>("ref_count");
    if ref_count > 1 {
        sqlx::query("UPDATE blobs SET ref_count = ref_count - 1 WHERE digest = ?")
            .bind(digest.as_str())
            .execute(&mut **tx)
            .await?;
        return Ok(false);
    }

    sqlx::query("UPDATE blobs SET ref_count = 0 WHERE digest = ?")
        .bind(digest.as_str())
        .execute(&mut **tx)
        .await?;
    Ok(true)
}

async fn finalize_blob_cleanup(
    pool: &SqlitePool,
    cleanup_digests: &[Digest],
    blob_dir: &Path,
) -> StoreResult<()> {
    for digest in cleanup_digests {
        remove_blob_file_if_present(&blob_path(blob_dir, digest))?;
        sqlx::query("DELETE FROM blobs WHERE digest = ? AND ref_count = 0")
            .bind(digest.as_str())
            .execute(pool)
            .await?;
    }
    Ok(())
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

const MIGRATION_003: &[&str] = &[
    "ALTER TABLE tasks ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE tasks ADD COLUMN dead_lettered INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE tasks ADD COLUMN dead_letter_reason TEXT",
];

const MIGRATION_004: &[&str] = &[
    "DROP TABLE IF EXISTS artifacts",
    "DROP TABLE IF EXISTS blobs",
    "CREATE TABLE artifacts (artifact_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, digest TEXT NOT NULL, media_type TEXT NOT NULL DEFAULT 'application/octet-stream', byte_len INTEGER NOT NULL, inline INTEGER NOT NULL DEFAULT 0, inline_blob BLOB, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (task_id) REFERENCES tasks(task_id))",
    "CREATE INDEX IF NOT EXISTS idx_artifacts_task ON artifacts(task_id)",
    "CREATE INDEX IF NOT EXISTS idx_artifacts_digest ON artifacts(digest)",
    "CREATE TABLE blobs (digest TEXT PRIMARY KEY, ref_count INTEGER NOT NULL DEFAULT 0, byte_len INTEGER NOT NULL)",
    "INSERT OR IGNORE INTO schema_migrations (version, name) VALUES (4, 'artifact_storage')",
];

const MIGRATION_005: &[&str] = &[
    "ALTER TABLE tasks ADD COLUMN deadline_ms INTEGER",
    "CREATE INDEX IF NOT EXISTS idx_tasks_deadline ON tasks(deadline_ms)",
];

const MIGRATION_006: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS task_envelopes (task_id TEXT PRIMARY KEY, encoded_envelope BLOB NOT NULL, received_at_ms INTEGER NOT NULL, FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE)",
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
        "created" | "accepted" | "queued" | "pending" => Ok(TaskStatus::Pending),
        "awaiting_approval" => Err(StoreError::Database(
            "legacy awaiting_approval task status requires explicit approval migration".to_string(),
        )),
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

    use keryx_core::{
        AgentId, IdempotencyKey, KeryxEventType, LeaseId, TaskId, TaskStatus, ValidationError,
    };

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
            artifacts: HashMap::new(),
            inline_artifacts: HashMap::new(),
            blobs: HashMap::new(),
            envelopes: HashMap::new(),
            transport_contexts: HashMap::new(),
            terminal_results: HashMap::new(),
            result_outbox: HashMap::new(),
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
                artifacts: HashMap::new(),
                inline_artifacts: HashMap::new(),
                blobs: HashMap::new(),
                envelopes: HashMap::new(),
                transport_contexts: HashMap::new(),
                terminal_results: HashMap::new(),
                result_outbox: HashMap::new(),
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
            artifacts: HashMap::new(),
            inline_artifacts: HashMap::new(),
            blobs: HashMap::new(),
            envelopes: HashMap::new(),
            transport_contexts: HashMap::new(),
            terminal_results: HashMap::new(),
            result_outbox: HashMap::new(),
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

    #[test]
    fn in_memory_replay_rejects_dead_lettered_snapshot_without_retry_count() {
        let mut task = task(
            "dead-letter-without-retry-task",
            TaskStatus::Failed,
            Some("dead-letter-without-retry-idem"),
        );
        task.dead_lettered = true;
        task.dead_letter_reason = Some("still broken".to_owned());
        let mut state = InMemoryState {
            tasks: HashMap::from([(task.task_id().clone(), task.clone())]),
            events: HashMap::new(),
            idempotency: HashMap::new(),
            leases: HashMap::new(),
            artifacts: HashMap::new(),
            inline_artifacts: HashMap::new(),
            blobs: HashMap::new(),
            envelopes: HashMap::new(),
            transport_contexts: HashMap::new(),
            terminal_results: HashMap::new(),
            result_outbox: HashMap::new(),
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
            KeryxEventType::TaskDeadLettered,
            Some(TaskStatus::Running),
            TaskStatus::Failed,
        );
        let store = InMemoryStore {
            inner: Mutex::new(state),
        };

        assert_eq!(
            store.replay_task(task.task_id()).unwrap_err(),
            StoreError::CorruptEventStream(task.task_id().clone())
        );
    }

    fn accepted_task(id: &str) -> TaskRecord {
        task(id, TaskStatus::Pending, Some("legacy-idem"))
    }

    #[test]
    fn accept_legacy_event_normalizes_task_leased_to_running_with_task_started_event() {
        let accepted = accepted_task("legacy-lease-task");
        let store = InMemoryStore::default();
        store.accept_task(accepted.clone()).unwrap();

        let updated = store
            .accept_legacy_event(accepted.task_id(), KeryxEventType::TaskLeased)
            .unwrap();
        assert_eq!(updated.status, TaskStatus::Running);

        let events = store.events_for_task(accepted.task_id()).unwrap();
        let last = events.last().unwrap();
        assert_eq!(last.event_type, KeryxEventType::TaskStarted);
        assert_eq!(last.from_status, Some(TaskStatus::Pending));
        assert_eq!(last.to_status, TaskStatus::Running);
        assert!(store.replay_task(accepted.task_id()).is_ok());
    }

    #[test]
    fn accept_legacy_event_appends_operational_task_queued_without_status_change() {
        let accepted = accepted_task("legacy-queued-task");
        let store = InMemoryStore::default();
        store.accept_task(accepted.clone()).unwrap();

        let updated = store
            .accept_legacy_event(accepted.task_id(), KeryxEventType::TaskQueued)
            .unwrap();
        assert_eq!(updated.status, TaskStatus::Pending);

        let events = store.events_for_task(accepted.task_id()).unwrap();
        let last = events.last().unwrap();
        assert_eq!(last.event_type, KeryxEventType::TaskQueued);
        assert!(store.replay_task(accepted.task_id()).is_ok());
    }

    #[test]
    fn accept_legacy_event_rejects_unknown_status_combination() {
        let accepted = accepted_task("legacy-invalid-task");
        let store = InMemoryStore::default();
        store.accept_task(accepted.clone()).unwrap();

        let err = store
            .accept_legacy_event(accepted.task_id(), KeryxEventType::TaskTimedOut)
            .unwrap_err();
        assert_eq!(
            err,
            StoreError::Validation(ValidationError::InvalidTaskTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Pending,
            })
        );
    }
}
