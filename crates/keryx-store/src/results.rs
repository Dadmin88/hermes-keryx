//! Durable authenticated task context, terminal results, and result-delivery outbox.

use std::{collections::HashSet, path::Path};

use keryx_core::{
    origin_result_artifact_id, AgentId, ArtifactMeta, Digest, KeryxEventType, LeaseId, PeerId,
    RetryPolicy, TaskId, TaskStatus, MAX_CROSS_NODE_RESULT_ARTIFACT_BYTES,
};
use sqlx::Row;

use super::*;

pub const AUTHENTICATED_SENDER_METADATA_KEY: &str = "keryx.authenticated_sender_peer_id";
pub const EXPECTED_EXECUTOR_METADATA_KEY: &str = "keryx.expected_executor_peer_id";

pub(super) const MIGRATION_007: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS task_transport_context (task_id TEXT PRIMARY KEY, authenticated_sender_peer_id TEXT, expected_executor_peer_id TEXT, destination_peer_id TEXT NOT NULL, relay_frame_id TEXT, received_at_ms INTEGER NOT NULL, FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE)",
    "CREATE TABLE IF NOT EXISTS task_terminal_results (task_id TEXT PRIMARY KEY, encoded_result BLOB NOT NULL, terminal_status TEXT NOT NULL, return_peer_id TEXT, executor_peer_id TEXT NOT NULL, created_at_ms INTEGER NOT NULL, FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE)",
    "CREATE TABLE IF NOT EXISTS result_outbox (delivery_id TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE, target_peer_id TEXT NOT NULL, state TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0, next_attempt_at_ms INTEGER NOT NULL, lease_owner TEXT, lease_expires_at_ms INTEGER, last_error TEXT, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL, FOREIGN KEY(task_id) REFERENCES task_terminal_results(task_id) ON DELETE CASCADE)",
    "CREATE INDEX IF NOT EXISTS result_outbox_due_idx ON result_outbox(state, next_attempt_at_ms, created_at_ms, delivery_id)",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteResultTerminalReason {
    DeadlineExpired,
    Canceled,
}

impl std::fmt::Display for RemoteResultTerminalReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::DeadlineExpired => "deadline_expired",
            Self::Canceled => "canceled",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteResultIngestOutcome {
    Applied(TaskRecord),
    Duplicate(TaskRecord),
    SettledTerminal {
        task: TaskRecord,
        reason: RemoteResultTerminalReason,
        canonical_result: Option<TerminalResultRecord>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTransportContextRecord {
    pub task_id: TaskId,
    pub authenticated_sender_peer_id: Option<PeerId>,
    pub expected_executor_peer_id: Option<PeerId>,
    pub destination_peer_id: PeerId,
    pub relay_frame_id: Option<String>,
    pub received_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalResultRecord {
    pub task_id: TaskId,
    pub encoded_result: Vec<u8>,
    pub terminal_status: TaskStatus,
    pub return_peer_id: Option<PeerId>,
    pub executor_peer_id: PeerId,
    pub created_at_ms: i64,
}

/// Store-level artifact ingress record. This intentionally has no protobuf dependency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginResultArtifact {
    /// The result-list ordinal assigned by the origin protocol boundary.
    pub ordinal: u32,
    /// Canonical descriptor that will be persisted after byte validation.
    pub meta: ArtifactMeta,
    /// Transport bytes; zero bytes are valid when this record exists.
    pub content: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultDeliveryState {
    Pending,
    Leased,
    Delivered,
    DeadLettered,
}

impl ResultDeliveryState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Delivered => "delivered",
            Self::DeadLettered => "dead_lettered",
        }
    }

    fn parse(value: &str) -> StoreResult<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "delivered" => Ok(Self::Delivered),
            "dead_lettered" => Ok(Self::DeadLettered),
            other => Err(StoreError::Database(format!(
                "unknown result delivery state {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultOutboxRecord {
    pub delivery_id: String,
    pub task_id: TaskId,
    pub target_peer_id: PeerId,
    pub state: ResultDeliveryState,
    pub attempt_count: u32,
    pub next_attempt_at_ms: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl TerminalResultRecord {
    fn validate(&self) -> StoreResult<()> {
        if !self.terminal_status.is_terminal() {
            return Err(StoreError::TerminalResultNotTerminal(self.task_id.clone()));
        }
        Ok(())
    }
}

fn delivery_id(task_id: &TaskId) -> String {
    format!("result-{}", task_id.as_str())
}

fn make_outbox(result: &TerminalResultRecord) -> Option<ResultOutboxRecord> {
    result
        .return_peer_id
        .as_ref()
        .map(|target| ResultOutboxRecord {
            delivery_id: delivery_id(&result.task_id),
            task_id: result.task_id.clone(),
            target_peer_id: target.clone(),
            state: ResultDeliveryState::Pending,
            attempt_count: 0,
            next_attempt_at_ms: result.created_at_ms,
            lease_owner: None,
            lease_expires_at_ms: None,
            last_error: None,
            created_at_ms: result.created_at_ms,
            updated_at_ms: result.created_at_ms,
        })
}

fn validate_origin_result_artifacts(
    task_id: &TaskId,
    artifacts: &[OriginResultArtifact],
) -> StoreResult<Vec<ArtifactRecord>> {
    let mut aggregate_len = 0_u64;
    let mut records = Vec::with_capacity(artifacts.len());
    let mut next_minimum_ordinal = 0_u32;
    for artifact in artifacts {
        if artifact.ordinal < next_minimum_ordinal {
            return Err(StoreError::OriginResultArtifactOrdinalMismatch {
                expected: next_minimum_ordinal,
                actual: artifact.ordinal,
            });
        }
        next_minimum_ordinal = artifact.ordinal.checked_add(1).ok_or(
            StoreError::OriginResultArtifactOrdinalMismatch {
                expected: artifact.ordinal,
                actual: artifact.ordinal,
            },
        )?;
        if artifact.meta.task_id != *task_id {
            return Err(StoreError::OriginResultArtifactTaskMismatch {
                task_id: task_id.clone(),
                artifact_task_id: artifact.meta.task_id.clone(),
            });
        }
        if artifact.meta.artifact_id != origin_result_artifact_id(task_id, artifact.ordinal) {
            return Err(StoreError::OriginResultArtifactIdMismatch {
                task_id: task_id.clone(),
                ordinal: artifact.ordinal,
            });
        }
        let actual_len = artifact.content.len() as u64;
        if artifact.meta.byte_len != actual_len {
            return Err(StoreError::ArtifactLengthMismatch {
                declared: artifact.meta.byte_len,
                actual: actual_len,
            });
        }
        let actual_digest = Digest::compute(&artifact.content);
        if artifact.meta.digest != actual_digest {
            return Err(StoreError::DigestMismatch {
                expected: artifact.meta.digest.as_str().to_owned(),
                actual: actual_digest.as_str().to_owned(),
            });
        }
        aggregate_len =
            aggregate_len
                .checked_add(actual_len)
                .ok_or(StoreError::ArtifactTooLarge {
                    byte_len: u64::MAX,
                    limit_bytes: MAX_CROSS_NODE_RESULT_ARTIFACT_BYTES as u64,
                })?;
        if aggregate_len > MAX_CROSS_NODE_RESULT_ARTIFACT_BYTES as u64 {
            return Err(StoreError::ArtifactTooLarge {
                byte_len: aggregate_len,
                limit_bytes: MAX_CROSS_NODE_RESULT_ARTIFACT_BYTES as u64,
            });
        }
        records.push(artifact_record_from_meta(
            &artifact.meta,
            actual_digest,
            actual_len,
        ));
    }
    Ok(records)
}

fn ensure_result_task(result: &TerminalResultRecord, task_id: &TaskId) -> StoreResult<()> {
    result.validate()?;
    if &result.task_id == task_id {
        Ok(())
    } else {
        Err(StoreError::TerminalResultTaskMismatch {
            task_id: task_id.clone(),
            result_task_id: result.task_id.clone(),
        })
    }
}

fn ensure_context_task(context: &TaskTransportContextRecord, task_id: &TaskId) -> StoreResult<()> {
    if &context.task_id == task_id {
        Ok(())
    } else {
        Err(StoreError::TransportContextTaskMismatch {
            task_id: task_id.clone(),
            context_task_id: context.task_id.clone(),
        })
    }
}

fn same_delivered_envelope(left: &TaskEnvelopeRecord, right: &TaskEnvelopeRecord) -> bool {
    left.task_id == right.task_id && left.encoded_envelope == right.encoded_envelope
}

fn same_transport_identity(
    left: &TaskTransportContextRecord,
    right: &TaskTransportContextRecord,
) -> bool {
    left.task_id == right.task_id
        && left.authenticated_sender_peer_id == right.authenticated_sender_peer_id
        && left.expected_executor_peer_id == right.expected_executor_peer_id
        && left.destination_peer_id == right.destination_peer_id
}

impl InMemoryStore {
    pub fn cancel_task_with_result(
        &self,
        task_id: &TaskId,
        _reason: &str,
        now_ms: i64,
        result: TerminalResultRecord,
    ) -> StoreResult<TaskRecord> {
        ensure_result_task(&result, task_id)?;
        result.validate()?;
        let mut state = self.lock()?;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()))?;
        if task.status == TaskStatus::Running {
            let active = state
                .leases
                .get(task_id)
                .cloned()
                .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
            ensure_active_lease_unexpired(&active, now_ms)?;
        }
        let transition = validate_cancel_transition(task.status)?;
        if state.terminal_results.contains_key(task_id) {
            return Err(StoreError::TerminalResultConflict(task_id.clone()));
        }
        if result.terminal_status != transition.to {
            return Err(StoreError::TerminalResultNotTerminal(task_id.clone()));
        }
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
        if let Some(outbox) = make_outbox(&result) {
            state
                .result_outbox
                .insert(outbox.delivery_id.clone(), outbox);
        }
        state.terminal_results.insert(task_id.clone(), result);
        Ok(updated)
    }

    pub fn put_transport_context(
        &self,
        context: TaskTransportContextRecord,
    ) -> StoreResult<TaskTransportContextRecord> {
        let mut state = self.lock()?;
        if !state.tasks.contains_key(&context.task_id) {
            return Err(StoreError::TaskNotFound(context.task_id));
        }
        match state.transport_contexts.get(&context.task_id) {
            Some(existing) if existing == &context => return Ok(existing.clone()),
            Some(_) => return Err(StoreError::TransportContextConflict(context.task_id)),
            None => {}
        }
        state
            .transport_contexts
            .insert(context.task_id.clone(), context.clone());
        Ok(context)
    }

    pub fn accept_task_with_envelope_and_context(
        &self,
        task: TaskRecord,
        envelope: TaskEnvelopeRecord,
        context: TaskTransportContextRecord,
    ) -> StoreResult<TaskRecord> {
        validate_accepted_task_status(&task)?;
        ensure_pending_accept(&task)?;
        ensure_matching_envelope_task_id(&task, &envelope)?;
        ensure_context_task(&context, task.task_id())?;
        let mut state = self.lock()?;
        if let Some(existing) = state.tasks.get(task.task_id()).cloned() {
            if existing == task
                && state
                    .envelopes
                    .get(task.task_id())
                    .is_some_and(|stored| same_delivered_envelope(stored, &envelope))
                && state
                    .transport_contexts
                    .get(task.task_id())
                    .is_some_and(|stored| same_transport_identity(stored, &context))
            {
                if state
                    .transport_contexts
                    .get(task.task_id())
                    .is_some_and(|stored| stored.relay_frame_id != context.relay_frame_id)
                {
                    state
                        .transport_contexts
                        .insert(task.task_id().clone(), context);
                }
                return Ok(existing);
            }
            return Err(StoreError::TransportContextConflict(task.task_id().clone()));
        }
        if let Some(key) = &task.idempotency_key {
            if let Some(existing_task_id) = state.idempotency.get(key) {
                return Err(StoreError::IdempotencyConflict {
                    key: key.clone(),
                    existing_task_id: existing_task_id.clone(),
                });
            }
        }
        let task_id = task.task_id().clone();
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
        state.envelopes.insert(task_id.clone(), envelope);
        state.transport_contexts.insert(task_id, context);
        Ok(task)
    }

    pub fn get_transport_context(
        &self,
        task_id: &TaskId,
    ) -> StoreResult<TaskTransportContextRecord> {
        self.lock()?
            .transport_contexts
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TransportContextNotFound(task_id.clone()))
    }

    pub fn complete_task_with_result(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        result: TerminalResultRecord,
    ) -> StoreResult<TaskRecord> {
        ensure_result_task(&result, task_id)?;
        let mut state = self.lock()?;
        if let Some(existing) = state.terminal_results.get(task_id) {
            if existing == &result {
                return state
                    .tasks
                    .get(task_id)
                    .cloned()
                    .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()));
            }
            return Err(StoreError::TerminalResultConflict(task_id.clone()));
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
        if let Some(outbox) = make_outbox(&result) {
            state
                .result_outbox
                .insert(outbox.delivery_id.clone(), outbox);
        }
        state.terminal_results.insert(task_id.clone(), result);
        Ok(updated)
    }

    pub fn fail_task_with_result(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        error_reason: &str,
        policy: &RetryPolicy,
        result: TerminalResultRecord,
    ) -> StoreResult<TaskRecord> {
        ensure_result_task(&result, task_id)?;
        let mut state = self.lock()?;
        if let Some(existing) = state.terminal_results.get(task_id) {
            if existing == &result {
                return state
                    .tasks
                    .get(task_id)
                    .cloned()
                    .ok_or_else(|| StoreError::TaskNotFound(task_id.clone()));
            }
            return Err(StoreError::TerminalResultConflict(task_id.clone()));
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
            return self.retry_task_in_state(&mut state, task_id, &active, task);
        } else {
            self.dead_letter_task_in_state(&mut state, task_id, &active, task, error_reason)?
        };
        if let Some(outbox) = make_outbox(&result) {
            state
                .result_outbox
                .insert(outbox.delivery_id.clone(), outbox);
        }
        state.terminal_results.insert(task_id.clone(), result);
        Ok(updated)
    }

    pub fn get_terminal_result(&self, task_id: &TaskId) -> StoreResult<TerminalResultRecord> {
        self.lock()?
            .terminal_results
            .get(task_id)
            .cloned()
            .ok_or_else(|| StoreError::TerminalResultNotFound(task_id.clone()))
    }

    pub fn result_delivery_for_task(
        &self,
        task_id: &TaskId,
    ) -> StoreResult<Option<ResultOutboxRecord>> {
        Ok(self
            .lock()?
            .result_outbox
            .values()
            .find(|row| &row.task_id == task_id)
            .cloned())
    }

    pub fn pending_result_deliveries(&self, limit: usize) -> StoreResult<Vec<ResultOutboxRecord>> {
        let state = self.lock()?;
        let mut values = state
            .result_outbox
            .values()
            .filter(|row| row.state == ResultDeliveryState::Pending)
            .cloned()
            .collect::<Vec<_>>();
        values.sort_by(|left, right| {
            left.next_attempt_at_ms
                .cmp(&right.next_attempt_at_ms)
                .then_with(|| left.delivery_id.cmp(&right.delivery_id))
        });
        values.truncate(limit);
        Ok(values)
    }
}

impl SqliteStore {
    pub async fn cancel_task_with_result(
        &self,
        task_id: &TaskId,
        _reason: &str,
        now_ms: i64,
        result: TerminalResultRecord,
    ) -> StoreResult<TaskRecord> {
        ensure_result_task(&result, task_id)?;
        result.validate()?;
        let mut tx = self.pool.begin().await?;
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        if task.status == TaskStatus::Running {
            let active = fetch_active_lease_with_executor(&mut tx, task_id)
                .await?
                .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
            ensure_active_lease_unexpired(&active, now_ms)?;
        }
        let transition = validate_cancel_transition(task.status)?;
        if fetch_terminal_result_optional(&mut tx, task_id)
            .await?
            .is_some()
        {
            return Err(StoreError::TerminalResultConflict(task_id.clone()));
        }
        if result.terminal_status != transition.to {
            return Err(StoreError::TerminalResultNotTerminal(task_id.clone()));
        }
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
        insert_terminal_result_and_outbox(&mut tx, &result).await?;
        tx.commit().await?;
        self.get_task(task_id).await
    }

    pub async fn put_transport_context(
        &self,
        context: TaskTransportContextRecord,
    ) -> StoreResult<TaskTransportContextRecord> {
        let mut tx = self.pool.begin().await?;
        match fetch_task_with_executor(&mut tx, &context.task_id).await {
            Ok(_) => {}
            Err(StoreError::TaskNotFound(_)) => {
                return Err(StoreError::TaskNotFound(context.task_id));
            }
            Err(error) => return Err(error),
        }
        if let Some(existing) = fetch_transport_context_optional(&mut tx, &context.task_id).await? {
            if existing == context {
                tx.commit().await?;
                return Ok(existing);
            }
            return Err(StoreError::TransportContextConflict(context.task_id));
        }
        insert_transport_context(&mut tx, &context).await?;
        tx.commit().await?;
        Ok(context)
    }

    pub async fn record_relay_receipt(
        &self,
        task_id: &TaskId,
        authenticated_source_peer_id: &PeerId,
        accepted_destination_peer_id: &PeerId,
        relay_frame_id: &str,
        accepted_at_ms: i64,
    ) -> StoreResult<TaskTransportContextRecord> {
        let frame_id = relay_frame_id.trim();
        if frame_id.is_empty() {
            return Err(StoreError::TransportContextConflict(task_id.clone()));
        }
        let mut tx = self.pool.begin().await?;
        let mut context = fetch_transport_context_optional(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::TransportContextNotFound(task_id.clone()))?;
        if context.expected_executor_peer_id.as_ref() != Some(accepted_destination_peer_id)
            || context.destination_peer_id != *accepted_destination_peer_id
        {
            return Err(StoreError::TransportContextConflict(task_id.clone()));
        }
        if let Some(existing_frame_id) = context.relay_frame_id.as_deref() {
            if existing_frame_id == frame_id
                && context.authenticated_sender_peer_id.as_ref()
                    == Some(authenticated_source_peer_id)
                && context.received_at_ms == accepted_at_ms
            {
                tx.commit().await?;
                return Ok(context);
            }
            if context.authenticated_sender_peer_id.as_ref() != Some(authenticated_source_peer_id) {
                return Err(StoreError::TransportContextConflict(task_id.clone()));
            }
        }
        let update = sqlx::query(
            "UPDATE task_transport_context SET authenticated_sender_peer_id = ?, relay_frame_id = ?, received_at_ms = ? WHERE task_id = ?",
        )
        .bind(authenticated_source_peer_id.as_str())
        .bind(frame_id)
        .bind(accepted_at_ms)
        .bind(task_id.as_str())
        .execute(&mut *tx)
        .await?;
        if update.rows_affected() != 1 {
            return Err(StoreError::TransportContextConflict(task_id.clone()));
        }
        context.authenticated_sender_peer_id = Some(authenticated_source_peer_id.clone());
        context.relay_frame_id = Some(frame_id.to_string());
        context.received_at_ms = accepted_at_ms;
        tx.commit().await?;
        Ok(context)
    }

    pub async fn accept_task_with_envelope_and_context(
        &self,
        task: TaskRecord,
        envelope: TaskEnvelopeRecord,
        context: TaskTransportContextRecord,
    ) -> StoreResult<TaskRecord> {
        validate_accepted_task_status(&task)?;
        ensure_pending_accept(&task)?;
        ensure_matching_envelope_task_id(&task, &envelope)?;
        ensure_context_task(&context, task.task_id())?;
        let mut tx = self.pool.begin().await?;

        if let Some(existing) = fetch_task_optional_with_executor(&mut tx, task.task_id()).await? {
            let existing_envelope =
                fetch_task_envelope_optional_with_executor(&mut tx, task.task_id()).await?;
            let existing_context =
                fetch_transport_context_optional(&mut tx, task.task_id()).await?;
            if existing == task
                && existing_envelope
                    .as_ref()
                    .is_some_and(|stored| same_delivered_envelope(stored, &envelope))
                && existing_context
                    .as_ref()
                    .is_some_and(|stored| same_transport_identity(stored, &context))
            {
                if existing_context
                    .as_ref()
                    .is_some_and(|stored| stored.relay_frame_id != context.relay_frame_id)
                {
                    sqlx::query(
                        "UPDATE task_transport_context SET relay_frame_id = ?, received_at_ms = ? WHERE task_id = ?",
                    )
                    .bind(context.relay_frame_id.as_deref())
                    .bind(context.received_at_ms)
                    .bind(task.task_id().as_str())
                    .execute(&mut *tx)
                    .await?;
                }
                tx.commit().await?;
                return Ok(existing);
            }
            return Err(StoreError::TransportContextConflict(task.task_id().clone()));
        }

        if let Some(key) = &task.idempotency_key {
            if let Some(row) = sqlx::query("SELECT task_id FROM idempotency_keys WHERE key = ?")
                .bind(key.as_str())
                .fetch_optional(&mut *tx)
                .await?
            {
                return Err(StoreError::IdempotencyConflict {
                    key: key.clone(),
                    existing_task_id: TaskId::new(row.get::<String, _>("task_id"))?,
                });
            }
        }

        sqlx::query("INSERT INTO tasks (task_id, status, idempotency_key, retry_count, dead_lettered, dead_letter_reason, deadline_ms) VALUES (?, ?, ?, ?, ?, ?, ?)")
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
        sqlx::query("INSERT INTO task_envelopes (task_id, encoded_envelope, received_at_ms) VALUES (?, ?, ?)")
            .bind(envelope.task_id.as_str())
            .bind(&envelope.encoded_envelope)
            .bind(envelope.received_at_ms)
            .execute(&mut *tx)
            .await?;
        insert_transport_context(&mut tx, &context).await?;
        tx.commit().await?;
        Ok(task)
    }

    pub async fn get_transport_context(
        &self,
        task_id: &TaskId,
    ) -> StoreResult<TaskTransportContextRecord> {
        let mut tx = self.pool.begin().await?;
        let value = fetch_transport_context_optional(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::TransportContextNotFound(task_id.clone()))?;
        tx.commit().await?;
        Ok(value)
    }

    pub async fn complete_task_with_result(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        result: TerminalResultRecord,
    ) -> StoreResult<TaskRecord> {
        ensure_result_task(&result, task_id)?;
        let mut tx = self.pool.begin().await?;
        if let Some(existing) = fetch_terminal_result_optional(&mut tx, task_id).await? {
            if existing == result {
                let task = fetch_task_with_executor(&mut tx, task_id).await?;
                tx.commit().await?;
                return Ok(task);
            }
            return Err(StoreError::TerminalResultConflict(task_id.clone()));
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
        insert_terminal_result_and_outbox(&mut tx, &result).await?;
        tx.commit().await?;
        Ok(updated)
    }

    pub async fn fail_task_with_result(
        &self,
        task_id: &TaskId,
        lease_id: &LeaseId,
        worker_id: &AgentId,
        error_reason: &str,
        policy: &RetryPolicy,
        result: TerminalResultRecord,
    ) -> StoreResult<TaskRecord> {
        ensure_result_task(&result, task_id)?;
        let mut tx = self.pool.begin().await?;
        if let Some(existing) = fetch_terminal_result_optional(&mut tx, task_id).await? {
            if existing == result {
                let task = fetch_task_with_executor(&mut tx, task_id).await?;
                tx.commit().await?;
                return Ok(task);
            }
            return Err(StoreError::TerminalResultConflict(task_id.clone()));
        }
        let task = fetch_task_with_executor(&mut tx, task_id).await?;
        let active = fetch_active_lease_with_executor(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::LeaseNotFound(task_id.clone()))?;
        ensure_matching_lease_id(task_id, &active, lease_id)?;
        ensure_matching_worker_id(task_id, &active, worker_id)?;
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
            let retry = sqlite_retry_task_in_tx(&mut tx, task_id, &task).await?;
            tx.commit().await?;
            return Ok(retry);
        } else {
            sqlite_dead_letter_task_in_tx(&mut tx, task_id, &task, error_reason).await?
        };
        insert_terminal_result_and_outbox(&mut tx, &result).await?;
        tx.commit().await?;
        Ok(updated)
    }

    pub async fn get_terminal_result(&self, task_id: &TaskId) -> StoreResult<TerminalResultRecord> {
        let mut tx = self.pool.begin().await?;
        let value = fetch_terminal_result_optional(&mut tx, task_id)
            .await?
            .ok_or_else(|| StoreError::TerminalResultNotFound(task_id.clone()))?;
        tx.commit().await?;
        Ok(value)
    }

    pub async fn result_delivery_for_task(
        &self,
        task_id: &TaskId,
    ) -> StoreResult<Option<ResultOutboxRecord>> {
        let row = sqlx::query("SELECT delivery_id, task_id, target_peer_id, state, attempt_count, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, created_at_ms, updated_at_ms FROM result_outbox WHERE task_id = ?")
            .bind(task_id.as_str())
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_to_outbox).transpose()
    }

    pub async fn pending_result_deliveries(
        &self,
        limit: usize,
    ) -> StoreResult<Vec<ResultOutboxRecord>> {
        let rows = sqlx::query("SELECT delivery_id, task_id, target_peer_id, state, attempt_count, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, created_at_ms, updated_at_ms FROM result_outbox WHERE state = 'pending' ORDER BY next_attempt_at_ms ASC, created_at_ms ASC, delivery_id ASC LIMIT ?")
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_outbox).collect()
    }

    pub async fn claim_next_result_delivery(
        &self,
        worker_id: &str,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> StoreResult<Option<(ResultOutboxRecord, TerminalResultRecord)>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE result_outbox SET state = 'pending', lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ? WHERE state = 'leased' AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms <= ?")
            .bind(now_ms)
            .bind(now_ms)
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query("SELECT delivery_id, task_id, target_peer_id, state, attempt_count, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, created_at_ms, updated_at_ms FROM result_outbox WHERE state = 'pending' AND next_attempt_at_ms <= ? ORDER BY next_attempt_at_ms ASC, created_at_ms ASC, delivery_id ASC LIMIT 1")
            .bind(now_ms)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let mut outbox = row_to_outbox(row)?;
        let lease_expires = now_ms.saturating_add(lease_duration_ms.max(1));
        let changed = sqlx::query("UPDATE result_outbox SET state = 'leased', lease_owner = ?, lease_expires_at_ms = ?, updated_at_ms = ? WHERE delivery_id = ? AND state = 'pending'")
            .bind(worker_id)
            .bind(lease_expires)
            .bind(now_ms)
            .bind(&outbox.delivery_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if changed != 1 {
            tx.rollback().await.ok();
            return Ok(None);
        }
        outbox.state = ResultDeliveryState::Leased;
        outbox.lease_owner = Some(worker_id.to_string());
        outbox.lease_expires_at_ms = Some(lease_expires);
        outbox.updated_at_ms = now_ms;
        let result = fetch_terminal_result_optional(&mut tx, &outbox.task_id)
            .await?
            .ok_or_else(|| StoreError::TerminalResultNotFound(outbox.task_id.clone()))?;
        tx.commit().await?;
        Ok(Some((outbox, result)))
    }

    pub async fn ack_result_delivery(
        &self,
        delivery_id: &str,
        worker_id: &str,
        lease_expires_at_ms: i64,
        now_ms: i64,
    ) -> StoreResult<()> {
        let changed = sqlx::query("UPDATE result_outbox SET state = 'delivered', lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ? WHERE delivery_id = ? AND state = 'leased' AND lease_owner = ? AND lease_expires_at_ms = ? AND lease_expires_at_ms > ?")
            .bind(now_ms)
            .bind(delivery_id)
            .bind(worker_id)
            .bind(lease_expires_at_ms)
            .bind(now_ms)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::ResultDeliveryLeaseMismatch(
                delivery_id.to_string(),
            ))
        }
    }

    pub async fn fail_result_delivery(
        &self,
        delivery_id: &str,
        claim: (&str, i64),
        now_ms: i64,
        retry_at_ms: i64,
        error: &str,
        dead_letter: bool,
    ) -> StoreResult<()> {
        let state = if dead_letter {
            "dead_lettered"
        } else {
            "pending"
        };
        let changed = sqlx::query("UPDATE result_outbox SET state = ?, attempt_count = attempt_count + 1, next_attempt_at_ms = ?, lease_owner = NULL, lease_expires_at_ms = NULL, last_error = ?, updated_at_ms = ? WHERE delivery_id = ? AND state = 'leased' AND lease_owner = ? AND lease_expires_at_ms = ? AND lease_expires_at_ms > ?")
            .bind(state)
            .bind(retry_at_ms)
            .bind(error)
            .bind(now_ms)
            .bind(delivery_id)
            .bind(claim.0)
            .bind(claim.1)
            .bind(now_ms)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::ResultDeliveryLeaseMismatch(
                delivery_id.to_string(),
            ))
        }
    }

    pub async fn apply_remote_result(
        &self,
        result: TerminalResultRecord,
        authenticated_executor_peer_id: &PeerId,
    ) -> StoreResult<TaskRecord> {
        self.apply_remote_result_with_artifacts(
            result,
            &[],
            authenticated_executor_peer_id,
            Path::new(""),
        )
        .await
    }

    /// Atomically applies an authenticated remote terminal result and its origin-owned artifacts.
    ///
    /// The caller supplies a descriptor-only result record; this store intentionally has no
    /// protocol dependency and persists that exact record only after all payload checks pass.
    pub async fn apply_remote_result_with_artifacts(
        &self,
        result: TerminalResultRecord,
        artifacts: &[OriginResultArtifact],
        authenticated_executor_peer_id: &PeerId,
        blob_dir: impl AsRef<Path>,
    ) -> StoreResult<TaskRecord> {
        let task_id = result.task_id.clone();
        match self
            .ingest_remote_result_with_artifacts(
                result,
                artifacts,
                authenticated_executor_peer_id,
                blob_dir,
            )
            .await?
        {
            RemoteResultIngestOutcome::Applied(task)
            | RemoteResultIngestOutcome::Duplicate(task) => Ok(task),
            RemoteResultIngestOutcome::SettledTerminal { reason, .. } => {
                Err(StoreError::RemoteResultTerminallySettled { task_id, reason })
            }
        }
    }

    /// Applies, deduplicates, or safely settles an authenticated remote terminal result.
    pub async fn ingest_remote_result_with_artifacts(
        &self,
        result: TerminalResultRecord,
        artifacts: &[OriginResultArtifact],
        authenticated_executor_peer_id: &PeerId,
        blob_dir: impl AsRef<Path>,
    ) -> StoreResult<RemoteResultIngestOutcome> {
        result.validate()?;
        if &result.executor_peer_id != authenticated_executor_peer_id {
            return Err(StoreError::RemoteResultExecutorMismatch {
                task_id: result.task_id.clone(),
                expected: authenticated_executor_peer_id.clone(),
                actual: result.executor_peer_id.clone(),
            });
        }
        let records = validate_origin_result_artifacts(&result.task_id, artifacts)?;
        let blob_dir = blob_dir.as_ref().to_path_buf();
        let mut tx = self.pool.begin().await?;
        let context = fetch_transport_context_optional(&mut tx, &result.task_id)
            .await?
            .ok_or_else(|| StoreError::TransportContextNotFound(result.task_id.clone()))?;
        if context.expected_executor_peer_id.as_ref() != Some(authenticated_executor_peer_id) {
            return Err(StoreError::RemoteResultExecutorMismatch {
                task_id: result.task_id.clone(),
                expected: context
                    .expected_executor_peer_id
                    .unwrap_or_else(|| context.destination_peer_id.clone()),
                actual: authenticated_executor_peer_id.clone(),
            });
        }

        let task = fetch_task_with_executor(&mut tx, &result.task_id).await?;
        let terminal_reason = remote_result_terminal_reason_with_executor(&mut tx, &task).await?;
        let existing = fetch_terminal_result_optional(&mut tx, &result.task_id).await?;
        if terminal_reason.is_none() && existing.as_ref().is_some_and(|stored| stored == &result) {
            let stored = fetch_artifacts_for_task_with_executor(&mut tx, &result.task_id).await?;
            if stored.len() != records.len() || stored.iter().any(|row| !records.contains(row)) {
                return Err(StoreError::TerminalResultConflict(result.task_id.clone()));
            }
            tx.commit().await?;
            for artifact in artifacts {
                let (_, persisted) = self
                    .get_artifact(&artifact.meta.artifact_id, &blob_dir)
                    .await?;
                if persisted != artifact.content {
                    return Err(StoreError::OriginResultArtifactConflict(
                        artifact.meta.artifact_id.clone(),
                    ));
                }
            }
            return self
                .get_task(&result.task_id)
                .await
                .map(RemoteResultIngestOutcome::Duplicate);
        }

        for record in &records {
            if fetch_artifact_optional_with_executor(&mut tx, &record.artifact_id)
                .await?
                .is_some()
            {
                return Err(StoreError::OriginResultArtifactConflict(
                    record.artifact_id.clone(),
                ));
            }
        }

        let settlement_reason = match (terminal_reason, existing.as_ref()) {
            (Some(RemoteResultTerminalReason::DeadlineExpired), None) => {
                Some(RemoteResultTerminalReason::DeadlineExpired)
            }
            (Some(RemoteResultTerminalReason::Canceled), Some(_)) => {
                Some(RemoteResultTerminalReason::Canceled)
            }
            _ => None,
        };
        if let Some(reason) = settlement_reason {
            if !records.is_empty() {
                return Err(StoreError::RemoteResultTerminalArtifactsRejected {
                    task_id: result.task_id.clone(),
                    reason,
                });
            }
            tx.commit().await?;
            return Ok(RemoteResultIngestOutcome::SettledTerminal {
                task,
                reason,
                canonical_result: existing,
            });
        }
        if existing.is_some() {
            return Err(StoreError::TerminalResultConflict(result.task_id.clone()));
        }

        let mut prepared = Vec::new();
        let mut prepared_digests = HashSet::new();
        for (artifact, record) in artifacts.iter().zip(&records) {
            if !record.inline && prepared_digests.insert(record.digest.clone()) {
                match prepare_blob_write_with_executor(
                    &mut tx,
                    &record.digest,
                    &artifact.content,
                    &blob_dir,
                )
                .await
                {
                    Ok(prepared_blob) => prepared.push(prepared_blob),
                    Err(error) => {
                        tx.rollback().await.ok();
                        for prepared_blob in &prepared {
                            rollback_prepared_blob_write(Some(prepared_blob))?;
                        }
                        return Err(error);
                    }
                }
            }
        }

        let apply_result = async {
            let mut task = task;
            if task.status == TaskStatus::Pending {
                let sequence = next_sequence_with_executor(&mut tx, &result.task_id).await?;
                update_task_status_with_executor(&mut tx, &result.task_id, TaskStatus::Running)
                    .await?;
                insert_event(
                    &mut tx,
                    &result.task_id,
                    sequence,
                    KeryxEventType::TaskStarted,
                    Some(TaskStatus::Pending),
                    TaskStatus::Running,
                )
                .await?;
                task.status = TaskStatus::Running;
            }
            let transition = validate_transition(task.status, result.terminal_status)?;
            let sequence = next_sequence_with_executor(&mut tx, &result.task_id).await?;
            update_task_status_with_executor(&mut tx, &result.task_id, result.terminal_status)
                .await?;
            insert_event(
                &mut tx,
                &result.task_id,
                sequence,
                transition.event_type,
                Some(transition.from),
                transition.to,
            )
            .await?;
            for (artifact, record) in artifacts.iter().zip(&records) {
                if !record.inline {
                    increment_blob_ref_with_executor(&mut tx, &record.digest, record.byte_len)
                        .await?;
                }
                insert_origin_artifact(&mut tx, record, &artifact.content).await?;
            }
            insert_terminal_result_only(&mut tx, &result).await
        }
        .await;
        if let Err(error) = apply_result {
            tx.rollback().await.ok();
            for prepared_blob in &prepared {
                rollback_prepared_blob_write(Some(prepared_blob))?;
            }
            return Err(error);
        }
        if let Err(error) = tx.commit().await {
            for prepared_blob in &prepared {
                rollback_prepared_blob_write(Some(prepared_blob))?;
            }
            return Err(error.into());
        }
        self.get_task(&result.task_id)
            .await
            .map(RemoteResultIngestOutcome::Applied)
    }
}

async fn remote_result_terminal_reason_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task: &TaskRecord,
) -> StoreResult<Option<RemoteResultTerminalReason>> {
    if task.status != TaskStatus::Failed {
        return Ok(None);
    }
    let rows = sqlx::query(
        "SELECT task_id, sequence, event_type, from_status, to_status FROM task_events WHERE task_id = ? ORDER BY sequence ASC",
    )
    .bind(task.task_id().as_str())
    .fetch_all(&mut **tx)
    .await?;
    let events = rows
        .into_iter()
        .map(row_to_event)
        .collect::<StoreResult<Vec<_>>>()?;
    replay_task_from_snapshot_and_events(task, &events)?;
    Ok(events
        .iter()
        .rev()
        .find_map(|event| match event.event_type {
            KeryxEventType::TaskTimedOut => Some(RemoteResultTerminalReason::DeadlineExpired),
            KeryxEventType::TaskCanceled => Some(RemoteResultTerminalReason::Canceled),
            _ => None,
        }))
}

async fn insert_transport_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    context: &TaskTransportContextRecord,
) -> StoreResult<()> {
    sqlx::query("INSERT INTO task_transport_context (task_id, authenticated_sender_peer_id, expected_executor_peer_id, destination_peer_id, relay_frame_id, received_at_ms) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(context.task_id.as_str())
        .bind(context.authenticated_sender_peer_id.as_ref().map(PeerId::as_str))
        .bind(context.expected_executor_peer_id.as_ref().map(PeerId::as_str))
        .bind(context.destination_peer_id.as_str())
        .bind(context.relay_frame_id.as_deref())
        .bind(context.received_at_ms)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn fetch_transport_context_optional(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<Option<TaskTransportContextRecord>> {
    let row = sqlx::query("SELECT task_id, authenticated_sender_peer_id, expected_executor_peer_id, destination_peer_id, relay_frame_id, received_at_ms FROM task_transport_context WHERE task_id = ?")
        .bind(task_id.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    row.map(row_to_transport_context).transpose()
}

fn row_to_transport_context(
    row: sqlx::sqlite::SqliteRow,
) -> StoreResult<TaskTransportContextRecord> {
    Ok(TaskTransportContextRecord {
        task_id: TaskId::new(row.get::<String, _>("task_id"))?,
        authenticated_sender_peer_id: row
            .try_get::<Option<String>, _>("authenticated_sender_peer_id")?
            .map(PeerId::new)
            .transpose()?,
        expected_executor_peer_id: row
            .try_get::<Option<String>, _>("expected_executor_peer_id")?
            .map(PeerId::new)
            .transpose()?,
        destination_peer_id: PeerId::new(row.get::<String, _>("destination_peer_id"))?,
        relay_frame_id: row.try_get("relay_frame_id")?,
        received_at_ms: row.get("received_at_ms"),
    })
}

async fn insert_terminal_result_and_outbox(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    result: &TerminalResultRecord,
) -> StoreResult<()> {
    insert_terminal_result_only(tx, result).await?;
    if let Some(outbox) = make_outbox(result) {
        sqlx::query("INSERT INTO result_outbox (delivery_id, task_id, target_peer_id, state, attempt_count, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&outbox.delivery_id)
            .bind(outbox.task_id.as_str())
            .bind(outbox.target_peer_id.as_str())
            .bind(outbox.state.as_str())
            .bind(i64::from(outbox.attempt_count))
            .bind(outbox.next_attempt_at_ms)
            .bind(outbox.lease_owner.as_deref())
            .bind(outbox.lease_expires_at_ms)
            .bind(outbox.last_error.as_deref())
            .bind(outbox.created_at_ms)
            .bind(outbox.updated_at_ms)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn insert_terminal_result_only(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    result: &TerminalResultRecord,
) -> StoreResult<()> {
    sqlx::query("INSERT INTO task_terminal_results (task_id, encoded_result, terminal_status, return_peer_id, executor_peer_id, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(result.task_id.as_str())
        .bind(&result.encoded_result)
        .bind(status_to_str(result.terminal_status))
        .bind(result.return_peer_id.as_ref().map(PeerId::as_str))
        .bind(result.executor_peer_id.as_str())
        .bind(result.created_at_ms)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn fetch_terminal_result_optional(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<Option<TerminalResultRecord>> {
    let row = sqlx::query("SELECT task_id, encoded_result, terminal_status, return_peer_id, executor_peer_id, created_at_ms FROM task_terminal_results WHERE task_id = ?")
        .bind(task_id.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    row.map(row_to_terminal_result).transpose()
}

async fn fetch_artifacts_for_task_with_executor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &TaskId,
) -> StoreResult<Vec<ArtifactRecord>> {
    let rows = sqlx::query(
        "SELECT artifact_id, task_id, digest, media_type, byte_len, inline, created_at FROM artifacts WHERE task_id = ?",
    )
    .bind(task_id.as_str())
    .fetch_all(&mut **tx)
    .await?;
    rows.into_iter().map(row_to_artifact).collect()
}

async fn insert_origin_artifact(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &ArtifactRecord,
    content: &[u8],
) -> StoreResult<()> {
    sqlx::query(
        "INSERT INTO artifacts (artifact_id, task_id, digest, media_type, byte_len, inline, inline_blob, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(record.artifact_id.as_str())
    .bind(record.task_id.as_str())
    .bind(record.digest.as_str())
    .bind(record.media_type.as_str())
    .bind(record.byte_len as i64)
    .bind(i64::from(record.inline))
    .bind(record.inline.then_some(content))
    .bind(&record.created_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn row_to_terminal_result(row: sqlx::sqlite::SqliteRow) -> StoreResult<TerminalResultRecord> {
    Ok(TerminalResultRecord {
        task_id: TaskId::new(row.get::<String, _>("task_id"))?,
        encoded_result: row.get("encoded_result"),
        terminal_status: str_to_status(&row.get::<String, _>("terminal_status"))?,
        return_peer_id: row
            .try_get::<Option<String>, _>("return_peer_id")?
            .map(PeerId::new)
            .transpose()?,
        executor_peer_id: PeerId::new(row.get::<String, _>("executor_peer_id"))?,
        created_at_ms: row.get("created_at_ms"),
    })
}

fn row_to_outbox(row: sqlx::sqlite::SqliteRow) -> StoreResult<ResultOutboxRecord> {
    Ok(ResultOutboxRecord {
        delivery_id: row.get("delivery_id"),
        task_id: TaskId::new(row.get::<String, _>("task_id"))?,
        target_peer_id: PeerId::new(row.get::<String, _>("target_peer_id"))?,
        state: ResultDeliveryState::parse(&row.get::<String, _>("state"))?,
        attempt_count: row.get::<i64, _>("attempt_count").max(0) as u32,
        next_attempt_at_ms: row.get("next_attempt_at_ms"),
        lease_owner: row.try_get("lease_owner")?,
        lease_expires_at_ms: row.try_get("lease_expires_at_ms")?,
        last_error: row.try_get("last_error")?,
        created_at_ms: row.get("created_at_ms"),
        updated_at_ms: row.get("updated_at_ms"),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use tempfile::tempdir;

    fn peer(value: &str) -> PeerId {
        PeerId::new(value).unwrap()
    }

    fn result(task_id: &TaskId, return_peer: Option<&str>) -> TerminalResultRecord {
        TerminalResultRecord {
            task_id: task_id.clone(),
            encoded_result: b"terminal-result".to_vec(),
            terminal_status: TaskStatus::Completed,
            return_peer_id: return_peer.map(peer),
            executor_peer_id: peer("receiver-peer"),
            created_at_ms: 100,
        }
    }

    #[tokio::test]
    async fn sqlite_terminal_result_and_outbox_survive_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("keryx.db");
        let task_id = TaskId::new("result-task").unwrap();
        let worker = AgentId::new("worker").unwrap();
        let lease_id = LeaseId::new("lease-result-task").unwrap();
        {
            let store = SqliteStore::connect(&path).await.unwrap();
            store.migrate().await.unwrap();
            let task = TaskRecord::new(task_id.clone(), TaskStatus::Pending, None);
            store.accept_task(task).await.unwrap();
            store
                .lease_task(
                    &task_id,
                    LeaseRecord::new(lease_id.clone(), task_id.clone(), worker.clone(), 1, 1000),
                )
                .await
                .unwrap();
            store
                .complete_task_with_result(
                    &task_id,
                    &lease_id,
                    &worker,
                    result(&task_id, Some("sender-peer")),
                )
                .await
                .unwrap();
            store.close().await;
        }
        let store = SqliteStore::connect(&path).await.unwrap();
        store.migrate().await.unwrap();
        assert_eq!(
            store
                .get_terminal_result(&task_id)
                .await
                .unwrap()
                .encoded_result,
            b"terminal-result"
        );
        let pending = store.pending_result_deliveries(10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].target_peer_id, peer("sender-peer"));
    }

    #[tokio::test]
    async fn retrying_failure_does_not_create_terminal_result() {
        let dir = tempdir().unwrap();
        let store = SqliteStore::connect(dir.path().join("keryx.db"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let task_id = TaskId::new("retry-task").unwrap();
        let worker = AgentId::new("worker").unwrap();
        let lease_id = LeaseId::new("lease-retry-task").unwrap();
        store
            .accept_task(TaskRecord::new(task_id.clone(), TaskStatus::Pending, None))
            .await
            .unwrap();
        store
            .lease_task(
                &task_id,
                LeaseRecord::new(lease_id.clone(), task_id.clone(), worker.clone(), 1, 1000),
            )
            .await
            .unwrap();
        let policy = RetryPolicy::default();
        let updated = store
            .fail_task_with_result(
                &task_id,
                &lease_id,
                &worker,
                "retry me",
                &policy,
                TerminalResultRecord {
                    terminal_status: TaskStatus::Failed,
                    ..result(&task_id, Some("sender-peer"))
                },
            )
            .await
            .unwrap();
        if updated.status == TaskStatus::Pending {
            assert!(matches!(
                store.get_terminal_result(&task_id).await,
                Err(StoreError::TerminalResultNotFound(_))
            ));
        }
    }

    #[test]
    fn terminal_results_require_terminal_status() {
        let task_id = TaskId::new("not-terminal").unwrap();
        let mut value = result(&task_id, None);
        value.terminal_status = TaskStatus::Running;
        assert!(matches!(
            value.validate(),
            Err(StoreError::TerminalResultNotTerminal(_))
        ));
    }

    #[test]
    fn delivery_states_are_unique() {
        let states = [
            ResultDeliveryState::Pending,
            ResultDeliveryState::Leased,
            ResultDeliveryState::Delivered,
            ResultDeliveryState::DeadLettered,
        ];
        let labels = states
            .into_iter()
            .map(ResultDeliveryState::as_str)
            .collect::<HashSet<_>>();
        assert_eq!(labels.len(), 4);
    }
}
