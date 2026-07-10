//! Durable authenticated task context, terminal results, and result-delivery outbox.

use keryx_core::{AgentId, KeryxEventType, LeaseId, PeerId, RetryPolicy, TaskId, TaskStatus};
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

impl InMemoryStore {
    pub fn accept_task_with_envelope_and_context(
        &self,
        task: TaskRecord,
        envelope: TaskEnvelopeRecord,
        context: TaskTransportContextRecord,
    ) -> StoreResult<TaskRecord> {
        ensure_context_task(&context, task.task_id())?;
        let accepted = self.accept_task_with_envelope(task, envelope)?;
        let mut state = self.lock()?;
        match state.transport_contexts.get(accepted.task_id()) {
            Some(existing) if existing == &context => return Ok(accepted),
            Some(_) => {
                return Err(StoreError::TransportContextConflict(
                    accepted.task_id().clone(),
                ))
            }
            None => {}
        }
        state
            .transport_contexts
            .insert(accepted.task_id().clone(), context);
        Ok(accepted)
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
                && existing_envelope.as_ref() == Some(&envelope)
                && existing_context.as_ref() == Some(&context)
            {
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
        now_ms: i64,
    ) -> StoreResult<()> {
        let changed = sqlx::query("UPDATE result_outbox SET state = 'delivered', lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ? WHERE delivery_id = ? AND state = 'leased' AND lease_owner = ?")
            .bind(now_ms)
            .bind(delivery_id)
            .bind(worker_id)
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
        worker_id: &str,
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
        let changed = sqlx::query("UPDATE result_outbox SET state = ?, attempt_count = attempt_count + 1, next_attempt_at_ms = ?, lease_owner = NULL, lease_expires_at_ms = NULL, last_error = ?, updated_at_ms = ? WHERE delivery_id = ? AND state = 'leased' AND lease_owner = ?")
            .bind(state)
            .bind(retry_at_ms)
            .bind(error)
            .bind(now_ms)
            .bind(delivery_id)
            .bind(worker_id)
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
        result.validate()?;
        if &result.executor_peer_id != authenticated_executor_peer_id {
            return Err(StoreError::RemoteResultExecutorMismatch {
                task_id: result.task_id.clone(),
                expected: authenticated_executor_peer_id.clone(),
                actual: result.executor_peer_id.clone(),
            });
        }
        let mut tx = self.pool.begin().await?;
        if let Some(existing) = fetch_terminal_result_optional(&mut tx, &result.task_id).await? {
            if existing == result {
                let task = fetch_task_with_executor(&mut tx, &result.task_id).await?;
                tx.commit().await?;
                return Ok(task);
            }
            return Err(StoreError::TerminalResultConflict(result.task_id.clone()));
        }
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
        let mut task = fetch_task_with_executor(&mut tx, &result.task_id).await?;
        if task.status == TaskStatus::Pending {
            let sequence = next_sequence_with_executor(&mut tx, &result.task_id).await?;
            update_task_status_with_executor(&mut tx, &result.task_id, TaskStatus::Running).await?;
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
        update_task_status_with_executor(&mut tx, &result.task_id, result.terminal_status).await?;
        insert_event(
            &mut tx,
            &result.task_id,
            sequence,
            transition.event_type,
            Some(transition.from),
            transition.to,
        )
        .await?;
        insert_terminal_result_only(&mut tx, &result).await?;
        tx.commit().await?;
        self.get_task(&result.task_id).await
    }
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
