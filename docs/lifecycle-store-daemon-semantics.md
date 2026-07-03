# Keryx Lifecycle Store/Daemon Semantics

## 1. Executive summary / decision record

Phase 1 decision: Keryx store and daemon code must adopt the strict public `keryx-core` lifecycle as the canonical persisted lifecycle contract:

```text
Pending -> Running -> Completed | Failed
```

Phase 6–7 adds worker RPCs and failure retries without new persisted lifecycle states. Retries return to `Pending`; exhausted retries dead-letter into `Failed` with `dead_lettered` metadata (see [worker-loop.md](worker-loop.md)).

```text
                         claim (ClaimTask / lease_task)
              ┌──────────────────────────────────────────┐
              ▼                                          │
         ┌─────────┐   complete (CompleteTask)      ┌───────────┐
         │ Pending │───────────────────────────────►│ Completed │
         └────┬────┘                                └───────────┘
              │                                          ▲
              │ claim                                    │ complete
              ▼                                          │
         ┌─────────┐   fail + RetryPolicy          ┌────┴──────┐
         │ Running │── should_retry ──────────────►│  Pending  │ (retry_count++)
         └────┬────┘                                └───────────┘
              │ fail (no retry / dead-letter)
              ▼
         ┌─────────┐
         │ Failed  │  (dead_lettered + reason when policy exhausts retries)
         └─────────┘

Stale lease on Running ── recover_stale_leases ──► Pending (RecoveryAction)
```

The store may keep operational metadata, including leases, retry counters, worker identity, timestamps, and recovery notes, but that metadata must not introduce extra lifecycle states. In particular, `leased`, `queued`, `awaiting_input`, `timed_out`, `canceled`, `rejected`, and `dead_lettered` are not canonical persisted `TaskStatus` values after this phase. Legacy values may be read through a compatibility adapter, but new writes must use only `pending`, `running`, `completed`, or `failed`.

Decisions:

- `TaskStatus` remains the lifecycle axis; leases are an ownership/processing metadata axis.
- Lease acquisition is the canonical transition from `Pending` to `Running` for daemon-dispatched work.
- Completion and failure are the only terminal lifecycle outcomes for this strict phase.
- Store APIs must validate lifecycle transitions through `keryx-core` and surface illegal lifecycle moves as validation-style errors.
- Terminal snapshots are immutable. Store/daemon recovery may clean up stale metadata around terminal tasks, but may not change `Completed` or `Failed` lifecycle state.
- Daemon startup must run migrations and deterministic stale-lease recovery before reporting readiness.
- Recovery actions are durable and observable. They must append `RecoveryAction` events and be reflected in status/doctor logs or metrics.

This document intentionally replaces the older broad RFC lifecycle vocabulary at the store/daemon boundary for the next implementation slice. Future richer states must be modeled as additional metadata or an explicit new core lifecycle RFC, not ad hoc store strings.

## 2. Canonical persisted task state model

Canonical task snapshot fields for the local store:

| Field | Required | Meaning | Notes |
| --- | --- | --- | --- |
| `task_id` | yes | Stable task identity | Validated by `TaskId`. |
| `status` | yes | Canonical lifecycle state | One of `pending`, `running`, `completed`, `failed`. |
| `idempotency_key` | no | Dispatch retry identity | Unique when present. |
| `created_at` | yes | Snapshot creation time | Store-owned timestamp. |
| `updated_at` | yes | Last snapshot mutation time | Must update on lifecycle transition and relevant metadata mutation. |
| `retry_count` | yes (Phase 7) | Failure-driven requeue attempts | Incremented on retry requeue and dead-letter; surfaced on `ClaimTask` / `FailTask` RPC responses. |
| `dead_lettered` | yes (Phase 7) | Task exhausted retry policy | Lifecycle status remains `failed`; not a fifth `TaskStatus` variant. |
| `dead_letter_reason` | no | Human-readable dead-letter cause | Set when `fail_task` dead-letters. |

Canonical lifecycle states:

| State | Description | Legal outgoing lifecycle transitions |
| --- | --- | --- |
| `Pending` | Accepted and durable, not currently owned by a worker lease. | `Running` only. |
| `Running` | A worker/daemon has active ownership or the task is considered in execution. | `Completed` or `Failed`. |
| `Completed` | Successful terminal outcome. | None. |
| `Failed` | Unsuccessful terminal outcome, including canceled, timed out, dead-lettered, rejected, or unrecoverable failure in this strict phase. | None. |

Event-log model:

- Every accepted task has an initial durable event (`TaskAccepted`) with `from_status = null` and `to_status = Pending`.
- Every legal lifecycle transition appends one task event in the same transaction as the snapshot mutation.
- Event sequence is per task and strictly monotonically increasing without gaps.
- Replayed latest lifecycle state must equal the task snapshot status. Mismatch is corruption and must surface through doctor/recovery rather than being silently accepted.
- `RecoveryAction` is an operational event that may have `from_status = Running` and `to_status = Pending` for stale-lease recovery. It is the only allowed store-level recovery mutation that moves a non-terminal task backward in the snapshot; it is not a normal public lifecycle transition and must be invoked only by explicit recovery APIs.

## 3. Lifecycle state vs operational metadata, especially leases

`TaskStatus` answers: "Where is this task in the public lifecycle?"

Lease metadata answers: "Who currently owns execution rights, and until when?"

They are separate axes:

| Concept | Lifecycle field? | Operational metadata? | Example |
| --- | --- | --- | --- |
| Accepted but not executing | yes | optional | `status = Pending`, no active lease. |
| Worker owns execution rights | yes | yes | `status = Running`, active lease row. |
| Worker still alive | no | yes | Lease renewal updates `expires_at_ms`; status remains `Running`. |
| Worker disappeared | no | yes | Lease expires; recovery clears active lease and returns recoverable task to `Pending`. |
| Task succeeded | yes | optional cleanup | `status = Completed`; active lease must be cleared/deactivated. |
| Task failed/canceled/timed out | yes | optional failure metadata | `status = Failed`; reason/cause/deadline metadata carries specifics. |

Required lease fields:

| Field | Meaning |
| --- | --- |
| `lease_id` | Unique ownership token. Completion/failure/renewal should require this token once runtime APIs exist. |
| `task_id` | Owned task. At most one active lease per task. |
| `worker_id` or `owner_id` | Runtime worker/agent identity. Add in Phase 3 if absent. |
| `leased_at_ms` | Acquisition timestamp from store/daemon clock. |
| `expires_at_ms` | Expiration timestamp. Must be greater than `leased_at_ms`. |
| `active` | Active lease marker. Only one active lease per task. |
| `fencing_token` or monotonically increasing generation | Optional but recommended before multi-worker concurrency. Prevents stale workers from completing after a newer lease. |

Rules:

- New code must not persist `status = leased`; use `status = running` plus an active lease.
- Lease renewal never changes lifecycle status.
- Lease expiration is not a lifecycle status. It is a recovery condition.
- Retry budget, failure reason, deadline, cancellation reason, and dead-letter reason belong in metadata/events, not `TaskStatus`, unless/until core expands the lifecycle.

## 4. Store transition API semantics

The store should expose narrow operations with explicit validation boundaries rather than a generic mutable status setter for all callers.

Recommended API shape:

```text
accept_task(task_id, idempotency_key, metadata) -> TaskRecord
lease_task(task_id, lease_request, now_ms) -> LeasedTaskRecord
renew_lease(task_id, lease_id, now_ms, extend_until_ms) -> LeaseRecord
complete_task(task_id, lease_id, completion_metadata) -> TaskRecord
fail_task(task_id, lease_id, failure_metadata) -> TaskRecord
recover_stale_leases(now_ms, limit) -> RecoveryReport
get_task(task_id) -> TaskRecord
events_for_task(task_id) -> Vec<TaskEventRecord>
replay_task(task_id) -> TaskRecord
```

`transition_task(task_id, to)` may remain as an internal/test helper, but production daemon paths should call intent-specific APIs so the store can enforce lease ownership, terminal immutability, idempotency, and observability.

Operation semantics:

### `accept_task`

- Input status must be absent or `Pending`.
- Writes task snapshot, idempotency row, and `TaskAccepted` in one transaction.
- Compatible duplicate idempotency returns the existing task without appending another event.
- Conflicting duplicate idempotency returns `StoreError::IdempotencyConflict`.

### `lease_task`

- Legal only when task snapshot is `Pending` and no active lease exists for the task.
- Validates `Pending -> Running` through `keryx-core`.
- Writes/activates lease, updates task status to `Running`, and appends lifecycle event in one transaction.
- The event emitted by strict core for `Pending -> Running` is currently `TaskStarted`. If Keryx wants a separate `TaskLeased` audit event, append it as an operational event in addition to, not instead of, the core lifecycle event, or rename semantics deliberately in core.
- If the task is already `Running` with an active unexpired lease, return a lease-conflict/busy error.
- If the task is terminal, return validation-style terminal transition error.

### `renew_lease`

- Requires matching active `lease_id` for `task_id`.
- Requires task snapshot `Running`.
- Requires `new_expires_at_ms > now_ms` and normally `new_expires_at_ms > current_expires_at_ms`.
- Mutates only lease metadata, not `TaskStatus`.
- Appends an optional operational event/observe record if event volume is acceptable; otherwise emits structured tracing/metrics.

### `complete_task`

- Requires task snapshot `Running`.
- Requires matching active lease unless an explicit admin/recovery completion path is introduced.
- Validates `Running -> Completed` through `keryx-core`.
- Updates snapshot to `Completed`, deactivates active lease, persists completion metadata/artifact metadata, and appends `TaskCompleted` in one transaction.
- Duplicate compatible completion should return the existing `Completed` record once completion idempotency keys are introduced. Without such a key, duplicate completion after terminal state must not append another event.

### `fail_task`

- Requires task snapshot `Running`.
- Requires matching active lease unless an explicit recovery failure path is introduced.
- Accepts `RetryPolicy` (Phase 7). When `max_retries > 0` and `should_retry_after_failure` holds, deactivates lease, increments `retry_count`, sets snapshot to `Pending`, and appends `RecoveryAction` (failure-driven requeue, not a core `Running -> Pending` transition).
- When retries are exhausted, validates `Running -> Failed`, sets `dead_lettered` and `dead_letter_reason`, deactivates lease, and appends `TaskDeadLettered`.
- When `max_retries == 0`, validates `Running -> Failed` immediately without dead-letter metadata (terminal fail).
- Cancellation, timeout, rejection, and dead-letter at the API boundary are still represented as `Failed` plus typed reason metadata for this strict phase when those paths are used.

### `recover_stale_leases`

- Requires explicit daemon/store recovery caller.
- Finds active leases with `expires_at_ms <= now_ms`.
- Applies `limit` after deterministic ordering by `expires_at_ms ASC, task_id ASC`; `None` means recover all currently stale active leases.
- For non-terminal `Running` tasks, deactivates lease, updates snapshot to `Pending`, and appends `RecoveryAction` in one transaction.
- For `Pending` tasks with an active stale lease, deactivates lease and appends `RecoveryAction` if the cleanup changes observable ownership metadata; status remains `Pending`.
- For terminal tasks, preserves lifecycle state and deactivates stale active lease metadata. Do not append a lifecycle transition; append/emit a cleanup recovery record if needed for audit.
- Must be deterministic: ordered by `expires_at_ms`, then `task_id`, with a configurable limit for large stores.
- Returns a typed `RecoveryReport` with `recovered_tasks`, `cleaned_terminal_leases`, and `corrupted_tasks`; convenience counts are derived from those fields so daemon status/doctor can report recovery without parsing event payloads.

## 5. Lease acquisition, renewal, expiration, stale cleanup, and recovery semantics

### Acquisition

A lease is an exclusive, time-bounded execution right.

Acceptance criteria for acquisition implementation:

- Atomic compare-and-set behavior: only one concurrent caller can move a given task from `Pending` to `Running` and create the active lease.
- SQLite should enforce active uniqueness through transaction logic and, ideally, an index/constraint for one active lease per task.
- Acquisition returns the task snapshot and lease token needed for renewal/completion.
- Acquisition must reject terminal tasks with a validation-style terminal transition error.
- Acquisition must reject non-expired active leases with a typed store conflict.

### Renewal

A renewal extends ownership without changing lifecycle state.

Acceptance criteria:

- Matching `lease_id` required.
- Expired leases cannot be renewed after recovery has deactivated them.
- A stale worker with an old lease token cannot renew after another lease generation exists.
- Renewal updates `expires_at_ms`, `updated_at`, and observability counters/logs.

### Expiration

Expiration is computed, not passively written by a timer.

- A lease is expired when `active = true AND expires_at_ms <= now_ms`.
- Expiration alone does not mutate rows. Mutation happens in `recover_stale_leases` or an equivalent explicit cleanup transaction.
- Store code must accept `now_ms` as an argument for deterministic tests.

### Stale cleanup

Cleanup should handle inconsistent metadata safely:

- Active lease for missing task: deactivate lease and report store corruption/recovery warning.
- Active lease for terminal task: deactivate lease; preserve terminal status.
- Active lease for `Pending` task: deactivate lease; preserve `Pending`; report cleanup.
- Multiple active leases for one task: choose no winner silently. Deactivate all expired leases; if multiple unexpired active leases exist, doctor should fail and recovery should require a deterministic repair policy.

### Recovery

Recovery is intentionally a store/daemon operational action rather than a normal core lifecycle transition.

- Recoverable non-terminal stale work returns to `Pending` so another worker can lease it.
- Recovery must append durable `RecoveryAction` rows for per-task audit (stale lease requeue and failure-driven retry requeue). `retry_count`, `dead_lettered`, and `dead_letter_reason` live on the task snapshot (schema migration v3).
- `RecoveryReport` is the startup/status summary surface: `recovered_tasks` preserves the previous `Vec<TaskRecord>` caller data, `cleaned_terminal_leases` counts stale terminal lease metadata removed without status changes, and `corrupted_tasks` lists event-log/snapshot mismatches in deterministic `task_id` order.
- Repeated lease expiry without worker failure still requeues via `RecoveryAction`; failure-driven retries increment `retry_count` and are bounded by daemon `RetryPolicy`.

## 6. Daemon startup recovery behavior

Daemon startup sequence:

1. Resolve data directory and database path.
2. Create data directory if needed.
3. Open SQLite store.
4. Run forward-only migrations.
5. Verify schema version is supported.
6. Run deterministic startup recovery:
   - deactivate stale leases;
   - requeue recoverable `Running` tasks to `Pending` with `RecoveryAction`;
   - preserve terminal tasks;
   - surface corrupt/mismatched event streams.
7. Build startup report with schema version, db path, recovered count, cleanup count, corruption count, and warnings.
8. Only after recovery completes, expose daemon readiness/status/doctor and accept RPC/task work.

Startup must be fail-closed for store integrity:

- Migration failure: daemon not ready; return startup error.
- Corrupt event stream or unsupported schema: daemon not ready unless a documented read-only degraded mode exists.
- Recovery transaction failure: daemon not ready; retry startup should be safe.
- Partial recovery must not be acknowledged as ready.

Status/doctor requirements:

- `status` should indicate ready/not-ready and include recovered counts in local CLI output.
- `doctor` should include named checks for `data_dir`, `sqlite_store`, `schema_version`, `startup_recovery`, and eventually `event_log_consistency`.
- Startup recovery must complete before `daemon_ready = true`.

## 7. Runtime worker/lease recovery behavior

Phase 6 exposes the worker loop as gRPC on `KeryxDaemon` (`proto/hermes/keryx/v1/daemon.proto`). Each RPC maps to store intent-specific APIs:

| RPC | Store operation | Notes |
| --- | --- | --- |
| `SubmitTask` | `accept_task` | Creates `pending` task from envelope `task_id` / idempotency |
| `ClaimTask` | `lease_task` | `Pending -> Running`; returns `lease_id`, TTL, `retry_count`, `dead_lettered` |
| `Heartbeat` | `renew_lease` | Extends lease; default TTL 300s when `lease_duration_ms` is 0 |
| `CompleteTask` | `complete_task` | Requires lease + worker match |
| `FailTask` | `fail_task` + daemon `RetryPolicy` | Requeue, dead-letter, or terminal fail; response includes `retry_count`, `dead_lettered` |

Operator CLI: `keryx task submit|claim|heartbeat|complete|fail` requires `HERMES_KERYX_DAEMON_ENDPOINT`. See [worker-loop.md](worker-loop.md).

Runtime behavior after startup:

- Workers receive tasks only after the store persists the lease and `Running` snapshot.
- Workers must renew leases periodically before expiration. Recommended interval: renew at 50% of lease TTL with jitter; fail local work if renewal fails after bounded retry.
- Completion/failure must include `lease_id`; the store must reject stale or mismatched lease ids.
- If a worker loses its lease, it must stop producing side effects or must rely on idempotent side-effect fencing outside Keryx.
- If a worker crashes, no immediate lifecycle mutation is required. The task remains `Running` until stale-lease recovery returns it to `Pending` or a future retry policy marks it `Failed`.
- The daemon may run periodic stale-lease recovery in addition to startup recovery. Periodic recovery must use the same store primitive and observability model as startup recovery.

Recommended runtime loop:

1. Poll/claim next `Pending` task via `lease_task`.
2. Deliver task and lease token to worker.
3. Start renewal loop.
4. On successful result, call `complete_task` with lease token.
5. On worker-reported failure, call `fail_task` with lease token and failure reason.
6. On renewal failure or lease loss, stop the worker if possible and let recovery decide requeue/failure.

## 8. Terminal immutability rules for `Completed` and `Failed` tasks

Terminal lifecycle state is immutable:

- `Completed -> *` is illegal.
- `Failed -> *` is illegal.
- `Completed -> Completed` and `Failed -> Failed` must not append duplicate lifecycle events. Compatible duplicate terminal acknowledgements may return the existing record through an explicit idempotent API.
- Lease acquisition on terminal tasks is illegal.
- Lease renewal on terminal tasks is illegal.
- Startup/runtime recovery must never move terminal tasks back to `Pending` or `Running`.

Allowed terminal-adjacent metadata changes:

- Deactivate/cleanup stale active lease rows associated with terminal tasks.
- Add immutable audit/observation records that do not alter lifecycle status.
- Attach completion artifacts or failure details only if the terminal write transaction has not already closed, or through a future explicitly versioned metadata append contract.

Validation-style errors:

- Any attempted terminal lifecycle transition should map to `ValidationError::TerminalTaskTransition` through `keryx-core`.
- Store and daemon layers may wrap this in their own error type, but callers must be able to distinguish validation/contract violation from I/O, lock, database, and availability failures.

## 9. Legacy state / migration compatibility policy

Existing docs and earlier prototypes referenced states such as `created`, `accepted`, `queued`, `leased`, `awaiting_input`, `canceled`, `timed_out`, `rejected`, and `dead_lettered`. Compatibility policy:

Read compatibility:

| Legacy value | Canonical read status | Metadata/event note |
| --- | --- | --- |
| `created`, `accepted`, `queued`, `awaiting_approval`, `pending` | `Pending` | Preserve original event type when replaying historical event rows. |
| `leased`, `awaiting_input`, `running` | `Running` | Lease/awaiting details become operational metadata. |
| `completed` | `Completed` | Terminal success. |
| `failed`, `canceled`, `timed_out`, `rejected`, `dead_lettered` | `Failed` | Preserve reason as failure metadata when possible. |

SQLite snapshot reads use `keryx-store` `str_to_status` for the table above. Unknown status strings fail load with a typed database error.

Legacy **event** normalization (`keryx-core::legacy`):

- Lifecycle collapse: e.g. `TaskLeased` / `TaskStarted` from `Pending` → canonical `TaskStarted` to `Running`; `TaskCanceled`, `TaskTimedOut`, `TaskDeadLettered` from `Running` → `TaskFailed` to `Failed`.
- Operational events (`TaskQueued`, approval events, `TaskAwaitingInput`) append without changing canonical status; validated via `is_valid_operational_legacy`.
- Store `accept_legacy_event` / replay uses `normalize_legacy_transition` and rejects unknown `(status, event)` pairs.

Write policy:

- New migrations and store writes must emit only canonical status strings in the `tasks.status`, `task_events.from_status`, and `task_events.to_status` lifecycle columns.
- Legacy values may remain accepted by `str_to_status` while migrating old stores.
- A future migration should normalize task snapshots to canonical strings. Event rows may either be normalized or preserved with replay compatibility; choose preservation if event audit fidelity is more important.
- If a legacy value cannot be mapped safely, startup must fail doctor with a migration/compatibility error instead of guessing.

Documentation cleanup:

- Existing architecture docs that say stale leases return tasks to `Queued` should be updated in a follow-up docs task to say `Pending` under the strict lifecycle.
- Existing transaction docs that say `TaskLeased` should be reconciled with current core behavior (`TaskStarted` for `Pending -> Running`) or core should be deliberately extended.

## 10. Error boundary model: core validation vs store/daemon errors

Error categories:

| Layer | Category | Examples | Caller handling |
| --- | --- | --- | --- |
| `keryx-core` | Validation/contract | invalid id, invalid transition, terminal transition | 4xx-style caller error; do not retry without changing request. |
| Store | Not found/conflict | missing task, duplicate task id, idempotency conflict, active lease conflict, stale lease token | Caller may retry lookup or return conflict. |
| Store | Persistence/integrity | SQLite error, lock poisoned, corrupt event stream, unsupported schema | Operational failure; retry startup or escalate doctor. |
| Daemon | Availability/startup | store unavailable, migration failed, recovery failed, not ready | Report not-ready; clients retry later. |
| Daemon | Runtime ownership | renewal failed, worker lost lease, completion rejected for stale token | Stop/abort worker side effects and recover via store. |

Rules:

- Lifecycle transition validation must call `keryx-core` helpers (`validate_transition` or equivalent) rather than duplicating transition tables in store/daemon code.
- `StoreError::Validation` should wrap core `ValidationError` for lifecycle and identifier validation.
- Store-specific conflicts should not be collapsed into database strings. Add typed variants such as `ActiveLeaseExists`, `LeaseTokenMismatch`, `LeaseExpired`, `UnsupportedSchema`, and `CorruptEventStream` as implementation advances.
- Daemon RPC/CLI should present validation errors differently from transient store availability failures.

## 11. Observability/logging requirements

Structured logs should exist for:

- task accepted: `task_id`, `idempotency_key_present`
- lease acquired: `task_id`, `lease_id`, `worker_id`, `expires_at_ms`
- lease renewed: `task_id`, `lease_id`, `old_expires_at_ms`, `new_expires_at_ms`
- lease renewal rejected: `task_id`, reason
- task completed/failed: `task_id`, `lease_id`, reason/result summary metadata keys only
- stale lease recovered: `task_id`, `lease_id`, `from_status`, `to_status`, `expires_at_ms`, `now_ms`
- terminal lease cleanup: `task_id`, `lease_id`, `terminal_status`
- startup recovery summary: `schema_version`, `recovered_tasks`, `cleaned_leases`, `corrupt_records`, `duration_ms`
- event replay mismatch/corruption: `task_id`, `expected_status`, `replayed_status`, sequence context

Metrics/counters should include:

- `keryx_store_task_accept_total`
- `keryx_store_lifecycle_transition_total{from,to}`
- `keryx_store_lifecycle_validation_error_total{kind}`
- `keryx_store_lease_acquire_total{result}`
- `keryx_store_lease_renew_total{result}`
- `keryx_store_lease_recovery_total{result}`
- `keryx_daemon_startup_recovery_duration_ms`
- `keryx_daemon_startup_recovered_tasks`
- `keryx_daemon_event_log_corruption_total`

Logging constraints:

- Do not log task payloads, secrets, or raw artifact contents.
- Include stable ids and enum reasons so tests and operators can assert behavior.
- Recovery events must be durable even when logs are unavailable.

## 12. Implementation task breakdown for Phases 2-8 with acceptance criteria

### Phase 2: Store API hardening

Goal: replace generic lifecycle writes in production paths with intent-specific store APIs.

Tasks:

- Add typed store errors for active lease conflict, lease token mismatch, lease expired, unsupported schema, and corruption.
- Add/adjust `complete_task`, `fail_task`, and `renew_lease` APIs for `InMemoryStore` and `SqliteStore`.
- Make `transition_task` internal/test-only or document it as unsafe for daemon production paths.
- Ensure all lifecycle mutations call `keryx-core` validation.

Acceptance criteria:

- Tests prove `Pending -> Running -> Completed` and `Pending -> Running -> Failed` succeed.
- Tests prove `Pending -> Completed`, `Pending -> Failed`, `Running -> Pending` through normal transition, and any terminal transition fail with validation errors.
- Tests prove duplicate/active lease conflicts are typed, not string database errors.

### Phase 3: Lease ownership and renewal contract

Goal: make leases enforce worker ownership and prevent stale completion.

Tasks:

- Add `worker_id`/`owner_id` and lease generation/fencing token to lease records or define why `lease_id` is sufficient for Phase 3.
- Implement `renew_lease` in memory and SQLite.
- Require matching lease token for complete/fail.
- Add indexes/constraints for one active lease per task.

Acceptance criteria:

- A stale lease token cannot renew, complete, or fail a task after cleanup/release.
- Renewal extends expiration without changing task status or appending lifecycle transitions.
- Concurrent lease attempts result in one winner.

### Phase 4: Recovery semantics and event-log consistency

Goal: make recovery deterministic, audited, and safe around terminal tasks.

Tasks:

- Expand `recover_stale_leases(now_ms, limit)` with deterministic ordering and cleanup counts.
- Deactivate stale leases for terminal tasks without changing terminal status.
- Add event replay consistency checks and corruption reporting.
- Decide where recovery metadata lives (`TaskEventRecord` payload vs separate recovery table).

Acceptance criteria:

- Stale `Running` leased tasks return to `Pending` with `RecoveryAction`.
- Terminal tasks remain terminal even with stale active leases.
- Recovery reports recovered task count, cleaned terminal lease count, and corruption count.
- Snapshot/event replay mismatch fails doctor or returns a typed corruption error.

### Phase 5: Daemon startup readiness gate

Goal: startup recovery must complete before daemon readiness.

Tasks:

- Extend `StartupReport`, `KeryxStatusReport`, and `KeryxDoctorReport` with recovery cleanup/corruption details.
- Fail startup on unsupported schema, migration failure, and unrepaired corruption.
- Add status/doctor output for schema and recovery results.

Acceptance criteria:

- Startup creates/migrates the SQLite store, runs recovery, and only then reports ready.
- Startup failure paths report not-ready/typed errors and do not accept work.
- CLI status/doctor includes recovered/cleaned/corruption counts without reading payloads.

### Phase 6: Runtime worker loop lease behavior — **implemented**

Goal: daemon runtime uses lease APIs correctly during work execution.

Tasks:

- Add runtime claim/deliver/renew/complete/fail loop around the store APIs.
- Add renewal interval configuration and bounded renewal retry.
- Stop or quarantine worker execution on lease loss.
- Add periodic stale-lease recovery using the same store primitive as startup.

Acceptance criteria:

- Worker receives task only after lease is durable.
- Completion/failure without valid lease token is rejected.
- Expired lease is recovered and can be re-leased by another worker.
- Periodic recovery and startup recovery produce the same durable semantics.

**Current:** gRPC `SubmitTask`, `ClaimTask`, `Heartbeat`, `CompleteTask`, `FailTask`; background lease recovery loop; `keryx task` CLI subcommands.

### Phase 7: Retry, dead-letter, and legacy migration compatibility — **implemented**

Goal: failure retries, dead-letter metadata, and safe read/replay of legacy stores.

Tasks:

- Keep read adapter for legacy status strings and legacy event normalization in `keryx-core::legacy`.
- Persist `retry_count`, `dead_lettered`, `dead_letter_reason` (schema v3).
- Apply `RetryPolicy` on `fail_task`; expose counts on claim/fail RPC responses.
- Run migration ordering so retry columns exist before legacy lease recovery reads them.

Acceptance criteria:

- Legacy snapshots map to canonical states according to the table in this document.
- New writes after migration use only canonical status strings.
- Unknown legacy values fail with typed compatibility error.
- Docs describe retry loop and RPC worker surface ([worker-loop.md](worker-loop.md)).

**Prior Phase 7 doc slice (legacy-only) merged above with retry/dead-letter deliverables.**

### Phase 8: Observability and operational hardening — **implemented**

Goal: make lifecycle/recovery behavior diagnosable in production.

Deliverables:

- Structured tracing: `keryx::rpc::*` RPC spans, store `#[instrument]` on lifecycle APIs, `keryx::daemon::health_tick` / `lease_recovery_tick`, structured store error logs on RPC.
- In-process metrics (`keryx-observe`): task counters, `active_leases` gauge, recovery/dead-letter counters; exposed on daemon `Status` RPC.
- Health probes: gRPC `Liveness`, `Readiness` (dynamic store probes), existing `Status` / `Doctor`.
- Graceful shutdown: in-flight RPC drain, gRPC `serve_with_incoming_shutdown`, store `close`.
- Operator docs: [observability.md](observability.md), [operations.md](operations.md).

Acceptance criteria:

- Logs include task/lease ids and enum reasons but not payloads/secrets.
- Metrics distinguish validation errors, conflicts, and database failures (via gRPC codes + structured error logs; counters for successful transitions).
- Doctor output gives actionable messages for stale lease cleanup and event-log corruption.

#### Observability (operator reference)

For tracing configuration, span names, metrics field reference, and health RPC semantics, see [observability.md](observability.md). For startup/shutdown order, health procedures, troubleshooting, and environment variables, see [operations.md](operations.md).

## 13. Open questions / explicit non-goals

Open questions:

- Should `TaskLeased` remain as an operational event separate from the core `TaskStarted` lifecycle event, or should core event naming change for `Pending -> Running`?
- Should stale lease recovery always return tasks to `Pending`, or should retry budget/deadline policy be introduced before requeueing repeatedly expired tasks?
- Where should structured event payloads live: extend `task_events`, add `task_event_payloads`, or introduce typed audit tables per domain?
- What is the minimum completion idempotency key contract for duplicate worker completion after acknowledgement uncertainty?
- What clock source should production daemon use for lease timing, and how should clock skew be handled when workers run outside the daemon process?
- When multiple active unexpired leases exist due to store corruption, should recovery fail closed or choose the newest fencing token? This spec recommends fail-closed until a deterministic policy is approved.

Explicit non-goals for this design phase:

- No Rust source or test changes.
- No production deployment or migration execution.
- No expansion of `keryx-core` beyond the strict four-state lifecycle.
- No AgentAnycast coupling.
- No distributed relay/PostgreSQL lease design beyond preserving the same conceptual boundaries.
- No payload schema for task inputs, outputs, artifacts, or failure details beyond noting where metadata belongs.
