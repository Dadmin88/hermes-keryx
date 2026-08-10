# Keryx worker loop

This document describes how workers interact with `keryxd` after Phases 6–7: gRPC task RPCs, lease semantics, retry policy, and dead-letter behavior. It complements [`lifecycle-store-daemon-semantics.md`](lifecycle-store-daemon-semantics.md).

## Overview

Workers do not mutate the store directly. They call the `KeryxDaemon` gRPC service (`proto/hermes/keryx/v1/daemon.proto`) while holding a lease token returned from claim.

Canonical persisted lifecycle remains four states: `pending`, `running`, `completed`, `failed`. Retries move a task back to `pending` with incremented `retry_count`; dead-lettering sets `failed` with `dead_lettered` metadata.

## Lifecycle sequence

```text
submit → claim → [heartbeat loop] → execute → complete | fail
```

| Step | RPC | Store effect |
| --- | --- | --- |
| Enqueue | `SubmitTask` | `accept_task` → `pending`, `TaskAccepted` |
| Acquire work | `ClaimTask` | `lease_task` → `running`, active lease, `TaskStarted` |
| Keep ownership | `Heartbeat` | `renew_lease` → extends `expires_at_ms`, status unchanged |
| Success | `CompleteTask` | `complete_task` → `completed`, lease cleared |
| Failure | `FailTask` | `fail_task` + `RetryPolicy` → requeue, dead-letter, or terminal `failed` |

### SubmitTask

- **Request:** `TaskEnvelope` with required `task_id`; optional `idempotency_key`.
- **Response:** `task_id`, `status` (typically `pending`).
- Duplicate compatible idempotency returns the existing task without a second accept event.

### ClaimTask

- **Request:** `task_id`, `worker_id`, optional `lease_duration_ms` (0 = daemon default TTL).
- **Response:** `lease_id`, `leased_at_ms`, `expires_at_ms`, `status`, `retry_count`, `dead_lettered`.
- Only one active lease per task; conflicting claims receive a lease conflict error.

### Heartbeat

- **Request:** `task_id`, `lease_id`, `worker_id`, optional `lease_duration_ms` (0 = default TTL).
- **Response:** updated `lease_id`, `expires_at_ms`.
- Renews from **now + duration**, not by adding to the previous expiry.

### CompleteTask

- **Request:** `task_id`, `lease_id`, `worker_id`, optional `duration_ms`, result metadata/artifacts.
- **Response:** `status` = `completed`.
- Requires matching active lease and `worker_id`.

### FailTask

- **Request:** `task_id`, `lease_id`, `worker_id`, `error_reason`, optional `duration_ms` and failure metadata.
- **Response:** `status` (`pending` if requeued, `failed` if terminal or dead-lettered), `retry_count`, `dead_lettered`.
- Retry behavior is governed by the daemon’s configured `RetryPolicy` (not per-request today).

## Lease semantics

| Setting | Default | Where |
| --- | --- | --- |
| Lease TTL when omitted | 300_000 ms (5 min) | `KeryxDaemonConfig::lease_default_ttl_ms` |
| Background stale-lease scan | every 30_000 ms | `lease_recovery_interval_ms` |

**TTL:** Pass `lease_duration_ms` on claim/heartbeat, or let the daemon apply the default.

**Renewal:** Workers should heartbeat before `expires_at_ms`. A common pattern is renewing at roughly half the TTL with jitter.

**Expiry:** Expiration is computed (`active` and `expires_at_ms <= now_ms`). Rows are not updated by a timer; startup recovery and the daemon’s periodic `recover_stale_leases` loop deactivate stale leases and requeue non-terminal `running` tasks to `pending` via `RecoveryAction`.

**Stale worker:** After recovery, the old `lease_id` cannot renew, complete, or fail the task. The worker must stop side effects or rely on idempotent work.

## Retry policy

Defined in `keryx_core::RetryPolicy`:

| Field | Default | Meaning |
| --- | --- | --- |
| `max_retries` | 3 | Max failure-driven requeues after incrementing `retry_count` |
| `backoff_ms` | 1_000 | Hint for worker sleep before reclaim (store does not schedule delays) |
| `dead_letter_after` | 4 | Attempt count at or above which the next failure dead-letters |

Helpers:

- `should_retry_after_failure(current_retry_count)` — if true, `fail_task` clears the lease, increments `retry_count`, sets `pending`, appends `RecoveryAction`.
- `should_dead_letter_after_failure(current_retry_count)` — if true (and retries exhausted), sets `failed`, `dead_lettered = true`, `dead_letter_reason`, appends `TaskDeadLettered`.
- `RetryPolicy::no_retries()` — first failure goes to `failed` without dead-letter metadata (legacy-style terminal fail).

Daemon configuration: `KeryxDaemonConfig::with_fail_retry_policy(...)` (see integration tests under `crates/keryx-daemon/tests/task_fail_retry.rs`).

## Error handling and dead-letter

| Outcome | Persisted status | Metadata |
| --- | --- | --- |
| Transient worker error, retries remain | `pending` | `retry_count` increased |
| Retries exhausted | `failed` | `dead_lettered = true`, `dead_letter_reason` |
| No retry policy (`max_retries = 0`) | `failed` | `retry_count` unchanged, not dead-lettered |
| Success | `completed` | lease cleared |

Cancellation, timeout, and rejection remain persisted as the strict four-state lifecycle value `failed`, while terminal result APIs and reattached handles expose their truthful typed outcomes (`canceled`, `timed_out`, or `rejected`). Worker `FailTask` uses `error_reason` and optional `failure_metadata`.

## CLI worker example

Start the daemon (listener is optional until `HERMES_KERYX_DAEMON_ADDR` is set):

```bash
export HERMES_KERYX_DATA_DIR="${PWD}/.keryx-data"
export HERMES_KERYX_DAEMON_ADDR=127.0.0.1:50051
cargo run -p keryx-daemon --bin keryxd
```

In another shell:

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051

cargo run -p keryx-cli --bin keryx -- task submit demo-task-1

cargo run -p keryx-cli --bin keryx -- task claim demo-task-1 \
  --worker worker-a --lease-duration-ms 120000
# Note lease_id from output

cargo run -p keryx-cli --bin keryx -- task heartbeat demo-task-1 \
  --lease '<lease_id>' --worker worker-a --lease-duration-ms 120000

cargo run -p keryx-cli --bin keryx -- task complete demo-task-1 \
  --lease '<lease_id>' --worker worker-a --duration-ms 5000
```

To exercise failure and retry (with daemon default policy):

```bash
cargo run -p keryx-cli --bin keryx -- task fail demo-task-1 \
  --lease '<lease_id>' --worker worker-a --reason transient
# status may be pending with retry_count=1; claim again to retry
```

## Related reading

- Store APIs: `lease_task`, `renew_lease`, `complete_task`, `fail_task` in `keryx-store`
- Legacy event ingestion: `keryx_core::legacy` and `accept_legacy_event` in the store
- Operator readiness: `keryx status`, `keryx doctor` (local runtime or via `HERMES_KERYX_DAEMON_ENDPOINT`)