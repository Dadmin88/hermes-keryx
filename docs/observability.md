# Hermes Keryx observability

Phase 8 adds structured tracing, in-process metrics, health RPC probes, and operator-facing status/doctor surfaces. This document describes how to configure logs, interpret spans, read metrics, and call health endpoints.

Related: [operations.md](operations.md) (startup/shutdown and troubleshooting), [lifecycle-store-daemon-semantics.md](lifecycle-store-daemon-semantics.md) (recovery semantics).

## Tracing

Keryx uses the Rust [`tracing`](https://docs.rs/tracing) ecosystem (`tracing` + `tracing-subscriber` in the workspace `Cargo.toml`, with `env-filter`, `fmt`, and `json` features enabled).

### Subscriber initialization (`keryxd`)

`keryxd` initializes logging in `crates/keryx-daemon/src/main.rs`:

```rust
tracing_subscriber::fmt().with_env_filter("info").init();
```

**Current behavior:** log lines are emitted at **INFO** and above through the default **human-readable** formatter. The filter is fixed to `info` in the binary entrypoint (it does not read `RUST_LOG` today).

**Operator tuning (conventional pattern):** to honor `RUST_LOG`, the subscriber would use `tracing_subscriber::EnvFilter::from_default_env()` (or `try_from_default_env()` with a fallback). Example levels:

```bash
# Illustrative — requires subscriber init that uses from_default_env()
export RUST_LOG=info,keryx_daemon=debug,keryx_store=debug
export RUST_LOG=keryx::rpc=trace
```

**JSON log lines:** the workspace builds `tracing-subscriber` with the `json` feature. Structured JSON output is available by switching the fmt layer to `.json()` (for example `.json().with_env_filter(...)`). That is not enabled in the stock `keryxd` `main` today; operators who need JSON should use a custom build or a log shipper that parses the default fmt output.

### Log fields and redaction

Structured events use stable `component` fields where applicable, for example:

| `component` | Source |
| --- | --- |
| `keryxd` | Daemon lifecycle (ready, listen, shutdown) |
| `health_loop` | Periodic store readiness probes |
| `lease_recovery_loop` | Background stale-lease recovery |

RPC store failures log via `store_error_to_status` with `error`, `grpc_code`, and message text derived from typed `StoreError` (not task payloads). Do not put secrets in task metadata; doctor/status paths avoid payload dumps.

### Span hierarchy

RPC handlers use explicit span names under `keryx::rpc::*`:

| Span name | RPC / role |
| --- | --- |
| `keryx::rpc::status` | `Status` |
| `keryx::rpc::doctor` | `Doctor` |
| `keryx::rpc::liveness` | `Liveness` |
| `keryx::rpc::readiness` | `Readiness` |
| `keryx::rpc::submit_task` | `SubmitTask` (field `task_id`) |
| `keryx::rpc::claim_task` | `ClaimTask` (fields `task_id`, `worker_id`) |
| `keryx::rpc::heartbeat` | `Heartbeat` (`task_id`, `lease_id`, `worker_id`) |
| `keryx::rpc::complete_task` | `CompleteTask` |
| `keryx::rpc::fail_task` | `FailTask` (+ `error_reason`) |

Background daemon work:

| Span name | Role |
| --- | --- |
| `keryx::daemon::health_tick` | Store probe tick (fields `ready`, `reason_count`) |
| `keryx::daemon::lease_recovery_tick` | Stale lease scan (fields `duration_ms`, `tasks_recovered`, `leases_cleaned`) |

Store operations invoked from RPC are wrapped with `#[instrument]` on `SqliteStore` methods. Span names follow the Rust function name (for example `accept_task`, `lease_task`, `renew_lease`, `complete_task`, `fail_task`, `recover_stale_leases`) and typically include `task_id` / `lease_id` fields. These nest under the active `keryx::rpc::*` span for a single request.

Typical request tree:

```text
keryx::rpc::claim_task
  └── lease_task (store)
```

Integration coverage: `crates/keryx-daemon/tests/tracing_instrumentation.rs`.

## Metrics

Metrics live in `keryx-observe` (`KeryxMetrics`). Counters and gauges are updated in the daemon RPC layer and lease recovery loop; they are **in-process** (not a Prometheus scrape endpoint yet).

### Snapshot fields

| Field | Type | Meaning |
| --- | --- | --- |
| `tasks_submitted` | counter | Successful `SubmitTask` accept paths |
| `tasks_claimed` | counter | Successful `ClaimTask` (also bumps `active_leases`) |
| `tasks_completed` | counter | Successful `CompleteTask` (decrements `active_leases`) |
| `tasks_failed` | counter | Successful `FailTask` (decrements `active_leases`) |
| `heartbeats` | counter | Successful `Heartbeat` renewals |
| `leases_recovered` | counter | Tasks moved off stale leases (startup + background recovery; each recovered task increments once) |
| `recovery_ticks` | counter | Background lease recovery loop iterations |
| `dead_letters` | counter | `FailTask` outcomes with `dead_lettered == true` |
| `active_leases` | gauge | In-flight leases tracked by metrics (claim +1, complete/fail/recovery −1) |

### Reading metrics

- **gRPC `Status`:** `StatusResponse` includes all snapshot fields (`tasks_submitted` … `dead_letters`, `active_leases`) plus startup recovery counts. See `proto/hermes/keryx/v1/daemon.proto`.
- **CLI `keryx status`:** when `HERMES_KERYX_DAEMON_ENDPOINT` is set, prints readiness and startup recovery; it does not yet print the Phase 8 counter block (use daemon `Status` RPC for full metrics).
- **Local CLI `keryx status`:** opens the store via `KeryxDaemonRuntime::startup` without a listener; metrics are zeroed for that ephemeral runtime.

Tests: `crates/keryx-observe/tests/metrics.rs`.

## Health and operator probes

`KeryxDaemon` gRPC (`proto/hermes/keryx/v1/daemon.proto`):

### Liveness

- **RPC:** `Liveness` → `LivenessResponse { alive }`
- **Semantics:** process is up and the RPC stack accepts the call. Returns `alive: true` when the handler runs.
- **Shutdown:** while shutting down, new RPCs (including liveness) receive `UNAVAILABLE` (`daemon is shutting down`) via `RpcInFlightGuard`.
- **Use:** orchestrstrator “is the process running?” checks. Does not validate SQLite.

### Readiness

- **RPC:** `Readiness` → `ReadinessResponse { ready, not_ready_reasons }`
- **Semantics:** reflects the cached `DynamicReadiness` snapshot updated at startup (initially ready after successful startup recovery) and on each **health loop** tick (default every 60s).
- **Probe logic** (`probe_store_readiness` in `health_loop.rs`):
  - Schema version must equal `CURRENT_SCHEMA_VERSION`.
  - `recover_stale_leases` must succeed with `corruption_count == 0`.
  - Failures append human-readable strings to `not_ready_reasons`.
- **Use:** load balancers and deploy gates (“may this instance take task traffic?”). Pair with startup doctor for bootstrap.

### Status

- **RPC:** `Status` → rich runtime report (paths, schema, startup recovery duration/counts, store kind, **metrics**).
- **CLI:** `keryx status` (local or via `HERMES_KERYX_DAEMON_ENDPOINT`).
- **`status` string:** `ready` when `daemon_ready` is true in the embedded report (post-startup gate).

### Doctor

- **RPC:** `Doctor` → `DoctorResponse { status, messages }` where `status` is `pass` or `fail`.
- **CLI:** `keryx doctor` (local or daemon-backed).
- **Named checks** (local `KeryxDoctorReport`):

| Check | Pass condition |
| --- | --- |
| `data_dir` | Data directory exists |
| `sqlite_store` | Store marked ready and `keryx.db` file present |
| `schema_version` | Applied schema equals supported version |
| `startup_recovery` | `corruption_count == 0` (includes recovered/cleaned counts in detail) |

`event_log_consistency` is planned in semantics docs but not a separate doctor check yet.

### gRPC examples

With `grpcurl` (daemon listening on loopback):

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051
# Use your proto import path and reflection if enabled.

grpcurl -plaintext 127.0.0.1:50051 hermes.keryx.v1.KeryxDaemon/Liveness
grpcurl -plaintext 127.0.0.1:50051 hermes.keryx.v1.KeryxDaemon/Readiness
grpcurl -plaintext 127.0.0.1:50051 hermes.keryx.v1.KeryxDaemon/Status
grpcurl -plaintext 127.0.0.1:50051 hermes.keryx.v1.KeryxDaemon/Doctor
```

CLI equivalents:

```bash
cargo run -p keryx-cli --bin keryx -- status
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 \
  cargo run -p keryx-cli --bin keryx -- doctor
```

Tests: `crates/keryx-daemon/tests/health_probes.rs`, `crates/keryx-daemon/tests/graceful_shutdown.rs`.