# Hermes Keryx operations

Operator runbook for `keryxd`, the `keryx` CLI, health checks, graceful shutdown, and common failure modes.

See also: [observability.md](observability.md) (tracing, metrics, probe RPCs), [worker-loop.md](worker-loop.md) (task RPC flow).

## Startup sequence

### 1. Configuration

Minimum for a listening daemon:

```bash
export HERMES_KERYX_DATA_DIR="${PWD}/.keryx-data"   # optional; default .keryx
export HERMES_KERYX_DAEMON_ADDR=127.0.0.1:50051      # loopback only
```

`HERMES_KERYX_DAEMON_ADDR` must parse to a **loopback** address (`127.0.0.1` or `[::1]`). Wildcard or public IPs are rejected at startup.

Clients (CLI task commands, remote status):

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051
```

### 2. Process bootstrap (`keryxd`)

Order of operations in `KeryxDaemonRuntime::startup`:

1. Create `HERMES_KERYX_DATA_DIR` if missing.
2. Open SQLite at `{data_dir}/keryx.db`.
3. Run migrations (`migrate`).
4. Read `schema_version`.
5. Run **startup** `recover_stale_leases` (fail-closed if unrepaired corruption).
6. Build `StartupReport` (recovery counts, duration).

If step 5 finds corruption that cannot be repaired, startup returns `StoreError::UnrepairedCorruption` and the process exits without serving RPCs.

### 3. Expected log output (INFO)

After successful startup:

```text
INFO ... component="keryxd" db_path=... schema_version=... recovered_tasks=... cleaned_terminal_leases=... corruption_count=... Hermes Keryx daemon runtime ready
```

When `HERMES_KERYX_DAEMON_ADDR` is set:

```text
INFO ... component="keryxd" lease_recovery_interval_ms=30000 health_check_interval_ms=60000 Hermes Keryx background loops started
INFO ... component="keryxd" listen_addr=127.0.0.1:50051 Hermes Keryx daemon RPC service listening
```

Background loops (defaults from `keryx-daemon`):

| Loop | Default interval | Purpose |
| --- | --- | --- |
| Lease recovery | 30s | `recover_stale_leases` for expired leases |
| Health | 60s | Refresh `Readiness` snapshot via `probe_store_readiness` |

### 4. Modes without a listener

If `HERMES_KERYX_DAEMON_ADDR` is unset or empty, `keryxd` performs startup recovery and logs **ready**, then **exits** (no gRPC, no background loops). Use this for one-shot migration/recovery validation, or prefer `keryx status` / `keryx doctor` for operator checks without keeping a daemon running.

### 5. Operator verification

Local (embedded runtime, no daemon):

```bash
cargo run -p keryx-cli --bin keryx -- status
cargo run -p keryx-cli --bin keryx -- doctor
```

Against a running daemon:

```bash
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 \
  cargo run -p keryx-cli --bin keryx -- status
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 \
  cargo run -p keryx-cli --bin keryx -- doctor
```

Expect `keryx status: ready` and `keryx doctor: pass` when the store and startup recovery are healthy.

## Graceful shutdown sequence

Triggered by **SIGINT** (`Ctrl+C`) in `keryxd` when the RPC listener is active.

1. Log `shutdown signal received`.
2. Stop **lease recovery** loop (wait for current tick).
3. Stop **health** loop.
4. `KeryxDaemonRuntime::shutdown`:
   - `initiate_shutdown`: set shutting-down flag and signal gRPC `serve_with_incoming_shutdown`.
   - Drain in-flight RPCs up to **shutdown timeout** (default 30s, `DEFAULT_SHUTDOWN_TIMEOUT_MS`).
   - `SqliteStore::close()`.
   - Log `daemon shutdown complete` with `duration_ms` and remaining in-flight count (may be > 0 if drain timed out).
5. Await gRPC server task completion.

**In-flight RPC behavior:** `RpcInFlightGuard` rejects **new** RPCs with gRPC `UNAVAILABLE` / `daemon is shutting down` once shutdown has started. Calls already in progress are allowed to finish until the drain timeout.

**Task traffic during shutdown:** workers should treat `UNAVAILABLE` as transient and retry against another instance or after restart; do not assume partial completion.

Integration tests: `crates/keryx-daemon/tests/graceful_shutdown.rs` (optional `KERYX_TEST_RPC_DELAY_MS` for drain timing).

## Health check procedures

### Bootstrap / deploy gate

1. Start `keryxd` with data dir and listen addr.
2. `keryx doctor` via endpoint → must be `pass`.
3. gRPC `Readiness` → `ready: true` and empty `not_ready_reasons`.
4. Optional: `Liveness` → `alive: true`.

### Steady state

- **Liveness:** cheap; use for process supervision.
- **Readiness:** use before sending worker traffic; re-evaluated every health interval and after store errors.
- **Status:** full picture including metrics and startup recovery stats.
- **Doctor:** actionable check list when debugging a failed deploy.

### Degraded readiness

When `Readiness` returns `ready: false`, inspect `not_ready_reasons` and daemon logs (`health_loop` warnings). Common strings:

- `schema_version mismatch` — run migrations or align binary with DB.
- `corruption_count=...` / `unrepaired_corruption` — stop traffic; see troubleshooting.
- `store connectivity failed` / `store health probe failed` — disk, permissions, or SQLite lock.

## Common issues and troubleshooting

### Daemon refuses to bind address

**Symptom:** error mentioning loopback when setting `HERMES_KERYX_DAEMON_ADDR`.

**Cause:** non-loopback bind is intentional policy.

**Action:** use `127.0.0.1:PORT` or `[::1]:PORT`; front with SSH or local proxy if remote operators need access.

### Startup fails with corruption

**Symptom:** process exits during startup; logs or CLI mention `UnrepairedCorruption` / `corruption_count > 0`.

**Cause:** event stream does not replay to snapshot for one or more tasks.

**Action:**

1. Do not delete `keryx.db` without a backup.
2. Run `keryx doctor` locally against a **copy** of the data dir.
3. Identify `corrupted_tasks` from recovery report or readiness reasons.
4. Restore from backup or apply a documented repair (fail-closed until repair policy is approved — see lifecycle semantics).

### Tasks stuck in `running`

**Symptom:** task never completes; workers gone.

**Cause:** expired lease not yet recovered.

**Action:**

1. Confirm lease recovery loop is running (listener mode).
2. Wait for next tick (default 30s) or restart daemon (startup recovery also requeues).
3. Check logs for `stale lease recovery` with `task_id`.
4. Verify worker heartbeats and `lease_duration_ms`.

### `ClaimTask` / heartbeat lease errors

| gRPC code | Typical store cause |
| --- | --- |
| `ABORTED` | Active lease conflict |
| `PERMISSION_DENIED` | Wrong `lease_id` or `worker_id` |
| `NOT_FOUND` | Unknown task or lease |
| `FAILED_PRECONDITION` | Illegal lifecycle transition |

See structured `rpc store error` logs for `grpc_code` and message.

### CLI cannot reach daemon

**Symptom:** `daemon unavailable at ...`

**Action:** verify `keryxd` is listening, endpoint URL matches `http://host:port`, firewall, and loopback binding.

### Shutdown hangs or forced close

**Symptom:** long shutdown; log `shutdown drain timed out with in-flight RPCs remaining`.

**Action:** ensure workers complete or fail tasks; increase shutdown timeout via `KeryxDaemonConfig::with_shutdown_timeout_ms` in embedded tests; production binary uses 30s default until env/config wiring exists.

## Environment variables reference

| Variable | Used by | Purpose |
| --- | --- | --- |
| `HERMES_KERYX_DATA_DIR` | `keryxd`, `keryx` CLI | SQLite directory (default `.keryx`). DB file: `{dir}/keryx.db`. |
| `HERMES_KERYX_DAEMON_ADDR` | `keryxd` | Optional `host:port` to bind gRPC (loopback only). Unset = no listener. |
| `HERMES_KERYX_DAEMON_ENDPOINT` | `keryx` CLI | Optional `http://host:port` for status, doctor, and `task` subcommands. |
| `RUST_LOG` | (not wired in stock `keryxd`) | Standard tracing filter when subscriber uses `EnvFilter::from_default_env()`. See [observability.md](observability.md). |
| `KERYX_TEST_RPC_DELAY_MS` | Integration tests only | Artificial RPC delay for graceful shutdown drain tests. |

Daemon-internal defaults (not env vars today):

| Setting | Default | Location |
| --- | --- | --- |
| Lease recovery interval | 30000 ms | `DEFAULT_LEASE_RECOVERY_INTERVAL_MS` |
| Health check interval | 60000 ms | `DEFAULT_HEALTH_CHECK_INTERVAL_MS` |
| Default lease TTL | 300000 ms (5 min) | When RPC omits `lease_duration_ms` |
| Shutdown drain timeout | 30000 ms | `DEFAULT_SHUTDOWN_TIMEOUT_MS` |

## Validation before release

```bash
cd /path/to/Hermes_Keryx
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Focused observability tests:

```bash
cargo test -p keryx-daemon --test health_probes
cargo test -p keryx-daemon --test tracing_instrumentation
cargo test -p keryx-daemon --test graceful_shutdown
cargo test -p keryx-observe
```