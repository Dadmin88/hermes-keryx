# Hermes Keryx observability

Keryx currently exposes structured tracing, in-process metrics, daemon and relay health probes, status/doctor surfaces, cancellation/deadline counters, relay health, and registry size metrics.

Related: [current-product.md](current-product.md), [operations.md](operations.md), [lifecycle-store-daemon-semantics.md](lifecycle-store-daemon-semantics.md).

## Tracing

Keryx uses the Rust `tracing` ecosystem (`tracing` + `tracing-subscriber` with `env-filter`, `fmt`, and `json` features enabled).

### Subscriber initialization

`keryxd`, `keryx-relay`, and `keryx-node` initialize logging with a fixed INFO filter today:

```rust
tracing_subscriber::fmt().with_env_filter("info").init();
```

Current behavior:

- INFO and above are emitted through the default human-readable formatter.
- `RUST_LOG` is not honored by the stock binary entrypoints yet.
- JSON output is available at the dependency level but not enabled by stock entrypoints.

### Stable log fields

| Field | Source |
|---|---|
| `component="keryxd"` | daemon lifecycle, listener, shutdown |
| `component="health_loop"` | periodic store readiness probes |
| `component="lease_recovery_loop"` | background stale-lease recovery |
| `component="deadline_enforcement_loop"` | deadline scans and expiry failures |
| `component="incoming_handler"` | relay-delivered task accept/reject |
| `component="keryx-relay"` | relay process lifecycle |
| `component="keryx-node"` | edge node lifecycle |
| `target="keryx.security"` | node auth and routing policy audit |

Do not put secrets in task metadata. Logs, doctor/status surfaces, and audit records should include stable identifiers and enum reasons, not task payloads or raw artifact contents.

### Daemon spans

RPC spans:

| Span name | RPC / role |
|---|---|
| `keryx::rpc::status` | `Status` |
| `keryx::rpc::doctor` | `Doctor` |
| `keryx::rpc::liveness` | `Liveness` |
| `keryx::rpc::readiness` | `Readiness` |
| `keryx::rpc::submit_task` | `SubmitTask` (`task_id`) |
| `keryx::rpc::claim_task` | `ClaimTask` (`task_id`, `worker_id`) |
| `keryx::rpc::heartbeat` | `Heartbeat` (`task_id`, `lease_id`, `worker_id`) |
| `keryx::rpc::complete_task` | `CompleteTask` |
| `keryx::rpc::fail_task` | `FailTask` |
| `keryx::rpc::cancel_task` | `CancelTask` |
| `keryx::rpc::put_artifact` | artifact upload |
| `keryx::rpc::get_artifact` | artifact retrieval |
| `keryx::rpc::list_artifacts` | artifact listing |
| `keryx::rpc::delete_artifact` | artifact delete |
| `keryx::rpc::send_task` | local/relay route request |
| `keryx::rpc::list_peers` | peer directory snapshot |
| `keryx::rpc::discover_skills` | daemon-backed registry discovery |

Background/runtime spans:

| Span name | Role |
|---|---|
| `keryx::daemon::health_tick` | store probe tick (`ready`, `reason_count`) |
| `keryx::daemon::lease_recovery_tick` | stale lease scan (`duration_ms`, recovered/cleaned counts) |
| `keryx::daemon::deadline_enforcement_tick` | deadline expiry scan (`duration_ms`, `failed_tasks`) |
| `keryx::daemon::incoming_task` | relay frame accept/reject (`frame_id`, `sender_node_id`, `task_id`) |
| `keryx::routing::send_task` | route policy + local/relay delivery |
| `keryx::routing::route_task` | route wrapper used by daemon RPC/tests |

Store operations invoked from RPC are instrumented with Rust function names such as `accept_task`, `lease_task`, `renew_lease`, `complete_task`, `fail_task`, `cancel_task`, `fail_expired_deadlines`, `recover_stale_leases`, and artifact methods.

## Daemon metrics

Metrics live in `keryx-observe` (`KeryxMetrics`) and cancellation state in `keryx-daemon`. They are in-process and exposed through the daemon `Status` RPC; there is no Prometheus scrape endpoint yet.

### StatusResponse fields

| Field | Meaning |
|---|---|
| `tasks_submitted` | successful `SubmitTask` and accepted incoming relay tasks |
| `tasks_claimed` | successful `ClaimTask` / auto-dispatch claim |
| `tasks_completed` | successful `CompleteTask` |
| `tasks_failed` | successful `FailTask` terminal/failure outcomes |
| `heartbeats` | successful lease renewals |
| `leases_recovered` | recovered stale leases |
| `recovery_ticks` | lease recovery loop iterations |
| `active_leases` | gauge tracked by claim/complete/fail/recovery |
| `dead_letters` | failures ending with `dead_lettered=true` |
| `max_pending_tasks` | pending queue limit (`0` = unlimited) |
| `max_envelope_bytes` | submit envelope byte limit (`0` = unlimited) |
| `current_pending_tasks` | pending task count when available |
| `cancel_requests` | `CancelTask` calls accepted by daemon layer |
| `tasks_canceled` | cancellation requests that transitioned a task |
| `deadline_ticks` | deadline loop scans |
| `deadline_failures` | total tasks failed by expired deadlines |
| `last_deadline_scan_ms` | Unix ms of last deadline scan |
| `last_deadline_failures` | tasks failed by the most recent deadline scan |
| `deadline_enforcement_interval_ms` | configured deadline scan interval |
| `warnings` | degraded status details (for example pending-count unavailable) |

`keryx status` currently prints readiness, store/schema, startup recovery, limits, and warnings. Use gRPC `Status` or SDK `node.status()` for the full metrics/cancellation/deadline field set.

## Relay metrics and health

`RelayRuntime` and `RelayMetrics` surface through `KeryxRelay/Health` and `keryx relay status`.

| Health field | Meaning |
|---|---|
| `healthy` | true after the libp2p transport has started listening |
| `connected_peers` | connected gRPC node streams + libp2p peers |
| `registry_size` | active skill registry registrations |
| `uptime_seconds` | process runtime |
| `transport_status` | `starting` or `listening` |
| `tasks_routed` | relay reservation/circuit and frame routing activity |
| `local_peer_id` | relay libp2p peer id |

HTTP health is available at `GET /health` when `health_http_bind` is configured. gRPC health is available through `KeryxRelay/Health` when `health_grpc_bind` is configured.

## Health and operator probes

### Daemon Liveness

- RPC: `KeryxDaemon/Liveness`
- Semantics: process/RPC stack is accepting calls.
- Shutdown: new calls receive `UNAVAILABLE` once graceful shutdown begins.

### Daemon Readiness

- RPC: `KeryxDaemon/Readiness`
- Semantics: cached `DynamicReadiness` updated at startup and on health-loop ticks.
- Store probe validates schema version and recovery integrity.
- Use before sending task traffic.

### Daemon Status

- RPC: `KeryxDaemon/Status`
- CLI: `keryx status`
- Includes startup recovery, schema/store info, limits, task metrics, cancellation/deadline counters, and warnings.

### Daemon Doctor

- RPC: `KeryxDaemon/Doctor`
- CLI: `keryx doctor`
- Named checks currently include:

| Check | Pass condition |
|---|---|
| `data_dir` | data directory exists |
| `sqlite_store` | store is ready and `keryx.db` exists |
| `schema_version` | applied schema equals supported schema |
| `startup_recovery` | corruption count is zero |
| `limits` | pending count is known and under configured limit |
| `cancellation` | cancellation/deadline counters are readable |

`event_log_consistency` is represented through startup recovery/corruption reporting; it is not a separate doctor check yet.

### Relay Health

- RPC: `KeryxRelay/Health`
- CLI: `keryx relay status`
- Use for relay deploy gates and dual-run validation.

## gRPC examples

With `grpcurl`:

```bash
grpcurl -plaintext -import-path proto -proto hermes/keryx/v1/daemon.proto \
  127.0.0.1:50051 hermes.keryx.v1.KeryxDaemon/Readiness

grpcurl -plaintext -import-path proto -proto hermes/keryx/v1/daemon.proto \
  127.0.0.1:50051 hermes.keryx.v1.KeryxDaemon/Status

grpcurl -plaintext -import-path proto -proto hermes/keryx/v1/relay.proto \
  127.0.0.1:51052 hermes.keryx.v1.KeryxRelay/Health
```

CLI equivalents:

```bash
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 keryx status
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 keryx doctor
HERMES_KERYX_RELAY_HEALTH_ENDPOINT=http://127.0.0.1:51052 keryx relay status
```

## Test coverage pointers

```bash
cargo test -p keryx-daemon --test health_probes
cargo test -p keryx-daemon --test tracing_instrumentation
cargo test -p keryx-daemon --test graceful_shutdown
cargo test -p keryx-daemon --test artifact_rpc
cargo test -p keryx-daemon --test task_routing
cargo test -p keryx-daemon --test discovery_integration
cargo test -p keryx-relay --test health
cargo test -p keryx-relay --test registry_grpc
cargo test -p keryx-relay --test security
cargo test -p keryx-observe
```
