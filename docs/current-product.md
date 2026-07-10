# Current Hermes Keryx product surface

This page is the repository-wide map of what is implemented today. Older RFCs and ADRs in this repository are design history; when they differ from this page, this page and the Rust/Python source are the current product contract.

## Components

| Component | Implemented surface |
|---|---|
| `keryxd` | Local daemon runtime; SQLite store; gRPC `KeryxDaemon` service; lifecycle, durable task envelopes, artifacts, cancellation, deadline enforcement, routing, discovery hooks, health/readiness/status/doctor. |
| `keryx-relay` | libp2p relay process; TCP + QUIC listen addresses; gRPC health; HTTP `/health`; task publication/mailbox delivery; in-memory skill registry with TTL cleanup and gossipsub sync; peer allowlist; node token auth primitives. |
| `keryx-node` | Edge node binary from `keryx-relay`; verifies daemon readiness, dials bootstrap peers, registers skills, consumes relay task frames, and submits delivered envelopes into its local daemon. |
| `keryx` | Operator CLI for `status`, `doctor`, `task`, `artifact`, `relay`, and `node` subcommands. |
| Python SDK | Package/import name `keryx`; async `KeryxNode`; daemon lifecycle methods; relay registry helpers; AgentAnycast-compatible transition helpers. Remote handler dispatch and terminal result return are not complete yet. |
| Ops scripts | `scripts/keryx-dual-run.sh` for one local daemon+relay pair; `scripts/migrate-to-keryx.sh` for Hermes config migration/revert. |

## Canonical lifecycle

The persisted lifecycle remains four-state:

```text
pending -> running -> completed | failed
```

Operational outcomes are metadata/events rather than extra task status values:

- retry requeue: `running -> pending`, increments `retry_count`, appends `RecoveryAction`
- dead-letter: `running -> failed`, sets `dead_lettered` and `dead_letter_reason`
- cancel: `pending` or `running` -> `failed`, marks cancellation counters and reason metadata
- deadline expiry: expired `deadline_ms` on `pending`/`running` -> `failed`
- routing approval hold: `SendTask` can return `awaiting_approval` as a routing outcome; it is not a canonical persisted `TaskStatus`

## Daemon gRPC API

`proto/hermes/keryx/v1/daemon.proto` implements:

- health/operator: `Status`, `Doctor`, `Liveness`, `Readiness`
- worker lifecycle: `SubmitTask`, `ClaimTask`, `ClaimNextTask`, `Heartbeat`, `CompleteTask`, `FailTask`, `CancelTask`
- artifacts: `PutArtifact`, `GetArtifact`, `ListArtifacts`, `DeleteArtifact`
- routing/discovery: `SendTask`, `ListPeers`, `DiscoverSkills`

Important defaults:

| Setting | Default |
|---|---:|
| schema version | `6` |
| lease TTL when omitted | `300_000 ms` |
| lease recovery interval | `30_000 ms` |
| deadline enforcement interval | `30_000 ms` |
| health probe interval | `60_000 ms` |
| shutdown drain timeout | `30_000 ms` |
| pending task limit | `10_000` (`0` means unlimited) |
| submit envelope limit | `4 MiB` (`0` means unlimited) |
| inline artifact threshold | `64 KiB` |
| max artifact/blob size | `256 MiB` |
| default local peer id | `node-local` |
| default `SendTask` timeout | `30_000 ms` |

## Storage

`keryx-store` provides `InMemoryStore` for tests and `SqliteStore` for runtime. The SQLite store owns:

- task snapshots and per-task event log
- complete encoded `TaskEnvelope` records keyed by task ID
- idempotency keys
- active/inactive leases
- retry/dead-letter metadata
- artifact metadata plus inline bytes/blob references
- deadline/cancellation fields

Schema v6 adds `task_envelopes`. `SubmitTask` now persists the complete encoded protobuf envelope atomically with the pending lifecycle row, idempotency key, and accepted event. Nested messages, raw bytes, metadata maps, correlation IDs, and requested capability hints therefore survive daemon restart.

The store intentionally treats the encoded envelope as opaque bytes and does not depend on `keryx-proto`; protobuf encoding and decoding remain daemon/SDK concerns. Idempotent retries must match both the lifecycle record and the stored envelope. Conflicting envelope bytes fail closed.

Default local CLI/runtime data directory is `.keryx` when `HERMES_KERYX_DATA_DIR` is unset. Operator dual-run uses `~/.hermes/.keryx/data`.

## Relay and registry

`keryx-relay` supports both JSON and TOML process config:

- JSON `RelayConfig` exposes direct fields such as `listen_addresses`, `health_grpc_bind`, `health_http_bind`, and `registry_grpc_bind`.
- TOML config supports `[relay]`, `[security]`, and `[registry]` sections. TOML enables allowlist files, empty-allowlist policy, inline/external node tokens, and registry TTL/max-skills settings.

Relay defaults in code are `0.0.0.0:4001` TCP/QUIC, `127.0.0.1:50052` gRPC health, `127.0.0.1:8081` HTTP health, and `127.0.0.1:50053` registry. The dual-run script intentionally overrides these to loopback non-conflicting ports.

## Cross-node delivery boundary

Keryx currently proves this one-way transport path:

```text
sender keryxd SendTask
  -> relay PublishTask
  -> destination keryx-node stream
  -> destination keryxd SubmitTask
  -> destination lifecycle row + durable full envelope
```

A complete Hermes Agency round trip is **not implemented yet**. The remaining Phase 17 work is tracked in [phase17-cross-node-agent-delivery.md](phase17-cross-node-agent-delivery.md) and [issue #10](https://github.com/DeployFaith/hermes-keryx/issues/10).

Phase 17.1 retains complete envelopes durably. Phase 17.2 adds atomic worker dequeue through `ClaimNextTask`, with deterministic selection, exact skill/capability filters, bounded long polling, and lease-safe concurrent claims.

Missing today:

- Python `serve_forever()` consumption of the available `ClaimNextTask` worker API
- transport-authenticated sender identity attached to the claimed envelope
- Python `serve_forever()` dispatch into registered `on_task()` handlers
- authenticated terminal result/artifact routing back to the origin
- a remotely updated `TaskHandle.wait()`
- a repeatable two-daemon/two-edge-node Agency E2E

This boundary matters for product claims: relay publication, mailbox delivery, destination daemon submission, durable envelope retention, registry discovery, and local lifecycle are implemented; remote Agent execution plus the result round trip are not yet complete.

## Operator CLI

Actual `keryx` CLI subcommands:

```text
keryx status
keryx doctor
keryx task submit|claim|heartbeat|complete|fail
keryx artifact put|get|ls|rm
keryx relay start|status|registry list
keryx node start|status|discover
```

Notes:

- `keryx task` currently has no `cancel` subcommand even though the daemon exposes `CancelTask`.
- `status` and `doctor` run an embedded local runtime when `HERMES_KERYX_DAEMON_ENDPOINT` is unset, or query the daemon endpoint when set.
- `artifact`, `task`, and `node status` require a daemon endpoint.
- `relay status` defaults to `http://127.0.0.1:50052` unless `HERMES_KERYX_RELAY_HEALTH_ENDPOINT` is set.
- `relay registry list` / `node discover` default to `http://127.0.0.1:50053` unless `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT` is set.

## Python SDK

The Python package is `keryx` and exports:

- `KeryxNode`, `KeryxConfig`, `load_config`
- `TaskState`, `TaskResult`, `TaskArtifact`
- `AgentCard`, `Skill`
- `Task`, `IncomingTask`, `TaskHandle`, `TaskStatus`
- `peer_id_to_did_key`, `register_agent`, `deregister_agent`

Native daemon lifecycle methods include `connect`, `status`, `doctor`, `peers`, `skills`, `submit`, `claim`, `claim_next`, `heartbeat`, `complete`, `fail`, and `cancel`. Compatibility helpers include `start`, `stop`, `discover`, `send_task`, `register_skills`, `deregister_skills`, and `serve_forever`.

Current compatibility limits:

- `serve_forever()` keeps the SDK process alive but does not claim daemon tasks or invoke registered task handlers.
- `send_task()` can submit through a configured daemon/relay route, but its compatibility `TaskHandle` is not attached to a remote terminal-status/result stream.
- `IncomingTask.complete()` / `.fail()` are not yet wired to a durable relay result route.

The SDK default daemon endpoint is `unix:///tmp/keryx-daemon.sock`; most repository examples override it to `127.0.0.1:50051` / `http://127.0.0.1:50051` for the current daemon binary and CLI.

## Dual-run defaults

`scripts/keryx-dual-run.sh` starts one local daemon and one relay without colliding with common AgentAnycast ports:

| Component | Default |
|---|---|
| daemon gRPC | `127.0.0.1:50051` |
| relay gRPC health | `127.0.0.1:51052` |
| relay HTTP health | `127.0.0.1:18081` |
| relay registry gRPC | `127.0.0.1:51053` |
| relay libp2p TCP | `/ip4/127.0.0.1/tcp/4101` |
| relay libp2p QUIC | `/ip4/127.0.0.1/udp/4101/quic-v1` |
| state root | `~/.hermes/.keryx` |

Dual-run validates infrastructure health. It does not start two edge nodes or prove a remote Hermes Agency handler/result round trip.

## Environment variables

Common variables:

| Variable | Used by | Purpose |
|---|---|---|
| `HERMES_KERYX_DATA_DIR` | daemon, CLI, dual-run | SQLite data directory |
| `HERMES_KERYX_DAEMON_ADDR` | daemon, dual-run | daemon bind address (loopback-only in `keryxd`) |
| `HERMES_KERYX_DAEMON_ENDPOINT` | CLI, SDK, node, scripts | daemon client endpoint |
| `HERMES_KERYX_RELAY_CONFIG` | relay, node, scripts | relay JSON/TOML config path |
| `HERMES_KERYX_RELAY_ENDPOINT` | daemon routing publisher, node stream | relay gRPC endpoint with scheme |
| `HERMES_KERYX_RELAY_HEALTH_ENDPOINT` | CLI, daemon fallback alias, node fallback alias | relay health/control gRPC endpoint with scheme |
| `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT` | relay CLI, node CLI, daemon discovery, node binary | relay registry gRPC endpoint with scheme for clients |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | Python SDK, dual-run script | SDK/dual-run registry endpoint alias |
| `HERMES_KERYX_DAEMON_SKILLS` | daemon discovery | comma-separated daemon skills to register |
| `HERMES_KERYX_NODE_SKILLS` | `keryx-node` | comma-separated edge-node skills to register |
| `HERMES_KERYX_WORKER_ID` | Python SDK | default worker id for claim/heartbeat/complete/fail |

## Validation commands

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

cd sdk/python
python -m pip install -e ".[dev]"
pytest

bash -n scripts/migrate-to-keryx.sh
bash -n scripts/keryx-dual-run.sh
./scripts/migrate-to-keryx.sh --dry-run
./scripts/keryx-dual-run.sh --status
```
