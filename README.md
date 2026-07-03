# Hermes Keryx

Hermes Keryx is a standalone Rust-native runtime substrate for the Hermes ecosystem: local daemon, relay, durable task transport, event log, persistence, Python SDK, recovery, and diagnostics.

Hermes Agency (Phase 12–13) integrates Keryx as its primary P2P transport layer, replacing the legacy AgentAnycast stack. See [docs/migration-from-agentanycast.md](docs/migration-from-agentanycast.md) for operator migration steps.

## Naming

- Product: Hermes Keryx
- CLI: `keryx`
- Daemon: `keryxd`
- Relay: `keryx-relay`
- Rust crates: `keryx-*`
- Protocol namespace: `hermes.keryx.v1`
- Config path: `~/.hermes/keryx/`
- Environment prefix: `HERMES_KERYX_*`

## Workspace

```text
crates/keryx-core      Pure domain model, task lifecycle, validation, errors
crates/keryx-proto     Generated/protocol-facing Rust types
crates/keryx-store     Persistence traits and stores
crates/keryx-daemon    Local daemon runtime and `keryxd`
crates/keryx-relay     Cross-node relay and `keryx-relay`
crates/keryx-cli       User/operator CLI and `keryx`
crates/keryx-policy    Policy and approvals
crates/keryx-observe   Logs, metrics, events, traces
crates/keryx-testkit   Test fixtures and crash helpers
```

## Implementation status (roadmap)

| Phase | Focus | Status |
| --- | --- | --- |
| 1–5 | Strict four-state lifecycle, SQLite store, leases, startup recovery, readiness gate | Implemented |
| 6 | Daemon gRPC worker loop (`Submit` / `Claim` / `Heartbeat` / `Complete` / `Fail`), periodic lease recovery, `keryx task` CLI | Implemented |
| 7 | `RetryPolicy`, dead-letter metadata (schema v3), legacy status/event normalization | Implemented |
| 8 | Observability hardening (metrics, structured tracing, health probes, graceful shutdown, operator playbooks) | Implemented |
| 12–13 | Hermes Agency transport backend (`KeryxNode`, pool routing, config migration) | Implemented (Agency repo) |

Runtime semantics: [docs/lifecycle-store-daemon-semantics.md](docs/lifecycle-store-daemon-semantics.md). Worker RPC flow: [docs/worker-loop.md](docs/worker-loop.md). Observability: [docs/observability.md](docs/observability.md). Operations: [docs/operations.md](docs/operations.md). Agency migration: [docs/migration-from-agentanycast.md](docs/migration-from-agentanycast.md).

## Operator quickstart

Build and test:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Local readiness without a listener (opens/migrates SQLite under `HERMES_KERYX_DATA_DIR` or `.keryx`):

```bash
cargo run -p keryx-cli --bin keryx -- status
cargo run -p keryx-cli --bin keryx -- doctor
```

Daemon with gRPC listener and task RPCs:

```bash
export HERMES_KERYX_DATA_DIR="${PWD}/.keryx-data"
export HERMES_KERYX_DAEMON_ADDR=127.0.0.1:50051
cargo run -p keryx-daemon --bin keryxd
```

Task lifecycle (requires running daemon and endpoint):

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051

cargo run -p keryx-cli --bin keryx -- task submit my-task-id

cargo run -p keryx-cli --bin keryx -- task claim my-task-id \
  --worker worker-1 --lease-duration-ms 120000

cargo run -p keryx-cli --bin keryx -- task heartbeat my-task-id \
  --lease '<lease_id_from_claim>' --worker worker-1

cargo run -p keryx-cli --bin keryx -- task complete my-task-id \
  --lease '<lease_id_from_claim>' --worker worker-1 --duration-ms 1000
```

Subcommands also include `task fail` (retry/dead-letter per daemon policy) and `task --help` for flags.

Query daemon-backed status/doctor:

```bash
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 \
  cargo run -p keryx-cli --bin keryx -- status
```

## First validation commands

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Python SDK (`keryx-py`)

The `sdk/python` package provides `KeryxNode`, `AgentCard`, `Skill`, and task helpers with an API aligned to the former `agentanycast` node contract. Hermes Agency loads it via `hermes-agency/transport.py` when `agency.transport_backend` is `keryx`.

```bash
cd sdk/python
pip install -e ".[dev]"
pytest
```

Environment variables used by Agency and standalone scripts:

| Variable | Purpose |
| --- | --- |
| `HERMES_KERYX_DAEMON_ENDPOINT` | Daemon gRPC (`http://127.0.0.1:50051`) |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | Relay skill registry (typical port `50053`) |
| `HERMES_KERYX_SDK_PATH` | Dev checkout path when not pip-installed |

Details: [sdk/python/README.md](sdk/python/README.md).

## Relay

`keryx-relay` exposes libp2p relay listeners (default TCP/QUIC `4001`) and an in-memory skill registry. Configure with TOML (`crates/keryx-relay/config.example.toml`) and point `HERMES_KERYX_RELAY_CONFIG` at the generated file under `~/.hermes/.keryx/relay.toml` after Agency migration.

Typical ports:

| Component | Port |
| --- | --- |
| `keryxd` gRPC | `50051` (loopback) |
| Registry gRPC | `50053` on relay |
| libp2p | `4001` |

## Hermes Agency integration

- Set `agency.transport_backend: keryx` or `HERMES_AGENCY_TRANSPORT_BACKEND=keryx`.
- Run `./scripts/migrate-to-keryx.sh` from the Hermes Agency repo to rewrite profile `config.yaml`, sync allowlists, and record a reversible backup.
- Pool dispatch (`agency_pool_send`) honors `HERMES_AGENCY_POOL_TRANSPORT=keryx` for wake/send routing.
- Doctor checks resolve `keryx-daemon` / `keryxd` before legacy daemons.

Cross-repo validation checklist: `Hermes_Agency/keryx-phase-12d-integration-validation.md`.