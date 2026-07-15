# Hermes Keryx

Hermes Keryx is a standalone Rust-native runtime for durable multi-agent task transport in the Hermes ecosystem.

It provides:

- local daemon (`keryxd`) with SQLite-backed task lifecycle
- cross-node relay (`keryx-relay`) with libp2p + skill registry
- operator CLI (`keryx`)
- Python SDK (`keryx`) for Hermes Agency and standalone clients
- cancellation, deadlines, artifacts, backpressure, security allowlists, and migration tooling

**Hermes Agency** uses Keryx as its primary transport. The Keryx Python SDK is also vendored into the Hermes Agency repo under `src/keryx/` for packaging; this repository remains the source of truth for the Rust runtime and is the intended upstream PR vehicle to Nous.

## Naming

| Item | Value |
|------|-------|
| Product | Hermes Keryx |
| CLI | `keryx` |
| Daemon | `keryxd` |
| Relay | `keryx-relay` |
| Rust crates | `keryx-*` |
| Python package | `keryx` (import name `keryx`) |
| Protocol namespace | `hermes.keryx.v1` |
| Runtime state | `~/.hermes/.keryx/` |
| Env prefix | `HERMES_KERYX_*` |

## Workspace layout

```text
crates/keryx-core      Pure domain model, task lifecycle, validation, errors
crates/keryx-proto     Protocol-facing Rust types / gRPC bindings
crates/keryx-store     Persistence traits + SQLite store (schema v5)
crates/keryx-daemon    Local daemon runtime + `keryxd`
crates/keryx-relay     Relay, health, registry, security + `keryx-relay`
crates/keryx-cli       Operator CLI + `keryx`
crates/keryx-policy    Policy, keys, tokens, approvals
crates/keryx-observe   Logs, metrics, events, traces
crates/keryx-testkit   Test fixtures and helpers
sdk/python/            Python SDK package `keryx`
scripts/               migrate-to-keryx.sh, keryx-dual-run.sh
docs/                  Semantics, migration, ops, architecture
proto/                 Protobuf definitions
```

## Implementation status

| Phase | Focus | Status |
|------|-------|--------|
| 1–5 | Four-state lifecycle, SQLite, leases, recovery, readiness | Implemented |
| 6 | Daemon worker RPCs (`Submit`/`Claim`/`Heartbeat`/`Complete`/`Fail`), CLI task verbs | Implemented |
| 7 | Retry/dead-letter metadata, legacy status normalization | Implemented |
| 8 | Observability, health probes, graceful shutdown | Implemented |
| 9 | Artifact storage | Implemented |
| 9b | Artifact RPC + CLI | Implemented |
| 10 | Backpressure + configurable limits | Implemented |
| 11A | Core cancellation types | Implemented |
| 11B | Store cancellation + deadlines (schema v5) | Implemented |
| 11C | CancelTask proto + daemon deadline loop | Implemented |
| 12A | Relay transport (gRPC, offline mailbox, peer identity) | Implemented |
| 13A | Peer discovery + skill registry (gossip sync) | Implemented |
| 14A | Security model + routing policy | Implemented |
| 15A | Python SDK (`KeryxNode`) | Implemented |
| 16A | Migration script + dual-run infrastructure | Implemented |

Semantics references:

- [docs/lifecycle-store-daemon-semantics.md](docs/lifecycle-store-daemon-semantics.md)
- [docs/worker-loop.md](docs/worker-loop.md)
- [docs/observability.md](docs/observability.md)
- [docs/operations.md](docs/operations.md)
- [docs/migration-from-agentanycast.md](docs/migration-from-agentanycast.md)

## Build and test

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Release binaries:

```bash
cargo build --release --bin keryxd --bin keryx-relay --bin keryx
```

## Operator quickstart

### Local dual-run (recommended for Agency migration)

```bash
# Start keryxd + keryx-relay on non-conflicting loopback ports
./scripts/keryx-dual-run.sh --start
./scripts/keryx-dual-run.sh --status
./scripts/keryx-dual-run.sh --stop
```

Dual-run defaults (loopback only; avoids legacy AgentAnycast 4001/50052):

| Component | Address |
|-----------|---------|
| `keryxd` gRPC | `127.0.0.1:50051` |
| Relay health gRPC | `127.0.0.1:51052` |
| Relay registry gRPC | `127.0.0.1:51053` |
| Relay HTTP health | `127.0.0.1:18081` |
| Relay libp2p TCP/QUIC | `127.0.0.1:4101` |

Runtime files live under `~/.hermes/.keryx/` (`logs/`, `run/`, `data/`, `relay.json`).

### Manual daemon

```bash
export HERMES_KERYX_DATA_DIR="${PWD}/.keryx-data"
export HERMES_KERYX_DAEMON_ADDR=127.0.0.1:50051
export HERMES_KERYX_DAEMON_TOKEN='<local-daemon-token>'
./target/release/keryxd
```

### Task CLI (daemon required)

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051
export HERMES_KERYX_DAEMON_TOKEN='<local-daemon-token>'

cargo run -p keryx-cli --bin keryx -- task submit my-task-id
cargo run -p keryx-cli --bin keryx -- task claim my-task-id \
  --worker worker-1 --lease-duration-ms 120000
cargo run -p keryx-cli --bin keryx -- task heartbeat my-task-id \
  --lease '<lease_id>' --worker worker-1
cargo run -p keryx-cli --bin keryx -- task complete my-task-id \
  --lease '<lease_id>' --worker worker-1 --duration-ms 1000
```

Also available: `task fail`, `task cancel` (when daemon supports CancelTask), `status`, `doctor`.

## Python SDK

Package name and import name: **`keryx`**.

```bash
cd sdk/python
python -m pip install -e ".[dev]"
pytest
```

```python
from keryx import AgentCard, KeryxNode, Skill

card = AgentCard(
    name="demo-agent",
    description="Example Keryx node",
    skills=[Skill(id="echo", description="echo messages")],
)

node = KeryxNode(
    card=card,
    daemon_endpoint="127.0.0.1:50051",
    relay_endpoint="127.0.0.1:51053",
)
```

Environment:

| Variable | Purpose |
|----------|---------|
| `HERMES_KERYX_DAEMON_ADDR` | Daemon bind address (`127.0.0.1:50051`) |
| `HERMES_KERYX_DAEMON_ENDPOINT` | Client endpoint (`http://127.0.0.1:50051`) |
| `HERMES_KERYX_DAEMON_TOKEN` | Bearer token required by daemon state-changing RPCs and sent by CLI task/artifact commands |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | Skill registry (`127.0.0.1:51053` dual-run default) |
| `HERMES_KERYX_RELAY_CONFIG` | Path to relay config JSON/TOML |
| `HERMES_KERYX_DATA_DIR` | SQLite/data root |

Details: [sdk/python/README.md](sdk/python/README.md).

## Migration from AgentAnycast

```bash
./scripts/migrate-to-keryx.sh --dry-run
./scripts/migrate-to-keryx.sh
./scripts/migrate-to-keryx.sh --revert   # if needed
```

The migrator rewrites Hermes config to `agency.transport_backend: keryx`, writes allowlist/relay config under `~/.hermes/.keryx/`, and keeps a timestamped backup.

Full guide: [docs/migration-from-agentanycast.md](docs/migration-from-agentanycast.md).

## Hermes Agency integration

Hermes Agency treats Keryx as the primary transport:

- Config: `agency.transport_backend: keryx`
- Vendored Python SDK: `Hermes_Agency/src/keryx/`
- Node/pool modules import `from keryx import ...` directly
- AgentAnycast remains a legacy fallback only

This repo stays independent so Keryx can be PR'd upstream without the full Agency product surface.

## Security notes

- Prefer loopback binds for daemon/registry on single-host installs
- Relay security allowlist: see `crates/keryx-relay/config.example.toml` and allowlist examples
- Do not commit peer IDs, tokens, private multiaddrs, or host paths into docs/examples

## License

See `LICENSE` (and `LICENSE-APACHE` if present in tree).
