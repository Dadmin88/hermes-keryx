# Hermes Keryx

Hermes Keryx is a standalone Rust-native runtime for durable multi-agent task transport in the Hermes ecosystem.

It provides:

- local daemon (`keryxd`) with SQLite-backed task lifecycle
- cross-node relay (`keryx-relay`) with libp2p, health probes, peer allowlisting, and a skill registry
- edge node binary (`keryx-node`) for relay bootstrap, registry advertisement, and relay-frame delivery into a local daemon
- operator CLI (`keryx`)
- Python SDK (`keryx`) for Hermes Agency and standalone clients
- cancellation, deadlines, artifacts, backpressure, routing policy, and migration tooling

**Hermes Agency** uses Keryx as its primary transport. The Keryx Python SDK may also be vendored into Hermes Agency under `src/keryx/` for packaging; this repository remains the source of truth for Rust crates, protobufs, and SDK evolution.

See [docs/current-product.md](docs/current-product.md) for the canonical current-product map. Older RFCs and ADRs are design history when they conflict with the implemented surface.

## Naming

| Item | Value |
|------|-------|
| Product | Hermes Keryx |
| CLI | `keryx` |
| Daemon | `keryxd` |
| Relay | `keryx-relay` |
| Edge node | `keryx-node` |
| Rust crates | `keryx-*` |
| Python package | `keryx` (import name `keryx`) |
| Protocol namespace | `hermes.keryx.v1` |
| Runtime state | `.keryx` by default; dual-run uses `~/.hermes/.keryx/` |
| Env prefix | `HERMES_KERYX_*` |

## Workspace layout

```text
crates/keryx-core      Pure domain model, identifiers, lifecycle, limits, artifacts
crates/keryx-proto     Protocol-facing Rust types / gRPC bindings
crates/keryx-store     Persistence traits + SQLite store (schema v5)
crates/keryx-daemon    Local daemon runtime + `keryxd`
crates/keryx-relay     Relay, health, registry, security + `keryx-relay` / `keryx-node`
crates/keryx-cli       Operator CLI + `keryx`
crates/keryx-policy    Policy, keys, tokens, approvals
crates/keryx-observe   In-process daemon and relay metrics
crates/keryx-testkit   Test fixtures and helpers
sdk/python/            Python SDK package `keryx`
scripts/               migrate-to-keryx.sh, keryx-dual-run.sh
docs/                  Current product docs, semantics, migration, ops, architecture, RFCs/ADRs
proto/                 Protobuf definitions
```

## Implementation status

| Phase | Focus | Status |
|------|-------|--------|
| 1–5 | Four-state lifecycle, SQLite, leases, recovery, readiness | Implemented |
| 6 | Daemon worker RPCs (`Submit`/`Claim`/`Heartbeat`/`Complete`/`Fail`), CLI task verbs | Implemented |
| 7 | Retry/dead-letter metadata, legacy status normalization | Implemented |
| 8 | Observability, health probes, graceful shutdown | Implemented |
| 9 | Artifact storage + artifact RPC/CLI | Implemented |
| 10 | Backpressure + configurable limits | Implemented |
| 11 | Cancellation + deadline fields, store APIs, daemon `CancelTask`, deadline loop | Implemented |
| 12 | Relay publication, node identity, offline mailbox, daemon `SendTask`/peers | Transport primitives implemented; full Agent round trip pending Phase 17 |
| 13 | Peer discovery + skill registry with TTL and registry gossip | Implemented |
| 14 | Security model + routing policy, relay allowlist and node-token auth primitives | Implemented |
| 15 | Python SDK (`KeryxNode`) | Local lifecycle/submission helpers implemented; remote handler/result loop pending Phase 17 |
| 16 | Migration script + dual-run infrastructure | Implemented |
| 17 | Durable daemon-to-Agent dispatch and terminal result/artifact return | Planned and tracked in issue #10 |

Primary references:

- [docs/current-product.md](docs/current-product.md)
- [docs/phase17-cross-node-agent-delivery.md](docs/phase17-cross-node-agent-delivery.md)
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
cargo build --release \
  --bin keryxd \
  --bin keryx-relay \
  --bin keryx-node \
  --bin keryx
```

## Operator quickstart

### Local dual-run

```bash
# Start one keryxd + one keryx-relay on non-conflicting loopback ports
./scripts/keryx-dual-run.sh --start
./scripts/keryx-dual-run.sh --status
./scripts/keryx-dual-run.sh --stop
```

Dual-run defaults (loopback only; avoids common legacy AgentAnycast ports 4001/50052):

| Component | Address |
|-----------|---------|
| `keryxd` gRPC | `127.0.0.1:50051` |
| Relay health gRPC | `127.0.0.1:51052` |
| Relay registry gRPC | `127.0.0.1:51053` |
| Relay HTTP health | `127.0.0.1:18081` |
| Relay libp2p TCP/QUIC | `127.0.0.1:4101` |

Runtime files live under `~/.hermes/.keryx/` (`logs/`, `run/`, `data/`, relay config).

This quickstart validates daemon/relay infrastructure health. It does **not** start two edge nodes or prove a complete remote Hermes Agency handler and result/artifact round trip. That work is Phase 17.

### Manual daemon

```bash
export HERMES_KERYX_DATA_DIR="${PWD}/.keryx-data"
export HERMES_KERYX_DAEMON_ADDR=127.0.0.1:50051
cargo run -p keryx-daemon --bin keryxd
```

### CLI

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051

cargo run -p keryx-cli --bin keryx -- status
cargo run -p keryx-cli --bin keryx -- doctor

cargo run -p keryx-cli --bin keryx -- task submit my-task-id
cargo run -p keryx-cli --bin keryx -- task claim my-task-id \
  --worker worker-1 --lease-duration-ms 120000
cargo run -p keryx-cli --bin keryx -- task heartbeat my-task-id \
  --lease '<lease_id>' --worker worker-1
cargo run -p keryx-cli --bin keryx -- task complete my-task-id \
  --lease '<lease_id>' --worker worker-1 --duration-ms 1000
```

Implemented CLI groups:

```text
keryx status
keryx doctor
keryx task submit|claim|heartbeat|complete|fail
keryx artifact put|get|ls|rm
keryx relay start|status|registry list
keryx node start|status|discover
```

`CancelTask` is implemented in the daemon and SDK, but the Rust CLI does not currently expose `keryx task cancel`.

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
    registry_endpoint="127.0.0.1:51053",
)
await node.connect()
try:
    state = await node.submit(message="hello", metadata={"skill": "echo"})
    print(state.task_id, state.status)
finally:
    await node.close()
```

This example proves local daemon submission. The AgentAnycast-compatible `serve_forever()` and `TaskHandle.wait()` surfaces do not yet complete a remote Agency execution/result loop. See [Phase 17](docs/phase17-cross-node-agent-delivery.md).

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

Hermes Agency treats Keryx as its primary transport:

- Config: `agency.transport_backend: keryx`
- Vendored Python SDK: `Hermes_Agency/src/keryx/` when packaging Agency
- Node/pool modules import `from keryx import ...` directly
- AgentAnycast remains a legacy fallback only

The current integration supports local lifecycle, registry discovery, relay publication, and destination daemon submission. Complete remote Agent handler execution and terminal result/artifact return remain Phase 17 work and should not be presented as shipped until the cross-process E2E passes.

This repo stays independent so Keryx can be PR'd upstream without the full Agency product surface.

## Security notes

- Prefer loopback binds for daemon/registry on single-host installs.
- `keryxd` rejects non-loopback `HERMES_KERYX_DAEMON_ADDR` in the current binary.
- Relay allowlists and node-token auth are documented in [docs/current-product.md](docs/current-product.md) and `crates/keryx-relay/config.example.toml`.
- Do not commit peer IDs, tokens, private multiaddrs, or host paths into docs/examples.

## License

See `LICENSE` (and `LICENSE-APACHE` if present in tree).
