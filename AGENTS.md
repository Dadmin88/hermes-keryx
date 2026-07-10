# Hermes Keryx Agent Guide

## Project boundary

Hermes Keryx is a **standalone Rust runtime workspace** plus Python SDK.

- Default work stays inside this repository.
- Do **not** modify Hermes Agency unless Kyle explicitly asks for an integration pass.
- Do **not** introduce new AgentAnycast coupling into this repo.
- Keep this repo PR-ready for eventual upstream submission to Nous (runtime only, not full Hermes Agency).
- Treat [docs/current-product.md](docs/current-product.md) as the canonical documentation summary of the implemented product.

## Product intent

Keryx is the durable task/runtime transport substrate for Hermes:

- `keryxd` — local daemon + SQLite lifecycle store
- `keryx-relay` — libp2p relay, health, skill registry, security
- `keryx-node` — edge node binary for relay bootstrap + skill registration
- `keryx` CLI — operator/status/doctor/task/artifact/relay/node verbs
- `sdk/python` — package/import name `keryx` (`KeryxNode`, cards, tasks)

Hermes Agency consumes Keryx as primary transport. Agency may vendor a copy of the Python SDK under `src/keryx/`; **this repo remains source of truth** for Rust crates, protobufs, and protocol evolution.

## Engineering rules

- Prefer Rust-native runtime code in `crates/`.
- Keep `keryx-core` pure: no daemon, network, database, or filesystem dependencies.
- Write tests for lifecycle and persistence semantics before implementation where practical.
- Every accepted task must eventually map to a durable event-log contract.
- Schema changes must bump store schema version and update tests (current: **v5**).
- Dual-run ports must not collide with common legacy AgentAnycast defaults (4001 / 50052).
- Do not commit secrets, real peer IDs, private hostnames, maintainer-local absolute paths, or private multiaddrs.
- Do not claim CI green without running the relevant validation commands.

## Current implemented surface (do not regress)

| Area | Notes |
|------|-------|
| Lifecycle store | Four-state lifecycle, leases, recovery, artifacts, cancel/deadlines, schema v5 |
| Daemon RPCs | Status/doctor/liveness/readiness, submit/claim/heartbeat/complete/fail/cancel, artifacts, send/list/discover |
| Relay | Transport, offline mailbox, registry/gossip, health, security allowlist |
| Policy | Node keys/tokens, permissions, routing policy/audit |
| CLI | `status`, `doctor`, `task`, `artifact`, `relay`, `node` |
| Python SDK | `KeryxNode`, AgentCard/Skill, lifecycle methods, registration helpers, AgentAnycast-compatible shims |
| Ops scripts | `scripts/migrate-to-keryx.sh`, `scripts/keryx-dual-run.sh` |

## Ports and paths (operator defaults)

| Component | Default |
|-----------|---------|
| Daemon gRPC | `127.0.0.1:50051` |
| Relay code default health gRPC | `127.0.0.1:50052` |
| Relay code default registry gRPC | `127.0.0.1:50053` |
| Relay code default HTTP health | `127.0.0.1:8081` |
| Relay code default libp2p | `0.0.0.0:4001` TCP/QUIC |
| Dual-run relay health gRPC | `127.0.0.1:51052` |
| Dual-run registry gRPC | `127.0.0.1:51053` |
| Dual-run libp2p | `127.0.0.1:4101` |
| Runtime root for dual-run | `~/.hermes/.keryx/` |
| Dual-run logs/pids | `~/.hermes/.keryx/logs/`, `~/.hermes/.keryx/run/` |

Env prefix: `HERMES_KERYX_*`. See [docs/current-product.md](docs/current-product.md) for exact env variables and aliases.

## Validation before reporting done

Always run the focused check for changed crates. For merges/releases:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

For Python SDK changes:

```bash
cd sdk/python
python -m pip install -e ".[dev]"
pytest
```

For ops scripts:

```bash
bash -n scripts/migrate-to-keryx.sh
bash -n scripts/keryx-dual-run.sh
./scripts/migrate-to-keryx.sh --dry-run
./scripts/keryx-dual-run.sh --status
```

If a tool is unavailable, report the blocker instead of claiming success.

## Documentation expectations

When behavior changes, update the relevant docs in the same change:

- `README.md` — operator overview + status table
- `docs/current-product.md` — canonical implemented product map
- `AGENTS.md` — contributor boundaries (this file)
- `sdk/python/README.md` — SDK install/API
- `docs/migration-from-agentanycast.md` — operator migration
- `docs/operations.md` / `docs/observability.md` / semantics docs when runtime behavior changes

Keep examples generic: use placeholders for peer IDs, hostnames, multiaddrs, and absolute paths.

## Commit / push policy

Follow Kyle's current autonomy policy for this workspace. Prefer validated commits; do not open noisy draft PRs. Never force-push `main`.
