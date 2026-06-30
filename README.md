# Hermes Keryx

Hermes Keryx is a standalone Rust-native runtime substrate for the Hermes ecosystem. It is planned to provide a local daemon, relay, durable task transport, event log, persistence layer, SDK foundation, identity, recovery, and diagnostics.

Keryx is intentionally built outside the current Hermes Agency runtime first. Hermes Agency integration will be evaluated only after Keryx is independently tested, packaged, documented, and proven through demos and conformance tests.

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

## First validation commands

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
