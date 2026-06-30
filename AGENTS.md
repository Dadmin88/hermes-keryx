# Hermes Keryx Agent Guide

## Project boundary

Hermes Keryx is a standalone Rust runtime workspace. Do not modify Hermes Agency while building Keryx unless Kyle explicitly asks for an integration pass.

## Engineering rules

- Prefer Rust-native runtime code in `crates/`.
- Keep `keryx-core` pure: no daemon, network, database, or filesystem dependencies.
- Write tests for lifecycle and persistence semantics before implementation where practical.
- Every accepted task must eventually map to a durable event-log contract.
- Do not commit or push unless Kyle explicitly asks.
- Do not introduce AgentAnycast coupling in this repo.

## Validation

Before reporting a coding slice complete, run the focused check and, when feasible:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

If a tool is unavailable, report the blocker instead of claiming success.
