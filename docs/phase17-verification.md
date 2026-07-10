# Phase 17 verification

This branch contains the durable terminal-result store, authenticated task context, result-delivery outbox, daemon and relay result transport, durable Python `TaskHandle` observation, and the real two-node process harness.

The topology harness uses isolated SQLite databases, dynamic loopback ports, deterministic edge identities, a real Python receiver worker, and preserved logs on failure.

Current verification gate:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python -m pytest sdk/python/tests -q
python scripts/e2e_two_node.py --bin-dir target/debug
```
