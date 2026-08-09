# Architecture Decision Records

These files preserve important architectural decisions made during Keryx development.

They are design history, not a substitute for the current runtime contract. When an older ADR conflicts with the implemented product surface, use [`../current-product.md`](../current-product.md) and the source code as the current authority.

Current ADRs:

- [0001 — Use Rust for the runtime](0001-use-rust-for-runtime.md)
- [0002 — Use tonic/protobuf for the control plane](0002-use-tonic-protobuf-for-control-plane.md)
- [0003 — Use SQLite for the local daemon store](0003-use-sqlite-for-local-daemon-store.md)
- [0004 — Relay-store decision](0004-use-postgres-for-relay-store.md)
- [0005 — Use at-least-once delivery](0005-use-at-least-once-delivery.md)
- [0006 — Start with a pure-Python SDK](0006-use-pure-python-sdk-first.md)

Some decisions describe an earlier intended architecture that later implementation may have refined or superseded. Preserve those records for historical context rather than rewriting them to look retroactively current.
