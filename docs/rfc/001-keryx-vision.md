# RFC 001: Keryx Vision

## What Keryx is

Hermes Keryx is a standalone Rust-native runtime substrate for durable agent task transport. It provides the daemon, relay, task state machine, event log, persistence, leases, queues, SDK foundation, identity, recovery, and diagnostics.

## What Keryx owns

- Local daemon runtime and durable local task queue.
- Relay runtime and offline mailbox.
- Task lifecycle semantics and event stream.
- Protocol contracts and SDK foundations.
- Runtime status, doctor, recovery, and operational surfaces.

## What Keryx does not own

Keryx does not own Hermes Agency orchestration, specialist profiles, model routing, Fabric UI, Discord/GPT intake, or product-level workflows.

## Why Keryx exists

Keryx exists to make Hermes task delivery durable, inspectable, crash-recoverable, and language-neutral before any Hermes Agency transport replacement is considered.

## What v1 complete means

Keryx v1 is complete when local and relay task dispatch, persistence, idempotency, policy, observability, SDKs, packaging, docs, demos, conformance tests, and chaos tests are independently proven.
