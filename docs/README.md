# Hermes Keryx Documentation

This directory contains the durable public documentation for Hermes Keryx. Current product references describe implemented behavior; RFCs and ADRs preserve design history and may be superseded by later runtime behavior.

## Product and operations

- [Current product surface](current-product.md) — implemented components, lifecycle, storage, relay, registry, SDK, control operations, and limits.
- [Cross-node delivery](cross-node-delivery.md) — authenticated remote task/result round trip, acknowledgement, artifacts, and failure semantics.
- [Lifecycle, store, and daemon semantics](lifecycle-store-daemon-semantics.md) — detailed persistence and lifecycle contract.
- [Worker loop](worker-loop.md) — worker claims, leases, heartbeats, completion, and failure behavior.
- [Operations](operations.md) — operator deployment and runtime guidance.
- [Observability](observability.md) — health, metrics, and operational signals.
- [Legacy AgentAnycast migration](migration-from-agentanycast.md) — migration guidance for older installations.

## Architecture and contracts

- [System contract](contracts/system-contract.md)
- [Storage](architecture/storage.md)
- [Transactions](architecture/transactions.md)
- [Idempotency](architecture/idempotency.md)
- [Recovery](architecture/recovery.md)
- [Event log](architecture/event-log.md)

## Design history

- [Architecture decision records](adr/)
- [RFCs](rfc/)

Design-history documents should be read in context. When they conflict with the current product surface or source code, the implemented current contract wins.

## Documentation policy

Keep durable behavior and reusable operator guidance here. Put implementation checkpoints, completed-phase checklists, temporary proof summaries, one-off CI numbers, personal machine paths, and agent work notes in issues, pull requests, release notes, CI artifacts, or local workspace state instead of the public reference tree.
