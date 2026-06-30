# RFC 002: Runtime Topology

## Local-only mode

Agents and clients connect to `keryxd` on the same machine. The daemon owns local registration, capabilities, queues, leases, events, and SQLite persistence.

## Relay mode

`keryxd` maintains an outbound connection to `keryx-relay`. The relay owns cross-node presence, capability publication, route resolution, offline mailbox, and PostgreSQL-backed relay state.

## Roles

- SDK: ergonomic client/agent API over the Keryx daemon protocol.
- Daemon: local source of truth for node task state and event log.
- Relay: cross-node message broker and mailbox, not a replacement for local durability.
- Agent process: registers a manifest, heartbeats, subscribes to inbound tasks, and completes/fails/updates work.
- SQLite: local daemon durable state.
- PostgreSQL: relay durable state.
