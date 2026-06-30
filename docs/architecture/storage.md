# Storage Architecture

## Store traits

Keryx storage is split by responsibility:

- `AgentStore`
- `CapabilityStore`
- `TaskStore`
- `EventStore`
- `LeaseStore`
- `RouteStore`
- `PolicyStore`
- `OutboxStore`
- `InboxStore`
- `ArtifactStore`

The first concrete implementation was `InMemoryStore` for unit tests and examples. The local daemon store now has an initial `SqliteStore` backed by SQLx and a forward-only migration runner. PostgreSQL remains the target relay store.

## Local SQLite target

Default path:

```text
~/.hermes/keryx/keryx.db
```

Current migration creates the required table names:

```text
schema_migrations
agents
capabilities
tasks
task_events
leases
routes
approvals
outbox
inbox
artifacts
blobs
settings
idempotency_keys
```

Implemented behavior currently covers:

- task snapshots
- task events
- idempotency keys
- active leases
- stale lease recovery/requeue events

The other tables remain placeholders for upcoming agent, route, policy, outbox/inbox, and artifact slices.
