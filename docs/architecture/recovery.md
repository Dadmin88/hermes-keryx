# Startup Recovery

Keryx recovery must be deterministic and explicit. Recovery actions are durable events, not hidden cleanup.

## Current implemented slice

`keryx-store` now supports stale lease recovery in both `InMemoryStore` and `SqliteStore`:

- active leases have `leased_at_ms` and `expires_at_ms`
- expired active leases are discovered by `recover_stale_leases(now_ms)`
- non-terminal leased/running tasks are requeued to `Queued`
- a `RecoveryAction` task event is appended before the recovered task is returned
- terminal tasks are preserved and not requeued

## Future daemon startup behavior

On daemon startup, `keryxd` should:

1. open the store and run migrations
2. preserve terminal tasks
3. expire stale active leases
4. requeue recoverable queued/leased/running work according to retry policy
5. mark stale agents offline
6. append recovery events before status/doctor exposes recovered state

## Non-goals in this slice

This slice does not yet implement agent-offline recovery, retry-budget accounting, task deadlines, stuck queue detection, or daemon integration. It establishes the store-level primitive needed for those flows.
