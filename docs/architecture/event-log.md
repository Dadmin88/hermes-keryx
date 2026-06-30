# Event Log

Every task state transition emits a durable event. The event log is the audit source for lifecycle history and the recovery source when snapshots need verification.

## Event ordering

Events are stored per task with monotonically increasing sequence numbers. Consumers must treat sequence gaps as corruption until recovery verifies or repairs them.

## Replay

Replay starts with the first accepted/created event and applies transition events in sequence. Replayed state should match the stored task snapshot. Mismatch means either snapshot corruption or incomplete event history; doctor/recovery must report it explicitly.

## Recovery events

Recovery is observable. Store-level stale lease recovery appends `RecoveryAction` when a non-terminal leased/running task is returned to `Queued`. Terminal tasks are preserved and are not silently mutated.

## Current implementation status

`keryx-store` includes `InMemoryStore` and an initial SQLx-backed `SqliteStore`. Both store task snapshots plus per-task events and can replay the latest state from the event stream. Strict full transition-by-transition replay validation is a later recovery-hardening slice.
