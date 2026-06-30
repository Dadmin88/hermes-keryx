# Idempotency

Keryx uses idempotency keys to make retry after acknowledgement uncertainty safe.

## Rules

- Compatible reuse of the same idempotency key returns the original durable task.
- Conflicting reuse returns an explicit conflict.
- Duplicate completion returns the already-durable terminal task when compatible.
- Duplicate relay frames are safe and must not duplicate terminal task state.

## Current implementation status

`InMemoryStore::accept_task` and `SqliteStore::accept_task` index accepted tasks by idempotency key. A compatible duplicate returns the original task without appending duplicate events. A conflicting duplicate returns `StoreError::IdempotencyConflict`.
