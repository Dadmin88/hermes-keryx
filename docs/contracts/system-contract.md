# Hermes Keryx System Contract

## Core guarantees

- Every accepted task is durably recorded before acknowledgement.
- Every task state transition emits an event.
- Every terminal task state is durable.
- Duplicate dispatch with the same idempotency key returns the original task when compatible or a defined conflict when incompatible.
- Duplicate completion is safe.
- Daemon restart does not silently lose accepted queued work.
- Relay restart does not silently lose stored mailbox work.

## DispatchTask success

After `DispatchTask` returns success, Keryx has persisted the task identity, idempotency key, initial task snapshot, and `TaskCreated`/`TaskAccepted` event boundary required to recover after process death.

## Agent receive

After an agent receives a task, the lease or ownership transfer has already been persisted. If the daemon crashes after delivery, startup recovery can expire or requeue the lease according to policy.

## CompleteTask success

After `CompleteTask` returns success, the terminal task state, completion metadata, artifacts metadata, and `TaskCompleted` event are durable before the caller is notified.

## Retry and duplication

Keryx uses at-least-once delivery. Handlers must tolerate duplicate delivery. Task completion and relay frame handling are idempotent by key.

## Restart behavior

- Daemon restart: preserve terminal tasks, expire stale leases, requeue recoverable work, emit recovery events.
- Relay restart: preserve mailbox items and terminal relay events, resume node sessions from fresh authentication.
- Agent crash: leases eventually expire and recoverable work returns to queue or dead-letter according to retry policy.
- Client disconnect: accepted work continues unless an explicit task deadline or cancellation policy applies.

## Never silently lost

Accepted tasks, terminal states, mailbox items, and recovery decisions must never be silently discarded. If Keryx cannot prove state, it must surface a doctor warning/failure or a typed recovery event.
