# RFC 004: Delivery Semantics

## Delivery mode

Keryx uses at-least-once delivery. Agents must tolerate duplicate delivery and make handlers idempotent where side effects matter.

## Idempotency keys

Duplicate dispatch with the same idempotency key returns the original task when the request is compatible. Conflicting reuse returns a typed idempotency conflict.

## Retry behavior

Recoverable failures can be retried until retry budget or deadline is exhausted. Exhausted work becomes `DeadLettered` with an event explaining the cause.

## Duplicate completion

Duplicate completion is safe. If the terminal result is compatible, Keryx returns the durable terminal task; incompatible terminal mutation is rejected.

## Lease expiry

A stale lease moves to `TimedOut` or returns to `Queued` according to retry policy. Recovery emits events before exposing recovered state.

## Mailbox delivery

Relay mailbox items are persisted before acknowledgement. Target nodes may fetch mailbox items after reconnect. Duplicate reconnect must not duplicate terminal tasks.

## Acknowledgement failure

If acknowledgement fails after persistence, retry may return the same durable task through idempotency lookup rather than creating a second task.
