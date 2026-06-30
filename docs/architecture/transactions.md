# Transaction Boundaries

Keryx persists state before acknowledging ownership-changing operations.

## Minimum boundaries

- `DispatchTask`: persist the task row, idempotency record, and task-created/accepted event before returning success.
- `SubscribeTasks`/leasing: persist the lease and `TaskLeased` event before streaming a task to an agent.
- `CompleteTask`: persist terminal task state and `TaskCompleted` event before notifying the sender.
- Relay send: persist an outbox frame before attempting network delivery.
- Relay receive: persist mailbox item or terminal state before acknowledging the relay frame.
- Startup recovery: persist recovery events before exposing recovered state.

## Current SQLite boundaries

`SqliteStore::accept_task` writes the task row, idempotency-key row, and first task event in one SQL transaction before returning.

`SqliteStore::transition_task` updates the task snapshot and appends the transition event in one SQL transaction.

`SqliteStore::lease_task` writes the lease row, updates task state to `Leased`, and appends `TaskLeased` in one SQL transaction before returning the leased task.

`SqliteStore::recover_stale_leases` updates recoverable tasks back to `Queued`, deactivates stale leases, and appends `RecoveryAction` before returning recovered tasks.

## Failure model

If acknowledgement fails after persistence, retry must discover the prior durable record through task ID or idempotency lookup. Creating a second task for the same idempotent dispatch is a bug.
