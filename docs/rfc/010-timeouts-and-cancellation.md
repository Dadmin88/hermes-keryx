# RFC 010: Timeouts and Cancellation

## Timeout types

- Client wait timeout: how long a caller waits for a response; it does not cancel work by itself.
- Task deadline: terminal deadline for a task.
- Lease timeout: ownership expiry for in-flight work.
- Agent handler timeout: local runtime guard around task handling.
- Relay mailbox expiry: retention window for offline delivery.
- Approval timeout: how long an approval-required task can wait.

## Cancellation

Cancellation is requested first and acknowledged by state transition. Queued tasks may move directly to `Canceled`. Running tasks require agent cooperation or deadline/lease timeout. Every cancellation request and terminal cancellation emits an event.
