# RFC 003: Task Lifecycle

## Statuses

Created, Accepted, Queued, AwaitingApproval, Leased, Running, AwaitingInput, Completed, Failed, Canceled, TimedOut, Rejected, DeadLettered.

## Legal transitions

- Created -> Accepted
- Accepted -> Queued
- Queued -> AwaitingApproval
- AwaitingApproval -> Queued
- AwaitingApproval -> Rejected
- Queued -> Leased
- Leased -> Running
- Running -> AwaitingInput
- AwaitingInput -> Running
- Running -> Completed
- Running -> Failed
- Running -> Canceled
- Running -> TimedOut
- Queued -> Canceled
- Queued -> DeadLettered
- Leased -> TimedOut
