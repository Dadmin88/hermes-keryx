# RFC 007: Backpressure and Limits

Keryx exposes configurable limits for:

- max queued tasks globally
- max queued tasks per agent
- max running tasks per agent
- max relay mailbox items per node
- max payload size
- max artifact size
- max event stream subscribers
- max retry count
- max lease duration
- max heartbeat silence

Exceeded limits produce typed errors or policy events. Keryx must not silently drop tasks, artifacts, events, or mailbox items due to backpressure.
