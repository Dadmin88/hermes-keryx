# Phase 17: durable cross-node agent delivery

Status: **required for a complete Hermes Agency remote round trip**

Tracking issue: [#10](https://github.com/DeployFaith/hermes-keryx/issues/10)

## Why this phase exists

Keryx currently implements the transport path from a sender daemon to a destination daemon:

```text
sender SDK
  -> sender keryxd SendTask
  -> relay PublishTask
  -> destination keryx-node stream
  -> destination keryxd SubmitTask
  -> destination SQLite lifecycle row
```

That is a useful transport foundation, but it is not yet a complete Agent-to-Agent execution loop.

The destination Hermes Agency process cannot durably claim the delivered envelope and invoke its registered task handler. The origin process also cannot observe the remote terminal result or receive returned artifacts.

## Current boundary

Implemented today:

- daemon `SendTask` routing and relay publication
- relay node streams and offline mailbox delivery
- `keryx-node` frame consumption into destination `SubmitTask`
- relay skill registration and discovery
- durable four-state task lifecycle
- lease, heartbeat, completion, failure, cancellation, deadlines, and artifacts for locally addressed daemon tasks

Not yet complete:

- durable retention of the full submitted envelope
- worker discovery of the next pending task
- daemon-backed Python `IncomingTask` dispatch
- registered `on_task()` handler invocation through `serve_forever()`
- remote result and artifact routing back to the origin
- a terminal, remotely updated Python `TaskHandle`
- a repeatable two-daemon/two-edge-node Hermes Agency E2E

## Verified source gaps

### Envelope retention

`KeryxDaemon::SubmitTask` validates the `TaskEnvelope`, calculates its size, then persists only a `TaskRecord`. The prompt messages, metadata, correlation ID, origin identity, and requested capability are not recoverable after submission.

### Worker consumption

`ClaimTask` requires a known task ID. There is no atomic `ClaimNextTask`, pending-task stream, or envelope retrieval RPC for an Agency worker.

### Python receive loop

`KeryxNode.serve_forever()` currently keeps the process alive but does not consume work from the daemon. Registered handlers remain in memory and are not invoked by relay-delivered tasks.

### Result propagation

Completion and failure are local daemon operations. No authenticated result frame returns terminal status, metadata, errors, or artifacts to the origin daemon.

### Sender observation

The compatibility `TaskHandle` is created as submitted but is not attached to a status/result stream. `wait()` cannot complete from a remote worker result.

## Required protocol and storage contract

### Durable inbox envelope

Persist an inbox record atomically with the lifecycle row. It must contain:

- serialized `TaskEnvelope`
- authoritative origin peer ID
- destination peer ID
- correlation ID
- received timestamp
- requested skill/capability
- claim/delivery state

Authenticated transport identity must override or reject spoofable sender metadata.

### Atomic worker claim

Add a durable worker API, preferably:

```proto
rpc ClaimNextTask(ClaimNextTaskRequest) returns (ClaimNextTaskResponse);
```

The request should include worker ID, optional accepted skills, lease duration, and bounded long-poll timeout. The response should include the full envelope, authoritative sender identity, task ID, lease ID, and lease expiry.

The operation must atomically select and lease one pending task, support concurrent workers, preserve deterministic ordering, and recover safely after lease expiry.

### Daemon-backed Python incoming task

`KeryxNode.serve_forever()` should long-poll the claim API, construct an `IncomingTask`, invoke registered handlers, heartbeat during work, and complete/fail through the lease-aware daemon RPCs.

### Authenticated result route

A terminal result must travel back through the relay:

```text
destination worker
  -> destination daemon
  -> destination result publisher / edge node
  -> relay
  -> origin edge node
  -> origin daemon
  -> sender TaskHandle
```

The result contract must carry task/correlation identity, authenticated responder peer ID, canonical terminal status, failure reason, result metadata, and artifact references. Duplicate delivery must be idempotent.

### Sender status/result API

Provide a durable `WatchTask` stream or bounded `GetTaskStatus` / `GetTaskResult` polling API. `TaskHandle.wait()` must terminate for completed, failed, cancelled, or rejected tasks and expose returned artifacts.

## Runtime topology required for proof

The final harness must start:

- one relay and registry
- sender daemon and sender edge node
- receiver daemon and receiver edge node
- separate peer IDs, keys, ports, and data directories
- one harmless Hermes Agency task

The harness must clean up all child processes and preserve inspectable logs outside the repository.

## Definition of done

- [ ] Full envelopes survive relay delivery and daemon restart
- [ ] A receiver worker atomically claims the next compatible task
- [ ] Python `serve_forever()` invokes the real Agency handler
- [ ] Completion, failure, and cancellation propagate to the origin
- [ ] Sender `TaskHandle.wait()` receives terminal status and artifacts
- [ ] Sender identity is transport-authenticated and cannot be spoofed through metadata
- [ ] Two-daemon/two-edge-node harness is repeatable
- [ ] Rust workspace checks pass
- [ ] Python SDK tests pass
- [ ] Hermes Agency cross-process E2E passes

Until these checks pass, documentation and public communication should describe Keryx as having the durable relay and lifecycle foundation for cross-node work, not a completed remote Agency execution round trip.
