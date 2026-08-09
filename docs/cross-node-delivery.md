# Cross-Node Delivery

This document describes Keryx's durable authenticated task/result round trip between two nodes.

The contract is transport-focused: Keryx proves peer identity, routes work, persists lifecycle state, and returns durable results. Application-level authorization remains the responsibility of the product using Keryx.

## Round trip

```text
origin client / Python SDK
  -> origin keryxd SendTask
  -> authenticated relay PublishTask
  -> destination keryx-node delivery stream
  -> destination keryxd SubmitRemoteTask
  -> durable task + immutable envelope
  -> destination worker ClaimNextTask
  -> handler execution
  -> durable terminal result + result-delivery outbox
  -> authenticated relay PublishResult
  -> origin keryxd IngestRemoteResult
  -> TaskHandle.wait() / task reattachment
```

## Submission

The origin daemon persists the task and its complete encoded envelope. Routing through the relay produces a submission receipt containing the actual task ID, routed peer, delivery route, and relay acceptance information when applicable.

A relay-accepted receipt proves that the authenticated relay accepted the frame for the intended destination. It does not prove that the destination executed the task.

Absolute execution deadlines are propagated only when the destination advertises the corresponding protocol feature. Keryx fails explicitly rather than silently removing an unsupported deadline.

## Authenticated relay publication

Task and result mutations use authenticated relay RPCs. The relay derives source ownership from configured node credentials and rejects missing, invalid, revoked, or mismatched credentials.

Request-body source identifiers cannot create authenticated identity.

For non-loopback control and registry endpoints, production deployments use TLS. Plaintext authenticated control is confined to loopback-only use.

## Destination ingestion

The destination edge node receives the relay frame and submits the immutable remote envelope to its local daemon with transport-authenticated context:

- authenticated source peer;
- local destination/executor identity;
- relay frame identity;
- original receive timestamp.

The daemon persists that context atomically with the remote task. Exact frame replay is idempotent. Changed transport context for the same immutable task/frame identity fails closed.

## Worker claim and execution

Workers claim compatible pending tasks through the daemon. Claims are lease-based and use a claim-generation fence so stale workers cannot complete or fail work after their lease has expired or been superseded.

A normal worker loop:

1. claims the next compatible task;
2. invokes the registered handler;
3. heartbeats while work remains active;
4. persists completion or failure;
5. releases/settles the lease through the daemon lifecycle.

The Python SDK's `serve_forever()` implements this pattern for registered handlers.

## Durable terminal results

Completion and failure persist terminal result state locally before cross-node delivery is attempted.

Remote result delivery uses a durable outbox. A result remains pending/retryable until the authenticated origin acknowledges the relay frame after `IngestRemoteResult` succeeds.

Timeout, relay restart, or response loss before that acknowledgement does not fabricate success. The outbox can retry with idempotent ingestion at the origin.

Result-delivery claims use their own lease/fencing value. An acknowledgement or failure attempt must match the active claim generation; stale claims fail closed.

## Result authentication

Terminal result publication requires authenticated node credentials when relay authentication is configured. A claimed executor field in a result body is not sufficient identity.

The relay binds the publication to the authenticated executor and intended authenticated destination. This keeps result provenance separate from arbitrary peer-produced metadata.

## Artifacts

Terminal results always support canonical artifact descriptors.

Byte-bearing remote result artifacts are capability-negotiated. The executor may attach bounded artifact bytes only when the authenticated origin advertises support for `result_artifact_bytes_v1`.

The origin verifies received artifact bytes before persistence. Remote logical names remain display metadata and cannot choose a local filesystem path. SDK download helpers write only to an explicit caller-selected path using bounded/atomic behavior.

Descriptor-only result delivery remains the compatibility baseline for peers that do not advertise byte-bearing artifact support.

## Deadlines

`TaskEnvelope.deadline_ms` is an absolute Unix epoch deadline.

Cross-node absolute deadlines are used only when the destination advertises `absolute_deadlines_v1`. Unsupported or unknown destinations fail before relay acceptance rather than receiving work with silently weakened semantics.

Deadline expiry is represented through the normal durable lifecycle/outcome model rather than inventing a transport-only success state.

## Cancellation

Local daemon cancellation is durable.

Cross-node cancellation is more restrictive. An origin-side cancellation record cannot prove that the remote worker observed cancellation and stopped active work. Keryx therefore fails closed where that proof is unavailable instead of reporting remote cancellation as successful.

When a destination locally cancels a remote-origin task, the normal durable canceled result can be returned to the authenticated origin through the result-delivery path.

## Offline mailbox behavior

The relay can hold bounded frames for temporarily disconnected peers and deliver them when the peer reconnects to the same running relay process.

Mailbox state is currently in memory and is not relay-restart durable.

Frames remain pending until the authenticated destination acknowledges the exact relay frame. Reconnect overflow that cannot be sent immediately remains queued subject to configured bounds. Old acknowledged delivery identities are eventually aged out, so a later publication may receive a new frame identity and cannot be removed by a stale acknowledgement.

## Discovery and protocol capabilities

The skill registry provides discovery metadata and protocol feature advertisement. Registry mutation ownership is authenticated; read-only skill discovery is separate from mutation authority.

Gossip/discovery information alone cannot assert security-sensitive protocol capabilities. Features that change delivery semantics are checked against authenticated/current registry state where required.

## Verification

The repository includes `scripts/e2e_two_node.py`, which starts an isolated relay/registry, two daemons, two edge nodes, and a real Python worker.

A changed revision should verify at least:

- authenticated sender identity;
- remote handler execution;
- durable full-envelope retention;
- lease/claim fencing;
- terminal result return;
- authenticated executor identity;
- idempotent result ingestion;
- canonical artifact descriptors;
- capability-gated artifact bytes and integrity checks;
- explicit-path artifact download;
- clean shutdown.

Recommended gate:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --bins

python -m pip install -e "sdk/python[dev]"
python -m pytest sdk/python/tests -q
python scripts/e2e_two_node.py --bin-dir target/debug
```

Run this gate against the exact revision being evaluated. Historical phase or merge evidence is not a substitute for current verification.
