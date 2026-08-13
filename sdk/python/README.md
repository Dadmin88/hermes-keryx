# keryx Python SDK

Python SDK for [Hermes Keryx](https://github.com/Dadmin88/hermes-keryx).

- **Package name:** `keryx`
- **Import name:** `keryx`
- **Python:** 3.11+
- **License:** Apache-2.0

The SDK is a client and worker interface to `keryxd` plus supported relay/registry surfaces. It can be used by Hermes Fleet, other Hermes integrations, or standalone applications without depending on a specific higher-level product.

## Install

```bash
cd sdk/python
python -m pip install -e ".[dev]"
```

After protobuf changes, regenerate Python stubs from the repository definitions:

```bash
python -m grpc_tools.protoc \
  -I ../../proto \
  --python_out=. \
  --grpc_python_out=. \
  ../../proto/hermes/keryx/v1/*.proto
```

## Local daemon quickstart

```python
import asyncio
from keryx import AgentCard, KeryxNode, Skill

card = AgentCard(
    name="demo-agent",
    description="Example Keryx node",
    skills=[Skill(id="echo", description="echo messages")],
)

async def main() -> None:
    node = KeryxNode(
        card=card,
        daemon_endpoint="127.0.0.1:50051",
        registry_endpoint="127.0.0.1:51053",
        worker_id="worker-a",
    )
    await node.connect()
    try:
        state = await node.submit(message="hello", metadata={"skill": "echo"})
        lease = await node.claim(state.task_id)
        await node.heartbeat(state.task_id, lease.lease_id)
        result = await node.complete(
            state.task_id,
            lease.lease_id,
            result_metadata={"ok": "true"},
        )
        print(result.status)
    finally:
        await node.close()

asyncio.run(main())
```

This example exercises the local durable daemon lifecycle. For the authenticated remote round trip, see [Cross-node delivery](../../docs/cross-node-delivery.md) and `scripts/e2e_two_node.py`.

## Core API

Native `KeryxNode` methods include:

- connection/status: `connect()`, `close()`, async context manager, `status()`, `doctor()`;
- discovery: `peers()`, `skills(...)`;
- lifecycle: `submit()`, `claim()`, `claim_next()`, `heartbeat()`, `complete()`, `fail()`, `cancel()`;
- task observation/reattachment through durable task handles;
- artifact retrieval and explicit-path download;
- registry registration helpers.

Public exports include:

- `KeryxNode`, `KeryxConfig`, `load_config`;
- `Task`, `TaskHandle`, `IncomingTask`, `TaskStatus`;
- `TaskState`, `TaskResult`, `TaskArtifact`;
- `AgentCard`, `Skill`;
- identity and registration helpers;
- `TaskResultUnavailableError` for historical terminal rows that do not contain durable result data.

## Remote worker loop

The SDK can run a durable worker with registered handlers.

A worker:

1. claims the next compatible daemon task;
2. invokes the registered handler;
3. heartbeats the active lease;
4. persists completion or failure;
5. participates in the authenticated result-delivery path for remote-origin work.

The worker relies on daemon claim/lease fencing. It does not treat in-process handler ownership as the durable source of truth.

## Remote submission and task handles

`send_task()` submits through the local daemon and returns a `TaskHandle` containing the daemon's actual submission receipt.

The receipt preserves transport facts such as:

- task ID;
- accepted status;
- routed peer;
- delivery route;
- relay acceptance information when available.

A relay-accepted receipt proves relay acceptance, not remote execution.

`TaskHandle.wait()` observes the origin daemon's durable result record. `node.task_handle(task_id)` can reopen status/result observation after the controller process restarts.

Controllers that must recover an uncertain submission can preassign both
`task_id` and `idempotency_key` to `send_task()`. Keryx carries those exact
identities into its durable envelope and rejects a response whose task identity
does not match the caller's request. Recovery should inspect the preassigned
task ID before deciding whether any submission is still required.

Historical terminal rows that predate durable result storage raise `TaskResultUnavailableError` rather than fabricating a terminal result.

## Deadlines

`send_task(..., deadline_ms=...)` accepts `0` for no execution deadline or a positive signed 64-bit absolute Unix epoch timestamp.

The execution deadline is separate from the client's transport/request timeout. Cross-node deadlines require destination support for `absolute_deadlines_v1`; unknown or unsupported destinations fail explicitly rather than silently dropping the deadline.

## Cancellation

Local daemon cancellation is supported.

Cross-node cancellation remains deliberately fail-closed where the origin cannot prove that the destination worker observed cancellation and stopped active work. A local origin record is not sufficient evidence of remote termination.

A reattached task handle is an observation surface and does not acquire a transferable cancellation authority simply from knowing a task ID.

## Artifacts

Task handles return canonical artifact descriptors with terminal results.

Bounded byte-bearing result artifacts traverse the authenticated result route only when the origin advertises `result_artifact_bytes_v1`.

Use SDK retrieval helpers to fetch verified bytes. File download requires an explicit caller-selected destination path; remote logical artifact names are metadata and do not select local filesystem paths.

## Registry authentication

Registration and deregistration are authenticated mutations.

The SDK supplies the local peer identity and node token as gRPC metadata. The relay rejects missing, invalid, revoked, or body/metadata-mismatched credentials.

For non-loopback registry endpoints:

- use `https://`;
- use normal certificate verification;
- set `HERMES_KERYX_REGISTRY_CA_CERT` when a private CA is required.

Read-only skill discovery is separate from registry mutation authority.

## Registration lifecycle

`register_skills()` is a one-shot registration primitive.

`start_registration()` adds an opt-in refresh lifecycle:

- registers immediately;
- refreshes before TTL expiry;
- retries after rejection or transient registry errors;
- exposes health/cleanup state through `registration_status()`;
- uses finite registry RPC deadlines;
- preserves refresh-before-deregister ordering during shutdown.

A prolonged registry outage can still allow the registry lease to expire. The lifecycle reports that condition rather than claiming permanent registration.

## Compatibility helpers

The SDK retains AgentAnycast-era compatibility helpers so older consumers can migrate incrementally. These are transition surfaces, not a separate transport implementation.

Examples include:

- `start()` / `stop()`;
- `discover()`;
- `send_task()`;
- `register_skills()` / `deregister_skills()`;
- `serve_forever()`;
- deprecated `agentanycast` compatibility modules that re-export the Keryx-backed surface.

New integrations should prefer the native Keryx API and current product contracts rather than copying assumptions from an older AgentAnycast integration.

## Configuration

`load_config()` reads TOML from an explicit path, `HERMES_KERYX_CONFIG`, or `KERYX_CONFIG`, then applies environment overrides.

Example:

```toml
daemon_endpoint = "127.0.0.1:50051"
registry_endpoint = "127.0.0.1:51053"
worker_id = "worker-a"
default_lease_duration_ms = 120000
request_timeout_ms = 30000

[daemon]
endpoint = "127.0.0.1:50051"

[registry]
endpoint = "127.0.0.1:51053"

[worker]
id = "worker-a"
```

Common environment variables:

| Variable | Purpose |
| --- | --- |
| `HERMES_KERYX_CONFIG` / `KERYX_CONFIG` | SDK config path |
| `HERMES_KERYX_DAEMON_ENDPOINT` / `KERYX_DAEMON_ENDPOINT` | daemon gRPC endpoint |
| `HERMES_KERYX_REGISTRY_ENDPOINT` / `KERYX_REGISTRY_ENDPOINT` | relay registry endpoint |
| `HERMES_KERYX_NODE_TOKEN` | authenticated registry/relay mutation credential |
| `HERMES_KERYX_REGISTRY_CA_CERT` | optional PEM CA for HTTPS registry/control endpoints |
| `HERMES_KERYX_RELAY_ENDPOINT` / `KERYX_RELAY_ENDPOINT` | relay endpoint alias |
| `HERMES_KERYX_WORKER_ID` / `KERYX_WORKER_ID` | default worker ID |
| `HERMES_KERYX_DEFAULT_LEASE_DURATION_MS` / `KERYX_DEFAULT_LEASE_DURATION_MS` | default claim/heartbeat lease duration |
| `HERMES_KERYX_REQUEST_TIMEOUT_MS` / `KERYX_REQUEST_TIMEOUT_MS` | caller request timeout |

## Tests

```bash
cd sdk/python
python -m pip install -e ".[dev]"
pytest
```

For cross-process transport changes, also run the repository's authenticated two-node integration test from the repository root.

### Local daemon RPC authorization

When `keryxd` listens on `HERMES_KERYX_DAEMON_ADDR`, `HERMES_KERYX_DAEMON_TOKEN` is required. Sensitive reads, task/result dequeue, artifact access, task lifecycle mutation, remote ingress, and `SendTask` use the same `Authorization: Bearer ...` credential. Status, doctor, liveness, readiness, peer listing, and skill discovery remain read-only public-local diagnostics. Running-task cancellation also requires the exact active lease id and worker id; the daemon token alone does not grant lease ownership.
