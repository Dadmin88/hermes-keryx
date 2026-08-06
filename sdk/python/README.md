# keryx (Python SDK)

Python SDK for [Hermes Keryx](https://github.com/DeployFaith/hermes-keryx).

- **Package name:** `keryx`
- **Import name:** `keryx`
- **Python:** 3.11+
- Replaces the former `agentanycast` Python package for Hermes Agency node lifecycle when `agency.transport_backend: keryx`.

Hermes Agency may vendor this SDK under `Hermes_Agency/src/keryx/` for packaging. Prefer developing protocol/SDK changes here and syncing into Agency when cutting an integration slice.

## Install

```bash
cd sdk/python
python -m pip install -e ".[dev]"
```

Regenerate protobuf stubs after proto changes:

```bash
python -m grpc_tools.protoc \
  -I ../../proto \
  --python_out=. \
  --grpc_python_out=. \
  ../../proto/hermes/keryx/v1/*.proto
```

## Quick start: native daemon API

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
        print(await node.status())
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

This quickstart exercises a local daemon lifecycle. It is not a remote Agent-to-Agent execution example.

Native `KeryxNode` daemon methods:

- connection/status: `connect()`, `close()`, async context manager, `status()`, `doctor()`
- discovery: `peers()`, `skills(skill_id="", tags=None, limit=10)`
- lifecycle: `submit()`, `claim()`, `heartbeat()`, `complete()`, `fail()`, `cancel()`
- aliases: `submit_task()`, `claim_task()`, `heartbeat_task()`, `complete_task()`, `fail_task()`, `cancel_task()`

Public exports include:

- `KeryxNode`, `KeryxConfig`, `load_config`, `TaskResultUnavailableError`
- `AgentCard`, `Skill`
- `Task`, `TaskHandle`, `IncomingTask`, `TaskStatus`
- `TaskState`, `TaskResult`, `TaskArtifact`
- `peer_id_to_did_key`, `register_agent`, `deregister_agent`

## AgentAnycast-compatible transition helpers

The SDK keeps transition helpers so older Hermes Agency call sites can migrate incrementally:

```python
async with KeryxNode(card=card, daemon_endpoint="127.0.0.1:50051", registry_endpoint="127.0.0.1:51053") as node:
    await node.start()
    await node.start_registration(ttl_seconds=300)
    agents = await node.discover("echo", limit=1)
    handle = await node.send_task(
        {"parts": [{"text": "hello"}]},
        peer_id=agents[0]["peer_id"],
        deadline_ms=1_800_000_000_000,
    )
    print(handle.receipt)  # immutable task_id/status/routed_to/delivery_route acknowledgement
    await node.stop()
```

Compatibility notes:

- `send_task(..., skill="...")` resolves the first registry match.
- `send_task(..., deadline_ms=...)` accepts `0` for no execution deadline or a positive signed 64-bit absolute Unix epoch timestamp. It is not the relay delivery `timeout_ms`.
- `send_task(..., url="...")` is not implemented.
- Cross-node cancellation is not implemented. Canceling a remote-target handle fails closed at the origin daemon and does not claim that the remote executor stopped. Cancellation on an original `send_task()` handle is a client convenience, not a transferable server-issued capability: a reattached `task_handle(task_id)` refuses handle-level cancellation, while the trust-local daemon `CancelTask` API remains a separate authorization boundary.
- The returned compatibility `TaskHandle` polls the origin daemon's durable result record; `wait()` receives remote terminal state and canonical artifact descriptors. `node.task_handle(task_id)` reopens the durable status/result view after controller restart. Canceled and rejected outcomes remain stable during reattachment. Pre-v7 terminal rows that lack durable result data raise `TaskResultUnavailableError` instead of fabricating a result or reopening the task. Bounded artifact bytes traverse the authenticated result route only when the origin advertises `result_artifact_bytes_v1`; they can be retrieved with `get_artifact()` or written only to an explicit caller-selected path with `download_artifact(..., path=...)`.
- `serve_forever()` claims compatible durable tasks, invokes registered `on_task()` handlers, heartbeats active leases, and persists completion/failure.
- Authenticated relay task/result routing and the permanent two-node proof were completed in [Phase 17](../../docs/phase17-cross-node-agent-delivery.md) by [PR #29](https://github.com/DeployFaith/hermes-keryx/pull/29).
- `Skill.tags` round-trips through card dictionaries, registry publication, and discovery.
- Registry registration and deregistration are owner-authenticated. The SDK sends the local peer ID and `node_token=` (or `HERMES_KERYX_NODE_TOKEN`) only as gRPC metadata; the relay rejects missing, invalid, revoked, or body/metadata-mismatched credentials. Remote registry endpoints must use `https://`; plaintext is accepted only on loopback. Set `HERMES_KERYX_REGISTRY_CA_CERT` to a PEM CA file for a private certificate authority. Credential-bearing clients cannot inject an arbitrary registry channel; provide the endpoint and optional CA so the SDK can enforce transport security. Skill discovery remains read-only and does not require node credentials.
- `send_task()` retains the daemon's exact routing fields and relay acceptance metadata in an immutable `SubmissionReceipt`. A `relay_accepted` receipt proves relay acceptance only, not remote execution. Absolute deadlines require destination feature `absolute_deadlines_v1`; incapable or unknown peers fail explicitly rather than silently dropping the deadline.
- `AgentCard.protocol_features` round-trips through dictionaries, registration, discovery, and `get_card()`; current clients advertise `absolute_deadlines_v1` and `result_artifact_bytes_v1`.
- `register_skills()` remains a one-shot primitive. `start_registration()` registers immediately, then makes best-effort refresh attempts before TTL expiry and retries after rejection or registry errors. `registration_status()` reports lifecycle health and pending cleanup; a prolonged outage can still let the registry lease expire. Registry mutations use finite RPC deadlines. One stop budget spans refresh cancellation acknowledgement and deregistration. Work exceeding that budget continues as tracked cleanup, blocks restart, and preserves refresh-before-deregister ordering. During node shutdown, ownership of the registry client transfers to pending cleanup so accepted deregistration and client close can finish in order.
- `agentanycast` and `keryx.compat.agentanycast` modules emit a deprecation warning and re-export the Keryx-backed surface.

## Configuration

`load_config()` reads a TOML file from an explicit path, `HERMES_KERYX_CONFIG`, or `KERYX_CONFIG`, then applies environment overrides.

Supported TOML forms:

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

[relay]
endpoint = "127.0.0.1:51053"

[worker]
id = "worker-a"
default_lease_duration_ms = 120000

[defaults]
request_timeout_ms = 30000
lease_duration_ms = 120000
```

Environment variables:

| Variable | Default / typical | Purpose |
|----------|-------------------|---------|
| `HERMES_KERYX_CONFIG` / `KERYX_CONFIG` | unset | SDK TOML config path |
| `HERMES_KERYX_DAEMON_ENDPOINT` / `KERYX_DAEMON_ENDPOINT` | SDK default `unix://~/.hermes/keryx/run/keryx-daemon.sock`; repo examples use `127.0.0.1:50051` | `keryxd` gRPC endpoint |
| `HERMES_KERYX_REGISTRY_ENDPOINT` / `KERYX_REGISTRY_ENDPOINT` | dual-run: `127.0.0.1:51053` | relay skill registry endpoint |
| `HERMES_KERYX_NODE_TOKEN` | unset | node credential attached as gRPC metadata to registry mutations |
| `HERMES_KERYX_REGISTRY_CA_CERT` | unset (system roots) | PEM CA certificate used to verify an HTTPS registry endpoint |
| `HERMES_KERYX_RELAY_ENDPOINT` / `KERYX_RELAY_ENDPOINT` | unset | compatibility relay endpoint alias |
| `HERMES_KERYX_WORKER_ID` / `KERYX_WORKER_ID` | unset | default worker id for claim/heartbeat/complete/fail |
| `HERMES_KERYX_DEFAULT_LEASE_DURATION_MS` / `KERYX_DEFAULT_LEASE_DURATION_MS` | `0` (daemon default) | claim/heartbeat lease duration |
| `HERMES_KERYX_REQUEST_TIMEOUT_MS` / `KERYX_REQUEST_TIMEOUT_MS` | unset | caller-managed request timeout hint |

`grpc_target()` strips `http://`, `https://`, and `tcp://` for Python gRPC channels; `unix://` endpoints are passed through.

## Tests

```bash
cd sdk/python
python -m pip install -e ".[dev]"
pytest
```

## Notes for Agency integration

- Agency config field: `agency.transport_backend: keryx`
- Agency imports should be direct: `from keryx import KeryxNode, AgentCard, Skill`
- Prefer lazy imports inside Hermes plugin load paths so Hermes can still start if optional runtime pieces are missing
- Keep card/task APIs stable unless Agency is updated in the same integration pass
- Run the Phase 17 cross-process E2E before claiming the remote round trip on a changed runtime/SDK revision
