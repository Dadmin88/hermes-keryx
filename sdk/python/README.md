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

- `KeryxNode`, `KeryxConfig`, `load_config`
- `AgentCard`, `Skill`
- `Task`, `TaskHandle`, `IncomingTask`, `TaskStatus`
- `TaskState`, `TaskResult`, `TaskArtifact`
- `peer_id_to_did_key`, `register_agent`, `deregister_agent`

## AgentAnycast-compatible transition helpers

The SDK keeps transition helpers so older Hermes Agency call sites can migrate incrementally:

```python
async with KeryxNode(card=card, daemon_endpoint="127.0.0.1:50051", registry_endpoint="127.0.0.1:51053") as node:
    await node.start()
    await node.register_skills(ttl_seconds=300)
    agents = await node.discover("echo", limit=1)
    handle = await node.send_task({"parts": [{"text": "hello"}]}, peer_id=agents[0]["peer_id"])
    print(handle.task_id)
    await node.deregister_skills()
    await node.stop()
```

Compatibility notes:

- `send_task(..., skill="...")` resolves the first registry match.
- `send_task(..., url="...")` is not implemented.
- The returned compatibility `TaskHandle` polls the origin daemon's durable result record; `wait()` receives remote terminal state and returned artifact descriptors/bounded text previews. Artifact bytes remain destination-local; general cross-node artifact-content retrieval is not implemented.
- `serve_forever()` claims compatible durable tasks, invokes registered `on_task()` handlers, heartbeats active leases, and persists completion/failure.
- Authenticated relay task/result routing and the permanent two-node proof were completed in [Phase 17](../../docs/phase17-cross-node-agent-delivery.md) by [PR #29](https://github.com/DeployFaith/hermes-keryx/pull/29).
- Registry tags exist in the protocol, but `Skill` and the high-level `register_skills()` helper do not yet propagate them.
- Registration is one-shot; the SDK does not automatically refresh before TTL expiry or deregister on shutdown.
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
