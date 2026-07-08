# keryx (Python SDK)

Python SDK for [Hermes Keryx](https://github.com/DeployFaith/hermes-keryx).

- **Package name:** `keryx`
- **Import name:** `keryx`
- Replaces the former `agentanycast` Python package for Hermes Agency node lifecycle when `agency.transport_backend: keryx`.

Hermes Agency also vendors this SDK under `Hermes_Agency/src/keryx/` for packaging. Prefer developing protocol/SDK changes here and syncing into Agency when cutting an integration slice.

## Install

```bash
cd sdk/python
python -m pip install -e ".[dev]"
```

Regenerate protobuf stubs after proto changes:

```bash
./scripts/generate_protos.sh
```

## Quick start

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
        relay_endpoint="127.0.0.1:51053",
    )
    await node.start()
    try:
        print("peer_id", getattr(node, "peer_id", None))
        agents = await node.discover("echo")
        print("discovered", agents)
    finally:
        await node.stop()

asyncio.run(main())
```

Public exports include:

- `KeryxNode`, `KeryxConfig`, `load_config`
- `AgentCard`, `Skill`
- `Task`, `TaskHandle`, `IncomingTask`, `TaskStatus`
- `TaskState`, `TaskResult`, `TaskArtifact`
- `peer_id_to_did_key`, `register_agent`, `deregister_agent`

## Environment

| Variable | Default / typical | Purpose |
|----------|-------------------|---------|
| `HERMES_KERYX_DAEMON_ENDPOINT` | `127.0.0.1:50051` or `http://127.0.0.1:50051` | `keryxd` gRPC |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | dual-run: `127.0.0.1:51053` | relay skill registry |
| `HERMES_KERYX_RELAY_CONFIG` | `~/.hermes/.keryx/relay.json` | relay config path |
| `HERMES_KERYX_DATA_DIR` | `~/.hermes/.keryx/data` | daemon data (when running binaries) |

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
