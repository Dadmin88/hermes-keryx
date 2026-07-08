# keryx-py

Python SDK for [Hermes Keryx](https://github.com/NousResearch/Hermes_Keryx) — replaces the `agentanycast` package for Hermes Agency node lifecycle.

## Install

```bash
cd sdk/python
pip install -e ".[dev]"
```

Regenerate protobuf stubs after proto changes:

```bash
./scripts/generate_protos.sh
```

## Quick start

```python
import asyncio
from keryx import AgentCard, KeryxNode, Skill, Task
from keryx.task import Message, Part

card = AgentCard(
    name="demo-agent",
    description="Example Keryx node",
    skills=[Skill(id="echo", description="echo messages")],
)

async def main() -> None:
    node = KeryxNode(
        relay_endpoint="127.0.0.1:50053",
        card=card,
        daemon_endpoint="127.0.0.1:50051",
    )
    async with node:
        print("peer_id", node.peer_id)
        agents = await node.discover("echo")
        print("discovered", agents)
        if agents:
            task = Task(messages=[Message(role="user", parts=[Part(text="hello")])])
            handle = await node.send_task(agents[0]["peer_id"], task)
            print("sent", handle.task_id)

asyncio.run(main())
```

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `HERMES_KERYX_DAEMON_ENDPOINT` | `127.0.0.1:50051` | keryx-daemon gRPC |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | derived from relay / `127.0.0.1:50053` | relay skill registry |

## Tests

```bash
cd sdk/python && pip install -e ".[dev]" && pytest
```