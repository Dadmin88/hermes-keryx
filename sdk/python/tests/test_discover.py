from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from keryx import AgentCard, KeryxNode, Skill
from keryx.client import DaemonClient


@pytest.mark.asyncio
async def test_discover_via_mock_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    card = AgentCard(name="discoverer", skills=[Skill(id="find-me")])
    client = AsyncMock(spec=DaemonClient)
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.local_peer_id = AsyncMock(return_value="peer-discoverer")
    client.discover = AsyncMock(
        return_value=[
            {
                "peer_id": "peer-remote",
                "agent_name": "Remote",
                "agent_description": "remote agent",
                "skills": ["find-me"],
            }
        ]
    )
    monkeypatch.setattr("keryx.node.DaemonClient", lambda **kwargs: client)

    node = KeryxNode(card, registry_endpoint="127.0.0.1:50053")
    await node.start()
    found = await node.discover("find-me", limit=5)
    assert found[0]["peer_id"] == "peer-remote"
    client.discover.assert_awaited_once_with("find-me", tags=[], limit=5)
    await node.stop()


@pytest.mark.asyncio
async def test_discover_empty_when_no_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    card = AgentCard(name="solo", skills=[])
    client = AsyncMock(spec=DaemonClient)
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.local_peer_id = AsyncMock(return_value="peer-solo")
    client.discover = AsyncMock(return_value=[])
    monkeypatch.setattr("keryx.node.DaemonClient", lambda **kwargs: client)

    node = KeryxNode(card)
    await node.start()
    assert await node.discover("missing-skill") == []
    await node.stop()