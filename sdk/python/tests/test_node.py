from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from keryx import AgentCard, KeryxNode, Skill
from keryx.client import DaemonClient, PeerInfo


@pytest.fixture
def sample_card() -> AgentCard:
    return AgentCard(
        name="test-agent",
        description="unit test agent",
        skills=[Skill(id="demo-skill", description="demo")],
    )


@pytest.fixture
def mock_client(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    client = AsyncMock(spec=DaemonClient)
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.local_peer_id = AsyncMock(return_value="peer-test-1")
    client.list_peers = AsyncMock(
        return_value=[PeerInfo(peer_id="peer-test-1", connected=True, local=True)]
    )
    monkeypatch.setattr(
        "keryx.node.DaemonClient",
        lambda **kwargs: client,
    )
    return client


@pytest.mark.asyncio
async def test_node_start_stop_lifecycle(
    sample_card: AgentCard, mock_client: AsyncMock
) -> None:
    node = KeryxNode(sample_card, daemon_endpoint="unix:///tmp/keryx.sock")
    await node.start()
    assert node.peer_id == "peer-test-1"
    assert sample_card.peer_id == "peer-test-1"
    await node.stop()
    mock_client.connect.assert_awaited_once()
    mock_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_peer_id_before_start_raises(sample_card: AgentCard) -> None:
    node = KeryxNode(sample_card)
    with pytest.raises(RuntimeError, match="not started"):
        _ = node.peer_id


@pytest.mark.asyncio
async def test_node_forwards_explicit_node_token_to_client_factory(
    sample_card: AgentCard,
) -> None:
    client = AsyncMock(spec=DaemonClient)
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.local_peer_id = AsyncMock(return_value="peer-test-1")
    captured: dict[str, object] = {}

    def factory(**kwargs: object) -> AsyncMock:
        captured.update(kwargs)
        return client

    node = KeryxNode(
        sample_card,
        daemon_endpoint="unix:///tmp/keryx.sock",
        node_token="explicit-node-token",
        client_factory=factory,
    )
    await node.start()
    await node.stop()

    assert captured["node_token"] == "explicit-node-token"
