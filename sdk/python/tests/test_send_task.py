from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from hermes.keryx.v1 import common_pb2, daemon_pb2

from keryx import AgentCard, KeryxNode, Skill
from keryx.client import DaemonClient


@pytest.fixture
def started_node(monkeypatch: pytest.MonkeyPatch) -> tuple[KeryxNode, AsyncMock]:
    card = AgentCard(name="sender", skills=[Skill(id="s")])
    client = AsyncMock(spec=DaemonClient)
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.local_peer_id = AsyncMock(return_value="peer-sender")
    client.send_task = AsyncMock(
        return_value=daemon_pb2.SendTaskResponse(
            task_id=common_pb2.TaskId(value="task-abc"),
            status="submitted",
            routed_to="peer-remote",
            delivery_route="local",
        )
    )
    monkeypatch.setattr("keryx.node.DaemonClient", lambda **kwargs: client)
    node = KeryxNode(card)
    return node, client


@pytest.mark.asyncio
async def test_send_task_to_mock_daemon(started_node: tuple[KeryxNode, AsyncMock]) -> None:
    node, client = started_node
    await node.start()
    handle = await node.send_task({"role": "user", "parts": [{"text": "hi"}]}, peer_id="peer-remote")
    assert handle.task_id == "task-abc"
    client.send_task.assert_awaited_once()
    kwargs = client.send_task.await_args.kwargs
    assert kwargs["target_peer_id"] == "peer-remote"
    assert kwargs["message_text"] == "hi"
    await node.stop()