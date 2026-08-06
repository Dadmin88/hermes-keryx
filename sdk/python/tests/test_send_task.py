from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import AsyncMock

import grpc
import pytest

from hermes.keryx.v1 import common_pb2, daemon_pb2

from keryx import (
    AgentCard,
    KeryxNode,
    Skill,
    SubmissionReceipt,
    TaskResultUnavailableError,
)
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
            delivery_route="relay",
            relay_frame_id="relay-frame-abc",
            authenticated_source_peer_id="peer-sender",
            accepted_destination_peer_id="peer-remote",
            accepted_route="relay",
            accepted_at_ms=1_800_000_000_000,
        )
    )
    monkeypatch.setattr("keryx.node.DaemonClient", lambda **kwargs: client)
    node = KeryxNode(card)
    return node, client


@pytest.mark.asyncio
async def test_send_task_to_mock_daemon(
    started_node: tuple[KeryxNode, AsyncMock],
) -> None:
    node, client = started_node
    await node.start()
    handle = await node.send_task(
        {"role": "user", "parts": [{"text": "hi"}]}, peer_id="peer-remote"
    )
    assert handle.task_id == "task-abc"
    expected_receipt = SubmissionReceipt(
        task_id="task-abc",
        status="submitted",
        routed_to="peer-remote",
        delivery_route="relay",
        relay_frame_id="relay-frame-abc",
        authenticated_source_peer_id="peer-sender",
        accepted_destination_peer_id="peer-remote",
        accepted_route="relay",
        accepted_at_ms=1_800_000_000_000,
    )
    assert handle.receipt == expected_receipt
    with pytest.raises(FrozenInstanceError):
        expected_receipt.status = "mutated"  # type: ignore[misc]
    client.send_task.assert_awaited_once()
    kwargs = client.send_task.await_args.kwargs
    assert kwargs["target_peer_id"] == "peer-remote"
    assert kwargs["message_text"] == "hi"
    await node.stop()


@pytest.mark.asyncio
async def test_remote_terminal_without_durable_result_raises_stable_error(
    started_node: tuple[KeryxNode, AsyncMock],
) -> None:
    node, client = started_node
    client.get_task_result = AsyncMock(
        return_value=daemon_pb2.GetTaskResultResponse(
            found=False,
            status="completed",
            terminal_result_unavailable=True,
            data_unavailable_reason="terminal_result_unavailable",
        )
    )
    await node.start()
    handle = await node.send_task(
        {"role": "user", "parts": [{"text": "legacy"}]},
        peer_id="peer-remote",
    )
    with pytest.raises(
        TaskResultUnavailableError,
        match="terminal_result_unavailable",
    ):
        await handle.wait(timeout=1)
    assert handle.status.value == "completed"
    with pytest.raises(TaskResultUnavailableError):
        await handle.wait(timeout=1)
    with pytest.raises(TaskResultUnavailableError):
        await handle.refresh()
    await node.stop()


class _UnknownPeerRpcError(Exception):
    def code(self):
        return grpc.StatusCode.NOT_FOUND

    def details(self):
        return "unknown peer: peer-discovered"


@pytest.mark.asyncio
async def test_skill_send_reports_keryx_cross_node_capability_gap(
    started_node: tuple[KeryxNode, AsyncMock],
) -> None:
    node, client = started_node
    client.discover = AsyncMock(
        return_value=[
            {
                "peer_id": "peer-discovered",
                "agent_name": "remote",
                "skills": ["remote-skill"],
            }
        ]
    )
    client.send_task = AsyncMock(side_effect=_UnknownPeerRpcError())

    await node.start()
    with pytest.raises(NotImplementedError, match="registry-discovered peers"):
        await node.send_task(
            {"role": "user", "parts": [{"text": "hi"}]}, skill="remote-skill"
        )
    await node.stop()
