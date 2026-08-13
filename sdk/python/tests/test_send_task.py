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
from keryx.task import Message, Part


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
    assert kwargs["message"].parts[0].text == "hi"
    assert kwargs["message"].parts[0].media_type == "text/plain"
    await node.stop()


@pytest.mark.asyncio
async def test_remote_send_preserves_caller_execution_identity(
    started_node: tuple[KeryxNode, AsyncMock],
) -> None:
    node, client = started_node
    client.send_task = AsyncMock(
        return_value=daemon_pb2.SendTaskResponse(
            task_id=common_pb2.TaskId(value="execution-123"),
            status="submitted",
            routed_to="peer-remote",
            delivery_route="relay",
        )
    )
    await node.start()

    handle = await node.send_task(
        {"role": "user", "parts": [{"text": "execute once"}]},
        peer_id="peer-remote",
        task_id="execution-123",
        idempotency_key="execution-123",
    )

    assert handle.task_id == "execution-123"
    kwargs = client.send_task.await_args.kwargs
    assert kwargs["task_id"] == "execution-123"
    assert kwargs["idempotency_key"] == "execution-123"
    await node.stop()


@pytest.mark.asyncio
async def test_remote_send_preserves_one_binary_task_part(
    started_node: tuple[KeryxNode, AsyncMock],
) -> None:
    node, client = started_node
    client.send_task = AsyncMock(
        return_value=daemon_pb2.SendTaskResponse(
            task_id=common_pb2.TaskId(value="execution-binary-1"),
            status="submitted",
            routed_to="peer-remote",
            delivery_route="relay",
        )
    )
    await node.start()

    await node.send_task(
        {
            "role": "user",
            "parts": [
                {
                    "raw": b"exact-package-bytes",
                    "media_type": "application/vnd.hermes.fleet.agency-package.v1+tar",
                    "metadata": {"sha256": "sha256:" + "a" * 64},
                }
            ],
        },
        peer_id="peer-remote",
        task_id="execution-binary-1",
        idempotency_key="execution-binary-1",
    )

    message = client.send_task.await_args.kwargs["message"]
    assert message.parts[0].raw == b"exact-package-bytes"
    assert message.parts[0].text == ""
    assert message.parts[0].media_type == (
        "application/vnd.hermes.fleet.agency-package.v1+tar"
    )
    assert dict(message.parts[0].metadata) == {"sha256": "sha256:" + "a" * 64}
    await node.stop()


@pytest.mark.asyncio
async def test_remote_send_preserves_typed_binary_part(
    started_node: tuple[KeryxNode, AsyncMock],
) -> None:
    node, client = started_node
    await node.start()

    await node.send_task(
        Message(
            parts=[
                Part(
                    raw=b"typed-package",
                    media_type="application/x-test",
                    metadata={"digest": "sha256:test"},
                )
            ],
            metadata={"contract": "v1"},
        ),
        peer_id="peer-remote",
    )

    message = client.send_task.await_args.kwargs["message"]
    assert message.parts[0].raw == b"typed-package"
    assert message.parts[0].media_type == "application/x-test"
    assert dict(message.parts[0].metadata) == {"digest": "sha256:test"}
    assert dict(message.metadata) == {"contract": "v1"}
    await node.stop()


@pytest.mark.asyncio
async def test_remote_send_rejects_ambiguous_or_unscoped_binary_parts(
    started_node: tuple[KeryxNode, AsyncMock],
) -> None:
    node, client = started_node
    await node.start()

    with pytest.raises(ValueError, match="requires a media type"):
        await node.send_task(
            {"role": "user", "parts": [{"raw": b"bytes"}]},
            peer_id="peer-remote",
        )
    with pytest.raises(ValueError, match="text and raw"):
        await node.send_task(
            {
                "role": "user",
                "parts": [
                    {
                        "text": "ambiguous",
                        "raw": b"bytes",
                        "media_type": "application/vnd.hermes.fleet.agency-package.v1+tar",
                    }
                ],
            },
            peer_id="peer-remote",
        )
    with pytest.raises(ValueError, match="text must be a string"):
        await node.send_task(
            {"role": "user", "parts": [{"text": 7}]},
            peer_id="peer-remote",
        )
    with pytest.raises(ValueError, match="raw content must be bytes"):
        await node.send_task(
            {
                "role": "user",
                "parts": [{"raw": "bytes", "media_type": "application/x-test"}],
            },
            peer_id="peer-remote",
        )
    with pytest.raises(ValueError, match="keys and values must be strings"):
        await node.send_task(
            {
                "role": "user",
                "parts": [{"text": "text", "metadata": {"bad": 7}}],
            },
            peer_id="peer-remote",
        )
    for invalid_part, message in (
        ({"text": "text", "raw": False}, "raw content must be bytes"),
        ({"text": "text", "raw": 0}, "raw content must be bytes"),
        ({"text": "text", "raw": None}, "raw content must be bytes"),
        ({"text": "text", "media_type": False}, "media type"),
        ({"text": "text", "media_type": 0}, "media type"),
        ({"text": "text", "metadata": False}, "metadata must be a mapping"),
    ):
        with pytest.raises(ValueError, match=message):
            await node.send_task(
                {"role": "user", "parts": [invalid_part]},
                peer_id="peer-remote",
            )
    with pytest.raises(ValueError, match="message metadata must be a mapping"):
        await node.send_task(
            {"role": "user", "parts": [{"text": "text"}], "metadata": False},
            peer_id="peer-remote",
        )

    client.send_task.assert_not_awaited()
    await node.stop()


@pytest.mark.asyncio
async def test_remote_send_rejects_mismatched_receipt_identity(
    started_node: tuple[KeryxNode, AsyncMock],
) -> None:
    node, _client = started_node
    await node.start()

    with pytest.raises(RuntimeError, match="task identity"):
        await node.send_task(
            {"role": "user", "parts": [{"text": "execute once"}]},
            peer_id="peer-remote",
            task_id="execution-expected",
            idempotency_key="execution-expected",
        )
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
    ) as first_error:
        await handle.wait(timeout=1)
    assert handle.status.value == "completed"
    with pytest.raises(TaskResultUnavailableError) as second_error:
        await handle.wait(timeout=1)
    with pytest.raises(TaskResultUnavailableError) as refresh_error:
        await handle.refresh()
    assert second_error.value is first_error.value
    assert refresh_error.value is first_error.value
    client.get_task_result.assert_awaited_once()
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
