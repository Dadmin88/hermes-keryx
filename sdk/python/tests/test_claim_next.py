from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from keryx import ClaimedTask, KeryxNode
from hermes.keryx.v1 import common_pb2, daemon_pb2, task_pb2


@pytest.mark.asyncio
async def test_claim_next_builds_request_and_returns_envelope() -> None:
    envelope = task_pb2.TaskEnvelope(
        task_id=common_pb2.TaskId(value="task-next"),
        metadata={"skill": "backend"},
    )
    stub = AsyncMock()
    stub.ClaimNextTask = AsyncMock(
        return_value=daemon_pb2.ClaimNextTaskResponse(
            has_task=True,
            envelope=envelope,
            task_id=common_pb2.TaskId(value="task-next"),
            lease_id=common_pb2.LeaseId(value="lease-next"),
            worker_id=common_pb2.AgentId(value="worker-next"),
            leased_at_ms=10,
            expires_at_ms=20,
            status="running",
            retry_count=1,
            sender_peer_id="",
        )
    )
    node = KeryxNode(daemon_stub=stub, worker_id="worker-next")

    claimed = await node.claim_next(
        accepted_skill_ids=["backend"],
        wait_timeout_ms=500,
    )

    assert isinstance(claimed, ClaimedTask)
    assert claimed.has_task
    assert claimed.task_id == "task-next"
    assert claimed.envelope.metadata["skill"] == "backend"
    request = stub.ClaimNextTask.await_args.args[0]
    assert request.worker_id.value == "worker-next"
    assert list(request.accepted_skill_ids) == ["backend"]
    assert request.wait_timeout_ms == 500


@pytest.mark.asyncio
async def test_claim_next_can_return_no_work() -> None:
    stub = AsyncMock()
    stub.ClaimNextTask = AsyncMock(
        return_value=daemon_pb2.ClaimNextTaskResponse(has_task=False)
    )
    node = KeryxNode(daemon_stub=stub, worker_id="worker-empty")

    claimed = await node.claim_next()

    assert not claimed.has_task
    assert claimed.envelope is None
    assert claimed.task_id == ""
