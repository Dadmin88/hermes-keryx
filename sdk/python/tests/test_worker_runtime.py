from __future__ import annotations

import asyncio
from collections import deque
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hermes.keryx.v1 import common_pb2, daemon_pb2, task_pb2
from keryx import AgentCard, KeryxNode, Skill
from keryx.task import Artifact, Part


def claimed_response(task_id: str = "task-worker") -> daemon_pb2.ClaimNextTaskResponse:
    return daemon_pb2.ClaimNextTaskResponse(
        has_task=True,
        envelope=task_pb2.TaskEnvelope(
            task_id=common_pb2.TaskId(value=task_id),
            correlation_id=common_pb2.CorrelationId(value="context-worker"),
            messages=[
                task_pb2.TaskMessage(
                    parts=[
                        task_pb2.TaskMessagePart(
                            media_type="text/plain",
                            text="perform the worker task",
                            metadata={"part": "prompt"},
                        )
                    ],
                    metadata={"role": "user"},
                )
            ],
            metadata={"skill": "backend", "custom": "value"},
        ),
        task_id=common_pb2.TaskId(value=task_id),
        lease_id=common_pb2.LeaseId(value=f"lease-{task_id}"),
        worker_id=common_pb2.AgentId(value="worker-runtime"),
        leased_at_ms=1_000,
        expires_at_ms=2_000,
        status="running",
    )


class Stub:
    def __init__(self, responses: list[daemon_pb2.ClaimNextTaskResponse]) -> None:
        self.responses = deque(responses)
        self.completed = asyncio.Event()
        self.failed = asyncio.Event()
        self.heartbeat_seen = asyncio.Event()
        self.ClaimNextTask = AsyncMock(side_effect=self._claim_next)
        self.CompleteTask = AsyncMock(side_effect=self._complete)
        self.FailTask = AsyncMock(side_effect=self._fail)
        self.Heartbeat = AsyncMock(side_effect=self._heartbeat)

    async def _claim_next(self, _request):
        if self.responses:
            return self.responses.popleft()
        await asyncio.sleep(0.01)
        return daemon_pb2.ClaimNextTaskResponse(has_task=False)

    async def _complete(self, request):
        self.completed.set()
        return daemon_pb2.CompleteTaskResponse(
            task_id=request.task_id,
            status="completed",
            duration_ms=request.duration_ms,
            result_metadata=request.result_metadata,
            output_artifacts=request.output_artifacts,
        )

    async def _fail(self, request):
        self.failed.set()
        return daemon_pb2.FailTaskResponse(
            task_id=request.task_id,
            status="failed",
            duration_ms=request.duration_ms,
            error_reason=request.error_reason,
            failure_metadata=request.failure_metadata,
        )

    async def _heartbeat(self, request):
        self.heartbeat_seen.set()
        return daemon_pb2.HeartbeatResponse(
            lease_id=request.lease_id,
            expires_at_ms=10_000,
        )


def running_node(stub: Stub, **kwargs) -> KeryxNode:
    card = AgentCard(
        name="worker-agent",
        skills=[Skill(id="backend", description="backend work")],
    )
    node = KeryxNode(
        card,
        daemon_stub=stub,
        worker_id="worker-runtime",
        **kwargs,
    )
    node._client = SimpleNamespace(close=AsyncMock())
    node._running = True
    node._peer_id = "peer-worker"
    return node


async def stop_node(node: KeryxNode, serve_task: asyncio.Task[None]) -> None:
    await node.stop()
    await serve_task


@pytest.mark.asyncio
async def test_worker_dispatches_full_task_and_completes() -> None:
    stub = Stub([claimed_response()])
    node = running_node(stub)

    @node.on_task
    async def handler(task) -> None:
        assert task.context_id == "context-worker"
        assert task.target_skill_id == "backend"
        assert task.metadata["custom"] == "value"
        assert task.messages[0].role == "user"
        assert task.messages[0].parts[0].text == "perform the worker task"
        assert task.messages[0].parts[0].metadata["part"] == "prompt"
        await task.update_status("working")
        await task.complete(
            [
                Artifact(
                    artifact_id="answer-1",
                    name="answer.txt",
                    parts=[Part(text="worker completed the task")],
                )
            ]
        )

    serving = asyncio.create_task(node.serve_forever())
    await asyncio.wait_for(stub.completed.wait(), timeout=2)
    await stop_node(node, serving)

    claim_request = stub.ClaimNextTask.await_args_list[0].args[0]
    assert list(claim_request.accepted_skill_ids) == ["backend"]
    complete_request = stub.CompleteTask.await_args.args[0]
    assert complete_request.result_metadata["result_text"] == "worker completed the task"
    assert complete_request.output_artifacts[0].path == "answer.txt"


@pytest.mark.asyncio
async def test_worker_keeps_heartbeat_alive_after_handler_queues_work() -> None:
    stub = Stub([claimed_response("task-queued")])
    node = running_node(stub, heartbeat_interval_ms=10)
    captured = asyncio.Future()

    @node.on_task
    async def handler(task) -> None:
        captured.set_result(task)

    serving = asyncio.create_task(node.serve_forever())
    incoming = await asyncio.wait_for(captured, timeout=1)
    await asyncio.wait_for(stub.heartbeat_seen.wait(), timeout=1)
    assert not stub.completed.is_set()
    await incoming.complete([Artifact(name="queued.txt", parts=[Part(text="later result")])])
    await asyncio.wait_for(stub.completed.wait(), timeout=1)
    await stop_node(node, serving)
    assert stub.Heartbeat.await_count >= 1


@pytest.mark.asyncio
async def test_handler_exception_fails_task() -> None:
    stub = Stub([claimed_response("task-error")])
    node = running_node(stub)

    @node.on_task
    async def handler(_task) -> None:
        raise RuntimeError("handler exploded")

    serving = asyncio.create_task(node.serve_forever())
    await asyncio.wait_for(stub.failed.wait(), timeout=2)
    await stop_node(node, serving)
    request = stub.FailTask.await_args.args[0]
    assert "RuntimeError: handler exploded" in request.error_reason


@pytest.mark.asyncio
async def test_single_worker_does_not_claim_second_task_while_first_is_active() -> None:
    stub = Stub([claimed_response("task-first"), claimed_response("task-second")])
    node = running_node(stub, worker_concurrency=1)
    first_started = asyncio.Event()
    release = asyncio.Event()

    @node.on_task
    async def handler(task) -> None:
        first_started.set()
        await release.wait()
        await task.complete()

    serving = asyncio.create_task(node.serve_forever())
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await asyncio.sleep(0.05)
    assert stub.ClaimNextTask.await_count == 1
    release.set()
    await asyncio.wait_for(stub.completed.wait(), timeout=1)
    await stop_node(node, serving)


@pytest.mark.asyncio
async def test_serve_forever_requires_handler() -> None:
    node = running_node(Stub([]))
    with pytest.raises(RuntimeError, match="at least one on_task handler"):
        await node.serve_forever()
    await node.close()
