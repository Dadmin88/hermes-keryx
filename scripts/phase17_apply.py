#!/usr/bin/env python3
"""Apply Phase 17.3 Python worker runtime with strict source anchors."""

from pathlib import Path
from textwrap import dedent


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:140]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


def insert_before(path: str, marker: str, addition: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"{path}: expected one marker, found {count}: {marker[:140]!r}")
    file.write_text(text.replace(marker, addition.rstrip() + "\n\n" + marker, 1), encoding="utf-8")


def write_new(path: str, content: str) -> None:
    file = Path(path)
    if file.exists():
        raise SystemExit(f"{path}: already exists")
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(dedent(content).lstrip(), encoding="utf-8")


TASK = "sdk/python/keryx/task.py"
NODE = "sdk/python/keryx/node.py"
PRODUCT = "docs/current-product.md"

replace_once(
    TASK,
    dedent(
        '''
        @dataclass
        class Part:
            text: str | None = None

            @classmethod
            def from_dict(cls, data: dict[str, Any]) -> Part:
                if not isinstance(data, dict):
                    raise ValueError("Part.from_dict expected a dictionary")
                text = data.get("text")
                if text is not None and not isinstance(text, str):
                    raise ValueError("Part text must be a string")
                return cls(text=text)


        @dataclass
        class Message:
            role: str = "user"
            parts: list[Part] = field(default_factory=list)
        '''
    ).lstrip(),
    dedent(
        '''
        @dataclass
        class Part:
            text: str | None = None
            raw: bytes = b""
            media_type: str = "text/plain"
            metadata: dict[str, str] = field(default_factory=dict)

            @classmethod
            def from_dict(cls, data: dict[str, Any]) -> Part:
                if not isinstance(data, dict):
                    raise ValueError("Part.from_dict expected a dictionary")
                text = data.get("text")
                if text is not None and not isinstance(text, str):
                    raise ValueError("Part text must be a string")
                raw = data.get("raw") or b""
                if not isinstance(raw, bytes):
                    raise ValueError("Part raw content must be bytes")
                metadata = data.get("metadata") or {}
                if not isinstance(metadata, dict):
                    raise ValueError("Part metadata must be a dictionary")
                return cls(
                    text=text,
                    raw=raw,
                    media_type=str(data.get("media_type") or "text/plain"),
                    metadata={str(key): str(value) for key, value in metadata.items()},
                )


        @dataclass
        class Message:
            role: str = "user"
            parts: list[Part] = field(default_factory=list)
            metadata: dict[str, str] = field(default_factory=dict)
        '''
    ).lstrip(),
)

replace_once(
    TASK,
    dedent(
        '''
        class IncomingTask:
            def __init__(
                self,
                task: Task,
                sender_card: Any | None,
                update_fn: Callable[[str, TaskStatus, list[Artifact] | None, str | None], Coroutine[Any, Any, None]],
            ) -> None:
                self._task = task
                self.sender_card = sender_card
                self._update_fn = update_fn

            @property
            def task_id(self) -> str:
                return self._task.task_id

            async def complete(self, artifacts: list[Artifact] | None = None) -> None:
                await self._update_fn(self.task_id, TaskStatus.COMPLETED, artifacts, None)

            async def fail(self, error: str) -> None:
                await self._update_fn(self.task_id, TaskStatus.FAILED, None, error)
        '''
    ).lstrip(),
    dedent(
        '''
        class IncomingTask:
            def __init__(
                self,
                task: Task,
                sender_card: Any | None,
                update_fn: Callable[[str, TaskStatus, list[Artifact] | None, str | None], Coroutine[Any, Any, None]],
                *,
                lease_id: str = "",
                worker_id: str = "",
                sender_peer_id: str = "",
            ) -> None:
                self._task = task
                self.sender_card = sender_card
                self._update_fn = update_fn
                self._lease_id = lease_id
                self._worker_id = worker_id
                self._sender_peer_id = sender_peer_id
                self._terminal = asyncio.Event()
                self._terminal_lock = asyncio.Lock()

            @property
            def task_id(self) -> str:
                return self._task.task_id

            @property
            def context_id(self) -> str:
                return self._task.context_id

            @property
            def messages(self) -> list[Message]:
                return self._task.messages

            @property
            def metadata(self) -> dict[str, str]:
                return dict(self._task.metadata or {})

            @property
            def target_skill_id(self) -> str:
                return self._task.target_skill_id

            @property
            def peer_id(self) -> str:
                return self._sender_peer_id or self._task.originator_peer_id

            @property
            def lease_id(self) -> str:
                return self._lease_id

            @property
            def worker_id(self) -> str:
                return self._worker_id

            @property
            def status(self) -> TaskStatus:
                return self._task.status

            @property
            def is_terminal(self) -> bool:
                return self._terminal.is_set()

            async def wait_terminal(self) -> TaskStatus:
                await self._terminal.wait()
                return self._task.status

            async def update_status(self, status: str | TaskStatus) -> None:
                resolved = status if isinstance(status, TaskStatus) else TaskStatus(str(status).lower())
                if resolved in (TaskStatus.SUBMITTED, TaskStatus.WORKING):
                    self._task.status = resolved
                    return
                if resolved == TaskStatus.COMPLETED:
                    await self.complete()
                    return
                await self.fail(f"task marked {resolved.value}")

            async def complete(self, artifacts: list[Artifact] | None = None) -> None:
                async with self._terminal_lock:
                    if self._terminal.is_set():
                        return
                    await self._update_fn(self.task_id, TaskStatus.COMPLETED, artifacts, None)
                    self._task.status = TaskStatus.COMPLETED
                    self._task.artifacts = list(artifacts or [])
                    self._terminal.set()

            async def fail(self, error: str) -> None:
                async with self._terminal_lock:
                    if self._terminal.is_set():
                        return
                    await self._update_fn(self.task_id, TaskStatus.FAILED, None, error)
                    self._task.status = TaskStatus.FAILED
                    self._terminal.set()
        '''
    ).lstrip(),
)

replace_once(NODE, "import sys\n", "import sys\nimport time\n")
replace_once(
    NODE,
    "    IncomingTask,\n    Message,\n",
    "    Artifact,\n    IncomingTask,\n    Message,\n",
)
replace_once(
    NODE,
    "        daemon_stub: Any | None = None,\n        client_factory: Callable[..., DaemonClient] | type[DaemonClient] | None = None,\n",
    "        daemon_stub: Any | None = None,\n        worker_concurrency: int = 1,\n        claim_wait_timeout_ms: int = 1_000,\n        heartbeat_interval_ms: int | None = None,\n        shutdown_grace_seconds: float = 5.0,\n        client_factory: Callable[..., DaemonClient] | type[DaemonClient] | None = None,\n",
)
replace_once(
    NODE,
    "        self._serve_stop = asyncio.Event()\n        self._task_handlers: list[TaskHandler] = []\n",
    "        if worker_concurrency < 1:\n            raise ValueError(\"worker_concurrency must be at least 1\")\n        if claim_wait_timeout_ms < 0:\n            raise ValueError(\"claim_wait_timeout_ms cannot be negative\")\n        if heartbeat_interval_ms is not None and heartbeat_interval_ms < 1:\n            raise ValueError(\"heartbeat_interval_ms must be positive\")\n        if shutdown_grace_seconds < 0:\n            raise ValueError(\"shutdown_grace_seconds cannot be negative\")\n        self._serve_stop = asyncio.Event()\n        self._serve_done = asyncio.Event()\n        self._serve_done.set()\n        self._worker_concurrency = worker_concurrency\n        self._claim_wait_timeout_ms = claim_wait_timeout_ms\n        self._heartbeat_interval_ms = heartbeat_interval_ms\n        self._shutdown_grace_seconds = shutdown_grace_seconds\n        self._task_handlers: list[TaskHandler] = []\n",
)

replace_once(
    NODE,
    dedent(
        '''
            async def stop(self) -> None:
                if not self._running and not self._connected:
                    return
                self._serve_stop.set()
                await self.close()
                logger.info("KeryxNode stopped")

            async def serve_forever(self) -> None:
                self._ensure_running()
                self._serve_stop.clear()
                while not self._serve_stop.is_set():
                    await asyncio.sleep(0.25)
        '''
    ),
    dedent(
        '''
            async def stop(self) -> None:
                if not self._running and not self._connected:
                    return
                self._serve_stop.set()
                if not self._serve_done.is_set():
                    try:
                        await asyncio.wait_for(
                            self._serve_done.wait(), timeout=self._shutdown_grace_seconds
                        )
                    except TimeoutError:
                        logger.warning("Keryx worker shutdown exceeded grace period")
                await self.close()
                logger.info("KeryxNode stopped")

            async def serve_forever(self) -> None:
                self._ensure_running()
                if not self._task_handlers:
                    raise RuntimeError("serve_forever requires at least one on_task handler")
                if not self._serve_done.is_set():
                    raise RuntimeError("serve_forever is already running")
                self._serve_stop.clear()
                self._serve_done.clear()
                workers = [
                    asyncio.create_task(
                        self._worker_loop(index), name=f"keryx-worker-{index}"
                    )
                    for index in range(self._worker_concurrency)
                ]
                stop_wait = asyncio.create_task(self._serve_stop.wait())
                try:
                    done, _ = await asyncio.wait(
                        [stop_wait, *workers], return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in done:
                        if task is stop_wait or task.cancelled():
                            continue
                        error = task.exception()
                        if error is not None:
                            raise error
                finally:
                    self._serve_stop.set()
                    stop_wait.cancel()
                    for worker in workers:
                        worker.cancel()
                    await asyncio.gather(stop_wait, *workers, return_exceptions=True)
                    self._serve_done.set()
        '''
    ),
)

insert_before(
    NODE,
    "    async def list_peers(self) -> list[dict[str, Any]]:",
    dedent(
        '''
            async def _worker_loop(self, worker_index: int) -> None:
                accepted_skills = [skill.id for skill in (self._card.skills if self._card else [])]
                while not self._serve_stop.is_set():
                    try:
                        claimed = await self.claim_next(
                            accepted_skill_ids=accepted_skills,
                            wait_timeout_ms=self._claim_wait_timeout_ms,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        if self._serve_stop.is_set():
                            return
                        logger.warning(
                            "Keryx worker %s could not claim work: %s: %s",
                            worker_index,
                            type(exc).__name__,
                            exc,
                        )
                        await self._wait_or_stop(0.5)
                        continue
                    if not claimed.has_task:
                        continue
                    await self._process_claimed_task(claimed)

            async def _process_claimed_task(self, claimed: ClaimedTask) -> None:
                if (
                    claimed.envelope is None
                    or not claimed.task_id
                    or not claimed.lease_id
                    or not claimed.worker_id
                ):
                    logger.error("Keryx daemon returned an incomplete claimed task")
                    return
                started = time.monotonic()

                async def update_remote(
                    task_id: str,
                    status: LegacyTaskStatus,
                    artifacts: list[Artifact] | None,
                    error: str | None,
                ) -> None:
                    duration_ms = int((time.monotonic() - started) * 1_000)
                    if status == LegacyTaskStatus.COMPLETED:
                        result_metadata, descriptors = _completion_payload(artifacts)
                        await self.complete(
                            task_id,
                            claimed.lease_id,
                            worker_id=claimed.worker_id,
                            duration_ms=duration_ms,
                            result_metadata=result_metadata,
                            output_artifacts=descriptors,
                        )
                    elif status == LegacyTaskStatus.FAILED:
                        await self.fail(
                            task_id,
                            claimed.lease_id,
                            error or "task failed",
                            worker_id=claimed.worker_id,
                            duration_ms=duration_ms,
                        )

                incoming = IncomingTask(
                    _task_from_claim(claimed),
                    None,
                    update_remote,
                    lease_id=claimed.lease_id,
                    worker_id=claimed.worker_id,
                    sender_peer_id=claimed.sender_peer_id,
                )
                heartbeat = asyncio.create_task(self._heartbeat_claim(claimed, incoming))
                terminal_wait: asyncio.Task[Any] | None = None
                stop_wait: asyncio.Task[Any] | None = None
                try:
                    try:
                        for handler in tuple(self._task_handlers):
                            await handler(incoming)
                            if incoming.is_terminal:
                                break
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        if not incoming.is_terminal:
                            try:
                                await incoming.fail(f"{type(exc).__name__}: {exc}")
                            except Exception:
                                logger.exception(
                                    "Keryx could not persist handler failure for task %s",
                                    claimed.task_id,
                                )
                        return

                    if incoming.is_terminal:
                        return
                    terminal_wait = asyncio.create_task(incoming.wait_terminal())
                    stop_wait = asyncio.create_task(self._serve_stop.wait())
                    done, _ = await asyncio.wait(
                        [terminal_wait, stop_wait, heartbeat],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if heartbeat in done and not incoming.is_terminal:
                        error = heartbeat.exception()
                        if error is not None:
                            logger.error(
                                "Keryx lease heartbeat failed for task %s: %s: %s",
                                claimed.task_id,
                                type(error).__name__,
                                error,
                            )
                finally:
                    for task in (terminal_wait, stop_wait, heartbeat):
                        if task is not None and not task.done():
                            task.cancel()
                    await asyncio.gather(
                        *(task for task in (terminal_wait, stop_wait, heartbeat) if task is not None),
                        return_exceptions=True,
                    )

            async def _heartbeat_claim(
                self, claimed: ClaimedTask, incoming: IncomingTask
            ) -> None:
                lease_ttl_ms = max(
                    1_000,
                    claimed.expires_at_ms - claimed.leased_at_ms,
                    self._config.default_lease_duration_ms,
                )
                interval_ms = self._heartbeat_interval_ms or max(250, lease_ttl_ms // 3)
                while not incoming.is_terminal and not self._serve_stop.is_set():
                    if await self._wait_or_stop(interval_ms / 1_000):
                        return
                    if incoming.is_terminal:
                        return
                    await self.heartbeat(
                        claimed.task_id,
                        claimed.lease_id,
                        worker_id=claimed.worker_id,
                        lease_duration_ms=lease_ttl_ms,
                    )

            async def _wait_or_stop(self, timeout_seconds: float) -> bool:
                try:
                    await asyncio.wait_for(
                        self._serve_stop.wait(), timeout=max(0.0, timeout_seconds)
                    )
                    return True
                except TimeoutError:
                    return False
        '''
    ),
)

insert_before(
    NODE,
    "def _task_envelope(\n",
    dedent(
        '''
        def _task_from_claim(claimed: ClaimedTask) -> Task:
            envelope = claimed.envelope
            if envelope is None:
                raise ValueError("claimed task is missing its envelope")
            metadata = {str(key): str(value) for key, value in envelope.metadata.items()}
            context_id = (
                envelope.correlation_id.value
                if envelope.HasField("correlation_id")
                else ""
            )
            messages = []
            for proto_message in envelope.messages:
                message_metadata = {
                    str(key): str(value) for key, value in proto_message.metadata.items()
                }
                messages.append(
                    Message(
                        role=message_metadata.get("role", "user"),
                        parts=[
                            Part(
                                text=part.text or None,
                                raw=bytes(part.raw),
                                media_type=part.media_type or "text/plain",
                                metadata={
                                    str(key): str(value)
                                    for key, value in part.metadata.items()
                                },
                            )
                            for part in proto_message.parts
                        ],
                        metadata=message_metadata,
                    )
                )
            target_skill_id = next(
                (
                    metadata[key]
                    for key in ("skill", "skill_id", "target_skill_id")
                    if metadata.get(key)
                ),
                "",
            )
            return Task(
                task_id=claimed.task_id,
                context_id=context_id,
                status=LegacyTaskStatus.WORKING,
                messages=messages,
                target_skill_id=target_skill_id,
                originator_peer_id=claimed.sender_peer_id,
                metadata=metadata,
            )


        def _completion_payload(
            artifacts: list[Artifact] | None,
        ) -> tuple[dict[str, str], list[TaskArtifact]]:
            descriptors: list[TaskArtifact] = []
            result_texts: list[str] = []
            for index, artifact in enumerate(artifacts or [], start=1):
                text = "\n".join(part.text for part in artifact.parts if part.text)
                metadata: dict[str, str] = {}
                if artifact.artifact_id:
                    metadata["artifact_id"] = artifact.artifact_id
                if text:
                    metadata["text_preview"] = text[:4_096]
                    result_texts.append(text)
                descriptors.append(
                    TaskArtifact(
                        path=artifact.name or artifact.artifact_id or f"artifact-{index}",
                        media_type="text/plain" if text else "application/octet-stream",
                        metadata=metadata,
                    )
                )
            result_metadata: dict[str, str] = {}
            if result_texts:
                result_metadata["result_text"] = "\n\n".join(result_texts)[:65_536]
            return result_metadata, descriptors
        '''
    ),
)

replace_once(
    PRODUCT,
    "Phase 17.1 retains complete envelopes durably. Phase 17.2 adds atomic worker dequeue through `ClaimNextTask`, with deterministic selection, exact skill/capability filters, bounded long polling, and lease-safe concurrent claims.\n",
    "Phase 17.1 retains complete envelopes durably. Phase 17.2 adds atomic worker dequeue through `ClaimNextTask`. Phase 17.3 makes Python `serve_forever()` a real worker runtime: it claims matching tasks, invokes registered handlers, maintains leases with heartbeats, and persists local completion or failure.\n",
)
replace_once(
    PRODUCT,
    "- Python `serve_forever()` consumption of the available `ClaimNextTask` worker API\n- transport-authenticated sender identity attached to the claimed envelope\n- Python `serve_forever()` dispatch into registered `on_task()` handlers\n",
    "- transport-authenticated sender identity attached to the claimed envelope\n",
)
replace_once(
    PRODUCT,
    "- `serve_forever()` keeps the SDK process alive but does not claim daemon tasks or invoke registered task handlers.\n",
    "- `serve_forever()` claims durable daemon tasks, dispatches them into registered handlers, and heartbeats until the `IncomingTask` completes, fails, or the worker stops.\n",
)
replace_once(
    PRODUCT,
    "The SDK default daemon endpoint is `unix:///tmp/keryx-daemon.sock`; most repository examples override it to `127.0.0.1:50051` / `http://127.0.0.1:50051` for the current daemon binary and CLI.\n",
    "The SDK default daemon endpoint is the current user's private `~/.hermes/keryx/run/keryx-daemon.sock`; repository integration examples may override it with `127.0.0.1:50051` / `http://127.0.0.1:50051`.\n",
)

write_new(
    "sdk/python/tests/test_worker_runtime.py",
    r'''
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
    ''',
)
