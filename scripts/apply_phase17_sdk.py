#!/usr/bin/env python3
"""Connect the Python TaskHandle to durable daemon result state."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text()


def write(path: str, value: str) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(value)


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    if new in text:
        return
    if old not in text:
        raise RuntimeError(f"anchor missing in {path}: {old[:100]!r}")
    write(path, text.replace(old, new, 1))


# Low-level daemon result query.
client_path = "sdk/python/keryx/client.py"
replace_once(
    client_path,
    '''    async def discover(self, skill_id: str, *, tags: list[str] | None = None, limit: int = 10) -> list[dict[str, Any]]:
''',
    '''    async def get_task_result(self, task_id: str) -> daemon_pb2.GetTaskResultResponse:
        assert self._daemon is not None
        return await self._daemon.GetTaskResult(
            daemon_pb2.GetTaskResultRequest(task_id=common_pb2.TaskId(value=task_id))
        )

    async def cancel_task(self, task_id: str, *, reason: str = "") -> daemon_pb2.CancelTaskResponse:
        assert self._daemon is not None
        return await self._daemon.CancelTask(
            daemon_pb2.CancelTaskRequest(
                task_id=common_pb2.TaskId(value=task_id),
                reason=reason,
            )
        )

    async def discover(self, skill_id: str, *, tags: list[str] | None = None, limit: int = 10) -> list[dict[str, Any]]:
''',
)

# Polling TaskHandle.
task_path = "sdk/python/keryx/task.py"
text = read(task_path)
old = '''class TaskHandle:
    def __init__(self, task: Task, cancel_fn: Callable[[], Coroutine[Any, Any, None]] | None = None) -> None:
        self._task = task
        self._cancel_fn = cancel_fn
        self._done = asyncio.Event()
        if task.status.is_terminal:
            self._done.set()

    @property
    def task_id(self) -> str:
        return self._task.task_id

    @property
    def status(self) -> TaskStatus:
        return self._task.status

    async def wait(self, timeout: float | None = None) -> Task:
        await asyncio.wait_for(self._done.wait(), timeout=timeout)
        return self._task

    async def cancel(self) -> None:
        if self._cancel_fn is not None:
            await self._cancel_fn()
'''
new = '''class TaskHandle:
    def __init__(
        self,
        task: Task,
        cancel_fn: Callable[[], Coroutine[Any, Any, None]] | None = None,
        refresh_fn: Callable[[], Coroutine[Any, Any, Task]] | None = None,
        *,
        poll_interval: float = 0.1,
        max_poll_interval: float = 1.0,
    ) -> None:
        self._task = task
        self._cancel_fn = cancel_fn
        self._refresh_fn = refresh_fn
        self._poll_interval = max(0.01, poll_interval)
        self._max_poll_interval = max(self._poll_interval, max_poll_interval)
        self._done = asyncio.Event()
        if task.status.is_terminal:
            self._done.set()

    @property
    def task_id(self) -> str:
        return self._task.task_id

    @property
    def status(self) -> TaskStatus:
        return self._task.status

    async def refresh(self) -> Task:
        if self._refresh_fn is not None and not self._task.status.is_terminal:
            self._task = await self._refresh_fn()
            if self._task.status.is_terminal:
                self._done.set()
        return self._task

    async def wait(self, timeout: float | None = None) -> Task:
        if self._refresh_fn is None:
            await asyncio.wait_for(self._done.wait(), timeout=timeout)
            return self._task

        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        delay = self._poll_interval
        while True:
            await self.refresh()
            if self._task.status.is_terminal:
                return self._task
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError
                await asyncio.sleep(min(delay, remaining))
            else:
                await asyncio.sleep(delay)
            delay = min(self._max_poll_interval, delay * 1.5)

    async def cancel(self) -> None:
        if self._cancel_fn is not None:
            await self._cancel_fn()
        await self.refresh()
'''
if new not in text:
    if old not in text:
        raise RuntimeError("TaskHandle anchor missing")
    text = text.replace(old, new, 1)
write(task_path, text)

# KeryxNode result mapping and handle construction.
node_path = "sdk/python/keryx/node.py"
text = read(node_path)
old_return = '''        task = Task(task_id=response.task_id.value or task_id, status=LegacyTaskStatus.SUBMITTED)
        return TaskHandle(task=task)
'''
new_return = '''        task = Task(task_id=response.task_id.value or task_id, status=LegacyTaskStatus.SUBMITTED)

        async def refresh_remote() -> Task:
            assert self._client is not None
            result_response = await self._client.get_task_result(task.task_id)
            task.status = _legacy_status(result_response.status)
            if result_response.found and result_response.HasField("result"):
                result = result_response.result
                task.status = _legacy_status(result_response.status, outcome=result.outcome)
                artifacts: list[Artifact] = []
                result_text = result.result_metadata.get("result_text", "")
                for item in result.output_artifacts:
                    preview = item.metadata.get("text_preview", "")
                    parts = [Part(text=preview, media_type=item.media_type or "text/plain")] if preview else []
                    artifacts.append(Artifact(name=item.path, parts=parts))
                if result_text and not artifacts:
                    artifacts.append(
                        Artifact(
                            name="result",
                            parts=[Part(text=result_text, media_type="text/plain")],
                        )
                    )
                task.artifacts = artifacts
                task.metadata = {
                    **dict(task.metadata or {}),
                    **{str(key): str(value) for key, value in result.result_metadata.items()},
                    "executor_peer_id": result.executor_peer_id,
                    "duration_ms": str(result.duration_ms),
                    "error_reason": result.error_reason,
                }
            return task

        async def cancel_remote() -> None:
            assert self._client is not None
            await self._client.cancel_task(task.task_id, reason="canceled by TaskHandle")

        return TaskHandle(
            task=task,
            refresh_fn=refresh_remote,
            cancel_fn=cancel_remote,
        )
'''
if new_return not in text:
    if old_return not in text:
        raise RuntimeError("send_task return anchor missing")
    text = text.replace(old_return, new_return, 1)
helper_anchor = "def _task_from_claim(claimed: ClaimedTask) -> Task:\n"
helper = '''def _legacy_status(status: str, *, outcome: int = 0) -> LegacyTaskStatus:
    normalized = status.strip().lower()
    if normalized == "completed":
        return LegacyTaskStatus.COMPLETED
    if normalized in {"failed", "dead_lettered", "timed_out"}:
        return LegacyTaskStatus.FAILED
    if normalized == "canceled":
        return LegacyTaskStatus.CANCELED
    if normalized == "rejected":
        return LegacyTaskStatus.REJECTED
    if normalized in {"running", "working", "leased"}:
        return LegacyTaskStatus.WORKING
    if outcome == 1:
        return LegacyTaskStatus.COMPLETED
    if outcome in {2, 4}:
        return LegacyTaskStatus.FAILED
    if outcome == 3:
        return LegacyTaskStatus.CANCELED
    if outcome == 5:
        return LegacyTaskStatus.REJECTED
    return LegacyTaskStatus.SUBMITTED


'''
if helper not in text:
    text = text.replace(helper_anchor, helper + helper_anchor)
write(node_path, text)

# Durable polling behavior tests.
write(
    "sdk/python/tests/test_task_handle_remote.py",
    '''import asyncio

import pytest

from keryx.task import Artifact, Part, Task, TaskHandle, TaskStatus


@pytest.mark.asyncio
async def test_task_handle_polls_until_terminal() -> None:
    task = Task(task_id="remote-1")
    calls = 0

    async def refresh() -> Task:
        nonlocal calls
        calls += 1
        if calls >= 3:
            task.status = TaskStatus.COMPLETED
            task.artifacts = [Artifact(name="answer", parts=[Part(text="done")])]
        return task

    handle = TaskHandle(task, refresh_fn=refresh, poll_interval=0.001, max_poll_interval=0.002)
    result = await handle.wait(timeout=1)
    assert result.status is TaskStatus.COMPLETED
    assert result.artifacts[0].parts[0].text == "done"
    assert calls >= 3


@pytest.mark.asyncio
async def test_task_handle_timeout_does_not_fabricate_completion() -> None:
    task = Task(task_id="remote-2")

    async def refresh() -> Task:
        return task

    handle = TaskHandle(task, refresh_fn=refresh, poll_interval=0.001, max_poll_interval=0.002)
    with pytest.raises(TimeoutError):
        await handle.wait(timeout=0.01)
    assert handle.status is TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_task_handle_cancel_refreshes_durable_state() -> None:
    task = Task(task_id="remote-3")
    canceled = asyncio.Event()

    async def cancel() -> None:
        canceled.set()
        task.status = TaskStatus.CANCELED

    async def refresh() -> Task:
        return task

    handle = TaskHandle(task, cancel_fn=cancel, refresh_fn=refresh)
    await handle.cancel()
    assert canceled.is_set()
    assert handle.status is TaskStatus.CANCELED
''',
)

print("Phase 17 Python TaskHandle polling applied")
