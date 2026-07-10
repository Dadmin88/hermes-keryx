import asyncio

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
