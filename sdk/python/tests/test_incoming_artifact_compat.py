from __future__ import annotations

import pytest

from keryx.task import Artifact, IncomingTask, Task, TaskStatus


@pytest.mark.asyncio
async def test_incoming_complete_normalizes_mapping_artifacts() -> None:
    captured: list[Artifact] = []

    async def update(_task_id, status, artifacts, error):  # type: ignore[no-untyped-def]
        assert status is TaskStatus.COMPLETED
        assert error is None
        captured.extend(artifacts or [])

    incoming = IncomingTask(
        Task(task_id="compat-task"),
        sender_card=None,
        update_fn=update,
    )
    await incoming.complete(
        [
            {
                "artifact_id": "report-1",
                "name": "report.md",
                "parts": [
                    {
                        "text": "completed report",
                        "media_type": "text/markdown",
                    }
                ],
            }
        ]
    )

    assert incoming.status is TaskStatus.COMPLETED
    assert captured == incoming._task.artifacts
    assert captured[0].artifact_id == "report-1"
    assert captured[0].name == "report.md"
    assert captured[0].parts[0].text == "completed report"
    assert captured[0].parts[0].media_type == "text/markdown"
