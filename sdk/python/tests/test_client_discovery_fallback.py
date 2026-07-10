from __future__ import annotations

from typing import Any

import pytest

from keryx import client as keryx_client


@pytest.mark.asyncio
async def test_discover_falls_back_to_bounded_full_registry_scan() -> None:
    registry_pb2 = keryx_client.registry_pb2

    class RegistryStub:
        def __init__(self) -> None:
            self.requests: list[Any] = []

        async def DiscoverBySkill(self, request):  # noqa: N802
            self.requests.append(request)
            if request.skill_id:
                return registry_pb2.DiscoverBySkillResponse()
            return registry_pb2.DiscoverBySkillResponse(
                registrations=[
                    registry_pb2.Registration(
                        peer_id="peer-local",
                        name="Local Agent",
                        description="local",
                        skills=[
                            registry_pb2.SkillInfo(skill_id="other"),
                            registry_pb2.SkillInfo(
                                skill_id="hermes-chat",
                                tags=["chat"],
                            ),
                        ],
                    ),
                    registry_pb2.Registration(
                        peer_id="peer-filtered-out",
                        skills=[registry_pb2.SkillInfo(skill_id="other")],
                    ),
                ]
            )

    registry = RegistryStub()
    client = keryx_client.DaemonClient(daemon_endpoint="127.0.0.1:50051")
    client._registry = registry

    result = await client.discover("hermes-chat", tags=["chat"], limit=1)

    assert result == [
        {
            "peer_id": "peer-local",
            "agent_name": "Local Agent",
            "agent_description": "local",
            "skills": ["other", "hermes-chat"],
        }
    ]
    assert [request.skill_id for request in registry.requests] == ["hermes-chat", ""]
    assert [request.limit for request in registry.requests] == [1, 1]


@pytest.mark.asyncio
async def test_discover_caps_unlimited_fallback_at_one_hundred() -> None:
    registry_pb2 = keryx_client.registry_pb2

    class RegistryStub:
        def __init__(self) -> None:
            self.requests: list[Any] = []

        async def DiscoverBySkill(self, request):  # noqa: N802
            self.requests.append(request)
            return registry_pb2.DiscoverBySkillResponse()

    registry = RegistryStub()
    client = keryx_client.DaemonClient(daemon_endpoint="127.0.0.1:50051")
    client._registry = registry

    assert await client.discover("hermes-chat", limit=0) == []
    assert [request.skill_id for request in registry.requests] == ["hermes-chat", ""]
    assert [request.limit for request in registry.requests] == [0, 100]
