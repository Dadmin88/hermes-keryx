from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = ROOT / "sdk" / "python"
PROTO_ROOT = SDK_ROOT / "keryx" / "proto"
for path in (SDK_ROOT, PROTO_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from hermes.keryx.v1 import common_pb2, daemon_pb2, registry_pb2  # noqa: E402
from keryx import KeryxConfig, KeryxNode, TaskArtifact, TaskResult, TaskState, load_config  # noqa: E402


class MockDaemonStub:
    def __init__(self) -> None:
        self.requests: dict[str, object] = {}

    async def Status(self, request: daemon_pb2.StatusRequest) -> daemon_pb2.StatusResponse:
        self.requests["status"] = request
        return daemon_pb2.StatusResponse(status="ready", data_dir="/tmp/keryx", store_ready=True)

    async def Doctor(self, request: daemon_pb2.DoctorRequest) -> daemon_pb2.DoctorResponse:
        self.requests["doctor"] = request
        return daemon_pb2.DoctorResponse(status="pass", messages=["sqlite_store ok"])

    async def ListPeers(self, request: daemon_pb2.ListPeersRequest) -> daemon_pb2.ListPeersResponse:
        self.requests["peers"] = request
        return daemon_pb2.ListPeersResponse(
            peers=[daemon_pb2.PeerDescriptor(peer_id="peer-local", connected=True, local=True)]
        )

    async def DiscoverSkills(
        self, request: daemon_pb2.DiscoverSkillsRequest
    ) -> daemon_pb2.DiscoverSkillsResponse:
        self.requests["skills"] = request
        return daemon_pb2.DiscoverSkillsResponse(
            registrations=[
                registry_pb2.Registration(
                    peer_id="peer-worker",
                    name="worker",
                    description="test worker",
                    skills=[
                        registry_pb2.SkillInfo(
                            skill_id="python", description="Python work", tags=["sdk"]
                        )
                    ],
                )
            ]
        )

    async def SubmitTask(self, request: daemon_pb2.SubmitTaskRequest) -> daemon_pb2.SubmitTaskResponse:
        self.requests["submit"] = request
        return daemon_pb2.SubmitTaskResponse(task_id=request.envelope.task_id, status="queued")

    async def ClaimTask(self, request: daemon_pb2.ClaimTaskRequest) -> daemon_pb2.ClaimTaskResponse:
        self.requests["claim"] = request
        return daemon_pb2.ClaimTaskResponse(
            task_id=request.task_id,
            lease_id=common_pb2.LeaseId(value="lease-1"),
            worker_id=request.worker_id,
            leased_at_ms=100,
            expires_at_ms=200,
            status="leased",
            retry_count=1,
            dead_lettered=False,
        )

    async def Heartbeat(self, request: daemon_pb2.HeartbeatRequest) -> daemon_pb2.HeartbeatResponse:
        self.requests["heartbeat"] = request
        return daemon_pb2.HeartbeatResponse(
            lease_id=request.lease_id,
            expires_at_ms=300,
        )

    async def CompleteTask(
        self, request: daemon_pb2.CompleteTaskRequest
    ) -> daemon_pb2.CompleteTaskResponse:
        self.requests["complete"] = request
        return daemon_pb2.CompleteTaskResponse(
            task_id=request.task_id,
            status="completed",
            duration_ms=request.duration_ms,
            result_metadata=request.result_metadata,
            output_artifacts=request.output_artifacts,
        )

    async def FailTask(self, request: daemon_pb2.FailTaskRequest) -> daemon_pb2.FailTaskResponse:
        self.requests["fail"] = request
        return daemon_pb2.FailTaskResponse(
            task_id=request.task_id,
            status="failed",
            duration_ms=request.duration_ms,
            error_reason=request.error_reason,
            failure_metadata=request.failure_metadata,
            retry_count=2,
            dead_lettered=True,
        )

    async def CancelTask(self, request: daemon_pb2.CancelTaskRequest) -> daemon_pb2.CancelTaskResponse:
        self.requests["cancel"] = request
        return daemon_pb2.CancelTaskResponse(
            task_id=request.task_id,
            status="canceled",
            reason=request.reason,
            canceled=True,
        )


@pytest.fixture
def stub() -> MockDaemonStub:
    return MockDaemonStub()


@pytest.fixture
def node(stub: MockDaemonStub) -> KeryxNode:
    return KeryxNode(
        config=KeryxConfig(
            daemon_endpoint="tcp://127.0.0.1:50051",
            worker_id="worker-1",
            default_lease_duration_ms=5_000,
        ),
        daemon_stub=stub,
    )


@pytest.mark.asyncio
async def test_query_status_doctor_peers_and_skills(node: KeryxNode, stub: MockDaemonStub) -> None:
    assert await node.status() == {
        "status": "ready",
        "data_dir": "/tmp/keryx",
        "db_path": "",
        "schema_version": 0,
        "supported_schema_version": 0,
        "recovered_tasks": 0,
        "cleaned_terminal_leases": 0,
        "corruption_count": 0,
        "startup_recovery_duration_ms": 0,
        "store_kind": "",
        "store_ready": True,
        "store_path": "",
        "tasks_submitted": 0,
        "tasks_claimed": 0,
        "tasks_completed": 0,
        "tasks_failed": 0,
        "heartbeats": 0,
        "leases_recovered": 0,
        "recovery_ticks": 0,
        "active_leases": 0,
        "dead_letters": 0,
        "max_pending_tasks": 0,
        "max_envelope_bytes": 0,
        "current_pending_tasks": None,
        "warnings": [],
        "cancel_requests": 0,
        "tasks_canceled": 0,
        "deadline_ticks": 0,
        "deadline_failures": 0,
        "last_deadline_scan_ms": 0,
        "last_deadline_failures": 0,
        "deadline_enforcement_interval_ms": 0,
    }
    assert await node.doctor() == {"status": "pass", "messages": ["sqlite_store ok"]}
    assert await node.peers() == [{"peer_id": "peer-local", "connected": True, "local": True}]

    skills = await node.skills("python", tags={"runtime": "sdk"}, limit=1)
    assert skills == [
        {
            "peer_id": "peer-worker",
            "skills": [{"skill_id": "python", "description": "Python work", "tags": ["sdk"]}],
            "name": "worker",
            "description": "test worker",
            "expires_at": None,
        }
    ]
    request = stub.requests["skills"]
    assert isinstance(request, daemon_pb2.DiscoverSkillsRequest)
    assert request.skill_id == "python"
    assert list(request.tags) == ["sdk"]
    assert request.limit == 1


@pytest.mark.asyncio
async def test_task_lifecycle_methods_build_expected_grpc_requests(
    node: KeryxNode, stub: MockDaemonStub
) -> None:
    submitted = await node.submit("task-1", message="hello", metadata={"tenant": "default"})
    assert submitted == TaskState(task_id="task-1", status="queued")
    submit_request = stub.requests["submit"]
    assert isinstance(submit_request, daemon_pb2.SubmitTaskRequest)
    assert submit_request.envelope.task_id.value == "task-1"
    assert submit_request.envelope.messages[0].parts[0].text == "hello"
    assert submit_request.envelope.metadata["tenant"] == "default"

    claimed = await node.claim("task-1")
    assert claimed.lease_id == "lease-1"
    assert claimed.worker_id == "worker-1"
    assert claimed.expires_at_ms == 200
    claim_request = stub.requests["claim"]
    assert isinstance(claim_request, daemon_pb2.ClaimTaskRequest)
    assert claim_request.worker_id.value == "worker-1"
    assert claim_request.lease_duration_ms == 5_000

    heartbeat = await node.heartbeat("task-1", "lease-1", lease_duration_ms=10_000)
    assert heartbeat.lease_id == "lease-1"
    assert heartbeat.expires_at_ms == 300
    heartbeat_request = stub.requests["heartbeat"]
    assert isinstance(heartbeat_request, daemon_pb2.HeartbeatRequest)
    assert heartbeat_request.lease_duration_ms == 10_000

    completed = await node.complete(
        "task-1",
        "lease-1",
        duration_ms=42,
        result_metadata={"ok": "true"},
        output_artifacts=[TaskArtifact(path="/tmp/result.txt", media_type="text/plain")],
    )
    assert completed == TaskResult(
        task_id="task-1",
        status="completed",
        duration_ms=42,
        result_metadata={"ok": "true"},
        output_artifacts=[TaskArtifact(path="/tmp/result.txt", media_type="text/plain")],
    )
    complete_request = stub.requests["complete"]
    assert isinstance(complete_request, daemon_pb2.CompleteTaskRequest)
    assert complete_request.output_artifacts[0].path == "/tmp/result.txt"

    failed = await node.fail(
        "task-1",
        "lease-1",
        "boom",
        duration_ms=7,
        failure_metadata={"kind": "test"},
    )
    assert failed.error_reason == "boom"
    assert failed.failure_metadata == {"kind": "test"}
    assert failed.retry_count == 2
    assert failed.dead_lettered is True

    canceled = await node.cancel(
        "task-1",
        reason="user requested",
        metadata={"actor": "test"},
        lease_id="lease-1",
        worker_id="worker-1",
    )
    assert canceled.task_id == "task-1"
    assert canceled.status == "canceled"
    assert canceled.canceled is True
    cancel_request = stub.requests["cancel"]
    assert isinstance(cancel_request, daemon_pb2.CancelTaskRequest)
    assert cancel_request.reason == "user requested"
    assert cancel_request.metadata["actor"] == "test"
    assert cancel_request.lease_id.value == "lease-1"
    assert cancel_request.worker_id.value == "worker-1"


@pytest.mark.asyncio
async def test_worker_id_is_required_when_not_configured(stub: MockDaemonStub) -> None:
    node = KeryxNode(config=KeryxConfig(), daemon_stub=stub)
    with pytest.raises(ValueError, match="worker_id is required"):
        await node.claim("task-1")


def test_load_config_from_toml_with_env_override(tmp_path: Path) -> None:
    config_path = tmp_path / "keryx.toml"
    config_path.write_text(
        """
        [daemon]
        endpoint = "http://127.0.0.1:50051"
        [worker]
        id = "toml-worker"
        default_lease_duration_ms = 123
        """,
        encoding="utf-8",
    )

    config = load_config(
        config_path,
        env={
            "HERMES_KERYX_WORKER_ID": "env-worker",
            "HERMES_KERYX_REQUEST_TIMEOUT_MS": "2500",
        },
    )

    assert config.daemon_endpoint == "http://127.0.0.1:50051"
    assert config.grpc_daemon_target == "127.0.0.1:50051"
    assert config.worker_id == "env-worker"
    assert config.default_lease_duration_ms == 123
    assert config.request_timeout_ms == 2500
