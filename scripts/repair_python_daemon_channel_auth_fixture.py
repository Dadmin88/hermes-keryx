#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "sdk/python/tests/test_artifact_sdk.py"
text = PATH.read_text(encoding="utf-8")

old = '''    calls: list[tuple[str, tuple[tuple[str, int], ...]]] = []

    class _Channel:
        async def close(self) -> None:
            return None

    def insecure_channel(target: str, options: tuple[tuple[str, int], ...]) -> _Channel:
        calls.append((target, options))
        return _Channel()

    monkeypatch.setattr(client_module.grpc.aio, "insecure_channel", insecure_channel)
    monkeypatch.setattr(node_module.grpc.aio, "insecure_channel", insecure_channel)
    monkeypatch.setattr(client_module.daemon_pb2_grpc, "KeryxDaemonStub", lambda channel: object())
    monkeypatch.setattr(node_module.daemon_pb2_grpc, "KeryxDaemonStub", lambda channel: object())

    client = DaemonClient(daemon_endpoint="127.0.0.1:50051")
    await client.connect()
    node = KeryxNode(daemon_endpoint="127.0.0.1:50051")
    await node.connect()

    expected = (
        ("grpc.max_send_message_length", RESULT_ARTIFACT_FRAME_MAX_BYTES),
        ("grpc.max_receive_message_length", RESULT_ARTIFACT_FRAME_MAX_BYTES),
    )
    assert calls == [("127.0.0.1:50051", expected), ("127.0.0.1:50051", expected)]
'''

new = '''    calls: list[tuple[str, tuple[tuple[str, int], ...], object | None]] = []

    class _Channel:
        async def close(self) -> None:
            return None

    def insecure_channel(
        target: str,
        options: tuple[tuple[str, int], ...],
        interceptors: object | None = None,
    ) -> _Channel:
        calls.append((target, options, interceptors))
        return _Channel()

    monkeypatch.setattr(client_module.grpc.aio, "insecure_channel", insecure_channel)
    monkeypatch.setattr(node_module.grpc.aio, "insecure_channel", insecure_channel)
    monkeypatch.setattr(client_module.daemon_pb2_grpc, "KeryxDaemonStub", lambda channel: object())
    monkeypatch.setattr(node_module.daemon_pb2_grpc, "KeryxDaemonStub", lambda channel: object())

    client = DaemonClient(
        daemon_endpoint="127.0.0.1:50051",
        daemon_token="sdk-channel-test-token",
    )
    await client.connect()
    node = KeryxNode(
        daemon_endpoint="127.0.0.1:50051",
        daemon_token="sdk-channel-test-token",
    )
    await node.connect()

    expected = (
        ("grpc.max_send_message_length", RESULT_ARTIFACT_FRAME_MAX_BYTES),
        ("grpc.max_receive_message_length", RESULT_ARTIFACT_FRAME_MAX_BYTES),
    )
    assert len(calls) == 2
    for target, options, interceptors in calls:
        assert target == "127.0.0.1:50051"
        assert options == expected
        assert isinstance(interceptors, list)
        assert len(interceptors) == 1
        assert isinstance(interceptors[0], client_module._DaemonAuthInterceptor)
'''

count = text.count(old)
if count != 1:
    raise SystemExit(f"expected exactly one stale daemon channel fixture, found {count}")
text = text.replace(old, new, 1)
PATH.write_text(text, encoding="utf-8")
print("Python daemon channel fixture now validates artifact limits and auth interceptor")
