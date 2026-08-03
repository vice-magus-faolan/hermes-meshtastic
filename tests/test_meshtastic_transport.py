from __future__ import annotations

import asyncio
import sys
from types import ModuleType, SimpleNamespace
from typing import cast

import requests
import pytest

from plugins.platforms.meshtastic import adapter
from plugins.platforms.meshtastic.transport import (
    HttpMeshtasticTransport,
    MeshtasticTransport,
    SerialMeshtasticTransport,
    TcpMeshtasticTransport,
    make_transport,
)
from tests.meshtastic_harness import (
    StubAdapterTransport,
    StubSendTransport,
    cfg,
    group_payload,
    valid_serial_extra,
    valid_tcp_extra,
)


class DummyTransport(MeshtasticTransport):
    def __init__(self, *, open_failures: int = 0, probe_failures: int = 0) -> None:
        super().__init__(
            transport_name="dummy",
            address="dummy://transport",
            keepalive_enabled=True,
            keepalive_interval_seconds=0.02,
        )
        self._open_failures_remaining = open_failures
        self._probe_failures_remaining = probe_failures
        self.open_count = 0
        self.close_count = 0

    def _open_client(self) -> object:
        self.open_count += 1
        if self._open_failures_remaining > 0:
            self._open_failures_remaining -= 1
            raise RuntimeError("open boom")
        return object()

    def _close_client(self, client: object) -> None:
        del client
        self.close_count += 1

    def _probe_client(self, client: object) -> dict[str, object]:
        del client
        if self._probe_failures_remaining > 0:
            self._probe_failures_remaining -= 1
            raise RuntimeError("probe boom")
        return {"healthy": True}

    def _send_text_client(
        self,
        client: object,
        text: str,
        destination_id: str | None,
        channel_index: int | None,
    ) -> dict[str, object]:
        del client, text, destination_id, channel_index
        return {"id": "dummy"}


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _FakeSession:
    def __init__(self, status_codes: list[int]) -> None:
        self._status_codes = list(status_codes)
        self.closed = False

    def get(self, url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        if not self._status_codes:
            raise AssertionError("fake session exhausted status codes")
        return _FakeResponse(self._status_codes.pop(0))

    def close(self) -> None:
        self.closed = True


class _SessionFactory:
    def __init__(self, session_status_codes: list[list[int]]) -> None:
        self._session_status_codes = [list(codes) for codes in session_status_codes]
        self.created_sessions: list[_FakeSession] = []

    def __call__(self) -> requests.Session:
        if not self._session_status_codes:
            raise AssertionError("no fake sessions remaining")
        session = _FakeSession(self._session_status_codes.pop(0))
        self.created_sessions.append(session)
        return cast(requests.Session, session)


class _FakePubSub:
    def __init__(self) -> None:
        self.subscriptions: list[tuple[object, str]] = []

    def subscribe(self, callback: object, topic: str) -> None:
        self.subscriptions.append((callback, topic))

    def unsubscribe(self, callback: object, topic: str) -> None:
        self.subscriptions.remove((callback, topic))


class _BrokenPubSub(_FakePubSub):
    def subscribe(self, callback: object, topic: str) -> None:
        del callback, topic
        raise RuntimeError("subscribe boom")


class _FakeSerialClient:
    def __init__(self) -> None:
        self.closed = False
        self.localNode = SimpleNamespace(nodeNum=1234)

    def close(self) -> None:
        self.closed = True

    def sendText(self, text: str, **kwargs: object) -> dict[str, object]:
        return {"text": text, **kwargs}


class _FakeTcpClient(_FakeSerialClient):
    def __init__(self) -> None:
        super().__init__()
        self.nodes = {"!43b64008": {"num": 1136017416}}


def test_transport_connect_disconnect_success() -> None:
    transport = DummyTransport()
    assert asyncio.run(transport.connect()) is True
    assert transport.connected is True

    status_after_connect = transport.status()
    assert status_after_connect.connected is True
    assert status_after_connect.last_connect_at is not None

    asyncio.run(transport.disconnect())
    status_after_disconnect = transport.status()
    assert status_after_disconnect.connected is False
    assert status_after_disconnect.last_disconnect_at is not None


def test_transport_reconnect_retries_until_success() -> None:
    transport = DummyTransport(open_failures=1)

    assert asyncio.run(transport.connect()) is False
    status = transport.status()
    assert status.connected is False
    assert status.last_error is not None

    assert asyncio.run(transport.reconnect()) is True
    status = transport.status()
    assert status.connected is True
    assert transport.open_count >= 2

    asyncio.run(transport.disconnect())


def test_keepalive_failures_trigger_reconnect_without_leaking_loop() -> None:
    async def _scenario() -> None:
        transport = DummyTransport(probe_failures=2)

        assert await transport.connect() is True
        await asyncio.sleep(0.12)

        keepalive_name = "meshtastic-dummy-keepalive"
        keepalive_tasks = [
            task
            for task in asyncio.all_tasks()
            if task.get_name() == keepalive_name and not task.done()
        ]

        status = transport.status()
        assert status.connected is True
        assert status.reconnect_attempts >= 1
        assert len(keepalive_tasks) == 1

        await transport.disconnect()
        await asyncio.sleep(0)

        keepalive_tasks = [
            task
            for task in asyncio.all_tasks()
            if task.get_name() == keepalive_name and not task.done()
        ]
        assert keepalive_tasks == []

    asyncio.run(_scenario())


def test_serial_transport_subscribes_inbound_pubsub_packets() -> None:
    async def _scenario() -> None:
        fake_pubsub = _FakePubSub()
        fake_client = _FakeSerialClient()
        transport = SerialMeshtasticTransport(
            "/dev/ttyUSB0",
            client_factory=lambda _: fake_client,
            pubsub_bus=fake_pubsub,
            keepalive_enabled=False,
        )

        received: list[dict[str, object]] = []
        delivered = asyncio.Event()

        async def fake_inbound_handler(payload: dict[str, object]) -> bool:
            received.append(payload)
            delivered.set()
            return True

        transport.set_inbound_handler(fake_inbound_handler)

        assert await transport.connect() is True
        assert len(fake_pubsub.subscriptions) == 1
        callback, topic = fake_pubsub.subscriptions[0]
        assert topic == "meshtastic.receive"
        assert callable(callback)

        callback(group_payload(text="bridge me", sender="89ABCDEF"), fake_client)
        await asyncio.wait_for(delivered.wait(), timeout=1.0)

        assert received == [group_payload(text="bridge me", sender="89ABCDEF")]

        await transport.disconnect()
        assert fake_pubsub.subscriptions == []
        assert fake_client.closed is True

    asyncio.run(_scenario())


def test_serial_transport_ignores_packets_from_other_interface() -> None:
    async def _scenario() -> None:
        fake_pubsub = _FakePubSub()
        fake_client = _FakeSerialClient()
        other_client = _FakeSerialClient()
        transport = SerialMeshtasticTransport(
            "/dev/ttyUSB0",
            client_factory=lambda _: fake_client,
            pubsub_bus=fake_pubsub,
            keepalive_enabled=False,
        )

        received: list[dict[str, object]] = []

        async def fake_inbound_handler(payload: dict[str, object]) -> bool:
            received.append(payload)
            return True

        transport.set_inbound_handler(fake_inbound_handler)

        assert await transport.connect() is True
        callback, _topic = fake_pubsub.subscriptions[0]
        assert callable(callback)

        callback(group_payload(text="ignore me", sender="89ABCDEF"), other_client)
        await asyncio.sleep(0.05)
        assert received == []

        callback(group_payload(text="accept me", sender="89ABCDEF"), fake_client)
        await asyncio.sleep(0.05)
        assert received == [group_payload(text="accept me", sender="89ABCDEF")]

        await transport.disconnect()

    asyncio.run(_scenario())


def test_serial_transport_connect_failure_closes_client_when_subscription_fails() -> None:
    async def _scenario() -> None:
        fake_client = _FakeSerialClient()
        transport = SerialMeshtasticTransport(
            "/dev/ttyUSB0",
            client_factory=lambda _: fake_client,
            pubsub_bus=_BrokenPubSub(),
            keepalive_enabled=False,
        )

        transport.set_inbound_handler(lambda payload: True)

        assert await transport.connect() is False
        assert transport.connected is False
        assert fake_client.closed is True
        assert transport.status().last_error == "connect failed: subscribe boom"

    asyncio.run(_scenario())


def test_tcp_transport_subscribes_inbound_pubsub_packets_and_reports_probe_details() -> None:
    async def _scenario() -> None:
        fake_pubsub = _FakePubSub()
        fake_client = _FakeTcpClient()
        transport = TcpMeshtasticTransport(
            "192.168.132.135",
            port=4403,
            client_factory=lambda host, port: fake_client,
            pubsub_bus=fake_pubsub,
            keepalive_enabled=False,
        )

        received: list[dict[str, object]] = []
        delivered = asyncio.Event()

        async def fake_inbound_handler(payload: dict[str, object]) -> bool:
            received.append(payload)
            delivered.set()
            return True

        transport.set_inbound_handler(fake_inbound_handler)

        assert await transport.connect() is True
        assert len(fake_pubsub.subscriptions) == 1
        callback, topic = fake_pubsub.subscriptions[0]
        assert topic == "meshtastic.receive"
        assert callable(callback)

        callback(group_payload(text="bridge over tcp", sender="89ABCDEF"), fake_client)
        await asyncio.wait_for(delivered.wait(), timeout=1.0)

        assert received == [group_payload(text="bridge over tcp", sender="89ABCDEF")]

        probe = await transport.probe()
        assert probe["ok"] is True
        assert probe["mode"] == "tcp"
        assert probe["local_node"] == 1234
        assert probe["node_count"] == 1

        await transport.disconnect()
        assert fake_pubsub.subscriptions == []
        assert fake_client.closed is True

    asyncio.run(_scenario())


def test_adapter_tcp_runtime_uses_official_library_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _scenario() -> None:
        fake_pubsub = _FakePubSub()
        meshtastic_pkg = ModuleType("meshtastic")
        meshtastic_pkg.__path__ = []  # type: ignore[attr-defined]
        tcp_module = ModuleType("meshtastic.tcp_interface")
        pubsub_module = ModuleType("pubsub")

        class FakeOfficialTcpInterface:
            created: list["FakeOfficialTcpInterface"] = []

            def __init__(self, *, hostname: str, portNumber: int) -> None:
                self.hostname = hostname
                self.portNumber = portNumber
                self.localNode = SimpleNamespace(nodeNum=1234)
                self.nodes = {"!43b64008": {"num": 1136017416}}
                self.closed = False
                self.send_calls: list[dict[str, object]] = []
                type(self).created.append(self)

            def close(self) -> None:
                self.closed = True

            def sendText(self, text: str, **kwargs: object) -> dict[str, object]:
                call = {"text": text, **kwargs}
                self.send_calls.append(call)
                return {"id": f"pkt-{len(self.send_calls)}", **call}

        setattr(tcp_module, "TCPInterface", FakeOfficialTcpInterface)
        setattr(pubsub_module, "pub", fake_pubsub)
        monkeypatch.setitem(sys.modules, "meshtastic", meshtastic_pkg)
        monkeypatch.setitem(sys.modules, "meshtastic.tcp_interface", tcp_module)
        monkeypatch.setitem(sys.modules, "pubsub", pubsub_module)

        meshtastic = adapter.MeshtasticAdapter(cfg(valid_tcp_extra(tcp_port=None)))

        assert await meshtastic.connect() is True
        assert len(FakeOfficialTcpInterface.created) == 1
        client = FakeOfficialTcpInterface.created[0]
        assert client.hostname == "192.168.132.135"
        assert client.portNumber == 4403
        assert len(fake_pubsub.subscriptions) == 1

        probe = await meshtastic.probe()
        assert probe["transport_connected"] is True
        assert probe["transport"] == "tcp"
        assert probe["transport_address"] == "192.168.132.135:4403"
        assert probe["transport_probe"]["mode"] == "tcp"
        assert probe["transport_probe"]["node_count"] == 1

        result = await meshtastic.send("node/89abcdef", "hello over tcp")
        assert result.success is True
        assert result.message_id == "pkt-1"
        assert result.raw_response is not None
        assert result.raw_response["chunk_count"] == 1
        assert client.send_calls == [
            {"text": "hello over tcp", "destinationId": "!89abcdef"}
        ]

        await meshtastic.disconnect()
        assert fake_pubsub.subscriptions == []
        assert client.closed is True

    asyncio.run(_scenario())


def test_make_transport_builds_tcp_transport() -> None:
    transport = make_transport(
        transport=valid_tcp_extra()["transport"],
        serial_path=None,
        http_base_url=None,
        tcp_host=valid_tcp_extra()["tcp_host"],
        tcp_port=valid_tcp_extra()["tcp_port"],
    )
    assert isinstance(transport, TcpMeshtasticTransport)
    assert transport.address == "192.168.132.135:4403"


def test_http_probe_5xx_is_unhealthy() -> None:
    async def _scenario() -> None:
        session = cast(requests.Session, _FakeSession([200, 500]))
        transport = HttpMeshtasticTransport(
            "http://mesh.local",
            session_factory=lambda: session,
            keepalive_enabled=False,
        )

        assert await transport.connect() is True

        probe = await transport.probe()
        assert probe["ok"] is False
        assert "HTTP 500" in str(probe.get("error"))

        status = transport.status()
        assert status.last_probe_at is not None
        assert status.last_probe_result is not None
        assert status.last_probe_result.get("ok") is False

        await transport.disconnect()

    asyncio.run(_scenario())


def test_http_keepalive_5xx_triggers_reconnect() -> None:
    async def _scenario() -> None:
        session_factory = _SessionFactory(
            [
                [200, 500, 500],
                [200, 200, 200, 200, 200, 200],
            ]
        )
        transport = HttpMeshtasticTransport(
            "http://mesh.local",
            session_factory=session_factory,
            keepalive_enabled=True,
        )
        transport._keepalive_interval_seconds = 0.02

        assert await transport.connect() is True
        await asyncio.sleep(0.14)

        status = transport.status()
        assert status.connected is True
        assert status.reconnect_attempts >= 1
        assert len(session_factory.created_sessions) >= 2

        await transport.disconnect()

    asyncio.run(_scenario())


def test_adapter_successful_connect_does_not_set_fatal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = StubAdapterTransport(connect_ok=True)
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()), transport=transport)

    def mark_fatal(code: str, message: str, *, retryable: bool) -> None:
        del code, message, retryable
        meshtastic._running = False

    monkeypatch.setattr(meshtastic, "_set_fatal_error", mark_fatal)

    assert asyncio.run(meshtastic.connect()) is True
    assert meshtastic._running is True


def test_adapter_connect_disconnect_uses_transport() -> None:
    transport = StubAdapterTransport(connect_ok=True)
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()), transport=transport)

    assert asyncio.run(meshtastic.connect()) is True
    probe = asyncio.run(meshtastic.probe())
    assert isinstance(probe["running"], bool)
    assert probe["transport"] == "stub"
    assert probe["transport_address"] == "stub://addr"
    assert probe["transport_connected"] is True

    asyncio.run(meshtastic.disconnect())
    probe = asyncio.run(meshtastic.probe())
    assert probe["running"] is False
    assert probe["transport_connected"] is False


def test_adapter_connect_failure_sets_probe_error() -> None:
    transport = StubAdapterTransport(connect_ok=False)
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()), transport=transport)

    assert asyncio.run(meshtastic.connect()) is False
    probe = asyncio.run(meshtastic.probe())
    assert probe["last_error"] == "forced failure"


def test_adapter_send_chunks_and_paces(monkeypatch: pytest.MonkeyPatch) -> None:
    extra = valid_serial_extra()
    extra["text_chunk_bytes"] = 10
    extra["chunk_delay_seconds"] = 0.25

    transport = StubSendTransport()
    meshtastic = adapter.MeshtasticAdapter(cfg(extra), transport=transport)

    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(adapter.asyncio, "sleep", fake_sleep)

    result = asyncio.run(meshtastic.send("channel/0", "alpha beta gamma delta"))

    assert result.success is True
    assert result.raw_response is not None
    assert result.raw_response["chunk_count"] == len(transport.calls)
    assert len(transport.calls) > 1
    assert len(sleeps) == len(transport.calls) - 1
    assert sleeps == [0.25] * (len(transport.calls) - 1)
    assert meshtastic._last_outbound_activity is not None

    for call in transport.calls:
        assert len(call["text"].encode("utf-8")) <= 10
        assert call["channel_index"] == 0
        assert call["destination_id"] is None


def test_adapter_send_rejects_non_text_payload_kwargs() -> None:
    transport = StubSendTransport()
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()), transport=transport)

    result = asyncio.run(
        meshtastic.send(
            "node/89abcdef",
            "hello",
            attachments=[{"url": "https://example.invalid/image.png"}],
        )
    )

    assert result.success is False
    assert "plain-text" in str(result.error)
    assert transport.calls == []


def test_adapter_send_partial_failure_is_non_retryable() -> None:
    extra = valid_serial_extra()
    extra["text_chunk_bytes"] = 10
    transport = StubSendTransport(fail_on_call=2)
    meshtastic = adapter.MeshtasticAdapter(cfg(extra), transport=transport)

    result = asyncio.run(meshtastic.send("channel/0", "alpha beta gamma delta"))

    assert result.success is False
    assert result.retryable is False
    assert "chunk 2/" in str(result.error)
    assert result.raw_response is not None
    assert result.raw_response["sent_chunks"] == 1
    assert result.raw_response["total_chunks"] >= 2


def test_adapter_send_normalizes_meshtastic_prefixed_node_target() -> None:
    transport = StubSendTransport()
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()), transport=transport)

    result = asyncio.run(meshtastic.send("meshtastic:node/89ABCDEF", "hello"))

    assert result.success is True
    assert len(transport.calls) == 1
    assert transport.calls[0]["destination_id"] == "!89abcdef"
    assert transport.calls[0]["channel_index"] is None


def test_adapter_send_chunks_utf8_without_splitting_characters() -> None:
    extra = valid_serial_extra(text_chunk_bytes=6, chunk_delay_seconds=0.01)
    transport = StubSendTransport()
    meshtastic = adapter.MeshtasticAdapter(cfg(extra), transport=transport)

    result = asyncio.run(meshtastic.send("channel/0", "ééééé"))

    assert result.success is True
    assert len(transport.calls) == 2
    assert transport.calls[0]["text"] == "ééé"
    assert transport.calls[1]["text"] == "éé"
