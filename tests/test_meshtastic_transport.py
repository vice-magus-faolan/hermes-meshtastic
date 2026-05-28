from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import requests
import pytest

from plugins.platforms.meshtastic import adapter
from plugins.platforms.meshtastic.transport import (
    HttpMeshtasticTransport,
    MeshtasticTransport,
    SendReceipt,
    TransportStatus,
)


def cfg(extra: dict) -> SimpleNamespace:
    return SimpleNamespace(extra=extra)


def valid_serial_extra() -> dict:
    return {
        "transport": "serial",
        "serial_path": "/dev/ttyUSB0",
        "dm_policy": "allowlist",
        "group_policy": "allowlist",
        "dm_allowlist": ["!89ABCDEF"],
        "allowed_channels": [0],
        "text_chunk_bytes": 200,
        "chunk_delay_seconds": 1.5,
    }


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


class StubAdapterTransport:
    def __init__(self, *, connect_ok: bool) -> None:
        self._connect_ok = connect_ok
        self.connected = False

    async def connect(self) -> bool:
        self.connected = self._connect_ok
        return self._connect_ok

    async def disconnect(self) -> None:
        self.connected = False

    async def probe(self) -> dict[str, object]:
        return {"ok": self.connected}

    def status(self) -> TransportStatus:
        return TransportStatus(
            transport="stub",
            address="stub://addr",
            connected=self.connected,
            reconnect_attempts=0,
            keepalive_failures=0,
            last_connect_at=None,
            last_disconnect_at=None,
            last_probe_success=None,
            last_probe_failure=None,
            last_probe_at=None,
            last_probe_result=None,
            last_error=None if self.connected else "forced failure",
        )


def test_adapter_connect_disconnect_uses_transport() -> None:
    transport = StubAdapterTransport(connect_ok=True)
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()), transport=transport)

    assert asyncio.run(meshtastic.connect()) is True
    probe = asyncio.run(meshtastic.probe())
    assert isinstance(probe["running"], bool)
    assert probe["transport"] == "stub"
    assert probe["transport_type"] == "stub"
    assert probe["transport_address"] == "stub://addr"
    assert probe["transport_path_or_address"] == "stub://addr"
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


class StubSendTransport:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.fail_on_call = fail_on_call
        self.calls: list[dict[str, Any]] = []

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def probe(self) -> dict[str, object]:
        return {"ok": True}

    def status(self) -> TransportStatus:
        return TransportStatus(
            transport="stub-send",
            address="stub://send",
            connected=True,
            reconnect_attempts=0,
            keepalive_failures=0,
            last_connect_at=None,
            last_disconnect_at=None,
            last_probe_success=None,
            last_probe_failure=None,
            last_probe_at=None,
            last_probe_result=None,
            last_error=None,
        )

    async def send_text(
        self,
        *,
        text: str,
        destination_id: str | None,
        channel_index: int | None,
    ) -> SendReceipt:
        call = {
            "text": text,
            "destination_id": destination_id,
            "channel_index": channel_index,
        }
        self.calls.append(call)

        if self.fail_on_call is not None and len(self.calls) == self.fail_on_call:
            raise RuntimeError("forced send failure")

        return SendReceipt(message_id=f"msg-{len(self.calls)}", raw_response={"ok": True})


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
