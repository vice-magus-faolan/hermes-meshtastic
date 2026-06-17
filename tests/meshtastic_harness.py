from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from plugins.platforms.meshtastic.transport import SendReceipt, TransportStatus


def _base_extra(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "dm_policy": "allowlist",
        "group_policy": "allowlist",
        "dm_allowlist": ["!89ABCDEF"],
        "allowed_channels": [0, 2],
        "text_chunk_bytes": 200,
        "chunk_delay_seconds": 1.5,
    }
    base.update(overrides)
    return base


def valid_serial_extra(**overrides: Any) -> dict[str, Any]:
    """Return a valid baseline serial config, with optional field overrides."""

    extra = _base_extra(transport="serial", serial_path="/dev/ttyUSB0")
    extra.update(overrides)
    return extra


def valid_tcp_extra(**overrides: Any) -> dict[str, Any]:
    """Return a valid baseline TCP config, with optional field overrides."""

    extra = _base_extra(transport="tcp", tcp_host="192.168.132.135", tcp_port=4403)
    extra.update(overrides)
    return extra


def cfg(extra: dict[str, Any]) -> SimpleNamespace:
    """Build a Hermes-like config object with an ``extra`` mapping."""

    return SimpleNamespace(extra=extra)


def dm_payload(
    text: str = "hello from mesh",
    sender: str = "89ABCDEF",
    sender_name: str | None = "Alice",
    **overrides: Any,
) -> dict[str, Any]:
    """Build a synthetic inbound DM payload."""

    payload: dict[str, Any] = {
        "text": text,
        "sender": sender,
    }
    if sender_name is not None:
        payload["sender_name"] = sender_name
    payload.update(overrides)
    return payload


def group_payload(
    text: str = "group ping",
    sender: str = "89ABCDEF",
    channel_index: int = 0,
    mentioned: bool | None = True,
    **overrides: Any,
) -> dict[str, Any]:
    """Build a synthetic inbound group payload."""

    payload: dict[str, Any] = {
        "text": text,
        "sender": sender,
        "isGroup": True,
        "channelIndex": channel_index,
    }
    if mentioned is not None:
        payload["mentioned"] = mentioned
    payload.update(overrides)
    return payload


def make_status(
    *,
    connected: bool,
    transport: str,
    address: str,
    last_error: str | None = None,
) -> TransportStatus:
    """Build a complete ``TransportStatus`` with stable defaults for tests."""

    return TransportStatus(
        transport=transport,
        address=address,
        connected=connected,
        reconnect_attempts=0,
        keepalive_failures=0,
        last_connect_at=None,
        last_disconnect_at=None,
        last_probe_success=None,
        last_probe_failure=None,
        last_probe_at=None,
        last_probe_result=None,
        last_error=last_error,
    )


@dataclass
class StubAdapterTransport:
    """Minimal synthetic transport for adapter connect/disconnect/probe tests."""

    connect_ok: bool
    connected: bool = False

    async def connect(self) -> bool:
        self.connected = self.connect_ok
        return self.connect_ok

    async def disconnect(self) -> None:
        self.connected = False

    async def probe(self) -> dict[str, object]:
        return {"ok": self.connected}

    def status(self) -> TransportStatus:
        return make_status(
            connected=self.connected,
            transport="stub",
            address="stub://addr",
            last_error=None if self.connected else "forced failure",
        )


@dataclass
class StubSendTransport:
    """Synthetic outbound transport that records chunks and can fail by call index."""

    fail_on_call: int | None = None

    def __post_init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def probe(self) -> dict[str, object]:
        return {"ok": True}

    def status(self) -> TransportStatus:
        return make_status(
            connected=True,
            transport="stub-send",
            address="stub://send",
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


@dataclass
class StubInboundTransport:
    """Synthetic inbound-capable transport for adapter bridge tests."""

    connected: bool = False

    def __post_init__(self) -> None:
        self._inbound_handler = None

    async def connect(self) -> bool:
        self.connected = True
        return True

    async def disconnect(self) -> None:
        self.connected = False

    async def probe(self) -> dict[str, object]:
        return {"ok": self.connected}

    def status(self) -> TransportStatus:
        return make_status(
            connected=self.connected,
            transport="stub-inbound",
            address="stub://inbound",
            last_error=None,
        )

    def set_inbound_handler(self, handler) -> None:
        self._inbound_handler = handler

    async def emit_inbound(self, payload: dict[str, Any]) -> bool:
        if self._inbound_handler is None:
            raise AssertionError("adapter did not register an inbound handler")
        return await self._inbound_handler(payload)
