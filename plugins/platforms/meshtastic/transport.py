"""Meshtastic transport lifecycle primitives for serial and HTTP connectivity.

This module isolates transport concerns from adapter message-routing concerns:
- connect/disconnect with timeout bounds
- bounded reconnect attempts with exponential backoff
- periodic keepalive probes to avoid stale sessions
- transport-specific diagnostics for operators

Message send/receive semantics are intentionally handled in other slices.
"""

from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import importlib
import logging
from typing import Any, Callable
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

_OPEN_TIMEOUT_SECONDS = 12.0
_PROBE_TIMEOUT_SECONDS = 5.0
_KEEPALIVE_INTERVAL_SECONDS = 45.0
_MAX_RECONNECT_ATTEMPTS = 3
_RECONNECT_BASE_DELAY_SECONDS = 1.0
_RECONNECT_MAX_DELAY_SECONDS = 8.0
_KEEPALIVE_FAILURES_BEFORE_RECONNECT = 2


@dataclass(frozen=True)
class TransportStatus:
    """Runtime status snapshot for adapter probe surfaces."""

    transport: str
    address: str
    connected: bool
    reconnect_attempts: int
    keepalive_failures: int
    last_connect_at: str | None
    last_disconnect_at: str | None
    last_probe_success: str | None
    last_probe_failure: str | None
    last_probe_at: str | None
    last_probe_result: dict[str, Any] | None
    last_error: str | None


@dataclass(frozen=True)
class SendReceipt:
    """Transport-agnostic outbound delivery receipt for one text chunk."""

    message_id: str | None
    raw_response: dict[str, Any]


class MeshtasticTransport(abc.ABC):
    """Lifecycle manager for a concrete Meshtastic transport backend."""

    def __init__(
        self,
        *,
        transport_name: str,
        address: str,
        keepalive_enabled: bool = True,
        keepalive_interval_seconds: float = _KEEPALIVE_INTERVAL_SECONDS,
    ) -> None:
        self.transport_name = transport_name
        self.address = address

        self._open_timeout_seconds = _OPEN_TIMEOUT_SECONDS
        self._probe_timeout_seconds = _PROBE_TIMEOUT_SECONDS
        self._keepalive_enabled = keepalive_enabled
        self._keepalive_interval_seconds = keepalive_interval_seconds

        self._client: Any = None
        self._connected = False
        self._connect_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._keepalive_task: asyncio.Task[None] | None = None

        self._reconnect_attempts = 0
        self._keepalive_failures = 0
        self._last_connect_at: datetime | None = None
        self._last_disconnect_at: datetime | None = None
        self._last_probe_success: datetime | None = None
        self._last_probe_failure: datetime | None = None
        self._last_probe_at: datetime | None = None
        self._last_probe_result: dict[str, Any] | None = None
        self._last_error: str | None = None

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self) -> bool:
        """Establish transport connectivity with timeout and diagnostics."""

        async with self._connect_lock:
            self._shutdown_event.clear()
            if self._connected:
                return True

            try:
                client = await asyncio.wait_for(
                    asyncio.to_thread(self._open_client),
                    timeout=self._open_timeout_seconds,
                )
            except Exception as exc:
                self._connected = False
                self._last_error = f"connect failed: {exc}"
                logger.warning(
                    "Meshtastic %s transport connect failed (%s): %s",
                    self.transport_name,
                    self.address,
                    exc,
                )
                return False

            self._client = client
            self._connected = True
            self._keepalive_failures = 0
            self._last_error = None
            self._last_connect_at = _utcnow()

            if self._keepalive_enabled and self._keepalive_task is None:
                self._keepalive_task = asyncio.create_task(
                    self._keepalive_loop(),
                    name=f"meshtastic-{self.transport_name}-keepalive",
                )

            return True

    async def disconnect(self) -> None:
        """Close transport and stop background keepalive safely."""

        async with self._connect_lock:
            self._shutdown_event.set()
            keepalive = self._keepalive_task
            current_task = asyncio.current_task()
            if keepalive is not None and keepalive is not current_task:
                self._keepalive_task = None
                keepalive.cancel()
                try:
                    await keepalive
                except asyncio.CancelledError:
                    pass

            if self._client is not None:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(self._close_client, self._client),
                        timeout=self._open_timeout_seconds,
                    )
                except Exception as exc:
                    self._last_error = f"disconnect failed: {exc}"
                    logger.warning(
                        "Meshtastic %s transport disconnect warning (%s): %s",
                        self.transport_name,
                        self.address,
                        exc,
                    )

            self._client = None
            self._connected = False
            self._last_disconnect_at = _utcnow()

    async def reconnect(self) -> bool:
        """Reconnect transport with bounded exponential backoff."""

        await self.disconnect()
        self._reconnect_attempts = 0

        delay = _RECONNECT_BASE_DELAY_SECONDS
        for attempt in range(1, _MAX_RECONNECT_ATTEMPTS + 1):
            self._reconnect_attempts = attempt
            if await self.connect():
                return True

            if attempt < _MAX_RECONNECT_ATTEMPTS:
                await asyncio.sleep(delay)
                delay = min(delay * 2.0, _RECONNECT_MAX_DELAY_SECONDS)

        return False

    async def ensure_connected(self) -> bool:
        """Ensure transport is connected, attempting reconnect if needed."""

        if self._connected:
            return True
        return await self.reconnect()

    async def probe(self) -> dict[str, Any]:
        """Run transport-specific liveness probe with timeout and diagnostics."""

        if not self._connected or self._client is None:
            probe_now = _utcnow()
            result = {"ok": False, "error": "not_connected"}
            self._last_probe_at = probe_now
            self._last_probe_failure = probe_now
            self._last_probe_result = result
            return result

        try:
            detail = await asyncio.wait_for(
                asyncio.to_thread(self._probe_client, self._client),
                timeout=self._probe_timeout_seconds,
            )
        except Exception as exc:
            probe_now = _utcnow()
            result = {"ok": False, "error": str(exc)}
            self._last_probe_at = probe_now
            self._last_probe_failure = probe_now
            self._last_error = f"probe failed: {exc}"
            self._last_probe_result = result
            return result

        self._last_probe_at = _utcnow()
        self._last_probe_success = _utcnow()
        self._keepalive_failures = 0
        self._last_error = None

        if isinstance(detail, dict):
            payload = dict(detail)
        else:
            payload = {"detail": detail}
        payload["ok"] = True
        self._last_probe_result = dict(payload)
        return payload

    async def send_text(
        self,
        *,
        text: str,
        destination_id: str | None = None,
        channel_index: int | None = None,
    ) -> SendReceipt:
        """Send one plain-text chunk through the active transport backend."""

        if not text:
            raise ValueError("text payload is empty")

        if not await self.ensure_connected() or self._client is None:
            raise RuntimeError("transport is not connected")

        try:
            raw = await asyncio.wait_for(
                asyncio.to_thread(
                    self._send_text_client,
                    self._client,
                    text,
                    destination_id,
                    channel_index,
                ),
                timeout=self._probe_timeout_seconds,
            )
        except Exception as exc:
            self._last_error = f"send failed: {exc}"
            raise

        payload = _as_payload(raw)
        return SendReceipt(message_id=_extract_message_id(raw), raw_response=payload)

    def status(self) -> TransportStatus:
        """Return serializable status for adapter probes and diagnostics."""

        return TransportStatus(
            transport=self.transport_name,
            address=self.address,
            connected=self._connected,
            reconnect_attempts=self._reconnect_attempts,
            keepalive_failures=self._keepalive_failures,
            last_connect_at=_ts(self._last_connect_at),
            last_disconnect_at=_ts(self._last_disconnect_at),
            last_probe_success=_ts(self._last_probe_success),
            last_probe_failure=_ts(self._last_probe_failure),
            last_probe_at=_ts(self._last_probe_at),
            last_probe_result=(
                dict(self._last_probe_result) if self._last_probe_result is not None else None
            ),
            last_error=self._last_error,
        )

    async def _keepalive_loop(self) -> None:
        """Periodic liveness checks that trigger reconnect on stale sessions."""

        try:
            while not self._shutdown_event.is_set():
                await asyncio.sleep(self._keepalive_interval_seconds)
                if self._shutdown_event.is_set() or not self._connected:
                    continue

                probe = await self.probe()
                if probe.get("ok"):
                    continue

                self._keepalive_failures += 1
                if self._keepalive_failures >= _KEEPALIVE_FAILURES_BEFORE_RECONNECT:
                    logger.warning(
                        "Meshtastic %s keepalive failed %s times for %s; reconnecting",
                        self.transport_name,
                        self._keepalive_failures,
                        self.address,
                    )
                    await self.reconnect()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._last_error = f"keepalive loop failed: {exc}"
            logger.exception("Meshtastic %s keepalive loop crashed", self.transport_name)

    @abc.abstractmethod
    def _open_client(self) -> Any:
        """Open and return transport client."""

    @abc.abstractmethod
    def _close_client(self, client: Any) -> None:
        """Close transport client and release local resources."""

    @abc.abstractmethod
    def _probe_client(self, client: Any) -> Any:
        """Run a lightweight liveness probe against an active client."""

    @abc.abstractmethod
    def _send_text_client(
        self,
        client: Any,
        text: str,
        destination_id: str | None,
        channel_index: int | None,
    ) -> Any:
        """Send one text payload on a transport-specific active client."""


class SerialMeshtasticTransport(MeshtasticTransport):
    """Serial Meshtastic transport wrapper."""

    def __init__(
        self,
        serial_path: str,
        *,
        client_factory: Callable[[str], Any] | None = None,
        keepalive_enabled: bool = True,
    ) -> None:
        super().__init__(
            transport_name="serial",
            address=serial_path,
            keepalive_enabled=keepalive_enabled,
        )
        self._serial_path = serial_path
        self._client_factory = client_factory or _default_serial_client_factory

    def _open_client(self) -> Any:
        return self._client_factory(self._serial_path)

    def _close_client(self, client: Any) -> None:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            close_fn()

    def _probe_client(self, client: Any) -> dict[str, Any]:
        local_node = getattr(client, "localNode", None)
        if local_node is None:
            return {"mode": "serial", "local_node": None}

        node_num = getattr(local_node, "nodeNum", None)
        if node_num is None:
            node_num = getattr(local_node, "num", None)
        return {"mode": "serial", "local_node": node_num}

    def _send_text_client(
        self,
        client: Any,
        text: str,
        destination_id: str | None,
        channel_index: int | None,
    ) -> Any:
        send_text = getattr(client, "sendText", None)
        if not callable(send_text):
            raise RuntimeError("serial transport client does not expose sendText")

        kwargs: dict[str, Any] = {}
        if destination_id is not None:
            kwargs["destinationId"] = destination_id
        if channel_index is not None:
            kwargs["channelIndex"] = channel_index

        call_attempts: list[tuple[tuple[Any, ...], dict[str, Any]]] = [
            ((text,), kwargs),
            ((), {"text": text, **kwargs}),
            ((text,), {}),
        ]

        last_exc: Exception | None = None
        for args, kw in call_attempts:
            try:
                return send_text(*args, **kw)
            except TypeError as exc:
                last_exc = exc
                continue

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("serial sendText invocation failed")


class HttpMeshtasticTransport(MeshtasticTransport):
    """HTTP Meshtastic transport wrapper.

    V0 uses base URL probing for connectivity/lifecycle health. Message semantics
    are implemented in downstream slices.
    """

    def __init__(
        self,
        base_url: str,
        *,
        session_factory: Callable[[], requests.Session] | None = None,
        keepalive_enabled: bool = True,
        healthcheck_path: str = "/",
    ) -> None:
        super().__init__(
            transport_name="http",
            address=base_url,
            keepalive_enabled=keepalive_enabled,
        )
        self._base_url = base_url.rstrip("/") + "/"
        self._healthcheck_path = healthcheck_path
        self._session_factory = session_factory or requests.Session

    def _open_client(self) -> requests.Session:
        session = self._session_factory()
        url = urljoin(self._base_url, self._healthcheck_path.lstrip("/"))
        response = session.get(url, timeout=self._probe_timeout_seconds)
        # We treat any non-5xx response as an alive endpoint. Some deployments
        # return 404 at root while still being healthy for API-specific paths.
        if response.status_code >= 500:
            raise RuntimeError(f"healthcheck returned HTTP {response.status_code}")
        return session

    def _close_client(self, client: requests.Session) -> None:
        client.close()

    def _probe_client(self, client: requests.Session) -> dict[str, Any]:
        url = urljoin(self._base_url, self._healthcheck_path.lstrip("/"))
        response = client.get(url, timeout=self._probe_timeout_seconds)
        if response.status_code >= 500:
            raise RuntimeError(f"healthcheck returned HTTP {response.status_code}")
        return {
            "mode": "http",
            "status_code": response.status_code,
            "url": url,
        }

    def _send_text_client(
        self,
        client: requests.Session,
        text: str,
        destination_id: str | None,
        channel_index: int | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"text": text}
        if destination_id is not None:
            payload["destinationId"] = destination_id
        if channel_index is not None:
            payload["channelIndex"] = channel_index

        candidate_paths = (
            "/api/v1/sendtext",
            "/api/v1/text",
            "/sendtext",
        )

        last_error: Exception | None = None
        for path in candidate_paths:
            url = urljoin(self._base_url, path.lstrip("/"))
            try:
                response = client.post(url, json=payload, timeout=self._probe_timeout_seconds)
            except Exception as exc:
                last_error = RuntimeError(f"http send request failed at {url}: {exc}")
                continue

            if response.status_code == 404:
                last_error = RuntimeError(f"http send endpoint not found at {url}")
                continue

            if response.status_code >= 400:
                body = (response.text or "").strip()
                raise RuntimeError(
                    f"http send failed at {url}: HTTP {response.status_code}"
                    + (f" body={body[:240]!r}" if body else "")
                )

            parsed = _response_payload(response)
            if not isinstance(parsed, dict):
                parsed = {"detail": parsed}
            parsed.setdefault("status_code", response.status_code)
            parsed.setdefault("url", url)
            return parsed

        if last_error is not None:
            raise last_error
        raise RuntimeError("http send failed: no endpoint attempts were executed")


def make_transport(
    *,
    transport: str,
    serial_path: str | None,
    http_base_url: str | None,
) -> MeshtasticTransport:
    """Instantiate a transport runtime from validated config fields."""

    if transport == "serial":
        if not serial_path:
            raise ValueError("serial transport requires serial_path")
        return SerialMeshtasticTransport(serial_path)

    if transport == "http":
        if not http_base_url:
            raise ValueError("http transport requires http_base_url")
        return HttpMeshtasticTransport(http_base_url)

    raise ValueError(f"unsupported transport: {transport}")


def _default_serial_client_factory(serial_path: str) -> Any:
    module = importlib.import_module("meshtastic.serial_interface")
    cls = getattr(module, "SerialInterface")

    # meshtastic-python versions differ in constructor names/signatures.
    try:
        return cls(devPath=serial_path)
    except TypeError:
        return cls(serial_path)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ts(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _response_payload(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        text = (response.text or "").strip()
        if not text:
            return {}
        return {"text": text}


def _as_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    return {"detail": value}


def _extract_message_id(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in ("message_id", "messageId", "id", "packet_id", "packetId"):
            raw = value.get(key)
            if raw is not None and str(raw).strip():
                return str(raw)

    for attr in ("message_id", "messageId", "id", "packet_id", "packetId"):
        raw = getattr(value, attr, None)
        if raw is not None and str(raw).strip():
            return str(raw)

    return None
