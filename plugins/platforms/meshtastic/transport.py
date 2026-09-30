"""Meshtastic transport lifecycle primitives for serial, HTTP, and TCP connectivity.

This module isolates transport concerns from adapter message-routing concerns:
- connect/disconnect with timeout bounds
- bounded reconnect attempts with exponential backoff
- periodic keepalive probes to avoid stale sessions
- transport-specific diagnostics for operators
- transport-originated inbound bridge hooks for adapter delivery
"""

from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import importlib
import inspect
import logging
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

InboundHandler = Callable[[dict[str, Any]], Awaitable[bool] | bool]

_OPEN_TIMEOUT_SECONDS = 12.0
_PROBE_TIMEOUT_SECONDS = 5.0
_SEND_TIMEOUT_SECONDS = 15.0
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


class SendOutcomeIndeterminate(TimeoutError):
    """Raised when a send deadline expires before delivery can be confirmed."""


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
        self._send_timeout_seconds = _SEND_TIMEOUT_SECONDS
        self._keepalive_enabled = keepalive_enabled
        self._keepalive_interval_seconds = keepalive_interval_seconds

        self._client: Any = None
        self._connected = False
        self._connection_generation = 0
        self._connect_lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._keepalive_task: asyncio.Task[None] | None = None
        self._inbound_handler: InboundHandler | None = None
        self._event_loop: asyncio.AbstractEventLoop | None = None

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

    def set_inbound_handler(self, handler: InboundHandler | None) -> None:
        """Register the adapter callback used for transport-originated inbound packets."""

        self._inbound_handler = handler

    async def emit_inbound(self, payload: dict[str, Any]) -> bool:
        """Deliver a normalized inbound payload into the registered adapter callback."""

        if self._inbound_handler is None:
            logger.debug(
                "Meshtastic %s inbound packet dropped because no handler is registered",
                self.transport_name,
            )
            return False

        result = self._inbound_handler(payload)
        if inspect.isawaitable(result):
            return bool(await result)
        return bool(result)

    async def connect(self) -> bool:
        """Establish transport connectivity with timeout and diagnostics."""

        async with self._connect_lock:
            self._shutdown_event.clear()
            if self._connected:
                return True

            self._event_loop = asyncio.get_running_loop()
            client: Any = None
            try:
                client = await asyncio.wait_for(
                    asyncio.to_thread(self._open_client),
                    timeout=self._open_timeout_seconds,
                )
                await asyncio.wait_for(
                    asyncio.to_thread(self._after_connect, client),
                    timeout=self._open_timeout_seconds,
                )
            except Exception as exc:
                if client is not None:
                    try:
                        await asyncio.wait_for(
                            asyncio.to_thread(self._close_client, client),
                            timeout=self._open_timeout_seconds,
                        )
                    except Exception as cleanup_exc:
                        logger.warning(
                            "Meshtastic %s transport cleanup after connect failure (%s): %s",
                            self.transport_name,
                            self.address,
                            cleanup_exc,
                        )
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
            self._connection_generation += 1
            self._keepalive_failures = 0
            self._last_error = None
            self._last_connect_at = _utcnow()

            if self._keepalive_enabled and self._keepalive_task is None:
                self._start_keepalive_task()

            return True

    async def disconnect(self) -> None:
        """Close transport and stop background keepalive safely."""

        async with self._connect_lock:
            self._shutdown_event.set()
            keepalive = self._keepalive_task
            current_task = asyncio.current_task()
            if keepalive is not current_task:
                self._keepalive_task = None
            if keepalive is not None and keepalive is not current_task:
                keepalive_loop = keepalive.get_loop()
                if not keepalive_loop.is_closed():
                    keepalive.cancel()
                    if keepalive_loop is asyncio.get_running_loop():
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
            self._event_loop = None

    async def reconnect(self) -> bool:
        """Reconnect transport with bounded exponential backoff."""

        requested_generation = self._connection_generation
        async with self._reconnect_lock:
            if (
                self._connected
                and self._connection_generation != requested_generation
            ):
                return True
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
                timeout=self._send_timeout_seconds,
            )
        except asyncio.TimeoutError as exc:
            self._last_error = (
                "send outcome indeterminate: delivery was not confirmed before "
                f"the {self._send_timeout_seconds:.1f}s deadline"
            )
            raise SendOutcomeIndeterminate(self._last_error) from exc
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

        current_task = asyncio.current_task()
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
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._last_error = f"keepalive loop failed: {exc}"
            logger.exception("Meshtastic %s keepalive loop crashed", self.transport_name)
        finally:
            if self._keepalive_task is current_task:
                self._keepalive_task = None
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    return
                if (
                    self._connected
                    and not self._shutdown_event.is_set()
                    and not loop.is_closed()
                ):
                    self._start_keepalive_task()

    def _start_keepalive_task(self) -> None:
        """Start exactly one named keepalive task for the active connection."""

        if self._keepalive_task is not None and not self._keepalive_task.done():
            return
        self._keepalive_task = asyncio.create_task(
            self._keepalive_loop(),
            name=f"meshtastic-{self.transport_name}-keepalive",
        )

    @abc.abstractmethod
    def _open_client(self) -> Any:
        """Open and return transport client."""

    @abc.abstractmethod
    def _close_client(self, client: Any) -> None:
        """Close transport client and release local resources."""

    def _after_connect(self, client: Any) -> None:
        """Run transport-specific post-open setup before the client is marked connected."""

        del client

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


class _PubSubMeshtasticTransport(MeshtasticTransport):
    """Shared Meshtastic library transport using the package pubsub receive hooks."""

    _INBOUND_TOPIC = "meshtastic.receive"

    def __init__(
        self,
        *,
        transport_name: str,
        address: str,
        pubsub_bus: Any | None = None,
        keepalive_enabled: bool = True,
    ) -> None:
        super().__init__(
            transport_name=transport_name,
            address=address,
            keepalive_enabled=keepalive_enabled,
        )
        self._pubsub_bus = pubsub_bus
        self._pubsub_callback: Callable[..., None] | None = None

    def set_inbound_handler(self, handler: InboundHandler | None) -> None:
        super().set_inbound_handler(handler)
        if handler is None:
            self._unsubscribe_inbound()
        elif self._client is not None:
            self._subscribe_inbound(self._client)

    def _after_connect(self, client: Any) -> None:
        self._subscribe_inbound(client)

    def _close_client(self, client: Any) -> None:
        self._unsubscribe_inbound()
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            close_fn()

    def _send_text_client(
        self,
        client: Any,
        text: str,
        destination_id: str | None,
        channel_index: int | None,
    ) -> Any:
        send_text = getattr(client, "sendText", None)
        if not callable(send_text):
            raise RuntimeError(f"{self.transport_name} transport client does not expose sendText")

        destination = destination_id if destination_id is not None else "^all"
        channel = channel_index if channel_index is not None else 0
        return send_text(text, destinationId=destination, channelIndex=channel)

    def _subscribe_inbound(self, client: Any) -> None:
        if self._pubsub_callback is not None or self._inbound_handler is None:
            return

        pubsub_bus = self._pubsub_bus or _default_pubsub_bus()

        def _callback(packet: Any, interface: Any = None) -> None:
            if interface is not None and interface is not client:
                logger.debug(
                    "Meshtastic %s inbound packet ignored because it came from a different interface",
                    self.transport_name,
                )
                return
            self._schedule_inbound_dispatch(packet)

        subscribe = getattr(pubsub_bus, "subscribe", None)
        if not callable(subscribe):
            raise RuntimeError(
                f"{self.transport_name} transport pubsub bus does not expose subscribe"
            )

        subscribe(_callback, self._INBOUND_TOPIC)
        self._pubsub_bus = pubsub_bus
        self._pubsub_callback = _callback

    def _unsubscribe_inbound(self) -> None:
        if self._pubsub_callback is None or self._pubsub_bus is None:
            return

        unsubscribe = getattr(self._pubsub_bus, "unsubscribe", None)
        if callable(unsubscribe):
            unsubscribe(self._pubsub_callback, self._INBOUND_TOPIC)
        self._pubsub_callback = None

    def _schedule_inbound_dispatch(self, packet: Any) -> None:
        loop = self._event_loop
        if loop is None:
            logger.debug(
                "Meshtastic %s inbound packet dropped because event loop is unavailable",
                self.transport_name,
            )
            return

        payload = _as_payload(packet)
        future = asyncio.run_coroutine_threadsafe(self.emit_inbound(payload), loop)

        def _log_result(done_future):
            try:
                done_future.result()
            except Exception:
                logger.exception("Meshtastic %s inbound dispatch failed", self.transport_name)

        future.add_done_callback(_log_result)


class SerialMeshtasticTransport(_PubSubMeshtasticTransport):
    """Serial Meshtastic transport wrapper."""

    def __init__(
        self,
        serial_path: str,
        *,
        client_factory: Callable[[str], Any] | None = None,
        pubsub_bus: Any | None = None,
        keepalive_enabled: bool = True,
    ) -> None:
        super().__init__(
            transport_name="serial",
            address=serial_path,
            pubsub_bus=pubsub_bus,
            keepalive_enabled=keepalive_enabled,
        )
        self._serial_path = serial_path
        self._client_factory = client_factory or _default_serial_client_factory

    def _open_client(self) -> Any:
        return self._client_factory(self._serial_path)

    def _probe_client(self, client: Any) -> dict[str, Any]:
        local_node = getattr(client, "localNode", None)
        if local_node is None:
            return {"mode": "serial", "local_node": None}

        node_num = getattr(local_node, "nodeNum", None)
        if node_num is None:
            node_num = getattr(local_node, "num", None)
        return {"mode": "serial", "local_node": node_num}


class TcpMeshtasticTransport(_PubSubMeshtasticTransport):
    """Meshtastic TCP/protobuf transport backed by the official Python library."""

    def __init__(
        self,
        host: str,
        *,
        port: int = 4403,
        client_factory: Callable[[str, int], Any] | None = None,
        pubsub_bus: Any | None = None,
        keepalive_enabled: bool = True,
    ) -> None:
        super().__init__(
            transport_name="tcp",
            address=f"{host}:{port}",
            pubsub_bus=pubsub_bus,
            keepalive_enabled=keepalive_enabled,
        )
        self._host = host
        self._port = port
        self._client_factory = client_factory or _default_tcp_client_factory

    def _open_client(self) -> Any:
        return self._client_factory(self._host, self._port)

    def _probe_client(self, client: Any) -> dict[str, Any]:
        local_node = getattr(client, "localNode", None)
        if local_node is None:
            return {"mode": "tcp", "local_node": None, "node_count": 0}

        node_num = getattr(local_node, "nodeNum", None)
        if node_num is None:
            node_num = getattr(local_node, "num", None)

        nodes = getattr(client, "nodes", None)
        node_count = len(nodes) if isinstance(nodes, dict) else 0
        return {"mode": "tcp", "local_node": node_num, "node_count": node_count}


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
        """Send a text packet using the Meshtastic HTTP protobuf contract."""

        mesh_pb2 = importlib.import_module("meshtastic.protobuf.mesh_pb2")
        portnums_pb2 = importlib.import_module("meshtastic.protobuf.portnums_pb2")

        data = mesh_pb2.Data(
            portnum=portnums_pb2.TEXT_MESSAGE_APP,
            payload=text.encode("utf-8"),
        )
        packet = mesh_pb2.MeshPacket(
            to=_destination_id_to_uint32(destination_id),
            channel=channel_index if channel_index is not None else 0,
        )
        packet.decoded.CopyFrom(data)

        to_radio = mesh_pb2.ToRadio(packet=packet)
        body = to_radio.SerializeToString()
        url = urljoin(self._base_url, "api/v1/toradio")
        response = client.put(
            url,
            data=body,
            headers={"Content-Type": "application/x-protobuf"},
            timeout=self._send_timeout_seconds,
        )
        if response.status_code >= 400:
            response_body = (response.text or "").strip()
            raise RuntimeError(
                f"http send failed at {url}: HTTP {response.status_code}"
                + (f" body={response_body[:240]!r}" if response_body else "")
            )

        return {
            "status_code": response.status_code,
            "url": url,
            "bytes": len(body),
        }


def make_transport(
    *,
    transport: str,
    serial_path: str | None,
    http_base_url: str | None,
    tcp_host: str | None = None,
    tcp_port: int = 4403,
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

    if transport == "tcp":
        if not tcp_host:
            raise ValueError("tcp transport requires tcp_host")
        return TcpMeshtasticTransport(tcp_host, port=tcp_port)

    raise ValueError(f"unsupported transport: {transport}")


def _default_serial_client_factory(serial_path: str) -> Any:
    module = importlib.import_module("meshtastic.serial_interface")
    cls = getattr(module, "SerialInterface")

    # meshtastic-python versions differ in constructor names/signatures.
    try:
        return cls(devPath=serial_path)
    except TypeError:
        return cls(serial_path)


def _default_tcp_client_factory(host: str, port: int) -> Any:
    module = importlib.import_module("meshtastic.tcp_interface")
    cls = getattr(module, "TCPInterface")
    return cls(hostname=host, portNumber=port)


def _default_pubsub_bus() -> Any:
    module = importlib.import_module("pubsub")
    return getattr(module, "pub")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ts(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _destination_id_to_uint32(destination_id: str | None) -> int:
    """Encode a canonical Meshtastic destination ID for a MeshPacket."""

    if destination_id is None or destination_id == "^all":
        return 0xFFFFFFFF

    value = str(destination_id).strip().lower()
    if value.startswith("!"):
        value = value[1:]
    elif value.startswith("0x"):
        value = value[2:]

    try:
        destination = int(value, 16)
    except ValueError as exc:
        raise ValueError(f"invalid Meshtastic destination ID: {destination_id!r}") from exc
    if not 0 <= destination <= 0xFFFFFFFF:
        raise ValueError(f"Meshtastic destination ID is outside uint32: {destination_id!r}")
    return destination


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
