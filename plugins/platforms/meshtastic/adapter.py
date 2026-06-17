"""Hermes Meshtastic platform adapter.

This adapter implements V0 transport lifecycle and message handling:
- timeout-bounded connect/disconnect
- reconnect handling and keepalive-driven stale-session recovery
- transport-level probe diagnostics
- inbound normalization and policy-gated routing for DM/group traffic
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional

from .config_schema import (
    ConfigValidationError,
    normalize_node_id,
    normalize_outbound_target,
    parse_config,
    validate_config as validate_meshtastic_config,
)
from .transport import MeshtasticTransport, make_transport

logger = logging.getLogger(__name__)

_MESHTASTIC_PLATFORM_HINT = (
    "You are chatting over Meshtastic radio. Keep responses short and plain "
    "text only. No markdown, no emojis, no long preambles. Prioritize the "
    "answer first, then only the minimum context needed."
)


@dataclass(frozen=True)
class InboundRoute:
    """Normalized inbound envelope used for Hermes session routing."""

    text: str
    message_id: str | None
    sender_node_id: str
    sender_name: str | None
    is_group: bool
    channel_index: int | None
    channel_name: str | None
    reply_target: str
    received_at: datetime
    raw_payload: Mapping[str, Any]


class _PlatformValue(str):
    """String wrapper that mimics Enum-style ``.value`` access."""

    @property
    def value(self) -> str:
        return str(self)


# Hermes runtime types are optional for local/unit testing in this standalone repo.
try:  # pragma: no cover - exercised in Hermes runtime, not local scaffold tests.
    _gateway_config = importlib.import_module("gateway.config")
    _gateway_base = importlib.import_module("gateway.platforms.base")
    Platform = _gateway_config.Platform
    BasePlatformAdapter = _gateway_base.BasePlatformAdapter
    MessageEvent = _gateway_base.MessageEvent
    MessageType = _gateway_base.MessageType
    SendResult = _gateway_base.SendResult
except Exception:  # pragma: no cover

    class MessageType(str, Enum):  # type: ignore[no-redef]
        TEXT = "text"

    @dataclass
    class MessageEvent:  # type: ignore[no-redef]
        text: str
        message_type: MessageType = MessageType.TEXT
        source: Any = None
        raw_message: Any = None
        message_id: Optional[str] = None
        platform_update_id: Optional[int] = None
        media_urls: list[str] = field(default_factory=list)
        media_types: list[str] = field(default_factory=list)
        reply_to_message_id: Optional[str] = None
        reply_to_text: Optional[str] = None
        auto_skill: Optional[str | list[str]] = None
        channel_prompt: Optional[str] = None
        channel_context: Optional[str] = None
        internal: bool = False
        timestamp: datetime = field(default_factory=datetime.now)

    @dataclass
    class _FallbackSessionSource:
        platform: Any
        chat_id: str
        chat_name: Optional[str] = None
        chat_type: str = "dm"
        user_id: Optional[str] = None
        user_name: Optional[str] = None
        thread_id: Optional[str] = None
        chat_topic: Optional[str] = None
        message_id: Optional[str] = None

    @dataclass
    class SendResult:  # type: ignore[no-redef]
        success: bool
        message_id: Optional[str] = None
        error: Optional[str] = None
        raw_response: Any = None
        retryable: bool = False

    class Platform(str):
        """Fallback shim used only for local tests."""

    class BasePlatformAdapter:  # type: ignore[no-redef]
        """Fallback shim used only for local tests."""

        def __init__(self, config: Any, platform: Any):
            self.config = config
            self.platform = platform
            self._fatal_error_code: str | None = None
            self._fatal_error_message: str | None = None
            self._fatal_error_retryable: bool = True
            self._running = False

        def _set_fatal_error(
            self,
            code: str,
            message: str,
            *,
            retryable: bool,
        ) -> None:
            self._fatal_error_code = code
            self._fatal_error_message = message
            self._fatal_error_retryable = retryable

        def _mark_connected(self) -> None:
            self._running = True

        def _mark_disconnected(self) -> None:
            self._running = False

        def build_source(
            self,
            chat_id: str,
            chat_name: Optional[str] = None,
            chat_type: str = "dm",
            user_id: Optional[str] = None,
            user_name: Optional[str] = None,
            thread_id: Optional[str] = None,
            chat_topic: Optional[str] = None,
            message_id: Optional[str] = None,
            **_: Any,
        ) -> _FallbackSessionSource:
            return _FallbackSessionSource(
                platform=self.platform,
                chat_id=str(chat_id),
                chat_name=chat_name,
                chat_type=chat_type,
                user_id=str(user_id) if user_id else None,
                user_name=user_name,
                thread_id=str(thread_id) if thread_id else None,
                chat_topic=chat_topic,
                message_id=str(message_id) if message_id else None,
            )

        async def handle_message(self, event: MessageEvent) -> None:
            del event


class MeshtasticAdapter(BasePlatformAdapter):  # type: ignore[misc]
    """Hermes Meshtastic adapter with transport lifecycle management."""

    def __init__(self, config: Any, **kwargs: Any):
        try:
            platform = Platform("meshtastic")
        except Exception:  # pragma: no cover - legacy enums without dynamic values
            platform = _PlatformValue("meshtastic")
        if not hasattr(platform, "value"):
            platform = _PlatformValue(str(platform))
        super().__init__(config=config, platform=platform)

        self._cfg = parse_config(config)
        self._last_inbound_activity: datetime | None = None
        self._last_outbound_activity: datetime | None = None
        self._transport: MeshtasticTransport = kwargs.get("transport") or make_transport(
            transport=self._cfg.transport,
            serial_path=self._cfg.serial_path,
            http_base_url=self._cfg.http_base_url,
            tcp_host=self._cfg.tcp_host,
            tcp_port=self._cfg.tcp_port,
        )

    @property
    def name(self) -> str:
        return "Meshtastic"

    async def connect(self) -> bool:
        """Connect configured transport and update adapter connection state."""

        self._set_transport_inbound_handler(self.handle_inbound)
        ok = await self._transport.connect()
        if ok:
            self._mark_connected()
            self._set_fatal_error("", "", retryable=True)
            return True

        self._mark_disconnected()
        status = self._transport.status()
        self._set_fatal_error(
            "transport_connect_failed",
            status.last_error or "transport connect failed",
            retryable=True,
        )
        return False

    async def disconnect(self) -> None:
        self._set_transport_inbound_handler(None)
        await self._transport.disconnect()
        self._mark_disconnected()

    def _set_transport_inbound_handler(self, handler: Any) -> None:
        """Register or clear the transport-originated inbound bridge when supported."""

        setter = getattr(self._transport, "set_inbound_handler", None)
        if callable(setter):
            setter(handler)

    def normalize_inbound(self, payload: Mapping[str, Any]) -> InboundRoute:
        """Normalize a transport packet into Hermes routing primitives."""

        return _normalize_inbound_payload(payload)

    def build_inbound_event(self, payload: Mapping[str, Any]) -> MessageEvent:
        """Build a Hermes ``MessageEvent`` with stable DM/group session identity."""

        route = self.normalize_inbound(payload)

        thread_id: str | None = None
        if route.is_group:
            assert route.channel_index is not None
            chat_id = f"channel/{route.channel_index}"
            chat_name = route.channel_name or f"Meshtastic channel {route.channel_index}"
            chat_type = "group"
            # Preserve channel-level outbound targeting while isolating Hermes
            # group sessions per sender within the same channel.
            thread_id = f"sender/{route.sender_node_id}"
        else:
            chat_id = f"node/{route.sender_node_id}"
            chat_name = route.sender_name or f"Meshtastic node {route.sender_node_id}"
            chat_type = "dm"

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=route.sender_node_id,
            user_name=route.sender_name,
            thread_id=thread_id,
            message_id=route.message_id,
        )

        raw_message = {
            "meshtastic": {
                "sender_node_id": route.sender_node_id,
                "sender_name": route.sender_name,
                "is_group": route.is_group,
                "channel_index": route.channel_index,
                "channel_name": route.channel_name,
                "reply_target": route.reply_target,
                "transport": self._cfg.transport,
            },
            "payload": dict(route.raw_payload),
        }

        return MessageEvent(
            text=route.text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=raw_message,
            message_id=route.message_id,
            channel_prompt=_MESHTASTIC_PLATFORM_HINT,
            timestamp=route.received_at,
        )

    async def handle_inbound(self, payload: Mapping[str, Any]) -> bool:
        """Normalize, policy-check, and route inbound packets into Hermes."""

        try:
            route = self.normalize_inbound(payload)
        except Exception as exc:
            logger.warning("Meshtastic inbound normalization failed: %s", exc)
            return False

        if not self._is_inbound_authorized(route, payload):
            return False

        event = self.build_inbound_event(route.raw_payload)
        self._last_inbound_activity = event.timestamp.astimezone(timezone.utc)
        await self.handle_message(event)
        return True

    def _is_inbound_authorized(
        self, route: InboundRoute, payload: Mapping[str, Any]
    ) -> bool:
        """Return True when inbound traffic passes V0 DM/group policy gates."""

        if route.is_group:
            return self._is_group_authorized(route=route, payload=payload)
        return self._is_dm_authorized(route)

    def _is_dm_authorized(self, route: InboundRoute) -> bool:
        """Apply DM policy gates using disabled/open/allowlist semantics."""

        policy = self._cfg.dm_policy
        if policy == "disabled":
            logger.info("Meshtastic DM denied: dm_policy=disabled sender=%s", route.sender_node_id)
            return False

        if policy == "open":
            return True

        authorized = route.sender_node_id in self._cfg.dm_allowlist
        if not authorized:
            logger.info(
                "Meshtastic DM denied: sender not in dm_allowlist sender=%s",
                route.sender_node_id,
            )
        return authorized

    def _is_group_authorized(
        self, *, route: InboundRoute, payload: Mapping[str, Any]
    ) -> bool:
        """Apply group policy, channel allowlist, sender allowlist, and mention gate."""

        assert route.channel_index is not None

        if _is_control_command(route.text):
            logger.info(
                "Meshtastic group denied: control commands are DM-only sender=%s channel=%s",
                route.sender_node_id,
                route.channel_index,
            )
            return False

        policy = self._cfg.group_policy
        if policy == "disabled":
            logger.info(
                "Meshtastic group denied: group_policy=disabled sender=%s channel=%s",
                route.sender_node_id,
                route.channel_index,
            )
            return False

        if (
            policy == "allowlist"
            and route.channel_index not in self._cfg.allowed_channels
        ):
            logger.info(
                "Meshtastic group denied: channel not in allowed_channels sender=%s channel=%s",
                route.sender_node_id,
                route.channel_index,
            )
            return False

        if self._cfg.group_sender_allowlist and (
            route.sender_node_id not in self._cfg.group_sender_allowlist
        ):
            logger.info(
                "Meshtastic group denied: sender not in group_sender_allowlist sender=%s channel=%s",
                route.sender_node_id,
                route.channel_index,
            )
            return False

        if self._cfg.require_mention and not _has_required_mention(
            payload=payload,
            text=route.text,
            node_name=self._cfg.node_name,
        ):
            logger.info(
                "Meshtastic group denied: require_mention unsatisfied sender=%s channel=%s",
                route.sender_node_id,
                route.channel_index,
            )
            return False

        return True

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        thread_id: Optional[str] = None,
        **kwargs: Any,
    ) -> SendResult:
        """Send plain-text replies over Meshtastic with chunking and pacing."""

        del reply_to, thread_id

        try:
            target = normalize_outbound_target(chat_id)
        except ConfigValidationError as exc:
            return SendResult(success=False, error=str(exc), retryable=False)

        if _contains_unsupported_payload(kwargs):
            return SendResult(
                success=False,
                error="Meshtastic supports plain-text outbound messages only",
                retryable=False,
            )

        try:
            text = _coerce_plain_text(content)
            chunks = _chunk_text(text, self._cfg.text_chunk_bytes)
        except ValueError as exc:
            return SendResult(success=False, error=str(exc), retryable=False)

        receipts: list[dict[str, Any]] = []

        for idx, chunk in enumerate(chunks):
            try:
                receipt = await self._transport.send_text(
                    text=chunk,
                    destination_id=target.value if target.kind == "node" else None,
                    channel_index=int(target.value) if target.kind == "channel" else None,
                )
            except Exception as exc:
                sent_chunks = len(receipts)
                retryable = sent_chunks == 0
                error = (
                    f"meshtastic outbound send failed on chunk {idx + 1}/{len(chunks)}: {exc}"
                )
                self._set_fatal_error("outbound_send_failed", error, retryable=retryable)
                return SendResult(
                    success=False,
                    error=error,
                    retryable=retryable,
                    raw_response={
                        "sent_chunks": sent_chunks,
                        "total_chunks": len(chunks),
                        "chunk_bytes_limit": self._cfg.text_chunk_bytes,
                    },
                )

            receipts.append(
                {
                    "chunk_index": idx,
                    "message_id": receipt.message_id,
                    "raw_response": receipt.raw_response,
                }
            )
            self._last_outbound_activity = datetime.now(timezone.utc)

            if idx + 1 < len(chunks):
                await asyncio.sleep(self._cfg.chunk_delay_seconds)

        self._set_fatal_error("", "", retryable=True)
        return SendResult(
            success=True,
            message_id=receipts[-1]["message_id"] if receipts else None,
            raw_response={
                "chunks": receipts,
                "chunk_count": len(receipts),
                "chunk_bytes_limit": self._cfg.text_chunk_bytes,
                "chunk_delay_seconds": self._cfg.chunk_delay_seconds,
            },
            retryable=False,
        )

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """Return minimal chat metadata for scaffold-resolved Meshtastic targets."""

        target = normalize_outbound_target(chat_id)
        if target.kind == "node":
            return {
                "id": target.canonical,
                "name": f"Meshtastic node {target.value}",
                "type": "dm",
            }

        return {
            "id": target.canonical,
            "name": f"Meshtastic channel {target.value}",
            "type": "channel",
        }

    async def probe(self) -> dict[str, Any]:
        """Return operator-visible diagnostics including transport liveness."""

        transport_probe = await self._transport.probe()
        status = self._transport.status()

        return {
            "running": bool(getattr(self, "_running", False)) and status.connected,
            "transport": status.transport,
            "transport_type": status.transport,
            "transport_address": status.address,
            "transport_path_or_address": status.address,
            "transport_connected": status.connected,
            "transport_probe": transport_probe,
            "last_probe_result": status.last_probe_result,
            "last_probe_at": status.last_probe_at,
            "reconnect_attempts": status.reconnect_attempts,
            "keepalive_failures": status.keepalive_failures,
            "last_inbound_activity": _ts(self._last_inbound_activity),
            "last_outbound_activity": _ts(self._last_outbound_activity),
            "last_successful_probe": status.last_probe_success,
            "last_probe_failure": status.last_probe_failure,
            "last_error": status.last_error or getattr(self, "_fatal_error_message", None),
            "implemented": True,
        }


def check_requirements() -> bool:
    """Transport dependencies are loaded lazily at connect-time."""

    return True


def validate_config(config: Any) -> bool:
    """PlatformEntry hook: strict config contract validation."""

    result = validate_meshtastic_config(config)
    if not result.valid:
        logger.warning("Meshtastic config invalid: %s", "; ".join(result.errors))
    return result.valid


def is_connected(config: Any) -> bool:
    """Conservative status hook for static platform inspection.

    ``is_connected`` is called without adapter runtime state in many contexts.
    We validate config for operator feedback but return ``False`` unless the
    live adapter instance reports transport state via ``probe()``.
    """

    try:
        parse_config(config)
    except ConfigValidationError as exc:
        logger.debug("Meshtastic is_connected config parse failed: %s", exc)
    return False


def _env_enablement() -> dict | None:
    """Seed ``PlatformConfig.extra`` from env vars for env-only setups.

    This allows ``gateway status`` to display canonical config fields even
    before adapter instantiation.
    """

    transport = os.getenv("MESHTASTIC_TRANSPORT", "").strip().lower()
    if transport == "meshtastic_tcp":
        transport = "tcp"
    if transport not in {"serial", "http", "tcp"}:
        return None

    seed: dict[str, Any] = {"transport": transport}

    serial_path = os.getenv("MESHTASTIC_SERIAL_PATH", "").strip()
    http_base_url = os.getenv("MESHTASTIC_HTTP_BASE_URL", "").strip()
    tcp_host = os.getenv("MESHTASTIC_TCP_HOST", "").strip()
    tcp_port = os.getenv("MESHTASTIC_TCP_PORT", "").strip()

    if transport == "serial":
        if not serial_path:
            return None
        seed["serial_path"] = serial_path
    elif transport == "http":
        if not http_base_url:
            return None
        seed["http_base_url"] = http_base_url
    elif transport == "tcp":
        if not tcp_host:
            return None
        seed["tcp_host"] = tcp_host
        seed["tcp_port"] = tcp_port or "4403"

    optional_map = {
        "MESHTASTIC_NODE_NAME": "node_name",
        "MESHTASTIC_DM_POLICY": "dm_policy",
        "MESHTASTIC_GROUP_POLICY": "group_policy",
        "MESHTASTIC_DM_ALLOWLIST": "dm_allowlist",
        "MESHTASTIC_GROUP_SENDER_ALLOWLIST": "group_sender_allowlist",
        "MESHTASTIC_ALLOWED_CHANNELS": "allowed_channels",
        "MESHTASTIC_REQUIRE_MENTION": "require_mention",
        "MESHTASTIC_TEXT_CHUNK_BYTES": "text_chunk_bytes",
        "MESHTASTIC_CHUNK_DELAY_SECONDS": "chunk_delay_seconds",
    }
    for env_key, cfg_key in optional_map.items():
        value = os.getenv(env_key, "").strip()
        if value:
            seed[cfg_key] = value

    home = os.getenv("MESHTASTIC_HOME_CHANNEL", "").strip()
    if home:
        try:
            target = normalize_outbound_target(home)
        except ConfigValidationError:
            logger.warning(
                "Ignoring invalid MESHTASTIC_HOME_CHANNEL=%r (expected node/... or channel/...)",
                home,
            )
        else:
            seed["home_channel"] = {
                "chat_id": target.canonical,
                "name": f"Meshtastic {target.canonical}",
            }

    return seed


def register(ctx) -> None:
    """Plugin entrypoint: register Meshtastic platform adapter."""

    ctx.register_platform(
        name="meshtastic",
        label="Meshtastic",
        adapter_factory=lambda cfg: MeshtasticAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["MESHTASTIC_TRANSPORT"],
        install_hint=(
            "Configure MESHTASTIC_TRANSPORT plus transport-specific env vars "
            "(MESHTASTIC_SERIAL_PATH, MESHTASTIC_HTTP_BASE_URL, or "
            "MESHTASTIC_TCP_HOST[/MESHTASTIC_TCP_PORT])."
        ),
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MESHTASTIC_HOME_CHANNEL",
        allowed_users_env="MESHTASTIC_DM_ALLOWLIST",
        allow_all_env="MESHTASTIC_ALLOW_ALL_USERS",
        max_message_length=200,
        emoji="📡",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=_MESHTASTIC_PLATFORM_HINT,
    )


def _normalize_inbound_payload(payload: Mapping[str, Any]) -> InboundRoute:
    """Normalize a raw Meshtastic inbound payload for Hermes routing."""

    text = _coerce_plain_text(payload.get("text"))

    sender_raw = payload.get("sender_node_id")
    if sender_raw is None:
        sender_raw = payload.get("sender")
    if sender_raw is None:
        sender_raw = payload.get("from")
    if sender_raw is None:
        raise ValueError("inbound payload missing sender_node_id")

    sender_node_id = _normalize_sender_node_id(str(sender_raw))

    sender_name_raw = payload.get("sender_name")
    if sender_name_raw is None:
        sender_name_raw = payload.get("senderName")
    sender_name = _normalize_optional_text(sender_name_raw)

    channel_index = _parse_optional_channel_index(
        payload.get("channel_index", payload.get("channelIndex"))
    )

    is_group_raw = payload.get("is_group", payload.get("isGroup"))
    is_group_parsed = _coerce_optional_bool(is_group_raw)
    if is_group_parsed is None:
        is_group = channel_index is not None
    else:
        is_group = is_group_parsed

    if is_group and channel_index is None:
        raise ValueError("group inbound payload requires channel_index")

    channel_name_raw = payload.get("channel_name")
    if channel_name_raw is None:
        channel_name_raw = payload.get("channelName")
    channel_name = _normalize_optional_text(channel_name_raw)

    message_id_raw = payload.get("message_id")
    if message_id_raw is None:
        message_id_raw = payload.get("messageId")
    message_id = _normalize_optional_text(message_id_raw)

    received_at = _parse_inbound_timestamp(payload.get("timestamp"))

    reply_target = f"channel/{channel_index}" if is_group else f"node/{sender_node_id}"

    return InboundRoute(
        text=text,
        message_id=message_id,
        sender_node_id=sender_node_id,
        sender_name=sender_name,
        is_group=is_group,
        channel_index=channel_index,
        channel_name=channel_name,
        reply_target=reply_target,
        received_at=received_at,
        raw_payload=dict(payload),
    )


def _contains_unsupported_payload(kwargs: dict[str, Any]) -> bool:
    """Return true when callers request non-text payload features.

    Meshtastic outbound delivery is plain-text only in V0.
    """

    unsupported_keys = (
        "attachments",
        "media",
        "files",
        "images",
        "embeds",
        "blocks",
        "audio",
        "video",
        "document",
    )
    for key in unsupported_keys:
        if kwargs.get(key):
            return True

    content_type = kwargs.get("content_type")
    if content_type and str(content_type).strip().lower() not in {"text", "text/plain"}:
        return True

    return False


def _coerce_plain_text(content: Any) -> str:
    """Normalize outbound content to plain text and reject empty payloads."""

    if not isinstance(content, str):
        raise ValueError("content must be a non-empty plain-text string")

    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    filtered = "".join(
        ch for ch in normalized if ch in {"\n", "\t"} or ch.isprintable()
    )
    text = "\n".join(line.rstrip() for line in filtered.split("\n")).strip()
    if not text:
        raise ValueError("content must be a non-empty plain-text string")
    return text


def _normalize_optional_text(value: Any) -> str | None:
    """Return stripped text or ``None`` when value is null/empty."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_sender_node_id(value: str) -> str:
    """Normalize inbound sender node id to canonical ``!<8hex>`` form."""

    sender_node_id = value.strip()
    if not sender_node_id:
        raise ValueError("inbound payload has empty sender_node_id")

    try:
        return normalize_node_id(sender_node_id)
    except ConfigValidationError as exc:
        raise ValueError(str(exc)) from exc


def _parse_optional_channel_index(value: Any) -> int | None:
    """Parse optional Meshtastic channel index from inbound payload."""

    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("channel_index must be an integer")
    if isinstance(value, int):
        if value < 0:
            raise ValueError("channel_index must be >= 0")
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError as exc:
        raise ValueError("channel_index must be an integer") from exc
    if parsed < 0:
        raise ValueError("channel_index must be >= 0")
    return parsed


def _coerce_optional_bool(value: Any) -> bool | None:
    """Parse optional boolean values from bool/int/string payload fields."""

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False

    raise ValueError("is_group must be a boolean")


def _is_control_command(text: str) -> bool:
    """Return True when inbound text is a control/admin command."""

    stripped = text.strip()
    return stripped.startswith("/")


def _has_required_mention(
    *,
    payload: Mapping[str, Any],
    text: str,
    node_name: str | None,
) -> bool:
    """Return True when mention gate is satisfied for group inbound traffic.

    Signal precedence:
    1) explicit mention booleans from payload
    2) structured mention lists from payload
    3) inline textual mention of configured node_name

    If no mention signal exists and no node_name is configured, this returns False.
    require_mention is deny-by-default: transports that do not expose mention metadata
    must set node_name for textual fallback or group traffic is dropped.
    """

    explicit = _extract_mention_bool(payload)
    if explicit is not None:
        return explicit

    aliases = _mention_aliases(node_name)

    mentions = _extract_mentions(payload)
    if mentions:
        for mention in mentions:
            normalized = _normalize_mention_token(mention)
            if normalized and normalized in aliases:
                return True
        return False

    if aliases:
        normalized_tokens = {
            _normalize_mention_token(token) for token in _tokenize_for_mentions(text)
        }
        normalized_tokens.discard("")
        if normalized_tokens.intersection(aliases):
            return True

        text_lower = text.lower()
        for alias in aliases:
            if alias in text_lower and f"@{alias}" in text_lower:
                return True
        return False

    return False


def _extract_mention_bool(payload: Mapping[str, Any]) -> bool | None:
    """Read optional boolean mention hints from inbound payload mappings."""

    for key in (
        "mentioned",
        "is_mentioned",
        "isMentioned",
        "mentions_me",
        "mentionsMe",
    ):
        if key in payload:
            return _coerce_optional_bool(payload.get(key))
    return None


def _extract_mentions(payload: Mapping[str, Any]) -> tuple[str, ...]:
    """Extract mention tokens from common payload fields."""

    for key in (
        "mentions",
        "mentioned_nodes",
        "mentionedNodes",
        "mentioned_node_ids",
        "mentionedNodeIds",
    ):
        raw = payload.get(key)
        if raw is None:
            continue
        if isinstance(raw, str):
            return tuple(part.strip() for part in raw.split(",") if part.strip())
        if isinstance(raw, (list, tuple, set)):
            return tuple(str(item).strip() for item in raw if str(item).strip())
    return ()


def _mention_aliases(node_name: str | None) -> set[str]:
    """Build normalized mention aliases for this adapter instance."""

    aliases: set[str] = set()
    if node_name:
        aliases.add(_normalize_mention_token(node_name))
    aliases.discard("")
    return aliases


def _normalize_mention_token(token: str) -> str:
    """Normalize mention token text for case-insensitive matching."""

    normalized = token.strip().lower()
    while normalized.startswith("@"):
        normalized = normalized[1:]
    return normalized.strip()


def _tokenize_for_mentions(text: str) -> tuple[str, ...]:
    """Split inbound text into coarse tokens for mention matching."""

    return tuple(text.replace("\n", " ").split())


def _parse_inbound_timestamp(value: Any) -> datetime:
    """Parse inbound timestamp into an aware UTC datetime."""

    if value is None:
        return datetime.now(timezone.utc)

    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, (int, float)):
        ts = datetime.fromtimestamp(float(value), tz=timezone.utc)
    else:
        text = str(value).strip()
        if not text:
            return datetime.now(timezone.utc)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            ts = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError("timestamp must be epoch seconds or ISO-8601") from exc

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _chunk_text(text: str, limit_bytes: int) -> list[str]:
    """Chunk plain text on UTF-8 byte boundaries with whitespace-aware splits."""

    if len(text.encode("utf-8")) <= limit_bytes:
        return [text]

    chunks: list[str] = []
    remaining = text

    while remaining:
        if len(remaining.encode("utf-8")) <= limit_bytes:
            chunks.append(remaining)
            break

        prefix_len = _prefix_fit_bytes(remaining, limit_bytes)
        if prefix_len <= 0:
            raise ValueError(
                f"text chunking failed: single character exceeds limit_bytes={limit_bytes}"
            )
        candidate = remaining[:prefix_len]

        break_at = candidate.rfind(" ")
        if break_at <= int(prefix_len * 0.4):
            break_at = prefix_len

        chunk = remaining[:break_at].rstrip()
        if not chunk:
            chunk = candidate
            break_at = len(chunk)

        chunks.append(chunk)
        remaining = remaining[break_at:].lstrip()

    return chunks


def _prefix_fit_bytes(text: str, limit_bytes: int) -> int:
    """Return the largest prefix length that fits within ``limit_bytes`` UTF-8 bytes."""

    used = 0
    for idx, ch in enumerate(text):
        size = len(ch.encode("utf-8"))
        if used + size > limit_bytes:
            return idx
        used += size
    return len(text)


def _ts(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()
