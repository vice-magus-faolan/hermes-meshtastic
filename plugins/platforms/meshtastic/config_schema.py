"""Configuration contract and normalization helpers for the Meshtastic plugin.

This module is intentionally Hermes-runtime agnostic so it can be unit-tested
in isolation from the main Hermes checkout.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping, cast
from urllib.parse import urlparse

Policy = Literal["disabled", "open", "allowlist"]
Transport = Literal["serial", "http", "tcp"]

_ALLOWED_POLICIES: set[str] = {"disabled", "open", "allowlist"}
_ALLOWED_TRANSPORT_ALIASES: dict[str, str] = {
    "serial": "serial",
    "http": "http",
    "tcp": "tcp",
    "meshtastic_tcp": "tcp",
}
_NODE_ID_RE = re.compile(r"^[0-9a-f]{8}$")


class ConfigValidationError(ValueError):
    """Raised when Meshtastic config fails contract validation."""


@dataclass(frozen=True)
class OutboundTarget:
    """Canonical outbound routing target."""

    kind: Literal["node", "channel"]
    value: str

    @property
    def canonical(self) -> str:
        return f"{self.kind}/{self.value}"


@dataclass(frozen=True)
class MeshtasticConfig:
    """Normalized Meshtastic platform configuration.

    Notes:
    - V0 allows serial, http, and Meshtastic TCP/protobuf transports.
    - Node IDs are canonicalized to ``!<8hex>``.
    - Chunk size is capped at 200 bytes for LoRa-safe behavior.
    """

    transport: Transport
    serial_path: str | None
    http_base_url: str | None
    tcp_host: str | None
    tcp_port: int
    node_name: str | None
    dm_policy: Policy
    group_policy: Policy
    dm_allowlist: tuple[str, ...]
    group_sender_allowlist: tuple[str, ...]
    allowed_channels: tuple[int, ...]
    require_mention: bool
    text_chunk_bytes: int
    chunk_delay_seconds: float


@dataclass(frozen=True)
class ConfigValidationResult:
    """Structured validation output used by tests and adapter wiring."""

    valid: bool
    errors: tuple[str, ...]
    config: MeshtasticConfig | None = None


def normalize_node_id(raw: str) -> str:
    """Normalize a Meshtastic node ID to canonical ``!<8hex>`` form."""

    value = str(raw or "").strip().lower()
    if value.startswith("!"):
        value = value[1:]
    elif value.startswith("0x"):
        value = value[2:]

    if not _NODE_ID_RE.fullmatch(value):
        raise ConfigValidationError(
            f"invalid node id '{raw}': expected 8 hex chars (canonical form !<8hex>)"
        )
    return f"!{value}"


def normalize_outbound_target(raw: str) -> OutboundTarget:
    """Normalize outbound targets to strict canonical forms.

    Accepted forms:
    - ``node/!89abcdef``
    - ``channel/0``
    - optional ``meshtastic:`` prefix on either form
    """

    value = str(raw or "").strip()
    if not value:
        raise ConfigValidationError("outbound target is empty")

    lower = value.lower()
    if lower.startswith("meshtastic:"):
        value = value[len("meshtastic:") :].strip()

    if value.startswith("node/"):
        node_id = normalize_node_id(value.split("/", 1)[1])
        return OutboundTarget(kind="node", value=node_id)

    if value.startswith("channel/"):
        channel_raw = value.split("/", 1)[1].strip()
        if not channel_raw or not channel_raw.isdigit():
            raise ConfigValidationError(
                f"invalid channel target '{raw}': expected channel/<non-negative-int>"
            )
        idx = int(channel_raw)
        if idx < 0:
            raise ConfigValidationError(
                f"invalid channel target '{raw}': channel index must be >= 0"
            )
        return OutboundTarget(kind="channel", value=str(idx))

    raise ConfigValidationError(
        "invalid outbound target format; expected node/!<8hex> or channel/<index>"
    )


def parse_config(config: Any) -> MeshtasticConfig:
    """Parse and validate a Hermes ``PlatformConfig``-like object."""

    extra = getattr(config, "extra", {}) or {}
    if not isinstance(extra, Mapping):
        raise ConfigValidationError("platform.extra must be a mapping")
    return parse_extra(extra)


def parse_extra(extra: Mapping[str, Any]) -> MeshtasticConfig:
    """Parse and validate ``PlatformConfig.extra`` for Meshtastic."""

    transport_raw = str(extra.get("transport", "") or "").strip().lower()
    transport = _ALLOWED_TRANSPORT_ALIASES.get(transport_raw)
    if transport is None:
        raise ConfigValidationError(
            "transport must be one of: serial, http, tcp (alias: meshtastic_tcp)"
        )

    serial_path = _normalize_optional_str(extra.get("serial_path"))
    http_base_url = _normalize_optional_str(extra.get("http_base_url"))
    tcp_host = _normalize_optional_str(extra.get("tcp_host"))
    tcp_port = _parse_tcp_port(extra.get("tcp_port", 4403))

    if transport == "serial":
        if not serial_path:
            raise ConfigValidationError("serial transport requires extra.serial_path")
        if http_base_url:
            raise ConfigValidationError(
                "serial transport must not set extra.http_base_url"
            )
        if tcp_host:
            raise ConfigValidationError("serial transport must not set extra.tcp_host")
        if "tcp_port" in extra and tcp_port != 4403:
            raise ConfigValidationError("serial transport must not set extra.tcp_port")

    if transport == "http":
        if not http_base_url:
            raise ConfigValidationError("http transport requires extra.http_base_url")
        _validate_http_base_url(http_base_url)
        if serial_path:
            raise ConfigValidationError("http transport must not set extra.serial_path")
        if tcp_host:
            raise ConfigValidationError("http transport must not set extra.tcp_host")
        if "tcp_port" in extra and tcp_port != 4403:
            raise ConfigValidationError("http transport must not set extra.tcp_port")

    if transport == "tcp":
        if not tcp_host:
            raise ConfigValidationError("tcp transport requires extra.tcp_host")
        if serial_path:
            raise ConfigValidationError("tcp transport must not set extra.serial_path")
        if http_base_url:
            raise ConfigValidationError("tcp transport must not set extra.http_base_url")

    dm_policy = _parse_policy(extra.get("dm_policy"), default="allowlist")
    group_policy = _parse_policy(extra.get("group_policy"), default="allowlist")

    dm_allowlist = _parse_node_allowlist(extra.get("dm_allowlist"))
    group_sender_allowlist = _parse_node_allowlist(extra.get("group_sender_allowlist"))

    allowed_channels = _parse_channel_allowlist(extra.get("allowed_channels"))
    require_mention = _parse_bool(extra.get("require_mention"), default=True)

    text_chunk_bytes = _parse_positive_int(
        extra.get("text_chunk_bytes", 200),
        field_name="text_chunk_bytes",
    )
    if text_chunk_bytes > 200:
        raise ConfigValidationError("text_chunk_bytes must be <= 200 for V0")

    chunk_delay_seconds = _parse_positive_float(
        extra.get("chunk_delay_seconds", 1.5),
        field_name="chunk_delay_seconds",
    )

    node_name = _normalize_optional_str(extra.get("node_name"))

    if dm_policy == "allowlist" and not dm_allowlist:
        raise ConfigValidationError(
            "dm_policy=allowlist requires at least one dm_allowlist node id"
        )

    if group_policy == "allowlist" and not allowed_channels:
        raise ConfigValidationError(
            "group_policy=allowlist requires at least one allowed_channels entry"
        )

    return MeshtasticConfig(
        transport=cast(Transport, transport),
        serial_path=serial_path,
        http_base_url=http_base_url,
        tcp_host=tcp_host,
        tcp_port=tcp_port,
        node_name=node_name,
        dm_policy=dm_policy,
        group_policy=group_policy,
        dm_allowlist=dm_allowlist,
        group_sender_allowlist=group_sender_allowlist,
        allowed_channels=allowed_channels,
        require_mention=require_mention,
        text_chunk_bytes=text_chunk_bytes,
        chunk_delay_seconds=chunk_delay_seconds,
    )


def validate_config(config: Any) -> ConfigValidationResult:
    """Validate config and return a structured result for adapter hooks."""

    try:
        parsed = parse_config(config)
        return ConfigValidationResult(valid=True, errors=(), config=parsed)
    except ConfigValidationError as exc:
        return ConfigValidationResult(valid=False, errors=(str(exc),), config=None)


def _normalize_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_policy(value: Any, *, default: Policy) -> Policy:
    if value is None or str(value).strip() == "":
        return default
    normalized = str(value).strip().lower()
    if normalized not in _ALLOWED_POLICIES:
        raise ConfigValidationError(
            f"invalid policy '{value}': expected one of {sorted(_ALLOWED_POLICIES)}"
        )
    return normalized  # type: ignore[return-value]


def _parse_node_allowlist(value: Any) -> tuple[str, ...]:
    if value is None or value == "":
        return ()

    if isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, (list, tuple, set)):
        raw_items = [str(item).strip() for item in value if str(item).strip()]
    else:
        raise ConfigValidationError("allowlist must be a list or comma-separated string")

    normalized = []
    for item in raw_items:
        node = normalize_node_id(item)
        if node not in normalized:
            normalized.append(node)
    return tuple(normalized)


def _parse_channel_allowlist(value: Any) -> tuple[int, ...]:
    if value is None or value == "":
        return ()

    if isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, (list, tuple, set)):
        raw_items = [str(item).strip() for item in value if str(item).strip()]
    else:
        raise ConfigValidationError(
            "allowed_channels must be a list or comma-separated string"
        )

    channels: list[int] = []
    for item in raw_items:
        if not item.isdigit():
            raise ConfigValidationError(
                f"invalid channel '{item}': expected non-negative integer"
            )
        idx = int(item)
        if idx < 0:
            raise ConfigValidationError("allowed channel index must be >= 0")
        if idx not in channels:
            channels.append(idx)
    return tuple(channels)


def _parse_tcp_port(value: Any) -> int:
    if value is None or value == "":
        return 4403
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigValidationError("tcp_port must be an integer") from exc
    if parsed <= 0 or parsed > 65535:
        raise ConfigValidationError("tcp_port must be between 1 and 65535")
    return parsed


def _parse_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigValidationError(f"invalid boolean value '{value}'")


def _parse_positive_int(value: Any, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigValidationError(f"{field_name} must be an integer") from exc
    if parsed <= 0:
        raise ConfigValidationError(f"{field_name} must be > 0")
    return parsed


def _parse_positive_float(value: Any, *, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigValidationError(f"{field_name} must be a number") from exc
    if parsed <= 0:
        raise ConfigValidationError(f"{field_name} must be > 0")
    return parsed


def _validate_http_base_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ConfigValidationError("http_base_url must start with http:// or https://")
    if not parsed.netloc:
        raise ConfigValidationError("http_base_url must include host[:port]")
