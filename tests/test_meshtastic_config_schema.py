from __future__ import annotations

import abc
import asyncio
import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from plugins.platforms.meshtastic import adapter
from plugins.platforms.meshtastic.config_schema import (
    ConfigValidationError,
    normalize_node_id,
    normalize_outbound_target,
    parse_extra,
    validate_config,
)
from tests.meshtastic_harness import cfg, valid_serial_extra


def test_normalize_node_id_accepts_canonical_and_variants() -> None:
    assert normalize_node_id("!89ABCDEF") == "!89abcdef"
    assert normalize_node_id("89abcdef") == "!89abcdef"
    assert normalize_node_id("0x89abcdef") == "!89abcdef"


def test_normalize_node_id_rejects_invalid() -> None:
    with pytest.raises(ConfigValidationError):
        normalize_node_id("!1234")


def test_normalize_target_node_with_prefix() -> None:
    target = normalize_outbound_target("meshtastic:node/89abcdef")
    assert target.kind == "node"
    assert target.value == "!89abcdef"
    assert target.canonical == "node/!89abcdef"


def test_normalize_target_channel() -> None:
    target = normalize_outbound_target("channel/7")
    assert target.kind == "channel"
    assert target.value == "7"


def test_normalize_target_rejects_ambiguous_format() -> None:
    with pytest.raises(ConfigValidationError):
        normalize_outbound_target("!89abcdef")


def test_parse_serial_config_ok() -> None:
    parsed = parse_extra(valid_serial_extra())
    assert parsed.transport == "serial"
    assert parsed.serial_path == "/dev/ttyUSB0"
    assert parsed.dm_allowlist == ("!89abcdef",)


def test_parse_http_config_ok() -> None:
    parsed = parse_extra(
        {
            "transport": "http",
            "http_base_url": "http://192.168.1.10:4403",
            "dm_policy": "allowlist",
            "group_policy": "allowlist",
            "dm_allowlist": ["!89abcdef"],
            "allowed_channels": [0],
        }
    )
    assert parsed.transport == "http"
    assert parsed.http_base_url == "http://192.168.1.10:4403"


def test_parse_rejects_transport_mismatch_fields() -> None:
    extra = valid_serial_extra()
    extra["http_base_url"] = "http://192.168.1.10:4403"
    with pytest.raises(ConfigValidationError):
        parse_extra(extra)


def test_parse_rejects_allowlist_policy_without_allowlist() -> None:
    extra = valid_serial_extra()
    extra["dm_allowlist"] = []
    with pytest.raises(ConfigValidationError):
        parse_extra(extra)


def test_parse_rejects_group_allowlist_without_channels() -> None:
    extra = valid_serial_extra()
    extra["allowed_channels"] = []
    with pytest.raises(ConfigValidationError):
        parse_extra(extra)


def test_parse_rejects_chunk_size_above_v0_cap() -> None:
    extra = valid_serial_extra()
    extra["text_chunk_bytes"] = 201
    with pytest.raises(ConfigValidationError):
        parse_extra(extra)


def test_validate_config_result_shape() -> None:
    ok = validate_config(cfg(valid_serial_extra()))
    assert ok.valid is True
    assert ok.errors == ()

    bad = validate_config(cfg({"transport": "serial"}))
    assert bad.valid is False
    assert bad.errors


def test_adapter_validate_config_and_is_connected_fail_closed() -> None:
    config = cfg(valid_serial_extra())
    assert adapter.validate_config(config) is True
    assert adapter.is_connected(config) is False


def test_adapter_get_chat_info_scaffold_shape() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    dm_info = asyncio.run(meshtastic.get_chat_info("node/89abcdef"))
    assert dm_info["id"] == "node/!89abcdef"
    assert dm_info["name"] == "Meshtastic node !89abcdef"
    assert dm_info["type"] == "dm"

    channel_info = asyncio.run(meshtastic.get_chat_info("channel/7"))
    assert channel_info["id"] == "channel/7"
    assert channel_info["name"] == "Meshtastic channel 7"
    assert channel_info["type"] == "channel"


def test_adapter_instantiates_against_contract_faithful_base(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway_pkg = ModuleType("gateway")
    gateway_pkg.__path__ = []  # type: ignore[attr-defined]
    gateway_config = ModuleType("gateway.config")
    gateway_base_pkg = ModuleType("gateway.platforms")
    gateway_base_pkg.__path__ = []  # type: ignore[attr-defined]
    gateway_base = ModuleType("gateway.platforms.base")

    class Platform(str):
        pass

    class SendResult(SimpleNamespace):
        def __init__(
            self,
            success: bool,
            message_id: str | None = None,
            error: str | None = None,
            raw_response: object | None = None,
            retryable: bool = False,
        ):
            super().__init__(
                success=success,
                message_id=message_id,
                error=error,
                raw_response=raw_response,
                retryable=retryable,
            )

    class BasePlatformAdapter(abc.ABC):
        def __init__(self, config: object, platform: object):
            self.config = config
            self.platform = platform
            self._running = False
            self._fatal_error_message: str | None = None

        def _set_fatal_error(self, code: str, message: str, *, retryable: bool) -> None:
            del code, retryable
            self._fatal_error_message = message

        def _mark_connected(self) -> None:
            self._running = True

        def _mark_disconnected(self) -> None:
            self._running = False

        @abc.abstractmethod
        async def get_chat_info(self, chat_id: str) -> dict[str, object]:
            raise NotImplementedError

    setattr(gateway_config, "Platform", Platform)
    setattr(gateway_base, "BasePlatformAdapter", BasePlatformAdapter)
    setattr(gateway_base, "SendResult", SendResult)

    monkeypatch.setitem(sys.modules, "gateway", gateway_pkg)
    monkeypatch.setitem(sys.modules, "gateway.config", gateway_config)
    monkeypatch.setitem(sys.modules, "gateway.platforms", gateway_base_pkg)
    monkeypatch.setitem(sys.modules, "gateway.platforms.base", gateway_base)

    adapter_path = Path(__file__).resolve().parents[1] / "plugins/platforms/meshtastic/adapter.py"
    module_name = "plugins.platforms.meshtastic.adapter_contract_probe"
    spec = importlib.util.spec_from_file_location(module_name, adapter_path)
    assert spec is not None and spec.loader is not None
    contract_adapter = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, contract_adapter)
    spec.loader.exec_module(contract_adapter)

    instance = contract_adapter.MeshtasticAdapter(cfg(valid_serial_extra()))
    info = asyncio.run(instance.get_chat_info("channel/0"))
    assert isinstance(instance, contract_adapter.BasePlatformAdapter)
    assert info["type"] == "channel"


def test_env_enablement_serial(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MESHTASTIC_TRANSPORT", "serial")
    monkeypatch.setenv("MESHTASTIC_SERIAL_PATH", "/dev/ttyUSB0")
    monkeypatch.setenv("MESHTASTIC_DM_ALLOWLIST", "89abcdef")
    seed = adapter._env_enablement()
    assert seed is not None
    assert seed["transport"] == "serial"
    assert seed["serial_path"] == "/dev/ttyUSB0"
    assert seed["dm_allowlist"] == "89abcdef"


def test_env_enablement_http_with_home_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MESHTASTIC_TRANSPORT", "http")
    monkeypatch.setenv("MESHTASTIC_HTTP_BASE_URL", "http://localhost:4403")
    monkeypatch.setenv("MESHTASTIC_HOME_CHANNEL", "channel/0")
    seed = adapter._env_enablement()
    assert seed is not None
    assert seed["transport"] == "http"
    assert seed["http_base_url"] == "http://localhost:4403"
    assert seed["home_channel"]["chat_id"] == "channel/0"
