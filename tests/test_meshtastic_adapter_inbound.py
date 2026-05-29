from __future__ import annotations

import asyncio

from plugins.platforms.meshtastic import adapter
from tests.meshtastic_harness import (
    StubInboundTransport,
    cfg,
    dm_payload,
    group_payload,
    valid_serial_extra,
)


def test_normalize_inbound_dm_route() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    route = meshtastic.normalize_inbound(
        {
            "text": "hello from mesh",
            "sender_node_id": "89ABCDEF",
            "sender_name": "Alice",
        }
    )

    assert route.text == "hello from mesh"
    assert route.sender_node_id == "!89abcdef"
    assert route.sender_name == "Alice"
    assert route.is_group is False
    assert route.channel_index is None
    assert route.reply_target == "node/!89abcdef"


def test_build_inbound_event_group_route() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    event = meshtastic.build_inbound_event(
        {
            "text": "group ping",
            "sender": "0x89ABCDEF",
            "senderName": "Relay-1",
            "isGroup": "true",
            "channelIndex": "2",
            "channelName": "ops",
            "messageId": "m-123",
            "timestamp": "2026-05-27T12:34:56Z",
        }
    )

    assert event.text == "group ping"
    assert event.channel_prompt == adapter._MESHTASTIC_PLATFORM_HINT
    assert event.message_id == "m-123"
    assert event.source.chat_id == "channel/2"
    assert event.source.chat_type == "group"
    assert event.source.thread_id == "sender/!89abcdef"
    assert event.source.user_id == "!89abcdef"
    assert event.source.user_name == "Relay-1"

    assert event.raw_message["meshtastic"]["is_group"] is True
    assert event.raw_message["meshtastic"]["channel_index"] == 2
    assert event.raw_message["meshtastic"]["reply_target"] == "channel/2"


def test_build_inbound_event_group_route_isolated_per_sender() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    sender_a = meshtastic.build_inbound_event(
        {
            "text": "from alpha",
            "sender": "89ABCDEF",
            "isGroup": True,
            "channelIndex": 2,
        }
    )
    sender_b = meshtastic.build_inbound_event(
        {
            "text": "from bravo",
            "sender": "12345678",
            "isGroup": True,
            "channelIndex": 2,
        }
    )

    assert sender_a.source.chat_id == "channel/2"
    assert sender_b.source.chat_id == "channel/2"
    assert sender_a.source.chat_type == "group"
    assert sender_b.source.chat_type == "group"
    assert sender_a.source.thread_id == "sender/!89abcdef"
    assert sender_b.source.thread_id == "sender/!12345678"
    assert sender_a.source.thread_id != sender_b.source.thread_id


def test_handle_inbound_routes_message_event() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    routed: list[adapter.MessageEvent] = []

    async def fake_handle_message(event: adapter.MessageEvent) -> None:
        routed.append(event)

    meshtastic.handle_message = fake_handle_message  # type: ignore[method-assign]

    ok = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "dm hello",
                "from": "89ABCDEF",
                "sender_name": "Node A",
            }
        )
    )

    assert ok is True
    assert len(routed) == 1
    assert routed[0].text == "dm hello"
    assert routed[0].source.chat_id == "node/!89abcdef"
    assert meshtastic._last_inbound_activity is not None


def test_handle_inbound_rejects_invalid_payload() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    ok = asyncio.run(meshtastic.handle_inbound({"text": "missing sender"}))

    assert ok is False


def test_handle_inbound_dm_policy_disabled() -> None:
    extra = valid_serial_extra()
    extra["dm_policy"] = "disabled"
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    routed: list[adapter.MessageEvent] = []

    async def fake_handle_message(event: adapter.MessageEvent) -> None:
        routed.append(event)

    meshtastic.handle_message = fake_handle_message  # type: ignore[method-assign]

    ok = asyncio.run(meshtastic.handle_inbound({"text": "hi", "sender": "89ABCDEF"}))

    assert ok is False
    assert routed == []


def test_handle_inbound_dm_policy_open_allows_unknown_sender() -> None:
    extra = valid_serial_extra()
    extra["dm_policy"] = "open"
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    routed: list[adapter.MessageEvent] = []

    async def fake_handle_message(event: adapter.MessageEvent) -> None:
        routed.append(event)

    meshtastic.handle_message = fake_handle_message  # type: ignore[method-assign]

    ok = asyncio.run(meshtastic.handle_inbound({"text": "hi", "sender": "12345678"}))

    assert ok is True
    assert len(routed) == 1


def test_handle_inbound_dm_allowlist_denies_unknown_sender() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    routed: list[adapter.MessageEvent] = []

    async def fake_handle_message(event: adapter.MessageEvent) -> None:
        routed.append(event)

    meshtastic.handle_message = fake_handle_message  # type: ignore[method-assign]

    ok = asyncio.run(meshtastic.handle_inbound({"text": "hi", "sender": "12345678"}))

    assert ok is False
    assert routed == []


def test_handle_inbound_group_policy_disabled() -> None:
    extra = valid_serial_extra()
    extra["group_policy"] = "disabled"
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    ok = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
                "mentioned": True,
            }
        )
    )

    assert ok is False


def test_handle_inbound_group_allowlist_denies_disallowed_channel() -> None:
    extra = valid_serial_extra()
    extra["allowed_channels"] = [0]
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    ok = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 2,
                "mentioned": True,
            }
        )
    )

    assert ok is False


def test_handle_inbound_group_sender_allowlist_denies_unknown_sender() -> None:
    extra = valid_serial_extra()
    extra["group_sender_allowlist"] = ["!12345678"]
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    ok = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
                "mentioned": True,
            }
        )
    )

    assert ok is False


def test_handle_inbound_group_require_mention_from_payload_flag() -> None:
    extra = valid_serial_extra()
    extra["group_policy"] = "open"
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    denied = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello everyone",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
                "mentioned": False,
            }
        )
    )
    allowed = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello everyone",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
                "mentioned": True,
            }
        )
    )

    assert denied is False
    assert allowed is True


def test_handle_inbound_group_require_mention_from_text_with_node_name() -> None:
    extra = valid_serial_extra()
    extra["group_policy"] = "open"
    extra["node_name"] = "RadioBot"
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    denied = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello everyone",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
            }
        )
    )
    allowed = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello @radiobot",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
            }
        )
    )

    assert denied is False
    assert allowed is True


def test_handle_inbound_group_require_mention_fails_closed_without_node_name_or_metadata() -> None:
    extra = valid_serial_extra()
    extra["group_policy"] = "open"
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    ok = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello everyone",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
            }
        )
    )

    assert ok is False


def test_handle_inbound_group_does_not_treat_generic_hermes_alias_as_configured_mention() -> None:
    extra = valid_serial_extra()
    extra["group_policy"] = "open"
    extra["node_name"] = "RadioBot"
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    ok = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "hello @hermes",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
            }
        )
    )

    assert ok is False


def test_handle_inbound_group_blocks_control_commands() -> None:
    extra = valid_serial_extra()
    extra["group_policy"] = "open"
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    ok = asyncio.run(
        meshtastic.handle_inbound(
            {
                "text": "/sethome",
                "sender": "89ABCDEF",
                "isGroup": True,
                "channelIndex": 0,
                "mentioned": True,
            }
        )
    )

    assert ok is False


def test_normalize_inbound_infers_group_from_channel_without_isgroup_flag() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    route = meshtastic.normalize_inbound(
        {
            "text": "inferred group",
            "sender": "89ABCDEF",
            "channelIndex": 2,
        }
    )

    assert route.is_group is True
    assert route.channel_index == 2
    assert route.reply_target == "channel/2"


def test_handle_inbound_group_require_mention_uses_structured_mentions() -> None:
    extra = valid_serial_extra(group_policy="open", node_name="RadioBot")
    meshtastic = adapter.MeshtasticAdapter(cfg(extra))

    denied = asyncio.run(
        meshtastic.handle_inbound(
            group_payload(
                text="hello team",
                sender="89ABCDEF",
                channel_index=0,
                mentioned=None,
                mentionedNodes=["OtherNode"],
            )
        )
    )
    allowed = asyncio.run(
        meshtastic.handle_inbound(
            group_payload(
                text="hello team",
                sender="89ABCDEF",
                channel_index=0,
                mentioned=None,
                mentionedNodes=["@RadioBot"],
            )
        )
    )

    assert denied is False
    assert allowed is True


def test_handle_inbound_dm_payload_builder_round_trip() -> None:
    meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra()))

    routed: list[adapter.MessageEvent] = []

    async def fake_handle_message(event: adapter.MessageEvent) -> None:
        routed.append(event)

    meshtastic.handle_message = fake_handle_message  # type: ignore[method-assign]

    ok = asyncio.run(meshtastic.handle_inbound(dm_payload(text="fixture hello", sender="89ABCDEF")))

    assert ok is True
    assert len(routed) == 1
    assert routed[0].text == "fixture hello"


def test_connect_registers_transport_inbound_bridge() -> None:
    async def _scenario() -> None:
        transport = StubInboundTransport()
        meshtastic = adapter.MeshtasticAdapter(cfg(valid_serial_extra(group_policy="open")), transport=transport)

        routed: list[adapter.MessageEvent] = []

        async def fake_handle_message(event: adapter.MessageEvent) -> None:
            routed.append(event)

        meshtastic.handle_message = fake_handle_message  # type: ignore[method-assign]

        assert await meshtastic.connect() is True

        ok = await transport.emit_inbound(
            group_payload(
                text="hello mesh",
                sender="89ABCDEF",
                channel_index=0,
                mentioned=True,
            )
        )

        assert ok is True
        assert len(routed) == 1
        assert routed[0].text == "hello mesh"
        assert routed[0].source.chat_id == "channel/0"
        assert routed[0].source.thread_id == "sender/!89abcdef"
        assert meshtastic._last_inbound_activity is not None

        await meshtastic.disconnect()

    asyncio.run(_scenario())
