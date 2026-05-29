# Meshtastic Hermes platform plugin

This directory contains the Hermes-native Meshtastic platform implementation in progress.

What is implemented now:
- Hermes plugin registration entrypoint (`register(ctx)`)
- plugin metadata (`plugin.yaml`) for setup/config surfaces
- strict configuration contract validation (`config_schema.py`)
- canonical target normalization (`node/!<8hex>` and `channel/<index>`)
- env-enablement hook to seed `PlatformConfig.extra` for env-only setups
- transport lifecycle manager for serial/http (`transport.py`)
  - timeout-bounded connect/disconnect
  - bounded reconnect retries with backoff
  - keepalive probes with stale-session reconnect
  - transport-specific status diagnostics
  - adapter bridge hook for transport-originated inbound packets

What is intentionally not implemented yet:
- standalone out-of-process sender for cron delivery
- explicit Hermes-native pairing/approval workflow design (separate follow-on task)

## Config contract (PlatformConfig.extra)

```yaml
gateway:
  platforms:
    meshtastic:
      enabled: true
      extra:
        transport: serial               # serial | http
        serial_path: /dev/ttyUSB0       # required when transport=serial
        # http_base_url: http://192.168.1.25:4403  # required when transport=http

        node_name: hermes-radio

        dm_policy: allowlist            # disabled | open | allowlist
        group_policy: allowlist         # disabled | open | allowlist
        dm_allowlist: ["!89abcdef"]
        allowed_channels: [0]
        group_sender_allowlist: []
        require_mention: true

        text_chunk_bytes: 200           # 1..200 (V0 cap)
        chunk_delay_seconds: 1.5        # > 0
```

Validation rules are strict:
- `transport=serial` requires `serial_path` and forbids `http_base_url`
- `transport=http` requires `http_base_url` and forbids `serial_path`
- node IDs are canonicalized to `!<8hex>`
- `allowlist` policies require non-empty allowlist entries
- `require_mention` uses explicit mention metadata when available, otherwise falls back to configured `node_name`
- if `require_mention=true` and neither mention metadata nor `node_name` is available, group traffic fails closed; operators must set `node_name` (or disable `require_mention`) on transports that do not expose mention metadata
- `text_chunk_bytes` must be <= 200 for LoRa-safe behavior

## Outbound target forms

Only these canonical outbound target forms are accepted:
- `node/!89abcdef`
- `channel/0`
- optional prefix: `meshtastic:node/!89abcdef` or `meshtastic:channel/0`

Malformed targets are rejected with explicit validation errors.

## Runtime semantics at this stage

The adapter now implements transport lifecycle state, inbound policy gates, and outbound text delivery:
- `connect()` uses configured serial/http transport, registers the transport-originated inbound bridge, and tracks lifecycle status
- `handle_inbound()` normalizes packets and enforces DM/group policy gates before routing
- serial transport subscribes to `meshtastic.receive` pubsub packets and forwards them through the adapter bridge when the client library emits decoded payloads
- `probe()` includes transport health, transport type/path, reconnect/keepalive diagnostics, and cached last probe result/error visibility
- `send()` enforces plain-text output, chunks long payloads by UTF-8 bytes, and paces chunk sends

Pairing/approval workflow and cron-specific out-of-process sender plumbing are still follow-on items.

## Dev bootstrap

Use the helper script to symlink this plugin into your Hermes home plugin path:

```bash
bash scripts/dev-bootstrap.sh
```

It creates/updates:
- `${HERMES_HOME:-~/.hermes}/plugins/platforms/meshtastic` -> this repo's plugin dir

Safety behavior:
- refuses to delete a real non-symlink destination by default
- supports `--force` to replace a real destination explicitly
- supports `--dry-run` to preview actions
