# Meshtastic Hermes platform plugin

This directory contains the Hermes-native Meshtastic platform implementation in progress.

What is implemented now:
- Hermes plugin registration entrypoint (`register(ctx)`)
- plugin metadata (`plugin.yaml`) for setup/config surfaces
- strict configuration contract validation (`config_schema.py`)
- canonical target normalization (`node/!<8hex>` and `channel/<index>`)
- env-enablement hook to seed `PlatformConfig.extra` for env-only setups
- transport lifecycle manager for serial/http/tcp (`transport.py`)
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
        transport: serial               # serial | http | tcp
        serial_path: /dev/ttyUSB0       # required when transport=serial
        # http_base_url: http://192.168.1.25:4403  # required when transport=http
        # tcp_host: 192.168.1.25        # required when transport=tcp
        # tcp_port: 4403                # optional when transport=tcp (default 4403)

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
- `transport=tcp` requires `tcp_host`, defaults `tcp_port` to `4403`, and forbids `serial_path` / `http_base_url`
- `transport=meshtastic_tcp` is accepted as a compatibility alias and normalizes to `tcp`
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
- `connect()` uses configured serial/http/tcp transport, registers the transport-originated inbound bridge, and tracks lifecycle status
- `handle_inbound()` normalizes packets and enforces DM/group policy gates before routing
- serial and tcp transports subscribe to `meshtastic.receive` pubsub packets and forward them through the adapter bridge when the client library emits decoded payloads
- `probe()` includes transport health, transport type/path, reconnect/keepalive diagnostics, and cached last probe result/error visibility
- `send()` enforces plain-text output, chunks long payloads by UTF-8 bytes, and paces chunk sends

## Env-only enablement

The plugin can self-seed `PlatformConfig.extra` from environment variables:

- Common:
  - `MESHTASTIC_TRANSPORT=serial|http|tcp`
  - `MESHTASTIC_HOME_CHANNEL=channel/<index>` (optional)
- Serial:
  - `MESHTASTIC_SERIAL_PATH=/dev/ttyUSB0`
- HTTP:
  - `MESHTASTIC_HTTP_BASE_URL=http://192.168.1.25:4403`
- TCP:
  - `MESHTASTIC_TCP_HOST=192.168.1.25`
  - `MESHTASTIC_TCP_PORT=4403` (optional)

`MESHTASTIC_TRANSPORT=meshtastic_tcp` is accepted and normalized to `tcp` for compatibility with older config naming.

Pairing/approval workflow and cron-specific out-of-process sender plumbing are still follow-on items.

## Dev bootstrap

Use the helper script to symlink this plugin into your Hermes home plugin path:

```bash
bash scripts/dev-bootstrap.sh
uv pip install --python ~/.hermes/hermes-agent/venv/bin/python 'meshtastic>=2.7,<3'
```

It creates/updates:
- `${HERMES_HOME:-~/.hermes}/plugins/platforms/meshtastic` -> this repo's plugin dir

Safety behavior:
- refuses to delete a real non-symlink destination by default
- supports `--force` to replace a real destination explicitly
- supports `--dry-run` to preview actions

Runtime dependency note:
- serial and tcp transports import the Meshtastic Python library at runtime
- install it into the same Python environment that launches `hermes gateway run`
