# Hermes Meshtastic Platform Plugin — V0 Specification

## Purpose

Define an implementation-ready V0 contract for a Hermes-native Meshtastic platform plugin. This spec resolves open decisions in `docs/requirements.md` so implementation and review can anchor to one baseline.

## Scope and non-scope

### In scope (V0)
- Native Hermes platform registration (`plugin.yaml`, `register(ctx)`, adapter hooks)
- Serial transport
- HTTP transport
- Inbound DM and group/channel message handling
- Outbound plain-text-only delivery
- UTF-8 byte-aware chunking and pacing for radio-safe sends
- Deny-by-default DM/group authorization with allowlist support
- Mention gating for group traffic
- Session mapping that preserves Meshtastic identity and reply targets
- Operator-visible transport and activity diagnostics

### Deferred (not V0)
- MQTT transport
- Pairing approval workflow
- Media upload/download
- Rich markdown rendering
- Multi-account runtime support (single account is acceptable in V0)
- Automatic radio/device mutation beyond explicit setup operations

## Architecture decisions

1) Hermes-native plugin boundary
- Reuse MeshClaw behavior as reference only.
- Do not import OpenClaw runtime coupling, pairing store semantics, or session internals.

2) Transport policy
- `transport=serial` and `transport=http` are the only valid transport values in V0.
- MQTT is explicitly deferred.

3) Authorization model
- DM and group paths are deny-by-default unless explicitly opened.
- Supported policy values:
  - `disabled`
  - `open`
  - `allowlist`
- When policy is `allowlist`, allowlist entries are required and strictly validated.
- Group flows support require-mention gating and optional sender allowlisting.
- Admin/control commands are DM-only and require authorization.

4) Session mapping model
- DM sessions key by peer node identity.
- Group sessions key by `(channel, sender)` identity, not shared channel-wide context.
- Normalized inbound envelope must preserve sender ID, sender name, channel index/name, DM-vs-group, reply target, and raw payload context.

5) Outbound target contract
- Canonical target forms:
  - `node/!<8hex>`
  - `channel/<index>`
- Optional platform prefix accepted and normalized:
  - `meshtastic:node/!<8hex>`
  - `meshtastic:channel/<index>`
- Malformed targets fail with explicit validation errors.

6) Outbound text behavior
- Plain text only.
- Reject attachments/media/rich payload variants.
- Normalize text for safe send (newline normalization, control-character stripping, empty-content rejection).
- Chunking is UTF-8 byte-based with a hard maximum of 200 bytes per chunk.
- Default chunk sizing and pacing:
  - `text_chunk_bytes`: 180
  - hard ceiling: 200
  - `chunk_delay_seconds`: 1.5

7) Failure and retry semantics
- Delivery errors include chunk index and chunk count context.
- Retryable behavior:
  - `retryable=true` only when zero chunks were sent
  - partial delivery failures are non-retryable to avoid blind duplicate retransmits

8) Observability
- Probe/status output must include at minimum:
  - running/stopped state
  - transport type and address/path
  - transport connected health
  - reconnect attempts / keepalive failure counters
  - last inbound activity
  - last outbound activity
  - last successful probe
  - last probe failure
  - last error

## Configuration contract (V0)

`PlatformConfig.extra` fields:
- Required/common:
  - `transport`: `serial | http`
- Serial path:
  - `serial_path` required when `transport=serial`
  - `http_base_url` forbidden when `transport=serial`
- HTTP path:
  - `http_base_url` required when `transport=http`
  - `serial_path` forbidden when `transport=http`
- Optional behavior controls:
  - `node_name`
  - `dm_policy`
  - `group_policy`
  - `dm_allowlist`
  - `allowed_channels`
  - `group_sender_allowlist`
  - `require_mention`
  - `text_chunk_bytes` (1..200)
  - `chunk_delay_seconds` (>0)

Validation must be strict and fail closed on invalid config.

## Acceptance criteria

A V0 implementation is acceptable when all conditions below are met:

1) Inbound routing
- Receives DM from authorized sender and routes to correct Hermes session.
- Receives group message from authorized channel/sender and routes to correct `(channel, sender)` session.
- Enforces mention gating when enabled.

2) Outbound behavior
- Sends plain-text replies to both DM and channel targets.
- Rejects unsupported non-text payloads with explicit non-retryable errors.
- Chunks long text by UTF-8 byte limits and applies pacing between chunks.

3) Reliability
- Connect/disconnect/probe lifecycle works for serial and HTTP transports.
- Reconnect behavior is bounded and observable.
- Transport failures surface explicit diagnostics instead of silent drop.

4) Operator visibility
- Probe/status exposes required health/activity fields.
- Last error and last probe failure are inspectable for debugging.

5) Validation
- Config validation enforces transport-specific required/forbidden keys.
- Target normalization and malformed-target rejection are covered by tests.

## Review anchor

Canonical V0 spec path for implementation and review: `docs/v0-spec.md`.
