# Hermes Meshtastic Platform Plugin — Requirements

## Goal

Build a native Hermes platform plugin that lets Hermes send and receive Meshtastic messages as a first-class messaging surface, with behavior shaped by radio constraints rather than generic chat assumptions.

## Source of truth for deconstruction

These requirements are derived from the existing `MeshClaw` OpenClaw plugin, especially:
- `src/client.ts`
- `src/channel.ts`
- `src/inbound.ts`
- `README.md`

The transport ideas are reusable. The OpenClaw runtime coupling is not.

## Design principles

1. **Hermes-native, not a line-for-line port**
   - Use Hermes platform plugin structure (`plugin.yaml`, `adapter.py`, `register(ctx)`, `BasePlatformAdapter`-style flow).
   - Do not import OpenClaw pairing, routing, or session abstractions as architecture.

2. **Constrained-medium first**
   - Meshtastic replies must be short, plain text, and resilient to truncation.
   - The adapter should shape prompts and delivery behavior around LoRa limitations.

3. **Safe by default**
   - Prefer allowlists and explicit enablement over open surfaces.
   - Avoid hidden device mutation.

4. **Observable failure**
   - Connection state, last inbound/outbound activity, send failures, and reconnect behavior must be inspectable.

## Functional requirements

### 1) Native Hermes platform registration
- Register as a real Hermes messaging platform plugin.
- Support inbound message handling through the gateway.
- Support outbound delivery through Hermes messaging surfaces and cron-compatible sending.
- Expose platform metadata, config hints, and enablement checks.

### 2) Transport support
#### V0
- Serial transport
- HTTP transport
- TCP transport via the Meshtastic Python library

#### Later phase
- MQTT transport

Rationale:
- MeshClaw supports serial, HTTP, and MQTT.
- Serial, HTTP, and library-backed TCP are simpler to reason about for a native plugin than MQTT.
- MQTT adds broker semantics, credential exposure risk, and different delivery/debugging behavior.

### 3) Conversation types
- Direct messages
- Group/channel messages

For each inbound message, the adapter must preserve:
- sender node id
- sender display name if known
- channel name or channel index for group traffic
- whether the message was DM or group
- stable conversation identity for reply routing

### 4) Target normalization
- Normalize node IDs consistently.
- Distinguish direct targets from group/channel targets.
- Reject malformed outbound targets clearly.

### 5) LoRa-aware outbound behavior
- Plain text only
- No markdown assumptions
- No emoji or rich formatting by default
- Chunk long responses to a radio-safe limit
- Insert pacing between chunks to avoid queue overruns
- Include attachment URLs only if explicitly supported and safe; otherwise omit

MeshClaw reference behavior:
- ~200-byte chunk target
- ~1.5s delay between chunks

### 6) Inbound gating and safety policy
Need Hermes-native equivalents for the MeshClaw policy model:
- `dmPolicy`: disabled / open / allowlist (pairing can be a later extension)
- `groupPolicy`: disabled / open / allowlist
- per-group require-mention behavior
- per-group sender allowlists if needed
- control-command authorization behavior

Recommended stance:
- V0 ships with explicit allowlist and mention-gating support.
- Pairing approval flow is a separate design task, not assumed to be identical to OpenClaw.

### 7) Prompt shaping for radio constraints
The adapter should inject a Meshtastic-specific system hint for conversations on this surface:
- keep replies extremely concise
- prioritize the answer over pleasantries
- plain text only
- avoid long enumerations unless chunking is unavoidable

### 8) Routing and session mapping
The adapter must map Meshtastic conversations into Hermes session semantics without relying on OpenClaw session stores.

Needs:
- one stable conversation key per DM peer or group context
- proper reply target preservation
- correct separation of group vs direct context
- enough metadata for audit and debugging

### 9) Reliability and connection lifecycle
Derived from MeshClaw transport handling:
- connect / disconnect lifecycle
- reconnect behavior
- serial-specific heartbeat or keepalive if required
- safe cleanup on shutdown
- clear errors when device config/load times out
- guard against noisy transport exceptions crashing the process

### 10) Diagnostics and observability
Need operator-visible status for:
- running vs stopped
- transport type
- transport address/path
- last inbound activity
- last outbound activity
- last error
- last successful probe

A lightweight probe command/path should exist for each enabled transport.

### 11) Setup and configuration
Need Hermes-native config shape and docs for:
- enable/disable plugin
- transport selection
- serial path or HTTP address
- optional node name
- DM policy
- group policy
- allowed channels
- allowlists
- text chunk limit override if needed

### 12) Multi-account / multi-surface readiness
MeshClaw supports multiple accounts. Hermes support should be designed intentionally.

Recommended stance:
- V0 may support a single configured account if that reduces risk.
- The config and adapter boundaries should not prevent later multi-account support.

## Non-goals for V0
- Media upload/download
- Rich markdown rendering
- Arbitrary long-form replies optimized for cloud chat UX
- Automatic radio configuration mutation beyond narrow, explicit setup actions
- Blind port of OpenClaw pairing/session internals
- Full parity with Telegram features

## Open design questions
1. Should V0 include pairing, or start with explicit allowlists only?
2. Should MQTT be included in V0, or deferred until serial/HTTP are stable?
3. How should Hermes expose Meshtastic-specific diagnostics in the gateway/dashboard?
4. Do we want multi-account in the first cut, or a clean single-account design with expansion points?

## Recommended implementation order
1. Platform skeleton and config contract
2. Transport abstraction for serial/HTTP
3. Inbound normalization and routing
4. Outbound send path with chunking and pacing
5. Policy gates for DM/group traffic
6. Diagnostics / status / probes
7. Tests with synthetic inbound/outbound fixtures
8. Evaluate pairing and MQTT as follow-on work

## Acceptance bar for a useful V0
A useful V0 can:
- receive a DM from an allowed node
- receive a message in an allowed group channel
- require a mention when configured
- route each conversation correctly inside Hermes
- send concise plain-text replies back over the same surface
- chunk long replies safely
- survive disconnect/reconnect without silent failure
- expose enough status for an operator to debug it
