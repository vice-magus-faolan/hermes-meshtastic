# hermes-meshtastic

Native Hermes platform plugin for Meshtastic.

This repo is intended to produce a *real Hermes messaging surface* for Meshtastic rather than a direct OpenClaw port. MeshClaw is the reference for functional requirements and failure modes, but Hermes needs a native adapter built around its own gateway and platform contracts.

## Current stance

**V0 should optimize for resilience, not breadth.**

Planned initial scope:
- plain-text messaging only
- direct messages and allowlisted group channels
- Hermes-native inbound/outbound routing
- 200-byte chunking with radio-safe pacing
- strong logging, probes, and reconnect behavior
- serial and HTTP transports first

Deferred until the base path is stable:
- MQTT transport
- rich pairing / approval UX
- media support
- automatic device mutation beyond clearly-scoped setup actions

## Working docs

- `docs/requirements.md` — MeshClaw-derived functional requirements and non-goals

## Why this exists

Meshtastic is not Telegram with worse bandwidth. It is a constrained radio medium with different failure modes, delivery limits, and abuse risks. The adapter should treat that as a first-class design constraint.
