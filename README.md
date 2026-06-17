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
- serial, HTTP, and TCP transports in V0

Deferred until the base path is stable:
- MQTT transport
- rich pairing / approval UX
- media support
- automatic device mutation beyond clearly-scoped setup actions

## Working docs

- `docs/requirements.md` — MeshClaw-derived functional requirements and non-goals
- `docs/v0-spec.md` — implementation-ready V0 contract and acceptance criteria
- `plugins/platforms/meshtastic/README.md` — plugin scaffold contract and current limits

## Local dev bootstrap

To load this plugin into a local Hermes profile during development:

- `bash scripts/dev-bootstrap.sh`
- `uv pip install --python ~/.hermes/hermes-agent/venv/bin/python 'meshtastic>=2.7,<3'`

The bootstrap script creates a symlink under `${HERMES_HOME:-~/.hermes}/plugins/platforms/meshtastic` and refuses to delete an existing non-symlink destination unless `--force` is explicit.

The Meshtastic Python library is a runtime dependency for serial and TCP transports. If Hermes is running from a different virtualenv, point `uv pip install --python ...` at that interpreter instead.

## Why this exists

Meshtastic is not Telegram with worse bandwidth. It is a constrained radio medium with different failure modes, delivery limits, and abuse risks. The adapter should treat that as a first-class design constraint.
