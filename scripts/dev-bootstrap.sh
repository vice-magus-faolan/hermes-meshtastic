#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/dev-bootstrap.sh [--force] [--dry-run]

Symlink this repo's Meshtastic platform plugin into Hermes user plugins:
  ${HERMES_HOME:-$HOME/.hermes}/plugins/platforms/meshtastic

Options:
  --force    Replace an existing non-symlink destination
  --dry-run  Print actions without changing filesystem
  -h, --help Show this help text
EOF
}

FORCE=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PLUGIN_SRC="$REPO_ROOT/plugins/platforms/meshtastic"
HERMES_HOME_DIR="${HERMES_HOME:-$HOME/.hermes}"
PLUGIN_DST="$HERMES_HOME_DIR/plugins/platforms/meshtastic"

if [[ ! -d "$PLUGIN_SRC" ]]; then
  echo "ERROR: plugin source directory not found: $PLUGIN_SRC" >&2
  exit 1
fi

run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $*"
    return 0
  fi
  "$@"
}

echo "Source:      $PLUGIN_SRC"
echo "Destination: $PLUGIN_DST"

run mkdir -p "$(dirname "$PLUGIN_DST")"

if [[ -L "$PLUGIN_DST" ]]; then
  CURRENT_TARGET="$(readlink "$PLUGIN_DST")"
  if [[ "$CURRENT_TARGET" == "$PLUGIN_SRC" ]]; then
    echo "Already linked; nothing to do."
    exit 0
  fi
  echo "Replacing existing symlink: $PLUGIN_DST -> $CURRENT_TARGET"
  run rm "$PLUGIN_DST"
elif [[ -e "$PLUGIN_DST" ]]; then
  if [[ "$FORCE" -ne 1 ]]; then
    echo "ERROR: destination exists and is not a symlink: $PLUGIN_DST" >&2
    echo "Refusing to delete it automatically. Re-run with --force if replacement is intentional." >&2
    exit 1
  fi
  echo "Force-removing existing destination: $PLUGIN_DST"
  run rm -rf "$PLUGIN_DST"
fi

run ln -s "$PLUGIN_SRC" "$PLUGIN_DST"
echo "Linked: $PLUGIN_DST -> $PLUGIN_SRC"
