#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/server_bin"

if [[ ! -x "$BIN" ]]; then
    echo "server binary missing. Run: sh compile.sh" >&2
    exit 1
fi

# Repeated local runs often leave an older listener on port 9000.
# Free the port before starting a fresh server instance.
fuser -k 9000/tcp >/dev/null 2>&1 || true
sleep 0.2

exec "$BIN" "$@"
