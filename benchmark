#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/benchmark_bin"

if [[ ! -x "$BIN" ]]; then
    echo "benchmark binary missing. Run: sh compile.sh" >&2
    exit 1
fi

exec "$BIN" "$@"
