#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/benchmark_bin"
SERVER="$DIR/server"
EMBEDDED_BIN="$DIR/benchmark_embedded_bin"
BENCH_DATA_DIR="$(mktemp -d /tmp/flexql-benchmark.XXXXXX)"
STARTED_SERVER=0
SERVER_PID=""

if [[ ! -x "$BIN" ]]; then
    echo "benchmark binary missing. Run: sh compile.sh" >&2
    exit 1
fi

if [[ ! -x "$SERVER" ]]; then
    echo "server binary missing. Run: sh compile.sh" >&2
    exit 1
fi

if [[ ! -x "$EMBEDDED_BIN" ]]; then
    echo "embedded benchmark binary missing. Run: sh compile.sh" >&2
    exit 1
fi

is_server_ready() {
    (echo > /dev/tcp/127.0.0.1/9000) >/dev/null 2>&1
}

cleanup() {
    if [[ "$STARTED_SERVER" -eq 1 && -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    rm -rf "$BENCH_DATA_DIR"
}

trap cleanup EXIT

if ! is_server_ready; then
    FLEXQL_DATA_DIR="$BENCH_DATA_DIR" "$SERVER" >/tmp/flexql-benchmark-server.log 2>&1 &
    SERVER_PID=$!
    STARTED_SERVER=1

    for _ in $(seq 1 50); do
        if is_server_ready; then
            break
        fi
        sleep 0.1
    done

    if ! is_server_ready; then
        echo "FlexQL server unavailable on 127.0.0.1:9000, running embedded benchmark fallback" >&2
        exec env FLEXQL_DATA_DIR="$BENCH_DATA_DIR" "$EMBEDDED_BIN" "$@"
    fi
fi

exec env FLEXQL_DATA_DIR="$BENCH_DATA_DIR" "$BIN" "$@"
