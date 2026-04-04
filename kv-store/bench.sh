#!/usr/bin/env bash
# Run the full benchmark suite against the kv-store server.
#
# Usage:
#   ./bench.sh                    # defaults: concurrency=20, requests=50000
#   CONCURRENCY=50 REQUESTS=100000 ./bench.sh
#
# The script builds release binaries, starts the server, warms it up,
# runs write / read / mixed workloads, then shuts the server down.
# Criterion HTML reports: target/criterion/

set -euo pipefail

CONCURRENCY=${CONCURRENCY:-20}
REQUESTS=${REQUESTS:-50000}
HOST="127.0.0.1:3000"

cd "$(dirname "$0")"

# ---------------------------------------------------------------------------
echo "==> Building release binaries..."
cargo build --release --bin kv-store --example load_test

# ---------------------------------------------------------------------------
echo "==> Starting server..."
./target/release/kv-store &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait until the server accepts connections (GET returns any HTTP response).
echo -n "    Waiting for server... "
for _ in $(seq 1 100); do
    if curl -s "http://${HOST}/key/_probe" -o /dev/null 2>/dev/null; then
        break
    fi
    sleep 0.05
done
echo "ready (PID $SERVER_PID)"

# ---------------------------------------------------------------------------
echo ""
echo "==> Warming up (writing 10 000 keys so reads have data)..."
./target/release/examples/load_test \
    --host "$HOST" --concurrency "$CONCURRENCY" \
    --requests 10000 --workload write

# ---------------------------------------------------------------------------
echo ""
echo "==> Benchmarks  (concurrency=$CONCURRENCY  requests=$REQUESTS)"
echo "--------------------------------------------------------------------"

for workload in write read mixed; do
    echo ""
    ./target/release/examples/load_test \
        --host "$HOST" \
        --concurrency "$CONCURRENCY" \
        --requests "$REQUESTS" \
        --workload "$workload"
done

echo ""
echo "--------------------------------------------------------------------"
echo "Done."

# To run criterion micro-benchmarks instead:
#   cargo bench
# HTML report: target/criterion/report/index.html
