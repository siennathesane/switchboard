#!/usr/bin/env bash
# Run the benchmark suite against a fresh N-node cluster and store the
# machine results in bench/results/N.json.
#
#   ./bench/run.sh N [scenario] [duration]
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
N="${1:-5}"
SCENARIO="${2:-all}"
DURATION="${3:-20}"

HOSTS="$(seq 1 "$N" | sed 's/^/sb/' | paste -sd, -)"

"$BENCH_DIR/cluster.sh" up "$N" >/dev/null

# Readiness: wait for every node's AMQP port inside the docker network.
sleep 3
for i in $(seq 1 "$N"); do
  for _ in $(seq 1 120); do
    if docker exec "sb$i" sh -c 'nc -z 127.0.0.1 5672 >/dev/null 2>&1'; then break; fi
    sleep 0.5
  done
done
echo "cluster sb1..sb$N ready"

mkdir -p "$BENCH_DIR/results"
docker rm -f sb-bench-run >/dev/null 2>&1 || true

# The runner prints the human table to stdout and the JSON block after the
# "=== RESULTS ===" marker; capture everything and extract.
set -o pipefail
docker run --rm --name sb-bench-run --network jepsen \
  switchboard-bench:latest \
  --hosts "$HOSTS" --scenario "$SCENARIO" --duration "$DURATION" \
  | tee "$BENCH_DIR/results/$N.log"

python3 - "$BENCH_DIR/results/$N.log" "$BENCH_DIR/results/$N.json" <<'EOF'
import sys
log = open(sys.argv[1]).read()
marker = "=== RESULTS ==="
if marker not in log:
    sys.exit("no results block found in runner output")
import json
open(sys.argv[2], "w").write(log.split(marker, 1)[1].strip())
print(f"results written: {sys.argv[2]}")
EOF

"$BENCH_DIR/cluster.sh" down "$N" >/dev/null
