#!/usr/bin/env bash
# Full benchmark sweep: 1/3/5/7-node clusters, all scenarios, then a
# comparison table.
#
#   ./bench/sweep.sh [scenario] [duration]
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
SCENARIO="${1:-all}"
DURATION="${2:-20}"

# The runner image must exist; cluster node images come from the jepsen
# pipeline.
"$BENCH_DIR/docker/build.sh"

for n in 1 3 5 7; do
  echo ""
  echo "================= $n-node cluster ================="
  "$BENCH_DIR/run.sh" "$n" "$SCENARIO" "$DURATION"
done

python3 "$BENCH_DIR/summarize.py"
