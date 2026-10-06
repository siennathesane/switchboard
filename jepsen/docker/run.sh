#!/usr/bin/env bash
# Run Jepsen tests in the control container, stream the log, and copy the
# result store back to jepsen/store/ when the run finishes.
#
#   ./docker/run.sh [jepsen args...]
#   e.g. ./docker/run.sh --test fifo --time-limit 30 --nemesis parts
set -euo pipefail

JEPSEN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
NODES="${NODES:-sb1,sb2,sb3,sb4,sb5}"

docker rm -f sb-jepsen-run 2>/dev/null || true

docker run -d --name sb-jepsen-run --network jepsen \
  switchboard-jepsen-control:latest \
  lein run --nodes "$NODES" "$@"

status=0
docker logs -f sb-jepsen-run || status=$?

mkdir -p "$JEPSEN_DIR/store"
docker cp "sb-jepsen-run:/jepsen/store/." "$JEPSEN_DIR/store/" 2>/dev/null || true

code="$(docker inspect -f '{{.State.ExitCode}}' sb-jepsen-run)"
docker rm sb-jepsen-run >/dev/null
echo "control container exit code: $code (results in jepsen/store/)"
exit "$code"
