#!/usr/bin/env bash
# Tear the lab down: containers, network.
set -euo pipefail

docker rm -f sb-jepsen-run 2>/dev/null || true
for c in $(docker ps -a --format '{{.Names}}' | grep -E '^sb[0-9]+$'); do
  docker rm -f "$c"
done
docker network rm jepsen 2>/dev/null || true
echo "lab down."
