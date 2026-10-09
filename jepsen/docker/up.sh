#!/usr/bin/env bash
# Boot the Jepsen lab: a bridge network, N switchboard node containers
# (sshd only; Jepsen starts the brokers), and the control image.
#
#   ./up.sh [N]      N = node count (default 5)
set -euo pipefail

JEPSEN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
N="${1:-5}"
SUBNET="${SUBNET:-172.30.0.0/16}"

docker network create jepsen --subnet "$SUBNET" 2>/dev/null || true

for i in $(seq 1 "$N"); do
  docker rm -f "sb$i" 2>/dev/null || true
  args=(
    -d --name "sb$i" --hostname "sb$i" --network jepsen
    --cap-add NET_ADMIN
    --stop-signal SIGKILL
    -e "SB_NODE_ID=$i"
    -e "SB_ADVERTISE=sb$i"
    -e "SB_EXPECTED=$N"
  )
  if [ "$i" = 1 ]; then args+=(-e SB_BOOTSTRAP=1);
  else args+=(-e "SB_SEEDS=sb1:5673"); fi
  docker run "${args[@]}" switchboard-jepsen-node:latest
done

# Broker environment for sshd-spawned starts (sshd does not inherit
# container env; sb-start sources /var/run/sb-env). CI sets
# SB_TIME_SCALE=0.25 to stretch the raft timers 4x: a contended 4-core
# runner cannot hold 100ms heartbeats across five brokers, and the meta
# group churns elections for the whole run.
if [ -n "${SB_TIME_SCALE:-}" ]; then
  for i in $(seq 1 "$N"); do
    docker exec "sb$i" sh -c "printf 'SB_TIME_SCALE=%s\n' '$SB_TIME_SCALE' > /var/run/sb-env"
  done
fi

docker build -f "$JEPSEN_DIR/docker/Dockerfile.control" \
  -t switchboard-jepsen-control:latest "$JEPSEN_DIR"

nodes="$(seq 1 "$N" | sed 's/^/sb/' | paste -sd, -)"
echo
echo "lab up: $N node containers (sb1..sb$N), control image built."
echo "run tests:  ./docker/run.sh --test all --nodes \"$nodes\""
