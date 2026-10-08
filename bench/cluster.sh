#!/usr/bin/env bash
# Boot/tear down a Switchboard benchmark cluster on the jepsen docker
# network. Reuses the jepsen node image (broker + sshd + watchdog).
#
#   ./bench/cluster.sh up N
#   ./bench/cluster.sh down N
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CMD="${1:-up}"
N="${2:-3}"

ensure_image() {
  if ! docker image inspect switchboard-jepsen-node:latest >/dev/null 2>&1; then
    echo "node image missing; building via jepsen pipeline (this compiles the broker)..."
    "$BENCH_DIR/../jepsen/docker/build-node-image.sh"
  fi
}

case "$CMD" in
  up)
    ensure_image
    docker network create jepsen --subnet 172.30.0.0/16 2>/dev/null || true
    # Wipe EVERY sb* container: leftovers from earlier (larger) clusters
    # keep running their watchdogs and re-join through background seed
    # retries, corrupting the new cluster's membership.
    for c in $(docker ps -aq --filter "name=^sb[0-9]+$"); do
      docker rm -f "$c" >/dev/null 2>&1 || true
    done
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
      docker run "${args[@]}" switchboard-jepsen-node:latest >/dev/null
    done
    # Start brokers directly (there is no jepsen control node here). The
    # container watchdog re-arms from the persisted args if a broker dies.
    for i in $(seq 1 "$N"); do
      extra="--seeds sb1:5673"
      if [ "$i" = 1 ]; then extra="--bootstrap"; fi
      docker exec "sb$i" sh -c "
        printf '%s\n' $i sb$i $N $extra > /var/run/sb-args
        touch /var/run/sb-enabled
        sb-start $i sb$i $N $extra" >/dev/null
    done
    echo "bench cluster up: sb1..sb$N"
    ;;
  down)
    for i in $(seq 1 "$N"); do docker rm -f "sb$i" >/dev/null 2>&1 || true; done
    echo "bench cluster down: sb1..sb$N"
    ;;
  *)
    echo "usage: cluster.sh up N | down N" >&2
    exit 1
    ;;
esac
