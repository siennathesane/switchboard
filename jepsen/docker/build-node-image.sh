#!/usr/bin/env bash
# Build the Jepsen node image (switchboard + sshd + iptables) on the Docker
# daemon. Assembles a minimal build context from the repo so the daemon never
# needs the whole working tree.
set -euo pipefail

JEPSEN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$JEPSEN_DIR/.." && pwd)"
CTX="$JEPSEN_DIR/docker/broker-ctx"

mkdir -p "$CTX"
rm -rf "$CTX/crates" "$CTX/.cargo"
cp "$REPO/Cargo.toml" "$REPO/Cargo.lock" "$CTX/"
cp -R "$REPO/.cargo" "$CTX/.cargo"
cp -R "$REPO/crates" "$CTX/crates"
cp "$JEPSEN_DIR/docker/node/Dockerfile" "$CTX/Dockerfile.node"
cp "$JEPSEN_DIR/docker/node/sb-start" "$CTX/sb-start"
cp "$JEPSEN_DIR/docker/node/sb-watchdog" "$CTX/sb-watchdog"
cp "$JEPSEN_DIR/docker/keys/id_ed25519.pub" "$CTX/sshd-key.pub"

docker build -f "$CTX/Dockerfile.node" -t switchboard-jepsen-node:latest "$CTX"
echo "node image built: switchboard-jepsen-node:latest"
