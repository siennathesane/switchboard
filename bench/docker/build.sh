#!/usr/bin/env bash
# Build the benchmark runner image on the daemon. Assembles a minimal
# context (workspace manifests + crates) so the whole repo isn't shipped.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$BENCH_DIR/.." && pwd)"
CTX="$BENCH_DIR/docker/runner-ctx"

mkdir -p "$CTX"
rm -rf "$CTX/crates" "$CTX/.cargo"
cp "$REPO/Cargo.toml" "$REPO/Cargo.lock" "$CTX/"
cp -R "$REPO/.cargo" "$CTX/.cargo"
cp -R "$REPO/crates" "$CTX/crates"
cp "$BENCH_DIR/docker/Dockerfile.runner" "$CTX/Dockerfile.runner"

docker build -f "$CTX/Dockerfile.runner" -t switchboard-bench:latest "$CTX"
echo "runner image built: switchboard-bench:latest"
