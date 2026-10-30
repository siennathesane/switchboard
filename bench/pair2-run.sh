#!/usr/bin/env bash
# Cold-redeploy the bench cluster on a given image tag, then run three
# benchmark suites with the paired-run protocol from bench/RESULTS.md.
# Runs ON alpha-0.  Usage: pair2-run.sh <tag> <prefix>
set -euo pipefail

TAG="${1:?tag}"
PREFIX="${2:?prefix}"
K="sudo kubectl -n bench"

mkdir -p ~/bench-runs

echo "===[$TAG] tearing down old cluster"
$K scale deploy/switchboard --replicas=0
$K delete pods -n bench -l app=switchboard --force --grace-period=0 2>/dev/null || true
sleep 3

echo "===[$TAG] setting image"
$K set image deploy/switchboard "switchboard=switchboard-bench-lab:$TAG"
$K set image deploy/bench-runner "runner=switchboard-bench-lab:$TAG"

echo "===[$TAG] bootstrap node (replicas=1)"
$K scale deploy/switchboard --replicas=1
$K rollout status deploy/switchboard --timeout=420s

echo "===[$TAG] scaling to 3"
$K scale deploy/switchboard --replicas=3
$K rollout status deploy/switchboard --timeout=420s
sleep 5

IPS=$($K get pods -l app=switchboard -o jsonpath='{range .items[*]}{.status.podIP}{"\n"}{end}' | sort | paste -sd,)
echo "===[$TAG] cluster ready: $IPS"

for R in A B C; do
  echo "===[$TAG] suite $R"
  $K exec deploy/bench-runner -- switchboard-bench \
    --hosts "$IPS" --scenario all --duration 15 \
    --publishers 2 --in-flight 64 --preload 2000 \
    | tee ~/bench-runs/$PREFIX-$R.log
  sleep 10
done

echo "===[$TAG] all suites done"
