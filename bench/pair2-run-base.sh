#!/usr/bin/env bash
# Baseline side of the paired re-verification: cold-redeploy the
# :baseline BROKERS only — the runner keeps the v16 image so both sides
# of the pair run under the identical bench client.
# Runs ON alpha-0.  Usage: pair2-run-base.sh <prefix>
set -euo pipefail

PREFIX="${1:?prefix}"
K="sudo kubectl -n bench"

mkdir -p ~/bench-runs

echo "===[$PREFIX] tearing down v16 cluster"
$K scale deploy/switchboard --replicas=0
$K delete pods -n bench -l app=switchboard --force --grace-period=0 2>/dev/null || true
sleep 3

echo "===[$PREFIX] setting broker image to baseline (runner stays on v16)"
$K set image deploy/switchboard "switchboard=switchboard-bench-lab:baseline"

echo "===[$PREFIX] bootstrap node (replicas=1)"
$K scale deploy/switchboard --replicas=1
$K rollout status deploy/switchboard --timeout=420s

echo "===[$PREFIX] scaling to 3"
$K scale deploy/switchboard --replicas=3
$K rollout status deploy/switchboard --timeout=420s
sleep 5

IPS=$($K get pods -l app=switchboard -o jsonpath='{range .items[*]}{.status.podIP}{"\n"}{end}' | sort | paste -sd,)
echo "===[$PREFIX] cluster ready: $IPS"

for R in A B C; do
  echo "===[$PREFIX] suite $R"
  $K exec deploy/bench-runner -- switchboard-bench \
    --hosts "$IPS" --scenario all --duration 15 \
    --publishers 2 --in-flight 64 --preload 2000 \
    | tee ~/bench-runs/$PREFIX-$R.log
  sleep 10
done

echo "===[$PREFIX] all suites done"
