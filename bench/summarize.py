#!/usr/bin/env python3
"""Print a node-count comparison table from bench/results/{1,3,5,7}.json."""

import json
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
SIZES = [1, 3, 5, 7]

SCENARIOS = [
    ("publish", "publish (no confirm, 1 queue)", "throughput_msg_s"),
    ("confirm", "confirm (1 queue, 1 raft log)", "throughput_msg_s"),
    ("sharded", "confirm (K queues = K raft logs)", "throughput_msg_s"),
    ("fanout", "fanout x3 (meta-log total order)", "throughput_msg_s"),
    ("fanout:delivered", "fanout deliveries (3 queues)", "delivered_msg_s"),
    ("drain", "drain (delivery only)", "throughput_msg_s"),
]


def load(n: int):
    p = RESULTS / f"{n}.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())


def main() -> None:
    data = {n: load(n) for n in SIZES}
    have = [n for n in SIZES if data[n]]
    if not have:
        sys.exit("no results found (run bench/sweep.sh first)")

    def cell(n: int, scen: str, key: str) -> str:
        d = data[n]
        if not d:
            return "-"
        base = scen.split(":")[0]
        r = d.get("results", {}).get(base)
        if r is None:
            return "-"
        if scen == "fanout:delivered":
            r = d.get("results", {}).get("fanout")
            key = "delivered_msg_s"
        v = r.get(key)
        if v is None:
            return "-"
        return f"{v:,}"

    header = f"{'scenario':<38}" + "".join(f"{f'{n} node':>12}" for n in have)
    print(header)
    print("-" * len(header))
    for scen, label, key in SCENARIOS:
        row = f"{label:<38}" + "".join(f"{cell(n, scen, key):>12}" for n in have)
        print(row)

    print()
    lat_header = f"{'latency (req->echo->reply)':<38}" + "".join(f"{f'{n} node':>12}" for n in have)
    print(lat_header)
    print("-" * len(lat_header))
    for key, label in [("p50_us", "p50"), ("p99_us", "p99"), ("p999_us", "p99.9")]:
        row = f"{label:<38}" + "".join(
            f"{(data[n]['results']['latency'][key] if data[n] and 'latency' in data[n].get('results', {}) else '-'):>12,}"
            for n in have
        )
        print(row)
    print("\n(latency in microseconds; message size and duration in each results/N.json)")


if __name__ == "__main__":
    main()
