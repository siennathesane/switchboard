#!/usr/bin/env python3
"""Switchboard multi-language conformance harness.

Boots a real multi-node Switchboard cluster and drives it with genuine
ecosystem clients in several languages. Every suite prints one line per
check (`PASS <name>` / `FAIL <name>: detail`); the harness prints a
summary matrix and exits non-zero on any failure.

Usage:
    harness-venv/bin/python harness/run.py [--nodes 3] [--only py,go]
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

HARNESS = Path(__file__).resolve().parent
REPO = HARNESS.parent
VENV_PY = HARNESS.parent / "harness-venv" / (
    "Scripts/python.exe" if os.name == "nt" else "bin/python")
sys.path.insert(0, str(HARNESS))
import support  # noqa: E402


def ensure_python_client() -> None:
    if importlib.util.find_spec("pika") is None:
        if VENV_PY.exists():
            os.execv(str(VENV_PY), [str(VENV_PY), __file__, *sys.argv[1:]])
        support.die("pika missing: create the venv — "
                    "python3 -m venv harness-venv && "
                    "harness-venv/bin/pip install pika paho-mqtt stomp.py")


@dataclass
class Suite:
    name: str
    language: str
    features: str
    cmd: list[str]


def build_suites() -> list[Suite]:
    py = str(VENV_PY if VENV_PY.exists() else sys.executable)
    node = HARNESS / "clients" / "node"
    go = HARNESS / "clients" / "go"
    ruby = HARNESS / "clients" / "ruby"
    suites = [
        Suite("python/amqp091", "python", "AMQP 0-9-1: full matrix",
              [py, str(HARNESS / "clients" / "python" / "amqp091_test.py")]),
        Suite("python/mqtt", "python", "MQTT 3.1.1: qos0-2, retained, wildcards, session",
              [py, str(HARNESS / "clients" / "python" / "mqtt_test.py")]),
        Suite("python/stomp", "python", "STOMP 1.2: sub/send/ack, tx, errors",
              [py, str(HARNESS / "clients" / "python" / "stomp_test.py")]),
        Suite("go/amqp091", "go", "AMQP 0-9-1: full matrix",
              ["go", "run", "amqp091_suite.go"],),
        Suite("node/amqp091", "node", "AMQP 0-9-1: full matrix",
              ["node", str(node / "amqp091_test.mjs")]),
        Suite("ruby/amqp091", "ruby", "AMQP 0-9-1: core matrix",
              ["ruby", str(ruby / "amqp091_test.rb")]),
    ]
    return suites


def run_suite(suite: Suite, env: dict[str, str], cwd_hint: Path | None,
              log_path: Path) -> tuple[bool, int, int, str]:
    """Run one suite; return (ok, passed, total, tail)."""
    proc = subprocess.run(
        suite.cmd, env={**os.environ, **env},
        capture_output=True, text=True, timeout=900,
        cwd=str(cwd_hint) if cwd_hint else None,
    )
    log_path.write_text(proc.stdout + ("\n--- stderr ---\n" + proc.stderr
                                       if proc.stderr.strip() else ""))
    passed = sum(1 for line in proc.stdout.splitlines() if line.startswith("PASS "))
    failed = sum(1 for line in proc.stdout.splitlines() if line.startswith("FAIL "))
    tail = "\n".join(line for line in proc.stdout.splitlines()
                     if line.startswith("FAIL ") or line.startswith("harness:"))[:1500]
    return proc.returncode == 0, passed, passed + failed, tail


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--only", type=str, default="",
                    help="comma list of suite names to run (default all)")
    ap.add_argument("--keep-going", action="store_true",
                    help="do not stop the cluster on a suite failure")
    args = ap.parse_args()

    ensure_python_client()

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    suites = [s for s in build_suites() if not only or s.name in only]

    print(f"harness: building broker…")
    subprocess.run(["cargo", "build", "-p", "switchboard"], cwd=str(REPO),
                   check=True, capture_output=True)
    print("harness: booting cluster…")
    cluster = support.start_cluster(args.nodes)
    print(f"harness: cluster up: {cluster.client_addrs}")

    out_dir = HARNESS / "out"
    out_dir.mkdir(exist_ok=True)

    env = cluster.env()
    cwd_by_lang = {
        "go": HARNESS / "clients" / "go",
        "node": HARNESS / "clients" / "node",
    }

    results: list[tuple[Suite, bool, int, int, str]] = []
    try:
        for suite in suites:
            print(f"\n=== {suite.name} — {suite.features} ===")
            log = out_dir / (suite.name.replace("/", "_") + ".log")
            try:
                ok, passed, total, tail = run_suite(
                    suite, env, cwd_by_lang.get(suite.language), log)
            except subprocess.TimeoutExpired:
                ok, passed, total, tail = False, 0, 0, "suite timed out"
            status = "OK  " if ok else "FAIL"
            print(f"[{status}] {suite.name}: {passed}/{total} checks")
            if tail:
                for line in tail.splitlines():
                    print(f"       {line}")
            results.append((suite, ok, passed, total, tail))

        # Durability across restart (orchestrator-level: needs process
        # control, so it lives here rather than in a client suite).
        print("\n=== durability/restart (orchestrator) ===")
        try:
            ok = check_restart_durability(cluster)
        except Exception as e:  # noqa: BLE001
            print(f"FAIL durability/restart: {e}")
            ok = False
        print(f"[{'OK  ' if ok else 'FAIL'}] durability/restart")
    finally:
        support.stop_cluster(cluster)

    print("\n=== summary ===")
    all_ok = True
    width = max(len(s.name) for s, *_ in results) if results else 10
    width = max(width, len("durability/restart"))
    for suite, ok, passed, total, _ in results:
        print(f"{suite.name:<{width}}  {'PASS' if ok else 'FAIL':4}  "
              f"{passed}/{total} checks  ({suite.features})")
        all_ok &= ok
    return 0 if all_ok else 1


def check_restart_durability(cluster: support.Cluster) -> bool:
    """Durable queue + persistent message survive a full cluster restart."""
    import pika

    url = cluster.amqp_urls[0]
    conn = pika.BlockingConnection(pika.URLParameters(url))
    ch = conn.channel()
    ch.queue_declare("harness-durable", durable=True)
    ch.confirm_delivery()
    ch.basic_publish("", "harness-durable", b"survive-me",
                     properties=pika.BasicProperties(delivery_mode=2))
    ch.queue_purge("harness-durable")
    ch.basic_publish("", "harness-durable", b"survive-me",
                     properties=pika.BasicProperties(delivery_mode=2))
    conn.close()

    support.restart_cluster(cluster)

    conn = pika.BlockingConnection(pika.URLParameters(cluster.amqp_urls[0]))
    ch = conn.channel()
    deadline = time.time() + 30
    got = (False, None, None)
    while time.time() < deadline:
        got = ch.basic_get("harness-durable", auto_ack=True)
        if got[0]:
            break
        time.sleep(0.3)
    conn.close()
    if not (got[0] and got[2] == b"survive-me"):
        print(f"FAIL durable message lost after restart: {got[0]}")
        return False
    print("PASS durable queue + persistent message survive restart")
    return True


if __name__ == "__main__":
    sys.exit(main())
