#!/usr/bin/env python3
"""Switchboard load + scale stability test.

Sustained multi-client traffic against a real cluster while it SCALES UP
(3 -> 6 nodes joined under load) and SCALES DOWN (6 -> 3 by SIGKILL),
followed by crash recovery (killed nodes restart from their data dirs)
and a full drain.

Event-driven: phases end when a message-count target is confirmed, not
after a fixed wall-clock sleep — a phase takes ~3-6 s instead of 20+.

Verdict: after crash recovery every group regains quorum, so every
confirmed sequence must be consumed (acked = raft-committed = durable).
No holes allowed anywhere.

Usage: harness-venv/bin/python harness/scale_test.py
       [--msgs-per-phase 150] [--min-phase-secs 3]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support  # noqa: E402

GO_LOADPUB = '''package main

import (
    "context"
    "fmt"
    "os"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
)

func dial(url string) *amqp.Connection {
    for i := 0; i < 40; i++ {
        conn, err := amqp.Dial(url)
        if err == nil {
            return conn
        }
        time.Sleep(500 * time.Millisecond)
    }
    return nil
}

func main() {
    conn := dial(os.Args[1])
    if conn == nil { fmt.Println(0); return }
    defer conn.Close()
    ch, err := conn.Channel()
    if err != nil { fmt.Println(0); return }
    ctx := context.Background()
    ch.Confirm(false)
    n := 0
    deadline := time.Now().Add(8 * time.Second)
    for time.Now().Before(deadline) {
        conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", os.Args[2],
            false, false, amqp.Publishing{Body: []byte(fmt.Sprintf("go:%d", n)),
            DeliveryMode: amqp.Persistent})
        if err != nil { break }
        if !conf.Wait() { break }
        n++
    }
    fmt.Println(n)
}
'''


class Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.confirmed: dict[str, set[int]] = {}
        self.consumed: dict[str, set[int]] = {}

    def pub(self, tag: str, seq: int) -> None:
        with self.lock:
            self.confirmed.setdefault(tag, set()).add(seq)

    def consume(self, tag: str, seq: int) -> None:
        with self.lock:
            self.consumed.setdefault(tag, set()).add(seq)

    def totals(self) -> tuple[int, int]:
        with self.lock:
            conf = sum(len(v) for v in self.confirmed.values())
            cons = sum(len(v) for v in self.consumed.values())
            return conf, cons

    def consumed_total(self) -> int:
        with self.lock:
            return sum(len(v) for v in self.consumed.values())

    def confirmed_total(self) -> int:
        with self.lock:
            return sum(len(v) for v in self.confirmed.values())

    def verdict(self) -> tuple[bool, list[str]]:
        with self.lock:
            lines: list[str] = []
            ok = True
            for tag in sorted(self.confirmed):
                conf = self.confirmed[tag]
                cons = self.consumed.get(tag, set())
                holes = sorted(conf - cons)
                if holes:
                    ok = False
                lines.append(
                    f"{tag}: confirmed={len(conf)} consumed={len(cons)} "
                    f"holes={len(holes)} {holes[:5]}")
            return ok, lines


def backoff_reset() -> float:
    return 0.5


def backoff_next(cur: float) -> float:
    return min(cur * 2, 5.0)


STOP = threading.Event()


def pika_publisher(url: str, queue: str, stats: Stats, tag: str) -> None:
    import pika

    seq = 0
    backoff = 0.5
    while not STOP.is_set():
        try:
            conn = pika.BlockingConnection(pika.URLParameters(url))
            ch = conn.channel()
            ch.confirm_delivery()
            while not STOP.is_set():
                ch.basic_publish("", queue, f"{tag}:{seq}".encode(),
                                 properties=pika.BasicProperties(delivery_mode=2))
                stats.pub(tag, seq)
                seq += 1
                time.sleep(0.02)
            conn.close()
            backoff = 0.5
        except Exception:  # noqa: BLE001 — scale events kill connections
            time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)


def pika_consumer(url: str, queue: str, stats: Stats) -> None:
    import pika

    backoff = 0.5
    while not STOP.is_set():
        try:
            conn = pika.BlockingConnection(pika.URLParameters(url))
            ch = conn.channel()
            for method, _props, body in ch.consume(queue, inactivity_timeout=1):
                if STOP.is_set():
                    break
                if method is None:
                    continue
                tag, seq = body.decode().split(":")
                stats.consume(tag, int(seq))
                ch.basic_ack(method.delivery_tag)
            try:
                ch.cancel()
            except Exception:  # noqa: BLE001
                pass
            conn.close()
            backoff = 0.5
        except Exception:  # noqa: BLE001
            time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)


def go_publisher(url: str, queue: str) -> int:
    path = Path("harness/clients/go/loadpub.go")
    path.write_text(GO_LOADPUB)
    try:
        r = subprocess.run(["go", "run", "loadpub.go", url, queue],
                           cwd="harness/clients/go",
                           capture_output=True, text=True, timeout=60)
        return int(r.stdout.strip().splitlines()[-1])
    except Exception:  # noqa: BLE001
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--scale-to", type=int, default=6)
    ap.add_argument("--msgs-per-phase", type=int, default=100)
    ap.add_argument("--min-phase-secs", type=float, default=2.0)
    args = ap.parse_args()

    queue = f"load-q-{uuid.uuid4().hex[:6]}"

    t0 = time.time()
    print("booting initial cluster…")
    cluster = support.start_cluster(args.nodes)
    stats = Stats()

    import pika
    conn = pika.BlockingConnection(pika.URLParameters(cluster.amqp_urls[0]))
    ch = conn.channel()
    ch.queue_declare(queue, durable=True)
    conn.close()

    def wait_confirmed(n: int, floor: float) -> None:
        """End the phase after n more confirmed messages (min `floor`)."""
        t = time.time()
        with stats.lock:
            base = sum(len(v) for v in stats.confirmed.values())
        while time.time() - t < floor:
            if stats.confirmed_total() - base >= n:
                return
            time.sleep(0.1)

    workers: list[threading.Thread] = []
    ok = True
    try:
        for i in range(min(2, args.nodes)):
            threading.Thread(target=pika_publisher,
                             args=(cluster.amqp_urls[i], queue, stats, f"py{i}"),
                             daemon=True).start()
            threading.Thread(target=pika_consumer,
                             args=(cluster.amqp_urls[-i - 1], queue, stats),
                             daemon=True).start()

        print(f"[t+{time.time()-t0:5.1f}s] phase 1: steady load on {args.nodes} nodes")
        wait_confirmed(args.msgs_per_phase, args.min_phase_secs)
        c, _ = stats.totals()
        print(f"  consumed: {c}")

        print(f"[t+{time.time()-t0:5.1f}s] phase 2: SCALE UP {args.nodes} -> {args.scale_to} under load")
        def add_all():
            for _ in range(args.scale_to - args.nodes):
                support.add_node(cluster)
                print(f"  node {len(cluster.client_addrs)} joined", flush=True)
        up = threading.Thread(target=add_all)
        up.start()
        time.sleep(args.min_phase_secs)
        go_n = go_publisher(cluster.amqp_urls[0], queue)
        for i in range(go_n):
            stats.pub("go", i)
        print(f"  go burst confirmed {go_n} messages during scale-up")
        up.join()
        wait_confirmed(args.msgs_per_phase, args.min_phase_secs)
        c, _ = stats.totals()
        print(f"  consumed: {c}")

        print(f"[t+{time.time()-t0:5.1f}s] phase 3: SCALE DOWN {args.scale_to} -> {args.nodes} by SIGKILL (crash)")
        killed = support.scale_down(cluster, args.nodes)
        wait_confirmed(args.msgs_per_phase, args.min_phase_secs)

        print(f"[t+{time.time()-t0:5.1f}s] phase 4: crash recovery — restart nodes {killed}")
        support.restart_killed(cluster, killed)

        print(f"[t+{time.time()-t0:5.1f}s] phase 5: steady load on {len(cluster.client_addrs)} nodes")
        wait_confirmed(args.msgs_per_phase, args.min_phase_secs)

        print(f"[t+{time.time()-t0:5.1f}s] phase 6: drain (publishers stopped, consumers finish)")
        deadline = time.time() + 300
        last_progress = (time.time(), stats.consumed_total())
        while time.time() < deadline:
            ok_now, _ = stats.verdict()
            if ok_now:
                break
            consumed_now = stats.consumed_total()
            now = time.time()
            if consumed_now > last_progress[1]:
                last_progress = (now, consumed_now)
            elif now - last_progress[0] > 30:
                print("  drain: no progress for 30s — stopping")
                break
            time.sleep(0.2)
        # Residual inspection: pull leftover queue messages. Anything that
        # comes out now was stuck in delivery (recoverable), not lost.
        residual: list[str] = []
        try:
            import pika as _pika
            conn = _pika.BlockingConnection(
                _pika.URLParameters(cluster.amqp_urls[0]))
            ch = conn.channel()
            while True:
                m = ch.basic_get(queue, auto_ack=True)
                if not m:
                    break
                residual.append(m[2].decode())
            ch.close(); conn.close()
        except Exception as e:  # noqa: BLE001
            residual = [f"<inspect failed: {e}>"]
    finally:
        STOP.set()
        for t in workers:
            t.join(timeout=2)
        conf, cons = stats.totals()
        ok, lines = stats.verdict()
        print("\n=== load/scale results (post-recovery full drain) ===")
        for line in lines:
            print(f"  {line}")
        if residual:
            lines.append(f"residual queue messages: {len(residual)} {residual[:6]}")
            print(f"  residual queue messages: {len(residual)} {residual[:6]}")
        print(f"  totals: confirmed={conf} consumed={cons}")
        print(f"verdict: {'PASS' if ok else 'FAIL'} (confirmed ⇒ consumed, no holes)")
        support.stop_cluster(cluster)
        print(f"total wall time: {time.time() - t0:.1f}s")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
