#!/usr/bin/env python3
"""Switchboard conformance suite — Python (stomp.py), STOMP 1.2."""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid

import stomp

CHECKS: list[tuple[str, bool]] = []
run_id = uuid.uuid4().hex[:8]


def check(name: str, fn):
    try:
        fn()
        CHECKS.append((name, True))
        print(f"PASS {name}")
    except Exception as e:  # noqa: BLE001
        CHECKS.append((name, False))
        print(f"FAIL {name}: {e}")


class Listener(stomp.ConnectionListener):
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []
        self.errors: list[str] = []
        self.receipts: set[str] = set()
        self.connected = threading.Event()

    def on_connected(self, frame):
        self.connected.set()

    def on_message(self, frame):
        self.messages.append((frame.headers.get("destination", ""), frame.body.encode()
                              if isinstance(frame.body, str) else frame.body))

    def on_error(self, frame):
        self.errors.append(frame.body or "")

    def on_receipt(self, frame):
        self.receipts.add(frame.headers.get("receipt-id", ""))


def connect(port: int) -> tuple[stomp.Connection, Listener]:
    l = Listener()
    c = stomp.Connection12([("127.0.0.1", port)], heartbeats=(0, 0))
    c.set_listener("h", l)
    c.connect("guest", "guest", wait=True)
    return c, l


def main() -> int:
    ports = [int(p) for p in os.environ["SB_STOMP_PORTS"].split(",")]
    port = ports[0]
    other = ports[1] if len(ports) > 1 else port

    conn, l = connect(port)

    # queue send/subscribe roundtrip with receipt
    q = f"/queue/hq-{run_id}"
    conn.subscribe(q, id="s1", ack="auto", receipt="r-sub")
    deadline = time.time() + 10
    while time.time() < deadline and "r-sub" not in l.receipts:
        time.sleep(0.05)
    check("subscribe + RECEIPT", lambda: None if "r-sub" in l.receipts
          else AssertionError("no receipt"))

    conn.send(q, b"hello-stomp", content_type="text/plain")
    got = None
    deadline = time.time() + 10
    while time.time() < deadline and not got:
        got = [m for m in l.messages if m[0] == q]
        time.sleep(0.05)
    check("send → MESSAGE roundtrip",
          lambda: None if (got and got[0][1] == b"hello-stomp")
          else AssertionError(f"{l.messages}"))

    # topic broadcast to two subscribers
    t = f"/topic/ht-{run_id}"
    conn2, l2 = connect(port)
    conn.subscribe(t, id="s2", ack="auto")
    conn2.subscribe(t, id="s1", ack="auto")
    time.sleep(0.3)
    conn.send(t, b"broadcast")
    deadline = time.time() + 10
    while time.time() < deadline and (len([m for m in l.messages if m[0] == t]) < 1
                                      or len([m for m in l2.messages if m[0] == t]) < 1):
        time.sleep(0.05)
    check("topic reaches both subscribers",
          lambda: None if ([m for m in l.messages if m[0] == t]
                           and [m for m in l2.messages if m[0] == t])
          else AssertionError("a subscriber missed the broadcast"))

    # client ack: message stays unacked for others, gone after ACK
    q2 = f"/queue/hq-ack-{run_id}"
    conn.subscribe(q2, id="s3", ack="client", receipt="r-ack3")
    deadline = time.time() + 10
    while time.time() < deadline and "r-ack3" not in l.receipts:
        time.sleep(0.05)
    conn.send(q2, b"needs-ack")
    deadline = time.time() + 10
    msg = None
    while time.time() < deadline and not msg:
        msg = [m for m in l.messages if m[0] == q2]
        time.sleep(0.05)
    check("client-ack mode holds message", lambda: None if msg else AssertionError("no delivery"))

    # cross-node publish reaches the STOMP subscriber
    c3, l3 = connect(other)
    c3.send(q2, b"cross-stomp")
    deadline = time.time() + 10
    msg2 = None
    while time.time() < deadline and not msg2:
        msg2 = [m for m in l.messages if m[0] == q2 and m[1] == b"cross-stomp"]
        time.sleep(0.05)
    check("cross-node STOMP delivery", lambda: None if msg2 else AssertionError("no delivery"))

    # unknown frame → ERROR
    conn.send_frame("BOGUS\n\n\0")
    deadline = time.time() + 10
    while time.time() < deadline and not l.errors:
        time.sleep(0.05)
    check("unknown frame yields ERROR", lambda: None if l.errors else AssertionError("no ERROR"))

    # disconnect cleanly
    conn.disconnect()
    conn2.disconnect()
    c3.disconnect()

    failed = [c for c in CHECKS if not c[1]]
    print(f"\npython/stomp: {len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
