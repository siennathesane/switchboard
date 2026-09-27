#!/usr/bin/env python3
"""Switchboard conformance suite — Python (paho-mqtt), MQTT 3.1.1."""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid

import paho.mqtt.client as mqtt

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


class Client:
    """Small synchronous wrapper around paho's async client."""

    def __init__(self, port: int, client_id: str, clean: bool = True):
        self.messages: list[tuple[str, bytes, dict]] = []
        self.connack: dict = {}
        self.c = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=clean if hasattr(mqtt.Client, "clean_session") else None,
            protocol=mqtt.MQTTv311,
        )
        self.c.on_connect = self._on_connect
        self.c.on_message = self._on_message
        self.connected = threading.Event()
        self.c.connect("127.0.0.1", port, keepalive=30)
        self.c.loop_start()
        assert self.connected.wait(15), "timed out waiting for CONNACK"

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        self.connack = flags
        self.connected.set()

    def _on_message(self, client, userdata, msg):
        props = dict(msg.properties) if msg.properties else {}
        self.messages.append((msg.topic, msg.payload, props))

    def subscribe(self, topic: str, qos: int = 0):
        got = threading.Event()
        res: dict = {}

        def on_sub(client, userdata, mid, reason_codes, properties=None):
            res["codes"] = reason_codes
            got.set()

        self.c.on_subscribe = on_sub
        self.c.subscribe(topic, qos=qos)
        assert got.wait(15), "no SUBACK"
        self.c.on_subscribe = None
        return res.get("codes")

    def publish(self, topic: str, payload: bytes, qos: int = 0, retain: bool = False):
        info = self.c.publish(topic, payload, qos=qos, retain=retain)
        info.wait_for_publish(15)
        assert info.is_published(), f"publish not acknowledged (rc={info.rc})"

    def wait_for(self, count: int, timeout: float = 10.0):
        deadline = time.time() + timeout
        while time.time() < deadline and len(self.messages) < count:
            time.sleep(0.05)
        return self.messages[:count]

    def close(self):
        self.c.disconnect()
        self.c.loop_stop()


def main() -> int:
    ports = [int(p) for p in os.environ["SB_MQTT_PORTS"].split(",")]
    port = ports[0]
    other = ports[1] if len(ports) > 1 else port

    a = Client(port, f"harness-a-{run_id}")
    check("connect + CONNACK", lambda: None)

    # qos 0/1/2 round trips
    a.subscribe(f"h/{run_id}/q0")
    a.subscribe(f"h/{run_id}/q1", qos=1)
    a.subscribe(f"h/{run_id}/q2", qos=2)
    a.publish(f"h/{run_id}/q0", b"m0", qos=0)
    a.publish(f"h/{run_id}/q1", b"m1", qos=1)
    a.publish(f"h/{run_id}/q2", b"m2", qos=2)
    got = a.wait_for(3)
    check("qos 0/1/2 delivery to one subscriber",
          lambda: None if sorted((t, p) for t, p, _ in got) == [
              (f"h/{run_id}/q0", b"m0"), (f"h/{run_id}/q1", b"m1"), (f"h/{run_id}/q2", b"m2")]
          else AssertionError(f"{got}"))

    # wildcards
    b = Client(port, f"harness-b-{run_id}")
    b.subscribe(f"h/{run_id}/+/x")
    b.subscribe(f"h/{run_id}/deep/#")
    b.publish(f"h/{run_id}/mid/x", b"plus")
    b.publish(f"h/{run_id}/deep/1/2", b"hash")
    b.publish(f"h/{run_id}/other", b"none")
    got = b.wait_for(2)
    check("wildcard + and # matching",
          lambda: None if {t for t, _, _ in got} == {f"h/{run_id}/mid/x", f"h/{run_id}/deep/1/2"}
          else AssertionError(f"{got}"))

    # retained
    r = Client(port, f"harness-r-{run_id}")
    r.publish(f"h/{run_id}/retained", b"keep-me", retain=True)
    time.sleep(0.4)
    n = Client(port, f"harness-n-{run_id}")
    n.subscribe(f"h/{run_id}/retained")
    got = n.wait_for(1)
    check("retained message delivered on subscribe",
          lambda: None if (got and got[0][1] == b"keep-me") else AssertionError(f"{got}"))

    # cross-node: subscribe on node1, publish on node2
    c2 = Client(other, f"harness-c2-{run_id}")
    a.subscribe(f"h/{run_id}/cross")
    c2.publish(f"h/{run_id}/cross", b"from-node2", qos=1)
    got = a.wait_for(len(a.messages) + 1)
    check("cross-node mqtt publish",
          lambda: None if any(t == f"h/{run_id}/cross" and p == b"from-node2" for t, p, _ in got)
          else AssertionError(f"{got}"))

    # persistent session: durable queue survives a clean disconnect? A
    # clean=False client that reconnects still receives messages sent
    # while it was away.
    p_id = f"harness-persist-{run_id}"
    p = Client(port, p_id, clean=False)
    p.subscribe(f"h/{run_id}/persist", qos=1)
    p.close()
    time.sleep(0.3)
    a.publish(f"h/{run_id}/persist", b"offline", qos=1)
    p2 = Client(port, p_id, clean=False)
    p2.subscribe(f"h/{run_id}/persist", qos=1)
    got = p2.wait_for(1)
    check("persistent session keeps offline qos1 messages",
          lambda: None if (got and got[0][1] == b"offline")
          else AssertionError(f"{got}"))

    for cl in (a, b, r, n, c2, p2):
        cl.close()

    failed = [c for c in CHECKS if not c[1]]
    print(f"\npython/mqtt: {len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
