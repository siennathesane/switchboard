#!/usr/bin/env python3
"""Embedded Switchboard broker from Python — standard library only (ctypes).

This drives the stable C ABI (crates/switchboard-capi, header at
crates/switchboard-capi/include/switchboard.h) over the in-process broker:
it starts a complete AMQP 0-9-1 broker inside THIS process, opens a
connection through the in-memory transport (no sockets on the client
path), declares a queue, publishes messages, consumes them, exercises a
transaction (commit and rollback), and shuts everything down cleanly.

Run from anywhere; the shared library is located RELATIVE TO THE REPO ROOT
(this file lives at <root>/examples/python/):

    cargo build -p switchboard-capi
    python3 examples/python/embedded_demo.py

Build artifact names differ per platform (macOS libswitchboard_capi.dylib,
Linux libswitchboard_capi.so, and legacy libc_switchboard* spellings), so
the finder globs both target/debug and target/release for either.
"""

import ctypes
import glob
import os
import sys
import tempfile
import time

# ---------------------------------------------------------------------------
# Locate and load the cdylib relative to the repo root.
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


def find_cdylib() -> str:
    """Find the built switchboard-capi shared library, robust to platform
    naming (libswitchboard_capi*.dylib / .so, libc_switchboard*.dylib) and
    to which profile was built (debug first, then release)."""
    patterns = []
    for profile in ("debug", "release"):
        base = os.path.join(REPO_ROOT, "target", profile)
        for where in ("", os.path.join(base, "deps")):
            patterns += [
                os.path.join(base, "libswitchboard_capi*.dylib"),
                os.path.join(base, "libswitchboard_capi*.so"),
                os.path.join(base, "libc_switchboard*.dylib"),
                os.path.join(base, "libc_switchboard*.so"),
                os.path.join(base, "switchboard_capi*.dll"),
                os.path.join(where, "libswitchboard_capi*.dylib"),
                os.path.join(where, "libswitchboard_capi*.so"),
            ]
    for pattern in patterns:
        for hit in sorted(glob.glob(pattern)):
            if os.path.isfile(hit):
                return hit
    raise SystemExit(
        "could not find the built switchboard-capi shared library.\n"
        "Build it first:\n"
        "  cargo build -p switchboard-capi"
    )


LIB_PATH = find_cdylib()
lib = ctypes.CDLL(LIB_PATH)

# ---------------------------------------------------------------------------
# ABI shapes (must mirror include/switchboard.h).
# ---------------------------------------------------------------------------


class SbMessage(ctypes.Structure):
    """Mirror of C `sb_message_t`. Pointer fields are c_void_p (NOT
    c_char_p) so the original pointers survive for sb_message_free."""

    _fields_ = [
        ("delivery_tag", ctypes.c_uint64),
        ("redelivered", ctypes.c_int32),
        ("exchange", ctypes.c_void_p),
        ("routing_key", ctypes.c_void_p),
        ("consumer_tag", ctypes.c_void_p),
        ("content_type", ctypes.c_void_p),
        ("body", ctypes.c_void_p),
        ("body_len", ctypes.c_size_t),
    ]


def init_signatures():
    """Attach restype/argtypes so ctypes passes/returns values correctly
    (on 64-bit platforms an undeclared restype would truncate pointers)."""
    i8p = ctypes.POINTER(ctypes.c_char)  # char* / const char*
    d = lib

    d.sb_version_string.restype = i8p
    d.sb_version_string.argtypes = []
    d.sb_last_error.restype = i8p
    d.sb_last_error.argtypes = []
    d.sb_string_free.restype = ctypes.c_int32
    d.sb_string_free.argtypes = [ctypes.c_void_p]
    d.sb_message_free.restype = ctypes.c_int32
    d.sb_message_free.argtypes = [ctypes.POINTER(SbMessage)]

    d.sb_config_create.restype = ctypes.c_int32
    d.sb_config_create.argtypes = [i8p, ctypes.POINTER(ctypes.c_void_p)]
    d.sb_config_set_node_id.restype = ctypes.c_int32
    d.sb_config_set_node_id.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    d.sb_config_set_expected_nodes.restype = ctypes.c_int32
    d.sb_config_set_expected_nodes.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    d.sb_config_set_janitor_interval_ms.restype = ctypes.c_int32
    d.sb_config_set_janitor_interval_ms.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    d.sb_config_free.restype = ctypes.c_int32
    d.sb_config_free.argtypes = [ctypes.c_void_p]

    d.sb_broker_start.restype = ctypes.c_int32
    d.sb_broker_start.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    d.sb_broker_is_ready.restype = ctypes.c_int32
    d.sb_broker_is_ready.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int32)]
    d.sb_broker_wait_ready.restype = ctypes.c_int32
    d.sb_broker_wait_ready.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    d.sb_broker_connect.restype = ctypes.c_int32
    d.sb_broker_connect.argtypes = [
        ctypes.c_void_p, i8p, i8p, ctypes.POINTER(ctypes.c_void_p),
    ]
    d.sb_client_open_channel.restype = ctypes.c_int32
    d.sb_client_open_channel.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p),
    ]
    d.sb_client_close.restype = ctypes.c_int32
    d.sb_client_close.argtypes = [ctypes.c_void_p]
    d.sb_client_free.restype = ctypes.c_int32
    d.sb_client_free.argtypes = [ctypes.c_void_p]
    d.sb_broker_stop.restype = ctypes.c_int32
    d.sb_broker_stop.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    d.sb_broker_free.restype = ctypes.c_int32
    d.sb_broker_free.argtypes = [ctypes.c_void_p]

    d.sb_channel_queue_declare.restype = ctypes.c_int32
    d.sb_channel_queue_declare.argtypes = [
        ctypes.c_void_p, i8p,
        ctypes.c_int32, ctypes.c_int32, ctypes.c_int32, ctypes.c_int32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    d.sb_channel_publish.restype = ctypes.c_int32
    d.sb_channel_publish.argtypes = [
        ctypes.c_void_p, i8p, i8p, i8p,
        ctypes.c_int32, ctypes.c_void_p, ctypes.c_size_t,
    ]
    d.sb_channel_get.restype = ctypes.c_int32
    d.sb_channel_get.argtypes = [
        ctypes.c_void_p, i8p, ctypes.c_int32,
        ctypes.POINTER(ctypes.c_int32), ctypes.POINTER(SbMessage),
    ]
    d.sb_channel_ack.restype = ctypes.c_int32
    d.sb_channel_ack.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    d.sb_channel_tx_select.restype = ctypes.c_int32
    d.sb_channel_tx_select.argtypes = [ctypes.c_void_p]
    d.sb_channel_tx_commit.restype = ctypes.c_int32
    d.sb_channel_tx_commit.argtypes = [ctypes.c_void_p]
    d.sb_channel_tx_rollback.restype = ctypes.c_int32
    d.sb_channel_tx_rollback.argtypes = [ctypes.c_void_p]
    d.sb_channel_free.restype = ctypes.c_int32
    d.sb_channel_free.argtypes = [ctypes.c_void_p]


init_signatures()

SB_OK = 0


# ---------------------------------------------------------------------------
# Small wrappers implementing the documented ownership rules.
# ---------------------------------------------------------------------------


def check(status: int, what: str) -> None:
    """Raise with the thread-local last-error message on any failure."""
    if status != SB_OK:
        p = lib.sb_last_error()
        detail = ctypes.string_at(p).decode("utf-8", "replace") if p else "(no message)"
        raise RuntimeError(f"{what} failed: status={status}: {detail}")


def as_bytes(ptr: int, length: int) -> bytes:
    return ctypes.string_at(ptr, length) if length else b""


def drain_queue(channel, queue: str, expect: int, no_ack: int = 0):
    """basic.get until `expect` messages are seen (publishes are applied by
    the shard asynchronously, so poll like the Rust e2e suite does). With
    no_ack=1 deliveries are auto-acknowledged (used inside tx sections,
    where a manual ack would join the transaction buffer)."""
    seen = []
    deadline = time.time() + 10
    while len(seen) < expect:
        has = ctypes.c_int32(0)
        msg = SbMessage()
        check(
            lib.sb_channel_get(
                channel, queue.encode(), no_ack, ctypes.byref(has), ctypes.byref(msg)
            ),
            f"sb_channel_get({queue})",
        )
        if has.value == 1:
            body = as_bytes(msg.body, msg.body_len)
            seen.append(body)
            print(f"    received: {body.decode()!r} (tag={msg.delivery_tag}, "
                  f"exchange={ctypes.string_at(msg.exchange).decode()}, "
                  f"routing_key={ctypes.string_at(msg.routing_key).decode()})")
            if not no_ack:
                check(lib.sb_channel_ack(channel, msg.delivery_tag), "sb_channel_ack")
        else:
            if time.time() > deadline:
                raise RuntimeError(f"only {len(seen)}/{expect} messages within 10s")
            time.sleep(0.025)
        check(lib.sb_message_free(ctypes.byref(msg)), "sb_message_free")
    return seen


def main() -> int:
    print(f"library: {LIB_PATH}")
    version = ctypes.string_at(lib.sb_version_string()).decode()
    print(f"switchboard-capi version: {version}")

    # One RocksDB data dir per broker, per process (restart = new process).
    data_dir = tempfile.mkdtemp(prefix="switchboard-capi-demo-")
    print(f"data_dir: {data_dir}")

    config = ctypes.c_void_p()
    check(lib.sb_config_create(data_dir.encode(), ctypes.byref(config)), "sb_config_create")
    check(lib.sb_config_set_node_id(config, 1), "sb_config_set_node_id")
    check(lib.sb_config_set_expected_nodes(config, 1), "sb_config_set_expected_nodes")
    # Transactions and the janitor tick: the embedded crate documents a
    # commit/tick race (Commit-Ok can mask a dropped tx) — widen the tick.
    check(
        lib.sb_config_set_janitor_interval_ms(config, 60_000),
        "sb_config_set_janitor_interval_ms",
    )

    broker = ctypes.c_void_p()
    check(lib.sb_broker_start(config, ctypes.byref(broker)), "sb_broker_start")
    print("broker: started (in-process; RocksDB + raft on a private runtime)")
    ready = ctypes.c_int32(0)
    check(lib.sb_broker_wait_ready(broker, 10_000), "sb_broker_wait_ready")
    check(lib.sb_broker_is_ready(broker, ctypes.byref(ready)), "sb_broker_is_ready")
    print(f"broker: ready={bool(ready.value)}")

    client = ctypes.c_void_p()
    check(
        lib.sb_broker_connect(
            broker, b"guest", b"guest", ctypes.byref(client)
        ),
        "sb_broker_connect",
    )
    print("client: connected as guest/guest through the in-memory transport")

    channel = ctypes.c_void_p()
    check(lib.sb_client_open_channel(client, ctypes.byref(channel)), "sb_client_open_channel")
    channel_id = ctypes.c_uint16(0)
    check(lib.sb_channel_id(channel, ctypes.byref(channel_id)), "sb_channel_id")
    print(f"channel: opened (id={channel_id.value})")

    queue = "demo.tasks"
    final_name = ctypes.c_void_p()
    check(
        lib.sb_channel_queue_declare(
            channel, queue.encode(), 1, 0, 0, 0, ctypes.byref(final_name)
        ),
        "sb_channel_queue_declare",
    )
    name = ctypes.string_at(final_name).decode()
    check(lib.sb_string_free(final_name), "sb_string_free")
    print(f"queue: declared durable queue {name!r}")

    # --- publish + consume -------------------------------------------------
    bodies = [b"hello", b"from", b"python"]
    for i, body in enumerate(bodies):
        check(
            lib.sb_channel_publish(
                channel,
                b"",                      # default exchange: direct route
                queue.encode(),
                b"text/plain",
                2,                        # persistent
                body, len(body),
            ),
            f"sb_channel_publish[{i}]",
        )
    print(f"publish: sent {len(bodies)} persistent messages to {queue!r}")
    got = drain_queue(channel, queue, len(bodies))
    print(f"consume: drained and acked {len(got)} messages")

    # --- transaction: commit ----------------------------------------------
    check(lib.sb_channel_tx_select(channel), "sb_channel_tx_select")
    print("tx: selected (buffered mode)")
    tx_body = b"tx-committed"
    check(
        lib.sb_channel_publish(
            channel, b"", queue.encode(), None, 0, tx_body, len(tx_body)
        ),
        "sb_channel_publish (tx)",
    )
    check(lib.sb_channel_tx_commit(channel), "sb_channel_tx_commit")
    print("tx: committed one message")
    drain_queue(channel, queue, 1, no_ack=1)

    # --- transaction: rollback --------------------------------------------
    rollback_body = b"tx-rolled-back"
    check(
        lib.sb_channel_publish(
            channel, b"", queue.encode(), None, 0, rollback_body, len(rollback_body)
        ),
        "sb_channel_publish (tx)",
    )
    check(lib.sb_channel_tx_rollback(channel), "sb_channel_tx_rollback")
    print("tx: rolled one message back (buffered publish vanishes)")
    has = ctypes.c_int32(1)
    msg = SbMessage()
    deadline = time.time() + 2  # give any (should-not-exist) write time to land
    while time.time() < deadline:
        check(lib.sb_channel_get(channel, queue.encode(), 1, ctypes.byref(has), ctypes.byref(msg)),
              "sb_channel_get after rollback")
        if has.value == 1:
            raise RuntimeError("rolled-back message was delivered — that is a bug")
        check(lib.sb_message_free(ctypes.byref(msg)), "sb_message_free")
        time.sleep(0.05)
    print("tx: verified the queue is empty after rollback")

    # --- clean shutdown ----------------------------------------------------
    check(lib.sb_channel_free(channel), "sb_channel_free")
    check(lib.sb_client_close(client), "sb_client_close")
    check(lib.sb_client_free(client), "sb_client_free")
    check(lib.sb_broker_stop(broker, 10_000), "sb_broker_stop")
    print("broker: stopped gracefully")
    check(lib.sb_broker_free(broker), "sb_broker_free")
    check(lib.sb_config_free(config), "sb_config_free")

    print("OK: embedded broker demo completed cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
