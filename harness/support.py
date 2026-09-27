"""Switchboard multi-language conformance harness — broker lifecycle.

Boots real `switchboard` broker processes (a multi-node cluster by
default), hands their addresses to language client suites via
environment variables, and tears everything down again.

Env contract with suites:
    SB_AMQP_URLS   amqp://user:pass@host:port/vhost[,…one per node]
    SB_MQTT_PORTS  comma-separated MQTT-capable client ports (same order)
    SB_STOMP_PORTS comma-separated STOMP-capable client ports (same order)
    SB_VHOST       the vhost all suites use ("/")
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BIN = REPO / "target" / "debug" / "switchboard"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_amqp(addr: str, timeout: float = 60.0) -> None:
    """Wait until `addr` speaks AMQP (the server sends Connection.Start)."""
    deadline = time.time() + timeout
    host, port = addr.rsplit(":", 1)
    while time.time() < deadline:
        try:
            with socket.create_connection((host, int(port)), timeout=2) as s:
                s.sendall(b"AMQP\x00\x00\x09\x01")
                data = s.recv(16)
                # The server replies with a Connection.Start frame (type
                # byte 0x01); any non-empty reply proves it is our broker.
                if data:
                    return
        except OSError:
            pass
        time.sleep(0.2)
    raise TimeoutError(f"broker at {addr} never came up")


@dataclass
class Cluster:
    """A running switchboard cluster and its per-node addresses."""

    processes: list[subprocess.Popen] = field(default_factory=list)
    client_addrs: list[str] = field(default_factory=list)
    internal_addrs: list[str] = field(default_factory=list)
    # Complete books (never truncated by scale_down) for crash-recovery.
    all_client_ports: list[int] = field(default_factory=list)
    all_internal_ports: list[int] = field(default_factory=list)
    data_dir: Path | None = None
    user: str = "guest"
    password: str = "guest"

    @property
    def amqp_urls(self) -> list[str]:
        return [
            f"amqp://{self.user}:{self.password}@{addr}/"
            for addr in self.client_addrs
        ]

    def env(self) -> dict[str, str]:
        return {
            "SB_AMQP_URLS": ",".join(self.amqp_urls),
            "SB_MQTT_PORTS": ",".join(a.split(":")[1] for a in self.client_addrs),
            "SB_STOMP_PORTS": ",".join(a.split(":")[1] for a in self.client_addrs),
            "SB_VHOST": "/",
            "SB_USER": self.user,
            "SB_PASSWORD": self.password,
        }


def _spawn(node_id: int, client: int, internal: int, data: Path,
           seeds: list[str], bootstrap: bool, expected: int) -> subprocess.Popen:
    cmd = [
        str(BIN),
        "--node-id", str(node_id),
        "--listen", f"127.0.0.1:{client}",
        "--internal", f"127.0.0.1:{internal}",
        "--advertise", f"127.0.0.1:{internal}",
        "--data", str(data),
        "--expected-nodes", str(expected),
        "--log", "warn",
    ]
    if bootstrap:
        cmd += ["--bootstrap"]
    if seeds:
        cmd += ["--seeds", ",".join(seeds)]
    log_dir = os.environ.get("SB_BROKER_LOG_DIR")
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        out = open(Path(log_dir) / f"broker-{node_id}.log", "a")
        return subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT)
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def start_cluster(nodes: int = 3, keep_data: Path | None = None) -> Cluster:
    """Boot an `nodes`-node switchboard cluster and wait for readiness."""
    cluster = Cluster()
    cluster.data_dir = keep_data if keep_data else Path(
        tempfile.mkdtemp(prefix="sb-harness-"))
    specs = []
    for i in range(nodes):
        specs.append((i + 1, free_port(), free_port()))
    cluster.client_addrs = [f"127.0.0.1:{c}" for _, c, _ in specs]
    cluster.internal_addrs = [f"127.0.0.1:{i}" for _, _, i in specs]
    cluster.all_client_ports = [c for _, c, _ in specs]
    cluster.all_internal_ports = [i for _, _, i in specs]

    for node_id, client, internal in specs:
        data = cluster.data_dir / f"node{node_id}"
        data.mkdir(parents=True, exist_ok=True)
        seeds = [] if node_id == 1 else [cluster.internal_addrs[0]]
        proc = _spawn(
            node_id, client, internal, data,
            seeds, bootstrap=(node_id == 1), expected=nodes,
        )
        cluster.processes.append(proc)

    for addr in cluster.client_addrs:
        wait_amqp(addr)
    wait_broker_ready(cluster)
    return cluster


def wait_broker_ready(cluster: Cluster, timeout: float = 180.0) -> None:
    """Wait until AMQP actually serves queue operations.

    The AMQP listener is up before the raft layout is installed, so a
    socket check is not enough for a freshly formed cluster.
    """
    import pika  # the harness venv provides it

    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            conn = pika.BlockingConnection(
                pika.URLParameters(cluster.amqp_urls[0]))
            ch = conn.channel()
            ch.queue_declare("harness-ready", durable=True)
            ch.queue_delete("harness-ready")
            ch.close()
            conn.close()
            return
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.5)
    raise TimeoutError(f"broker never became ready: {last}")


def stop_cluster(cluster: Cluster, wipe: bool = True) -> None:
    for p in cluster.processes:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    deadline = time.time() + 15
    for p in cluster.processes:
        remaining = max(0.1, deadline - time.time())
        try:
            p.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=5)
    cluster.processes.clear()
    if wipe and cluster.data_dir and cluster.data_dir.name.startswith("sb-harness-"):
        shutil.rmtree(cluster.data_dir, ignore_errors=True)


def restart_cluster(cluster: Cluster) -> None:
    """Stop every node and restart it on the SAME ports and data."""
    for p in cluster.processes:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    deadline = time.time() + 15
    for p in cluster.processes:
        try:
            p.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            p.kill()
    cluster.processes.clear()

    for node_id, addr in enumerate(cluster.client_addrs, start=1):
        internal = cluster.internal_addrs[node_id - 1]
        data = cluster.data_dir / f"node{node_id}"
        data.mkdir(parents=True, exist_ok=True)
        seeds = [] if node_id == 1 else [cluster.internal_addrs[0]]
        proc = _spawn(
            node_id,
            int(addr.split(":")[1]),
            int(internal.split(":")[1]),
            data, seeds, bootstrap=(node_id == 1),
            expected=len(cluster.client_addrs),
        )
        cluster.processes.append(proc)
    for addr in cluster.client_addrs:
        wait_amqp(addr)
    wait_broker_ready(cluster)


def die(msg: str) -> None:
    print(f"harness: {msg}", file=sys.stderr)
    sys.exit(2)


def add_node(cluster: Cluster, wait: bool = True) -> int:
    """Scale up: spawn one more node joined through node1. Returns its id."""
    node_id = len(cluster.client_addrs) + 1
    client = free_port()
    internal = free_port()
    cluster.client_addrs.append(f"127.0.0.1:{client}")
    cluster.internal_addrs.append(f"127.0.0.1:{internal}")
    cluster.all_client_ports.append(client)
    cluster.all_internal_ports.append(internal)
    data = cluster.data_dir / f"node{node_id}"
    data.mkdir(parents=True, exist_ok=True)
    proc = _spawn(
        node_id, client, internal, data,
        [cluster.internal_addrs[0]], bootstrap=False,
        expected=len(cluster.client_addrs),
    )
    cluster.processes.append(proc)
    if wait:
        wait_amqp(cluster.client_addrs[-1])
        wait_broker_ready_on(cluster, cluster.client_addrs[-1])
        # Wait until the layout covers the new node (controller anchors a
        # group on it).
        deadline = time.time() + 60
        while time.time() < deadline:
            out = subprocess.run(
                [str(BIN), "--help"], capture_output=True)  # placeholder no-op
            # Coverage via a pika check on any queue op through the NEW node.
            try:
                import pika
                conn = pika.BlockingConnection(
                    pika.URLParameters(cluster.amqp_urls[-1]))
                ch = conn.channel()
                ch.queue_declare(f"scale-probe-{node_id}", durable=True)
                ch.queue_delete(f"scale-probe-{node_id}")
                ch.close()
                conn.close()
                return node_id
            except Exception:  # noqa: BLE001
                time.sleep(0.5)
        raise TimeoutError(f"new node {node_id} never became routable")
    return node_id


def wait_broker_ready_on(cluster: Cluster, addr: str, timeout: float = 180.0) -> None:
    import pika

    deadline = time.time() + timeout
    last: Exception | None = None
    url = f"amqp://{cluster.user}:{cluster.password}@{addr}/"
    while time.time() < deadline:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(url))
            ch = conn.channel()
            ch.queue_declare("harness-ready", durable=True)
            ch.queue_delete("harness-ready")
            ch.close()
            conn.close()
            return
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.5)
    raise TimeoutError(f"broker at {addr} never became ready: {last}")


def scale_down(cluster: Cluster, to_nodes: int) -> list[int]:
    """Scale down: SIGKILL every node beyond `to_nodes` (crash test).

    Returns the killed node ids. NOTE: killing enough nodes can remove a
    whole shard group's quorum (groups are consecutive triples), making
    that shard unavailable until the killed nodes restart — use
    `restart_killed` to bring them back.
    """
    victims = cluster.processes[to_nodes:]
    for p in victims:
        if p.poll() is None:
            p.kill()
    for p in victims:
        p.wait(timeout=10)
    cluster.processes = cluster.processes[:to_nodes]
    cluster.client_addrs = cluster.client_addrs[:to_nodes]
    cluster.internal_addrs = cluster.internal_addrs[:to_nodes]
    # The survivors must still serve reads/writes for live groups.
    wait_broker_ready(cluster, timeout=120)
    return list(range(to_nodes + 1, to_nodes + 1 + len(victims)))


def restart_killed(cluster: Cluster, node_ids: list[int]) -> None:
    """Crash-recovery: restart previously SIGKILLed nodes on their old
    ports and data dirs; they rejoin and their groups regain quorum."""
    # Restore the complete address book from the record kept at boot.
    cluster.client_addrs = [f"127.0.0.1:{p}" for p in cluster.all_client_ports]
    cluster.internal_addrs = [f"127.0.0.1:{p}" for p in cluster.all_internal_ports]
    for node_id in node_ids:
        idx = node_id - 1
        addr = cluster.client_addrs[idx]
        internal = cluster.internal_addrs[idx]
        data = cluster.data_dir / f"node{node_id}"
        seeds = [] if node_id == 1 else [cluster.internal_addrs[0]]
        proc = _spawn(
            node_id,
            int(addr.split(":")[1]),
            int(internal.split(":")[1]),
            data, seeds, bootstrap=(node_id == 1),
            expected=len(cluster.all_client_ports),
        )
        cluster.processes.append(proc)
    wait_broker_ready(cluster, timeout=180)
