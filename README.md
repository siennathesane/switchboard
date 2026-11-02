# Switchboard

A **multi-master AMQP 0-9-1 broker** in modern Rust: writes are accepted on
every node, state is replicated through raft groups of at most three voters
([OpenRaft](https://github.com/databendlabs/openraft)) persisted in
RocksDB, and client connections are served over TCP or TLS (rustls with
the aws-lc-sys crypto provider).

Implemented from the specification (`docs/amqp0-9-1.pdf`) plus its
normative method registry — not derived from any existing broker.

## A New Start

Dear User,

This technology was designed by me, but built entirely from scratch by an AI/LLM using only the AMQP 0-9-1 specification documentation and an architecture requirement I pulled from my own previous work with Raft. While this is production-ready, and I will be using it for my own production projects, this project is an exploration and experimentation of internet-grade technology not built by humans. I have code-reviewed all of this, and I think the architecture meets my personal requirements, but some people feel certain ways about AI-/LLM-generated code.

In the init.jsonl.parts folder is the entire initial session history, including my original prompt & some steering prompts, up to the point where I maxed out long-horizon capabilities - run `cat init.jsonl.parts/part-* > init.jsonl` to reconstruct. When properly managed, AI is a tool for usefulness, not a replacement for human expertise. This codebase is a testament to the power and potential of AI in building reliable, scalable, and internet-grade technology when properly managed by a human expert.

This is considered an exploration and experiment due to the inherent complexity of message broker design patterns. Distributed state, realtime updates, message routing, proxying, and transforming, multiple network procotols, and multiple access methods. The complexity of this system, despite the age of the AMQP 0.9.1 protocol, is a direct reflection of an AI'/LLM's ability to constructively and concretely manage systemic complexity at this scale. A single human engineer can design, build, operate, and maintain this kind of system; however it is not inherently clear across the industry whether or not an AI/LLM can. Yes it has the inherent knowledge, and it has strong capabilities, yet more complex distributed systems are difficult as it requires contextual knowledge larger than a single context window.

I will be maintaining this project and adding features as I see fit, but I am not planning to make any major changes to the architecture. Any fixes moving forward will be small, incremental improvements, bug fixes, and performance improvements. I will not be providing distribution-ready packages of any format as my needs of this project will be different than yours. You best know your needs, and whether that's Docker, Kuberentes, Nomad, Systemd, SysV, or a custom build, implementing your own needs is the best way to maintain human expertise in your own projects. This is a single-binary, single-library project, with a C ABI; integration into any part of your stack is easy.

And in finality, this is a love letter to @rabbitmq. RabbitMQ was the message broker that taught me distributed systems design, and this project is a tribute to its legacy.

With love, Sienna

## Quick start

```bash
# single node
cargo run --release -- --bootstrap --node-id 1 --listen 0.0.0.0:5672 --internal 0.0.0.0:5673

# nine-node cluster: node 1 bootstraps, nodes 2..9 join it
#   node k: --bootstrap=false --node-id k --seeds 127.0.0.1:5673 --expected-nodes 9
```

Any standard AMQP 0-9-1 client (default `guest`/`guest`, vhost `/`) can
then publish and consume **through any of the nodes**.

## Embedded library

The broker is also a library. `switchboard-embedded` runs a full
raft-replicated node in-process and connects a typed AMQP 0-9-1 client to
it over an in-memory duplex — no sockets on the client path, same
handshake/auth/semantics as wire clients:

```toml
[dependencies]
switchboard-embedded = "1.0.0-rc.1"
```

```rust
use switchboard_embedded::{Broker, BrokerConfig, QueueOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::start(BrokerConfig::new("./data")).await?;
    let conn = broker.connect("guest", "guest").await?;
    let channel = conn.open_channel().await?;
    channel.queue_declare("tasks", &QueueOptions::new().durable(true)).await?;
    channel.publish_confirmed("", "tasks", Default::default(), b"hello".to_vec()).await?;
    let delivery = channel.get("tasks", true).await?.expect("message");
    assert_eq!(delivery.body, b"hello");
    conn.close().await;
    broker.stop(std::time::Duration::from_secs(5)).await?;
    Ok(())
}
```

Restart is a new process on the same `data_dir` (durable queues and their
messages reappear); see `docs/architecture.md` ("Embedded library") for
the full runtime contract.

## C ABI (embed from any language)

`switchboard-capi` exposes the same embedded broker over a stable C ABI
(`sb_`-prefixed symbols, header at
[`crates/switchboard-capi/include/switchboard.h`](crates/switchboard-capi/include/switchboard.h)):
synchronous calls, frozen status codes, panic containment, polling-only
consumption, and a handle table that turns double-free/use-after-free
into errors instead of undefined behavior. A standard-library-only
ctypes demo drives the whole lifecycle:

```bash
cargo build -p switchboard-capi
python3 examples/python/embedded_demo.py
```

See `docs/architecture.md` ("C ABI over the embedded library") for the
full ABI contract.

## Operations surface

* **Prometheus metrics** — `GET /metrics` on the HTTP protocol port:
  message-lifecycle counters, per-queue depth, per-protocol connection
  gauges, node vitals (text exposition 0.0.4, no auth, like `/health`
  and `/stats`).
* **Admin API** — JSON over HTTP basic auth under `/api/…` (same argon2
  check as every protocol gateway): users/vhosts/permissions CRUD plus
  queue/exchange listings; restricted users are refused, mirroring
  RabbitMQ's administrator-only management API.
* **Backup/restore** — `switchboard backup --data <dir> --to <file>`
  writes a consistent single-file archive of a *stopped* broker's entire
  store (raft logs + state machines + message journal, via RocksDB
  checkpoints); `switchboard restore <archive> --data <new-dir>`
  validates the storage-format version gate, then materializes it into a
  fresh directory.

## Protocols

One client port carries every supported protocol, selected by sniffing
the first bytes (with or without TLS):

| protocol | selection | notes |
|---|---|---|
| AMQP 0-9-1 | `AMQP\0\0\9\1` | native protocol, always on |
| AMQP 1.0 | `AMQP\0\1\0\0` | settled + unsettled deliveries with dispositions (SASL PLAIN/ANONYMOUS) |
| MQTT 3.1.1 | `0x10 …` CONNECT | full QoS 0/1/2, replicated retained messages, persistent sessions |
| STOMP | `STOMP\n` / `CONNECT\n` | 1.0–1.2 subset, ack modes, transactions, heart-beats |
| WebSocket | `Upgrade: websocket` | wraps MQTT/STOMP/AMQP 1.0 (`?mqtt`, path or first-frame sniffing) |
| HTTP | `GET /health` | JSON health probe |

Pick the set with `--protocols amqp,mqtt,stomp,ws,amqp1,http`.

## Self-discovery

Nodes assemble without static peer lists; all sources feed the join
protocol:

```bash
# node 1 forms the cluster
switchboard --bootstrap --node-id 1 --internal 0.0.0.0:5673

# node 2 knows nothing in advance: it discovers node 1 via DNS or mDNS
switchboard --node-id 2 --dns-seed switchboard.internal.example.com   # A/AAAA records
switchboard --node-id 2 --dns-srv internal.example.com                # _switchboard._tcp.<domain> SRV
switchboard --node-id 2 --mdns                                        # mDNS LAN discovery
```

* `--dns-seed host[:port]` — members' internal addresses in DNS A/AAAA
  records (re-resolved every `--discovery-interval` seconds, default 30).
* `--dns-srv <domain>` — `_switchboard._tcp.<domain>` SRV records.
* `--mdns` — advertise + browse `_switchboard._tcp.local.` (TXT: `id`,
  `internal`, `client`); zero-configuration LAN assembly.

A node with neither `--bootstrap` nor `--seeds` starts *pending* and
joins as soon as discovery finds the cluster.

## Design

See [`docs/architecture.md`](docs/architecture.md) for the cluster design,
the internal management protocol (join, formation, forwarding, credit
based delivery) and known limitations, and
[`docs/conformance.md`](docs/conformance.md) for the specification
conformance matrix.

Crates:

```
switchboard-wire      AMQP codec (frames, methods, field tables)
switchboard-core      AMQ model state machines (topology, queues, routing)
switchboard-store     RocksDB + OpenRaft storage (+ backup/restore tools)
switchboard-cluster   multi-raft node, internal RPC, forwarding, membership
switchboard-server    client connections, channels, TLS, gateway, admin/metrics
switchboard-embedded  in-process broker + typed AMQP client library
switchboard-capi      stable C ABI over the embedded library
switchboard           the broker binary
```

## Testing

Every push runs a 13-job CI matrix (`.github/workflows/ci.yml`). Each
layer exists to catch a different class of failure:

**Unit & in-process integration** — 600+ tests
(`cargo test --workspace`): byte-level conformance of the entire AMQP
method table against the normative registry, state-machine suites for
every subsystem, and end-to-end tests speaking raw AMQP over TCP to an
in-process broker (handshake, routing, confirms, transactions, requeue,
authz refusals). Fast and deterministic — the correctness floor.

**Multi-language conformance** — *other people's clients* — pika,
paho-mqtt, stomp.py (Python), amqp091-go (Go), amqplib (Node), and
bunny (Ruby) — drive a real three-node TCP cluster, followed by a
durability check (durable queue + persistent message survive a hard
restart) and a scale test pushing load across a 3→6→3 cluster with
hard-kill churn. Self-tests share the broker's own assumptions about
the wire; somebody else's client library does not.

**Jepsen** — five-node clusters in Docker, three consistency models
(fifo, fanout, topology) × three nemeses (network partitions, hard
kills, chaos — SIGSTOP mid-flight), 60-second windows with Knossos
linearizability and timeline checking, a fresh lab per combo, on both
x64 and arm64 Linux. The full matrix runs on every push and every
pull request: linearizability violations are identified and resolved
in development, not in production. It has done exactly that — the
matrix found and drove the fixes for a real write-stranding liveness
bug in the meta→shard queue-declare path and a write-budget overshoot
against frozen peers, and no linearizability violation has ever been
detected in any run.

**Bench sweeps** — 1/3/5/7-node clusters through every scenario on
both architectures: the throughput/latency regression surface.

**Soak** — a k3s lab churns a cluster between 1 and 9 nodes for hours
under load with a zero-error bar; a smoke variant runs in-suite on
every push.

**Cross-platform, both architectures** — the same suites run on:

|              | amd64                                        | arm64                                        |
| ------------ | -------------------------------------------- | -------------------------------------------- |
| Linux        | rust tests · language harness · jepsen · bench | rust tests · language harness · jepsen · bench |
| Windows      | rust tests · language harness                 | rust tests                                    |
| macOS        | — (a)                                        | rust tests · language harness                 |

(a) GitHub retired its last Intel macOS runners in 2025; macOS x86_64
is not coverable in CI.

The matrix is not ceremony: Windows' default timer resolution is
15.6 ms — too coarse for the sub-15 ms raft heartbeats a 50× warp
produces — so the Windows suite runs at identity time-scale; Linux
and macOS run at 50×. Jepsen and bench require Linux containers with
`NET_ADMIN`, so they run only there.

**Supply-chain & hygiene gates** — every push is also held to
`cargo audit` (0 known vulnerabilities), zero-warning
`cargo check --workspace --all-targets`, enforced `rustfmt`, and the
repository's code-scanning analyzers.
