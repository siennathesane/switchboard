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

This technology was designed by me, but built entirely from scratch by AI using only the AMQP 0-9-1 specification documentation and an architecture requirement I pulled from my own previous work with Raft. While this is production-ready, and I will be using it for my own production projects, this project is an exploration and experimentation of internet-grade technology not built by humans. I have code-reviewed all of this, and I think the architecture meets my personal requirements, but some people feel certain ways about AI-generated code.

In the init.jsonl.parts folder is the entire initial session history, including my original prompt & some steering prompts, up to the point where I maxed out long-horizon capabilities - run `cat init.jsonl.parts/part-* > init.jsonl` to reconstruct. When properly managed, AI is a tool for usefulness, not a replacement for human expertise. This codebase is a testament to the power and potential of AI in building reliable, scalable, and internet-grade technology when properly managed by a human expert.

I will be maintaining this project and adding features as I see fit, but I am not planning to make any major changes to the architecture. Any fixes moving forward will be small, incremental improvements, bug fixes, and performance improvements. I will not be providing distribution-ready packages of any format as my needs of this project will be different than yours. You best know your needs, and whether that's Docker, Kuberentes, Nomad, Systemd, SysV, or a custom build, implementing your own needs is the best way to maintain human expertise in your own projects. This is a single-binary, single-library project, with a C ABI; integration into any part of your stack is easy.

And in finality, this is a love letter to @rabbitmq 🐇 RabbitMQ was the message broker that taught me distributed systems design, and this project is a tribute to its legacy.

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
switchboard-store     RocksDB + OpenRaft storage
switchboard-cluster   multi-raft node, internal RPC, forwarding, membership
switchboard-server    client connections, channels, TLS
switchboard           the broker binary
```

## Testing

```bash
cargo test --workspace
```

includes end-to-end tests that speak raw AMQP over TCP against an
in-process broker (handshake, routing, confirms, transactions, requeue),
a byte-level conformance test of the whole method table against the
normative registry, and unit suites for every state machine.
