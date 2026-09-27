# Switchboard Multi-Language Conformance Harness

Black-box correctness tests that drive a **real Switchboard cluster**
(real broker processes, real network) with **genuine ecosystem clients**
in five languages — no test doubles, no in-process shims. The harness
found and fixed four real broker bugs that in-process tests could not
see (see "Bugs this harness caught" below).

## Layout

| Path | What |
|---|---|
| `support.py` | Broker lifecycle: boot N-node cluster, readiness gates, scale up/down, restart |
| `run.py` | Orchestrator: boots a 3-node cluster, runs every suite, plus an orchestrator-level durability/restart check; prints the summary matrix |
| `scale_test.py` | Sustained load **during scale up (3→6) and crash-down (6→3 by SIGKILL)** with zero-loss accounting |
| `clients/python/amqp091_test.py` | pika — full AMQP 0-9-1 matrix (32 checks) |
| `clients/python/mqtt_test.py` | paho-mqtt — MQTT 3.1.1 (QoS 0/1/2 both directions, retained, wildcards, persistent sessions) |
| `clients/python/stomp_test.py` | stomp.py — STOMP 1.2 (subscribe/send, receipts, topics, client-ack, cross-node, ERROR frames) |
| `clients/go/amqp091_suite.go` | amqp091-go — full matrix (16 checks) |
| `clients/node/amqp091_test.mjs` | amqplib — full matrix (11 checks) |
| `clients/ruby/amqp091_test.rb` | bunny — core matrix (10 checks) |

## Running

```sh
python3 -m venv harness-venv
harness-venv/bin/pip install pika paho-mqtt stomp.py

# toolchain clients (once):
(cd harness/clients/go   && go get github.com/rabbitmq/amqp091-go)
(cd harness/clients/node && npm install amqplib)
gem install bunny --user-install

# everything (builds the broker, boots a 3-node cluster, runs all suites):
harness-venv/bin/python harness/run.py

# subset:
harness-venv/bin/python harness/run.py --only python/amqp091,go/amqp091

# load + scale stability (multi-language traffic across scale events):
harness-venv/bin/python harness/scale_test.py --seconds-per-phase 20
```

## Feature matrix (per AMQP 0-9-1 suite)

- connection handshake, channel open, bad-credentials rejection
- exchange declare/delete/passive (direct, fanout, topic, headers)
- queue declare/passive/arguments (`x-message-ttl`, `x-dead-letter-*`), delete
- bind/unbind; topic wildcards `*`/`#`; fanout fan-out; headers `x-match`
- exchange-to-exchange bindings
- publish→basic.get roundtrip (body, routing key, properties, delivery mode)
- consume push + ack; reject(requeue) redelivery; nack → DLX dead-lettering
- basic.qos prefetch windows; publisher confirms; tx commit/rollback
- **cross-node**: publish on node N, consume on node M (every suite)
- **durability**: durable queue + persistent message survive a full cluster
  restart (orchestrator check)

## Verdict semantics (scale test)

Each publisher tags every confirmed message with a per-publisher
sequence number. The gate is *no holes above each tag's first delivered
sequence*: holes would mean an acknowledged message vanished. The
unconfirmed tail at shutdown is bounded in-flight, not loss. Clients
retry transient connection errors during scale events — a client crash
fails the test.

## Bugs this harness caught (all fixed)

1. **Full-cluster restart lost raft membership** — the state machine's
   `applied_state` reported a default (empty) membership, so a restarted
   group had no voters and served nothing. Membership is now persisted
   per group and restored.
2. **Publisher-confirm numbers shared the delivery-tag counter** — after
   any basic.get/consumer delivery, confirms numbered 3,4,… while
   clients expected 1,2,… and orphaned their pending-confirm tracking.
   Confirms now use a dedicated per-channel sequence.
3. **Empty vhost rejected** — every client library maps an amqp URI with
   no vhost to `"/"`; the broker rejected `""`. Now mapped.
4. **Stale-topology routing loss** — a node whose cached topology
   predated a just-replicated queue declaration dropped routed messages
   as "unroutable". Both the effect-path and client-publish routers now
   refresh once and retry when a route comes up empty.

The qos/DLX flakes observed during development were these routing drops
plus the confirm-sequence bug — after the fixes, repeated suite trials
(6+ consecutive) run clean.
