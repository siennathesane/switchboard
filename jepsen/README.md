# Switchboard Jepsen Tests

Black-box distributed-systems tests for Switchboard (multi-master AMQP
0-9-1 broker, per-queue shard raft groups, OpenRaft + RocksDB), run with
[jepsen 0.3.14](https://github.com/jepsen-io/jepsen) against a real
5-node cluster in Docker.

## What "perfectly ordered" means here, and where it should hold

Switchboard's architecture makes specific ordering claims, and the tests
map onto them one-to-one:

| Test | Property | Where it comes from |
|---|---|---|
| `fifo` | Per-queue FIFO: first deliveries of one publisher's messages arrive in publish order, confirmed publishes are never lost, and repeat deliveries are always flagged `redelivered` | All enqueues and hand-outs for a queue serialize through that queue's single shard raft group |
| `fanout` | Total-order broadcast: N queues bound to a fanout exchange observe confirmed messages in the **same order** | NOT architecturally guaranteed — each destination queue's enqueue is an independent raft write in a different shard group, so concurrent publishes can interleave differently per group. This test measures whether the system is *in fact* totally ordered, and produces counterexamples when it is not. |
| `topo` | Linearizability of queue existence (declare / delete / passive-declare) | Topology is replicated through the meta raft group; synchronous replies pass through raft |

Fault injection (`jepsen.nemesis.combined`):

* `--nemesis parts` — network partitions (single-node isolation, clean
  majority/minority splits, majorities ring) via iptables.
* `--nemesis kill` — SIGKILL of random broker processes (containers stay
  up; the raft data dir is preserved and the process restarts), plus
  SIGSTOP/SIGCONT pauses.
* `--nemesis chaos` — both at once.
* `--nemesis none` — quiescent baseline.

## Test design notes

* **Publisher confirms are the acknowledgment backbone.** A publish op
  returns `:ok` only when the broker confirmed the message (quorum-applied
  on *every* routed queue); timeouts and channel errors are `:info`.
  Loss/anomaly checks apply to confirmed messages only, so in-flight
  ambiguity never produces false positives.
* **One poller per queue, `basic.get` + ack.** A single sequential
  observer per queue makes the delivery stream an authoritative order;
  competing consumers would make cross-node observation order meaningless.
  Drains end with a passive-declare `:depth` op so the checker can tell
  "queue empty" from "drain incomplete" (`:valid? :unknown` rather than a
  false loss claim).
* **At-least-once discipline.** A message delivered twice is legal only if
  the repeat carried `redelivered = true` (failover/requeue). An
  unauthorized duplicate is a violation.
* **The topo register** uses `queue.declare` (unconditional write),
  `queue.delete` (404 when absent = definitive negative result) and
  passive declare (404 = definitive absent read), checked with Knossos via
  the `ExReg` model.

## Layout

| Path | What |
|---|---|
| `src/jepsen/switchboard.clj` | CLI entry point |
| `src/jepsen/switchboard/tests.clj` | The three workloads (roles, generators, checkers) |
| `src/jepsen/switchboard/client.clj` | AMQP 0-9-1 client (confirms, gets, register ops) |
| `src/jepsen/switchboard/amqp.clj` | RabbitMQ Java-client interop wrapper |
| `src/jepsen/switchboard/checkers.clj` | FIFO / fanout-order checkers + `ExReg` model |
| `src/jepsen/switchboard/db.clj` | Broker process control over SSH (start/kill/pause) |
| `docker/` | Lab orchestration (node + control images, up/down/run scripts) |

## Running

Prereqs: Docker CLI pointed at a Linux daemon (`DOCKER_HOST`; the sunbeam
workspace `.envrc` exports one), `direnv allow` or manual exports.

```sh
# one-time: generate the lab SSH key
mkdir -p docker/keys && ssh-keygen -t ed25519 -N '' -f docker/keys/id_ed25519

# build the node image (compiles switchboard for linux/amd64; ~10 min first time)
./docker/build-node-image.sh

# boot: 5 node containers + control image
./docker/up.sh 5

# run tests (each run copies results to jepsen/store/)
./docker/run.sh --test fifo   --nemesis parts --time-limit 60
./docker/run.sh --test fanout --nemesis parts --time-limit 60
./docker/run.sh --test topo   --nemesis chaos --time-limit 60
./docker/run.sh --test all    --nemesis chaos --time-limit 60

# tear down
./docker/down.sh
```

Useful flags: `--time-limit`, `--nemesis none|parts|kill|chaos`,
`--interval`, `--publish-rate`, `--rate`, `--drain-limit`,
`--fanout-queues`, `--fanout-publishers`, `--nodes sb1,sb2,...`.

Results land in `store/<test>/<timestamp>/` (history, checker output,
`results.edn`, broker logs, timeline HTML).

## Known test limitations

* One poller per queue (sequential `basic.get`) — competing-consumer
  semantics (fairness, prefetch across consumers) are out of scope here;
  the conformance harness covers functional behavior.
* Publishers use per-channel confirm sequences embedded in message bodies;
  a channel recreation bumps a generation counter in the pid, so history
  analysis treats each channel's stream independently.
* `immediate`/`mandatory` publish paths, transactions, and MQTT/STOMP/AMQP
  1.0 gateways are untested here — same-core, but different code paths.


## Known broker behaviors the tests work around (not hide)

* **Concurrent declare race.** A re-declare that lands after the creating
  node's meta write but before its shard-side `CreateQueueData` write gets
  a 404 from the shard and its channel is closed. Clients treat this as
  benign and retry (the queue *is* present — the meta write landed); the
  race itself is worth a closer look upstream.
* **Stale-topology publishes.** A publish on a node whose local meta
  snapshot predates a queue declaration routes as unroutable and is
  *confirmed* (non-mandatory). Setups warm up routing and wait for the
  500 ms reconciliation tick before load starts, so steady-state runs
  don't observe this — but a mandatory-publish + confirm interaction is
  an interesting future test.
