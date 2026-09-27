# Switchboard Architecture

Switchboard is a multi-master AMQP 0-9-1 broker. Any node in the cluster
accepts any operation — connections, topology changes, publishes,
consumes — and the cluster replicates state through multiple small raft
groups (never more than three voters each) built on
[OpenRaft](https://github.com/databendlabs/openraft) with RocksDB storage.

## Crate layout

| crate | responsibility |
|---|---|
| `switchboard-wire` | AMQP 0-9-1 codec: data types, field tables, all methods, frames. Verified byte-for-byte against the normative method registry (`tests/registry_conformance.rs`). |
| `switchboard-core` | The AMQ model as pure state machines: topology (meta), queues (shard), topic matching, routing, auth. No I/O. |
| `switchboard-store` | RocksDB KV + OpenRaft `RaftLogStorage` / `RaftStateMachine` (feature `storage-v2`). |
| `switchboard-cluster` | Multi-raft node: raft lifecycle, internal RPC protocol (TCP + optional TLS via rustls/aws-lc-rs), write forwarding, join protocol, shard-map formation. |
| `switchboard-server` | Client-facing AMQP: TCP/TLS listeners, connection handshake, per-channel state machines, heartbeats, transactions, publisher confirms. |
| `switchboard` | The binary + CLI. |

## Cluster layout

* **Group 0 — the meta group.** Replicates the control plane
  (`switchboard_core::topology::MetaState`): vhosts, users, exchanges,
  queue metadata, bindings, the node directory, and the shard map. Voters:
  the first `min(3, N)` registered nodes.
* **Groups 1..N — shard groups.** Each owns the message data of the queues
  assigned to it. Assignment is a stable FNV-1a hash of the queue name over
  the sorted group ids, computed inside the replicated meta state machine
  (deterministic across replicas). Group *membership* at formation is a
  sliding window over sorted node ids: for N ≥ 3 nodes there are N groups
  of 3; each node sits in 3 groups; each group tolerates 1 voter failure.
  With 1–2 nodes a single group holds everyone.
* Every group has **at most 3 voters** — enforced by construction
  (`MAX_VOTERS`).

## Write path (multi-master)

Any node accepts any command:

1. If the command targets a group the node hosts, it goes through the
   local raft instance (`client_write`). openraft returns
   `ForwardToLeader` when the node is a follower; the node re-sends to the
   leader.
2. Otherwise the command is forwarded over the internal network to a
   member of the target group, which applies rule 1. One hop suffices: the
   receiving member either is the leader or knows who is.

Topology reads (routing, queue lookup) use the node's local applied meta
snapshot, refreshed immediately after each of its own meta writes and by a
reconciliation loop (500 ms) otherwise. Synchronous replies (§4.4
visibility) always pass through raft, so a client that observed a
successful reply will never see the object vanish on a subsequent
linearizable operation.

## Delivery protocol (credit-based pull)

Delivery is pulled by the node hosting the consumer so channel prefetch
windows (§3.1.7) stay with the channel, not the shard:

1. `RegisterSubscription` (raft command on the owning shard) records the
   consumer.
2. The node grants `Credit { sub, count }`; the shard state machine hands
   out ready messages — marking them unacked, or deleting them for
   `no_ack` consumers — and emits `MessageReady` effects carrying the full
   message.
3. The shard leader ships effects to the consumer's node over the internal
   network; that node writes `basic.deliver` frames.
4. Acks return as `Ack` (+ credit refill when the window reopens).
   Reject/nack/requeue/recover/cancel/close send `Release`, returning
   messages to *ready*; the next hand-out sets `redelivered`.

Both the message and its unacked marker live in the raft log, so leader
failover preserves them exactly — at-least-once delivery with no loss of
acknowledged messages.

## Transactions (§2.2.9)

Channels buffer publishes and acks. On commit, the node coordinates a
two-phase commit across the involved shards: `PrepareTx` stores the intent
per shard, then `CommitTx` applies everywhere (idempotent; queues deleted
mid-transaction drop their prepared ops). Rollback is local: buffered
publishes were never sent and buffered acks simply leave their messages
unacked — exactly §2.2.9's semantics. Abandoned prepared transactions
expire by a janitor sweep (`ExpirePrepared`).

## Publisher confirms

Confirm mode sequences publishes per channel; the confirm for a publish is
sent once every routed queue's enqueue has been applied by its shard raft
group (quorum-committed). Unroutable mandatory messages are returned with
`basic.return` and also confirmed.

## Internal management protocol

The internal port carries framed bincode envelopes (4-byte length prefix),
optionally wrapped in TLS (rustls + aws-lc-sys):

* `Raft { group, payload }` — openraft RPCs (vote / append-entries /
  install-snapshot) between members of a group.
* `Forward { group, command }` — apply-on-leader requests (the write path
  above).
* `AdminRequest`:
  * `Join` — the join protocol. A new node sends `Join` to each seed; the
    seed registers it in meta (raft-replicated) and returns the node
    directory, which the joiner adopts so it can reach every peer.
  * `Topology` — a peer's local applied meta state; used by nodes that do
    not host the meta group to fill their routing view.
  * `Deliver` / `CancelConsumer` — the consumer data path from shard
    leaders to consumer-hosting nodes.
* Formation: the bootstrap node initializes meta alone, applies the
  bootstrap entities (default vhost, exchanges, user), and — once
  `expected_nodes` nodes have registered — installs the shard layout.
  Each shard group is initialized by its smallest member to keep the race
  deterministic; openraft resolves identical concurrent initializes.
* `ReconfigureGroup { group, voters, forwarded }` — the scale-up/down
  convergence step (below). The receiver adds missing nodes as learners
  and replaces the voter set, on its group's leader (`forwarded` bounds
  the leader hop to one).

## Scaling (auto-scale up and down)

The meta group's voter set is fixed at formation (the first
`min(3, expected_nodes)` node ids). Shard groups, however, reconfigure
automatically as nodes come and go; every step is driven by the
*membership controller* — a loop that runs on the meta leader:

* **Join (scale up).** The joiner registers through a seed (`Join`), the
  controller notices an uncovered node and anchors one new shard group on
  it, backed by two established members (deterministic round-robin keyed
  by the new group id). Existing groups are frozen, so a queue's
  group assignment never moves; capacity extends. The new group's raft is
  initialized by its smallest member as usual.
* **Leave (scale down).** The departing node announces `Forgetter` (its
  directory entry is removed; group member lists are left for the
  controller) and stops its raft cores. The controller heals every group
  that lost a member: it drops the departed node and *refills* the group
  back to 3 members from the live pool (`SetGroups`), then drives each
  affected group's raft to the new voter set via `ReconfigureGroup`
  (learner catch-up, then `ReplaceAllVoters`). Groups never lose strength,
  so writes keep landing everywhere — the 9→3 departure sequence is
  covered end-to-end by `e2e_autoscale_down.rs`.
* **Forwarding safety.** A forwarded write is answered one-shot by its
  receiver (transient = "no leader yet", authoritative = broker reply);
  only the originating node retries and re-routes. Members never chain
  forwards, so stale mutual leader hints cannot ping-pong a request, and
  a leaderless group simply waits out its election inside the writer's
  30 s budget.

## Client protocol gateway

One client port carries every supported protocol. The gateway sniffs the
first bytes of each connection (after TLS terminates, if configured) and
routes accordingly:

| first bytes | protocol | notes |
|---|---|---|
| `AMQP\0\0\9\1` | AMQP 0-9-1 | the native session (this crate's primary path) |
| `AMQP\0\1\0\0` | AMQP 1.0 | SASL PLAIN/ANONYMOUS, open/begin/attach, transfers in both directions with per-link credit, **unsettled deliveries** (snd-settle-mode honored: `accepted` acks, `released`/`rejected` requeue-redeliver); STOMP-style address mapping (`/topic/`, `/queue/`, `/exchange/`, anonymous relay via message `to`) |
| `0x10 …` (CONNECT) | MQTT 3.1.1 | publishes → `amq.topic` (full QoS 0/1/**2** with PUBREC/PUBREL/PUBCOMP and per-packet-id dedupe); `SUBSCRIBE` → private `amq.gen-…` queue bound with `+`→`*`, `#`→`#`; **retained messages replicated through meta**; **persistent sessions** (durable per-client session queues survive offline periods); keepalive enforced |
| `STOMP\n` / `CONNECT\n` | STOMP 1.0–1.2 | `/topic/`, `/queue/`, `/exchange/`, `/amq/queue/` destinations; `ack:auto|client|client-individual`; **server-side transactions via shard 2PC** (`BEGIN`/`COMMIT`/`ABORT`); heart-beats |
| `GET …` + upgrade | WebSocket (RFC 6455) | inner protocol by `Sec-WebSocket-Protocol` (`mqtt`/`stomp`/`amqp`), by path (`/mqtt`, `/stomp`, `/amqp10`), or by sniffing the first frame |
| `GET /health` | HTTP | tiny JSON health probe for load balancers; other paths get 404 |

Every protocol rides the same broker core (topology, shard groups,
forwarding), so an MQTT publish and an AMQP publish to the same topic
exchange meet in the same queue, and any protocol's consumer receives
either. Protocol selection is configurable via `--protocols`.

## Self-discovery (DNS + mDNS)

Nodes find each other without static peer lists. All sources feed the
same join protocol (`ClusterNode::introduce` → `AdminRequest::Join`),
with per-address backoff; introduced nodes register in meta and the
membership controller extends the shard layout to cover them (see
"Scaling").

* **DNS A/AAAA seeds** (`--dns-seed host[:port]`): a hostname whose
  address records list member internal addresses; resolved with the
  system resolver and re-resolved every tick (default 30 s).
* **DNS SRV** (`--dns-srv <domain>`): `_switchboard._tcp.<domain>` SRV
  records (one per node: target + port); resolved via hickory-resolver.
* **mDNS** (`--mdns`): advertises `_switchboard._tcp.local.` with TXT
  properties `id`, `internal`, `client`, and browses the same service —
  zero-configuration LAN self-assembly; `id` lets nodes skip themselves.
* A node started with neither `--bootstrap` nor `--seeds` is *pending*:
  it starts no raft groups of its own until discovery introduces it.

## Known limitations

* **Meta voter set is fixed at formation** (first `min(3, expected_nodes)`
  node ids); re-voting the meta group at runtime is admin-driven.
* **Byte-level prefetch (`prefetch_size`) is rejected** with 540, matching
  RabbitMQ; count-based prefetch is fully implemented.
* **`immediate` publishes** require an active consumer on the first
  destination queue.
* **Priority queues / TTL / dead-lettering** are not implemented (the spec
  marks these as server extensions).
