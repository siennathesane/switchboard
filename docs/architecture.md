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
| `switchboard-embedded` | The broker as a library: one in-process `ClusterNode` plus a typed AMQP 0-9-1 client over an in-memory duplex into the unmodified session core (see "Embedded library" below). |
| `switchboard-capi` | A stable C ABI over `switchboard-embedded` (`sb_`-prefixed symbols, header at `crates/switchboard-capi/include/switchboard.h`), so non-Rust programs can embed the broker in-process (see "C ABI" below). |
| `switchboard` | The binary + CLI (including the offline `backup` / `restore` store tools). |
| `switchboard-bench` | Benchmark + soak library and drivers (not published). |

## Cluster layout

* **Group 0 — the meta group.** Replicates the control plane
  (`switchboard_core::topology::MetaState`): vhosts, users, exchanges,
  queue metadata, bindings, the node directory, and the shard map. Voters:
  the first `min(3, N)` registered nodes. The voter set is *not* frozen
  there: the membership controller re-votes the meta group through the
  same `ReconfigureGroup` machinery as shard groups as nodes come and go
  (`switchboard-cluster/src/node/mod.rs`, the "grow/shrink meta
  membership" step), keeping 3 voters once ≥3 nodes are registered — at
  exactly 2 registered the second node stays a learner, because a
  2-voter meta group loses quorum permanently on one unclean loss. If
  the meta group is ever leaderless past a 30 s window (quorum lost in
  one churn event), the smallest registered node rebuilds the group from
  its applied state (`meta_recovery_loop`).
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

Count-based prefetch (`basic.qos` `prefetch_count`) is the credit
window itself. Byte-based prefetch (`prefetch_size`) is enforced too:
each subscription carries a `byte_limit`, and hand-out pauses once the
bytes of unacknowledged held messages reach it
(`switchboard-core/src/shard/mod.rs`, subscription `byte_limit` /
`held_bytes`).

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

## Queue policies: priority, TTL, dead-lettering

Queue declare arguments (`switchboard_core::shard::QueuePolicy`, parsed in
`crates/switchboard-core/src/shard/mod.rs`) turn on the server-extension
behaviors the base spec leaves open:

* **Priority.** The `basic` `priority` property orders delivery
  best-first (priority descending, FIFO within a priority). The shard's
  ready index is keyed by `(priority, sequence)` so the next message for
  a queue is an O(log n) lookup (`QueueData::best_ready` /
  `ShardState::next_ready` in `shard/mod.rs`).
* **Message TTL.** A queue-level `x-message-ttl` (ms) is the default for
  messages without their own; a per-message `expiration` property
  overrides it per message (applied at enqueue, `shard/mod.rs` — the
  queue TTL "covers messages without their own"). Expired *ready*
  messages are dropped by the shard's expiry pass via an
  earliest-deadline-first index and counted in the `expired` lifecycle
  metric.
* **Dead-lettering.** `x-dead-letter-exchange` (and optional
  `x-dead-letter-routing-key`, default: the original key) republishes
  messages to a DLX instead of dropping them — both expired messages and
  messages nacked/rejected with `requeue=false`
  (`ShardQueueData::dead_letter` and the dead-letter target resolution in
  `shard/mod.rs`). The routing integration is covered end-to-end by
  `e2e_admin_ops.rs` (priority delivery order, message TTL expiry → DLQ,
  nack dead-lettering).

## Embedded library

`switchboard-embedded` (crates/switchboard-embedded) is the broker as a
library: a [`Broker`](crates/switchboard-embedded/src/broker.rs) wraps an
unmodified in-process `switchboard_cluster::ClusterNode` (raft-replicated
topology and queues over RocksDB, single-node by default), and
`Broker::connect` returns a typed AMQP 0-9-1 client whose traffic flows
through an in-memory duplex into the exact `serve_rw` session core the TCP
gateways drive — same handshake, same argon2 authentication including the
guest-loopback rule, same channel state machines. There is no bypass flag
and no divergence from wire clients.

```rust
use switchboard_embedded::{Broker, BrokerConfig, QueueOptions};

let broker = Broker::start(BrokerConfig::new(data_dir)).await?;
let conn = broker.connect("guest", "guest").await?;
let channel = conn.open_channel().await?;
channel.queue_declare("tasks", &QueueOptions::new().durable(true)).await?;
channel.publish("", "tasks", Default::default(), b"hello".to_vec()).await?;
let delivery = channel.get("tasks", true).await?.expect("message");
assert_eq!(delivery.body, b"hello");
conn.close().await;
broker.stop(std::time::Duration::from_secs(5)).await?;
```

The typed channel API covers declares/binds/deletes, `publish`,
`publish_mandatory`, `publish_confirmed`, `get`/`ack`/`nack`, `qos`,
`consume` (returns a `Consumer` stream with async `next()`), and
`tx_select`/`tx_commit`/`tx_rollback`. Broker-side administration
(`declare_vhost`, `create_user`, `delete_user`, `set_permissions`) is on
the `Broker` itself.

Runtime contract: everything requires a live tokio runtime; restart is a
new process on the same `data_dir` (the broker holds the RocksDB
exclusive lock for its whole lifetime, so a second `Broker::start` on a
live directory fails with `StartError::Storage`); `Broker::stop(grace)`
closes every tracked connection, then leaves the cluster. Authorization
parity with the wire path is inherited including its gaps — see "Known
limitations" for the exchange.bind/unbind gate still open.

## C ABI over the embedded library

`switchboard-capi` (crates/switchboard-capi) exposes a stable C ABI over
`switchboard-embedded`, so non-Rust programs (Python via ctypes, Go via
cgo, C/C++, …) embed the same broker in-process. The header lives at
`crates/switchboard-capi/include/switchboard.h`; build the cdylib with
`cargo build -p switchboard-capi`.

Shape of the ABI (full contract in the crate docs,
`crates/switchboard-capi/src/lib.rs`):

* Every exported symbol starts `sb_`; every entry point returns an
  `sb_status` whose numeric values are frozen for 1.0 (`SB_OK` = 0).
* Failures leave a message on `sb_last_error()` — thread-local, valid
  until the next `sb_*` call on that thread.
* Every entry point runs under `catch_unwind`: no Rust panic crosses the
  boundary; a caught panic surfaces as `SB_ERR_PANIC`.
* Message bodies travel as explicit `(ptr, len)` pairs; names and
  credentials are NUL-terminated UTF-8 strings. Returned strings and
  messages are owned by the caller and released with `sb_string_free` /
  `sb_message_free`; every handle has exactly one matching free.
  Freed-then-used and alive-but-gone handles are detected errors
  (`SB_ERR_INVALID_HANDLE` / `SB_ERR_HANDLE_DEAD`), never undefined
  behavior — a global handle table backs every check.
* Consumption is polling only (`sb_channel_get`, or
  `sb_channel_consume` + `sb_consumer_next` with a timeout). There is
  deliberately no callback-with-user-data flavor, so no reentrancy or
  thread-affinity contract exists.
* Every call is synchronous — it blocks the calling OS thread on the
  broker's private multi-thread tokio runtime. Distinct handles may be
  used from distinct threads; do not call `sb_*` from inside a running
  async context.

Lifecycle: `sb_config_create` → (`sb_config_set_*`) → `sb_broker_start`
→ `sb_broker_connect` → `sb_client_open_channel` → ops →
`sb_client_close` → `sb_broker_stop` → frees. As with the Rust library,
one live broker per `data_dir` per process (the RocksDB lock); restart
means a new process. The embedded caveats carry over verbatim — e.g.
`sb_config_set_janitor_interval_ms` widens the janitor tick away from
the prepared-transaction race documented under "Known limitations".

`examples/python/embedded_demo.py` is a standard-library-only (ctypes)
demo driving the whole lifecycle — start, connect, declare, publish,
consume, a transaction commit and rollback, and a clean shutdown:

```bash
cargo build -p switchboard-capi
python3 examples/python/embedded_demo.py
```

```python
lib = ctypes.CDLL(find_cdylib())                 # target/{debug,release}
config = ctypes.c_void_p()
lib.sb_config_create(data_dir.encode(), ctypes.byref(config))
lib.sb_config_set_janitor_interval_ms(config, 60_000)
broker = ctypes.c_void_p()
lib.sb_broker_start(config, ctypes.byref(broker))
lib.sb_broker_wait_ready(broker, 10_000)
client = ctypes.c_void_p()
lib.sb_broker_connect(broker, b"guest", b"guest", ctypes.byref(client))
# ... open a channel, declare/publish/get/ack, tx_select/commit ...
lib.sb_broker_stop(broker, 10_000)
```

## Admin HTTP API

The gateway routes `/api/…` to a JSON-over-HTTP admin API
(`crates/switchboard-server/src/protocols/admin.rs`), authenticated with
HTTP basic auth against the same argon2 credential check every protocol
gateway uses. Authorization mirrors RabbitMQ's management model: the
`guest` user and any unrestricted user (no permissions-table entry) may
call it; a user *restricted* by any permissions entry is refused with 403
— the control plane belongs to administrators. Unknown
method/path combinations answer 405 rather than improvising.

| route | behavior |
|---|---|
| `GET /api/users` | list users with their permissions |
| `GET /api/users/{name}` | one user, or 404 |
| `PUT /api/users/{name}` `{"password": …}` | create user or rotate password |
| `DELETE /api/users/{name}` | delete user (cascades its permissions) |
| `GET /api/vhosts` | list vhosts with queue/exchange/binding counts |
| `PUT /api/vhosts/{name}` | declare vhost (idempotent) |
| `DELETE /api/vhosts/{name}` | 405 — no meta command exists |
| `GET /api/permissions` | list permission entries |
| `GET /api/permissions/{vhost}/{user}` | one entry, or 404 |
| `PUT /api/permissions/{vhost}/{user}` | set the configure/write/read regex triple |
| `DELETE /api/permissions/{vhost}/{user}` | clear (all-empty clears) |
| `GET /api/queues`, `GET /api/exchanges` | topology listings |

Every mutating route maps onto a meta state-machine command
(`CreateUser`, `DeleteUser`, `DeclareVhost`, `SetPermissions`), so admin
changes are raft-replicated and immediately visible to every gateway.

## Prometheus metrics

`GET /metrics` serves Prometheus text exposition (version 0.0.4,
hand-rolled — no new dependencies; `crates/switchboard-server/src/protocols/metrics.rs`).
It rides the same `http` protocol flag as `/health` and `/stats` and
needs no authentication. Exported:

* `switchboard_messages_{enqueued,handed_out,acked,noack_removed,released,purged,expired,deleted}_total`
  — the shard state-machine message-lifecycle counters (the same
  accounting `/stats` trends);
* `switchboard_queue_ready_messages{queue}` /
  `switchboard_queue_held_messages{queue}` — per-queue depth gauges;
* `switchboard_connections{protocol}` — per-protocol connection gauges;
* node vitals (same sources as `/stats`).

## Backup & restore

`switchboard backup` / `switchboard restore`
(`crates/switchboard-store/src/backup/`, CLI wiring in
`crates/switchboard/src/backup_cmd.rs`) produce a consistent, versioned,
single-file archive of a node's entire durable broker state and restore
it into a fresh data directory.

* **What is captured.** A node keeps all durable state in one RocksDB
  instance — meta raft state, every shard group's raft log and state
  machine, and the per-message journal the shard state machine writes
  alongside every queue mutation. One atomic store snapshot therefore
  covers everything, logs included.
* **Consistency point.** The tool opens the data dir with the same
  `RocksKv::open` call the broker uses; RocksDB's exclusive lock means a
  *running* broker cannot be backed up (the open fails with a
  "stop the broker first" hint). On a quiesced store the snapshot is
  materialized with RocksDB's Checkpoint API, then archived — a
  complete, directly-openable staging directory exists before a single
  byte of archive is written.
* **The storage version gate.** The archive is `"SWBK"` magic + a `u32`
  LE format version + a JSON header (file list with lengths and sha1
  digests) + file records. This build writes and accepts exactly
  `BACKUP_FORMAT_VERSION = 1` (`backup/mod.rs`); restore validates magic
  and version *before* touching the target directory and refuses a newer
  (`VersionTooNew`), unknown/older (`VersionUnknown`), or non-archive
  (`NotABackup`) file with a clear error. Bump the constant only with a
  deliberate format change.
* **Restore.** `switchboard restore <archive> --data <new-dir>` requires
  a fresh (absent or empty) directory — a restore never clobbers — and
  reproduces the store exactly; the node opens it like any data dir after
  a graceful stop and raft resumes from the captured vote/log/applied
  state.
* **Multi-node caveat.** An archive carries one member's view; recover a
  cluster by quiescing every node and restoring members from the same
  instant, or restore one member and let raft re-form around it. TLS key
  material and environment are not part of the archive.

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
| `GET /stats` · `GET /metrics` | HTTP | one-line JSON resource snapshot (the soak monitor's feed); Prometheus text exposition (`text/plain; version=0.0.4`, hand-rolled): message-lifecycle counters, per-queue depth, per-protocol connection gauges, node vitals |
| `/api/…` | HTTP | admin API, JSON over HTTP basic auth (same argon2 credential check as every protocol gateway): users/vhosts/permissions CRUD mapped onto meta state-machine commands, plus queue/exchange listings; restricted users (any permissions-table entry) are refused — the control plane belongs to unrestricted users, mirroring RabbitMQ's administrator-only management API |

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

* **`immediate` publishes check only the first destination queue.** The
  `immediate` arm in `crates/switchboard-server/src/methods/publish.rs`
  consults an active-consumer `Stats` probe on `destinations[0]` alone —
  if that queue has a consumer, the message is delivered even when other
  destination queues have none.
* **`exchange.bind` / `exchange.unbind` are not authorization-gated.**
  Every other topology operation checks `configure`/`write`/`read`
  permissions via `authorize_ch` (declares, deletes, queue binds and
  unbinds, purge, publish, get, consume), but the `ExchangeBind` and
  `ExchangeUnbind` arms in
  `crates/switchboard-server/src/methods/mod.rs` issue the meta bind /
  unbind without a permission check. It is on the `fix(broker):`
  hardening track; embedded clients inherit the gap with everything
  else.
* **Prepared transactions race the janitor.** The shard's
  `ExpirePrepared` sweep drops prepared transactions older than
  `tx_timeout_ticks` ticks — a field that defaults to 0
  (`crates/switchboard-core/src/shard/mod.rs`, `ShardState` derives
  `Default`) — so a `tx.commit` that arrives after an intervening janitor
  tick can find its prepared ops silently gone while `commit-ok` still
  returns. Brokers/embedders running transactions should widen
  `janitor_interval`/`tx_timeout_ticks` until this is closed.

Formerly-listed limitations that no longer hold: priority queues, message
TTL, and dead-lettering are implemented (see "Queue policies" above);
byte-level `prefetch_size` is enforced (see "Delivery protocol"); the
meta group's voter set is re-voted automatically by the membership
controller with leaderless self-healing (see "Cluster layout").
