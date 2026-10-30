# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-rc.1] - 2026-10-30

First public release candidate: a multi-master AMQP 0-9-1 broker with
raft-replicated topology and queues, a protocol-sniffing client gateway,
an embedded-library distribution, and an operational surface covering
Prometheus metrics, an admin HTTP API, and versioned backup/restore.

### Added

- **Broker core.** Multi-master AMQP 0-9-1 broker: every node accepts
  every operation; state replicates through multiple small raft groups
  (at most three voters each, `MAX_VOTERS`) built on OpenRaft with
  RocksDB storage. Automatic shard-group scale-up on join and
  drop-and-refill healing on departure; the meta group's voter set is
  re-voted automatically as membership changes, with leaderless
  meta-quorum self-healing. Delivery is credit-based pull with
  at-least-once semantics; transactions use a two-phase commit across
  involved shards; publisher confirms fire once every routed queue's
  enqueue is quorum-committed.
- **Queue policies.** Priority delivery (best-first, FIFO within a
  priority), message TTL via queue-level `x-message-ttl` and per-message
  `expiration`, and dead-lettering via `x-dead-letter-exchange` /
  `x-dead-letter-routing-key` for expired and negatively-acknowledged
  messages.
- **Client gateway.** One port serves AMQP 0-9-1, AMQP 1.0 (settled and
  unsettled deliveries), MQTT 3.1.1 (QoS 0/1/2, replicated retained
  messages, persistent sessions), STOMP 1.0–1.2 (including server-side
  transactions), and WebSocket-wrapped variants, selected by sniffing
  the first bytes. Protocol selection is configurable via `--protocols`.
- **Self-discovery.** DNS A/AAAA seeds, DNS SRV records, and mDNS
  advertisement all feed the same join protocol, so nodes assemble
  without static peer lists.
- **Embedded library** (`switchboard-embedded`). The broker as a
  library: one in-process node plus a typed AMQP 0-9-1 client connected
  over an in-memory duplex into the unmodified session core — same
  handshake, authentication, and semantics as wire clients, no sockets
  on the client path. Includes `Broker` lifecycle (`start`/`stop`),
  typed channel operations, `Consumer` streams, and broker-side
  user/vhost/permission administration.
- **C ABI** (`switchboard-capi`). A stable C ABI over the embedded
  library (`sb_`-prefixed symbols, header at
  `crates/switchboard-capi/include/switchboard.h`): synchronous calls
  with frozen status codes, thread-local last-error messages, panic
  containment, polling-only consumption, and a handle table that turns
  double-free / use-after-free into detected errors. Ships with a
  standard-library-only Python ctypes demo
  (`examples/python/embedded_demo.py`) driving the full lifecycle.
- **Prometheus metrics.** `GET /metrics` serves text exposition 0.0.4:
  message-lifecycle counters, per-queue depth gauges, per-protocol
  connection gauges, and node vitals.
- **Admin HTTP API.** JSON over HTTP basic auth under `/api/…` with the
  same argon2 credential check as every protocol gateway: users,
  vhosts, and permissions CRUD mapped onto raft-replicated meta
  commands, plus queue/exchange listings. Restricted users are refused,
  mirroring RabbitMQ's administrator-only management API.
- **Backup/restore.** `switchboard backup` writes a consistent,
  single-file archive of a stopped broker's entire store (meta raft
  state, shard logs and state machines, message journal) via RocksDB
  checkpoints; `switchboard restore` validates the archive's storage
  format version before touching the target and refuses to clobber a
  non-empty directory.
- **Security model.** argon2 credential hashing with
  deployment-tunable parameters; a RabbitMQ-shaped permissions model
  (configure/write/read regexes per vhost) enforced across the protocol
  gateways on declare/delete/bind/purge/exchange operations, publish,
  get, and consume; the guest user is restricted to loopback
  connections.
- **CI.** GitHub Actions matrix running nextest, the language harness
  suites, the Jepsen-style tests, and the bench sweep across platforms.
- **Testing infrastructure.** Jepsen-style tests (nine-node scale and
  autoscale matrices, contended runs, slow-mo timers), a soak-test
  driver with smoke and fanout-regression coverage, a benchmark library
  with lab scripts, and k3s deployment assets for the bench/soak labs.

### Changed

- **Performance overhaul.** Ready/expiry indexes and a per-message
  storage journal in the shard state machine; per-message persistence,
  batched commands, and O(1) log-state reads in the store; write
  coalescing and notify-driven fanout with clone-free hot paths in the
  cluster; `TCP_NODELAY` at accept; mimalloc as the global allocator.
- Consumers with `no-ack` no longer consume delivery credit, and
  byte-level `prefetch_size` windows are enforced alongside count-based
  prefetch.

### Fixed

- **Cluster correctness.** Fanout confirms can no longer fire before
  every leg is visible; four known broker races closed; a lost
  CreateQueueData delivery drive healed (silent delivery stranding is
  now logged); broker restart no longer races the watchdog on stale
  pids; the soak reconcile gate checks membership, not count equality.
- **AMQP 1.0 transport.** §2.3 frame layout corrected; protocol errors
  close the connection gracefully; delivery credit is refunded for
  no-ack consumers.
- **Windows/portability.** Empty-string tempo export handled;
  identity-tempo main-suite runs on Windows; scale test can no longer
  hang forever in phase 2/6; cross-platform CI fixes from the first
  matrix runs.
- **Test harness robustness.** The Go QoS check redrives once on a
  purged queue and self-diagnoses on starvation; the consume push check
  redrives once on a starved stream; the contended runner is hardened
  against the failure modes surfaced by repeated lab runs.
