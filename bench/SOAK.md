# Switchboard Soak + Stability Tests

Long-duration load testing with a single acceptance bar: **not one
client-visible error, for as long as the soak runs** — hours, days, or a
month. Where the Jepsen suite answers "is the protocol correct under
faults?", the soak answers "can this run unattended under load for
months?".

## What runs

`switchboard-soak` (in `crates/switchboard-bench`) drives every cluster
feature concurrently, at sustainable (non-saturating) rates:

| workload | what it exercises | invariants checked |
|---|---|---|
| `pipeline` | confirmed publish → acked consume, 3 queues | per-publisher FIFO order, exactly-once (redelivered-flagged repeats legal), CRC integrity, confirmed == delivered |
| `fanout` | fanout exchange → 3 queues | each queue receives every confirmed publish exactly once, in order |
| `tx` | channel transactions (commit/abort, shard 2PC) | committed lands, aborted never delivered |
| `get` | `basic.get` + ack pull path | count conservation, FIFO |
| `topo` | queue/exchange declare/bind/purge/unbind/delete, exclusive+auto-delete lifecycle | every op answered Ok under load |
| `connchurn` | full connect/open-channels/declare/delete/close cycles | fd and connection-state cleanup (leak driver) |
| `mandatory` | raw-wire mandatory publish + `basic.return` | unroutable → return (312 NO_ROUTE) *and* confirm; routable → confirm only |
| `mqtt` | MQTT 3.1.1 QoS1 roundtrips, retained messages, persistent sessions across abrupt drops | PUBACK for every publish, in-order delivery, retained served on subscribe, session_present + offline backlog on reconnect |
| `stomp` | STOMP 1.2 SEND/MESSAGE/ACK, transactions, heart-beat negotiation | in-order exactly-once, server heartbeat present when idle |
| `amqp10` | AMQP 1.0 settled transfers + unsettled receiver with dispositions | in-order exactly-once |
| `idle` | one AMQP session held open, zero traffic | heartbeat keepalive keeps idle sessions alive indefinitely |
| `health` | `GET /health` on every node, every few seconds | always 200 `{"status":"ok"}` |

Every message body carries a publisher tag, a sequence number, and a
CRC-32; every delivery is verified (integrity + per-tag FIFO +
redelivery discipline) and every confirmed message must come back out.
Publishers retire their sequence space on any publish anomaly (the
ambiguity window where a message may or may not have landed), so churn
never produces false loss verdicts — reconciliation is exact.

## Error policy — the zero-error bar

Any client-visible anomaly fails the run: connect/handshake failures,
server-driven channel closes, confirm Nacks or timeouts, consumer stream
errors, ordering or integrity violations, non-200 health, STOMP ERROR
frames, MQTT CONNACK refusals, missed heartbeats, and reconciliation
mismatches at drain. The first 200 are recorded verbatim; totals stay
exact beyond that.

Infrastructure churn (pods deleted, scaled away, SIGKILLed) is
*expected* in the k8s soak and absorbed as **transitions** — counted,
never errors. The distinction is deliberate: reconnections are the
harness's job, data discipline is the broker's. `--chaos` selects this
classification for static hosts too.

## The k8s scale churn (a requirement, not an option)

Deployed against the k3s host with the broker as a **Deployment** (no
StatefulSet identities — every pod is an interchangeable node; node id =
pod-name hash, discovery = headless-service DNS, first pod bootstraps
with `expected-nodes=1` and the membership controller grows meta to the
first 3 registered nodes automatically).

The driver's scaler runs **for the entire soak**:

* one scale move per `--scale-every` (default 90s): a random walk over
  `--scale=soak/switchboard:1:9`, so nodes come and go **non-ordered** —
  7 → 2 → 9 → 4 → 1 → 8 …;
* one unclean **SIGKILL** of a random pod per `--kill-every` (default 3m)
  at ≥3 replicas — the path that exercises the dead-node reaper and
  orphaned-consumer release with zero graceful leave;
* **sub-quorum discipline**: at <3 replicas a node loss is a total
  cluster loss (nothing replicates), so the scaler closes the load gate,
  drains every queue, scales, waits for pod readiness *and* a canary
  round-trip, and reopens. Protocol sessions stay connected the whole
  time — churn still pounds joins/leaves/elections with live sessions.
  Between 3 and 9 replicas load runs **through** every scale move.

Broker machinery this leans on (added for the soak, generally useful):

* **SIGTERM → graceful `leave()`** — Deployment scale-down announces
  ForgetNode through meta immediately;
* **dead-node reaper** — the meta leader probes registered nodes and
  reaps any whose internal port refuses connections long enough
  (`--reap-dead-after`, ~10 s at defaults). SIGKILL/OOM/eviction used to
  leave a node registered forever, rotting the meta voter set and group
  member lists — fatal on a months horizon;
* **`GET /stats`** — pid, RSS, fds, data-dir size, uptime, `joined`;
  feeds the leak monitor and the readiness probe;
* readiness = `joined` (`sb-ready`), so load only ever lands on nodes
  that are real cluster members.

## Resource monitoring

Every `--monitor-every` the driver samples `/stats` per pod and trends
RSS, fd count, and data-dir size (least-squares per-hour slope over a
sliding hour, thresholds `--leak rss,fds,disk` MiB/h). In k8s mode it
also tails pod logs and counts new ERROR/panic lines (openraft's benign
election noise excluded), and tracks pod restart counts. Leaks fail the
soak — a month without errors includes not growing without bound.

## Shutdown semantics

On deadline or SIGINT/SIGTERM: close the load gate, let queues drain,
reconcile every stream (confirmed == delivered per publisher tag per
queue), sample resources one last time, then print the verdict — human
summary plus a machine block after `=== SOAK RESULTS ===`. Exit code 0
only on a clean pass.

## Running

### CI smoke (in-process, ~41 s)

The same engine, workloads, and zero-error bar against a real in-process
3-node cluster with the full protocol gateway:

```sh
SB_TIME_SCALE=50 cargo nextest run -p switchboard-server --test soak_smoke
```

### The real thing (k3s)

```sh
# image (from repo root; linux/amd64 for the k3s host)
docker buildx build --platform linux/amd64 -f k8s/Dockerfile -t switchboard-soak-lab:dev .
docker save switchboard-soak-lab:dev | ssh alpha-0.app.alpha.labs.cdg.sunbeam.pt 'sudo k3s ctr images import -'

export KUBECONFIG=~/.kube/config.alpha0
kubectl apply -f k8s/soak.yaml

# watch
kubectl -n soak logs -f deploy/soak-driver
```

Manifest defaults are a 30-day soak with 90 s scale moves and a SIGKILL
every 3 min. Static-host mode (e.g. the docker lab) is the same binary
without `--scale`:

```sh
switchboard-soak --hosts sb1:5672,sb2:5672,sb3:5672 --duration=7d
```

## Open findings (live k3s validation, in progress)

The first live 1↔9 churn validations surfaced two issues that need a
focused session each:

1. **Confirmed messages vanish on pipeline/fanout/get during churn.**
   A 27 min run with 45 s scale moves + 90 s SIGKILLs conserved tx
   exactly (3540 == 3540 through shard 2PC) and stomp exactly, but the
   lapin-consumer workloads lost ~40 % of confirmed messages — whole
   publisher *generations* (post-transport-error restarts) show
   `delivered == 0` despite confirms, and some tags show delivered ==
   confirmed + 1. The unclean-kill + reaper + group-heal interaction is
   the suspect window; tx's conservation suggests the shard core is
   sound and the loss lives in the confirm/delivery/consumer-reconnect
   layer. Evidence preserved in the driver's final JSON
   (`kubectl -n soak logs deploy/soak-driver`).
2. **Broker RSS grows fast under churn** — ~600 MiB in 27 min on the
   1-replica survivor (limit 1 GiB) with data-dir at 183 MB, at soak
   rates. Suspects: meta-log growth from reaper/reconfigure writes
   outpacing compaction, or per-churn connection/group-handle
   accumulation. The leak monitor caught it; root cause TBD.

Also fixed during live validation: the k8s entrypoint used
`--dns-seeds` (flag is `--dns-seed`), duplicated `--node-id`, and the
liveness probe killed pods during bootstrap discovery (initialDelay now
covers the entrypoint window); the amqp10 workload never replenished
link credit (deliveries stopped after exactly 64 transfers); the drain
phase now keeps consumers alive (`halt` token) so the last in-flight
deliveries are acked before reconciliation.

## Bugs the soak found (so far)

1. **AMQP 1.0 frames were spec-malformed** — the encoder wrote
   `[doff][type][channel][size]`; §2.3 mandates
   `[size:4][doff:1][type:1][channel:2]`. Any real AMQP 1.0 client was
   rejected; fixed encoder, decoder, and read loop.
2. **`heartbeat=0` panicked the session reader** — the silence deadline
   computed `Instant + u64::MAX seconds`, overflowing and killing every
   heartbeat-disabled connection.
3. **Consumer subscription ids collided across channels** —
   `SubscriptionId` is keyed node-global, but the counter was
   per-channel, so the first consumer of every channel on a node shared
   `{node, 1}` and sessions received each other's deliveries. Now a
   node-global counter.
4. **Followers shipped deliveries when the leader hint read `None`** —
   the effect pump treated "leader unknown" as "I lead", so busy
   followers double-delivered to remote consumers. Now gated on the
   node's own raft role.
5. **STOMP negotiated heart-beat but never sent one** — added the
   server-side heartbeat task (idle STOMP sessions would eventually be
   disconnected by any conforming client).
6. **Unroutable mandatory publishes returned 404, not 312 NO_ROUTE** —
   spec deviation; fixed.
