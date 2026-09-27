# Conformance Matrix — AMQP 0-9-1 (`docs/amqp0-9-1.pdf`)

Every normative requirement of the specification mapped to its
implementing code and verifying test. The per-method details deferred by
§3.2.2 to the generated registry are verified byte-for-byte against the
canonical registry in `crates/switchboard-wire/tests/registry_conformance.rs`
(all 62 implemented methods: ids, argument order/widths, synchronous and
content flags; the skipped `access` class and `update-secret` extension are
asserted to be rejected).

## Chapter 4 — Transport layer (wire format)

| Spec item | Requirement | Implementation | Test |
|---|---|---|---|
| §4.2.1 field-value grammar | all 18 types | `wire::field::FieldValue` (+ `x` byte-array extension) | `field::tests::every_field_type_roundtrips` |
| §4.2.1 protocol header | `AMQP\0\0\9\1` | `frame::{PROTOCOL_HEADER, parse_protocol_header}` | `frame::tests::protocol_header_is_exact`; e2e `protocol_header_garbage…` |
| §4.2.2 header reject | reply valid header + close | `session::serve_rw` reject path | e2e `protocol_header_garbage…` |
| §4.2.3 frame format | 7-byte header + payload + `0xCE` | `frame::{Frame, FrameReader}` | `frame::tests::*` |
| §4.2.3 frame types 1/2/3/8 | unknown types fatal | `FrameType::from_u8`, `FrameReader::next_frame` | `frame::tests::unknown_frame_type_is_fatal` |
| §4.2.3 frame-end | `0xCE` validated | `FrameReader::next_frame` | `frame::tests::bad_frame_end_is_fatal` |
| §4.2.3 frame-max | oversized = 501 | `FrameReader::next_frame(frame_max)` | `frame::tests::oversized_frame_is_rejected_early` |
| §4.2.3 channel rules | heartbeat/connection on channel 0 | `session::handle_frame` | unit `channel_zero_violations…`; e2e handshake |
| §4.2.4 method payloads | class/method ids + arguments | `method::Method` table | `registry_conformance` (byte-exact vs registry) |
| §4.2.5 integers/bits/strings | BE, bit packing low-first | `wireio::{Encoder, Decoder}` | `wireio::tests::*` |
| §4.2.5.5 field names | charset + 128 chars (see erratum) | `field::validate_field_name`, `FieldTable::validate` | `field::tests::field_name_validation` |
| §4.2.6 content framing | header class/weight rules | `properties::ContentHeader` | `properties::tests::content_header_roundtrip_and_checks` |
| §4.2.6.1 property flags | bit 15 first, continuation | `properties::{encode, decode}` | `properties::tests::*` |
| §4.2.7 heartbeats | channel 0, silent-interval close | session writer/reader timers | `session` heartbeat logic; e2e manual |
| §4.3 channels | multiplexing, 504 on misuse | `session::handle_channel_method` | `channel::tests::channel_zero_violations…` |
| §4.5 channel closure | unacked marked for redelivery | `Channel::teardown` → shard `Release` | e2e `unacked_messages_are_requeued_on_channel_close` |
| §4.7 ordering | per-path FIFO | per-queue seq from raft log order | `shard::tests::enqueue_allocates_sequence_numbers_in_order` |
| §4.8 exceptions | channel vs connection levels | `core::error::{BrokerError, Level}` + session mapping | `error::tests::*`, session tests |

## Chapter 2/3 — Functional layer

| Spec item | Requirement | Implementation | Test |
|---|---|---|---|
| §2.1.1 model | exchanges, queues, bindings | `core::model`, `core::topology` | `topology::tests::*` |
| §2.1.2 message flow | route → queue → consumer | `routing::route`, shard pump | `routing::tests::*`, e2e roundtrip |
| §2.1.3.1 direct + default | nameless exchange, name binding | `routing::route` default-exchange arm | e2e `handshake…roundtrip`, `routing::tests::default_exchange…` |
| §2.1.3.2 fanout | all bound queues | `routing::tests::fanout_hits_every_queue_once` | idem |
| §2.1.3.3 topic | `*`/`#` patterns | `topic::words_match` | `topic::tests::*` (incl. §3.1.3.3 example) |
| §3.1.3.4 headers | `x-match` all/any, `x-` reserved | `routing::headers_match` | `routing::tests::headers_*` |
| §3.1.3 exchanges pre-declared | amq.direct/fanout/topic/match | `topology::bootstrap` | `topology::tests::bootstrap_has_default_vhost_exchanges_and_user` |
| §3.1.4 queues | durable/exclusive/auto-delete | `topology::MetaCmd::DeclareQueue` | `topology::tests::queue_*` |
| §3.1.5 bindings | key + arguments | `topology::MetaCmd::Bind/Unbind` | `topology::tests::bind_unbind_rules` |
| §3.1.6 consumers | register/cancel/exclusive | `shard::ShardCmd::Register*` | `shard::tests::exclusive_consumer_blocks_other_connections` |
| §3.1.7 prefetch | credit windows | shard `Credit` + channel refill | `shard::tests::credit_delivers…`, e2e consumer |
| §3.1.8 acknowledgements | auto + explicit, requeue | shard `Ack`/`Release`, `redelivered` | `shard::tests::ack_removes_and_release_redelivers` |
| §3.1.9 flow control | `channel.flow` | shard `Flow` | `shard::tests::flow_pauses_and_resumes`, e2e |
| §3.1.10 naming | `amq.` reserved, server names `amq.gen-` | `topology`, `model::generate_queue_name` | `topology::tests::queue_declare_reserved_names_and_server_naming` |
| §2.2.4 connection class | Start/StartOk/Tune/Open lifecycle, SASL | `session::handshake`, `core::auth` | `auth::tests::*`, e2e handshake |
| §2.2.5 channel class | open/flow/close | `session`, `methods` | e2e suite |
| §2.2.8 basic class | publish/consume/get/ack/return | `methods::*` | e2e suite (publish/get/mandatory/consume) |
| §2.2.9 tx class | buffered publish + ack, commit/rollback | `methods::commit`, shard `PrepareTx/CommitTx/AbortTx` | `shard::tests::transactions_*`, e2e `transactions_commit_and_rollback` |
| §2.1.2.1 unroutable | mandatory → return | `methods::publish` | e2e `mandatory_unroutable_gets_returned` |
| §2.3.3 negotiation | lowest agreed limits | `session::handshake` (agreed/agreed16) | handshake unit tests |
| §2.3.7 close handshaking | Close → Close-Ok | session + channel close paths | e2e suite |
| §3.1.2 virtual hosts | 402 on unknown vhost | meta `vhost()` + session open | e2e `bad_vhost_is_rejected_before_open` |
| §4.4 visibility | declare reply ⇒ observable | meta ops are raft-applied before reply | `topology::tests`, e2e roundtrip |

## Cluster test matrix (e2e, `switchboard-server/tests/`)

| suite | what it proves |
|---|---|
| `e2e_single.rs` (12) | full protocol stack on one node: handshake, routing, mandatory returns, passive declares, confirms, consumers, requeue, tx, redeclare 406 |
| `e2e_sizes.rs` (9) | clusters of exactly 1…9 nodes: multi-master writes through *every* node land exactly once; ≤3-voters-per-group invariant at every size |
| `e2e_autoscale_up.rs` | 3→9 nodes one joiner at a time: registration, layout extension, voter cap, coverage, writes through each fresh joiner, pinned queues survive growth |
| `e2e_autoscale_down.rs` | 9→3 nodes one departure at a time: `Forgetter` + group heal (drop & refill to 3 live voters), raft voter-set convergence, writes keep landing after every round |
| `e2e_admin_ops.rs` (10) | purge depth, delete preconditions (406/404), passive asserts, exchange delete lifecycle + default-exchange 403, QoS (`prefetch_size` accepted), nack-requeue, recover (`recover-ok` + redelivery), **priority delivery order**, **message TTL expiry → DLQ**, **nack-requeue=false dead-lettering** |
| `registry_conformance.rs` (wire) | all 62 implemented method payloads decode byte-for-byte per the normative registry JSON |
| `protocols_e2e.rs` (14) | gateway demux; AMQP over the gateway; HTTP health; unrecognized-preamble close; protocol disable; MQTT pub/sub, QoS 1 PUBACK, bad-credential refusal, cross-protocol to AMQP; STOMP roundtrip + client-ack semantics; WebSocket MQTT and sniffed-STOMP; AMQP 1.0 ANONYMOUS connect/close and publish into an AMQP queue |
| `discovery_e2e.rs` (2) | DNS-seed discovery joins a zero-config node (registration, layout coverage, voter cap); mDNS multicast self-assembly of two unconfigured nodes |

## Extensions (documented, not in the base spec)

* `basic.nack`, `confirm.select/ok`, `connection.blocked/unblocked` —
  advertised in `Connection.Start` capabilities.
* AMQPLAIN + PLAIN SASL (`core::auth`).
* `x` byte-array field type (RabbitMQ parity).
* Consumer cancellation notifications on queue deletion.

## Deviations & errata (documented)

* **Field-name charset vs `x-match`** (§4.2.5.5 vs §3.1.3.4): the name rule
  forbids `-`, yet `x-match` is required. Resolved permissively
  (`field::validate_field_name` accepts `-` as a continuation character).
* **Duplicate field-table keys** are undefined per spec; we reject with a
  syntax error.
* **`exchange.unbind-ok` uses method id 51** per the deployed registry.
* **Heartbeat frame type is 8** per the formal grammar (the prose listing
  "4" is a spec typo).
