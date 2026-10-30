/*
 * switchboard.h — stable C ABI over the switchboard-embedded in-process
 * broker (Switchboard 1.0-rc). Hand-written; the Rust implementation is
 * crates/switchboard-capi/src/.
 *
 * EMBEDDING MODEL
 * ===============
 * One function call starts a complete AMQP 0-9-1 broker (raft-replicated
 * topology and queues over RocksDB, single-node by default) inside YOUR
 * process, and a typed client that talks to it through an in-memory
 * transport — no sockets anywhere on the client path. The client drives
 * the exact same session core a loopback TCP client gets: same handshake,
 * same PLAIN auth (loopback guest rule included), same frame demux, same
 * channel state machines.
 *
 * LIFECYCLE
 * =========
 *   sb_config_create(dir)            -> sb_config_t*      (free it)
 *   [sb_config_set_*]
 *   sb_broker_start(cfg, &broker)                          (one per dir,
 *                                                           per PROCESS)
 *   sb_broker_connect(broker, u, pw, &client)
 *   sb_client_open_channel(client, &channel)
 *   sb_channel_queue_declare / publish / get / ack / tx_* / consume ...
 *   sb_client_close(client)          -> graceful AMQP close
 *   sb_broker_stop(broker, grace_ms) -> graceful broker stop
 *   sb_*_free(...)                   -> release memory (see OWNERSHIP)
 *
 * RESTART CONTRACT: one live broker per data_dir per process (the RocksDB
 * exclusive LOCK is held for the whole process). Restarting means starting
 * a NEW process whose sb_broker_start uses the same data_dir; durable
 * queues and their messages reappear there. A second start on the same
 * dir in the same process fails with SB_ERR_STORAGE.
 *
 * OWNERSHIP
 * =========
 * Every function that returns SB_OK and has an out-parameter of handle or
 * buffer type hands out exactly one owned object:
 *   - handles are released by exactly one sb_*_free (or sb_client_close
 *     followed by sb_client_free for clients);
 *   - returned strings (char*) are released by sb_string_free;
 *   - returned messages (sb_message_t) are released by sb_message_free,
 *     which tolerates NULL and zeroed structs.
 * Freeing twice is DETECTED, not undefined: a global handle table backs
 * every use, so a stale pointer fails with SB_ERR_INVALID_HANDLE instead
 * of being dereferenced. A handle whose broker/connection/consumer is
 * already gone stays freeable but fails every operation with
 * SB_ERR_HANDLE_DEAD. sb_broker_stop/sb_broker_free and
 * sb_client_close/sb_broker_stop also mark every dependent handle dead.
 *
 * THREAD-SAFETY CONTRACT
 * ======================
 * - Every sb_* call is SYNCHRONOUS: it blocks the calling OS thread until
 *   the operation completes. The broker runs on a private multi-thread
 *   tokio runtime created by sb_broker_start; embedders need no runtime.
 * - Distinct handles may be used concurrently from distinct threads.
 *   Sharing ONE handle across threads is safe: operations are internally
 *   synchronized, and the one &mut-requiring operation (consumer polling)
 *   serializes on the handle.
 * - sb_last_error() is THREAD-LOCAL: it reports the most recent failure
 *   ON THE CALLING THREAD. Its returned pointer stays valid until the
 *   next sb_* call on that thread.
 * - Never call sb_* from inside a running async context (a callback into
 *   an event loop): block_on from an async worker panics by design. The
 *   panic is contained as SB_ERR_PANIC — the process survives, the call
 *   fails.
 * - This library never calls INTO embedder code: the consume model is
 *   polling only (sb_channel_get / sb_consumer_next). There are no
 *   callbacks and therefore no callback-reentrancy or thread-affinity
 *   rules to uphold.
 *
 * CONSUME MODEL (documented choice)
 * =================================
 * Polling only, two flavors:
 *   - sb_channel_get: non-blocking pull (Basic.Get; *out_has_message = 0
 *     when the queue is empty — SB_OK either way);
 *   - sb_channel_consume + sb_consumer_next: a push consumer drained
 *     synchronously with a timeout (SB_OK + *out_has_message = 0 on
 *     timeout; SB_ERR_CONSUMER_CANCELLED when the consumer is gone).
 * Set a prefetch window with sb_channel_qos when you use push consumers:
 * the delivery pipeline is unbounded, prefetch is the real flow control.
 *
 * STRINGS AND BODIES
 * ==================
 * Names and credentials are NUL-terminated UTF-8 C strings; invalid UTF-8
 * fails with SB_ERR_INVALID_UTF8. Message bodies are passed with an
 * EXPLICIT LENGTH (ptr + len); NULL with len 0 is an empty body.
 *
 * PANICS
 * ======
 * Every entry point runs under catch_unwind. A Rust panic (library bug or
 * misuse of an async context) surfaces as SB_ERR_PANIC plus a last-error
 * message; it never crosses into C and never aborts the process.
 */

#ifndef SWITCHBOARD_CAPI_H
#define SWITCHBOARD_CAPI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------------
 * Status codes (numeric values frozen for 1.0).
 * ---------------------------------------------------------------------- */
typedef enum sb_status {
    SB_OK = 0,                     /* success                                   */
    SB_ERR_NULL_ARG = 1,           /* a required pointer argument was NULL      */
    SB_ERR_INVALID_UTF8 = 2,       /* a string argument was not valid UTF-8     */
    SB_ERR_INVALID_HANDLE = 3,     /* freed, never issued, or wrong kind        */
    SB_ERR_HANDLE_DEAD = 4,        /* allocated but its broker/conn is gone     */
    SB_ERR_BROKER_STOPPED = 5,     /* the broker has already been stopped       */
    SB_ERR_CONNECT_ACCESS_REFUSED = 6, /* bad credentials at the handshake      */
    SB_ERR_NO_SUCH_VHOST = 7,      /* connect_vhost named an unknown vhost      */
    SB_ERR_CONNECTION_CLOSED = 8,  /* the server closed the connection          */
    SB_ERR_CHANNEL_CLOSED = 9,     /* the server closed the channel             */
    SB_ERR_ACCESS_REFUSED = 10,    /* authorization gate refused (close 403)    */
    SB_ERR_NOT_FOUND = 11,         /* missing queue/exchange (close 404)        */
    SB_ERR_PRECONDITION_FAILED = 12, /* inequivalent re-declare / mode clash 406*/
    SB_ERR_IO = 13,                /* transport or internal I/O failure         */
    SB_ERR_ADMIN_REFUSED = 14,     /* meta state machine rejected an admin call */
    SB_ERR_RAFT = 15,              /* replicated write failed                   */
    SB_ERR_STOP_TIMED_OUT = 16,    /* stop exceeded its grace (teardown done)   */
    SB_ERR_STOP_LEAVE_FAILED = 17, /* cluster leave failed (teardown done)      */
    SB_ERR_INVALID_CONFIG = 18,    /* start rejected the configuration          */
    SB_ERR_STORAGE = 19,           /* store LOCK held / store failed to open    */
    SB_ERR_FORMATION = 20,         /* cluster formation/readiness failed        */
    SB_ERR_TIMEOUT = 21,           /* a bounded wait ran out of budget          */
    SB_ERR_CONSUMER_CANCELLED = 22,/* the consumer is gone (queue deleted etc.) */
    SB_ERR_PANIC = 23              /* a Rust panic was contained at the boundary*/
} sb_status;

/* Exchange-kind argument for sb_channel_exchange_declare. */
enum {
    SB_EXCHANGE_DIRECT  = 0,
    SB_EXCHANGE_FANOUT  = 1,
    SB_EXCHANGE_TOPIC   = 2,
    SB_EXCHANGE_HEADERS = 3
};

/* Opaque handles. Never allocated by the embedder; only passed through. */
typedef struct SbConfig   sb_config_t;
typedef struct SbBroker   sb_broker_t;
typedef struct SbClient   sb_client_t;
typedef struct SbChannel  sb_channel_t;
typedef struct SbConsumer sb_consumer_t;

/* A delivered message (from sb_channel_get / sb_consumer_next).
 *
 * Owned memory: free the whole struct with sb_message_free. On a FAILED
 * call the library zeroes the struct first, so calling sb_message_free on
 * it afterwards is always safe. exchange/routing_key/consumer_tag are
 * never NULL ("" is possible); content_type is NULL when the publisher
 * did not set one; body is NULL only when body_len is 0. */
typedef struct sb_message {
    uint64_t delivery_tag;   /* channel-scoped tag for ack/nack            */
    int32_t  redelivered;    /* 1 when this is a redelivery                */
    char    *exchange;       /* the exchange the publisher addressed       */
    char    *routing_key;    /* the routing key the publisher used         */
    char    *consumer_tag;   /* delivering consumer; "" for sb_channel_get */
    char    *content_type;   /* NULL when unset                            */
    uint8_t *body;           /* opaque body octets                         */
    size_t   body_len;       /* length of body in octets                   */
} sb_message_t;

/* ------------------------------------------------------------------------
 * Utilities
 * ---------------------------------------------------------------------- */

/* The library version, e.g. "1.0.0-rc.1". Static storage; never free. */
const char *sb_version_string(void);

/* The last error message ON THE CALLING THREAD (set by any failed sb_*
 * call), or NULL when this thread has never seen a failure. Valid until
 * the next sb_* call on that thread. */
const char *sb_last_error(void);

/* Free one owned string returned by this library (sb_broker_data_dir,
 * sb_channel_queue_declare, sb_consumer_tag). NULL is a no-op. */
sb_status sb_string_free(char *s);

/* Free one sb_message_t filled by sb_channel_get / sb_consumer_next.
 * NULL and zeroed structs are tolerated (safe no-ops); the struct is
 * re-zeroed so freeing twice is safe. */
sb_status sb_message_free(sb_message_t *message);

/* ------------------------------------------------------------------------
 * Broker configuration
 * ---------------------------------------------------------------------- */

/* A config for a fresh SINGLE-NODE cluster rooted at data_dir (the RocksDB
 * location). Defaults: node id 1, expected-nodes 1, no bootstrap write,
 * default node timeouts. On success *out receives an owned handle. */
sb_status sb_config_create(const char *data_dir, sb_config_t **out);

/* Overrides (all optional). */
sb_status sb_config_set_node_id(sb_config_t *config, uint64_t node_id);
sb_status sb_config_set_expected_nodes(sb_config_t *config, uint64_t expected_nodes);

/* Ensure this user exists with this password AFTER readiness (an existing
 * user's password is NOT overwritten — rotated passwords survive). The
 * reserved name "guest" is refused at start with SB_ERR_INVALID_CONFIG;
 * rotate guest via sb_broker_create_user after start instead. */
sb_status sb_config_set_bootstrap_credentials(sb_config_t *config,
                                              const char *user,
                                              const char *password);

/* Override the node's janitor tick, in milliseconds. The embedded crate
 * documents a tx-commit/tick race: with the default 1000 ms tick a tx
 * commit racing the janitor can be silently dropped while Tx.Commit-Ok
 * still returns. If you use sb_channel_tx_*, set this well above your
 * longest transaction (e.g. 60000). */
sb_status sb_config_set_janitor_interval_ms(sb_config_t *config, uint64_t interval_ms);

/* Free a config. NULL is a no-op; double-free is detected. */
sb_status sb_config_free(sb_config_t *config);

/* ------------------------------------------------------------------------
 * Broker lifecycle
 * ---------------------------------------------------------------------- */

/* Start one in-process broker (on a private multi-thread runtime this
 * library creates). *out receives an owned handle; left untouched on
 * failure. SB_ERR_STORAGE = the RocksDB LOCK on data_dir is held (one
 * live broker per dir per process; restart = new process, same dir). */
sb_status sb_broker_start(const sb_config_t *config, sb_broker_t **out);

/* 1/0 into *out: bootstrap entities visible in the local topology. */
sb_status sb_broker_is_ready(sb_broker_t *broker, int32_t *out);

/* Block up to timeout_ms for readiness; SB_ERR_TIMEOUT when the budget
 * elapses (formation may still be pending). */
sb_status sb_broker_wait_ready(sb_broker_t *broker, uint32_t timeout_ms);

/* The configured data directory as an owned string (sb_string_free). */
sb_status sb_broker_data_dir(sb_broker_t *broker, char **out);

/* Graceful stop: close every tracked connection, run the bounded cluster
 * leave, then the shutdown fallback. The handle becomes DEAD (release it
 * with sb_broker_free); every client/channel/consumer created from it
 * becomes DEAD too. Teardown completes even when this returns
 * SB_ERR_STOP_TIMED_OUT / SB_ERR_STOP_LEAVE_FAILED. */
sb_status sb_broker_stop(sb_broker_t *broker, uint32_t grace_ms);

/* Free a broker handle. A still-live broker is stopped first with a 10 s
 * fallback grace — prefer sb_broker_stop to observe teardown errors.
 * NULL is a no-op; double-free is detected. */
sb_status sb_broker_free(sb_broker_t *broker);

/* ------------------------------------------------------------------------
 * Connections (clients)
 * ---------------------------------------------------------------------- */

/* Open an embedded connection to vhost "/" — a real PLAIN handshake
 * against the replicated user table (embedded clients are ordinary
 * loopback clients; the guest-loopback rule applies). Owned handle in
 * *out. SB_ERR_CONNECT_ACCESS_REFUSED for bad credentials. */
sb_status sb_broker_connect(sb_broker_t *broker,
                            const char *user,
                            const char *password,
                            sb_client_t **out);

/* Same, for a specific vhost (SB_ERR_NO_SUCH_VHOST when unknown). */
sb_status sb_broker_connect_vhost(sb_broker_t *broker,
                                  const char *user,
                                  const char *password,
                                  const char *vhost,
                                  sb_client_t **out);

/* Open a new channel (numbers are connection-scoped, start at 1). */
sb_status sb_client_open_channel(sb_client_t *client, sb_channel_t **out);

/* Graceful AMQP close: Connection.Close / Close-Ok, server-side teardown
 * (consumers cancelled, unacked deliveries requeued), broker registry
 * updated. The client handle becomes DEAD (free with sb_client_free);
 * channels/consumers created from it become DEAD too. */
sb_status sb_client_close(sb_client_t *client);

/* Free a client handle. Prefer sb_client_close first (freeing without
 * closing leaves the session registered with the broker until the broker
 * stops). NULL is a no-op; double-free is detected. */
sb_status sb_client_free(sb_client_t *client);

/* ------------------------------------------------------------------------
 * Admin (replicated; same path the protocol gateways use)
 * ---------------------------------------------------------------------- */

/* Declare a vhost (idempotent upsert of the mandatory bootstrap
 * exchanges). */
sb_status sb_broker_declare_vhost(sb_broker_t *broker, const char *name);

/* Create a user or REPLACE its password (upsert) — the explicit set or
 * rotate path for any user, guest included. */
sb_status sb_broker_create_user(sb_broker_t *broker,
                                const char *user,
                                const char *password);

/* Delete a user and their permissions (idempotent). */
sb_status sb_broker_delete_user(sb_broker_t *broker, const char *user);

/* Set one user's permissions on one vhost (RabbitMQ-shaped regexes; all
 * empty patterns clear the entry). Missing user -> SB_ERR_ADMIN_REFUSED. */
sb_status sb_broker_set_permissions(sb_broker_t *broker,
                                    const char *user,
                                    const char *vhost,
                                    const char *configure,
                                    const char *write,
                                    const char *read);

/* ------------------------------------------------------------------------
 * Channels: topology
 * ---------------------------------------------------------------------- */

/* Declare a queue. Flags are 0/1. An empty name ("") asks the server for
 * a generated one (amq.gen-…); the FINAL name is returned as an owned
 * string in *out (sb_string_free). */
sb_status sb_channel_queue_declare(sb_channel_t *channel,
                                   const char *name,
                                   int32_t durable,
                                   int32_t exclusive,
                                   int32_t auto_delete,
                                   int32_t passive,
                                   char **out);

sb_status sb_channel_queue_bind(sb_channel_t *channel,
                                const char *queue,
                                const char *exchange,
                                const char *routing_key);
sb_status sb_channel_queue_unbind(sb_channel_t *channel,
                                  const char *queue,
                                  const char *exchange,
                                  const char *routing_key);

/* Delete a queue (flags are 0/1 preconditions); *out_count (may be NULL)
 * receives the number of messages purged with it. */
sb_status sb_channel_queue_delete(sb_channel_t *channel,
                                  const char *name,
                                  int32_t if_unused,
                                  int32_t if_empty,
                                  uint32_t *out_count);

/* Purge a queue; *out_count (may be NULL) receives the ready-message
 * count that was purged. */
sb_status sb_channel_queue_purge(sb_channel_t *channel,
                                 const char *name,
                                 uint32_t *out_count);

/* Declare an exchange; kind is SB_EXCHANGE_{DIRECT,FANOUT,TOPIC,HEADERS}.
 * Flags are 0/1. */
sb_status sb_channel_exchange_declare(sb_channel_t *channel,
                                      const char *name,
                                      int32_t kind,
                                      int32_t durable,
                                      int32_t auto_delete,
                                      int32_t internal,
                                      int32_t passive);

sb_status sb_channel_exchange_delete(sb_channel_t *channel,
                                     const char *name,
                                     int32_t if_unused);

/* Exchange-to-exchange binding. */
sb_status sb_channel_exchange_bind(sb_channel_t *channel,
                                   const char *dest,
                                   const char *source,
                                   const char *key);
sb_status sb_channel_exchange_unbind(sb_channel_t *channel,
                                     const char *dest,
                                     const char *source,
                                     const char *key);

/* ------------------------------------------------------------------------
 * Channels: publish
 * ---------------------------------------------------------------------- */

/* Fire-and-forget publish. body/body_len is the EXPLICIT-LENGTH byte
 * slice (NULL + 0 = the empty body). delivery_mode: 0 unset, 1 transient,
 * 2 persistent. content_type may be NULL. SB_OK means the frames were
 * written; protocol errors surface on this or a later call of the
 * channel. Unroutable non-mandatory messages are silently dropped.
 *
 * KNOWN BROKER CAVEAT (pre-existing, affects TCP clients identically):
 * the session demux completes content only in its Body-frame arm, but a
 * body_size == 0 publish carries NO body frame on the wire, so an
 * EMPTY-BODY message is silently never applied by the broker. Do not
 * rely on empty-body delivery until a fix(broker) change lands. */
sb_status sb_channel_publish(sb_channel_t *channel,
                             const char *exchange,
                             const char *routing_key,
                             const char *content_type,
                             int32_t delivery_mode,
                             const uint8_t *body,
                             size_t body_len);

/* Publish with the mandatory flag: an unroutable message is returned by
 * the broker; *out_routed receives 0 then (1 when routed). SB_OK in both
 * cases — unroutable is an outcome, not an error. */
sb_status sb_channel_publish_mandatory(sb_channel_t *channel,
                                       const char *exchange,
                                       const char *routing_key,
                                       const char *content_type,
                                       int32_t delivery_mode,
                                       const uint8_t *body,
                                       size_t body_len,
                                       int32_t *out_routed);

/* Publish in publisher-confirms mode (enables confirm mode on the channel
 * first if needed): resolves when the broker acks the publish sequence or
 * returns the message. *out_confirmed: 1 = acked, 0 = returned. Note the
 * per-channel confirm sequence counts EVERY publish; this ABI exposes
 * confirms only through this call. */
sb_status sb_channel_publish_confirmed(sb_channel_t *channel,
                                       const char *exchange,
                                       const char *routing_key,
                                       const char *content_type,
                                       int32_t delivery_mode,
                                       const uint8_t *body,
                                       size_t body_len,
                                       int32_t *out_confirmed);

/* Turn publisher-confirms mode on explicitly (refused on a tx channel:
 * SB_ERR_PRECONDITION_FAILED). */
sb_status sb_channel_confirm_select(sb_channel_t *channel);

/* ------------------------------------------------------------------------
 * Channels: pull, ack/nack, flow control
 * ---------------------------------------------------------------------- */

/* Polling consumption, flavor 1: fetch one message (Basic.Get). Stores
 * 1/0 in *out_has_message; when 1, *out holds an owned sb_message_t.
 * no_ack != 0 delivers without acknowledgement. SB_OK + 0 = queue empty.
 * With no_ack = 0 the delivery stays unacked until sb_channel_ack/nack —
 * or the channel/connection closes, which requeues it. */
sb_status sb_channel_get(sb_channel_t *channel,
                         const char *queue,
                         int32_t no_ack,
                         int32_t *out_has_message,
                         sb_message_t *out);

/* Acknowledge one delivery by its delivery_tag. */
sb_status sb_channel_ack(sb_channel_t *channel, uint64_t delivery_tag);

/* Acknowledge everything up to and including delivery_tag (multiple). */
sb_status sb_channel_ack_many(sb_channel_t *channel, uint64_t up_to_delivery_tag);

/* Reject one delivery; requeue != 0 requeues it (as redelivered),
 * requeue == 0 dead-letters it. */
sb_status sb_channel_nack(sb_channel_t *channel, uint64_t delivery_tag, int32_t requeue);

/* Reject everything up to and including delivery_tag. */
sb_status sb_channel_nack_many(sb_channel_t *channel,
                               uint64_t up_to_delivery_tag,
                               int32_t requeue);

/* Prefetch window: at most prefetch_count unacknowledged messages per
 * consumer (or per channel when global != 0). Set this for push
 * consumers — the delivery pipeline is unbounded. */
sb_status sb_channel_qos(sb_channel_t *channel, uint16_t prefetch_count, int32_t global);

/* ------------------------------------------------------------------------
 * Channels: consumers (polling drain) and cancellation
 * ---------------------------------------------------------------------- */

/* Start a push consumer on queue. consumer_tag may be NULL (server
 * generates one; retrieve it with sb_consumer_tag). no_ack/exclusive are
 * 0/1. Drain with sb_consumer_next. */
sb_status sb_channel_consume(sb_channel_t *channel,
                             const char *queue,
                             const char *consumer_tag,
                             int32_t no_ack,
                             int32_t exclusive,
                             sb_consumer_t **out);

/* Cancel a consumer BY TAG through its channel (Cancel-Ok even for
 * unknown tags, like RabbitMQ). A consumer handle created for that tag
 * stays owned by the caller (free it separately). */
sb_status sb_channel_cancel(sb_channel_t *channel, const char *consumer_tag);

/* This channel's number (1-based, connection-scoped), into *out. */
sb_status sb_channel_id(sb_channel_t *channel, uint16_t *out);

/* Free a channel handle. There is no per-channel close in the protocol
 * surface: channels tear down when their connection closes. NULL is a
 * no-op; double-free is detected. */
sb_status sb_channel_free(sb_channel_t *channel);

/* ------------------------------------------------------------------------
 * Consumers: polling drain
 * ---------------------------------------------------------------------- */

/* Block up to timeout_ms for the consumer's next delivery.
 *   SB_OK + *out_has_message = 1: *out holds an owned sb_message_t;
 *   SB_OK + *out_has_message = 0: idle timeout, consumer still live;
 *   SB_ERR_CONSUMER_CANCELLED:    the consumer is gone (its queue was
 *                                 deleted, sb_consumer_cancel ran, or the
 *                                 connection died). Handle is now DEAD.
 * Concurrent calls on one consumer serialize. */
sb_status sb_consumer_next(sb_consumer_t *consumer,
                           uint32_t timeout_ms,
                           int32_t *out_has_message,
                           sb_message_t *out);

/* The consumer's tag (server-generated ct-N when none requested), as an
 * owned string. */
sb_status sb_consumer_tag(sb_consumer_t *consumer, char **out);

/* Cancel this consumer (Basic.Cancel/Cancel-Ok); the handle becomes DEAD
 * (still owned — release with sb_consumer_free). Already-cancelled ->
 * SB_ERR_CONSUMER_CANCELLED. */
sb_status sb_consumer_cancel(sb_consumer_t *consumer);

/* Free a consumer handle. NULL is a no-op; double-free is detected;
 * freeing a cancelled (dead) consumer is the normal release path. */
sb_status sb_consumer_free(sb_consumer_t *consumer);

/* ------------------------------------------------------------------------
 * Transactions
 * ---------------------------------------------------------------------- */

/* Transactional mode: publishes and acks buffer until tx_commit.
 * Mutually exclusive with confirm mode (406 -> PRECONDITION_FAILED).
 * Widen sb_config_set_janitor_interval_ms when you use transactions. */
sb_status sb_channel_tx_select(sb_channel_t *channel);
sb_status sb_channel_tx_commit(sb_channel_t *channel);
sb_status sb_channel_tx_rollback(sb_channel_t *channel);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* SWITCHBOARD_CAPI_H */
