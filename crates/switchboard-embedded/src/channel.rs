//! The typed per-channel client: topology operations, publish paths,
//! get/ack/nack/qos, consume, and transactions.
//!
//! Every op maps onto the same wire methods a TCP client would send — the
//! embedded client drives the identical session core (embedded design
//! §4.2). A channel closed by the server is INVALIDATED across all clones
//! (§4.3): later calls fail fast locally with the stored close pair instead
//! of writing frames that would kill the connection (a later frame on a
//! removed channel number is a connection-level 504,
//! session/mod.rs:670-671).

use std::sync::Arc;

use switchboard_wire::method::Method;

use crate::driver::ChannelShared;
use crate::driver::call;
use crate::driver::publish_bytes;
use crate::driver::send_bytes;
use crate::error::OpError;
use crate::types::Arguments;
use crate::types::ConfirmOutcome;
use crate::types::Delivery;
use crate::types::MessageProperties;
use crate::types::PublishOutcome;
use crate::types::QueueState;

/// Queue properties selected at declare time (§2.1.4.1). Builder-style;
/// all fields default false/empty.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueOptions {
    /// Survives a restart (durable queues reappear with their messages).
    pub durable: bool,
    /// Owned by one connection; deleted when it closes.
    pub exclusive: bool,
    /// Deleted when the last consumer detaches.
    pub auto_delete: bool,
    /// Assert-only declare: the queue must already exist with equivalent
    /// properties (§4.8's assertion model) or the channel is closed 404.
    pub passive: bool,
    /// Declare arguments (queue-type, dead-letter policy, ...).
    pub arguments: Arguments,
}

impl QueueOptions {
    /// All defaults: non-durable, non-exclusive, no auto-delete, active
    /// declare, no arguments.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set durability.
    pub fn durable(mut self, v: bool) -> Self {
        self.durable = v;
        self
    }

    /// Set exclusivity (owned by this connection).
    pub fn exclusive(mut self, v: bool) -> Self {
        self.exclusive = v;
        self
    }

    /// Set auto-delete (deleted when the last consumer detaches).
    pub fn auto_delete(mut self, v: bool) -> Self {
        self.auto_delete = v;
        self
    }

    /// Make the declare passive (assert-only).
    pub fn passive(mut self, v: bool) -> Self {
        self.passive = v;
        self
    }

    /// Set declare arguments.
    pub fn arguments(mut self, v: Arguments) -> Self {
        self.arguments = v;
        self
    }
}

/// Flags for [`EmbeddedChannel::queue_delete`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueDeleteOptions {
    /// Delete only if the queue has no consumers (§4.8 precondition).
    pub if_unused: bool,
    /// Delete only if the queue is empty (§4.8 precondition).
    pub if_empty: bool,
}

impl QueueDeleteOptions {
    /// No preconditions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Delete only when consumer-less.
    pub fn if_unused(mut self, v: bool) -> Self {
        self.if_unused = v;
        self
    }

    /// Delete only when empty.
    pub fn if_empty(mut self, v: bool) -> Self {
        self.if_empty = v;
        self
    }
}

/// Flags for [`EmbeddedChannel::queue_delete`] — alias of
/// [`QueueDeleteOptions`] naming rules for
/// [`EmbeddedChannel::exchange_delete`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExchangeDeleteOptions {
    /// Delete only if the exchange has no bindings.
    pub if_unused: bool,
}

impl ExchangeDeleteOptions {
    /// No preconditions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Delete only when binding-less.
    pub fn if_unused(mut self, v: bool) -> Self {
        self.if_unused = v;
        self
    }
}

/// Exchange properties selected at declare time (§3.1.3.2).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExchangeOptions {
    /// Survives a restart.
    pub durable: bool,
    /// Deleted when its last binding is removed.
    pub auto_delete: bool,
    /// Internal exchanges accept no publishes.
    pub internal: bool,
    /// Assert-only declare (§4.8's assertion model).
    pub passive: bool,
    /// Declare arguments.
    pub arguments: Arguments,
}

impl ExchangeOptions {
    /// All defaults: non-durable, no auto-delete, not internal, active
    /// declare.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set durability.
    pub fn durable(mut self, v: bool) -> Self {
        self.durable = v;
        self
    }

    /// Set auto-delete.
    pub fn auto_delete(mut self, v: bool) -> Self {
        self.auto_delete = v;
        self
    }

    /// Mark the exchange internal.
    pub fn internal(mut self, v: bool) -> Self {
        self.internal = v;
        self
    }

    /// Make the declare passive (assert-only).
    pub fn passive(mut self, v: bool) -> Self {
        self.passive = v;
        self
    }

    /// Set declare arguments.
    pub fn arguments(mut self, v: Arguments) -> Self {
        self.arguments = v;
        self
    }
}

/// Options for [`EmbeddedChannel::consume`]. The consumer tag may be left
/// empty — the server generates one (`ct-N`) and it arrives in the
/// returned [`Consumer`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumeOptions {
    /// Requested consumer tag; empty = server-generated.
    pub consumer_tag: String,
    /// no-ack mode: deliveries need no acknowledgement (at-most-once).
    pub no_ack: bool,
    /// Exclusive consumer: no other connection may consume this queue.
    pub exclusive: bool,
    /// Consume arguments.
    pub arguments: Arguments,
}

impl ConsumeOptions {
    /// Server-generated tag, acknowledged mode, non-exclusive.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request a specific consumer tag.
    pub fn tag(mut self, v: impl Into<String>) -> Self {
        self.consumer_tag = v.into();
        self
    }

    /// Enable no-ack mode.
    pub fn no_ack(mut self, v: bool) -> Self {
        self.no_ack = v;
        self
    }

    /// Make this an exclusive consumer.
    pub fn exclusive(mut self, v: bool) -> Self {
        self.exclusive = v;
        self
    }

    /// Set consume arguments.
    pub fn arguments(mut self, v: Arguments) -> Self {
        self.arguments = v;
        self
    }
}

/// Stable alias for the four standard exchange types
/// (`switchboard_core::model::ExchangeKind`): Direct, Fanout, Topic,
/// Headers. The AMQP content format is the durable contract, so this wire
/// data type is safe to expose (embedded design §3 stability rules).
pub use switchboard_core::model::ExchangeKind;

/// Cloneable per-channel handle. All ops are async and typed.
///
/// A channel closed by the server is invalidated across ALL clones (§4.3):
/// the op that triggered the close receives the decoded error
/// (403/404/406 fold into named [`OpError`] variants), and every later
/// call — on the original or any clone — fails fast with
/// [`OpError::ChannelClosed`] carrying the stored §4.8 close pair. The
/// connection itself stays usable.
#[derive(Clone)]
pub struct EmbeddedChannel {
    pub(crate) shared: Arc<ChannelShared>,
}

impl EmbeddedChannel {
    /// This channel's number (1-based, connection-scoped).
    pub fn id(&self) -> u16 {
        self.shared.id
    }

    // ------------------------------------------------------------------
    // topology: queues
    // ------------------------------------------------------------------

    /// Declare a queue (active or passive per [`QueueOptions::passive`]).
    /// An empty name asks the server to generate one (`amq.gen-…`); the
    /// final name arrives in the returned [`QueueState`].
    pub async fn queue_declare(
        &self,
        name: &str,
        opts: &QueueOptions,
    ) -> Result<QueueState, OpError> {
        let m = Method::QueueDeclare {
            ticket: 0,
            queue: name.to_string(),
            passive: opts.passive,
            durable: opts.durable,
            exclusive: opts.exclusive,
            auto_delete: opts.auto_delete,
            nowait: false,
            arguments: opts.arguments.clone(),
        };
        match call(&self.shared, &m).await? {
            (
                Method::QueueDeclareOk {
                    queue,
                    message_count,
                    consumer_count,
                },
                _,
            ) => Ok(QueueState {
                queue,
                message_count,
                consumer_count,
            }),
            (other, _) => Err(unexpected_reply(&other)),
        }
    }

    /// Bind `queue` to `exchange` under `routing_key` (§2.1.1).
    pub async fn queue_bind(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Result<(), OpError> {
        self.expect_ok(Method::QueueBind {
            ticket: 0,
            queue: queue.to_string(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            nowait: false,
            arguments: Default::default(),
        })
        .await
    }

    /// Remove the binding of `queue` to `exchange` under `routing_key`.
    pub async fn queue_unbind(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Result<(), OpError> {
        self.expect_ok(Method::QueueUnbind {
            ticket: 0,
            queue: queue.to_string(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            arguments: Default::default(),
        })
        .await
    }

    /// Delete a queue; returns the number of messages purged with it.
    pub async fn queue_delete(
        &self,
        name: &str,
        opts: &QueueDeleteOptions,
    ) -> Result<u32, OpError> {
        match call(
            &self.shared,
            &Method::QueueDelete {
                ticket: 0,
                queue: name.to_string(),
                if_unused: opts.if_unused,
                if_empty: opts.if_empty,
                nowait: false,
            },
        )
        .await?
        {
            (Method::QueueDeleteOk { message_count }, _) => Ok(message_count),
            (other, _) => Err(unexpected_reply(&other)),
        }
    }

    /// Purge all ready messages from a queue; returns the count.
    pub async fn queue_purge(&self, name: &str) -> Result<u32, OpError> {
        match call(
            &self.shared,
            &Method::QueuePurge {
                ticket: 0,
                queue: name.to_string(),
                nowait: false,
            },
        )
        .await?
        {
            (Method::QueuePurgeOk { message_count }, _) => Ok(message_count),
            (other, _) => Err(unexpected_reply(&other)),
        }
    }

    // ------------------------------------------------------------------
    // topology: exchanges
    // ------------------------------------------------------------------

    /// Declare an exchange of `kind` (§3.1.3).
    pub async fn exchange_declare(
        &self,
        name: &str,
        kind: ExchangeKind,
        opts: &ExchangeOptions,
    ) -> Result<(), OpError> {
        self.expect_ok(Method::ExchangeDeclare {
            ticket: 0,
            exchange: name.to_string(),
            exchange_type: kind.as_str().to_string(),
            passive: opts.passive,
            durable: opts.durable,
            auto_delete: opts.auto_delete,
            internal: opts.internal,
            nowait: false,
            arguments: opts.arguments.clone(),
        })
        .await
    }

    /// Delete an exchange.
    pub async fn exchange_delete(
        &self,
        name: &str,
        opts: &ExchangeDeleteOptions,
    ) -> Result<(), OpError> {
        self.expect_ok(Method::ExchangeDelete {
            ticket: 0,
            exchange: name.to_string(),
            if_unused: opts.if_unused,
            nowait: false,
        })
        .await
    }

    /// Bind destination exchange `dest` to source exchange `source` under
    /// `key` (exchange-exchange binding, advertised in the Start
    /// capabilities).
    pub async fn exchange_bind(&self, dest: &str, source: &str, key: &str) -> Result<(), OpError> {
        self.expect_ok(Method::ExchangeBind {
            ticket: 0,
            destination: dest.to_string(),
            source: source.to_string(),
            routing_key: key.to_string(),
            nowait: false,
            arguments: Default::default(),
        })
        .await
    }

    /// Remove an exchange-exchange binding.
    pub async fn exchange_unbind(
        &self,
        dest: &str,
        source: &str,
        key: &str,
    ) -> Result<(), OpError> {
        self.expect_ok(Method::ExchangeUnbind {
            ticket: 0,
            destination: dest.to_string(),
            source: source.to_string(),
            routing_key: key.to_string(),
            nowait: false,
            arguments: Default::default(),
        })
        .await
    }

    // ------------------------------------------------------------------
    // publish
    // ------------------------------------------------------------------

    /// Publish a message through `exchange` with `routing_key`.
    ///
    /// Fire-and-forget at the protocol level (Basic.Publish carries no
    /// reply): `Ok(())` means the frames were written. Protocol errors
    /// surface as a channel close, either to this call (if the close races
    /// it) or to a later op on this channel.
    ///
    /// Unroutable non-mandatory messages are silently dropped by the
    /// broker (§2.1.2.1). Use [`publish_mandatory`](Self::publish_mandatory)
    /// / [`publish_confirmed`](Self::publish_confirmed) for outcomes.
    pub async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        props: MessageProperties,
        body: Vec<u8>,
    ) -> Result<(), OpError> {
        self.shared.ensure_usable()?;
        // Confirm-mode accounting: every publish on a confirm channel
        // advances the server's per-channel confirm sequence
        // (methods/mod.rs:143-153), so the client's counter must advance
        // too or confirmed publishes would mis-correlate.
        if self
            .shared
            .confirm_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            *self.shared.confirm_next.lock().unwrap() += 1;
        }
        let bytes = publish_bytes(&self.shared, exchange, routing_key, false, props, &body);
        send_bytes(&self.shared, bytes)
    }

    /// Publish with `mandatory=true`: an unroutable message comes back as
    /// [`PublishOutcome::Returned`] instead of going to the
    /// [returns stream](crate::EmbeddedConnection::take_returns).
    ///
    /// A routed message produces no protocol traffic, so the `Routed`
    /// outcome is decided by a bounded settle wait (2 s, test-tempo
    /// scaled); prefer [`publish_confirmed`](Self::publish_confirmed) when
    /// you need exact, wait-free outcomes.
    pub async fn publish_mandatory(
        &self,
        exchange: &str,
        routing_key: &str,
        props: MessageProperties,
        body: Vec<u8>,
    ) -> Result<PublishOutcome, OpError> {
        let _guard = self.shared.publish_lock.lock().await;
        self.shared.ensure_usable()?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shared
            .conn
            .mandatory_returns
            .lock()
            .unwrap()
            .entry(self.shared.id)
            .or_default()
            .push(tx);
        let bytes = publish_bytes(&self.shared, exchange, routing_key, true, props, &body);
        if let Err(e) = send_bytes(&self.shared, bytes) {
            // Remove our correlation slot again.
            if let Some(v) = self
                .shared
                .conn
                .mandatory_returns
                .lock()
                .unwrap()
                .get_mut(&self.shared.id)
            {
                v.retain(|s| !s.is_closed());
            }
            return Err(e);
        }
        match tokio::time::timeout(
            switchboard_core::tempo::scale(std::time::Duration::from_secs(2)),
            rx,
        )
        .await
        {
            Ok(Ok(Ok(crate::driver::PublishOutcomeReply::Returned { code, text }))) => {
                Ok(PublishOutcome::Returned { code, text })
            }
            _ => Ok(PublishOutcome::Routed),
        }
    }

    /// Publish in confirm mode: enables confirm mode first
    /// (`Confirm.Select`) if the channel is not in it, then publishes with
    /// the mandatory flag set, and resolves when the broker acks the
    /// publish sequence or returns the message (methods/publish.rs:83-99).
    ///
    /// The confirm sequence is per channel and counts EVERY publish sent
    /// while the mode is active ([`publish`](Self::publish) included),
    /// mirroring the server (channel/mod.rs:123-129). Channels in tx mode
    /// cannot enter confirm mode (406 — the broker refuses the mode
    /// switch, methods/mod.rs:469-475).
    ///
    /// Concurrent `publish_confirmed` calls on one channel serialize on an
    /// internal publish lock, which keeps `basic.return` correlation
    /// unambiguous (embedded design §9/R5).
    pub async fn publish_confirmed(
        &self,
        exchange: &str,
        routing_key: &str,
        props: MessageProperties,
        body: Vec<u8>,
    ) -> Result<ConfirmOutcome, OpError> {
        let _guard = self.shared.publish_lock.lock().await;
        if !self
            .shared
            .confirm_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.expect_ok(Method::ConfirmSelect { nowait: false })
                .await?;
            self.shared
                .confirm_active
                .store(true, std::sync::atomic::Ordering::SeqCst);
            *self.shared.confirm_next.lock().unwrap() = 0;
        }
        let seq = {
            let mut next = self.shared.confirm_next.lock().unwrap();
            *next += 1;
            *next
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shared.confirm_waiters.lock().unwrap().insert(seq, tx);
        let bytes = publish_bytes(&self.shared, exchange, routing_key, true, props, &body);
        if let Err(e) = send_bytes(&self.shared, bytes) {
            self.shared.confirm_waiters.lock().unwrap().remove(&seq);
            return Err(e);
        }
        match rx.await {
            Ok(reply) => reply,
            Err(_) => Err(OpError::Io(std::io::Error::other(
                "connection driver is gone",
            ))),
        }
    }

    /// Turn confirm mode on explicitly (Confirm.Select/Select-Ok).
    /// Idempotent while already active; refuses on a tx channel (406).
    pub async fn confirm_select(&self) -> Result<(), OpError> {
        let _guard = self.shared.publish_lock.lock().await;
        self.expect_ok(Method::ConfirmSelect { nowait: false })
            .await?;
        if !self
            .shared
            .confirm_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.shared
                .confirm_active
                .store(true, std::sync::atomic::Ordering::SeqCst);
            *self.shared.confirm_next.lock().unwrap() = 0;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // get / ack / qos
    // ------------------------------------------------------------------

    /// Fetch one message by pull (Basic.Get). `Ok(None)` = queue empty.
    /// With `no_ack = false` the delivery is unacknowledged until
    /// [`ack`](Self::ack) / [`nack`](Self::nack) — or the channel/connection
    /// closes, which requeues it (§4.5).
    pub async fn get(&self, queue: &str, no_ack: bool) -> Result<Option<Delivery>, OpError> {
        match call(
            &self.shared,
            &Method::BasicGet {
                ticket: 0,
                queue: queue.to_string(),
                no_ack,
            },
        )
        .await?
        {
            (
                Method::BasicGetOk {
                    delivery_tag,
                    redelivered,
                    exchange,
                    routing_key,
                    ..
                },
                Some((properties, body)),
            ) => Ok(Some(Delivery {
                consumer_tag: String::new(),
                delivery_tag,
                redelivered,
                exchange,
                routing_key,
                properties,
                body,
            })),
            (Method::BasicGetOk { .. }, None) => Err(OpError::Io(std::io::Error::other(
                "Get-Ok arrived without content",
            ))),
            (Method::BasicGetEmpty { .. }, _) => Ok(None),
            (other, _) => Err(unexpected_reply(&other)),
        }
    }

    /// Acknowledge one delivery (Basic.Ack, multiple=false).
    pub async fn ack(&self, delivery_tag: u64) -> Result<(), OpError> {
        self.send_ack(delivery_tag, false)
    }

    /// Acknowledge everything up to and including `delivery_tag`
    /// (Basic.Ack, multiple=true).
    pub async fn ack_many(&self, up_to_delivery_tag: u64) -> Result<(), OpError> {
        self.send_ack(up_to_delivery_tag, true)
    }

    /// Reject one delivery; `requeue` puts it back (redelivered) or drops
    /// it to the queue's dead-letter path (§3.1.8, shard/mod.rs:724-750).
    pub async fn nack(&self, delivery_tag: u64, requeue: bool) -> Result<(), OpError> {
        self.send_nack(delivery_tag, false, requeue)
    }

    /// Reject everything up to and including `delivery_tag`.
    pub async fn nack_many(&self, up_to_delivery_tag: u64, requeue: bool) -> Result<(), OpError> {
        self.send_nack(up_to_delivery_tag, true, requeue)
    }

    fn send_ack(&self, delivery_tag: u64, multiple: bool) -> Result<(), OpError> {
        send_bytes(
            &self.shared,
            crate::driver::frame_bytes(
                self.shared.id,
                &Method::BasicAck {
                    delivery_tag,
                    multiple,
                },
            ),
        )
    }

    fn send_nack(&self, delivery_tag: u64, multiple: bool, requeue: bool) -> Result<(), OpError> {
        send_bytes(
            &self.shared,
            crate::driver::frame_bytes(
                self.shared.id,
                &Method::BasicNack {
                    delivery_tag,
                    multiple,
                    requeue,
                },
            ),
        )
    }

    /// Set the prefetch window (§3.1.7): at most `prefetch_count`
    /// unacknowledged messages per consumer (or per channel when `global`).
    /// This is the real flow control of the unbounded embedded pipelines
    /// (embedded design §9/R1) — set prefetch on embedded consumers too.
    pub async fn qos(&self, prefetch_count: u16, global: bool) -> Result<(), OpError> {
        self.expect_ok(Method::BasicQos {
            prefetch_size: 0,
            prefetch_count,
            global_: global,
        })
        .await
    }

    // ------------------------------------------------------------------
    // consume
    // ------------------------------------------------------------------

    /// Start a consumer on `queue`; deliveries flow through the returned
    /// [`Consumer`]. Server-side cancellation (queue deletion) surfaces as
    /// [`Consumer::next`] returning `None`
    /// (methods/consume.rs:257-269).
    pub async fn consume(
        &self,
        queue: &str,
        opts: &ConsumeOptions,
    ) -> Result<crate::consumer::Consumer, OpError> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let requested = opts.consumer_tag.clone();
        // Pre-register a client-chosen tag; a server-generated tag is
        // registered when the Consume-Ok names it (deliveries can never
        // precede the Consume-Ok — same outbound queue on the server).
        if !requested.is_empty() {
            self.shared
                .conn
                .consumers
                .lock()
                .unwrap()
                .insert(requested.clone(), (self.shared.id, tx.clone()));
        }
        let m = Method::BasicConsume {
            ticket: 0,
            queue: queue.to_string(),
            consumer_tag: requested.clone(),
            no_local: false,
            no_ack: opts.no_ack,
            exclusive: opts.exclusive,
            nowait: false,
            arguments: opts.arguments.clone(),
        };
        let result = match call(&self.shared, &m).await {
            Ok((Method::BasicConsumeOk { consumer_tag: tag }, _)) => {
                if requested.is_empty() {
                    self.shared
                        .conn
                        .consumers
                        .lock()
                        .unwrap()
                        .insert(tag.clone(), (self.shared.id, tx.clone()));
                }
                // Flush any deliveries buffered for this tag (defensive).
                let buffered = self
                    .shared
                    .conn
                    .pending_deliveries
                    .lock()
                    .unwrap()
                    .remove(&tag)
                    .unwrap_or_default();
                for d in buffered {
                    let _ = tx.send(d);
                }
                Ok(crate::consumer::Consumer::new(tag, rx, self.shared.clone()))
            }
            Ok((other, _)) => Err(unexpected_reply(&other)),
            Err(e) => Err(e),
        };
        if result.is_err() && !requested.is_empty() {
            self.shared
                .conn
                .consumers
                .lock()
                .unwrap()
                .remove(&requested);
        }
        result
    }

    /// Cancel a consumer by tag (Basic.Cancel/Cancel-Ok). Cancelling an
    /// unknown tag is answered Cancel-Ok by the broker, like RabbitMQ.
    pub async fn cancel(&self, consumer_tag: &str) -> Result<(), OpError> {
        match call(
            &self.shared,
            &Method::BasicCancel {
                consumer_tag: consumer_tag.to_string(),
                nowait: false,
            },
        )
        .await?
        {
            (Method::BasicCancelOk { .. }, _) => {
                self.shared
                    .conn
                    .consumers
                    .lock()
                    .unwrap()
                    .remove(consumer_tag);
                Ok(())
            }
            (other, _) => Err(unexpected_reply(&other)),
        }
    }

    // ------------------------------------------------------------------
    // tx
    // ------------------------------------------------------------------

    /// Put the channel in transactional mode (§2.2.9): publishes and acks
    /// buffer until [`tx_commit`](Self::tx_commit); a rollback drops the
    /// buffer without requeueing (methods/mod.rs:452-466). Mutually
    /// exclusive with confirm mode (the broker refuses the switch with
    /// 406).
    pub async fn tx_select(&self) -> Result<(), OpError> {
        self.expect_ok(Method::TxSelect {}).await
    }

    /// Commit the transaction buffer (two-phase prepare/commit across the
    /// involved shards, methods/tx.rs:76-91).
    pub async fn tx_commit(&self) -> Result<(), OpError> {
        self.expect_ok(Method::TxCommit {}).await
    }

    /// Roll the transaction buffer back: buffered publishes vanish,
    /// buffered acks leave their messages unacked/still held (§2.2.9).
    pub async fn tx_rollback(&self) -> Result<(), OpError> {
        self.expect_ok(Method::TxRollback {}).await
    }

    // ------------------------------------------------------------------
    // internals
    // ------------------------------------------------------------------

    async fn expect_ok(&self, m: Method) -> Result<(), OpError> {
        match call(&self.shared, &m).await? {
            (
                Method::QueueBindOk {}
                | Method::QueueUnbindOk {}
                | Method::ExchangeDeclareOk {}
                | Method::ExchangeDeleteOk {}
                | Method::ExchangeBindOk {}
                | Method::ExchangeUnbindOk {}
                | Method::BasicQosOk {}
                | Method::BasicCancelOk { .. }
                | Method::TxSelectOk {}
                | Method::TxCommitOk {}
                | Method::TxRollbackOk {}
                | Method::ConfirmSelectOk {},
                _,
            ) => Ok(()),
            (other, _) => Err(unexpected_reply(&other)),
        }
    }
}

fn unexpected_reply(m: &Method) -> OpError {
    OpError::Io(std::io::Error::other(format!(
        "unexpected reply {} to a client call",
        m.name()
    )))
}

impl std::fmt::Debug for EmbeddedChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedChannel")
            .field("id", &self.shared.id)
            .field(
                "open",
                &self.shared.valid.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}
