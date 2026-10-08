//! The client→server method implementations for a channel.
//!
//! Everything the AMQP functional layer promises happens here: declare
//! assertions (§4.8's assertion model), publish routing (§3.1.3), consumer
//! registration with credit windows (§3.1.7), acknowledgements and
//! redelivery (§3.1.8), flow control (§3.1.9), transactions (§2.2.9), and
//! publisher confirms (the deployed extension advertised in
//! `Connection.Start` capabilities).
//!
//! Cluster calls funnel through [`shard_call`] / [`meta_call`]; broker-level
//! rejections (404, 406, ...) pass through as channel exceptions, transport
//! trouble becomes a 506.

use std::sync::Arc;

use switchboard_cluster::BrokerCommand;
use switchboard_cluster::ClusterNode;
use switchboard_core::error::BrokerError;
use switchboard_core::model::ExchangeKind;
use switchboard_core::topology::MetaCmd;
use switchboard_core::shard::ShardCmd;
use switchboard_core::shard::ShardReply;
use switchboard_core::topology::GroupId;
use switchboard_core::topology::MetaReply;
use switchboard_wire::method::Method;

use crate::channel::Channel;
use crate::outbound::OutboundFrame;
use crate::channel::ChannelResult;

/// Convert a cluster error into the closest broker exception: broker-level
mod basic;
mod consume;
mod publish;
mod queue;
mod tx;

fn ce(e: switchboard_cluster::ClusterError) -> BrokerError {
    match e {
        switchboard_cluster::ClusterError::Broker(b) => b,
        other => BrokerError::resource_error(other.to_string()).channel_level(),
    }
}

/// Apply one shard command through the (possibly forwarding) write path.
async fn shard_call(
    node: &Arc<ClusterNode>,
    shard: GroupId,
    cmd: ShardCmd,
) -> ChannelResult<ShardReply> {
    let reply = node.write(shard, BrokerCommand::Shard(cmd)).await.map_err(ce)?;
    tracing::debug!(node = node.id, shard, reply = ?reply, "shard_call");
    Channel::shard_reply(reply)
}

/// Apply one meta command. The local topology view is refreshed before the
/// reply so the acting connection immediately observes its own change
/// (§4.4 visibility guarantee); other nodes converge on their next
/// reconciliation tick.
async fn meta_call(
    node: &Arc<ClusterNode>,
    cmd: MetaCmd,
) -> ChannelResult<MetaReply> {
    let reply = node.write(switchboard_cluster::META_GROUP, BrokerCommand::Meta(cmd)).await.map_err(ce)?;
    let out = Channel::meta_reply(reply)?;
    node.refresh_topology().await;
    Ok(out)
}

pub(crate) fn send_frame(ch: &Channel, f: OutboundFrame) -> ChannelResult<()> {
    ch.outbound
        .send(f)
        .map_err(|_| BrokerError::resource_error("connection is closing"))
}

pub(crate) fn inner_vhost(ch: &Channel) -> String {
    ch.inner.lock().unwrap().vhost.clone()
}

/// Queue a reply method (lock-free: `Channel::reply` uses the outer
/// immutable fields).
pub(crate) fn reply_method(ch: &Channel, m: Method) {
    ch.reply(m);
}

/// The current publish sequence number (confirm mode only); allocated at
/// publish start so confirms match publish order.
fn take_confirm_seq(ch: &Channel) -> ChannelResult<u64> {
    let mut inner = ch.inner.lock().unwrap();
    if inner.confirm.is_none() {
        return Ok(0);
    }
    // Confirms number publishes in their OWN per-channel sequence,
    // independent of consumer delivery tags (§4.2.4 / RabbitMQ
    // semantics: the first confirmed publish is 1).
    inner.confirm_seq += 1;
    Ok(inner.confirm_seq)
}

impl Channel {
    /// Dispatch one client→server method. Ok(()) continues the channel;
    /// errors close the channel (§4.8.1) or connection (§4.8.2) by level.
    pub async fn handle(&self, node: &Arc<ClusterNode>, m: Method) -> ChannelResult<()> {
        match m {
            // ---- basic ----
            Method::BasicQos { prefetch_size, prefetch_count, global_ } => {
                // §3.1.7: prefetch_size windows unacknowledged BYTES,
                // prefetch_count windows unacknowledged messages. The
                // window applies per-consumer (global semantics affect
                // new consumers only, matching most servers).
                let mut inner = self.inner.lock().unwrap();
                inner.prefetch_size = u64::from(prefetch_size);
                inner.prefetch_count = u32::from(prefetch_count);
                inner.prefetch_global = global_;
                reply_method(self, Method::BasicQosOk {});
                Ok(())
            }
            Method::BasicConsume {
                queue,
                consumer_tag,
                no_ack,
                exclusive,
                nowait,
                ..
            } => {
                let tag = self.consume(node, &queue, &consumer_tag, no_ack, exclusive).await?;
                if !nowait {
                    reply_method(self, Method::BasicConsumeOk { consumer_tag: tag });
                }
                Ok(())
            }
            Method::BasicCancel { consumer_tag, nowait } => {
                self.cancel(node, &consumer_tag).await?;
                if !nowait {
                    reply_method(self, Method::BasicCancelOk { consumer_tag });
                }
                Ok(())
            }
            Method::BasicGet { queue, no_ack, .. } => self.get(node, &queue, no_ack).await,
            Method::BasicAck { delivery_tag, multiple } => {
                self.ack(node, delivery_tag, multiple).await
            }
            Method::BasicReject { delivery_tag, requeue } => {
                self.nack(node, delivery_tag, false, requeue).await
            }
            Method::BasicNack { delivery_tag, multiple, requeue } => {
                self.nack(node, delivery_tag, multiple, requeue).await
            }
            Method::BasicRecoverAsync { requeue } => self.recover(node, requeue).await,
            Method::BasicRecover { requeue } => self.recover(node, requeue).await,
            Method::BasicPublish { .. } => {
                // Content assembly starts here; session completes it.
                self.inner.lock().unwrap().pending_method = Some(m);
                Ok(())
            }

            // ---- exchange ----
            Method::ExchangeDeclare {
                exchange,
                exchange_type,
                passive,
                durable,
                auto_delete,
                internal,
                nowait,
                arguments,
                ..
            } => {
                let Some(kind) = ExchangeKind::from_str(&exchange_type) else {
                    return Err(if exchange_type.starts_with("x-") {
                        BrokerError::not_implemented(format!(
                            "exchange type {exchange_type:?} is not implemented"
                        ))
                        .for_method(40, 10)
                        .channel_level()
                    } else {
                        BrokerError::command_invalid_channel(format!(
                            "exchange type {exchange_type:?} is invalid"
                        ))
                        .for_method(40, 10)
                    });
                };
                meta_call(
                    node,
                    MetaCmd::DeclareExchange {
                        vhost: inner_vhost(self),
                        name: exchange,
                        kind,
                        passive,
                        durable,
                        auto_delete,
                        internal,
                        arguments,
                    },
                )
                .await?;
                if !nowait {
                    reply_method(self, Method::ExchangeDeclareOk {});
                }
                Ok(())
            }
            Method::ExchangeDelete { exchange, if_unused, nowait, .. } => {
                meta_call(
                    node,
                    MetaCmd::DeleteExchange {
                        vhost: inner_vhost(self),
                        name: exchange,
                        if_unused,
                    },
                )
                .await?;
                if !nowait {
                    reply_method(self, Method::ExchangeDeleteOk {});
                }
                Ok(())
            }
            Method::ExchangeBind {
                destination,
                source,
                routing_key,
                nowait,
                arguments,
                ..
            } => {
                meta_call(
                    node,
                    MetaCmd::Bind {
                        vhost: inner_vhost(self),
                        exchange: source,
                        queue: destination,
                        routing_key,
                        arguments,
                    },
                )
                .await?;
                if !nowait {
                    reply_method(self, Method::ExchangeBindOk {});
                }
                Ok(())
            }
            Method::ExchangeUnbind {
                destination,
                source,
                routing_key,
                nowait,
                arguments,
                ..
            } => {
                meta_call(
                    node,
                    MetaCmd::Unbind {
                        vhost: inner_vhost(self),
                        exchange: source,
                        queue: destination,
                        routing_key,
                        arguments,
                    },
                )
                .await?;
                if !nowait {
                    reply_method(self, Method::ExchangeUnbindOk {});
                }
                Ok(())
            }

            // ---- queue ----
            Method::QueueDeclare {
                queue,
                passive,
                durable,
                exclusive,
                auto_delete,
                nowait,
                arguments,
                ..
            } => {
                let name = if queue.is_empty() {
                    switchboard_core::model::generate_queue_name()
                } else {
                    queue
                };
                let (name, depth, consumers) = self
                    .queue_declare(node, &name, passive, durable, exclusive, auto_delete, arguments)
                    .await?;
                if !nowait {
                    reply_method(
                        self,
                        Method::QueueDeclareOk { queue: name, message_count: depth, consumer_count: consumers },
                    );
                }
                Ok(())
            }
            Method::QueueBind {
                queue,
                exchange,
                routing_key,
                nowait,
                arguments,
                ..
            } => {
                meta_call(
                    node,
                    MetaCmd::Bind {
                        vhost: inner_vhost(self),
                        exchange,
                        queue,
                        routing_key,
                        arguments,
                    },
                )
                .await?;
                if !nowait {
                    reply_method(self, Method::QueueBindOk {});
                }
                Ok(())
            }
            Method::QueueUnbind {
                queue,
                exchange,
                routing_key,
                arguments,
                ..
            } => {
                meta_call(
                    node,
                    MetaCmd::Unbind {
                        vhost: inner_vhost(self),
                        exchange,
                        queue,
                        routing_key,
                        arguments,
                    },
                )
                .await?;
                // Queue.Unbind has no nowait flag: always reply (§3.2.2).
                reply_method(self, Method::QueueUnbindOk {});
                Ok(())
            }
            Method::QueuePurge { queue, nowait, .. } => {
                let count = self.queue_purge(node, &queue).await?;
                if !nowait {
                    reply_method(self, Method::QueuePurgeOk { message_count: count });
                }
                Ok(())
            }
            Method::QueueDelete {
                queue,
                if_unused,
                if_empty,
                nowait,
                ..
            } => {
                let count = self.queue_delete(node, &queue, if_unused, if_empty).await?;
                if !nowait {
                    reply_method(self, Method::QueueDeleteOk { message_count: count });
                }
                Ok(())
            }

            // ---- channel ----
            Method::ChannelFlow { active } => {
                self.set_flow(node, active).await?;
                reply_method(self, Method::ChannelFlowOk { active });
                Ok(())
            }

            // ---- tx ----
            Method::TxSelect {} => {
                let in_confirm = self.inner.lock().unwrap().confirm.is_some();
                if in_confirm {
                    return Err(BrokerError::precondition_failed(
                        "cannot switch from confirm to tx mode",
                    )
                    .for_method(90, 10));
                }
                {
                    let mut inner = self.inner.lock().unwrap();
                    if inner.tx.is_none() {
                        inner.tx = Some(crate::channel::TxState::default());
                    }
                }
                self.reply(Method::TxSelectOk {});
                Ok(())
            }
            Method::TxCommit {} => {
                self.commit(node).await?;
                reply_method(self, Method::TxCommitOk {});
                Ok(())
            }
            Method::TxRollback {} => {
                let mut inner = self.inner.lock().unwrap();
                let Some(tx) = inner.tx.as_mut() else {
                    return Err(
                        BrokerError::precondition_failed("channel is not transacted")
                            .for_method(90, 30)
                    );
                };
                // Rollback drops the buffer: publishes were never sent and
                // buffered acks leave their messages unacked — exactly
                // §2.2.9's rule ("a rollback does not requeue or redeliver").
                tx.ops.clear();
                reply_method(self, Method::TxRollbackOk {});
                Ok(())
            }

            // ---- confirm ----
            Method::ConfirmSelect { .. } => {
                let mut inner = self.inner.lock().unwrap();
                if inner.tx.is_some() {
                    return Err(BrokerError::precondition_failed(
                        "cannot switch from tx to confirm mode",
                    )
                    .for_method(85, 10));
                }
                if inner.confirm.is_none() {
                    inner.confirm = Some(crate::channel::ConfirmState::default());
                }
                reply_method(self, Method::ConfirmSelectOk {});
                Ok(())
            }

            other => Err(BrokerError::command_invalid(format!(
                "method {} cannot be sent by a client here",
                other.name()
            ))
            .for_method(other.class_id(), other.method_id())),
        }
    }

}
