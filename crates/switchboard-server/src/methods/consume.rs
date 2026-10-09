use std::sync::Arc;
use tokio::sync::mpsc;

use switchboard_cluster::ClusterNode;
use switchboard_core::error::BrokerError;
use switchboard_core::model::SubscriptionId;
use switchboard_core::shard::ShardCmd;
use switchboard_wire::method::Method;

use crate::channel::Channel;
use crate::channel::ChannelResult;
use crate::methods::inner_vhost;
use crate::methods::send_frame;
use crate::methods::shard_call;
use crate::methods::shard_call_lenient;
use crate::outbound::OutboundFrame;

use crate::channel::LocalConsumer;
use switchboard_cluster::Delivery;
use crate::channel::Unacked;
use crate::outbound::message_frames;
use switchboard_cluster::ConsumerSink;
use switchboard_core::model::ConnectionId;
use switchboard_core::topology::GroupId;

impl Channel {
        pub(crate) async fn consume(
        &self,
        node: &Arc<ClusterNode>,
        queue: &str,
        requested_tag: &str,
        no_ack: bool,
        exclusive: bool,
    ) -> ChannelResult<String> {
        // The local topology snapshot can lag a just-replicated declare by
        // up to the reconciliation interval; refresh once before giving up.
        let resolve = |node: &Arc<ClusterNode>| {
            node.topology()
                .vhosts
                .get(&inner_vhost(self))
                .and_then(|v| v.queues.get(queue))
                .cloned()
        };
        let qi = match resolve(node) {
            Some(qi) => qi,
            None => {
                node.refresh_topology().await;
                resolve(node).ok_or_else(|| {
                    BrokerError::not_found(format!(
                        "no queue {queue:?} in vhost {:?}",
                        inner_vhost(self)
                    ))
                    .channel_level()
                })?
            }
        };
        let shard = qi.shard;
        let (prefetch, prefetch_size) = {
            let inner = self.inner.lock().unwrap();
            (inner.prefetch_count, inner.prefetch_size)
        };
        let node_id = self.inner.lock().unwrap().node_id;

        let sub = node.next_subscription_id();
        let conn: ConnectionId = self.inner.lock().unwrap().conn;
        let queue_owned = queue.to_string();

        let (dtx, drx) = mpsc::unbounded_channel::<Delivery>();
        let (ctx, crx) = mpsc::unbounded_channel::<String>();
        node.attach_consumer(sub, ConsumerSink { deliveries: dtx, cancelled: ctx }).await;

        let tag = {
            let mut inner = self.inner.lock().unwrap();
            let tag = if requested_tag.is_empty() {
                format!("ct-{}", inner.next_delivery_tag)
            } else {
                requested_tag.to_string()
            };
            inner.consumers.insert(
                tag.clone(),
                LocalConsumer {
                    tag: tag.clone(),
                    queue: queue.to_string(),
                    sub,
                    shard,
                    no_ack,
                    exclusive,
                    prefetch,
                    flow_active: true,
                },
            );
            inner.sub_shards.insert(sub, shard);
            tag
        };

        shard_call_lenient(
            node,
            shard,
            ShardCmd::RegisterSubscription {
                sub,
                queue: queue.to_string(),
                node: node_id,
                consumer_tag: tag.clone(),
                no_ack,
                exclusive,
                conn,
                byte_limit: u64::from(prefetch_size),
            },
        )
        .await?;

        // Initial pull window (§3.1.7): prefetch or a bounded batch.
        let credit = crate::channel::Channel::initial_credit(prefetch);
        if credit > 0 {
            shard_call(node, shard, ShardCmd::Credit { sub, count: credit }).await?;
        }

        // The pump ships shard effects to the socket as basic.deliver.
        let pump_channel = self.clone();
        let node2 = node.clone();
        let _tag_out = tag.clone();
        let pump_tag = tag.clone();
        tokio::spawn(async move {
            delivery_pump(
                pump_channel,
                node2,
                LocalConsumer {
                    tag: pump_tag,
                    queue: queue_owned,
                    sub,
                    shard,
                    no_ack,
                    exclusive,
                    prefetch,
                    flow_active: true,
                },
                drx,
                crx,
            )
            .await;
        });

        Ok(tag)
    }

    pub(crate) async fn cancel(&self, node: &Arc<ClusterNode>, consumer_tag: &str) -> ChannelResult<()> {
        let consumer = self.inner.lock().unwrap().consumers.remove(consumer_tag);
        let Some(c) = consumer else {
            // Unknown tag: RabbitMQ answers cancel-ok anyway.
            return Ok(());
        };
        node.detach_consumer(c.sub).await;
        shard_call(
            node,
            c.shard,
            ShardCmd::UnregisterSubscription { sub: c.sub },
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn set_flow(&self, node: &Arc<ClusterNode>, active: bool) -> ChannelResult<()> {
        let subs: Vec<(SubscriptionId, GroupId)> = {
            let inner = self.inner.lock().unwrap();
            inner.consumers.values().map(|c| (c.sub, c.shard)).collect()
        };
        for (sub, shard) in subs {
            shard_call(node, shard, ShardCmd::Flow { sub, active }).await?;
            let mut inner = self.inner.lock().unwrap();
            if let Some(c) = inner.consumers.values_mut().find(|c| c.sub == sub) {
                c.flow_active = active;
            }
        }
        Ok(())
    }

}

/// The delivery pump: shard effects → basic.deliver frames for one
/// consumer, plus consumer-cancellation notifications.
pub(crate) async fn delivery_pump(
    channel: Channel,
    node: Arc<ClusterNode>,
    consumer: LocalConsumer,
    mut rx: mpsc::UnboundedReceiver<Delivery>,
    mut cancelled: mpsc::UnboundedReceiver<String>,
) {
    // No-ack consumers never ack, so their credit only comes back from
    // here: refund it as deliveries leave for the wire (RabbitMQ's
    // unlimited-flow semantics for `no-ack`). Refunded in batches so the
    // pump's write rate is per-chunk, not per-message.
    let mut uncredited: u32 = 0;
    let credit_chunk = 64u32;
    loop {
        tokio::select! {
            d = rx.recv() => {
                let Some(d) = d else { break };
                let frames = {
                    let mut inner = channel.inner.lock().unwrap();
                    if !inner.open || !inner.flow_active {
                        // Flow-controlled: the message stays held in the
                        // shard until flow resumes (we did not consume it).
                        continue;
                    }
                    let tag = inner.allocate_delivery_tag();
                    if !consumer.no_ack && !d.deleted {
                        inner.unacked.insert(
                            tag,
                            Unacked {
                                queue: d.queue.clone(),
                                shard: consumer.shard,
                                seq: d.seq,
                                sub: Some(consumer.sub),
                                consumer_tag: Some(consumer.tag.clone()),
                            },
                        );
                    }
                    let m = Method::BasicDeliver {
                        consumer_tag: consumer.tag.clone(),
                        delivery_tag: tag,
                        redelivered: d.redelivered,
                        exchange: d.message.exchange.clone(),
                        routing_key: d.message.routing_key.clone(),
                    };
                    let fm = inner.limits.frame_max;
                    message_frames(channel.id, m, d.message.properties.clone(), &d.message.body, fm)
                };
                for f in frames {
                    if send_frame(&channel, f).is_err() {
                        return;
                    }
                }
                if consumer.no_ack {
                    uncredited += 1;
                    if uncredited >= credit_chunk {
                        // Fire and forget: a lost refund only delays the
                        // next chunk (the consumer's window absorbs it);
                        // the consumer is gone if this fails repeatedly.
                        let node = node.clone();
                        let sub = consumer.sub;
                        let shard = consumer.shard;
                        tokio::spawn(async move {
                            let _ = shard_call(
                                &node,
                                shard,
                                ShardCmd::Credit { sub, count: credit_chunk },
                            )
                            .await;
                        });
                        uncredited = 0;
                    }
                }
            }
            c = cancelled.recv() => {
                let Some(tag) = c else { break };
                channel.inner.lock().unwrap().consumers.remove(&tag);
                // Consumer-cancellation notification (advertised in
                // Connection.Start capabilities).
                let _ = channel.outbound.send(OutboundFrame::Method {
                    channel: channel.id,
                    method: Method::BasicCancel {
                        consumer_tag: tag,
                        nowait: true,
                    },
                });
            }
        }
    }
}
