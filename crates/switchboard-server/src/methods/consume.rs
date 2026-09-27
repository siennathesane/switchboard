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
        let topo = node.topology();
        let Some(qi) = topo
            .vhosts
            .get(&inner_vhost(self))
            .and_then(|v| v.queues.get(queue))
            .cloned()
        else {
            return Err(BrokerError::not_found(format!(
                "no queue {queue:?} in vhost {:?}",
                inner_vhost(self)
            ))
            .channel_level());
        };
        let shard = qi.shard;
        let (prefetch, prefetch_size) = {
            let inner = self.inner.lock().unwrap();
            (inner.prefetch_count, inner.prefetch_size)
        };
        let node_id = self.inner.lock().unwrap().node_id;

        let sub = SubscriptionId {
            node: node_id,
            sub: self
                .sub_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        };
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

        shard_call(
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
async fn delivery_pump(
    channel: Channel,
    _node: Arc<ClusterNode>,
    consumer: LocalConsumer,
    mut rx: mpsc::UnboundedReceiver<Delivery>,
    mut cancelled: mpsc::UnboundedReceiver<String>,
) {
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
