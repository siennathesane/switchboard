use std::collections::BTreeMap;
use std::sync::Arc;

use switchboard_cluster::ClusterNode;
use switchboard_core::error::BrokerError;
use switchboard_core::model::SubscriptionId;
use switchboard_core::shard::ShardCmd;
use switchboard_core::shard::ShardReply;
use switchboard_core::topology::GroupId;
use switchboard_wire::method::Method;

use crate::channel::Channel;
use crate::channel::ChannelResult;
use crate::methods::reply_method;
use crate::methods::send_frame;
use crate::methods::shard_call;
use crate::outbound::OutboundFrame;

use crate::channel::Unacked;
use crate::channel::TxOp as ChannelTxOp;
use crate::outbound::message_frames;
use std::collections::BTreeSet;

impl Channel {
        pub(crate) async fn get(&self, node: &Arc<ClusterNode>, queue: &str, no_ack: bool) -> ChannelResult<()> {
        let shard = self.queue_shard(node, queue).await?;
        let get_id = self
            .sub_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let reply = shard_call(
            node,
            shard,
            ShardCmd::Get {
                queue: queue.to_string(),
                no_ack,
                get_id,
            },
        )
        .await?;

        let channel = self.id;
        match reply {
            ShardReply::Got {
                seq,
                redelivered,
                depth,
                message,
            } => {
                let tag = self.inner.lock().unwrap().allocate_delivery_tag();
                if !no_ack {
                    let mut inner = self.inner.lock().unwrap();
                    inner.unacked.insert(
                        tag,
                        Unacked {
                            queue: queue.to_string(),
                            shard,
                            seq,
                            sub: Some(SubscriptionId {
                                node: switchboard_core::shard::GET_HOLDER_NODE,
                                sub: get_id,
                            }),
                            consumer_tag: None,
                        },
                    );
                }
                let m = Method::BasicGetOk {
                    delivery_tag: tag,
                    redelivered,
                    exchange: message.exchange.clone(),
                    routing_key: message.routing_key.clone(),
                    message_count: depth,
                };
                let fm = self.inner.lock().unwrap().limits.frame_max;
                for f in message_frames(channel, m, message.properties, &message.body, fm) {
                    send_frame(self, f)?;
                }
                Ok(())
            }
            ShardReply::GetEmpty { .. } => {
                send_frame(
                    self,
                    OutboundFrame::Method {
                        channel,
                        method: Method::BasicGetEmpty { cluster_id: String::new() },
                    },
                )
            }
            other => Err(BrokerError::resource_error(format!("unexpected get reply {other:?}"))),
        }
    }

    // ------------------------------------------------------------------
    // Acknowledgements
    // ------------------------------------------------------------------

    pub(crate) async fn ack(
        &self,
        node: &Arc<ClusterNode>,
        delivery_tag: u64,
        multiple: bool,
    ) -> ChannelResult<()> {
        // In tx mode acks join the transaction buffer (§2.2.9).
        let in_tx = self.inner.lock().unwrap().tx.is_some();
        if in_tx {
            let mut inner = self.inner.lock().unwrap();
            let tags: Vec<u64> = if multiple {
                Channel::multiple_range(&inner.unacked, delivery_tag)
            } else {
                vec![delivery_tag]
            };
            let mut taken: Vec<ChannelTxOp> = Vec::with_capacity(tags.len());
            for t in tags {
                if let Some(u) = inner.unacked.remove(&t) {
                    taken.push(ChannelTxOp::Ack {
                        queue: u.queue,
                        shard: u.shard,
                        seq: u.seq,
                    });
                }
            }
            if let Some(tx) = inner.tx.as_mut() {
                tx.ops.extend(taken);
            }
            return Ok(());
        }

        let resolved = self.take_unacked(delivery_tag, multiple);
        self.settle_acks(node, resolved).await
    }

    pub(crate) async fn nack(
        &self,
        node: &Arc<ClusterNode>,
        delivery_tag: u64,
        multiple: bool,
        requeue: bool,
    ) -> ChannelResult<()> {
        let resolved = self.take_unacked(delivery_tag, multiple);
        if requeue {
            // Back to ready; the pump re-hands them with redelivered set.
            self.settle_requeues(node, &resolved).await?;
        } else {
            // Dead-letter: remove and hand to the queue's DLX (if any) via
            // the shard's DeadLettered effects.
            self.settle_dead(node, &resolved).await?;
        }
        Ok(())
    }

    pub(crate) fn take_unacked(&self, delivery_tag: u64, multiple: bool) -> Vec<(u64, Unacked)> {
        let mut inner = self.inner.lock().unwrap();
        let tags = if multiple {
            Channel::multiple_range(&inner.unacked, delivery_tag)
        } else {
            vec![delivery_tag]
        };
        let mut out = Vec::with_capacity(tags.len());
        for t in tags {
            if let Some(u) = inner.unacked.remove(&t) {
                out.push((t, u));
            }
        }
        out
    }

    /// Acknowledge resolved deliveries on their shards and refill windows.
    pub(crate) async fn settle_acks(
        &self,
        node: &Arc<ClusterNode>,
        resolved: Vec<(u64, Unacked)>,
    ) -> ChannelResult<()> {
        // Group acks per (shard, queue).
        let mut per_queue: BTreeMap<(GroupId, String), BTreeSet<u64>> = BTreeMap::new();
        let mut credits: BTreeMap<SubscriptionId, (GroupId, u32)> = BTreeMap::new();
        for (_, u) in &resolved {
            per_queue
                .entry((u.shard, u.queue.clone()))
                .or_default()
                .insert(u.seq);
            if let Some(sub) = u.sub {
                credits
                    .entry(sub)
                    .or_insert((u.shard, 0))
                    .1 += 1;
            }
        }
        for ((shard, queue), seqs) in per_queue {
            shard_call(node, shard, ShardCmd::Ack { queue, seqs }).await?;
        }
        // Window refill: windowed consumers get exactly their acks back;
        // unlimited consumers keep their standing batch alive the same way.
        for (sub, (shard, n)) in credits {
            if n > 0 {
                shard_call(node, shard, ShardCmd::Credit { sub, count: n }).await?;
            }
        }
        Ok(())
    }

    /// Remove resolved deliveries, emitting dead-letter effects (the
    /// leader republishes them to the queue's DLX when configured).
    pub(crate) async fn settle_dead(
        &self,
        node: &Arc<ClusterNode>,
        resolved: &[(u64, Unacked)],
    ) -> ChannelResult<()> {
        // Every unacked delivery is held under a subscription (Basic.Get
        // uses the synthetic GET_HOLDER sub), so releases are sub-keyed.
        let mut by_shard: BTreeMap<GroupId, BTreeMap<String, BTreeMap<SubscriptionId, Vec<u64>>>> =
            BTreeMap::new();
        for (_, u) in resolved {
            if let Some(s) = u.sub {
                by_shard
                    .entry(u.shard)
                    .or_default()
                    .entry(u.queue.clone())
                    .or_default()
                    .entry(s)
                    .or_default()
                    .push(u.seq);
            }
        }
        for (shard, queues) in by_shard {
            for (queue, by_sub) in queues {
                for (sub, seqs) in by_sub {
                    shard_call(
                        node,
                        shard,
                        ShardCmd::Release {
                            queue: queue.clone(),
                            sub: Some(sub),
                            seqs,
                            dead: true,
                        },
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Return resolved deliveries to their queues (reject/nack-requeue).
    pub(crate) async fn settle_requeues(
        &self,
        node: &Arc<ClusterNode>,
        resolved: &[(u64, Unacked)],
    ) -> ChannelResult<()> {
        // Consumer holds release by sub (whole hold set of that seq).
        // Every unacked delivery carries a subscription holder (Basic.Get
        // uses the synthetic GET_HOLDER sub), so releases are sub-keyed.
        let mut by_shard: BTreeMap<GroupId, BTreeMap<String, BTreeMap<SubscriptionId, Vec<u64>>>> =
            BTreeMap::new();
        for (_, u) in resolved {
            if let Some(s) = u.sub {
                by_shard
                    .entry(u.shard)
                    .or_default()
                    .entry(u.queue.clone())
                    .or_default()
                    .entry(s)
                    .or_default()
                    .push(u.seq);
            }
        }
        for (shard, queues) in by_shard {
            for (queue, by_sub) in queues {
                for (sub, seqs) in by_sub {
                    shard_call(
                        node,
                        shard,
                        ShardCmd::Release {
                            queue: queue.clone(),
                            sub: Some(sub),
                            seqs,
                            dead: false,
                        },
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Basic.Recover (§3.2.2): redeliver all unacknowledged messages.
    pub(crate) async fn recover(&self, node: &Arc<ClusterNode>, _requeue: bool) -> ChannelResult<()> {
        let holds: Vec<(u64, Unacked)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .unacked
                .iter()
                .map(|(tag, u)| (*tag, u.clone()))
                .collect()
        };
        self.settle_requeues(node, &holds).await?;
        // Those holds were released; the channel forgets them and every
        // consumer window reopens.
        self.inner.lock().unwrap().unacked.clear();
        let credits: Vec<(GroupId, SubscriptionId, u32)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .consumers
                .values()
                .map(|c| (c.shard, c.sub, c.prefetch))
                .collect()
        };
        for (shard, sub, prefetch) in credits {
            let credit = crate::channel::Channel::initial_credit(prefetch);
            shard_call(node, shard, ShardCmd::Credit { sub, count: credit }).await?;
        }
        // §3.2.2: recover carries no nowait bit — Recover-Ok is mandatory.
        reply_method(self, Method::BasicRecoverOk {});
        Ok(())
    }

    // ------------------------------------------------------------------
    // Queues
    // ------------------------------------------------------------------
}
