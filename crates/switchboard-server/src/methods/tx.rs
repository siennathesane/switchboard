use std::collections::BTreeMap;
use std::sync::Arc;

use switchboard_cluster::ClusterNode;
use switchboard_core::error::BrokerError;
use switchboard_core::shard::ShardCmd;
use switchboard_core::topology::GroupId;
use switchboard_wire::method::Method;

use crate::channel::Channel;
use crate::channel::ChannelResult;
use crate::methods::shard_call;
use crate::outbound::OutboundFrame;

use switchboard_cluster::BrokerCommand;
use switchboard_core::shard::TxOp;
use crate::channel::TxOp as ChannelTxOp;
use crate::channel::LocalConsumer;
use crate::channel::Unacked;
use switchboard_core::model::SubscriptionId;

impl Channel {
        pub(crate) async fn commit(&self, node: &Arc<ClusterNode>) -> ChannelResult<()> {
        let (ops, tx_id) = {
            let mut inner = self.inner.lock().unwrap();
            let conn = inner.conn;
            let Some(tx) = inner.tx.as_mut() else {
                return Err(BrokerError::precondition_failed("channel is not transacted")
                    .for_method(90, 20));
            };
            let ops = std::mem::take(&mut tx.ops);
            tx.counter += 1;
            // Unique per (node, connection, commit): prepared state on the
            // shards is keyed by this id.
            let tx_id: (u64, u64) = (conn.node, conn.conn.wrapping_mul(1_000_003).wrapping_add(tx.counter));
            (ops, tx_id)
        };
        if ops.is_empty() {
            return Ok(());
        }

        // Group ops per shard for the two-phase commit.
        let mut per_shard: BTreeMap<GroupId, Vec<TxOp>> = BTreeMap::new();
        for op in &ops {
            match op {
                ChannelTxOp::Publish {
                    queue,
                    shard,
                    message,
                    ..
                } => {
                    per_shard
                        .entry(*shard)
                        .or_default()
                        .push(TxOp::Enqueue {
                            queue: queue.clone(),
                            message: message.clone(),
                        });
                }
                ChannelTxOp::Ack {
                    queue,
                    shard,
                    seq,
                } => {
                    per_shard
                        .entry(*shard)
                        .or_default()
                        .push(TxOp::Ack {
                            queue: queue.clone(),
                            seq: *seq,
                        });
                }
            }
        }

        // Phase 1: prepare on every involved shard.
        for (shard, shard_ops) in &per_shard {
            shard_call(
                node,
                *shard,
                ShardCmd::PrepareTx {
                    tx: tx_id,
                    ops: shard_ops.clone(),
                },
            )
            .await?;
        }
        // Phase 2: commit everywhere (apply is idempotent on re-commit).
        for shard in per_shard.keys() {
            shard_call(node, *shard, ShardCmd::CommitTx { tx: tx_id }).await?;
        }

        // Transacted publishes confirm at commit, each with the
        // publisher-confirm number it was allocated at publish time.
        if self.inner.lock().unwrap().confirm.is_some() {
            for op in ops {
                if let ChannelTxOp::Publish {
                    confirm_seq: Some(seq),
                    ..
                } = op
                {
                    let _ = self
                        .outbound
                        .send(OutboundFrame::Method {
                            channel: self.id,
                            method: Method::BasicAck {
                                delivery_tag: seq,
                                multiple: false,
                            },
                        });
                }
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Teardown
    // ------------------------------------------------------------------

    /// Cancel consumers and release holds. Called on channel close and
    /// connection teardown (§4.5: unacked messages are marked for
    pub async fn teardown(&self, node: &Arc<ClusterNode>) {
        let consumers: Vec<LocalConsumer> = {
            let mut inner = self.inner.lock().unwrap();
            inner.open = false;
            inner.consumers.values().cloned().collect()
        };
        for c in consumers {
            node.detach_consumer(c.sub).await;
            let _ = node
                .write(
                    c.shard,
                    BrokerCommand::Shard(ShardCmd::UnregisterSubscription { sub: c.sub }),
                )
                .await;
        }
        // Release remaining holds (gets and everything not consumer-tied).
        let holds: Vec<(GroupId, String, Option<SubscriptionId>, Vec<u64>)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .unacked
                .values()
                .map(|u| (u.shard, u.queue.clone(), u.sub, vec![u.seq]))
                .collect()
        };
        let requeues: Vec<(u64, Unacked)> = holds
            .into_iter()
            .flat_map(|(shard, queue, sub, seqs)| {
                seqs.into_iter()
                    .map(move |seq| {
                        (
                            0,
                            Unacked {
                                queue: queue.clone(),
                                shard,
                                seq,
                                sub,
                                consumer_tag: None,
                            },
                        )
                    })
            })
            .collect();
        let _ = self.settle_requeues(node, &requeues).await;
        self.inner.lock().unwrap().unacked.clear();
    }
}
