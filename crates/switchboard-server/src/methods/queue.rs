use std::sync::Arc;

use switchboard_cluster::ClusterNode;
use switchboard_core::error::BrokerError;
use switchboard_core::shard::ShardCmd;
use switchboard_core::shard::ShardReply;

use crate::channel::Channel;
use crate::channel::ChannelResult;
use crate::methods::shard_call;

use switchboard_core::model::QueueOptions;
use switchboard_core::topology::MetaCmd;
use switchboard_core::topology::MetaReply;
use crate::methods::inner_vhost;
use crate::methods::meta_call;
use switchboard_core::topology::GroupId;

impl Channel {
        pub(crate) async fn queue_declare(
        &self,
        node: &Arc<ClusterNode>,
        name: &str,
        passive: bool,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        arguments: switchboard_wire::field::FieldTable,
    ) -> ChannelResult<(String, u32, u32)> {
        let options = QueueOptions {
            durable,
            exclusive,
            auto_delete,
            arguments,
        };
        let owner = self.inner.lock().unwrap().conn;
        let vhost_name = inner_vhost(self);
        let policy = switchboard_core::shard::QueuePolicy::from_arguments(&options.arguments);
        let reply = meta_call(
            node,
            MetaCmd::DeclareQueue {
                vhost: vhost_name,
                name: name.to_string(),
                passive,
                options,
                owner,
            },
        )
        .await?;
        let MetaReply::QueueDeclared {
            name: final_name,
            shard,
            created,
        } = reply
        else {
            return Err(BrokerError::resource_error("unexpected declare reply"));
        };
        if created {
            shard_call(
                node,
                shard,
                ShardCmd::CreateQueueData {
                    queue: final_name.clone(),
                    policy,
                },
            )
            .await?;
            Ok((final_name, 0, 0))
        } else {
            // Live counts come from the owning shard (linearizable read).
            match Self::stats_lenient(node, shard, &final_name).await {
                Ok(ShardReply::Stats {
                    depth,
                    consumer_count,
                }) => Ok((final_name, depth, consumer_count)),
                Ok(_) => Ok((final_name, 0, 0)),
                // Lost-drive heal: the queue exists in meta but the creating
                // node's shard-side `CreateQueueData` never landed — it was
                // paused or killed in the window between the meta commit
                // and the shard write (chaos / restart). Nothing re-drives
                // `CreateQueueData` from the meta state machine (an existing
                // declare replies `created: false` forever), so the queue
                // would 404 "on this shard" for every subsequent passive
                // declare, get, publish and delete — permanently. Re-driving
                // it here is safe: the shard SM's `CreateQueueData` is
                // idempotent (`entry().or_insert_with`) and raft-durable, so
                // the repair survives restarts. An active declare asserts
                // usability (§4.8's declaration contract), so it may repair;
                // a passive declare stays assertion-only and still 404s.
                Err(e) if !passive && e.code == 404 => {
                    tracing::warn!(
                        queue = %final_name,
                        shard,
                        "queue exists in meta without shard-side data; re-driving CreateQueueData"
                    );
                    shard_call(
                        node,
                        shard,
                        ShardCmd::CreateQueueData {
                            queue: final_name.clone(),
                            policy,
                        },
                    )
                    .await?;
                    Ok((final_name, 0, 0))
                }
                Err(e) => Err(e),
            }
        }
    }

    pub(crate) async fn queue_purge(&self, node: &Arc<ClusterNode>, queue: &str) -> ChannelResult<u32> {
        let shard = self.queue_shard(node, queue).await?;
        match shard_call(node, shard, ShardCmd::Purge { queue: queue.to_string() }).await? {
            ShardReply::Purged { message_count } => Ok(message_count),
            other => Err(BrokerError::resource_error(format!("unexpected purge reply {other:?}"))),
        }
    }

    pub(crate) async fn queue_delete(
        &self,
        node: &Arc<ClusterNode>,
        queue: &str,
        if_unused: bool,
        if_empty: bool,
    ) -> ChannelResult<u32> {
        let shard = self.queue_shard(node, queue).await?;
        // Linearizable stats feed the if_empty/if_unused assertions, which
        // the meta state machine re-checks under raft. A 404 from the shard
        // means its queue data is missing — the lost-drive state (meta
        // committed, the creating node died before `CreateQueueData` landed)
        // or the create-race window. Either way the queue provably holds
        // zero messages (an enqueue requires queue data) and zero consumers
        // (a subscription registration requires queue data), so the
        // assertions are vacuously satisfied and the delete must proceed —
        // otherwise the queue could never be deleted by anyone.
        let (depth, consumers) = match Self::stats_lenient(node, shard, queue).await {
            Ok(ShardReply::Stats {
                depth,
                consumer_count,
            }) => (depth, consumer_count),
            Ok(_) => (0, 0),
            Err(e) if e.code == 404 => (0, 0),
            Err(e) => return Err(e),
        };
        let reply = meta_call(
            node,
            MetaCmd::DeleteQueue {
                vhost: inner_vhost(self),
                name: queue.to_string(),
                if_unused,
                if_empty,
                depth: depth as u64,
                consumers,
            },
        )
        .await?;
        let MetaReply::QueueDeleted { message_count } = reply else {
            return Err(BrokerError::resource_error("unexpected delete reply"));
        };
        // Drop shard-side data (also cancels consumers via effects).
        let _ = shard_call(
            node,
            shard,
            ShardCmd::DeleteQueueData {
                queue: queue.to_string(),
            },
        )
        .await;
        Ok(message_count)
    }

    pub(crate) async fn queue_shard(
        &self,
        node: &Arc<ClusterNode>,
        queue: &str,
    ) -> ChannelResult<GroupId> {
        let topo = node.topology();
        if let Some(shard) = topo
            .vhosts
            .get(&inner_vhost(self))
            .and_then(|v| v.queues.get(queue))
            .map(|qi| qi.shard)
        {
            return Ok(shard);
        }
        // This node's cached topology can lag a just-replicated declare by
        // up to the reconciliation interval; a queue whose declare-ok was
        // already returned to some client must not 404 here. Refresh once
        // and retry before reporting not-found.
        node.refresh_topology().await;
        let topo = node.topology();
        topo.vhosts
            .get(&inner_vhost(self))
            .and_then(|v| v.queues.get(queue))
            .map(|qi| qi.shard)
            .ok_or_else(|| BrokerError::not_found(format!("no queue {queue:?}")).channel_level())
    }

    /// Shard stats with a bounded retry for the declare race: the meta
    /// write may have applied here before the creating node's shard-side
    /// `CreateQueueData` landed, and the linearizing stats read would
    /// otherwise see a 404 for a queue that exists.
    async fn stats_lenient(
        node: &Arc<ClusterNode>,
        shard: GroupId,
        queue: &str,
    ) -> ChannelResult<ShardReply> {
        const TRIES: usize = 8;
        for attempt in 0..TRIES {
            match shard_call(node, shard, ShardCmd::Stats { queue: queue.to_string() }).await {
                Err(e) if e.code == 404 && attempt + 1 < TRIES => {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                other => return other,
            }
        }
        unreachable!("loop always returns")
    }

    // ------------------------------------------------------------------
    // Transactions
    // ------------------------------------------------------------------
}
