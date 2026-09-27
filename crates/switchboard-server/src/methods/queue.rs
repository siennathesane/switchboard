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
            let (depth, consumers) = match shard_call(
                node,
                shard,
                ShardCmd::Stats {
                    queue: final_name.clone(),
                },
            )
            .await?
            {
                ShardReply::Stats {
                    depth,
                    consumer_count,
                } => (depth, consumer_count),
                _ => (0, 0),
            };
            Ok((final_name, depth, consumers))
        }
    }

    pub(crate) async fn queue_purge(&self, node: &Arc<ClusterNode>, queue: &str) -> ChannelResult<u32> {
        let shard = self.queue_shard(node, queue)?;
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
        let shard = self.queue_shard(node, queue)?;
        // Linearizable stats feed the if_empty/if_unused assertions, which
        // the meta state machine re-checks under raft.
        let (depth, consumers) = match shard_call(
            node,
            shard,
            ShardCmd::Stats {
                queue: queue.to_string(),
            },
        )
        .await?
        {
            ShardReply::Stats {
                depth,
                consumer_count,
            } => (depth, consumer_count),
            _ => (0, 0),
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

    pub(crate) fn queue_shard(&self, node: &Arc<ClusterNode>, queue: &str) -> ChannelResult<GroupId> {
        let topo = node.topology();
        topo.vhosts
            .get(&inner_vhost(self))
            .and_then(|v| v.queues.get(queue))
            .map(|qi| qi.shard)
            .ok_or_else(|| BrokerError::not_found(format!("no queue {queue:?}")).channel_level())
    }

    // ------------------------------------------------------------------
    // Transactions
    // ------------------------------------------------------------------
}
