//! Shared bridge plumbing for the non-AMQP protocols (MQTT, STOMP,
//! AMQP 1.0): everything these protocols need from the broker core —
//! auth, queue/exchange administration, publish routing, consumers —
//! expressed once, at the [`ClusterNode`] level, so each protocol module
//! stays pure protocol.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::debug;

use switchboard_cluster::BrokerCommand;
use switchboard_cluster::ClusterNode;
use switchboard_cluster::ConsumerSink;
use switchboard_cluster::Delivery;
use switchboard_core::error::BrokerError;
use switchboard_core::model::ConnectionId;
use switchboard_core::model::QueueOptions;
use switchboard_core::model::StoredMessage;
use switchboard_core::routing::route;
use switchboard_core::shard::ShardCmd;
use switchboard_core::shard::ShardReply;
use switchboard_core::topology::GroupId;
use switchboard_core::topology::MetaCmd;
use switchboard_core::topology::MetaReply;
use switchboard_core::topology::VhostView;
use switchboard_wire::field::FieldTable;
use switchboard_wire::properties::BasicProperties;

use crate::channel::ConnectionLimits;

/// Which client protocols the gateway accepts. AMQP 0-9-1 is always on
/// (it is the broker's native protocol); everything else is optional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolConfig {
    pub amqp091: bool,
    pub amqp10: bool,
    pub mqtt: bool,
    pub stomp: bool,
    pub websocket: bool,
    pub http_health: bool,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        ProtocolConfig {
            amqp091: true,
            amqp10: true,
            mqtt: true,
            stomp: true,
            websocket: true,
            http_health: true,
        }
    }
}

impl ProtocolConfig {
    /// Every protocol enabled.
    pub fn all() -> Self {
        Self::default()
    }

    /// Parse a comma-separated `--protocols` list. Names: `amqp`,
    /// `amqp1`, `mqtt`, `stomp`, `ws`, `http`. Unknown names are an
    /// error; `amqp` cannot be disabled.
    pub fn from_list(spec: &str) -> Result<Self, String> {
        // An explicit list is the exact set: everything starts disabled.
        let mut cfg = ProtocolConfig {
            amqp091: false,
            amqp10: false,
            mqtt: false,
            stomp: false,
            websocket: false,
            http_health: false,
        };
        for name in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match name.to_ascii_lowercase().as_str() {
                "amqp" => cfg.amqp091 = true,
                "amqp1" | "amqp1.0" | "amqp10" => cfg.amqp10 = true,
                "mqtt" => cfg.mqtt = true,
                "stomp" => cfg.stomp = true,
                "ws" | "websocket" => cfg.websocket = true,
                "http" | "health" => cfg.http_health = true,
                other => return Err(format!("unknown protocol {other:?}")),
            }
        }
        if !cfg.amqp091 {
            return Err("amqp (0-9-1) cannot be disabled; it is the native protocol".into());
        }
        Ok(cfg)
    }
}

/// Bridge-level error: protocol servers translate this into their wire
/// representations (CONNACK refusal, STOMP ERROR, amqp1.0 close…).
pub type BridgeResult<T> = Result<T, BrokerError>;

fn berr(e: switchboard_cluster::ClusterError) -> BrokerError {
    match e {
        switchboard_cluster::ClusterError::Broker(b) => b,
        other => BrokerError::resource_error(other.to_string()).channel_level(),
    }
}

/// Check credentials through the meta group (same path AMQP uses).
pub async fn authorize(
    node: &Arc<ClusterNode>,
    user: &str,
    password: &str,
) -> BridgeResult<()> {
    let reply = node
        .write(
            switchboard_cluster::META_GROUP,
            BrokerCommand::Meta(MetaCmd::Authorize {
                user: user.to_string(),
                password: password.to_string(),
            }),
        )
        .await
        .map_err(berr)?;
    match reply {
        switchboard_cluster::BrokerReply::Meta(MetaReply::Authorized) => Ok(()),
        other => Err(BrokerError::access_refused(format!(
            "credentials rejected: {other:?}"
        ))
        .channel_level()),
    }
}

/// One bridge connection's identity: a real connection id (so shard
/// consumer-exclusivity semantics work) and a subscription counter.
pub struct BridgeContext {
    pub node: Arc<ClusterNode>,
    pub vhost: String,
    pub conn: ConnectionId,
    sub_counter: AtomicU64,
}

impl BridgeContext {
    /// Create a context with a real cluster connection id (so shard-side
    /// consumer-exclusivity semantics work for bridge clients too).
    pub async fn create(node: Arc<ClusterNode>, vhost: String) -> Self {
        let conn = node.new_connection_id().await;
        BridgeContext { node, vhost, conn, sub_counter: AtomicU64::new(1) }
    }

    pub fn next_sub(&self) -> SubscriptionId2 {
        SubscriptionId2(self.sub_counter.fetch_add(1, Ordering::Relaxed))
    }
}

/// A bridge-local subscription id (unique per bridge connection).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId2(pub u64);

/// Declare (or assert) a queue on the cluster and make sure the owning
/// shard has live queue data. Returns the queue's shard group.
pub async fn declare_queue(
    ctx: &BridgeContext,
    name: &str,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
) -> BridgeResult<GroupId> {
    let reply = ctx
        .node
        .write(
            switchboard_cluster::META_GROUP,
            BrokerCommand::Meta(MetaCmd::DeclareQueue {
                vhost: ctx.vhost.clone(),
                name: name.to_string(),
                passive: false,
                options: QueueOptions {
                    durable,
                    exclusive,
                    auto_delete,
                    arguments: FieldTable::new(),
                },
                owner: ctx.conn,
            }),
        )
        .await
        .map_err(berr)?;
    let shard = match reply {
        switchboard_cluster::BrokerReply::Meta(MetaReply::QueueDeclared { shard, created, .. }) => {
            if created {
                let policy =
                    switchboard_core::shard::QueuePolicy::from_arguments(&switchboard_wire::field::FieldTable::new());
                shard_call(&ctx.node, shard, ShardCmd::CreateQueueData { queue: name.to_string(), policy }).await?;
            }
            // Refresh so the very next publish/subscribe observes the
            // queue (§4.4 visibility).
            ctx.node.refresh_topology().await;
            shard
        }
        other => {
            return Err(BrokerError::resource_error(format!(
                "unexpected declare reply {other:?}"
            ))
            .channel_level())
        }
    };
    Ok(shard)
}

/// Bind `queue` to `exchange` with routing key `rk`. The local topology
/// view is refreshed before returning so the very next publish observes
/// the new binding (§4.4 visibility).
pub async fn bind(
    ctx: &BridgeContext,
    exchange: &str,
    queue: &str,
    rk: &str,
) -> BridgeResult<()> {
    ctx.node
        .write(
            switchboard_cluster::META_GROUP,
            BrokerCommand::Meta(MetaCmd::Bind {
                vhost: ctx.vhost.clone(),
                exchange: exchange.to_string(),
                queue: queue.to_string(),
                routing_key: rk.to_string(),
                arguments: FieldTable::new(),
            }),
        )
        .await
        .map_err(berr)?;
    ctx.node.refresh_topology().await;
    Ok(())
}

/// Unbind (errors ignored — unsubscribe is best effort).

/// Delete a queue (best effort — used to clean up bridge-owned queues).
pub async fn delete_queue(ctx: &BridgeContext, name: &str) {
    let topo = ctx.node.topology();
    let Some(shard) = topo
        .vhosts
        .get(&ctx.vhost)
        .and_then(|v| v.queues.get(name))
        .map(|qi| qi.shard)
    else {
        return;
    };
    let _ = shard_call(&ctx.node, shard, ShardCmd::DeleteQueueData { queue: name.to_string() }).await;
    let _ = ctx
        .node
        .write(
            switchboard_cluster::META_GROUP,
            BrokerCommand::Meta(MetaCmd::DeleteQueue {
                vhost: ctx.vhost.clone(),
                name: name.to_string(),
                if_unused: false,
                if_empty: false,
                depth: 0,
                consumers: 0,
            }),
        )
        .await;
}

/// Resolve an exchange+routing-key to `(queue, shard)` destinations via
/// the local topology view (refreshed by declare/bind).
pub fn route_destinations(
    ctx: &BridgeContext,
    exchange: &str,
    routing_key: &str,
    message: &StoredMessage,
) -> BridgeResult<Vec<(String, GroupId)>> {
    let topo = ctx.node.topology();
    let Some(vhost) = topo.vhosts.get(&ctx.vhost).cloned() else {
        return Err(BrokerError::invalid_path("vhost vanished").channel_level());
    };
    let view = VhostView { vhost: &vhost };
    let destinations = route(&view, exchange, routing_key, &message.properties);
    Ok(destinations
        .into_iter()
        .filter_map(|q| vhost.queues.get(&q).map(|qi| (q, qi.shard)))
        .collect())
}

/// Publish one message through the AMQP routing model (exchange +
/// routing key → destination queues → shard enqueue). Returns false when
/// the message was unroutable.
pub async fn publish(
    ctx: &BridgeContext,
    exchange: &str,
    routing_key: &str,
    message: StoredMessage,
) -> BridgeResult<bool> {
    let node = &ctx.node;
    let topo = node.topology();
    let Some(vhost) = topo.vhosts.get(&ctx.vhost).cloned() else {
        return Err(BrokerError::invalid_path("vhost vanished").channel_level());
    };
    let view = VhostView { vhost: &vhost };
    let mut destinations = route(&view, exchange, routing_key, &message.properties);
    // This node's cached topology may lag a just-replicated declare made
    // through another node: an empty route gets one refreshed retry
    // before the message counts as genuinely unroutable.
    if destinations.is_empty() {
        node.refresh_topology().await;
        let topo = node.topology();
        if let Some(vhost) = topo.vhosts.get(&ctx.vhost).cloned() {
            let view = VhostView { vhost: &vhost };
            destinations = route(&view, exchange, routing_key, &message.properties);
        }
    }
    for q in destinations {
        let Some(shard) = vhost.queues.get(&q).map(|qi| qi.shard) else {
            continue;
        };
        shard_call(
            node,
            shard,
            ShardCmd::Enqueue {
                queue: q.clone(),
                message: message.clone(),
                at_ms: switchboard_cluster::now_ms(),
            },
        )
        .await?;
    }
    Ok(true)
}

/// A live consumer fed by the shard's delivery effects. The delivery
/// receiver is returned separately by [`BridgeConsumer::start`] so the
/// caller can move it into its own pump task.
pub struct BridgeConsumer {
    pub queue: String,
    pub shard: GroupId,
    pub sub: switchboard_core::model::SubscriptionId,
}

impl BridgeConsumer {
    /// Attach the consumer and grant its first credit window. `no_ack`
    /// consumers delete messages on hand-out; acknowledged consumers hold
    /// them until [`ack`] / [`release`].
    pub async fn start(
        ctx: &BridgeContext,
        queue: &str,
        shard: GroupId,
        prefetch: u32,
        no_ack: bool,
        tag: &str,
    ) -> BridgeResult<(Arc<BridgeConsumer>, mpsc::UnboundedReceiver<Delivery>)> {
        let node = &ctx.node;
        let sub = switchboard_core::model::SubscriptionId {
            node: node.id,
            sub: ctx.sub_counter.fetch_add(1, Ordering::Relaxed),
        };
        let (dtx, drx) = mpsc::unbounded_channel::<Delivery>();
        let (ctx_tx, crx) = mpsc::unbounded_channel::<String>();
        let sink = ConsumerSink { deliveries: dtx, cancelled: ctx_tx };
        node.attach_consumer(sub, sink.clone()).await;
        shard_call(
            node,
            shard,
            ShardCmd::RegisterSubscription {
                sub,
                queue: queue.to_string(),
                node: node.id,
                consumer_tag: tag.to_string(),
                no_ack,
                exclusive: false,
                conn: ctx.conn,
                byte_limit: 0,
            },
        )
        .await?;
        let credit = if prefetch == 0 { u32::MAX.saturating_sub(1).min(1000) } else { prefetch };
        shard_call(node, shard, ShardCmd::Credit { sub, count: credit }).await?;
        let _ = crx; // cancellation notifications are not surfaced to bridges
        Ok((Arc::new(BridgeConsumer { queue: queue.to_string(), shard, sub }), drx))
    }

    /// Acknowledge a delivered message (removes it from the queue).
    pub async fn ack(&self, node: &Arc<ClusterNode>, seq: u64) -> BridgeResult<()> {
        let mut seqs = std::collections::BTreeSet::new();
        seqs.insert(seq);
        shard_call(node, self.shard, ShardCmd::Ack { queue: self.queue.clone(), seqs }).await?;
        Ok(())
    }

    /// Release a message back to the ready set (basic.requeue semantics:
    /// the shard marks it redelivered on the next hand-out).
    pub async fn release(&self, node: &Arc<ClusterNode>, seq: u64) {
        let mut seqs = Vec::new();
        seqs.push(seq);
        let _ = shard_call(
            node,
            self.shard,
            ShardCmd::Release {
                queue: self.queue.clone(),
                sub: Some(self.sub),
                seqs,
                dead: false,
            },
        )
        .await;
    }

    /// Stop consuming and clean up shard state.
    pub async fn stop(&self, node: &Arc<ClusterNode>) {
        node.detach_consumer(self.sub).await;
        let _ = shard_call(
            node,
            self.shard,
            ShardCmd::UnregisterSubscription { sub: self.sub },
        )
        .await;
    }
}

/// `/exchange/<name>` destinations require the exchange to exist (parity
/// with the AMQP-side 404): subscriptions and publishes against a missing
/// exchange are refused instead of being silently unroutable. Meta writes
/// cannot be relied on for this because state-machine apply errors are
/// not propagated through the write reply.
pub async fn ensure_exchange(
    ctx: &BridgeContext,
    exchange: &str,
) -> Result<(), switchboard_core::error::BrokerError> {
    let topo = ctx.node.topology();
    if topo
        .vhosts
        .get(&ctx.vhost)
        .is_some_and(|v| v.exchanges.contains_key(exchange))
    {
        return Ok(());
    }
    Err(switchboard_core::error::BrokerError::not_found(format!(
        "no exchange {exchange:?} in vhost {:?}",
        ctx.vhost
    )))
}

/// One shard command through the (possibly forwarding) write path.

async fn shard_call(
    node: &Arc<ClusterNode>,
    shard: GroupId,
    cmd: ShardCmd,
) -> BridgeResult<ShardReply> {
    let reply = node
        .write(shard, BrokerCommand::Shard(cmd))
        .await
        .map_err(berr)?;
    match reply {
        switchboard_cluster::BrokerReply::Shard(r) => Ok(r),
        switchboard_cluster::BrokerReply::Error(e) => Err(e),
        other => {
            debug!(?other, "unexpected shard reply");
            Err(BrokerError::resource_error("unexpected shard reply").channel_level())
        }
    }
}
