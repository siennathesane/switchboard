use std::sync::Arc;

use switchboard_cluster::ClusterNode;
use switchboard_core::error::BrokerError;
use switchboard_core::shard::ShardCmd;
use switchboard_wire::method::Method;

use crate::channel::Channel;
use crate::channel::ChannelResult;
use crate::methods::inner_vhost;
use crate::methods::send_frame;
use crate::methods::shard_call;
use crate::methods::take_confirm_seq;
use crate::outbound::OutboundFrame;

use switchboard_wire::BasicProperties;
use crate::channel::TxOp as ChannelTxOp;
use switchboard_core::routing::route;
use switchboard_core::topology::VhostView;
use switchboard_core::shard::ShardReply;
use crate::outbound::message_frames;

impl Channel {
        pub async fn publish(
        &self,
        node: &Arc<ClusterNode>,
        method: Method,
        props: BasicProperties,
        body: Vec<u8>,
    ) -> ChannelResult<()> {
        let (exchange, routing_key, mandatory, immediate) = match &method {
            Method::BasicPublish {
                exchange,
                routing_key,
                mandatory,
                immediate,
                ..
            } => (exchange.clone(), routing_key.clone(), *mandatory, *immediate),
            _ => {
                return Err(BrokerError {
                    code: switchboard_wire::constants::reply::UNEXPECTED_FRAME,
                    text: "content completed without Basic.Publish".into(),
                    level: switchboard_core::error::Level::Connection,
                    class_id: 0,
                    method_id: 0,
                })
            }
        };

        let confirm_seq = take_confirm_seq(self)?;
        let topo = node.topology();
        let Some(vhost) = topo.vhosts.get(&inner_vhost(self)).cloned() else {
            return Err(BrokerError::invalid_path("vhost vanished").channel_level());
        };
        let view = VhostView { vhost: &vhost };
        let destinations = route(&view, &exchange, &routing_key, &props);

        if destinations.is_empty() {
            if mandatory {
                self.return_message(404, "NO_ROUTE", &exchange, &routing_key, props, body)
                    .await?;
                self.confirm_fire(confirm_seq);
                return Ok(());
            }
            if immediate {
                self.return_message(312, "NO_CONSUMERS", &exchange, &routing_key, props, body)
                    .await?;
                self.confirm_fire(confirm_seq);
                return Ok(());
            }
            // Unroutable and uninteresting: silently dropped (§2.1.2.1);
            // in confirm mode still confirmed.
            self.confirm_fire(confirm_seq);
            return Ok(());
        }

        // immediate: every destination needs an active consumer right now.
        if immediate {
            let Some(first) = topo.vhosts.get(&inner_vhost(self)).and_then(|v| v.queues.get(&destinations[0])) else {
                return Err(BrokerError::not_found(format!("no queue {:?}", destinations[0])).channel_level());
            };
            let stats = shard_call(
                node,
                first.shard,
                ShardCmd::Stats { queue: destinations[0].clone() },
            )
            .await?;
            let empty = matches!(&stats, ShardReply::Stats { consumer_count: 0, .. });
            if empty {
                self.return_message(312, "NO_CONSUMERS", &exchange, &routing_key, props, body)
                    .await?;
                self.confirm_fire(confirm_seq);
                return Ok(());
            }
        }

        let message = crate::channel::stored_message(
            exchange.clone(),
            routing_key.clone(),
            props,
            body,
        );

        // In tx mode the publishes join the transaction buffer (§2.2.9);
        // confirms fire at commit.
        let in_tx = self.inner.lock().unwrap().tx.is_some();
        eprintln!("[pub-probe] ch={} rk={} in_tx={in_tx} confirm_seq={confirm_seq}", self.id, message.routing_key);
        if in_tx {
            let mut inner = self.inner.lock().unwrap();
            let Some(tx) = inner.tx.as_mut() else { return Ok(()) };
            for q in &destinations {
                let Some(shard) = vhost.queues.get(q).map(|qi| qi.shard) else {
                    continue;
                };
                tx.ops.push(ChannelTxOp::Publish {
                    queue: q.clone(),
                    shard,
                    message: message.clone(),
                    confirm_seq: (confirm_seq != 0).then_some(confirm_seq),
                });
            }
            return Ok(());
        }

        // Fire the enqueues in order (§4.7: per-path ordering). Each write
        // resolves after quorum apply — the publisher-confirm guarantee.
        // An enqueue error must surface: dropping the message silently is
        // only sanctioned for unroutable destinations, never internal
        // failures.
        for q in &destinations {
            let Some(shard) = vhost.queues.get(q).map(|qi| qi.shard) else {
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
        self.confirm_fire(confirm_seq);
        Ok(())
    }

    /// Send a BasicAck confirming `seq` (no-op outside confirm mode).
    pub(crate) fn confirm_fire(&self, seq: u64) {
        let inner = self.inner.lock().unwrap();
        if inner.confirm.is_some() {
            let _ = self
                .outbound
                .send(OutboundFrame::Method {
                    channel: self.id,
                    method: Method::BasicAck { delivery_tag: seq, multiple: false },
                });
        }
    }

    pub(crate) async fn return_message(
        &self,
        code: u16,
        text: &str,
        exchange: &str,
        routing_key: &str,
        props: BasicProperties,
        body: Vec<u8>,
    ) -> ChannelResult<()> {
        let channel = self.id;
        let fm = self.inner.lock().unwrap().limits.frame_max;
        let m = Method::BasicReturn {
            reply_code: code,
            reply_text: text.to_string(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
        };
        for f in message_frames(channel, m, props, &body, fm) {
            send_frame(self, f)?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Consumers
    // ------------------------------------------------------------------
}
