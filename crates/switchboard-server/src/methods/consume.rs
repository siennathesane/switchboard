use std::sync::Arc;
use tokio::sync::mpsc;

use switchboard_cluster::ClusterNode;
use switchboard_core::error::BrokerError;
use switchboard_core::model::SubscriptionId;
use switchboard_core::shard::ShardCmd;
use switchboard_wire::method::Method;

use crate::channel::Channel;
use crate::channel::ChannelResult;
use crate::methods::authorize_ch;
use crate::methods::inner_vhost;
use crate::methods::send_frame;
use crate::methods::shard_call;
use crate::methods::shard_call_lenient;
use crate::outbound::OutboundFrame;
use switchboard_core::topology::Perm;

use crate::channel::LocalConsumer;
use crate::channel::PumpGate;
use crate::channel::Unacked;
use crate::outbound::message_frames;
use switchboard_cluster::ConsumerSink;
use switchboard_cluster::Delivery;
use switchboard_core::model::ConnectionId;
use switchboard_core::shard::shard_key;
use switchboard_core::topology::GroupId;
use tokio::select;
use tokio::spawn;

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
        // Exclusive-queue ownership (code-scanning alert #13): consuming
        // another connection's exclusive queue is 405 RESOURCE_LOCKED.
        self.assert_queue_ownership(&qi)?;
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
        node.attach_consumer(
            sub,
            ConsumerSink {
                deliveries: dtx,
                cancelled: ctx,
            },
        )
        .await;

        let (tag, gate) = {
            let mut inner = self.inner.lock().unwrap();
            let tag = if requested_tag.is_empty() {
                format!("ct-{}", inner.next_delivery_tag)
            } else {
                requested_tag.to_string()
            };
            let gate = Arc::new(PumpGate::default());
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
            // Registered atomically with the consumer so a cancel that
            // finds the consumer always finds its pump gate too (and a
            // cancel that finds neither waits on nothing).
            inner.pump_gates.insert(sub, gate.clone());
            inner.sub_shards.insert(sub, shard);
            (tag, gate)
        };

        let registration: ChannelResult<()> = async {
            authorize_ch(self, node, Perm::Read, queue)?;
            shard_call_lenient(
                node,
                shard,
                ShardCmd::RegisterSubscription {
                    sub,
                    // Vhost-qualified shard-data key (alert #14): consumers
                    // attach to their own vhost's entry; deliveries carry
                    // the same key back out for ack round-trips.
                    queue: shard_key(&inner_vhost(self), queue),
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
            let credit = Channel::initial_credit(prefetch);
            if credit > 0 {
                shard_call(node, shard, ShardCmd::Credit { sub, count: credit }).await?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = registration {
            // The consumer never went live (refused read access, queue
            // vanished mid-registration, shard trouble): roll the local
            // registration back. Leaving the pump gate behind would park
            // a later cancel/teardown on a pump that will never spawn;
            // leaving the consumer behind leaked a phantom registration
            // into teardown.
            {
                let mut inner = self.inner.lock().unwrap();
                if inner.consumers.get(&tag).is_some_and(|c| c.sub == sub) {
                    inner.consumers.remove(&tag);
                }
                inner.pump_gates.remove(&sub);
                inner.sub_shards.remove(&sub);
            }
            node.detach_consumer(sub).await;
            return Err(e);
        }

        // The pump ships shard effects to the socket as basic.deliver.
        let pump_channel = self.clone();
        let node2 = node.clone();
        let _tag_out = tag.clone();
        let pump_tag = tag.clone();
        spawn(async move {
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
                gate,
            )
            .await;
        });

        Ok(tag)
    }

    pub(crate) async fn cancel(
        &self,
        node: &Arc<ClusterNode>,
        consumer_tag: &str,
    ) -> ChannelResult<()> {
        let (consumer, gate) = {
            let mut inner = self.inner.lock().unwrap();
            let c = inner.consumers.remove(consumer_tag);
            let gate = c
                .as_ref()
                .and_then(|c| inner.pump_gates.get(&c.sub).cloned());
            (c, gate)
        };
        let Some(c) = consumer else {
            // Unknown tag: RabbitMQ answers cancel-ok anyway.
            return Ok(());
        };
        // Order matters: unregister FIRST (the shard stops dispatching to
        // the sub from the moment this applies), THEN drop the local
        // sink. Effects the shard already emitted before the unregister
        // applied may still be in flight; with the sink present they land
        // in the pump's channel — the barrier below writes them before
        // Cancel-ok, exactly RabbitMQ's "dispatched before the cancel
        // completes" set. (Detaching first opened a wider window where a
        // dispatch raced between the two and its no-ack message was
        // dropped at the vanished sink after the shard had already
        // deleted it.)
        shard_call(
            node,
            c.shard,
            ShardCmd::UnregisterSubscription { sub: c.sub },
        )
        .await?;
        node.detach_consumer(c.sub).await;
        // Cancel-ok is a delivery barrier for this consumer (RabbitMQ's
        // observable contract, which amqplib leans on: a Deliver for a
        // tag the client no longer knows is a fatal 541). Waiting here
        // costs nothing when the pump already exited (`finish` stores its
        // permit); when it has not, detach has dropped the sink so the
        // pump's inbound channels are closed — it drains every delivery
        // the shard dispatched for this consumer onto the outbound
        // stream, then exits and releases us. The Cancel-ok the handler
        // queues after this returns therefore follows the tag's last
        // Deliver on the wire, and nothing for the tag follows it.
        if let Some(g) = gate {
            g.wait().await;
        }
        Ok(())
    }

    pub(crate) async fn set_flow(
        &self,
        node: &Arc<ClusterNode>,
        active: bool,
    ) -> ChannelResult<()> {
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
///
/// Exits only when the consumer's inbound channels are closed and
/// drained; every exit path retires the consumer's [`PumpGate`] so
/// `Basic.Cancel` (and channel/connection teardown) can treat its reply
/// as a per-consumer delivery barrier.
pub(crate) async fn delivery_pump(
    channel: Channel,
    node: Arc<ClusterNode>,
    consumer: LocalConsumer,
    rx: mpsc::UnboundedReceiver<Delivery>,
    cancelled: mpsc::UnboundedReceiver<String>,
    gate: Arc<PumpGate>,
) {
    let sub = consumer.sub;
    pump_loop(channel.clone(), node, consumer, rx, cancelled).await;
    // Pump exit is the barrier's proof point: its inbound channels had no
    // senders left, so every delivery dispatched for this consumer has
    // been written to the outbound stream. Retire the gate (cancel may be
    // parked on it) and drop the registry entry.
    channel.inner.lock().unwrap().pump_gates.remove(&sub);
    gate.finish();
}

async fn pump_loop(
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
        // `biased` with the delivery arm first makes the Cancel-ok
        // barrier's drain deterministic: a delivery buffered in `rx` is
        // always written before the close of the sibling channel (both
        // close together with the consumer sink) can take the pump down.
        // Random arm order could otherwise exit with deliveries still
        // buffered — `recv` yields items from a closed-but-buffered
        // channel only when polled.
        select! {
            biased;
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
                        spawn(async move {
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

#[cfg(test)]
mod cancel_barrier_tests {
    use super::*;
    use crate::channel::ConnectionLimits;
    use crate::channel::PumpGate;
    use std::env::set_var;
    use std::env::temp_dir;
    use std::env::var;
    use std::net::TcpListener;
    use std::process::id;
    use std::sync::mpsc::channel as mpsc_channel;
    use std::thread::spawn;
    use std::time::Duration;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use switchboard_cluster::NodeConfig;
    use switchboard_core::model::StoredMessage;
    use switchboard_wire::BasicProperties;
    use switchboard_wire::constants::DEFAULT_PASSWORD;
    use switchboard_wire::constants::DEFAULT_USER;
    use tokio::runtime::Builder;
    use tokio::spawn as tokio_spawn;
    use tokio::time::sleep;
    use tokio::time::timeout;

    const TAG: &str = "ct-barr";

    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// A bootstrapped single node: the cancel path needs one live shard
    /// group for its `UnregisterSubscription` raft write (an unknown sub
    /// round-trips fine: the shard answers `Unsubscribed{0}`).
    async fn barrier_node() -> Arc<ClusterNode> {
        if var("SB_TIME_SCALE").is_err() {
            // Same test tempo the integration support module installs.
            unsafe { set_var("SB_TIME_SCALE", "50") };
        }
        if var("SWITCHBOARD_ARGON2_M_KIB").is_err() {
            unsafe {
                set_var("SWITCHBOARD_ARGON2_M_KIB", "2048");
                set_var("SWITCHBOARD_ARGON2_T", "1");
            }
        }
        let dir = temp_dir().join(format!(
            "switchboard-cancel-barrier-{}-{}",
            id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cfg = NodeConfig {
            id: 1,
            data_dir: dir,
            client_addr: format!("127.0.0.1:{}", free_port()),
            internal_addr: format!("127.0.0.1:{}", free_port()),
            seeds: vec![],
            bootstrap: true,
            expected_nodes: 1,
            peers: vec![],
            timeouts: Default::default(),
            bootstrap_user: DEFAULT_USER.into(),
            bootstrap_password: DEFAULT_PASSWORD.into(),
            internal_auth_secret: None,
        };
        let node = ClusterNode::start(cfg).await.unwrap();
        for _ in 0..3000 {
            let topo = node.topology();
            if !topo.groups.is_empty() && !topo.vhosts.is_empty() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        // A beat for the shard group's own election (same discipline as
        // tests/support): topology visibility does not mean the group's
        // raft core is ready to take the cancel's write yet.
        sleep(Duration::from_millis(700)).await;
        assert!(
            !node.topology().groups.is_empty(),
            "bootstrap never installed the shard layout"
        );
        node
    }

    fn delivery_for(sub: SubscriptionId, seq: u64, body: &str) -> Delivery {
        Delivery {
            sub,
            queue: "q".into(),
            seq,
            message: StoredMessage {
                properties: BasicProperties::new(),
                body: body.as_bytes().to_vec(),
                exchange: String::new(),
                routing_key: String::new(),
                persistent: false,
            },
            redelivered: false,
            deleted: false,
        }
    }

    /// The exact interleaving of the field crash (harness
    /// `node_amqp091.log`: amqplib died on "Unknown consumer: ct-5"):
    /// a Deliver dispatched for a consumer is still unwritten when the
    /// broker processes `Basic.Cancel` for it. The pump runs on a thread
    /// this test gates, so the ordering is pinned, not raced: with the
    /// barrier, `cancel()` may not return (and the handler may not queue
    /// Cancel-ok) until the pump has written every staged delivery and
    /// exited; without it, Cancel-ok precedes the Deliver on the wire.
    #[tokio::test]
    async fn cancel_waits_until_the_delivery_pump_has_drained_its_consumer() {
        let node = barrier_node().await;
        let shard = node
            .topology()
            .groups
            .keys()
            .copied()
            .next()
            .expect("shard layout installed");

        let (otx, mut orx) = mpsc::unbounded_channel();
        let channel = Channel::new(
            1,
            ConnectionId {
                node: node.id,
                conn: 7,
            },
            node.id,
            "guest".into(),
            "/".into(),
            ConnectionLimits::default(),
            otx,
        );

        // Local consumer registration exactly as `consume()` performs it.
        let sub = SubscriptionId {
            node: node.id,
            sub: 1,
        };
        let (dtx, drx) = mpsc::unbounded_channel();
        let (ctx, crx) = mpsc::unbounded_channel();
        node.attach_consumer(
            sub,
            ConsumerSink {
                deliveries: dtx.clone(),
                cancelled: ctx,
            },
        )
        .await;
        let consumer = LocalConsumer {
            tag: TAG.into(),
            queue: "q".into(),
            sub,
            shard,
            no_ack: false,
            exclusive: false,
            prefetch: 0,
            flow_active: true,
        };
        let gate = Arc::new(PumpGate::default());
        {
            let mut inner = channel.inner.lock().unwrap();
            inner.consumers.insert(TAG.into(), consumer.clone());
            inner.pump_gates.insert(sub, gate.clone());
        }

        // Two deliveries dispatched (buffered in the pump's inbound
        // channel) while the pump task does not exist yet: nothing can
        // have written them. Dropping our sender closes the feed, so
        // after draining both the pump exits on its own.
        dtx.send(delivery_for(sub, 1, "m1")).unwrap();
        dtx.send(delivery_for(sub, 2, "m2")).unwrap();
        drop(dtx);

        let (go_tx, go_rx) = mpsc_channel::<()>();
        let pump_channel = channel.clone();
        let pump_node = node.clone();
        let pump_gate = gate.clone();
        let pump_thread = spawn(move || {
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("pump runtime");
            go_rx.recv().expect("pump go signal");
            rt.block_on(delivery_pump(
                pump_channel,
                pump_node,
                consumer,
                drx,
                crx,
                pump_gate,
            ));
        });

        // Basic.Cancel arrives while the pump is parked mid-stream.
        let cancel_node = node.clone();
        let cancel_channel = channel.clone();
        let mut cancel_task =
            tokio_spawn(async move { cancel_channel.cancel(&cancel_node, TAG).await });

        // With the barrier, cancel cannot return while the pump has not
        // drained and exited — it must still be pending here. (A broker
        // without the barrier returns after one local raft round-trip;
        // the branch below then reproduces the handler's immediate
        // Cancel-ok and the wire assertion catches the Deliver that
        // follows it, like amqplib's 541.)
        let premature = timeout(Duration::from_millis(300), &mut cancel_task).await;
        if let Ok(joined) = premature {
            joined
                .expect("cancel task")
                .expect("cancel must succeed shard-side");
            // What the unfixed handler does next: queue Cancel-ok with
            // two deliveries for the tag still unwritten, then let the
            // pump write them.
            channel.reply(Method::BasicCancelOk {
                consumer_tag: TAG.into(),
            });
            go_tx.send(()).unwrap();
            pump_thread.join().unwrap();
            assert_wire_order(&mut orx); // must fail: a Deliver follows Cancel-ok
            unreachable!("unfixed broker: a Deliver for {TAG} must follow its Cancel-ok");
        }

        // Barrier held: release the pump. It writes both staged
        // deliveries (dispatched before the cancel — RabbitMQ still
        // delivers those, and the client must see them before Cancel-ok),
        // finds its feed closed, exits, and only then does cancel return.
        go_tx.send(()).unwrap();
        cancel_task.await.unwrap().unwrap();
        channel.reply(Method::BasicCancelOk {
            consumer_tag: TAG.into(),
        });
        pump_thread.join().unwrap();

        assert_wire_order(&mut orx);
        // The pump retired its gate; a repeat cancel answers immediately
        // (unknown tag: RabbitMQ still replies cancel-ok).
        assert!(!channel.inner.lock().unwrap().pump_gates.contains_key(&sub));
        assert!(channel.inner.lock().unwrap().consumers.get(TAG).is_none());
        channel.cancel(&node, TAG).await.unwrap();
        node.shutdown().await;
    }

    /// Wire order: exactly the two pre-cancel deliveries (method frames)
    /// for the tag, then Cancel-ok — never a Deliver for the tag after
    /// its Cancel-ok.
    fn assert_wire_order(orx: &mut mpsc::UnboundedReceiver<OutboundFrame>) {
        let mut delivers = 0usize;
        let mut cancel_ok = false;
        while let Ok(f) = orx.try_recv() {
            match f {
                OutboundFrame::Method {
                    method: Method::BasicDeliver { consumer_tag, .. },
                    ..
                } => {
                    assert_eq!(consumer_tag, TAG);
                    assert!(!cancel_ok, "Deliver for {TAG} written after Cancel-ok");
                    delivers += 1;
                }
                OutboundFrame::Method {
                    method: Method::BasicCancelOk { .. },
                    ..
                } => {
                    assert!(!cancel_ok, "second Cancel-ok");
                    cancel_ok = true;
                }
                _ => {}
            }
        }
        assert_eq!(delivers, 2, "both pre-cancel deliveries must be written");
        assert!(cancel_ok, "Cancel-ok must be queued");
    }
}
