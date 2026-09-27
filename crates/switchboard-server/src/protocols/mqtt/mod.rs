//! MQTT 3.1.1 bridged onto the AMQP broker core.
//!
//! Mapping (RabbitMQ-compatible):
//! * publishes go to the `amq.topic` exchange with the MQTT topic as
//!   routing key (AMQP topic matching treats `/` and `.` levels
//!   identically after conversion),
//! * `SUBSCRIBE` creates a private `amq.gen-…` queue bound to
//!   `amq.topic` with `+`→`*`, `#`→`#` wildcards,
//! * full QoS 2: PUBREC/PUBREL/PUBCOMP with per-packet-id dedupe,
//! * persistent sessions: a `clean_session = false` client keeps a
//!   durable per-client session queue across connections, so messages
//!   published while it is offline are delivered on reconnect,
//! * retained messages are stored in the meta group — raft-replicated,
//!   so every node serves the same retained state,
//! * will messages are ignored (documented server extension gap).

pub mod packet;
use std::collections::HashMap;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use switchboard_cluster::ClusterNode;
use switchboard_core::model::StoredMessage;
use switchboard_wire::properties::BasicProperties;

use crate::protocols::shared;
use crate::protocols::shared::BridgeConsumer;
use crate::protocols::shared::BridgeContext;

use self::packet::Out;
use self::packet::Packet;

/// Store (or clear) a retained message through meta — raft-replicated, so
/// every node serves the same retained state.
async fn set_retained(ctx: &Arc<BridgeContext>, topic: &str, message: Option<StoredMessage>) {
    let _ = ctx
        .node
        .write(
            switchboard_cluster::META_GROUP,
            switchboard_cluster::BrokerCommand::Meta(switchboard_core::topology::MetaCmd::SetRetained {
                vhost: ctx.vhost.clone(),
                topic: topic.to_string(),
                message,
            }),
        )
        .await;
    ctx.node.refresh_topology().await;
}

/// Retained messages matching `filter`, from the (replicated) meta view.
fn matching_retained(ctx: &BridgeContext, filter: &str) -> Vec<(String, StoredMessage)> {
    ctx.node
        .topology()
        .retained
        .iter()
        .filter(|((vhost, topic), _)| {
            vhost == &ctx.vhost && topic_matches(filter, &topic.replace('/', "."))
        })
        .map(|((_, topic), m)| (topic.clone(), m.clone()))
        .collect()
}

/// Durable session-queue name for a client id (sanitized for AMQP naming).
pub fn session_queue_name(client_id: &str) -> String {
    let safe: String = client_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    format!("mqtt-session-{safe}")
}

/// One live subscription.
struct SubEntry {
    /// Bridge-local id feeding the merged delivery channel.
    local: shared::SubscriptionId2,
    filter: String,
    qos: u8,
    consumer: Arc<BridgeConsumer>,
}

/// MQTT topic → AMQP topic pattern (`+`→`*`, `#`→`#`).
fn mqtt_to_amqp_filter(filter: &str) -> String {
    filter
        .split('/')
        .map(|level| if level == "+" { "*".to_string() } else { level.to_string() })
        .collect::<Vec<_>>()
        .join(".")
}

/// AMQP routing key → MQTT topic for deliveries.
fn amqp_to_mqtt_topic(rk: &str) -> String {
    rk.replace('.', "/")
}

/// Does the (MQTT) topic match the (MQTT) filter? Uses the AMQP topic
/// matcher after wildcard conversion.
fn topic_matches(filter: &str, topic: &str) -> bool {
    switchboard_core::topic::matches(&mqtt_to_amqp_filter(filter), topic)
}

/// Serve one MQTT connection to completion.
pub async fn serve<S>(io: S, node: Arc<ClusterNode>) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(io);

    // ---- CONNECT (must be the first packet) ----
    let Some(Packet::Connect { clean_session, keep_alive, username, password, client_id }) =
        self::packet::read_packet(&mut reader).await?
    else {
        return Ok(()); // hung up, or spoke before CONNECT
    };
    let (user, pass) = match (&username, &password) {
        (Some(u), p) => (u.clone(), p.clone().unwrap_or_default()),
        _ => ("guest".into(), b"guest".to_vec()),
    };
    let auth = shared::authorize(&node, &user, &String::from_utf8_lossy(&pass)).await;
    if let Err(e) = auth {
        // 4 = bad user name or password.
        let nack = Out::ConnAck { session_present: false, code: 4 };
        let _ = writer.write_all(&nack.encode()).await;
        let _ = writer.flush().await;
        tracing::debug!(err = %e, "mqtt: auth failed");
        return Ok(());
    }

    let ctx = Arc::new(BridgeContext::create(node.clone(), "/".into()).await);

    // Persistent sessions: a clean=false session keeps its durable queue
    // and bindings across connections (messages published while the
    // client is offline wait in the queue); clean=true wipes it.
    let session_queue: Option<String> = if clean_session {
        let name = session_queue_name(&client_id);
        shared::delete_queue(&ctx, &name).await;
        None
    } else if !client_id.is_empty() {
        Some(session_queue_name(&client_id))
    } else {
        None
    };
    // Session-present flag: a persistent session that already existed.
    let session_present = session_queue
        .as_ref()
        .map(|q| {
            node.topology()
                .vhosts
                .get("/")
                .map(|v| v.queues.contains_key(q))
                .unwrap_or(false)
        })
        .unwrap_or(false);

    writer
        .write_all(&Out::ConnAck { session_present, code: 0 }.encode())
        .await?;
    writer.flush().await?;

    let mut next_pid = AtomicU16::new(1);
    let mut subs: HashMap<u64, SubEntry> = HashMap::new();
    // QoS 2 in-flight state, both directions (§4.4 QoS 2 flow).
    // Inbound: packet ids published, awaiting PUBREL (dedupe window).
    let mut inbound_qos2: HashMap<u16, ()> = HashMap::new();
    // Outbound at PUBREC stage: pid → (consumer, seq).
    let mut outbound_rec: HashMap<u16, (Arc<BridgeConsumer>, u64)> = HashMap::new();
    // Outbound at PUBCOMP stage: pid → (consumer, seq).
    let mut outbound_comp: HashMap<u16, (Arc<BridgeConsumer>, u64)> = HashMap::new();
    // Merged deliveries from every consumer: (local sub id, delivery).
    let (merged_tx, mut merged_rx) = tokio::sync::mpsc::unbounded_channel::<(
        shared::SubscriptionId2,
        switchboard_cluster::Delivery,
    )>();
    // Pending qos1 outbound deliveries awaiting PUBACK: pid → (consumer, seq).
    let mut awaiting_puback: HashMap<u16, (Arc<BridgeConsumer>, u64)> = HashMap::new();

    let keepalive = if keep_alive > 0 {
        std::time::Duration::from_millis(u64::from(keep_alive) * 1500)
    } else {
        std::time::Duration::MAX
    };

    let result = loop {
        // `Instant + Duration::MAX` (keep-alive 0 = disabled) overflows
        // and panics the session task; far_future() is the safe "never".
        // Ten years: effectively "never", with no Instant overflow.
        const KEEPALIVE_OFF: std::time::Duration =
            std::time::Duration::from_secs(10 * 365 * 24 * 3600);
        let deadline = if keep_alive > 0 {
            tokio::time::Instant::now() + keepalive
        } else {
            tokio::time::Instant::now() + KEEPALIVE_OFF
        };
        let packet = tokio::select! {
            p = self::packet::read_packet(&mut reader) => match p {
                Ok(Some(p)) => p,
                Ok(None) => break Ok(()),
                Err(e) => {
                    tracing::debug!(err = %e, "mqtt: packet error");
                    break Err(e);
                }
            },
            _ = tokio::time::sleep_until(deadline) => {
                tracing::debug!("mqtt: keepalive expired");
                break Ok(());
            }
            Some((local, d)) = merged_rx.recv() => {
                // Deliver one message to its subscriber.
                let Some(entry) = subs.get(&(local.0)) else { continue };
                let topic = amqp_to_mqtt_topic(&d.message.routing_key);
                let qos = entry.qos;
                let pid = (qos > 0).then(|| next_pid.fetch_add(1, Ordering::Relaxed));
                let out = Out::Publish {
                    qos,
                    retain: false,
                    topic: topic.clone(),
                    packet_id: pid,
                    payload: d.message.body.clone(),
                };
                if writer.write_all(&out.encode()).await.is_err() || writer.flush().await.is_err() {
                    break Ok(());
                }
                match (qos, pid) {
                    (1, Some(pid)) => {
                        awaiting_puback.insert(pid, (entry.consumer.clone(), d.seq));
                    }
                    (2, Some(pid)) => {
                        outbound_rec.insert(pid, (entry.consumer.clone(), d.seq));
                    }
                    _ => {}
                }
                continue;
            }
        };
        match packet {
            Packet::Connect { .. } => break Ok(()), // second CONNECT is a violation
            Packet::PingReq => {
                writer.write_all(&Out::PingResp.encode()).await?;
                writer.flush().await?;
            }
            Packet::Disconnect => break Ok(()),
            Packet::PubAck { packet_id } => {
                if let Some((consumer, seq)) = awaiting_puback.remove(&packet_id) {
                    let _ = consumer.ack(&node, seq).await;
                }
            }
            Packet::PubRec { packet_id } => {
                // Outbound qos2: hand-off acknowledged, release the hold.
                if let Some((consumer, seq)) = outbound_rec.remove(&packet_id) {
                    outbound_comp.insert(packet_id, (consumer.clone(), seq));
                    let rel = Out::PubRel { packet_id };
                    if writer.write_all(&rel.encode()).await.is_err() || writer.flush().await.is_err()
                    {
                        break Ok(());
                    }
                }
            }
            Packet::PubRel { packet_id } => {
                // Inbound qos2: the handshake completes; nothing to undo.
                inbound_qos2.remove(&packet_id);
                let comp = Out::PubComp { packet_id };
                if writer.write_all(&comp.encode()).await.is_err() || writer.flush().await.is_err() {
                    break Ok(());
                }
            }
            Packet::PubComp { packet_id } => {
                // Outbound qos2: fully acknowledged.
                if let Some((consumer, seq)) = outbound_comp.remove(&packet_id) {
                    let _ = consumer.ack(&node, seq).await;
                }
            }
            Packet::Publish { qos, retain, topic, packet_id, payload } => {
                let amqp_rk = topic.replace('/', ".");
                let duplicate = qos == 2 && inbound_qos2.contains_key(&packet_id.unwrap_or(0));
                if !duplicate {
                    let message = StoredMessage {
                        properties: BasicProperties::new(),
                        body: payload.clone(),
                        exchange: "amq.topic".into(),
                        routing_key: amqp_rk.clone(),
                        persistent: false,
                    };
                    if let Err(e) = shared::publish(&ctx, "amq.topic", &amqp_rk, message).await {
                        tracing::debug!(err = %e, "mqtt: publish failed");
                    }
                }
                if retain {
                    let stored = (!payload.is_empty()).then(|| StoredMessage {
                        properties: BasicProperties::new(),
                        body: payload,
                        exchange: "amq.topic".into(),
                        routing_key: topic.replace('/', "."),
                        persistent: false,
                    });
                    set_retained(&ctx, &topic, stored).await;
                }
                match qos {
                    1 => {
                        if let Some(pid) = packet_id {
                            let ack = Out::PubAck { packet_id: pid };
                            if writer.write_all(&ack.encode()).await.is_err()
                                || writer.flush().await.is_err()
                            {
                                break Ok(());
                            }
                        }
                    }
                    2 => {
                        if let Some(pid) = packet_id {
                            inbound_qos2.insert(pid, ());
                            let rec = Out::PubRec { packet_id: pid };
                            if writer.write_all(&rec.encode()).await.is_err()
                                || writer.flush().await.is_err()
                            {
                                break Ok(());
                            }
                        }
                    }
                    _ => {}
                }
            }
            Packet::Subscribe { packet_id, filters } => {
                let mut codes = Vec::new();
                for (filter, qos) in &filters {
                    match subscribe_one(
                        &ctx,
                        filter,
                        *qos,
                        session_queue.as_deref(),
                        &mut subs,
                        &merged_tx,
                    )
                    .await
                    {
                        Ok(()) => codes.push((*qos).min(2)),
                        Err(e) => {
                            tracing::debug!(err = %e, filter, "mqtt: subscribe failed");
                            codes.push(0x80);
                        }
                    }
                }
                // SUBACK first (the ack acknowledges the subscription),
                // then any retained messages for the fresh filters.
                writer.write_all(&Out::SubAck { packet_id, codes }.encode()).await?;
                writer.flush().await?;
                for (filter, _) in &filters {
                    for (topic, msg) in matching_retained(&ctx, filter) {
                        let out = Out::Publish {
                            qos: 0,
                            retain: true,
                            topic,
                            packet_id: None,
                            payload: msg.body.clone(),
                        };
                        if writer.write_all(&out.encode()).await.is_err() {
                            break;
                        }
                    }
                }
                writer.flush().await?;
            }
            Packet::Unsubscribe { packet_id, filters } => {
                let dead: Vec<u64> = subs
                    .iter()
                    .filter(|(_, e)| filters.contains(&e.filter))
                    .map(|(k, _)| *k)
                    .collect();
                for k in dead {
                    if let Some(e) = subs.remove(&k) {
                        cleanup_consumer(&ctx, &e.consumer, session_queue.is_none()).await;
                    }
                }
                writer.write_all(&Out::UnsubAck { packet_id }.encode()).await?;
                writer.flush().await?;
            }
        }
    };

    // Session teardown: stop consumers. For transient sessions the
    // private queue goes too; persistent session queues survive.
    for (_, e) in subs {
        cleanup_consumer(&ctx, &e.consumer, session_queue.is_none()).await;
    }
    result
}

/// Subscribe one filter: private (or session) queue + binding + consumer
/// + forwarder.
#[allow(clippy::too_many_arguments)]
async fn subscribe_one(
    ctx: &Arc<BridgeContext>,
    filter: &str,
    qos: u8,
    session_queue: Option<&str>,
    subs: &mut HashMap<u64, SubEntry>,
    merged_tx: &tokio::sync::mpsc::UnboundedSender<(shared::SubscriptionId2, switchboard_cluster::Delivery)>,
) -> Result<(), switchboard_core::error::BrokerError> {
    let local = ctx.next_sub();
    let queue = match session_queue {
        Some(q) => {
            // Persistent session: one durable queue per client, bound per
            // filter. Create it on first use; reuse afterwards.
            let topo = ctx.node.topology();
            let exists = topo
                .vhosts
                .get(&ctx.vhost)
                .map(|v| v.queues.contains_key(q))
                .unwrap_or(false);
            if !exists {
                shared::declare_queue(ctx, q, true, false, false).await?;
            }
            q.to_string()
        }
        None => switchboard_core::model::generate_queue_name(),
    };
    let shard = match session_queue {
        Some(q) => {
            let topo = ctx.node.topology();
            match topo.vhosts.get(&ctx.vhost).and_then(|v| v.queues.get(q)) {
                Some(qi) => qi.shard,
                None => shared::declare_queue(ctx, q, true, false, false).await?,
            }
        }
        None => shared::declare_queue(ctx, &queue, false, false, true).await?,
    };
    shared::bind(ctx, "amq.topic", &queue, &mqtt_to_amqp_filter(filter)).await?;
    let no_ack = qos == 0;
    let (consumer, mut rx) =
        BridgeConsumer::start(ctx, &queue, shard, 0, no_ack, &format!("mqtt-{local:?}")).await?;
    // Forward deliveries into the merged channel.
    let forward_tx = merged_tx.clone();
    let forward_local = local;
    tokio::spawn(async move {
        while let Some(d) = rx.recv().await {
            if forward_tx.send((forward_local, d)).is_err() {
                break;
            }
        }
    });
    subs.insert(
        local.0,
        SubEntry { local, filter: filter.to_string(), qos: qos.min(2), consumer },
    );
    Ok(())
}

/// Stop a consumer; drop the queue too for transient (generated) queues.
async fn cleanup_consumer(ctx: &Arc<BridgeContext>, consumer: &BridgeConsumer, transient: bool) {
    consumer.stop(&ctx.node).await;
    if transient {
        shared::delete_queue(ctx, &consumer.queue).await;
    }
}

/// Re-exported for tests: the MQTT filter conversion.
pub fn convert_filter(filter: &str) -> String {
    mqtt_to_amqp_filter(filter)
}
