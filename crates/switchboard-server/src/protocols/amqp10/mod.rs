//! AMQP 1.0 bridged onto the AMQP broker core (a deliberately minimal,
//! settled-mode subset).
//!
//! What works: SASL PLAIN/ANONYMOUS, open/begin/attach, client→broker
//! transfers (publish, anonymous relay included), broker→client
//! deliveries with per-link credit, **unsettled deliveries**: when the
//! receiver attaches with `snd-settle-mode = unsettled`, transfers carry
//! `settled = false` and the client's `disposition` (accepted / released
//! / rejected) drives shard ack / requeue-redelivery. Detach/end/close
//! and message annotations complete the surface used by clients.
//!
//! Address mapping matches the STOMP conventions: `/topic/<t>` →
//! `amq.topic`, `/queue/<q>` and a bare name → the named queue,
//! `/exchange/<ex>/<rk>` → that exchange.

pub mod frames;
pub mod types;
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use switchboard_cluster::ClusterNode;
use switchboard_core::model::StoredMessage;
use switchboard_wire::properties::BasicProperties;

use crate::protocols::shared;
use crate::protocols::shared::BridgeConsumer;
use crate::protocols::shared::BridgeContext;

use self::frames::codes;
use self::types::decode;
use self::types::Value;
use switchboard_core::error::BrokerError;

/// Address mapping (STOMP-compatible).
#[derive(Debug, Clone)]
enum Dest {
    Topic { exchange: String, rk: String },
    Queue(String),
}

fn resolve_address(addr: &str) -> Dest {
    if let Some(rk) = addr.strip_prefix("/topic/") {
        return Dest::Topic { exchange: "amq.topic".into(), rk: rk.to_string() };
    }
    if let Some(rest) = addr.strip_prefix("/exchange/") {
        if let Some((ex, rk)) = rest.split_once('/') {
            return Dest::Topic { exchange: ex.to_string(), rk: rk.to_string() };
        }
    }
    if let Some(q) = addr.strip_prefix("/queue/").or_else(|| addr.strip_prefix("/amq/queue/")) {
        return Dest::Queue(q.to_string());
    }
    Dest::Queue(addr.to_string())
}

/// One attached link.
struct Link {
    handle: u32,
    /// true = the client is the receiver (we deliver to it).
    client_is_receiver: bool,
    /// Broker→client deliveries.
    consumer: Option<Arc<BridgeConsumer>>,
    /// Client→broker target address (None = anonymous relay).
    target: Option<Dest>,
    /// Credit the client granted us for broker→client transfers.
    credit: Arc<AtomicI64>,
    notify: Arc<tokio::sync::Notify>,
    /// True when broker→client transfers must be sent `settled = false`
    /// (receiver attached with snd-settle-mode = unsettled).
    unsettled: bool,
}

/// The message content extracted from an inbound transfer payload.
struct ParsedMessage {
    body: Vec<u8>,
    content_type: Option<String>,
    to: Option<String>,
}

/// Parse message sections (properties, data, amqp-value).
fn parse_message(payload: &[u8]) -> Result<ParsedMessage, String> {
    let mut body = Vec::new();
    let mut content_type = None;
    let mut to = None;
    let mut pos = 0usize;
    while pos < payload.len() {
        let (v, used) = decode(&payload[pos..])?;
        pos += used;
        if let Value::Described(d, value) = &v {
            if let Value::ULong(code) = **d {
                match code {
                    codes::SECTION_DATA => {
                        if let Value::Binary(b) = &**value {
                            body.extend_from_slice(b);
                        }
                    }
                    codes::SECTION_AMQP_VALUE => match &**value {
                        Value::Binary(b) => body.extend_from_slice(b),
                        Value::String(s) => body.extend_from_slice(s.as_bytes()),
                        _ => {}
                    },
                    codes::SECTION_PROPERTIES => {
                        if let Value::List(fields) = &**value {
                            if let Some(ct) = fields.get(6).and_then(Value::as_str) {
                                content_type = Some(ct.to_string());
                            }
                            if let Some(t) = fields.get(2).and_then(Value::as_str) {
                                to = Some(t.to_string());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(ParsedMessage { body, content_type, to })
}

/// Encode broker→client message bytes (properties + data sections).
fn encode_message(content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(ct) = content_type {
        let mut fields: Vec<Value> = (0..6).map(|_| Value::Null).collect();
        fields.push(Value::Symbol(ct.to_string()));
        let props = Value::Described(
            Box::new(Value::ULong(codes::SECTION_PROPERTIES)),
            Box::new(Value::List(fields)),
        );
        self::types::encode(&props, &mut out);
    }
    let data = Value::Described(
        Box::new(Value::ULong(codes::SECTION_DATA)),
        Box::new(Value::Binary(body.to_vec())),
    );
    self::types::encode(&data, &mut out);
    out
}

type SharedWriter<W> = Arc<tokio::sync::Mutex<W>>;

/// Serve one AMQP 1.0 connection to completion.
pub async fn serve<S>(io: S, node: Arc<ClusterNode>) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, writer) = tokio::io::split(io);
    let writer: SharedWriter<_> = Arc::new(tokio::sync::Mutex::new(writer));
    use tokio::io::AsyncReadExt;

    // ---- protocol header ----
    let mut header = [0u8; 8];
    if reader.read_exact(&mut header).await.is_err() || header != *b"AMQP\0\x01\0\0" {
        return Ok(());
    }

    // ---- SASL ----
    // Per spec the server offers mechanisms immediately. A client that
    // skips SASL and opens directly still gets in (as guest).
    {
        writer
            .lock()
            .await
            .write_all(&frames::sasl_mechanisms(&["PLAIN", "ANONYMOUS"]))
            .await?;
        writer.lock().await.flush().await?;
    }
    let mut frame: Option<frames::Frame> = read_frame(&mut reader).await?;
    let mut sasl_done = false;
    if matches!(&frame, Some(f) if f.frame_type == frames::FRAME_TYPE_SASL) {
        let Some(f) = frame.take() else { return Ok(()) };
        if f.code() != Some(codes::SASL_INIT) {
            return Ok(());
        }
        let mechanism = f.field(0).and_then(Value::as_str).unwrap_or("").to_string();
        let response = match f.field(2) {
            Some(Value::Binary(b)) => b.clone(),
            _ => Vec::new(),
        };
        let (user, pass) = match mechanism.as_str() {
            "ANONYMOUS" => ("guest".to_string(), "guest".to_string()),
            "PLAIN" => {
                // response = \0 authzid \0 passwd
                let parts: Vec<&[u8]> = response.split(|&b| b == 0).collect();
                let user = parts.get(1).map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default();
                let pass = parts.get(2).map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default();
                (user, pass)
            }
            other => {
                tracing::debug!(mechanism = %other, "amqp10: unsupported SASL mechanism");
                return Ok(());
            }
        };
        if shared::authorize(&node, &user, &pass).await.is_err() {
            writer.lock().await.write_all(&frames::sasl_outcome(1)).await?;
            writer.lock().await.flush().await?;
            return Ok(());
        }
        writer.lock().await.write_all(&frames::sasl_outcome(0)).await?;
        writer.lock().await.flush().await?;
        // The client restarts the connection: a fresh protocol header.
        let mut h2 = [0u8; 8];
        if reader.read_exact(&mut h2).await.is_err() || h2 != *b"AMQP\0\x01\0\0" {
            return Ok(());
        }
        sasl_done = true;
        frame = read_frame(&mut reader).await?;
    }
    let _ = sasl_done;

    // ---- session ----
    let ctx = Arc::new(BridgeContext::create(node.clone(), "/".into()).await);
    let links: Arc<tokio::sync::Mutex<HashMap<u32, Arc<tokio::sync::Mutex<Link>>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    // delivery-id → (consumer, seq) for unsettled broker→client transfers.
    let pending_settlements: Arc<tokio::sync::Mutex<HashMap<u32, (Arc<BridgeConsumer>, u64)>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let delivery_counter = Arc::new(AtomicU32::new(1));
    let mut session_begun = false;
    let mut next_outgoing_id: u32 = 1;

    loop {
        let Some(f) = frame.take() else { break };
        let ch = f.channel;
        match f.code() {
            Some(codes::OPEN) => {
                writer.lock().await.write_all(&frames::open("switchboard")).await?;
                writer.lock().await.flush().await?;
            }
            Some(codes::BEGIN) => {
                writer.lock().await.write_all(&frames::begin(Some(ch), next_outgoing_id)).await?;
                writer.lock().await.flush().await?;
                session_begun = true;
            }
            Some(codes::ATTACH) if session_begun => {
                let name = f.field(0).and_then(Value::as_str).unwrap_or("").to_string();
                let handle = f.field(1).and_then(Value::as_uint).unwrap_or(0);
                let client_is_receiver = f.field(2).and_then(Value::as_bool) == Some(true);
                // §3.4.3: snd-settle-mode (field 3) of the RECEIVER's
                // attach decides whether our transfers are settled.
                let snd_settle = f.field(3).and_then(Value::as_uint).unwrap_or(0); // 0 = unsettled
                let link = if client_is_receiver {
                    // We deliver: source address (field 5 — after
                    // snd/rcv-settle-mode) selects content.
                    let source_addr = f
                        .field(5)
                        .and_then(|v| v.map_get("address"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let dest = resolve_address(&source_addr);
                    match start_link_consumer(
                        &ctx,
                        &dest,
                        handle,
                        writer.clone(),
                        delivery_counter.clone(),
                        snd_settle == 0,
                        pending_settlements.clone(),
                    )
                    .await
                    {
                        Ok((consumer, credit, notify)) => {
                            let link = Arc::new(tokio::sync::Mutex::new(Link {
                                handle,
                                client_is_receiver,
                                consumer: Some(consumer.clone()),
                                target: None,
                                credit,
                                notify,
                                unsettled: snd_settle == 0,
                            }));
                            links.lock().await.insert(handle, link.clone());
                            link
                        }
                        Err(e) => {
                            tracing::debug!(err = %e, "amqp10: attach source failed");
                            let _ = writer
                                .lock()
                                .await
                                .write_all(&frames::detach(handle))
                                .await;
                            return Ok(());
                        }
                    }
                } else {
                    // The client sends: capture the target address.
                    let target = f
                        .field(6)
                        .and_then(|v| v.map_get("address"))
                        .and_then(Value::as_str)
                        .map(resolve_address);
                    let link = Arc::new(tokio::sync::Mutex::new(Link {
                        handle,
                        client_is_receiver,
                        consumer: None,
                        target,
                        credit: Arc::new(AtomicI64::new(0)),
                        notify: Arc::new(tokio::sync::Notify::new()),
                        unsettled: false,
                    }));
                    links.lock().await.insert(handle, link.clone());
                    link
                };
                // Reply attach with OUR role (opposite of the client's),
                // echoing the receiver's settle expectations.
                let reply = if link.lock().await.client_is_receiver {
                    frames::attach_sender_mode(&name, handle, link.lock().await.unsettled)
                } else {
                    frames::attach_receiver(&name, handle, "")
                };
                writer.lock().await.write_all(&reply).await?;
                writer.lock().await.flush().await?;
            }
            Some(codes::FLOW) => {
                let handle = f.field(8).and_then(Value::as_uint);
                let credit = f.field(3).and_then(Value::as_uint).unwrap_or(0);
                if let Some(h) = handle {
                    if let Some(link) = links.lock().await.get(&h) {
                        let l = link.lock().await;
                        l.credit.fetch_add(credit as i64, Ordering::Relaxed);
                        l.notify.notify_waiters();
                    }
                }
            }
            Some(codes::TRANSFER) => {
                let handle = f.field(0).and_then(Value::as_uint).unwrap_or(0);
                let delivery_id = f.field(1).and_then(Value::as_uint);
                let settled = f.field(4).and_then(Value::as_bool).unwrap_or(true);
                if let Some(link) = links.lock().await.get(&handle).cloned() {
                    let target = link.lock().await.target.clone();
                    // A malformed payload is skipped (no publish) but MUST
                    // NOT skip the loop's frame refill — a `continue` here
                    // would drop the connection with the client's next
                    // frame unread.
                    let message = parse_message(&f.payload).unwrap_or_else(|e| {
                        tracing::debug!(err = %e, "amqp10: bad message payload");
                        ParsedMessage { body: Vec::new(), content_type: None, to: None }
                    });
                    // Destination: link target, else the message's `to`
                    // (anonymous relay).
                    let dest = target.clone().or_else(|| message.to.as_ref().map(|t| resolve_address(t)));
                    let publish_result = match dest {
                        Some(dest) => publish_to(&ctx, &dest, &message).await,
                        None => Err(BrokerError::invalid_path(
                            "no target address and no message `to` (anonymous relay target missing)",
                        )
                        .channel_level()),
                    };
                    if let (Some(first), false) = (delivery_id, settled) {
                        writer
                            .lock()
                            .await
                            .write_all(&frames::disposition_accepted(ch, first, first))
                            .await?;
                        writer.lock().await.flush().await?;
                    }
                    if let Err(e) = publish_result {
                        tracing::debug!(err = %e, "amqp10: transfer rejected");
                    }
                }
            }
            Some(codes::DISPOSITION) => {
                // Settle unsettled outbound deliveries: role=sender
                // dispositions from the receiver cover OUR transfers.
                // accepted → shard-ack; released/rejected → requeue (the
                // shard re-delivers with `redelivered` set).
                let role_receiver = f.field(0).and_then(Value::as_bool).unwrap_or(false);
                let first = f.field(1).and_then(Value::as_uint);
                let last = f
                    .field(2)
                    .and_then(Value::as_uint)
                    .or(first);
                // Receiver-role or malformed dispositions are ignored;
                // fall through to the frame refill either way.
                if !role_receiver {
                    if let (Some(first), Some(last)) = (first, last) {
                        let state_is_release = match f.field(4) {
                            Some(Value::Described(d, _)) => {
                                matches!(**d, Value::ULong(0x25) | Value::ULong(0x26))
                            }
                            _ => false,
                        };
                        let mut pending = pending_settlements.lock().await;
                        for id in first..=last {
                            if let Some((consumer, seq)) = pending.remove(&id) {
                                if state_is_release {
                                    consumer.release(&node, seq).await;
                                } else {
                                    let _ = consumer.ack(&node, seq).await;
                                }
                            }
                        }
                    }
                }
            }
            Some(codes::DETACH) => {
                let handle = f.field(0).and_then(Value::as_uint).unwrap_or(0);
                if let Some(link) = links.lock().await.remove(&handle) {
                    teardown_link(&ctx, &link).await;
                }
                writer.lock().await.write_all(&frames::detach(handle)).await?;
                writer.lock().await.flush().await?;
            }
            Some(codes::END) => {
                writer.lock().await.write_all(&frames::end()).await?;
                writer.lock().await.flush().await?;
                break;
            }
            Some(codes::CLOSE) => {
                writer.lock().await.write_all(&frames::close()).await?;
                writer.lock().await.flush().await?;
                break;
            }
            _ => {}
        }
        frame = match read_frame(&mut reader).await {
            Ok(f) => f,
            Err(e) => {
                tracing::debug!(err = %e, "amqp10: frame read failed; closing connection");
                break;
            }
        };
    }

    // Teardown remaining links.
    for (_, link) in links.lock().await.drain() {
        teardown_link(&ctx, &link).await;
    }
    // A protocol error can fire while the peer still has bytes in flight
    // (a pipelined frame body behind the offending header). Closing now
    // would RST the socket under those bytes; drain what has already
    // arrived (bounded, brief) so the peer sees a clean EOF.
    {
        use tokio::io::AsyncReadExt;
        let mut scratch = [0u8; 8192];
        let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, reader.read(&mut scratch)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(_)) => {}
                Ok(Err(_)) => break,
            }
        }
    }
    Ok(())
}

/// Read one transport frame; `Ok(None)` on EOF.
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<frames::Frame>> {
    use tokio::io::AsyncReadExt;
    let mut head = [0u8; 8];
    if reader.read_exact(&mut head).await? == 0 {
        return Ok(None);
    }
    // §2.3 frame header: size (4), doff (1), type (1), channel (2).
    let size = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
    let doff = head[4] as usize;
    if doff < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "amqp1: bad frame offset",
        ));
    }
    let header_len = doff * 4;
    if size < header_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "amqp1: frame smaller than fixed header",
        ));
    }
    let mut extended = vec![0u8; header_len - 8];
    if header_len > 8 {
        reader.read_exact(&mut extended).await?;
    }
    let mut body = vec![0u8; size - header_len];
    if !body.is_empty() {
        reader.read_exact(&mut body).await?;
    }
    // Reassemble header + extended + body for the decoder.
    let mut full = Vec::with_capacity(size);
    full.extend_from_slice(&head);
    if header_len > 8 {
        full.extend_from_slice(&extended[..header_len - 8]);
    }
    full.extend_from_slice(&body);
    match frames::decode_frame(&full) {
        Ok((f, _)) if f.frame_type == 0xFF => Ok(None), // "need more" sentinel
        Ok((f, _)) => Ok(Some(f)),
        Err(e) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
    }
}

/// Create queue/binding for a destination and return its shard.
async fn ensure_destination(
    ctx: &Arc<BridgeContext>,
    dest: &Dest,
) -> Result<(String, switchboard_core::topology::GroupId), BrokerError> {
    match dest {
        Dest::Topic { exchange, rk } => {
            shared::ensure_exchange(ctx, exchange).await?;
            let queue = switchboard_core::model::generate_queue_name();
            let shard = shared::declare_queue(ctx, &queue, false, false, true).await?;
            shared::bind(ctx, exchange, &queue, rk).await?;
            Ok((queue, shard))
        }
        Dest::Queue(q) => {
            let topo = ctx.node.topology();
            let shard = match topo.vhosts.get(&ctx.vhost).and_then(|v| v.queues.get(q)) {
                Some(qi) => qi.shard,
                None => shared::declare_queue(ctx, q, true, false, false).await?,
            };
            Ok((q.clone(), shard))
        }
    }
}

/// Publish one inbound transfer through the routing model.
async fn publish_to(
    ctx: &Arc<BridgeContext>,
    dest: &Dest,
    message: &ParsedMessage,
) -> Result<(), BrokerError> {
    let mut props = BasicProperties::new();
    props.content_type = message.content_type.clone();
    match dest {
        Dest::Topic { exchange, rk } => {
            let msg = StoredMessage {
                properties: props,
                body: message.body.clone(),
                exchange: exchange.clone(),
                routing_key: rk.clone(),
                persistent: false,
            };
            shared::publish(ctx, exchange, rk, msg).await?;
        }
        Dest::Queue(q) => {
            let topo = ctx.node.topology();
            if !topo.vhosts.get(&ctx.vhost).map(|v| v.queues.contains_key(q)).unwrap_or(false) {
                shared::declare_queue(ctx, q, true, false, false).await?;
            }
            let msg = StoredMessage {
                properties: props,
                body: message.body.clone(),
                exchange: String::new(),
                routing_key: q.clone(),
                persistent: false,
            };
            shared::publish(ctx, "", q, msg).await?;
        }
    }
    Ok(())
}

/// Wire a broker→client link: queue + consumer + forwarder honoring
/// client-granted credit. Returns the consumer plus the credit counter
/// and notify handle the Link must store (so FLOW updates wake this
/// forwarder).
async fn start_link_consumer<W>(
    ctx: &Arc<BridgeContext>,
    dest: &Dest,
    handle: u32,
    writer: SharedWriter<W>,
    delivery_counter: Arc<AtomicU32>,
    unsettled: bool,
    pending_settlements: Arc<tokio::sync::Mutex<HashMap<u32, (Arc<BridgeConsumer>, u64)>>>,
) -> Result<(Arc<BridgeConsumer>, Arc<AtomicI64>, Arc<tokio::sync::Notify>), BrokerError>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (queue, shard) = ensure_destination(ctx, dest).await?;
    // Unsettled links must HOLD messages until the client's disposition
    // settles them (ack on accepted, requeue on released/rejected).
    let (consumer, mut rx) =
        BridgeConsumer::start(ctx, &queue, shard, 0, !unsettled, &format!("amqp10-{handle}")).await?;
    let credit_counter = Arc::new(AtomicI64::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());
    let task_consumer = consumer.clone();
    let task_credit = credit_counter.clone();
    let task_notify = notify.clone();
    let task_handle = handle;
    let task_writer = writer.clone();
    let task_delivery = delivery_counter.clone();
    let task_unsettled = unsettled;
    let task_pending = pending_settlements.clone();
    tokio::spawn(async move {
        let consumer = task_consumer;
        let credit_counter = task_credit;
        let notify = task_notify;
        let handle = task_handle;
        let writer = task_writer;
        let delivery_counter = task_delivery;
        let unsettled = task_unsettled;
        let pending_settlements = task_pending;
        loop {
            // Wait for credit.
            while credit_counter.load(Ordering::Relaxed) <= 0 {
                notify.notified().await;
            }
            let Some(d) = rx.recv().await else { break };
            if credit_counter.fetch_sub(1, Ordering::Relaxed) <= 0 {
                // Credit vanished between check and take: put it back and
                // wait for the next grant.
                credit_counter.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let delivery_id = delivery_counter.fetch_add(1, Ordering::Relaxed);
            let tag = delivery_id.to_be_bytes();
            let payload = encode_message(d.message.properties.content_type.as_deref(), &d.message.body);
            let frame = frames::transfer(0, handle, delivery_id, &tag, !unsettled, &payload);
            if unsettled {
                // Unsettled: remember the delivery until the client's
                // disposition settles it.
                pending_settlements
                    .lock()
                    .await
                    .insert(delivery_id, (consumer.clone(), d.seq));
            }
            let mut guard = writer.lock().await;
            if guard.write_all(&frame).await.is_err() || guard.flush().await.is_err() {
                break;
            }
        }
    });
    Ok((consumer, credit_counter, notify))
}

/// Stop a link's consumer and drop its bridge queue.
async fn teardown_link(ctx: &Arc<BridgeContext>, link: &Arc<tokio::sync::Mutex<Link>>) {
    let mut l = link.lock().await;
    if let Some(consumer) = l.consumer.take() {
        consumer.stop(&ctx.node).await;
        shared::delete_queue(ctx, &consumer.queue).await;
    }
}
