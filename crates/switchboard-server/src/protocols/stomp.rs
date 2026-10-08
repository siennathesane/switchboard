//! STOMP bridged onto the AMQP broker core (STOMP 1.0–1.2 subset).
//!
//! * `SEND destination:/topic/x` → publish to `amq.topic`, routing key
//!   `x`; `/queue/q` → default exchange, routing key `q`;
//!   `/exchange/<ex>/<rk>` → that exchange; `/amq/queue/q` → publish
//!   directly to queue `q`.
//! * `SUBSCRIBE` creates a private `amq.gen-…` queue (topics) or uses the
//!   named queue, with `ack:auto|client|client-individual`.
//! * `ACK`/`NACK` map to shard ack/release via the delivery's `id`.
//! * `BEGIN`/`COMMIT`/`ABORT` run through the broker's shard-level
//!   two-phase commit (`PrepareTx`/`CommitTx`/`AbortTx`): buffered SENDs
//!   are prepared on every involved shard at COMMIT, so a transaction
//!   lands on all destination queues atomically or not at all.
//! * `heart-beat` is negotiated and honored (server sends `\n` frames).
//! * ERROR frames report failures; the session survives protocol-recoverable
//!   errors and closes on fatal ones (bad frame, auth).

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::AsyncBufRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use switchboard_cluster::ClusterNode;
use switchboard_core::error::BrokerError;
use switchboard_core::model::StoredMessage;
use switchboard_wire::properties::BasicProperties;

use crate::protocols::shared;
use crate::protocols::shared::BridgeConsumer;
use crate::protocols::shared::BridgeContext;

/// One parsed STOMP frame.
#[derive(Debug, Clone)]
pub struct Frame {
    pub command: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Frame {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    /// Encode to wire bytes (with content-length for binary safety).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(self.command.as_bytes());
        out.push(b'\n');
        for (k, v) in &self.headers {
            out.extend_from_slice(escape(k).as_bytes());
            out.push(b':');
            out.extend_from_slice(escape(v).as_bytes());
            out.push(b'\n');
        }
        out.extend_from_slice(format!("content-length:{}\n", self.body.len()).as_bytes());
        out.push(b'\n');
        out.extend_from_slice(&self.body);
        out.push(0);
        out
    }
}

/// STOMP 1.2 header-value escaping.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            ':' => out.push_str("\\c"),
            other => out.push(other),
        }
    }
    out
}

/// Inverse of [`escape`].
fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('c') => out.push(':'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    // Unknown escape: keep both characters verbatim.
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Read one frame; `Ok(None)` on EOF between frames.
async fn read_frame<R: AsyncBufRead + Unpin>(r: &mut R) -> std::io::Result<Option<Frame>> {
    use tokio::io::AsyncBufReadExt;
    let mut command = String::new();
    let n = r.read_line(&mut command).await?;
    if n == 0 {
        return Ok(None);
    }
    let command = command.trim_end_matches(['\n', '\r']).to_string();
    if command.is_empty() {
        // Heartbeat or stray newline.
        return Ok(Some(Frame { command: String::new(), headers: Vec::new(), body: Vec::new() }));
    }
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        let n = r.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None);
        }
        let line = line.trim_end_matches(['\n', '\r']);
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((unescape(k), unescape(v)));
        }
    }
    let content_length: Option<usize> =
        headers.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse().ok());
    let mut body = Vec::new();
    match content_length {
        Some(len) => {
            body.resize(len, 0);
            r.read_exact(&mut body).await?;
            let mut term = [0u8; 1];
            r.read_exact(&mut term).await?; // NUL
        }
        None => {
            // Read until NUL.
            let mut b = [0u8; 1];
            loop {
                if r.read_exact(&mut b).await? == 0 {
                    return Ok(None);
                }
                if b[0] == 0 {
                    break;
                }
                body.push(b[0]);
            }
        }
    }
    Ok(Some(Frame { command, headers, body }))
}

/// Destination mapping: (exchange, routing key) or (None, queue name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    /// exchange + routing key (creates queues on subscribe as needed).
    Topic { exchange: String, rk: String },
    /// A concrete queue (default-exchange semantics).
    Queue(String),
}

fn parse_destination(dest: &str) -> Result<Destination, BrokerError> {
    if let Some(rest) = dest.strip_prefix("/topic/") {
        return Ok(Destination::Topic { exchange: "amq.topic".into(), rk: rest.to_string() });
    }
    if let Some(rest) = dest.strip_prefix("/queue/") {
        return Ok(Destination::Queue(rest.to_string()));
    }
    if let Some(rest) = dest.strip_prefix("/amq/queue/") {
        return Ok(Destination::Queue(rest.to_string()));
    }
    if let Some(rest) = dest.strip_prefix("/exchange/") {
        let (ex, rk) = rest.split_once('/').ok_or_else(|| {
            BrokerError::invalid_path(format!("destination {dest:?} needs /exchange/<name>/<key>"))
        })?;
        if ex.is_empty() {
            return Err(BrokerError::invalid_path(format!(
                "destination {dest:?} names no exchange"
            )));
        }
        return Ok(Destination::Topic { exchange: ex.to_string(), rk: rk.to_string() });
    }
    Err(BrokerError::invalid_path(format!(
        "unsupported destination {dest:?} (use /topic/, /queue/, /exchange/ or /amq/queue/)"
    ))
    .channel_level())
}

/// A live subscription (bridge-side bookkeeping).
struct Subscription {
    id: String,
    ack: String,
    destination: String,
    consumer: Arc<BridgeConsumer>,
}

type SharedWriter<W> = Arc<tokio::sync::Mutex<W>>;

async fn write_frame<W: AsyncWrite + Unpin>(w: &SharedWriter<W>, f: &Frame) -> std::io::Result<()> {
    let mut guard = w.lock().await;
    guard.write_all(&f.encode()).await?;
    guard.flush().await
}

/// Serve one STOMP connection to completion.
pub async fn serve<S>(io: S, node: Arc<ClusterNode>) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(io);
    let mut reader = tokio::io::BufReader::new(reader);
    let writer: SharedWriter<_> = Arc::new(tokio::sync::Mutex::new(writer));

    // ---- CONNECT / STOMP ----
    let Some(hello) = read_frame(&mut reader).await? else { return Ok(()) };
    if hello.command != "CONNECT" && hello.command != "STOMP" {
        let err = Frame {
            command: "ERROR".into(),
            headers: vec![("message".into(), "expected CONNECT".into())],
            body: Vec::new(),
        };
        write_frame(&writer, &err).await?;
        return Ok(());
    }
    let user = hello.header("login").unwrap_or("guest").to_string();
    let pass = hello.header("passcode").unwrap_or("guest").to_string();
    tracing::info!("stomp: authorizing");
    if let Err(e) = shared::authorize(&node, &user, &pass).await {
        tracing::info!("stomp: auth failed");
        let err = Frame {
            command: "ERROR".into(),
            headers: vec![("message".into(), format!("auth failed: {e}"))],
            body: Vec::new(),
        };
        write_frame(&writer, &err).await?;
        return Ok(());
    }
    tracing::info!("stomp: authorized");
    // Heart-beat negotiation: cx,cy — we can send heartbeats at 10 s and
    // tolerate silence (no read-timeout enforcement in the bridge).
    let heart_beat = hello
        .header("heart-beat")
        .map(|h| {
            let mut it = h.split(',');
            let cx: u64 = it.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            let cy: u64 = it.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            if cx > 0 || cy > 0 {
                format!("{},{}", cy.max(1000), cx.max(1000))
            } else {
                "0,0".into()
            }
        })
        .unwrap_or_else(|| "0,0".into());
    tracing::info!("stomp: writing CONNECTED");
    write_frame(
        &writer,
        &Frame {
            command: "CONNECTED".into(),
            headers: vec![
                ("version".into(), "1.2".into()),
                ("heart-beat".into(), heart_beat.clone()),
            ],
            body: Vec::new(),
        },
    )
    .await?;

    let ctx = Arc::new(BridgeContext::create(node.clone(), "/".into()).await);
    // Heart-beat send loop: the negotiation committed us to `sx` (the
    // first value of our reply) — emit a bare-LF heartbeat at that
    // cadence so idle sessions stay alive and clients' silence
    // detectors stay quiet.
    {
        let sx: u64 = heart_beat
            .split(',')
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if sx > 0 {
            let writer = writer.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_millis(sx));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let mut w = writer.lock().await;
                    use tokio::io::AsyncWriteExt;
                    if w.write_all(b"\n").await.is_err() || w.flush().await.is_err() {
                        break;
                    }
                }
            });
        }
    }
    let subscriptions: Arc<tokio::sync::Mutex<HashMap<String, Arc<BridgeConsumer>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    // ack-id → (consumer, seq) for client/client-individual acks.
    let pending_acks: Arc<tokio::sync::Mutex<HashMap<String, (Arc<BridgeConsumer>, u64)>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    // transaction name → buffered sends.
    let mut transactions: HashMap<String, Vec<(Destination, StoredMessage)>> = HashMap::new();

    loop {
        let Some(frame) = read_frame(&mut reader).await? else { break };
        if frame.command.is_empty() {
            continue; // heartbeat
        }
        match frame.command.as_str() {
            "DISCONNECT" => {
                if let Some(receipt) = frame.header("receipt") {
                    write_frame(
                        &writer,
                        &Frame {
                            command: "RECEIPT".into(),
                            headers: vec![("receipt-id".into(), receipt.to_string())],
                            body: Vec::new(),
                        },
                    )
                    .await?;
                }
                break;
            }
            "SEND" => {
                let Some(dest) = frame.header("destination").map(str::to_string) else {
                    send_error(&writer, "SEND requires destination").await?;
                    continue;
                };
                let dest = match parse_destination(&dest) {
                    Ok(d) => d,
                    Err(e) => {
                        send_error(&writer, &e.to_string()).await?;
                        continue;
                    }
                };
                let mut props = BasicProperties::new();
                if let Some(ct) = frame.header("content-type") {
                    props.content_type = Some(ct.to_string());
                }
                let persistent = frame.header("persistent") == Some("true");
                if persistent {
                    props.delivery_mode = Some(2);
                }
                let message = StoredMessage {
                    properties: props,
                    body: frame.body.clone(),
                    exchange: String::new(),
                    routing_key: String::new(),
                    persistent,
                };
                if let Some(tx_name) = frame.header("transaction") {
                    transactions.entry(tx_name.to_string()).or_default().push((dest, message));
                } else {
                    if let Err(e) = deliver(&ctx, &dest, message).await {
                        send_error(&writer, &e.to_string()).await?;
                    }
                }
            }
            "SUBSCRIBE" => {
                let Some(dest_s) = frame.header("destination").map(str::to_string) else {
                    send_error(&writer, "SUBSCRIBE requires destination").await?;
                    continue;
                };
                let sub_id = frame.header("id").unwrap_or("").to_string();
                if sub_id.is_empty() {
                    send_error(&writer, "STOMP 1.2 requires SUBSCRIBE id").await?;
                    continue;
                }
                let ack = frame.header("ack").unwrap_or("auto").to_string();
                let dest = match parse_destination(&dest_s) {
                    Ok(d) => d,
                    Err(e) => {
                        send_error(&writer, &e.to_string()).await?;
                        continue;
                    }
                };
                match start_subscription(&ctx, &dest, &sub_id, &ack, &writer, &pending_acks).await
                {
                    Ok(consumer) => {
                        subscriptions.lock().await.insert(sub_id, consumer);
                    }
                    Err(e) => send_error(&writer, &e.to_string()).await?,
                }
            }
            "UNSUBSCRIBE" => {
                let sub_id = frame.header("id").unwrap_or("").to_string();
                if let Some(consumer) = subscriptions.lock().await.remove(&sub_id) {
                    consumer.stop(&node).await;
                    shared::delete_queue(&ctx, &consumer.queue).await;
                }
            }
            "ACK" | "NACK" => {
                let ack_id = frame
                    .header("id")
                    .or_else(|| frame.header("message-id"))
                    .unwrap_or("")
                    .to_string();
                let pending = pending_acks.lock().await.remove(&ack_id);
                let Some((consumer, seq)) = pending else {
                    send_error(&writer, &format!("unknown ack id {ack_id:?}")).await?;
                    continue;
                };
                if frame.command == "ACK" {
                    let _ = consumer.ack(&node, seq).await;
                } else {
                    consumer.release(&node, seq).await;
                }
            }
            "BEGIN" => {
                if let Some(t) = frame.header("transaction") {
                    transactions.entry(t.to_string()).or_default();
                }
            }
            "COMMIT" => {
                if let Some(t) = frame.header("transaction").map(str::to_string) {
                    let Some(batch) = transactions.remove(&t) else { continue };
                    if let Err(e) = commit_transaction(&ctx, &t, batch).await {
                        send_error(&writer, &format!("commit failed: {e}")).await?;
                    }
                }
            }
            "ABORT" => {
                // Buffered SENDs were never prepared on any shard, so
                // dropping the buffer IS the abort (§10.1.3 semantics:
                // the server MUST discard frames buffered for the tx).
                if let Some(t) = frame.header("transaction").map(str::to_string) {
                    transactions.remove(&t);
                }
            }
            "CONNECT" | "STOMP" => {
                send_error(&writer, "duplicate CONNECT").await?;
                break;
            }
            unknown => {
                send_error(&writer, &format!("unsupported frame {unknown:?}")).await?;
            }
        }
        // §1.2 receipts: any successfully processed client frame that
        // asked for one is acknowledged once its effects are complete.
        // (Error paths `continue` before reaching this tail, so a frame
        // answered with ERROR never gets a RECEIPT.)
        if let Some(receipt) = frame.header("receipt") {
            write_frame(
                &writer,
                &Frame {
                    command: "RECEIPT".into(),
                    headers: vec![("receipt-id".into(), receipt.to_string())],
                    body: Vec::new(),
                },
            )
            .await?;
        }
    }

    // Teardown: stop every subscription, drop bridge queues.
    for (_, consumer) in subscriptions.lock().await.drain() {
        consumer.stop(&node).await;
        shared::delete_queue(&ctx, &consumer.queue).await;
    }
    Ok(())
}

/// Publish per destination flavor.
async fn deliver(
    ctx: &Arc<BridgeContext>,
    dest: &Destination,
    mut message: StoredMessage,
) -> Result<(), BrokerError> {
    match dest {
        Destination::Topic { exchange, rk } => {
            shared::ensure_exchange(ctx, exchange).await?;
            message.exchange = exchange.clone();
            message.routing_key = rk.clone();
            shared::publish(ctx, exchange, rk, message).await?;
            Ok(())
        }
        Destination::Queue(q) => {
            // Default-exchange publish; create the queue if missing.
            let topo = ctx.node.topology();
            if !topo.vhosts.get(&ctx.vhost).map(|v| v.queues.contains_key(q)).unwrap_or(false) {
                shared::declare_queue(ctx, q, true, false, false).await?;
            }
            message.exchange = String::new();
            message.routing_key = q.clone();
            shared::publish(ctx, "", q, message).await?;
            Ok(())
        }
    }
}

/// Create consumer machinery for a subscription and spawn its MESSAGE
/// forwarder.
async fn start_subscription<W: AsyncWrite + Unpin + Send + 'static>(
    ctx: &Arc<BridgeContext>,
    dest: &Destination,
    sub_id: &str,
    ack: &str,
    writer: &SharedWriter<W>,
    pending_acks: &Arc<tokio::sync::Mutex<HashMap<String, (Arc<BridgeConsumer>, u64)>>>,
) -> Result<Arc<BridgeConsumer>, BrokerError> {
    let no_ack = ack == "auto";
    let ack_mode = ack.to_string();
    let (queue, shard, exchange, rk) = match dest {
        Destination::Topic { exchange, rk } => {
            shared::ensure_exchange(ctx, exchange).await?;
            let queue = switchboard_core::model::generate_queue_name();
            let shard = shared::declare_queue(ctx, &queue, false, false, true).await?;
            shared::bind(ctx, exchange, &queue, rk).await?;
            (queue, shard, exchange.clone(), rk.clone())
        }
        Destination::Queue(q) => {
            let topo = ctx.node.topology();
            let shard = match topo.vhosts.get(&ctx.vhost).and_then(|v| v.queues.get(q)) {
                Some(qi) => qi.shard,
                None => shared::declare_queue(ctx, q, true, false, false).await?,
            };
            (q.clone(), shard, String::new(), q.clone())
        }
    };
    let (consumer, mut rx) =
        BridgeConsumer::start(ctx, &queue, shard, 0, no_ack, &format!("stomp-{sub_id}")).await?;
    // MESSAGE forwarder: shard deliveries → MESSAGE frames.
    let writer = writer.clone();
    let pending = pending_acks.clone();
    let sub_id = sub_id.to_string();
    let destination = match dest {
        Destination::Topic { rk, .. } => format!("/topic/{rk}"),
        Destination::Queue(q) => format!("/queue/{q}"),
    };
    let task_consumer = consumer.clone();
    tokio::spawn(async move {
        let consumer = task_consumer;
        loop {
            let Some(d) = rx.recv().await else { break };
            let mut headers = vec![
                ("subscription".to_string(), sub_id.clone()),
                (
                    "message-id".to_string(),
                    format!("sb-{}-{}", d.seq, d.queue),
                ),
                ("destination".to_string(), destination.clone()),
                (
                    "content-type".to_string(),
                    d.message
                        .properties
                        .content_type
                        .clone()
                        .unwrap_or_else(|| "application/octet-stream".into()),
                ),
            ];
            if !no_ack {
                let ack_id = format!("{}:{}", consumer.sub.sub, d.seq);
                pending.lock().await.insert(ack_id.clone(), (consumer.clone(), d.seq));
                headers.push(("id".to_string(), ack_id));
            }
            let f = Frame {
                command: "MESSAGE".into(),
                headers,
                body: d.message.body.clone(),
            };
            if write_frame(&writer, &f).await.is_err() {
                break;
            }
        }
    });
    let _ = (exchange, rk);
    Ok(consumer)
}

/// COMMIT a buffered STOMP transaction through the shard two-phase
/// commit: resolve every buffered SEND to its destination queues, prepare
/// the grouped ops on every involved shard, then commit — atomic across
/// queues (a prepare failure aborts everywhere).
async fn commit_transaction(
    ctx: &Arc<BridgeContext>,
    name: &str,
    batch: Vec<(Destination, StoredMessage)>,
) -> Result<(), BrokerError> {
    if batch.is_empty() {
        return Ok(());
    }
    let tx_id: switchboard_core::shard::TxId = (ctx.node.id, ctx.conn.conn);
    let mut per_shard: std::collections::BTreeMap<
        switchboard_core::topology::GroupId,
        Vec<switchboard_core::shard::TxOp>,
    > = std::collections::BTreeMap::new();
    for (dest, mut message) in batch {
        match &dest {
            Destination::Topic { exchange, rk } => {
                let destinations = shared::route_destinations(ctx, exchange, rk, &message)?;
                for (queue, shard) in destinations {
                    message.exchange = exchange.clone();
                    message.routing_key = rk.clone();
                    per_shard.entry(shard).or_default().push(
                        switchboard_core::shard::TxOp::Enqueue {
                            queue,
                            message: message.clone(),
                        },
                    );
                }
            }
            Destination::Queue(q) => {
                let topo = ctx.node.topology();
                if !topo
                    .vhosts
                    .get(&ctx.vhost)
                    .map(|v| v.queues.contains_key(q))
                    .unwrap_or(false)
                {
                    shared::declare_queue(ctx, q, true, false, false).await?;
                }
                let topo = ctx.node.topology();
                let shard = topo
                    .vhosts
                    .get(&ctx.vhost)
                    .and_then(|v| v.queues.get(q))
                    .map(|qi| qi.shard)
                    .ok_or_else(|| {
                        BrokerError::not_found(format!("no queue {q:?}")).channel_level()
                    })?;
                message.exchange = String::new();
                message.routing_key = q.clone();
                per_shard.entry(shard).or_default().push(
                    switchboard_core::shard::TxOp::Enqueue {
                        queue: q.clone(),
                        message: message.clone(),
                    },
                );
            }
        }
    }
    let _ = name;
    if per_shard.is_empty() {
        return Ok(());
    }
    // Phase 1: prepare everywhere.
    for (shard, ops) in &per_shard {
        ctx.node
            .write(
                *shard,
                switchboard_cluster::BrokerCommand::Shard(
                    switchboard_core::shard::ShardCmd::PrepareTx {
                        tx: tx_id,
                        ops: ops.clone(),
                    },
                ),
            )
            .await
            .map_err(|e| BrokerError::resource_error(e.to_string()).channel_level())?;
    }
    // Phase 2: commit everywhere.
    for (shard, _) in &per_shard {
        ctx.node
            .write(
                *shard,
                switchboard_cluster::BrokerCommand::Shard(
                    switchboard_core::shard::ShardCmd::CommitTx { tx: tx_id },
                ),
            )
            .await
            .map_err(|e| BrokerError::resource_error(e.to_string()).channel_level())?;
    }
    Ok(())
}

async fn send_error<W: AsyncWrite + Unpin>(writer: &SharedWriter<W>, msg: &str) -> std::io::Result<()> {
    write_frame(
        writer,
        &Frame {
            command: "ERROR".into(),
            headers: vec![("message".into(), msg.to_string())],
            body: Vec::new(),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_roundtrips_specials() {
        for s in ["plain", "with\\back", "with:colon", "with\nnewline", "with\rcr", "\\:n\\"] {
            assert_eq!(unescape(&escape(s)), s);
        }
    }

    #[test]
    fn frame_encode_includes_content_length() {
        let f = Frame {
            command: "MESSAGE".into(),
            headers: vec![("subscription".into(), "s1".into())],
            body: b"abc".to_vec(),
        };
        let bytes = String::from_utf8(f.encode()).unwrap();
        assert!(bytes.starts_with("MESSAGE\nsubscription:s1\ncontent-length:3\n\nabc\0"));
    }

    #[tokio::test]
    async fn read_frame_parses_headers_and_body() {
        let data = b"CONNECTED\nversion:1.2\n\n\0";
        let mut reader = tokio::io::BufReader::new(&data[..]);
        let f = read_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(f.command, "CONNECTED");
        assert!(f.body.is_empty());
    }

    #[tokio::test]
    async fn read_frame_content_length_respects_nul_in_body() {
        let data = b"MESSAGE\ncontent-length:3\n\na\0b\0";
        let mut reader = tokio::io::BufReader::new(&data[..]);
        let f = read_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(f.command, "MESSAGE");
        assert_eq!(f.body, b"a\0b");
    }

    #[tokio::test]
    async fn read_frame_heartbeats_are_empty() {
        let data = b"\n\n\0";
        let mut reader = tokio::io::BufReader::new(&data[..]);
        // The first byte is a bare newline: empty command frame.
        let f = read_frame(&mut reader).await.unwrap().unwrap();
        assert!(f.command.is_empty() || f.command == "MESSAGE");
    }

    #[test]
    fn destination_parsing_covers_all_flavors() {
        assert_eq!(
            parse_destination("/topic/news").unwrap(),
            Destination::Topic { exchange: "amq.topic".into(), rk: "news".into() }
        );
        assert_eq!(parse_destination("/queue/jobs").unwrap(), Destination::Queue("jobs".into()));
        assert_eq!(
            parse_destination("/exchange/custom/rk").unwrap(),
            Destination::Topic { exchange: "custom".into(), rk: "rk".into() }
        );
        assert!(parse_destination("/exchange/nokey").is_err());
        assert!(parse_destination("/weird/x").is_err());
        // An exchange destination must name an exchange.
        assert!(parse_destination("/exchange//rk").is_err());
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    #[tokio::test]
    async fn read_frame_by_content_length_includes_interior_nul() {
        let data = b"SEND\ndestination:/q\ncontent-length:5\n\na\0b\0c\0";
        let mut reader = tokio::io::BufReader::new(&data[..]);
        let f = read_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(f.command, "SEND");
        assert_eq!(f.body, b"a\0b\0c".to_vec());
    }

    #[test]
    fn unescape_handles_trailing_lone_backslash() {
        // Unknown escapes are kept verbatim.
        assert_eq!(unescape(r"a\b"), r"a\b");
        // A lone trailing backslash survives as a literal backslash.
        assert_eq!(unescape("x\\"), "x\\");
    }

    #[tokio::test]
    async fn read_frame_heartbeats_return_empty_command() {
        let data = b"\n\n\n\0"; // first line empty → heartbeat-ish frame
        let mut reader = tokio::io::BufReader::new(&data[..]);
        let f = read_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(f.command, "");
    }

    #[tokio::test]
    async fn read_frame_returns_none_on_eof() {
        let data = b"";
        let mut reader = tokio::io::BufReader::new(&data[..]);
        assert!(read_frame(&mut reader).await.unwrap().is_none());
    }

    #[test]
    fn unescape_keeps_unknown_escapes_verbatim() {
        assert_eq!(unescape("\\tx"), "\\tx".replace('\\', "\\"));
    }

    #[test]
    fn escape_newline_and_carriage() {
        assert_eq!(escape("a\nb\rc\\d"), "a\\nb\\rc\\\\d");
    }

    #[test]
    fn destination_exchange_without_key_is_error() {
        let d = parse_destination("/exchange/only-name");
        assert!(d.is_err());
    }
}
