//! AMQP 0-9-1 client plumbing shared by the soak workloads.
//!
//! Every fallible operation classifies into exactly one of:
//! * an error already recorded in the ledger (callers back off / retry),
//! * clean `Ok` — including the token-cancelled case (`None`), where
//!   the workload exits without a sound.

use std::sync::Arc;
use std::time::Duration;

use lapin::options::BasicAckOptions;
use lapin::options::BasicConsumeOptions;
use lapin::options::BasicPublishOptions;
use lapin::options::BasicQosOptions;
use lapin::options::ConfirmSelectOptions;
use lapin::options::QueueDeclareOptions;
use lapin::publisher_confirm::Confirmation;
use lapin::types::FieldTable;
use lapin::Channel;
use lapin::Connection;
use lapin::ConnectionProperties;
use lapin::message::Delivery;

use super::Ctx;

/// Connect with the soak error policy: in chaos-shaped runs a refused
/// connect is an infrastructure transition (pods are dying by design);
/// in static runs it is an error. Either way, retry forever with
/// backoff until it works or the token fires. The returned connection
/// is fresh; the caller re-establishes channels on top.
pub async fn connect(ctx: &Arc<Ctx>, workload: &str) -> Option<(Connection, String)> {
    loop {
        if ctx.token.is_cancelled() {
            return None;
        }
        let host = ctx.endpoints.random().await;
        match host {
            None => {
                // No live endpoints at all: the cluster is mid-churn.
                ctx.infra_bounce(workload, "-", "no endpoints available".into());
                tokio::select! {
                    _ = ctx.token.cancelled() => return None,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
            }
            Some(h) => match Connection::connect(&ctx.uri(&h), ConnectionProperties::default()).await {
                Ok(c) => return Some((c, h)),
                Err(e) => {
                    ctx.infra_bounce(workload, &h, format!("amqp connect failed: {e}"));
                    tokio::select! {
                        _ = ctx.token.cancelled() => return None,
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    }
                }
            },
        }
    }
}

/// Connection for one-shot setup operations (setup phase runs before
/// chaos starts, so failures here are plain errors).
pub async fn connect_setup(ctx: &Arc<Ctx>, host: &str, workload: &str) -> Option<Connection> {
    let mut attempt = 0;
    loop {
        if ctx.token.is_cancelled() {
            return None;
        }
        match Connection::connect(&ctx.uri(host), ConnectionProperties::default()).await {
            Ok(c) => return Some(c),
            Err(e) => {
                attempt += 1;
                if attempt > 30 {
                    ctx.error(workload, "connect", host, format!("amqp connect failed: {e}"));
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

/// Open a channel, or `None` on shutdown/error (already recorded).
pub async fn channel(
    ctx: &Arc<Ctx>,
    conn: &Connection,
    workload: &str,
    host: &str,
) -> Option<Channel> {
    match conn.create_channel().await {
        Ok(c) => Some(c),
        Err(e) => {
            ctx.error(workload, "channel.open", host, format!("create_channel failed: {e}"));
            None
        }
    }
}

/// Put a channel in confirm mode.
pub async fn confirm_mode(ctx: &Arc<Ctx>, ch: &Channel, workload: &str, host: &str) -> bool {
    match ch.confirm_select(ConfirmSelectOptions::default()).await {
        Ok(_) => true,
        Err(e) => {
            ctx.error(workload, "confirm.select", host, format!("confirm_select failed: {e}"));
            false
        }
    }
}

/// Declare a durable queue (equivalent re-declares are fine).
pub async fn declare_durable(
    ctx: &Arc<Ctx>,
    host: &str,
    queue: &str,
    workload: &str,
) -> Result<(), String> {
    let Some(conn) = connect_setup(ctx, host, workload).await else {
        return Err("shutting down".into());
    };
    let Some(ch) = channel(ctx, &conn, workload, host).await else {
        return Err("shutting down".into());
    };
    match ch
        .queue_declare(
            queue,
            QueueDeclareOptions { durable: true, ..Default::default() },
            FieldTable::default(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            ctx.error(workload, "queue.declare", host, format!("{queue}: {e}"));
            Err(e.to_string())
        }
    }
}

/// Per-host cached polling connection: depth polls run at a few Hz per
/// host during drains; a fresh TCP+AMQP handshake per poll was both
/// wasteful and polluting the churn signal. The cache is process-global
/// so the drain loop and the scaler share it. A dead channel is dropped
/// and re-established on the next poll.
type PollCache = std::collections::HashMap<String, Channel>;
fn poll_cache() -> &'static tokio::sync::Mutex<PollCache> {
    static CACHE: std::sync::OnceLock<tokio::sync::Mutex<PollCache>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::Mutex::new(Default::default()))
}

async fn polling_connection(
    ctx: &Arc<Ctx>,
    host: &str,
    workload: &str,
) -> Option<Channel> {
    let cache = poll_cache();
    let mut map = cache.lock().await;
    if let Some(ch) = map.get(host) {
        // A channel whose connection died reports a closed state; clone
        // of a live channel shares the underlying connection.
        if ch.status().connected() {
            return Some(ch.clone());
        }
        map.remove(host);
    }
    let conn = connect_setup(ctx, host, workload).await?;
    let ch = channel(ctx, &conn, workload, host).await?;
    map.insert(host.to_string(), ch.clone());
    Some(ch)
}

/// Depth read on the cached polling connection; returns the queue's
/// message count (or an error already recorded). Used by the readiness
/// gate, the drain loop, and the scaler's quiesce drain.
pub async fn declare_durable_expect(
    ctx: &Arc<Ctx>,
    host: &str,
    queue: &str,
    workload: &str,
) -> Result<u32, String> {
    let Some(ch) = polling_connection(ctx, host, workload).await else {
        return Err("shutting down".into());
    };
    match ch
        .queue_declare(
            queue,
            QueueDeclareOptions { durable: true, ..Default::default() },
            FieldTable::default(),
        )
        .await
    {
        Ok(q) => Ok(q.message_count()),
        Err(e) => {
            // The channel is now poisoned (server closed it); drop the
            // cache entry so the next poll reconnects.
            invalidate_polling(host).await;
            ctx.error(workload, "queue.declare", host, format!("{queue}: {e}"));
            Err(e.to_string())
        }
    }
}

async fn invalidate_polling(host: &str) {
    poll_cache().lock().await.remove(host);
}

/// Publish one message and await its publisher confirm. Returns:
/// * `Some(true)` — confirmed (ack),
/// * `Some(false)` — Nack/timeout/error (recorded),
/// * `None` — cancelled, or a publish failure (recorded).
pub async fn publish_confirmed(
    ctx: &Arc<Ctx>,
    ch: &Channel,
    host: &str,
    workload: &str,
    exchange: &str,
    routing_key: &str,
    body: &[u8],
) -> Option<bool> {
    let props = lapin::BasicProperties::default().with_delivery_mode(2);
    let fut = ch.basic_publish(
        exchange,
        routing_key,
        BasicPublishOptions::default(),
        body,
        props,
    );
    let confirm = match fut.await {
        Ok(c) => c,
        Err(e) => {
            // A dead channel/connection: the publish never left, so this
            // is a transport failure, not a lost message.
            ctx.infra_bounce(workload, host, format!("basic_publish failed: {e}"));
            return None;
        }
    };
    match tokio::time::timeout(ctx.cfg.confirm_timeout, confirm).await {
        Ok(Ok(Confirmation::Ack(_))) => Some(true),
        Ok(Ok(other)) => {
            ctx.error(workload, "confirm", host, format!("publish not acked: {other:?}"));
            Some(false)
        }
        Ok(Err(e)) => {
            ctx.error(workload, "confirm", host, format!("confirm error: {e}"));
            Some(false)
        }
        Err(_) => {
            ctx.error(
                workload,
                "confirm.timeout",
                host,
                format!("no confirm within {:?}", ctx.cfg.confirm_timeout),
            );
            Some(false)
        }
    }
}

/// Subscribe with per-message acks and a prefetch window. Delivery
/// errors surface through the returned stream as `Err(lapin::Error)`.
pub async fn consume_acked(
    ctx: &Arc<Ctx>,
    ch: &Channel,
    host: &str,
    workload: &str,
    queue: &str,
    prefetch: u16,
) -> Option<lapin::Consumer> {
    if let Err(e) = ch.basic_qos(prefetch, BasicQosOptions::default()).await {
        ctx.error(workload, "qos", host, format!("{queue}: {e}"));
        return None;
    }
    match ch
        .basic_consume(
            queue,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        Ok(c) => Some(c),
        Err(e) => {
            ctx.error(workload, "consume", host, format!("{queue}: {e}"));
            None
        }
    }
}

/// Ack a delivery; failure is a client-visible error.
pub async fn ack(ctx: &Arc<Ctx>, d: &Delivery, workload: &str, host: &str) {
    if let Err(e) = d.acker.ack(BasicAckOptions::default()).await {
        ctx.error(workload, "ack", host, format!("basic_ack failed: {e}"));
    }
}

/// True while publisher-type work should keep running.
pub fn alive(ctx: &Ctx) -> bool {
    !ctx.token.is_cancelled()
}

/// True while consumer-type work should keep running: consumers outlive
/// the publisher phase so the drain sees every confirmed message.
pub fn consuming(ctx: &Ctx) -> bool {
    !ctx.halt.is_cancelled()
}

/// Connect for a consumer during the drain phase (publishers are
/// already stopped; the exit condition is `halt`, not `token`).
pub async fn connect_drain(ctx: &Arc<Ctx>, workload: &str) -> Option<(Connection, String)> {
    loop {
        if ctx.halt.is_cancelled() {
            return None;
        }
        let host = ctx.endpoints.random().await;
        match host {
            None => {
                ctx.infra_bounce(workload, "-", "no endpoints available".into());
                tokio::select! {
                    _ = ctx.halt.cancelled() => return None,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
            }
            Some(h) => match Connection::connect(&ctx.uri(&h), ConnectionProperties::default()).await {
                Ok(c) => return Some((c, h)),
                Err(e) => {
                    ctx.infra_bounce(workload, &h, format!("amqp connect failed: {e}"));
                    tokio::select! {
                        _ = ctx.halt.cancelled() => return None,
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    }
                }
            },
        }
    }
}
