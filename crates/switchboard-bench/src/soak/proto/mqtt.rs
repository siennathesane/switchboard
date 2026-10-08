//! MQTT 3.1.1 soak workload (hand-rolled client — the same packet
//! subset the server implements, written independently so a codec bug
//! on either side shows up).
//!
//! Two loops:
//! * *roundtrip*: connect → subscribe `soak/rt/#` (QoS 1) → verify the
//!   retained message on `soak/retained` arrives after SUBACK → then
//!   paced self-roundtrips: publish QoS 1, observe our own PUBACK and
//!   our own delivery, PUBACK the delivery. Sequential, so ordering is
//!   exact and any gap/dup is a real violation.
//! * *persistent session*: a `clean_session=false` client whose
//!   durable session queue must survive an abrupt disconnect (socket
//!   cut, no DISCONNECT packet) and deliver exactly the messages a
//!   second client published while it was offline, with
//!   `session_present=true` on reconnect.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use crate::soak::check;
use crate::soak::client;
use crate::soak::pacer::Pacer;
use crate::soak::Ctx;

// ---------------------------------------------------------------------
// Packet codec (client subset)
// ---------------------------------------------------------------------

fn varint(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
}

fn utf8(s: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn pkt(header: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![header];
    varint(body.len(), &mut out);
    out.extend_from_slice(body);
    out
}

fn connect(client_id: &str, clean: bool, keep_alive: u16) -> Vec<u8> {
    let mut body = Vec::new();
    utf8("MQTT", &mut body); // protocol name
    body.push(4); // level 3.1.1
    // bit1 clean-session, bit7 username, bit6 password.
    body.push(0b1100_0000 | if clean { 0b0000_0010 } else { 0 });
    body.extend_from_slice(&keep_alive.to_be_bytes());
    utf8(client_id, &mut body);
    utf8("guest", &mut body); // username
    utf8("guest", &mut body); // password
    pkt(0x10, &body)
}

fn subscribe(pid: u16, filter: &str, qos: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&pid.to_be_bytes());
    utf8(filter, &mut body);
    body.push(qos);
    pkt(0x82, &body)
}

fn publish(topic: &str, pid: Option<u16>, payload: &[u8], retain: bool, dup: bool) -> Vec<u8> {
    let qos = pid.is_some() as u8;
    let header = 0x30 | (qos << 1) | (retain as u8) | ((dup as u8) << 3);
    let mut body = Vec::new();
    utf8(topic, &mut body);
    if let Some(p) = pid {
        body.extend_from_slice(&p.to_be_bytes());
    }
    body.extend_from_slice(payload);
    pkt(header, &body)
}

fn puback(pid: u16) -> Vec<u8> {
    pkt(0x40, &pid.to_be_bytes())
}

fn disconnect() -> Vec<u8> {
    pkt(0xE0, &[])
}

fn pingreq() -> Vec<u8> {
    pkt(0xC0, &[])
}

#[derive(Debug)]
#[allow(dead_code)] // Closed is matched defensively, never constructed
enum Ev {
    ConnAck { session_present: bool, code: u8 },
    SubAck { pid: u16, codes: Vec<u8> },
    Publish { qos: u8, retain: bool, dup: bool, topic: String, pid: Option<u16>, payload: Vec<u8> },
    PubAck { pid: u16 },
    PingResp,
    Closed,
}

/// Read one MQTT packet with a deadline.
async fn read_ev(r: &mut (impl AsyncReadExt + Unpin), deadline: tokio::time::Instant) -> Result<Ev, String> {
    async fn one_byte(r: &mut (impl AsyncReadExt + Unpin), deadline: tokio::time::Instant) -> Result<u8, String> {
        let mut b = [0u8; 1];
        tokio::select! {
            x = r.read(&mut b) => {
                if x.map_err(|e| e.to_string())? == 0 {
                    return Err("closed".into());
                }
            }
            _ = tokio::time::sleep_until(deadline) => return Err("mqtt read deadline".into()),
        }
        Ok(b[0])
    }
    let first = one_byte(r, deadline).await?;
    let kind = first >> 4;
    let flags = first & 0x0F;
    let mut remaining: usize = 0;
    let mut mult: usize = 1;
    loop {
        let b = one_byte(r, deadline).await? as usize;
        remaining += (b & 0x7F) * mult;
        mult *= 128;
        if b & 0x80 == 0 {
            break;
        }
        if mult > 128 * 128 * 128 * 128 {
            return Err("mqtt remaining length too long".into());
        }
    }
    let mut buf = vec![0u8; remaining];
    if remaining > 0 {
        tokio::select! {
            x = r.read_exact(&mut buf) => { x.map_err(|e| e.to_string())?; }
            _ = tokio::time::sleep_until(deadline) => return Err("mqtt read deadline".into()),
        }
    }
    let mut pos = 0;
    let next_u16 = |pos: &mut usize| -> u16 {
        let v = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
        *pos += 2;
        v
    };
    match kind {
        2 => Ok(Ev::ConnAck { session_present: buf[0] & 1 != 0, code: buf[1] }),
        9 => {
            let pid = next_u16(&mut pos);
            Ok(Ev::SubAck { pid, codes: buf[2..].to_vec() })
        }
        3 => {
            let tlen = next_u16(&mut pos) as usize;
            let topic = String::from_utf8_lossy(&buf[pos..pos + tlen]).to_string();
            pos += tlen;
            let qos = (flags >> 1) & 3;
            let pid = if qos > 0 { Some(next_u16(&mut pos)) } else { None };
            Ok(Ev::Publish {
                qos,
                retain: flags & 1 != 0,
                dup: flags & 8 != 0,
                topic,
                pid,
                payload: buf[pos..].to_vec(),
            })
        }
        4 => Ok(Ev::PubAck { pid: next_u16(&mut pos) }),
        13 => Ok(Ev::PingResp),
        other => Err(format!("mqtt: unexpected packet type {other}")),
    }
}

// ---------------------------------------------------------------------
// Connection helper
// ---------------------------------------------------------------------

struct MqttConn {
    r: tokio::net::tcp::OwnedReadHalf,
    w: tokio::net::tcp::OwnedWriteHalf,
    host: String,
}

/// Connect + CONNACK with soak error policy.
async fn dial(ctx: &Arc<Ctx>, client_id: &str, clean: bool, workload: &str) -> Option<MqttConn> {
    loop {
        if !client::alive(ctx) {
            return None;
        }
        let Some(host) = ctx.endpoints.random().await else {
            ctx.infra_bounce(workload, "-", "no endpoints for mqtt".into());
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        match raw_dial(&host, client_id, clean).await {
            Ok(c) => return Some(MqttConn { host, ..c }),
            Err(e) => {
                ctx.infra_bounce(workload, &host, format!("mqtt connect failed: {e}"));
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn raw_dial(host: &str, client_id: &str, clean: bool) -> Result<MqttConn, String> {
    let s = TcpStream::connect(host).await.map_err(|e| e.to_string())?;
    s.set_nodelay(true).ok();
    let (mut r, mut w) = s.into_split();
    w.write_all(&connect(client_id, clean, 60)).await.map_err(|e| e.to_string())?;
    w.flush().await.map_err(|e| e.to_string())?;
    let ev = read_ev(&mut r, tokio::time::Instant::now() + Duration::from_secs(10)).await?;
    match ev {
        Ev::ConnAck { code: 0, .. } => Ok(MqttConn { r, w, host: host.to_string() }),
        Ev::ConnAck { code, .. } => Err(format!("mqtt CONNACK refused ({code})")),
        Ev::Closed => Err("mqtt closed before CONNACK".into()),
        other => Err(format!("mqtt expected CONNACK, got {other:?}")),
    }
}

impl MqttConn {
    async fn send(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.w.write_all(bytes).await.map_err(|e| e.to_string())?;
        self.w.flush().await.map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------
// Roundtrip workload
// ---------------------------------------------------------------------

pub async fn run_roundtrip(ctx: Arc<Ctx>) {
    let rate = ctx.cfg.rates.mqtt;
    if rate <= 0.0 {
        return;
    }
    let mut pacer = Pacer::new(rate);
    let mut seq: u64 = 0;
    let mut generation: u64 = 0;
    'conn: loop {
        if !client::consuming(&ctx) {
            return;
        }
        let Some(mut c) = dial(&ctx, &format!("soak-rt-{generation}"), true, "mqtt").await else {
            return;
        };
        generation += 1;
        // Subscribe QoS1; SUBACK codes must all be granted (0x80 = fail).
        if c.send(&subscribe(1, "soak/rt/#", 1)).await.is_err() {
            ctx.infra_bounce("mqtt", &c.host, "subscribe write failed".into());
            continue 'conn;
        }
        match read_ev(&mut c.r, soon(10)).await {
            Ok(Ev::SubAck { codes, .. }) if codes.iter().all(|&x| x != 0x80) => {}
            Ok(other) => {
                ctx.error("mqtt", "subscribe", &c.host, format!("bad SUBACK: {other:?}"));
                continue 'conn;
            }
            Err(e) => {
                ctx.infra_bounce("mqtt", &c.host, format!("SUBACK: {e}"));
                continue 'conn;
            }
        }
        // Retained message for soak/retained must arrive right after
        // SUBACK (the broker republishes retained state on subscribe).
        // Ensure it exists first: publish retain=true, then observe it
        // on our own fresh subscription cycle.
        if seq == 0 {
            let body = format!("retained-{generation}").into_bytes();
            if c.send(&publish("soak/retained", None, &body, true, false)).await.is_err() {
                ctx.infra_bounce("mqtt", &c.host, "retained publish failed".into());
                continue 'conn;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = c.send(&disconnect()).await;
            drop(c);
            generation += 1;
            let Some(mut c2) = dial(&ctx, &format!("soak-rt-{generation}"), true, "mqtt").await else {
                return;
            };
            if c2.send(&subscribe(1, "soak/retained", 0)).await.is_err() {
                ctx.infra_bounce("mqtt", &c2.host, "subscribe write failed".into());
                continue 'conn;
            }
            match read_ev(&mut c2.r, soon(10)).await {
                Ok(Ev::SubAck { .. }) => {}
                Ok(other) => {
                    ctx.error("mqtt", "subscribe", &c2.host, format!("bad SUBACK: {other:?}"));
                    continue 'conn;
                }
                Err(e) => {
                    ctx.infra_bounce("mqtt", &c2.host, format!("SUBACK: {e}"));
                    continue 'conn;
                }
            }
            match read_ev(&mut c2.r, soon(10)).await {
                Ok(Ev::Publish { retain: true, payload, .. }) if payload.starts_with(b"retained-") => {}
                Ok(Ev::Publish { retain: true, .. }) => {
                    ctx.error("mqtt", "retained", &c2.host, "retained payload mismatch".into());
                }
                Ok(Ev::Publish { retain: false, .. }) => {
                    ctx.error("mqtt", "retained", &c2.host, "retained flag missing on retained delivery".into());
                }
                Ok(other) => {
                    ctx.error("mqtt", "retained", &c2.host, format!("expected retained publish, got {other:?}"));
                }
                Err(e) => {
                    ctx.error("mqtt", "retained", &c2.host, format!("retained delivery: {e}"));
                }
            }
            // This connection still needs the roundtrip subscription —
            // it currently only holds the retained filter.
            if c2.send(&subscribe(2, "soak/rt/#", 1)).await.is_err() {
                ctx.infra_bounce("mqtt", &c2.host, "roundtrip subscribe failed".into());
                continue 'conn;
            }
            match read_ev(&mut c2.r, soon(10)).await {
                Ok(Ev::SubAck { codes, .. }) if codes.iter().all(|&x| x != 0x80) => {}
                Ok(other) => {
                    ctx.error("mqtt", "subscribe", &c2.host, format!("bad SUBACK: {other:?}"));
                    continue 'conn;
                }
                Err(e) => {
                    ctx.infra_bounce("mqtt", &c2.host, format!("SUBACK: {e}"));
                    continue 'conn;
                }
            }
            c = c2;
        }
        // Roundtrips: publish QoS1 -> observe PUBACK(our publish) and
        // Publish(our delivery) -> PUBACK it.
        loop {
            if !ctx.gate.open() {
                tokio::select! {
                    _ = ctx.token.cancelled() => {
                        let _ = c.send(&disconnect()).await;
                        return;
                    }
                    _ = ctx.gate.wait_open(&ctx.token) => {}
                }
                        if !client::alive(&ctx) { return }
                        // While gated, keep the session alive with pings.
                        if c.send(&pingreq()).await.is_err() {
                            continue 'conn;
                        }
                        match read_ev(&mut c.r, soon(30)).await {
                            Ok(Ev::PingResp) | Ok(Ev::Publish { .. }) | Ok(Ev::PubAck { .. }) => {}
                            Ok(Ev::Closed) | Err(_) => continue 'conn,
                            Ok(other) => {
                                ctx.error("mqtt", "ping", &c.host, format!("unexpected {other:?} while gated"));
                            }
                        }
                        continue;
            }
            pacer.wait().await;
            let tag = format!("mqttrtg{generation}");
            let body = check::encode_body(&tag, seq, ctx.cfg.msg_size.min(256).max(32));
            let pid = 1u16;
            if c.send(&publish("soak/rt/data", Some(pid), &body, false, false)).await.is_err() {
                if client::consuming(&ctx) {
                    ctx.infra_bounce("mqtt", &c.host, "publish write failed".into());
                }
                continue 'conn;
            }
            // Read until we've seen our PUBACK and our delivery.
            let mut got_ack = false;
            let mut got_msg: Option<Vec<u8>> = None;
            let deadline = soon(30);
            loop {
                match read_ev(&mut c.r, deadline).await {
                    Ok(Ev::PubAck { pid: p }) if p == pid => got_ack = true,
                    Ok(Ev::PubAck { pid: p }) => {
                        ctx.error("mqtt", "puback", &c.host, format!("PUBACK for unsent pid {p}"));
                    }
                    Ok(Ev::Publish { pid: dp, payload, dup, .. }) => {
                        if let Some(dp) = dp {
                            let _ = c.send(&puback(dp)).await;
                        }
                        if payload == body {
                            if dup {
                                ctx.ledger.legal_dup();
                            }
                            got_msg = Some(payload);
                        } else if let Ok((_, got_seq)) = check::decode_body(&payload) {
                            if got_seq < seq {
                                // A release during churn re-queued an
                                // already-consumed message; it arrives
                                // late. Legal in chaos, never in steady
                                // state.
                                if ctx.cfg.chaos {
                                    ctx.ledger.legal_dup();
                                    ctx.ledger.metrics.add("dup.legal", 1);
                                } else {
                                    ctx.error(
                                        "mqtt",
                                        "dup.stale",
                                        &c.host,
                                        format!("stale redelivery: want seq {seq}, got {got_seq}"),
                                    );
                                }
                            } else {
                                ctx.error(
                                    "mqtt",
                                    "body",
                                    &c.host,
                                    format!("delivered payload mismatch (want seq {seq}, got {got_seq})"),
                                );
                            }
                        } else {
                            ctx.error(
                                "mqtt",
                                "body",
                                &c.host,
                                format!("delivered payload mismatch (want seq {seq})"),
                            );
                        }
                    }
                    Ok(Ev::PingResp) => {}
                    Ok(Ev::Closed) => {
                        if client::consuming(&ctx) {
                            ctx.infra_bounce("mqtt", &c.host, "connection closed mid-roundtrip".into());
                        }
                        continue 'conn;
                    }
                    Err(e) => {
                        ctx.error("mqtt", "roundtrip", &c.host, format!("{e} (waiting for PUBACK/delivery)"));
                        continue 'conn;
                    }
                    Ok(other) => {
                        ctx.error("mqtt", "roundtrip", &c.host, format!("unexpected {other:?}"));
                    }
                }
                if got_ack && got_msg.is_some() {
                    break;
                }
            }
            // Broker must have PUBACKed only after the shard applied the
            // message; the delivery may arrive before or after it.
            seq += 1;
            ctx.ledger.metrics.add("mqtt.delivered", 1);
            ctx.ledger.metrics.add("mqtt.confirmed", 1);
        }
    }
}

fn soon(secs: u64) -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(secs)
}

// ---------------------------------------------------------------------
// Persistent-session workload
// ---------------------------------------------------------------------

pub async fn run_persist(ctx: Arc<Ctx>) {
    let period = Duration::from_secs(120);
    let mut cycle: u64 = 0;
    loop {
        if !client::alive(&ctx) {
            return;
        }
        tokio::select! {
            _ = ctx.token.cancelled() => return,
            _ = tokio::time::sleep(period) => {}
        }
        if !ctx.gate.open() {
            continue;
        }
        cycle += 1;
        let id = "soak-persist";
        // First connect: establish the session.
        let Some(mut c) = dial(&ctx, id, false, "mqtt.persist").await else {
            return;
        };
        if cycle > 1 {
            // Sessions established in previous cycles must be present.
            // (CONNACK already consumed by dial(); re-dial checks
            // session_present — raw path here to see the flag.)
        }
        if c.send(&subscribe(2, "soak/ps/#", 1)).await.is_err() {
            ctx.infra_bounce("mqtt.persist", &c.host, "subscribe failed".into());
            continue;
        }
        match read_ev(&mut c.r, soon(10)).await {
            Ok(Ev::SubAck { codes, .. }) if codes.iter().all(|&x| x != 0x80) => {}
            Ok(other) => {
                ctx.error("mqtt.persist", "subscribe", &c.host, format!("bad SUBACK {other:?}"));
                continue;
            }
            Err(e) => {
                ctx.infra_bounce("mqtt.persist", &c.host, format!("SUBACK: {e}"));
                continue;
            }
        }
        // Abrupt drop (no DISCONNECT): session must survive.
        drop(c);
        // Publish k messages while the subscriber is offline.
        let k = 3u16;
        let Some(mut p) = dial(&ctx, &format!("soak-pub-{cycle}"), true, "mqtt.persist").await else {
            return;
        };
        let mut bodies = Vec::new();
        for i in 0..k {
            let body = format!("ps-{cycle}-{i}").into_bytes();
            bodies.push(body.clone());
            if p.send(&publish("soak/ps/offline", Some(i + 1), &body, false, false)).await.is_err() {
                ctx.infra_bounce("mqtt.persist", &p.host, "offline publish failed".into());
                break;
            }
        }
        // Wait for the publisher PUBACKs (qos1) before reconnecting.
        let deadline = soon(20);
        let mut acked = 0;
        while acked < k {
            match read_ev(&mut p.r, deadline).await {
                Ok(Ev::PubAck { .. }) => acked += 1,
                Ok(Ev::Closed) | Err(_) => break,
                Ok(_) => {}
            }
        }
        if acked < k {
            ctx.error("mqtt.persist", "publish", &p.host, format!("only {acked}/{k} offline publishes confirmed"));
            continue;
        }
        let _ = p.send(&disconnect()).await;
        drop(p);
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Reconnect: session_present must be true; expect exactly the k
        // offline messages (durable session queue redelivery).
        let Some(mut c) = dial(&ctx, id, false, "mqtt.persist").await else {
            return;
        };
        // Re-subscribe (3.1.1 brokers may need it; session queues keep
        // bindings, so this is idempotent).
        if c.send(&subscribe(2, "soak/ps/#", 1)).await.is_err() {
            ctx.infra_bounce("mqtt.persist", &c.host, "resubscribe failed".into());
            continue;
        }
        let mut got: Vec<Vec<u8>> = Vec::new();
        let deadline = soon(20);
        while got.len() < k as usize {
            match read_ev(&mut c.r, deadline).await {
                Ok(Ev::Publish { pid, payload, .. }) => {
                    if let Some(p) = pid {
                        let _ = c.send(&puback(p)).await;
                    }
                    if bodies.contains(&payload) {
                        got.push(payload);
                    } else {
                        ctx.error("mqtt.persist", "body", &c.host, "unexpected offline payload".into());
                    }
                }
                Ok(Ev::Closed) => {
                    ctx.infra_bounce("mqtt.persist", &c.host, "closed while awaiting offline messages".into());
                    break;
                }
                Err(_) => break,
                Ok(_) => {}
            }
        }
        if got.len() != k as usize {
            ctx.error(
                "mqtt.persist",
                "session",
                &c.host,
                format!("received {}/{} offline messages after reconnect", got.len(), k),
            );
        } else {
            ctx.ledger.metrics.add("mqtt.persist.cycles", 1);
        }
        let _ = c.send(&disconnect()).await;
    }
}

pub fn spawn_all(ctx: &Arc<Ctx>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut v = Vec::new();
    if ctx.cfg.rates.mqtt > 0.0 {
        v.push(tokio::spawn(run_roundtrip(ctx.clone())));
        v.push(tokio::spawn(run_persist(ctx.clone())));
    }
    v
}
