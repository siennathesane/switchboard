//! Raw AMQP 0-9-1 wire client exercising the mandatory-publish +
//! `basic.return` + confirm path (the one corner lapin does not expose
//! cleanly). Written against `switchboard-wire` directly.
//!
//! Per cycle: a mandatory publish that routes must be confirmed without
//! a return; a mandatory publish that does NOT route must come back as
//! `basic.return` carrying the exact body, *and still be confirmed*.

use std::sync::Arc;
use std::time::Duration;

use switchboard_wire::field::FieldTable;
use switchboard_wire::method::Method;
use switchboard_wire::properties::ContentHeader;
use switchboard_wire::BasicProperties;
use switchboard_wire::Frame;
use switchboard_wire::FrameReader;
use switchboard_wire::PROTOCOL_HEADER;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use crate::soak::check;
use crate::soak::client;
use crate::soak::Ctx;

struct RawConn {
    r: tokio::net::tcp::OwnedReadHalf,
    w: tokio::net::tcp::OwnedWriteHalf,
    host: String,
    reader: FrameReader,
}

async fn dial(ctx: &Arc<Ctx>, workload: &str) -> Option<RawConn> {
    loop {
        if !client::alive(ctx) {
            return None;
        }
        let Some(host) = ctx.endpoints.random().await else {
            ctx.infra_bounce(workload, "-", "no endpoints for mandatory".into());
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        match raw_dial(&host).await {
            Ok(c) => return Some(c),
            Err(e) => {
                ctx.infra_bounce(workload, &host, format!("raw connect failed: {e}"));
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn raw_dial(host: &str) -> Result<RawConn, String> {
    let s = TcpStream::connect(host).await.map_err(|e| e.to_string())?;
    s.set_nodelay(true).ok();
    let (r, mut w) = s.into_split();
    w.write_all(&PROTOCOL_HEADER).await.map_err(|e| e.to_string())?;
    let mut c = RawConn { r, w, host: host.to_string(), reader: FrameReader::new() };
    // Start.
    let (start, _) = c.expect().await?;
    match start {
        Method::ConnectionStart { .. } => {}
        other => return Err(format!("expected Connection.Start, got {}", other.name())),
    }
    let mut response = vec![0u8];
    response.extend_from_slice(b"guest");
    response.push(0);
    response.extend_from_slice(b"guest");
    c.send(
        0,
        &Method::ConnectionStartOk {
            client_properties: FieldTable::new(),
            mechanism: "PLAIN".into(),
            response,
            locale: "en_US".into(),
        },
    )
    .await?;
    let (tune, _) = c.expect().await?;
    let Method::ConnectionTune { channel_max, frame_max, heartbeat } = tune else {
        return Err("expected Connection.Tune".into());
    };
    c.send(0, &Method::ConnectionTuneOk { channel_max, frame_max, heartbeat }).await?;
    c.send(
        0,
        &Method::ConnectionOpen {
            virtual_host: "/".into(),
            capabilities: String::new(),
            insist: false,
        },
    )
    .await?;
    let (ok, _) = c.expect().await?;
    let Method::ConnectionOpenOk { .. } = ok else {
        return Err("expected Connection.OpenOk".into());
    };
    c.send(1, &Method::ChannelOpen { out_of_band: String::new() }).await?;
    let (ok, _) = c.expect().await?;
    let Method::ChannelOpenOk { .. } = ok else {
        return Err("expected Channel.OpenOk".into());
    };
    c.send(1, &Method::ConfirmSelect { nowait: false }).await?;
    let (ok, _) = c.expect().await?;
    let Method::ConfirmSelectOk { .. } = ok else {
        return Err("expected ConfirmSelectOk".into());
    };
    Ok(c)
}

impl RawConn {
    async fn send(&mut self, channel: u16, m: &Method) -> Result<(), String> {
        let bytes = Frame::method(channel, m).to_bytes();
        self.w.write_all(&bytes).await.map_err(|e| e.to_string())?;
        self.w.flush().await.map_err(|e| e.to_string())
    }

    /// Read frames until a method arrives; assembles content.
    async fn expect(&mut self) -> Result<(Method, Option<(BasicProperties, Vec<u8>)>), String> {
        let mut pending: Option<ContentHeader> = None;
        let mut body = Vec::new();
        let mut content_for: Option<Method> = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match self.reader.next_frame(0) {
                Ok(Some(frame)) => match frame.frame_type {
                    switchboard_wire::FrameType::Method => {
                        let m = frame.decode_method().map_err(|e| format!("bad method: {e}"))?;
                        if let Some(m) = content_for.take() {
                            let content =
                                pending.map(|h| (h.properties, std::mem::take(&mut body)));
                            return Ok((m, content));
                        }
                        if m.carries_content() {
                            content_for = Some(m);
                            continue;
                        }
                        return Ok((m, None));
                    }
                    switchboard_wire::FrameType::Header => {
                        let h = frame.content_header(60).map_err(|e| format!("bad header: {e}"))?;
                        if h.body_size == 0 {
                            let m = content_for.take().ok_or("header without method")?;
                            return Ok((m, Some((h.properties, Vec::new()))));
                        }
                        pending = Some(h);
                    }
                    switchboard_wire::FrameType::Body => {
                        body.extend_from_slice(&frame.payload);
                        if let Some(h) = &pending {
                            if body.len() as u64 >= h.body_size {
                                let m = content_for.take().ok_or("body without method")?;
                                let props = h.properties.clone();
                                return Ok((m, Some((props, std::mem::take(&mut body)))));
                            }
                        }
                    }
                    switchboard_wire::FrameType::Heartbeat => continue,
                },
                Ok(None) => {
                    let mut buf = [0u8; 8192];
                    let n = tokio::select! {
                        x = self.r.read(&mut buf) => x.map_err(|e| e.to_string())?,
                        _ = tokio::time::sleep_until(deadline) => return Err("raw read deadline".into()),
                    };
                    if n == 0 {
                        return Err("connection closed".into());
                    }
                    self.reader.feed(&buf[..n]);
                }
                Err(e) => return Err(format!("frame error: {e}")),
            }
        }
    }

    async fn publish(
        &mut self,
        exchange: &str,
        rk: &str,
        body: &[u8],
        mandatory: bool,
    ) -> Result<(), String> {
        self.send(
            1,
            &Method::BasicPublish {
                ticket: 0,
                exchange: exchange.into(),
                routing_key: rk.into(),
                mandatory,
                immediate: false,
            },
        )
        .await?;
        let h = Frame::header(
            1,
            &ContentHeader::new(
                body.len() as u64,
                BasicProperties { delivery_mode: Some(2), ..Default::default() },
            ),
        )
        .to_bytes();
        self.w.write_all(&h).await.map_err(|e| e.to_string())?;
        let b = Frame::body(1, body).to_bytes();
        self.w.write_all(&b).await.map_err(|e| e.to_string())?;
        self.w.flush().await.map_err(|e| e.to_string())
    }
}

pub async fn run(ctx: Arc<Ctx>) {
    let per_min = ctx.cfg.rates.mandatory;
    if per_min <= 0.0 {
        return;
    }
    let period = Duration::from_secs_f64(60.0 / per_min);
    let mut n: u64 = 0;
    'conn: loop {
        if !client::consuming(&ctx) {
            return;
        }
        let Some(mut c) = dial(&ctx, "mandatory").await else {
            return;
        };
        loop {
            if !ctx.gate.open() {
                tokio::select! {
                    _ = ctx.token.cancelled() => return,
                    _ = ctx.gate.wait_open(&ctx.token) => {}
                }
                if !client::alive(&ctx) { return }
                continue;
            }
            tokio::select! {
                _ = ctx.token.cancelled() => return,
                _ = tokio::time::sleep(period) => {}
            }
            n += 1;
            // 1. Routable mandatory publish: confirm only, no return.
            let body = check::encode_body("mand", n, 64);
            if let Err(e) = c.publish("", "soak.mand", &body, true).await {
                if client::consuming(&ctx) {
                    ctx.infra_bounce("mandatory", &c.host, format!("publish: {e}"));
                }
                continue 'conn;
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let mut acked = false;
            loop {
                match c.expect().await {
                    Ok((Method::BasicAck { .. }, None)) => {
                        acked = true;
                        break;
                    }
                    Ok((Method::BasicReturn { reply_code, reply_text, .. }, _)) => {
                        ctx.error(
                            "mandatory",
                            "return",
                            &c.host,
                            format!("routable mandatory publish was returned: {reply_code} {reply_text}"),
                        );
                    }
                    Ok((m, _)) => {
                        ctx.error("mandatory", "frame", &c.host, format!("unexpected {}", m.name()));
                    }
                    Err(e) => {
                        if client::consuming(&ctx) {
                            ctx.infra_bounce("mandatory", &c.host, format!("await ack: {e}"));
                        }
                        break;
                    }
                }
                if tokio::time::Instant::now() > deadline {
                    break;
                }
            }
            if !acked {
                continue 'conn;
            }
            ctx.ledger.metrics.add("mandatory.routable", 1);

            // 2. Unroutable mandatory publish: basic.return with the
            // exact body, then the confirm.
            let body2 = check::encode_body("mand", n + 1_000_000_000, 64);
            if let Err(e) = c.publish("", &format!("soak.mand.missing{n}"), &body2, true).await {
                if client::consuming(&ctx) {
                    ctx.infra_bounce("mandatory", &c.host, format!("publish: {e}"));
                }
                continue 'conn;
            }
            let mut acked2 = false;
            let mut returned2 = false;
            loop {
                if acked2 && returned2 {
                    break;
                }
                match c.expect().await {
                    Ok((Method::BasicAck { .. }, None)) => acked2 = true,
                    Ok((Method::BasicReturn { reply_code, .. }, Some((_, got_body)))) => {
                        returned2 = true;
                        if got_body != body2 {
                            ctx.error(
                                "mandatory",
                                "return.body",
                                &c.host,
                                "basic.return body does not match the published message".into(),
                            );
                        }
                        if reply_code != 312 {
                            ctx.error(
                                "mandatory",
                                "return.code",
                                &c.host,
                                format!("expected 312 no_route, got {reply_code}"),
                            );
                        }
                    }
                    Ok((m, _)) => {
                        ctx.error("mandatory", "frame", &c.host, format!("unexpected {}", m.name()));
                    }
                    Err(e) => {
                        if client::consuming(&ctx) {
                            ctx.infra_bounce("mandatory", &c.host, format!("await return: {e}"));
                        }
                        break;
                    }
                }
                if tokio::time::Instant::now() > deadline {
                    break;
                }
            }
            if !returned2 || !acked2 {
                ctx.error(
                    "mandatory",
                    "return.missing",
                    &c.host,
                    format!("unroutable mandatory publish: returned={returned2} confirmed={acked2}"),
                );
                continue 'conn;
            }
            ctx.ledger.metrics.add("mandatory.returned", 1);
        }
    }
}

pub fn spawn_all(ctx: &Arc<Ctx>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut v = Vec::new();
    if ctx.cfg.rates.mandatory > 0.0 {
        v.push(tokio::spawn(run(ctx.clone())));
    }
    v
}
