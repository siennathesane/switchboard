//! AMQP 1.0 soak workload. Uses the server crate's own frame encoders
//! (`switchboard_server::protocols::amqp10::{frames, types}`) — the
//! *server's* decode path is the reference, so encoding with its
//! builders while checking behavior keeps the soak about the broker,
//! not about codec archaeology. (The 0-9-1 smoke path in
//! switchboard-server tests is deliberately independent instead.)
//!
//! Sequential link pair on `/queue/soak.a10`: settled sender →
//! unsettled receiver. Each cycle: transfer one message, read it back,
//! disposition `accepted` (which must ack it server-side via the
//! unsettled path). In-order exactly-once via the shared body codec.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use switchboard_server::protocols::amqp10::frames;
use switchboard_server::protocols::amqp10::types::Value;

use crate::soak::check;
use crate::soak::client;
use crate::soak::pacer::Pacer;
use crate::soak::Ctx;

const HEADER: &[u8; 8] = b"AMQP\0\x01\0\0";

struct A10Conn {
    r: tokio::net::tcp::OwnedReadHalf,
    w: tokio::net::tcp::OwnedWriteHalf,
    host: String,
}

/// Attach as receiver with snd-settle-mode = unsettled (0): broker→
/// client transfers arrive unsettled; our disposition completes them.
fn attach_receiver_unsettled(name: &str, handle: u32, address: &str) -> Vec<u8> {
    let source = Value::Map(vec![(
        Value::Symbol("address".into()),
        Value::String(address.into()),
    )]);
    frames::encode_frame(
        0,
        frames::codes::ATTACH,
        vec![
            Value::String(name.into()),
            Value::UInt(handle),
            Value::Bool(true),  // role: receiver
            Value::UByte(0),    // snd-settle-mode: unsettled
            Value::Null,
            source,
            Value::Null,
            Value::Null,
            Value::Bool(false),
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    )
}

fn data_section(body: &[u8]) -> Vec<u8> {
    let v = Value::Described(
        Box::new(Value::ULong(frames::codes::SECTION_DATA)),
        Box::new(Value::Binary(body.to_vec())),
    );
    let mut out = Vec::new();
    switchboard_server::protocols::amqp10::types::encode(&v, &mut out);
    out
}

async fn read_frame(
    r: &mut (impl AsyncReadExt + Unpin),
    deadline: tokio::time::Instant,
) -> Result<(u8, u16, Option<u64>, Vec<u8>), String> {
    // (frame_type, channel, code, payload)
    let mut hdr = [0u8; 8];
    tokio::select! {
        x = r.read_exact(&mut hdr) => { x.map_err(|e| e.to_string())?; }
        _ = tokio::time::sleep_until(deadline) => return Err("amqp10 read deadline".into()),
    }
    let size = u32::from_be_bytes(hdr[0..4].try_into().unwrap()) as usize;
    if size < 8 || size > 4 * 1024 * 1024 {
        return Err(format!("amqp10 bad frame size {size}"));
    }
    let doff = hdr[4] as usize * 4;
    if doff < 8 || doff > size {
        return Err(format!("amqp10 bad data offset {doff}"));
    }
    let frame_type = hdr[5];
    let channel = u16::from_be_bytes(hdr[6..8].try_into().unwrap());
    let mut payload = vec![0u8; size - doff];
    if !payload.is_empty() {
        tokio::select! {
            x = r.read_exact(&mut payload) => { x.map_err(|e| e.to_string())?; }
            _ = tokio::time::sleep_until(deadline) => return Err("amqp10 read deadline (body)".into()),
        }
    }
    // payload begins with the performative; decode its code (first
    // described-type constructor byte + varint).
    let code = if payload.is_empty() || payload[0] != 0x00 {
        return Err("amqp10 frame without described performative".into());
    } else {
        // descriptor: smallulong (0x52/0x53) or ulong (0x80)
        match payload[1] {
            0x52 | 0x53 => Some(payload[2] as u64),
            0x80 => Some(u64::from_be_bytes(payload[2..10].try_into().unwrap())),
            other => return Err(format!("amqp10 bad descriptor {other:#x}")),
        }
    };
    Ok((frame_type, channel, code, payload))
}

async fn dial(ctx: &Arc<Ctx>, workload: &str) -> Option<A10Conn> {
    loop {
        if !client::alive(ctx) {
            return None;
        }
        let Some(host) = ctx.endpoints.random().await else {
            ctx.infra_bounce(workload, "-", "no endpoints for amqp10".into());
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        match raw_dial(&host).await {
            Ok(c) => return Some(c),
            Err(e) => {
                ctx.infra_bounce(workload, &host, format!("amqp10 connect failed: {e}"));
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn raw_dial(host: &str) -> Result<A10Conn, String> {
    let s = TcpStream::connect(host).await.map_err(|e| e.to_string())?;
    s.set_nodelay(true).ok();
    let (mut r, mut w) = s.into_split();
    w.write_all(HEADER).await.map_err(|e| e.to_string())?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    // Server offers SASL mechanisms; we proceed without SASL (guest).
    let (ft, _ch, code, _) = read_frame(&mut r, deadline).await?;
    if ft != frames::FRAME_TYPE_SASL || code != Some(frames::codes::SASL_MECHANISMS) {
        return Err(format!("expected sasl-mechanisms, got type={ft} code={code:?}"));
    }
    w.write_all(&frames::open("switchboard-soak")).await.map_err(|e| e.to_string())?;
    let (ft, _ch, code, _) = read_frame(&mut r, deadline).await?;
    if ft != 0 || code != Some(frames::codes::OPEN) {
        return Err(format!("expected open, got type={ft} code={code:?}"));
    }
    w.write_all(&frames::begin(None, 0)).await.map_err(|e| e.to_string())?;
    let (ft, _ch, code, _) = read_frame(&mut r, deadline).await?;
    if ft != 0 || code != Some(frames::codes::BEGIN) {
        return Err(format!("expected begin, got type={ft} code={code:?}"));
    }
    Ok(A10Conn { r, w, host: host.to_string() })
}

impl A10Conn {
    async fn send(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.w.write_all(bytes).await.map_err(|e| e.to_string())?;
        self.w.flush().await.map_err(|e| e.to_string())
    }
}

pub async fn run(ctx: Arc<Ctx>) {
    let rate = ctx.cfg.rates.amqp10;
    if rate <= 0.0 {
        return;
    }
    let mut pacer = Pacer::new(rate);
    let mut seq: u64 = 0;
    let mut delivery_id: u32 = 0;
    'conn: loop {
        if !client::consuming(&ctx) {
            return;
        }
        let Some(mut c) = dial(&ctx, "amqp10").await else {
            return;
        };
        let deadline0 = tokio::time::Instant::now() + Duration::from_secs(10);
        // Receiver link (unsettled) with credit, and sender link
        // (settled), both onto the same queue.
        if c.send(&attach_receiver_unsettled("soak-a10-rx", 0, "/queue/soak.a10")).await.is_err()
            || c.send(&frames::attach_sender("soak-a10-tx", 1, Some("/queue/soak.a10"))).await.is_err()
            || c.send(&frames::flow_credit(0, 0, 64)).await.is_err()
        {
            ctx.infra_bounce("amqp10", &c.host, "attach/flow write failed".into());
            continue 'conn;
        }
        // Wait for both attach acknowledgements (role-swapped attaches
        // from the server) before transferring.
        let mut attach_acks = 0;
        while attach_acks < 2 {
            match read_frame(&mut c.r, deadline0).await {
                Ok((0, _, Some(frames::codes::ATTACH), _)) => attach_acks += 1,
                Ok((0, _, Some(frames::codes::FLOW), _)) => {}
                Ok((_, _, Some(frames::codes::DETACH), _)) => {
                    ctx.error("amqp10", "attach", &c.host, "server detached our link".into());
                    continue 'conn;
                }
                Ok(other) => {
                    ctx.error("amqp10", "attach", &c.host, format!("unexpected frame {other:?}"));
                    continue 'conn;
                }
                Err(e) => {
                    ctx.infra_bounce("amqp10", &c.host, format!("attach ack: {e}"));
                    continue 'conn;
                }
            }
        }
        loop {
            if !ctx.gate.open() {
                tokio::select! {
                    _ = ctx.halt.cancelled() => {
                        let _ = c.send(&frames::close()).await;
                        return;
                    }
                    _ = ctx.gate.wait_open(&ctx.token) => {}
                }
                if !client::consuming(&ctx) { return }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            pacer.wait().await;
            let body = check::encode_body("a10", seq, ctx.cfg.msg_size.min(256).max(32));
            delivery_id = delivery_id.wrapping_add(1).max(1);
            let payload = data_section(&body);
            let tr = frames::transfer(0, 1, delivery_id, b"soak", true, &payload);
            if c.send(&tr).await.is_err() {
                if client::consuming(&ctx) {
                    ctx.infra_bounce("amqp10", &c.host, "transfer write failed".into());
                }
                continue 'conn;
            }
            // Read until our delivery comes back on handle 0.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let mut got = false;
            // Link credit decays per delivered transfer; top it up or
            // the broker silently stops delivering after the initial
            // window.
            if seq % 32 == 0 {
                if c.send(&frames::flow_credit(0, 0, 64)).await.is_err() {
                    if client::consuming(&ctx) {
                        ctx.infra_bounce("amqp10", &c.host, "flow write failed".into());
                    }
                    continue 'conn;
                }
            }
            loop {
                match read_frame(&mut c.r, deadline).await {
                    Ok((0, _, Some(frames::codes::TRANSFER), payload)) => {
                        // The delivered frame is performative + message
                        // sections; find our exact encoded data section
                        // within it (bodies are unique per seq).
                        if contains_slice(&payload, &payload_marker(&body)) {
                            got = true;
                            // disposition accepted (unsettled path).
                            let d = frames::disposition_accepted(0, delivery_id, delivery_id);
                            if c.send(&d).await.is_err() {
                                ctx.infra_bounce("amqp10", &c.host, "disposition write failed".into());
                                continue 'conn;
                            }
                            ctx.ledger.metrics.add("amqp10.delivered", 1);
                        } else if ctx.cfg.chaos {
                            // A release during churn re-delivers an older
                            // transfer. Accept and move on — integrity is
                            // the pipeline workload's job.
                            let d = frames::disposition_accepted(0, delivery_id, delivery_id);
                            let _ = c.send(&d).await;
                            ctx.ledger.legal_dup();
                            ctx.ledger.metrics.add("dup.legal", 1);
                        } else {
                            ctx.error("amqp10", "body", &c.host, "delivered payload mismatch".into());
                            got = true;
                        }
                    }
                    Ok((0, _, Some(frames::codes::FLOW), _)) => {}
                    Ok((0, _, Some(frames::codes::DISPOSITION), _)) => {}
                    Ok((_, _, Some(frames::codes::DETACH), _)) => {
                        ctx.error("amqp10", "link", &c.host, "server detached mid-stream".into());
                        continue 'conn;
                    }
                    Ok(other) => {
                        ctx.error("amqp10", "frame", &c.host, format!("unexpected {other:?}"));
                    }
                    Err(e) => {
                        if client::consuming(&ctx) {
                            ctx.infra_bounce("amqp10", &c.host, format!("await transfer: {e}"));
                        }
                        continue 'conn;
                    }
                }
                if got {
                    break;
                }
            }
            seq += 1;
            ctx.ledger.metrics.add("amqp10.confirmed", 1);
        }
    }
}

fn payload_marker(body: &[u8]) -> Vec<u8> {
    // The exact encoded data-section for this body.
    let mut out = Vec::new();
    let v = Value::Described(
        Box::new(Value::ULong(frames::codes::SECTION_DATA)),
        Box::new(Value::Binary(body.to_vec())),
    );
    switchboard_server::protocols::amqp10::types::encode(&v, &mut out);
    out
}

fn contains_slice(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len().max(1)).any(|w| w == needle)
}

pub fn spawn_all(ctx: &Arc<Ctx>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut v = Vec::new();
    if ctx.cfg.rates.amqp10 > 0.0 {
        v.push(tokio::spawn(run(ctx.clone())));
    }
    v
}
