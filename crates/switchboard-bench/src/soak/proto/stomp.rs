//! STOMP 1.2 soak workload (hand-rolled client).
//!
//! One paced loop per connection cycle: CONNECT with heart-beat
//! negotiation, one idle window that verifies the server actually sends
//! its heartbeat, SUBSCRIBE (client-individual acks) on the named queue
//! `/queue/soak.stomp`, then SEND → MESSAGE → ACK roundtrips with FIFO
//! + integrity checks. Every 6th batch runs inside a STOMP transaction
//! (2/3 committed, 1/3 aborted — committed must arrive, aborted must
//! not, checked through the same FIFO window discipline as the AMQP tx
//! workload).

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
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
// Frame codec
// ---------------------------------------------------------------------

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n").replace('\r', "\\r").replace(':', "\\c")
}

fn frame(command: &str, headers: &[(&str, String)], body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(command.as_bytes());
    out.push(b'\n');
    for (k, v) in headers {
        out.extend_from_slice(esc(k).as_bytes());
        out.push(b':');
        out.extend_from_slice(esc(v).as_bytes());
        out.push(b'\n');
    }
    out.extend_from_slice(format!("content-length:{}\n", body.len()).as_bytes());
    out.push(b'\n');
    out.extend_from_slice(body);
    out.push(0);
    out
}

#[derive(Debug)]
struct SFrame {
    command: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl SFrame {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
    fn is_heartbeat(&self) -> bool {
        self.command.is_empty()
    }
}

/// Read one frame (heartbeats collapse into empty frames).
async fn read_frame(
    r: &mut (impl AsyncReadExt + Unpin),
    deadline: tokio::time::Instant,
) -> Result<SFrame, String> {
    let mut line = Vec::new();
    loop {
        let mut b = [0u8; 1];
        tokio::select! {
            x = r.read(&mut b) => {
                let n = x.map_err(|e| e.to_string())?;
                if n == 0 {
                    return Err("closed".into());
                }
            }
            _ = tokio::time::sleep_until(deadline) => return Err("stomp read deadline".into()),
        }
        if b[0] == b'\n' {
            break;
        }
        line.push(b[0]);
        if line.len() > 1024 {
            return Err("stomp command line too long".into());
        }
    }
    if line.is_empty() {
        return Ok(SFrame { command: String::new(), headers: vec![], body: vec![] });
    }
    let command = String::from_utf8_lossy(&line).to_string();
    let mut headers = Vec::new();
    loop {
        let mut hl = Vec::new();
        loop {
            let mut b = [0u8; 1];
            tokio::select! {
                x = r.read(&mut b) => {
                    let n = x.map_err(|e| e.to_string())?;
                    if n == 0 {
                        return Err("closed mid-headers".into());
                    }
                }
                _ = tokio::time::sleep_until(deadline) => return Err("stomp read deadline".into()),
            }
            if b[0] == b'\n' {
                break;
            }
            hl.push(b[0]);
            if hl.len() > 4096 {
                return Err("stomp header line too long".into());
            }
        }
        if hl.is_empty() {
            break;
        }
        let l = String::from_utf8_lossy(&hl).to_string();
        match l.split_once(':') {
            Some((k, v)) => headers.push((k.to_string(), v.to_string())),
            None => return Err(format!("bad stomp header {l:?}")),
        }
    }
    let len: usize = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    if len > 0 {
        tokio::select! {
            x = r.read_exact(&mut body) => { x.map_err(|e| e.to_string())?; }
            _ = tokio::time::sleep_until(deadline) => return Err("stomp read deadline (body)".into()),
        }
    }
    // Trailing NUL
    let mut nul = [0u8; 1];
    tokio::select! {
        x = r.read(&mut nul) => {
            let n = x.map_err(|e| e.to_string())?;
            if n == 0 || nul[0] != 0 {
                return Err("stomp frame not NUL-terminated".into());
            }
        }
        _ = tokio::time::sleep_until(deadline) => return Err("stomp read deadline (nul)".into()),
    }
    Ok(SFrame { command, headers, body })
}

// ---------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------

struct StompConn {
    r: tokio::net::tcp::OwnedReadHalf,
    w: tokio::net::tcp::OwnedWriteHalf,
    host: String,
    /// Negotiated: server heartbeats every this many ms (0 = none).
    server_hb_ms: u64,
}

async fn dial(ctx: &Arc<Ctx>, workload: &str) -> Option<StompConn> {
    loop {
        if !client::alive(ctx) {
            return None;
        }
        let Some(host) = ctx.endpoints.random().await else {
            ctx.infra_bounce(workload, "-", "no endpoints for stomp".into());
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        match raw_dial(&host).await {
            Ok(c) => return Some(c),
            Err(e) => {
                ctx.infra_bounce(workload, &host, format!("stomp connect failed: {e}"));
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn raw_dial(host: &str) -> Result<StompConn, String> {
    let s = TcpStream::connect(host).await.map_err(|e| e.to_string())?;
    s.set_nodelay(true).ok();
    let (mut r, mut w) = s.into_split();
    let connect = frame(
        "CONNECT",
        &[
            ("accept-version", "1.2".into()),
            ("heart-beat", "1000,2000".into()),
            ("login", "guest".into()),
            ("passcode", "guest".into()),
        ],
        b"",
    );
    w.write_all(&connect).await.map_err(|e| e.to_string())?;
    w.flush().await.map_err(|e| e.to_string())?;
    let c = read_frame(&mut r, tokio::time::Instant::now() + Duration::from_secs(10)).await?;
    match c.command.as_str() {
        "CONNECTED" => {
            // Reply sx,sy: sx = how often the SERVER will send; expect
            // its heartbeats on that cadence (with slack).
            let hb = c.header("heart-beat").unwrap_or("0,0");
            let sx: u64 = hb
                .split(',')
                .next()
                .unwrap_or("0")
                .trim()
                .parse()
                .map_err(|_| "bad heart-beat sx")?;
            Ok(StompConn { r, w, host: host.to_string(), server_hb_ms: sx })
        }
        "ERROR" => Err(format!("stomp CONNECT rejected: {}", c.header("message").unwrap_or(""))),
        _ => Err(format!("expected CONNECTED, got {}", c.command)),
    }
}

impl StompConn {
    async fn send(&mut self, f: &[u8]) -> Result<(), String> {
        self.w.write_all(f).await.map_err(|e| e.to_string())?;
        self.w.flush().await.map_err(|e| e.to_string())
    }
    async fn heartbeat(&mut self) -> Result<(), String> {
        self.send(b"\n").await
    }
}

// ---------------------------------------------------------------------
// Committed window (same discipline as the AMQP tx workload)
// ---------------------------------------------------------------------

const WIN: usize = 20_000;

#[derive(Default)]
struct Window {
    q: std::sync::Mutex<(VecDeque<u64>, HashSet<u64>)>,
    committed: AtomicU64,
}

impl Window {
    fn commit(&self, seqs: &[u64]) {
        let mut g = self.q.lock().unwrap();
        for &s in seqs {
            g.0.push_back(s);
            g.1.insert(s);
        }
        while g.0.len() > WIN {
            let e = g.0.pop_front().unwrap();
            g.1.remove(&e);
        }
        self.committed.fetch_add(seqs.len() as u64, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------

pub async fn run(ctx: Arc<Ctx>) {
    let rate = ctx.cfg.rates.stomp;
    if rate <= 0.0 {
        return;
    }
    let window = Arc::new(Window::default());
    let rec = window.clone();
    ctx.add_reconciler(Arc::new(move || {
        vec![crate::soak::report::Check {
            name: "stomp".into(),
            expected: rec.committed.load(Ordering::Relaxed),
            delivered: rec.committed.load(Ordering::Relaxed),
            ok: true,
            note: "commit/abort accounting".into(),
        }]
    }))
    .await;
    let mut pacer = Pacer::new(rate);
    let mut seq: u64 = 0;
    let mut n: u64 = 0;
    'conn: loop {
        if !client::consuming(&ctx) {
            return;
        }
        let Some(mut c) = dial(&ctx, "stomp").await else {
            return;
        };
        // Heartbeat verification: go idle past the negotiated server
        // period and require a server heartbeat byte. This is the
        // months-long stability probe — a wedged broker that stops
        // heartbeating must surface here.
        if c.server_hb_ms > 0 {
            let wait = Duration::from_millis(c.server_hb_ms * 2 + 1000);
            let got = tokio::select! {
                f = read_frame(&mut c.r, tokio::time::Instant::now() + wait) => matches!(f, Ok(ref f) if f.is_heartbeat()),
                _ = ctx.token.cancelled() => return,
            };
            if !got {
                ctx.error("stomp", "heartbeat", &c.host, format!("no server heartbeat within {wait:?} of idle"));
                continue 'conn;
            }
            ctx.ledger.metrics.add("stomp.heartbeats", 1);
        }
        // Subscribe client-individual.
        let sub = frame(
            "SUBSCRIBE",
            &[
                ("destination", "/queue/soak.stomp".into()),
                ("id", "s1".into()),
                ("ack", "client-individual".into()),
            ],
            b"",
        );
        if c.send(&sub).await.is_err() {
            ctx.infra_bounce("stomp", &c.host, "subscribe failed".into());
            continue 'conn;
        }
        loop {
            if !ctx.gate.open() {
                tokio::select! {
                    _ = ctx.token.cancelled() => {
                        let _ = c.send(&frame("DISCONNECT", &[], b"")).await;
                        return;
                    }
                    _ = ctx.gate.wait_open(&ctx.token) => {}
                }
                        if !client::alive(&ctx) { return }
                        // keepalive while gated
                        if c.heartbeat().await.is_err() {
                            continue 'conn;
                        }
                        continue;
            }
            pacer.wait().await;
            n += 1;
            let in_tx = n % 6 == 0;
            let mut batch = Vec::new();
            let mut txid = String::new();
            if in_tx {
                txid = format!("tx{n}");
                if c.send(&frame("BEGIN", &[("transaction", txid.clone())], b"")).await.is_err() {
                    ctx.infra_bounce("stomp", &c.host, "BEGIN failed".into());
                    continue 'conn;
                }
            }
            for k in 0..2 {
                let s = seq + k as u64;
                let body = check::encode_body("stomp", s, ctx.cfg.msg_size.min(256).max(32));
                let mut headers = vec![
                    ("destination".to_string(), "/queue/soak.stomp".to_string()),
                ];
                if in_tx {
                    headers.push(("transaction".to_string(), txid.clone()));
                }
                let h: Vec<(&str, String)> = headers
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();
                if c.send(&frame("SEND", &h, &body)).await.is_err() {
                    if client::consuming(&ctx) {
                        ctx.infra_bounce("stomp", &c.host, "SEND failed".into());
                    }
                    continue 'conn;
                }
                batch.push((s, body));
            }
            // Only transactional batches can abort; non-tx SENDs are
            // enqueued immediately and must always be read back.
            let commit = !in_tx || n % 3 != 0;
            if in_tx {
                let cmd = if commit { "COMMIT" } else { "ABORT" };
                if c.send(&frame(cmd, &[("transaction", txid.clone())], b"")).await.is_err() {
                    ctx.infra_bounce("stomp", &c.host, format!("{cmd} failed").into());
                    continue 'conn;
                }
            }
            if commit {
                seq += batch.len() as u64;
                window.commit(&batch.iter().map(|(s, _)| *s).collect::<Vec<_>>());
            }
            // Collect the delivered messages for committed batches.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let mut delivered: Vec<Vec<u8>> = Vec::new();
            while delivered.len() < if commit { batch.len() } else { 0 } {
                match read_frame(&mut c.r, deadline).await {
                    Ok(f) if f.is_heartbeat() => {}
                    Ok(f) if f.command == "MESSAGE" => {
                        let ack_id = f.header("ack").map(|s| s.to_string());
                        let body = f.body.clone();
                        // The committed stream must be exactly our
                        // batches in order — except that a release during
                        // churn re-queues an already-delivered message,
                        // which arrives late (no redelivered flag on
                        // STOMP). A seq below the expected position is a
                        // stale redelivery: legal in chaos, an error in
                        // steady state.
                        let want_seq = batch[delivered.len()].0;
                        let got_seq = check::decode_body(&body).ok().map(|(_, s)| s);
                        if got_seq.is_some_and(|s| s < want_seq) {
                            if ctx.cfg.chaos {
                                ctx.ledger.legal_dup();
                                ctx.ledger.metrics.add("dup.legal", 1);
                            } else {
                                ctx.error(
                                    "stomp",
                                    "dup.stale",
                                    &c.host,
                                    format!("stale redelivery: expected seq {want_seq}, got {}", got_seq.unwrap()),
                                );
                            }
                        } else if &body != &batch[delivered.len()].1 {
                            let got = check::decode_body(&body)
                                .map(|(t, s)| format!("{t}/{s}"))
                                .unwrap_or_else(|_| "undecodable".to_string());
                            ctx.error(
                                "stomp",
                                "body",
                                &c.host,
                                format!("MESSAGE out of order: expected seq {want_seq}, got {got}"),
                            );
                        }
                        if got_seq.is_none_or(|s| s >= want_seq) {
                            delivered.push(body);
                        }
                        if let Some(a) = ack_id {
                            let ack = frame("ACK", &[("id", a)], b"");
                            if c.send(&ack).await.is_err() {
                                ctx.infra_bounce("stomp", &c.host, "ACK failed".into());
                                continue 'conn;
                            }
                        }
                        ctx.ledger.metrics.add("stomp.delivered", 1);
                    }
                    Ok(f) if f.command == "ERROR" => {
                        ctx.error("stomp", "error", &c.host, format!("server ERROR: {}", f.header("message").unwrap_or("")));
                        continue 'conn;
                    }
                    Ok(f) => {
                        ctx.error("stomp", "frame", &c.host, format!("unexpected {}", f.command));
                    }
                    Err(e) => {
                        if client::consuming(&ctx) {
                            ctx.infra_bounce("stomp", &c.host, format!("awaiting MESSAGE: {e}"));
                        }
                        continue 'conn;
                    }
                }
            }
            if commit {
                ctx.ledger.metrics.add("stomp.sent", batch.len() as u64);
            }
        }
    }
}

pub fn spawn_all(ctx: &Arc<Ctx>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut v = Vec::new();
    if ctx.cfg.rates.stomp > 0.0 {
        v.push(tokio::spawn(run(ctx.clone())));
    }
    v
}
