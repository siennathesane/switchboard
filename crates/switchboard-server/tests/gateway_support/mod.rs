//! Test support for the protocol-gateway suite: a single broker node
//! behind the full protocol gateway, plus tiny raw-socket client helpers
//! for MQTT / STOMP / WebSocket / AMQP 1.0.

#![allow(dead_code)]

use std::sync::Arc;

static TRACING: std::sync::Once = std::sync::Once::new();

fn init_tracing() {
    TRACING.call_once(|| {
    // Test tempo: shrink fixed delays (raft elections, ticks, join
    // deadlines) 50x. Uniform time-warp — ordering unchanged.
    unsafe { std::env::set_var("SB_TIME_SCALE", "50"); }

        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init()
            .ok();
    });
}
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// Grab a free TCP port.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One SHARED gateway for the whole test binary: starting a fresh broker
/// per test accumulates RocksDB/raft state that degrades later tests. The
/// node runs on a dedicated runtime stored in this static and lives for
/// the whole binary; tests are pure clients.
static LAST_NODE: std::sync::OnceLock<Arc<switchboard_cluster::ClusterNode>> =
    std::sync::OnceLock::new();

static SHARED: std::sync::OnceLock<(tokio::runtime::Runtime, String)> =
    std::sync::OnceLock::new();

/// The shared gateway's client address (starts the gateway on first use).
/// Initialization happens on a dedicated thread because callers are inside
/// their own tokio runtimes.
pub async fn shared_gateway() -> String {
    if let Some((rt, addr)) = SHARED.get() {
        // Health check: the accept loop must still answer. A dead runtime
        // returns EOF and the gateway is rebuilt once.
        let probe = tokio::net::TcpStream::connect(addr).await;
        match probe {
            Ok(_) => return addr.clone(),
            Err(_) => {
                let _ = rt; // old runtime dropped below with the tuple replaced
            }
        }
    }
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(6)
            .build()
            .unwrap();
        let (addr, guard) = rt.block_on(async {
            start_gateway(switchboard_server::ProtocolConfig::all()).await
        });
        // The shared node lives for the whole binary.
        std::mem::forget(guard);
        let _ = SHARED.set((rt, addr.clone()));
        tx.send(addr).unwrap();
    });
    rx.recv().unwrap()
}

/// Owns a test node and shuts its raft cores down on drop. Without this,
/// every gateway test leaks a live broker (background loops hold their own
/// `Arc`s), and after ~20 tests the zombie janitors starve new commits.
pub struct NodeGuard(pub Arc<switchboard_cluster::ClusterNode>);

impl Drop for NodeGuard {
    fn drop(&mut self) {
        // Blocking shutdown on a dedicated mini-runtime: the test's own
        // runtime is about to be dropped and would cancel a spawned
        // shutdown future, leaving zombie RocksDB instances (whose C++
        // background threads keep compacting and starve later tests).
        let node = self.0.clone();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(10), node.shutdown()).await;
            });
        })
        .join();
    }
}

/// Start one bootstrap node and serve the FULL protocol gateway on the
/// returned address. The returned guard shuts the node down when dropped.
pub async fn start_gateway(
    protocols: switchboard_server::ProtocolConfig,
) -> (String, NodeGuard) {
    start_gateway_tagged(protocols, "gw").await
}

/// Start a gateway with an explicit data-dir tag (for retry isolation).
pub async fn start_gateway_tagged(
    protocols: switchboard_server::ProtocolConfig,
    tag: &str,
) -> (String, NodeGuard) {
    init_tracing();
    let internal = format!("127.0.0.1:{}", free_port());
    let client = format!("127.0.0.1:{}", free_port());
    let dir = std::env::temp_dir().join(format!(
        "switchboard-gw-{tag}-{}-{}",
        std::process::id(),
        free_port()
    ));
    let cfg = switchboard_cluster::NodeConfig {
        id: 1,
        data_dir: dir,
        client_addr: client.clone(),
        internal_addr: internal,
        seeds: vec![],
        bootstrap: true,
        expected_nodes: 1,
        peers: vec![],
            timeouts: Default::default(),
    };
    let node: Arc<switchboard_cluster::ClusterNode> =
        switchboard_cluster::ClusterNode::start(cfg).await.unwrap();
    let _ = LAST_NODE.set(node.clone());
    let listener = tokio::net::TcpListener::bind(&client).await.unwrap();
    let node2 = node.clone();
    tokio::spawn(async move {
        let node = node2;
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                continue;
            };
            let node2 = node.clone();
            let protocols = protocols.clone();
            tokio::spawn(async move {
                let _ = switchboard_server::protocols::serve_client(
                    sock,
                    node2,
                    Default::default(),
                    protocols,
                )
                .await;
            });
        }
    });
    // Formation beat: wait until the shard layout is INSTALLED (not just
    // visible), then prove AMQP queue ops work end-to-end. A node whose
    // layout is still forming rejects the first subscribe/publish with
    // "no shard groups", which intermittently wedged whole test flows.
    for _ in 0..400 {
        let topo = node.topology();
        if !topo.vhosts.is_empty() && !topo.groups.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for _ in 0..400 {
        match switchboard_server::protocols::shared::declare_queue(
            &switchboard_server::protocols::BridgeContext::create(
                node.clone(),
                "/".into(),
            )
            .await,
            "gw-ready-probe",
            false,
            false,
            true,
        )
        .await
        {
            Ok(_) => break,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    (client, NodeGuard(node))
}

// ---------------------------------------------------------------------
// MQTT client (raw bytes)
// ---------------------------------------------------------------------

pub fn mqtt_connect(client_id: &str) -> Vec<u8> {
    mqtt_connect_creds(client_id, None, None)
}

/// CONNECT with optional username/password.
pub fn mqtt_connect_creds(client_id: &str, user: Option<&str>, pass: Option<&str>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&("MQTT".len() as u16).to_be_bytes());
    for b in "MQTT".as_bytes() {
        body.push(*b);
    }
    body.push(4); // level
    let mut flags = 0x02; // clean session
    if user.is_some() {
        flags |= 0x80;
    }
    if pass.is_some() {
        flags |= 0x40;
    }
    body.push(flags);
    body.extend_from_slice(&60u16.to_be_bytes()); // keepalive
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    if let Some(u) = user {
        body.extend_from_slice(&(u.len() as u16).to_be_bytes());
        body.extend_from_slice(u.as_bytes());
    }
    if let Some(p) = pass {
        body.extend_from_slice(&(p.len() as u16).to_be_bytes());
        body.extend_from_slice(p.as_bytes());
    }
    let mut out = vec![0x10];
    mqtt_remaining(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}

pub fn mqtt_subscribe(pid: u16, filter: &str, qos: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
    body.extend_from_slice(filter.as_bytes());
    body.push(qos);
    let mut out = vec![0x82];
    mqtt_remaining(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}

pub fn mqtt_publish(topic: &str, payload: &[u8], qos: u8, pid: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if qos > 0 {
        body.extend_from_slice(&pid.to_be_bytes());
    }
    body.extend_from_slice(payload);
    let mut out = vec![0x30 | (qos << 1)];
    mqtt_remaining(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}

pub fn mqtt_publish_retained(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(payload);
    let mut out = vec![0x31]; // PUBLISH, qos0, retain
    mqtt_remaining(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}

pub fn mqtt_remaining(mut n: usize, out: &mut Vec<u8>) {
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

/// Read one MQTT packet: returns (type, body).
pub async fn mqtt_read<R: AsyncRead + Unpin>(
    r: &mut R,
) -> std::io::Result<Option<(u8, Vec<u8>)>> {
    let mut b = [0u8; 1];
    let n = r.read(&mut b).await?;
    if n == 0 {
        return Ok(None);
    }
    let ptype = b[0] >> 4;
    let mut remaining = 0usize;
    let mut mult = 1usize;
    loop {
        r.read_exact(&mut b).await?;
        remaining += ((b[0] & 0x7F) as usize) * mult;
        mult *= 128;
        if b[0] & 0x80 == 0 {
            break;
        }
    }
    let mut body = vec![0u8; remaining];
    r.read_exact(&mut body).await?;
    Ok(Some((ptype, body)))
}

// ---------------------------------------------------------------------
// WebSocket client
// ---------------------------------------------------------------------

pub async fn ws_connect(addr: &str, path: &str, subprotocol: Option<&str>) -> WsClient {
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
    );
    if let Some(p) = subprotocol {
        req.push_str(&format!("Sec-WebSocket-Protocol: {p}\r\n"));
    }
    req.push_str("\r\n");
    let sock = TcpStream::connect(addr).await.unwrap();
    let (mut r, mut w) = tokio::io::split(sock);
    w.write_all(req.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    // Read the 101 response head.
    let mut head = Vec::new();
    let mut buf = [0u8; 512];
    loop {
        let n = tokio::io::AsyncReadExt::read(&mut r, &mut buf).await.unwrap();
        assert!(n > 0, "ws upgrade failed");
        head.extend_from_slice(&buf[..n]);
        if head.windows(4).any(|x| x == b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&head);
    assert!(text.contains("101 Switching Protocols"), "got: {text}");
    let leftover = head
        .windows(4)
        .position(|x| x == b"\r\n\r\n")
        .map(|i| head[i + 4..].to_vec())
        .unwrap_or_default();
    WsClient { reader: r, writer: w, leftover }
}

pub struct WsClient {
    reader: tokio::io::ReadHalf<TcpStream>,
    writer: tokio::io::WriteHalf<TcpStream>,
    leftover: Vec<u8>,
}

impl WsClient {
    /// Send one masked binary frame.
    pub async fn send(&mut self, payload: &[u8]) {
        let mut frame = vec![0x82];
        let len = payload.len();
        if len < 126 {
            frame.push(0x80 | len as u8);
        } else {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        }
        let mask = [0x11, 0x22, 0x33, 0x44];
        frame.extend_from_slice(&mask);
        for (i, b) in payload.iter().enumerate() {
            frame.push(b ^ mask[i % 4]);
        }
        self.writer.write_all(&frame).await.unwrap();
        self.writer.flush().await.unwrap();
    }

    /// Read one unmasked data frame's payload.
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        let r = &mut self.reader;
        // Drain leftover first.
        let mut raw = std::mem::take(&mut self.leftover);
        loop {
            if let Some((payload, used)) = try_parse_frame(&raw) {
                raw.drain(..used);
                self.leftover = raw;
                return Some(payload);
            }
            let mut buf = [0u8; 4096];
            let n = AsyncReadExt::read(&mut self.reader, &mut buf).await.unwrap();
            if n == 0 {
                return None;
            }
            raw.extend_from_slice(&buf[..n]);
        }
    }
}

fn try_parse_frame(raw: &[u8]) -> Option<(Vec<u8>, usize)> {
    if raw.len() < 2 {
        return None;
    }
    let opcode = raw[0] & 0x0F;
    let mut len = (raw[1] & 0x7F) as usize;
    let mut offset = 2;
    if len == 126 {
        if raw.len() < 4 {
            return None;
        }
        len = u16::from_be_bytes([raw[2], raw[3]]) as usize;
        offset = 4;
    } else if len == 127 {
        if raw.len() < 10 {
            return None;
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&raw[2..10]);
        len = usize::try_from(u64::from_be_bytes(b)).ok()?;
        offset = 10;
    }
    if raw.len() < offset + len {
        return None;
    }
    let payload = raw[offset..offset + len].to_vec();
    let _ = opcode;
    Some((payload, offset + len))
}

/// Unused placeholder kept out of tests.
pub fn basic_get_unused() {}

/// Convenience: run a closure with a timeout (tests must not hang).
pub async fn with_timeout<F: std::future::Future>(
    fut: F,
    secs: u64,
) -> F::Output {
    tokio::time::timeout(Duration::from_secs(secs), fut)
        .await
        .expect("test timed out")
}

/// Read until `pred` matches a line (for STOMP frames).
pub async fn read_stomp_frame<R: AsyncRead + Unpin>(
    r: &mut R,
) -> std::io::Result<Option<(String, Vec<u8>)>> {
    let mut buf = Vec::new();
    let mut b = [0u8; 1];
    loop {
        if r.read_exact(&mut b).await? == 0 {
            return Ok(None);
        }
        if b[0] == 0 {
            break;
        }
        buf.push(b[0]);
    }
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (command, rest) = text.split_once('\n').unwrap_or((text.as_str(), ""));
    let body_start = rest.find("\n\n").map(|i| i + 2).unwrap_or(rest.len());
    Ok(Some((command.to_string(), rest[body_start..].as_bytes().to_vec())))
}

/// Like [`read_stomp_frame`], but also returns the frame's headers
/// (needed when a test must echo a server-generated header back, e.g.
/// the ack id of a client-individual MESSAGE).
pub async fn read_stomp_frame_full<R: AsyncRead + Unpin>(
    r: &mut R,
) -> std::io::Result<Option<(String, Vec<(String, String)>, Vec<u8>)>> {
    let mut buf = Vec::new();
    let mut b = [0u8; 1];
    loop {
        r.read_exact(&mut b).await?;
        if b[0] == 0 {
            break;
        }
        buf.push(b[0]);
    }
    let text = String::from_utf8_lossy(&buf).into_owned();
    let mut parts = text.splitn(2, "\n\n");
    let head = parts.next().unwrap_or("").to_owned();
    let body = parts.next().unwrap_or("").as_bytes().to_vec();
    let mut lines = head.split('\n');
    let command = lines.next().unwrap_or("").to_owned();
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    Ok(Some((command, headers, body)))
}
