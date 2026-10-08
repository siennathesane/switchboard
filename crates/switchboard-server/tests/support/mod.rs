//! A minimal AMQP 0-9-1 test client speaking raw wire protocol over TCP.
//!
//! This is deliberately independent from the server implementation: it
//! encodes frames exactly as §4.2 describes, so a bug on either side shows
//! up as a cross-check failure.
//!
//! Also home of the cluster test helpers (formation, teardown, polling).
#![allow(dead_code)] // each test binary uses a different subset

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use switchboard_wire::field::FieldTable;
use switchboard_wire::method::Method;
use switchboard_wire::properties::ContentHeader;
use switchboard_wire::BasicProperties;
use switchboard_wire::FrameReader;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;

static COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn test_tag() -> u64 {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Install a tracing subscriber once (tests observe node internals).
pub fn init_tracing() {
    static LOG: std::sync::Once = std::sync::Once::new();
    LOG.call_once(|| {
    // Test tempo: shrink fixed delays (raft elections, ticks, join
    // deadlines) 50x. Uniform time-warp — ordering unchanged.
    unsafe { std::env::set_var("SB_TIME_SCALE", "50"); }

        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .try_init()
            .ok();
    });
}

/// Grab a free TCP port (tiny race window; fine for tests).
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub struct TestClient {
    pub reader: FrameReader,
    pub read_half: OwnedReadHalf,
    pub writer: OwnedWriteHalf,
    /// Last non-content method seen (diagnostics for failed expectations).
    pub last_method: Option<String>,
    /// Methods decoded on a channel other than the one an `expect` was
    /// waiting on (server-side notifications race replies across
    /// channels); handed back when that channel is asked for.
    backlog: Vec<(u16, Method)>,
}

impl TestClient {
    pub async fn connect(addr: &str) -> std::io::Result<Self> {
        Self::connect_raw(addr, Some(&switchboard_wire::PROTOCOL_HEADER)).await
    }

    /// Connect, optionally emitting `first` as the very first octets.
    pub async fn connect_raw(
        addr: &str,
        first: Option<&[u8]>,
    ) -> std::io::Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true).ok();
        let (r, mut w) = socket.into_split();
        // §4.2.2: the client MUST start with the 8-octet protocol header.
        if let Some(first) = first {
            w.write_all(first).await?;
            w.flush().await?;
        }
        Ok(TestClient { reader: FrameReader::new(), read_half: r, writer: w, last_method: None, backlog: Vec::new() })
    }

    pub async fn send_method(&mut self, channel: u16, m: &Method) -> std::io::Result<()> {
        let bytes = switchboard_wire::Frame::method(channel, m).to_bytes();
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await
    }

    pub async fn send_content(
        &mut self,
        channel: u16,
        props: &BasicProperties,
        body: &[u8],
    ) -> std::io::Result<()> {
        let h = switchboard_wire::Frame::header(
            channel,
            &ContentHeader::new(body.len() as u64, props.clone()),
        )
        .to_bytes();
        self.writer.write_all(&h).await?;
        if !body.is_empty() {
            let b = switchboard_wire::Frame::body(channel, body).to_bytes();
            self.writer.write_all(&b).await?;
        }
        self.writer.flush().await
    }

    /// Read frames until a method arrives on `channel`; returns it with any
    /// assembled content (header properties + body). Content-bearing
    /// methods hold until their header and body frames have arrived.
    /// Methods that arrive on a *different* channel are buffered and
    /// returned by a later `expect` for their own channel.
    pub async fn expect_method(
        &mut self,
        channel: u16,
    ) -> Result<(Method, Option<(BasicProperties, Vec<u8>)>), String> {
        if let Some(pos) = self.backlog.iter().position(|(ch, _)| *ch == channel) {
            let (_, m) = self.backlog.remove(pos);
            return Ok((m, None));
        }
        let mut pending: Option<ContentHeader> = None;
        let mut body: Vec<u8> = Vec::new();
        let mut content_for: Option<Method> = None;
        loop {
            match self.reader.next_frame(0) {
                Ok(Some(frame)) => match frame.frame_type {
                    switchboard_wire::FrameType::Method => {
                        let m = frame.decode_method().map_err(|e| format!("bad method: {e}"))?;
                        self.last_method = Some(format!("{m:?}"));
                        if let Some(m) = content_for.take() {
                            let content =
                                pending.map(|h| (h.properties, std::mem::take(&mut body)));
                            return Ok((m, content));
                        }
                        if m.carries_content() {
                            content_for = Some(m);
                            continue;
                        }
                        if frame.channel != channel {
                            self.backlog.push((frame.channel, m));
                            continue;
                        }
                        return Ok((m, None));
                    }
                    switchboard_wire::FrameType::Header => {
                        let h =
                            frame.content_header(60).map_err(|e| format!("bad header: {e}"))?;
                        // Zero-size content completes on the header itself.
                        if h.body_size == 0 {
                            let m = content_for.take().expect("header without method");
                            return Ok((m, Some((h.properties, Vec::new()))));
                        }
                        pending = Some(h);
                    }
                    switchboard_wire::FrameType::Body => {
                        body.extend_from_slice(&frame.payload);
                        // Content completes when the announced body size is
                        // reached (§4.2.6.1) — do not wait for more frames.
                        if let Some(h) = &pending {
                            if body.len() as u64 >= h.body_size {
                                let m = content_for.take().expect("body without method");
                                let props = h.properties.clone();
                                return Ok((m, Some((props, std::mem::take(&mut body)))));
                            }
                        }
                    }
                    switchboard_wire::FrameType::Heartbeat => continue,
                },
                Ok(None) => {
                    let mut buf = [0u8; 8192];
                    let n = tokio::io::AsyncReadExt::read(&mut self.read_half, &mut buf)
                        .await
                        .map_err(|e| format!("read: {e}"))?;
                    if n == 0 {
                        return Err("connection closed".into());
                    }
                    self.reader.feed(&buf[..n]);
                }
                Err(e) => return Err(format!("frame error: {e}")),
            }
        }
    }

    pub async fn expect(&mut self, channel: u16) -> Result<Method, String> {
        self.expect_method(channel).await.map(|(m, _)| m)
    }
}

/// Full connection handshake against a broker (PLAIN, guest/guest).
/// Retries transient connect/EOF failures while the node warms up.
pub async fn connect_and_open(addr: &str, vhost: &str) -> Result<TestClient, String> {
    let mut last = String::new();
    for _ in 0..25 {
        match connect_and_open_once(addr, vhost).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                last = e;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(last)
}

async fn connect_and_open_once(addr: &str, vhost: &str) -> Result<TestClient, String> {
    let mut c = TestClient::connect(addr).await.map_err(|e| e.to_string())?;
    // Server speaks first: Connection.Start.
    let start = c.expect(0).await?;
    let Method::ConnectionStart {
        version_major,
        version_minor,
        mechanisms,
        ..
    } = start
    else {
        return Err(format!("expected Connection.Start, got {}", start.name()));
    };
    assert_eq!((version_major, version_minor), (0, 9));
    assert!(String::from_utf8_lossy(&mechanisms).contains("PLAIN"));

    // Start-Ok with PLAIN guest/guest.
    let mut response = vec![0u8];
    response.extend_from_slice(b"guest");
    response.push(0);
    response.extend_from_slice(b"guest");
    c.send_method(
        0,
        &Method::ConnectionStartOk {
            client_properties: FieldTable::new(),
            mechanism: "PLAIN".into(),
            response,
            locale: "en_US".into(),
        },
    )
    .await
    .map_err(|e| e.to_string())?;

    // Tune.
    let Method::ConnectionTune {
        channel_max,
        frame_max,
        heartbeat,
    } = c.expect(0).await?
    else {
        return Err("expected Connection.Tune".into());
    };
    c.send_method(
        0,
        &Method::ConnectionTuneOk {
            channel_max,
            frame_max,
            heartbeat,
        },
    )
    .await
    .map_err(|e| e.to_string())?;

    // Open.
    c.send_method(
        0,
        &Method::ConnectionOpen {
            virtual_host: vhost.into(),
            capabilities: String::new(),
            insist: false,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let Method::ConnectionOpenOk { .. } = c.expect(0).await? else {
        return Err("expected Connection.OpenOk".into());
    };

    // Channel 1.
    c.send_method(1, &Method::ChannelOpen { out_of_band: String::new() })
        .await
        .map_err(|e| e.to_string())?;
    let Method::ChannelOpenOk { .. } = c.expect(1).await? else {
        return Err("expected Channel.OpenOk".into());
    };
    Ok(c)
}

/// Publish one message on `channel` to the default exchange.
pub async fn publish(
    c: &mut TestClient,
    channel: u16,
    exchange: &str,
    routing_key: &str,
    props: &BasicProperties,
    body: &[u8],
    mandatory: bool,
) -> Result<(), String> {
    c.send_method(
        channel,
        &Method::BasicPublish {
            ticket: 0,
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            mandatory,
            immediate: false,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    c.send_content(channel, props, body).await.map_err(|e| e.to_string())
}

/// Basic.Get one message; Ok(None) = empty.
pub async fn basic_get(
    c: &mut TestClient,
    channel: u16,
    queue: &str,
    no_ack: bool,
) -> Result<Option<(u64, Vec<u8>)>, String> {
    c.send_method(
        channel,
        &Method::BasicGet {
            ticket: 0,
            queue: queue.into(),
            no_ack,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let (m, content) = c.expect_method(channel).await?;
    match m {
        Method::BasicGetOk { delivery_tag, .. } => {
            Ok(content.map(|(_, body)| (delivery_tag, body)))
        }
        Method::BasicGetEmpty { .. } => Ok(None),
        Method::ChannelClose {
            reply_code,
            reply_text,
            ..
        } => {
            let _ = c.send_method(channel, &Method::ChannelCloseOk {}).await;
            Err(format!("channel closed {reply_code}: {reply_text}"))
        }
        other => Err(format!("unexpected {}", other.name())),
    }
}

/// Spin up a single-node broker and return the client-listener address.
pub async fn start_broker(name: &str) -> (Arc<switchboard_cluster::ClusterNode>, String) {
    
    static LOG: std::sync::Once = std::sync::Once::new();
    LOG.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init()
            .ok();
    });
    let internal = format!("127.0.0.1:{}", free_port());
    let client = format!("127.0.0.1:{}", free_port());
    let unique = format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("switchboard-e2e-{unique}"));
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
    let node = switchboard_cluster::ClusterNode::start(cfg).await.unwrap();

    let listener = tokio::net::TcpListener::bind(&client).await.unwrap();
    let node2 = node.clone();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                continue;
            };
            let node3 = node2.clone();
            tokio::spawn(async move {
                let _ = switchboard_server::session::serve(sock, node3, Default::default()).await;
            });
        }
    });
    // Wait until the bootstrap entities are applied (leader elected).
    for _ in 0..800 {
        let topo = node.topology();
        if !topo.vhosts.is_empty() && !topo.groups.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    // Give the reconciliation loop a couple of ticks to open the shard
    // group raft handles after the layout lands.
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    (node, client)
}

use std::sync::Arc;

/// Start one node of a cluster with explicit addresses.
#[allow(clippy::too_many_arguments)]
pub async fn start_broker_node(
    name: &str,
    id: u64,
    client_addr: String,
    internal_addr: String,
    seeds: Vec<String>,
    bootstrap: bool,
    expected_nodes: u64,
    peers: Vec<(u64, String)>,
) -> (
    Arc<switchboard_cluster::ClusterNode>,
    tokio::net::TcpListener,
) {
    let dir = std::env::temp_dir().join(format!(
        "switchboard-node-{name}-{}-{}",
        std::process::id(),
        test_tag()
    ));
    let cfg = switchboard_cluster::NodeConfig {
        id,
        data_dir: dir,
        client_addr: client_addr.clone(),
        internal_addr,
        seeds,
        bootstrap,
        expected_nodes,
        peers,
        timeouts: Default::default(),
    };
    init_tracing();
    let node = switchboard_cluster::ClusterNode::start(cfg).await.unwrap();
    // Serve clients.
    let listener = tokio::net::TcpListener::bind(&client_addr).await.unwrap();
    (node, listener)
}

/// Spawn the client-accept loop for a bound listener.
pub fn spawn_client_accepts(
    listener: tokio::net::TcpListener,
    node: Arc<switchboard_cluster::ClusterNode>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                continue;
            };
            let node = node.clone();
            tokio::spawn(async move {
                let _ = switchboard_server::session::serve(sock, node, Default::default()).await;
            });
        }
    })
}

// ---------------------------------------------------------------------
// Multi-node cluster helpers
// ---------------------------------------------------------------------

/// Wait (up to `secs`) until `check` holds; false on timeout.
pub async fn wait_until(mut check: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    while tokio::time::Instant::now() < deadline {
        if check() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}

/// Start an N-node cluster where every node knows every peer, and serve
/// client connections on every node. Returns the nodes, their accept-loop
/// handles (abort on teardown), and the client addresses in node order.
pub async fn start_cluster(
    tag: &str,
    n: u64,
) -> (
    Vec<Arc<switchboard_cluster::ClusterNode>>,
    Vec<tokio::task::JoinHandle<()>>,
    Vec<String>,
) {
    let internal: Vec<String> = (0..n as usize)
        .map(|_| format!("127.0.0.1:{}", free_port()))
        .collect();
    let clients: Vec<String> = (0..n as usize)
        .map(|_| format!("127.0.0.1:{}", free_port()))
        .collect();
    let peers: Vec<(u64, String)> = (1..=n)
        .map(|id| (id, internal[id as usize - 1].clone()))
        .collect();

    let mut nodes = Vec::new();
    let mut listeners = Vec::new();
    for id in 1..=n {
        let i = id as usize - 1;
        let (node, listener) = start_broker_node(
            tag,
            id,
            clients[i].clone(),
            internal[i].clone(),
            if id == 1 { vec![] } else { vec![internal[0].clone()] },
            id == 1,
            n,
            peers.clone(),
        )
        .await;
        listeners.push(spawn_client_accepts(listener, node.clone()));
        nodes.push(node);
    }

    // Full formation everywhere: bootstrap entities applied and the shard
    // layout visible on every node's view (joiners fetch it via forward).
    let ok_vhosts = wait_until(
        || nodes.iter().all(|n| !n.topology().vhosts.is_empty()),
        120,
    )
    .await;
    assert!(ok_vhosts, "cluster {tag}: vhosts never became visible");
    let ok_layout = wait_until(
        || nodes.iter().all(|n| !n.topology().groups.is_empty()),
        120,
    )
    .await;
    assert!(ok_layout, "cluster {tag}: shard layout never became visible");
    // A beat for shard-group elections.
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    (nodes, listeners, clients)
}

/// Tear a cluster down: stop every node's raft cores and abort the
/// client-accept loops so nothing leaks into the next test.
pub async fn teardown_cluster(
    nodes: &[Arc<switchboard_cluster::ClusterNode>],
    listeners: Vec<tokio::task::JoinHandle<()>>,
) {
    for node in nodes {
        node.shutdown().await;
    }
    for l in listeners {
        l.abort();
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

/// Declare a durable queue through `client` and wait for it to be visible
/// on every node (routing propagation).
pub async fn declare_queue_everywhere(
    client: &mut TestClient,
    nodes: &[Arc<switchboard_cluster::ClusterNode>],
    queue: &str,
) {
    client
        .send_method(
            1,
            &Method::QueueDeclare {
                ticket: 0,
                queue: queue.into(),
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                arguments: Default::default(),
            },
        )
        .await
        .unwrap();
    let got = client.expect(1).await.unwrap();
    let Method::QueueDeclareOk { queue: name, .. } = got else {
        panic!("expected QueueDeclareOk, got {got:?}");
    };
    assert_eq!(name, queue);

    for node in nodes {
        let ok = wait_until(
            || {
                node.topology()
                    .vhosts
                    .get("/")
                    .map(|v| v.queues.contains_key(queue))
                    .unwrap_or(false)
            },
            60,
        )
        .await;
        assert!(ok, "queue {queue:?} never became visible cluster-wide");
    }
}

/// Run `fut` with a hard timeout (panics on expiry).
pub async fn with_timeout<F: std::future::Future>(fut: F, secs: u64) -> F::Output {
    tokio::time::timeout(std::time::Duration::from_secs(secs), fut)
        .await
        .expect("test timed out")
}
