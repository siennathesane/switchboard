//! Alert #5 e2e: the opt-in shared-secret gate on the internal cluster
//! transport.
//!
//! * a three-node cluster where every member shares
//!   `internal_auth_secret` forms and serves multi-master writes over
//!   authenticated internal connections;
//! * a stranger presenting NO proof, or a peer presenting a WRONG
//!   proof, is refused before any Raft/Forward/Admin envelope runs, and
//!   the cluster is unaffected;
//! * a node WITHOUT the secret still serves secret-configured dialers
//!   (the mismatched-deployment compatibility direction);
//! * the shared secret never appears in captured log output.

use std::sync::Arc;
use std::time::Duration;

use switchboard_cluster::ClusterNode;
use switchboard_cluster::NodeConfig;
use switchboard_cluster::proto::AdminRequest;
use switchboard_cluster::proto::AdminResponse;
use switchboard_cluster::proto::Envelope;
use switchboard_cluster::proto::InternalMessage;
use switchboard_cluster::proto::InternalRequest;
use switchboard_cluster::proto::InternalResponse;
use switchboard_cluster::transport::PeerChannel;

// ---------------------------------------------------------------------
// Log capture: one process-global buffer behind a tracing writer, so
// every test can assert its secret never leaked into any log line.
// ---------------------------------------------------------------------

static LOG_BUF: std::sync::Mutex<Vec<u8>> = std::sync::Mutex::new(Vec::new());

struct CapturedWriter;

impl std::io::Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LOG_BUF.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct CaptureMaker;

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureMaker {
    type Writer = CapturedWriter;
    fn make_writer(&'a self) -> CapturedWriter {
        CapturedWriter
    }
}

fn init_capture() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
            .with_writer(CaptureMaker)
            .try_init()
            .ok();
    });
}

/// Assert `secret` never appears in the captured log output — and that
/// the capture is actually live (non-empty), so the assertion cannot
/// pass vacuously.
fn assert_secret_never_logged(secret: &str, because: &str) {
    let buf = LOG_BUF.lock().unwrap().clone();
    assert!(
        !buf.is_empty(),
        "log capture must be live for the no-leak assertion to mean anything"
    );
    assert!(
        !buf.windows(secret.len()).any(|w| w == secret.as_bytes()),
        "the shared secret leaked into logs ({because})"
    );
}

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One node of a shared-secret three-node cluster: every node knows
/// every peer, node 1 bootstraps, the others join through node 1 (the
/// same recipe `switchboard-server`'s cluster harness uses).
async fn start_node(
    tag: &str,
    id: u64,
    internal: String,
    peers: Vec<(u64, String)>,
    seeds: Vec<String>,
    secret: &str,
) -> Arc<ClusterNode> {
    start_node_n(tag, id, internal, peers, seeds, secret, 3).await
}

/// [`start_node`] with an explicit formation size.
async fn start_node_n(
    tag: &str,
    id: u64,
    internal: String,
    peers: Vec<(u64, String)>,
    seeds: Vec<String>,
    secret: &str,
    expected_nodes: u64,
) -> Arc<ClusterNode> {
    let cfg = NodeConfig {
        id,
        data_dir: std::env::temp_dir().join(format!(
            "sb-iauth-{tag}-{}-{}",
            std::process::id(),
            free_port()
        )),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: internal,
        seeds,
        bootstrap: id == 1,
        expected_nodes,
        peers,
        timeouts: Default::default(),
        bootstrap_user: switchboard_wire::constants::DEFAULT_USER.into(),
        bootstrap_password: switchboard_wire::constants::DEFAULT_PASSWORD.into(),
        internal_auth_secret: (!secret.is_empty()).then(|| secret.to_string()),
    };
    ClusterNode::start(cfg).await.unwrap()
}

/// A fresh three-node cluster, every member sharing `secret`.
async fn start_secret_cluster(secret: &str) -> [Arc<ClusterNode>; 3] {
    let internal: Vec<String> = (0..3)
        .map(|_| format!("127.0.0.1:{}", free_port()))
        .collect();
    let peers: Vec<(u64, String)> = (1..=3)
        .map(|id| (id, internal[id as usize - 1].clone()))
        .collect();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let seeds = if id == 1 {
            vec![]
        } else {
            vec![internal[0].clone()]
        };
        nodes.push(
            start_node(
                &format!("c{id}"),
                id,
                internal[id as usize - 1].clone(),
                peers.clone(),
                seeds,
                secret,
            )
            .await,
        );
    }
    // Full formation everywhere: bootstrap entities + shard layout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let ready = nodes.iter().all(|n| {
            let t = n.topology();
            !t.vhosts.is_empty() && !t.groups.is_empty()
        });
        if ready {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "three-node formation with shared secrets never completed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A beat for shard-group elections, like the server harness.
    tokio::time::sleep(Duration::from_millis(700)).await;
    [nodes.remove(0), nodes.remove(0), nodes.remove(0)]
}

/// Exchange one envelope with the node's internal listener.
async fn roundtrip(
    conn: &mut switchboard_cluster::transport::PeerConn,
    env: &Envelope,
) -> Option<InternalResponse> {
    conn.send(&bincode::serialize(env).unwrap()).await.ok()?;
    let back = conn.recv().await.ok()?;
    let InternalMessage::Response(resp) = bincode::deserialize(&back).ok()? else {
        return None;
    };
    Some(resp)
}

fn ping(from: u64) -> Envelope {
    Envelope {
        from,
        message: InternalMessage::Request(InternalRequest::Admin(AdminRequest::Ping)),
    }
}

async fn declare_vhost(node: &Arc<ClusterNode>, name: &str) {
    node.write(
        switchboard_cluster::META_GROUP,
        switchboard_cluster::BrokerCommand::Meta(
            switchboard_core::topology::MetaCmd::DeclareVhost { name: name.into() },
        ),
    )
    .await
    .expect("meta write over the authenticated internal transport");
}

// ---------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------

/// The full cluster scenario: three nodes share the secret, form, and
/// keep serving multi-master writes while strangers (no proof / wrong
/// proof) are refused.
#[tokio::test(flavor = "multi_thread")]
async fn shared_secret_cluster_forms_and_refuses_strangers() {
    init_capture();
    let secret = format!("iauth-form-{}-{}", std::process::id(), free_port());
    let [n1, n2, n3] = start_secret_cluster(&secret).await;

    // Multi-master over authenticated internal connections: a meta write
    // through node 3 (forwarded to the meta leader and replicated back)
    // becomes visible on node 1.
    declare_vhost(&n3, "iauth-vh").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !n1.topology().vhosts.contains_key("iauth-vh") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cross-node write visibility never landed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ---- Stranger A: no proof at all ----
    let mut raw = tokio::net::TcpStream::connect(&n1.cfg.internal_addr)
        .await
        .unwrap();
    let bytes = bincode::serialize(&ping(99)).unwrap();
    let mut framed = (bytes.len() as u32).to_le_bytes().to_vec();
    framed.extend_from_slice(&bytes);
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    raw.write_all(&framed).await.unwrap();
    raw.flush().await.unwrap();
    // The server must close without ever answering the ping.
    let mut got = [0u8; 16];
    let n = raw.read(&mut got).await.unwrap_or(0);
    assert_eq!(n, 0, "unauthenticated stranger must be cut off, not served");

    // ---- Stranger B: WRONG secret ----
    let wrong = PeerChannel::with_internal_auth(
        None,
        "sb".into(),
        Duration::from_secs(2),
        Some(format!("wrong-{secret}")),
    );
    let err = wrong
        .rpc(
            &n1.cfg.internal_addr,
            &bincode::serialize(&ping(99)).unwrap(),
        )
        .await
        .expect_err("a wrong proof must be refused");
    assert!(
        err.kind() == std::io::ErrorKind::UnexpectedEof
            || err.kind() == std::io::ErrorKind::ConnectionReset,
        "refusal should manifest as the server closing: {err:?}"
    );

    // ---- The cluster is unaffected: an authenticated peer still works. ----
    let good = PeerChannel::with_internal_auth(
        None,
        "sb".into(),
        Duration::from_secs(5),
        Some(secret.clone()),
    );
    let mut conn = good.connect(&n1.cfg.internal_addr).await.unwrap();
    let resp = roundtrip(&mut conn, &ping(7))
        .await
        .expect("authenticated ping must round-trip");
    assert!(matches!(resp, InternalResponse::Admin(AdminResponse::Pong)));

    // And the members still serve each other (a write from node 2 lands).
    declare_vhost(&n2, "iauth-vh-2").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !n3.topology().vhosts.contains_key("iauth-vh-2") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster health after refused strangers never recovered"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_secret_never_logged(&secret, "three-node formation scenario");

    n1.shutdown().await;
    n2.shutdown().await;
    n3.shutdown().await;
}

/// A node WITHOUT the secret configured keeps serving peers that DO
/// present a proof (the frame is consumed, replies stay lined up) — the
/// mismatched-deployment compatibility direction.
#[tokio::test(flavor = "multi_thread")]
async fn authless_node_still_serves_a_secret_configured_dialer() {
    init_capture();
    let secret = format!("iauth-mix-{}-{}", std::process::id(), free_port());
    let internal = format!("127.0.0.1:{}", free_port());
    let plain = start_node_n("plain", 1, internal, Vec::new(), Vec::new(), "", 1).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let t = plain.topology();
        if !t.vhosts.is_empty() && !t.groups.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "authless node never finished formation"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let chan = PeerChannel::with_internal_auth(
        None,
        "sb".into(),
        Duration::from_secs(5),
        Some(secret.clone()),
    );
    let mut conn = chan.connect(&plain.cfg.internal_addr).await.unwrap();
    let resp = roundtrip(&mut conn, &ping(5))
        .await
        .expect("proof frame must be consumed, not answered, by an authless node");
    assert!(matches!(resp, InternalResponse::Admin(AdminResponse::Pong)));

    assert_secret_never_logged(&secret, "authless-node scenario");
    let _ = plain.shutdown().await;
}
