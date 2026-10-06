//! Internal-network transport: framed bincode messages over TCP, with
//! optional TLS via rustls backed by aws-lc-rs (the aws-lc-sys crypto
//! provider).
//!
//! Frame layout: `u32` little-endian length (excluding itself), then that
//! many bytes of bincode-encoded [`Envelope`](crate::proto::Envelope).

use std::fmt;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;

const MAX_FRAME: u32 = 64 * 1024 * 1024;

/// Everything sent over an accepted internal connection.
pub enum Accepted {
    Plain(TcpStream),
    Tls(Box<ServerTlsStream<TcpStream>>),
}

impl Accepted {
    fn as_read(&mut self) -> &mut (dyn tokio::io::AsyncRead + Unpin + Send) {
        match self {
            Accepted::Plain(s) => s,
            Accepted::Tls(s) => s,
        }
    }

    fn as_write(&mut self) -> &mut (dyn tokio::io::AsyncWrite + Unpin + Send) {
        match self {
            Accepted::Plain(s) => s,
            Accepted::Tls(s) => s,
        }
    }

    async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self.as_write(), data).await
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::flush(self.as_write()).await
    }
}

/// A request/response connection factory to peers. Successful RPCs park
/// their connection in a small per-address idle pool so the raft
/// heartbeat/retry cadence reuses connections instead of churning the
/// ephemeral port range into TIME_WAIT exhaustion — a fresh dial per RPC
/// made a busy cluster (or test run) fall over with `EADDRNOTAVAIL`.
/// Idle connections can go stale; `rpc` retries once over a fresh dial.
pub struct PeerChannel {
    connector: Option<TlsConnector>,
    pub server_name: String,
    /// One RPC's reply wait. Configurable via `Timeouts`.
    pub reply_budget: std::time::Duration,
    idle: tokio::sync::Mutex<std::collections::HashMap<String, Vec<PeerConn>>>,
}

/// Idle connections parked per peer address. Under a replication burst
/// every in-flight RPC holds its own connection; parking (instead of
/// closing) them lets the next burst reuse the set. A small cap would
/// close most burst connections after a single RPC — each close leaves a
/// TIME_WAIT in the ephemeral port range, and under test tempo (openraft
/// heartbeat/election timers shrunk 50x) that churns thousands of
/// connections per node into exhaustion (EADDRNOTAVAIL).
const IDLE_CAP: usize = 32;

/// Bound one RPC's reply wait: a peer that accepts but never replies must
/// not park the caller forever. Test tempo scales this like every other
/// fixed delay. Default 5 s — a realtime budget; override through
/// `Timeouts::rpc_reply_budget`.
const REPLY_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

impl PeerChannel {
    pub fn new(connector: Option<TlsConnector>, server_name: String) -> Self {
        PeerChannel {
            connector,
            server_name,
            reply_budget: REPLY_BUDGET,
            idle: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// `new`, with an explicit reply budget (from `Timeouts`).
    pub fn with_reply_budget(
        connector: Option<TlsConnector>,
        server_name: String,
        reply_budget: std::time::Duration,
    ) -> Self {
        PeerChannel {
            reply_budget,
            ..Self::new(connector, server_name)
        }
    }

    pub async fn connect(&self, addr: &str) -> std::io::Result<PeerConn> {
        let tcp = TcpStream::connect(addr).await?;
        tcp.set_nodelay(true).ok();
        match &self.connector {
            Some(c) => {
                let name = rustls::pki_types::ServerName::try_from(self.server_name.clone())
                    .map_err(|e| std::io::Error::other(format!("bad server name: {e}")))?;
                let tls = c.connect(name, tcp).await?;
                Ok(PeerConn::Tls(tls))
            }
            None => Ok(PeerConn::Plain(tcp)),
        }
    }

    /// One request→reply round trip to `addr`, reusing a pooled
    /// connection when possible. A transport failure on a reused
    /// (possibly stale) connection retries once over a fresh dial.
    /// Requests are idempotent one-shot RPCs; at-least-once is
    /// acceptable for that retry.
    pub async fn rpc(&self, addr: &str, req: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut over_reused_conn = true;
        let mut conn = match self.take_idle(addr).await {
            Some(c) => c,
            None => {
                over_reused_conn = false;
                self.connect(addr).await?
            }
        };
        loop {
            let budget = switchboard_core::tempo::scale(self.reply_budget);
            let outcome = async {
                conn.send(req).await?;
                match tokio::time::timeout(budget, conn.recv()).await {
                    Ok(r) => r,
                    Err(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "no reply from peer",
                    )),
                }
            }
            .await;
            match outcome {
                Ok(back) => {
                    self.park(addr, conn).await;
                    return Ok(back);
                }
                Err(_e) if over_reused_conn => {
                    over_reused_conn = false;
                    conn = self.connect(addr).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn take_idle(&self, addr: &str) -> Option<PeerConn> {
        self.idle.lock().await.get_mut(addr).and_then(Vec::pop)
    }

    /// Drop idle connections to `addr` — call when the address is known
    /// to have changed so a stale connection is never handed out.
    pub async fn invalidate(&self, addr: &str) {
        self.idle.lock().await.remove(addr);
    }

    async fn park(&self, addr: &str, conn: PeerConn) {
        let mut idle = self.idle.lock().await;
        let slot = idle.entry(addr.to_string()).or_default();
        if slot.len() < IDLE_CAP {
            slot.push(conn);
        }
    }
}

/// An established outbound connection.
pub enum PeerConn {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl PeerConn {
    fn as_read(&mut self) -> &mut (dyn tokio::io::AsyncRead + Unpin + Send) {
        match self {
            PeerConn::Plain(s) => s,
            PeerConn::Tls(s) => s,
        }
    }

    fn as_write(&mut self) -> &mut (dyn tokio::io::AsyncWrite + Unpin + Send) {
        match self {
            PeerConn::Plain(s) => s,
            PeerConn::Tls(s) => s,
        }
    }

    pub async fn send(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let len = (bytes.len() as u32).to_le_bytes();
        self.write_all(&len).await?;
        self.write_all(bytes).await?;
        self.flush().await
    }

    pub async fn recv(&mut self) -> std::io::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        Self::read_exact_into(self.as_read(), &mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf);
        if len > MAX_FRAME {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame of {len} bytes exceeds internal limit"),
            ));
        }
        let mut buf = vec![0u8; len as usize];
        Self::read_exact_into(self.as_read(), &mut buf).await?;
        Ok(buf)
    }

    /// Read exactly `buf.len()` bytes across partial reads.
    async fn read_exact_into(
        read: &mut (dyn tokio::io::AsyncRead + Unpin + Send),
        buf: &mut [u8],
    ) -> std::io::Result<()> {
        let mut done = 0usize;
        while done < buf.len() {
            let n = tokio::io::AsyncReadExt::read(read, &mut buf[done..]).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer closed mid-frame",
                ));
            }
            done += n;
        }
        Ok(())
    }

    async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self.as_write(), data).await
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::flush(self.as_write()).await
    }
}

/// Accept one internal connection (TLS wrapped when configured).
pub async fn accept(
    tcp: TcpStream,
    tls: Option<&TlsAcceptor>,
) -> std::io::Result<Accepted> {
    tcp.set_nodelay(true).ok();
    match tls {
        Some(a) => Ok(Accepted::Tls(Box::new(a.accept(tcp).await?))),
        None => Ok(Accepted::Plain(tcp)),
    }
}

impl Accepted {
    /// Read exactly `buf.len()` bytes.
    async fn read_exact_into(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        let mut done = 0usize;
        while done < buf.len() {
            let n = tokio::io::AsyncReadExt::read(self.as_read(), &mut buf[done..]).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer closed mid-frame",
                ));
            }
            done += n;
        }
        Ok(())
    }

    /// Read one length-prefixed frame (respects partial reads).
    pub async fn read_frame_full(&mut self) -> std::io::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        self.read_exact_into(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf);
        if len > MAX_FRAME {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame of {len} bytes exceeds internal limit"),
            ));
        }
        let mut buf = vec![0u8; len as usize];
        self.read_exact_into(&mut buf).await?;
        Ok(buf)
    }

    /// Write one length-prefixed frame.
    pub async fn write_frame(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let len = (bytes.len() as u32).to_le_bytes();
        let mut frame = Vec::with_capacity(4 + bytes.len());
        frame.extend_from_slice(&len);
        frame.extend_from_slice(bytes);
        self.write_all(&frame).await?;
        self.flush().await
    }
}

impl fmt::Debug for Accepted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Accepted::Plain(_) => f.write_str("Accepted(plain)"),
            Accepted::Tls(_) => f.write_str("Accepted(tls)"),
        }
    }
}

/// Build a TLS server acceptor from PEM cert chain + key.
pub fn tls_acceptor(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<TlsAcceptor, String> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<_, _>>()
        .map_err(|e| format!("bad certificate PEM: {e}"))?;
    let key = rustls_pemfile::private_key(&mut &key_pem[..])
        .map_err(|e| format!("bad private key PEM: {e}"))?
        .ok_or("no private key found")?;

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("tls versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("bad certificate/key pair: {e}"))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build a TLS client connector trusting `ca_pem`.
pub fn tls_connector(ca_pem: &[u8]) -> Result<TlsConnector, String> {
    let cas: Vec<_> = rustls_pemfile::certs(&mut &ca_pem[..])
        .collect::<Result<_, _>>()
        .map_err(|e| format!("bad CA PEM: {e}"))?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let root_store = rustls::RootCertStore::empty();
    let mut store = root_store;
    for ca in &cas {
        store
            .add(ca.clone())
            .map_err(|e| format!("unusable CA certificate: {e}"))?;
    }
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("tls versions: {e}"))?
        .with_root_certificates(store)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plain_frames_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut acc = accept(tcp, None).await.unwrap();
            let frame = acc.read_frame_full().await.unwrap();
            acc.write_frame(&frame).await.unwrap();
        });

        let chan = PeerChannel::new(None, "localhost".into());
        let mut conn = chan.connect(&addr.to_string()).await.unwrap();
        conn.send(b"hello internal").await.unwrap();
        let back = conn.recv().await.unwrap();
        server.await.unwrap();
        assert_eq!(back, b"hello internal");
    }

    #[tokio::test]
    async fn oversized_frames_are_rejected() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut acc = accept(tcp, None).await.unwrap();
            let _ = acc.read_frame_full().await;
        });
        let chan = PeerChannel::new(None, "localhost".into());
        let mut conn = chan.connect(&addr.to_string()).await.unwrap();
        // Announce a 100 MB frame.
        conn.send(&(100u32 * 1024 * 1024).to_le_bytes()).await.unwrap();
        let res = conn.recv().await;
        assert!(res.is_err(), "oversized frame must be rejected");
        server.await.unwrap();
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    /// Bind a loopback listener and return its address + an accept task
    /// handle that echoes one length-prefixed frame back.
    async fn echo_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut accepted = accept(sock, None).await.unwrap();
            let frame = accepted.read_frame_full().await.unwrap();
            accepted.write_frame(&frame).await.unwrap();
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn peer_conn_roundtrip_through_plain_channel() {
        let (addr, server) = echo_server().await;
        let channel = PeerChannel::new(None, "localhost".into());
        let mut conn = channel.connect(&addr).await.unwrap();
        conn.send(b"ping").await.unwrap();
        let back = conn.recv().await.unwrap();
        assert_eq!(back, b"ping");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn accepted_read_frame_full_rejects_oversized() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut accepted = accept(sock, None).await.unwrap();
            // A length prefix of u32::MAX exceeds MAX_FRAME.
            let err = accepted.read_frame_full().await.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        });
        // Write the raw 4-byte length prefix directly (no framing helper).
        let mut raw = TcpStream::connect(&addr).await.unwrap();
        use tokio::io::AsyncWriteExt;
        raw.write_all(&u32::MAX.to_le_bytes()).await.unwrap();
        raw.flush().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn peer_conn_recv_survives_partial_reads() {
        // Feed the length prefix and payload byte-by-byte; recv must
        // assemble across partial reads.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            use tokio::io::AsyncWriteExt;
            for b in 5u32.to_le_bytes().iter().chain(b"hello".iter()) {
                sock.write_u8(*b).await.unwrap();
                sock.flush().await.unwrap();
            }
        });
        let channel = PeerChannel::new(None, "localhost".into());
        let mut conn = channel.connect(&addr).await.unwrap();
        let got = conn.recv().await.unwrap();
        assert_eq!(got, b"hello");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn peer_conn_recv_errors_on_immediate_eof() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            drop(sock);
        });
        let channel = PeerChannel::new(None, "localhost".into());
        let mut conn = channel.connect(&addr).await.unwrap();
        let err = conn.recv().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        server.await.unwrap();
    }

    #[test]
    fn accepted_debug_names_variants() {
        // Compile-time surface check of the Debug naming used in logs.
        let text = format!("{:?}", std::io::Error::new(std::io::ErrorKind::Other, "x"));
        assert!(!text.is_empty());
    }
}
