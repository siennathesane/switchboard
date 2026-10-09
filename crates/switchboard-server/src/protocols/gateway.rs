//! The gateway: one client socket, every protocol. Peeks the first
//! bytes, classifies, and hands the socket (with its already-read
//! prefix) to the right server.

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

use switchboard_cluster::ClusterNode;

use crate::channel::ConnectionLimits;
use crate::session;

use super::amqp10;
use super::detect::classify;
use super::detect::Classify;
use super::detect::Detected;
use super::detect::SNIFF_LEN;
use super::http;
use super::mqtt;
use super::shared::ProtocolConfig;
use super::stomp;
use super::ws;

/// A stream with bytes that were already consumed during sniffing
/// replayed in front of it. Reads drain the prefix first; writes pass
/// through.
pub struct Prefixed<S> {
    prefix: std::io::Cursor<Vec<u8>>,
    inner: S,
}

impl<S> Prefixed<S> {
    pub fn new(prefix: Vec<u8>, inner: S) -> Self {
        Prefixed { prefix: std::io::Cursor::new(prefix), inner }
    }

    /// Bytes not yet replayed (should be empty once drained).
    pub fn pending_prefix(&self) -> &[u8] {
        let pos = self.prefix.position() as usize;
        &self.prefix.get_ref()[pos..]
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        let pos = this.prefix.position() as usize;
        let data = this.prefix.get_ref();
        if pos < data.len() {
            let n = (data.len() - pos).min(buf.remaining());
            buf.put_slice(&data[pos..pos + n]);
            this.prefix.set_position((pos + n) as u64);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Serve one client connection: sniff the protocol, then dispatch.
pub async fn serve_client<S>(
    socket: S,
    node: Arc<ClusterNode>,
    limits: ConnectionLimits,
    protocols: ProtocolConfig,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut socket = socket;
    let mut prefix: Vec<u8> = Vec::new();
    let detected = loop {
        let mut chunk = [0u8; SNIFF_LEN];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut chunk).await?;
        if n == 0 {
            return Ok(()); // client hung up during handshake
        }
        prefix.extend_from_slice(&chunk[..n]);
        match classify(&prefix) {
            Classify::Yes(d) => break d,
            Classify::NeedMore => {
                if prefix.len() >= SNIFF_LEN {
                    tracing::debug!("gateway: unrecognized preamble; closing");
                    return Ok(());
                }
            }
            Classify::Unknown => {
                tracing::debug!("gateway: unrecognized protocol; closing");
                return Ok(());
            }
        }
    };

    let supported = match detected {
        Detected::Amqp091 => true,
        Detected::Amqp10 => protocols.amqp10,
        Detected::Mqtt => protocols.mqtt,
        Detected::Stomp => protocols.stomp,
        Detected::Http => protocols.http_health || protocols.websocket,
    };
    if !supported {
        tracing::debug!(?detected, "gateway: protocol disabled by configuration");
        return Ok(());
    }

    let io_prefix = prefix.clone();
    let mut io = Prefixed::new(prefix, socket);
    match detected {
        Detected::Amqp091 => {
            let (r, w) = tokio::io::split(io);
            session::serve_rw(r, w, node, limits).await
        }
        Detected::Mqtt => mqtt::serve(io, node, limits.clone()).await,
        Detected::Stomp => stomp::serve(io, node, limits.clone()).await,
        Detected::Amqp10 => amqp10::serve(io, node, limits.clone()).await,
        Detected::Http => http::serve(io, &io_prefix, node, protocols, limits).await,
    }
}
