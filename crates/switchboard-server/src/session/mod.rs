//! Connection serving: the protocol handshake, frame dispatch, heartbeats,
//! and every client→server method handler.
//!
//! Handshake (§2.2.4): protocol-header → `Connection.Start` →
//! `Start-Ok` (SASL via [`switchboard_core::auth`]) → `Tune`/`Tune-Ok` →
//! `Open` (vhost selection, §3.1.2) → `Open-Ok`. Any error before Open
//! closes the socket without further data, as §2.2.4 requires.
//!
//! After Open, a reader task demultiplexes frames onto per-channel state
//! ([`crate::channel::Channel`]); a writer task drains outbound frames and
//! injects heartbeats (§4.2.7: "heartbeats only have to be sent if no
//! non-heartbeat AMQP traffic is sent for longer than one heartbeat
//! interval"; two silent intervals close the connection).

use std::collections::HashMap;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::BytesMut;
use switchboard_cluster::ClusterNode;
use switchboard_cluster::META_GROUP;
use switchboard_core::auth;
use switchboard_core::error::BrokerError;
use switchboard_core::error::Level;
use switchboard_core::model::ConnectionId;
use switchboard_wire::constants::reply;
use switchboard_wire::field::FieldValue;
use switchboard_wire::field::FieldTable;
use switchboard_wire::method::Method;
use switchboard_wire::FrameReader;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::warn;

use crate::channel::close_for;
use crate::channel::unexpected_content;
use crate::channel::Channel;
use crate::channel::ConnectionLimits;
use crate::outbound::OutboundFrame;
use switchboard_cluster::BrokerCommand;
use switchboard_core::topology::MetaCmd;

/// Serve one accepted plain-TCP client connection.
pub async fn serve(
    socket: tokio::net::TcpStream,
    node: Arc<ClusterNode>,
    limits: ConnectionLimits,
) -> std::io::Result<()> {
    socket.set_nodelay(true).ok();
    let (r, w) = socket.into_split();
    serve_rw(r, w, node, limits).await
}

/// Serve a connection over arbitrary halves (plain TCP or TLS).
pub async fn serve_rw<R, W>(
    mut reader: R,
    mut writer: W,
    node: Arc<ClusterNode>,
    limits: ConnectionLimits,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut fr = FrameReader::new();

    // ---- protocol header (§4.2.2) ----
    loop {
        let mut buf = [0u8; 512];
        let n = tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        fr.feed(&buf[..n]);
        match fr.take_protocol_header() {
            Ok(true) => break,
            Ok(false) => continue, // need more bytes
            Err(_) => {
                // Reject: write a valid protocol header, flush, close (§4.2.2).
                use tokio::io::AsyncWriteExt as _;
                let _ = writer.write_all(&switchboard_wire::PROTOCOL_HEADER).await;
                let _ = writer.flush().await;
                return Ok(());
            }
        }
    }

    // ---- handshake ----
    let mut session = match handshake(&mut reader, &mut fr, &mut writer, &node, &limits).await {
        Ok(s) => {
            s
        }
        Err(HandshakeError::Io(e)) => {
            return Err(e);
        }
        Err(HandshakeError::Rejected) => {
            return Ok(());
        } // socket closed per spec
    };

    // ---- writer task + reader loop ----
    let (tx, mut rx) = mpsc::unbounded_channel::<OutboundFrame>();
    session.outbound = tx.clone();

    let heartbeat = if limits.heartbeat == 0 {
        Duration::ZERO
    } else {
        switchboard_core::tempo::scale(Duration::from_secs(limits.heartbeat as u64))
    };

    let writer_task = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt as _;
        let mut buf = BytesMut::new();
        let mut last_write = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_millis(200));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let msg = tokio::select! {
                m = rx.recv() => match m {
                    Some(m) => m,
                    None => break,
                },
                _ = ticker.tick(), if heartbeat > Duration::ZERO => {
                    // §4.2.7: send a heartbeat only when the wire has been
                    // quiet for half an interval.
                    if last_write.elapsed() >= heartbeat / 2 {
                        OutboundFrame::Heartbeat
                    } else {
                        continue;
                    }
                }
            };
            let shutdown = matches!(msg, OutboundFrame::Shutdown);
            crate::outbound::encode(&msg, &mut buf);
            if writer.write_all(&buf).await.is_err() || shutdown {
                let _ = writer.shutdown().await;
                break;
            }
            buf.clear();
            last_write = Instant::now();
        }
    });

    session.reader_loop(&mut reader, &mut fr).await;
    let _ = tx.send(OutboundFrame::Shutdown);
    let _ = writer_task.await;
    Ok(())
}

#[derive(Debug)]
enum HandshakeError {
    Io(std::io::Error),
    Rejected,
}

impl From<std::io::Error> for HandshakeError {
    fn from(e: std::io::Error) -> Self {
        HandshakeError::Io(e)
    }
}

/// Sequential method read during the handshake. Returns Ok(None) on EOF.
async fn read_method<R: AsyncRead + Unpin>(
    reader: &mut R,
    fr: &mut FrameReader,
) -> std::io::Result<Option<Method>> {
    loop {
        if let Some(f) = fr.next_frame(0).map_err(io_err)? {
            if matches!(f.frame_type, switchboard_wire::FrameType::Method) {
                let m = f.decode_method().map_err(io_err)?;
                return Ok(Some(m));
            }
            continue;
        }
        let mut buf = [0u8; 4096];
        let n = tokio::io::AsyncReadExt::read(reader, &mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        fr.feed(&buf[..n]);
    }
}

async fn send_method<W: AsyncWrite + Unpin>(writer: &mut W, m: &Method) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    let bytes = switchboard_wire::Frame::method(0, m).to_bytes();
    writer.write_all(&bytes).await?;
    writer.flush().await
}

/// The Connection.Start/Start-Ok/Tune/Tune-Ok/Open/Open-Ok exchange.
async fn handshake<R, W>(
    reader: &mut R,
    fr: &mut FrameReader,
    writer: &mut W,
    node: &Arc<ClusterNode>,
    limits: &ConnectionLimits,
) -> Result<Session, HandshakeError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // 1. Connection.Start
    let mut server_properties = FieldTable::new();
    server_properties.insert(
        "product",
        FieldValue::LongString(switchboard_wire::constants::PRODUCT_NAME.as_bytes().to_vec()),
    );
    server_properties.insert(
        "version",
        FieldValue::LongString(switchboard_wire::constants::PRODUCT_VERSION.as_bytes().to_vec()),
    );
    let mut capabilities = FieldTable::new();
    capabilities.insert("publisher_confirms", FieldValue::Boolean(true));
    capabilities.insert("basic.nack", FieldValue::Boolean(true));
    capabilities.insert("consumer_cancel_notify", FieldValue::Boolean(true));
    capabilities.insert("exchange_exchange_bindings", FieldValue::Boolean(true));
    server_properties.insert("capabilities", FieldValue::FieldTable(capabilities));

    send_method(
        writer,
        &Method::ConnectionStart {
            version_major: switchboard_wire::VERSION_MAJOR,
            version_minor: switchboard_wire::VERSION_MINOR,
            server_properties,
            mechanisms: auth::MECHANISMS.as_bytes().to_vec(),
            locales: auth::LOCALES.as_bytes().to_vec(),
        },
    )
    .await?;

    // 2. Connection.Start-Ok — authenticate via meta.
    let got = read_method(reader, fr).await?;
    let Some(Method::ConnectionStartOk {
        client_properties: _,
        mechanism,
        response,
        locale: _,
    }) = got
    else {
        return Err(HandshakeError::Rejected); // §2.2.4: close without data
    };
    let Some((user, pass)) = auth::credentials(&mechanism, &response) else {
        return Err(HandshakeError::Rejected); // §2.2.4: close without data
    };
    // §2.2.4: authentication is decided by the replicated reply; a
    // broker-level refusal arrives as a BrokerReply::Error inside an Ok
    // write, so the reply value — not the transport result — decides.
    let authorized = node
        .write(
            META_GROUP,
            BrokerCommand::Meta(MetaCmd::Authorize { user, password: pass }),
        )
        .await;
    let authed = matches!(
        &authorized,
        Ok(switchboard_cluster::BrokerReply::Meta(
            switchboard_core::topology::MetaReply::Authorized
        ))
    );
    if !authed {
        return Err(HandshakeError::Rejected);
    }

    // 3. Tune / Tune-Ok (client may only lower, §2.3.3).
    send_method(
        writer,
        &Method::ConnectionTune {
            channel_max: limits.channel_max,
            frame_max: limits.frame_max,
            heartbeat: limits.heartbeat,
        },
    )
    .await?;
    let got2 = read_method(reader, fr).await?;
    let Some(Method::ConnectionTuneOk {
        channel_max: client_channel_max,
        frame_max: client_frame_max,
        heartbeat: client_heartbeat,
    }) = got2
    else {
        return Err(HandshakeError::Rejected);
    };
    let mut session_limits = limits.clone();
    session_limits.channel_max = agreed16(limits.channel_max, client_channel_max);
    session_limits.frame_max = agreed(limits.frame_max, client_frame_max);
    session_limits.heartbeat = agreed16(limits.heartbeat, client_heartbeat);

    // 4. Connection.Open — vhost selection (§3.1.2). Per the amqp-uri
    // convention every client library follows, an empty vhost names the
    // default "/" (RabbitMQ behavior).
    let got3 = read_method(reader, fr).await?;
    let Some(Method::ConnectionOpen { virtual_host, .. }) = got3 else {
        return Err(HandshakeError::Rejected);
    };
    let virtual_host = if virtual_host.is_empty() { "/".to_string() } else { virtual_host };
    let vhosts = node.topology().vhosts.clone();
    if !vhosts.contains_key(&virtual_host) {
        // 402 INVALID_PATH (§4.8.2); pre-Open errors close the socket.
        return Err(HandshakeError::Rejected);
    }
    send_method(writer, &Method::ConnectionOpenOk { known_hosts: String::new() }).await?;

    let conn_id = node.new_connection_id().await;
    Ok(Session {
        node: node.clone(),
        conn: conn_id,
        limits: session_limits,
        vhost: virtual_host,
        channels: HashMap::new(),
        outbound: mpsc::unbounded_channel().0,
    })
}

fn agreed(server: u32, client: u32) -> u32 {
    if client == 0 {
        server
    } else {
        server.min(client)
    }
}

fn agreed16(server: u16, client: u16) -> u16 {
    if client == 0 {
        server
    } else {
        server.min(client)
    }
}

/// One open AMQP connection.
pub struct Session {
    pub node: Arc<ClusterNode>,
    pub conn: ConnectionId,
    pub limits: ConnectionLimits,
    pub vhost: String,
    pub channels: HashMap<u16, Channel>,
    pub outbound: mpsc::UnboundedSender<OutboundFrame>,
}

impl Session {
    // ------------------------------------------------------------------
    // Reader loop
    // ------------------------------------------------------------------

    async fn reader_loop<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        fr: &mut FrameReader,
    ) {
        let heartbeat_timeout = if self.limits.heartbeat == 0 {
            Duration::from_secs(u64::MAX)
        } else {
            switchboard_core::tempo::scale(Duration::from_secs(
                self.limits.heartbeat as u64 * 2,
            ))
        };
        let mut last_octets = Instant::now();
        loop {
            // 1) Dispatch any complete buffered frame.
            match fr.next_frame(self.limits.frame_max) {
                Ok(Some(frame)) => {
                    last_octets = Instant::now();
                    match self.handle_frame(frame).await {
                        Flow::Continue => {}
                        Flow::Close => break,
                    }
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    // Fatal framing error: 501-class, close after reporting.
                    warn!(err = ?e, "framing error");
                    let (code, text, ..) = close_for(&BrokerError::frame_error(e.to_string()));
                    let m = Method::ConnectionClose {
                        reply_code: code,
                        reply_text: text,
                        class_id: 0,
                        method_id: 0,
                    };
                    let _ = self.outbound.send(OutboundFrame::Method { channel: 0, method: m });
                    break;
                }
            }

            // 2) Enforce the silence deadline (§4.2.7): two silent
            //    intervals without client octets → close without
            //    handshaking. Checked here and, while waiting, via the
            //    read deadline below.
            if last_octets.elapsed() > heartbeat_timeout {
                debug!("heartbeat timeout");
                break;
            }

            // 3) Race client octets against the silence deadline. Reads
            //    collect into `pending` (bytes not yet forming a frame)
            //    and loop until either data arrives or the deadline hits.
            let deadline = tokio::time::Instant::from_std(last_octets + heartbeat_timeout);
            let mut chunk = [0u8; 4096];
            let mut read_buf = ReadBuf::new(&mut chunk);
            let sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(sleep);
            let mut got_octets = false;
            let read_result = std::future::poll_fn(|cx| loop {
                // Client octets win whenever they are ready.
                match Pin::new(&mut *reader).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {}
                }
                if sleep.as_mut().poll(cx).is_ready() {
                    // Deadline: stop waiting even though the read pends.
                    return Poll::Ready(Ok(()));
                }
                return Poll::Pending;
            })
            .await;
            match read_result {
                Err(e) => {
                    debug!(err = ?e, "read error");
                    break;
                }
                Ok(()) => {
                    let filled = read_buf.filled();
                    if filled.is_empty() {
                        // Either the deadline fired or the client hung up.
                        if last_octets.elapsed() > heartbeat_timeout {
                            debug!("heartbeat timeout");
                            break;
                        }
                        // EOF with octets recently received: the next
                        // iteration's deadline check closes us.
                        break;
                    }
                    got_octets = true;
                    last_octets = Instant::now();
                    fr.feed(filled);
                }
            }
            let _ = got_octets;
        }

        // Channel teardown: cancel consumers, release unacked (§4.5).
        self.teardown().await;
    }

    async fn teardown(&mut self) {
        let ids: Vec<u16> = self.channels.keys().copied().collect();
        for id in ids {
            if let Some(ch) = self.channels.remove(&id) {
                ch.teardown(&self.node).await;
            }
        }
    }

    // ------------------------------------------------------------------
    // Frame dispatch
    // ------------------------------------------------------------------

    async fn handle_frame(&mut self, frame: switchboard_wire::Frame) -> Flow {
        use switchboard_wire::FrameType;
        match frame.frame_type {
            FrameType::Heartbeat => {
                if frame.channel != 0 {
                    return self.connection_exception(BrokerError::command_invalid(
                        "heartbeat frame on non-zero channel",
                    ));
                }
                Flow::Continue
            }
            FrameType::Method => {
                let Ok(method) = frame.decode_method() else {
                    return self.connection_exception(BrokerError::syntax_error("malformed method"));
                };
                if frame.channel == 0 {
                    if !crate::channel::method_allowed_on_channel_zero(&method) {
                        return self.connection_exception(BrokerError::command_invalid(
                            "non-connection method on channel 0",
                        ));
                    }
                    return self.handle_connection_method(method).await;
                }
                self.handle_channel_method(frame.channel, method).await
            }
            FrameType::Header => {
                if frame.channel == 0 {
                    return self
                        .connection_exception(BrokerError::channel_error("content on channel 0"));
                }
                let Some(ch) = self.channels.get(&frame.channel).cloned() else {
                    return self.connection_exception(crate::channel::unknown_channel(frame.channel));
                };
                let expected = {
                    let inner = ch.inner.lock().unwrap();
                    match &inner.pending_method {
                        Some(m) => m.class_id(),
                        None => return self.connection_exception(unexpected_content()),
                    }
                };
                match frame.content_header(expected) {
                    Ok(h) => {
                        ch.inner.lock().unwrap().pending_header = Some(h);
                        Flow::Continue
                    }
                    Err(e) => self.connection_exception(BrokerError::frame_error(e.to_string())),
                }
            }
            FrameType::Body => {
                if frame.channel == 0 {
                    return self
                        .connection_exception(BrokerError::channel_error("content on channel 0"));
                }
                let Some(ch) = self.channels.get(&frame.channel).cloned() else {
                    return self.connection_exception(crate::channel::unknown_channel(frame.channel));
                };
                let (done, complete) = {
                    let mut inner = ch.inner.lock().unwrap();
                    let Some(header) = inner.pending_header.as_ref().map(|h| h.body_size) else {
                        return self.connection_exception(unexpected_content());
                    };
                    inner.pending_body.extend_from_slice(&frame.payload);
                    (inner.pending_body.len() as u64 >= header, true)
                };
                if done && complete {
                    let content = ch.inner.lock().unwrap().take_content();
                    if let Some((method, props, body)) = content {
                        return self.content_complete(frame.channel, method, props, body).await;
                    }
                }
                Flow::Continue
            }
        }
    }

    /// Queue a Connection.Close for a structural error, then close.
    fn connection_exception(&mut self, e: BrokerError) -> Flow {
        let (code, text, class, method) = close_for(&e);
        let m = Method::ConnectionClose {
            reply_code: code,
            reply_text: text,
            class_id: class,
            method_id: method,
        };
        let _ = self.outbound.send(OutboundFrame::Method { channel: 0, method: m });
        Flow::Close
    }

    /// Queue a Channel.Close for an operational error, closing the channel.
    async fn channel_exception(&mut self, id: u16, e: BrokerError) -> Flow {
        warn!(channel = id, code = e.code, text = %e.text, "channel exception");
        let (code, text, class, method) = close_for(&e);
        let m = Method::ChannelClose {
            reply_code: code,
            reply_text: text,
            class_id: class,
            method_id: method,
        };
        let _ = self.outbound.send(OutboundFrame::Method { channel: id, method: m });
        if let Some(ch) = self.channels.remove(&id) {
            ch.teardown(&self.node).await;
        }
        Flow::Continue
    }

    // ------------------------------------------------------------------
    // Connection-class methods
    // ------------------------------------------------------------------

    async fn handle_connection_method(&mut self, m: Method) -> Flow {
        match m {
            Method::ConnectionClose { .. } => {
                // Respond Close-Ok, then close (§2.3.7).
                let _ = self.outbound.send(OutboundFrame::Method {
                    channel: 0,
                    method: Method::ConnectionCloseOk {},
                });
                Flow::Close
            }
            Method::ConnectionCloseOk {} => Flow::Close,
            other => {
                let (code, text, ..) = close_for(&BrokerError::command_invalid(format!(
                    "unexpected {} on channel 0",
                    other.name()
                )));
                let m = Method::ConnectionClose {
                    reply_code: code,
                    reply_text: text,
                    class_id: 0,
                    method_id: 0,
                };
                let _ = self.outbound.send(OutboundFrame::Method { channel: 0, method: m });
                Flow::Close
            }
        }
    }

    // ------------------------------------------------------------------
    // Channel-class dispatch
    // ------------------------------------------------------------------

    async fn handle_channel_method(&mut self, id: u16, m: Method) -> Flow {
        match &m {
            Method::ChannelOpen { .. } => {
                if self.channels.contains_key(&id) {
                    return self.connection_exception(crate::channel::channel_already_open(id));
                }
                if self.limits.channel_max != 0 && id > self.limits.channel_max {
                    return self.connection_exception(BrokerError::channel_error(format!(
                        "channel {id} exceeds negotiated channel-max {}",
                        self.limits.channel_max
                    )));
                }
                let ch = Channel::new(
                    id,
                    self.conn,
                    self.node.id,
                    self.vhost.clone(),
                    self.limits.clone(),
                    self.outbound.clone(),
                );
                self.channels.insert(id, ch);
                let _ = self.outbound.send(OutboundFrame::Method {
                    channel: id,
                    method: Method::ChannelOpenOk { channel_id: vec![] },
                });
                return Flow::Continue;
            }
            Method::ChannelClose { .. } => {
                if let Some(ch) = self.channels.remove(&id) {
                    ch.teardown(&self.node).await;
                }
                let _ = self.outbound.send(OutboundFrame::Method {
                    channel: id,
                    method: Method::ChannelCloseOk {},
                });
                return Flow::Continue;
            }
            Method::ChannelCloseOk {} => {
                let _ = self.channels.remove(&id);
                return Flow::Continue;
            }
            _ => {}
        }

        let Some(ch) = self.channels.get(&id).cloned() else {
            return self.connection_exception(crate::channel::unknown_channel(id));
        };
        match ch.handle(&self.node, m).await {
            Ok(()) => Flow::Continue,
            Err(e) if e.level == Level::Connection => self.connection_exception(e),
            Err(e) => self.channel_exception(id, e).await,
        }
    }

    // ------------------------------------------------------------------
    // Content completion (publish)
    // ------------------------------------------------------------------

    async fn content_complete(
        &mut self,
        channel: u16,
        method: Method,
        props: switchboard_wire::BasicProperties,
        body: Vec<u8>,
    ) -> Flow {
        let Some(ch) = self.channels.get(&channel).cloned() else {
            return self.connection_exception(crate::channel::unknown_channel(channel));
        };
        match ch.publish(&self.node, method, props, body).await {
            Ok(()) => Flow::Continue,
            Err(e) if e.level == Level::Connection => self.connection_exception(e),
            Err(e) => self.channel_exception(channel, e).await,
        }
    }
}

enum Flow {
    Continue,
    Close,
}

fn io_err(e: switchboard_wire::CodecError) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

/// Silence the unused-import lint for the reply constants module used by
/// handler diagnostics.
#[allow(dead_code)]
fn reply_name(code: u16) -> u16 {
    let _ = reply::REPLY_SUCCESS;
    code
}
