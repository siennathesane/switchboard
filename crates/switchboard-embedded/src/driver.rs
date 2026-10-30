//! Client-side machinery: the handshake, the per-connection demux driver,
//! and the writer task (embedded design §4.2–§4.4).
//!
//! This is the mirror image of the server's session loop
//! (session/mod.rs:364-469): one driver task owns the client read half of
//! the duplex and demultiplexes incoming frames into typed slots; one
//! writer task owns the write half. The only new protocol machinery in the
//! whole crate lives here — the server side runs unmodified.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use bytes::BytesMut;
use switchboard_server::outbound::max_body_size;
use switchboard_wire::BasicProperties;
use switchboard_wire::Frame;
use switchboard_wire::FrameReader;
use switchboard_wire::FrameType;
use switchboard_wire::constants::reply;
use switchboard_wire::field::FieldTable;
use switchboard_wire::frame::PROTOCOL_HEADER;
use switchboard_wire::method::Method;
use switchboard_wire::properties::ContentHeader;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::debug;

use crate::error::ConnectError;
use crate::error::OpError;
use crate::error::decode_channel_close;
use crate::types::ConfirmOutcome;
use crate::types::Delivery;
use crate::types::ReturnedMessage;

/// Budget for one handshake and one close handshake. Generous in
/// production; shrinks with the test tempo like every other wait.
pub(crate) fn handshake_budget() -> Duration {
    switchboard_core::tempo::scale(Duration::from_secs(10))
}

/// One message for the writer task.
pub(crate) enum WriteMsg {
    /// Pre-encoded frames (a call's method frame, or a full publish
    /// triplet) — one mpsc message keeps a frame group atomic in the
    /// writer queue so concurrent channels cannot interleave a triplet.
    Frame(Bytes),
    /// Terminate the writer: shut the write half down (the server's reader
    /// sees EOF and runs its normal teardown).
    Shutdown,
}

/// Reply to one outstanding synchronous call.
pub(crate) type OpReply = Result<(Method, Option<(BasicProperties, Vec<u8>)>), OpError>;

/// Reply to one outstanding `publish_confirmed`.
pub(crate) type ConfirmReply = Result<ConfirmOutcome, OpError>;

/// In-flight content assembly state, mirroring session/mod.rs:517-547.
pub(crate) struct PendingContent {
    method: Method,
    header: Option<ContentHeader>,
    body: Vec<u8>,
}

/// State shared between a connection's handles, its driver task, and its
/// writer task. All mutation goes through the driver; handles only read
/// flags and enqueue writes.
pub(crate) struct ConnShared {
    pub(crate) writer: mpsc::UnboundedSender<WriteMsg>,
    /// False once the connection is closed (by us, by the server, or by
    /// EOF). Handles check this before every operation.
    pub(crate) valid: AtomicBool,
    /// Guards the client-initiated close handshake: exactly one actor
    /// (the embedder's `close()` or the broker registry) runs it.
    pub(crate) closing: AtomicBool,
    /// The stored close pair for `ConnectionClosed` errors after the fact.
    pub(crate) close_pair: Mutex<(u16, String)>,
    /// Open channels by number.
    pub(crate) channels: Mutex<HashMap<u16, Arc<ChannelShared>>>,
    /// Registered consumers by tag → (channel, delivery stream).
    pub(crate) consumers: Mutex<HashMap<String, (u16, mpsc::UnboundedSender<Delivery>)>>,
    /// Deliveries for consumer tags not yet registered client-side
    /// (server-generated tags race the Consume-Ok only in principle; the
    /// buffer makes the race impossible to lose).
    pub(crate) pending_deliveries: Mutex<HashMap<String, Vec<Delivery>>>,
    /// Mandatory-unroutable returns stream; taken by `take_returns`.
    pub(crate) returns_tx: Mutex<Option<mpsc::UnboundedSender<ReturnedMessage>>>,
    /// Completed `basic.return` awaiting a `publish_mandatory` correlation,
    /// per channel (at most one mandatory publish is in flight per channel
    /// — both mandatory-ish ops hold the channel's publish lock).
    pub(crate) mandatory_returns:
        Mutex<HashMap<u16, Vec<oneshot::Sender<Result<PublishOutcomeReply, OpError>>>>>,
    /// Waiters for the client-initiated close handshake.
    pub(crate) close_waiters: Mutex<Vec<oneshot::Sender<()>>>,
    /// Next channel number (starts at 1; channel 0 is connection-class).
    pub(crate) next_channel: AtomicU16,
    /// Negotiated limits (§2.3.3: min of both sides' Tune values). The
    /// agreed heartbeat also went to the writer task at spawn time; it is
    /// recorded here only as documentation of the negotiated value.
    pub(crate) channel_max: u16,
    pub(crate) frame_max: u32,
    /// Content assembly per channel.
    pub(crate) pending: Mutex<HashMap<u16, PendingContent>>,
}

impl ConnShared {
    /// The stored connection-close error for post-close failures.
    pub(crate) fn closed_error(&self) -> OpError {
        let (code, text) = self.close_pair.lock().unwrap().clone();
        OpError::ConnectionClosed { code, text }
    }

    /// Fail everything on the connection and poison all channels. Safe to
    /// call twice (idempotent through the `valid` flag for the channel
    /// maps, which are drained).
    pub(crate) fn mark_closed(self: &Arc<Self>, code: u16, text: impl Into<String>) {
        let text = text.into();
        {
            let mut pair = self.close_pair.lock().unwrap();
            *pair = (code, text.clone());
        }
        let channels: Vec<Arc<ChannelShared>> = {
            let mut map = self.channels.lock().unwrap();
            map.drain().map(|(_, ch)| ch).collect()
        };
        for ch in channels {
            ch.valid.store(false, Ordering::SeqCst);
            {
                let mut info = ch.close_info.lock().unwrap();
                if info.is_none() {
                    *info = Some((code, text.clone(), 0, 0));
                }
            }
            fail_channel_waiters(&ch, || OpError::ConnectionClosed {
                code,
                text: text.clone(),
            });
        }
        self.consumers.lock().unwrap().clear();
        self.pending_deliveries.lock().unwrap().clear();
        {
            let mut mand = self.mandatory_returns.lock().unwrap();
            for (_, waiters) in mand.drain() {
                for tx in waiters {
                    let _ = tx.send(Err(OpError::ConnectionClosed {
                        code,
                        text: text.clone(),
                    }));
                }
            }
        }
        *self.returns_tx.lock().unwrap() = None;
        for tx in std::mem::take(&mut *self.close_waiters.lock().unwrap()) {
            let _ = tx.send(());
        }
    }
}

/// Per-channel client state shared by all clones of an
/// [`EmbeddedChannel`](crate::EmbeddedChannel).
pub(crate) struct ChannelShared {
    pub(crate) id: u16,
    pub(crate) conn: Arc<ConnShared>,
    /// False once the server closed this channel. Every clone fails fast
    /// with the stored close pair instead of writing frames that would kill
    /// the connection (§4.3 self-invalidation).
    pub(crate) valid: AtomicBool,
    /// The stored §4.8 close pair (code, text, class-id, method-id).
    pub(crate) close_info: Mutex<Option<(u16, String, u16, u16)>>,
    /// Outstanding synchronous calls, FIFO: replies arrive in the order the
    /// calls were written (push+send happen under one lock).
    pub(crate) waiters: Mutex<VecDeque<oneshot::Sender<OpReply>>>,
    /// Confirm mode (Confirm.Select) active on this channel.
    pub(crate) confirm_active: AtomicBool,
    /// Publishes sent while confirm mode is active; the server numbers
    /// them per channel starting at 1 (channel/mod.rs:123-129, methods
    /// /mod.rs:143-153).
    pub(crate) confirm_next: Mutex<u64>,
    /// publish-seq → `publish_confirmed` waiter.
    pub(crate) confirm_waiters: Mutex<BTreeMap<u64, oneshot::Sender<ConfirmReply>>>,
    /// Serializes the mandatory-ish publish paths (`publish_mandatory`,
    /// `publish_confirmed`, `confirm_select`) so a `basic.return` is never
    /// ambiguous between two in-flight publishes.
    pub(crate) publish_lock: tokio::sync::Mutex<()>,
}

impl ChannelShared {
    pub(crate) fn new(id: u16, conn: Arc<ConnShared>) -> Arc<Self> {
        Arc::new(ChannelShared {
            id,
            conn,
            valid: AtomicBool::new(true),
            close_info: Mutex::new(None),
            waiters: Mutex::new(VecDeque::new()),
            confirm_active: AtomicBool::new(false),
            confirm_next: Mutex::new(0),
            confirm_waiters: Mutex::new(BTreeMap::new()),
            publish_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// The stored channel-close error for post-close failures. Deliberately
    /// NOT re-decoded into 403/404/406: the named variants are for the op
    /// that TRIGGERED the close; later calls (on this handle or any clone)
    /// fail fast with the raw §4.8 pair as `ChannelClosed` (embedded
    /// design §3, §4.3).
    pub(crate) fn closed_error(&self) -> OpError {
        if let Some((code, text, class_id, method_id)) = self.close_info.lock().unwrap().clone() {
            return OpError::ChannelClosed {
                code,
                text,
                class_id,
                method_id,
            };
        }
        // Closed without a pair (connection-level death): fall through to
        // the connection's stored pair.
        self.conn.closed_error()
    }

    /// Fail fast if this channel or its connection is gone.
    pub(crate) fn ensure_usable(&self) -> Result<(), OpError> {
        if !self.valid.load(Ordering::SeqCst) {
            return Err(self.closed_error());
        }
        if !self.conn.valid.load(Ordering::SeqCst) {
            return Err(self.conn.closed_error());
        }
        Ok(())
    }
}

fn fail_channel_waiters(ch: &Arc<ChannelShared>, make: impl Fn() -> OpError) {
    for tx in std::mem::take(&mut *ch.waiters.lock().unwrap()) {
        let _ = tx.send(Err(make()));
    }
    for (_, tx) in std::mem::take(&mut *ch.confirm_waiters.lock().unwrap()) {
        let _ = tx.send(Err(make()));
    }
    if let Some(waiters) = ch.conn.mandatory_returns.lock().unwrap().remove(&ch.id) {
        for tx in waiters {
            let _ = tx.send(Err(make()));
        }
    }
}

/// The server closed channel `id` (session/mod.rs:583-586 removes and
/// tears the channel down immediately; any later frame on that number
/// would kill the whole connection with 504).
fn server_closed_channel(conn: &Arc<ConnShared>, id: u16, close: (u16, String, u16, u16)) {
    let Some(ch) = conn.channels.lock().unwrap().remove(&id) else {
        return;
    };
    ch.valid.store(false, Ordering::SeqCst);
    *ch.close_info.lock().unwrap() = Some(close.clone());
    // Reply Close-Ok (§2.3.7) — the server then answers Close-Ok again on
    // its side, which the driver ignores (no waiters).
    let mut buf = BytesMut::new();
    Frame::method(id, &Method::ChannelCloseOk {}).encode(&mut buf);
    let _ = conn.writer.send(WriteMsg::Frame(buf.freeze()));
    let mk = || decode_channel_close(close.0, close.1.clone(), close.2, close.3);
    fail_channel_waiters(&ch, mk);
    conn.consumers.lock().unwrap().retain(|_, (c, _)| *c != id);
}

// ---------------------------------------------------------------------------
// Handshake (client side of session/mod.rs:199-330)
// ---------------------------------------------------------------------------

pub(crate) struct Agreed {
    pub(crate) channel_max: u16,
    pub(crate) frame_max: u32,
    pub(crate) heartbeat: u16,
}

fn io_other(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

/// The client may only lower (§2.3.3) — the same function the server uses
/// (session/mod.rs:332-338); zero means "no opinion, take the server's".
fn agreed16(server: u16, client: u16) -> u16 {
    if client == 0 {
        server
    } else {
        server.min(client)
    }
}

fn agreed(server: u32, client: u32) -> u32 {
    if client == 0 {
        server
    } else {
        server.min(client)
    }
}

async fn send_method<W: AsyncWrite + Unpin>(writer: &mut W, m: &Method) -> std::io::Result<()> {
    let bytes = Frame::method(0, m).to_bytes();
    writer.write_all(&bytes).await?;
    writer.flush().await
}

/// Sequential method read during the handshake; `Ok(None)` on EOF. The
/// first read detects the 8-byte protocol-header reject echo
/// (session/mod.rs:86-91 writes a valid header before closing).
async fn read_method_or_eof<R: AsyncRead + Unpin>(
    reader: &mut R,
    fr: &mut FrameReader,
) -> std::io::Result<Option<Method>> {
    let mut first = true;
    loop {
        if let Some(f) = fr.next_frame(0).map_err(io_other)? {
            if matches!(f.frame_type, FrameType::Method) {
                return Ok(Some(f.decode_method().map_err(io_other)?));
            }
            continue;
        }
        let mut buf = [0u8; 4096];
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        if first && n >= 8 && buf[..8] == PROTOCOL_HEADER {
            // Server rejected our header: a valid protocol header then EOF.
            first = false;
            fr.feed(&buf[8..n]);
            continue;
        }
        first = false;
        fr.feed(&buf[..n]);
    }
}

/// Run the client side of the handshake over the given halves.
///
/// Error mapping is by STAGE (embedded design §4.3): EOF before Start-Ok →
/// `Closed`; EOF after Start-Ok → `AccessRefused`; EOF after Tune-Ok+Open →
/// `NoSuchVhost(vhost)`. The handshake is bounded by
/// [`handshake_budget`].
pub(crate) async fn handshake<R, W>(
    mut reader: R,
    mut writer: W,
    user: &str,
    password: &str,
    vhost: &str,
    limits: &crate::config::EmbeddedLimits,
) -> Result<(Agreed, R, W), ConnectError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let fut = async {
        // §4.2.2: the client MUST start with the 8-octet protocol header.
        writer
            .write_all(&PROTOCOL_HEADER)
            .await
            .map_err(ConnectError::Io)?;
        writer.flush().await.map_err(ConnectError::Io)?;
        let mut fr = FrameReader::new();

        // 1. Connection.Start (the server speaks first).
        match read_method_or_eof(&mut reader, &mut fr)
            .await
            .map_err(ConnectError::Io)?
        {
            Some(Method::ConnectionStart { .. }) => {}
            _ => return Err(ConnectError::Closed),
        }

        // 2. Connection.Start-Ok with PLAIN credentials (RFC 4616:
        //    \0authzid\0authcid\0passwd; auth/mod.rs:24).
        let mut response = vec![0u8];
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        response.extend_from_slice(password.as_bytes());
        send_method(
            &mut writer,
            &Method::ConnectionStartOk {
                client_properties: FieldTable::new(),
                mechanism: "PLAIN".into(),
                response,
                locale: switchboard_core::auth::LOCALES.into(),
            },
        )
        .await
        .map_err(ConnectError::Io)?;

        // 3. Connection.Tune. EOF here = the server refused our
        //    credentials/mechanism (it never writes before closing,
        //    session/mod.rs:248-252, 276-278).
        let tuned = match read_method_or_eof(&mut reader, &mut fr)
            .await
            .map_err(ConnectError::Io)?
        {
            Some(Method::ConnectionTune {
                channel_max,
                frame_max,
                heartbeat,
            }) => (channel_max, frame_max, heartbeat),
            _ => return Err(ConnectError::AccessRefused),
        };
        send_method(
            &mut writer,
            &Method::ConnectionTuneOk {
                channel_max: limits.channel_max,
                frame_max: limits.frame_max,
                heartbeat: limits.heartbeat,
            },
        )
        .await
        .map_err(ConnectError::Io)?;

        // 4. Connection.Open (vhost selection; empty names "/" —
        //    session/mod.rs:305-312).
        send_method(
            &mut writer,
            &Method::ConnectionOpen {
                virtual_host: vhost.into(),
                capabilities: String::new(),
                insist: false,
            },
        )
        .await
        .map_err(ConnectError::Io)?;

        // 5. Connection.Open-Ok. EOF here = unknown vhost
        //    (session/mod.rs:314-317).
        match read_method_or_eof(&mut reader, &mut fr)
            .await
            .map_err(ConnectError::Io)?
        {
            Some(Method::ConnectionOpenOk { .. }) => {}
            _ => return Err(ConnectError::NoSuchVhost(vhost.to_string())),
        }

        let agreed_values = Agreed {
            channel_max: agreed16(tuned.0, limits.channel_max),
            frame_max: agreed(tuned.1, limits.frame_max),
            heartbeat: agreed16(tuned.2, limits.heartbeat),
        };
        Ok((agreed_values, reader, writer))
    };
    match tokio::time::timeout(handshake_budget(), fut).await {
        Ok(r) => r,
        Err(_elapsed) => Err(ConnectError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "handshake did not complete in time",
        ))),
    }
}

// ---------------------------------------------------------------------------
// Driver task (the demux; mirror of session/mod.rs reader_loop)
// ---------------------------------------------------------------------------

/// Run the client driver until the connection is closed (by us, by the
/// server, or by EOF). `close_rx` is the broker registry's close trigger.
pub(crate) async fn driver_task<R>(
    mut reader: R,
    conn: Arc<ConnShared>,
    mut close_rx: watch::Receiver<bool>,
) where
    R: AsyncRead + Unpin + Send,
{
    let mut fr = FrameReader::new();
    // A close request may already be pending (stop raced connect).
    if *close_rx.borrow() {
        close_handshake(&conn).await;
        return;
    }
    let mut buf = [0u8; 8192];
    loop {
        // 1) Dispatch any complete buffered frames.
        loop {
            match fr.next_frame(0) {
                Ok(Some(frame)) => {
                    if !handle_frame(&conn, frame).await {
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    // Fatal framing error from the peer.
                    conn.mark_closed(reply::FRAME_ERROR, format!("frame error: {e}"));
                    let _ = conn.writer.send(WriteMsg::Shutdown);
                    return;
                }
            }
        }
        // 2) Wait for a close trigger or more octets.
        tokio::select! {
            biased;
            changed = close_rx.changed() => {
                match changed {
                    Ok(()) if *close_rx.borrow_and_update() => {
                        close_handshake(&conn).await;
                        return;
                    }
                    Ok(()) => {} // spurious write of `false`; keep reading
                    Err(_) => {} // sender gone (broker dropped); keep reading
                }
            }
            n = reader.read(&mut buf) => {
                match n {
                    Ok(0) => {
                        // EOF: the peer went away (or its writer shut down).
                        conn.mark_closed(0, "connection closed by peer");
                        let _ = conn.writer.send(WriteMsg::Shutdown);
                        return;
                    }
                    Ok(n) => fr.feed(&buf[..n]),
                    Err(e) => {
                        conn.mark_closed(0, format!("read error: {e}"));
                        let _ = conn.writer.send(WriteMsg::Shutdown);
                        return;
                    }
                }
            }
        }
    }
}

/// Handle one inbound frame. Returns false when the driver must exit.
async fn handle_frame(conn: &Arc<ConnShared>, frame: Frame) -> bool {
    match frame.frame_type {
        FrameType::Heartbeat => true, // octets count; nothing to do
        FrameType::Method => {
            let Ok(method) = frame.decode_method() else {
                conn.mark_closed(reply::SYNTAX_ERROR, "malformed method frame");
                let _ = conn.writer.send(WriteMsg::Shutdown);
                return false;
            };
            handle_method(conn, frame.channel, method).await
        }
        FrameType::Header => {
            let pending_class = {
                let map = conn.pending.lock().unwrap();
                map.get(&frame.channel).map(|p| p.method.class_id())
            };
            let Some(expected) = pending_class else {
                // Content without a content-bearing method: protocol
                // violation (§4.2.6.1) — the server never sends one.
                conn.mark_closed(reply::UNEXPECTED_FRAME, "content header without method");
                let _ = conn.writer.send(WriteMsg::Shutdown);
                return false;
            };
            let header = match frame.content_header(expected) {
                Ok(h) => h,
                Err(e) => {
                    conn.mark_closed(reply::FRAME_ERROR, format!("bad content header: {e}"));
                    let _ = conn.writer.send(WriteMsg::Shutdown);
                    return false;
                }
            };
            let complete = {
                let mut map = conn.pending.lock().unwrap();
                let Some(p) = map.get_mut(&frame.channel) else {
                    return false;
                };
                p.header = Some(header);
                let size = p.header.as_ref().map(|h| h.body_size).unwrap_or(0);
                size == 0 // zero-size content completes on the header itself
            };
            if complete {
                dispatch_content(conn, frame.channel).await;
            }
            true
        }
        FrameType::Body => {
            let done = {
                let mut map = conn.pending.lock().unwrap();
                let Some(p) = map.get_mut(&frame.channel) else {
                    conn.mark_closed(reply::UNEXPECTED_FRAME, "body frame without method");
                    return false;
                };
                let Some(header) = p.header.as_ref() else {
                    conn.mark_closed(reply::UNEXPECTED_FRAME, "body frame without header");
                    return false;
                };
                p.body.extend_from_slice(&frame.payload);
                p.body.len() as u64 >= header.body_size
            };
            if done {
                dispatch_content(conn, frame.channel).await;
            }
            true
        }
    }
}

async fn handle_method(conn: &Arc<ConnShared>, channel: u16, method: Method) -> bool {
    match method {
        // ---- channel 0: connection class ----
        Method::ConnectionClose {
            reply_code,
            reply_text,
            ..
        } => {
            // Server-side connection exception: reply Close-Ok (§2.3.7),
            // fail everything, and let the EOF that follows end the loop.
            let mut buf = BytesMut::new();
            Frame::method(0, &Method::ConnectionCloseOk {}).encode(&mut buf);
            let _ = conn.writer.send(WriteMsg::Frame(buf.freeze()));
            conn.mark_closed(reply_code, reply_text);
            true
        }
        Method::ConnectionCloseOk {} => {
            // Our own close handshake completed.
            for tx in std::mem::take(&mut *conn.close_waiters.lock().unwrap()) {
                let _ = tx.send(());
            }
            let _ = conn.writer.send(WriteMsg::Shutdown);
            false
        }
        Method::ConnectionBlocked { .. } | Method::ConnectionUnblocked {} => true,

        // ---- channel N ----
        Method::BasicDeliver { .. } => {
            start_content(conn, channel, method);
            true
        }
        Method::BasicReturn { .. } => {
            start_content(conn, channel, method);
            true
        }
        Method::BasicGetOk { .. } => {
            // A reply WITH content: assemble, then resolve the front waiter.
            start_content(conn, channel, method);
            true
        }
        Method::BasicAck {
            delivery_tag,
            multiple,
        } => {
            // Inbound acks are publisher confirms (methods/publish.rs:201-211).
            resolve_confirms(conn, channel, delivery_tag, multiple);
            true
        }
        Method::BasicCancel { consumer_tag, .. } => {
            // Consumer-cancellation notification (methods/consume.rs:257-269):
            // dropping the sender makes Consumer::next() return None.
            let mut consumers = conn.consumers.lock().unwrap();
            if consumers.remove(&consumer_tag).is_none() {
                debug!(tag = %consumer_tag, "cancel for unknown consumer");
            }
            true
        }
        Method::ChannelClose {
            reply_code,
            reply_text,
            class_id,
            method_id,
        } => {
            server_closed_channel(conn, channel, (reply_code, reply_text, class_id, method_id));
            true
        }
        Method::ChannelCloseOk {} => true, // nothing waits on channel close
        Method::ChannelFlowOk { .. } => {
            resolve_front_waiter(conn, channel, method, None);
            true
        }
        other => {
            // Everything else inbound is a synchronous reply (*-Ok,
            // Basic.Get-Empty). Content-bearing replies assemble first and
            // resolve on completion.
            if other.carries_content() {
                start_content(conn, channel, other);
            } else {
                resolve_front_waiter(conn, channel, other, None);
            }
            true
        }
    }
}

fn start_content(conn: &Arc<ConnShared>, channel: u16, method: Method) {
    conn.pending.lock().unwrap().insert(
        channel,
        PendingContent {
            method,
            header: None,
            body: Vec::new(),
        },
    );
}

/// A content triplet completed: dispatch by method.
async fn dispatch_content(conn: &Arc<ConnShared>, channel: u16) {
    let Some((method, props, body)) = (|| {
        let mut map = conn.pending.lock().unwrap();
        let p = map.remove(&channel)?;
        let header = p.header?;
        Some((p.method, header.properties, p.body))
    })() else {
        return;
    };
    match method {
        Method::BasicDeliver {
            consumer_tag,
            delivery_tag,
            redelivered,
            exchange,
            routing_key,
        } => {
            let delivery = Delivery {
                consumer_tag: consumer_tag.clone(),
                delivery_tag,
                redelivered,
                exchange,
                routing_key,
                properties: props,
                body,
            };
            let mut consumers = conn.consumers.lock().unwrap();
            match consumers.get_mut(&consumer_tag) {
                Some((_, tx)) => {
                    let _ = tx.send(delivery);
                }
                None => {
                    conn.pending_deliveries
                        .lock()
                        .unwrap()
                        .entry(consumer_tag)
                        .or_default()
                        .push(delivery);
                }
            }
        }
        Method::BasicReturn {
            reply_code,
            reply_text,
            exchange,
            routing_key,
        } => {
            let returned = ReturnedMessage {
                code: reply_code,
                text: reply_text.clone(),
                exchange: exchange.clone(),
                routing_key: routing_key.clone(),
                properties: props,
                body,
            };
            // A mandatory publish awaiting correlation gets it first (at
            // most one is in flight per channel — publish_lock).
            {
                let mut mand = conn.mandatory_returns.lock().unwrap();
                if let Some(waiters) = mand.get_mut(&channel) {
                    if !waiters.is_empty() {
                        let waiter = waiters.remove(0);
                        let _ = waiter.send(Ok(PublishOutcomeReply::Returned {
                            code: reply_code,
                            text: reply_text,
                        }));
                        return;
                    }
                }
            }
            // Otherwise it flows to the returns stream...
            let returns_tx = conn.returns_tx.lock().unwrap().clone();
            if let Some(tx) = returns_tx {
                let _ = tx.send(returned);
            }
            // ...and resolves the oldest outstanding confirmed publish
            // (server sends Return then the confirm ack,
            // methods/publish.rs:83-99).
            resolve_oldest_confirm(
                conn,
                channel,
                ConfirmOutcome::Returned {
                    code: reply_code,
                    text: reply_text,
                },
            );
        }
        get_ok @ Method::BasicGetOk { .. } => {
            resolve_front_waiter(conn, channel, get_ok, Some((props, body)));
        }
        other => {
            // A content-bearing reply we do not expect (defensive).
            let _ = (props, body);
            resolve_front_waiter(conn, channel, other, None);
        }
    }
}

/// The reply type of a `publish_mandatory` correlation slot.
pub(crate) enum PublishOutcomeReply {
    Returned { code: u16, text: String },
}

fn resolve_front_waiter(
    conn: &Arc<ConnShared>,
    channel: u16,
    method: Method,
    content: Option<(BasicProperties, Vec<u8>)>,
) {
    let ch = conn.channels.lock().unwrap().get(&channel).cloned();
    let Some(ch) = ch else { return };
    let waiter = ch.waiters.lock().unwrap().pop_front();
    if let Some(tx) = waiter {
        let _ = tx.send(Ok((method, content)));
    }
}

fn resolve_confirms(conn: &Arc<ConnShared>, channel: u16, seq: u64, multiple: bool) {
    let ch = conn.channels.lock().unwrap().get(&channel).cloned();
    let Some(ch) = ch else { return };
    let mut waiters = ch.confirm_waiters.lock().unwrap();
    if multiple {
        let upto: Vec<u64> = waiters.keys().copied().take_while(|k| *k <= seq).collect();
        for k in upto {
            if let Some(tx) = waiters.remove(&k) {
                let _ = tx.send(Ok(ConfirmOutcome::Acked));
            }
        }
    } else if let Some(tx) = waiters.remove(&seq) {
        let _ = tx.send(Ok(ConfirmOutcome::Acked));
    }
}

fn resolve_oldest_confirm(conn: &Arc<ConnShared>, channel: u16, outcome: ConfirmOutcome) {
    let ch = conn.channels.lock().unwrap().get(&channel).cloned();
    let Some(ch) = ch else { return };
    let mut waiters = ch.confirm_waiters.lock().unwrap();
    if let Some(first) = waiters.keys().next().copied() {
        if let Some(tx) = waiters.remove(&first) {
            let _ = tx.send(Ok(outcome));
        }
    }
}

// ---------------------------------------------------------------------------
// Client-initiated close (§5.2 registry trigger and EmbeddedConnection::close)
// ---------------------------------------------------------------------------

/// Send `Connection.Close`, await `Close-Ok`, then tear the client side
/// down. Idempotent via the `closing` flag; concurrent callers wait for
/// completion.
pub(crate) async fn close_handshake(conn: &Arc<ConnShared>) {
    if conn.closing.swap(true, Ordering::SeqCst) {
        // Another actor is already closing; piggyback on its waiter.
        let (tx, rx) = oneshot::channel();
        conn.close_waiters.lock().unwrap().push(tx);
        let _ = tokio::time::timeout(handshake_budget(), rx).await;
        return;
    }
    let (tx, rx) = oneshot::channel();
    conn.close_waiters.lock().unwrap().push(tx);
    let mut buf = BytesMut::new();
    Frame::method(
        0,
        &Method::ConnectionClose {
            reply_code: reply::REPLY_SUCCESS,
            reply_text: "connection closed by the embedder".into(),
            class_id: 0,
            method_id: 0,
        },
    )
    .encode(&mut buf);
    let _ = conn.writer.send(WriteMsg::Frame(buf.freeze()));
    let _ = tokio::time::timeout(handshake_budget(), rx).await;
    conn.mark_closed(reply::REPLY_SUCCESS, "connection closed");
    let _ = conn.writer.send(WriteMsg::Shutdown);
}

// ---------------------------------------------------------------------------
// Writer task (mirror of session/mod.rs:118-149)
// ---------------------------------------------------------------------------

pub(crate) async fn writer_task<W: AsyncWrite + Unpin + Send>(
    mut writer: W,
    mut rx: mpsc::UnboundedReceiver<WriteMsg>,
    heartbeat_secs: u16,
) {
    let heartbeat = if heartbeat_secs == 0 {
        Duration::ZERO
    } else {
        switchboard_core::tempo::scale(Duration::from_secs(heartbeat_secs as u64))
    };
    let mut last_write = tokio::time::Instant::now();
    let mut ticker = tokio::time::interval(if heartbeat > Duration::ZERO {
        heartbeat / 2
    } else {
        Duration::from_secs(3600)
    });
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
                    WriteMsg::Frame(Frame::heartbeat().to_bytes().freeze())
                } else {
                    continue;
                }
            }
        };
        let shutdown = matches!(msg, WriteMsg::Shutdown);
        if let WriteMsg::Frame(bytes) = &msg {
            if writer.write_all(bytes).await.is_err() {
                let _ = writer.shutdown().await;
                break;
            }
            let _ = writer.flush().await;
            last_write = tokio::time::Instant::now();
        }
        if shutdown {
            let _ = writer.shutdown().await;
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Typed-op plumbing: begin/await calls, fire-and-forget sends, publishes
// ---------------------------------------------------------------------------

/// Register a synchronous call and write its method frame. The waiter is
/// pushed and the frame enqueued under one lock so wire order always equals
/// waiter order (front-first reply matching is correct even with pipelined
/// calls on one channel).
pub(crate) fn begin_call(
    ch: &Arc<ChannelShared>,
    m: &Method,
) -> Result<oneshot::Receiver<OpReply>, OpError> {
    ch.ensure_usable()?;
    let (tx, rx) = oneshot::channel();
    let mut buf = BytesMut::new();
    Frame::method(ch.id, m).encode(&mut buf);
    let mut waiters = ch.waiters.lock().unwrap();
    ch.conn
        .writer
        .send(WriteMsg::Frame(buf.freeze()))
        .map_err(|_| OpError::Io(std::io::Error::other("connection is closed")))?;
    waiters.push_back(tx);
    Ok(rx)
}

/// Await a call's reply; a dropped waiter means the driver tore the
/// connection down between send and reply.
pub(crate) async fn await_call(rx: oneshot::Receiver<OpReply>) -> OpReply {
    match rx.await {
        Ok(reply) => reply,
        Err(_) => Err(OpError::Io(std::io::Error::other(
            "connection driver is gone",
        ))),
    }
}

/// One synchronous method call: send, await, return the reply.
pub(crate) async fn call(
    ch: &Arc<ChannelShared>,
    m: &Method,
) -> Result<(Method, Option<(BasicProperties, Vec<u8>)>), OpError> {
    let rx = begin_call(ch, m)?;
    await_call(rx).await
}

/// Fire-and-forget send of pre-encoded frames (acks, publishes).
pub(crate) fn send_bytes(ch: &Arc<ChannelShared>, bytes: Bytes) -> Result<(), OpError> {
    ch.ensure_usable()?;
    ch.conn
        .writer
        .send(WriteMsg::Frame(bytes))
        .map_err(|_| OpError::Io(std::io::Error::other("connection is closed")))
}

/// Encode one method frame for channel `id` as an atomic writer message.
pub(crate) fn frame_bytes(id: u16, m: &Method) -> Bytes {
    let mut buf = BytesMut::new();
    Frame::method(id, m).encode(&mut buf);
    buf.freeze()
}

/// The publish triplet (method + content header + body frames split to the
/// agreed frame-max, mirroring outbound/mod.rs:49-71) as one atomic
/// writer-queue message.
pub(crate) fn publish_bytes(
    ch: &Arc<ChannelShared>,
    exchange: &str,
    routing_key: &str,
    mandatory: bool,
    props: BasicProperties,
    body: &[u8],
) -> Bytes {
    let mut buf = BytesMut::new();
    Frame::method(
        ch.id,
        &Method::BasicPublish {
            ticket: 0,
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            mandatory,
            immediate: false,
        },
    )
    .encode(&mut buf);
    Frame::header(ch.id, &ContentHeader::new(body.len() as u64, props)).encode(&mut buf);
    let chunk = max_body_size(ch.conn.frame_max);
    if !body.is_empty() {
        for piece in body.chunks(chunk.max(1)) {
            Frame::body(ch.id, piece).encode(&mut buf);
        }
    }
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agreed_lowers_to_the_client_maximum() {
        assert_eq!(agreed16(2047, 2047), 2047);
        assert_eq!(agreed16(2047, 128), 128);
        assert_eq!(agreed16(60, 0), 60, "zero means no opinion");
        assert_eq!(agreed(131_072, 65_536), 65_536);
        assert_eq!(agreed(131_072, 0), 131_072);
    }

    #[test]
    fn publish_bytes_splits_to_frame_max_and_atoms_as_one_message() {
        let (tx, _rx) = mpsc::unbounded_channel::<WriteMsg>();
        let conn = Arc::new(ConnShared {
            writer: tx,
            valid: AtomicBool::new(true),
            closing: AtomicBool::new(false),
            close_pair: Mutex::new((0, String::new())),
            channels: Mutex::new(HashMap::new()),
            consumers: Mutex::new(HashMap::new()),
            pending_deliveries: Mutex::new(HashMap::new()),
            returns_tx: Mutex::new(None),
            mandatory_returns: Mutex::new(HashMap::new()),
            close_waiters: Mutex::new(Vec::new()),
            next_channel: AtomicU16::new(1),
            channel_max: 2047,
            frame_max: 128,
            pending: Mutex::new(HashMap::new()),
        });
        let ch = ChannelShared::new(3, conn);
        let body = vec![7u8; 500];
        let bytes = publish_bytes(&ch, "amq.direct", "k", false, BasicProperties::new(), &body);
        // 120 bytes of body per 128-octet frame-max frame, like the server.
        let mut r = FrameReader::new();
        r.feed(&bytes);
        let mut bodies = 0;
        let mut total = 0usize;
        while let Some(f) = r.next_frame(0).unwrap() {
            match f.frame_type {
                FrameType::Method => assert_eq!(f.channel, 3),
                FrameType::Header => assert_eq!(f.channel, 3),
                FrameType::Body => {
                    bodies += 1;
                    total += f.payload.len();
                }
                FrameType::Heartbeat => panic!("unexpected heartbeat"),
            }
        }
        assert_eq!(bodies, 5, "ceil(500/120) body frames");
        assert_eq!(total, 500);
    }
}
