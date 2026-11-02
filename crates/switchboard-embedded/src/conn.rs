//! The embedded connection handle.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::io::Error;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use switchboard_wire::method::Method;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::broker::TrackedConn;
use crate::channel::EmbeddedChannel;
use crate::driver::ConnShared;
use crate::driver::{self};
use crate::error::ConnectError;
use crate::types::ReturnedMessage;

/// One open AMQP connection to the in-process broker.
///
/// Obtained only through [`Broker::connect`](crate::Broker::connect) /
/// [`connect_vhost`](crate::Broker::connect_vhost) — every connection is
/// registered with the broker so `stop` can close it; there is no
/// untracked way to obtain one (embedded design §5.2).
///
/// The value is cheap to clone-free share through its channels; dropping it
/// without [`close`](EmbeddedConnection::close) leaves the session alive
/// until the broker stops (documented §9/R6 behavior — prefer closing).
pub struct EmbeddedConnection {
    pub(crate) conn: Arc<ConnShared>,
    pub(crate) registry: Arc<Mutex<HashMap<u64, TrackedConn>>>,
    pub(crate) key: u64,
    pub(crate) returns_rx: Mutex<Option<mpsc::UnboundedReceiver<ReturnedMessage>>>,
}

impl EmbeddedConnection {
    /// Open a new channel on this connection. Channel numbers are
    /// connection-scoped and start at 1.
    ///
    /// Fails with [`ConnectError::Closed`] once the connection is gone,
    /// or when the negotiated channel-max is exhausted.
    pub async fn open_channel(&self) -> Result<EmbeddedChannel, ConnectError> {
        if !self.conn.valid.load(Ordering::SeqCst) {
            return Err(ConnectError::Closed);
        }
        let id = self.conn.next_channel.fetch_add(1, Ordering::SeqCst);
        if self.conn.channel_max != 0 && id > self.conn.channel_max {
            return Err(ConnectError::Io(Error::other(format!(
                "channel limit {} reached on this connection",
                self.conn.channel_max
            ))));
        }
        let ch = driver::ChannelShared::new(id, self.conn.clone());
        self.conn.channels.lock().unwrap().insert(id, ch.clone());

        let open = Method::ChannelOpen {
            out_of_band: String::new(),
        };
        let rx = driver::begin_call(&ch, &open).map_err(ConnectError::from)?;
        let reply = match timeout(driver::handshake_budget(), driver::await_call(rx)).await {
            Ok(r) => r,
            Err(_elapsed) => {
                return Err(ConnectError::Io(Error::new(
                    ErrorKind::TimedOut,
                    "Channel.Open-Ok did not arrive in time",
                )));
            }
        };
        match reply {
            Ok((Method::ChannelOpenOk { .. }, _)) => {}
            Ok((other, _)) => {
                return Err(ConnectError::Io(Error::other(format!(
                    "unexpected reply {} to Channel.Open",
                    other.name()
                ))));
            }
            Err(e) => return Err(ConnectError::from(e)),
        }
        Ok(EmbeddedChannel { shared: ch })
    }

    /// Close the connection: send `Connection.Close`, await `Close-Ok`
    /// (§2.3.7), then let the server's reader loop exit on EOF and run the
    /// normal teardown — consumers cancelled, unacked deliveries requeued
    /// (session/mod.rs:471-478, methods/tx.rs:123-167). Tears down all
    /// channels (§4.5).
    ///
    /// This also deregisters the connection from the broker, so `stop`
    /// will not wait for it.
    pub async fn close(self) {
        driver::close_handshake(&self.conn).await;
        self.registry.lock().unwrap().remove(&self.key);
    }

    /// Take the stream of mandatory-unroutable deliveries (basic.return,
    /// methods/publish.rs:213-234). The second call returns a closed
    /// stream. Messages returned while a `publish_mandatory` /
    /// `publish_confirmed` call is awaiting their own correlation do NOT
    /// appear here (embedded design §3).
    pub fn take_returns(&self) -> mpsc::UnboundedReceiver<ReturnedMessage> {
        let mut slot = self.returns_rx.lock().unwrap();
        slot.take().unwrap_or_else(|| {
            // Already taken: a channel whose sender was dropped — recv()
            // returns None immediately.
            let (tx, rx) = mpsc::unbounded_channel();
            drop(tx);
            rx
        })
    }
}

impl Debug for EmbeddedConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("EmbeddedConnection")
            .field("open", &self.conn.valid.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}
