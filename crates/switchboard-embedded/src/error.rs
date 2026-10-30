//! The crate-owned error enums. Broker internals (ClusterNode, Channel,
//! BrokerError, ...) never appear in a public signature — these five types
//! are the entire error surface, and their variant sets are frozen for 1.0
//! (embedded design §3, §9/R7).

use std::fmt;

/// Failure to create and start a [`crate::Broker`].
///
/// Discrimination:
/// * [`StartError::Storage`] — the RocksDB lock on `data_dir` is held
///   (another live broker on the same directory, same or different
///   process). The restart contract is "new process, same `data_dir`";
///   this error is its in-process negative case (§5.3).
/// * [`StartError::Formation`] — cluster formation or the post-readiness
///   bootstrap write failed.
/// * [`StartError::InvalidConfig`] — a statically-invalid configuration
///   (e.g. `bootstrap_credentials("guest", …)`).
#[derive(Debug)]
pub enum StartError {
    /// Binding a listener failed.
    Io(std::io::Error),
    /// The store at `data_dir` could not be opened — almost always the
    /// RocksDB exclusive LOCK held by another live broker. A broker that
    /// returned `Storage` (or `Formation`) leaves the store locked for the
    /// rest of the process; retrying on the same `data_dir` in the same
    /// process cannot succeed (§5.3).
    Storage(String),
    /// Cluster formation, the readiness wait, or the bootstrap-credentials
    /// write failed. The freshly opened store is locked for the rest of the
    /// process either way; restart in a NEW process.
    Formation(String),
    /// The configuration is invalid. The builder methods themselves are
    /// infallible; validation happens here, where the `Result` already
    /// exists.
    InvalidConfig(&'static str),
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StartError::Io(e) => write!(f, "io error: {e}"),
            StartError::Storage(e) => write!(f, "storage: {e}"),
            StartError::Formation(e) => write!(f, "cluster formation failed: {e}"),
            StartError::InvalidConfig(why) => write!(f, "invalid configuration: {why}"),
        }
    }
}

impl std::error::Error for StartError {}

/// Failure of [`crate::Broker::connect`] / [`crate::EmbeddedConnection::open_channel`].
///
/// Discrimination is by STAGE OF HANDSHAKE, not by server reply: every
/// pre-Open-Ok failure path in the server writes ZERO bytes before closing,
/// and the embedded client drives the sequence, so EOF is unambiguous by
/// what the client sent last (embedded design §4.3). If the server ever
/// learns to send explicit pre-Open rejection replies, this mapping table is
/// the single adaptation point — no surface change (§9/R7).
#[derive(Debug)]
pub enum ConnectError {
    /// EOF after `Connection.Start-Ok` was sent: wrong password,
    /// unsupported mechanism, or remote-guest refusal — indistinguishable
    /// by protocol, so they fold into one variant.
    AccessRefused,
    /// EOF after `Connection.Open` was sent (post Tune-Ok): unknown vhost.
    NoSuchVhost(String),
    /// The server went away before the handshake progressed (EOF before
    /// Start-Ok, or the 8-byte protocol-header reject preamble followed by
    /// EOF). Also used when a channel cannot be opened because the
    /// connection is gone.
    Closed,
    /// A transport error (the in-memory duplex, or a handshake timeout).
    Io(std::io::Error),
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectError::AccessRefused => write!(f, "access refused (bad credentials?)"),
            ConnectError::NoSuchVhost(v) => write!(f, "no such vhost {v:?}"),
            ConnectError::Closed => write!(f, "connection closed before the handshake completed"),
            ConnectError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for ConnectError {}

impl From<OpError> for ConnectError {
    fn from(e: OpError) -> Self {
        match e {
            OpError::Io(inner) => ConnectError::Io(inner),
            _ => ConnectError::Closed,
        }
    }
}

/// Failure of one [`crate::EmbeddedChannel`] operation.
///
/// `ChannelClosed` carries the §4.8 close pair and poisons the channel —
/// including all prior clones — while `AccessRefused` / `NotFound` /
/// `PreconditionFailed` are decoded from close codes on the gated
/// operations (403 / 404 / 406).
#[derive(Debug)]
pub enum OpError {
    /// The server closed the channel. The channel is invalid after this —
    /// every clone fails fast with the same stored pair (§4.3).
    ChannelClosed {
        code: u16,
        text: String,
        class_id: u16,
        method_id: u16,
    },
    /// The server closed the whole connection.
    ConnectionClosed { code: u16, text: String },
    /// Decoded from close code 403 on a gated operation. `resource` carries
    /// the server's reply text (it names the refused resource).
    AccessRefused { resource: String },
    /// Decoded from close code 404. `resource` carries the server's reply
    /// text (it names the missing resource).
    NotFound { resource: String },
    /// Decoded from close code 406 (re-declare with different arguments).
    PreconditionFailed { text: String },
    /// A transport failure on the in-memory duplex.
    Io(std::io::Error),
}

impl fmt::Display for OpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpError::ChannelClosed {
                code,
                text,
                class_id,
                method_id,
            } => write!(f, "channel closed {code} ({class_id}/{method_id}): {text}"),
            OpError::ConnectionClosed { code, text } => {
                write!(f, "connection closed {code}: {text}")
            }
            OpError::AccessRefused { resource } => write!(f, "access refused: {resource}"),
            OpError::NotFound { resource } => write!(f, "not found: {resource}"),
            OpError::PreconditionFailed { text } => write!(f, "precondition failed: {text}"),
            OpError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for OpError {}

/// Failure of a [`crate::Broker`] typed admin call
/// ([`crate::Broker::declare_vhost`] / [`create_user`](crate::Broker::create_user)
/// / [`delete_user`](crate::Broker::delete_user) /
/// [`set_permissions`](crate::Broker::set_permissions)).
///
/// There is deliberately NO "no such user" variant: both user commands
/// reply Ok unconditionally in the meta state machine (`DeleteUser` removes
/// unconditionally, `CreateUser` upserts), so that failure mode cannot
/// occur (embedded design §3).
#[derive(Debug)]
pub enum AdminError {
    /// The meta state machine rejected the command (e.g. `set_permissions`
    /// for a user that does not exist).
    Refused(String),
    /// The broker has already been stopped.
    Closed,
    /// The replicated write failed (raft/transport).
    Raft(String),
}

impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdminError::Refused(why) => write!(f, "refused: {why}"),
            AdminError::Closed => write!(f, "broker is stopped"),
            AdminError::Raft(why) => write!(f, "replicated write failed: {why}"),
        }
    }
}

impl std::error::Error for AdminError {}

/// Failure of [`crate::Broker::stop`].
#[derive(Debug)]
pub enum StopError {
    /// `ClusterNode::leave` returned an error. `shutdown()` was still
    /// called explicitly (§5.2 step 3), so no raft core keeps running.
    LeaveFailed(String),
    /// Closing the connections or the bounded `leave` exceeded `grace`.
    /// `shutdown()` was still called explicitly, so the rafts are stopped;
    /// the error signals that the deadline was not met.
    TimedOut,
}

impl fmt::Display for StopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopError::LeaveFailed(why) => write!(f, "leave failed: {why}"),
            StopError::TimedOut => write!(f, "stop exceeded its grace budget"),
        }
    }
}

impl std::error::Error for StopError {}

/// Decode a §4.8 channel-close pair into the operation error the caller
/// sees. 403/404/406 fold into their named variants; everything else stays
/// `ChannelClosed` (embedded design §3).
pub(crate) fn decode_channel_close(
    code: u16,
    text: String,
    class_id: u16,
    method_id: u16,
) -> OpError {
    match code {
        403 => OpError::AccessRefused { resource: text },
        404 => OpError::NotFound { resource: text },
        406 => OpError::PreconditionFailed { text },
        _ => OpError::ChannelClosed {
            code,
            text,
            class_id,
            method_id,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_codes_decode_to_named_variants() {
        assert!(matches!(
            decode_channel_close(403, "no queue".into(), 50, 10),
            OpError::AccessRefused { .. }
        ));
        assert!(matches!(
            decode_channel_close(404, "no queue 'q'".into(), 50, 10),
            OpError::NotFound { .. }
        ));
        assert!(matches!(
            decode_channel_close(406, "inequivalent".into(), 50, 10),
            OpError::PreconditionFailed { .. }
        ));
        let other = decode_channel_close(405, "locked".into(), 50, 10);
        assert!(matches!(other, OpError::ChannelClosed { code: 405, .. }));
    }

    #[test]
    fn op_error_maps_to_connect_error() {
        assert!(matches!(
            ConnectError::from(OpError::ConnectionClosed {
                code: 320,
                text: "x".into()
            }),
            ConnectError::Closed
        ));
    }
}
