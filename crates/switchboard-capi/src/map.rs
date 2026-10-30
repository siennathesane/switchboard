//! Mapping the embedded crate's five frozen error enums onto `sb_status`
//! values (+ the thread-local message). The variant sets are frozen for
//! 1.0 by the embedded design (§3), so this table is stable.

use switchboard_embedded::{AdminError, ConnectError, OpError, StartError, StopError};

use crate::status::{
    SB_ERR_ACCESS_REFUSED, SB_ERR_ADMIN_REFUSED, SB_ERR_BROKER_STOPPED, SB_ERR_CHANNEL_CLOSED,
    SB_ERR_CONNECT_ACCESS_REFUSED, SB_ERR_CONNECTION_CLOSED, SB_ERR_FORMATION,
    SB_ERR_INVALID_CONFIG, SB_ERR_INVALID_HANDLE, SB_ERR_IO, SB_ERR_NO_SUCH_VHOST,
    SB_ERR_NOT_FOUND, SB_ERR_PRECONDITION_FAILED, SB_ERR_RAFT, SB_ERR_STOP_LEAVE_FAILED,
    SB_ERR_STOP_TIMED_OUT, SB_ERR_STORAGE, SbStatus, fail,
};

pub(crate) fn start_status(e: StartError) -> SbStatus {
    match &e {
        StartError::Io(_) => fail(SB_ERR_IO, format_args!("broker start failed: {e}")),
        StartError::Storage(_) => fail(SB_ERR_STORAGE, format_args!("broker start failed: {e}")),
        StartError::Formation(_) => {
            fail(SB_ERR_FORMATION, format_args!("broker start failed: {e}"))
        }
        StartError::InvalidConfig(_) => fail(
            SB_ERR_INVALID_CONFIG,
            format_args!("broker start failed: {e}"),
        ),
    }
}

pub(crate) fn connect_status(e: ConnectError) -> SbStatus {
    match &e {
        ConnectError::AccessRefused => fail(
            SB_ERR_CONNECT_ACCESS_REFUSED,
            format_args!("connect failed: {e}"),
        ),
        ConnectError::NoSuchVhost(_) => {
            fail(SB_ERR_NO_SUCH_VHOST, format_args!("connect failed: {e}"))
        }
        ConnectError::Closed => fail(
            SB_ERR_CONNECTION_CLOSED,
            format_args!("connect/open-channel failed: {e}"),
        ),
        ConnectError::Io(_) => fail(SB_ERR_IO, format_args!("connect failed: {e}")),
    }
}

pub(crate) fn op_status(e: OpError) -> SbStatus {
    match &e {
        OpError::ChannelClosed { .. } => fail(
            SB_ERR_CHANNEL_CLOSED,
            format_args!("the server closed the channel (the handle is now dead): {e}"),
        ),
        OpError::ConnectionClosed { .. } => fail(
            SB_ERR_CONNECTION_CLOSED,
            format_args!("the server closed the connection: {e}"),
        ),
        OpError::AccessRefused { .. } => fail(
            SB_ERR_ACCESS_REFUSED,
            format_args!("operation refused: {e}"),
        ),
        OpError::NotFound { .. } => fail(SB_ERR_NOT_FOUND, format_args!("operation failed: {e}")),
        OpError::PreconditionFailed { .. } => fail(
            SB_ERR_PRECONDITION_FAILED,
            format_args!("operation failed: {e}"),
        ),
        OpError::Io(_) => fail(SB_ERR_IO, format_args!("operation failed: {e}")),
    }
}

pub(crate) fn admin_status(e: AdminError) -> SbStatus {
    match &e {
        AdminError::Refused(_) => {
            fail(SB_ERR_ADMIN_REFUSED, format_args!("admin call failed: {e}"))
        }
        AdminError::Closed => fail(
            SB_ERR_BROKER_STOPPED,
            format_args!("admin call failed: {e}"),
        ),
        AdminError::Raft(_) => fail(SB_ERR_RAFT, format_args!("admin call failed: {e}")),
    }
}

pub(crate) fn stop_status(e: StopError) -> SbStatus {
    match &e {
        StopError::TimedOut => fail(SB_ERR_STOP_TIMED_OUT, format_args!("broker stop: {e}")),
        StopError::LeaveFailed(_) => {
            fail(SB_ERR_STOP_LEAVE_FAILED, format_args!("broker stop: {e}"))
        }
    }
}

/// A connection/consumer op that found the handle's embedded value already
/// taken (close/cancel ran first) — a dead-handle variant that slipped past
/// the table (should be unreachable; mapped defensively).
pub(crate) fn already_gone(what: &str) -> SbStatus {
    fail(
        SB_ERR_INVALID_HANDLE,
        format_args!("{what} was already closed or cancelled on this handle"),
    )
}
