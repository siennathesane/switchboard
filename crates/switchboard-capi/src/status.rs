//! The ABI status-code vocabulary and the thread-local last-error message.
//!
//! Contract (see `include/switchboard.h` for the C-facing version):
//! * every entry point returns [`SbStatus`]; `SB_OK` (0) means success;
//! * on any non-`SB_OK` return, `sb_last_error()` on the CALLING thread
//!   yields a NUL-terminated human-readable message for the failure;
//! * the message pointer stays valid until the next `sb_*` call on the
//!   same thread (it lives in a thread-local slot that gets replaced).

use std::cell::RefCell;
use std::ffi::CString;
use std::fmt;

/// The status code every `sb_*` entry point returns. The numeric values
/// are part of the ABI (frozen for 1.0): C callers may switch on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SbStatus {
    /// Success.
    Ok = 0,
    /// A required pointer argument was NULL (or a byte slice was NULL with
    /// a non-zero length).
    ErrNullArg = 1,
    /// A C-string argument was not valid UTF-8 (Switchboard names and
    /// credentials are UTF-8).
    ErrInvalidUtf8 = 2,
    /// The handle was never issued by this library, or has already been
    /// freed (`sb_*_free` / `sb_*_close` bookkeeping). Safe to detect; the
    /// handle was not dereferenced.
    ErrInvalidHandle = 3,
    /// The handle is still allocated but no longer usable: its broker was
    /// stopped/freed, its connection was closed, or its consumer was
    /// cancelled. Free it with the matching `sb_*_free`.
    ErrHandleDead = 4,
    /// The broker has already been stopped (or is stopping).
    ErrBrokerStopped = 5,
    /// `sb_broker_connect*` was refused: wrong password, unsupported
    /// mechanism, or remote-guest refusal.
    ErrConnectAccessRefused = 6,
    /// `sb_broker_connect_vhost` named a vhost that does not exist.
    ErrNoSuchVhost = 7,
    /// The server closed the whole connection.
    ErrConnectionClosed = 8,
    /// The server closed the channel (the channel handle is now dead).
    ErrChannelClosed = 9,
    /// A gated operation was refused by the broker's authorization rules
    /// (close code 403).
    ErrAccessRefused = 10,
    /// A named resource (queue/exchange) does not exist (close code 404).
    ErrNotFound = 11,
    /// Re-declaration with inequivalent arguments, or a tx/confirm mode
    /// switch conflict (close code 406).
    ErrPreconditionFailed = 12,
    /// A transport or internal I/O failure.
    ErrIo = 13,
    /// A broker admin call was rejected by the replicated meta state
    /// machine (e.g. `sb_broker_set_permissions` for a missing user).
    ErrAdminRefused = 14,
    /// A broker admin call's replicated write failed (raft/transport).
    ErrRaft = 15,
    /// `sb_broker_stop` exceeded its grace budget. Teardown still
    /// completed (the node runtime's shutdown fallback ran).
    ErrStopTimedOut = 16,
    /// `sb_broker_stop`'s leave step failed. Teardown still completed.
    ErrStopLeaveFailed = 17,
    /// `sb_broker_start` rejected the configuration (e.g. bootstrap
    /// credentials naming the reserved `guest` user).
    ErrInvalidConfig = 18,
    /// `sb_broker_start` could not open the store: almost always the
    /// RocksDB exclusive LOCK on `data_dir` held by a live broker (the
    /// restart contract is "new process, same data_dir").
    ErrStorage = 19,
    /// `sb_broker_start` failed during cluster formation, the readiness
    /// wait, or the bootstrap write.
    ErrFormation = 20,
    /// A bounded wait (`sb_broker_wait_ready`) ran out of budget.
    ErrTimeout = 21,
    /// The consumer was cancelled (server-side, e.g. queue deleted, or
    /// `sb_consumer_cancel`). The consumer handle is now dead.
    ErrConsumerCancelled = 22,
    /// A Rust panic was caught at the ABI boundary (it did NOT cross).
    ErrPanic = 23,
}

/// Rust-side aliases named exactly like the C enum members, so tests and
/// the header stay word-for-word comparable.
pub const SB_OK: SbStatus = SbStatus::Ok;
pub const SB_ERR_NULL_ARG: SbStatus = SbStatus::ErrNullArg;
pub const SB_ERR_INVALID_UTF8: SbStatus = SbStatus::ErrInvalidUtf8;
pub const SB_ERR_INVALID_HANDLE: SbStatus = SbStatus::ErrInvalidHandle;
pub const SB_ERR_HANDLE_DEAD: SbStatus = SbStatus::ErrHandleDead;
pub const SB_ERR_BROKER_STOPPED: SbStatus = SbStatus::ErrBrokerStopped;
pub const SB_ERR_CONNECT_ACCESS_REFUSED: SbStatus = SbStatus::ErrConnectAccessRefused;
pub const SB_ERR_NO_SUCH_VHOST: SbStatus = SbStatus::ErrNoSuchVhost;
pub const SB_ERR_CONNECTION_CLOSED: SbStatus = SbStatus::ErrConnectionClosed;
pub const SB_ERR_CHANNEL_CLOSED: SbStatus = SbStatus::ErrChannelClosed;
pub const SB_ERR_ACCESS_REFUSED: SbStatus = SbStatus::ErrAccessRefused;
pub const SB_ERR_NOT_FOUND: SbStatus = SbStatus::ErrNotFound;
pub const SB_ERR_PRECONDITION_FAILED: SbStatus = SbStatus::ErrPreconditionFailed;
pub const SB_ERR_IO: SbStatus = SbStatus::ErrIo;
pub const SB_ERR_ADMIN_REFUSED: SbStatus = SbStatus::ErrAdminRefused;
pub const SB_ERR_RAFT: SbStatus = SbStatus::ErrRaft;
pub const SB_ERR_STOP_TIMED_OUT: SbStatus = SbStatus::ErrStopTimedOut;
pub const SB_ERR_STOP_LEAVE_FAILED: SbStatus = SbStatus::ErrStopLeaveFailed;
pub const SB_ERR_INVALID_CONFIG: SbStatus = SbStatus::ErrInvalidConfig;
pub const SB_ERR_STORAGE: SbStatus = SbStatus::ErrStorage;
pub const SB_ERR_FORMATION: SbStatus = SbStatus::ErrFormation;
pub const SB_ERR_TIMEOUT: SbStatus = SbStatus::ErrTimeout;
pub const SB_ERR_CONSUMER_CANCELLED: SbStatus = SbStatus::ErrConsumerCancelled;
pub const SB_ERR_PANIC: SbStatus = SbStatus::ErrPanic;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Pointer to the thread-local last-error message, or NULL when this
/// thread has never seen a failure. The pointer stays valid until the
/// next error is recorded on this thread.
pub(crate) fn last_error_ptr() -> *const std::ffi::c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|c| c.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// Record the thread-local last-error message (already NUL-free — it is a
/// Rust `String` truncated at the first NUL when marshalled).
pub(crate) fn record_error(msg: String) {
    let truncated: String = msg.chars().take_while(|&c| c != '\0').collect();
    LAST_ERROR.with(|slot| {
        if let Ok(c) = CString::new(truncated) {
            *slot.borrow_mut() = Some(c);
        }
    });
}

/// Set the thread-local error message and return `status` — the standard
/// failure path of every entry point.
pub(crate) fn fail(status: SbStatus, msg: impl fmt::Display) -> SbStatus {
    record_error(format!("{}: {msg}", status_name(status)));
    status
}

/// Run `f` inside `catch_unwind`: a Rust panic becomes `SB_ERR_PANIC` plus
/// a thread-local message instead of crossing the C boundary. This is
/// applied by EVERY entry point (panic containment is a documented ABI
/// guarantee).
pub(crate) fn entry<T>(f: impl FnOnce() -> Result<T, SbStatus>) -> Result<T, SbStatus> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "opaque panic payload".to_string()
            };
            Err(fail(
                SB_ERR_PANIC,
                format_args!("panic contained at the C ABI boundary: {msg}"),
            ))
        }
    }
}

/// The status-returning flavor of [`entry`]: what every
/// `pub extern "C" fn` body wraps.
pub(crate) fn entry_status(f: impl FnOnce() -> Result<(), SbStatus>) -> SbStatus {
    match entry(f) {
        Ok(()) => SB_OK,
        Err(status) => status,
    }
}

/// Human-readable name for a status (used to prefix last-error messages).
pub(crate) fn status_name(status: SbStatus) -> &'static str {
    match status {
        SbStatus::Ok => "SB_OK",
        SbStatus::ErrNullArg => "SB_ERR_NULL_ARG",
        SbStatus::ErrInvalidUtf8 => "SB_ERR_INVALID_UTF8",
        SbStatus::ErrInvalidHandle => "SB_ERR_INVALID_HANDLE",
        SbStatus::ErrHandleDead => "SB_ERR_HANDLE_DEAD",
        SbStatus::ErrBrokerStopped => "SB_ERR_BROKER_STOPPED",
        SbStatus::ErrConnectAccessRefused => "SB_ERR_CONNECT_ACCESS_REFUSED",
        SbStatus::ErrNoSuchVhost => "SB_ERR_NO_SUCH_VHOST",
        SbStatus::ErrConnectionClosed => "SB_ERR_CONNECTION_CLOSED",
        SbStatus::ErrChannelClosed => "SB_ERR_CHANNEL_CLOSED",
        SbStatus::ErrAccessRefused => "SB_ERR_ACCESS_REFUSED",
        SbStatus::ErrNotFound => "SB_ERR_NOT_FOUND",
        SbStatus::ErrPreconditionFailed => "SB_ERR_PRECONDITION_FAILED",
        SbStatus::ErrIo => "SB_ERR_IO",
        SbStatus::ErrAdminRefused => "SB_ERR_ADMIN_REFUSED",
        SbStatus::ErrRaft => "SB_ERR_RAFT",
        SbStatus::ErrStopTimedOut => "SB_ERR_STOP_TIMED_OUT",
        SbStatus::ErrStopLeaveFailed => "SB_ERR_STOP_LEAVE_FAILED",
        SbStatus::ErrInvalidConfig => "SB_ERR_INVALID_CONFIG",
        SbStatus::ErrStorage => "SB_ERR_STORAGE",
        SbStatus::ErrFormation => "SB_ERR_FORMATION",
        SbStatus::ErrTimeout => "SB_ERR_TIMEOUT",
        SbStatus::ErrConsumerCancelled => "SB_ERR_CONSUMER_CANCELLED",
        SbStatus::ErrPanic => "SB_ERR_PANIC",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_values_match_the_frozen_header() {
        assert_eq!(SB_OK as i32, 0);
        assert_eq!(SB_ERR_NULL_ARG as i32, 1);
        assert_eq!(SB_ERR_INVALID_HANDLE as i32, 3);
        assert_eq!(SB_ERR_HANDLE_DEAD as i32, 4);
        assert_eq!(SB_ERR_PANIC as i32, 23);
    }

    #[test]
    fn last_error_is_replaced_per_thread() {
        record_error("first".into());
        record_error("second".into());
        LAST_ERROR.with(|slot| {
            let s = slot
                .borrow()
                .as_ref()
                .unwrap()
                .to_string_lossy()
                .to_string();
            assert_eq!(s, "second");
        });
    }
}
