//! `sb_consumer_*`: polling a push consumer's stream, cancellation, free.
//!
//! The C ABI's consume model is POLLING by design: no Rust future,
//! closure, or thread ever invokes C code the embedder registered, so
//! there is no callback-reentrancy or thread-affinity contract to uphold.
//! A consumer's deliveries are drained synchronously with
//! `sb_consumer_next`, which parks the calling thread for up to
//! `timeout_ms`. `sb_channel_get` is the non-blocking alternative.

use std::ffi::c_char;
use std::ffi::c_int;
use std::sync::Mutex;
use std::time::Duration;

use switchboard_embedded::Consumer;
use tokio::runtime::Handle;

use crate::handles::as_consumer;
use crate::map::op_status;
use crate::message::SbMessage;
use crate::message::alloc_string;
use crate::status::SB_ERR_CONSUMER_CANCELLED;
use crate::status::SB_ERR_NULL_ARG;
use crate::status::SbStatus;
use crate::status::entry_status;
use crate::status::fail;
use crate::table::Kind;
use crate::table::{self};

/// One active push consumer (C side of `sb_consumer_t`).
pub struct SbConsumer {
    pub(crate) rt: Handle,
    pub(crate) tag: String,
    /// `Consumer::next` needs `&mut`, and C callers may share the handle
    /// across threads, so the consumer sits behind a mutex: concurrent
    /// `sb_consumer_next` calls serialize instead of racing. `None` once
    /// cancelled (by either side) — None-ness IS the cancelled flag.
    pub(crate) consumer: Mutex<Option<Consumer>>,
}

/// Block up to `timeout_ms` for the consumer's next delivery.
///
/// Outcomes:
/// * `SB_OK` with `*out_has_message = 1`: `*out` holds an owned
///   `sb_message_t` (free with `sb_message_free`);
/// * `SB_OK` with `*out_has_message = 0`: nothing arrived within the
///   budget — the consumer is still live, just idle;
/// * `SB_ERR_CONSUMER_CANCELLED`: the consumer was cancelled (its queue
///   was deleted, `sb_consumer_cancel` ran, or the connection died). The
///   handle is now DEAD.
///
/// # Safety
/// `consumer` must be a live handle; `out_has_message`/`out` writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_consumer_next(
    consumer: *mut SbConsumer,
    timeout_ms: u32,
    out_has_message: *mut c_int,
    out: *mut SbMessage,
) -> SbStatus {
    entry_status(|| {
        if out_has_message.is_null() || out.is_null() {
            return Err(fail(
                SB_ERR_NULL_ARG,
                format_args!("out_has_message and out must not be NULL"),
            ));
        }
        // Zero the out-struct FIRST: on any failure below (timeout,
        // cancellation) the caller may still sb_message_free it.
        unsafe { *out = std::mem::zeroed() };
        unsafe { *out_has_message = 0 };
        let c = as_consumer(consumer)?;
        let mut slot = c.consumer.lock().unwrap_or_else(|p| p.into_inner());
        let Some(consumer_val) = slot.as_mut() else {
            unsafe { *out = std::mem::zeroed() };
            unsafe { *out_has_message = 0 };
            return Err(SB_ERR_CONSUMER_CANCELLED);
        };
        let waited = c.rt.block_on(async {
            tokio::time::timeout(
                Duration::from_millis(timeout_ms as u64),
                consumer_val.next(),
            )
            .await
        });
        match waited {
            Ok(Some(delivery)) => {
                SbMessage::fill_from_delivery(out, &delivery);
                unsafe { *out_has_message = 1 };
                Ok(())
            }
            Ok(None) => {
                // Server-side cancellation or a gone connection: the
                // consumer stream is over; the handle is dead from here on.
                *slot = None;
                drop(slot);
                table::mark_dead(consumer as usize);
                unsafe { *out = std::mem::zeroed() };
                unsafe { *out_has_message = 0 };
                Err(SB_ERR_CONSUMER_CANCELLED)
            }
            Err(_elapsed) => {
                // Timeout: consumer stays live and parked for the next call.
                unsafe { *out = std::mem::zeroed() };
                unsafe { *out_has_message = 0 };
                Ok(())
            }
        }
    })
}

/// Copy this consumer's tag (the server-generated `ct-N` when none was
/// requested) into an owned string.
///
/// # Safety
/// `consumer` live; `out` writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_consumer_tag(consumer: *mut SbConsumer, out: *mut *mut c_char) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let c = as_consumer(consumer)?;
        unsafe { *out = alloc_string(&c.tag) };
        Ok(())
    })
}

/// Cancel this consumer (Basic.Cancel/Cancel-Ok) and mark the handle dead.
/// The consumer is still owned by the caller: release it with
/// `sb_consumer_free`. Cancelling an already-dead consumer reports
/// `SB_ERR_CONSUMER_CANCELLED`.
///
/// # Safety
/// `consumer` must be NULL or a live handle issued by this library.
#[unsafe(no_mangle)]
pub extern "C" fn sb_consumer_cancel(consumer: *mut SbConsumer) -> SbStatus {
    entry_status(|| {
        if consumer.is_null() {
            return Err(fail(
                SB_ERR_NULL_ARG,
                format_args!("consumer must not be NULL"),
            ));
        }
        let c = as_consumer(consumer)?;
        let taken = c.consumer.lock().unwrap_or_else(|p| p.into_inner()).take();
        match taken {
            None => {
                // Already cancelled (either side); the handle is dead.
                table::mark_dead(consumer as usize);
                Err(SB_ERR_CONSUMER_CANCELLED)
            }
            Some(consumer_val) => {
                // `cancel(self)` consumes the Rust consumer whichever way
                // the Basic.Cancel goes, so the handle is dead either way.
                let res = c.rt.block_on(consumer_val.cancel());
                table::mark_dead(consumer as usize);
                res.map_err(op_status)?;
                Ok(())
            }
        }
    })
}

/// Free a consumer handle. Passing NULL is a no-op; double-free is
/// DETECTED (`SB_ERR_INVALID_HANDLE`), not UB. Freeing a cancelled (dead)
/// consumer is the normal release path.
///
/// # Safety
/// `consumer` must be NULL or a handle issued by this library that has not
/// been freed yet.
#[unsafe(no_mangle)]
pub extern "C" fn sb_consumer_free(consumer: *mut SbConsumer) -> SbStatus {
    entry_status(|| {
        if consumer.is_null() {
            return Ok(());
        }
        if !table::is_registered(consumer as usize, Kind::Consumer) {
            return Err(fail(
                SbStatus::ErrInvalidHandle,
                format_args!("consumer was already freed (or never issued by this library)"),
            ));
        }
        table::unregister(consumer as usize);
        drop(unsafe { Box::from_raw(consumer) });
        Ok(())
    })
}
