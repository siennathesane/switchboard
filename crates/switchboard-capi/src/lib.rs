//! # switchboard-capi
//!
//! A stable C ABI over [`switchboard_embedded`], Switchboard's in-process
//! broker + typed AMQP 0-9-1 client, so non-Rust programs (Python via
//! ctypes, Go via cgo, C/C++, ...) can embed the broker in-process.
//!
//! ## Shape of the ABI
//!
//! * **Symbol prefix**: every exported symbol starts `sb_`.
//! * **Status codes**: every entry point returns `sb_status`; `SB_OK` (0)
//!   is success. The numeric values are frozen for 1.0.
//! * **Last error**: after any failure, `sb_last_error()` on the CALLING
//!   thread returns a NUL-terminated message; the pointer is valid until
//!   the next `sb_*` call on that thread (it lives in a thread-local slot).
//! * **Panic containment**: every entry point runs under
//!   `std::panic::catch_unwind` — no Rust panic ever crosses the boundary;
//!   a caught panic surfaces as `SB_ERR_PANIC` plus a last-error message.
//! * **Explicit lengths**: message bodies travel as
//!   `(const uint8_t* ptr, size_t len)` pairs; names/credentials are
//!   NUL-terminated UTF-8 C strings (invalid UTF-8 →
//!   `SB_ERR_INVALID_UTF8`).
//! * **Owned returns**: returned strings and messages are allocated by
//!   this library and released with `sb_string_free` / `sb_message_free`.
//!   Every handle comes from a create/start/connect/open/consume call and
//!   is released by exactly one matching free.
//! * **Handle validity**: freed-then-used → `SB_ERR_INVALID_HANDLE`;
//!   alive-but-gone (broker stopped, connection closed, consumer
//!   cancelled) → `SB_ERR_HANDLE_DEAD`. Double-free is detected, not UB —
//!   a global handle table backs every check. A garbage pointer is never
//!   dereferenced.
//!
//! ## Consume model (documented choice)
//!
//! This ABI exposes consumption as POLLING only:
//! * `sb_channel_get` — one non-blocking pull (Basic.Get / Get-Empty);
//! * `sb_channel_consume` + `sb_consumer_next` — a push consumer drained
//!   synchronously with a timeout.
//!
//! There is deliberately NO callback-with-user_data flavor: no Rust
//! future, thread, or panic inside this library ever invokes embedder
//! code, so no callback reentrancy/thread-affinity contract exists.
//!
//! ## Threading
//!
//! Every `sb_*` call is SYNCHRONOUS: it blocks the calling OS thread for
//! the duration of the underlying async op (`Handle::block_on` into the
//! broker's private multi-thread tokio runtime — the embedder creates no
//! runtime). Distinct handles may be used concurrently from distinct
//! threads; sharing one handle across threads is safe (the library
//! serializes where the Rust API requires `&mut`, e.g. consumer polling).
//! Do NOT call `sb_*` from inside a running async context — that is the
//! one thing `block_on` cannot do (it panics, and the panic is contained
//! as `SB_ERR_PANIC`, but the call still fails).
//!
//! ## Lifecycle
//!
//! `sb_config_create` → (`sb_config_set_*`) → `sb_broker_start` →
//! `sb_broker_connect` → `sb_client_open_channel` → ops →
//! `sb_client_close` → `sb_broker_stop` → frees. One live broker per
//! `data_dir` per PROCESS (RocksDB LOCK); restart means a new process.
//!
//! The embedded crate's known caveats carry over verbatim: see
//! `sb_config_set_janitor_interval_ms` (tx-commit/tick race) and the
//! embedded crate docs (restart contract, authorization parity, §9/R1
//! prefetch guidance). One further pre-existing broker gap was found while
//! testing this ABI: the session demux completes content only in its
//! Body-frame arm (`switchboard_server::session` — `take_content` is
//! called from the `FrameType::Body` branch), but a `body_size == 0`
//! publish carries NO body frame on the wire, so EMPTY-body messages are
//! silently never applied by the broker (TCP clients included). This ABI
//! accepts `body == NULL, body_len == 0` as the empty body (documented),
//! but embedders should not rely on empty-body DELIVERY until a
//! `fix(broker):` change lands.

#![deny(rustdoc::broken_intra_doc_links)]

mod broker_api;
mod channel_api;
mod client_api;
mod consumer_api;
mod handles;
mod map;
mod message;
mod status;
mod table;

pub use channel_api::SB_EXCHANGE_DIRECT;
pub use channel_api::SB_EXCHANGE_FANOUT;
pub use channel_api::SB_EXCHANGE_HEADERS;
pub use channel_api::SB_EXCHANGE_TOPIC;
// The extern fns themselves, re-exported so Rust callers (and this
// crate's own tests) can `use switchboard_capi::*` and reach the exact
// surface the cdylib exports.
pub use broker_api::sb_broker_connect;
pub use broker_api::sb_broker_connect_vhost;
pub use broker_api::sb_broker_create_user;
pub use broker_api::sb_broker_data_dir;
pub use broker_api::sb_broker_declare_vhost;
pub use broker_api::sb_broker_delete_user;
pub use broker_api::sb_broker_free;
pub use broker_api::sb_broker_is_ready;
pub use broker_api::sb_broker_set_permissions;
pub use broker_api::sb_broker_start;
pub use broker_api::sb_broker_stop;
pub use broker_api::sb_broker_wait_ready;
pub use broker_api::sb_config_create;
pub use broker_api::sb_config_free;
pub use broker_api::sb_config_set_bootstrap_credentials;
pub use broker_api::sb_config_set_expected_nodes;
pub use broker_api::sb_config_set_janitor_interval_ms;
pub use broker_api::sb_config_set_node_id;
pub use broker_api::sb_string_free;
pub use channel_api::sb_channel_ack;
pub use channel_api::sb_channel_ack_many;
pub use channel_api::sb_channel_cancel;
pub use channel_api::sb_channel_confirm_select;
pub use channel_api::sb_channel_consume;
pub use channel_api::sb_channel_exchange_bind;
pub use channel_api::sb_channel_exchange_declare;
pub use channel_api::sb_channel_exchange_delete;
pub use channel_api::sb_channel_exchange_unbind;
pub use channel_api::sb_channel_free;
pub use channel_api::sb_channel_get;
pub use channel_api::sb_channel_id;
pub use channel_api::sb_channel_nack;
pub use channel_api::sb_channel_nack_many;
pub use channel_api::sb_channel_publish;
pub use channel_api::sb_channel_publish_confirmed;
pub use channel_api::sb_channel_publish_mandatory;
pub use channel_api::sb_channel_qos;
pub use channel_api::sb_channel_queue_bind;
pub use channel_api::sb_channel_queue_declare;
pub use channel_api::sb_channel_queue_delete;
pub use channel_api::sb_channel_queue_purge;
pub use channel_api::sb_channel_queue_unbind;
pub use channel_api::sb_channel_tx_commit;
pub use channel_api::sb_channel_tx_rollback;
pub use channel_api::sb_channel_tx_select;
pub use client_api::sb_client_close;
pub use client_api::sb_client_free;
pub use client_api::sb_client_open_channel;
pub use consumer_api::sb_consumer_cancel;
pub use consumer_api::sb_consumer_free;
pub use consumer_api::sb_consumer_next;
pub use consumer_api::sb_consumer_tag;
// The opaque handle types (C: sb_config_t / sb_broker_t / sb_client_t /
// sb_channel_t / sb_consumer_t) and the message struct, so Rust-side
// tests can declare pointers to them.
pub use consumer_api::SbConsumer;
pub use handles::SbBroker;
pub use handles::SbChannel;
pub use handles::SbClient;
pub use handles::SbConfig;
pub use message::SbMessage;
pub use status::SB_OK;
pub use status::SbStatus;
// The full status vocabulary is exported under its C names.
pub use status::SB_ERR_ACCESS_REFUSED;
pub use status::SB_ERR_ADMIN_REFUSED;
pub use status::SB_ERR_BROKER_STOPPED;
pub use status::SB_ERR_CHANNEL_CLOSED;
pub use status::SB_ERR_CONNECT_ACCESS_REFUSED;
pub use status::SB_ERR_CONNECTION_CLOSED;
pub use status::SB_ERR_CONSUMER_CANCELLED;
pub use status::SB_ERR_FORMATION;
pub use status::SB_ERR_HANDLE_DEAD;
pub use status::SB_ERR_INVALID_CONFIG;
pub use status::SB_ERR_INVALID_HANDLE;
pub use status::SB_ERR_INVALID_UTF8;
pub use status::SB_ERR_IO;
pub use status::SB_ERR_NO_SUCH_VHOST;
pub use status::SB_ERR_NOT_FOUND;
pub use status::SB_ERR_NULL_ARG;
pub use status::SB_ERR_PANIC;
pub use status::SB_ERR_PRECONDITION_FAILED;
pub use status::SB_ERR_RAFT;
pub use status::SB_ERR_STOP_LEAVE_FAILED;
pub use status::SB_ERR_STOP_TIMED_OUT;
pub use status::SB_ERR_STORAGE;
pub use status::SB_ERR_TIMEOUT;

use std::ffi::CStr;
use std::ffi::c_char;

/// The library version (the workspace version, e.g. `"1.0.0-rc.1"`), as a
/// static NUL-terminated string that never needs freeing.
///
/// # Safety
/// Nothing to dereference — no arguments.
#[unsafe(no_mangle)]
pub extern "C" fn sb_version_string() -> *const c_char {
    unsafe {
        CStr::from_bytes_with_nul_unchecked(concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes())
            .as_ptr()
    }
}

/// The last error message on the CALLING thread (set by any failed
/// `sb_*` call), or NULL when this thread has never seen a failure.
/// The pointer stays valid until the next `sb_*` call on this thread.
///
/// # Safety
/// Nothing to dereference — no arguments.
#[unsafe(no_mangle)]
pub extern "C" fn sb_last_error() -> *const c_char {
    status::last_error_ptr()
}

/// Free one owned `sb_message_t` filled by `sb_channel_get` /
/// `sb_consumer_next` (its strings and body buffer). Tolerates NULL and a
/// zeroed struct (what failed calls leave behind); the struct is re-zeroed
/// so freeing twice is a safe no-op.
///
/// # Safety
/// `message` must be NULL, zeroed, or a struct this library filled.
#[unsafe(no_mangle)]
pub extern "C" fn sb_message_free(message: *mut SbMessage) -> SbStatus {
    status::entry_status(|| {
        if message.is_null() {
            return Ok(());
        }
        // SAFETY: the caller owns the struct per the documented contract;
        // every field is either NULL or memory this library allocated.
        unsafe {
            if !(*message).exchange.is_null() {
                drop(std::ffi::CString::from_raw((*message).exchange));
            }
            if !(*message).routing_key.is_null() {
                drop(std::ffi::CString::from_raw((*message).routing_key));
            }
            if !(*message).consumer_tag.is_null() {
                drop(std::ffi::CString::from_raw((*message).consumer_tag));
            }
            if !(*message).content_type.is_null() {
                drop(std::ffi::CString::from_raw((*message).content_type));
            }
            if !(*message).body.is_null() {
                let slice_ptr =
                    std::ptr::slice_from_raw_parts_mut((*message).body, (*message).body_len);
                drop(Box::from_raw(slice_ptr));
            }
            *message = SbMessage {
                delivery_tag: 0,
                redelivered: 0,
                exchange: std::ptr::null_mut(),
                routing_key: std::ptr::null_mut(),
                consumer_tag: std::ptr::null_mut(),
                content_type: std::ptr::null_mut(),
                body: std::ptr::null_mut(),
                body_len: 0,
            };
        }
        Ok(())
    })
}

/// TEST-ONLY: raise a Rust panic inside the ABI boundary to prove the
/// catch_unwind containment works (status `SB_ERR_PANIC`, message in
/// `sb_last_error`, process alive). Never call from production code.
///
/// # Safety
/// Nothing to dereference — no arguments.
#[unsafe(no_mangle)]
#[doc(hidden)]
pub extern "C" fn sb_test_force_panic() -> SbStatus {
    status::entry_status(|| {
        panic!("sb_test_force_panic: intentional test panic");
    })
}
