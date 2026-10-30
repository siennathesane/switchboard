//! Message marshalling: the `sb_message_t` layout, owned-buffer
//! allocation for returned strings/bodies, and the explicit free fn.

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::slice::from_raw_parts;

use switchboard_embedded::Delivery;
use switchboard_embedded::ReturnedMessage;

/// A delivered message, marshalled into C-owned memory. Returned by
/// `sb_channel_get` and `sb_consumer_next`.
///
/// The caller frees the whole struct with `sb_message_free` (which
/// tolerates a zeroed struct, so "call free even when nothing was filled"
/// is always safe). On a failed call the library zeroes `*out` first, so
/// freeing it afterwards is a no-op.
#[repr(C)]
pub struct SbMessage {
    /// Channel-scoped delivery tag used for `sb_channel_ack`/`nack`.
    pub delivery_tag: u64,
    /// 1 when this is a redelivery.
    pub redelivered: i32,
    /// The exchange the publisher addressed (owned; never NULL, "" when
    /// the default exchange).
    pub exchange: *mut c_char,
    /// The routing key the publisher used (owned; never NULL).
    pub routing_key: *mut c_char,
    /// The delivering consumer's tag; "" for `sb_channel_get` (owned).
    pub consumer_tag: *mut c_char,
    /// The content-type property; NULL when the publisher did not set one
    /// (owned).
    pub content_type: *mut c_char,
    /// The opaque body octets (owned; NULL only when `body_len == 0`).
    pub body: *mut u8,
    /// Length of `body` in octets.
    pub body_len: usize,
}

impl SbMessage {
    /// Zero everything, then fill from a Rust `Delivery`. Body and strings
    /// are COPIES into freshly allocated C-owned memory; the Rust value
    /// stays owned by the caller.
    pub(crate) fn fill_from_delivery(out: *mut SbMessage, d: &Delivery) {
        unsafe {
            (*out) = SbMessage {
                delivery_tag: 0,
                redelivered: 0,
                exchange: std::ptr::null_mut(),
                routing_key: std::ptr::null_mut(),
                consumer_tag: std::ptr::null_mut(),
                content_type: std::ptr::null_mut(),
                body: std::ptr::null_mut(),
                body_len: 0,
            };
            (*out).delivery_tag = d.delivery_tag;
            (*out).redelivered = d.redelivered as i32;
            (*out).exchange = alloc_string(&d.exchange);
            (*out).routing_key = alloc_string(&d.routing_key);
            (*out).consumer_tag = alloc_string(&d.consumer_tag);
            if let Some(ct) = d.properties.content_type.as_deref() {
                (*out).content_type = alloc_string(ct);
            }
            let boxed = d.body.clone().into_boxed_slice();
            (*out).body_len = boxed.len();
            (*out).body = Box::into_raw(boxed).cast::<u8>();
        }
    }

    /// Fill from a returned (mandatory-unroutable) message: there is no
    /// delivery tag; `consumer_tag` stays empty.
    #[allow(dead_code)] // reserved for the returns-stream ABI surface
    pub(crate) fn fill_from_return(out: *mut SbMessage, r: &ReturnedMessage) {
        let d = Delivery {
            consumer_tag: String::new(),
            delivery_tag: 0,
            redelivered: false,
            exchange: r.exchange.clone(),
            routing_key: r.routing_key.clone(),
            properties: r.properties.clone(),
            body: r.body.clone(),
        };
        Self::fill_from_delivery(out, &d);
    }
}

/// Copy a Rust `str` into a C-owned NUL-terminated string. Interior NULs
/// cannot survive the wire string formats in practice; if one ever does,
/// the copy is truncated at it (documented in the header).
pub(crate) fn alloc_string(s: &str) -> *mut c_char {
    let truncated: String = s.chars().take_while(|&c| c != '\0').collect();
    match CString::new(truncated) {
        Ok(c) => c.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Validate a required C-string argument and copy it to UTF-8.
///
/// # Safety
/// `p` must be NULL or point to a valid NUL-terminated C string (the same
/// precondition every C library string argument has).
pub(crate) unsafe fn cstr(arg: &str, p: *const c_char) -> Result<String, crate::status::SbStatus> {
    if p.is_null() {
        return Err(crate::status::fail(
            crate::status::SB_ERR_NULL_ARG,
            format_args!("{arg} must not be NULL"),
        ));
    }
    let s = unsafe { CStr::from_ptr(p) };
    match s.to_str() {
        Ok(s) => Ok(s.to_owned()),
        Err(_) => Err(crate::status::fail(
            crate::status::SB_ERR_INVALID_UTF8,
            format_args!("{arg} is not valid UTF-8"),
        )),
    }
}

/// Validate an optional C-string argument (NULL = absent).
///
/// # Safety
/// Same precondition as [`cstr`].
pub(crate) unsafe fn opt_cstr(
    arg: &str,
    p: *const c_char,
) -> Result<Option<String>, crate::status::SbStatus> {
    if p.is_null() {
        return Ok(None);
    }
    let s = unsafe { cstr(arg, p) };
    s.map(Some)
}

/// Borrow a caller byte slice. `NULL` with `len == 0` is the empty slice;
/// `NULL` with `len > 0` is rejected.
///
/// # Safety
/// When non-null, `ptr` must point to `len` readable octets.
pub(crate) unsafe fn bytes<'a>(
    arg: &str,
    ptr: *const u8,
    len: usize,
) -> Result<&'a [u8], crate::status::SbStatus> {
    if ptr.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(crate::status::fail(
            crate::status::SB_ERR_NULL_ARG,
            format_args!("{arg} is NULL but {arg}_len is {len}"),
        ));
    }
    Ok(unsafe { from_raw_parts(ptr, len) })
}
