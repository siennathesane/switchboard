//! `sb_channel_*`: queue/exchange topology, publish paths, polling get,
//! ack/nack/qos, consumer creation, cancellation, and transactions.

use std::ffi::{c_char, c_int};

use switchboard_embedded::{
    ConsumeOptions, ExchangeDeleteOptions, ExchangeKind, ExchangeOptions, MessageProperties,
    QueueDeleteOptions, QueueOptions,
};

use crate::handles::{SbChannel, as_channel};
use crate::map::op_status;
use crate::message::{SbMessage, alloc_string, bytes, cstr, opt_cstr};
use crate::status::{SB_ERR_NULL_ARG, SbStatus, entry_status, fail};
use crate::table::{self, Kind};

/// Exchange-kind argument values for `sb_channel_exchange_declare` (part
/// of the ABI; frozen).
pub const SB_EXCHANGE_DIRECT: i32 = 0;
pub const SB_EXCHANGE_FANOUT: i32 = 1;
pub const SB_EXCHANGE_TOPIC: i32 = 2;
pub const SB_EXCHANGE_HEADERS: i32 = 3;

fn kind_from_i32(v: i32) -> Result<ExchangeKind, SbStatus> {
    match v {
        SB_EXCHANGE_DIRECT => Ok(ExchangeKind::Direct),
        SB_EXCHANGE_FANOUT => Ok(ExchangeKind::Fanout),
        SB_EXCHANGE_TOPIC => Ok(ExchangeKind::Topic),
        SB_EXCHANGE_HEADERS => Ok(ExchangeKind::Headers),
        other => Err(fail(
            SbStatus::ErrNullArg,
            format_args!("exchange kind {other} is not one of 0=direct/1=fanout/2=topic/3=headers"),
        )),
    }
}

// ---------------------------------------------------------------------------
// topology: queues
// ---------------------------------------------------------------------------

/// Declare a queue. Flags are 0/1. An EMPTY name ("") asks the server to
/// generate one; the final name is returned as an owned string in `*out`
/// (free with `sb_string_free`).
///
/// # Safety
/// `channel` live; `name` valid NUL-terminated string; `out` writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_queue_declare(
    channel: *mut SbChannel,
    name: *const c_char,
    durable: c_int,
    exclusive: c_int,
    auto_delete: c_int,
    passive: c_int,
    out: *mut *mut c_char,
) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let ch = as_channel(channel)?;
        let name = unsafe { cstr("name", name)? };
        let opts = QueueOptions::new()
            .durable(durable != 0)
            .exclusive(exclusive != 0)
            .auto_delete(auto_delete != 0)
            .passive(passive != 0);
        let state = ch
            .rt
            .block_on(ch.channel.queue_declare(&name, &opts))
            .map_err(op_status)?;
        unsafe { *out = alloc_string(&state.queue) };
        Ok(())
    })
}

/// Bind `queue` to `exchange` under `routing_key`.
///
/// # Safety
/// `channel` live; all names valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_queue_bind(
    channel: *mut SbChannel,
    queue: *const c_char,
    exchange: *const c_char,
    routing_key: *const c_char,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let queue = unsafe { cstr("queue", queue)? };
        let exchange = unsafe { cstr("exchange", exchange)? };
        let key = unsafe { cstr("routing_key", routing_key)? };
        ch.rt
            .block_on(ch.channel.queue_bind(&queue, &exchange, &key))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Remove a queue binding.
///
/// # Safety
/// Same as `sb_channel_queue_bind`.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_queue_unbind(
    channel: *mut SbChannel,
    queue: *const c_char,
    exchange: *const c_char,
    routing_key: *const c_char,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let queue = unsafe { cstr("queue", queue)? };
        let exchange = unsafe { cstr("exchange", exchange)? };
        let key = unsafe { cstr("routing_key", routing_key)? };
        ch.rt
            .block_on(ch.channel.queue_unbind(&queue, &exchange, &key))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Delete a queue; the number of purged messages lands in `*out_count`
/// (when non-NULL). Flags are 0/1 preconditions.
///
/// # Safety
/// `channel` live; `name` valid; `out_count` writable or NULL.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_queue_delete(
    channel: *mut SbChannel,
    name: *const c_char,
    if_unused: c_int,
    if_empty: c_int,
    out_count: *mut u32,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let name = unsafe { cstr("name", name)? };
        let opts = QueueDeleteOptions::new()
            .if_unused(if_unused != 0)
            .if_empty(if_empty != 0);
        let count = ch
            .rt
            .block_on(ch.channel.queue_delete(&name, &opts))
            .map_err(op_status)?;
        if !out_count.is_null() {
            unsafe { *out_count = count };
        }
        Ok(())
    })
}

/// Purge all ready messages from a queue; count lands in `*out_count`.
///
/// # Safety
/// `channel` live; `name` valid; `out_count` writable or NULL.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_queue_purge(
    channel: *mut SbChannel,
    name: *const c_char,
    out_count: *mut u32,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let name = unsafe { cstr("name", name)? };
        let count = ch
            .rt
            .block_on(ch.channel.queue_purge(&name))
            .map_err(op_status)?;
        if !out_count.is_null() {
            unsafe { *out_count = count };
        }
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// topology: exchanges
// ---------------------------------------------------------------------------

/// Declare an exchange. `kind` is one of 0=direct/1=fanout/2=topic/3=headers.
///
/// # Safety
/// `channel` live; `name` valid NUL-terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_exchange_declare(
    channel: *mut SbChannel,
    name: *const c_char,
    kind: c_int,
    durable: c_int,
    auto_delete: c_int,
    internal: c_int,
    passive: c_int,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let name = unsafe { cstr("name", name)? };
        let kind = kind_from_i32(kind)?;
        let opts = ExchangeOptions::new()
            .durable(durable != 0)
            .auto_delete(auto_delete != 0)
            .internal(internal != 0)
            .passive(passive != 0);
        ch.rt
            .block_on(ch.channel.exchange_declare(&name, kind, &opts))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Delete an exchange (`if_unused`: only when binding-less).
///
/// # Safety
/// `channel` live; `name` valid NUL-terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_exchange_delete(
    channel: *mut SbChannel,
    name: *const c_char,
    if_unused: c_int,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let name = unsafe { cstr("name", name)? };
        let opts = ExchangeDeleteOptions::new().if_unused(if_unused != 0);
        ch.rt
            .block_on(ch.channel.exchange_delete(&name, &opts))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Bind destination exchange to source exchange under a key
/// (exchange-exchange binding).
///
/// # Safety
/// `channel` live; all names valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_exchange_bind(
    channel: *mut SbChannel,
    dest: *const c_char,
    source: *const c_char,
    key: *const c_char,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let dest = unsafe { cstr("dest", dest)? };
        let source = unsafe { cstr("source", source)? };
        let key = unsafe { cstr("key", key)? };
        ch.rt
            .block_on(ch.channel.exchange_bind(&dest, &source, &key))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Remove an exchange-exchange binding.
///
/// # Safety
/// Same as `sb_channel_exchange_bind`.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_exchange_unbind(
    channel: *mut SbChannel,
    dest: *const c_char,
    source: *const c_char,
    key: *const c_char,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let dest = unsafe { cstr("dest", dest)? };
        let source = unsafe { cstr("source", source)? };
        let key = unsafe { cstr("key", key)? };
        ch.rt
            .block_on(ch.channel.exchange_unbind(&dest, &source, &key))
            .map_err(op_status)?;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// publish
// ---------------------------------------------------------------------------

fn props_from(content_type: Option<String>, delivery_mode: i32) -> MessageProperties {
    let mut p = MessageProperties::default();
    if let Some(ct) = content_type {
        p.content_type = Some(ct);
    }
    if delivery_mode != 0 {
        p.delivery_mode = Some(delivery_mode as u8);
    }
    p
}

/// Publish fire-and-forget through `exchange` with `routing_key`. The body
/// is passed with an EXPLICIT LENGTH (`body`/`body_len`); NULL with
/// `body_len == 0` publishes an empty body. `delivery_mode` 0 = unset,
/// 1 = transient, 2 = persistent. `content_type` may be NULL.
///
/// `SB_OK` means the frames were written; protocol errors surface on this
/// or a later call on the channel (see the embedded crate's publish docs).
///
/// # Safety
/// `channel` live; name args valid NUL-terminated strings; when non-NULL,
/// `body` must point to `body_len` readable octets.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_publish(
    channel: *mut SbChannel,
    exchange: *const c_char,
    routing_key: *const c_char,
    content_type: *const c_char,
    delivery_mode: c_int,
    body: *const u8,
    body_len: usize,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let exchange = unsafe { cstr("exchange", exchange)? };
        let key = unsafe { cstr("routing_key", routing_key)? };
        let content_type = unsafe { opt_cstr("content_type", content_type)? };
        let body = unsafe { bytes("body", body, body_len)? };
        ch.rt
            .block_on(ch.channel.publish(
                &exchange,
                &key,
                props_from(content_type, delivery_mode),
                body.to_vec(),
            ))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Publish with the mandatory flag: an unroutable message is returned by
/// the broker instead of being enqueued; `*out_routed` receives 0 then
/// (and 1 when routed). `SB_OK` in both cases — unroutable is an OUTCOME,
/// not an error.
///
/// # Safety
/// Same argument contract as `sb_channel_publish`; `out_routed` writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_publish_mandatory(
    channel: *mut SbChannel,
    exchange: *const c_char,
    routing_key: *const c_char,
    content_type: *const c_char,
    delivery_mode: c_int,
    body: *const u8,
    body_len: usize,
    out_routed: *mut c_int,
) -> SbStatus {
    entry_status(|| {
        if out_routed.is_null() {
            return Err(fail(
                SB_ERR_NULL_ARG,
                format_args!("out_routed must not be NULL"),
            ));
        }
        let ch = as_channel(channel)?;
        let exchange = unsafe { cstr("exchange", exchange)? };
        let key = unsafe { cstr("routing_key", routing_key)? };
        let content_type = unsafe { opt_cstr("content_type", content_type)? };
        let body = unsafe { bytes("body", body, body_len)? };
        let outcome = ch
            .rt
            .block_on(ch.channel.publish_mandatory(
                &exchange,
                &key,
                props_from(content_type, delivery_mode),
                body.to_vec(),
            ))
            .map_err(op_status)?;
        use switchboard_embedded::PublishOutcome;
        unsafe { *out_routed = matches!(outcome, PublishOutcome::Routed) as c_int };
        Ok(())
    })
}

/// Publish in publisher-confirms mode (enabling it on the channel first if
/// needed): resolves when the broker acks the publish sequence or returns
/// the message. `*out_confirmed` receives 1 = acked, 0 = returned
/// (unroutable). Note the embedded crate's per-channel confirm sequence
/// counts EVERY publish on a confirm channel; this ABI exposes confirms
/// only through this call, so mixing it with `sb_channel_publish` on the
/// same channel is safe but pointless (the plain publish just advances the
/// sequence).
///
/// # Safety
/// Same argument contract as `sb_channel_publish`; `out_confirmed`
/// writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_publish_confirmed(
    channel: *mut SbChannel,
    exchange: *const c_char,
    routing_key: *const c_char,
    content_type: *const c_char,
    delivery_mode: c_int,
    body: *const u8,
    body_len: usize,
    out_confirmed: *mut c_int,
) -> SbStatus {
    entry_status(|| {
        if out_confirmed.is_null() {
            return Err(fail(
                SB_ERR_NULL_ARG,
                format_args!("out_confirmed must not be NULL"),
            ));
        }
        let ch = as_channel(channel)?;
        let exchange = unsafe { cstr("exchange", exchange)? };
        let key = unsafe { cstr("routing_key", routing_key)? };
        let content_type = unsafe { opt_cstr("content_type", content_type)? };
        let body = unsafe { bytes("body", body, body_len)? };
        use switchboard_embedded::ConfirmOutcome;
        let outcome = ch
            .rt
            .block_on(ch.channel.publish_confirmed(
                &exchange,
                &key,
                props_from(content_type, delivery_mode),
                body.to_vec(),
            ))
            .map_err(op_status)?;
        unsafe { *out_confirmed = matches!(outcome, ConfirmOutcome::Acked) as c_int };
        Ok(())
    })
}

/// Turn publisher-confirms mode on explicitly. Refused on a tx channel
/// (`SB_ERR_PRECONDITION_FAILED`, close code 406).
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_confirm_select(channel: *mut SbChannel) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt
            .block_on(ch.channel.confirm_select())
            .map_err(op_status)?;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// polling get + ack/nack/qos
// ---------------------------------------------------------------------------

/// Polling consumption, part 1 of the documented consume model: fetch one
/// message by pull (Basic.Get). Stores 1/0 in `*out_has_message`. When 1,
/// `*out` is filled with an owned `sb_message_t` (free with
/// `sb_message_free` — safe to call on it either way, it is zeroed first).
/// With `no_ack = 0` the delivery stays unacknowledged until
/// `sb_channel_ack`/`nack` (or the channel/connection closes, which
/// requeues it).
///
/// # Safety
/// `channel` live; `queue` valid; `out_has_message`/`out` writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_get(
    channel: *mut SbChannel,
    queue: *const c_char,
    no_ack: c_int,
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
        // Zero the out-struct FIRST: on any failure below the caller may
        // still sb_message_free it (a documented no-op on a zeroed struct).
        unsafe { *out = std::mem::zeroed() };
        unsafe { *out_has_message = 0 };
        let ch = as_channel(channel)?;
        let queue = unsafe { cstr("queue", queue)? };
        let delivery = ch
            .rt
            .block_on(ch.channel.get(&queue, no_ack != 0))
            .map_err(op_status)?;
        match delivery {
            Some(d) => {
                SbMessage::fill_from_delivery(out, &d);
                unsafe { *out_has_message = 1 };
            }
            None => {
                unsafe { *out = std::mem::zeroed() };
                unsafe { *out_has_message = 0 };
            }
        }
        Ok(())
    })
}

/// Acknowledge one delivery (by its `sb_message_t.delivery_tag`).
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_ack(channel: *mut SbChannel, delivery_tag: u64) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt
            .block_on(ch.channel.ack(delivery_tag))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Acknowledge everything up to and including `delivery_tag`
/// (multiple=true).
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_ack_many(
    channel: *mut SbChannel,
    up_to_delivery_tag: u64,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt
            .block_on(ch.channel.ack_many(up_to_delivery_tag))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Reject one delivery; `requeue != 0` puts it back (as redelivered),
/// `requeue == 0` drops it to the dead-letter path.
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_nack(
    channel: *mut SbChannel,
    delivery_tag: u64,
    requeue: c_int,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt
            .block_on(ch.channel.nack(delivery_tag, requeue != 0))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Reject everything up to and including `delivery_tag`.
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_nack_many(
    channel: *mut SbChannel,
    up_to_delivery_tag: u64,
    requeue: c_int,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt
            .block_on(ch.channel.nack_many(up_to_delivery_tag, requeue != 0))
            .map_err(op_status)?;
        Ok(())
    })
}

/// Set the prefetch window: at most `prefetch_count` unacknowledged
/// messages per consumer (or per channel when `global != 0`). This is the
/// real flow control of the (unbounded) delivery pipeline — set it for
/// push consumers.
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_qos(
    channel: *mut SbChannel,
    prefetch_count: u16,
    global: c_int,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt
            .block_on(ch.channel.qos(prefetch_count, global != 0))
            .map_err(op_status)?;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// consumers
// ---------------------------------------------------------------------------

/// Start a push consumer on `queue`; deliveries are drained through
/// `sb_consumer_next` (the documented consume model is POLLING — this ABI
/// deliberately crosses no callback boundary; see the header).
/// `consumer_tag` may be NULL/empty (the server generates one, retrievable
/// via `sb_consumer_tag`); `no_ack`/`exclusive` are 0/1.
///
/// # Safety
/// `channel` live; `queue` valid; `consumer_tag` valid or NULL; `out`
/// writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_consume(
    channel: *mut SbChannel,
    queue: *const c_char,
    consumer_tag: *const c_char,
    no_ack: c_int,
    exclusive: c_int,
    out: *mut *mut crate::consumer_api::SbConsumer,
) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let ch = as_channel(channel)?;
        let queue = unsafe { cstr("queue", queue)? };
        let tag = unsafe { opt_cstr("consumer_tag", consumer_tag)? }.unwrap_or_default();
        let opts = ConsumeOptions::new()
            .tag(tag)
            .no_ack(no_ack != 0)
            .exclusive(exclusive != 0);
        let consumer = ch
            .rt
            .block_on(ch.channel.consume(&queue, &opts))
            .map_err(op_status)?;
        let boxed = Box::new(crate::consumer_api::SbConsumer {
            rt: ch.rt.clone(),
            tag: consumer.tag.clone(),
            consumer: std::sync::Mutex::new(Some(consumer)),
        });
        let ptr = Box::into_raw(boxed);
        let broker_addr = table::owner_of(channel as usize).unwrap_or(channel as usize);
        table::register(ptr as usize, Kind::Consumer, broker_addr);
        unsafe { *out = ptr };
        Ok(())
    })
}

/// Cancel a consumer BY TAG through its channel (Basic.Cancel/Cancel-Ok;
/// cancelling an unknown tag is answered Cancel-Ok by the broker). The
/// consumer handle (if any) stays owned by the caller.
///
/// # Safety
/// `channel` live; `consumer_tag` valid.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_cancel(
    channel: *mut SbChannel,
    consumer_tag: *const c_char,
) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        let tag = unsafe { cstr("consumer_tag", consumer_tag)? };
        ch.rt.block_on(ch.channel.cancel(&tag)).map_err(op_status)?;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// transactions
// ---------------------------------------------------------------------------

/// Put the channel in transactional mode (Tx.Select): publishes and acks
/// buffer until `sb_channel_tx_commit`. Mutually exclusive with confirm
/// mode (406 → `SB_ERR_PRECONDITION_FAILED`).
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_tx_select(channel: *mut SbChannel) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt.block_on(ch.channel.tx_select()).map_err(op_status)?;
        Ok(())
    })
}

/// Commit the transaction buffer (two-phase prepare/commit across the
/// involved shards). See `sb_config_set_janitor_interval_ms` for the
/// known commit/tick race and its mitigation.
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_tx_commit(channel: *mut SbChannel) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt.block_on(ch.channel.tx_commit()).map_err(op_status)?;
        Ok(())
    })
}

/// Roll the transaction buffer back: buffered publishes vanish, buffered
/// acks leave their messages unacked/still held.
///
/// # Safety
/// `channel` live.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_tx_rollback(channel: *mut SbChannel) -> SbStatus {
    entry_status(|| {
        let ch = as_channel(channel)?;
        ch.rt
            .block_on(ch.channel.tx_rollback())
            .map_err(op_status)?;
        Ok(())
    })
}

/// This channel's number (1-based, connection-scoped), stored in `*out`.
///
/// # Safety
/// `channel` live; `out` writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_id(channel: *mut SbChannel, out: *mut u16) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let ch = as_channel(channel)?;
        unsafe { *out = ch.channel.id() };
        Ok(())
    })
}

/// Free a channel handle. There is no per-channel close in the embedded
/// protocol surface: channels tear down when their connection closes
/// (`sb_client_close` / broker stop). Passing NULL is a no-op; double-free
/// is DETECTED, not UB.
///
/// # Safety
/// `channel` must be NULL or a handle issued by this library that has not
/// been freed yet.
#[unsafe(no_mangle)]
pub extern "C" fn sb_channel_free(channel: *mut SbChannel) -> SbStatus {
    entry_status(|| {
        if channel.is_null() {
            return Ok(());
        }
        if !table::is_registered(channel as usize, Kind::Channel) {
            return Err(fail(
                SbStatus::ErrInvalidHandle,
                format_args!("channel was already freed (or never issued by this library)"),
            ));
        }
        table::unregister(channel as usize);
        drop(unsafe { Box::from_raw(channel) });
        Ok(())
    })
}
