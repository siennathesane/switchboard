//! `sb_client_*`: the connection handle — channel opening, graceful
//! close, free.

use crate::handles::SbChannel;
use crate::handles::SbClient;
use crate::handles::as_client;
use crate::map::already_gone;
use crate::map::connect_status;
use crate::status::SB_ERR_NULL_ARG;
use crate::status::SbStatus;
use crate::status::entry_status;
use crate::status::fail;
use crate::table::Kind;
use crate::table::{self};

/// Open a new channel on this connection (channel numbers are
/// connection-scoped, start at 1). On success `*out` receives an owned
/// `sb_channel_t*`.
///
/// # Safety
/// `client` must be a live handle; `out` must point to writable memory.
#[unsafe(no_mangle)]
pub extern "C" fn sb_client_open_channel(
    client: *mut SbClient,
    out: *mut *mut SbChannel,
) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let c = as_client(client)?;
        let conn = c.conn.as_ref().ok_or_else(|| already_gone("client"))?;
        let channel = c.rt.block_on(conn.open_channel()).map_err(connect_status)?;
        // The owning broker's address: channels registered on the client
        // die with the same broker. Recover it from the client's
        // registration owner.
        let broker_addr = table::owner_of(client as usize).ok_or_else(|| {
            fail(
                SbStatus::ErrInvalidHandle,
                format_args!("client handle vanished from the registry"),
            )
        })?;
        let ptr = Box::into_raw(Box::new(SbChannel {
            rt: c.rt.clone(),
            channel,
        }));
        table::register(ptr as usize, Kind::Channel, broker_addr);
        unsafe { *out = ptr };
        Ok(())
    })
}

/// Close the connection: send `Connection.Close`, await `Close-Ok`, let
/// the server run its teardown (consumers cancelled, unacked deliveries
/// requeued), and deregister from the broker. THE CLIENT HANDLE BECOMES
/// DEAD (release it with `sb_client_free`; every channel/consumer created
/// from it becomes dead too).
///
/// # Safety
/// `client` must be NULL or a live handle issued by this library.
#[unsafe(no_mangle)]
pub extern "C" fn sb_client_close(client: *mut SbClient) -> SbStatus {
    entry_status(|| {
        if client.is_null() {
            return Err(fail(
                SB_ERR_NULL_ARG,
                format_args!("client must not be NULL"),
            ));
        }
        let c = as_client(client)?;
        let conn = c.conn.take().ok_or_else(|| already_gone("client"))?;
        c.rt.block_on(conn.close());
        table::mark_dead(client as usize);
        Ok(())
    })
}

/// Free a client handle. Passing NULL is a no-op; double-free is DETECTED
/// (`SB_ERR_INVALID_HANDLE`), not UB. Prefer `sb_client_close` first for a
/// graceful close; `sb_client_free` alone just drops the handle (the
/// session stays registered with the broker until the broker stops — the
/// embedded crate's documented §9/R6 drop behavior).
///
/// # Safety
/// `client` must be NULL or a handle issued by this library that has not
/// been freed yet.
#[unsafe(no_mangle)]
pub extern "C" fn sb_client_free(client: *mut SbClient) -> SbStatus {
    entry_status(|| {
        if client.is_null() {
            return Ok(());
        }
        // Validate presence (freed → error), then drop the box. Presence
        // (not aliveness) is the criterion: freeing a dead-but-present
        // handle is exactly the documented "close, then free" flow.
        if !table::is_registered(client as usize, Kind::Client) {
            return Err(fail(
                SbStatus::ErrInvalidHandle,
                format_args!("client was already freed (or never issued by this library)"),
            ));
        }
        table::unregister(client as usize);
        drop(unsafe { Box::from_raw(client) });
        Ok(())
    })
}
