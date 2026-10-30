//! The opaque handle types (Rust side of `sb_config_t` / `sb_broker_t` /
//! `sb_client_t` / `sb_channel_t` / `sb_consumer_t`) plus the typed
//! accessors every entry point starts with.
//!
//! Ownership model (mirrored in `include/switchboard.h`):
//! * every `sb_*_create/start/connect/open/consume` that returns SB_OK
//!   hands out exactly one handle the caller owns;
//! * the caller releases it with exactly one `sb_*_free` (or, for the
//!   client, `sb_client_close` followed by `sb_client_free`);
//! * using a freed handle → `SB_ERR_INVALID_HANDLE`; using a handle whose
//!   broker/connection/consumer is gone → `SB_ERR_HANDLE_DEAD`. Neither
//!   dereferences the pointee.
//!
//! Each broker handle owns a private multi-thread tokio runtime: the C
//! caller needs no runtime of its own. Client/channel/consumer handles
//! carry a `Handle` clone into that runtime and run each synchronous ABI
//! call with `block_on` — the calling OS thread drives the future, the
//! runtime's worker pool runs the broker's background tasks.

use std::ffi::c_void;

use switchboard_embedded::{Broker, EmbeddedChannel, EmbeddedConnection};
use tokio::runtime::{Handle, Runtime};

use crate::status::SbStatus;
use crate::table::{self, Kind};

/// Builder for one broker: `sb_config_create` + `sb_config_set_*` fns.
pub struct SbConfig {
    pub(crate) data_dir: String,
    pub(crate) node_id: u64,
    pub(crate) expected_nodes: u64,
    pub(crate) bootstrap_credentials: Option<(String, String)>,
    /// `Some(ms)` overrides the node's janitor tick. The embedded crate
    /// documents a janitor/tx race (its crate docs, "Authorization parity
    /// ... inherited behavior"): with the default 1 s tick, a tx commit
    /// racing a tick can be silently dropped. Embedders that use
    /// transactions should widen this.
    pub(crate) janitor_interval_ms: Option<u64>,
}

/// One started in-process broker + its private tokio runtime.
pub struct SbBroker {
    pub(crate) rt: Runtime,
    /// `None` once `stop` has taken it for the graceful teardown.
    pub(crate) broker: Option<Broker>,
}

/// One embedded AMQP connection (`switchboard_embedded::EmbeddedConnection`).
pub struct SbClient {
    pub(crate) rt: Handle,
    /// `None` once `sb_client_close` ran the close handshake.
    pub(crate) conn: Option<EmbeddedConnection>,
}

/// One AMQP channel on a [`SbClient`].
pub struct SbChannel {
    pub(crate) rt: Handle,
    pub(crate) channel: EmbeddedChannel,
}

// The consumer handle (`sb_consumer_t`) lives in `consumer_api.rs` next to
// its functions; it needs the consumer mutex up front.

/// Typed accessors: validate via the global table, then project the
/// pointee. They return the pieces entry points actually need so no
/// `&mut` aliasing question ever arises.
pub(crate) struct BrokerRef<'a> {
    pub(crate) rt: &'a Runtime,
    pub(crate) broker: &'a mut Option<Broker>,
}

pub(crate) fn as_broker(ptr: *mut SbBroker) -> Result<BrokerRef<'static>, SbStatus> {
    table::check(ptr as *mut c_void, Kind::Broker, "broker")?;
    // SAFETY: the table proves the address is a live SbBroker box issued
    // by this library and not yet freed; aliasing is the C caller's
    // contract to avoid, exactly as with any C library handle.
    let b = unsafe { &mut *ptr };
    Ok(BrokerRef {
        rt: &b.rt,
        broker: &mut b.broker,
    })
}

pub(crate) fn as_config(ptr: *mut SbConfig) -> Result<&'static mut SbConfig, SbStatus> {
    table::check(ptr as *mut c_void, Kind::Config, "config")?;
    Ok(unsafe { &mut *ptr })
}

pub(crate) struct ClientRef<'a> {
    pub(crate) rt: &'a Handle,
    pub(crate) conn: &'a mut Option<EmbeddedConnection>,
}

pub(crate) fn as_client(ptr: *mut SbClient) -> Result<ClientRef<'static>, SbStatus> {
    table::check(ptr as *mut c_void, Kind::Client, "client")?;
    let c = unsafe { &mut *ptr };
    Ok(ClientRef {
        rt: &c.rt,
        conn: &mut c.conn,
    })
}

pub(crate) fn as_channel(ptr: *mut SbChannel) -> Result<&'static SbChannel, SbStatus> {
    table::check(ptr as *mut c_void, Kind::Channel, "channel")?;
    Ok(unsafe { &*ptr })
}

pub(crate) fn as_consumer(
    ptr: *mut crate::consumer_api::SbConsumer,
) -> Result<&'static crate::consumer_api::SbConsumer, SbStatus> {
    table::check(ptr as *mut c_void, Kind::Consumer, "consumer")?;
    Ok(unsafe { &*ptr })
}
