//! `sb_config_*` and `sb_broker_*`: configuration, lifecycle, readiness,
//! connections, typed admin, and the string free fn.

use std::ffi::{CString, c_char, c_int};
use std::time::Duration;

use switchboard_cluster::node::Timeouts;
use switchboard_embedded::{Broker, BrokerConfig};
use tokio::runtime::Runtime;

use crate::handles::{SbBroker, SbClient, SbConfig, as_broker, as_config};
use crate::map::{admin_status, already_gone, connect_status, start_status, stop_status};
use crate::message::{alloc_string, cstr};
use crate::status::{
    SB_ERR_INVALID_HANDLE, SB_ERR_IO, SB_ERR_NULL_ARG, SbStatus, entry_status, fail,
};
use crate::table::{self, Kind};

/// The grace budget `sb_broker_free` uses when it must shut down a live
/// broker itself (the caller skipped `sb_broker_stop`).
const FREE_FALLBACK_GRACE: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

/// Create a broker configuration for a fresh single-node cluster rooted at
/// `data_dir` (the RocksDB location). Defaults: node id 1, expected-nodes
/// 1, no bootstrap-credentials write, default node timeouts.
///
/// On success `*out` receives an owned `sb_config_t*` — release it with
/// exactly one `sb_config_free`. On failure `*out` is left untouched.
///
/// # Safety
/// `data_dir` must be NULL or a valid NUL-terminated string; `out` must
/// point to writable memory.
#[unsafe(no_mangle)]
pub extern "C" fn sb_config_create(data_dir: *const c_char, out: *mut *mut SbConfig) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let dir = unsafe { cstr("data_dir", data_dir)? };
        let cfg = Box::new(SbConfig {
            data_dir: dir,
            node_id: 1,
            expected_nodes: 1,
            bootstrap_credentials: None,
            janitor_interval_ms: None,
        });
        let ptr = Box::into_raw(cfg);
        table::register(ptr as usize, Kind::Config, ptr as usize);
        unsafe { *out = ptr };
        Ok(())
    })
}

/// Override the cluster node id (default 1).
///
/// # Safety
/// `config` must be a live `sb_config_t*`.
#[unsafe(no_mangle)]
pub extern "C" fn sb_config_set_node_id(config: *mut SbConfig, node_id: u64) -> SbStatus {
    entry_status(|| {
        let cfg = as_config(config)?;
        cfg.node_id = node_id;
        Ok(())
    })
}

/// Override the expected cluster size (default 1).
///
/// # Safety
/// `config` must be a live `sb_config_t*`.
#[unsafe(no_mangle)]
pub extern "C" fn sb_config_set_expected_nodes(
    config: *mut SbConfig,
    expected_nodes: u64,
) -> SbStatus {
    entry_status(|| {
        let cfg = as_config(config)?;
        cfg.expected_nodes = expected_nodes;
        Ok(())
    })
}

/// Ensure this user exists with this password after the broker is ready
/// (ensure-user-if-absent; an existing user's password is NOT overwritten).
/// The reserved name `guest` is refused at start with
/// `SB_ERR_INVALID_CONFIG` — rotate guest via `sb_broker_create_user`
/// after start instead.
///
/// # Safety
/// `config`, `user`, `password` must be valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub extern "C" fn sb_config_set_bootstrap_credentials(
    config: *mut SbConfig,
    user: *const c_char,
    password: *const c_char,
) -> SbStatus {
    entry_status(|| {
        let cfg = as_config(config)?;
        let user = unsafe { cstr("user", user)? };
        let password = unsafe { cstr("password", password)? };
        cfg.bootstrap_credentials = Some((user, password));
        Ok(())
    })
}

/// Override the node's janitor tick interval in milliseconds. The embedded
/// crate documents that a tx commit racing the default 1 s janitor tick
/// can be silently dropped while Commit-Ok still returns (see its crate
/// docs): embedders that use `sb_channel_tx_*` should set this well above
/// their longest transaction (e.g. 60000).
///
/// # Safety
/// `config` must be a live `sb_config_t*`.
#[unsafe(no_mangle)]
pub extern "C" fn sb_config_set_janitor_interval_ms(
    config: *mut SbConfig,
    interval_ms: u64,
) -> SbStatus {
    entry_status(|| {
        let cfg = as_config(config)?;
        cfg.janitor_interval_ms = Some(interval_ms);
        Ok(())
    })
}

/// Free a configuration created by `sb_config_create`. Passing NULL is a
/// no-op. Freeing twice is DETECTED (`SB_ERR_INVALID_HANDLE`), not UB.
///
/// # Safety
/// `config` must be NULL or a handle issued by this library that has not
/// been freed yet.
#[unsafe(no_mangle)]
pub extern "C" fn sb_config_free(config: *mut SbConfig) -> SbStatus {
    entry_status(|| {
        if config.is_null() {
            return Ok(());
        }
        if !table::unregister(config as usize) {
            return Err(fail(
                SB_ERR_INVALID_HANDLE,
                format_args!("config was already freed (or never issued by this library)"),
            ));
        }
        drop(unsafe { Box::from_raw(config) });
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// broker lifecycle
// ---------------------------------------------------------------------------

/// Create and start one in-process broker from `config`, storing an owned
/// handle in `*out`. The broker runs on a private multi-thread tokio
/// runtime this library creates — the embedder supplies no runtime of its
/// own.
///
/// Typical failures: `SB_ERR_STORAGE` (the RocksDB LOCK on `data_dir` is
/// held — one live broker per directory per PROCESS; restart means a new
/// process), `SB_ERR_INVALID_CONFIG` (bootstrap credentials naming
/// `guest`), `SB_ERR_FORMATION`, `SB_ERR_IO`. On failure `*out` is left
/// untouched.
///
/// # Safety
/// `config` must be a live handle; `out` must point to writable memory.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_start(config: *const SbConfig, out: *mut *mut SbBroker) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let cfg = as_config(config as *mut SbConfig)?;
        let rt = Runtime::new().map_err(|e| {
            fail(
                SB_ERR_IO,
                format_args!("failed to create the embedded tokio runtime: {e}"),
            )
        })?;
        let mut bcfg = BrokerConfig::new(cfg.data_dir.clone())
            .node_id(cfg.node_id)
            .expected_nodes(cfg.expected_nodes);
        if let Some((user, password)) = &cfg.bootstrap_credentials {
            bcfg = bcfg.bootstrap_credentials(user, password);
        }
        if let Some(ms) = cfg.janitor_interval_ms {
            let mut timeouts = Timeouts::default();
            timeouts.janitor_interval = Duration::from_millis(ms);
            bcfg = bcfg.timeouts(timeouts);
        }
        let broker: Broker = rt.block_on(Broker::start(bcfg)).map_err(start_status)?;
        let boxed = Box::new(SbBroker {
            rt,
            broker: Some(broker),
        });
        let ptr = Box::into_raw(boxed);
        table::register(ptr as usize, Kind::Broker, ptr as usize);
        unsafe { *out = ptr };
        Ok(())
    })
}

/// Store 1/0 in `*out`: true once the broker's bootstrap entities are
/// visible in the local topology.
///
/// # Safety
/// `broker` must be a live handle; `out` must point to writable memory.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_is_ready(broker: *mut SbBroker, out: *mut c_int) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let b = as_broker(broker)?;
        let ready = b
            .broker
            .as_ref()
            .map(|inner| inner.is_ready())
            .unwrap_or(false);
        unsafe { *out = ready as c_int };
        Ok(())
    })
}

/// Block until `sb_broker_is_ready` would report true, for at most
/// `timeout_ms`. `SB_ERR_TIMEOUT` when the budget elapses (formation may
/// still be pending, e.g. unreachable join seeds).
///
/// # Safety
/// `broker` must be a live handle.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_wait_ready(broker: *mut SbBroker, timeout_ms: u32) -> SbStatus {
    entry_status(|| {
        let b = as_broker(broker)?;
        let inner = b.broker.as_ref().ok_or_else(|| already_gone("broker"))?;
        b.rt.block_on(inner.wait_ready(Duration::from_millis(timeout_ms as u64)))
            .map_err(start_status)?;
        Ok(())
    })
}

/// The configured data directory (owned string; free with
/// `sb_string_free`).
///
/// # Safety
/// `broker` must be a live handle; `out` must point to writable memory.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_data_dir(broker: *mut SbBroker, out: *mut *mut c_char) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let b = as_broker(broker)?;
        let inner = b.broker.as_ref().ok_or_else(|| already_gone("broker"))?;
        let dir = inner.data_dir().to_string_lossy().to_string();
        unsafe { *out = alloc_string(&dir) };
        Ok(())
    })
}

/// Graceful stop: close every tracked connection, run the bounded cluster
/// leave with `grace_ms`, then the node runtime's shutdown fallback. THE
/// HANDLE BECOMES DEAD (release it afterwards with `sb_broker_free`;
/// every client/channel/consumer ever created from it becomes dead too).
/// Teardown completes even when the status is `SB_ERR_STOP_TIMED_OUT` or
/// `SB_ERR_STOP_LEAVE_FAILED`.
///
/// # Safety
/// `broker` must be NULL or a live handle issued by this library.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_stop(broker: *mut SbBroker, grace_ms: u32) -> SbStatus {
    entry_status(|| {
        if broker.is_null() {
            return Err(fail(
                SB_ERR_NULL_ARG,
                format_args!("broker must not be NULL"),
            ));
        }
        let b = as_broker(broker)?;
        let inner = b.broker.take().ok_or_else(|| already_gone("broker"))?;
        let res =
            b.rt.block_on(inner.stop(Duration::from_millis(grace_ms as u64)));
        // Every dependent handle dies with the broker's runtime, whatever
        // the leave outcome was.
        table::mark_owner_dependents_dead(broker as usize);
        table::mark_dead(broker as usize);
        res.map_err(stop_status)?;
        Ok(())
    })
}

/// Free a broker handle. If the broker is still live this performs a
/// graceful stop first (10 s fallback grace) — prefer `sb_broker_stop` to
/// observe teardown errors. Freeing a dead-but-present handle (i.e. after
/// `sb_broker_stop`) is the normal release path. Passing NULL is a no-op;
/// double-free is DETECTED (`SB_ERR_INVALID_HANDLE`), not UB.
///
/// # Safety
/// `broker` must be NULL or a handle issued by this library that has not
/// been freed yet.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_free(broker: *mut SbBroker) -> SbStatus {
    entry_status(|| {
        if broker.is_null() {
            return Ok(());
        }
        // Presence, not aliveness: dead-but-present handles (post-stop)
        // MUST stay freeable — that is the documented ownership flow.
        if !table::is_registered(broker as usize, Kind::Broker) {
            return Err(fail(
                SB_ERR_INVALID_HANDLE,
                format_args!("broker was already freed (or never issued by this library)"),
            ));
        }
        // SAFETY: registry presence proves the box is an un-freed
        // SbBroker issued by this library.
        let b = unsafe { &mut *broker };
        if let Some(inner) = b.broker.take() {
            let _ = b.rt.block_on(inner.stop(FREE_FALLBACK_GRACE));
            table::mark_owner_dependents_dead(broker as usize);
        }
        table::mark_dead(broker as usize);
        table::unregister(broker as usize);
        drop(unsafe { Box::from_raw(broker) });
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// connections
// ---------------------------------------------------------------------------

/// Open an embedded connection to vhost `"/"` (real PLAIN handshake against
/// the replicated user table; embedded clients are ordinary loopback
/// clients, guest-loopback rule included). On success `*out` receives an
/// owned handle.
///
/// # Safety
/// `broker` live; `user`/`password` valid NUL-terminated strings; `out`
/// writable.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_connect(
    broker: *mut SbBroker,
    user: *const c_char,
    password: *const c_char,
    out: *mut *mut SbClient,
) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let b = as_broker(broker)?;
        let user = unsafe { cstr("user", user)? };
        let password = unsafe { cstr("password", password)? };
        let inner = b.broker.as_ref().ok_or_else(|| already_gone("broker"))?;
        let conn =
            b.rt.block_on(inner.connect(&user, &password))
                .map_err(connect_status)?;
        let client = Box::new(SbClient {
            rt: b.rt.handle().clone(),
            conn: Some(conn),
        });
        let ptr = Box::into_raw(client);
        table::register(ptr as usize, Kind::Client, broker as usize);
        unsafe { *out = ptr };
        Ok(())
    })
}

/// [`sb_broker_connect`](fn@sb_broker_connect) for a specific vhost.
/// Unknown vhost → `SB_ERR_NO_SUCH_VHOST`; bad credentials →
/// `SB_ERR_CONNECT_ACCESS_REFUSED`.
///
/// # Safety
/// Same as `sb_broker_connect`; `vhost` must be a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_connect_vhost(
    broker: *mut SbBroker,
    user: *const c_char,
    password: *const c_char,
    vhost: *const c_char,
    out: *mut *mut SbClient,
) -> SbStatus {
    entry_status(|| {
        if out.is_null() {
            return Err(fail(SB_ERR_NULL_ARG, format_args!("out must not be NULL")));
        }
        let b = as_broker(broker)?;
        let user = unsafe { cstr("user", user)? };
        let password = unsafe { cstr("password", password)? };
        let vhost = unsafe { cstr("vhost", vhost)? };
        let inner = b.broker.as_ref().ok_or_else(|| already_gone("broker"))?;
        let conn =
            b.rt.block_on(inner.connect_vhost(&user, &password, &vhost))
                .map_err(connect_status)?;
        let client = Box::new(SbClient {
            rt: b.rt.handle().clone(),
            conn: Some(conn),
        });
        let ptr = Box::into_raw(client);
        table::register(ptr as usize, Kind::Client, broker as usize);
        unsafe { *out = ptr };
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// admin
// ---------------------------------------------------------------------------

/// Declare a vhost (idempotent upsert of the mandatory bootstrap
/// exchanges). `SB_ERR_ADMIN_REFUSED` when the replicated meta state
/// machine rejects the command.
///
/// # Safety
/// `broker` live; `name` a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_declare_vhost(broker: *mut SbBroker, name: *const c_char) -> SbStatus {
    entry_status(|| {
        let b = as_broker(broker)?;
        let name = unsafe { cstr("name", name)? };
        let inner = b.broker.as_ref().ok_or_else(|| already_gone("broker"))?;
        b.rt.block_on(inner.declare_vhost(&name))
            .map_err(admin_status)?;
        Ok(())
    })
}

/// Create a user or REPLACE the existing user's password (upsert) — the
/// explicit set/rotate path for any user, `guest` included.
///
/// # Safety
/// `broker` live; `user`/`password` valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_create_user(
    broker: *mut SbBroker,
    user: *const c_char,
    password: *const c_char,
) -> SbStatus {
    entry_status(|| {
        let b = as_broker(broker)?;
        let user = unsafe { cstr("user", user)? };
        let password = unsafe { cstr("password", password)? };
        let inner = b.broker.as_ref().ok_or_else(|| already_gone("broker"))?;
        b.rt.block_on(inner.create_user(&user, &password))
            .map_err(admin_status)?;
        Ok(())
    })
}

/// Delete a user and their permissions (idempotent: deleting a missing
/// user succeeds).
///
/// # Safety
/// `broker` live; `user` a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_delete_user(broker: *mut SbBroker, user: *const c_char) -> SbStatus {
    entry_status(|| {
        let b = as_broker(broker)?;
        let user = unsafe { cstr("user", user)? };
        let inner = b.broker.as_ref().ok_or_else(|| already_gone("broker"))?;
        b.rt.block_on(inner.delete_user(&user))
            .map_err(admin_status)?;
        Ok(())
    })
}

/// Set one user's permissions on one vhost (RabbitMQ-shaped regexes; all
/// empty patterns clear the entry). A missing user →
/// `SB_ERR_ADMIN_REFUSED`.
///
/// # Safety
/// `broker` live; all name arguments valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub extern "C" fn sb_broker_set_permissions(
    broker: *mut SbBroker,
    user: *const c_char,
    vhost: *const c_char,
    configure: *const c_char,
    write: *const c_char,
    read: *const c_char,
) -> SbStatus {
    entry_status(|| {
        let b = as_broker(broker)?;
        let user = unsafe { cstr("user", user)? };
        let vhost = unsafe { cstr("vhost", vhost)? };
        let configure = unsafe { cstr("configure", configure)? };
        let write = unsafe { cstr("write", write)? };
        let read = unsafe { cstr("read", read)? };
        let inner = b.broker.as_ref().ok_or_else(|| already_gone("broker"))?;
        b.rt.block_on(inner.set_permissions(
            &user,
            &vhost,
            switchboard_embedded::Permissions {
                configure,
                write,
                read,
            },
        ))
        .map_err(admin_status)?;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// string free
// ---------------------------------------------------------------------------

/// Free one owned string previously returned by this library
/// (`sb_broker_data_dir`, `sb_channel_queue_declare`, `sb_consumer_tag`).
/// Passing NULL is a no-op. Free each returned string AT MOST ONCE.
///
/// # Safety
/// `s` must be NULL or a pointer returned by this library on this process.
#[unsafe(no_mangle)]
pub extern "C" fn sb_string_free(s: *mut c_char) -> SbStatus {
    entry_status(|| {
        if s.is_null() {
            return Ok(());
        }
        drop(unsafe { CString::from_raw(s) });
        Ok(())
    })
}
