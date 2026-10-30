//! The global handle table: the ABI's use-after-free and cross-kind
//! confusion detector.
//!
//! Every handle the library issues (configs, brokers, clients, channels,
//! consumers) is registered here at issue time under its address with a
//! [`Kind`] tag, an `alive` flag, and the address of the broker that owns
//! it (its runtime). Every entry point validates its handle against this
//! table BEFORE touching the pointee:
//!
//! * address absent → [`SB_ERR_INVALID_HANDLE`](crate::SB_ERR_INVALID_HANDLE)
//!   (never issued, or already freed — the free removed the entry), so a
//!   double-free is detected instead of being a double-drop;
//! * address present but tagged with another kind → invalid (a stale
//!   pointer whose address was reused by a different handle kind cannot
//!   masquerade);
//! * present, right kind, `alive == false` →
//!   [`SB_ERR_HANDLE_DEAD`](crate::SB_ERR_HANDLE_DEAD) (its broker was
//!   stopped/freed, its connection closed, or its consumer cancelled).
//!
//! When a broker stops, all of its dependent handles (clients, channels,
//! consumers — everything whose `owner` is that broker) are marked dead,
//! not removed: they stay freeable via the matching `sb_*_free`, which is
//! what makes the C ownership rule "every create gets exactly one free"
//! safe even after the broker is gone.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::status::SB_ERR_HANDLE_DEAD;
use crate::status::SB_ERR_INVALID_HANDLE;
use crate::status::SB_ERR_NULL_ARG;
use crate::status::SbStatus;
use crate::status::fail;

/// Which kind of opaque handle an address holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Config,
    Broker,
    Client,
    Channel,
    Consumer,
}

struct Entry {
    kind: Kind,
    alive: bool,
    /// Address of the owning `SbBroker` box (clients/channels/consumers),
    /// or the handle's own address (configs, brokers).
    owner: usize,
}

/// The whole-table lock. Contention is irrelevant at C-API call rates.
static TABLE: Mutex<Option<HashMap<usize, Entry>>> = Mutex::new(None);

fn with_map<R>(f: impl FnOnce(&mut HashMap<usize, Entry>) -> R) -> R {
    let mut guard = TABLE.lock().unwrap_or_else(|p| p.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    f(map)
}

pub(crate) fn register(ptr: usize, kind: Kind, owner: usize) {
    with_map(|m| {
        m.insert(
            ptr,
            Entry {
                kind,
                alive: true,
                owner,
            },
        );
    });
}

/// Validate `ptr` for entry-point use. `expected_kind` must match; the
/// handle must still be alive. `arg` names the argument for the error
/// message.
pub(crate) fn check(
    ptr: *mut std::ffi::c_void,
    expected_kind: Kind,
    arg: &str,
) -> Result<usize, SbStatus> {
    if ptr.is_null() {
        return Err(fail(SB_ERR_NULL_ARG, format!("{arg} must not be NULL")));
    }
    let addr = ptr as usize;
    let entry = with_map(|m| m.get(&addr).map(|e| (e.kind, e.alive)));
    match entry {
        None => Err(fail(
            SB_ERR_INVALID_HANDLE,
            format!(
                "{arg} is not a live handle issued by this library (freed already, or never valid)"
            ),
        )),
        Some((kind, _)) if kind != expected_kind => Err(fail(
            SB_ERR_INVALID_HANDLE,
            format!("{arg} is not a handle of the expected kind"),
        )),
        Some((_, false)) => Err(fail(
            SB_ERR_HANDLE_DEAD,
            format!("{arg} refers to a handle whose broker/connection/consumer is already gone"),
        )),
        Some((_, true)) => Ok(addr),
    }
}

/// Mark one handle dead (its broker/connection/consumer is gone) while
/// KEEPING it registered so `sb_*_free` still owns and frees it.
pub(crate) fn mark_dead(ptr: usize) {
    with_map(|m| {
        if let Some(e) = m.get_mut(&ptr) {
            e.alive = false;
        }
    });
}

/// Mark every client/channel/consumer owned by broker `broker_addr` dead
/// (called when a broker stops or is freed).
pub(crate) fn mark_owner_dependents_dead(broker_addr: usize) {
    with_map(|m| {
        for e in m.values_mut() {
            if e.owner == broker_addr && !matches!(e.kind, Kind::Config | Kind::Broker) {
                e.alive = false;
            }
        }
    });
}

/// Remove an entry (the free path). Returns false when the address was
/// never registered — the caller must NOT drop the pointee then, which is
/// exactly what turns double-free into a detected error instead of UB.
pub(crate) fn unregister(ptr: usize) -> bool {
    with_map(|m| m.remove(&ptr).is_some())
}

/// Is `ptr` registered as `kind` (alive OR dead)? The free path checks
/// presence, not aliveness: freeing a dead-but-present handle is the
/// documented "close, then free" flow.
pub(crate) fn is_registered(ptr: usize, kind: Kind) -> bool {
    with_map(|m| m.get(&ptr).map(|e| e.kind == kind).unwrap_or(false))
}

/// The registered owner (broker address) of a handle, for attaching new
/// dependent handles to the same broker.
pub(crate) fn owner_of(ptr: usize) -> Option<usize> {
    with_map(|m| m.get(&ptr).map(|e| e.owner))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_check_free_lifecycle() {
        let fake = 0x_dead_c0_ffee_u64 as *mut std::ffi::c_void;
        // Never issued.
        assert!(check(fake, Kind::Channel, "ch").is_err());
        register(fake as usize, Kind::Channel, 7);
        assert_eq!(check(fake, Kind::Channel, "ch"), Ok(fake as usize));
        // Kind mismatch cannot alias.
        assert!(check(fake, Kind::Client, "cl").is_err());
        // Dead-but-present stays freeable.
        mark_dead(fake as usize);
        assert!(matches!(
            check(fake, Kind::Channel, "ch"),
            Err(SB_ERR_HANDLE_DEAD)
        ));
        assert!(unregister(fake as usize));
        // Second unregister (double free) reports absence.
        assert!(!unregister(fake as usize));
        assert!(matches!(
            check(fake, Kind::Channel, "ch"),
            Err(SB_ERR_INVALID_HANDLE)
        ));
    }

    #[test]
    fn owner_dependents_die_with_the_broker() {
        let broker = 0x_b0_0001_usize;
        let client = 0x_c1_0001_usize;
        let other_client = 0x_c1_0002_usize;
        register(broker, Kind::Broker, broker);
        register(client, Kind::Client, broker);
        register(other_client, Kind::Client, 0x_b0_0002);
        mark_owner_dependents_dead(broker);
        {
            let mut guard = TABLE.lock().unwrap_or_else(|p| p.into_inner());
            let m = guard.as_ref().unwrap();
            assert!(!m[&client].alive);
            assert!(
                m[&other_client].alive,
                "another broker's client is untouched"
            );
            assert!(
                m[&broker].alive,
                "the broker entry itself is not its own dependent"
            );
        }
        assert!(unregister(broker) && unregister(client) && unregister(other_client));
    }
}
