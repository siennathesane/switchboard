//! Per-protocol connection accounting for `GET /metrics`: a live gauge
//! plus a since-start total for every protocol the gateway can sniff.
//!
//! The gateway constructs a [`ConnGuard`] once a connection's protocol is
//! known (and supported); the guard bumps both counters on creation and
//! drops the gauge when the connection finishes — including panics and
//! early returns, because the bump is released in `Drop`.

use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::detect::Detected;

/// Protocol identities as they appear in `switchboard_connections{protocol=…}`.
pub const PROTOCOLS: [&str; 5] = ["amqp091", "amqp10", "mqtt", "stomp", "http"];

struct ConnStats {
    /// Currently open connections, per [`PROTOCOLS`] index.
    current: [AtomicU64; 5],
    /// Connections accepted since process start, per [`PROTOCOLS`] index.
    total: [AtomicU64; 5],
}

static STATS: OnceLock<ConnStats> = OnceLock::new();

fn stats() -> &'static ConnStats {
    STATS.get_or_init(|| ConnStats {
        current: Default::default(),
        total: Default::default(),
    })
}

fn index_of(protocol: Detected) -> usize {
    match protocol {
        Detected::Amqp091 => 0,
        Detected::Amqp10 => 1,
        Detected::Mqtt => 2,
        Detected::Stomp => 3,
        Detected::Http => 4,
    }
}

/// Bump the counters and hold the gauge slot until the connection ends.
/// The name identifies the protocol family assigned by first-byte
/// sniffing; a WebSocket upgrade is counted under `http` (its transport
/// is an HTTP request), consistent with the gateway's classification.
pub struct ConnGuard(usize);

impl ConnGuard {
    pub fn enter(protocol: Detected) -> Self {
        let i = index_of(protocol);
        stats().current[i].fetch_add(1, Ordering::Relaxed);
        stats().total[i].fetch_add(1, Ordering::Relaxed);
        ConnGuard(i)
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        stats().current[self.0].fetch_sub(1, Ordering::Relaxed);
    }
}

/// `(protocol, currently_open, accepted_total)` for every protocol.
pub fn snapshot() -> Vec<(&'static str, u64, u64)> {
    let s = stats();
    PROTOCOLS
        .iter()
        .enumerate()
        .map(|(i, name)| {
            (
                *name,
                s.current[i].load(Ordering::Relaxed),
                s.total[i].load(Ordering::Relaxed),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_balances_gauge() {
        assert_eq!(index_of(Detected::Amqp091), 0);
        assert_eq!(index_of(Detected::Http), 4);
        let before = snapshot();
        {
            let _g1 = ConnGuard::enter(Detected::Amqp091);
            let _g2 = ConnGuard::enter(Detected::Amqp091);
            let _g3 = ConnGuard::enter(Detected::Mqtt);
            let now = snapshot();
            assert_eq!(now[0].1, before[0].1 + 2);
            assert_eq!(now[2].1, before[2].1 + 1);
            assert_eq!(now[0].2, before[0].2 + 2);
            assert_eq!(now[2].2, before[2].2 + 1);
        }
        let after = snapshot();
        assert_eq!(after[0].1, before[0].1);
        assert_eq!(after[2].1, before[2].1);
        // Totals never go back down.
        assert_eq!(after[0].2, before[0].2 + 2);
    }
}
