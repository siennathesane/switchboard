//! Test-tempo scaling: shrink fixed delays without changing ordering.
//!
//! Production reads no env var and every [`tempo`] call is the identity.
//! Test binaries set `SB_TIME_SCALE` (e.g. `50`) once at startup; every
//! wait that goes through [`tempo::scale`] then runs 50× shorter while keeping
//! the exact same scheduling order — ticks still serialize the same way,
//! just faster. Openraft election/heartbeat intervals pass through the
//! same scaling so formation converges in milliseconds under test.
//!
//! Determinism note: this is a uniform time-warp, not a fake clock with
//! jumps — no deadline can fire "before" an earlier one, so tests keep
//! their serialization guarantees.

use std::time::Duration;

fn factor_milli() -> u64 {
    let parsed = std::env::var("SB_TIME_SCALE")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|f| *f >= 1.0)
        .unwrap_or(1.0);
    ((parsed * 1000.0).round() as u64).max(1)
}

/// Scale a duration by the configured test tempo. Identity in production.
pub fn scale(d: Duration) -> Duration {
    let f = factor_milli();
    if f == 1000 {
        return d;
    }
    // SB_TIME_SCALE=50 ⇒ durations run 50× shorter.
    d.mul_f64(1000.0 / f as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_without_env() {
        // Unit-test process has no SB_TIME_SCALE: identity. (Race-prone
        // env mutation lives in the integration binaries; nextest runs
        // each test as its own process.)
        assert_eq!(scale(Duration::from_secs(60)), Duration::from_secs(60));
    }
}
