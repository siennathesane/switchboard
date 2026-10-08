//! Steady-rate pacing for soak workloads.
//!
//! Soak load is deliberately *sustainable*, not saturating: each
//! workload emits at a fixed rate via a drifting-free ticker with
//! `MissedTickBehavior::Delay` (a stall must not turn into a burst —
//! bursts are what break systems that were otherwise fine).

use std::time::Duration;

use tokio::time::Instant;

/// Emits permission to proceed once every `1 / rate` seconds.
pub struct Pacer {
    period: Duration,
    next: Option<Instant>,
}

impl Pacer {
    pub fn new(rate_per_sec: f64) -> Self {
        let period = if rate_per_sec <= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(1.0 / rate_per_sec)
        };
        Pacer { period, next: None }
    }

    pub fn disabled(&self) -> bool {
        self.period.is_zero()
    }

    /// Wait until the next tick. First call returns immediately (phase
    /// anchored at first use, so workload startup doesn't stampede).
    pub async fn wait(&mut self) {
        if self.period.is_zero() {
            return;
        }
        match self.next {
            None => {
                self.next = Some(Instant::now() + self.period);
            }
            Some(at) => {
                tokio::time::sleep_until(at).await;
                let now = Instant::now();
                // Delay behavior: a late tick reschedules from now, so a
                // slow operation never accumulates a catch-up burst.
                let mut slot = at + self.period;
                while slot <= now {
                    slot += self.period;
                }
                self.next = Some(slot);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

#[tokio::test]
async fn paces_at_rate() {
    let mut p = Pacer::new(100.0); // 10ms period
    let t0 = std::time::Instant::now();
    p.wait().await; // immediate
    p.wait().await; // +10ms
    p.wait().await; // +20ms
    let el = t0.elapsed();
    assert!(el >= Duration::from_millis(18), "paced too fast: {el:?}");
    assert!(el < Duration::from_secs(2), "paced too slow: {el:?}");
}
}
