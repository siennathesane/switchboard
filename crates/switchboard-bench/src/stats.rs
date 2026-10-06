//! Benchmark statistics: rate sampling over atomic counters and latency
//! percentiles.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A monotonic message counter shared between load tasks and the sampler.
#[derive(Debug, Default)]
pub struct Counter {
    msgs: AtomicU64,
    bytes: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self, msgs: u64, bytes: u64) {
        self.msgs.fetch_add(msgs, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.msgs.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
    }

    pub fn msgs(&self) -> u64 {
        self.msgs.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Samples a counter once per second, keeping per-second rates.
pub struct RateSampler {
    counter: Arc<Counter>,
    samples: Arc<Mutex<Vec<u64>>>,
    byte_samples: Arc<Mutex<Vec<u64>>>,
    stop: Arc<AtomicU64>,
    handle: tokio::task::JoinHandle<()>,
}

impl RateSampler {
    /// Samples `counter` every second until [`Self::stop`].
    pub fn start(counter: Arc<Counter>) -> Self {
        let samples = Arc::new(Mutex::new(Vec::new()));
        let byte_samples = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicU64::new(0));
        let handle = {
            let samples = samples.clone();
            let byte_samples = byte_samples.clone();
            let stop = stop.clone();
            let counter = counter.clone();
            tokio::spawn(async move {
                let mut last = counter.msgs();
                let mut last_bytes = counter.bytes();
                while stop.load(Ordering::Relaxed) == 0 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let now = counter.msgs();
                    let now_bytes = counter.bytes();
                    let rate = now.saturating_sub(last);
                    let bytes = now_bytes.saturating_sub(last_bytes);
                    last = now;
                    last_bytes = now_bytes;
                    samples.lock().unwrap().push(rate);
                    byte_samples.lock().unwrap().push(bytes);
                }
            })
        };
        RateSampler { counter, samples, byte_samples, stop, handle }
    }

    /// Stops sampling; returns (total msgs over the window, per-second
    /// rates, per-second byte rates).
    pub async fn stop(self) -> (u64, Vec<u64>, Vec<u64>) {
        self.stop.store(1, Ordering::Relaxed);
        self.handle.await.ok();
        let total = self.counter.msgs();
        let samples = self.samples.lock().unwrap().clone();
        let bytes = self.byte_samples.lock().unwrap().clone();
        (total, samples, bytes)
    }
}

pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn mean(v: &[u64]) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.iter().sum::<u64>() / v.len() as u64
}
