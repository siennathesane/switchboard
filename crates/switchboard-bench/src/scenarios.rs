//! Benchmark scenarios. Every throughput number is measured at a *receiver*
//! (consumer delivery, or publisher-confirm commit) rather than at the send
//! side, so client-side buffering can't inflate results.
//!
//! Flow control: publishers hold one semaphore permit per in-flight
//! message. Confirm scenarios release the permit when the broker acks;
//! fire-and-forget scenarios release it when a consumer receives — this
//! bounds client memory and measures the sustainable pipeline.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use lapin::message::Delivery;
use lapin::options::*;
use lapin::publisher_confirm::Confirmation;
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use serde_json::{json, Map, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use switchboard_bench::stats::{mean, percentile, Counter, RateSampler};

static FAILED: AtomicU64 = AtomicU64::new(0);

/// Benchmark configuration (CLI-derived).
#[derive(Clone)]
pub struct Bench {
    pub hosts: Vec<String>,
    pub user: String,
    pub password: String,
    pub duration: u64,
    pub warmup: u64,
    pub publishers: usize,
    pub size: usize,
    pub in_flight: usize,
    pub fanout_queues: usize,
    pub preload: u64,
}

impl Bench {
    fn uri(&self, host: &str) -> String {
        format!("amqp://{}:{}@{}/%2F", self.user, self.password, host)
    }

    fn host(&self, i: usize) -> String {
        self.hosts[i % self.hosts.len()].clone()
    }
}

async fn connect_uri(uri: &str) -> Result<Connection> {
    Ok(Connection::connect(uri, ConnectionProperties::default()).await?)
}

fn payload(size: usize) -> Vec<u8> {
    // Content is irrelevant to the broker (no compression); a cheap stable
    // pattern keeps construction off the hot path.
    let mut v = vec![0u8; size];
    for (i, b) in v.iter_mut().enumerate().step_by(4096) {
        *b = (i % 251) as u8;
    }
    v
}

fn persistent_props() -> BasicProperties {
    BasicProperties::default().with_delivery_mode(2)
}

async fn reset_queue(conn: &Connection, name: &str) -> Result<()> {
    // Durable queues survive across scenarios; delete + redeclare to start
    // empty. A delete of a missing queue 404s and closes its channel, so
    // each step gets a fresh one.
    {
        let ch = conn.create_channel().await?;
        let _ = ch.queue_delete(name, QueueDeleteOptions::default()).await;
    }
    let ch = conn.create_channel().await?;
    ch.queue_declare(name, QueueDeclareOptions { durable: true, ..Default::default() }, FieldTable::default())
        .await?;
    Ok(())
}

#[derive(Clone, Default)]
struct TaskCounters {
    /// Counted when the broker acks a publish (confirm scenarios).
    acks: Option<Arc<Counter>>,
    /// Counted when a consumer receives a message.
    received: Option<Arc<Counter>>,
    /// Counted at publish-call time (send attempts).
    sent: Option<Arc<Counter>>,
}

#[allow(clippy::too_many_arguments)]
async fn publisher_task(
    token: CancellationToken,
    uri: String,
    exchange: Option<String>,
    queue: String,
    payload: Arc<Vec<u8>>,
    sem: Arc<Semaphore>,
    counters: TaskCounters,
    wait_for_ack: bool,
) {
    // Connection/channel are re-established whenever they break (broker
    // restart, channel-level error): a benchmark must survive infra churn.
    loop {
        if token.is_cancelled() {
            return;
        }
        let conn = match connect_uri(&uri).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("bench: publisher connect failed ({e}); retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let Ok(ch) = conn.create_channel().await else { continue };
        // Ack-counting publishers require confirm mode; without it the
        // broker never sends Basic.Ack and every awaited confirm times
        // out.
        if wait_for_ack {
            let _ = ch.confirm_select(lapin::options::ConfirmSelectOptions::default()).await;
        }
        let props = persistent_props();
        let size = payload.len() as u64;
        loop {
            if token.is_cancelled() {
                return;
            }
            let permit: OwnedSemaphorePermit = match sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            if token.is_cancelled() {
                return;
            }
            let confirm = match &exchange {
                Some(x) => ch.basic_publish(x, "", BasicPublishOptions::default(), &payload, props.clone()).await,
                None => ch.basic_publish("", &queue, BasicPublishOptions::default(), &payload, props.clone()).await,
            };
            match confirm {
                Ok(pc) => {
                    if let Some(c) = &counters.sent {
                        c.add(1, size);
                    }
                    if wait_for_ack {
                        let counters = counters.clone();
                        let sem = sem.clone();
                        tokio::spawn(async move {
                            match tokio::time::timeout(Duration::from_secs(5), pc).await {
                                Ok(Ok(Confirmation::Ack(_))) => {
                                    if let Some(c) = &counters.acks {
                                        c.add(1, size);
                                    }
                                }
                                _ => {
                                    FAILED.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            drop(permit);
                        });
                    } else {
                        drop(permit); // consumers release pipeline credits
                    }
                }
                Err(e) => {
                    drop(permit);
                    if token.is_cancelled() {
                        return;
                    }
                    eprintln!("bench: publish failed ({e}); reconnecting");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    break; // fresh connection + channel
                }
            }
        }
    }
}

/// A no-ack consumer task: counts deliveries and releases pipeline permits.
async fn consumer_task(
    token: CancellationToken,
    uri: String,
    queue: String,
    size: usize,
    received: Arc<Counter>,
    release: Option<Arc<Semaphore>>,
) {
    let conn = loop {
        if token.is_cancelled() {
            return;
        }
        match connect_uri(&uri).await {
            Ok(c) => break c,
            Err(e) => {
                eprintln!("bench: consumer connect failed ({e}); retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };
    let mut consumer = loop {
        // A channel that took a channel-level error (e.g. a 404 racing a
        // concurrent re-declare) is closed for good: retry on a fresh one.
        let ch = match conn.create_channel().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("bench: consumer channel to {queue} failed ({e}); retrying");
                tokio::time::sleep(Duration::from_millis(300)).await;
                continue;
            }
        };
        match ch
            .basic_consume(&queue, "", BasicConsumeOptions { no_ack: true, ..Default::default() }, FieldTable::default())
            .await
        {
            Ok(c) => break c,
            Err(e) => {
                eprintln!("bench: consumer subscribe to {queue} failed ({e}); retrying");
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
    };
    loop {
        let delivery: Delivery = tokio::select! {
            _ = token.cancelled() => return,
            d = futures_lite::StreamExt::next(&mut consumer) => match d {
                Some(Ok(d)) => d,
                Some(Err(e)) => { eprintln!("bench: consume error: {e}"); return }
                None => return,
            }
        };
        let _ = delivery;
        received.add(1, size as u64);
        if let Some(sem) = &release {
            sem.add_permits(1);
        }
    }
}

/// Measurement frame: warmup (discarded), then a measured window over one
/// counter.
struct Window {
    token: CancellationToken,
    counter: Arc<Counter>,
    sampler: Option<RateSampler>,
}

impl Window {
    fn new(counter: Arc<Counter>) -> Self {
        Window { token: CancellationToken::new(), counter, sampler: None }
    }

    async fn warmup(&self, secs: u64) {
        tokio::time::sleep(Duration::from_secs(secs)).await;
    }

    async fn begin(&mut self) {
        self.counter.reset();
        FAILED.store(0, Ordering::Relaxed);
        self.sampler = Some(RateSampler::start(self.counter.clone()));
    }

    async fn await_duration(&self, secs: u64) {
        tokio::time::sleep(Duration::from_secs(secs)).await;
    }

    async fn end(mut self) -> (u64, Vec<u64>, Vec<u64>, u64) {
        self.token.cancel();
        let (total, rates, bytes) = match self.sampler.take() {
            Some(s) => s.stop().await,
            None => (0, vec![], vec![]),
        };
        let failed = FAILED.load(Ordering::Relaxed);
        (total, rates, bytes, failed)
    }
}

fn summary(total: u64, rates: &[u64], bytes_total: u64, secs: u64) -> (u64, f64, u64) {
    let secs = secs.max(1) as f64;
    let avg = (total as f64 / secs).round() as u64;
    let mib = ((bytes_total as f64) / 1024.0 / 1024.0 / secs * 100.0).round() / 100.0;
    let peak = rates.iter().copied().max().unwrap_or(0);
    (avg, mib, peak)
}

// ---------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------

/// `publish`: fire-and-forget ingest into one durable queue, drained by
/// no-ack consumers. Sustainable end-to-end pipeline of the write path
/// without per-message commit round trips. Measured at the consumers.
pub async fn publish(b: &Bench) -> Result<Value> {
    let setup = connect_uri(&b.uri(&b.host(0))).await?;
    reset_queue(&setup, "bench.pub").await?;
    drop(setup);

    let received = Arc::new(Counter::new());
    let sent = Arc::new(Counter::new());
    let release = Arc::new(Semaphore::new(b.in_flight));
    let payload = Arc::new(payload(b.size));
    let mut window = Window::new(received.clone());
    let mut tasks = Vec::new();

    for c in 0..2 {
        tasks.push(tokio::spawn(consumer_task(
            window.token.clone(),
            b.uri(&b.host(c)),
            "bench.pub".into(),
            b.size,
            received.clone(),
            Some(release.clone()),
        )));
    }
    for p in 0..b.publishers {
        tasks.push(tokio::spawn(publisher_task(
            window.token.clone(),
            b.uri(&b.host(p)),
            None,
            "bench.pub".into(),
            payload.clone(),
            release.clone(),
            TaskCounters { sent: Some(sent.clone()), ..Default::default() },
            false,
        )));
    }

    window.warmup(b.warmup).await;
    window.begin().await;
    window.await_duration(b.duration).await;
    let (total, rates, bytes, _) = window.end().await;
    for t in tasks {
        t.abort();
    }
    let (avg, mib, peak) = summary(total, &rates, bytes.iter().sum(), b.duration);
    Ok(json!({
        "throughput_msg_s": avg,
        "throughput_mib_s": mib,
        "peak_msg_s": peak,
        "delivered": total,
        "sent": sent.msgs(),
    }))
}

/// `confirm`: one durable queue, per-message publisher confirms — one raft
/// commit per message. Measures the single-log commit rate with a
/// pipelined window. Measured at acks.
pub async fn confirm(b: &Bench) -> Result<Value> {
    sharded_with_queues(b, 1, "bench.confirm").await
}

/// `sharded`: K queues with K = node count, publishers round-robin across
/// them, confirms on. Queues hash to different shard groups, so this
/// measures how commit throughput scales with the cluster.
pub async fn sharded(b: &Bench) -> Result<Value> {
    let k = b.hosts.len();
    sharded_with_queues(b, k, "bench.shard").await
}

async fn sharded_with_queues(b: &Bench, k: usize, prefix: &str) -> Result<Value> {
    let setup = connect_uri(&b.uri(&b.host(0))).await?;
    let mut queues = Vec::new();
    for i in 0..k {
        let q = format!("{prefix}.{i}");
        reset_queue(&setup, &q).await?;
        queues.push(q);
    }
    drop(setup);

    let acks = Arc::new(Counter::new());
    let received = Arc::new(Counter::new());
    let sent = Arc::new(Counter::new());
    let sem = Arc::new(Semaphore::new(b.in_flight));
    let payload = Arc::new(payload(b.size));
    let mut window = Window::new(acks.clone());
    let mut tasks = Vec::new();

    for (i, q) in queues.iter().enumerate() {
        tasks.push(tokio::spawn(consumer_task(
            window.token.clone(),
            b.uri(&b.host(i)),
            q.clone(),
            b.size,
            received.clone(),
            None,
        )));
    }
    for p in 0..b.publishers {
        tasks.push(tokio::spawn(publisher_task(
            window.token.clone(),
            b.uri(&b.host(p)),
            None,
            queues[p % queues.len()].clone(),
            payload.clone(),
            sem.clone(),
            TaskCounters { acks: Some(acks.clone()), sent: Some(sent.clone()), ..Default::default() },
            true,
        )));
    }

    window.warmup(b.warmup).await;
    window.begin().await;
    window.await_duration(b.duration).await;
    let (total, rates, bytes, failed) = window.end().await;
    for t in tasks {
        t.abort();
    }
    let (avg, mib, peak) = summary(total, &rates, bytes.iter().sum(), b.duration);
    Ok(json!({
        "throughput_msg_s": avg,
        "throughput_mib_s": mib,
        "peak_msg_s": peak,
        "confirmed": total,
        "failed": failed,
        "queues": queues.len(),
        "delivered_total": received.msgs(),
    }))
}

/// `fanout`: one publish to a fanout exchange bound to 3 durable queues,
/// confirms on — multi-destination publishes take the meta-log total-order
/// path. Measured at acks (publishes/s) and at consumers (deliveries/s).
pub async fn fanout(b: &Bench) -> Result<Value> {
    let k = b.fanout_queues;
    let setup = connect_uri(&b.uri(&b.host(0))).await?;
    let sch = setup.create_channel().await?;
    sch.exchange_declare(
        "bench.fx",
        lapin::ExchangeKind::Fanout,
        ExchangeDeclareOptions { durable: true, ..Default::default() },
        FieldTable::default(),
    )
    .await?;
    let mut queues = Vec::new();
    for i in 0..k {
        let q = format!("bench.f.{i}");
        reset_queue(&setup, &q).await?;
        let bind_ch = setup.create_channel().await?;
        bind_ch.queue_bind(&q, "bench.fx", "", QueueBindOptions::default(), FieldTable::default()).await?;
        queues.push(q);
    }
    drop(setup);

    let acks = Arc::new(Counter::new());
    let received = Arc::new(Counter::new());
    let sent = Arc::new(Counter::new());
    let sem = Arc::new(Semaphore::new(b.in_flight));
    let payload = Arc::new(payload(b.size));
    let mut window = Window::new(acks.clone());
    let mut tasks = Vec::new();

    for (i, q) in queues.iter().enumerate() {
        tasks.push(tokio::spawn(consumer_task(
            window.token.clone(),
            b.uri(&b.host(i)),
            q.clone(),
            b.size,
            received.clone(),
            None,
        )));
    }
    // One publisher: multi-destination publishes are serialized by design
    // (the meta-log fanout order).
    tasks.push(tokio::spawn(publisher_task(
        window.token.clone(),
        b.uri(&b.host(0)),
        Some("bench.fx".into()),
        String::new(),
        payload.clone(),
        sem.clone(),
        TaskCounters { acks: Some(acks.clone()), sent: Some(sent.clone()), ..Default::default() },
        true,
    )));

    window.warmup(b.warmup).await;
    window.begin().await;
    window.await_duration(b.duration).await;
    let (acked, rates, bytes, failed) = window.end().await;
    for t in tasks {
        t.abort();
    }
    let secs = b.duration.max(1) as f64;
    let delivered_total = received.msgs();
    let (avg, mib, peak) = summary(acked, &rates, bytes.iter().sum(), b.duration);
    Ok(json!({
        "throughput_msg_s": avg,
        "throughput_mib_s": mib,
        "peak_msg_s": peak,
        "confirmed": acked,
        "failed": failed,
        "delivered_total": delivered_total,
        "delivered_msg_s": (delivered_total as f64 / secs).round() as u64,
        "queues": queues.len(),
    }))
}

async fn queue_depth(b: &Bench, queue: &str) -> Result<u64> {
    let conn = connect_uri(&b.uri(&b.host(0))).await?;
    let ch = conn.create_channel().await?;
    let q = ch
        .queue_declare(queue, QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default())
        .await?;
    let _ = ch.close(0, "");
    drop(conn);
    Ok(u64::from(q.message_count()))
}

/// `drain`: preload a durable queue, then empty it with no-ack consumers.
/// Pure delivery throughput without concurrent publishes.
pub async fn drain(b: &Bench) -> Result<Value> {
    let setup = connect_uri(&b.uri(&b.host(0))).await?;
    reset_queue(&setup, "bench.drain").await?;
    drop(setup);

    // Preload without consumers: fire-and-forget; the broker applies as
    // fast as it can.
    let sent = Arc::new(Counter::new());
    let payload = Arc::new(payload(b.size));
    let preload_token = CancellationToken::new();
    let sem = Arc::new(Semaphore::new(4096));
    {
        let t = preload_token.clone();
        tokio::spawn(publisher_task(
            t,
            b.uri(&b.host(0)),
            None,
            "bench.drain".into(),
            payload,
            sem,
            TaskCounters { sent: Some(sent.clone()), ..Default::default() },
            false,
        ));
    }
    let target = b.preload;
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let depth = queue_depth(b, "bench.drain").await?;
        if depth >= target || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    preload_token.cancel();
    let preloaded = queue_depth(b, "bench.drain").await?;
    if preloaded == 0 {
        bail!("drain: preloading produced no messages");
    }

    // Drain with 3 no-ack consumers spread across nodes.
    let received = Arc::new(Counter::new());
    let token = CancellationToken::new();
    let mut tasks = Vec::new();
    for c in 0..3 {
        tasks.push(tokio::spawn(consumer_task(
            token.clone(),
            b.uri(&b.host(c)),
            "bench.drain".into(),
            b.size,
            received.clone(),
            None,
        )));
    }
    let start = Instant::now();
    loop {
        let got = received.msgs();
        if got >= preloaded || start.elapsed() > Duration::from_secs(180) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    token.cancel();
    for t in tasks {
        t.abort();
    }
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let drained = received.msgs().min(preloaded);
    Ok(json!({
        "throughput_msg_s": (drained as f64 / elapsed).round() as u64,
        "throughput_mib_s": ((drained as f64 * b.size as f64) / 1024.0 / 1024.0 / elapsed * 100.0).round() / 100.0,
        "preloaded": preloaded,
        "drained": drained,
        "elapsed_s": (elapsed * 100.0).round() / 100.0,
    }))
}

/// `latency`: closed-loop request/reply over two queues (publish request →
/// echo consumer republishes to the reply queue → client consumes it).
/// One request in flight; reports round-trip percentiles through the raft
/// write path.
pub async fn latency(b: &Bench) -> Result<Value> {
    let setup = connect_uri(&b.uri(&b.host(0))).await?;
    reset_queue(&setup, "bench.lat.req").await?;
    reset_queue(&setup, "bench.lat.rep").await?;
    drop(setup);

    let payload = Arc::new(payload(b.size));
    let token = CancellationToken::new();

    // Echo service: req -> rep.
    {
        let t = token.clone();
        let uri = b.uri(&b.host(0));
        let payload = payload.clone();
        tokio::spawn(async move {
            let conn = match connect_uri(&uri).await {
                Ok(c) => c,
                Err(e) => { eprintln!("latency: echo connect failed: {e}"); return },
            };
            let in_ch = match conn.create_channel().await {
                Ok(c) => c,
                Err(e) => { eprintln!("latency: echo in-channel failed: {e}"); return },
            };
            let out_ch = match conn.create_channel().await {
                Ok(c) => c,
                Err(e) => { eprintln!("latency: echo out-channel failed: {e}"); return },
            };
            match in_ch
                .basic_consume("bench.lat.req", "", BasicConsumeOptions { no_ack: true, ..Default::default() }, FieldTable::default())
                .await
            {
                Ok(mut consumer) => {
                    loop {
                        let d: Delivery = tokio::select! {
                            _ = t.cancelled() => return,
                            d = futures_lite::StreamExt::next(&mut consumer) => match d {
                                Some(Ok(d)) => d,
                                Some(Err(e)) => { eprintln!("latency: echo consume error: {e}"); return }
                                None => return,
                            }
                        };
                        let _ = d;
                        if let Err(e) = out_ch
                            .basic_publish("", "bench.lat.rep", BasicPublishOptions::default(), &payload, persistent_props())
                            .await
                        {
                            eprintln!("latency: echo republish failed: {e}");
                            return;
                        }
                    }
                }
                Err(e) => { eprintln!("latency: echo subscribe failed: {e}"); return },
            }
        });
    }

    let rtts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let conn = connect_uri(&b.uri(&b.host(0))).await?;
    let pub_ch = conn.create_channel().await?;
    let sub_ch = conn.create_channel().await?;
    let mut replies = sub_ch
        .basic_consume("bench.lat.rep", "", BasicConsumeOptions { no_ack: true, ..Default::default() }, FieldTable::default())
        .await?;
    let deadline = Instant::now() + Duration::from_secs(b.duration + 10);
    let measure_from = Instant::now() + Duration::from_secs(b.warmup);
    let mut measured = 0u64;
    // A shared host can stall one probe past the 5 s window without the
    // echo path being broken; only give up when replies stop arriving.
    let mut consecutive_misses = 0u32;
    loop {
        if Instant::now() > deadline || measured >= 200_000 {
            break;
        }
        let t0 = Instant::now();
        pub_ch
            .basic_publish("", "bench.lat.req", BasicPublishOptions::default(), &payload, persistent_props())
            .await?;
        let got = tokio::time::timeout(Duration::from_secs(5), futures_lite::StreamExt::next(&mut replies)).await;
        match got {
            Ok(Some(Ok(_))) => consecutive_misses = 0,
            _ => {
                consecutive_misses += 1;
                if consecutive_misses >= 3 {
                    bail!("latency: three consecutive probes got no reply (timeout or consumer error)");
                }
                continue;
            }
        }
        let rtt = t0.elapsed().as_micros() as u64;
        if Instant::now() >= measure_from {
            rtts.lock().unwrap().push(rtt);
            measured += 1;
        }
    }
    token.cancel();

    let mut samples = rtts.lock().unwrap().clone();
    samples.sort_unstable();
    if samples.is_empty() {
        bail!("latency: no samples");
    }
    Ok(json!({
        "p50_us": percentile(&samples, 50.0),
        "p90_us": percentile(&samples, 90.0),
        "p99_us": percentile(&samples, 99.0),
        "p999_us": percentile(&samples, 99.9),
        "max_us": samples[samples.len() - 1],
        "mean_us": mean(&samples),
        "count": samples.len(),
    }))
}

/// Run the named scenario(s). Returns {scenario: result-map}.
pub async fn run(b: &Bench, scenario: &str) -> Result<Map<String, Value>> {
    let names: Vec<&str> = match scenario {
        "all" => vec!["publish", "confirm", "sharded", "fanout", "drain", "latency"],
        other => vec![other],
    };
    let mut out = Map::new();
    for name in names {
        eprintln!("bench: running scenario {name}");
        let v = match name {
            "publish" => publish(b).await?,
            "confirm" => confirm(b).await?,
            "sharded" => sharded(b).await?,
            "fanout" => fanout(b).await?,
            "drain" => drain(b).await?,
            "latency" => latency(b).await?,
            other => bail!("unknown scenario {other}"),
        };
        eprintln!("bench: {name} done");
        out.insert(name.to_string(), v);
    }
    Ok(out)
}
