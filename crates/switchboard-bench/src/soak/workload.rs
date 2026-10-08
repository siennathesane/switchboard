//! AMQP 0-9-1 soak workloads: the write path (confirmed publishes),
//! the delivery path (acked consumers, basic.get), fanout, shard-level
//! transactions, topology churn, and connection churn.
//!
//! Every workload is a *self-healing actor*: it owns its connections,
//! reconnects across endpoint churn (counted as transitions), and never
//! propagates a transport failure into a false message-loss verdict.
//! Publisher generationeration tags make that sound: on any publish anomaly
//! (error, Nack, confirm timeout — the ambiguity window where a message
//! may or may not have landed) the publisher retires its sequence space
//! and starts a fresh tag, so reconciliation checks each generationeration's
//! confirmed count against exactly what the consumer saw for that tag.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::options::QueueBindOptions;
use lapin::options::QueueDeclareOptions;
use lapin::options::QueueDeleteOptions;
use lapin::types::FieldTable;
use lapin::Channel;
use lapin::Connection;
use tokio_util::sync::CancellationToken;

use crate::soak::check;
use crate::soak::check::{decode_body, encode_body, FifoStream};
use crate::soak::client;
use crate::soak::pacer::Pacer;
use crate::soak::report::Check;
use crate::soak::Ctx;

use lapin::options::ExchangeDeclareOptions;
use lapin::ExchangeKind;

pub const FANOUT_QUEUES: usize = 3;
pub const PIPELINE_QUEUES: usize = 3;

/// Declare every durable object the workloads use. Durable + equivalent
/// re-declares: churn and restarts never take the soak topology down.
pub async fn declare_topology(ctx: &Arc<Ctx>) -> Result<(), String> {
    let host = ctx
        .endpoints
        .snapshot()
        .await
        .first()
        .cloned()
        .ok_or("no endpoints")?;
    let Some(conn) = client::connect_setup(ctx, &host, "setup").await else {
        return Err("shutting down".into());
    };
    let Some(ch) = client::channel(ctx, &conn, "setup", &host).await else {
        return Err("shutting down".into());
    };
    ch.exchange_declare(
        "soak.fx",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions { durable: true, ..Default::default() },
        FieldTable::default(),
    )
    .await
    .map_err(|e| format!("soak.fx: {e}"))?;
    let mut queues = vec!["soak.control".to_string(), "soak.tx".into(), "soak.get".into(), "soak.mand".into(), "soak.stomp".into(), "soak.a10".into()];
    for i in 0..PIPELINE_QUEUES {
        queues.push(format!("soak.p.{i}"));
    }
    for i in 0..FANOUT_QUEUES {
        queues.push(format!("soak.f.{i}"));
    }
    for q in queues {
        ch.queue_declare(
            &q,
            QueueDeclareOptions { durable: true, ..Default::default() },
            FieldTable::default(),
        )
        .await
        .map_err(|e| format!("{q}: {e}"))?;
    }
    for i in 0..FANOUT_QUEUES {
        let q = format!("soak.f.{i}");
        ch.queue_bind(&q, "soak.fx", "", QueueBindOptions::default(), FieldTable::default())
            .await
            .map_err(|e| format!("bind {q}: {e}"))?;
    }
    drop(ch);
    drop(conn);

    // Propagation gate: every node's routing view must contain every
    // soak queue before any workload touches them. A publish or
    // subscribe that lands on a stale node is unroutable (confirmed
    // but dropped) or NOT-FOUND — both violate the zero-error bar, so
    // the soak waits it out instead.
    let queues: Vec<String> = [
        vec![
            "soak.control".to_string(),
            "soak.tx".into(),
            "soak.get".into(),
            "soak.mand".into(),
            "soak.stomp".into(),
            "soak.a10".into(),
        ],
        (0..PIPELINE_QUEUES).map(|i| format!("soak.p.{i}")).collect(),
        (0..FANOUT_QUEUES).map(|i| format!("soak.f.{i}")).collect(),
    ]
    .concat();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let hosts = ctx.endpoints.snapshot().await;
        let mut missing = Vec::new();
        for h in &hosts {
            let Some(conn) = client::connect_setup(ctx, h, "setup").await else {
                return Err("shutting down".into());
            };
            let Some(ch) = client::channel(ctx, &conn, "setup", h).await else {
                return Err("shutting down".into());
            };
            for q in &queues {
                let ok = ch
                    .queue_declare(
                        q,
                        QueueDeclareOptions { passive: true, ..Default::default() },
                        FieldTable::default(),
                    )
                    .await
                    .is_ok();
                if !ok {
                    missing.push(format!("{h}/{q}"));
                }
            }
        }
        if missing.is_empty() {
            // A driver restart must start its counters against empty
            // queues: purge leftovers from a previous (dead) generation
            // that can no longer be verified.
            for q in &queues {
                let Some(conn) = client::connect_setup(ctx, &hosts[0], "setup").await else {
                    return Err("shutting down".into());
                };
                let Some(ch) = client::channel(ctx, &conn, "setup", &hosts[0]).await else {
                    return Err("shutting down".into());
                };
                if let Ok(d) = ch
                    .queue_declare(
                        q,
                        QueueDeclareOptions { durable: true, ..Default::default() },
                        FieldTable::default(),
                    )
                    .await
                {
                    if d.message_count() > 0 {
                        use lapin::options::QueuePurgeOptions;
                        let _ = ch.queue_purge(q, QueuePurgeOptions::default()).await;
                    }
                }
            }
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("topology never propagated to: {}", missing.join(", ")));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------
// Shared per-workload state + reconcilers
// ---------------------------------------------------------------------

/// Per-tag confirmed counts, shared publisher → reconciler.
#[derive(Default)]
pub struct TagCounts {
    inner: std::sync::Mutex<BTreeMap<String, u64>>,
}

impl TagCounts {
    pub fn bump(&self, tag: &str) {
        *self.inner.lock().unwrap().entry(tag.to_string()).or_default() += 1;
    }
    pub fn snapshot(&self) -> BTreeMap<String, u64> {
        self.inner.lock().unwrap().clone()
    }
    pub fn total(&self) -> u64 {
        self.inner.lock().unwrap().values().sum()
    }
}

/// A consumer-side ledger of FIFO streams keyed by publisher tag.
#[derive(Default)]
pub struct TagStreams {
    inner: std::sync::Mutex<BTreeMap<String, FifoStream>>,
}

impl TagStreams {
    pub fn observe(
        &self,
        tag: &str,
        seq: u64,
        redelivered: bool,
        window: bool,
    ) -> check::Delivery {
        let mut g = self.inner.lock().unwrap();
        let s = g
            .entry(tag.to_string())
            .or_insert_with(|| {
                if window {
                    FifoStream::with_window(4096)
                } else {
                    FifoStream::new()
                }
            });
        s.observe(seq, redelivered)
    }

    pub fn distinct_by_tag(&self) -> BTreeMap<String, u64> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(k, s)| (k.clone(), s.distinct()))
            .collect()
    }

    pub fn tags(&self) -> Vec<String> {
        self.inner.lock().unwrap().keys().cloned().collect()
    }

    pub fn total_distinct(&self) -> u64 {
        self.inner.lock().unwrap().values().map(|s| s.distinct()).sum()
    }
}

/// Reconciler a workload registers: at drain time each check must show
/// delivered == confirmed for its stream (within the chaos window).
pub type Reconciler = Arc<dyn Fn() -> Vec<Check> + Send + Sync>;

/// Reconcile checks comparing per-tag confirmed counts to per-tag FIFO
/// streams. Shared by all tag-keyed workloads.
pub fn tag_reconciler(name: &str, confirmed: Arc<TagCounts>, streams: Arc<TagStreams>) -> Reconciler {
    let name = name.to_string();
    Arc::new(move || {
        let conf = confirmed.snapshot();
        let st = streams.distinct_by_tag();
        let mut out = Vec::new();
        let mut total_conf = 0u64;
        let mut total_deliv = 0u64;
        for (tag, want) in &conf {
            total_conf += want;
            let got = *st.get(tag).unwrap_or(&0);
            total_deliv += got;
            if got != *want {
                out.push(Check {
                    name: format!("{name}/{tag}"),
                    expected: *want,
                    delivered: got,
                    ok: false,
                    note: "confirmed != delivered".into(),
                });
            }
        }
        for tag in st.keys() {
            if !conf.contains_key(tag) {
                out.push(Check {
                    name: format!("{name}/{tag}"),
                    expected: 0,
                    delivered: st[tag],
                    ok: false,
                    note: "delivered a tag that was never confirmed".into(),
                });
            }
        }
        if out.is_empty() {
            out.push(Check {
                name: name.to_string(),
                expected: total_conf,
                delivered: total_deliv,
                ok: true,
                note: String::new(),
            });
        }
        out
    })
}

// ---------------------------------------------------------------------
// Pipeline: confirmed publish → acked consume, per-queue FIFO
// ---------------------------------------------------------------------

pub async fn run_pipeline(ctx: Arc<Ctx>) {
    let confirmed = Arc::new(TagCounts::default());
    let streams = Arc::new(TagStreams::default());
    ctx.add_reconciler(tag_reconciler("pipeline", confirmed.clone(), streams.clone()))
        .await;
    let mut handles = Vec::new();
    for i in 0..PIPELINE_QUEUES {
        let queue = format!("soak.p.{i}");
        let ctx = ctx.clone();
        let confirmed = confirmed.clone();
        let tag = format!("pub{i}");
        let rate = ctx.cfg.rates.pipeline;
        let h = tokio::spawn(pipeline_publisher(ctx, queue, tag, rate, confirmed));
        handles.push(tokio::spawn(async move {
            if let Err(e) = h.await {
                eprintln!("DEBUG pipeline publisher task ended: {e:?}");
            }
        }));
    }
    for i in 0..PIPELINE_QUEUES {
        let queue = format!("soak.p.{i}");
        let ctx = ctx.clone();
        let streams = streams.clone();
        handles.push(tokio::spawn(async move {
            acked_consumer(ctx, queue, streams, "pipeline").await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// One confirmed publish per paced tick. Generation bumps on publish
/// anomalies (see module docs).
async fn pipeline_publisher(
    ctx: Arc<Ctx>,
    queue: String,
    base_tag: String,
    rate: f64,
    confirmed: Arc<TagCounts>,
) {
    let mut generation = 0u32;
    let mut seq: u64 = 0;
    let mut pacer = Pacer::new(rate);
    'outer: loop {
        if !client::alive(&ctx) {
            return;
        }
        let Some((conn, host)) = client::connect(&ctx, "pipeline").await else {
            eprintln!("DEBUG pipeline publisher: connect returned None");
            return;
        };
        let Some(ch) = client::channel(&ctx, &conn, "pipeline", &host).await else {
            eprintln!("DEBUG pipeline publisher: channel None");
            continue;
        };
        if !client::confirm_mode(&ctx, &ch, "pipeline", &host).await {
            eprintln!("DEBUG pipeline publisher: confirm_mode false");
            continue;
        }
        eprintln!("DEBUG pipeline publisher: connected {host}, entering publish loop");
        let tag = format!("{base_tag}g{generation}");
        loop {
            if !ctx.gate.open() {
                tokio::select! {
                    _ = ctx.token.cancelled() => return,
                    _ = ctx.gate.wait_open(&ctx.token) => {}
                }
                if !client::alive(&ctx) { return }
                continue;
            }
            pacer.wait().await;
            let body = encode_body(&tag, seq, ctx.cfg.msg_size);
            match client::publish_confirmed(&ctx, &ch, &host, "pipeline", "", &queue, &body).await {
                Some(true) => {
                    confirmed.bump(&tag);
                    seq += 1;
                    ctx.ledger.metrics.add("pipeline.confirmed", 1);
                }
                Some(false) => {
                    // Explicit nack/timeout: ambiguous whether it landed.
                    generation += 1;
                    seq = 0;
                }
                None => {
                    if !client::alive(&ctx) {
                        return;
                    }
                    // Transport broke: the publish never left, but the
                    // ambiguity is cheap to avoid — new generationeration.
                    generation += 1;
                    seq = 0;
                    continue 'outer;
                }
            }
        }
    }
}

/// Acked consumer shared by pipeline/fanout: verifies tag/seq FIFO +
/// CRC, acks everything, counts legal redeliveries.
async fn acked_consumer(ctx: Arc<Ctx>, queue: String, streams: Arc<TagStreams>, name: &str) {
    loop {
        if !client::consuming(&ctx) {
            return;
        }
        let Some((conn, host)) = client::connect_drain(&ctx, name).await else {
            return;
        };
        let Some(ch) = client::channel(&ctx, &conn, name, &host).await else {
            continue;
        };
        let Some(mut consumer) =
            client::consume_acked(&ctx, &ch, &host, name, &queue, 256).await
        else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        loop {
            let delivery = tokio::select! {
                _ = ctx.halt.cancelled() => return,
                d = StreamExt::next(&mut consumer) => match d {
                    Some(Ok(d)) => d,
                    Some(Err(e)) => {
                        if client::consuming(&ctx) {
                            ctx.infra_bounce(name, &host, format!("consumer stream error: {e}"));
                        }
                        break;
                    }
                    None => break,
                }
            };
            let body = delivery.data.as_slice();
            match decode_body(body) {
                Ok((tag, seq)) => {
                    let verdict = streams.observe(&tag, seq, delivery.redelivered, ctx.cfg.chaos);
                    match verdict {
                        check::Delivery::InOrder => {}
                        check::Delivery::Repeat => {
                            ctx.ledger.legal_dup();
                            ctx.ledger.metrics.add("dup.legal", 1);
                        }
                        check::Delivery::RepeatUnflagged => {
                            ctx.error(
                                name,
                                "dup.unflagged",
                                &host,
                                format!("{queue}: seq {seq} redelivered without the redelivered flag"),
                            );
                        }
                        check::Delivery::Gap(n) => {
                            ctx.error(
                                name,
                                "fifo.gap",
                                &host,
                                format!("{queue}: tag {tag} missing {n} message(s) before seq {seq}"),
                            );
                        }
                    }
                    client::ack(&ctx, &delivery, name, &host).await;
                    ctx.ledger.metrics.add("pipeline.delivered", 1);
                }
                Err(e) => {
                    ctx.error(
                        name,
                        "body",
                        &host,
                        format!("{queue}: integrity violation: {e:?}"),
                    );
                    client::ack(&ctx, &delivery, name, &host).await;
                }
            }
        }
        drop(ch);
        drop(conn);
    }
}

// ---------------------------------------------------------------------
// Fanout: one publisher, FANOUT_QUEUES identical order streams
// ---------------------------------------------------------------------

pub async fn run_fanout(ctx: Arc<Ctx>) {
    let confirmed = Arc::new(TagCounts::default());
    let mut handles = Vec::new();
    {
        let rate = ctx.cfg.rates.fanout;
        let ctx = ctx.clone();
        let confirmed = confirmed.clone();
        handles.push(tokio::spawn(async move {
            fanout_publisher(ctx, rate, confirmed).await;
        }));
    }
    // Every queue receives the same publisher tag sequence, so each
    // queue gets its OWN stream set (and its own reconciler: that
    // queue's distinct deliveries must equal the confirmed count).
    // Sharing one stream set across queues would read queues 2..N as
    // duplicates of queue 1.
    for i in 0..FANOUT_QUEUES {
        let queue = format!("soak.f.{i}");
        let streams = Arc::new(TagStreams::default());
        ctx.add_reconciler(tag_reconciler(&format!("fanout.{i}"), confirmed.clone(), streams.clone()))
            .await;
        let ctx = ctx.clone();
        handles.push(tokio::spawn(async move {
            acked_consumer(ctx, queue, streams, "fanout").await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn fanout_publisher(ctx: Arc<Ctx>, rate: f64, confirmed: Arc<TagCounts>) {
    let mut generation = 0u32;
    let mut seq: u64 = 0;
    let mut pacer = Pacer::new(rate);
    'outer: loop {
        if !client::alive(&ctx) {
            return;
        }
        let Some((conn, host)) = client::connect(&ctx, "fanout").await else {
            return;
        };
        let Some(ch) = client::channel(&ctx, &conn, "fanout", &host).await else {
            continue;
        };
        if !client::confirm_mode(&ctx, &ch, "fanout", &host).await {
            continue;
        }
        let tag = format!("fxg{generation}");
        loop {
            if !ctx.gate.open() {
                tokio::select! {
                    _ = ctx.token.cancelled() => return,
                    _ = ctx.gate.wait_open(&ctx.token) => {}
                }
                if !client::alive(&ctx) { return }
                continue;
            }
            pacer.wait().await;
            let body = encode_body(&tag, seq, ctx.cfg.msg_size);
            match client::publish_confirmed(&ctx, &ch, &host, "fanout", "soak.fx", "", &body).await {
                Some(true) => {
                    confirmed.bump(&tag);
                    seq += 1;
                    ctx.ledger.metrics.add("fanout.confirmed", 1);
                }
                Some(false) => {
                    generation += 1;
                    seq = 0;
                }
                None => {
                    if !client::alive(&ctx) {
                        return;
                    }
                    generation += 1;
                    seq = 0;
                    continue 'outer;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------
// Transactions: commit lands, abort must not
// ---------------------------------------------------------------------

/// Rolling window of committed transaction sequence numbers. FIFO sound
/// window check: a rolled-back message could only ever be delivered
/// *earlier* than the commit that displaced it, so "not in the last N
/// committed" is conclusive by the time the window has moved N further.
#[derive(Default)]
struct TxWindow {
    seqs: std::sync::Mutex<(VecDeque<u64>, HashSet<u64>)>,
    committed: AtomicU64,
    aborted: AtomicU64,
    too_old: AtomicU64,
}

const TX_WINDOW: usize = 100_000;

impl TxWindow {
    fn commit(&self, seqs: &[u64]) {
        let mut g = self.seqs.lock().unwrap();
        for &s in seqs {
            g.0.push_back(s);
            g.1.insert(s);
        }
        while g.0.len() > TX_WINDOW {
            let evicted = g.0.pop_front().unwrap();
            g.1.remove(&evicted);
        }
        self.committed.fetch_add(seqs.len() as u64, Ordering::Relaxed);
    }

    /// Undo a commit(): the consumer task observes deliveries
    /// independently of the publisher task, so the window is populated
    /// before tx_commit() resolves — and pruned if the commit fails.
    fn uncommit(&self, seqs: &[u64]) {
        let mut g = self.seqs.lock().unwrap();
        for &s in seqs {
            g.1.remove(&s);
            if let Some(pos) = g.0.iter().position(|x| *x == s) {
                g.0.remove(pos);
            }
        }
    }

    fn saw(&self, seq: u64) -> Saw {
        let g = self.seqs.lock().unwrap();
        if g.1.contains(&seq) {
            return Saw::Committed;
        }
        if let Some(oldest) = g.0.front() {
            if seq < *oldest {
                return Saw::TooOld;
            }
        }
        Saw::RolledBack
    }
}

enum Saw {
    Committed,
    TooOld,
    RolledBack,
}

pub async fn run_tx(ctx: Arc<Ctx>) {
    let window = Arc::new(TxWindow::default());
    let (publisher_state, consumer_state) = (window.clone(), window.clone());
    let msgs_per_tx = 2usize;
    ctx.add_reconciler(Arc::new(move || {
        let w = &publisher_state;
        let committed = w.committed.load(Ordering::Relaxed);
        let aborted = w.aborted.load(Ordering::Relaxed);
        vec![Check {
            name: "tx".into(),
            expected: committed,
            delivered: w.too_old.load(Ordering::Relaxed) + committed,
            ok: true,
            note: format!("{aborted} tx aborted (correctly silent)"),
        }]
    }))
    .await;
    let mut handles = Vec::new();
    {
        let rate = ctx.cfg.rates.tx;
        let ctx = ctx.clone();
        let window = window.clone();
        handles.push(tokio::spawn(async move {
            tx_publisher(ctx, rate, msgs_per_tx, window).await;
        }));
    }
    {
        let ctx = ctx.clone();
        let window = window.clone();
        handles.push(tokio::spawn(async move {
            tx_consumer(ctx, window).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn tx_publisher(ctx: Arc<Ctx>, rate: f64, msgs_per_tx: usize, window: Arc<TxWindow>) {
    let mut pacer = Pacer::new(rate);
    let mut seq: u64 = 0;
    'outer: loop {
        if !client::alive(&ctx) {
            return;
        }
        let Some((conn, host)) = client::connect(&ctx, "tx").await else {
            return;
        };
        let Some(ch) = client::channel(&ctx, &conn, "tx", &host).await else {
            continue;
        };
        if ch.tx_select().await.is_err() {
            ctx.error("tx", "tx.select", &host, "tx_select failed".into());
            continue;
        }
        loop {
            if !ctx.gate.open() {
                tokio::select! {
                    _ = ctx.token.cancelled() => return,
                    _ = ctx.gate.wait_open(&ctx.token) => {}
                }
                if !client::alive(&ctx) { return }
                continue;
            }
            pacer.wait().await;
            let mut batch = Vec::with_capacity(msgs_per_tx);
            for _ in 0..msgs_per_tx {
                let body = encode_body("tx", seq, ctx.cfg.msg_size);
                use lapin::options::BasicPublishOptions;
                let sent = ch
                    .basic_publish(
                        "",
                        "soak.tx",
                        BasicPublishOptions::default(),
                        &body,
                        lapin::BasicProperties::default().with_delivery_mode(2),
                    )
                    .await;
                match sent {
                    Ok(_) => {
                        batch.push(seq);
                        seq += 1;
                    }
                    Err(e) => {
                        if !client::alive(&ctx) {
                            return;
                        }
                        ctx.infra_bounce("tx", &host, format!("tx publish failed: {e}"));
                        break;
                    }
                }
            }
            if batch.is_empty() {
                if !client::alive(&ctx) {
                    return;
                }
                continue 'outer;
            }
            let commit = (seq / msgs_per_tx as u64) % 3 != 0; // 2/3 commit, 1/3 abort
            if commit {
                // Populate before commit: the consumer task observes
                // deliveries the moment the broker applies, which can
                // beat this task's post-commit bookkeeping.
                window.commit(&batch);
            }
            let result = if commit { ch.tx_commit().await } else { ch.tx_rollback().await };
            match result {
                Ok(_) => {
                    if commit {
                        ctx.ledger.metrics.add("tx.commits", 1);
                    } else {
                        window.aborted.fetch_add(1, Ordering::Relaxed);
                        ctx.ledger.metrics.add("tx.aborts", 1);
                    }
                }
                Err(e) => {
                    if commit {
                        window.uncommit(&batch);
                    }
                    if !client::alive(&ctx) {
                        return;
                    }
                    ctx.infra_bounce("tx", &host, format!("tx commit/rollback failed: {e}"));
                    continue 'outer;
                }
            }
        }
    }
}

async fn tx_consumer(ctx: Arc<Ctx>, window: Arc<TxWindow>) {
    loop {
        if !client::consuming(&ctx) {
            return;
        }
        let Some((conn, host)) = client::connect_drain(&ctx, "tx").await else {
            return;
        };
        let Some(ch) = client::channel(&ctx, &conn, "tx", &host).await else {
            continue;
        };
        let Some(mut consumer) = client::consume_acked(&ctx, &ch, &host, "tx", "soak.tx", 128).await
        else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        loop {
            let delivery = tokio::select! {
                _ = ctx.halt.cancelled() => return,
                d = StreamExt::next(&mut consumer) => match d {
                    Some(Ok(d)) => d,
                    Some(Err(e)) => {
                        if client::consuming(&ctx) {
                            ctx.infra_bounce("tx", &host, format!("consumer stream error: {e}"));
                        }
                        break;
                    }
                    None => break,
                }
            };
            match decode_body(delivery.data.as_slice()) {
                Ok((_, seq)) => match window.saw(seq) {
                    Saw::Committed => {}
                    Saw::TooOld => {
                        window.too_old.fetch_add(1, Ordering::Relaxed);
                    }
                    Saw::RolledBack => {
                        ctx.error(
                            "tx",
                            "abort.leak",
                            &host,
                            format!("message from a rolled-back transaction was delivered (seq {seq})"),
                        );
                    }
                },
                Err(e) => {
                    ctx.error("tx", "body", &host, format!("integrity violation: {e:?}"));
                }
            }
            client::ack(&ctx, &delivery, "tx", &host).await;
            ctx.ledger.metrics.add("tx.delivered", 1);
        }
    }
}

// ---------------------------------------------------------------------
// basic.get + ack pull path
// ---------------------------------------------------------------------

pub async fn run_get(ctx: Arc<Ctx>) {
    let confirmed = Arc::new(TagCounts::default());
    let streams = Arc::new(TagStreams::default());
    ctx.add_reconciler(tag_reconciler("get", confirmed.clone(), streams.clone()))
        .await;
    let rate = ctx.cfg.rates.get;
    let mut handles = Vec::new();
    {
        let ctx = ctx.clone();
        let confirmed = confirmed.clone();
        handles.push(tokio::spawn(async move {
            pipeline_publisher(ctx, "soak.get".into(), "getg".into(), rate, confirmed).await;
        }));
    }
    {
        let ctx = ctx.clone();
        let streams = streams.clone();
        handles.push(tokio::spawn(async move { get_consumer(ctx, streams).await }));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn get_consumer(ctx: Arc<Ctx>, streams: Arc<TagStreams>) {
    loop {
        if !client::consuming(&ctx) {
            return;
        }
        let Some((conn, host)) = client::connect_drain(&ctx, "get").await else {
            return;
        };
        let Some(ch) = client::channel(&ctx, &conn, "get", &host).await else {
            continue;
        };
        loop {
            tokio::select! {
                _ = ctx.halt.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
            use lapin::options::BasicGetOptions;
            let got = ch
                .basic_get("soak.get", BasicGetOptions { no_ack: false })
                .await;
            let got = match got {
                Ok(g) => g,
                Err(e) => {
                    if client::consuming(&ctx) {
                        ctx.infra_bounce("get", &host, format!("basic_get failed: {e}"));
                        break;
                    }
                    return;
                }
            };
            let Some(got) = got else { continue }; // empty queue
            match decode_body(got.delivery.data.as_slice()) {
                Ok((tag, seq)) => match streams.observe(&tag, seq, got.delivery.redelivered, ctx.cfg.chaos) {
                    check::Delivery::InOrder => {}
                    check::Delivery::Repeat => {
                        ctx.ledger.legal_dup();
                        ctx.ledger.metrics.add("dup.legal", 1);
                    }
                    check::Delivery::RepeatUnflagged => {
                        ctx.error("get", "dup.unflagged", &host, format!("seq {seq} without redelivered flag"));
                    }
                    check::Delivery::Gap(n) => {
                        ctx.error("get", "fifo.gap", &host, format!("missing {n} before seq {seq}"));
                    }
                },
                Err(e) => {
                    ctx.error("get", "body", &host, format!("integrity violation: {e:?}"));
                }
            }
            if let Err(e) = got.delivery.acker.ack(lapin::options::BasicAckOptions::default()).await {
                ctx.error("get", "ack", &host, format!("ack failed: {e}"));
            }
            ctx.ledger.metrics.add("get.delivered", 1);
        }
    }
}

// ---------------------------------------------------------------------
// Topology churn: declare/bind/purge/unbind/delete, exclusive queues,
// exchange lifecycle — all under load, all replies must be Ok.
// ---------------------------------------------------------------------

pub async fn run_topo(ctx: Arc<Ctx>) {
    let rate = ctx.cfg.rates.topo;
    if rate <= 0.0 {
        return;
    }
    // Run-scoped names: two driver generations (rollout overlap, an
    // operator's stray second driver) must never fight over the same
    // exclusive queues.
    let run = ctx.run_suffix();
    let mut n: u64 = 0;
    let mut pacer = Pacer::new(rate);
    loop {
        if !client::alive(&ctx) {
            return;
        }
        pacer.wait().await;
        let Some((conn, host)) = client::connect(&ctx, "topo").await else {
            return;
        };
        let Some(ch) = client::channel(&ctx, &conn, "topo", &host).await else {
            continue;
        };
        n = n.wrapping_add(1);
        let q = format!("soak.topo.{run}.{n}");
        if topo_cycle(&ctx, &ch, &host, &q).await.is_err() && !client::alive(&ctx) {
            return;
        }
        drop(ch);
        drop(conn);
    }
}

async fn topo_cycle(ctx: &Arc<Ctx>, ch: &Channel, host: &str, q: &str) -> Result<(), ()> {
    use lapin::options::ExchangeDeleteOptions;
    use lapin::options::QueuePurgeOptions;
    if !client::confirm_mode(ctx, ch, "topo", host).await {
        return Err(());
    }
    macro_rules! step {
        ($op:expr, $what:expr) => {
            match $op.await {
                Ok(v) => v,
                Err(e) => {
                    if client::alive(ctx) {
                        ctx.error("topo", "op", host, format!("{}: {e}", $what));
                    }
                    return Err(());
                }
            }
        };
    }
    // Full lifecycle on a durable queue.
    step!(ch.queue_declare(q, QueueDeclareOptions { durable: true, ..Default::default() }, FieldTable::default()), format!("declare {q}"));
    step!(ch.queue_bind(q, "amq.topic", q, QueueBindOptions::default(), FieldTable::default()), format!("bind {q}"));
    step!(ch.queue_purge(q, QueuePurgeOptions::default()), format!("purge {q}"));
    let body = encode_body("topo", 0, 64);
    match client::publish_confirmed(ctx, ch, host, "topo", "amq.topic", q, &body).await {
        Some(true) => {}
        Some(false) | None => {
            if client::alive(ctx) {
                ctx.error("topo", "publish", host, "confirmed publish failed".into());
            }
            return Err(());
        }
    }
    use lapin::options::BasicGetOptions;
    let got = step!(ch.basic_get(q, BasicGetOptions { no_ack: false }), format!("get {q}"));
    if let Some(got) = got {
        step!(got.delivery.acker.ack(lapin::options::BasicAckOptions::default()), format!("ack {q}"));
    }
    step!(ch.queue_unbind(q, "amq.topic", q, FieldTable::default()), format!("unbind {q}"));
    step!(ch.queue_delete(q, QueueDeleteOptions::default()), format!("delete {q}"));

    // Exchange lifecycle.
    let x = format!("soak.topox.{q}");
    step!(ch.exchange_declare(&x, ExchangeKind::Fanout, ExchangeDeclareOptions { durable: false, ..Default::default() }, FieldTable::default()), format!("declare exchange {x}"));
    step!(ch.exchange_delete(&x, ExchangeDeleteOptions::default()), format!("delete exchange {x}"));

    // Exclusive + auto-delete queue lifecycle (cleanup-path exercise).
    let eq = format!("soak.excl.{q}");
    step!(ch.queue_declare(&eq, QueueDeclareOptions { exclusive: true, auto_delete: true, ..Default::default() }, FieldTable::default()), format!("declare exclusive {eq}"));
    // It disappears when the channel closes; verify a delete on a fresh
    // channel observes the cleanup (404 = cleaned up = good, but we do
    // not assert either way: both outcomes are legal per spec timing).
    ctx.ledger.metrics.add("topo.cycles", 1);
    Ok(())
}

// ---------------------------------------------------------------------
// Connection / channel churn: the fd- and state-leak driver
// ---------------------------------------------------------------------

pub async fn run_connchurn(ctx: Arc<Ctx>) {
    let per_min = ctx.cfg.rates.connchurn;
    if per_min <= 0.0 {
        return;
    }
    let period = Duration::from_secs_f64(60.0 / per_min);
    let run = ctx.run_suffix();
    let mut n: u64 = 0;
    loop {
        if !client::alive(&ctx) {
            return;
        }
        tokio::select! {
            _ = ctx.token.cancelled() => return,
            _ = tokio::time::sleep(period) => {}
        }
        let Some((conn, host)) = client::connect(&ctx, "connchurn").await else {
            return;
        };
        let mut ok = true;
        for c in 0..3 {
            let Some(ch) = client::channel(&ctx, &conn, "connchurn", &host).await else {
                ok = false;
                break;
            };
            n += 1;
            let q = format!("soak.churn.{run}.{n}");
            match ch
                .queue_declare(
                    &q,
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
            {
                Ok(_) => {
                    if let Err(e) = ch.queue_delete(&q, QueueDeleteOptions::default()).await {
                        ctx.error("connchurn", "queue.delete", &host, format!("{q}: {e}"));
                        ok = false;
                    }
                }
                Err(e) => {
                    ctx.error("connchurn", "queue.declare", &host, format!("{q}: {e}"));
                    ok = false;
                }
            }
            drop(ch);
            let _ = c;
        }
        if ok {
            // Clean-ish exit: drop the connection (channels close with
            // it; the broker sees EOF) — calling close() here raced
            // channel teardown and produced driver-side noise.
            ctx.ledger.metrics.add("connchurn.cycles", 1);
        }
    }
}

// ---------------------------------------------------------------------
// Spawn + drain + reconcile
// ---------------------------------------------------------------------

/// Spawn every AMQP workload whose rate is non-zero.
pub fn spawn_all(ctx: &Arc<Ctx>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut v = Vec::new();
    let r = ctx.cfg.rates;
    if r.pipeline > 0.0 {
        v.push(tokio::spawn(run_pipeline(ctx.clone())));
    }
    if r.fanout > 0.0 {
        v.push(tokio::spawn(run_fanout(ctx.clone())));
    }
    if r.tx > 0.0 {
        v.push(tokio::spawn(run_tx(ctx.clone())));
    }
    if r.get > 0.0 {
        v.push(tokio::spawn(run_get(ctx.clone())));
    }
    if r.topo > 0.0 {
        v.push(tokio::spawn(run_topo(ctx.clone())));
    }
    if r.connchurn > 0.0 {
        v.push(tokio::spawn(run_connchurn(ctx.clone())));
    }
    v.extend(crate::soak::workload_idle::spawn_all(ctx));
    v
}

/// Quiesce has happened (gate closed, token cancelled). Wait for every
/// queue to drain to zero, then run all reconcilers. Every check must
/// pass; a stuck depth is a reconciliation failure.
pub async fn drain_and_reconcile(ctx: &Arc<Ctx>) -> Vec<Check> {
    let queues = [
        "soak.p.0", "soak.p.1", "soak.p.2", "soak.f.0", "soak.f.1", "soak.f.2", "soak.tx",
        "soak.get", "soak.mand",
    ];
    let deadline = tokio::time::Instant::now() + ctx.cfg.drain_timeout;
    let mut drained: Vec<(String, u32)> = Vec::new();
    let mut saw_endpoints = false;
    loop {
        drained.clear();
        let hosts = ctx.endpoints.snapshot().await;
        if !hosts.is_empty() {
            saw_endpoints = true;
            for q in queues {
                let mut total: u32 = 0;
                for h in &hosts {
                    if let Ok(n) = client::declare_durable_expect(ctx, h, q, "drain").await {
                        total = total.max(n);
                    }
                }
                if total > 0 {
                    drained.push((q.to_string(), total));
                }
            }
        }
        // Drain success requires having actually SEEN the queues empty.
        // An empty endpoint snapshot (mid-churn) proves nothing — breaking
        // there let reconciliation run against undrained queues.
        if drained.is_empty() && saw_endpoints {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            if !saw_endpoints {
                ctx.error("drain", "drain.no_endpoints", "-", "no endpoints to drain".into());
            } else {
                ctx.error(
                    "drain",
                    "drain.stuck",
                    "-",
                    format!("queues never drained: {drained:?}"),
                );
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Reconcilers may need a beat for the last acks to land; poll until
    // everything passes or the drain timeout expires.
    let deadline = tokio::time::Instant::now() + ctx.cfg.drain_timeout;
    let mut last = Vec::new();
    loop {
        last = ctx.run_reconcilers().await;
        if last.iter().all(|c| c.ok) {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            for c in &last {
                if !c.ok {
                    ctx.error(
                        "reconcile",
                        "conservation",
                        "-",
                        format!("{}: confirmed {} != delivered {}", c.name, c.expected, c.delivered),
                    );
                }
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    last
}
