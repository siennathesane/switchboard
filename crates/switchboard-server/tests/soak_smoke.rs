//! Soak smoke: the full soak engine against a real in-process 3-node
//! cluster for a short window, with every protocol gateway enabled.
//! This is the CI-scale version of the months-long soak: same engine,
//! same workloads, same zero-error bar — any client-visible error or
//! reconciliation failure fails the test.

mod support;

use std::sync::Arc;
use std::time::Duration;

use switchboard_bench::soak;
use switchboard_cluster::ClusterNode;
use switchboard_server::channel::ConnectionLimits;
use switchboard_server::ProtocolConfig;

/// Start a 3-node cluster fronted by the full protocol gateway (the
/// production listener: AMQP 0-9-1, AMQP 1.0, MQTT, STOMP, WebSocket,
/// HTTP health — one port, sniffed).
async fn start_gateway_cluster() -> Vec<String> {
    support::init_tracing();
    let internal: Vec<String> =
        (0..3).map(|_| format!("127.0.0.1:{}", support::free_port())).collect();
    let clients: Vec<String> =
        (0..3).map(|_| format!("127.0.0.1:{}", support::free_port())).collect();
    let peers: Vec<(u64, String)> =
        (1..=3u64).map(|id| (id, internal[id as usize - 1].clone())).collect();

    let mut nodes: Vec<Arc<ClusterNode>> = Vec::new();
    for id in 1..=3u64 {
        let i = id as usize - 1;
        let seeds = if id == 1 { vec![] } else { vec![internal[0].clone()] };
        let (node, _listener) = support::start_broker_node(
            &format!("soak-smoke-{id}"),
            id,
            clients[i].clone(),
            internal[i].clone(),
            seeds,
            id == 1,
            3,
            peers.clone(),
        )
        .await;
        let addr: &'static str = Box::leak(clients[i].clone().into_boxed_str());
        // Heartbeat disabled here: SB_TIME_SCALE=50 warps the broker's
        // 60 s heartbeat down to ~1.2 s real time while lapin still
        // ticks in real time — idle sessions would be killed spuriously.
        // Heartbeat keepalive under real time is exercised by the k8s
        // soak's idle probe instead.
        let limits = ConnectionLimits { heartbeat: 0, ..ConnectionLimits::default() };
        let node_for_listener = node.clone();
        let addr2 = addr;
        tokio::spawn(async move {
            if let Err(e) = switchboard_server::listener::run(
                node_for_listener,
                addr2,
                limits,
                None,
                ProtocolConfig::default(),
            )
            .await
            {
                eprintln!("DEBUG gateway listener on {addr2} exited: {e}");
            }
        });
        nodes.push(node);
    }

    // Formation: bootstrap entities visible everywhere, shard layout
    // installed, then a beat for shard-group elections.
    let ok = support::wait_until(
        || nodes.iter().all(|n| !n.topology().vhosts.is_empty() && !n.topology().groups.is_empty()),
        120,
    )
    .await;
    assert!(ok, "cluster did not form");
    tokio::time::sleep(Duration::from_millis(700)).await;
    clients
}

#[tokio::test(flavor = "multi_thread")]
async fn soak_smoke_passes_with_zero_errors() {
    let hosts = start_gateway_cluster().await;
    // SOAK_ONLY=fanout etc. isolates one workload while debugging.
    let only = std::env::var("SOAK_ONLY").unwrap_or_default();
    let r = |name: &str, base: f64| if only.is_empty() || only == name { base } else { 0.0 };
    let cfg = soak::SoakConfig {
        hosts,
        user: "guest".into(),
        password: "guest".into(),
        vhost: "%2F".into(),
        duration: Duration::from_secs(40),
        msg_size: 256,
        rates: soak::Rates {
            pipeline: r("pipeline", 10.0),
            fanout: r("fanout", 3.0),
            tx: r("tx", 1.0),
            get: r("get", 1.0),
            topo: r("topo", 1.0),
            connchurn: r("connchurn", 12.0),
            mqtt: r("mqtt", 5.0),
            stomp: r("stomp", 2.0),
            amqp10: r("amqp10", 4.0),
            mandatory: r("mandatory", 12.0),
        },
        scale: soak::ScaleMode::Static,
        monitor: true,
        monitor_every: Duration::from_secs(5),
        report_every: Duration::from_secs(10),
        health_every: Duration::from_secs(2),
        chaos: false,
        confirm_timeout: Duration::from_secs(30),
        drain_timeout: Duration::from_secs(90),
        backlog_bound: 5_000,
        leak: soak::LeakThresholds {
            rss_mb_h: 1_000_000.0, // irrelevant at this scale
            disk_mb_h: 1_000_000.0,
            fds_h: 1_000_000.0,
        },
    };
    let report = soak::run(cfg).await;
    if !report.passed {
        eprintln!(
            "soak: {} error(s), {} transitions; elapsed {:?}; metrics: {:?}",
            report.error_total,
            report.transitions,
            report.elapsed,
            report.metrics
        );
        for e in &report.errors {
            eprintln!("soak error t={}s [{}/{}] {}: {}", e.elapsed_s, e.workload, e.kind, e.host, e.detail);
        }
        for c in &report.reconcile {
            if !c.ok {
                eprintln!(
                    "reconcile FAILED: {} confirmed={} delivered={}",
                    c.name, c.expected, c.delivered
                );
            }
        }
    }
    assert!(report.passed, "soak smoke must pass with zero errors");
    assert!(
        report.metrics.iter().any(|(k, v)| k == "pipeline.confirmed" && *v > 100),
        "pipeline must have moved real traffic: {:?}",
        report.metrics
    );
}
