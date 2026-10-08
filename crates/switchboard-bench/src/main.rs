//! Switchboard benchmark driver.
//!
//! Runs throughput/latency scenarios against a live cluster and prints a
//! human table plus a machine-readable JSON block (after the
//! `=== RESULTS ===` marker on stdout) for machine consumption.

mod scenarios;

use anyhow::{bail, Result};
use clap::Parser;
use serde_json::json;
use switchboard_bench::stats;

#[derive(Parser, Debug)]
#[command(name = "switchboard-bench")]
struct Args {
    /// Cluster AMQP hosts, comma-separated (e.g. sb1,sb2,sb3).
    #[arg(long, value_delimiter = ',')]
    hosts: Vec<String>,

    /// Scenario: all, publish, confirm, sharded, fanout, drain, latency.
    #[arg(long, default_value = "all")]
    scenario: String,

    /// Measured window per scenario, seconds.
    #[arg(long, default_value_t = 20)]
    duration: u64,

    /// Warmup per scenario, seconds (discarded).
    #[arg(long, default_value_t = 5)]
    warmup: u64,

    /// Concurrent publisher connections.
    #[arg(long, default_value_t = 8)]
    publishers: usize,

    /// Message size in bytes.
    #[arg(long, default_value_t = 1024)]
    size: usize,

    /// Pipeline window (in-flight messages) across all publishers.
    #[arg(long, default_value_t = 1024)]
    in_flight: usize,

    /// Queues bound to the fanout exchange.
    #[arg(long, default_value_t = 3)]
    fanout_queues: usize,

    /// Messages preloaded for the drain scenario.
    #[arg(long, default_value_t = 50000)]
    preload: u64,

    #[arg(long, default_value = "guest")]
    user: String,

    #[arg(long, default_value = "guest")]
    password: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.hosts.is_empty() {
        bail!("--hosts is required (e.g. --hosts sb1,sb2,sb3)");
    }
    if args.size == 0 {
        bail!("--size must be positive");
    }
    let bench = scenarios::Bench {
        hosts: args.hosts.clone(),
        user: args.user,
        password: args.password,
        duration: args.duration,
        warmup: args.warmup,
        publishers: args.publishers,
        size: args.size,
        in_flight: args.in_flight,
        fanout_queues: args.fanout_queues,
        preload: args.preload,
    };

    println!(
        "switchboard-bench: {} node(s) ({}), scenario {}, {}s window, {} B messages",
        args.hosts.len(),
        args.hosts.join(","),
        args.scenario,
        args.duration,
        args.size
    );

    let results = scenarios::run(&bench, &args.scenario).await?;

    // Human table.
    println!();
    println!(
        "{:<12} {:>14} {:>12} {:>10} {:>22}",
        "scenario", "throughput/s", "MiB/s", "peak/s", "latency"
    );
    println!("{}", "-".repeat(76));
    for (name, v) in &results {
        match name.as_str() {
            "latency" => {
                let p50 = v["p50_us"].as_u64().unwrap_or(0);
                let p99 = v["p99_us"].as_u64().unwrap_or(0);
                let max = v["max_us"].as_u64().unwrap_or(0);
                println!(
                    "{:<12} {:>14} {:>12} {:>10} {:>22}",
                    name,
                    "-",
                    "-",
                    "-",
                    format!("p50 {p50}µs p99 {p99}µs max {max}µs")
                );
            }
            _ => {
                let t = v["throughput_msg_s"].as_u64().unwrap_or(0);
                let mib = v["throughput_mib_s"].as_f64().unwrap_or(0.0);
                let peak = v["peak_msg_s"].as_u64().unwrap_or(0);
                println!("{:<12} {:>14} {:>12} {:>10} {:>22}", name, t, mib, peak, "-");
            }
        }
    }

    // Machine block.
    println!();
    println!("=== RESULTS ===");
    println!(
        "{}",
        json!({
            "nodes": args.hosts.len(),
            "hosts": args.hosts,
            "duration": args.duration,
            "warmup": args.warmup,
            "publishers": args.publishers,
            "size": args.size,
            "in_flight": args.in_flight,
            "results": results,
        })
    );
    Ok(())
}
