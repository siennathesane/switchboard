//! switchboard-soak: run every cluster feature under sustainable load
//! against a live Switchboard cluster — optionally scaling the k8s
//! Deployment 1↔9 continuously — and fail on the first client-visible
//! error. See `bench/SOAK.md`.

use std::time::Duration;

use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use switchboard_bench::soak;

#[derive(Parser, Debug)]
#[command(name = "switchboard-soak")]
struct Args {
    /// Cluster AMQP endpoints, comma-separated (host:port). Required
    /// for static mode; k8s mode discovers pods itself.
    #[arg(long, value_delimiter = ',')]
    hosts: Vec<String>,

    /// Total duration: `90s`, `30m`, `6h`, `7d`, `30d`.
    #[arg(long, default_value = "1h")]
    duration: String,

    /// Message size in bytes.
    #[arg(long, default_value_t = 512)]
    size: usize,

    /// Rate override, repeatable: `pipeline=50`, `mqtt=0` (disable), ...
    #[arg(long)]
    rate: Vec<String>,

    /// Continuously scale a k8s Deployment between min and max
    /// replicas: `ns/deployment:min:max`. Implies in-cluster API use.
    #[arg(long)]
    scale: Option<String>,

    /// One scale move per this interval.
    #[arg(long, default_value = "90s")]
    scale_every: String,

    /// Unclean (SIGKILL) pod deletion cadence at ≥3 replicas; 0
    /// disables.
    #[arg(long, default_value = "3m")]
    kill_every: String,

    #[arg(long, default_value = "guest")]
    user: String,

    #[arg(long, default_value = "guest")]
    password: String,

    /// Treat infrastructure bounces (refused connects, dropped
    /// sessions) as transitions instead of errors. Implied by
    /// `--scale`; use for static hosts when injecting faults externally.
    #[arg(long, default_value_t = false)]
    chaos: bool,

    /// Disable the /stats resource monitor.
    #[arg(long, default_value_t = false)]
    no_monitor: bool,

    /// Leak thresholds: rss MiB/h,fds/h,disk MiB/h.
    #[arg(long, default_value = "8,60,100")]
    leak: String,

    /// Status line cadence.
    #[arg(long, default_value = "30s")]
    report_every: String,
}

fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let mult = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        _ => {
            // plain seconds
            let v: u64 = s.parse()?;
            return Ok(Duration::from_secs(v));
        }
    };
    let v: u64 = num.parse()?;
    Ok(Duration::from_secs(v * mult))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.size == 0 {
        bail!("--size must be positive");
    }
    let mut cfg = soak::SoakConfig {
        hosts: args.hosts.clone(),
        user: args.user,
        password: args.password,
        chaos: args.chaos,
        monitor: !args.no_monitor,
        msg_size: args.size,
        ..Default::default()
    };
    cfg.duration = parse_duration(&args.duration)?;
    if cfg.duration.is_zero() {
        bail!("--duration must be positive");
    }
    cfg.report_every = parse_duration(&args.report_every)?;
    for r in &args.rate {
        cfg.rates.apply(r).map_err(|e| anyhow::anyhow!(e))?;
    }
    let leak: Vec<f64> = args
        .leak
        .split(',')
        .map(|v| v.trim().parse())
        .collect::<Result<_, _>>()
        .map_err(|_| anyhow::anyhow!("--leak wants rss_mb_h,fds_h,disk_mb_h"))?;
    if leak.len() != 3 {
        bail!("--leak wants rss_mb_h,fds_h,disk_mb_h");
    }
    cfg.leak.rss_mb_h = leak[0];
    cfg.leak.fds_h = leak[1];
    cfg.leak.disk_mb_h = leak[2];
    match &args.scale {
        Some(spec) => {
            let parts: Vec<&str> = spec.split(':').collect();
            if parts.len() != 3 {
                bail!("--scale wants ns/deployment:min:max");
            }
            cfg.scale = soak::ScaleMode::K8s {
                namespace: parts[0].split('/').next().unwrap_or("soak").to_string(),
                deployment: parts[0].split('/').nth(1).unwrap_or(parts[0]).to_string(),
                min: parts[1].parse()?,
                max: parts[2].parse()?,
                every: parse_duration(&args.scale_every)?,
                kill_every: parse_duration(&args.kill_every)?,
            };
            // Scaling is churn by definition.
            cfg.chaos = true;
        }
        None => {
            if cfg.hosts.is_empty() {
                bail!("--hosts is required without --scale");
            }
        }
    }

    let report = soak::run(cfg).await;
    if !report.passed {
        std::process::exit(1);
    }
    Ok(())
}
