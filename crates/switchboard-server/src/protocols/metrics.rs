//! `GET /metrics`: Prometheus text exposition (version 0.0.4), hand-rolled
//! — zero new dependencies. The renderer walks exactly the state the other
//! operational surfaces already expose:
//!
//! * the shard state-machine message-lifecycle counters the soak monitor
//!   trends through `GET /stats` (`ClusterNode::message_accounting()`),
//! * per-protocol connection gauges from [`super::connstats`],
//! * topology entity counts and process vitals (same sources as `/stats`).
//!
//! Serving lives in [`super::http::serve`], next to `/stats`; the endpoint
//! rides the same `http` protocol flag as the rest of the health surface
//! and needs no authentication, like `/stats`.

use std::sync::Arc;

use switchboard_cluster::ClusterNode;

/// Render the Prometheus exposition for this node.
pub fn render(node: &Arc<ClusterNode>) -> String {
    let mut out = String::with_capacity(4096);

    // ---- message lifecycle (reuses the /stats accounting) ----
    let accounting = node.message_accounting();
    let mut totals: Vec<(&str, u64)> = accounting["totals"]
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_u64().map(|n| (k.as_str(), n)))
                .collect()
        })
        .unwrap_or_default();
    totals.sort_unstable_by_key(|(k, _)| *k);
    for (name, value) in &totals {
        let metric = format!("switchboard_messages_{name}_total");
        help_type(
            &mut out,
            &metric,
            "counter",
            &match *name {
                "enqueued" => "Messages enqueued across all shard groups this node hosts.",
                "handed_out" => "Messages handed out to consumers.",
                "acked" => "Messages acknowledged (removed from their queue).",
                "noack_removed" => "Messages removed on hand-out by no-ack consumers.",
                "released" => "Messages released back to the ready set.",
                "purged" => "Messages dropped by queue purge.",
                "expired" => "Messages dropped by TTL expiry.",
                "deleted" => "Message payloads deleted with their queue.",
                other => return_invariant(other),
            },
        );
        out.push_str(&metric);
        out.push(' ');
        push_u64(&mut out, *value);
    }

    // ---- per-queue depth (top queues, same list /stats trends) ----
    help_type(
        &mut out,
        "switchboard_queue_ready_messages",
        "gauge",
        "Messages ready for delivery, per queue (top queues by total depth).",
    );
    help_type(
        &mut out,
        "switchboard_queue_held_messages",
        "gauge",
        "Messages delivered but unacknowledged, per queue.",
    );
    if let Some(queues) = accounting["queues"].as_array() {
        for q in queues {
            let name = q["queue"].as_str().unwrap_or("");
            let ready = q["ready"].as_u64().unwrap_or(0);
            let held = q["held"].as_u64().unwrap_or(0);
            labeled(&mut out, "switchboard_queue_ready_messages", &[("queue", name)], ready);
            labeled(&mut out, "switchboard_queue_held_messages", &[("queue", name)], held);
        }
    }

    // ---- per-protocol connections ----
    help_type(
        &mut out,
        "switchboard_connections",
        "gauge",
        "Client connections currently open, per sniffed protocol.",
    );
    help_type(
        &mut out,
        "switchboard_connections_total",
        "counter",
        "Client connections accepted since process start, per protocol.",
    );
    for (name, current, total) in super::connstats::snapshot() {
        labeled(&mut out, "switchboard_connections", &[("protocol", name)], current);
        labeled(&mut out, "switchboard_connections_total", &[("protocol", name)], total);
    }

    // ---- topology entity counts ----
    let topo = node.topology();
    let mut queues = 0u64;
    let mut exchanges = 0u64;
    let mut bindings = 0u64;
    for v in topo.vhosts.values() {
        queues += v.queues.len() as u64;
        exchanges += v.exchanges.len() as u64;
        bindings += v.bindings.len() as u64;
    }
    help_type(&mut out, "switchboard_vhosts", "gauge", "Configured vhosts.");
    emit_gauge(&mut out, "switchboard_vhosts", topo.vhosts.len() as u64);
    emit_gauge(&mut out, "switchboard_users", topo.users.len() as u64);
    emit_gauge(&mut out, "switchboard_queues", queues);
    emit_gauge(&mut out, "switchboard_exchanges", exchanges);
    emit_gauge(&mut out, "switchboard_bindings", bindings);

    // ---- process vitals (same source /stats uses) ----
    let (rss_kb, fds) = super::http::proc_self_stats();
    emit_gauge(&mut out, "switchboard_uptime_seconds", node.uptime_secs());
    emit_gauge(&mut out, "switchboard_rss_kb", rss_kb);
    emit_gauge(&mut out, "switchboard_open_fds", fds);

    // ---- node identity ----
    help_type(
        &mut out,
        "switchboard_node_info",
        "gauge",
        "Node identity and build version; the value is always 1.",
    );
    out.push_str(&format!(
        "switchboard_node_info{{node=\"{}\",version=\"{}\"}} 1\n",
        node.id,
        env!("CARGO_PKG_VERSION"),
    ));

    out
}

/// `# HELP` + `# TYPE` block; HELP text must not contain newlines.
fn help_type(out: &mut String, metric: &str, kind: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(metric);
    out.push(' ');
    out.push_str(&help.replace('\\', "\\\\").replace('\n', " "));
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(metric);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

/// One bare (unlabeled) gauge sample.
fn emit_gauge(out: &mut String, metric: &str, value: u64) {
    out.push_str(metric);
    out.push(' ');
    push_u64(out, value);
}

/// One labeled sample: `metric{label="escaped",…} value`.
fn labeled(out: &mut String, metric: &str, labels: &[(&str, &str)], value: u64) {
    out.push_str(metric);
    out.push('{');
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(k);
        out.push_str("=\"");
        for c in v.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                other => out.push(other),
            }
        }
        out.push('"');
    }
    out.push('}');
    out.push(' ');
    push_u64(out, value);
}

fn push_u64(out: &mut String, value: u64) {
    let mut buf = [0u8; 20];
    let mut n = value;
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.push_str(std::str::from_utf8(&buf[i..]).unwrap_or("0"));
    out.push('\n');
}

/// Counter names come from the state machine's own `snapshot_u64` keys;
/// an unknown key is an invariant break worth a loud, parseable line —
/// not a silent skip.
fn return_invariant(name: &str) -> &'static str {
    tracing::warn!(counter = %name, "unknown message-lifecycle counter in /metrics");
    "Unknown message-lifecycle counter (see broker log)."
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_escaping() {
        let mut s = String::new();
        labeled(&mut s, "m", &[("queue", "a\"b\\c\nd")], 7);
        assert_eq!(s, "m{queue=\"a\\\"b\\\\c\\nd\"} 7\n");
    }

    #[test]
    fn number_rendering() {
        let mut s = String::new();
        push_u64(&mut s, 0);
        push_u64(&mut s, 42);
        push_u64(&mut s, 1_000_000_000);
        assert_eq!(s, "0\n42\n1000000000\n");
    }

    #[test]
    fn help_newlines_are_folded() {
        let mut s = String::new();
        help_type(&mut s, "m", "gauge", "two\nlines");
        assert!(s.starts_with("# HELP m two lines\n# TYPE m gauge\n"), "got: {s}");
    }
}
