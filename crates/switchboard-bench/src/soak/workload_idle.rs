//! Idle-connection keepalive probe: one AMQP session held open for the
//! whole soak with no application traffic — only protocol heartbeats.
//! Months-long stability requires that idle clients are kept alive by
//! the negotiated heartbeat on both sides, so this connection dying is
//! an error. A cheap AMQP op every minute proves the wire is live,
//! not just un-torn-down.

use std::sync::Arc;
use std::time::Duration;

use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;

use crate::soak::client;
use crate::soak::Ctx;

pub async fn run(ctx: Arc<Ctx>) {
    loop {
        if !client::alive(&ctx) {
            return;
        }
        let Some((conn, host)) = client::connect(&ctx, "idle").await else {
            return;
        };
        // Hold this connection until it breaks or the soak ends.
        loop {
            let Some(ch) = client::channel(&ctx, &conn, "idle", &host).await else {
                break;
            };
            let mut broken = false;
            for _ in 0..1000 {
                tokio::select! {
                    _ = ctx.token.cancelled() => {
                        let _ = conn.close(200, "soak done").await;
                        return;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
                match ch
                    .queue_declare(
                        "soak.control",
                        QueueDeclareOptions { passive: true, ..Default::default() },
                        FieldTable::default(),
                    )
                    .await
                {
                    Ok(_) => {
                        ctx.ledger.metrics.add("idle.probes_ok", 1);
                    }
                    Err(e) => {
                        if !client::alive(&ctx) {
                            return;
                        }
                        // Idle for a full minute and the session broke:
                        // a heartbeat-keepalive failure on one side or
                        // the other.
                        ctx.error(
                            "idle",
                            "keepalive",
                            &host,
                            format!("idle session did not survive a minute: {e}"),
                        );
                        broken = true;
                        break;
                    }
                }
            }
            if !broken {
                return; // soak over
            }
            drop(ch);
            break;
        }
        drop(conn);
    }
}

pub fn spawn_all(ctx: &Arc<Ctx>) -> Vec<tokio::task::JoinHandle<()>> {
    vec![tokio::spawn(run(ctx.clone()))]
}
