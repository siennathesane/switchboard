//! Protocol-bridge workloads: MQTT (TCP), STOMP, AMQP 1.0, raw 0-9-1
//! mandatory/return, and the health probes — every gateway protocol
//! under soak load.

pub mod amqp10w;
pub mod health;
pub mod mqtt;
pub mod rawamqp;
pub mod stomp;

use std::sync::Arc;

use crate::soak::Ctx;

/// Spawn every protocol workload its rate enables (health always runs).
pub fn spawn_all(ctx: &Arc<Ctx>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut v = Vec::new();
    v.extend(health::spawn_all(ctx));
    v.extend(mqtt::spawn_all(ctx));
    v.extend(stomp::spawn_all(ctx));
    v.extend(amqp10w::spawn_all(ctx));
    v.extend(rawamqp::spawn_all(ctx));
    v
}
