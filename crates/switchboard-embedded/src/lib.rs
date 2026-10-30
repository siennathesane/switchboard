//! # switchboard-embedded
//!
//! Switchboard as a library: one in-process broker plus a typed AMQP 0-9-1
//! client, with **no sockets anywhere on the client path** (embedded
//! design, out/release-push/embedded-design.md).
//!
//! A [`Broker`] wraps the unmodified node runtime
//! (`switchboard_cluster::ClusterNode`): raft-replicated topology and
//! queues over RocksDB, single-node by default. [`Broker::connect`]
//! returns an [`EmbeddedConnection`] whose traffic flows through an
//! in-memory duplex into the exact `serve_rw` session core the TCP
//! gateways drive — same handshake, same authentication (loopback guest
//! rule included), same frame demux, same channel state machines, same
//! teardown. There is no bypass flag and no divergence from wire clients.
//!
//! ## Quick start
//!
//! ```rust
//! use switchboard_embedded::{Broker, BrokerConfig, QueueOptions};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = std::env::temp_dir().join(format!(
//!     "switchboard-embedded-doc-{}-{}",
//!     std::process::id(),
//!     std::time::SystemTime::now()
//!         .duration_since(std::time::UNIX_EPOCH)?
//!         .as_nanos()
//! ));
//! let broker = Broker::start(BrokerConfig::new(dir)).await?;
//!
//! let conn = broker.connect("guest", "guest").await?;
//! let channel = conn.open_channel().await?;
//! channel
//!     .queue_declare("tasks", &QueueOptions::new().durable(true))
//!     .await?;
//! channel
//!     .publish("", "tasks", Default::default(), b"hello".to_vec())
//!     .await?;
//! let delivery = channel.get("tasks", true).await?.expect("message");
//! assert_eq!(delivery.body, b"hello");
//!
//! conn.close().await;
//! broker.stop(std::time::Duration::from_secs(5)).await?;
//! # Ok(())
//! # }
//! # #[tokio::main]
//! # async fn main() { demo().await.unwrap(); }
//! ```
//!
//! ## Runtime contract (§3.1, 1.0-visible)
//!
//! Everything here requires a live **tokio** runtime context: the node
//! spawns background loops and the client spawns a driver task per
//! connection, and `tokio::spawn` panics when polled outside a runtime —
//! calling an async fn from a non-async thread does not panic at the call
//! site; the panic fires at first poll. Call every method from within
//! `#[tokio::main]` / `#[tokio::test]`, or via `Handle::block_on` from
//! foreign threads. Current-thread runtimes are supported (all spawned
//! futures are `Send`), and `Broker`, `EmbeddedConnection`,
//! `EmbeddedChannel`, and `Consumer` are `Send + Sync` values. Drop a
//! `Runtime` only after [`Broker::stop`] — dropping it under a live broker
//! kills the spawned loops and is process-exit semantics, not a supported
//! stop path.
//!
//! ## Lifecycle and durability (§5)
//!
//! * [`stop`](Broker::stop) closes every tracked connection, then runs the
//!   existing bounded `ClusterNode::leave`, with an explicit `shutdown()`
//!   fallback when the leave deadline passes.
//! * **Restart = new process, same `data_dir`.** In-process
//!   stop-then-start is contractually impossible in 1.0: the node keeps
//!   the RocksDB exclusive LOCK for the whole process, so a second
//!   `Broker::start` on the same directory fails with
//!   [`StartError::Storage`]. Durable queues and their messages reappear
//!   in the new process.
//! * Unacked deliveries are requeued by graceful stop (connection close
//!   → session teardown); a hard kill re-releases them on the first
//!   colliding subscription registration (§5.3, §9/R4).
//!
//! ## Authorization parity (§6)
//!
//! Embedded clients are ordinary loopback clients: PLAIN credentials, the
//! guest-loopback rule, and the replicated permission regexes all apply.
//! The documented coverage of `authorize_ch` gates is inherited
//! *including its gaps* (publish gained its `write`-on-exchange gate with
//! the control-plane hardening — methods/publish.rs:54-69; exchange-bind
//! is still ungated today and remains on the separate `fix(broker):`
//! hardening track) — no bypass, no divergence.
//!
//! One further inherited behavior to know about: the broker's janitor
//! expires prepared transactions on every tick and the shard state
//! machine's `tx_timeout_ticks` defaults to 0, so a tx commit racing a
//! janitor tick can be silently dropped (Commit-Ok still returns). This
//! affects TCP clients identically; until a `fix(broker):` change lands,
//! embedders running transactions should widen `timeouts.janitor_interval`
//! via [`BrokerConfig::timeouts`].
//!
//! ## Stability (§3)
//!
//! No broker internals (`ClusterNode`, `Channel`, `OutboundFrame`,
//! `Method`, `BrokerError`) appear in a public signature. Wire *data*
//! types are re-exported under crate aliases ([`MessageProperties`],
//! [`ExchangeKind`], [`Arguments`]): the AMQP content format is the
//! durable contract. All builder methods take `self` and return `Self`;
//! error enums are frozen shapes for 1.0.

#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]

mod broker;
mod channel;
mod config;
mod conn;
mod consumer;
mod driver;
mod error;
mod types;

pub use broker::Broker;
pub use channel::ConsumeOptions;
pub use channel::EmbeddedChannel;
pub use channel::ExchangeDeleteOptions;
pub use channel::ExchangeKind;
pub use channel::ExchangeOptions;
pub use channel::QueueDeleteOptions;
pub use channel::QueueOptions;
pub use config::BrokerConfig;
pub use config::EmbeddedLimits;
pub use config::JoinSpec;
pub use conn::EmbeddedConnection;
pub use consumer::Consumer;
pub use error::AdminError;
pub use error::ConnectError;
pub use error::OpError;
pub use error::StartError;
pub use error::StopError;
pub use switchboard_core::topology::Permissions;
pub use types::Arguments;
pub use types::ConfirmOutcome;
pub use types::Delivery;
pub use types::MessageProperties;
pub use types::PublishOutcome;
pub use types::QueueState;
pub use types::ReturnedMessage;
