//! The consumer handle: a push stream of deliveries for one subscription.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::io::Error;
use std::sync::Arc;

use switchboard_wire::method::Method;
use tokio::sync::mpsc;

use crate::driver::ChannelShared;
use crate::driver::call;
use crate::error::OpError;
use crate::types::Delivery;

/// One active consumer (Basic.Consume/Consume-Ok).
///
/// [`next`](Consumer::next) yields deliveries pushed by the broker's
/// delivery pump (methods/consume.rs:126). It returns `None` when the
/// consumer is cancelled server-side (consumer-cancellation notification,
/// e.g. its queue was deleted) or the connection is gone.
///
/// Backpressure note (embedded design §9/R1): the delivery pipeline is
/// unbounded, exactly like a slow TCP client's. Set a prefetch window via
/// [`EmbeddedChannel::qos`](crate::EmbeddedChannel::qos) and consume
/// promptly.
pub struct Consumer {
    /// The consumer tag (server-generated `ct-N` when not requested).
    pub tag: String,
    rx: mpsc::UnboundedReceiver<Delivery>,
    channel: Arc<ChannelShared>,
}

impl Consumer {
    pub(crate) fn new(
        tag: String,
        rx: mpsc::UnboundedReceiver<Delivery>,
        channel: Arc<ChannelShared>,
    ) -> Self {
        Consumer { tag, rx, channel }
    }

    /// The next delivery, or `None` once the consumer is cancelled
    /// (consumer-cancellation notification) or the connection is gone.
    pub async fn next(&mut self) -> Option<Delivery> {
        self.rx.recv().await
    }

    /// Cancel this consumer (Basic.Cancel/Cancel-Ok) and drop the stream.
    pub async fn cancel(self) -> Result<(), OpError> {
        match call(
            &self.channel,
            &Method::BasicCancel {
                consumer_tag: self.tag.clone(),
                nowait: false,
            },
        )
        .await
        {
            Ok((Method::BasicCancelOk { .. }, _)) => {
                self.channel
                    .conn
                    .consumers
                    .lock()
                    .unwrap()
                    .remove(&self.tag);
                Ok(())
            }
            Ok((other, _)) => Err(OpError::Io(Error::other(format!(
                "unexpected reply {} to Basic.Cancel",
                other.name()
            )))),
            Err(e) => Err(e),
        }
    }
}

impl Debug for Consumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Consumer")
            .field("tag", &self.tag)
            .finish_non_exhaustive()
    }
}
