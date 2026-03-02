// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Handlers for acknowledging or rejecting messages.
//!
//! To acknowledge (ack) a message, you call [`Handler::ack()`].
//!
//! To reject (nack) a message, you [`drop()`][Drop::drop] the handler. The
//! message will be redelivered.
//!
//! # Example
//!
//! ```
//! use google_cloud_pubsub::model::Message;
//! # use google_cloud_pubsub::subscriber::handler::Handler;
//! fn on_message(m: Message, h: Handler) {
//!   match process(m) {
//!     Ok(_) => h.ack(),
//!     Err(e) => {
//!         println!("failed to process message: {e:?}");
//!         drop(h);
//!     }
//!   }
//! }
//!
//! fn process(m: Message) -> anyhow::Result<()> {
//!   // some business logic here...
//!   # panic!()
//! }
//! ```

use crate::error::AckError;
use tokio::sync::mpsc::UnboundedSender;

/// The action an application does with a message.
#[derive(Debug, PartialEq)]
pub(super) enum Action {
    Ack(String),
    Nack(String),
    ExactlyOnceAck(String),
    ExactlyOnceNack(String),
}

/// A handler for acknowledging or rejecting messages.
///
/// # Example
///
/// ```
/// use google_cloud_pubsub::model::Message;
/// # use google_cloud_pubsub::subscriber::handler::Handler;
/// fn on_message(m: Message, h: Handler) {
///   match process(m) {
///     Ok(_) => h.ack(),
///     Err(e) => {
///         println!("failed to process message: {e:?}");
///         drop(h);
///     }
///   }
/// }
///
/// fn process(m: Message) -> anyhow::Result<()> {
///   // some business logic here...
///   # panic!()
/// }
/// ```
///
/// To acknowledge (ack) a message, you call [`Handler::ack()`].
///
/// To reject (nack) a message, you [`drop()`][Drop::drop] the handler. The
/// message will be redelivered.
///
/// ## Exactly-once delivery
///
/// If your subscription has [exactly-once delivery] enabled, you need to
/// destructure this enum into its [`Handler::ExactlyOnce`] branch.
///
/// Only when `ExactlyOnce::confirmed_ack()` returns `Ok` can you be certain
/// that the message will not be redelivered.
///
/// [exactly-once delivery]: https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery
///
/// ```no_rust
/// use google_cloud_pubsub::model::Message;
/// # use google_cloud_pubsub::subscriber::handler::Handler;
/// async fn on_message(m: Message, h: Handler) {
///   let Handler::ExactlyOnce(h) = h else {
///     panic!("Oops, my subscription does not have exactly-once delivery enabled.")
///   };
///   match h.confirmed_ack().await {
///     Ok(()) => println!("Confirmed ack for message={m:?}. The message will not be redelivered.")
///     Err(e) => println!("Failed to confirm ack for message={m:?} with error={e:?}"),
///   }
/// }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Handler {
    AtLeastOnce(AtLeastOnce),
    ExactlyOnce(ExactlyOnce),
}

impl Handler {
    /// Acknowledge the message associated with this handler.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_pubsub::model::Message;
    /// # use google_cloud_pubsub::subscriber::handler::Handler;
    /// fn on_message(m: Message, h: Handler) {
    ///   println!("Received message: {m:?}");
    ///   h.ack();
    /// }
    /// ```
    ///
    /// Note that the acknowledgement is best effort. The message may still be
    /// redelivered to this client, or another client, even if exactly-once
    /// delivery is enabled on the subscription.
    pub fn ack(self) {
        match self {
            Handler::AtLeastOnce(h) => h.ack(),
            Handler::ExactlyOnce(h) => h.ack(),
        }
    }

    #[cfg(test)]
    pub(crate) fn ack_id(&self) -> &str {
        match self {
            Handler::AtLeastOnce(h) => h.ack_id(),
            Handler::ExactlyOnce(h) => h.ack_id(),
        }
    }
}

#[derive(Debug)]
struct AtLeastOnceImpl {
    ack_id: String,
    ack_tx: UnboundedSender<Action>,
}

impl AtLeastOnceImpl {
    fn ack(self) {
        let _ = self.ack_tx.send(Action::Ack(self.ack_id));
    }

    fn nack(self) {
        let _ = self.ack_tx.send(Action::Nack(self.ack_id));
    }
}

/// A handler for at-least-once delivery.
#[derive(Debug)]
pub struct AtLeastOnce {
    inner: Option<AtLeastOnceImpl>,
}

impl AtLeastOnce {
    pub(super) fn new(ack_id: String, ack_tx: UnboundedSender<Action>) -> Self {
        Self {
            inner: Some(AtLeastOnceImpl { ack_id, ack_tx }),
        }
    }

    /// Acknowledge the message associated with this handler.
    ///
    /// Note that the acknowledgement is best effort. The message may still be
    /// redelivered to this client, or another client.
    pub fn ack(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.ack();
        }
    }

    #[cfg(test)]
    pub(crate) fn ack_id(&self) -> &str {
        self.inner
            .as_ref()
            .map(|i| i.ack_id.as_str())
            .unwrap_or_default()
    }
}

impl Drop for AtLeastOnce {
    /// Rejects the message associated with this handler.
    ///
    /// The message will be removed from this `Subscriber`'s lease management.
    /// The service will redeliver this message, possibly to another client.
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.nack();
        }
    }
}

/// A handler for exactly-once delivery.
#[derive(Debug)]
pub struct ExactlyOnce {
    inner: Option<ExactlyOnceImpl>,
}

impl ExactlyOnce {
    pub(super) fn new(
        ack_id: String,
        ack_tx: UnboundedSender<Action>,
        // TODO(#3964): support confirmed acks
    ) -> Self {
        Self {
            inner: Some(ExactlyOnceImpl {
                ack_id,
                ack_tx,
                // TODO(#3964): support confirmed acks
            }),
        }
    }

    /// Acknowledge the message associated with this handler.
    ///
    /// Note that the acknowledgement is best effort. The message may still be
    /// redelivered to this client, or another client.
    pub fn ack(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.ack();
        }
    }

    // TODO(#3964): add confirmed_ack()

    #[cfg(test)]
    pub(crate) fn ack_id(&self) -> &str {
        self.inner
            .as_ref()
            .map(|i| i.ack_id.as_str())
            .unwrap_or_default()
    }
}

impl Drop for ExactlyOnce {
    /// Rejects the message associated with this handler.
    ///
    /// The message will be removed from this `Subscriber`'s lease management.
    /// The service will redeliver this message, possibly to another client.
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.nack();
        }
    }
}

#[derive(Debug)]
struct ExactlyOnceImpl {
    pub(super) ack_id: String,
    pub(super) ack_tx: UnboundedSender<Action>,
    // TODO(#3964): support confirmed acks
}

impl ExactlyOnceImpl {
    pub fn ack(self) {
        let _ = self.ack_tx.send(Action::ExactlyOnceAck(self.ack_id));
    }

    pub fn nack(self) {
        let _ = self.ack_tx.send(Action::ExactlyOnceNack(self.ack_id));
    }

    // TODO(#3964): add confirmed_ack()
}

/// The result of a confirmed acknowledgement.
pub(crate) type AckResult = std::result::Result<(), AckError>;

#[cfg(test)]
mod tests {
    use super::super::lease_state::tests::test_id;
    use super::*;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn handler_at_least_once_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = Handler::AtLeastOnce(AtLeastOnce::new(test_id(1), ack_tx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::Ack(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_at_least_once_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = Handler::AtLeastOnce(AtLeastOnce::new(test_id(1), ack_tx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_exactly_once_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = Handler::ExactlyOnce(ExactlyOnce::new(test_id(1), ack_tx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceAck(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_exactly_once_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = Handler::ExactlyOnce(ExactlyOnce::new(test_id(1), ack_tx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceNack(test_id(1)));

        Ok(())
    }

    #[test]
    fn at_least_once_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = AtLeastOnce::new(test_id(1), ack_tx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::Ack(test_id(1)));

        Ok(())
    }

    #[test]
    fn at_least_once_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = AtLeastOnce::new(test_id(1), ack_tx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn exactly_once_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceAck(test_id(1)));

        Ok(())
    }

    #[test]
    fn exactly_once_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceNack(test_id(1)));

        Ok(())
    }
}
