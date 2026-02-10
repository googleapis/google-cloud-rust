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

use tokio::sync::mpsc::UnboundedSender;

/// The action an application does with a message.
#[derive(Debug, PartialEq)]
pub(super) enum AckResult {
    Ack(String),
    Nack(String),
    // TODO(#3964) - support exactly once acking
}

/// A handler for acknowledging or rejecting messages.
///
/// To acknowledge (ack) a message, you call [`Handler::ack()`].
///
/// To reject (nack) a message, you [`drop()`][Drop::drop] the handler. The
/// message will be redelivered.
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
#[derive(Debug)]
#[non_exhaustive]
pub enum Handler {
    AtLeastOnce(AtLeastOnce),
    // TODO(#3964) - support exactly once acking
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
    /// redelivered to this client, or another client.
    pub fn ack(self) {
        match self {
            Handler::AtLeastOnce(h) => h.ack(),
        }
    }
}

#[derive(Debug)]
struct AtLeastOnceImpl {
    ack_id: String,
    ack_tx: UnboundedSender<AckResult>,
}

impl AtLeastOnceImpl {
    fn ack(self) {
        let _ = self.ack_tx.send(AckResult::Ack(self.ack_id));
    }

    fn nack(self) {
        let _ = self.ack_tx.send(AckResult::Nack(self.ack_id));
    }
}

/// A handler for at-least-once delivery.
#[derive(Debug)]
pub struct AtLeastOnce {
    inner: Option<AtLeastOnceImpl>,
}

impl AtLeastOnce {
    pub(super) fn new(ack_id: String, ack_tx: UnboundedSender<AckResult>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::super::lease_state::tests::test_id;
    use super::*;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn handler_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = Handler::AtLeastOnce(AtLeastOnce::new(test_id(1), ack_tx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Ack(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = Handler::AtLeastOnce(AtLeastOnce::new(test_id(1), ack_tx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn at_least_once_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = AtLeastOnce::new(test_id(1), ack_tx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Ack(test_id(1)));

        Ok(())
    }

    #[test]
    fn at_least_once_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = AtLeastOnce::new(test_id(1), ack_tx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn at_least_once_drop_nacks() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = AtLeastOnce::new(test_id(1), ack_tx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Nack(test_id(1)));

        Ok(())
    }
}
