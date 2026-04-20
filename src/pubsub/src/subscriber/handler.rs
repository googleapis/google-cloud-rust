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
//! To reject (nack) a message, you call [`Handler::nack()`]. The
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
//!         h.nack();
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
use crate::subscriber::lease_state::NACK_SHUTDOWN_ERROR;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Receiver;

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
///         h.nack();
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
/// To reject (nack) a message, you call [`Handler::nack()`]. The
/// service will redeliver the message.
///
/// ## Exactly-once delivery
///
/// If your subscription has [exactly-once delivery] enabled, you should
/// destructure this enum into its [`Handler::ExactlyOnce`] branch.
///
/// Only when `ExactlyOnce::confirmed_ack()` returns `Ok` can you be certain
/// that the message will not be redelivered.
///
/// [exactly-once delivery]: https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery
///
/// ```
/// use google_cloud_pubsub::model::Message;
/// # use google_cloud_pubsub::subscriber::handler::Handler;
/// async fn on_message(m: Message, h: Handler) {
///   let Handler::ExactlyOnce(h) = h else {
///     panic!("Oops, my subscription does not have exactly-once delivery enabled.")
///   };
///   match h.confirmed_ack().await {
///     Ok(()) => println!("Confirmed ack for message={m:?}. The message will not be redelivered."),
///     Err(e) => println!("Failed to confirm ack for message={m:?} with error={e:?}"),
///   }
/// }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Handler {
    /// A handler for at-least-once delivery.
    ///
    /// The handler type is determined by the subscription configuration.
    ///
    /// ```
    /// # use google_cloud_pubsub::subscriber::handler::{Handler, AtLeastOnce};
    /// # fn on_message(h: Handler) {
    /// if let Handler::AtLeastOnce(h) = h {
    ///     h.ack();
    /// }
    /// # }
    /// ```
    AtLeastOnce(AtLeastOnce),
    /// A handler for exactly-once delivery.
    ///
    /// The handler type is determined by the subscription configuration.
    ///
    /// ```
    /// # use google_cloud_pubsub::subscriber::handler::{Handler, ExactlyOnce};
    /// # async fn on_message(h: Handler) {
    /// if let Handler::ExactlyOnce(h) = h {
    ///     let _ = h.confirmed_ack().await;
    /// }
    /// # }
    /// ```
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

    /// Rejects the message associated with this handler.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_pubsub::model::Message;
    /// # use google_cloud_pubsub::subscriber::handler::Handler;
    /// fn on_message(m: Message, h: Handler) {
    ///   println!("Received message: {m:?}");
    ///   h.nack();
    /// }
    /// ```
    ///
    /// The message will be removed from this `Subscriber`'s lease management.
    /// The service will redeliver this message, possibly to another client.
    pub fn nack(self) {
        match self {
            Handler::AtLeastOnce(h) => h.nack(),
            Handler::ExactlyOnce(h) => h.nack(),
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

    /// Rejects the message associated with this handler.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_pubsub::model::Message;
    /// # use google_cloud_pubsub::subscriber::handler::AtLeastOnce;
    /// fn on_message(m: Message, h: AtLeastOnce) {
    ///   println!("Received message: {m:?}");
    ///   h.nack();
    /// }
    /// ```
    ///
    /// The message will be removed from this `Subscriber`'s lease management.
    /// The service will redeliver this message, possibly to another client.
    pub fn nack(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.nack();
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
        result_rx: Receiver<AckResult>,
    ) -> Self {
        Self {
            inner: Some(ExactlyOnceImpl {
                ack_id,
                ack_tx,
                result_rx,
            }),
        }
    }

    /// Acknowledge the message associated with this handler.
    ///
    /// Note that the acknowledgement is best effort. The message may still be
    /// redelivered to this client, or another client.
    pub(crate) fn ack(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.ack();
        }
    }

    pub(crate) fn nack(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.nack();
        }
    }

    /// Strongly acknowledge the message associated with this handler.
    ///
    /// ```
    /// use google_cloud_pubsub::model::Message;
    /// # use google_cloud_pubsub::subscriber::handler::ExactlyOnce;
    /// async fn on_message(m: Message, h: ExactlyOnce) {
    ///   match h.confirmed_ack().await {
    ///     Ok(()) => println!("Confirmed ack for message={m:?}. The message will not be redelivered."),
    ///     Err(e) => println!("Failed to confirm ack for message={m:?} with error={e:?}"),
    ///   }
    /// }
    /// ```
    ///
    /// If the result is an `Ok`, the message is guaranteed not to be delivered
    /// again.
    ///
    /// If the result is an `Err`, the message may be redelivered, but this is
    /// not guaranteed. If no redelivery occurs a sufficient interval after an
    /// error, the acknowledgement likely succeeded.
    pub async fn confirmed_ack(mut self) -> std::result::Result<(), AckError> {
        let inner = self.inner.take().expect("handler impl is always some");
        inner.confirmed_ack().await
    }

    /// Rejects the message associated with this handler and waits for
    /// confirmation.
    ///
    /// ```
    /// use google_cloud_pubsub::model::Message;
    /// # use google_cloud_pubsub::subscriber::handler::ExactlyOnce;
    /// async fn on_message(m: Message, h: ExactlyOnce) {
    ///   match h.confirmed_nack().await {
    ///     Ok(()) => println!("Confirmed nack for message={m:?}. The message will be redelivered."),
    ///     Err(e) => println!("Failed to confirm nack for message={m:?} with error={e:?}"),
    ///   }
    /// }
    /// ```
    ///
    /// If the result is an `Ok`, the message is guaranteed to be immediately
    /// considered for redelivery. If an error occurs, the message will still
    /// be redelivered, but it may be held for the remainder of its
    /// `max_lease_extension`.
    pub async fn confirmed_nack(mut self) -> std::result::Result<(), AckError> {
        let inner = self.inner.take().expect("handler impl is always some");
        inner.confirmed_nack().await
    }

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
    pub(super) result_rx: Receiver<AckResult>,
}

impl ExactlyOnceImpl {
    pub fn ack(self) {
        let _ = self.ack_tx.send(Action::ExactlyOnceAck(self.ack_id));
    }

    pub fn nack(self) {
        let _ = self.ack_tx.send(Action::ExactlyOnceNack(self.ack_id));
    }

    pub async fn confirmed_ack(self) -> AckResult {
        self.ack_tx
            .send(Action::ExactlyOnceAck(self.ack_id))
            .map_err(|_| AckError::ShutdownBeforeAck)?;
        self.result_rx
            .await
            .map_err(|e| AckError::Shutdown(e.into()))?
    }

    pub async fn confirmed_nack(self) -> AckResult {
        self.ack_tx
            .send(Action::ExactlyOnceNack(self.ack_id))
            .map_err(|_| AckError::Shutdown(NACK_SHUTDOWN_ERROR.into()))?;
        self.result_rx
            .await
            .map_err(|e| AckError::Shutdown(e.into()))?
    }
}

/// The result of a confirmed acknowledgement.
pub(super) type AckResult = std::result::Result<(), AckError>;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::super::lease_state::tests::test_id;
    use super::*;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot::channel;

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

        h.nack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_exactly_once_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (_result_tx, result_rx) = channel();
        let h = Handler::ExactlyOnce(ExactlyOnce::new(test_id(1), ack_tx, result_rx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceAck(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_exactly_once_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (_result_tx, result_rx) = channel();
        let h = Handler::ExactlyOnce(ExactlyOnce::new(test_id(1), ack_tx, result_rx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.nack();
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

        h.nack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn exactly_once_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (_result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceAck(test_id(1)));

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_success() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        let task = tokio::task::spawn(async move { h.confirmed_ack().await });

        let ack = ack_rx.recv().await.expect("ack should be sent");
        assert_eq!(ack, Action::ExactlyOnceAck(test_id(1)));

        result_tx
            .send(Ok(()))
            .expect("sending on a channel succeeds");
        task.await??;

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_nack_success() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        let task = tokio::task::spawn(async move { h.confirmed_nack().await });

        let nack = ack_rx.recv().await.expect("ack should be sent");
        assert_eq!(nack, Action::ExactlyOnceNack(test_id(1)));

        result_tx
            .send(Ok(()))
            .expect("sending on a channel succeeds");
        task.await??;

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_error() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        let task = tokio::task::spawn(async move { h.confirmed_ack().await });

        let ack = ack_rx.recv().await.expect("ack should be sent");
        assert_eq!(ack, Action::ExactlyOnceAck(test_id(1)));

        result_tx
            .send(Err(AckError::LeaseExpired))
            .expect("sending on a channel succeeds");
        let err = task.await?.expect_err("ack should fail");
        assert!(matches!(err, AckError::LeaseExpired), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_nack_error() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        let task = tokio::task::spawn(async move { h.confirmed_nack().await });

        let nack = ack_rx.recv().await.expect("ack should be sent");
        assert_eq!(nack, Action::ExactlyOnceNack(test_id(1)));

        result_tx
            .send(Err(AckError::LeaseExpired))
            .expect("sending on a channel succeeds");
        let err = task.await?.expect_err("ack should fail");
        assert!(matches!(err, AckError::LeaseExpired), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_action_channel_closed() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (_result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));
        drop(ack_rx);

        let err = h.confirmed_ack().await.expect_err("ack should fail");
        assert!(matches!(err, AckError::ShutdownBeforeAck), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_nack_action_channel_closed() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (_result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));
        drop(ack_rx);

        let err = h.confirmed_nack().await.expect_err("nack should fail");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");
        assert_eq!(
            err.source()
                .expect("shutdown errors have a source")
                .to_string(),
            NACK_SHUTDOWN_ERROR
        );

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_result_channel_closed() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        let task = tokio::task::spawn(async move { h.confirmed_ack().await });

        let ack = ack_rx.recv().await.expect("ack should be sent");
        assert_eq!(ack, Action::ExactlyOnceAck(test_id(1)));

        drop(result_tx);
        let err = task.await?.expect_err("ack should fail");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");

        Ok(())
    }

    #[test]
    fn exactly_once_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (_result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.nack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceNack(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_at_least_once_nack_on_drop() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = Handler::AtLeastOnce(AtLeastOnce::new(test_id(1), ack_tx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_exactly_once_nack_on_drop() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (_result_tx, result_rx) = channel();
        let h = Handler::ExactlyOnce(ExactlyOnce::new(test_id(1), ack_tx, result_rx));
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceNack(test_id(1)));

        Ok(())
    }

    #[test]
    fn at_least_once_nack_on_drop() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = AtLeastOnce::new(test_id(1), ack_tx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn exactly_once_nack_on_drop() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let (_result_tx, result_rx) = channel();
        let h = ExactlyOnce::new(test_id(1), ack_tx, result_rx);
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        drop(h);
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, Action::ExactlyOnceNack(test_id(1)));

        Ok(())
    }
}
