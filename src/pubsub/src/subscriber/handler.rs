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

use tokio::sync::mpsc::UnboundedSender;

/// The action an application does with a message.
#[derive(Debug, PartialEq)]
pub(super) enum AckResult {
    Ack(String),
    Nack(String),
    // TODO(#3964) - support exactly once acking
}

/// A handler for acknowledging or rejecting messages.
#[derive(Debug)]
#[non_exhaustive]
pub enum Handler {
    AtLeastOnce(AtLeastOnce),
    // TODO(#3964) - support exactly once acking
}

impl Handler {
    /// Acknowledge the message associated with this handler.
    ///
    /// Note that the acknowledgement is best effort. The message may still be
    /// redelivered to this client, or another client.
    pub fn ack(self) {
        match self {
            Handler::AtLeastOnce(h) => h.ack(),
        }
    }

    /// Rejects the message associated with this handler.
    ///
    /// The message will be removed from this `Subscriber`'s lease management.
    /// The service will redeliver this message, possibly to another client.
    pub fn nack(self) {
        match self {
            Handler::AtLeastOnce(h) => h.nack(),
        }
    }
}

/// A handler for at-least-once delivery.
#[derive(Debug)]
pub struct AtLeastOnce {
    pub(super) ack_id: String,
    pub(super) ack_tx: UnboundedSender<AckResult>,
}

impl AtLeastOnce {
    /// Acknowledge the message associated with this handler.
    ///
    /// Note that the acknowledgement is best effort. The message may still be
    /// redelivered to this client, or another client.
    pub fn ack(self) {
        let _ = self.ack_tx.send(AckResult::Ack(self.ack_id));
    }

    /// Rejects the message associated with this handler.
    ///
    /// The message will be removed from this `Subscriber`'s lease management.
    /// The service will redeliver this message, possibly to another client.
    pub fn nack(self) {
        let _ = self.ack_tx.send(AckResult::Nack(self.ack_id));
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
        let h = Handler::AtLeastOnce(AtLeastOnce {
            ack_id: test_id(1),
            ack_tx,
        });
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Ack(test_id(1)));

        Ok(())
    }

    #[test]
    fn handler_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = Handler::AtLeastOnce(AtLeastOnce {
            ack_id: test_id(1),
            ack_tx,
        });
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.nack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Nack(test_id(1)));

        Ok(())
    }

    #[test]
    fn at_least_once_ack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = AtLeastOnce {
            ack_id: test_id(1),
            ack_tx,
        };
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.ack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Ack(test_id(1)));

        Ok(())
    }

    #[test]
    fn at_least_once_nack() -> anyhow::Result<()> {
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let h = AtLeastOnce {
            ack_id: test_id(1),
            ack_tx,
        };
        assert_eq!(ack_rx.try_recv(), Err(TryRecvError::Empty));

        h.nack();
        let ack = ack_rx.try_recv()?;
        assert_eq!(ack, AckResult::Nack(test_id(1)));

        Ok(())
    }
}
