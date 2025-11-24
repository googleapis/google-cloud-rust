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

use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::publisher::worker::BundledMessage;
use futures::stream::FuturesUnordered;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct Batch {
    // TODO(#3686): A batch should also keep track of its total size
    // for improved performance.
    messages: Vec<BundledMessage>,
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

impl Batch {
    pub(crate) fn new() -> Self {
        Batch {
            messages: Vec::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.messages.len()
    }

    pub(crate) fn push(&mut self, msg: BundledMessage) {
        self.messages.push(msg);
    }

    /// Drains the batch and spawns a task to send the messages.
    ///
    /// This method mutably drains the messages from the current batch, leaving it
    /// empty, and returns a `JoinHandle` for the spawned send operation. This allows
    /// the `Worker` to immediately begin creating a new batch while the old one is
    /// being sent in the background.
    pub(crate) fn flush(
        &mut self,
        client: GapicPublisher,
        topic: String,
        inflight: &mut FuturesUnordered<tokio::task::JoinHandle<()>>,
    ) {
        if self.is_empty() {
            return;
        }
        let batch_to_send = Self {
            messages: self.messages.drain(..).collect(),
        };
        inflight.push(tokio::spawn(batch_to_send.send(client, topic)));
    }

    /// Send the batch to the service and process the results.
    async fn send(self, client: GapicPublisher, topic: String) {
        let (msgs, txs): (Vec<_>, Vec<_>) = self
            .messages
            .into_iter()
            .map(|msg| (msg.msg, msg.tx))
            .unzip();
        let request = client.publish().set_topic(topic).set_messages(msgs);

        // Handle the response by extracting the message ID on success.
        match request.send().await {
            Err(e) => {
                let e = Arc::new(e);
                for tx in txs {
                    // The user may have dropped the handle, so it is ok if this fails.
                    // TODO(#3689): The error type for this is incorrect, will need to handle
                    // this error propagation more fully.
                    let _ = tx.send(Err(gax::error::Error::io(e.clone())));
                }
            }
            Ok(result) => {
                txs.into_iter()
                    .zip(result.message_ids.into_iter())
                    .for_each(|(tx, result)| {
                        // The user may have dropped the handle, so it is ok if this fails.
                        let _ = tx.send(Ok(result));
                    });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::PubsubMessage;
    use crate::publisher::batch::Batch;
    use crate::publisher::worker::BundledMessage;

    #[tokio::test]
    async fn test_push_batch() {
        let mut batch = Batch::new();
        assert!(batch.is_empty());

        let (message_a, _rx_a) = create_bundled_message_helper("hello".to_string());
        batch.push(message_a);
        assert_eq!(batch.len(), 1);

        let (message_b, _rx_b) = create_bundled_message_helper(", ".to_string());
        batch.push(message_b);
        assert_eq!(batch.len(), 2);

        let (message_c, _rx_c) = create_bundled_message_helper("world".to_string());
        batch.push(message_c);
        assert_eq!(batch.len(), 3);
    }

    fn create_bundled_message_helper(
        data: String,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<crate::Result<String>>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            BundledMessage {
                tx,
                msg: PubsubMessage::new().set_data(data),
            },
            rx,
        )
    }
}
