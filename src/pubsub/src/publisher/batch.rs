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

use tokio::task::JoinSet;

use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::publisher::worker::BundledMessage;
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct Batch {
    messages: Vec<BundledMessage>,
    initial_size: u32,
    messages_byte_size: u32,
}

impl Batch {
    pub(crate) fn new(initial_size: u32) -> Self {
        Batch {
            initial_size,
            messages_byte_size: initial_size,
            ..Batch::default()
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.messages.len()
    }

    pub(crate) fn size(&self) -> u32 {
        self.messages_byte_size
    }

    pub(crate) fn push(&mut self, msg: BundledMessage) {
        self.messages_byte_size += Self::message_size(&msg.msg) as u32;
        self.messages.push(msg);
    }

    fn message_size(msg: &crate::model::PubsubMessage) -> usize {
        // This is only an estimate and not the wire length.
        // TODO(#3963): If we move on to use protobuf crate, then it may be
        // possible to use compute_size to find the wire length.
        msg.attributes
            .iter()
            .fold(msg.data.len() + msg.ordering_key.len(), |acc, (k, v)| {
                acc + k.len() + v.len()
            })
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
        inflight: &mut JoinSet<()>,
    ) {
        let batch_to_send = Self {
            initial_size: self.initial_size,
            messages: self.messages.drain(..).collect(),
            messages_byte_size: self.messages_byte_size,
        };
        self.messages_byte_size = self.initial_size;
        inflight.spawn(batch_to_send.send(client, topic));
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
                // TODO(#4013): To support message ordering retry, we need to correctly handle
                // the send error here with either retry or propagate to the user.
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
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{PublishResponse, PubsubMessage},
        publisher::batch::Batch,
        publisher::worker::BundledMessage,
    };
    use tokio::task::JoinSet;

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<crate::model::PublishResponse>>;
        }
    }

    #[tokio::test]
    async fn test_push_and_flush_batch() {
        let mut batch = Batch::new("topic".len() as u32);
        assert_eq!(batch.len(), 0);

        let (message_a, _rx_a) = create_bundled_message_from_bytes("hello");
        batch.push(message_a);
        assert_eq!(batch.len(), 1);

        let (message_b, _rx_b) = create_bundled_message_from_bytes(", ");
        batch.push(message_b);
        assert_eq!(batch.len(), 2);

        let (message_c, _rx_c) = create_bundled_message_from_bytes("world");
        batch.push(message_c);
        assert_eq!(batch.len(), 3);

        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once({
            |r, _| {
                assert_eq!(r.topic, "topic");
                assert_eq!(r.messages.len(), 3);
                Ok(gax::response::Response::from(PublishResponse::new()))
            }
        });
        let client = GapicPublisher::from_stub(mock);
        let mut inflight = JoinSet::new();
        batch.flush(client, "topic".to_string(), &mut inflight);
        assert_eq!(batch.len(), 0);
    }

    #[tokio::test]
    async fn test_size() {
        use std::collections::HashMap;

        let topic = "topic";
        let mut batch: Batch = Batch::new(topic.len() as u32);
        let mut expected_encoded_len = topic.len();
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let (message_data_only, _rx) = create_bundled_message_from_bytes("message_data_only");
        expected_encoded_len += message_data_only.msg.data.len();
        batch.push(message_data_only);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let (message_with_ordering, _rx) = create_bundled_message_from_pubsub_message(
            PubsubMessage::new().set_ordering_key("ordering_key"),
        );
        expected_encoded_len += message_with_ordering.msg.ordering_key.len();
        batch.push(message_with_ordering);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let attributes = HashMap::from([("k1", "v1"), ("key2", "value2")]);
        let (message_with_attributes, _rx) = create_bundled_message_from_pubsub_message(
            PubsubMessage::new().set_attributes(attributes),
        );
        expected_encoded_len += 14;
        batch.push(message_with_attributes);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once({
            |r, _| {
                assert_eq!(r.topic, "topic");
                assert_eq!(r.messages.len(), 3);
                Ok(gax::response::Response::from(PublishResponse::new()))
            }
        });
        let client = GapicPublisher::from_stub(mock);
        let mut inflight = JoinSet::new();
        batch.flush(client, "topic".to_string(), &mut inflight);
        assert_eq!(batch.size(), "topic".len() as u32);
    }

    fn create_bundled_message_from_bytes<T: Into<::bytes::Bytes>>(
        data: T,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<crate::Result<String>>,
    ) {
        create_bundled_message_from_pubsub_message(PubsubMessage::new().set_data(data.into()))
    }

    fn create_bundled_message_from_pubsub_message(
        msg: PubsubMessage,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<crate::Result<String>>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (BundledMessage { tx, msg }, rx)
    }
}
