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

use super::options::BatchingOptions;
use crate::error::PublishError;
use crate::publisher::actor::BundledMessage;

#[derive(Debug, Default)]
pub(crate) struct Batch {
    messages: Vec<BundledMessage>,
    initial_size: u32,
    messages_byte_size: u32,
    batching_options: BatchingOptions,
}

impl Batch {
    pub(crate) fn new(initial_size: u32, batching_options: BatchingOptions) -> Self {
        Batch {
            initial_size,
            messages_byte_size: initial_size,
            batching_options,
            ..Batch::default()
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.messages.is_empty()
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

    fn message_size(msg: &crate::model::Message) -> usize {
        // This is only an estimate and not the wire length.
        // TODO(#3963): If we move on to use protobuf crate, then it may be
        // possible to use compute_size to find the wire length.
        msg.attributes
            .iter()
            .fold(msg.data.len() + msg.ordering_key.len(), |acc, (k, v)| {
                acc + k.len() + v.len()
            })
    }

    pub(crate) fn at_threshold(&mut self) -> bool {
        self.len() as u32 >= self.batching_options.message_count_threshold
            || self.size() >= self.batching_options.byte_threshold
    }

    // Return true if adding the next message is within the byte threshold.
    pub(crate) fn can_add(&mut self, next: &BundledMessage) -> bool {
        self.size() + Self::message_size(&next.msg) as u32 <= self.batching_options.byte_threshold
    }

    /// Drains all messages from the batch, resetting the message byte size back
    /// to the initial size, and returns a tuple of `(Vec<Message>, Vec<Sender>)`.
    pub(crate) fn drain_messages(
        &mut self,
    ) -> (
        Vec<crate::model::Message>,
        Vec<tokio::sync::oneshot::Sender<Result<String, PublishError>>>,
    ) {
        self.messages_byte_size = self.initial_size;
        self.messages.drain(..).map(|msg| (msg.msg, msg.tx)).unzip()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        model::Message,
        publisher::actor::BundledMessage,
        publisher::batch::{Batch, BatchingOptions},
    };
    use google_cloud_test_macros::tokio_test_no_panics;

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: crate::RequestOptions) -> crate::Result<crate::Response<crate::model::PublishResponse>>;
        }
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_push_and_drain_batch() -> anyhow::Result<()> {
        let mut batch = Batch::new("topic".len() as u32, BatchingOptions::default());
        assert!(batch.is_empty());

        let (message_a, _rx_a) = create_bundled_message_from_bytes("hello");
        batch.push(message_a);
        assert_eq!(batch.len(), 1);

        let (message_b, _rx_b) = create_bundled_message_from_bytes(", ");
        batch.push(message_b);
        assert_eq!(batch.len(), 2);

        let (message_c, _rx_c) = create_bundled_message_from_bytes("world");
        batch.push(message_c);
        assert_eq!(batch.len(), 3);

        let (msgs, txs) = batch.drain_messages();
        assert_eq!(batch.len(), 0);
        assert_eq!(msgs.len(), 3);
        assert_eq!(txs.len(), 3);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_size() -> anyhow::Result<()> {
        use std::collections::HashMap;

        let topic = "topic";
        let mut batch: Batch = Batch::new(topic.len() as u32, BatchingOptions::default());
        let mut expected_encoded_len = topic.len();
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let (message_data_only, _rx) = create_bundled_message_from_bytes("message_data_only");
        expected_encoded_len += message_data_only.msg.data.len();
        batch.push(message_data_only);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let (message_with_ordering, _rx) = create_bundled_message_from_pubsub_message(
            Message::new().set_ordering_key("ordering_key"),
        );
        expected_encoded_len += message_with_ordering.msg.ordering_key.len();
        batch.push(message_with_ordering);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let attributes = HashMap::from([("k1", "v1"), ("key2", "value2")]);
        let (message_with_attributes, _rx) =
            create_bundled_message_from_pubsub_message(Message::new().set_attributes(attributes));
        expected_encoded_len += 14;
        batch.push(message_with_attributes);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let (msgs, _txs) = batch.drain_messages();
        assert_eq!(msgs.len(), 3);
        assert_eq!(batch.size(), "topic".len() as u32);

        Ok(())
    }

    fn create_bundled_message_from_bytes<T: Into<::bytes::Bytes>>(
        data: T,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<std::result::Result<String, crate::error::PublishError>>,
    ) {
        create_bundled_message_from_pubsub_message(Message::new().set_data(data.into()))
    }

    fn create_bundled_message_from_pubsub_message(
        msg: Message,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<std::result::Result<String, crate::error::PublishError>>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (BundledMessage { tx, msg }, rx)
    }
}
