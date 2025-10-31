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

use crate::{generated::gapic_dataplane, publisher::options::BatchingOptions};
use tokio::sync::{mpsc, oneshot};

/// Object that is passed to the worker task over the
/// main channel. This represents a single message and the sender
/// half of the channel to resolve the [PublishHandle].
pub struct ToWorker {
    pub msg: crate::model::PubsubMessage,
    pub tx: oneshot::Sender<crate::Result<String>>,
}

/// The worker is spawned in a background task and handles
/// batching and publishing all messages that are sent to the publisher.
#[derive(Debug)]
pub struct Worker {
    topic_name: String,
    client: gapic_dataplane::client::Publisher,
    #[allow(dead_code)]
    batching_options: BatchingOptions,
    rx: mpsc::UnboundedReceiver<ToWorker>,
}

impl Worker {
    pub(crate) fn new(
        topic_name: String,
        client: gapic_dataplane::client::Publisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<ToWorker>,
    ) -> Self {
        Self {
            topic_name,
            client,
            rx,
            batching_options,
        }
    }

    pub(crate) async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            let client = self.client.clone();
            let topic = self.topic_name.clone();
            // In the future, we may also want to keep track of JoinHandles in order to
            // flush the results.
            let _handle = tokio::spawn(async move {
                // For now, we just send the message immediately.
                // We will want to batch these requests.
                let request = client
                    .publish()
                    .set_topic(topic)
                    .set_messages(vec![msg.msg]);

                // Handle the response by extracting the message ID on success.
                let result = request
                    .send()
                    .await
                    .map(|response| response.message_ids.get(0).cloned().unwrap_or_default());

                // The user may have dropped the handle, so it is ok if this fails.
                let _ = msg.tx.send(result);
            });
        }
    }
}
