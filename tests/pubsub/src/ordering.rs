// Copyright 2026 Google LLC
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

use futures::future::join_all;
use google_cloud_pubsub::client::{Publisher, Subscriber};
use google_cloud_pubsub::model::Message;
use std::collections::HashMap;

pub async fn roundtrip(topic_name: &str, subscription_name: &str) -> anyhow::Result<()> {
    const MESSAGES_PER_KEY: i32 = 1_000;
    const ORDERING_PREFIX: &str = "ordering-key-";
    const NUM_KEYS: usize = 4;

    tracing::info!("testing ordered delivery keys()");
    let publisher = Publisher::builder(topic_name)
        // Force the publisher to send multiple batches per key. This gives us
        // some extra confidence in the Publisher's ordering logic.
        .set_message_count_threshold(100)
        .build()
        .await?;
    let subscriber = Subscriber::builder().build().await?;
    let mut session = subscriber.streaming_pull(subscription_name).start();

    let subscribe = tokio::spawn(async move {
        let mut expected_indices = HashMap::new();
        while expected_indices.values().all(|&v| v == MESSAGES_PER_KEY) {
            let Some((m, h)) = session.next().await.transpose()? else {
                anyhow::bail!("Stream somehow ended.")
            };
            if !m.ordering_key.starts_with(ORDERING_PREFIX) {
                continue;
            }
            let index = parse_index(&m.data)?;
            let expected_index = expected_indices.entry(m.ordering_key).or_default();
            if index > *expected_index {
                // Messages can be redelivered. The only guarantee we can make
                // is that there is not a gap in delivery for each key.
                anyhow::bail!("Received messages out of order.")
            } else if index == *expected_index {
                *expected_index += 1;
            }
            h.ack();
        }
        Ok(())
    });

    let mut handles = Vec::new();
    for i in 0..MESSAGES_PER_KEY {
        for k in 0..NUM_KEYS {
            handles.push(
                publisher.publish(
                    Message::new()
                        .set_ordering_key(format!("{ORDERING_PREFIX}{k}"))
                        .set_data(format!("{i}")),
                ),
            );
        }
    }
    join_all(handles)
        .await
        .into_iter()
        .try_for_each(|r| r.map(|_| ()))?;
    tracing::info!("successfully published messages");
    subscribe.await??;
    tracing::info!("successfully received all messages in order.");

    Ok(())
}

fn parse_index(data: &bytes::Bytes) -> anyhow::Result<i32> {
    Ok(std::str::from_utf8(data)?.trim().parse::<i32>()?)
}
