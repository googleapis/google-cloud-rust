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

pub async fn roundtrip(topic_name: &str, subscription_name: &str) -> anyhow::Result<()> {
    const MESSAGE_COUNT: i32 = 1_000;
    const ORDERING_KEY: &str = "ordering-key";

    tracing::info!("testing ordered delivery()");
    let publisher = Publisher::builder(topic_name).build().await?;
    let subscriber = Subscriber::builder().build().await?;
    let mut session = subscriber.streaming_pull(subscription_name).start();
    let subscribe = tokio::spawn(async move {
        let mut expected_index = 0;
        while expected_index < MESSAGE_COUNT {
            let Some((m, h)) = session.next().await.transpose()? else {
                anyhow::bail!("Stream somehow ended.")
            };
            if m.ordering_key != ORDERING_KEY {
                continue;
            }
            let index = parse_index(&m.data)?;
            if index > expected_index {
                // Messages can be redelivered. The only guarantee we can make
                // is that there is not a gap in delivery.
                anyhow::bail!("Received messages out of order.")
            } else if index == expected_index {
                expected_index += 1;
            }
            h.ack();
        }

        Ok(())
    });

    let mut handles = Vec::new();
    for i in 0..MESSAGE_COUNT {
        handles.push(
            publisher.publish(
                Message::new()
                    .set_ordering_key(ORDERING_KEY)
                    .set_data(format!("{i}")),
            ),
        );
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
