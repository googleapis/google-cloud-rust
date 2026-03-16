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

use anyhow::Error;
use google_cloud_pubsub::client::{Publisher, Subscriber};
use google_cloud_pubsub::model::Message;
use google_cloud_pubsub::subscriber::handler::Handler;
use tokio::task::JoinSet;

pub async fn roundtrip(topic_name: &str, subscription_name: &str) -> anyhow::Result<()> {
    const MESSAGE_COUNT: usize = 1_000;

    tracing::info!("testing exactly once subscription");
    let publisher = Publisher::builder(topic_name).build().await?;
    let subscriber = Subscriber::builder().build().await?;
    let mut stream = subscriber.subscribe(subscription_name).build();

    let subscribe: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
        let mut handles = JoinSet::new();
        while handles.len() < MESSAGE_COUNT
            && let Some((_, Handler::ExactlyOnce(h))) = stream.next().await.transpose()?
        {
            handles.spawn(h.confirmed_ack());
        }
        assert_eq!(handles.len(), MESSAGE_COUNT);
        while let Some(r) = handles.join_next().await {
            r?.inspect_err(|e| tracing::info!("received unexpected confirm_ack error: {e:?}"))?;
        }
        Ok(())
    });

    let mut publish = JoinSet::new();
    for i in 0..MESSAGE_COUNT {
        publish.spawn(publisher.publish(Message::new().set_data(format!("{i}"))));
    }
    while let Some(r) = publish.join_next().await {
        r?.inspect_err(|e| tracing::info!("unexpected publish error: {e:?}"))?;
    }
    tracing::info!("successfully published messages");

    subscribe.await??;
    tracing::info!("successfully confirmed all messages");

    Ok(())
}
