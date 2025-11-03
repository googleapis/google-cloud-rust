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

use crate::{Error, Result};

use futures::future::join_all;
use pubsub::client::PublisherClient;
use pubsub::model::PubsubMessage;
pub use pubsub_samples::{cleanup_test_topic, create_test_topic};

pub async fn basic_publisher() -> Result<()> {
    let (topic_admin, topic) = create_test_topic().await?;

    tracing::info!("testing publish()");
    let client = PublisherClient::builder().build().await?;
    let publisher = client.publisher(topic.name.clone()).build();
    let messages: [PubsubMessage; 2] = [
        PubsubMessage::new().set_data("Hello"),
        PubsubMessage::new().set_data("World"),
    ];

    let mut handles = Vec::new();
    for msg in messages {
        handles.push(publisher.publish(msg));
    }

    let results = join_all(handles).await;
    let message_ids: Vec<_> = results
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::from)?;
    tracing::info!("successfully published messages: {:#?}", message_ids);
    cleanup_test_topic(&topic_admin, topic.name).await?;

    Ok(())
}

pub async fn basic_topic() -> Result<()> {
    let (client, topic) = create_test_topic().await?;

    tracing::info!("testing get_topic()");
    let get_topic = client
        .get_topic()
        .set_topic(topic.name.clone())
        .send()
        .await?;
    tracing::info!("success with get_topic={get_topic:?}");
    assert_eq!(get_topic.name, topic.name);

    cleanup_test_topic(&client, topic.name).await?;

    Ok(())
}
