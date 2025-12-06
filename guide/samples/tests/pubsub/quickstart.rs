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

// ANCHOR: quickstart
pub async fn quickstart(project_id: &str, topic_id: &str) -> anyhow::Result<()> {
    // ANCHOR: topicadmin
    use google_cloud_pubsub::client::TopicAdmin;
    let topic_admin = TopicAdmin::builder().build().await?;
    // ANCHOR_END: topicadmin

    // ANCHOR: createtopic
    let topic = topic_admin
        .create_topic()
        .set_name(format!("projects/{project_id}/topics/{topic_id}"))
        .send()
        .await?;
    // ANCHOR_END: createtopic

    // ANCHOR: publisher
    use google_cloud_pubsub::client::PublisherFactory;
    let factory = PublisherFactory::builder().build().await?;
    let publisher = factory.publisher(topic.name.clone()).build();
    // ANCHOR_END: publisher

    // ANCHOR: publish
    let mut handles = Vec::new();
    for i in 0..10 {
        use google_cloud_pubsub::model::PubsubMessage;
        let msg = PubsubMessage::new().set_data(format!("message {}", i));
        handles.push(publisher.publish(msg));
    }
    // ANCHOR_END: publish

    // ANCHOR: publishresults
    for (i, handle) in handles.into_iter().enumerate() {
        let message_id = handle.await?;
        println!("Message {i} sent with ID: {message_id}");
    }
    // ANCHOR_END: publishresults

    // ANCHOR: cleanup
    topic_admin
        .delete_topic()
        .set_topic(topic.name)
        .send()
        .await?;
    // ANCHOR_END: cleanup
    Ok(())
}
// ANCHOR_END: quickstart
