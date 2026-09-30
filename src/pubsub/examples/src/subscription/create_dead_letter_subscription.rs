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

// [START pubsub_dead_letter_create_subscription]
use google_cloud_pubsub::client::SubscriptionAdmin;
use google_cloud_pubsub::model::DeadLetterPolicy;

pub async fn sample(
    client: &SubscriptionAdmin,
    project_id: &str,
    topic_id: &str,
    subscription_id: &str,
    dead_letter_topic_id: &str,
) -> anyhow::Result<()> {
    let subscription_name = format!("projects/{project_id}/subscriptions/{subscription_id}");
    let topic_name = format!("projects/{project_id}/topics/{topic_id}");
    let dead_letter_topic_name = format!("projects/{project_id}/topics/{dead_letter_topic_id}");

    let subscription = client
        .create_subscription()
        .set_name(subscription_name)
        .set_topic(topic_name)
        .set_dead_letter_policy(
            DeadLetterPolicy::new()
                .set_dead_letter_topic(dead_letter_topic_name)
                .set_max_delivery_attempts(10),
        )
        .send()
        .await?;

    println!("successfully created subscription {subscription:?}");
    Ok(())
}
// [END pubsub_dead_letter_create_subscription]
