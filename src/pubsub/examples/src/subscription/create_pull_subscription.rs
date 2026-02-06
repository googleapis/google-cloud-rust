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

// [START pubsub_create_pull_subscription]
use google_cloud_pubsub::client::SubscriptionAdmin;

pub async fn sample(
    client: &SubscriptionAdmin,
    project_id: &str,
    topic_id: &str,
    subscription_id: &str,
) -> anyhow::Result<()> {
    let subscription_name = format!("projects/{project_id}/subscriptions/{subscription_id}");
    let topic_name = format!("projects/{project_id}/topics/{topic_id}");

    let subscription = client
        .create_subscription()
        .set_name(subscription_name)
        .set_topic(topic_name)
        .send()
        .await?;

    println!("successfully created subscription {subscription:?}");
    Ok(())
}
// [END pubsub_create_pull_subscription]
