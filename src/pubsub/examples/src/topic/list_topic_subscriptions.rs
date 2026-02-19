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

// [START pubsub_list_topic_subscriptions]
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_pubsub::client::TopicAdmin;

pub async fn sample(client: &TopicAdmin, project_id: &str, topic_id: &str) -> anyhow::Result<()> {
    let mut subscriptions = client
        .list_topic_subscriptions()
        .set_topic(format!("projects/{project_id}/topics/{topic_id}"))
        .by_item();

    println!("listing topic subscriptions");
    while let Some(subscription) = subscriptions.next().await.transpose()? {
        println!("{subscription:?}");
    }
    println!("DONE");
    Ok(())
}
// [END pubsub_list_topic_subscriptions]
