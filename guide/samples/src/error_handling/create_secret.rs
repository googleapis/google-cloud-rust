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

// ANCHOR: create-secret
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::AlwaysRetry;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use google_cloud_secretmanager_v1::model::{Replication, Secret, replication};
use std::time::Duration;

pub async fn create_secret(
    client: &SecretManagerService,
    project_id: &str,
    secret_id: &str,
) -> anyhow::Result<Secret> {
    let secret = client
        .create_secret()
        .set_parent(format!("projects/{project_id}"))
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .set_secret_id(secret_id)
        .set_secret(
            Secret::new()
                .set_replication(Replication::new().set_replication(
                    replication::Replication::Automatic(replication::Automatic::new().into()),
                ))
                .set_labels([("integration-test", "true")]),
        )
        .send()
        .await?;
    Ok(secret)
}
// ANCHOR_END: create-secret
