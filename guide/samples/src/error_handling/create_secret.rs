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

use google_cloud_gax as gax;
use google_cloud_secretmanager_v1 as sm;

// ANCHOR: create-secret
pub async fn create_secret(
    client: &sm::client::SecretManagerService,
    project_id: &str,
    secret_id: &str,
) -> gax::Result<sm::model::Secret> {
    use google_cloud_gax::options::RequestOptionsBuilder;
    use google_cloud_gax::retry_policy::AlwaysRetry;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use std::time::Duration;

    client
        .create_secret()
        .set_parent(format!("projects/{project_id}"))
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .set_secret_id(secret_id)
        .set_secret(
            sm::model::Secret::new()
                .set_replication(sm::model::Replication::new().set_replication(
                    sm::model::replication::Replication::Automatic(
                        sm::model::replication::Automatic::new().into(),
                    ),
                ))
                .set_labels([("integration-test", "true")]),
        )
        .send()
        .await
}
// ANCHOR_END: create-secret
