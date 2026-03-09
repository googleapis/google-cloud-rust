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

// [START rust_request_retry] ANCHOR: request-retry
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::AlwaysRetry;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use std::time::Duration;

pub async fn sample(
    client: &SecretManagerService,
    project_id: &str,
    secret_id: &str,
) -> anyhow::Result<()> {
    // [START rust_request_retry_request] ANCHOR: request-retry-request
    client
        .delete_secret()
        .set_name(format!("projects/{project_id}/secrets/{secret_id}"))
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .send()
        .await?;
    // [END rust_request_retry_request] ANCHOR_END: request-retry-request

    Ok(())
}
// [END rust_request_retry] ANCHOR_END: request-retry
