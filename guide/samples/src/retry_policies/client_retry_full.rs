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

// [START rust_client_retry_full] ANCHOR: client-retry-full
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_gax::retry_policy::Aip194Strict;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use std::time::Duration;

pub async fn sample(project_id: &str) -> crate::Result<()> {
    // [START rust_client_retry_full_client] ANCHOR: client-retry-full-client
    let client = SecretManagerService::builder()
        .with_retry_policy(
            Aip194Strict
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;
    // [END rust_client_retry_full_client] ANCHOR_END: client-retry-full-client

    // [START rust_client_retry_full_request] ANCHOR: client-retry-full-request
    let mut list = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    while let Some(secret) = list.next().await {
        let secret = secret?;
        println!("  secret={}", secret.name);
    }
    // [END rust_client_retry_full_request] ANCHOR_END: client-retry-full-request

    Ok(())
}
// [END rust_client_retry_full] ANCHOR_END: client-retry-full
