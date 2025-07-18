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

//! Examples showing how to configure retry policies.

// ANCHOR: client-retry
pub async fn client_retry(project_id: &str) -> crate::Result<()> {
    use google_cloud_gax::paginator::ItemPaginator as _;
    use google_cloud_gax::retry_policy::Aip194Strict;
    use google_cloud_secretmanager_v1 as secret_manager;

    // ANCHOR: client-retry-client
    let client = secret_manager::client::SecretManagerService::builder()
        .with_retry_policy(Aip194Strict)
        .build()
        .await?;
    // ANCHOR_END: client-retry-client

    // ANCHOR: client-retry-request
    let mut list = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    while let Some(secret) = list.next().await {
        let secret = secret?;
        println!("  secret={}", secret.name);
    }
    // ANCHOR_END: client-retry-request

    Ok(())
}
// ANCHOR_END: client-retry

// ANCHOR: client-retry-full
pub async fn client_retry_full(project_id: &str) -> crate::Result<()> {
    use google_cloud_gax::paginator::ItemPaginator as _;
    use google_cloud_gax::retry_policy::Aip194Strict;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_secretmanager_v1 as secret_manager;
    use std::time::Duration;

    // ANCHOR: client-retry-full-client
    let client = secret_manager::client::SecretManagerService::builder()
        .with_retry_policy(
            Aip194Strict
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;
    // ANCHOR_END: client-retry-full-client

    // ANCHOR: client-retry-full-request
    let mut list = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    while let Some(secret) = list.next().await {
        let secret = secret?;
        println!("  secret={}", secret.name);
    }
    // ANCHOR_END: client-retry-full-request

    Ok(())
}
// ANCHOR_END: client-retry-full

// ANCHOR: request-retry
use google_cloud_secretmanager_v1 as secret_manager;
pub async fn request_retry(
    client: &secret_manager::client::SecretManagerService,
    project_id: &str,
    secret_id: &str,
) -> crate::Result<()> {
    use google_cloud_gax::options::RequestOptionsBuilder;
    use google_cloud_gax::retry_policy::AlwaysRetry;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use std::time::Duration;

    // ANCHOR: request-retry-request
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
    // ANCHOR_END: request-retry-request

    Ok(())
}
// ANCHOR_END: request-retry
