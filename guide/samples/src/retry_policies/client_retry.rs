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

// [START rust_client_retry] ANCHOR: client-retry
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_gax::retry_policy::Aip194Strict;
use google_cloud_secretmanager_v1::client::SecretManagerService;

pub async fn client_retry(project_id: &str) -> crate::Result<()> {
    // [START rust_client_retry_client] ANCHOR: client-retry-client
    let client = SecretManagerService::builder()
        .with_retry_policy(Aip194Strict)
        .build()
        .await?;
    // [END rust_client_retry_client] ANCHOR_END: client-retry-client

    // [START rust_client_retry_request] ANCHOR: client-retry-request
    let mut list = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    while let Some(secret) = list.next().await {
        let secret = secret?;
        println!("  secret={}", secret.name);
    }
    // [END rust_client_retry_request] ANCHOR_END: client-retry-request

    Ok(())
}
// [END rust_client_retry] ANCHOR_END: client-retry
