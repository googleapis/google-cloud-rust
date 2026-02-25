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

// [START rust_paginator_stream_items_use] ANCHOR: paginator-stream-items-use
use futures::stream::StreamExt;
use google_cloud_gax::paginator::ItemPaginator as _;
// [END rust_paginator_stream_items_use] ANCHOR_END: paginator-stream-items-use
use google_cloud_gax as gax;
use google_cloud_gax::retry_policy::AlwaysRetry;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use std::time::Duration;

pub async fn paginator_stream_items(project_id: &str) -> crate::Result<()> {
    let client = SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;

    // [START rust_paginator_stream_items] ANCHOR: paginator-stream-items
    let list = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item()
        .into_stream();
    list.map(|secret| -> gax::Result<()> {
        println!("  secret={}", secret?.name);
        Ok(())
    })
    .fold(Ok(()), async |acc, result| -> gax::Result<()> {
        acc.and(result)
    })
    .await?;
    // [END rust_paginator_stream_items] ANCHOR_END: paginator-stream-items

    Ok(())
}
