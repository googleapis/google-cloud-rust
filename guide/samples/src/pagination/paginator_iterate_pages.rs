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

// [START rust_paginator_use] ANCHOR: paginator-use
use google_cloud_gax::paginator::Paginator as _;
// [END rust_paginator_use] ANCHOR_END: paginator-use
use google_cloud_gax::retry_policy::AlwaysRetry;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use std::time::Duration;

pub async fn paginator_iterate_pages(project_id: &str) -> crate::Result<()> {
    let client = SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;

    // [START rust_paginator_iterate_pages] ANCHOR: paginator-iterate-pages
    let mut list = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_page();
    while let Some(page) = list.next().await {
        let page = page?;
        println!("  next_page_token={}", page.next_page_token);
        page.secrets
            .into_iter()
            .for_each(|secret| println!("    secret={}", secret.name));
    }
    // [END rust_paginator_iterate_pages] ANCHOR_END: paginator-iterate-pages

    Ok(())
}
