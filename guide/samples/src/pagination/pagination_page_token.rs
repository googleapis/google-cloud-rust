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

use google_cloud_gax::retry_policy::AlwaysRetry;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use std::time::Duration;

pub async fn sample(project_id: &str) -> crate::Result<()> {
    let client = SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;

    // [START rust_paginator_page_token] ANCHOR: paginator-page-token
    let page = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .send()
        .await;
    let page = page?;
    let mut next_page_token = page.next_page_token.clone();
    page.secrets
        .into_iter()
        .for_each(|secret| println!("    secret={}", secret.name));

    while !next_page_token.is_empty() {
        println!("  next_page_token={next_page_token}");

        let page = client
            .list_secrets()
            .set_parent(format!("projects/{project_id}"))
            .set_page_token(next_page_token)
            .send()
            .await;
        let page = page?;
        next_page_token = page.next_page_token.clone();

        page.secrets
            .into_iter()
            .for_each(|secret| println!("    secret={}", secret.name));
    }
    // [END rust_paginator_page_token] ANCHOR_END: paginator-page-token

    Ok(())
}
