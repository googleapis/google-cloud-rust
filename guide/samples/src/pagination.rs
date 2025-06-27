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

//! Examples showing itearting Google API List methods with paginator.

use google_cloud_gax as gax;

pub async fn paginator_iterate_pages(project_id: &str) -> crate::Result<()> {
    use google_cloud_gax::paginator::Paginator as _;
    use google_cloud_gax::retry_policy::AlwaysRetry;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_secretmanager_v1 as secret_manager;
    use std::time::Duration;

    let client = secret_manager::client::SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;

    // ANCHOR: paginator-iterate-pages
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
    // ANCHOR_END: paginator-iterate-pages

    Ok(())
}

pub async fn paginator_stream_pages(project_id: &str) -> crate::Result<()> {
    use futures::stream::StreamExt;
    use google_cloud_gax::paginator::Paginator as _;
    use google_cloud_gax::retry_policy::AlwaysRetry;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_secretmanager_v1 as secret_manager;
    use std::time::Duration;

    let client = secret_manager::client::SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;

    // ANCHOR: paginator-stream-pages
    let list = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_page()
        .into_stream();
    list.enumerate()
        .map(|(index, page)| -> gax::Result<()> {
            println!("page={}, next_page_token={}", index, page?.next_page_token);
            Ok(())
        })
        .fold(Ok(()), async |acc, result| -> gax::Result<()> {
            acc.and(result)
        })
        .await?;
    // ANCHOR_END: paginator-stream-pages

    Ok(())
}

pub async fn paginator_iterate_items(project_id: &str) -> crate::Result<()> {
    // ANCHOR: paginator-use
    use google_cloud_gax::paginator::ItemPaginator as _;
    // ANCHOR_END: paginator-use
    use google_cloud_gax::retry_policy::AlwaysRetry;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_secretmanager_v1 as secret_manager;
    use std::time::Duration;

    let client = secret_manager::client::SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;

    // ANCHOR: paginator-iterate-items
    let mut list = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    while let Some(secret) = list.next().await {
        let secret = secret?;
        println!("  secret={}", secret.name)
    }
    // ANCHOR_END: paginator-iterate-items

    Ok(())
}

pub async fn paginator_stream_items(project_id: &str) -> crate::Result<()> {
    use futures::stream::StreamExt;
    use google_cloud_gax::paginator::ItemPaginator as _;
    use google_cloud_gax::retry_policy::AlwaysRetry;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_secretmanager_v1 as secret_manager;
    use std::time::Duration;

    let client = secret_manager::client::SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;

    // ANCHOR: paginator-stream-items
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
    // ANCHOR_END: paginator-stream-items

    Ok(())
}

pub async fn pagination_page_token(project_id: &str) -> crate::Result<()> {
    use google_cloud_gax::retry_policy::AlwaysRetry;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_secretmanager_v1 as secret_manager;
    use std::time::Duration;

    let client = secret_manager::client::SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;

    // ANCHOR: paginator-page-token
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
    // ANCHOR_END: paginator-page-token

    Ok(())
}
