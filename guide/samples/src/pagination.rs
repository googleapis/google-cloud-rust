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

pub async fn paginator_iterate_pages(project_id: &str) -> crate::Result<()> {
    use google_cloud_gax::paginator::Paginator as _;
    use google_cloud_secretmanager_v1 as secret_manager;
    use google_cloud_secretmanager_v1::model::ListSecretsResponse;

    let client = secret_manager::client::SecretManagerService::builder()
        .build()
        .await?;

    // ANCHOR: paginator-iterate-pages
    let mut list = client
        .list_secrets(format!("projects/{project_id}"))
        .paginator()
        .await;
    let mut responses: Vec<ListSecretsResponse> = Vec::new();
    while let Some(response) = list.next().await {
        responses.push(response?);
    }
    // ANCHOR_END: paginator-iterate-pages

    Ok(())
}

pub async fn paginator_stream_pages(project_id: &str) -> crate::Result<()> {
    use futures::stream::StreamExt;
    use google_cloud_gax::paginator::Paginator as _;
    use google_cloud_secretmanager_v1 as secret_manager;
    use google_cloud_secretmanager_v1::model::ListSecretsResponse;

    let client = secret_manager::client::SecretManagerService::builder()
        .build()
        .await?;

    // ANCHOR: paginator-stream-pages
    let mut list = client
        .list_secrets(format!("projects/{project_id}"))
        .paginator()
        .await
        .into_stream();
    let mut responses: Vec<ListSecretsResponse> = Vec::new();
    while let Some(response) = list.next().await {
        responses.push(response?);
    }
    // ANCHOR_END: paginator-stream-pages

    Ok(())
}

pub async fn paginator_iterate_items(project_id: &str) -> crate::Result<()> {
    use google_cloud_gax::paginator::{ItemPaginator as _, Paginator as _};
    use google_cloud_secretmanager_v1 as secret_manager;

    let client = secret_manager::client::SecretManagerService::builder()
        .build()
        .await?;

    // ANCHOR: paginator-iterate-items
    let mut list = client
        .list_secrets(format!("projects/{project_id}"))
        .paginator()
        .await
        .items();
    let mut secrets: Vec<String> = Vec::new();
    while let Some(secret) = list.next().await {
        secrets.push(secret?.name);
    }
    // ANCHOR_END: paginator-iterate-items

    Ok(())
}

pub async fn paginator_stream_items(project_id: &str) -> crate::Result<()> {
    use futures::stream::StreamExt;
    use google_cloud_gax::paginator::{ItemPaginator as _, Paginator as _};
    use google_cloud_secretmanager_v1 as secret_manager;

    let client = secret_manager::client::SecretManagerService::builder()
        .build()
        .await?;

    // ANCHOR: paginator-iterate-items
    let mut list = client
        .list_secrets(format!("projects/{project_id}"))
        .paginator()
        .await
        .items()
        .into_stream();
    let mut secrets: Vec<String> = Vec::new();
    while let Some(secret) = list.next().await {
        secrets.push(secret?.name);
    }
    // ANCHOR_END: paginator-iterate-items

    Ok(())
}

pub async fn pagination_page_token(project_id: &str) -> crate::Result<()> {
    use google_cloud_secretmanager_v1 as secret_manager;

    let client = secret_manager::client::SecretManagerService::builder()
        .build()
        .await?;

    // ANCHOR: paginator-page-token
    let _list = client
        .list_secrets(format!("projects/{project_id}"))
        .set_page_token("page-token")
        .paginator()
        .await;
    // ANCHOR_END: paginator-page-token

    Ok(())
}
