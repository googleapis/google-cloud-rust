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

//! This module contains submodules with error handling examples and some
//! helper functions to simplify the integration test.

pub mod create_secret;
pub mod update_attempt;
pub mod update_secret;

use google_cloud_gax::retry_policy::AlwaysRetry;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use google_cloud_test_utils::resource_names::random_secret_id;
use google_cloud_test_utils::runtime_config::project_id;
use std::time::Duration;

pub async fn drive_update_secret() -> anyhow::Result<()> {
    let project_id = project_id()?;
    let secret_id = random_secret_id();

    let client = SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;
    // The secret is immediately deleted. If that fails, the cleanup step
    // for the integration tests will garbage collect it in a couple of
    // days.
    let _ = create_secret::create_secret(&client, &project_id, &secret_id).await?;
    let version = update_secret::sample(
        &project_id,
        &secret_id,
        "The quick brown fox jumps over the lazy dog".into(),
    )
    .await?;
    let _ = client
        .destroy_secret_version()
        .set_name(&version.name)
        .send()
        .await?;
    client
        .delete_secret()
        .set_name(format!("projects/{project_id}/secrets/{secret_id}"))
        .send()
        .await?;
    Ok(())
}

pub async fn drive_update_secret_not_found() -> anyhow::Result<()> {
    let project_id = project_id()?;
    let secret_id = random_secret_id();

    let version = update_secret::sample(
        &project_id,
        &secret_id,
        "The quick brown fox jumps over the lazy dog".into(),
    )
    .await?;

    let client = SecretManagerService::builder()
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(15)),
        )
        .build()
        .await?;
    let _ = client
        .destroy_secret_version()
        .set_name(&version.name)
        .send()
        .await?;
    client
        .delete_secret()
        .set_name(format!("projects/{project_id}/secrets/{secret_id}"))
        .send()
        .await?;
    Ok(())
}
