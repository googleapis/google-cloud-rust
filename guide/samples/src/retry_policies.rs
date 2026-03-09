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

pub mod client_retry;
pub mod client_retry_full;
pub mod request_retry;

use google_cloud_secretmanager_v1::client::SecretManagerService;
use google_cloud_test_utils::resource_names::random_secret_id;
use google_cloud_test_utils::runtime_config::project_id;

pub async fn drive_request_retry() -> anyhow::Result<()> {
    let project_id = project_id()?;
    let secret_id = random_secret_id();

    let client = SecretManagerService::builder().build().await?;
    // The sample will delete this secret. If that fails, the cleanup step
    // for the integration tests will garbage collect it in a couple of
    // days.
    let _ = crate::error_handling::create_secret::create_secret(&client, &project_id, &secret_id)
        .await?;
    request_retry::sample(&client, &project_id, &secret_id).await
}
