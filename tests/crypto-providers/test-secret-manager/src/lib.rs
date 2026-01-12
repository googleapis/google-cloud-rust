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

use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_secretmanager_v1::client::SecretManagerService;

/// Verify the google_cloud_secretmanager_v1 crate is minimally functional.
///
/// The caller may need to configure the default crypto provider.
pub async fn run() -> anyhow::Result<()> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    let client = SecretManagerService::builder().build().await?;
    let mut secrets = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    let _ = secrets
        .next()
        .await
        .expect("expected at least one secret")?;
    Ok(())
}
