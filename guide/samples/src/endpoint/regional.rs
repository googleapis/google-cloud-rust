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

// [START rust_endpoint_regional] ANCHOR: rust_endpoint_regional
// [START rust_endpoint_regional_parameters] ANCHOR: rust_endpoint_regional_parameters
/// # Parameters
/// - `project_id`: the id of a Google Cloud project, or its numeric ID.
///   For example: `my-project`.
/// - `region`: the id of a Gooogle Cloud region. For example `us-central1`.
pub async fn sample(project_id: &str, region: &str) -> anyhow::Result<()> {
    // [END rust_endpoint_regional_parameters] ANCHOR_END: rust_endpoint_regional_parameters
    // [START rust_endpoint_regional_use] ANCHOR: rust_endpoint_regional_use
    pub use google_cloud_gax::paginator::ItemPaginator;
    pub use google_cloud_secretmanager_v1::client::SecretManagerService;
    // [END rust_endpoint_regional_use] ANCHOR_END: rust_endpoint_regional_use

    // [START rust_endpoint_regional_client] ANCHOR: rust_endpoint_regional_client
    let client = SecretManagerService::builder()
        .with_endpoint(format!("https://secretmanager.{region}.rep.googleapis.com"))
        .build()
        .await?;
    // [END rust_endpoint_regional_client] ANCHOR_END: rust_endpoint_regional_client
    // [START rust_endpoint_regional_call] ANCHOR: rust_endpoint_regional_call
    let mut items = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}/locations/{region}"))
        .by_item();
    println!("listing all secrets in project {project_id} and region {region}");
    while let Some(secret) = items.next().await.transpose()? {
        println!("  {secret:?}");
    }
    println!("DONE");
    // [END rust_endpoint_regional_call] ANCHOR_END: rust_endpoint_regional_call
    Ok(())
}
// [END rust_endpoint_regional] ANCHOR_END: rust_endpoint_regional
