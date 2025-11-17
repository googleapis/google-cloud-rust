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

// [START rust_logging] ANCHOR: rust_logging
// [START rust_logging_parameters] ANCHOR: rust_logging_parameters
/// # Parameters
/// - `project_id`: the id of a Google Cloud project, or its numeric ID.
///   For example: `my-project`.
pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    // [END rust_logging_parameters] ANCHOR_END: rust_logging_parameters
    // [START rust_logging_use] ANCHOR: rust_logging_use
    use google_cloud_gax::paginator::ItemPaginator;
    use google_cloud_secretmanager_v1::client::SecretManagerService;
    use tracing_subscriber;
    // [END rust_logging_use] ANCHOR_END: rust_logging_use

    // [START rust_logging_init] ANCHOR: rust_logging_init
    tracing_subscriber::fmt::init();
    // [END rust_logging_init] ANCHOR_END: rust_logging_init

    // [START rust_logging_client] ANCHOR: rust_logging_client
    let client = SecretManagerService::builder()
        .with_tracing()
        .build()
        .await?;
    // [END rust_logging_client] ANCHOR_END: rust_logging_client
    // [START rust_logging_call] ANCHOR: rust_logging_call
    let mut items = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    println!("listing all secrets in project {project_id}");
    while let Some(secret) = items.next().await.transpose()? {
        println!("  {secret:?}");
    }
    println!("DONE");
    // [END rust_logging_call] ANCHOR_END: rust_logging_call
    Ok(())
}
// [END rust_logging] ANCHOR_END: rust_logging
