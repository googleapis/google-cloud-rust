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

// [START rust_initialize_client_all] ANCHOR: all
pub async fn initialize_client(project_id: &str) -> anyhow::Result<()> {
    // [START rust_initialize_client_use] ANCHOR: use
    use google_cloud_secretmanager_v1::client::SecretManagerService;
    // [END rust_initialize_client_use] ANCHOR_END: use

    // Initialize a client with the default configuration. This is an
    // asynchronous operation that may fail, as it requires acquiring an an
    // access token.
    // [START rust_initialize_client_new_client] ANCHOR: new-client
    let client = SecretManagerService::builder().build().await?;
    // [END rust_initialize_client_new_client] ANCHOR_END: new-client

    // Once initialized, use the client to make requests.
    // [START rust_initialize_client_make_rpc] ANCHOR: make-rpc
    use google_cloud_gax::paginator::Paginator as _;
    let mut items = client
        .list_locations()
        .set_name(format!("projects/{project_id}"))
        .by_page();
    while let Some(page) = items.next().await {
        let page = page?;
        for location in page.locations {
            println!("{}", location.name);
        }
    }
    // [END rust_initialize_client_make_rpc] ANCHOR_END: make-rpc

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let project_id = std::env::args().nth(1).unwrap();

    initialize_client(&project_id).await
}
// [END rust_initialize_client_all] ANCHOR_END: all

#[cfg(all(test, feature = "run-integration-tests"))]
mod tests {
    use super::*;
    use google_cloud_test_utils::errors::anydump;
    use google_cloud_test_utils::runtime_config::project_id;

    #[tokio::test(flavor = "multi_thread")]
    async fn driver() -> anyhow::Result<()> {
        let project_id = project_id()?;
        initialize_client(&project_id).await.inspect_err(anydump)
    }
}
