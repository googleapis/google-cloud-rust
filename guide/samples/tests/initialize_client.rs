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

// [START test_only_snippet] ANCHOR: all
type Result = std::result::Result<(), Box<dyn std::error::Error>>;

async fn initialize_client(project_id: &str) -> Result {
    // [START test_only] ANCHOR: use
    use gcp_sdk_secretmanager_v1::client::SecretManagerService;
    // [END test_only] ANCHOR_END: use

    // Initialize a client with the default configuration. This is an
    // asynchronous operation that may fail, as it requires acquiring an an
    // access token.
    // ANCHOR: new-client
    let client = SecretManagerService::new().await?;
    // ANCHOR_END: new-client

    // Once initialized, use the client to make requests.
    // ANCHOR: make-rpc
    let mut items = client
        .list_locations(format!("projects/{project_id}"))
        .stream()
        .await;
    while let Some(page) = items.next().await {
        let page = page?;
        for location in page.locations {
            println!("{}", location.name);
        }
    }
    // ANCHOR_END: make-rpc

    Ok(())
}
// [END test_only_snippet] ANCHOR_END: all

#[tokio::main]
async fn main() -> Result {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: initialize_client <project-id>");
        return Ok(());
    }
    initialize_client(&args[1]).await?;
    Ok(())
}

#[cfg(all(test, feature = "run-integration-tests"))]
mod test {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn driver() -> Result {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        initialize_client(&project_id).await
    }
}
