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

use google_cloud_devtools_artifactregistry_v1::client::ArtifactRegistry;
use std::env;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

async fn list_resources(project_id: &str) -> Result {
    let client = ArtifactRegistry::new().await?;

    let mut items = client
        .list_repositories(format!("projects/{project_id}"))
        .stream()
        .await
        .items();
    while let Some(i) = items.next().await {
        match i {
            Ok(item) => {
                println!("{item:?}");
            }
            Err(e) => {
                println!("ERROR while iterating over the repositories {e}");
                return Ok(());
            }
        }
    }

    Ok(())
}

#[cfg(all(test, feature = "run-integration-tests"))]
mod test {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn driver() -> Result {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        list_resources(&project_id).await?;
        Ok(())
    }
}
