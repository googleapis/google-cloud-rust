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

use google_cloud_gax as gax;
use google_cloud_secretmanager_v1 as sm;

use crate::error_handling::create_secret::create_secret;
use crate::error_handling::update_attempt::update_attempt;

// ANCHOR: update-secret
pub async fn update_secret(
    project_id: &str,
    secret_id: &str,
    data: Vec<u8>,
) -> crate::Result<sm::model::SecretVersion> {
    // ANCHOR: update-secret-client
    let client = sm::client::SecretManagerService::builder().build().await?;
    // ANCHOR_END: update-secret-client

    // ANCHOR: update-secret-initial-attempt
    match update_attempt(&client, project_id, secret_id, data.clone()).await {
        // ANCHOR_END: update-secret-initial-attempt
        // ANCHOR: update-secret-success
        Ok(version) => {
            println!("new version is {}", version.name);
            Ok(version)
        }
        // ANCHOR_END: update-secret-success
        // ANCHOR: update-secret-svc-error
        Err(e) => {
            if let Some(status) = e.status() {
                // ANCHOR_END: update-secret-svc-error
                // ANCHOR: update-secret-not-found
                use gax::error::rpc::Code;
                if status.code == Code::NotFound {
                    // ANCHOR_END: update-secret-not-found
                    // ANCHOR: update-secret-create
                    let _ = create_secret(&client, project_id, secret_id).await?;
                    // ANCHOR_END: update-secret-create
                    // ANCHOR: update-secret-try-again
                    let version = update_attempt(&client, project_id, secret_id, data).await?;
                    println!("new version is {}", version.name);
                    return Ok(version);
                    // ANCHOR_END: update-secret-try-again
                }
            }
            Err(e.into())
        }
    }
}
// ANCHOR_END: update-secret
