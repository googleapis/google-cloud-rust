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

use crate::error_handling::create_secret::create_secret;
use crate::error_handling::update_attempt::update_attempt;

// ANCHOR: update-secret
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use google_cloud_secretmanager_v1::model::SecretVersion;

pub async fn update_secret(
    project_id: &str,
    secret_id: &str,
    data: Vec<u8>,
) -> anyhow::Result<SecretVersion> {
    // ANCHOR: update-secret-client
    let client = SecretManagerService::builder().build().await?;
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
            if let Some(status) = e.downcast_ref::<Error>().and_then(|e| e.status()) {
                // ANCHOR_END: update-secret-svc-error
                // ANCHOR: update-secret-not-found
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
            Err(e)
        }
    }
}
// ANCHOR_END: update-secret
