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

use crate::error_handling::create_secret::create_secret;
use crate::error_handling::update_attempt::update_attempt;

// [START rust_update_secret] ANCHOR: update-secret
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use google_cloud_secretmanager_v1::model::SecretVersion;

pub async fn update_secret(
    project_id: &str,
    secret_id: &str,
    data: Vec<u8>,
) -> anyhow::Result<SecretVersion> {
    // [START rust_update_secret_client] ANCHOR: update-secret-client
    let client = SecretManagerService::builder().build().await?;
    // [END rust_update_secret_client] ANCHOR_END: update-secret-client

    // [START rust_update_secret_initial_attempt] ANCHOR: update-secret-initial-attempt
    match update_attempt(&client, project_id, secret_id, data.clone()).await {
        // [END rust_update_secret_initial_attempt] ANCHOR_END: update-secret-initial-attempt
        // [START rust_update_secret_success] ANCHOR: update-secret-success
        Ok(version) => {
            println!("new version is {}", version.name);
            Ok(version)
        }
        // [END rust_update_secret_success] ANCHOR_END: update-secret-success
        // [START rust_update_secret_svc_error] ANCHOR: update-secret-svc-error
        Err(e) => {
            if let Some(status) = e.downcast_ref::<Error>().and_then(|e| e.status()) {
                // [END rust_update_secret_svc_error] ANCHOR_END: update-secret-svc-error
                // [START rust_update_secret_not_found] ANCHOR: update-secret-not-found
                if status.code == Code::NotFound {
                    // [END rust_update_secret_not_found] ANCHOR_END: update-secret-not-found
                    // [START rust_update_secret_create] ANCHOR: update-secret-create
                    let _ = create_secret(&client, project_id, secret_id).await?;
                    // [END rust_update_secret_create] ANCHOR_END: update-secret-create
                    // [START rust_update_secret_try_again] ANCHOR: update-secret-try-again
                    let version = update_attempt(&client, project_id, secret_id, data).await?;
                    println!("new version is {}", version.name);
                    return Ok(version);
                    // [END rust_update_secret_try_again] ANCHOR_END: update-secret-try-again
                }
            }
            Err(e)
        }
    }
}
// [END rust_update_secret] ANCHOR_END: update-secret
