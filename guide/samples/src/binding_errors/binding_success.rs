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

use google_cloud_secretmanager_v1::client::SecretManagerService;

pub async fn binding_success() -> anyhow::Result<()> {
    let client = SecretManagerService::builder().build().await?;

    // [START rust_binding_request_success_1] ANCHOR: request-success-1
    let secret = client
        .get_secret()
        .set_name("projects/my-project/secrets/my-secret")
        .send()
        .await;
    // [END rust_binding_request_success_1] ANCHOR_END: request-success-1

    let e = secret.unwrap_err();
    assert!(!e.is_binding(), "{e:?}");

    // [START rust_binding_request_success_2] ANCHOR: request-success-2
    let secret = client
        .get_secret()
        .set_name("projects/my-project/locations/us-central1/secrets/my-secret")
        .send()
        .await;
    // [END rust_binding_request_success_2] ANCHOR_END: request-success-2

    let e = secret.unwrap_err();
    assert!(!e.is_binding(), "{e:?}");

    Ok(())
}
