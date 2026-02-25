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

use google_cloud_gax::error::binding::BindingError;
use google_cloud_secretmanager_v1::client::SecretManagerService;
use std::error::Error as _;

pub async fn binding_fail() -> anyhow::Result<()> {
    let client = SecretManagerService::builder().build().await?;

    // [START rust_binding_error_inspect] ANCHOR: inspect
    // [START rust_binding_error_request] ANCHOR: request
    let secret = client
        .get_secret()
        //.set_name("projects/my-project/secrets/my-secret")
        .send()
        .await;
    // [END rust_binding_error_request] ANCHOR_END: request

    let e = secret.unwrap_err();
    assert!(e.is_binding(), "{e:?}");
    assert!(e.source().is_some(), "{e:?}");
    let _ = e
        .source()
        .and_then(|e| e.downcast_ref::<BindingError>())
        .expect("should be a BindingError");
    // [END rust_binding_error_inspect] ANCHOR_END: inspect

    Ok(())
}
