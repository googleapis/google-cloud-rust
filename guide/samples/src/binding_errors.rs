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
use std::error::Error as _;

pub async fn binding_fail() -> crate::Result<()> {
    let client = sm::client::SecretManagerService::builder().build().await?;

    // ANCHOR: inspect
    // ANCHOR: request
    let secret = client
        .get_secret()
        //.set_name("projects/my-project/secrets/my-secret")
        .send()
        .await;
    // ANCHOR_END: request

    use gax::error::binding::BindingError;
    let e = secret.unwrap_err();
    assert!(e.is_binding(), "{e:?}");
    assert!(e.source().is_some(), "{e:?}");
    let _ = e
        .source()
        .and_then(|e| e.downcast_ref::<BindingError>())
        .expect("should be a BindingError");
    // ANCHOR_END: inspect

    Ok(())
}

pub async fn binding_success() -> crate::Result<()> {
    let client = sm::client::SecretManagerService::builder().build().await?;

    // ANCHOR: request-success-1
    let secret = client
        .get_secret()
        .set_name("projects/my-project/secrets/my-secret")
        .send()
        .await;
    // ANCHOR_END: request-success-1

    let e = secret.unwrap_err();
    assert!(!e.is_binding(), "{e:?}");

    // ANCHOR: request-success-2
    let secret = client
        .get_secret()
        .set_name("projects/my-project/locations/us-central1/secrets/my-secret")
        .send()
        .await;
    // ANCHOR_END: request-success-2

    let e = secret.unwrap_err();
    assert!(!e.is_binding(), "{e:?}");

    Ok(())
}
